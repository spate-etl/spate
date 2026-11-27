//! Classification of ClickHouse client errors into framework error classes.

use spate_core::error::ErrorClass;

/// Server exception codes that cannot succeed on retry: schema, parse,
/// settings, authentication, size-limit and block-decoding classes. Everything else is retried:
/// with deduplication tokens a spurious retry is idempotent, and the circuit
/// breaker plus the stalled-watermark alert surface persistent failures.
///
/// 271 is the block-decoding code. A `decompress=1` body whose block fails its
/// checksum or does not decode answers 271. The sink makes those blocks once,
/// at seal, and a retry resends the same bytes. An insert answers 271 too when
/// the server reads a stored block that passes its checksum and fails to
/// decode, such as a materialized view reading a damaged part, so
/// [`insert_error`](crate::writer::insert_error) keeps 271 `Fatal` only for an
/// insert whose body is coded.
///
/// An insert answers 50 or 62 when a type name it reads names no type or does
/// not parse. The server reads the whole name before it parses it, so a body
/// cut short never answers either. A stored `Dynamic` or `JSON` part that a
/// materialized view reads during the insert can answer them too, and that
/// insert fails as well.
///
/// An insert answers 128 or 131 when its body is over a size limit: a block
/// header's column, row or name bounds, or a RowBinary value over
/// `format_binary_max_array_size` or `format_binary_max_string_size`. A
/// function a materialized view runs on the rows, a damaged stored part, or a
/// replica whose profile sets a lower cap can answer them too, and that insert
/// fails as well.
///
/// A replica answers 452 when a settings constraint pins a setting the sink
/// sends, such as `deduplicate_insert`, and 467 when a `settings` value does
/// not parse as the bool the setting takes. Both answer every attempt the
/// same way, at startup and at insert.
const FATAL_EXCEPTION_CODES: &[u32] = &[
    6,   // CANNOT_PARSE_TEXT
    16,  // NO_SUCH_COLUMN_IN_TABLE
    20,  // NUMBER_OF_COLUMNS_DOESNT_MATCH
    27,  // CANNOT_PARSE_INPUT_ASSERTION_FAILED
    50,  // UNKNOWN_TYPE
    53,  // TYPE_MISMATCH
    60,  // UNKNOWN_TABLE
    62,  // SYNTAX_ERROR
    73,  // UNKNOWN_FORMAT
    81,  // UNKNOWN_DATABASE
    115, // UNKNOWN_SETTING
    117, // INCORRECT_DATA
    128, // TOO_LARGE_ARRAY_SIZE
    131, // TOO_LARGE_STRING_SIZE
    192, // UNKNOWN_USER
    CANNOT_DECOMPRESS,
    452, // SETTING_CONSTRAINT_VIOLATION
    467, // CANNOT_PARSE_BOOL
    497, // ACCESS_DENIED
    516, // AUTHENTICATION_FAILED
];

/// The server's block-decoding exception code.
pub(crate) const CANNOT_DECOMPRESS: u32 = 271;

/// The class of a client error, for an insert and for a startup request
/// alike:
///
/// - a TLS rejection ([`tls_rejection!`](spate_core::tls_rejection)) →
///   `Fatal`, whatever the variant carrying it;
/// - other transport (`Network`, `TimedOut`) and uncategorized client errors
///   (`Other`) → `Retryable`;
/// - server exceptions (`BadResponse`) → fatal only for the schema, parse,
///   settings, auth, size-limit and block-decoding codes above, `Retryable`
///   otherwise (e.g. `TOO_MANY_PARTS`, memory pressure, shutdown races, or a
///   proxy's 503 with no code);
/// - client-side params and both codec directions (`InvalidParams`,
///   `SchemaMismatch`, `Unsupported`, `Compression`, `Decompression`) →
///   `Fatal`;
/// - any other variant, such as a reply that does not decode → `otherwise`.
pub(crate) fn classify_client_error(
    err: &clickhouse::error::Error,
    otherwise: ErrorClass,
) -> ErrorClass {
    use clickhouse::error::Error as ChError;
    match err {
        _ if spate_core::tls_rejection!(rustls, err).is_some() => ErrorClass::Fatal,
        ChError::Network(_) | ChError::TimedOut | ChError::Other(_) => ErrorClass::Retryable,
        ChError::BadResponse(reason) => {
            if exception_code(reason).is_some_and(|c| FATAL_EXCEPTION_CODES.contains(&c)) {
                ErrorClass::Fatal
            } else {
                ErrorClass::Retryable
            }
        }
        ChError::InvalidParams(_)
        | ChError::SchemaMismatch(_)
        | ChError::Unsupported(_)
        | ChError::Compression(_)
        | ChError::Decompression(_) => ErrorClass::Fatal,
        _ => otherwise,
    }
}

/// Extract the exception code from a ClickHouse error reason. Servers send
/// `"Code: NNN. DB::Exception: ..."` bodies; the client's standardized
/// fallback (empty/unreadable body) is the bare code from the
/// `X-ClickHouse-Exception-Code` header, e.g. `"60"`.
pub(crate) fn exception_code(reason: &str) -> Option<u32> {
    let digits_at = |s: &str| -> Option<u32> {
        let digits: String = s.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    };
    match reason.find("Code: ") {
        Some(at) => digits_at(&reason[at + 6..]),
        None => digits_at(reason.trim()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exception_codes_are_extracted() {
        assert_eq!(
            exception_code("Code: 60. DB::Exception: Table default.x does not exist"),
            Some(60)
        );
        assert_eq!(
            exception_code("bad response: Code: 252. DB::Exception: Too many parts"),
            Some(252)
        );
        assert_eq!(exception_code("no code here"), None);
        // Standardized fallback: the bare header value.
        assert_eq!(exception_code("60"), Some(60));
        assert_eq!(exception_code(" 252 "), Some(252));
    }

    /// `classify_client_error` gives each variant it names the same class whatever the
    /// caller's `otherwise`, and every other variant `otherwise`. A server
    /// answer with no exception code, as a proxy's 503 page has, is
    /// retryable.
    #[test]
    fn class_of_names_each_variant_and_defers_the_rest() {
        use ErrorClass::{Fatal, Retryable};
        use clickhouse::error::Error as E;
        let boxed = || -> Box<dyn std::error::Error + Send + Sync> { "x".into() };
        let named: Vec<(E, ErrorClass)> = vec![
            (E::Network(boxed()), Retryable),
            (E::TimedOut, Retryable),
            (E::Other(boxed()), Retryable),
            (
                E::BadResponse("Code: 516. DB::Exception: Authentication failed".into()),
                Fatal,
            ),
            (
                E::BadResponse(
                    "Code: 452. DB::Exception: Setting deduplicate_insert should not be changed"
                        .into(),
                ),
                Fatal,
            ),
            (
                E::BadResponse(
                    "Code: 467. DB::Exception: Cannot parse bool from string 'maybe'".into(),
                ),
                Fatal,
            ),
            (
                E::BadResponse("Code: 241. DB::Exception: Memory limit exceeded".into()),
                Retryable,
            ),
            (
                E::BadResponse("<html><body>503 Service Unavailable</body></html>".into()),
                Retryable,
            ),
            (E::InvalidParams(boxed()), Fatal),
            (E::SchemaMismatch("x".into()), Fatal),
            (E::Unsupported("x".into()), Fatal),
            (E::Compression(boxed()), Fatal),
            (E::Decompression(boxed()), Fatal),
        ];
        for (err, class) in &named {
            for otherwise in [Fatal, Retryable] {
                assert_eq!(
                    classify_client_error(err, otherwise),
                    *class,
                    "{err:?} under {otherwise:?}"
                );
            }
        }
        let unnamed = [
            E::NotEnoughData,
            E::DataFormat(boxed()),
            E::RowNotFound,
            E::Custom("x".into()),
        ];
        for err in &unnamed {
            for otherwise in [Fatal, Retryable] {
                assert_eq!(classify_client_error(err, otherwise), otherwise, "{err:?}");
            }
        }
    }
}
