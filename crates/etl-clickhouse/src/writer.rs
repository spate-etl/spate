//! The I/O half of the sink: writing sealed batches to replica endpoints.

use etl_core::error::{ErrorClass, SinkError};
use etl_core::sink::{SealedBatch, ShardWriter};
use std::fmt;
use std::time::Duration;

/// One connected ClickHouse replica. Wraps a `clickhouse::Client` (its own
/// hyper connection pool) — the crate type stays private so its 0.x
/// breaking releases never become ours.
pub struct ClickHouseEndpoint {
    client: clickhouse::Client,
    url: String,
}

impl ClickHouseEndpoint {
    pub(crate) fn new(client: clickhouse::Client, url: String) -> Self {
        ClickHouseEndpoint { client, url }
    }

    /// The replica URL this endpoint writes to.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Crate-internal access for schema validation queries; the client
    /// type stays out of the public API.
    pub(crate) fn client(&self) -> &clickhouse::Client {
        &self.client
    }
}

impl fmt::Debug for ClickHouseEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseEndpoint")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

/// Writes sealed batches: one `INSERT ... FORMAT <fmt>` per batch (the
/// format — RowBinary or Native — is baked into `insert_sql`), carrying the
/// batch's deduplication token so retries — including on other replicas —
/// are idempotent within the server's dedup window. The transport is
/// format-agnostic: frames concatenate to the request body (RowBinary rows
/// or a stream of Native blocks). `write_batch` returning `Ok` is the
/// durable-ack point (the server confirmed the insert, materialized views
/// included, thanks to `wait_end_of_query=1`).
#[derive(Clone, Debug)]
pub struct ClickHouseWriter {
    insert_sql: String,
    settings: Vec<(String, String)>,
    send_timeout: Option<Duration>,
    end_timeout: Option<Duration>,
}

impl ClickHouseWriter {
    pub(crate) fn new(
        insert_sql: String,
        settings: Vec<(String, String)>,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        ClickHouseWriter {
            insert_sql,
            settings,
            send_timeout,
            end_timeout,
        }
    }

    /// The exact `INSERT` statement this writer issues.
    #[must_use]
    pub fn insert_sql(&self) -> &str {
        &self.insert_sql
    }
}

impl ShardWriter for ClickHouseWriter {
    type Endpoint = ClickHouseEndpoint;

    async fn write_batch(
        &self,
        endpoint: &ClickHouseEndpoint,
        batch: &SealedBatch,
    ) -> Result<(), SinkError> {
        let mut insert = endpoint
            .client
            .insert_formatted_with(self.insert_sql.clone())
            .with_setting("insert_deduplicate", "1")
            .with_setting("wait_end_of_query", "1")
            .with_setting("insert_deduplication_token", batch.dedup_token.clone())
            .with_timeouts(self.send_timeout, self.end_timeout);
        for (name, value) in &self.settings {
            insert = insert.with_setting(name.clone(), value.clone());
        }
        for frame in &batch.frames {
            // `Bytes` clones are refcounted views, not copies.
            insert.send(frame.clone()).await.map_err(classify)?;
        }
        insert.end().await.map_err(classify)
    }

    async fn probe(&self, endpoint: &ClickHouseEndpoint) -> Result<(), SinkError> {
        endpoint
            .client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map(drop)
            .map_err(classify)
    }
}

/// Server exception codes that cannot succeed on retry: schema, parse,
/// and authentication classes. Everything else is retried — with
/// deduplication tokens a spurious retry is idempotent, and the circuit
/// breaker plus the stalled-watermark alert surface persistent failures.
const FATAL_EXCEPTION_CODES: &[u32] = &[
    6,   // CANNOT_PARSE_TEXT
    16,  // NO_SUCH_COLUMN_IN_TABLE
    20,  // NUMBER_OF_COLUMNS_DOESNT_MATCH
    27,  // CANNOT_PARSE_INPUT_ASSERTION_FAILED
    53,  // TYPE_MISMATCH
    60,  // UNKNOWN_TABLE
    73,  // UNKNOWN_FORMAT
    81,  // UNKNOWN_DATABASE
    117, // INCORRECT_DATA
    192, // UNKNOWN_USER
    497, // ACCESS_DENIED
    516, // AUTHENTICATION_FAILED
];

/// Map a client error onto the framework's retryable/fatal taxonomy.
///
/// - transport (`Network`, `TimedOut`, compression) → `Retryable`;
/// - server exceptions (`BadResponse`) → fatal only for the schema/parse/
///   auth codes above, `Retryable` otherwise (e.g. `TOO_MANY_PARTS`,
///   memory pressure, shutdown races);
/// - client-side encoding/params problems → `Fatal` (retrying identical
///   bytes cannot help).
fn classify(err: clickhouse::error::Error) -> SinkError {
    use clickhouse::error::Error as ChError;
    let class = match &err {
        ChError::Network(_)
        | ChError::TimedOut
        | ChError::Compression(_)
        | ChError::Decompression(_)
        | ChError::Other(_) => ErrorClass::Retryable,
        ChError::BadResponse(reason) => {
            if exception_code(reason).is_some_and(|c| FATAL_EXCEPTION_CODES.contains(&c)) {
                ErrorClass::Fatal
            } else {
                ErrorClass::Retryable
            }
        }
        ChError::InvalidParams(_) | ChError::SchemaMismatch(_) | ChError::Unsupported(_) => {
            ErrorClass::Fatal
        }
        // Anything unanticipated: retry — idempotent under dedup tokens,
        // and visible through breaker metrics if persistent.
        _ => ErrorClass::Retryable,
    };
    SinkError::Client {
        class,
        reason: err.to_string(),
    }
}

/// Extract the exception code from a ClickHouse error reason. Servers send
/// `"Code: NNN. DB::Exception: ..."` bodies; the client's standardized
/// fallback (empty/unreadable body) is the bare code from the
/// `X-ClickHouse-Exception-Code` header, e.g. `"60"`.
fn exception_code(reason: &str) -> Option<u32> {
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

    #[test]
    fn classification_matches_the_documented_taxonomy() {
        let fatal = classify(clickhouse::error::Error::BadResponse(
            "Code: 60. DB::Exception: Table default.orders does not exist".into(),
        ));
        assert!(matches!(
            fatal,
            SinkError::Client {
                class: ErrorClass::Fatal,
                ..
            }
        ));

        let retryable = classify(clickhouse::error::Error::BadResponse(
            "Code: 252. DB::Exception: Too many parts".into(),
        ));
        assert!(matches!(
            retryable,
            SinkError::Client {
                class: ErrorClass::Retryable,
                ..
            }
        ));

        let timeout = classify(clickhouse::error::Error::TimedOut);
        assert!(matches!(
            timeout,
            SinkError::Client {
                class: ErrorClass::Retryable,
                ..
            }
        ));

        let schema = classify(clickhouse::error::Error::SchemaMismatch("boom".into()));
        assert!(matches!(
            schema,
            SinkError::Client {
                class: ErrorClass::Fatal,
                ..
            }
        ));
    }
}
