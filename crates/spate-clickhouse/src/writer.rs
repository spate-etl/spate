//! The I/O half of the sink: writing sealed batches to replica endpoints.

use crate::codec::BlockMethod;
use bytes::Bytes;
use spate_core::config::redact;
use spate_core::error::{ErrorClass, SinkError};
use spate_core::sink::{SealedBatch, ShardWriter};
use std::fmt;
use std::time::Duration;

/// One connected ClickHouse replica. Wraps a `clickhouse::Client` (its own
/// hyper connection pool). The crate type stays private so its 0.x
/// breaking releases never become ours.
pub struct ClickHouseEndpoint {
    client: clickhouse::Client,
    /// `client` with compression off, sharing its connection pool, for
    /// inserts whose blocks the sink's frame codec already made.
    insert_client: clickhouse::Client,
    url: String,
}

impl ClickHouseEndpoint {
    pub(crate) fn new(client: clickhouse::Client, url: String) -> Self {
        let insert_client = client
            .clone()
            .with_compression(clickhouse::Compression::None);
        ClickHouseEndpoint {
            client,
            insert_client,
            url,
        }
    }

    /// The replica URL this endpoint writes to.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// [`url`](Self::url) with its userinfo, query and fragment redacted, for
    /// error text and metric labels.
    pub(crate) fn display_url(&self) -> impl fmt::Display + '_ {
        redact::url(&self.url)
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
            .field("url", &redact::url(&self.url))
            .finish_non_exhaustive()
    }
}

/// Writes sealed batches: one `INSERT ... FORMAT <fmt>` per batch (the
/// format, RowBinaryWithNamesAndTypes or Native, is baked into `insert_sql`),
/// carrying the batch's deduplication token so retries, including retries on
/// other replicas, are idempotent within the server's dedup window. The
/// transport is format-agnostic: frames concatenate to the request body
/// (RowBinary rows behind their header, or a stream of Native blocks).
/// `write_batch` returning `Ok` is the durable-ack point (the server confirmed
/// the insert, materialized views included, thanks to `wait_end_of_query=1`).
///
/// Under `compression: lz4` or `zstd`, each frame must already be one
/// compressed block, made on the pipeline thread by the sink's frame codec
/// ([`ClickHouseSink::frame_codec`](crate::ClickHouseSink::frame_codec)). The
/// writer sends the blocks as they are, with `decompress=1`, and compresses
/// nothing itself. A frame that is not a block of the configured method fails
/// the write `Fatal` before any request is made. Under `off`, frames are sent
/// as the encoder wrote them.
#[derive(Clone)]
pub struct ClickHouseWriter {
    insert_sql: String,
    /// The RowBinary header, coded as one block when `coded` is set.
    header: Option<Bytes>,
    /// The block method every frame carries, or `None` under `off`.
    coded: Option<BlockMethod>,
    settings: Vec<(String, String)>,
    send_timeout: Option<Duration>,
    end_timeout: Option<Duration>,
}

// Hand-written: the `settings` values can carry credentials. The destructure
// lists every field so a new one cannot reach `Debug` unredacted.
impl fmt::Debug for ClickHouseWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ClickHouseWriter {
            insert_sql,
            header,
            coded,
            settings,
            send_timeout,
            end_timeout,
        } = self;
        f.debug_struct("ClickHouseWriter")
            .field("insert_sql", insert_sql)
            .field("header", header)
            .field("coded", coded)
            .field("settings", &redact::map(settings.iter().map(|(k, _)| k)))
            .field("send_timeout", send_timeout)
            .field("end_timeout", end_timeout)
            .finish()
    }
}

impl ClickHouseWriter {
    pub(crate) fn new(
        insert_sql: String,
        header: Option<Bytes>,
        coded: Option<BlockMethod>,
        settings: Vec<(String, String)>,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        ClickHouseWriter {
            insert_sql,
            header,
            coded,
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
        let client = match self.coded {
            Some(method) => {
                check_frames(method, batch)?;
                &endpoint.insert_client
            }
            None => &endpoint.client,
        };
        let mut insert = client
            .insert_formatted_with(self.insert_sql.clone())
            .with_setting("insert_deduplicate", "1")
            .with_setting("wait_end_of_query", "1")
            .with_setting("insert_deduplication_token", batch.dedup_token.clone())
            // The server checks the header against the table only while these
            // are on. They default on; setting them per insert keeps a server
            // or profile default from turning the check off silently.
            .with_setting("input_format_with_names_use_header", "1")
            .with_setting("input_format_with_types_use_header", "1")
            .with_timeouts(self.send_timeout, self.end_timeout);
        if self.coded.is_some() {
            // What a compressing client adds to its own inserts.
            insert = insert.with_setting("decompress", "1");
        }
        for (name, value) in &self.settings {
            insert = insert.with_setting(name.clone(), value.clone());
        }
        // Once per request body, ahead of the rows, so a retry re-sends
        // identical bytes under an identical token.
        let coded = self.coded.is_some();
        let classify = |err| classify_insert(err, coded);
        if let Some(header) = &self.header {
            insert.send(header.clone()).await.map_err(classify)?;
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

/// Fails `Fatal` unless every frame of `batch` is one block of `method`.
///
/// The sink's frame codec makes these blocks at seal. A frame that skipped it
/// would reach the server as a body it cannot decode, and every retry would
/// send the same bytes again.
fn check_frames(method: BlockMethod, batch: &SealedBatch) -> Result<(), SinkError> {
    for (i, frame) in batch.frames.iter().enumerate() {
        method.check(frame).map_err(|why| SinkError::Client {
            class: ErrorClass::Fatal,
            reason: format!(
                "frame {i} of the batch is not a compressed block ({why}); a compressing \
                 sink's frames must be coded by its frame codec: attach it with \
                 `ShardQueues::with_frame_codec`, or code frames with \
                 `ClickHouseSink::frame_codec` before calling `write_batch` directly"
            ),
        })?;
    }
    Ok(())
}

/// Server exception codes that cannot succeed on retry: schema, parse,
/// authentication and block-decoding classes. Everything else is retried:
/// with deduplication tokens a spurious retry is idempotent, and the circuit
/// breaker plus the stalled-watermark alert surface persistent failures.
///
/// 271 is the block-decoding code. A `decompress=1` body whose block fails its
/// checksum or does not decode answers 271. The sink makes those blocks once,
/// at seal, and a retry resends the same bytes. An insert answers 271 too when
/// the server reads a stored block that passes its checksum and fails to
/// decode, such as a materialized view reading a damaged part, so
/// [`classify_insert`] keeps 271 `Fatal` only for an insert whose body is coded.
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
    CANNOT_DECOMPRESS,
    497, // ACCESS_DENIED
    516, // AUTHENTICATION_FAILED
];

/// The server's block-decoding exception code.
const CANNOT_DECOMPRESS: u32 = 271;

/// Classifies an insert's error as [`classify`] does, except that 271 is
/// `Retryable` when the body is not `coded`.
///
/// An uncoded body carries no compressed block, so its 271 comes from data the
/// server reads itself, and another replica can take the insert.
fn classify_insert(err: clickhouse::error::Error, coded: bool) -> SinkError {
    let stored_block = !coded
        && matches!(
            &err,
            clickhouse::error::Error::BadResponse(reason)
                if exception_code(reason) == Some(CANNOT_DECOMPRESS)
        );
    match classify(err) {
        SinkError::Client { reason, .. } if stored_block => SinkError::Client {
            class: ErrorClass::Retryable,
            reason,
        },
        classified => classified,
    }
}

/// Map a client error onto the framework's retryable/fatal taxonomy.
///
/// - a TLS rejection ([`tls_rejection!`](spate_core::tls_rejection)) →
///   `Fatal`, whatever the variant carrying it;
/// - other transport (`Network`, `TimedOut`) and uncategorized client errors
///   (`Other`) → `Retryable`;
/// - server exceptions (`BadResponse`) → fatal only for the schema, parse,
///   auth and block-decoding codes above, `Retryable` otherwise (e.g.
///   `TOO_MANY_PARTS`, memory pressure, shutdown races);
/// - client-side params and both codec directions (`InvalidParams`,
///   `SchemaMismatch`, `Unsupported`, `Compression`, `Decompression`) →
///   `Fatal`.
fn classify(err: clickhouse::error::Error) -> SinkError {
    use clickhouse::error::Error as ChError;
    let class = match &err {
        _ if spate_core::tls_rejection!(rustls, &err).is_some() => ErrorClass::Fatal,
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
        // Anything unanticipated: retry, which is idempotent under dedup
        // tokens and visible through breaker metrics if persistent.
        _ => ErrorClass::Retryable,
    };
    SinkError::Client {
        class,
        reason: crate::http::error_reason(&err),
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
    use crate::test_tls::{TestCa, endpoint_trusting, failed_query, serve};
    use tokio::io::AsyncWriteExt as _;
    use tokio::net::TcpListener;

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

    /// Both codec directions classify `Fatal`.
    ///
    /// Regression for #566.
    #[test]
    fn codec_errors_classify_fatal() {
        let compression = classify(clickhouse::error::Error::Compression(
            "zstd: out of memory".into(),
        ));
        assert!(matches!(
            compression,
            SinkError::Client {
                class: ErrorClass::Fatal,
                ..
            }
        ));

        let decompression = classify(clickhouse::error::Error::Decompression(
            "incorrect magic number".into(),
        ));
        assert!(matches!(
            decompression,
            SinkError::Client {
                class: ErrorClass::Fatal,
                ..
            }
        ));
    }

    /// A server that cannot decode a coded insert's blocks answers 271, which
    /// classifies `Fatal`: a retry would resend the same blocks.
    #[test]
    fn server_decode_failures_are_fatal() {
        for reason in [
            "Code: 271. DB::Exception: Checksum doesn't match: corrupted data.",
            "Code: 271. DB::Exception: Cannot decompress: LZ4 decompression failed.",
        ] {
            let err = classify_insert(clickhouse::error::Error::BadResponse(reason.into()), true);
            assert!(
                matches!(
                    err,
                    SinkError::Client {
                        class: ErrorClass::Fatal,
                        ..
                    }
                ),
                "{reason}: {err:?}"
            );
        }
    }

    /// A checksum failure in data the server reads itself, here a materialized
    /// view reading a corrupted part, answers 40 and classifies `Retryable`, so
    /// the next attempt can reach a healthy replica. A stored block that passes
    /// its checksum and fails to decode answers 271 instead.
    #[test]
    fn a_server_side_checksum_failure_is_retryable() {
        let reason = "Code: 40. DB::Exception: Checksum doesn't match: corrupted data. Reference: 2338a6e26c614fec6feb62deae67c37e. Actual: 71bfc9638d4ed97f67949d6eba179620. Size of compressed block: 32780: While reading or decompressing /var/lib/clickhouse/store/c50/c5066d20-e7a6-4ee3-83d5-a200e66893db/all_1_1_0/data.bin (position: 32796, typename: DB::AsynchronousReadBufferFromFileWithDescriptorsCache, compressed data header: <uninitialized>): (while reading column k): (while reading from part /var/lib/clickhouse/store/c50/c5066d20-e7a6-4ee3-83d5-a200e66893db/all_1_1_0/ in table default.dim (c5066d20-e7a6-4ee3-83d5-a200e66893db) located on disk default of type local, from mark 0 with max_rows_to_read = 65536): While reading part all_1_1_0: While executing MergeTreeSelect(pool: ReadPoolInOrder, algorithm: InOrder): while pushing to view default.mv: While executing WaitForAsyncInsert. (CHECKSUM_DOESNT_MATCH) (version 26.8.17.4 (official build))";
        let err = classify(clickhouse::error::Error::BadResponse(reason.into()));
        assert!(
            matches!(
                err,
                SinkError::Client {
                    class: ErrorClass::Retryable,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    /// A stored block that passes its checksum and fails to decode, here read
    /// by a materialized view, answers an insert 271. It classifies
    /// `Retryable` for an uncoded insert and `Fatal` for a coded one.
    #[test]
    fn a_server_side_decode_failure_is_fatal_only_when_coded() {
        let reason = "Code: 271. DB::Exception: Cannot decompress LZ4-encoded data: While reading or decompressing /var/lib/clickhouse/store/a16/a1609cef-2585-4c66-be6e-1707b7b286c6/all_1_1_0/k.bin (position: 32796, typename: DB::AsynchronousReadBufferFromFileWithDescriptorsCache, compressed data header: <uninitialized>): (while reading column k): (while reading from part /var/lib/clickhouse/store/a16/a1609cef-2585-4c66-be6e-1707b7b286c6/all_1_1_0/ in table default.dim (a1609cef-2585-4c66-be6e-1707b7b286c6) located on disk default of type local, from mark 0 with max_rows_to_read = 49152): While reading part all_1_1_0: While executing MergeTreeSelect(pool: ReadPoolInOrder, algorithm: InOrder): while pushing to view default.mv: While executing WaitForAsyncInsert. (CANNOT_DECOMPRESS) (version 26.8.17.4 (official build))";
        for (coded, expected) in [(false, ErrorClass::Retryable), (true, ErrorClass::Fatal)] {
            let err = classify_insert(clickhouse::error::Error::BadResponse(reason.into()), coded);
            let SinkError::Client { class, .. } = &err else {
                panic!("{err:?}");
            };
            assert_eq!(*class, expected, "coded: {coded}, {err:?}");
        }
    }

    /// A replica whose certificate does not verify fails both a header-led
    /// insert and a probe `Fatal`, with the rejection and its remedy in the
    /// reason.
    ///
    /// Regression for #627.
    #[tokio::test]
    async fn an_unverified_certificate_classifies_fatal_with_its_cause() {
        let (server, other) = (TestCa::new("server"), TestCa::new("other"));
        let url = serve(&server, None).await;
        let endpoint = endpoint_trusting(&other, &url);
        let writer = ClickHouseWriter::new(
            "INSERT INTO t FORMAT RowBinaryWithNamesAndTypes".into(),
            Some(Bytes::from_static(b"header")),
            None,
            Vec::new(),
            None,
            None,
        );
        let batch = SealedBatch {
            frames: vec![Bytes::from_static(b"rows")],
            rows: 1,
            bytes: 4,
            dedup_token: "t".into(),
        };
        let write = writer.write_batch(&endpoint, &batch).await.unwrap_err();
        let probe = writer.probe(&endpoint).await.unwrap_err();
        for err in [write, probe] {
            let SinkError::Client { class, reason } = &err else {
                panic!("{err:?}");
            };
            assert_eq!(*class, ErrorClass::Fatal, "{reason}");
            assert!(reason.contains("UnknownIssuer"), "{reason}");
            assert!(reason.contains("`tls.root_ca`"), "{reason}");
        }
    }

    /// A rustls error that is not a certificate rejection, behind the same
    /// `io::Error` wrappers, stays `Retryable`.
    #[tokio::test]
    async fn a_tls_protocol_failure_stays_retryable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let _ = tcp.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
            }
        });
        let err = failed_query(&TestCa::new("any"), &url).await;
        assert!(format!("{err:?}").contains("InvalidMessage"), "{err:?}");
        let SinkError::Client { class, reason } = classify(err) else {
            unreachable!()
        };
        assert_eq!(class, ErrorClass::Retryable, "{reason}");
    }

    /// A handshake alert that rejects the client classifies `Fatal` and names
    /// the alert; `decode_error`, which reports a malformed message, stays
    /// `Retryable`.
    #[tokio::test]
    async fn a_rejecting_tls_alert_classifies_fatal() {
        use rustls::AlertDescription as A;
        let ca = TestCa::new("any");
        for (alert, expected) in [
            (A::HandshakeFailure, ErrorClass::Fatal),
            (A::ProtocolVersion, ErrorClass::Fatal),
            (A::DecodeError, ErrorClass::Retryable),
        ] {
            let name = format!("{alert:?}");
            let addr = spate_test::tls_alert_server(b"", u8::from(alert));
            let err = failed_query(&ca, &format!("https://{addr}")).await;
            assert!(format!("{err:?}").contains(&name), "{err:?}");
            let SinkError::Client { class, reason } = classify(err) else {
                unreachable!()
            };
            assert_eq!(class, expected, "{reason}");
            if expected == ErrorClass::Fatal {
                assert!(reason.contains(&name), "{reason}");
            }
        }
    }

    /// A TLS 1.3 server refuses a missing client certificate after the
    /// handshake, and the alert on the first read classifies `Fatal`.
    #[tokio::test]
    async fn a_refused_client_certificate_classifies_fatal() {
        let server = TestCa::new("server");
        let url = serve(&server, Some(&TestCa::new("clients"))).await;
        let err = failed_query(&server, &url).await;
        let SinkError::Client { class, reason } = classify(err) else {
            unreachable!()
        };
        assert_eq!(class, ErrorClass::Fatal, "{reason}");
        assert!(reason.contains("CertificateRequired"), "{reason}");
    }
}
