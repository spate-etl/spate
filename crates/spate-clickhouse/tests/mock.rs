//! Integration tests against the `clickhouse` crate's mock server (no
//! Docker). The mock's `record` handler decodes request bodies through the
//! crate's own RowBinary deserializer, so every row that round-trips here
//! proves our serializer is wire-compatible with the crate's.
//!
//! That handler decodes from byte zero, so the rows it reads back are posted
//! as a plain `FORMAT RowBinary` body rather than through the writer, whose
//! body opens with a `RowBinaryWithNamesAndTypes` header. The writer's own
//! framing is pinned by a byte test on the header and by the container tier.
//!
//! Building a sink reads `system.columns`, so every mock here queues that
//! answer first: handlers are consumed in order.

mod support;

use bytes::BytesMut;
use clickhouse::test::{Mock, handlers};
use serde::{Deserialize, Serialize};
use spate_clickhouse::ClickHouseRow;
use spate_clickhouse::ClickHouseSinkBuilder;
use spate_clickhouse::SchemaError;
use spate_clickhouse::config::{self, ClickHouseSinkConfig};
use spate_clickhouse::testing::serialize_row;
use spate_clickhouse::testing::{ColumnRow, system_columns};
use spate_core::deser::Owned;
use spate_core::error::{ConnectError, ConnectErrorKind, ErrorClass, SinkError, find_source};
use spate_core::sink::{SealedBatch, ShardWriter};
use spate_test::record;
use support::sealed;

#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize, ClickHouseRow)]
struct TestRow {
    id: u64,
    name: String,
    score: Option<f64>,
}

/// The table matching `TestRow`.
fn matching_columns() -> Vec<ColumnRow> {
    system_columns(&[
        ("id", "UInt64", ""),
        ("name", "String", ""),
        ("score", "Nullable(Float64)", ""),
    ])
}

/// Answers what building a sink asks one replica: the `system.columns` read,
/// then the settings check.
fn answer_build(mock: &Mock, columns: Vec<ColumnRow>) {
    mock.add(handlers::provide(columns));
    mock.add(handlers::provide::<u8>([1u8]));
}

async fn sink_for(url: &str) -> config::ClickHouseSink {
    sink_for_table(url, "orders").await
}

async fn sink_for_table(url: &str, table: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        // `none`: the crate's `record` handler decodes the request body with no
        // decompression, so a decodable round-trip requires an uncompressed body.
        r#"
table: {table}
compression: none
shards:
  - replicas: ["{url}"]
"#
    ))
    .expect("config yaml");
    ClickHouseSinkBuilder::new(cfg)
        .expect("valid sink config")
        .connect::<Owned<TestRow>>()
        .await
        .expect("schema fetch and column check")
}

fn rows(n: u64) -> Vec<TestRow> {
    (0..n)
        .map(|i| TestRow {
            id: i,
            name: format!("row-{i}"),
            score: (i % 3 == 0).then_some(i as f64 * 0.5),
        })
        .collect()
}

/// Post the frames of a sealed batch as a plain `FORMAT RowBinary` body, the
/// shape `RecordControl::collect` can decode.
async fn post_frames(url: &str, batch: &SealedBatch) {
    let client = clickhouse::Client::default()
        .with_url(url)
        .with_compression(clickhouse::Compression::None);
    let mut insert = client.insert_formatted_with("INSERT INTO orders FORMAT RowBinary".to_owned());
    for frame in &batch.frames {
        insert.send(frame.clone()).await.expect("send frame");
    }
    insert.end().await.expect("end");
}

#[tokio::test]
async fn our_frames_decode_through_the_crates_deserializer() {
    let mock = Mock::new();
    let recorder = mock.add(handlers::record::<TestRow>());

    let expected = rows(101);
    post_frames(mock.url(), &sealed(&expected, "tok-1", 2)).await;

    let received: Vec<TestRow> = recorder.collect().await;
    assert_eq!(
        received, expected,
        "crate-decoded rows match what we encoded"
    );
}

/// A multi-frame batch is one request the server accepts. What the body
/// contains is the container tier's to check: `provide` ignores it.
#[tokio::test]
async fn write_batch_sends_every_frame() {
    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    mock.add(handlers::provide::<u8>([]));
    let sink = sink_for(mock.url()).await;

    sink.writer
        .write_batch(&sink.endpoints[0][0], &sealed(&rows(101), "tok-1", 2))
        .await
        .expect("write");
}

#[tokio::test]
async fn transport_failures_are_retryable() {
    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    mock.add(handlers::failure(hyper::StatusCode::INTERNAL_SERVER_ERROR));
    let sink = sink_for(mock.url()).await;

    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &sealed(&rows(3), "tok-2", 2))
        .await
        .expect_err("must fail");
    assert_eq!(err.class(), ErrorClass::Retryable, "{err}");
}

#[tokio::test]
async fn schema_class_exceptions_are_fatal() {
    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    // 60 = UNKNOWN_TABLE.
    mock.add(handlers::exception(60));
    let sink = sink_for(mock.url()).await;

    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &sealed(&rows(3), "tok-3", 2))
        .await
        .expect_err("must fail");
    let reason = err.reason();
    assert_eq!(err.class(), ErrorClass::Fatal, "reason: {reason}");
}

#[tokio::test]
async fn capacity_class_exceptions_stay_retryable() {
    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    // 252 = TOO_MANY_PARTS: transient merge pressure, retry is correct.
    mock.add(handlers::exception(252));
    let sink = sink_for(mock.url()).await;

    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &sealed(&rows(3), "tok-4", 2))
        .await
        .expect_err("must fail");
    let reason = err.reason();
    assert_eq!(err.class(), ErrorClass::Retryable, "reason: {reason}");
}

// ---- startup schema validation ----------------------------------------------

/// The error from building a sink whose schema fetch cannot succeed.
async fn failed_sink(url: &str) -> ConnectError {
    failed_sink_as(url, "rowbinary").await
}

/// The schema check's error under a startup error.
fn schema(err: &ConnectError) -> &SchemaError {
    find_source(err).unwrap_or_else(|| panic!("no schema error under {err:?}"))
}

async fn failed_sink_as(url: &str, format: &str) -> ConnectError {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: orders
format: {format}
compression: none
shards:
  - replicas: ["{url}"]
"#
    ))
    .expect("config yaml");
    ClickHouseSinkBuilder::new(cfg)
        .expect("valid sink config")
        .connect::<Owned<TestRow>>()
        .await
        .expect_err("the fetch must fail")
}

#[tokio::test]
async fn matching_schema_passes_and_first_record_encodes() {
    use spate_core::sink::RowEncoder;

    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    let sink = sink_for(mock.url()).await;

    // The first-record struct check passes and the bytes are the row's alone:
    // the header is the writer's, not the encoder's.
    let mut encoder = sink.encoder::<Owned<TestRow>>();
    let row = TestRow {
        id: 7,
        name: "x".into(),
        score: None,
    };
    let mut buf = BytesMut::new();
    encoder
        .encode(&record(row.clone()), &mut buf)
        .expect("encode");
    let mut expected = BytesMut::new();
    serialize_row(&row, &mut expected).unwrap();
    assert_eq!(buf, expected);

    // Second record: the check is done, encoding still works.
    encoder
        .encode(&record(row), &mut buf)
        .expect("second encode");
}

#[tokio::test]
async fn struct_order_mismatch_is_fatal_at_the_first_record() {
    use spate_core::sink::RowEncoder;

    #[derive(Clone, Serialize)]
    struct WrongOrder {
        name: String,
        id: u64,
        score: Option<f64>,
    }

    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    let mut encoder = sink_for(mock.url()).await.encoder::<Owned<WrongOrder>>();
    let err = encoder
        .encode(
            &record(WrongOrder {
                name: "x".into(),
                id: 7,
                score: None,
            }),
            &mut BytesMut::new(),
        )
        .expect_err("order mismatch must fail");
    let reason = err.reason();
    assert_eq!(err.class(), ErrorClass::Fatal, "{reason}");
    assert!(
        reason.contains("position 0: struct field `name` vs declared column `id`"),
        "{reason}"
    );
}

#[tokio::test]
async fn a_type_mismatch_is_fatal_at_the_first_record() {
    use spate_core::sink::RowEncoder;

    // score: Option<u32> against Nullable(Float64), a wrong width/class.
    #[derive(Clone, Serialize)]
    struct WrongType {
        id: u64,
        name: String,
        score: Option<u32>,
    }
    let wrong = WrongType {
        id: 1,
        name: "x".into(),
        score: Some(3),
    };

    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    let err = sink_for(mock.url())
        .await
        .encoder::<Owned<WrongType>>()
        .encode(&record(wrong), &mut BytesMut::new())
        .expect_err("the type check runs on every sink");
    let reason = err.reason();
    assert_eq!(err.class(), ErrorClass::Fatal, "{reason}");
    assert!(
        reason.contains("not compatible with `score` Nullable(Float64)"),
        "{reason}"
    );
}

#[tokio::test]
async fn the_native_encoder_checks_wrapper_scale_against_the_fetched_precision() {
    use spate_clickhouse::DateTime64Millis;
    use spate_core::sink::RowEncoder;

    #[derive(Clone, Serialize, ClickHouseRow)]
    struct EventRow {
        id: u64,
        ts: DateTime64Millis,
    }
    let row = EventRow {
        id: 1,
        ts: DateTime64Millis(1_700_000_000_000),
    };

    async fn native_sink(url: &str) -> config::ClickHouseSink {
        let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
            r#"
table: events
format: native
compression: none
shards:
  - replicas: ["{url}"]
"#
        ))
        .expect("config yaml");
        ClickHouseSinkBuilder::new(cfg)
            .expect("valid sink config")
            .connect::<Owned<EventRow>>()
            .await
            .expect("schema fetch")
    }

    // The live table is micro precision; the struct declares milli via the
    // wrapper, so the first record is rejected before any block is built.
    let mock = Mock::new();
    answer_build(
        &mock,
        system_columns(&[("id", "UInt64", ""), ("ts", "DateTime64(6)", "")]),
    );
    let err = native_sink(mock.url())
        .await
        .encoder::<Owned<EventRow>>()
        .encode(&record(row.clone()), &mut BytesMut::new())
        .expect_err("the scale mismatch is rejected");
    let reason = err.reason();
    assert_eq!(err.class(), ErrorClass::Fatal, "{reason}");
    assert!(
        reason.contains("DateTime64Millis") && reason.contains("DateTime64(6)"),
        "{reason}"
    );

    // The precision the wrapper declares encodes.
    let mock = Mock::new();
    answer_build(
        &mock,
        system_columns(&[("id", "UInt64", ""), ("ts", "DateTime64(3)", "")]),
    );
    native_sink(mock.url())
        .await
        .encoder::<Owned<EventRow>>()
        .encode(&record(row), &mut BytesMut::new())
        .expect("matching precision encodes");
}

#[tokio::test]
async fn both_encoders_check_decimal_scale_against_the_fetched_column() {
    use spate_clickhouse::Decimal64;
    use spate_core::sink::RowEncoder;

    #[derive(Clone, Serialize, ClickHouseRow)]
    struct PriceRow {
        id: u64,
        amount: Decimal64<4>,
    }
    let row = PriceRow {
        id: 1,
        amount: Decimal64::<4>(15_000),
    };

    async fn price_sink(url: &str, format: &str) -> config::ClickHouseSink {
        let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
            r#"
table: events
format: {format}
compression: none
shards:
  - replicas: ["{url}"]
"#
        ))
        .expect("config yaml");
        ClickHouseSinkBuilder::new(cfg)
            .expect("valid sink config")
            .connect::<Owned<PriceRow>>()
            .await
            .expect("schema fetch")
    }

    fn assert_fatal_scale_mismatch(err: SinkError) {
        let reason = err.reason();
        assert_eq!(err.class(), ErrorClass::Fatal, "{reason}");
        // Both formats report the mismatch in the same words.
        assert!(
            reason.starts_with("row struct does not match declared columns for `events`:"),
            "{reason}"
        );
        assert!(
            reason.contains("Decimal64<4>") && reason.contains("Decimal(18, 2)"),
            "{reason}"
        );
    }

    fn mismatched_columns() -> Vec<ColumnRow> {
        system_columns(&[("id", "UInt64", ""), ("amount", "Decimal(18, 2)", "")])
    }

    // The live column is scale 2 and the struct declares scale 4, so the
    // raw values are 100x the column's. RowBinary rejects the first
    // record, before any frame is written.
    let mock = Mock::new();
    answer_build(&mock, mismatched_columns());
    let err = price_sink(mock.url(), "rowbinary")
        .await
        .encoder::<Owned<PriceRow>>()
        .encode(&record(row.clone()), &mut BytesMut::new())
        .expect_err("the scale mismatch is rejected");
    assert_fatal_scale_mismatch(err);

    // Native runs the same check.
    let mock = Mock::new();
    answer_build(&mock, mismatched_columns());
    let err = price_sink(mock.url(), "native")
        .await
        .encoder::<Owned<PriceRow>>()
        .encode(&record(row.clone()), &mut BytesMut::new())
        .expect_err("the scale mismatch is rejected");
    assert_fatal_scale_mismatch(err);

    // An agreeing scale encodes, in both formats.
    let agreeing = || system_columns(&[("id", "UInt64", ""), ("amount", "Decimal(18, 4)", "")]);

    let mock = Mock::new();
    answer_build(&mock, agreeing());
    price_sink(mock.url(), "rowbinary")
        .await
        .encoder::<Owned<PriceRow>>()
        .encode(&record(row.clone()), &mut BytesMut::new())
        .expect("rowbinary encodes an agreeing scale");

    let mock = Mock::new();
    answer_build(&mock, agreeing());
    price_sink(mock.url(), "native")
        .await
        .encoder::<Owned<PriceRow>>()
        .encode(&record(row), &mut BytesMut::new())
        .expect("native encodes an agreeing scale");
}

#[tokio::test]
async fn missing_and_materialized_columns_fail_startup() {
    let mock = Mock::new();
    mock.add(handlers::provide(system_columns(&[
        ("id", "UInt64", ""),
        // `name` is absent; `score` is MATERIALIZED.
        ("score", "Nullable(Float64)", "MATERIALIZED"),
    ])));
    let err = failed_sink(mock.url()).await;
    let msg = err.to_string();
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    assert!(matches!(schema(&err), SchemaError::Mismatch(_)));
    assert!(
        msg.contains("declared column `name` does not exist in the table"),
        "{msg}"
    );
    assert!(
        msg.contains("declared column `score` is MATERIALIZED and cannot be inserted into"),
        "{msg}"
    );
    assert!(msg.contains("table columns:"), "{msg}");
    assert!(msg.contains("declared columns:   id, name, score"), "{msg}");
}

/// Pins that a column type nested past ClickHouse's own parser depth, or a
/// `FixedString` wider than ClickHouse creates, fails startup with an error
/// naming the column. Regression for #1776.
#[tokio::test]
async fn a_column_type_past_what_clickhouse_accepts_fails_startup() {
    let deep = format!("{}String{}", "Array(".repeat(100_000), ")".repeat(100_000));
    for (ty, says) in [
        (
            deep.as_str(),
            "declared column `name` has a type nested more than 256 levels deep",
        ),
        (
            "Nullable(FixedString(4294967295))",
            "declared column `name` has a FixedString wider than 16777215 bytes",
        ),
    ] {
        for format in ["rowbinary", "native"] {
            let mock = Mock::new();
            mock.add(handlers::provide(system_columns(&[
                ("id", "UInt64", ""),
                ("name", ty, ""),
                ("score", "Nullable(Float64)", ""),
            ])));
            let err = failed_sink_as(mock.url(), format).await;
            assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
            assert!(matches!(schema(&err), SchemaError::Mismatch(_)));
            let msg = err.to_string();
            assert!(msg.contains(says), "{format}: {msg}");
            assert!(
                msg.len() < 2048,
                "{format}: the error shows {} bytes",
                msg.len()
            );
        }
    }
}

/// Pins that `format: native` refuses, at `connect`, a declared column whose
/// type the Native format does not support, with the whole schema diff,
/// and that `rowbinary` builds the same table.
#[tokio::test]
async fn native_refuses_a_column_it_cannot_write_at_connect() {
    let columns = || {
        system_columns(&[
            ("id", "UInt64", ""),
            ("name", "Time64(3)", ""),
            ("score", "Nullable(Float64)", ""),
        ])
    };
    let mock = Mock::new();
    mock.add(handlers::provide(columns()));
    let err = failed_sink_as(mock.url(), "native").await;
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    assert!(matches!(schema(&err), SchemaError::Mismatch(_)));
    assert_eq!(
        err.reason(),
        format!(
            "sink.clickhouse: schema validation failed for `orders` (replica {}):\n  \
             - declared column `name` has type `Time64(3)`, which the Native format does not support\n  \
             table columns:      id UInt64, name Time64(3), score Nullable(Float64)\n  \
             declared columns:   id, name, score",
            mock.url()
        )
    );

    let mock = Mock::new();
    answer_build(&mock, columns());
    let sink = sink_with_format(mock.url(), "rowbinary").await;
    assert_eq!(sink.format(), config::ClickHouseFormat::RowBinary);
}

/// Pins that the refusal shows a server-sent type cut to a bounded length.
#[tokio::test]
async fn native_refusal_cuts_a_long_type() {
    let long = format!("Variant({}String)", "Int64, ".repeat(60));
    assert!(long.len() > 400);
    let mock = Mock::new();
    mock.add(handlers::provide(system_columns(&[
        ("id", "UInt64", ""),
        ("name", long.as_str(), ""),
        ("score", "Nullable(Float64)", ""),
    ])));
    let msg = failed_sink_as(mock.url(), "native").await.to_string();
    let shown = format!("{}…", &long[..256]);
    assert!(
        msg.contains(&format!(
            "declared column `name` has type `{shown}`, which the Native"
        )),
        "{msg}"
    );
    assert!(!msg.contains(&long), "{msg}");
    assert!(msg.len() < 2048, "the error shows {} bytes", msg.len());
}

async fn sink_with_format(url: &str, format: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "table: orders\nformat: {format}\ncompression: none\nshards:\n  - replicas: [\"{url}\"]\n"
    ))
    .expect("config yaml");
    ClickHouseSinkBuilder::new(cfg)
        .expect("valid sink config")
        .connect::<Owned<TestRow>>()
        .await
        .expect("schema fetch and column check")
}

/// Pins that the sink's encoder writes the configured format, byte for byte
/// as the concrete encoder for that format writes a chunk.
#[tokio::test]
async fn the_encoder_follows_the_configured_format() {
    use spate_clickhouse::testing::{ClickHouseNativeEncoder, NativeSchema};
    use spate_core::sink::RowEncoder;

    let rows = [
        TestRow {
            id: 7,
            name: "x".into(),
            score: None,
        },
        TestRow {
            id: 8,
            name: "yz".into(),
            score: Some(0.5),
        },
    ];
    fn chunk<E: RowEncoder<Owned<TestRow>>>(enc: &mut E, rows: &[TestRow]) -> BytesMut {
        let mut buf = BytesMut::new();
        for row in rows {
            enc.encode(&record(row.clone()), &mut buf).expect("encode");
        }
        enc.finish_chunk(&mut buf).expect("finish the chunk");
        buf
    }

    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    let sink = sink_with_format(mock.url(), "rowbinary").await;
    assert_eq!(sink.format(), config::ClickHouseFormat::RowBinary);
    let mut expected = BytesMut::new();
    for row in &rows {
        serialize_row(row, &mut expected).expect("encode");
    }
    assert_eq!(
        chunk(&mut sink.encoder::<Owned<TestRow>>(), &rows),
        expected
    );

    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    let sink = sink_with_format(mock.url(), "native").await;
    assert_eq!(sink.format(), config::ClickHouseFormat::Native);
    let schema = NativeSchema::from_columns(&[
        ("id", "UInt64"),
        ("name", "String"),
        ("score", "Nullable(Float64)"),
    ])
    .expect("schema builds");
    let expected = chunk(
        &mut ClickHouseNativeEncoder::<Owned<TestRow>>::new(schema),
        &rows,
    );
    assert_eq!(
        chunk(&mut sink.encoder::<Owned<TestRow>>(), &rows),
        expected
    );
}

/// Pins that a declared column's type at the deepest nesting the schema
/// check accepts builds the sink in both formats on a test thread's 2 MiB
/// stack, for each type that recurses per level.
#[tokio::test]
async fn the_deepest_accepted_column_type_builds_the_sink() {
    let nest = |open: &str, close: &str, n: usize, leaf: &str| {
        format!("{}{leaf}{}", open.repeat(n), close.repeat(n))
    };
    for ty in [
        nest("Array(", ")", 256, "String"),
        nest("Tuple(a ", ")", 256, "String"),
        nest("Map(String, Tuple(a ", "))", 128, "String"),
        nest("Array(Nullable(", "))", 127, "LowCardinality(String)"),
    ] {
        for format in ["rowbinary", "native"] {
            let mock = Mock::new();
            answer_build(
                &mock,
                system_columns(&[
                    ("id", "UInt64", ""),
                    ("name", ty.as_str(), ""),
                    ("score", "Nullable(Float64)", ""),
                ]),
            );
            let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
                "table: orders\nformat: {format}\ncompression: none\nshards:\n  - replicas: [\"{}\"]\n",
                mock.url()
            ))
            .expect("config yaml");
            ClickHouseSinkBuilder::new(cfg)
                .expect("valid sink config")
                .connect::<Owned<TestRow>>()
                .await
                .unwrap_or_else(|e| panic!("{format}: {e}"));
        }
    }
}

#[tokio::test]
async fn empty_result_means_table_not_found() {
    let mock = Mock::new();
    mock.add(handlers::provide(Vec::<ColumnRow>::new()));
    let err = failed_sink(mock.url()).await;
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    assert!(err.to_string().contains("not found"), "{err}");
}

#[tokio::test]
async fn fetch_failures_fail_startup_distinguishably() {
    let mock = Mock::new();
    mock.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
    let err = failed_sink(mock.url()).await;
    assert_eq!(err.kind(), ConnectErrorKind::Unavailable, "{err}");
    match schema(&err) {
        SchemaError::Fetch { table, .. } => {
            assert_eq!(table, "`orders`");
        }
        other => panic!("expected Fetch, got {other:?}"),
    }
}

/// A replica that refuses a setting fails startup with the server's code and
/// that replica's URL, after an earlier replica accepted it.
#[tokio::test]
async fn a_replica_refusing_a_setting_fails_startup() {
    let (accepts, refuses) = (Mock::new(), Mock::new());
    for mock in [&accepts, &refuses] {
        mock.add(handlers::provide(matching_columns()));
    }
    accepts.add(handlers::provide::<u8>([1u8]));
    // 115 = UNKNOWN_SETTING.
    refuses.add(handlers::exception(115));
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: orders
compression: none
shards:
  - replicas: ["{}", "{}"]
settings: {{ insert_qourum: "2" }}
"#,
        accepts.url(),
        refuses.url(),
    ))
    .expect("config yaml");
    let err = ClickHouseSinkBuilder::new(cfg)
        .expect("valid sink config")
        .connect::<Owned<TestRow>>()
        .await
        .expect_err("a refused setting must fail startup");
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    match schema(&err) {
        SchemaError::Settings { url, reason } => {
            assert_eq!(url, refuses.url());
            assert!(reason.contains("115"), "{reason}");
        }
        other => panic!("expected Settings, got {other:?}"),
    }
}

#[tokio::test]
async fn replica_disagreement_fails_startup() {
    let a = Mock::new();
    let b = Mock::new();
    a.add(handlers::provide(matching_columns()));
    b.add(handlers::provide(system_columns(&[
        ("id", "UInt64", ""),
        ("name", "String", ""),
        ("score", "Float64", ""), // drifted: no longer Nullable
    ])));
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: orders
compression: none
shards:
  - replicas: ["{}", "{}"]
"#,
        a.url(),
        b.url()
    ))
    .unwrap();
    let err = ClickHouseSinkBuilder::new(cfg)
        .unwrap()
        .connect::<Owned<TestRow>>()
        .await
        .expect_err("drift must fail");
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    let msg = err.to_string();
    assert!(msg.contains("replicas disagree"), "{msg}");
    assert!(msg.contains("Nullable(Float64)"), "{msg}");
}

#[tokio::test]
async fn probe_fn_covers_every_replica_with_its_own_clients() {
    use spate_core::sink::SinkBundle;

    // Healthy server: the probe closure succeeds and hits the mock once
    // per replica per call.
    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    let sink = sink_for(mock.url()).await;
    mock.add(handlers::provide::<u8>([1u8]));
    let probe = sink.probe_fn();
    probe().await.expect("probe healthy replica");

    // The bundle decomposition carries the probe, the clickhouse
    // component type, and URL replica labels.
    let parts = sink.into_parts();
    assert_eq!(parts.component_type, "clickhouse");
    assert_eq!(parts.replica_labels, vec![vec![mock.url().to_string()]]);
    assert_eq!(
        parts.pool,
        spate_core::sink::SinkPoolConfig::default(),
        "the base pool"
    );
    let bundled_probe = parts.probe.expect("bundle carries a probe");
    mock.add(handlers::provide::<u8>([1u8]));
    bundled_probe().await.expect("bundled probe works");

    // A server that stops answering after the sink is built: the probe fails.
    let dead = Mock::new();
    answer_build(&dead, matching_columns());
    let sink = sink_for(dead.url()).await;
    dead.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
    assert!(sink.probe_fn()().await.is_err());
}

/// A credential in a replica URL's query never reaches `Debug`, the replica
/// metric labels or schema errors. Regression for #754.
#[tokio::test]
async fn replica_url_credentials_never_reach_debug_labels_or_errors() {
    use spate_core::sink::SinkBundle;

    let with_query = |url: &str| format!("{url}/?password=hunter2");
    let redacted = |url: &str| format!("{url}/?<redacted>");

    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    let sink = sink_for(&with_query(mock.url())).await;
    let printed = format!("{sink:?}");
    assert!(!printed.contains("hunter2"), "{printed}");
    assert!(printed.contains(&redacted(mock.url())), "{printed}");
    let parts = sink.into_parts();
    assert_eq!(parts.replica_labels, vec![vec![redacted(mock.url())]]);

    let missing = Mock::new();
    missing.add(handlers::provide(Vec::<ColumnRow>::new()));
    let err = failed_sink(&with_query(missing.url())).await.to_string();
    assert!(!err.contains("hunter2"), "{err}");
    assert!(err.contains(&redacted(missing.url())), "{err}");

    let down = Mock::new();
    down.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
    let err = failed_sink(&with_query(down.url())).await.to_string();
    assert!(!err.contains("hunter2"), "{err}");
    assert!(err.contains(&redacted(down.url())), "{err}");
}

#[tokio::test]
async fn probe_maps_select_one() {
    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    let sink = sink_for(mock.url()).await;
    mock.add(handlers::provide::<u8>([1u8]));
    sink.writer
        .probe(&sink.endpoints[0][0])
        .await
        .expect("probe ok");

    let dead = Mock::new();
    answer_build(&dead, matching_columns());
    let sink = sink_for(dead.url()).await;
    dead.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
    assert!(sink.writer.probe(&sink.endpoints[0][0]).await.is_err());
}

// ---- property-based round-trips ----------------------------------------------

/// Arbitrary rows through our serializer, decoded by the crate's
/// deserializer: the strongest no-Docker wire-compatibility oracle for the
/// ecosystem serde modules and wrapper types.
#[cfg(all(feature = "uuid", feature = "chrono"))]
mod prop_round_trip {
    use super::*;
    use ::chrono::{DateTime, Duration, NaiveDate, Utc};
    use proptest::prelude::*;
    use spate_clickhouse::Decimal64;
    use std::net::Ipv4Addr;
    use std::sync::LazyLock;
    use uuid::Uuid;

    static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    });

    #[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
    struct PropRow {
        id: u64,
        #[serde(with = "spate_clickhouse::serde::uuid")]
        uid: Uuid,
        #[serde(with = "spate_clickhouse::serde::ipv4")]
        ip: Ipv4Addr,
        #[serde(with = "spate_clickhouse::serde::chrono::date")]
        day: NaiveDate,
        #[serde(with = "spate_clickhouse::serde::chrono::datetime64::millis")]
        ts: DateTime<Utc>,
        price: Decimal64<4>,
        tags: Vec<String>,
        score: Option<f64>,
        name: String,
    }

    fn arb_row() -> impl Strategy<Value = PropRow> {
        (
            any::<u64>(),
            any::<(u64, u64)>(),
            any::<u32>(),
            0..=u16::MAX,
            // DateTime64(3) around the epoch, well inside chrono's range.
            -100_000_000_000_000i64..=100_000_000_000_000i64,
            any::<i64>(),
            prop::collection::vec(any::<String>(), 0..3),
            prop::option::of(-1e12..1e12f64),
            any::<String>(),
        )
            .prop_map(
                |(id, (hi, lo), ip, days, millis, raw_price, tags, score, name)| PropRow {
                    id,
                    uid: Uuid::from_u64_pair(hi, lo),
                    ip: Ipv4Addr::from(ip),
                    day: NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                        + Duration::days(i64::from(days)),
                    ts: DateTime::<Utc>::from_timestamp_millis(millis).unwrap(),
                    price: Decimal64::<4>(raw_price),
                    tags,
                    score,
                    name,
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]
        #[test]
        fn arbitrary_rows_survive_the_crate_deserializer(
            rows in prop::collection::vec(arb_row(), 1..20)
        ) {
            let received: Vec<PropRow> = RT.block_on(async {
                let mock = Mock::new();
                let recorder = mock.add(handlers::record::<PropRow>());

                let mut buf = BytesMut::new();
                for row in &rows {
                    serialize_row(row, &mut buf).expect("encode");
                }
                let bytes = buf.len() as u64;
                let batch = SealedBatch {
                    frames: vec![buf.freeze()],
                    rows: rows.len() as u64,
                    bytes,
                    dedup_token: "prop".into(),
                };
                post_frames(mock.url(), &batch).await;
                recorder.collect().await
            });
            prop_assert_eq!(received, rows);
        }
    }
}

// ---- distributed_check (DDL-parity guard) -----------------------------------
//
// The guard's query order is fixed at cluster topology then table engine,
// and these FIFO-mock tests depend on it (a comment in
// `distributed::DistributedCheck::verify` pins the contract from the other
// side). Early-failure tests queue only the handlers that will be consumed:
// the mock panics on drop if a queued handler goes unused.

/// What the guard's `system.clusters` query returns.
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
struct ClusterRow {
    shard_num: u32,
    shard_weight: u32,
    host_name: String,
}

fn cluster_row(shard_num: u32, weight: u32, host: &str) -> ClusterRow {
    ClusterRow {
        shard_num,
        shard_weight: weight,
        host_name: host.into(),
    }
}

/// What the guard's `system.tables` query returns.
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
struct EngineRow {
    engine: String,
    engine_full: String,
}

fn engine_row(engine: &str, engine_full: &str) -> EngineRow {
    EngineRow {
        engine: engine.into(),
        engine_full: engine_full.into(),
    }
}

/// A sink with per-shard weights and a `distributed_check` block, all
/// replicas pointing at the mock.
///
/// Built first in each test, so the schema answer it queues is consumed
/// before the guard's own handlers go on the queue behind it.
/// A builder with `check` whose build requests `mock` answers. Queue the
/// check's answers after this, then [`connect`].
fn checked_builder(mock: &Mock, weights: &[u32], check: &str) -> ClickHouseSinkBuilder {
    let url = mock.url();
    // One replica per shard. The fetch reads every one of them, then the
    // settings check asks each of them.
    for _ in weights {
        mock.add(handlers::provide(matching_columns()));
    }
    for _ in weights {
        mock.add(handlers::provide::<u8>([1u8]));
    }
    let shards: String = weights
        .iter()
        .map(|w| format!("  - replicas: [\"{url}\"]\n    weight: {w}\n"))
        .collect();
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "table: orders\ncompression: none\nshards:\n{shards}{check}\n"
    ))
    .expect("config yaml");
    ClickHouseSinkBuilder::new(cfg).expect("valid sink config")
}

async fn connect(builder: ClickHouseSinkBuilder) -> Result<config::ClickHouseSink, ConnectError> {
    builder.connect::<Owned<TestRow>>().await
}

/// The distributed check's error under a startup error.
fn check_error(err: &ConnectError) -> &spate_clickhouse::DistributedCheckError {
    find_source(err).unwrap_or_else(|| panic!("no distributed check error under {err:?}"))
}

const CHECK_ON_ID: &str =
    "distributed_check: { cluster: prod, table: orders_dist, sharding_key: id }";

#[tokio::test]
async fn distributed_check_passes_when_cluster_and_ddl_match() {
    let mock = Mock::new();
    let builder = checked_builder(&mock, &[1, 1], CHECK_ON_ID);
    mock.add(handlers::provide::<ClusterRow>(vec![
        cluster_row(1, 1, "ch-0"),
        cluster_row(2, 1, "ch-1"),
    ]));
    mock.add(handlers::provide::<EngineRow>(vec![engine_row(
        "Distributed",
        "Distributed('prod', 'db', 'orders', xxHash64(id))",
    )]));
    connect(builder).await.expect("parity holds");
}

#[tokio::test]
async fn distributed_check_off_issues_no_queries() {
    // Only the build's two requests are answered. Nothing is queued behind
    // them, and the mock panics on drop if a queued handler goes unconsumed,
    // so a clean Ok proves the guard never talked to the server without a
    // `distributed_check` block.
    let mock = Mock::new();
    answer_build(&mock, matching_columns());
    sink_for(mock.url()).await;
}

#[tokio::test]
async fn shard_count_mismatch_fails_the_distributed_check() {
    let mock = Mock::new();
    let builder = checked_builder(&mock, &[1, 1], CHECK_ON_ID);
    mock.add(handlers::provide::<ClusterRow>(vec![
        cluster_row(1, 1, "ch-0"),
        cluster_row(2, 1, "ch-1"),
        cluster_row(3, 1, "ch-2"),
    ]));
    let err = connect(builder).await.unwrap_err();
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    let msg = err.to_string();
    assert!(
        msg.contains("2 shard(s)") && msg.contains("has 3"),
        "names both sides: {msg}"
    );
}

#[tokio::test]
async fn weight_mismatch_names_the_offending_shard() {
    let mock = Mock::new();
    let builder = checked_builder(&mock, &[9, 10], CHECK_ON_ID);
    mock.add(handlers::provide::<ClusterRow>(vec![
        cluster_row(1, 9, "ch-0"),
        cluster_row(2, 1, "ch-1"),
    ]));
    let err = connect(builder).await.unwrap_err();
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    let msg = err.to_string();
    assert!(
        msg.contains("config shard 1 has weight 10") && msg.contains("shard_num 2"),
        "names the offending shard in both numberings: {msg}"
    );
}

#[tokio::test]
async fn sharding_expression_mismatch_prints_both_sides() {
    let mock = Mock::new();
    let builder = checked_builder(&mock, &[1], CHECK_ON_ID);
    mock.add(handlers::provide::<ClusterRow>(vec![cluster_row(
        1, 1, "ch-0",
    )]));
    mock.add(handlers::provide::<EngineRow>(vec![engine_row(
        "Distributed",
        "Distributed('prod', 'db', 'orders', cityHash64(id))",
    )]));
    let err = connect(builder).await.unwrap_err();
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    let msg = err.to_string();
    assert!(
        msg.contains("cityHash64(id)") && msg.contains("xxHash64(id)"),
        "prints the DDL and expected expressions: {msg}"
    );
    assert!(
        msg.contains("Distributed('prod'"),
        "prints the raw engine_full for diagnosis: {msg}"
    );
}

#[tokio::test]
async fn non_distributed_engine_fails_with_the_engine_name() {
    let mock = Mock::new();
    let builder = checked_builder(&mock, &[1], CHECK_ON_ID);
    mock.add(handlers::provide::<ClusterRow>(vec![cluster_row(
        1, 1, "ch-0",
    )]));
    mock.add(handlers::provide::<EngineRow>(vec![engine_row(
        "ReplacingMergeTree",
        "ReplacingMergeTree(v) ORDER BY id",
    )]));
    let err = connect(builder).await.unwrap_err();
    assert!(
        err.to_string().contains("ReplacingMergeTree"),
        "names the actual engine: {err}"
    );
}

/// A credential in the `distributed_check` endpoint URL never reaches the
/// check's mismatch or fetch errors. Regression for #754.
#[tokio::test]
async fn distributed_check_errors_never_print_endpoint_credentials() {
    let check = |url: &str| {
        format!(
            "distributed_check: {{ cluster: prod, table: orders_dist, sharding_key: id, \
             endpoint: \"{url}/?password=hunter2\" }}"
        )
    };

    let mock = Mock::new();
    let builder = checked_builder(&mock, &[1], &check(mock.url()));
    mock.add(handlers::provide::<ClusterRow>(Vec::new()));
    let err = connect(builder).await.unwrap_err().to_string();
    assert!(!err.contains("hunter2"), "{err}");
    assert!(
        err.contains(&format!("{}/?<redacted>", mock.url())),
        "{err}"
    );

    let failing = Mock::new();
    let builder = checked_builder(&failing, &[1], &check(failing.url()));
    failing.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
    let err = connect(builder).await.unwrap_err().to_string();
    assert!(!err.contains("hunter2"), "{err}");
    assert!(
        err.contains(&format!("{}/?<redacted>", failing.url())),
        "{err}"
    );
}

#[tokio::test]
async fn unknown_cluster_fails_distinguishably() {
    use spate_clickhouse::DistributedCheckError;

    // An empty system.clusters result is a Mismatch (wrong cluster name)…
    let mock = Mock::new();
    let builder = checked_builder(&mock, &[1], CHECK_ON_ID);
    mock.add(handlers::provide::<ClusterRow>(Vec::new()));
    let err = connect(builder).await.unwrap_err();
    assert_eq!(err.kind(), ConnectErrorKind::Refused, "{err}");
    assert!(
        matches!(check_error(&err), DistributedCheckError::Mismatch(m) if m.contains("not found")),
        "empty topology is a Mismatch: {err}"
    );

    // …while a failing query is a Fetch: connectivity, not configuration.
    let failing = Mock::new();
    let builder = checked_builder(&failing, &[1], CHECK_ON_ID);
    failing.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
    let err = connect(builder).await.unwrap_err();
    assert_eq!(err.kind(), ConnectErrorKind::Unavailable, "{err}");
    assert!(
        matches!(check_error(&err), DistributedCheckError::Fetch { what, .. } if *what == "cluster topology"),
        "transport failure is a Fetch: {err}"
    );
}

/// Each startup request classes its failure the same way: a 503 with no
/// exception code is unavailable, an unknown-user exception is refused,
/// and the check's own error is the startup error's source. The four
/// requests are the schema read, the settings check and the two
/// `distributed_check` queries.
#[tokio::test]
async fn each_startup_request_classes_its_failure() {
    type Answered = fn(&Mock);
    let sites: [(&str, Answered); 4] = [
        ("schema read", |_| {}),
        ("settings check", |mock| {
            mock.add(handlers::provide(matching_columns()));
        }),
        ("cluster topology", |mock| {
            answer_build(mock, matching_columns());
        }),
        ("table engine", |mock| {
            answer_build(mock, matching_columns());
            mock.add(handlers::provide::<ClusterRow>(vec![cluster_row(
                1, 1, "ch-0",
            )]));
        }),
    ];
    for (site, answered) in sites {
        for (failure, kind) in [
            ("503", ConnectErrorKind::Unavailable),
            ("192", ConnectErrorKind::Refused),
        ] {
            let mock = Mock::new();
            answered(&mock);
            if failure == "503" {
                mock.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
            } else {
                mock.add(handlers::exception(192));
            }
            let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
                "table: orders\ncompression: none\nshards:\n  - replicas: [\"{}\"]\n{CHECK_ON_ID}\n",
                mock.url()
            ))
            .expect("config yaml");
            let builder = ClickHouseSinkBuilder::new(cfg).expect("valid sink config");
            let err = connect(builder).await.unwrap_err();
            assert_eq!(err.kind(), kind, "{site}, {failure}: {err}");
            let failed = match site {
                "schema read" => matches!(schema(&err), SchemaError::Fetch { .. }),
                "settings check" => matches!(schema(&err), SchemaError::Settings { .. }),
                what => matches!(
                    check_error(&err),
                    spate_clickhouse::DistributedCheckError::Fetch { what: named, .. } if *named == what
                ),
            };
            assert!(failed, "{site}, {failure}: {err:?}");
            assert_eq!(
                err.reason(),
                std::error::Error::source(&err).unwrap().to_string(),
                "{site}, {failure}"
            );
        }
    }
}
