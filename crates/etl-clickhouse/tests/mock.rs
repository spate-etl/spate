//! Integration tests against the `clickhouse` crate's mock server (no
//! Docker). The mock's `record` handler decodes request bodies through the
//! crate's own RowBinary deserializer — every row that round-trips here
//! proves our serializer is wire-compatible with the crate's.

use bytes::BytesMut;
use clickhouse::test::{Mock, handlers};
use etl_clickhouse::config::{self, ClickHouseSinkConfig};
use etl_clickhouse::serialize_row;
use etl_core::error::{ErrorClass, SinkError};
use etl_core::sink::{SealedBatch, ShardWriter};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
struct TestRow {
    id: u64,
    name: String,
    score: Option<f64>,
}

fn sink_for(url: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        // `off`: the crate's `record` handler decodes the request body with no
        // decompression, so a decodable round-trip requires an uncompressed body.
        r#"
table: orders
columns: [id, name, score]
compression: off
shards:
  - replicas: ["{url}"]
"#
    ))
    .expect("config yaml");
    config::build(cfg).expect("valid sink config")
}

fn sealed(rows: &[TestRow], token: &str) -> SealedBatch {
    // Two frames to exercise multi-frame sends.
    let mid = rows.len() / 2;
    let mut frames = Vec::new();
    let mut bytes = 0u64;
    for part in [&rows[..mid], &rows[mid..]] {
        let mut buf = BytesMut::new();
        for row in part {
            serialize_row(row, &mut buf).expect("encode");
        }
        bytes += buf.len() as u64;
        frames.push(buf.freeze());
    }
    SealedBatch {
        frames,
        rows: rows.len() as u64,
        bytes,
        dedup_token: token.to_string(),
    }
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

#[tokio::test]
async fn write_batch_lands_all_frames_decodable_by_the_crate() {
    let mock = Mock::new();
    let recorder = mock.add(handlers::record::<TestRow>());
    let sink = sink_for(mock.url());

    let expected = rows(101);
    let batch = sealed(&expected, "tok-1");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");

    let received: Vec<TestRow> = recorder.collect().await;
    assert_eq!(
        received, expected,
        "crate-decoded rows match what we encoded"
    );
}

#[tokio::test]
async fn transport_failures_are_retryable() {
    let mock = Mock::new();
    mock.add(handlers::failure(hyper::StatusCode::INTERNAL_SERVER_ERROR));
    let sink = sink_for(mock.url());

    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &sealed(&rows(3), "tok-2"))
        .await
        .expect_err("must fail");
    match err {
        SinkError::Client { class, .. } => assert_eq!(class, ErrorClass::Retryable),
        other => panic!("unexpected error shape: {other:?}"),
    }
}

#[tokio::test]
async fn schema_class_exceptions_are_fatal() {
    let mock = Mock::new();
    // 60 = UNKNOWN_TABLE.
    mock.add(handlers::exception(60));
    let sink = sink_for(mock.url());

    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &sealed(&rows(3), "tok-3"))
        .await
        .expect_err("must fail");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Fatal, "reason: {reason}");
        }
        other => panic!("unexpected error shape: {other:?}"),
    }
}

#[tokio::test]
async fn capacity_class_exceptions_stay_retryable() {
    let mock = Mock::new();
    // 252 = TOO_MANY_PARTS: transient merge pressure, retry is correct.
    mock.add(handlers::exception(252));
    let sink = sink_for(mock.url());

    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &sealed(&rows(3), "tok-4"))
        .await
        .expect_err("must fail");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Retryable, "reason: {reason}");
        }
        other => panic!("unexpected error shape: {other:?}"),
    }
}

// ---- startup schema validation ----------------------------------------------

/// What the validator's `system.columns` query returns; the mock encodes
/// it with the crate's own serializer, proving our fetch decodes the real
/// wire shape.
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
struct SysColumn {
    name: String,
    #[serde(rename = "type")]
    type_: String,
    default_kind: String,
}

fn sys_col(name: &str, type_: &str, default_kind: &str) -> SysColumn {
    SysColumn {
        name: name.into(),
        type_: type_.into(),
        default_kind: default_kind.into(),
    }
}

/// The table matching `TestRow` / `sink_for`'s columns.
fn matching_columns() -> Vec<SysColumn> {
    vec![
        sys_col("id", "UInt64", ""),
        sys_col("name", "String", ""),
        sys_col("score", "Nullable(Float64)", ""),
    ]
}

fn sink_with(url: &str, mode: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        // `off`: the mock's `provide` handler returns uncompressed responses,
        // so the schema-validation SELECTs must not request compression.
        r#"
table: orders
columns: [id, name, score]
compression: off
shards:
  - replicas: ["{url}"]
validate_schema: {mode}
"#
    ))
    .expect("config yaml");
    config::build(cfg).expect("valid sink config")
}

fn record<T>(payload: T) -> etl_core::record::Record<T> {
    let (ack, rx) = etl_core::checkpoint::AckRef::test_pair();
    std::mem::forget(rx);
    etl_core::record::Record {
        payload,
        meta: etl_core::record::RecordMeta {
            partition: etl_core::record::PartitionId(0),
            offset: 0,
            event_time_ms: 0,
            key_hash: None,
        },
        ack,
    }
}

#[tokio::test]
async fn off_mode_issues_no_queries() {
    // No handlers queued: a request would error the fetch, and the mock
    // itself panics on drop if a queued handler goes unconsumed — so a
    // clean Ok(None) proves validation never talked to the server.
    let dead = Mock::new();
    let sink = sink_with(dead.url(), "off");
    let schema = sink.validate_schema().await.expect("off must not fetch");
    assert!(schema.is_none());
}

#[tokio::test]
async fn matching_schema_passes_and_first_record_encodes() {
    use etl_core::sink::RowEncoder;

    let mock = Mock::new();
    mock.add(handlers::provide::<SysColumn>(matching_columns()));
    let sink = sink_with(mock.url(), "full");
    let schema = sink
        .validate_schema()
        .await
        .expect("validation passes")
        .expect("names/full returns a schema");

    // The first-record struct check passes and the bytes are identical
    // to an unvalidated encoder's.
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<TestRow>::with_schema(schema);
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
    use etl_core::sink::RowEncoder;

    #[derive(Clone, Serialize)]
    struct WrongOrder {
        name: String,
        id: u64,
        score: Option<f64>,
    }

    let mock = Mock::new();
    mock.add(handlers::provide::<SysColumn>(matching_columns()));
    let sink = sink_with(mock.url(), "names");
    let schema = sink.validate_schema().await.unwrap().unwrap();

    let mut encoder = etl_clickhouse::ClickHouseEncoder::<WrongOrder>::with_schema(schema);
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
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Fatal, "{reason}");
            assert!(
                reason.contains("position 0: struct field `name` vs configured column `id`"),
                "{reason}"
            );
        }
        other => panic!("unexpected error shape: {other:?}"),
    }
}

#[tokio::test]
async fn type_mismatch_fails_full_but_passes_names() {
    use etl_core::sink::RowEncoder;

    // score: Option<u32> against Nullable(Float64) — wrong width/class.
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
    mock.add(handlers::provide::<SysColumn>(matching_columns()));
    let schema = sink_with(mock.url(), "full")
        .validate_schema()
        .await
        .unwrap()
        .unwrap();
    let err = etl_clickhouse::ClickHouseEncoder::<WrongType>::with_schema(schema)
        .encode(&record(wrong.clone()), &mut BytesMut::new())
        .expect_err("full mode checks type classes");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Fatal, "{reason}");
            assert!(
                reason.contains("not compatible with `score` Nullable(Float64)"),
                "{reason}"
            );
        }
        other => panic!("unexpected error shape: {other:?}"),
    }

    // Same struct under `names`: permissiveness is by design.
    let mock = Mock::new();
    mock.add(handlers::provide::<SysColumn>(matching_columns()));
    let schema = sink_with(mock.url(), "names")
        .validate_schema()
        .await
        .unwrap()
        .unwrap();
    etl_clickhouse::ClickHouseEncoder::<WrongType>::with_schema(schema)
        .encode(&record(wrong), &mut BytesMut::new())
        .expect("names mode skips type classes");
}

#[tokio::test]
async fn missing_and_materialized_columns_fail_startup() {
    let mock = Mock::new();
    mock.add(handlers::provide::<SysColumn>(vec![
        sys_col("id", "UInt64", ""),
        // `name` is absent; `score` is MATERIALIZED.
        sys_col("score", "Nullable(Float64)", "MATERIALIZED"),
    ]));
    let err = sink_with(mock.url(), "names")
        .validate_schema()
        .await
        .expect_err("must fail");
    let msg = err.to_string();
    assert!(matches!(err, etl_clickhouse::SchemaError::Mismatch(_)));
    assert!(
        msg.contains("configured column `name` does not exist in the table"),
        "{msg}"
    );
    assert!(
        msg.contains("configured column `score` is MATERIALIZED and cannot be inserted into"),
        "{msg}"
    );
    assert!(msg.contains("table columns:"), "{msg}");
    assert!(msg.contains("configured columns: id, name, score"), "{msg}");
}

#[tokio::test]
async fn empty_result_means_table_not_found() {
    let mock = Mock::new();
    mock.add(handlers::provide::<SysColumn>(Vec::<SysColumn>::new()));
    let err = sink_with(mock.url(), "names")
        .validate_schema()
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("not found"), "{err}");
}

#[tokio::test]
async fn fetch_failures_fail_startup_distinguishably() {
    let mock = Mock::new();
    mock.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
    let err = sink_with(mock.url(), "full")
        .validate_schema()
        .await
        .expect_err("must fail");
    match err {
        etl_clickhouse::SchemaError::Fetch { table, .. } => {
            assert_eq!(table, "`orders`");
        }
        other => panic!("expected Fetch, got {other:?}"),
    }
}

#[tokio::test]
async fn replica_disagreement_fails_startup() {
    let a = Mock::new();
    let b = Mock::new();
    a.add(handlers::provide::<SysColumn>(matching_columns()));
    b.add(handlers::provide::<SysColumn>(vec![
        sys_col("id", "UInt64", ""),
        sys_col("name", "String", ""),
        sys_col("score", "Float64", ""), // drifted: no longer Nullable
    ]));
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: orders
columns: [id, name, score]
compression: off
shards:
  - replicas: ["{}", "{}"]
validate_schema: names
"#,
        a.url(),
        b.url()
    ))
    .unwrap();
    let err = config::build(cfg)
        .unwrap()
        .validate_schema()
        .await
        .expect_err("drift must fail");
    let msg = err.to_string();
    assert!(msg.contains("replicas disagree"), "{msg}");
    assert!(msg.contains("Nullable(Float64)"), "{msg}");
}

#[tokio::test]
async fn probe_maps_select_one() {
    let mock = Mock::new();
    mock.add(handlers::provide::<u8>([1u8]));
    let sink = sink_for(mock.url());
    sink.writer
        .probe(&sink.endpoints[0][0])
        .await
        .expect("probe ok");

    let dead = Mock::new();
    dead.add(handlers::failure(hyper::StatusCode::SERVICE_UNAVAILABLE));
    let sink = sink_for(dead.url());
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
    use etl_clickhouse::Decimal64;
    use proptest::prelude::*;
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
        #[serde(with = "etl_clickhouse::serde::uuid")]
        uid: Uuid,
        #[serde(with = "etl_clickhouse::serde::ipv4")]
        ip: Ipv4Addr,
        #[serde(with = "etl_clickhouse::serde::chrono::date")]
        day: NaiveDate,
        #[serde(with = "etl_clickhouse::serde::chrono::datetime64::millis")]
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
                let sink = sink_for(mock.url());

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
                sink.writer
                    .write_batch(&sink.endpoints[0][0], &batch)
                    .await
                    .expect("write");
                recorder.collect().await
            });
            prop_assert_eq!(received, rows);
        }
    }
}
