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
        r#"
table: orders
columns: [id, name, score]
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
