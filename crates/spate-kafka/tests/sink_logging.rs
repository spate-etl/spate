//! Kafka sink logs emitted from the producer poll thread.

use bytes::BytesMut;
use rdkafka::mocking::MockCluster;
use spate_core::checkpoint::AckRef;
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::{RowEncoder, SealedBatch, ShardWriter};
use spate_kafka::sink::KafkaSinkConfig;
use spate_test::LogCapture;
use std::time::Duration;

/// Producer debug lines reach the tracing subscriber after startup.
/// Regression for #651.
#[tokio::test]
async fn producer_debug_logs_reach_tracing() {
    let capture = LogCapture::new();
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .init();

    let cluster = MockCluster::new(1).expect("mock cluster");
    cluster
        .create_topic("log-level-sink", 1, 1)
        .expect("create topic");
    let mut cfg = KafkaSinkConfig::new(cluster.bootstrap_servers(), "log-level-sink");
    cfg.statistics_interval = Duration::ZERO;
    cfg.rdkafka.insert("debug".into(), "msg".into());
    let sink = spate_kafka::sink::build(cfg).expect("sink build");

    let (ack, _rx) = AckRef::test_pair();
    let record = Record {
        payload: b"payload".to_vec(),
        meta: RecordMeta {
            partition: PartitionId(0),
            offset: 0,
            event_time_ms: 0,
            key_hash: None,
        },
        ack,
    };
    let mut encoder = sink.encoder_bytes();
    let mut frame = BytesMut::new();
    encoder.encode(&record, &mut frame).expect("encode");
    let batch = SealedBatch {
        rows: 1,
        bytes: frame.len() as u64,
        frames: vec![frame.freeze()],
        dedup_token: "unused-by-kafka".to_string(),
    };
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write_batch");

    let lines = capture.lines();
    assert!(
        lines.iter().any(|line| {
            line.contains("librdkafka")
                && line.contains("fac=\"PRODUCE\"")
                && line.contains("Produce MessageSet")
        }),
        "no produce debug line: {lines:#?}"
    );
}
