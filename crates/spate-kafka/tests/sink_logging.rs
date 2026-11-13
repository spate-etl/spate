//! Kafka sink logs emitted from the producer poll thread.

#[path = "support/mock.rs"]
mod mock;
#[path = "support/sink.rs"]
mod sink;

use mock::MockKafka;
use sink::sealed;
use spate_core::sink::ShardWriter;
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

    let cluster = MockKafka::new();
    cluster
        .create_topic("log-level-sink", 1, 1)
        .expect("create topic");
    let mut cfg = KafkaSinkConfig::new(cluster.bootstrap_servers(), "log-level-sink");
    cfg.statistics_interval = Duration::ZERO;
    cfg.rdkafka.insert("debug".into(), "msg".into());
    let sink = spate_kafka::sink::build(cfg).expect("sink build");

    let batch = sealed(&mut sink.encoder_bytes(), [b"payload".as_slice()]);
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
