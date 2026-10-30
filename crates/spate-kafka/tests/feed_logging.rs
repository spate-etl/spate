//! Kafka feed logs emitted from the producer poll thread.

use rdkafka::mocking::MockCluster;
use spate_core::config::FeedSection;
use spate_core::feed::{Feed as _, Unit};
use spate_kafka::feed::KafkaFeedPublisher;
use spate_test::{LogCapture, wait_until};
use std::time::Duration;

/// The feed producer's debug lines reach the tracing subscriber, from the
/// producer's own poll thread.
#[test]
fn feed_producer_debug_logs_reach_tracing() {
    let capture = LogCapture::new();
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .init();

    let cluster = MockCluster::new(1).expect("mock cluster");
    for topic in [
        "log-units",
        "log-units-retry",
        "log-units-dead",
        "log-units-attempts",
    ] {
        cluster.create_topic(topic, 1, 1).expect("create topic");
    }
    let section: FeedSection = serde_yaml::from_str(&format!(
        "kafka:\n  brokers: {}\n  group_id: log-workers\n  topic: log-units\n  \
         retry_topic: log-units-retry\n  dead_letter_topic: log-units-dead\n  \
         attempt_topic: log-units-attempts\n  statistics_interval: 0s\n  \
         rdkafka:\n    debug: msg\n",
        cluster.bootstrap_servers()
    ))
    .expect("feed section");
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let publisher = KafkaFeedPublisher::connect_without_config_checks(&section, io.handle())
        .expect("publisher");
    io.block_on(publisher.publish(Unit::new("a", b"body".to_vec())))
        .expect("publish");

    wait_until(Duration::from_secs(10), "a produce debug line", || {
        capture
            .lines()
            .iter()
            .any(|line| line.contains("librdkafka") && line.contains("fac=\"PRODUCE\""))
    });
}
