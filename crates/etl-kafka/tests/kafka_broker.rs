//! Integration tests against a real Kafka broker (Docker via
//! testcontainers). Run explicitly:
//!
//! ```sh
//! cargo test -p etl-kafka --test kafka_broker -- --ignored
//! ```

use etl_core::checkpoint::Checkpointer;
use etl_core::source::{PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use etl_kafka::{KafkaSource, KafkaSourceConfig};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

const TOPIC: &str = "orders";

fn config(brokers: &str, group: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.to_string(),
        topic: TOPIC.to_string(),
        group_id: group.to_string(),
        commit_interval: Duration::from_millis(200),
        startup_timeout: Duration::from_secs(60),
        statistics_interval: Duration::from_secs(1),
        rdkafka: BTreeMap::new(),
    }
}

#[test]
#[ignore = "requires Docker"]
fn real_broker_full_lifecycle() {
    let container = Kafka::default().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");

    // Auto-created topic has a single partition — sufficient for the
    // lifecycle; the multi-partition rebalance path is covered by the
    // MockCluster suite.
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    for i in 0..100 {
        let payload = format!("real-{i}");
        producer
            .send(
                BaseRecord::to(TOPIC)
                    .payload(payload.as_bytes())
                    .key(format!("k{i}").as_bytes())
                    .partition(0),
            )
            .expect("enqueue");
    }
    producer.flush(Duration::from_secs(30)).expect("flush");

    let mut cp = Checkpointer::new();
    let mut source = KafkaSource::new(config(&brokers, "real-life"));
    source.open(SourceCtx::new(cp.handle())).expect("open");

    // Await assignment.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut lanes = loop {
        assert!(Instant::now() < deadline, "no assignment");
        if let SourceEvent::LanesAssigned(lanes) = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            break lanes;
        }
    };
    assert_eq!(lanes.len(), 1);
    cp.begin_epoch(&[lanes[0].partition()], 1);

    // Drain all 100 records.
    let mut got = 0usize;
    let deadline = Instant::now() + Duration::from_secs(60);
    while got < 100 {
        assert!(Instant::now() < deadline, "drained {got}/100");
        if let Some(mut batch) = lanes[0]
            .poll(64, Duration::from_millis(500))
            .expect("lane poll")
        {
            while let Some(raw) = batch.next_payload() {
                assert!(raw.bytes.starts_with(b"real-"));
                got += 1;
            }
        }
    }
    drop(lanes);

    // Ack → watermark → store → sync commit.
    cp.drain();
    let watermarks = cp.take_watermarks();
    assert_eq!(watermarks.len(), 1);
    assert_eq!(watermarks[0].1, 100);
    source.commit(&watermarks).expect("store");
    source.flush_commits().expect("sync commit");

    // The group's committed offset is visible to a probe member.
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "real-life")
        .create()
        .expect("probe");
    let mut tpl = rdkafka::TopicPartitionList::new();
    tpl.add_partition(TOPIC, 0);
    let committed = probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed");
    assert_eq!(
        committed.elements()[0].offset(),
        rdkafka::Offset::Offset(100)
    );
}
