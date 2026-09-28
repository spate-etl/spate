//! The pinned broker, and the produce, consume and lane-draining helpers
//! shared by the Kafka integration suites.

// A per-item `expect` goes unfulfilled in whichever target uses the item.
#![allow(dead_code, reason = "each target uses a different subset")]

use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::source::{PayloadBatch, Source, SourceEvent, SourceLane};
use spate_kafka::KafkaSource;
use spate_test_support::container_image;
use std::time::{Duration, Instant};
use testcontainers::{ContainerRequest, ImageExt};
use testcontainers_modules::kafka::apache::Kafka;

/// A broker on the image `ci/kafka/` pins, pulled by digest.
pub(crate) fn broker() -> ContainerRequest<Kafka> {
    let (name, tag) = container_image(&["--pull", "kafka"]);
    Kafka::default().with_name(name).with_tag(tag)
}

/// Produce `per_partition` records to each of `partitions` partitions of
/// `topic`, with payload `{tag}-p{p}-{i}` and key `k{p}-{i}`, and flush.
pub(crate) fn produce(
    brokers: &str,
    topic: &str,
    per_partition: usize,
    partitions: i32,
    tag: &str,
) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");
    for p in 0..partitions {
        for i in 0..per_partition {
            let payload = format!("{tag}-p{p}-{i}");
            let key = format!("k{p}-{i}");
            producer
                .send(
                    BaseRecord::to(topic)
                        .partition(p)
                        .payload(payload.as_bytes())
                        .key(key.as_bytes()),
                )
                .expect("enqueue");
        }
    }
    producer.flush(Duration::from_secs(60)).expect("flush");
}

/// Drive `poll_events` until an assignment arrives. Panics if none does
/// within `timeout`.
pub(crate) fn await_assignment(
    source: &mut KafkaSource,
    timeout: Duration,
) -> Vec<<KafkaSource as Source>::Lane> {
    let deadline = Instant::now() + timeout;
    loop {
        assert!(
            Instant::now() < deadline,
            "no assignment within {timeout:?}"
        );
        if let SourceEvent::LanesAssigned(lanes) = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            return lanes;
        }
    }
}

/// Read `topic` from the earliest offset as consumer group `group`, keeping
/// what `keep` returns for each message until `n` are kept. Panics if they
/// are not kept within `timeout`.
pub(crate) fn consume<T>(
    brokers: &str,
    topic: &str,
    group: &str,
    n: usize,
    timeout: Duration,
    mut keep: impl FnMut(&BorrowedMessage<'_>) -> Option<T>,
) -> Vec<T> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Alert)
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = Instant::now() + timeout;
    let mut out = Vec::new();
    while out.len() < n {
        assert!(
            Instant::now() < deadline,
            "{topic}: consumed only {} of {n} within {timeout:?}",
            out.len()
        );
        if let Some(message) = consumer.poll(Duration::from_millis(250)) {
            out.extend(keep(&message.expect("message")));
        }
    }
    out
}

/// Serve the source's control plane once. Panics on any event but `Idle` and
/// on a non-retryable error; retryable errors are appended to `errors`.
pub(crate) fn serve_events(source: &mut KafkaSource, errors: &mut Vec<String>) {
    match source.poll_events(Duration::from_millis(50)) {
        Ok(SourceEvent::Idle) => {}
        Ok(other) => panic!("unexpected source event: {other:?}"),
        Err(
            e @ SourceError::Client {
                class: ErrorClass::Retryable,
                ..
            },
        ) => errors.push(e.to_string()),
        Err(e) => panic!("poll_events: {e}"),
    }
}

/// Poll a lane until `want` payloads arrive, serving the source between
/// polls; returns (payload, key, offset). Panics if they do not arrive within
/// `timeout`.
pub(crate) fn drain_lane(
    source: &mut KafkaSource,
    lane: &mut <KafkaSource as Source>::Lane,
    want: usize,
    timeout: Duration,
) -> Vec<(Vec<u8>, Vec<u8>, i64)> {
    let mut got = Vec::new();
    let mut errors = Vec::new();
    let deadline = Instant::now() + timeout;
    while got.len() < want {
        assert!(
            Instant::now() < deadline,
            "lane delivered {}/{want} before deadline; retryable errors: {errors:?}",
            got.len()
        );
        serve_events(source, &mut errors);
        let Some(mut batch) = lane
            .poll(64, Duration::from_millis(500))
            .expect("lane poll")
        else {
            continue;
        };
        while let Some(raw) = batch.next_payload() {
            got.push((
                raw.bytes.to_vec(),
                raw.key.unwrap_or(&[]).to_vec(),
                raw.offset,
            ));
        }
    }
    got
}
