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
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use testcontainers::{ContainerRequest, ImageExt};
use testcontainers_modules::kafka::apache::Kafka;

pub(crate) mod feed;

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

fn zigzag(buf: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let b = (z & 0x7f) as u8;
        z >>= 7;
        if z == 0 {
            buf.push(b);
            break;
        }
        buf.push(b | 0x80);
    }
}

/// One magic-2 record batch holding one keyed record, with a CRC that does
/// not match its contents.
pub(crate) fn corrupt_batch(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut rec = vec![0u8];
    zigzag(&mut rec, 0);
    zigzag(&mut rec, 0);
    zigzag(&mut rec, key.len() as i64);
    rec.extend_from_slice(key);
    zigzag(&mut rec, value.len() as i64);
    rec.extend_from_slice(value);
    zigzag(&mut rec, 0);
    let mut records = Vec::new();
    zigzag(&mut records, rec.len() as i64);
    records.extend_from_slice(&rec);
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let mut tail = Vec::new();
    tail.extend_from_slice(&0i16.to_be_bytes());
    tail.extend_from_slice(&0i32.to_be_bytes());
    tail.extend_from_slice(&now.to_be_bytes());
    tail.extend_from_slice(&now.to_be_bytes());
    tail.extend_from_slice(&(-1i64).to_be_bytes());
    tail.extend_from_slice(&(-1i16).to_be_bytes());
    tail.extend_from_slice(&(-1i32).to_be_bytes());
    tail.extend_from_slice(&1i32.to_be_bytes());
    tail.extend_from_slice(&records);
    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes());
    batch.extend_from_slice(&((4 + 1 + 4 + tail.len()) as i32).to_be_bytes());
    batch.extend_from_slice(&(-1i32).to_be_bytes());
    batch.push(2);
    batch.extend_from_slice(&0xdead_beefu32.to_be_bytes());
    batch.extend_from_slice(&tail);
    batch
}

/// Send `batch` to partition 0 of `topic` as a version 3 Produce request
/// and return the partition's error code. A client library would compute
/// the batch's CRC itself; the mock stores the batch as sent.
pub(crate) fn produce_batch(broker: &str, topic: &str, batch: &[u8]) -> i16 {
    let mut b = Vec::new();
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&3i16.to_be_bytes());
    b.extend_from_slice(&7i32.to_be_bytes());
    b.extend_from_slice(&5i16.to_be_bytes());
    b.extend_from_slice(b"spate");
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&10_000i32.to_be_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&(topic.len() as i16).to_be_bytes());
    b.extend_from_slice(topic.as_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&(batch.len() as i32).to_be_bytes());
    b.extend_from_slice(batch);
    let mut s = TcpStream::connect(broker).unwrap();
    s.write_all(&(b.len() as i32).to_be_bytes()).unwrap();
    s.write_all(&b).unwrap();
    let mut len = [0u8; 4];
    s.read_exact(&mut len).unwrap();
    let mut resp = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
    s.read_exact(&mut resp).unwrap();
    let at = 4 + 4 + 2 + topic.len() + 4 + 4;
    i16::from_be_bytes([resp[at], resp[at + 1]])
}

/// CRC-32 (IEEE), the checksum of a magic-0/1 message.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

/// CRC-32C, the checksum of a magic-2 batch.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

/// A valid magic-2 batch holding one record, whose header reserves
/// `reserve` offsets in the mock's log.
pub(crate) fn v2_batch(key: &[u8], value: &[u8], reserve: i32) -> Vec<u8> {
    let mut rec = vec![0u8];
    zigzag(&mut rec, 0);
    zigzag(&mut rec, 0);
    zigzag(&mut rec, key.len() as i64);
    rec.extend_from_slice(key);
    zigzag(&mut rec, value.len() as i64);
    rec.extend_from_slice(value);
    zigzag(&mut rec, 0);
    let mut records = Vec::new();
    zigzag(&mut records, rec.len() as i64);
    records.extend_from_slice(&rec);
    let mut tail = Vec::new();
    tail.extend_from_slice(&0i16.to_be_bytes());
    tail.extend_from_slice(&0i32.to_be_bytes());
    tail.extend_from_slice(&1i64.to_be_bytes());
    tail.extend_from_slice(&1i64.to_be_bytes());
    tail.extend_from_slice(&(-1i64).to_be_bytes());
    tail.extend_from_slice(&(-1i16).to_be_bytes());
    tail.extend_from_slice(&(-1i32).to_be_bytes());
    tail.extend_from_slice(&reserve.to_be_bytes());
    tail.extend_from_slice(&records);
    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes());
    batch.extend_from_slice(&((4 + 1 + 4 + tail.len()) as i32).to_be_bytes());
    batch.extend_from_slice(&0i32.to_be_bytes());
    batch.push(2);
    batch.extend_from_slice(&crc32c(&tail).to_be_bytes());
    batch.extend_from_slice(&tail);
    batch
}

/// One magic-1 message at `offset`; `corrupt` gives it a CRC that does
/// not match its contents.
pub(crate) fn v1_message(offset: i64, key: &[u8], value: &[u8], corrupt: bool) -> Vec<u8> {
    let mut body = vec![1u8, 0u8];
    body.extend_from_slice(&1i64.to_be_bytes());
    body.extend_from_slice(&(key.len() as i32).to_be_bytes());
    body.extend_from_slice(key);
    body.extend_from_slice(&(value.len() as i32).to_be_bytes());
    body.extend_from_slice(value);
    let crc = crc32_ieee(&body) ^ u32::from(corrupt);
    let mut msg = Vec::new();
    msg.extend_from_slice(&offset.to_be_bytes());
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&crc.to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}
