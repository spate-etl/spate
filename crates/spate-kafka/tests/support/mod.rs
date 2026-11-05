//! The pinned broker, and the produce, consume and lane-draining helpers
//! shared by the Kafka integration suites.

// A per-item `expect` goes unfulfilled in whichever target uses the item.
#![allow(dead_code, reason = "each target uses a different subset")]

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::record::PartitionId;
use spate_core::source::{DrainBarrier, LaneId, PayloadBatch, Source, SourceEvent, SourceLane};
use spate_kafka::KafkaSource;
use spate_test_support::container_image;
use std::io::{Read as _, Write as _};
use std::mem::ManuallyDrop;
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use testcontainers::{ContainerRequest, ImageExt};
use testcontainers_modules::kafka::apache::Kafka;

pub(crate) mod feed;
pub(crate) mod latest;

/// How the member under test answers the offer it held.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Answer {
    /// `assign` of an empty list.
    Empty,
    /// `assign` of the offer.
    Offer,
}

/// A consumer context that records each rebalance, answering it inline only
/// when told to.
struct Recording {
    answers: bool,
    offers: Mutex<Vec<TopicPartitionList>>,
    revocations: Mutex<usize>,
}

impl ClientContext for Recording {}

impl ConsumerContext for Recording {
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        if err == RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
            self.offers.lock().expect("offers").push(tpl.clone());
            if self.answers {
                base_consumer.assign(tpl).expect("assign");
            }
        } else {
            *self.revocations.lock().expect("revocations") += 1;
            if self.answers {
                base_consumer.unassign().expect("unassign");
            }
        }
    }
}

/// A member of `group` on `topic` that records each rebalance, answering it
/// inline only when `answers`. It never closes, on success or on a panic: closing
/// a member holding an unanswered offer waits on an answer that never comes.
fn recording_member(
    brokers: &str,
    topic: &str,
    group: &str,
    answers: bool,
) -> ManuallyDrop<BaseConsumer<Recording>> {
    let consumer: BaseConsumer<Recording> = raw_config()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("session.timeout.ms", "6000")
        .set("max.poll.interval.ms", "6000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .build_with(Recording {
            answers,
            offers: Mutex::new(Vec::new()),
            revocations: Mutex::new(0),
        })
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    ManuallyDrop::new(consumer)
}

/// A joins alone and holds its offer. B joins, is offered the partition and
/// commits it. A's commit with its old generation is then refused, and B's offset
/// stands.
pub(crate) fn a_stale_member_cannot_replace_a_peers_offset(
    brokers: &str,
    topic: &str,
    group: &str,
) {
    let offers = |c: &BaseConsumer<Recording>| c.context().offers.lock().expect("offers").len();
    let a = recording_member(brokers, topic, group, false);
    spate_test::wait_until(Duration::from_secs(60), "A's offer", || {
        let _ = a.poll(Duration::from_millis(100));
        offers(&a) == 1
    });
    let b = recording_member(brokers, topic, group, true);
    spate_test::wait_until(Duration::from_secs(60), "B's offer", || {
        let _ = a.poll(Duration::from_millis(50));
        let _ = b.poll(Duration::from_millis(50));
        offers(&b) >= 1
    });
    let commit = |c: &BaseConsumer<Recording>, offset: i64| {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, 0, Offset::Offset(offset))
            .expect("offset");
        c.commit(&tpl, CommitMode::Sync)
    };
    commit(&b, 5).expect("the current member's commit");
    assert!(commit(&a, 9).is_err(), "the old generation's commit");
    assert_eq!(committed_by(brokers, group, topic, 1), [Offset::Offset(5)]);
}

/// Pins the client and group sequence around a member that holds an offer past
/// the group's patience. A joins alone and leaves its offer unanswered. B joins
/// and is offered A's partition, while A's client reports nothing lost. Once A
/// answers with `answer`, a revocation marked lost follows, and `unassign` brings
/// a new offer. [`Answer::Empty`] sends no commit, as a discard does, so A's next
/// heartbeat brings the revocation. [`Answer::Offer`] commits the partition, the
/// group refuses that commit, and its refusal brings the revocation.
pub(crate) fn a_member_that_never_answers_its_offer(
    brokers: &str,
    topic: &str,
    group: &str,
    answer: Answer,
) {
    let member = |answers| recording_member(brokers, topic, group, answers);
    let offers = |c: &BaseConsumer<Recording>| c.context().offers.lock().expect("offers").len();
    let revocations =
        |c: &BaseConsumer<Recording>| *c.context().revocations.lock().expect("revocations");

    let a = member(false);
    spate_test::wait_until(Duration::from_secs(60), "A's offer", || {
        let _ = a.poll(Duration::from_millis(100));
        offers(&a) == 1
    });
    let offer = a.context().offers.lock().expect("offers")[0].clone();
    assert!(offer.count() > 0, "A was offered the topic");
    let partition = offer.elements()[0].partition();

    let b = member(true);
    spate_test::wait_until(
        Duration::from_secs(60),
        "B's offer of A's partition",
        || {
            let _ = a.poll(Duration::from_millis(50));
            let _ = b.poll(Duration::from_millis(50));
            b.context()
                .offers
                .lock()
                .expect("offers")
                .iter()
                .any(|o| o.elements().iter().any(|e| e.partition() == partition))
        },
    );
    assert!(!a.assignment_lost(), "a held member is not marked lost");

    match answer {
        Answer::Empty => a.assign(&TopicPartitionList::new()).expect("assign empty"),
        Answer::Offer => a.assign(&offer).expect("assign the offer"),
    }
    if matches!(answer, Answer::Offer) {
        let mut commit = TopicPartitionList::new();
        commit
            .add_partition_offset(topic, partition, Offset::Offset(1))
            .expect("offset");
        a.commit(&commit, CommitMode::Sync)
            .expect_err("the group refuses a commit from the member it left");
    }

    spate_test::wait_until(
        Duration::from_secs(60),
        "A's revocation marked lost",
        || {
            let _ = a.poll(Duration::from_millis(50));
            let _ = b.poll(Duration::from_millis(50));
            revocations(&a) > 0 && a.assignment_lost()
        },
    );
    a.unassign().expect("unassign");
    spate_test::wait_until(Duration::from_secs(60), "a new offer for A", || {
        let _ = a.poll(Duration::from_millis(50));
        let _ = b.poll(Duration::from_millis(50));
        offers(&a) >= 2
    });
}

/// A client configuration a test builds itself, outside the crate's client
/// factory: a seeding producer, a reading consumer, an admin client.
#[expect(
    clippy::disallowed_methods,
    reason = "a test client is configured outside the factory's roles"
)]
pub(crate) fn raw_config() -> ClientConfig {
    ClientConfig::new()
}

/// Builds a client from a [`raw_config`].
pub(crate) trait Raw {
    fn build<T: rdkafka::config::FromClientConfig>(&self) -> rdkafka::error::KafkaResult<T>;

    fn build_with<C, T>(&self, context: C) -> rdkafka::error::KafkaResult<T>
    where
        C: rdkafka::client::ClientContext,
        T: rdkafka::config::FromClientConfigAndContext<C>;
}

impl Raw for ClientConfig {
    #[expect(
        clippy::disallowed_methods,
        reason = "a test client is built outside the factory's roles"
    )]
    fn build<T: rdkafka::config::FromClientConfig>(&self) -> rdkafka::error::KafkaResult<T> {
        self.create()
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "a test client is built outside the factory's roles"
    )]
    fn build_with<C, T>(&self, context: C) -> rdkafka::error::KafkaResult<T>
    where
        C: rdkafka::client::ClientContext,
        T: rdkafka::config::FromClientConfigAndContext<C>,
    {
        self.create_with_context(context)
    }
}

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
    let producer: BaseProducer = raw_config()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "30000")
        .build()
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

/// Produce one record per offset of `offsets` to `partition`, each carrying its
/// own offset as its payload, and flush. The caller keeps `offsets` to the
/// partition's next offsets, so a test can name what a sink should have.
pub(crate) fn produce_to(
    brokers: &str,
    topic: &str,
    partition: i32,
    offsets: std::ops::Range<i64>,
) {
    let producer: BaseProducer = raw_config()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "30000")
        .build()
        .expect("producer");
    for offset in offsets {
        let payload = format!("{partition}:{offset}");
        producer
            .send(
                BaseRecord::to(topic)
                    .partition(partition)
                    .payload(payload.as_bytes())
                    .key(payload.as_bytes()),
            )
            .expect("enqueue");
    }
    producer.flush(Duration::from_secs(60)).expect("flush");
}

/// Produce one record with `payload` to `partition`, and flush.
pub(crate) fn produce_payload(brokers: &str, topic: &str, partition: i32, payload: &[u8]) {
    let producer: BaseProducer = raw_config()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "30000")
        .build()
        .expect("producer");
    producer
        .send(
            BaseRecord::<[u8], _>::to(topic)
                .partition(partition)
                .payload(payload),
        )
        .expect("enqueue");
    producer.flush(Duration::from_secs(60)).expect("flush");
}

/// Create `topic` with `partitions` partitions and one replica.
pub(crate) fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = raw_config()
        .set("bootstrap.servers", brokers)
        .build()
        .expect("admin client");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(admin.create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        ))
        .expect("create topics")
        .into_iter()
        .for_each(|r| {
            r.expect("topic created");
        });
}

/// What `probe`'s group has committed on `topic`'s partitions `0..partitions`.
/// No commit reads as `Offset::Invalid`.
pub(crate) fn committed(probe: &BaseConsumer, topic: &str, partitions: i32) -> Vec<Offset> {
    let mut tpl = TopicPartitionList::new();
    for p in 0..partitions {
        tpl.add_partition(topic, p);
    }
    probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed")
        .elements()
        .iter()
        .map(|e| e.offset())
        .collect()
}

/// [`committed`] through a probe of its own in `group`.
pub(crate) fn committed_by(
    brokers: &str,
    group: &str,
    topic: &str,
    partitions: i32,
) -> Vec<Offset> {
    let probe: BaseConsumer = raw_config()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .build()
        .expect("probe");
    committed(&probe, topic, partitions)
}

/// Every offset of `from..upto` is in `written`.
pub(crate) fn assert_no_gap(
    partition: i32,
    from: i64,
    upto: i64,
    written: &std::collections::BTreeSet<i64>,
) {
    let missing: Vec<i64> = (from..upto).filter(|o| !written.contains(o)).collect();
    assert!(
        missing.is_empty(),
        "partition {partition}: committed {upto} past {} unwritten records, first {:?}",
        missing.len(),
        missing.first()
    );
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
    let consumer: BaseConsumer = raw_config()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Alert)
        .build()
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

/// The reason `outcome` failed retryably, which the runtime logs and carries
/// on from. Panics with `call` on any other failure.
pub(crate) fn retryable<T>(call: &str, outcome: Result<T, SourceError>) -> Result<T, String> {
    match outcome {
        Ok(value) => Ok(value),
        Err(SourceError::Client {
            class: ErrorClass::Retryable,
            reason,
        }) => Err(reason),
        Err(e) => panic!("{call}: {e}"),
    }
}

/// Serve the source's control plane once. Panics on any event but `Idle` and
/// on a non-retryable error; retryable errors are appended to `errors`.
pub(crate) fn serve_events(source: &mut KafkaSource, errors: &mut Vec<String>) {
    match retryable("poll_events", source.poll_events(Duration::from_millis(50))) {
        Ok(SourceEvent::Idle) => {}
        Ok(other) => panic!("unexpected source event: {other:?}"),
        Err(reason) => errors.push(reason),
    }
}

/// Plays the runtime's part after a revocation: arrives once per lane, stores
/// `watermarks` and returns the flush's result.
pub(crate) fn complete_revocation(
    source: &mut KafkaSource,
    lanes: &[LaneId],
    barrier: &DrainBarrier,
    watermarks: &[(PartitionId, i64)],
) -> Result<(), SourceError> {
    for _ in lanes {
        barrier.arrive();
    }
    source.commit(watermarks).expect("commit");
    source.flush_commits()
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
