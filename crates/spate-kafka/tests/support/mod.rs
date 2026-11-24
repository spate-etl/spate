//! The pinned broker, and the produce, consume and lane-draining helpers
//! shared by the Kafka integration suites.

// A per-item `expect` goes unfulfilled in whichever target uses the item.
#![allow(dead_code, reason = "each target uses a different subset")]

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext};
use rdkafka::message::{BorrowedMessage, Headers as _, Message as _};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::{Offset, TopicPartitionList};
use spate_core::checkpoint::Checkpointer;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::metrics::names;
use spate_core::record::PartitionId;
use spate_core::source::{
    DrainBarrier, LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane,
};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test::HANG;
use spate_test_support::KafkaBroker;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use testcontainers::ContainerRequest;

pub(crate) mod feed;
pub(crate) mod group;
pub(crate) mod latest;
pub(crate) mod mock;
pub(crate) mod raw;
pub(crate) mod sink;
#[cfg(feature = "tls")]
pub(crate) mod tls;
pub(crate) mod topics;
pub(crate) mod wire;

#[allow(unused_imports, reason = "each target uses a different subset")]
pub(crate) use mock::MockKafka;
#[allow(unused_imports, reason = "each target uses a different subset")]
pub(crate) use raw::{Raw, raw_config};
#[allow(unused_imports, reason = "each target uses a different subset")]
pub(crate) use wire::{
    BatchCrc, Marker, RawBatch, control_batch, corrupt_batch, produce_batch, try_produce_batch,
    v1_message, v2_batch, write_marker,
};

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
    spate_test::wait_until(HANG, "A's offer", || {
        let _ = a.poll(Duration::from_millis(100));
        offers(&a) == 1
    });
    let b = recording_member(brokers, topic, group, true);
    spate_test::wait_until(HANG, "B's offer", || {
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
    spate_test::wait_until(HANG, "A's offer", || {
        let _ = a.poll(Duration::from_millis(100));
        offers(&a) == 1
    });
    let offer = a.context().offers.lock().expect("offers")[0].clone();
    assert!(offer.count() > 0, "A was offered the topic");
    let partition = offer.elements()[0].partition();

    let b = member(true);
    spate_test::wait_until(HANG, "B's offer of A's partition", || {
        let _ = a.poll(Duration::from_millis(50));
        let _ = b.poll(Duration::from_millis(50));
        b.context()
            .offers
            .lock()
            .expect("offers")
            .iter()
            .any(|o| o.elements().iter().any(|e| e.partition() == partition))
    });
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

    spate_test::wait_until(HANG, "A's revocation marked lost", || {
        let _ = a.poll(Duration::from_millis(50));
        let _ = b.poll(Duration::from_millis(50));
        revocations(&a) > 0 && a.assignment_lost()
    });
    a.unassign().expect("unassign");
    spate_test::wait_until(HANG, "a new offer for A", || {
        let _ = a.poll(Duration::from_millis(50));
        let _ = b.poll(Duration::from_millis(50));
        offers(&a) >= 2
    });
}

/// The broker the selected `ci/kafka/` lane pins, pulled by digest.
pub(crate) fn broker() -> ContainerRequest<KafkaBroker> {
    KafkaBroker::selected().into()
}

/// [`broker`], taking transactions.
pub(crate) fn transactional_broker() -> ContainerRequest<KafkaBroker> {
    KafkaBroker::selected().transactional().into()
}

/// Enqueue `count` records to partition `p`, with payload `{tag}-p{p}-{i}` and
/// key `k{p}-{i}`.
fn send_records(producer: &BaseProducer, topic: &str, p: i32, count: usize, tag: &str) {
    let deadline = Instant::now() + HANG;
    for i in 0..count {
        let payload = format!("{tag}-p{p}-{i}");
        let key = format!("k{p}-{i}");
        let record = BaseRecord::to(topic)
            .partition(p)
            .payload(payload.as_bytes())
            .key(key.as_bytes());
        spate_test_support::send(producer, record, deadline).expect("enqueue");
    }
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
    let producer = spate_test_support::producer(brokers).expect("producer");
    for p in 0..partitions {
        send_records(&producer, topic, p, per_partition, tag);
    }
    producer.flush(Duration::from_secs(60)).expect("flush");
}

/// How [`produce_transaction`] leaves its transaction.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Transaction {
    Commit,
    Abort,
    /// Still open, for the returned producer to end. The broker aborts it once
    /// the transaction timeout passes, whether or not the producer lives.
    Open,
}

/// How long a transactional call waits on the broker.
pub(crate) const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

/// A producer on `brokers` initialised for transactions under an id derived
/// from `topic` and `tag`.
pub(crate) fn transactional_producer(brokers: &str, topic: &str, tag: &str) -> BaseProducer {
    spate_test_support::transactional_producer(
        brokers,
        &format!("{topic}-{tag}"),
        TRANSACTION_TIMEOUT,
    )
    .expect("init")
}

/// Produce `count` records to partition 0 of `topic` in one transaction under
/// a transactional id derived from `tag`, flush them, and end the transaction
/// as `ending` says. Returns the producer, which an open transaction needs.
#[must_use = "an open transaction ends through this producer"]
pub(crate) fn produce_transaction(
    brokers: &str,
    topic: &str,
    count: usize,
    tag: &str,
    ending: Transaction,
) -> BaseProducer {
    let producer = transactional_producer(brokers, topic, tag);
    producer.begin_transaction().expect("begin");
    send_records(&producer, topic, 0, count, tag);
    producer.flush(TRANSACTION_TIMEOUT).expect("flush");
    match ending {
        Transaction::Commit => producer
            .commit_transaction(TRANSACTION_TIMEOUT)
            .expect("commit"),
        Transaction::Abort => producer
            .abort_transaction(TRANSACTION_TIMEOUT)
            .expect("abort"),
        Transaction::Open => {}
    }
    producer
}

/// The end of `partition`'s log. A `read_committed` client reads the last stable
/// offset here instead, which an open transaction holds below the log's end.
pub(crate) fn log_end(brokers: &str, topic: &str, partition: i32) -> i64 {
    high_watermark(brokers, topic, partition, "read_uncommitted")
}

/// [`log_end`] as one client call, for a caller whose own wait reads again.
pub(crate) fn log_end_once(brokers: &str, topic: &str, partition: i32) -> Result<i64, String> {
    high_watermark_once(brokers, topic, partition, "read_uncommitted")
}

/// The end of `partition`'s log once no transaction on it is open, so the
/// marker of each ended transaction is in it. A producer's commit or abort
/// returns before the broker writes the markers.
pub(crate) fn settled_end(brokers: &str, topic: &str, partition: i32) -> i64 {
    let ends = spate_test::wait_for_value(
        HANG,
        "the transaction markers",
        || {
            Ok::<_, String>((
                log_end_once(brokers, topic, partition)?,
                high_watermark_once(brokers, topic, partition, "read_committed")?,
            ))
        },
        |read| read.as_ref().is_ok_and(|(end, stable)| end == stable),
    );
    let (end, _) = ends.expect("the wait ends on a read");
    end
}

/// The high watermark of `partition` as a client at `isolation` reads it.
fn high_watermark(brokers: &str, topic: &str, partition: i32, isolation: &str) -> i64 {
    spate_test_support::high_watermark(&isolated(brokers, isolation), topic, partition, HANG)
        .unwrap_or_else(|e| panic!("{e}"))
}

/// [`high_watermark`] as one client call.
fn high_watermark_once(
    brokers: &str,
    topic: &str,
    partition: i32,
    isolation: &str,
) -> Result<i64, String> {
    spate_test_support::high_watermark_once(&isolated(brokers, isolation), topic, partition)
}

/// A reader of `brokers` at `isolation`.
fn isolated(brokers: &str, isolation: &str) -> BaseConsumer {
    spate_test_support::consumer(brokers, &[("isolation.level", isolation)])
        .unwrap_or_else(|e| panic!("{e}"))
}

/// A consumer in `group` that reads or writes the group's committed offsets
/// and joins nothing.
pub(crate) fn probe(brokers: &str, group: &str) -> BaseConsumer {
    spate_test_support::consumer(brokers, &[("group.id", group)]).unwrap_or_else(|e| panic!("{e}"))
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
    let producer = spate_test_support::producer(brokers).expect("producer");
    let deadline = Instant::now() + HANG;
    for offset in offsets {
        send_at(&producer, topic, partition, offset, deadline).expect("enqueue");
    }
    producer.flush(Duration::from_secs(60)).expect("flush");
}

/// [`produce_to`] for one offset, sent and flushed within 10 s, for a caller
/// whose own wait produces again.
pub(crate) fn produce_once(
    brokers: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<(), String> {
    let producer = spate_test_support::producer(brokers)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    send_at(&producer, topic, partition, offset, deadline)?;
    producer
        .flush(deadline.saturating_duration_since(Instant::now()))
        .map_err(|e| format!("a flush to {topic}/{partition}: {e}"))
}

/// Enqueue the record [`produce_to`] writes for `offset`.
fn send_at(
    producer: &BaseProducer,
    topic: &str,
    partition: i32,
    offset: i64,
    deadline: Instant,
) -> Result<(), String> {
    let payload = format!("{partition}:{offset}");
    let record = BaseRecord::to(topic)
        .partition(partition)
        .payload(payload.as_bytes())
        .key(payload.as_bytes());
    spate_test_support::send(producer, record, deadline)
}

/// Produce one record with `payload` to `partition`, and flush.
pub(crate) fn produce_payload(brokers: &str, topic: &str, partition: i32, payload: &[u8]) {
    let producer = spate_test_support::producer(brokers).expect("producer");
    let record = BaseRecord::<[u8], _>::to(topic)
        .partition(partition)
        .payload(payload);
    spate_test_support::send(&producer, record, Instant::now() + HANG).expect("enqueue");
    producer.flush(Duration::from_secs(60)).expect("flush");
}

/// Create `topic` with `partitions` partitions and one replica.
pub(crate) fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    create_visible(
        brokers,
        &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
    );
}

/// Create `topics` and wait until the broker's metadata lists each one. A
/// broker can answer a topic it has just created as unknown for a while.
pub(crate) fn create_visible(brokers: &str, topics: &[NewTopic<'_>]) {
    spate_test_support::create_topics(brokers, topics, HANG).unwrap_or_else(|e| panic!("{e}"));
}

/// Delete `partition`'s records below `before`, which moves its log start there.
///
/// # Panics
///
/// When the request fails or the broker refuses the partition.
pub(crate) fn delete_records_below(brokers: &str, topic: &str, partition: i32, before: i64) {
    let admin: AdminClient<DefaultClientContext> = raw_config()
        .set("bootstrap.servers", brokers)
        .build()
        .expect("admin client");
    let mut below = TopicPartitionList::new();
    below
        .add_partition_offset(topic, partition, Offset::Offset(before))
        .expect("offset");
    let deleted = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(admin.delete_records(&below, &AdminOptions::new()))
        .expect("delete records");
    for element in deleted.elements() {
        assert!(element.error().is_ok(), "{:?}", element.error());
    }
}

/// What `probe`'s group has committed on `topic`'s partitions `0..partitions`,
/// read again while the broker refuses. No commit reads as `Offset::Invalid`.
pub(crate) fn committed(probe: &BaseConsumer, topic: &str, partitions: i32) -> Vec<Offset> {
    committed_within(probe, topic, partitions, HANG)
}

/// [`committed`], reading again for at most `bound`, for a read that cannot
/// wait a whole [`HANG`].
pub(crate) fn committed_within(
    probe: &BaseConsumer,
    topic: &str,
    partitions: i32,
    bound: Duration,
) -> Vec<Offset> {
    spate_test_support::committed_offsets(probe, topic, partitions, bound)
        .unwrap_or_else(|e| panic!("{e}"))
}

/// [`committed`] as one client call, for a caller whose own wait reads again.
pub(crate) fn committed_once(
    probe: &BaseConsumer,
    topic: &str,
    partitions: i32,
) -> Result<Vec<Offset>, String> {
    spate_test_support::committed_offsets_once(probe, topic, partitions)
}

/// [`committed`] through a new [`probe`] in `group`.
pub(crate) fn committed_by(
    brokers: &str,
    group: &str,
    topic: &str,
    partitions: i32,
) -> Vec<Offset> {
    committed(&probe(brokers, group), topic, partitions)
}

/// [`committed_by`] as one client call, for a caller whose own wait reads
/// again.
pub(crate) fn committed_by_once(
    brokers: &str,
    group: &str,
    topic: &str,
    partitions: i32,
) -> Result<Vec<Offset>, String> {
    committed_once(&probe(brokers, group), topic, partitions)
}

/// One group as the broker's group listing shows it.
#[derive(Debug)]
pub(crate) struct ListedGroup {
    pub(crate) state: String,
    pub(crate) members: usize,
}

/// `group`'s listing once `done` holds for it. A failed listing is asked
/// again, since a slow broker fails one with a local timeout.
pub(crate) fn group_listing(
    brokers: &str,
    group: &str,
    what: &str,
    mut done: impl FnMut(&[ListedGroup]) -> bool,
) -> Vec<ListedGroup> {
    let probe: BaseConsumer = raw_config()
        .set("bootstrap.servers", brokers)
        .build()
        .expect("probe");
    spate_test::wait_for_value(
        HANG,
        what,
        || {
            probe
                .fetch_group_list(Some(group), Duration::from_secs(10))
                .map(|list| {
                    list.groups()
                        .iter()
                        .map(|g| ListedGroup {
                            state: g.state().to_owned(),
                            members: g.members().len(),
                        })
                        .collect::<Vec<_>>()
                })
        },
        |listing| listing.as_ref().is_ok_and(|groups| done(groups)),
    )
    .expect("the wait ends on a listing")
}

/// Commit `offset` for partition 0 of `topic` in `group` through a new
/// [`probe`] outside the group, sent again while the broker refuses. The
/// broker checks no offset, so a commit above the partition's end stands as a
/// truncation leaves one.
pub(crate) fn commit_at(brokers: &str, group: &str, topic: &str, offset: i64) {
    commit_with_epoch(brokers, group, topic, offset, -1);
}

/// [`commit_at`] with leader epoch `epoch`, or none for -1. The broker checks
/// no epoch either, so an epoch the partition's leader never had stands as a
/// commit from before an unclean election does.
pub(crate) fn commit_with_epoch(brokers: &str, group: &str, topic: &str, offset: i64, epoch: i32) {
    let probe = probe(brokers, group);
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Offset(offset))
        .expect("offset");
    let elem = find_elem(&tpl, topic, 0);
    // SAFETY: `elem` is a non-null element of `tpl`, written under this
    // exclusive borrow.
    unsafe { rdkafka::bindings::rd_kafka_topic_partition_set_leader_epoch(elem, epoch) };
    spate_test::wait_until(HANG, "the commit lands", || {
        probe.commit(&tpl, CommitMode::Sync).is_ok()
    });
}

/// `tpl`'s element for `partition` of `topic`, which must be there.
fn find_elem(
    tpl: &TopicPartitionList,
    topic: &str,
    partition: i32,
) -> *mut rdkafka::bindings::rd_kafka_topic_partition_t {
    let name = std::ffi::CString::new(topic).expect("topic name");
    // SAFETY: `tpl` owns its list while it is borrowed, and `name` outlives
    // the call, which keeps neither pointer.
    let elem = unsafe {
        rdkafka::bindings::rd_kafka_topic_partition_list_find(tpl.ptr(), name.as_ptr(), partition)
    };
    assert!(!elem.is_null(), "{topic} [{partition}] is in the list");
    elem
}

/// The offset and leader epoch `group` has committed on partition 0 of
/// `topic`, read again while the broker refuses.
pub(crate) fn committed_epoch(brokers: &str, group: &str, topic: &str) -> (Offset, i32) {
    let read = spate_test_support::committed(&probe(brokers, group), topic, 1, HANG)
        .unwrap_or_else(|e| panic!("{e}"));
    let elem = find_elem(&read, topic, 0);
    // SAFETY: `elem` is a non-null element of `read`, borrowed across the read.
    let epoch = unsafe { rdkafka::bindings::rd_kafka_topic_partition_get_leader_epoch(elem) };
    (read.elements()[0].offset(), epoch)
}

/// The leader epoch the broker sends with the record at `offset` on
/// partition 0 of `topic`.
pub(crate) fn record_epoch(brokers: &str, topic: &str, offset: i64) -> i32 {
    let reader: BaseConsumer = raw_config()
        .set("bootstrap.servers", brokers)
        .set("group.id", spate_test::unique_name("record-epoch"))
        .set("enable.auto.commit", "false")
        .build()
        .expect("reader");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Offset(offset))
        .expect("offset");
    reader.assign(&tpl).expect("assign");
    let mut epoch = None;
    spate_test::wait_until(HANG, "the record arrives", || {
        if let Some(Ok(message)) = reader.poll(Duration::from_millis(100)) {
            // SAFETY: `message` keeps its pointer valid while it is borrowed.
            epoch =
                Some(unsafe { rdkafka::bindings::rd_kafka_message_leader_epoch(message.ptr()) });
        }
        epoch.is_some()
    });
    epoch.expect("read before the wait ended")
}

/// The lag `pipeline` publishes for `partition`, or `None` before it does.
pub(crate) fn lag_of(rendered: &str, pipeline: &str, partition: i32) -> Option<f64> {
    spate_test::metric_value(
        rendered,
        names::SOURCE_LAG_RECORDS,
        &[
            ("pipeline", pipeline),
            ("partition", &partition.to_string()),
        ],
    )
}

/// The ops partition 0's fetch queue holds, as `rendered` under `pipeline` shows
/// it. A poll finds them without waiting on the fetcher. 0 before it is published.
pub(crate) fn fetch_queue_entries(rendered: &str, pipeline: &str) -> f64 {
    spate_test::metric_value(
        rendered,
        "spate_kafka_partition_fetch_queue_messages",
        &[("pipeline", pipeline), ("partition", "0")],
    )
    .unwrap_or(0.0)
}

/// A recorder of the test's own, its handle, and a meter labelling series with
/// `pipeline`.
pub(crate) fn recorded_meter(
    pipeline: impl Into<metrics::SharedString>,
) -> (
    metrics_exporter_prometheus::PrometheusRecorder,
    metrics_exporter_prometheus::PrometheusHandle,
    spate_core::metrics::Meter,
) {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let meter = spate_core::metrics::Meter::with_namespace("kafka", pipeline, "source", "kafka");
    (recorder, handle, meter)
}

/// The offsets of `batch`, none for one that retires offsets, and acknowledges
/// it by dropping it.
pub(crate) fn offsets_of(
    batch: Option<<<KafkaSource as Source>::Lane as SourceLane>::Batch<'_>>,
) -> Option<Vec<i64>> {
    batch.map(|mut b| {
        let mut offsets = Vec::new();
        while let Some(raw) = b.next_payload() {
            offsets.push(raw.offset);
        }
        offsets
    })
}

/// A source driven by hand as the controller and a pipeline thread drive it,
/// publishing to a recorder of its own under a pipeline name of its own.
pub(crate) struct Watched {
    // Declared before `source`: a lane holds the consumer the source closes.
    pub(crate) lanes: Vec<<KafkaSource as Source>::Lane>,
    pub(crate) source: KafkaSource,
    pub(crate) cp: Checkpointer,
    pub(crate) pipeline: String,
    recorder: metrics_exporter_prometheus::PrometheusRecorder,
    handle: metrics_exporter_prometheus::PrometheusHandle,
}

impl Watched {
    /// Open `cfg` with its statistics on and begin an epoch on its first
    /// assignment.
    pub(crate) fn open(mut cfg: KafkaSourceConfig) -> Watched {
        cfg.statistics_interval = Duration::from_millis(100);
        let pipeline = spate_test::unique_name("markers");
        let (recorder, handle, meter) = recorded_meter(pipeline.clone());
        let mut cp = Checkpointer::new();
        let mut source = KafkaSource::new(cfg).expect("the config validates");
        let lanes = metrics::with_local_recorder(&recorder, || {
            let labels =
                spate_core::metrics::ComponentLabels::new(pipeline.clone(), "source", "kafka");
            source
                .open(
                    SourceCtx::new(cp.handle())
                        .with_stage_metrics(Some(Arc::new(
                            spate_core::metrics::SourceMetrics::new(&labels),
                        )))
                        .with_meter(Some(meter))
                        .with_partition_detail(true),
                )
                .expect("open");
            await_assignment(&mut source, HANG)
        });
        let partitions: Vec<PartitionId> = lanes.iter().map(SourceLane::partition).collect();
        cp.begin_epoch(&partitions, 1);
        Watched {
            lanes,
            source,
            cp,
            pipeline,
            recorder,
            handle,
        }
    }

    /// Serve the control plane once.
    pub(crate) fn serve(&mut self) {
        let source = &mut self.source;
        metrics::with_local_recorder(&self.recorder, || {
            serve_events(source, &mut Vec::new());
        });
    }

    /// Poll lane `lane` once with a timeout, so an empty return reads the
    /// position, and drop the batch, which acknowledges it. `None` when nothing
    /// came back, and otherwise the batch's offsets, none for one that retires
    /// offsets.
    pub(crate) fn poll(&mut self, lane: usize) -> Option<Vec<i64>> {
        offsets_of(
            self.lanes[lane]
                .poll(64, Duration::from_millis(100))
                .expect("lane poll"),
        )
    }

    /// Poll lane `lane` until it has delivered `want` records, acknowledging
    /// each batch, and return their offsets.
    pub(crate) fn read(&mut self, lane: usize, want: usize) -> Vec<i64> {
        let (source, lane) = (&mut self.source, &mut self.lanes[lane]);
        metrics::with_local_recorder(&self.recorder, || drain_lane(source, lane, want, HANG))
            .into_iter()
            .map(|(_, _, offset)| offset)
            .collect()
    }

    /// Commit what the checkpointer holds, as `commit_cycle` does: nothing
    /// when no watermark moved. Returns the watermarks committed.
    pub(crate) fn commit(&mut self) -> Vec<(PartitionId, i64)> {
        self.cp.drain();
        let watermarks = self.cp.take_watermarks();
        if !watermarks.is_empty() {
            self.source.commit(&watermarks).expect("store");
            self.source.flush_commits().expect("flush");
        }
        watermarks
    }

    /// Poll lane `lane` and commit until partition `partition`'s watermark
    /// reads `want`, and return every watermark committed on the way.
    pub(crate) fn poll_until_committed(
        &mut self,
        lane: usize,
        partition: u32,
        want: i64,
    ) -> Vec<i64> {
        let mut seen = Vec::new();
        spate_test::wait_until(HANG, "the watermark reaches its offset", || {
            self.serve();
            let _ = self.poll(lane);
            seen.extend(
                self.commit()
                    .into_iter()
                    .filter(|(p, _)| p.0 == partition)
                    .map(|(_, w)| w),
            );
            seen.last() == Some(&want)
        });
        seen
    }

    /// The exposition this source has published.
    pub(crate) fn rendered(&self) -> String {
        self.handle.render()
    }

    /// The framework lag of `partition`, or `None` before it is published.
    pub(crate) fn lag(&self, partition: i32) -> Option<f64> {
        lag_of(&self.rendered(), &self.pipeline, partition)
    }

    /// Serve the control plane until `partition`'s framework lag reads `want`.
    pub(crate) fn wait_lag(&mut self, partition: i32, want: f64) {
        spate_test::wait_until(HANG, "the lag reads its value", || {
            self.serve();
            self.lag(partition) == Some(want)
        });
    }
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
    let mut assigned = None;
    spate_test::wait_until(timeout, "an assignment", || {
        if let SourceEvent::LanesAssigned(lanes) = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        {
            assigned = Some(lanes);
        }
        assigned.is_some()
    });
    assigned.expect("an assignment")
}

/// Read `topic` from the earliest offset as consumer group `group`, keeping
/// what `keep` returns for each message until `n` are kept. Panics if they
/// are not kept within [`spate_test::HANG`].
pub(crate) fn consume<T>(
    brokers: &str,
    topic: &str,
    group: &str,
    n: usize,
    keep: impl FnMut(&BorrowedMessage<'_>) -> Option<T>,
) -> Vec<T> {
    let consumer: BaseConsumer = raw_config()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Alert)
        .build()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    read_kept(
        &consumer,
        Take::Exactly(n),
        &format!("{n} messages kept from {topic}"),
        keep,
    )
}

/// How many values [`read_kept`] waits for.
#[derive(Clone, Copy)]
pub(crate) enum Take {
    /// Stop at `n`.
    Exactly(usize),
    /// Wait for `n`, then keep every message already ready, so a test sees
    /// a record written more often than it asks for.
    AtLeast(usize),
}

/// The values `keep` returns for the messages `consumer` reads, as many as
/// `take` asks for; `what` names them in a hang. A failed poll counts as
/// nothing read yet, and a hang prints the last one.
pub(crate) fn read_kept<T>(
    consumer: &BaseConsumer,
    take: Take,
    what: &str,
    mut keep: impl FnMut(&BorrowedMessage<'_>) -> Option<T>,
) -> Vec<T> {
    let (n, exact) = match take {
        Take::Exactly(n) => (n, true),
        Take::AtLeast(n) => (n, false),
    };
    let mut got = Vec::new();
    let mut failed = None;
    spate_test::wait_for_value(
        HANG,
        what,
        || {
            while !exact || got.len() < n {
                match consumer.poll(Duration::from_millis(100)) {
                    Some(Ok(message)) => {
                        failed = None;
                        got.extend(keep(&message));
                    }
                    // Ends the turn, so error events cannot hold it open.
                    Some(Err(e)) => {
                        failed = Some(e.to_string());
                        break;
                    }
                    None => break,
                }
            }
            (got.len(), failed.clone())
        },
        |(kept, _)| *kept >= n,
    );
    got
}

/// One message as read back from a topic: key, value and headers.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Read {
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) headers: Vec<(String, Option<Vec<u8>>)>,
}

impl Read {
    /// A copy of `message`'s key, value and headers.
    pub(crate) fn of(message: &BorrowedMessage<'_>) -> Read {
        Read {
            key: message.key().map(<[u8]>::to_vec),
            payload: message.payload().map(<[u8]>::to_vec),
            headers: message
                .headers()
                .map(|hs| {
                    hs.iter()
                        .map(|h| (h.key.to_owned(), h.value.map(<[u8]>::to_vec)))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .and_then(|(_, v)| std::str::from_utf8(v.as_deref()?).ok())
    }
}

/// A consumer assigned partitions `0..partitions` of `topic` from their
/// start, outside any group's coordination, with `settings` added to its
/// config.
pub(crate) fn from_start(
    brokers: &str,
    topic: &str,
    partitions: i32,
    settings: &[(&str, &str)],
) -> BaseConsumer {
    let group = spate_test::unique_name("from-start");
    let all = [&[("group.id", group.as_str())], settings].concat();
    let partitions: Vec<i32> = (0..partitions).collect();
    spate_test_support::from_start(brokers, topic, &partitions, &all)
        .unwrap_or_else(|e| panic!("{e}"))
}

/// The reason `outcome` failed retryably, which the runtime logs and carries
/// on from. Panics with `call` on any other failure.
pub(crate) fn retryable<T>(call: &str, outcome: Result<T, SourceError>) -> Result<T, String> {
    match outcome {
        Ok(value) => Ok(value),
        Err(e) if e.class() == ErrorClass::Retryable => Err(e.reason().to_owned()),
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

/// A second member joins a group and stays polling until the guard drops, forcing
/// a revocation.
pub(crate) struct Joiner {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Joiner {
    #[expect(
        clippy::disallowed_methods,
        reason = "dropping the guard joins the thread"
    )]
    pub(crate) fn join_group(brokers: &str, topic: &str, group: &str) -> Joiner {
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (stop, brokers, topic, group) = (
                Arc::clone(&stop),
                brokers.to_owned(),
                topic.to_owned(),
                group.to_owned(),
            );
            std::thread::spawn(move || {
                // No auto-commit: this member reads past a marker on its own, and
                // the tests assert what the source committed.
                let consumer: BaseConsumer = raw_config()
                    .set("bootstrap.servers", &brokers)
                    .set("group.id", &group)
                    .set("enable.auto.commit", "false")
                    .build()
                    .expect("joiner");
                consumer.subscribe(&[topic.as_str()]).expect("subscribe");
                while !stop.load(Ordering::Relaxed) {
                    let _ = consumer.poll(Duration::from_millis(100));
                }
            })
        };
        Joiner {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Joiner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("joiner thread");
        }
    }
}

/// Serve `source` until a revocation arrives, and complete it with the
/// watermarks `drain` returns.
pub(crate) fn complete_next_revocation(
    source: &mut KafkaSource,
    mut drain: impl FnMut() -> Vec<(PartitionId, i64)>,
) {
    spate_test::wait_until(HANG, "the revocation", || {
        let SourceEvent::LanesRevoked { lanes, barrier } = source
            .poll_events(Duration::from_millis(200))
            .expect("poll_events")
        else {
            return false;
        };
        let watermarks = drain();
        complete_revocation(source, &lanes, &barrier, &watermarks).expect("flush");
        true
    });
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
    spate_test::wait_for_value(
        timeout,
        &format!("{want} payloads from the lane"),
        || {
            serve_events(source, &mut errors);
            if let Some(mut batch) = lane
                .poll(64, Duration::from_millis(500))
                .expect("lane poll")
            {
                while let Some(raw) = batch.next_payload() {
                    got.push((
                        raw.bytes.to_vec(),
                        raw.key.unwrap_or(&[]).to_vec(),
                        raw.offset,
                    ));
                }
            }
            (got.len(), errors.clone())
        },
        |(delivered, _)| *delivered >= want,
    );
    got
}

/// Serve `source` until `partitions` partitions publish
/// `spate_source_lag_records` in `statistics`, and return those series.
///
/// Waiting for the first series is not enough: lag is per partition, so a
/// snapshot can carry a number for one partition while another still reads
/// `-1`, which publishes no series.
pub(crate) fn await_lag_series(
    source: &mut KafkaSource,
    statistics: &metrics_exporter_prometheus::PrometheusHandle,
    partitions: usize,
) -> Vec<spate_test::MetricSample> {
    spate_test::wait_for_value(
        HANG,
        &format!("consumer lag on {partitions} partitions"),
        || {
            source
                .poll_events(Duration::from_millis(100))
                .expect("poll_events");
            spate_test::metric_series(&statistics.render(), names::SOURCE_LAG_RECORDS, &[])
        },
        |series| series.len() == partitions,
    )
}

/// A cluster holding one record, `one`, on partition 0 of a one-partition
/// `topic`, whose next Fetch is refused, and its bootstrap servers. The
/// refusal reaches a poll as an error, and the client fetches again after its
/// fetch backoff.
pub(crate) fn refused_fetch(topic: &str) -> (MockKafka, String) {
    let cluster = MockKafka::new();
    let brokers = cluster.bootstrap_servers();
    cluster.create_topic(topic, 1, 1).expect("create topic");
    produce_payload(&brokers, topic, 0, b"one");
    cluster.request_errors(
        rdkafka::types::RDKafkaApiKey::Fetch,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    );
    (cluster, brokers)
}
