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
use spate_core::checkpoint::Checkpointer;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::metrics::names;
use spate_core::record::PartitionId;
use spate_core::source::{
    DrainBarrier, LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane,
};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test::HANG;
use spate_test_support::container_image;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use testcontainers::{ContainerRequest, ImageExt};
use testcontainers_modules::kafka::apache::Kafka;

pub(crate) mod feed;
pub(crate) mod latest;
pub(crate) mod mock;
pub(crate) mod wire;

#[allow(unused_imports, reason = "each target uses a different subset")]
pub(crate) use mock::MockKafka;
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

/// [`broker`] with the transaction state topic on one replica. Its defaults
/// ask for three, which one broker cannot satisfy.
pub(crate) fn transactional_broker() -> ContainerRequest<Kafka> {
    broker()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
}

/// The config of a producer on `brokers` that gives up on a record after 30
/// seconds.
pub(crate) fn producer_config(brokers: &str) -> ClientConfig {
    let mut config = raw_config();
    config
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "30000");
    config
}

/// Enqueue `count` records to partition `p`, with payload `{tag}-p{p}-{i}` and
/// key `k{p}-{i}`.
fn send_records(producer: &BaseProducer, topic: &str, p: i32, count: usize, tag: &str) {
    for i in 0..count {
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

/// Produce `per_partition` records to each of `partitions` partitions of
/// `topic`, with payload `{tag}-p{p}-{i}` and key `k{p}-{i}`, and flush.
pub(crate) fn produce(
    brokers: &str,
    topic: &str,
    per_partition: usize,
    partitions: i32,
    tag: &str,
) {
    let producer: BaseProducer = producer_config(brokers).build().expect("producer");
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
    let producer: BaseProducer = producer_config(brokers)
        .set("transactional.id", format!("{topic}-{tag}"))
        // Kafka's 60 s default can abort an open transaction under a slow suite.
        .set("transaction.timeout.ms", "600000")
        .build()
        .expect("producer");
    producer
        .init_transactions(TRANSACTION_TIMEOUT)
        .expect("init");
    producer
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
    let probe: BaseConsumer = raw_config()
        .set("bootstrap.servers", brokers)
        .set("isolation.level", "read_uncommitted")
        .build()
        .expect("probe");
    probe
        .fetch_watermarks(topic, partition, Duration::from_secs(10))
        .expect("watermarks")
        .1
}

/// A consumer in `group` that reads or writes the group's committed offsets
/// and joins nothing.
pub(crate) fn probe(brokers: &str, group: &str) -> BaseConsumer {
    raw_config()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .build()
        .expect("probe")
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
    create_visible(
        brokers,
        &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
    );
}

/// Create `topics` and wait until the broker's metadata lists each one. A
/// broker can answer a topic it has just created as unknown for a while.
pub(crate) fn create_visible(brokers: &str, topics: &[NewTopic<'_>]) {
    let admin: AdminClient<DefaultClientContext> = raw_config()
        .set("bootstrap.servers", brokers)
        .build()
        .expect("admin client");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(admin.create_topics(topics, &AdminOptions::new()))
        .expect("create topics")
        .into_iter()
        .for_each(|r| {
            r.expect("topic created");
        });
    for topic in topics {
        spate_test::wait_until(HANG, &format!("{} in metadata", topic.name), || {
            feed::exists(brokers, topic.name)
        });
    }
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

/// [`committed`] through a new [`probe`] in `group`, read again while the broker
/// refuses: it answers `NotCoordinator` while the group's coordinator moves.
pub(crate) fn committed_by(
    brokers: &str,
    group: &str,
    topic: &str,
    partitions: i32,
) -> Vec<Offset> {
    let probe = probe(brokers, group);
    let mut tpl = TopicPartitionList::new();
    for p in 0..partitions {
        tpl.add_partition(topic, p);
    }
    let mut read = None;
    spate_test::wait_until(HANG, "the committed offsets read", || {
        read = probe
            .committed_offsets(tpl.clone(), Duration::from_secs(10))
            .ok();
        read.is_some()
    });
    read.expect("read before the wait ended")
        .elements()
        .iter()
        .map(|e| e.offset())
        .collect()
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
        let mut source = KafkaSource::new(cfg);
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
    let deadline = Instant::now() + HANG;
    let mut out = Vec::new();
    while out.len() < n {
        assert!(
            Instant::now() < deadline,
            "{topic}: consumed only {} of {n} within {:?}",
            out.len(),
            HANG
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
    spate_test::wait_until(Duration::from_secs(120), "the revocation", || {
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
