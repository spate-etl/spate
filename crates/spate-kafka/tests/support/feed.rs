//! The Kafka feed's test harness: a worker pipeline that reads units through
//! a `feed: { kafka: … }` section, a feed driven outside a pipeline, and
//! readers of committed offsets and of the retry, dead-letter and attempt
//! topics.

use object_store::ObjectStoreExt as _;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use spate_core::clock::TestClock as StopClock;
use spate_core::config::{FeedSection, PipelineConfig};
use spate_core::feed::{
    Confirmation, Delivery, DepthOf, DepthReading, Feed, FeedError, FeedErrorKind, FeedWaker,
    Receipt, Unit, wake_channel,
};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions, ShutdownHandle};
use spate_core::sink::{KeyHashRouter, SinkPoolConfig};
use spate_json::NdjsonFramer;
use spate_kafka::feed::{KafkaFeed, KafkaFeedError, KafkaFeedPublisher};
use spate_s3::{DescriptorObject, S3Source, SplitDescriptor};
use spate_test::{
    BytesPassthrough, PipelineRun, ScriptedResult, SinkScript, TestEncoder, capture_sink,
    decode_rows, wait_until,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;

pub(crate) const DEADLINE: Duration = Duration::from_secs(60);

pub(crate) const WORK: &str = "units";
pub(crate) const RETRY: &str = "units-retry";
pub(crate) const DEAD: &str = "units-dead";
pub(crate) const ATTEMPT: &str = "units-attempts";

/// The attempt topic's partition count on the MockCluster and the broker.
pub(crate) const ATTEMPT_PARTITIONS: i32 = 2;

/// The feed's commit interval, read from the outside: several of these pass
/// in a window that pins an offset as not committed.
pub(crate) const COMMIT_PERIOD: Duration = Duration::from_secs(1);

/// A `feed:` section for `group` on `brokers`, with `body` appended to the
/// `kafka:` body and `section` to the section.
pub(crate) fn feed_yaml(brokers: &str, group: &str, section: &str, body: &str) -> String {
    format!(
        "feed:\n{section}  kafka:\n    brokers: \"{brokers}\"\n    group_id: {group}\n    \
         topic: {WORK}\n    retry_topic: {RETRY}\n    dead_letter_topic: {DEAD}\n    \
         attempt_topic: {ATTEMPT}\n    statistics_interval: 0s\n    rdkafka:\n      session.timeout.ms: \"6000\"\n      \
         heartbeat.interval.ms: \"2000\"\n{body}"
    )
}

/// A [`Feed`] that passes every call to the Kafka feed, records each
/// delivery's count and receipt, counts the reads of the feed's depth, and
/// records the stop's calls.
#[derive(Clone)]
pub(crate) struct Recording {
    pub(crate) inner: Arc<KafkaFeed>,
    pub(crate) seen: Arc<Mutex<Vec<u32>>>,
    receipts: Arc<Mutex<Vec<Receipt>>>,
    pub(crate) depth_reads: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    handed_back: Arc<Mutex<Vec<Receipt>>>,
    closes: Arc<Mutex<Vec<Duration>>>,
}

impl Recording {
    fn new(inner: Arc<KafkaFeed>) -> Recording {
        Recording {
            inner,
            seen: Arc::default(),
            receipts: Arc::default(),
            depth_reads: Arc::default(),
            stops: Arc::default(),
            handed_back: Arc::default(),
            closes: Arc::default(),
        }
    }

    /// Reads of the feed's depth, the startup probe included.
    pub(crate) fn depth_reads(&self) -> usize {
        self.depth_reads.load(Ordering::SeqCst)
    }

    /// Each delivery's receipt, in the order received.
    pub(crate) fn receipts(&self) -> Vec<Receipt> {
        self.receipts.lock().unwrap().clone()
    }

    pub(crate) fn stopped_receiving(&self) -> usize {
        self.stops.load(Ordering::SeqCst)
    }

    pub(crate) fn handed_back(&self) -> Vec<Receipt> {
        self.handed_back.lock().unwrap().clone()
    }

    /// What each close was given.
    pub(crate) fn closes(&self) -> Vec<Duration> {
        self.closes.lock().unwrap().clone()
    }
}

impl Feed for Recording {
    fn backend(&self) -> &str {
        self.inner.backend()
    }

    fn set_waker(&self, waker: FeedWaker) {
        self.inner.set_waker(waker);
    }

    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        let got = self.inner.receive(max)?;
        self.seen
            .lock()
            .unwrap()
            .extend(got.iter().map(|d| d.deliveries));
        self.receipts
            .lock()
            .unwrap()
            .extend(got.iter().map(|d| d.receipt));
        Ok(got)
    }

    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.renew(receipt)
    }

    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.acknowledge(receipt)
    }

    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.inner.fail(receipt, reason)
    }

    fn publish(&self, unit: Unit) -> Confirmation {
        self.inner.publish(unit)
    }

    fn read_depth(&self, of: DepthOf) -> Option<DepthReading> {
        if of == DepthOf::Feed {
            self.depth_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.read_depth(of)
    }

    fn hands_back(&self) -> bool {
        self.inner.hands_back()
    }

    fn stop_receiving(&self) {
        self.stops.fetch_add(1, Ordering::SeqCst);
        self.inner.stop_receiving();
    }

    fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.handed_back.lock().unwrap().push(receipt);
        self.inner.hand_back(receipt)
    }

    fn close(&self, within: Duration) -> Option<Confirmation> {
        self.closes.lock().unwrap().push(within);
        self.inner.close(within)
    }
}

/// A pipeline reading `data` through the Kafka feed its `feed:` section
/// names.
pub(crate) struct Worker {
    pub(crate) feed: Arc<KafkaFeed>,
    pub(crate) recording: Recording,
    pub(crate) run: PipelineRun,
    pub(crate) script: SinkScript,
    pub(crate) shutdown: ShutdownHandle,
}

/// Create the work, retry and dead-letter topics, one partition each, the
/// work topic stamped on append, and the compacted attempt topic. The feed
/// never creates a topic.
pub(crate) fn create_topics(brokers: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let mut topics = vec![
        NewTopic::new(WORK, 1, TopicReplication::Fixed(1))
            .set("message.timestamp.type", "LogAppendTime"),
        NewTopic::new(RETRY, 1, TopicReplication::Fixed(1)),
        NewTopic::new(DEAD, 1, TopicReplication::Fixed(1)),
    ];
    topics.push(
        NewTopic::new(ATTEMPT, ATTEMPT_PARTITIONS, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact"),
    );
    let io = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for created in io
        .block_on(admin.create_topics(&topics, &AdminOptions::new()))
        .expect("create topics")
    {
        created.expect("topic created");
    }
}

/// How a test connects the feed.
pub(crate) type Connect = fn(&FeedSection, &Handle) -> Result<KafkaFeed, KafkaFeedError>;

/// Connect as on rdkafka's MockCluster, which answers no request for a
/// topic's configuration: only the topics' existence is checked.
pub(crate) const ON_MOCK: Connect = KafkaFeed::connect_without_config_checks;

/// [`launch_with`] connecting as on the MockCluster.
pub(crate) fn launch(data: &Path, feed_section: &str, pre: impl FnOnce(&SinkScript)) -> Worker {
    launch_with(ON_MOCK, data, feed_section, pre)
}

/// [`launch_source`] over the files under `data`.
pub(crate) fn launch_with(
    connect: Connect,
    data: &Path,
    feed_section: &str,
    pre: impl FnOnce(&SinkScript),
) -> Worker {
    let source = format!("  s3:\n    url: \"file://{}/\"\n", data.display());
    launch_source(connect, &source, feed_section, pre)
}

/// Launch a worker with one record per source batch and one row per sink
/// write, so an object's records span several writes, reading the source
/// `source_yaml` (the `source:` section's body) and connecting its feed
/// with `connect`. `pre` scripts the sink before data flows.
pub(crate) fn launch_source(
    connect: Connect,
    source_yaml: &str,
    feed_section: &str,
    pre: impl FnOnce(&SinkScript),
) -> Worker {
    launch_customized(
        connect,
        source_yaml,
        feed_section,
        pre,
        |source| source,
        CHECKPOINT,
        None,
    )
}

/// The `checkpoint:` section a worker reads unless a test gives its own.
pub(crate) const CHECKPOINT: &str = "{ interval: 100ms }";

/// The drain timeout a worker's stop runs under: the default, which
/// [`CHECKPOINT`] leaves alone.
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// The pipeline file a worker over `source_yaml` reads.
pub(crate) fn pipeline_yaml(source_yaml: &str, feed_section: &str) -> String {
    pipeline_yaml_with(source_yaml, feed_section, CHECKPOINT)
}

/// [`pipeline_yaml`] with `checkpoint` as its `checkpoint:` section.
pub(crate) fn pipeline_yaml_with(
    source_yaml: &str,
    feed_section: &str,
    checkpoint: &str,
) -> String {
    format!(
        "pipeline: {{ name: kafka-feed, threads: 1 }}\nadmin: {{ listen: none }}\n\
         checkpoint: {checkpoint}\nmetrics: {{ exporter: none }}\n\
         source:\n{source_yaml}\
         sink: {{ capture: {{ chunk: {{ target_bytes: 64 }} }} }}\n{feed_section}"
    )
}

/// [`launch_source`], with `customize` applied to the source, the
/// `checkpoint:` section `checkpoint`, and the stop timed on `stop_clock`
/// when one is given.
pub(crate) fn launch_customized(
    connect: Connect,
    source_yaml: &str,
    feed_section: &str,
    pre: impl FnOnce(&SinkScript),
    customize: impl FnOnce(S3Source) -> S3Source,
    checkpoint: &str,
    stop_clock: Option<Arc<StopClock>>,
) -> Worker {
    let yaml = pipeline_yaml_with(source_yaml, feed_section, checkpoint);
    let mut config = PipelineConfig::from_str(&yaml).unwrap();
    config.pipeline.name = spate_test::unique_name(&config.pipeline.name);
    let pipeline = Pipeline::from_config(config).unwrap();
    let section = pipeline.config().feed.clone().unwrap();
    let io = pipeline.io_handle();
    let feed = Arc::new(connect(&section, &io).unwrap());
    let recording = Recording::new(Arc::clone(&feed));
    let source = customize(
        S3Source::from_component_config(&pipeline.config().source, io)
            .unwrap()
            .with_framer(|| Box::new(NdjsonFramer::new(1 << 20))),
    );
    let mut pool = SinkPoolConfig::default();
    pool.batch.max_rows = 1;
    pool.batch.linger = Duration::from_millis(10);
    let (sink, script) = capture_sink(1, 1);
    pre(&script);
    let runtime = pipeline
        .feed(Arc::new(recording.clone()))
        .sink(sink.with_pool_config(pool))
        .unwrap()
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            max_records: 1,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .unwrap();
    let runtime = match stop_clock {
        Some(clock) => runtime.with_stop_clock(clock),
        None => runtime,
    };
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());
    Worker {
        feed,
        recording,
        run,
        script,
        shutdown,
    }
}

impl Worker {
    /// Stop the run and return how it exited.
    pub(crate) fn stop(&self) -> ExitState {
        self.shutdown.trigger();
        self.run
            .wait_exit(DEADLINE)
            .expect("the run exits")
            .unwrap()
            .state
    }
}

pub(crate) fn data_dir(dir: &tempfile::TempDir) -> PathBuf {
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    data
}

/// Stage `rows` as the object `name` and return the unit naming it.
pub(crate) fn stage(data: &Path, name: &str, rows: &[String]) -> Unit {
    let mut bytes = Vec::new();
    for row in rows {
        bytes.extend_from_slice(row.as_bytes());
        bytes.push(b'\n');
    }
    std::fs::write(data.join(name), &bytes).unwrap();
    let object = DescriptorObject {
        key: data
            .join(name)
            .to_string_lossy()
            .trim_start_matches('/')
            .to_owned(),
        size: bytes.len() as u64,
        etag: None,
        last_modified_ms: 1,
    };
    Unit::new(name, SplitDescriptor::new(vec![object]).encode().unwrap())
}

/// `n` records, each longer than the sink's 64-byte chunk target so each
/// takes a write of its own.
pub(crate) fn recs(prefix: &str, n: usize) -> Vec<String> {
    let pad = "p".repeat(64);
    (0..n)
        .map(|i| format!("{{\"k\":\"{prefix}-{i}\",\"pad\":\"{pad}\"}}"))
        .collect()
}

pub(crate) fn publish(feed: &KafkaFeed, unit: Unit) {
    publish_to(feed, unit);
}

/// Publish `unit` through any feed and wait for its confirmation.
pub(crate) fn publish_to(feed: &dyn Feed, unit: Unit) {
    let mut confirmation = feed.publish(unit);
    wait_until(DEADLINE, "the publish settles", || {
        confirmation.try_outcome().is_some()
    });
    confirmation.try_outcome().unwrap().unwrap();
}

/// Rows the sink wrote successfully, sorted.
pub(crate) fn written(script: &SinkScript) -> Vec<String> {
    let mut rows: Vec<String> = script
        .writes()
        .iter()
        .filter(|w| w.result == ScriptedResult::Ok)
        .flat_map(|w| decode_rows(&w.payload))
        .map(|r| String::from_utf8(r).unwrap())
        .collect();
    rows.sort();
    rows
}

pub(crate) fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

/// The offset `group` has committed for partition 0 of `topic`, if any.
pub(crate) fn committed(brokers: &str, group: &str, topic: &str) -> Option<i64> {
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .create()
        .expect("probe consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    let committed = probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed offsets");
    match committed.elements()[0].offset() {
        Offset::Offset(offset) => Some(offset),
        _ => None,
    }
}

/// One message as read back from a topic: key, value and headers.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Read {
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) headers: Vec<(String, Option<Vec<u8>>)>,
}

impl Read {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .and_then(|(_, v)| std::str::from_utf8(v.as_deref()?).ok())
    }
}

/// The first `n` messages of partition 0 of `topic`, read from its start.
pub(crate) fn read_topic(brokers: &str, topic: &str, n: usize) -> Vec<Read> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("read-{topic}"))
        .create()
        .expect("reader");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();
    let mut got = Vec::new();
    wait_until(DEADLINE, &format!("{n} messages on {topic}"), || {
        while let Some(message) = consumer.poll(Duration::from_millis(100)) {
            let message = message.expect("read");
            got.push(Read {
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
            });
        }
        got.len() >= n
    });
    got
}

/// One attempt record as read back: its partition, its key, and its count,
/// `None` for a tombstone.
pub(crate) type AttemptRecord = (i32, String, Option<u32>);

/// The first `n` records of the attempt topic's `partitions` partitions,
/// read from their start, in offset order within each partition.
pub(crate) fn read_attempts(brokers: &str, partitions: i32, n: usize) -> Vec<AttemptRecord> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", spate_test::unique_name("read-attempts"))
        .create()
        .expect("reader");
    let mut tpl = TopicPartitionList::new();
    for partition in 0..partitions {
        tpl.add_partition_offset(ATTEMPT, partition, Offset::Beginning)
            .unwrap();
    }
    consumer.assign(&tpl).unwrap();
    let mut got = Vec::new();
    wait_until(DEADLINE, &format!("{n} attempt records"), || {
        while let Some(message) = consumer.poll(Duration::from_millis(100)) {
            let message = message.expect("read");
            let key = String::from_utf8(message.key().expect("keyed").to_vec()).unwrap();
            let count = message.payload().map(|value| {
                let record: serde_json::Value = serde_json::from_slice(value).unwrap();
                assert_eq!(record["v"], 1, "{record}");
                u32::try_from(record["attempts"].as_u64().unwrap()).unwrap()
            });
            got.push((message.partition(), key, count));
        }
        got.len() >= n
    });
    got
}

/// How many messages partition 0 of `topic` holds.
pub(crate) fn high_watermark(brokers: &str, topic: &str) -> i64 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("watermark reader");
    consumer
        .fetch_watermarks(topic, 0, Duration::from_secs(10))
        .expect("watermarks")
        .1
}

/// How many messages the first `partitions` partitions of `topic` hold
/// between them.
pub(crate) fn high_watermarks(brokers: &str, topic: &str, partitions: i32) -> i64 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("watermark reader");
    (0..partitions)
        .map(|partition| {
            consumer
                .fetch_watermarks(topic, partition, Duration::from_secs(10))
                .expect("watermarks")
                .1
        })
        .sum()
}

/// A publish-only handle connected as on the MockCluster, with the runtime
/// it connected on.
pub(crate) fn publisher_on_mock(
    yaml: &str,
) -> (
    Result<KafkaFeedPublisher, KafkaFeedError>,
    tokio::runtime::Runtime,
) {
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let publisher = KafkaFeedPublisher::connect_without_config_checks(&section(yaml), io.handle());
    (publisher, io)
}

/// Produce `messages` to partition 0 of `topic` as given, key included.
pub(crate) fn produce_raw(brokers: &str, topic: &str, messages: &[(Option<&[u8]>, &[u8])]) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("producer");
    for (key, payload) in messages {
        let mut record = BaseRecord::<[u8], [u8]>::to(topic)
            .partition(0)
            .payload(*payload);
        if let Some(key) = key {
            record = record.key(*key);
        }
        producer.send(record).map_err(|(e, _)| e).expect("enqueue");
    }
    producer.flush(Duration::from_secs(30)).expect("flush");
}

/// The `feed:` key of a pipeline file, alone.
#[derive(serde::Deserialize)]
pub(crate) struct PipelineConfigFeed {
    pub(crate) feed: FeedSection,
}

/// A feed connected outside a pipeline, with the runtime `connect` takes.
pub(crate) struct Direct {
    pub(crate) feed: KafkaFeed,
    pub(crate) park: spate_core::feed::FeedPark,
    pub(crate) _io: tokio::runtime::Runtime,
}

/// The `feed:` section of `yaml`, a pipeline file's `feed:` key alone.
pub(crate) fn section(yaml: &str) -> FeedSection {
    serde_yaml::from_str::<PipelineConfigFeed>(yaml)
        .unwrap()
        .feed
}

pub(crate) fn direct(yaml: &str) -> Direct {
    let section = section(yaml);
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let feed = ON_MOCK(&section, io.handle()).unwrap();
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    Direct {
        feed,
        park,
        _io: io,
    }
}

impl Direct {
    /// The next delivery, waiting for it under the deadline. A transient
    /// error is passed over, as a reader does.
    pub(crate) fn take(&self, what: &str) -> Delivery {
        let mut got = None;
        wait_until(DEADLINE, what, || {
            got = match self.feed.receive(1) {
                Ok(mut got) => got.pop(),
                Err(e) if e.kind == FeedErrorKind::Retryable => None,
                Err(e) => panic!("{e}"),
            };
            if got.is_none() {
                self.park.park(Duration::from_millis(100));
            }
            got.is_some()
        });
        got.unwrap()
    }
}

/// Whether `topic` exists on the cluster at `brokers`, read with
/// auto-creation off so the read cannot create it.
pub(crate) fn exists(brokers: &str, topic: &str) -> bool {
    let probe: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("allow.auto.create.topics", "false")
        .create()
        .expect("admin client");
    let metadata = probe
        .inner()
        .fetch_metadata(None, Duration::from_secs(10))
        .expect("metadata");
    metadata
        .topics()
        .iter()
        .any(|t| t.name() == topic && t.error().is_none() && !t.partitions().is_empty())
}

/// The step a bounded job's end check reads the feed at.
pub(crate) const STEP: Duration = Duration::from_secs(15);

/// The `feed:` section of a bounded job for `group` on `brokers`, at a
/// `file://` location under `dir`.
pub(crate) fn bounded_section(brokers: &str, group: &str, dir: &tempfile::TempDir) -> String {
    let location = format!(
        "  location: \"file://{}/\"\n",
        dir.path().join("jobs").display()
    );
    feed_yaml(brokers, group, &location, "")
}

/// Run the job's lister once through `publisher`, a publish-only handle and
/// the runtime it runs on: publish a unit per object under `data` and write
/// the fingerprint and end marker.
pub(crate) fn list_once(
    publisher: (
        Result<KafkaFeedPublisher, KafkaFeedError>,
        tokio::runtime::Runtime,
    ),
    data: &Path,
    section_yaml: &str,
) {
    let (publisher, io) = publisher;
    let source = format!("  s3:\n    url: \"file://{}/\"\n", data.display());
    let config = PipelineConfig::from_str(&pipeline_yaml(&source, section_yaml)).unwrap();
    S3Source::from_component_config(&config.source, io.handle().clone())
        .unwrap()
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)))
        .list_once(config.feed.as_ref().unwrap(), Arc::new(publisher.unwrap()))
        .unwrap();
}

/// A bounded worker over `data`, connected with `connect`, whose end check
/// runs on `clock`.
pub(crate) fn launch_bounded(
    connect: Connect,
    data: &Path,
    section_yaml: &str,
    clock: &Arc<spate_core::clock::tokio::TestClock>,
) -> Worker {
    let source = format!("  s3:\n    url: \"file://{}/\"\n", data.display());
    let clock = Arc::clone(clock);
    launch_customized(
        connect,
        &source,
        section_yaml,
        |_| {},
        move |s| s.with_clock(clock),
        CHECKPOINT,
        None,
    )
}

/// [`launch_source`] over the files under `data` with `source_extra`
/// appended to the `s3:` section, connected with `connect`, and the stop
/// timed on `clock`.
pub(crate) fn launch_stopping(
    connect: Connect,
    data: &Path,
    source_extra: &str,
    feed_section: &str,
    clock: &Arc<StopClock>,
    pre: impl FnOnce(&SinkScript),
) -> Worker {
    let source = format!(
        "  s3:\n    url: \"file://{}/\"\n{source_extra}",
        data.display()
    );
    launch_customized(
        connect,
        &source,
        feed_section,
        pre,
        |source| source,
        CHECKPOINT,
        Some(Arc::clone(clock)),
    )
}

/// The source setting under which [`two_range_unit`] is read in two ranges.
pub(crate) const TWO_RANGES: &str = "    split_target_bytes: 1MiB\n";

/// Stage a unit read in two byte ranges under [`TWO_RANGES`], named
/// `prefix`, and return it with its rows: 68 lines of 16 KiB, 1 MiB + 64 KiB.
pub(crate) fn two_range_unit(data: &Path, prefix: &str) -> (Unit, Vec<String>) {
    let pad = "p".repeat(16 * 1024 - 24);
    let rows: Vec<String> = (0..68)
        .map(|i| format!("{{\"k\":\"{prefix}-{i:04}\",\"pad\":\"{pad}\"}}"))
        .collect();
    let unit = stage_with_etag(data, &format!("{prefix}.ndjson"), &rows);
    (unit, rows)
}

/// [`stage`], with the ETag the file store reports in the descriptor, so a
/// unit over an object above `split_target_bytes` is read in ranges.
fn stage_with_etag(data: &Path, name: &str, rows: &[String]) -> Unit {
    let unit = stage(data, name, rows);
    let mut descriptor = SplitDescriptor::decode(&unit.body).unwrap();
    let object = &mut descriptor.objects[0];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let path = object_store::path::Path::parse(&object.key).unwrap();
    let meta = rt
        .block_on(object_store::local::LocalFileSystem::new().head(&path))
        .unwrap();
    object.etag = Some(meta.e_tag.expect("the file store reports an ETag"));
    Unit::new(name, descriptor.encode().unwrap())
}

/// Step `clock` until the worker exits, waiting after each step for its
/// next depth read. The run hands its report out once, so this keeps it.
///
/// # Panics
///
/// When the worker has not exited after 80 steps.
pub(crate) fn run_to_exit(
    worker: &Worker,
    clock: &spate_core::clock::tokio::TestClock,
) -> spate_core::pipeline::ExitReport {
    for _ in 0..80 {
        let reads = worker.recording.depth_reads();
        clock.advance(STEP);
        let mut exit = None;
        wait_until(DEADLINE, "the next depth read or the end", || {
            if exit.is_none() {
                exit = worker.run.wait_exit(Duration::ZERO);
            }
            exit.is_some() || worker.recording.depth_reads() > reads
        });
        if let Some(report) = exit {
            return report.unwrap();
        }
    }
    panic!("the job never ended");
}
