//! The Kafka feed's test harness: a worker pipeline that reads units through
//! a `feed: { kafka: … }` section, a feed driven outside a pipeline, and
//! readers of committed offsets and of the retry, dead-letter and attempt
//! topics.

use rdkafka::ClientConfig;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use spate_core::config::{FeedSection, PipelineConfig};
use spate_core::feed::{
    Confirmation, Delivery, Feed, FeedError, FeedErrorKind, FeedWaker, Receipt, Unit, wake_channel,
};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions, ShutdownHandle};
use spate_core::sink::{KeyHashRouter, SinkPoolConfig};
use spate_json::NdjsonFramer;
use spate_kafka::feed::{KafkaFeed, KafkaFeedError};
use spate_s3::{DescriptorObject, S3Source, SplitDescriptor};
use spate_test::{
    BytesPassthrough, PipelineRun, ScriptedResult, SinkScript, TestEncoder, capture_sink,
    decode_rows, wait_until,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;

pub(crate) const DEADLINE: Duration = Duration::from_secs(60);

pub(crate) const WORK: &str = "units";
pub(crate) const RETRY: &str = "units-retry";
pub(crate) const DEAD: &str = "units-dead";
pub(crate) const ATTEMPT: &str = "units-attempts";

/// The attempt topic's partition count on the MockCluster.
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

/// A [`Feed`] that passes every call to the Kafka feed and records each
/// delivery's count.
#[derive(Clone)]
pub(crate) struct Recording {
    pub(crate) inner: Arc<KafkaFeed>,
    pub(crate) seen: Arc<Mutex<Vec<u32>>>,
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

/// How a test connects the feed.
pub(crate) type Connect = fn(&FeedSection, &Handle) -> Result<KafkaFeed, KafkaFeedError>;

/// Connect as on rdkafka's MockCluster, which answers no request for a
/// topic's configuration: only the topics' existence is checked.
pub(crate) const ON_MOCK: Connect = KafkaFeed::connect_without_config_checks;

/// [`launch_with`] connecting as on the MockCluster.
pub(crate) fn launch(data: &Path, feed_section: &str, pre: impl FnOnce(&SinkScript)) -> Worker {
    launch_with(ON_MOCK, data, feed_section, pre)
}

/// Launch a worker with one record per source batch and one row per sink
/// write, so an object's records span several writes, connecting its feed
/// with `connect`. `pre` scripts the sink before data flows.
pub(crate) fn launch_with(
    connect: Connect,
    data: &Path,
    feed_section: &str,
    pre: impl FnOnce(&SinkScript),
) -> Worker {
    let yaml = format!(
        "pipeline: {{ name: kafka-feed, threads: 1 }}\nadmin: {{ listen: none }}\n\
         checkpoint: {{ interval: 100ms }}\nmetrics: {{ exporter: none }}\n\
         source:\n  s3:\n    url: \"file://{}/\"\n\
         sink: {{ capture: {{ chunk: {{ target_bytes: 64 }} }} }}\n{feed_section}",
        data.display()
    );
    let mut config = PipelineConfig::from_str(&yaml).unwrap();
    config.pipeline.name = spate_test::unique_name(&config.pipeline.name);
    let pipeline = Pipeline::from_config(config).unwrap();
    let section = pipeline.config().feed.clone().unwrap();
    let io = pipeline.io_handle();
    let feed = Arc::new(connect(&section, &io).unwrap());
    let recording = Recording {
        inner: Arc::clone(&feed),
        seen: Arc::default(),
    };
    let source = S3Source::from_component_config(&pipeline.config().source, io)
        .unwrap()
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)));
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
