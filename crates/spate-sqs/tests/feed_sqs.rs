//! The SQS feed against ElasticMQ: the S3 source reads units from a real
//! queue through a `feed: { sqs: … }` section, and the queue's own counts
//! show when a unit is deleted, renewed or handed back.
//!
//! Every test needs Docker and is `#[ignore]`d. Run them with:
//!
//! ```sh
//! cargo nextest run --profile docker -p spate-sqs --all-features --locked \
//!   --run-ignored ignored-only
//! ```
//!
//! At a 6 s visibility the long-poll wait is 2 s and the renewal period 2 s,
//! so a unit that arrives at the end of a poll still has 2 s above the
//! threshold under which the feed drops it unseen.

use spate_core::config::{FeedSection, PipelineConfig};
use spate_core::feed::{
    Confirmation, Delivery, Feed, FeedError, FeedWaker, Receipt, Unit, wake_channel,
};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions, ShutdownHandle};
use spate_core::sink::{KeyHashRouter, SinkPoolConfig};
use spate_json::NdjsonFramer;
use spate_s3::{DescriptorObject, S3Source, SplitDescriptor};
use spate_sqs::{SqsError, SqsFeed};
use spate_test::{
    BytesPassthrough, PipelineRun, ScriptedResult, SinkScript, TestEncoder, WriteOutcome,
    capture_sink, decode_rows, wait_until,
};
use spate_test_support::ElasticMq;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEADLINE: Duration = Duration::from_secs(60);

/// The visibility period of the pipeline tests.
const VISIBILITY: Duration = Duration::from_secs(6);

/// Creates `name` with a dead-letter queue of its own, five receives before
/// a unit moves there, and returns the queue's URL.
fn queue_with_dead_letter(mq: &ElasticMq, name: &str) -> String {
    let dead = mq.create_queue(&format!("{name}-dlq")).unwrap();
    let arn = mq.queue_arn(&dead).unwrap();
    let policy = format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":"5"}}"#);
    let queue = mq
        .create_queue_with_attributes(name, &[("RedrivePolicy", &policy)])
        .unwrap();
    format!("{}{queue}", mq.endpoint())
}

/// The path part of a queue URL, as the ElasticMQ helpers take it.
fn path_of(url: &str) -> &str {
    let rest = url.split_once("://").unwrap().1;
    &rest[rest.find('/').unwrap()..]
}

fn feed_yaml(mq: &ElasticMq, queue_url: &str, visibility: Duration, extra: &str) -> String {
    format!(
        "feed:\n  sqs:\n    queue_url: \"{queue_url}\"\n    visibility: {}s\n    \
         region: eu-west-2\n    endpoint: \"{}\"\n{extra}",
        visibility.as_secs(),
        mq.endpoint()
    )
}

/// A [`Feed`] that passes every call to the SQS feed and records each
/// delivery's count and when the reader took it.
#[derive(Clone)]
struct Recording {
    inner: Arc<SqsFeed>,
    seen: Arc<Mutex<Vec<(u32, Instant)>>>,
}

impl Recording {
    fn new(inner: Arc<SqsFeed>) -> Recording {
        Recording {
            inner,
            seen: Arc::default(),
        }
    }

    fn deliveries(&self) -> Vec<u32> {
        self.seen.lock().unwrap().iter().map(|(n, _)| *n).collect()
    }

    fn first_seen(&self) -> Option<(u32, Instant)> {
        self.seen.lock().unwrap().first().copied()
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
        let now = Instant::now();
        self.seen
            .lock()
            .unwrap()
            .extend(got.iter().map(|d| (d.deliveries, now)));
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

/// A pipeline reading `data` through the SQS feed its `feed:` section names.
struct Worker {
    feed: Arc<SqsFeed>,
    recording: Recording,
    run: PipelineRun,
    script: SinkScript,
    shutdown: ShutdownHandle,
}

/// Launch a worker with one record per source batch and one row per sink
/// write, so an object's records span several writes. `pre` scripts the sink
/// before data flows.
fn launch(data: &Path, feed_section: &str, pre: impl FnOnce(&SinkScript)) -> Worker {
    let yaml = format!(
        "pipeline: {{ name: sqs-feed, threads: 1 }}\nadmin: {{ listen: none }}\n\
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
    let feed =
        Arc::new(SqsFeed::connect_with_static_credentials(&section, &io, "test", "test").unwrap());
    let recording = Recording::new(Arc::clone(&feed));
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
    fn stop(&self) -> ExitState {
        self.shutdown.trigger();
        self.run
            .wait_exit(DEADLINE)
            .expect("the run exits")
            .unwrap()
            .state
    }
}

fn data_dir(dir: &tempfile::TempDir) -> PathBuf {
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    data
}

/// Stage `rows` as the object `name` and return the unit naming it.
fn stage(data: &Path, name: &str, rows: &[String]) -> Unit {
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
fn recs(prefix: &str, n: usize) -> Vec<String> {
    let pad = "p".repeat(64);
    (0..n)
        .map(|i| format!("{{\"k\":\"{prefix}-{i}\",\"pad\":\"{pad}\"}}"))
        .collect()
}

fn publish(feed: &SqsFeed, unit: Unit) {
    let mut confirmation = feed.publish(unit);
    wait_until(DEADLINE, "the publish settles", || {
        confirmation.try_outcome().is_some()
    });
    confirmation.try_outcome().unwrap().unwrap();
}

/// Rows the sink wrote successfully, sorted.
fn written(script: &SinkScript) -> Vec<String> {
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

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

/// Pins INV-1 on SQS: while the unit's last write is held past two
/// visibility periods, the queue never reports the unit gone; once the write
/// succeeds the unit is deleted with every row written.
#[test]
#[ignore = "requires Docker"]
fn a_unit_is_deleted_only_after_its_last_record_is_acknowledged() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "deleted");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("d", 3);
    let unit = stage(&data, "d.ndjson", &rows);

    let worker = launch(&data, &feed_yaml(&mq, &url, VISIBILITY, ""), |sink| {
        sink.enqueue_global(WriteOutcome::ok());
        sink.enqueue_global(WriteOutcome::ok());
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    publish(&worker.feed, unit);
    wait_until(DEADLINE, "the last write is held", || {
        worker.script.held_writes() == 1
    });

    // Elapsed time is the property: the hold outlasts two visibility periods.
    for _ in 0..24 {
        std::thread::sleep(Duration::from_millis(500));
        let counts = mq.approximate_counts(path_of(&url)).unwrap();
        assert_ne!(
            counts,
            (0, 0),
            "the unit was deleted while its last write was held"
        );
    }
    worker.script.release_held();
    wait_until(DEADLINE, "the unit is deleted", || {
        mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
    });
    assert_eq!(written(&worker.script), sorted(rows));
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins that the units of a worker that stops without handing them back
/// return only when their visibility lapses, and within the visibility period
/// plus a margin after the worker stops. The second worker reads the unit as
/// its second delivery.
#[test]
#[ignore = "requires Docker"]
fn a_unit_whose_worker_is_killed_mid_read_is_delivered_again() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "killed");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("k", 3);
    let unit = stage(&data, "k.ndjson", &rows);

    // One unit held, so A's last receive leaves no long poll in flight.
    let a = launch(
        &data,
        &feed_yaml(&mq, &url, VISIBILITY, "  units_held: 1\n"),
        |sink| sink.enqueue_global(WriteOutcome::ok().held()),
    );
    // A's receive comes after the publish, so SQS hides the unit until at
    // least one visibility period after this instant.
    let published = Instant::now();
    publish(&a.feed, unit);
    wait_until(DEADLINE, "A holds the unit's write", || {
        a.script.held_writes() == 1
    });
    let b = launch(&data, &feed_yaml(&mq, &url, VISIBILITY, ""), |_| {});
    a.feed.abandon();
    let abandoned = Instant::now();

    wait_until(
        VISIBILITY + Duration::from_secs(15),
        "B receives the unit",
        || b.recording.first_seen().is_some(),
    );
    let (deliveries, seen) = b.recording.first_seen().unwrap();
    assert_eq!(deliveries, 2, "B reads the unit's second delivery");
    let after = seen.duration_since(published);
    assert!(
        after >= VISIBILITY,
        "the unit returned {after:?} after it was published, {:?} after A stopped, \
         before its visibility lapsed",
        seen.duration_since(abandoned)
    );
    wait_until(
        DEADLINE,
        "B writes every row and the unit is deleted",
        || {
            written(&b.script) == sorted(rows.clone())
                && mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
        },
    );

    a.script.release_held();
    let _ = a.stop();
    assert_eq!(b.stop(), ExitState::Completed);
}

/// Pins that the feed renews a held unit while the sink stalls for more than
/// three visibility periods: SQS never delivers it again, and each row is
/// written once.
#[test]
#[ignore = "requires Docker"]
fn held_units_are_renewed_while_the_sink_stalls() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "renewed");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("r", 3);
    let unit = stage(&data, "r.ndjson", &rows);

    let worker = launch(&data, &feed_yaml(&mq, &url, VISIBILITY, ""), |sink| {
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    publish(&worker.feed, unit);
    wait_until(DEADLINE, "the first write is held", || {
        worker.script.held_writes() == 1
    });
    // Elapsed time is the property: the stall outlasts three visibility
    // periods.
    std::thread::sleep(Duration::from_secs(20));
    assert_eq!(
        worker.recording.deliveries(),
        [1],
        "the unit was delivered again during the stall"
    );
    worker.script.release_held();
    wait_until(DEADLINE, "the unit is deleted", || {
        mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
    });
    let got = written(&worker.script);
    assert_eq!(got, sorted(rows), "each row written once");
    assert_eq!(worker.recording.deliveries(), [1]);
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// Pins that a failed unit is made visible at once: at a 300 s visibility,
/// it is received again within 10 s, as its second delivery.
#[test]
#[ignore = "requires Docker"]
fn a_failed_unit_is_set_visible_at_once() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "failed");
    let yaml = feed_yaml(&mq, &url, Duration::from_secs(300), "");
    let section: FeedSection = serde_yaml::from_str::<PipelineConfigFeed>(&yaml)
        .unwrap()
        .feed;
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let feed =
        SqsFeed::connect_with_static_credentials(&section, io.handle(), "test", "test").unwrap();
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    publish(&feed, Unit::new("f", "a unit to fail"));

    let take = |what: &str, within: Duration| {
        let until = Instant::now() + within;
        loop {
            let got = feed.receive(1).unwrap();
            if let Some(delivery) = got.into_iter().next() {
                return delivery;
            }
            let left = until.saturating_duration_since(Instant::now());
            assert!(
                !left.is_zero(),
                "timed out after {within:?} waiting for {what}"
            );
            park.park(left);
        }
    };
    let first = take("the first delivery", DEADLINE);
    assert_eq!(first.deliveries, 1);
    feed.fail(first.receipt, "test").unwrap();
    let again = take("the failed unit again", Duration::from_secs(10));
    assert_eq!(again.unit.id, "f");
    assert_eq!(again.deliveries, 2);
    feed.acknowledge(again.receipt).unwrap();
    drop(feed);
    io.shutdown_timeout(Duration::from_secs(2));
}

/// Connect a feed to `queue_url` at the default visibility, on a runtime of
/// its own, and stop it again.
fn connect(mq: &ElasticMq, queue_url: &str) -> Result<(), SqsError> {
    let yaml = feed_yaml(mq, queue_url, Duration::from_secs(60), "");
    let section: FeedSection = serde_yaml::from_str::<PipelineConfigFeed>(&yaml)
        .unwrap()
        .feed;
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let connected =
        SqsFeed::connect_with_static_credentials(&section, io.handle(), "test", "test").map(drop);
    io.shutdown_timeout(Duration::from_secs(2));
    connected
}

/// Pins that `connect` refuses a queue with no redrive policy, since a unit
/// that keeps failing would then be received forever.
#[test]
#[ignore = "requires Docker"]
fn startup_refuses_a_queue_with_no_redrive_policy() {
    let mq = ElasticMq::start().unwrap();
    let queue = mq.create_queue("no-redrive").unwrap();
    let url = format!("{}{queue}", mq.endpoint());
    match connect(&mq, &url) {
        Err(SqsError::Refused(message)) => {
            assert!(message.contains("redrive"), "{message}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Pins that `connect` refuses a queue whose redrive policy names a
/// dead-letter queue it cannot read. ElasticMQ answers the read of a deleted
/// queue with the bare `QueueDoesNotExist` code, which the SDK leaves
/// untyped.
#[test]
#[ignore = "requires Docker"]
fn startup_refuses_an_unreadable_dead_letter_queue() {
    let mq = ElasticMq::start().unwrap();
    let dead = mq.create_queue("lost-dlq").unwrap();
    let arn = mq.queue_arn(&dead).unwrap();
    let policy = format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":"5"}}"#);
    let queue = mq
        .create_queue_with_attributes("lost", &[("RedrivePolicy", &policy)])
        .unwrap();
    mq.delete_queue(&dead).unwrap();
    let url = format!("{}{queue}", mq.endpoint());
    match connect(&mq, &url) {
        Err(SqsError::Refused(message)) => {
            assert!(
                message.contains("cannot read the dead-letter queue"),
                "{message}"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Pins the fields of the line connecting logs: the queue's default
/// visibility timeout, both retentions, `maxReceiveCount` and the
/// dead-letter queue's ARN.
#[test]
#[ignore = "requires Docker"]
fn connecting_logs_what_the_startup_checks_read() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "logged");
    let mut connected = None;
    let lines = spate_test::capture_logs(tracing::Level::INFO, || {
        connected = Some(connect(&mq, &url));
    });
    connected.unwrap().unwrap();
    let line = lines
        .iter()
        .find(|l| l.contains("the SQS feed is connected"))
        .unwrap_or_else(|| panic!("no connect line: {lines:#?}"));
    for field in [
        "queue_visibility_timeout",
        "max_receive_count",
        "dead_letter_queue",
        "dead_letter_retention_seconds",
        ":logged-dlq",
    ] {
        assert!(line.contains(field), "{field}: {line}");
    }
    // `retention_seconds` and `dead_letter_retention_seconds`.
    assert_eq!(line.matches("retention_seconds").count(), 2, "{line}");
}

/// The `feed:` key of a pipeline file, alone.
#[derive(serde::Deserialize)]
struct PipelineConfigFeed {
    feed: FeedSection,
}
