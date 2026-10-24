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

use object_store::ObjectStoreExt as _;
use spate_core::clock::TestClock as StopClock;
use spate_core::clock::tokio::TestClock;
use spate_core::config::{FeedSection, PipelineConfig};
use spate_core::feed::{
    Confirmation, Delivery, DepthOf, DepthReading, Feed, FeedError, FeedWaker, Receipt,
    STOP_RESERVE, Unit, wake_channel,
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEADLINE: Duration = Duration::from_secs(60);

/// The visibility period of the pipeline tests.
const VISIBILITY: Duration = Duration::from_secs(6);

/// The default `checkpoint.drain_timeout`, from which the stop's reserve
/// falls `STOP_RESERVE` early.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// The `checkpoint:` section of every worker but the startup check's.
const CHECKPOINT: &str = "{ interval: 100ms }";

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
/// delivery's count, receipt and when the reader took it, counts the reads
/// of the queue's depth and the stops, and records each hand-back and close.
#[derive(Clone)]
struct Recording {
    inner: Arc<SqsFeed>,
    seen: Arc<Mutex<Vec<(u32, Receipt, Instant)>>>,
    depth_reads: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    handed_back: Arc<Mutex<Vec<Receipt>>>,
    closes: Arc<Mutex<Vec<Duration>>>,
}

impl Recording {
    fn new(inner: Arc<SqsFeed>) -> Recording {
        Recording {
            inner,
            seen: Arc::default(),
            depth_reads: Arc::default(),
            stops: Arc::default(),
            handed_back: Arc::default(),
            closes: Arc::default(),
        }
    }

    fn receipts(&self) -> Vec<Receipt> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|(_, r, _)| *r)
            .collect()
    }

    fn stopped_receiving(&self) -> usize {
        self.stops.load(Ordering::SeqCst)
    }

    fn handed_back(&self) -> Vec<Receipt> {
        self.handed_back.lock().unwrap().clone()
    }

    fn closes(&self) -> Vec<Duration> {
        self.closes.lock().unwrap().clone()
    }

    /// Reads of the queue's depth, the startup probe included.
    fn depth_reads(&self) -> usize {
        self.depth_reads.load(Ordering::SeqCst)
    }

    fn deliveries(&self) -> Vec<u32> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _, _)| *n)
            .collect()
    }

    fn first_seen(&self) -> Option<(u32, Instant)> {
        self.seen
            .lock()
            .unwrap()
            .first()
            .map(|(n, _, at)| (*n, *at))
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
            .extend(got.iter().map(|d| (d.deliveries, d.receipt, now)));
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
    launch_with_source(data, "", feed_section, pre)
}

/// [`launch`] with `source_extra` appended to the `s3:` source section.
fn launch_with_source(
    data: &Path,
    source_extra: &str,
    feed_section: &str,
    pre: impl FnOnce(&SinkScript),
) -> Worker {
    launch_customized(
        data,
        source_extra,
        feed_section,
        pre,
        |source| source,
        CHECKPOINT,
        None,
    )
}

/// [`launch_with_source`], with the stop timed on `clock`.
fn launch_stopping(
    data: &Path,
    source_extra: &str,
    feed_section: &str,
    clock: &Arc<StopClock>,
    pre: impl FnOnce(&SinkScript),
) -> Worker {
    launch_customized(
        data,
        source_extra,
        feed_section,
        pre,
        |source| source,
        CHECKPOINT,
        Some(Arc::clone(clock)),
    )
}

/// The pipeline file every worker and lister run reads, with `checkpoint` as
/// its `checkpoint:` section.
fn pipeline_yaml(data: &Path, source_extra: &str, feed_section: &str, checkpoint: &str) -> String {
    format!(
        "pipeline: {{ name: sqs-feed, threads: 1 }}\nadmin: {{ listen: none }}\n\
         checkpoint: {checkpoint}\nmetrics: {{ exporter: none }}\n\
         source:\n  s3:\n    url: \"file://{}/\"\n{source_extra}\
         sink: {{ capture: {{ chunk: {{ target_bytes: 64 }} }} }}\n{feed_section}",
        data.display()
    )
}

/// [`launch_with_source`], with `customize` applied to the source, the
/// `checkpoint:` section `checkpoint`, and the stop timed on `stop_clock`
/// when one is given.
fn launch_customized(
    data: &Path,
    source_extra: &str,
    feed_section: &str,
    pre: impl FnOnce(&SinkScript),
    customize: impl FnOnce(S3Source) -> S3Source,
    checkpoint: &str,
    stop_clock: Option<Arc<StopClock>>,
) -> Worker {
    let yaml = pipeline_yaml(data, source_extra, feed_section, checkpoint);
    let mut config = PipelineConfig::from_str(&yaml).unwrap();
    config.pipeline.name = spate_test::unique_name(&config.pipeline.name);
    let pipeline = Pipeline::from_config(config).unwrap();
    let section = pipeline.config().feed.clone().unwrap();
    let io = pipeline.io_handle();
    let feed =
        Arc::new(SqsFeed::connect_with_static_credentials(&section, &io, "test", "test").unwrap());
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

/// The source setting under which [`two_range_unit`] is read in two ranges.
const TWO_RANGES: &str = "    split_target_bytes: 1MiB\n";

/// Stage a unit read in two byte ranges under [`TWO_RANGES`], named
/// `prefix`, and return it with its rows: 68 lines of 16 KiB, 1 MiB + 64 KiB.
fn two_range_unit(data: &Path, prefix: &str) -> (Unit, Vec<String>) {
    let pad = "p".repeat(16 * 1024 - 24);
    let rows: Vec<String> = (0..68)
        .map(|i| format!("{{\"k\":\"{prefix}-{i:04}\",\"pad\":\"{pad}\"}}"))
        .collect();
    let unit = stage_with_etag(data, &format!("{prefix}.ndjson"), &rows);
    (unit, rows)
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

/// Pins INV-1 for a unit read in two byte ranges: while the last write is
/// held the queue still holds the unit, and once it succeeds the unit is
/// deleted with every row of both ranges written once.
#[test]
#[ignore = "requires Docker"]
fn a_unit_read_in_ranges_is_deleted_after_its_last_range_is_acknowledged() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "ranged");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (unit, rows) = two_range_unit(&data, "r");

    let worker = launch_with_source(
        &data,
        TWO_RANGES,
        &feed_yaml(&mq, &url, VISIBILITY, ""),
        |sink| {
            for _ in 1..rows.len() {
                sink.enqueue_global(WriteOutcome::ok());
            }
            sink.enqueue_global(WriteOutcome::ok().held());
        },
    );
    publish(&worker.feed, unit);
    wait_until(DEADLINE, "the last write is held", || {
        worker.script.held_writes() == 1 && worker.script.writes().len() == rows.len()
    });
    assert_ne!(
        mq.approximate_counts(path_of(&url)).unwrap(),
        (0, 0),
        "the unit was deleted while a range's last write was held"
    );
    worker.script.release_held();
    wait_until(DEADLINE, "the unit is deleted", || {
        mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
    });
    assert_eq!(
        written(&worker.script),
        sorted(rows),
        "each row written once"
    );
    assert_eq!(worker.recording.deliveries(), [1]);
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

/// Pins that a stopping worker reads its unit to its end inside the drain
/// deadline: with the reserve never reached, the unit is read whole after the
/// stop, written once, never handed back, and deleted by the close before the
/// run exits.
#[test]
#[ignore = "requires Docker"]
fn a_stopping_worker_finishes_units_inside_the_drain_deadline() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "finished");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (unit, rows) = two_range_unit(&data, "f");
    let clock = StopClock::frozen();
    let worker = launch_stopping(
        &data,
        TWO_RANGES,
        &feed_yaml(&mq, &url, VISIBILITY, ""),
        &clock,
        |sink| sink.enqueue_global(WriteOutcome::ok().held()),
    );
    publish(&worker.feed, unit);
    wait_until(DEADLINE, "the first write is held", || {
        worker.script.held_writes() == 1
    });
    assert!(worker.script.writes().len() < rows.len());
    worker.shutdown.trigger();
    wait_until(DEADLINE, "the stop begins", || {
        worker.recording.stopped_receiving() == 1
    });
    worker.script.release_held();
    let report = worker
        .run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(
        written(&worker.script),
        sorted(rows),
        "each row written once"
    );
    assert_eq!(worker.recording.deliveries(), [1]);
    assert!(worker.recording.handed_back().is_empty());
    assert_eq!(worker.recording.closes().len(), 1);
    assert_eq!(
        mq.approximate_counts(path_of(&url)).unwrap(),
        (0, 0),
        "the unit is deleted by the time the run exits"
    );
}

/// Pins that a stopping worker hands back at the reserve the unit it has not
/// finished, and SQS hands it to another worker at once: at a 300 s
/// visibility the peer receives it within 10 s, as its second delivery, and
/// writes every row.
#[test]
#[ignore = "requires Docker"]
fn a_stopping_worker_hands_back_what_it_still_holds_at_once() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "handed");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (unit, rows) = two_range_unit(&data, "h");
    let section = feed_yaml(&mq, &url, Duration::from_secs(300), "");
    // One unit held, so A has no receive in flight to take the unit back.
    let held_one = feed_yaml(&mq, &url, Duration::from_secs(300), "  units_held: 1\n");
    let clock = StopClock::frozen();
    let a = launch_stopping(&data, TWO_RANGES, &held_one, &clock, |sink| {
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    publish(&a.feed, unit);
    wait_until(DEADLINE, "A holds the unit's first write", || {
        a.script.held_writes() == 1
    });
    let b = launch_with_source(&data, TWO_RANGES, &section, |_| {});
    a.shutdown.trigger();
    wait_until(DEADLINE, "A's stop begins", || {
        a.recording.stopped_receiving() == 1
    });
    clock.advance(DRAIN_TIMEOUT - STOP_RESERVE);
    let receipt = a.recording.receipts()[0];
    wait_until(DEADLINE, "A hands the unit back", || {
        a.recording.handed_back() == [receipt]
    });
    a.script.release_held();
    wait_until(Duration::from_secs(10), "B receives the unit", || {
        b.recording.first_seen().is_some()
    });
    assert_eq!(b.recording.deliveries(), [2]);
    let report = a.run.wait_exit(DEADLINE).expect("A exits").unwrap();
    assert_eq!(report.state, ExitState::Completed);
    wait_until(
        DEADLINE,
        "B writes every row and the unit is deleted",
        || {
            written(&b.script) == sorted(rows.clone())
                && mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
        },
    );
    assert_eq!(b.stop(), ExitState::Completed);
}

/// Pins that a unit the stopping worker's own receive takes back after its
/// hand-back is handed back again before the worker exits: at a 300 s
/// visibility, a peer started after the exit receives it within 10 s, as its
/// third delivery.
#[test]
#[ignore = "requires Docker"]
fn a_unit_a_stopping_worker_receives_again_is_handed_back_again() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "again");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (unit, rows) = two_range_unit(&data, "a");
    let section = feed_yaml(&mq, &url, Duration::from_secs(300), "");
    let clock = StopClock::frozen();
    // A wants more units than it holds, so its 20 s receive is in flight
    // when it hands the unit back, and no other worker is receiving.
    let a = launch_stopping(&data, TWO_RANGES, &section, &clock, |sink| {
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    publish(&a.feed, unit);
    wait_until(DEADLINE, "A holds the unit's first write", || {
        a.script.held_writes() == 1
    });
    a.shutdown.trigger();
    wait_until(DEADLINE, "A's stop begins", || {
        a.recording.stopped_receiving() == 1
    });
    clock.advance(DRAIN_TIMEOUT - STOP_RESERVE);
    wait_until(DEADLINE, "A hands the unit back", || {
        !a.recording.handed_back().is_empty()
    });
    a.script.release_held();
    let report = a.run.wait_exit(DEADLINE).expect("A exits").unwrap();
    assert_eq!(report.state, ExitState::Completed);

    let b = launch_with_source(&data, TWO_RANGES, &section, |_| {});
    wait_until(Duration::from_secs(10), "B receives the unit", || {
        b.recording.first_seen().is_some()
    });
    assert_eq!(b.recording.deliveries(), [3]);
    wait_until(
        DEADLINE,
        "B writes every row and the unit is deleted",
        || {
            written(&b.script) == sorted(rows.clone())
                && mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
        },
    );
    assert_eq!(b.stop(), ExitState::Completed);
}

/// Pins the order of a stop on SQS inside the drain timeout: the unit not
/// finished at the reserve is handed back, the sink drains the finished one,
/// and the close deletes it, with no clock moved past the reserve. At a 300 s
/// visibility the queue then holds only the handed-back unit, visible.
#[test]
#[ignore = "requires Docker"]
fn the_hand_back_and_sink_drain_finish_inside_the_drain_timeout() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "drained");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let finished = stage(&data, "f.ndjson", &recs("f", 1));
    let (unfinished, _) = two_range_unit(&data, "u");
    let clock = StopClock::frozen();
    let worker = launch_stopping(
        &data,
        TWO_RANGES,
        &feed_yaml(&mq, &url, Duration::from_secs(300), ""),
        &clock,
        |sink| sink.enqueue_global(WriteOutcome::ok().held()),
    );
    publish(&worker.feed, finished);
    wait_until(DEADLINE, "the finished unit's write is held", || {
        worker.script.held_writes() == 1
    });
    publish(&worker.feed, unfinished);
    wait_until(DEADLINE, "the unfinished unit is received", || {
        worker.recording.deliveries().len() == 2
    });
    worker.shutdown.trigger();
    wait_until(DEADLINE, "the stop begins", || {
        worker.recording.stopped_receiving() == 1
    });
    clock.advance(DRAIN_TIMEOUT - STOP_RESERVE);
    let receipt = worker.recording.receipts()[1];
    wait_until(DEADLINE, "the unfinished unit is handed back", || {
        worker.recording.handed_back() == [receipt]
    });
    worker.script.release_held();
    let report = worker
        .run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .unwrap();
    assert_eq!(report.state, ExitState::Completed);
    wait_until(
        Duration::from_secs(10),
        "the unfinished unit is visible and the finished one deleted",
        || mq.approximate_counts(path_of(&url)).unwrap() == (1, 0),
    );
}

/// Pins that startup refuses a `checkpoint.drain_timeout` within the stop's
/// reserve on the SQS feed, naming the key and the feed.
#[test]
#[ignore = "requires Docker"]
fn startup_refuses_a_drain_timeout_within_the_stop_reserve() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "reserve");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let worker = launch_customized(
        &data,
        "",
        &feed_yaml(&mq, &url, VISIBILITY, ""),
        |_| {},
        |source| source,
        "{ interval: 100ms, drain_timeout: 5s }",
        None,
    );
    let e = worker
        .run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .unwrap_err();
    let message = e.to_string();
    assert!(
        message.contains("checkpoint.drain_timeout") && message.contains("`sqs`"),
        "{message}"
    );
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
/// visibility timeout, both retentions, `maxReceiveCount`, the dead-letter
/// queue's ARN and a message an earlier job left in it.
#[test]
#[ignore = "requires Docker"]
fn connecting_logs_what_the_startup_checks_read() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "logged");
    mq.send_message(&format!("{}-dlq", path_of(&url)), "left by an earlier job")
        .unwrap();
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
        "dead_letter_visible=1",
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

/// The step a bounded job's end check reads the queue at.
const STEP: Duration = Duration::from_secs(15);

/// Steps in the default five-minute settling window.
const STEPS: usize = 20;

/// The `feed:` section of a bounded job at a `file://` location under
/// `dir`, outside the data prefix.
fn bounded_yaml(mq: &ElasticMq, url: &str, dir: &tempfile::TempDir) -> (String, PathBuf) {
    let jobs = dir.path().join("jobs");
    let extra = format!("  location: \"file://{}/\"\n", jobs.display());
    (feed_yaml(mq, url, VISIBILITY, &extra), jobs)
}

/// Run the job's lister once: publish a unit per object under `data` to
/// the queue and write the fingerprint and the end marker at the location.
fn list_once(data: &Path, feed_section: &str) {
    let config =
        PipelineConfig::from_str(&pipeline_yaml(data, "", feed_section, CHECKPOINT)).unwrap();
    let section = config.feed.clone().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let feed =
        SqsFeed::connect_with_static_credentials(&section, rt.handle(), "test", "test").unwrap();
    S3Source::from_component_config(&config.source, rt.handle().clone())
        .unwrap()
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)))
        .list_once(&section, Arc::new(feed))
        .unwrap();
}

/// Launch a bounded worker whose end check runs on `clock`.
fn launch_bounded(data: &Path, feed_section: &str, clock: &Arc<TestClock>) -> Worker {
    let clock = Arc::clone(clock);
    launch_customized(
        data,
        "",
        feed_section,
        |_| {},
        move |source| source.with_clock(clock),
        CHECKPOINT,
        None,
    )
}

/// Wait until the worker has read the queue's depth `n` times, the startup
/// probe included.
fn wait_for_reads(worker: &Worker, n: usize) {
    wait_until(DEADLINE, &format!("{n} depth reads"), || {
        worker.recording.depth_reads() >= n
    });
}

/// Advance `steps` steps, waiting after each for one more depth read, from
/// `reads` already taken. Returns the count.
fn step_reads(worker: &Worker, clock: &TestClock, mut reads: usize, steps: usize) -> usize {
    for _ in 0..steps {
        clock.advance(STEP);
        reads += 1;
        wait_for_reads(worker, reads);
    }
    reads
}

fn running(worker: &Worker) -> bool {
    worker.run.wait_exit(Duration::ZERO).is_none()
}

/// Advance one step and wait until the worker takes one more depth read
/// than `reads`, or exits. Returns its exit report once it has exited; the
/// run hands its report out once, so this keeps it.
fn step_or_exit(
    worker: &Worker,
    clock: &TestClock,
    reads: usize,
) -> Option<spate_core::pipeline::ExitReport> {
    clock.advance(STEP);
    let mut exit = None;
    wait_until(DEADLINE, "the next depth read or the end", || {
        if exit.is_none() {
            exit = worker.run.wait_exit(Duration::ZERO);
        }
        exit.is_some() || worker.recording.depth_reads() > reads
    });
    exit.map(|report| report.expect("the run starts"))
}

/// Pins that a bounded job on SQS waits for its lister's end marker: with
/// the marker gone the worker reads its unit and keeps running, taking no
/// depth read, and once the marker is back it ends `Completed` one window of
/// empty reads later.
#[test]
#[ignore = "requires Docker"]
fn a_bounded_job_waits_for_the_end_marker() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "marker");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("m", 2);
    let _unit = stage(&data, "m.ndjson", &rows);
    let (section, jobs) = bounded_yaml(&mq, &url, &dir);
    list_once(&data, &section);
    let end = jobs.join("end");
    let marker = std::fs::read(&end).expect("the lister wrote the end marker");
    std::fs::remove_file(&end).unwrap();

    let clock = TestClock::frozen();
    let worker = launch_bounded(&data, &section, &clock);
    wait_until(DEADLINE, "the unit is read and deleted", || {
        written(&worker.script) == sorted(rows.clone())
            && mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
    });
    let probe = 1;
    clock.advance(STEP * STEPS as u32);
    assert!(running(&worker), "no marker, no end");
    assert_eq!(worker.recording.depth_reads(), probe, "no window opened");

    std::fs::write(&end, marker).unwrap();
    clock.advance(STEP);
    wait_for_reads(&worker, probe + 1);
    step_reads(&worker, &clock, probe + 1, STEPS - 1);
    assert!(running(&worker), "one step short of the window");
    clock.advance(STEP);
    let report = worker
        .run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.exit_code(), 0);
}

/// Pins that a unit SQS moved to the dead-letter queue ends a bounded job
/// `Stalled`: once the queue reads empty over the window, the dead-letter
/// queue's count fails the run with exit code 1, naming it.
#[test]
#[ignore = "requires Docker"]
fn a_dead_lettered_unit_ends_the_job_stalled() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "stalled");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let _unit = stage(&data, "gone.ndjson", &recs("g", 1));
    let (section, _jobs) = bounded_yaml(&mq, &url, &dir);
    list_once(&data, &section);
    // The unit now names an object that is gone, so every read fails it.
    std::fs::remove_file(data.join("gone.ndjson")).unwrap();

    let clock = TestClock::frozen();
    let worker = launch_bounded(&data, &section, &clock);
    let dead = format!("{}-dlq", path_of(&url));
    wait_until(DEADLINE, "the unit moves to the dead-letter queue", || {
        mq.approximate_counts(&dead).unwrap() == (1, 0)
            && mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
    });
    // The worker reads the queue each step while the unit is held, so the
    // window opens on whichever read finds it empty.
    let first = worker.recording.depth_reads();
    let report = loop {
        let reads = worker.recording.depth_reads();
        if let Some(report) = step_or_exit(&worker, &clock, reads) {
            break report;
        }
        assert!(reads < first + 4 * STEPS, "the job never ended");
    };
    assert_eq!(report.exit_code(), 1);
    let ExitState::Failed(failure) = &report.state else {
        panic!("expected a failed run, got {:?}", report.state);
    };
    assert!(
        failure.reason.contains("bounded feed job stalled")
            && failure.reason.contains("stalled-dlq")
            && failure.reason.contains("1 visible"),
        "{}",
        failure.reason
    );
}

/// Pins that an idle worker stays to the end: while a stopped peer holds
/// the job's last unit the worker keeps reading the queue past a whole
/// window, reads the unit when SQS delivers it again, and ends `Completed`
/// one window after the queue reads empty.
#[test]
#[ignore = "requires Docker"]
fn an_idle_worker_reads_a_unit_redelivered_after_its_owner_dies() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "owner");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("o", 2);
    let _unit = stage(&data, "o.ndjson", &rows);
    let (section, _jobs) = bounded_yaml(&mq, &url, &dir);
    list_once(&data, &section);

    // The owner takes the unit and stops as a crashed worker would.
    let config = PipelineConfig::from_str(&pipeline_yaml(&data, "", &section, CHECKPOINT)).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let owner = SqsFeed::connect_with_static_credentials(
        config.feed.as_ref().unwrap(),
        rt.handle(),
        "test",
        "test",
    )
    .unwrap();
    let (waker, park) = wake_channel();
    owner.set_waker(waker);
    let mut taken = Vec::new();
    wait_until(DEADLINE, "the owner takes the unit", || {
        taken.extend(owner.receive(1).unwrap());
        park.park(Duration::from_millis(100));
        !taken.is_empty()
    });
    owner.abandon();

    let clock = TestClock::frozen();
    let worker = launch_bounded(&data, &section, &clock);
    wait_for_reads(&worker, 2);
    let reads = step_reads(&worker, &clock, 2, STEPS);
    assert!(
        running(&worker),
        "the worker stands by while a peer holds a unit"
    );
    wait_until(DEADLINE, "the unit is delivered again and read", || {
        worker.recording.deliveries().contains(&2)
            && written(&worker.script) == sorted(rows.clone())
            && mq.approximate_counts(path_of(&url)).unwrap() == (0, 0)
    });
    let report = loop {
        let now = worker.recording.depth_reads();
        if let Some(report) = step_or_exit(&worker, &clock, now) {
            break report;
        }
        assert!(now < reads + 4 * STEPS, "the job never ended");
    };
    assert_eq!(report.state, ExitState::Completed);
}

/// Pins that a delayed message keeps a bounded job running: the queue's
/// delayed count holds the window open past a whole window.
#[test]
#[ignore = "requires Docker"]
fn a_delayed_unit_keeps_the_job_running() {
    let mq = ElasticMq::start().unwrap();
    let url = queue_with_dead_letter(&mq, "delayed");
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (section, _jobs) = bounded_yaml(&mq, &url, &dir);
    list_once(&data, &section);
    mq.send_delayed(path_of(&url), "later", Duration::from_secs(900))
        .unwrap();

    let clock = TestClock::frozen();
    let worker = launch_bounded(&data, &section, &clock);
    wait_for_reads(&worker, 2);
    step_reads(&worker, &clock, 2, STEPS + 2);
    assert!(running(&worker), "a delayed unit holds the window open");
    assert_eq!(worker.stop(), ExitState::Completed);
}
