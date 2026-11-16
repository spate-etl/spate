//! A pipeline worker that reads S3 units through a feed, for the feed suites
//! of spate-sqs and spate-kafka.
//!
//! [`launch_customized`] builds a one-thread pipeline over the S3 source and a
//! capture sink, with one record per source batch and one row per sink write,
//! so an object's records span several writes. Each suite connects its own
//! feed and writes its own `feed:` section; the worker wraps the feed in a
//! [`Recording`] that sees every call the pipeline makes.
//!
//! This is a crate of its own because it depends on spate-s3 and spate-core.
//! In `spate-test-support`, every crate whose tests use that crate would
//! depend on them.

use object_store::ObjectStoreExt as _;
use spate_core::clock::TestClock as StopClock;
use spate_core::config::{FeedSection, PipelineConfig};
use spate_core::feed::{
    Confirmation, ConnectError, Delivery, DepthOf, DepthReading, Feed, FeedError, FeedMetricsCtx,
    FeedWaker, Receipt, Unit,
};
use spate_core::pipeline::{ExitReport, ExitState, Pipeline, RuntimeOptions, ShutdownHandle};
use spate_core::sink::SinkPoolConfig;
use spate_json::NdjsonFramer;
use spate_s3::{DescriptorObject, S3Source, SplitDescriptor};
use spate_test::{
    HANG, PipelineRun, ScriptedResult, SinkScript, TestEncoder, capture_sink, decode_rows,
    passthrough_runtime, wait_until,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

/// The `checkpoint:` section a worker reads unless a test gives its own.
pub const CHECKPOINT: &str = "{ interval: 100ms }";

/// The drain timeout a worker's stop runs under: the default, which
/// [`CHECKPOINT`] leaves alone.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// The step a bounded job's end check reads the feed at.
pub const STEP: Duration = Duration::from_secs(15);

/// The source setting under which [`two_range_unit`] is read in two ranges.
pub const TWO_RANGES: &str = "    split_target_bytes: 1MiB\n";

/// A [`Feed`] that passes every call to `F` and records each delivery's
/// count, receipt and when the reader took it. It counts the reads of the
/// feed's depth and the stops, and records each hand-back and close.
pub struct Recording<F> {
    inner: Arc<F>,
    seen: Arc<Mutex<Vec<(u32, Receipt, Instant)>>>,
    depth_reads: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    handed_back: Arc<Mutex<Vec<Receipt>>>,
    closes: Arc<Mutex<Vec<Duration>>>,
}

impl<F> Clone for Recording<F> {
    fn clone(&self) -> Self {
        Recording {
            inner: Arc::clone(&self.inner),
            seen: Arc::clone(&self.seen),
            depth_reads: Arc::clone(&self.depth_reads),
            stops: Arc::clone(&self.stops),
            handed_back: Arc::clone(&self.handed_back),
            closes: Arc::clone(&self.closes),
        }
    }
}

impl<F> std::fmt::Debug for Recording<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recording")
            .field("deliveries", &self.deliveries())
            .field("depth_reads", &self.depth_reads())
            .field("stops", &self.stopped_receiving())
            .finish_non_exhaustive()
    }
}

impl<F> Recording<F> {
    /// Records the calls made on `inner`.
    pub fn new(inner: Arc<F>) -> Recording<F> {
        Recording {
            inner,
            seen: Arc::default(),
            depth_reads: Arc::default(),
            stops: Arc::default(),
            handed_back: Arc::default(),
            closes: Arc::default(),
        }
    }

    /// The feed it records.
    pub fn inner(&self) -> &Arc<F> {
        &self.inner
    }

    /// Each delivery's count, in the order received.
    pub fn deliveries(&self) -> Vec<u32> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _, _)| *n)
            .collect()
    }

    /// Each delivery's receipt, in the order received.
    pub fn receipts(&self) -> Vec<Receipt> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|(_, r, _)| *r)
            .collect()
    }

    /// The first delivery's count and when the reader took it.
    pub fn first_seen(&self) -> Option<(u32, Instant)> {
        self.seen
            .lock()
            .unwrap()
            .first()
            .map(|(n, _, at)| (*n, *at))
    }

    /// Reads of the feed's depth, the startup probe included. Reads of the
    /// retry and dead-letter depths are not counted.
    pub fn depth_reads(&self) -> usize {
        self.depth_reads.load(Ordering::SeqCst)
    }

    /// Calls of [`Feed::stop_receiving`].
    pub fn stopped_receiving(&self) -> usize {
        self.stops.load(Ordering::SeqCst)
    }

    /// Each receipt handed back, in order.
    pub fn handed_back(&self) -> Vec<Receipt> {
        self.handed_back.lock().unwrap().clone()
    }

    /// What each close was given.
    pub fn closes(&self) -> Vec<Duration> {
        self.closes.lock().unwrap().clone()
    }
}

impl<F: Feed> Feed for Recording<F> {
    fn backend(&self) -> &str {
        self.inner.backend()
    }

    fn set_waker(&self, waker: FeedWaker) {
        self.inner.set_waker(waker);
    }

    fn attach_metrics(&self, cx: FeedMetricsCtx) {
        self.inner.attach_metrics(cx);
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

/// A pipeline reading units through the feed `F`.
pub struct Worker<F> {
    /// The feed as connected, outside its recording.
    pub feed: Arc<F>,
    /// The feed as the pipeline sees it.
    pub recording: Recording<F>,
    /// The pipeline's run thread.
    pub run: PipelineRun,
    /// The capture sink's script and record of writes.
    pub script: SinkScript,
    /// Stops the run.
    pub shutdown: ShutdownHandle,
}

impl<F> std::fmt::Debug for Worker<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("recording", &self.recording)
            .finish_non_exhaustive()
    }
}

impl<F> Worker<F> {
    /// Stops the run and returns how it exited.
    ///
    /// # Panics
    ///
    /// Panics when the run has not exited within [`HANG`], or exited
    /// without a report.
    pub fn stop(&self) -> ExitState {
        self.shutdown.trigger();
        self.run
            .wait_exit(HANG)
            .expect("the run exits")
            .unwrap()
            .state
    }
}

/// Launches a worker over the `source:` body `source_yaml` and the `feed:`
/// section `feed_section`, connecting its feed with `connect` on the
/// pipeline's I/O runtime.
///
/// `pre` scripts the sink before data flows, `customize` is applied to the
/// source, `checkpoint` is the `checkpoint:` section, and the stop is timed on
/// `stop_clock` when one is given.
///
/// # Panics
///
/// Panics when the pipeline file does not load, `connect` fails, or the
/// source cannot be built.
pub fn launch_customized<F: Feed + 'static>(
    connect: impl FnOnce(&FeedSection, &Handle) -> Result<F, ConnectError>,
    source_yaml: &str,
    feed_section: &str,
    pre: impl FnOnce(&SinkScript),
    customize: impl FnOnce(S3Source) -> S3Source,
    checkpoint: &str,
    stop_clock: Option<Arc<StopClock>>,
) -> Worker<F> {
    let yaml = pipeline_yaml(source_yaml, feed_section, checkpoint);
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
    let runtime = passthrough_runtime(
        pipeline.feed(Arc::new(recording.clone())),
        source,
        sink.with_pool_config(pool),
        TestEncoder,
        RuntimeOptions {
            max_records: 1,
            ..RuntimeOptions::default()
        },
    );
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

/// The pipeline file a worker reads.
pub fn pipeline_yaml(source_yaml: &str, feed_section: &str, checkpoint: &str) -> String {
    format!(
        "pipeline: {{ name: feed-worker, threads: 1 }}\nadmin: {{ listen: none }}\n\
         checkpoint: {checkpoint}\nmetrics: {{ exporter: none }}\n\
         source:\n{source_yaml}\
         sink: {{ capture: {{ chunk: {{ target_bytes: 64 }} }} }}\n{feed_section}"
    )
}

/// The `source:` body for an `s3:` source over the files under `data`, with
/// `extra` appended to the `s3:` section.
pub fn file_source(data: &Path, extra: &str) -> String {
    format!("  s3:\n    url: \"file://{}/\"\n{extra}", data.display())
}

/// The `feed:` key of a pipeline file, alone.
#[derive(Debug, serde::Deserialize)]
pub struct PipelineConfigFeed {
    /// The `feed:` section.
    pub feed: FeedSection,
}

/// A `data` directory under `dir`.
///
/// # Panics
///
/// Panics when the directory cannot be created.
pub fn data_dir(dir: &tempfile::TempDir) -> PathBuf {
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    data
}

/// Stages `rows` as the object `name` and returns the unit naming it.
///
/// # Panics
///
/// Panics when the file cannot be written.
pub fn stage(data: &Path, name: &str, rows: &[String]) -> Unit {
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
///
/// # Panics
///
/// Panics when the file cannot be written or the file store reports no ETag.
pub fn stage_with_etag(data: &Path, name: &str, rows: &[String]) -> Unit {
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

/// Stages a unit read in two byte ranges under [`TWO_RANGES`], named
/// `prefix`, and returns it with its rows: 68 lines of 16 KiB, 1 MiB + 64 KiB.
pub fn two_range_unit(data: &Path, prefix: &str) -> (Unit, Vec<String>) {
    let pad = "p".repeat(16 * 1024 - 24);
    let rows: Vec<String> = (0..68)
        .map(|i| format!("{{\"k\":\"{prefix}-{i:04}\",\"pad\":\"{pad}\"}}"))
        .collect();
    let unit = stage_with_etag(data, &format!("{prefix}.ndjson"), &rows);
    (unit, rows)
}

/// `n` records, each longer than the sink's 64-byte chunk target so each
/// takes a write of its own.
pub fn recs(prefix: &str, n: usize) -> Vec<String> {
    let pad = "p".repeat(64);
    (0..n)
        .map(|i| format!("{{\"k\":\"{prefix}-{i}\",\"pad\":\"{pad}\"}}"))
        .collect()
}

/// Publishes `unit` through `feed` and waits for its confirmation.
///
/// # Panics
///
/// Panics when the publish does not settle within [`HANG`] or fails.
pub fn publish(feed: &dyn Feed, unit: Unit) {
    let mut confirmation = feed.publish(unit);
    wait_until(HANG, "the publish settles", || {
        confirmation.try_outcome().is_some()
    });
    confirmation.try_outcome().unwrap().unwrap();
}

/// Rows the sink wrote successfully, sorted.
pub fn written(script: &SinkScript) -> Vec<String> {
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

/// `rows`, sorted.
pub fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

/// Asserts that a bounded job ended stalled: exit code 1, a failed run, and
/// a reason that names the stall and carries each of `needles`.
///
/// # Panics
///
/// Panics when any of those does not hold.
pub fn assert_stalled(report: &ExitReport, needles: &[&str]) {
    assert_eq!(report.exit_code(), 1);
    let ExitState::Failed(failure) = &report.state else {
        panic!("expected a failed run, got {:?}", report.state);
    };
    let reason = &failure.reason;
    assert!(reason.contains("bounded feed job stalled"), "{reason}");
    for needle in needles {
        assert!(reason.contains(needle), "no {needle:?} in: {reason}");
    }
}
