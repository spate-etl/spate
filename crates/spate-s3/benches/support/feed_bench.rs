//! The rig for `feed_small_objects_wall.rs`: a staged corpus of small
//! newline-delimited objects, and one whole pipeline per run that reads it
//! through the feed path or the coordinated solo path.
//!
//! `tests/feed_bench_fixture.rs` drives the same rig at a smaller object count,
//! so what a case measures is also what the test checks: every row reaches the
//! sink on every run, not only on the first.

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, ObjectStoreExt as _, PutPayload,
    PutResult,
};
use spate_core::config::PipelineConfig;
use spate_core::error::SinkError;
use spate_core::feed::memory::MemoryFeed;
use spate_core::feed::{Confirmation, Delivery, Feed, FeedError, FeedWaker, Receipt, Unit};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, PipelineRuntime, RuntimeOptions, ShutdownHandle};
use spate_core::sink::{KeyHashRouter, SealedBatch, ShardWriter, SinkParts, SinkPoolConfig};
use spate_json::NdjsonFramer;
use spate_s3::{DescriptorObject, S3Source, SplitDescriptor};
use spate_test::{BytesPassthrough, PipelineRun, TestEncoder};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Objects in the benchmark corpus: three waves of 96 coordinated splits at
/// 16 objects a split.
pub(crate) const OBJECTS: usize = 4_608;

/// Lines in every object.
pub(crate) const LINES: usize = 1_000;

/// Bytes in every line, its newline included.
pub(crate) const LINE_BYTES: usize = 64;

/// Splits the coordinated arm holds at once, the lane count of the measured
/// cloud runs.
pub(crate) const READ_PARALLELISM: u32 = 96;

/// Pipeline threads, the same for both arms.
pub(crate) const THREADS: usize = 4;

/// Sink linger of every case, the value the measured cloud runs used.
pub(crate) const SHORT_LINGER: Duration = Duration::from_millis(5);

/// How each run reads the corpus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Arm {
    /// One one-object unit per object from a fresh in-process feed.
    Feed,
    /// The coordinated solo path over a fresh in-process store, at
    /// [`READ_PARALLELISM`] splits in flight.
    Coordinated,
}

/// The staged file name of object `object`.
pub(crate) fn object_name(object: usize) -> String {
    format!("obj-{object:05}.ndjson")
}

/// Line `line` of object `object`: a JSON record padded to [`LINE_BYTES`],
/// a pure function of the two indices.
pub(crate) fn line(object: usize, line: usize) -> [u8; LINE_BYTES] {
    let head = format!("{{\"object\":{object},\"line\":{line},\"pad\":\"");
    let mut out = [b'x'; LINE_BYTES];
    out[..head.len()].copy_from_slice(head.as_bytes());
    out[LINE_BYTES - 3..].copy_from_slice(b"\"}\n");
    out
}

/// The bytes of object `object`.
pub(crate) fn object_bytes(object: usize) -> Vec<u8> {
    (0..LINES).flat_map(|l| line(object, l)).collect()
}

/// A corpus staged in a temporary directory, and the settings every run over
/// it shares.
#[derive(Debug)]
pub(crate) struct Rig {
    _dir: tempfile::TempDir,
    data: PathBuf,
    arm: Arm,
    hold: Duration,
    linger: Duration,
    deadline: Duration,
    /// One descriptor body per object, carrying the size and ETag the store
    /// reports, as the coordinated planner's descriptors do.
    bodies: Vec<(String, Vec<u8>)>,
}

impl Rig {
    /// Stages `objects` objects and fixes how each run reads them.
    ///
    /// `hold` is added before every GET. `linger` is the sink's batch linger.
    /// A run that has not finished within `deadline` panics.
    ///
    /// # Panics
    ///
    /// Panics when the corpus cannot be written or read back.
    pub(crate) fn new(
        arm: Arm,
        objects: usize,
        hold: Duration,
        linger: Duration,
        deadline: Duration,
    ) -> Rig {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let data = dir.path().join("data");
        fs::create_dir_all(&data).expect("the data directory");
        for object in 0..objects {
            fs::write(data.join(object_name(object)), object_bytes(object))
                .expect("the object is written");
        }
        let bodies = descriptor_bodies(&data, objects);
        Rig {
            _dir: dir,
            data,
            arm,
            hold,
            linger,
            deadline,
            bodies,
        }
    }

    /// Objects in the corpus.
    pub(crate) fn objects(&self) -> usize {
        self.bodies.len()
    }

    /// Builds one run: its feed or coordination section, store wrapper, sink
    /// and a uniquely named pipeline. Nothing is shared with an earlier run.
    ///
    /// # Panics
    ///
    /// Panics when the pipeline does not assemble.
    pub(crate) fn make(&self) -> Run {
        let feed = match self.arm {
            Arm::Feed => Some(Arc::new(self.feed())),
            Arm::Coordinated => None,
        };
        let mut config = PipelineConfig::from_str(&self.yaml()).expect("the config parses");
        config.pipeline.name = spate_test::unique_name(&config.pipeline.name);
        let pipeline = Pipeline::from_config(config).expect("the pipeline builds");
        let io = pipeline.io_handle();
        let store: Arc<dyn ObjectStore> = Arc::new(DelayStore {
            inner: LocalFileSystem::new(),
            hold: self.hold,
        });
        let mut source = S3Source::from_component_config(&pipeline.config().source, io)
            .expect("the source config")
            .with_framer(|| Box::new(NdjsonFramer::new(LINE_BYTES)))
            .with_store(store);
        if let Some(feed) = &feed {
            source = source.with_feed(Arc::clone(feed) as Arc<dyn Feed>);
        }
        let rows = Arc::new(AtomicU64::new(0));
        let mut pool = SinkPoolConfig::default();
        pool.batch.linger = self.linger;
        let sink = SinkParts::new(
            CountWriter {
                rows: Arc::clone(&rows),
            },
            vec![vec![()]],
            pool,
        )
        .with_component_type("count");
        let runtime = pipeline
            .sink(sink)
            .expect("the sink installs")
            .chains(|ctx| {
                let chunk = ctx.chunk();
                chain_owned::<Vec<u8>, _>(BytesPassthrough)
                    .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
                    .build()
            })
            .runtime_options(RuntimeOptions {
                handle_signals: false,
                ..RuntimeOptions::default()
            })
            .into_runtime(source)
            .expect("the runtime assembles");
        Run {
            shutdown: runtime.shutdown_handle(),
            runtime,
            feed,
            rows,
            objects: self.objects() as u64,
            deadline: self.deadline,
        }
    }

    /// A fresh in-process feed holding one one-object unit per object.
    fn feed(&self) -> BenchFeed {
        let inner = MemoryFeed::new();
        for (name, body) in &self.bodies {
            drop(inner.publish(Unit::new(name.clone(), body.clone())));
        }
        BenchFeed {
            inner,
            waker: Mutex::new(None),
            acknowledged: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    fn yaml(&self) -> String {
        let mut yaml = format!(
            "pipeline: {{ name: s3-feed-bench, threads: {THREADS} }}\n\
             admin: {{ listen: none }}\n\
             metrics: {{ exporter: none }}\n\
             source:\n  s3:\n    url: \"file://{}/\"\n    split_target_bytes: 1MiB\n\
             sink: {{ count: {{}} }}\n",
            self.data.display()
        );
        if self.arm == Arm::Coordinated {
            yaml.push_str(&format!(
                "coordination:\n  max_in_flight: {READ_PARALLELISM}\n  store: {{ memory: {{}} }}\n"
            ));
        }
        yaml
    }
}

/// One descriptor body per staged object, named by its file name.
fn descriptor_bodies(data: &Path, objects: usize) -> Vec<(String, Vec<u8>)> {
    let store = LocalFileSystem::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime for the HEADs");
    (0..objects)
        .map(|object| {
            let name = object_name(object);
            let key = data
                .join(&name)
                .to_string_lossy()
                .trim_start_matches('/')
                .to_owned();
            let meta = rt
                .block_on(store.head(&StorePath::from(key.as_str())))
                .expect("the staged object is readable");
            let descriptor = SplitDescriptor::new(vec![DescriptorObject {
                key,
                size: meta.size,
                etag: meta.e_tag,
                last_modified_ms: meta.last_modified.timestamp_millis(),
            }]);
            (name, descriptor.encode().expect("the descriptor encodes"))
        })
        .collect()
}

/// One assembled pipeline, ready to run once.
#[derive(Debug)]
pub(crate) struct Run {
    runtime: PipelineRuntime<S3Source>,
    shutdown: ShutdownHandle,
    feed: Option<Arc<BenchFeed>>,
    rows: Arc<AtomicU64>,
    objects: u64,
    deadline: Duration,
}

/// What one run did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Drove {
    /// Rows this run's sink wrote.
    pub(crate) rows: u64,
    /// Units the feed acknowledged during this run; `None` on the coordinated
    /// arm.
    pub(crate) acknowledged: Option<u64>,
}

impl Run {
    /// Runs the pipeline to its end and reports what it did.
    ///
    /// The coordinated arm ends on its own when the plan completes. The feed
    /// path never drains, so the feed arm waits until every unit is
    /// acknowledged, then triggers shutdown and wakes the reader through the
    /// waker the feed was handed, so the run pays no poll timeout after its
    /// last unit.
    ///
    /// # Panics
    ///
    /// Panics when the run fails or misses the rig's deadline.
    pub(crate) fn drive(self) -> Drove {
        let Run {
            runtime,
            shutdown,
            feed,
            rows,
            objects,
            deadline,
        } = self;
        let started = Instant::now();
        let before = feed.as_ref().map(|f| f.acknowledged());
        let run = PipelineRun::spawn(move || runtime.run());
        if let Some(feed) = &feed {
            feed.wait_for(objects, deadline);
            shutdown.trigger();
            feed.wake();
        }
        let left = deadline.saturating_sub(started.elapsed());
        let report = run
            .wait_exit(left)
            .expect("the run exits within the deadline")
            .expect("the run starts");
        assert_eq!(report.state, ExitState::Completed, "the run completes");
        Drove {
            rows: rows.load(Ordering::Relaxed),
            acknowledged: feed
                .as_ref()
                .zip(before)
                .map(|(f, before)| f.acknowledged() - before),
        }
    }
}

/// A [`MemoryFeed`] that counts acknowledgments and keeps the waker the
/// reader hands it.
#[derive(Debug)]
struct BenchFeed {
    inner: MemoryFeed,
    waker: Mutex<Option<FeedWaker>>,
    acknowledged: Mutex<u64>,
    changed: Condvar,
}

impl BenchFeed {
    fn acknowledged(&self) -> u64 {
        *self.acknowledged.lock().expect("never poisoned")
    }

    /// Blocks until `target` units have been acknowledged.
    fn wait_for(&self, target: u64, deadline: Duration) {
        let count = self.acknowledged.lock().expect("never poisoned");
        let (count, timeout) = self
            .changed
            .wait_timeout_while(count, deadline, |count| *count < target)
            .expect("never poisoned");
        assert!(
            !timeout.timed_out(),
            "{} of {target} units acknowledged within {deadline:?}",
            *count
        );
    }

    fn wake(&self) {
        if let Some(waker) = &*self.waker.lock().expect("never poisoned") {
            waker.wake();
        }
    }
}

impl Feed for BenchFeed {
    fn backend(&self) -> &str {
        self.inner.backend()
    }
    fn set_waker(&self, waker: FeedWaker) {
        *self.waker.lock().expect("never poisoned") = Some(waker.clone());
        self.inner.set_waker(waker);
    }
    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        self.inner.receive(max)
    }
    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.renew(receipt)
    }
    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.acknowledge(receipt)?;
        *self.acknowledged.lock().expect("never poisoned") += 1;
        self.changed.notify_all();
        Ok(())
    }
    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.inner.fail(receipt, reason)
    }
    fn publish(&self, unit: Unit) -> Confirmation {
        self.inner.publish(unit)
    }
}

/// A sink writer that counts rows and discards them.
#[derive(Debug)]
struct CountWriter {
    rows: Arc<AtomicU64>,
}

impl ShardWriter for CountWriter {
    type Endpoint = ();

    async fn write_batch(&self, (): &(), batch: &SealedBatch) -> Result<(), SinkError> {
        self.rows.fetch_add(batch.rows, Ordering::Relaxed);
        Ok(())
    }
}

/// A local filesystem store that waits `hold` before every GET.
///
/// `object_store`'s throttling wrapper panics on every GET whose payload is a
/// local file, so it cannot wrap this store.
#[derive(Debug)]
struct DelayStore {
    inner: LocalFileSystem,
    hold: Duration,
}

impl fmt::Display for DelayStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DelayStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for DelayStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !self.hold.is_zero() {
            tokio::time::sleep(self.hold).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
