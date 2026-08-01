//! A delegating `ObjectStore` that records the requests a backfill issues.
//!
//! It is the teeth behind two properties the store is the only place to
//! observe. LIST counting backs the "workers never list" acceptance
//! criterion: a coordinated job's total LIST count across every instance
//! must be exactly the planner's. GET recording backs `request_shape.rs`:
//! how many reads a run issues, which byte ranges it asks for, and how many
//! it keeps in flight are all invisible to the metrics layer, which counts
//! bytes rather than requests.

use futures_util::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, ObjectMeta, ObjectStore, PutPayload, PutResult,
};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

/// The shape of a `GetOptions::range`, mirroring the `RangeKind` the fetcher's
/// own tests use (`src/fetch.rs`): the ETag-pinned read path issues `Bounded`
/// windows, the streaming fallback a single un-ranged (`Full`) or `Offset`
/// resume GET.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RangeKind {
    Full,
    Offset(u64),
    Bounded(u64, u64),
    /// The fetcher issues no suffix range. Recorded rather than panicked on:
    /// a panic here runs on an I/O-runtime task, where it would surface as a
    /// hung pipeline instead of a failed assertion.
    Suffix(u64),
}

fn range_kind(range: &Option<GetRange>) -> RangeKind {
    match range {
        None => RangeKind::Full,
        Some(GetRange::Offset(o)) => RangeKind::Offset(*o),
        Some(GetRange::Bounded(r)) => RangeKind::Bounded(r.start, r.end),
        Some(GetRange::Suffix(n)) => RangeKind::Suffix(*n),
    }
}

/// One recorded `get_opts` call.
#[derive(Clone, Debug)]
pub(crate) struct GetRecord {
    /// Full store key (an absolute filesystem path for `LocalFileSystem`).
    pub(crate) key: String,
    /// The range the fetcher asked for.
    pub(crate) range: RangeKind,
}

impl GetRecord {
    /// The key's last path segment — the staged file name, which is what a
    /// test names its objects by.
    pub(crate) fn object(&self) -> &str {
        self.key.rsplit('/').next().unwrap_or(&self.key)
    }
}

/// How the spy interferes with the reads it records.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SpyOptions {
    /// Hold every GET until this many are in flight at once, so a
    /// concurrency assertion measures the source's read parallelism rather
    /// than how the scheduler happened to interleave fast local reads. Zero
    /// disables the gate.
    pub(crate) gate_depth: usize,
    /// How long a gated GET waits before the gate latches open for good.
    /// The backstop is what makes a collapse in parallelism fail the depth
    /// assertion instead of hanging the run.
    pub(crate) gate_deadline: Duration,
    /// Latency injected into every GET, for runs that must outlive a
    /// coordination tick.
    pub(crate) hold: Duration,
}

impl Default for SpyOptions {
    fn default() -> SpyOptions {
        SpyOptions {
            gate_depth: 0,
            gate_deadline: Duration::from_secs(10),
            hold: Duration::ZERO,
        }
    }
}

/// Wrap a root-scoped local filesystem store, counting `list` calls.
/// Returns the store (for `S3Source::with_store`) and the counter.
pub(crate) fn counting_local_store() -> (Arc<dyn ObjectStore>, Arc<AtomicUsize>) {
    let (store, spy) = spying_local_store(SpyOptions::default());
    (store, Arc::clone(&spy.lists))
}

/// Wrap a root-scoped local filesystem store, recording every LIST and GET.
/// Returns the store (for `S3Source::with_store`) and the observation handle.
pub(crate) fn spying_local_store(options: SpyOptions) -> (Arc<dyn ObjectStore>, StoreSpy) {
    let spy = StoreSpy {
        lists: Arc::new(AtomicUsize::new(0)),
        gets: Arc::new(Mutex::new(Vec::new())),
        depth: Arc::new(Depth {
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            gate: options.gate_depth,
            open: watch::channel(false).0,
            deadline: options.gate_deadline,
        }),
    };
    let store = SpyStore {
        inner: LocalFileSystem::new(),
        spy: spy.clone(),
        hold: options.hold,
    };
    (Arc::new(store), spy)
}

/// Everything a test reads back from the store, shared with the wrapper.
#[derive(Clone, Debug)]
pub(crate) struct StoreSpy {
    lists: Arc<AtomicUsize>,
    gets: Arc<Mutex<Vec<GetRecord>>>,
    depth: Arc<Depth>,
}

impl StoreSpy {
    /// LIST calls (`list` and `list_with_delimiter` together).
    pub(crate) fn lists(&self) -> usize {
        self.lists.load(Ordering::Relaxed)
    }

    /// Every recorded GET, in the order the calls entered the store.
    pub(crate) fn gets(&self) -> Vec<GetRecord> {
        self.gets.lock().expect("spy log is never poisoned").clone()
    }

    /// The most GETs ever in flight at one instant.
    pub(crate) fn peak_concurrent_gets(&self) -> usize {
        self.depth.peak.load(Ordering::SeqCst)
    }
}

/// In-flight accounting, plus the optional gate that makes a depth
/// observation deterministic.
#[derive(Debug)]
struct Depth {
    current: AtomicUsize,
    peak: AtomicUsize,
    /// Depth the gate waits for; zero means no gate.
    gate: usize,
    /// Latches true once the gate opens — either because `gate` reads were
    /// in flight together, or because a waiter hit `deadline`. It never
    /// closes again, so a run pays the gate at most once.
    open: watch::Sender<bool>,
    deadline: Duration,
}

impl Depth {
    /// Count one read in, and hold it at the gate if one is configured. The
    /// returned guard counts it back out when it drops.
    async fn enter(self: &Arc<Depth>) -> InFlight {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let guard = InFlight(Arc::clone(self));
        if self.gate == 0 {
            return guard;
        }
        if now >= self.gate {
            self.open.send_replace(true);
            return guard;
        }
        let mut rx = self.open.subscribe();
        // `wait_for` tests the current value first, so a read arriving after
        // the gate opened is never parked.
        if tokio::time::timeout(self.deadline, rx.wait_for(|open| *open))
            .await
            .is_err()
        {
            self.open.send_replace(true);
        }
        guard
    }
}

/// Decrements the in-flight count when the `get_opts` future resolves.
#[derive(Debug)]
struct InFlight(Arc<Depth>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.current.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct SpyStore {
    inner: LocalFileSystem,
    spy: StoreSpy,
    hold: Duration,
}

impl fmt::Display for SpyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpyStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SpyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.spy
            .gets
            .lock()
            .expect("spy log is never poisoned")
            .push(GetRecord {
                key: location.to_string(),
                range: range_kind(&options.range),
            });
        let _in_flight = self.spy.depth.enter().await;
        if !self.hold.is_zero() {
            tokio::time::sleep(self.hold).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.spy.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.spy.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
