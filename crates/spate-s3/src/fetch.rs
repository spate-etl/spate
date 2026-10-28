//! The async edge: the shared listing helper and one fetcher task per
//! unit part in progress, reading its member objects' bytes to the pipeline thread.
//!
//! Runs on the pipeline's I/O runtime. Every channel send `await`s, so a
//! fetcher never blocks a runtime worker and sink workers sharing the
//! runtime keep running while lanes are back-pressured.
//!
//! Each ETag-pinned object is read as a sequence of **bounded ranged GETs,
//! each drained fully into memory before any of its chunks is forwarded**.
//! The next window's GET runs while the current window is handed on. Each
//! body is drained as it arrives, so a back-pressured lane parks on buffered
//! bytes, never an idle S3 body that the client's request timeout (or an
//! intermediary) would reset mid-stream. See [`stream_object`].
//!
//! # Determinism
//!
//! A fetcher never lists: it reads its unit's member objects in descriptor
//! order, so every (ordinal, record index) offset is minted against the
//! immutable descriptor, identical under every reader of the unit.
//!
//! # Retries
//!
//! Transient GET failures are retried inside the fetcher with capped
//! exponential backoff, resuming from the last delivered byte with a
//! ranged GET **conditioned on the object's ETag**, so a splice of two
//! different object versions is impossible. Persistent failure (attempt
//! budget exhausted) and the object-level non-retryable classes (missing
//! key, failed precondition) **poison the unit**, which is failed back to the
//! feed for another delivery and dead-lettered at the feed's cap, while
//! credentials/configuration failures stay fatal for the pipeline. An
//! object whose store returns no ETag cannot be resumed mid-stream safely
//! and poisons the unit on a mid-object break instead.
//!
//! A failed precondition on a notification unit that names no version, or a
//! range the store cannot satisfy on one, is an overwrite
//! ([`is_overwrite`](crate::error::is_overwrite)). The fetcher
//! HEADs the key and reports its current content as
//! [`SplitFailure::Overwritten`], so the reader can re-enqueue the key.

use crate::error::classify;
use crate::lane_ctx::{LaneLabel, PoisonKind};
use crate::split::SplitRange;
use crate::trim::RangeTrim;
use crate::unit::{Origin, quoted};
use bytes::Bytes;
use futures_util::StreamExt as _;
use futures_util::future::{Either, select};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt as _};
use spate_core::error::{ErrorClass, SourceError};
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// One listed object, in a lane's slice order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectEntry {
    /// Full object key (an `object_store` path string).
    pub(crate) key: String,
    /// Object size in bytes, from the listing.
    pub(crate) size: u64,
    /// ETag from the listing, if the store reports one.
    pub(crate) etag: Option<String>,
    /// Last-modified time (ms since epoch), used as the records' event time.
    pub(crate) last_modified_ms: i64,
}

/// Messages a fetcher streams to its lane, strictly in stream order.
#[derive(Debug)]
pub(crate) enum ChunkMsg {
    /// The next object is starting.
    ObjectStart {
        /// Ordinal within the lane's slice.
        ordinal: u32,
        /// Object key.
        key: String,
        /// The object's last-modified time (ms since epoch).
        last_modified_ms: i64,
    },
    /// The next bytes of the current object (at most `chunk_bytes` each).
    Chunk(Bytes),
    /// The current object's bytes are complete.
    ObjectEnd,
    /// The lane cannot continue reading its split.
    LaneFailed(SplitFailure),
}

/// How a unit read failed. The classification decides whether one object
/// condemns the unit (poison, failed back to the feed) or the whole
/// pipeline (fatal).
#[derive(Debug)]
pub(crate) enum SplitFailure {
    /// Object-level failure: a member was deleted after listing, was
    /// overwritten under its ETag pin, is corrupt, or exhausted its read
    /// budget. The unit is failed back to the feed; it never fails the
    /// pipeline directly.
    Poison(PoisonKind, String),
    /// Instance- or configuration-level failure (credentials, permissions,
    /// client misconfiguration): terminal for the pipeline.
    Fatal(SourceError),
    /// A notification unit's key was overwritten under its ETag pin. The
    /// records framed before the failure stand, and the reader re-enqueues
    /// the key at the content the HEAD found.
    Overwritten(Box<Overwrite>),
}

impl std::fmt::Display for SplitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SplitFailure::Poison(_, reason) => write!(f, "split poisoned: {reason}"),
            SplitFailure::Fatal(e) => write!(f, "{e}"),
            SplitFailure::Overwritten(o) => write!(f, "\"{}\" was overwritten", o.key),
        }
    }
}

/// The content a key holds after an overwrite cut its read short, as a HEAD
/// reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Overwrite {
    /// The key read.
    pub(crate) key: String,
    /// The current ETag, quoted, or `None` when the store sent none.
    pub(crate) etag: Option<String>,
    /// The current size in bytes.
    pub(crate) size: u64,
    /// The current last-modified time (ms since epoch).
    pub(crate) last_modified_ms: i64,
}

/// Collect and sort the full listing under `prefix`.
#[cfg(test)]
pub(crate) async fn list_all(
    store: &Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
) -> Result<Vec<ObjectEntry>, object_store::Error> {
    let mut entries = Vec::new();
    let mut stream = store.list(prefix);
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        entries.push(ObjectEntry {
            key: meta.location.to_string(),
            size: meta.size,
            etag: meta.e_tag,
            last_modified_ms: meta.last_modified.timestamp_millis(),
        });
    }
    entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
    Ok(entries)
}

/// Everything one fetcher task needs.
pub(crate) struct FetcherParams {
    /// The split being read; names the work in failure reports and logs.
    pub(crate) split: LaneLabel,
    pub(crate) store: Arc<dyn ObjectStore>,
    /// The split's member objects, ordinal order.
    pub(crate) slice: Arc<Vec<ObjectEntry>>,
    /// First ordinal to fetch (the committed watermark's object).
    pub(crate) start_ordinal: u32,
    /// Committed ETag of the resume object, pinning its content.
    pub(crate) resume_etag: Option<String>,
    /// Upper bound on a single [`ChunkMsg::Chunk`].
    pub(crate) chunk_bytes: usize,
    /// Target size of a bounded ranged GET (`prefetch_bytes`); byte-range
    /// boundary windows can add `chunk_bytes`. Each GET is fully buffered
    /// before its chunks are handed on, and the next window's GET runs while
    /// they are.
    pub(crate) range_bytes: usize,
    pub(crate) tx: mpsc::Sender<ChunkMsg>,
    /// Backpressure pause (set by `Source::pause`): checked between sends.
    pub(crate) pause: Arc<AtomicBool>,
    /// First backoff step (doubles per attempt, capped; tests shrink it).
    pub(crate) retry_base: Duration,
    /// `spate_s3_source_get_retries_total`, when metrics are attached.
    pub(crate) retries: Option<spate_core::metrics::Counter>,
    /// The byte range of the slice's one member, for a ranged unit.
    pub(crate) range: Option<SplitRange>,
    /// `spate_s3_source_bytes_read_total`, counting the bytes a ranged read
    /// fetches and trims; the lane counts the bytes it forwards.
    pub(crate) bytes_read: Option<spate_core::metrics::Counter>,
    /// Where the split's unit came from: a notification may name a version
    /// to read, and a failed precondition on one is an overwrite.
    pub(crate) origin: Origin,
}

/// How many attempts one object GET gets before escalating. Failing fast (a
/// poisoned unit) beats a backfill wedged invisibly in an endless retry loop.
pub(crate) const MAX_ATTEMPTS: u32 = 8;
pub(crate) const BACKOFF_CAP: Duration = Duration::from_secs(5);
const PAUSE_POLL: Duration = Duration::from_millis(25);

/// Stream the split's member objects. Ends by dropping `tx` (the lane
/// observes a closed channel = end of input) or after a `LaneFailed`.
pub(crate) async fn run_fetcher(params: FetcherParams) {
    let FetcherParams {
        split,
        store,
        slice,
        start_ordinal,
        resume_etag,
        chunk_bytes,
        range_bytes,
        tx,
        pause,
        retry_base,
        retries,
        range,
        bytes_read,
        origin,
    } = params;
    let mut ahead = ReadAhead::default();

    for (ordinal, entry) in slice.iter().enumerate().skip(start_ordinal as usize) {
        // A unit holds at most `MAX_UNIT_MEMBERS` members, so ordinals always
        // fit the composite-offset layout.
        let ordinal = ordinal as u32;
        // Pin the GET to the exact content the offsets were (or will be)
        // minted against: the committed ETag for the resume object, the
        // listed ETag otherwise.
        let pinned_etag = if ordinal == start_ordinal && resume_etag.is_some() {
            resume_etag.clone()
        } else {
            entry.etag.clone()
        };
        let start = ChunkMsg::ObjectStart {
            ordinal,
            key: entry.key.clone(),
            last_modified_ms: entry.last_modified_ms,
        };
        if !ahead.alongside(gated_send(&pause, &tx, start)).await {
            return; // lane dropped: shutdown
        }
        // The next member's first window is read while this member is handed
        // on. A byte-range unit has one member.
        let next = match range {
            Some(_) => None,
            None => slice
                .iter()
                .skip(ordinal as usize + 1)
                .find(|later| later.size > 0)
                .and_then(|later| {
                    // A later member is never the resume object, so its own
                    // listed ETag pins it. One with no ETag takes the
                    // streaming path, whose body must not open early.
                    let etag = later.etag.clone()?;
                    let path = object_path(&split, later).ok()?;
                    Some(WindowRead {
                        split: Arc::clone(&split),
                        store: Arc::clone(&store),
                        path,
                        entry: later.clone(),
                        etag,
                        start: 0,
                        end: (range_bytes.max(1) as u64).min(later.size),
                        retry_base,
                        retries: retries.clone(),
                        origin: origin.clone(),
                    })
                }),
        };
        match stream_object(
            &split,
            &store,
            entry,
            pinned_etag.as_deref(),
            range,
            range_bytes,
            chunk_bytes,
            &tx,
            &pause,
            retry_base,
            retries.as_ref(),
            bytes_read.as_ref(),
            &origin,
            &mut ahead,
            next,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => return, // lane dropped: shutdown
            Err(e) => {
                let _ = tx.send(ChunkMsg::LaneFailed(e)).await;
                return;
            }
        }
        if ahead.alongside(tx.send(ChunkMsg::ObjectEnd)).await.is_err() {
            return;
        }
    }
    // Slice exhausted: dropping tx closes the channel, which is the lane's
    // end-of-slice signal.
}

/// Wait while paused. Fails when the lane is gone (channel closed), so a
/// paused fetcher still exits promptly on shutdown.
async fn pause_gate(pause: &AtomicBool, tx: &mpsc::Sender<ChunkMsg>) -> Result<(), ()> {
    while pause.load(Ordering::Relaxed) {
        if tx.is_closed() {
            return Err(());
        }
        tokio::time::sleep(PAUSE_POLL).await;
    }
    Ok(())
}

/// Wait out a pause, then send `msg`. `false` when the lane is gone.
async fn gated_send(pause: &AtomicBool, tx: &mpsc::Sender<ChunkMsg>, msg: ChunkMsg) -> bool {
    pause_gate(pause, tx).await.is_ok() && tx.send(msg).await.is_ok()
}

/// One window's bytes, or the failure that ended its read.
type Window = Result<Bytes, SplitFailure>;

/// One bounded ranged GET of an ETag-pinned object.
struct WindowRead {
    split: LaneLabel,
    store: Arc<dyn ObjectStore>,
    path: Path,
    entry: ObjectEntry,
    etag: String,
    start: u64,
    end: u64,
    retry_base: Duration,
    retries: Option<spate_core::metrics::Counter>,
    origin: Origin,
}

impl WindowRead {
    /// Read the window into memory, retrying a transient failure on a budget
    /// of its own.
    async fn fetch(self) -> Window {
        let mut attempt: u32 = 0;
        loop {
            let options = GetOptions {
                if_match: Some(self.etag.clone()),
                range: Some(GetRange::Bounded(self.start..self.end)),
                version: version_of(&self.origin),
                ..Default::default()
            };
            // The body is drained before the window is handed on, so the
            // connection is released and a back-pressured lane never leaves
            // an idle S3 body open.
            let fetched = match self.store.get_opts(&self.path, options).await {
                Ok(result) => result.bytes().await,
                Err(e) => Err(e),
            };
            match fetched {
                // A conforming store never returns an empty bounded range;
                // guard anyway so a misbehaving one poisons the object rather
                // than spinning.
                Ok(bytes) if bytes.is_empty() => {
                    return Err(SplitFailure::Poison(
                        PoisonKind::Undecodable,
                        format!(
                            "split {}: ranged read of \"{}\" at byte {} returned no bytes",
                            self.split, self.entry.key, self.start
                        ),
                    ));
                }
                Ok(bytes) => return Ok(bytes),
                Err(e) if crate::error::is_overwrite(&e, &self.origin) => {
                    return Err(head_after_overwrite(
                        &self.split,
                        &self.store,
                        &self.path,
                        &self.entry,
                        &self.etag,
                        self.start,
                        self.retry_base,
                        self.retries.as_ref(),
                    )
                    .await);
                }
                Err(e) => {
                    retry_or_fail(
                        &self.split,
                        &self.entry,
                        &e,
                        self.start,
                        &mut attempt,
                        self.retry_base,
                        self.retries.as_ref(),
                    )
                    .await?;
                }
            }
        }
    }
}

/// The one window a fetcher reads ahead: the next window of the object it is
/// handing on, or the first window of the unit's next member.
///
/// The read is a future of the fetcher's own task, polled only inside
/// [`alongside`](Self::alongside) and [`take`](Self::take). Dropping the
/// fetcher cancels it.
#[derive(Default)]
struct ReadAhead(Option<Ahead>);

enum Ahead {
    Reading(Pin<Box<dyn Future<Output = Window> + Send>>),
    Read(Window),
}

impl ReadAhead {
    /// Start reading `read`. At most one window is ahead at a time.
    fn start(&mut self, read: WindowRead) {
        debug_assert!(self.0.is_none(), "a second window read ahead");
        self.0 = Some(Ahead::Reading(Box::pin(read.fetch())));
    }

    /// Run `work` to completion while the window ahead keeps reading.
    async fn alongside<T>(&mut self, work: impl Future<Output = T>) -> T {
        let work = pin!(work);
        let Some(Ahead::Reading(reading)) = &mut self.0 else {
            return work.await;
        };
        match select(work, reading.as_mut()).await {
            Either::Left((out, _)) => out,
            Either::Right((window, work)) => {
                self.0 = Some(Ahead::Read(window));
                work.await
            }
        }
    }

    /// The window read ahead, awaited to the end of its read, or `None` when
    /// none was started.
    async fn take(&mut self) -> Option<Window> {
        Some(match self.0.take()? {
            Ahead::Reading(reading) => reading.await,
            Ahead::Read(window) => window,
        })
    }
}

/// Stream one object's bytes as bounded [`ChunkMsg::Chunk`]s. `Ok(true)` =
/// complete, `Ok(false)` = receiver dropped (shutdown), `Err` = terminal
/// failure.
///
/// When the object's content is pinned by an ETag (S3 always supplies one, as
/// does `object_store`'s `LocalFileSystem`) the read is a sequence of **bounded
/// ranged GETs, each drained fully into memory before any of its chunks is
/// forwarded** ([`stream_object_ranged`]), so a lane parked on backpressure
/// only holds buffered bytes, never an idle GET body that S3 (or an
/// intermediary) could reset or time out mid-stream. Without an ETag a resumed
/// ranged read could splice two object versions, so such stores keep a single
/// continuous stream ([`stream_object_streaming`]).
///
/// `ahead` holds this object's first window when it was read ahead, and `next`
/// is the next member's first window, read while this object's last window is
/// handed on.
#[expect(
    clippy::too_many_arguments,
    reason = "internal seam between run_fetcher and the read loops"
)]
async fn stream_object(
    split: &LaneLabel,
    store: &Arc<dyn ObjectStore>,
    entry: &ObjectEntry,
    pinned_etag: Option<&str>,
    range: Option<SplitRange>,
    range_bytes: usize,
    chunk_bytes: usize,
    tx: &mpsc::Sender<ChunkMsg>,
    pause: &AtomicBool,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
    bytes_read: Option<&spate_core::metrics::Counter>,
    origin: &Origin,
    ahead: &mut ReadAhead,
    next: Option<WindowRead>,
) -> Result<bool, SplitFailure> {
    // A whole object with no bytes is read without a request, so its key
    // need not be one the store can address.
    if entry.size == 0 && range.is_none() {
        return Ok(true);
    }
    match pinned_etag {
        Some(etag) => {
            stream_object_ranged(
                split,
                store,
                entry,
                etag,
                range.map(|range| RangeTrim::new(range, entry.size)),
                range_bytes,
                chunk_bytes,
                tx,
                pause,
                retry_base,
                retries,
                bytes_read,
                origin,
                ahead,
                next,
            )
            .await
        }
        None if range.is_some() => Err(SplitFailure::Fatal(SourceError::Client {
            class: ErrorClass::Fatal,
            reason: format!(
                "split {split}: ranged read of \"{}\" has no ETag pin — internal wiring bug",
                entry.key
            ),
        })),
        None => {
            stream_object_streaming(
                split,
                store,
                entry,
                chunk_bytes,
                tx,
                pause,
                retry_base,
                retries,
                origin,
            )
            .await
        }
    }
}

/// Pinned read path: walk the object in `range_bytes` windows, each a
/// short-lived ranged GET fully buffered before its hand-off. While one window
/// is handed on, the next is read: the object's next window, or `next` after
/// its last. Queued chunks can retain the previous window's buffer, so a lane
/// holds up to three windows. The ETag pin keeps the multi-GET read
/// splice-safe: an overwrite between windows trips the `if_match` precondition.
///
/// A window read ahead that fails is reported only after the window before it
/// is handed on, so the lane sees the same messages as if it had been read
/// later.
///
/// With a `trim`, the windows follow [`RangeTrim::next_window`] and only the
/// owned bytes are forwarded. A window reaching the range's end may run
/// `chunk_bytes` past `range_bytes`.
#[expect(
    clippy::too_many_arguments,
    reason = "internal read loop, mirrors stream_object_streaming"
)]
async fn stream_object_ranged(
    split: &LaneLabel,
    store: &Arc<dyn ObjectStore>,
    entry: &ObjectEntry,
    etag: &str,
    mut trim: Option<RangeTrim>,
    range_bytes: usize,
    chunk_bytes: usize,
    tx: &mpsc::Sender<ChunkMsg>,
    pause: &AtomicBool,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
    bytes_read: Option<&spate_core::metrics::Counter>,
    origin: &Origin,
    ahead: &mut ReadAhead,
    mut next: Option<WindowRead>,
) -> Result<bool, SplitFailure> {
    let path = object_path(split, entry)?;
    let window = range_bytes.max(1) as u64;
    // The end of the window starting at `at`, or `None` past the last one.
    // Saturating: `entry.size` is remote listing data and may be
    // adversarially close to u64::MAX.
    let window_end = |trim: &Option<RangeTrim>, at: u64| match trim {
        Some(trim) => trim.next_window(at, window, chunk_bytes as u64),
        None if at < entry.size => Some(at.saturating_add(window).min(entry.size)),
        None => None,
    };
    let read_at = |start: u64, end: u64| WindowRead {
        split: Arc::clone(split),
        store: Arc::clone(store),
        path: path.clone(),
        entry: entry.clone(),
        etag: etag.to_owned(),
        start,
        end,
        retry_base,
        retries: retries.cloned(),
        origin: origin.clone(),
    };
    // `delivered` is the GET position of the window in hand.
    let mut delivered: u64 = trim.as_ref().map_or(0, RangeTrim::first_byte);
    let mut buffered = match ahead.take().await {
        Some(window) => window?,
        None => match window_end(&trim, delivered) {
            Some(end) => read_at(delivered, end).fetch().await?,
            None => return Ok(true),
        },
    };

    loop {
        let read = buffered.len() as u64;
        let mut bytes = match &mut trim {
            Some(trim) => {
                let owned = trim.feed(delivered, buffered);
                if let Some(counter) = bytes_read {
                    counter.increment(read - owned.len() as u64);
                }
                owned
            }
            None => buffered,
        };
        // The trim is fed before the next window is planned: its end depends
        // on whether this window reached the owned range's end.
        let following = delivered + read;
        let own_next = window_end(&trim, following);
        match own_next {
            Some(end) => ahead.start(read_at(following, end)),
            None => {
                if let Some(read) = next.take() {
                    ahead.start(read);
                }
            }
        }
        while !bytes.is_empty() {
            let take = bytes.len().min(chunk_bytes);
            let chunk = bytes.split_to(take);
            if !ahead
                .alongside(gated_send(pause, tx, ChunkMsg::Chunk(chunk)))
                .await
            {
                return Ok(false);
            }
        }
        if own_next.is_none() {
            return Ok(true);
        }
        delivered = following;
        let Some(window) = ahead.take().await else {
            unreachable!("the object's next window was started above");
        };
        buffered = window?;
    }
}

/// Unpinned read path (store reports no ETag): a single continuous stream. A
/// resumed ranged GET across a failure could splice two object versions when
/// the content is not pinned, so a mid-object break is fatal rather than
/// resumed. This holds the body open across the hand-off, and is the
/// fallback for stores that cannot be safely range-resumed; S3 and
/// `LocalFileSystem` always report an ETag and take the ranged path.
#[expect(
    clippy::too_many_arguments,
    reason = "internal read loop, mirrors stream_object_ranged"
)]
async fn stream_object_streaming(
    split: &LaneLabel,
    store: &Arc<dyn ObjectStore>,
    entry: &ObjectEntry,
    chunk_bytes: usize,
    tx: &mpsc::Sender<ChunkMsg>,
    pause: &AtomicBool,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
    origin: &Origin,
) -> Result<bool, SplitFailure> {
    let path = object_path(split, entry)?;
    let mut delivered: u64 = 0;
    let mut attempt: u32 = 0;

    'attempts: loop {
        // In this fallback `delivered` is always 0: a mid-object stream error
        // is fatal below (no ETag to pin a resumed read), so a retry never
        // resumes partway, and every GET re-reads the whole object un-ranged.
        let options = GetOptions {
            version: version_of(origin),
            ..Default::default()
        };
        let result = match store.get_opts(&path, options).await {
            Ok(r) => r,
            Err(e) => {
                retry_or_fail(
                    split,
                    entry,
                    &e,
                    delivered,
                    &mut attempt,
                    retry_base,
                    retries,
                )
                .await?;
                continue 'attempts;
            }
        };
        let mut stream = result.into_stream();
        loop {
            match stream.next().await {
                Some(Ok(mut bytes)) => {
                    delivered += bytes.len() as u64;
                    while !bytes.is_empty() {
                        let take = bytes.len().min(chunk_bytes);
                        let chunk = bytes.split_to(take);
                        if pause_gate(pause, tx).await.is_err() {
                            return Ok(false);
                        }
                        if tx.send(ChunkMsg::Chunk(chunk)).await.is_err() {
                            return Ok(false);
                        }
                    }
                    // Progress resets the attempt budget: an object larger
                    // than one failure-free window still completes.
                    attempt = 0;
                }
                Some(Err(e)) => {
                    if delivered > 0 {
                        // Scope decides, same as `retry_or_fail`: a
                        // credentials/permission/config failure holds for
                        // every object on every instance, so it is
                        // pipeline-fatal even mid-stream. Everything else
                        // stays object-level: without an ETag a resumed read
                        // could splice two object versions, so a mid-object
                        // break cannot be retried by this owner. Poison.
                        if crate::error::is_pipeline_fatal(&e) {
                            return Err(SplitFailure::Fatal(SourceError::Client {
                                class: ErrorClass::Fatal,
                                reason: format!(
                                    "split {split}: reading \"{}\" failed mid-object at byte \
                                     {delivered}: {}",
                                    entry.key,
                                    crate::error::bounded_reason(&e)
                                ),
                            }));
                        }
                        return Err(SplitFailure::Poison(
                            PoisonKind::Undecodable,
                            format!(
                                "split {split}: read of \"{}\" failed mid-object at byte \
                                 {delivered} and the store reports no ETag to pin a \
                                 resumed read to: {}",
                                entry.key,
                                crate::error::bounded_reason(&e)
                            ),
                        ));
                    }
                    retry_or_fail(
                        split,
                        entry,
                        &e,
                        delivered,
                        &mut attempt,
                        retry_base,
                        retries,
                    )
                    .await?;
                    continue 'attempts;
                }
                None => return Ok(true),
            }
        }
    }
}

/// The object version a part from `origin` reads, `None` for the key.
fn version_of(origin: &Origin) -> Option<String> {
    match origin {
        Origin::Notification { version, .. } => version.clone(),
        Origin::Lister => None,
    }
}

/// HEAD `path` after the store refused a read at byte `delivered` pinned to
/// `pinned` (see [`is_overwrite`](crate::error::is_overwrite)), and report
/// what the key holds now.
///
/// A HEAD answering the pinned ETag poisons the unit as `EtagDrift`, since
/// re-enqueueing at the same content would fail the same way. Any other
/// answer, an absent ETag included, is [`SplitFailure::Overwritten`]. A HEAD
/// that fails is classified as a read is: 404 poisons, 401 and 403 are
/// fatal, and a transient failure is retried on the read's budget. Its
/// reason says the HEAD failed.
#[expect(
    clippy::too_many_arguments,
    reason = "internal seam beside the read loops, which carry the same context"
)]
async fn head_after_overwrite(
    split: &LaneLabel,
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    entry: &ObjectEntry,
    pinned: &str,
    delivered: u64,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
) -> SplitFailure {
    let mut attempt = 0;
    loop {
        match store.head(path).await {
            Ok(meta) => {
                let etag = meta.e_tag.map(quoted);
                if etag.as_deref() == Some(pinned) {
                    return SplitFailure::Poison(
                        PoisonKind::EtagDrift,
                        format!(
                            "split {split}: the store refused reading \"{}\" at byte \
                             {delivered} under its ETag pin, and the object still reports the \
                             ETag {pinned}",
                            entry.key
                        ),
                    );
                }
                return SplitFailure::Overwritten(Box::new(Overwrite {
                    key: entry.key.clone(),
                    etag,
                    size: meta.size,
                    last_modified_ms: meta.last_modified.timestamp_millis(),
                }));
            }
            Err(e) => {
                if let Err(failure) = retry_or_fail(
                    split,
                    entry,
                    &e,
                    delivered,
                    &mut attempt,
                    retry_base,
                    retries,
                )
                .await
                {
                    let headed = |reason: String| {
                        format!("{reason} (the HEAD after the store refused the pinned read)")
                    };
                    return match failure {
                        SplitFailure::Poison(kind, reason) => {
                            SplitFailure::Poison(kind, headed(reason))
                        }
                        SplitFailure::Fatal(SourceError::Client { class, reason }) => {
                            SplitFailure::Fatal(SourceError::Client {
                                class,
                                reason: headed(reason),
                            })
                        }
                        other => other,
                    };
                }
            }
        }
    }
}

/// The store path of a listed object, under the exact key the listing gave.
///
/// `Path::from` would read the key as unencoded parts and percent-encode `%`,
/// `#`, `[`, non-ASCII and other reserved bytes, addressing a different
/// object. A key that is not [`addressable`](crate::listing::addressable)
/// cannot be read, or would be read under another key, and poisons the unit.
fn object_path(split: &LaneLabel, entry: &ObjectEntry) -> Result<Path, SplitFailure> {
    match Path::parse(&entry.key) {
        Ok(path) if path.as_ref() == entry.key => Ok(path),
        parsed => {
            let why = parsed.map_or_else(
                |e| e.to_string(),
                |path| format!("the object store would read \"{path}\" in its place"),
            );
            Err(SplitFailure::Poison(
                PoisonKind::NotFound,
                format!(
                    "split {split}: \"{}\" is not an object key: {why}",
                    entry.key
                ),
            ))
        }
    }
}

/// Back off and bump the attempt counter for a retryable error; escalate
/// non-retryable classes and exhausted budgets.
///
/// Escalation splits by scope: credentials/config failures are pipeline
/// [`Fatal`](SplitFailure::Fatal). Everything else is object-level
/// [`Poison`](SplitFailure::Poison), handled by handing the split back: a
/// deleted or overwritten object (`NotFound`, a failed `if_match`
/// precondition), or a read that keeps failing past the attempt budget.
async fn retry_or_fail(
    split: &LaneLabel,
    entry: &ObjectEntry,
    e: &object_store::Error,
    delivered: u64,
    attempt: &mut u32,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
) -> Result<(), SplitFailure> {
    if classify(e) != ErrorClass::Retryable {
        let reason = format!(
            "split {split}: reading \"{}\" failed at byte {delivered}: {}",
            entry.key,
            crate::error::bounded_reason(e)
        );
        return Err(if crate::error::is_pipeline_fatal(e) {
            SplitFailure::Fatal(SourceError::Client {
                class: ErrorClass::Fatal,
                reason,
            })
        } else {
            SplitFailure::Poison(crate::error::poison_kind(e), reason)
        });
    }
    *attempt += 1;
    if let Some(c) = retries {
        c.increment(1);
    }
    if *attempt >= MAX_ATTEMPTS {
        return Err(SplitFailure::Poison(
            PoisonKind::RetriesExhausted,
            format!(
                "split {split}: reading \"{}\" still failing at byte {delivered} after \
             {MAX_ATTEMPTS} attempts: {}",
                entry.key,
                crate::error::bounded_reason(e)
            ),
        ));
    }
    let backoff = retry_base
        .saturating_mul(1 << (*attempt - 1).min(16))
        .min(BACKOFF_CAP);
    tracing::warn!(
        split = %split,
        key = %entry.key,
        attempt = *attempt,
        delivered,
        error = %crate::error::bounded_reason(e),
        "transient object read failure; backing off and resuming with a ranged get"
    );
    tokio::time::sleep(backoff).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{GetResult, GetResultPayload, PutPayload};
    use std::fmt;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc as std_mpsc;

    fn entry(key: &str, size: u64, etag: Option<&str>) -> ObjectEntry {
        ObjectEntry {
            key: key.into(),
            size,
            etag: etag.map(str::to_owned),
            last_modified_ms: 0,
        }
    }

    // ---------------------------------------------------- fetcher tests --

    /// The shape of a `GetOptions::range`, recorded so tests can assert which
    /// read path ran: the pinned path issues `Bounded` windows, the streaming
    /// fallback a single un-ranged (`Full`) or `Offset` resume GET.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RangeKind {
        Full,
        Offset(u64),
        Bounded(u64, u64),
    }

    fn range_kind(range: &Option<GetRange>) -> RangeKind {
        match range {
            None => RangeKind::Full,
            Some(GetRange::Offset(o)) => RangeKind::Offset(*o),
            Some(GetRange::Bounded(r)) => RangeKind::Bounded(r.start, r.end),
            Some(GetRange::Suffix(_)) => unreachable!("the fetcher never issues a suffix range"),
        }
    }

    /// Wraps a store, failing the first `fail_gets` `get_opts` calls with a
    /// retryable error, failing the first `fail_past_gets` bounded GETs that
    /// end past byte `fail_past` the same way, and cutting the first
    /// `cut_streams` result streams after `cut_after` bytes (with a retryable
    /// error, or `PermissionDenied` when `cut_fatal` is set). `error_text`,
    /// when set, replaces the injected errors' text. Records every requested
    /// range in `ranges` for read-path assertions.
    #[derive(Debug)]
    struct FlakyStore {
        inner: InMemory,
        fail_gets: AtomicU32,
        fail_past: u64,
        fail_past_gets: AtomicU32,
        cut_streams: AtomicU32,
        cut_after: usize,
        cut_fatal: bool,
        error_text: Option<String>,
        gets: AtomicU32,
        ranges: std::sync::Mutex<Vec<RangeKind>>,
        /// Payload streams drained to completion (a cut or abandoned body
        /// never counts). This is the observable that separates "window
        /// fully buffered before hand-off" from "connection held open".
        bodies_drained: Arc<AtomicU32>,
        /// Each GET's key and `If-Match`, in request order.
        requests: std::sync::Mutex<Vec<(String, Option<String>)>>,
        /// The GET, counted from 1, that waits for a permit on `release`
        /// before it reads; 0 holds none.
        hold_on: u32,
        release: Arc<tokio::sync::Semaphore>,
        /// The GET, counted from 1, answered with an empty body; 0 answers
        /// none.
        empty_on: u32,
        /// Held GETs started, and held GET futures dropped, finished or not.
        held: AtomicU32,
        held_dropped: Arc<AtomicU32>,
        /// Window buffers alive now and the most alive at once, counted when
        /// `count_live` is set.
        count_live: bool,
        live: Arc<AtomicU32>,
        max_live: Arc<AtomicU32>,
    }

    /// Counts its own drop.
    struct DropCount(Arc<AtomicU32>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A window's bytes, counted alive until the last chunk sharing them is
    /// dropped.
    struct LiveWindow {
        bytes: Bytes,
        live: Arc<AtomicU32>,
    }

    impl AsRef<[u8]> for LiveWindow {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for LiveWindow {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::Relaxed);
        }
    }

    impl FlakyStore {
        fn new(inner: InMemory) -> FlakyStore {
            FlakyStore {
                inner,
                fail_gets: AtomicU32::new(0),
                fail_past: u64::MAX,
                fail_past_gets: AtomicU32::new(0),
                cut_streams: AtomicU32::new(0),
                cut_after: 0,
                cut_fatal: false,
                error_text: None,
                gets: AtomicU32::new(0),
                ranges: std::sync::Mutex::new(Vec::new()),
                bodies_drained: Arc::new(AtomicU32::new(0)),
                requests: std::sync::Mutex::new(Vec::new()),
                hold_on: 0,
                empty_on: 0,
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                held: AtomicU32::new(0),
                held_dropped: Arc::new(AtomicU32::new(0)),
                count_live: false,
                live: Arc::new(AtomicU32::new(0)),
                max_live: Arc::new(AtomicU32::new(0)),
            }
        }

        /// Each GET's key and `If-Match`, in request order.
        fn requests(&self) -> Vec<(String, Option<String>)> {
            self.requests.lock().unwrap().clone()
        }

        fn bodies_drained(&self) -> u32 {
            self.bodies_drained.load(Ordering::Relaxed)
        }

        /// Chain an end-of-stream marker onto the payload: it fires only
        /// when the body is polled all the way to its natural end.
        fn track_drain(&self, result: GetResult) -> GetResult {
            let GetResult {
                payload,
                meta,
                range,
                attributes,
                extensions,
            } = result;
            let stream = match payload {
                GetResultPayload::Stream(s) => s,
                #[allow(unreachable_patterns)]
                _ => unreachable!("InMemory yields streams"),
            };
            let stream = if self.count_live {
                let live = Arc::clone(&self.live);
                let max_live = Arc::clone(&self.max_live);
                stream
                    .map(move |item| {
                        item.map(|bytes| {
                            let now = live.fetch_add(1, Ordering::Relaxed) + 1;
                            max_live.fetch_max(now, Ordering::Relaxed);
                            Bytes::from_owner(LiveWindow {
                                bytes,
                                live: Arc::clone(&live),
                            })
                        })
                    })
                    .boxed()
            } else {
                stream
            };
            let drained = Arc::clone(&self.bodies_drained);
            let marker = futures_util::stream::poll_fn(move |_| {
                drained.fetch_add(1, Ordering::Relaxed);
                std::task::Poll::Ready(None::<object_store::Result<Bytes>>)
            });
            GetResult {
                payload: GetResultPayload::Stream(Box::pin(stream.chain(marker))),
                meta,
                range,
                attributes,
                extensions,
            }
        }

        fn recorded_ranges(&self) -> Vec<RangeKind> {
            self.ranges.lock().unwrap().clone()
        }

        fn generic(what: &str) -> object_store::Error {
            object_store::Error::Generic {
                store: "flaky",
                source: what.to_owned().into(),
            }
        }

        fn cut_error(fatal: bool, text: Option<&str>) -> object_store::Error {
            if fatal {
                object_store::Error::PermissionDenied {
                    path: "k".into(),
                    source: text.unwrap_or("injected permission loss").to_owned().into(),
                }
            } else {
                Self::generic(text.unwrap_or("injected stream cut"))
            }
        }
    }

    impl fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FlakyStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
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
            let nth = self.gets.fetch_add(1, Ordering::Relaxed) + 1;
            self.ranges.lock().unwrap().push(range_kind(&options.range));
            self.requests
                .lock()
                .unwrap()
                .push((location.to_string(), options.if_match.clone()));
            if nth == self.hold_on {
                self.held.fetch_add(1, Ordering::Relaxed);
                let _dropped = DropCount(Arc::clone(&self.held_dropped));
                self.release.acquire().await.unwrap().forget();
            }
            if self
                .fail_gets
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(Self::generic(
                    self.error_text.as_deref().unwrap_or("injected get failure"),
                ));
            }
            if let Some(GetRange::Bounded(r)) = &options.range
                && r.end > self.fail_past
                && self
                    .fail_past_gets
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                    .is_ok()
            {
                return Err(Self::generic(
                    self.error_text
                        .as_deref()
                        .unwrap_or("injected get failure past the range end"),
                ));
            }
            let result = self.inner.get_opts(location, options).await?;
            if nth == self.empty_on {
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(Box::pin(futures_util::stream::empty())),
                    ..result
                });
            }
            if self
                .cut_streams
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                let cut_after = self.cut_after;
                let cut_fatal = self.cut_fatal;
                let error_text = self.error_text.clone();
                let GetResult {
                    payload,
                    meta,
                    range,
                    attributes,
                    extensions,
                } = result;
                let inner_stream = match payload {
                    GetResultPayload::Stream(s) => s,
                    #[allow(unreachable_patterns)]
                    _ => unreachable!("InMemory yields streams"),
                };
                let mut sent = 0usize;
                let cut = inner_stream.flat_map(move |item| {
                    let out: Vec<object_store::Result<Bytes>> = match item {
                        Ok(bytes) => {
                            if sent >= cut_after {
                                vec![Err(Self::cut_error(cut_fatal, error_text.as_deref()))]
                            } else {
                                let take = bytes.len().min(cut_after - sent);
                                sent += take;
                                let mut out = vec![Ok(bytes.slice(0..take))];
                                if sent >= cut_after {
                                    out.push(Err(Self::cut_error(
                                        cut_fatal,
                                        error_text.as_deref(),
                                    )));
                                }
                                out
                            }
                        }
                        Err(e) => vec![Err(e)],
                    };
                    futures_util::stream::iter(out)
                });
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(Box::pin(cut)),
                    meta,
                    range,
                    attributes,
                    extensions,
                });
            }
            Ok(self.track_drain(result))
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
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

    async fn seeded(objects: &[(&str, &[u8])]) -> InMemory {
        let store = InMemory::new();
        for (key, body) in objects {
            store
                .put(&Path::from(*key), PutPayload::from(body.to_vec()))
                .await
                .unwrap();
        }
        store
    }

    async fn listed(store: &Arc<dyn ObjectStore>) -> Vec<ObjectEntry> {
        list_all(store, None).await.unwrap()
    }

    /// Drive a fetcher over `slice` and collect its message stream.
    async fn collect_fetch(
        store: Arc<dyn ObjectStore>,
        slice: Vec<ObjectEntry>,
        start_ordinal: u32,
        chunk_bytes: usize,
        range_bytes: usize,
    ) -> Vec<ChunkMsg> {
        let (tx, mut rx) = mpsc::channel(64);
        let params = FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal,
            resume_etag: None,
            chunk_bytes,
            range_bytes,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: None,
            bytes_read: None,
            origin: Origin::Lister,
        };
        let task = tokio::spawn(run_fetcher(params));
        let mut msgs = Vec::new();
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        task.await.unwrap();
        msgs
    }

    fn assembled(msgs: &[ChunkMsg]) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        for m in msgs {
            match m {
                ChunkMsg::ObjectStart { key, .. } => out.push((key.clone(), Vec::new())),
                ChunkMsg::Chunk(b) => out.last_mut().unwrap().1.extend_from_slice(b),
                ChunkMsg::ObjectEnd => {}
                ChunkMsg::LaneFailed(e) => panic!("lane failed: {e}"),
            }
        }
        out
    }

    /// Listed keys a read must address verbatim. `Path::from` percent-encodes
    /// every one except `dir/a b.ndjson`.
    const ENCODED_KEYS: [&str; 4] = [
        "dir/a b.ndjson",
        "dir/a%20b.ndjson",
        "dir/x#y[1].ndjson",
        "dir/\u{e9}.ndjson",
    ];

    async fn seeded_verbatim(keys: &[&str]) -> Arc<dyn ObjectStore> {
        let store = InMemory::new();
        for key in keys {
            store
                .put(
                    &Path::parse(*key).unwrap(),
                    PutPayload::from(key.as_bytes().to_vec()),
                )
                .await
                .unwrap();
        }
        Arc::new(store)
    }

    /// Each key with its body (the key's bytes), sorted as the listing is.
    fn verbatim_bodies() -> Vec<(String, Vec<u8>)> {
        let mut want: Vec<_> = ENCODED_KEYS
            .iter()
            .map(|k| ((*k).to_owned(), k.as_bytes().to_vec()))
            .collect();
        want.sort();
        want
    }

    /// Pins that the fetcher starts the next member's GET while its lane has
    /// not read the previous member.
    #[tokio::test(start_paused = true)]
    async fn the_next_members_get_starts_before_the_lane_reads() {
        let store = Arc::new(FlakyStore::new(
            seeded(&[("a.ndjson", b"a\n"), ("b.ndjson", b"b\n")]).await,
        ));
        let slice = listed(&(Arc::clone(&store) as Arc<dyn ObjectStore>)).await;
        assert!(slice.iter().all(|e| e.etag.is_some()), "the ranged path");
        let (tx, _rx) = mpsc::channel(crate::lane_ctx::LANE_HANDOFF_CHUNKS);
        let params = FetcherParams {
            split: "s3-test".into(),
            store: Arc::clone(&store) as Arc<dyn ObjectStore>,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: None,
            bytes_read: None,
            origin: Origin::Lister,
        };
        let task = tokio::spawn(run_fetcher(params));
        park().await;
        assert_eq!(
            store.gets.load(Ordering::Relaxed),
            2,
            "the second member's GET started with nothing read"
        );
        task.abort();
    }

    /// A ranged read fetches each listed object under the key the listing
    /// gave, whatever characters it holds. Regression for #1081.
    #[tokio::test]
    async fn ranged_read_fetches_the_listed_key_verbatim() {
        let store = seeded_verbatim(&ENCODED_KEYS).await;
        let slice = listed(&store).await;
        assert!(slice.iter().all(|e| e.etag.is_some()));
        let got = assembled(&collect_fetch(store, slice, 0, 4, 64).await);
        assert_eq!(got, verbatim_bodies());
    }

    /// The no-ETag streaming read fetches each listed object under the key
    /// the listing gave. Regression for #1081.
    #[tokio::test]
    async fn streaming_read_fetches_the_listed_key_verbatim() {
        let store = seeded_verbatim(&ENCODED_KEYS).await;
        let mut slice = listed(&store).await;
        for entry in &mut slice {
            entry.etag = None;
        }
        let got = assembled(&collect_fetch(store, slice, 0, 4, 64).await);
        assert_eq!(got, verbatim_bodies());
    }

    /// A key that is not a valid store path poisons the unit as `NotFound`
    /// on both read paths, before any GET, and does not fail the pipeline.
    #[tokio::test]
    async fn an_unparseable_key_poisons_the_split() {
        for etag in [Some("\"e\""), None] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let slice = vec![entry("dir//x.ndjson", 1, etag)];
            let msgs = collect_fetch(store, slice, 0, 4, 64).await;
            let poison = msgs.iter().find_map(|m| match m {
                ChunkMsg::LaneFailed(SplitFailure::Poison(kind, reason)) => {
                    Some((*kind, reason.clone()))
                }
                _ => None,
            });
            let (kind, reason) = poison.expect("an unparseable key is split poison");
            assert_eq!(kind, PoisonKind::NotFound);
            assert!(
                reason.contains("\"dir//x.ndjson\" is not an object key"),
                "{reason}"
            );
        }
    }

    /// A key that parses to a different key poisons the unit rather than
    /// reading the object the parsed key names. Regression for #1116.
    #[tokio::test]
    async fn a_key_the_store_would_rewrite_poisons_the_split() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"other\n")]).await);
        let msgs = collect_fetch(store, vec![entry("/p/a", 6, None)], 0, 4, 64).await;
        assert!(
            !msgs.iter().any(|m| matches!(m, ChunkMsg::Chunk(_))),
            "no bytes of p/a are read for /p/a"
        );
        let reason = msgs.iter().find_map(|m| match m {
            ChunkMsg::LaneFailed(SplitFailure::Poison(PoisonKind::NotFound, reason)) => {
                Some(reason.clone())
            }
            _ => None,
        });
        let reason = reason.expect("a rewritten key is not_found split poison");
        assert!(reason.contains("\"/p/a\" is not an object key"), "{reason}");
        assert!(reason.contains("\"p/a\" in its place"), "{reason}");
    }

    #[tokio::test]
    async fn streams_objects_in_order_with_bounded_chunks() {
        let store: Arc<dyn ObjectStore> =
            Arc::new(seeded(&[("p/a", b"aaaaaaaaaa"), ("p/b", b"bb")]).await);
        let slice = listed(&store).await;
        let msgs = collect_fetch(store, slice, 0, 4, 64).await;
        for m in &msgs {
            if let ChunkMsg::Chunk(b) = m {
                assert!(b.len() <= 4, "chunk over the bound: {}", b.len());
            }
        }
        assert_eq!(
            assembled(&msgs),
            vec![
                ("p/a".to_string(), b"aaaaaaaaaa".to_vec()),
                ("p/b".to_string(), b"bb".to_vec())
            ]
        );
    }

    #[tokio::test]
    async fn start_ordinal_skips_committed_objects() {
        let flaky = Arc::new(FlakyStore::new(
            seeded(&[("p/a", b"first"), ("p/b", b"second")]).await,
        ));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        let msgs = collect_fetch(store, slice, 1, 64, 64).await;
        assert_eq!(
            assembled(&msgs),
            vec![("p/b".to_string(), b"second".to_vec())]
        );
        let keys: Vec<String> = flaky.requests().into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["p/b"], "a skipped member is never read");
    }

    #[tokio::test]
    async fn transient_get_failures_are_retried() {
        let flaky = FlakyStore::new(seeded(&[("p/a", b"payload")]).await);
        flaky.fail_gets.store(2, Ordering::Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(flaky);
        let slice = listed(&store).await;
        let msgs = collect_fetch(store, slice, 0, 64, 64).await;
        assert_eq!(
            assembled(&msgs),
            vec![("p/a".to_string(), b"payload".to_vec())]
        );
    }

    #[tokio::test]
    async fn mid_stream_cut_resumes_with_a_ranged_get_without_gap_or_dup() {
        let body = b"0123456789abcdefghij";
        let flaky = FlakyStore {
            cut_after: 7,
            ..FlakyStore::new(seeded(&[("p/a", body)]).await)
        };
        flaky.cut_streams.store(1, Ordering::Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(flaky);
        let slice = listed(&store).await;
        // range_bytes 8 → the 20-byte object spans three windows; the cut lands
        // inside the first, which is re-read whole (nothing was forwarded yet).
        let msgs = collect_fetch(store, slice, 0, 4, 8).await;
        assert_eq!(
            assembled(&msgs),
            vec![("p/a".to_string(), body.to_vec())],
            "a window that fails mid-read is retried cleanly, without gap or dup"
        );
    }

    #[tokio::test]
    async fn missing_object_poisons_the_split() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"x")]).await);
        // A slice naming a key that does not exist (deleted after planning);
        // an ETag from the stale listing routes it through the ranged path.
        let slice = vec![entry("p/ghost", 1, Some("\"e\""))];
        let (tx, mut rx) = mpsc::channel(8);
        let params = FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: None,
            bytes_read: None,
            origin: Origin::Lister,
        };
        tokio::spawn(run_fetcher(params));
        let mut saw_poison = false;
        while let Some(m) = rx.recv().await {
            if let ChunkMsg::LaneFailed(failure) = m {
                let SplitFailure::Poison(kind, reason) = failure else {
                    panic!("a missing object is split poison, not pipeline-fatal: {failure}");
                };
                assert_eq!(kind, PoisonKind::NotFound);
                assert!(reason.contains("p/ghost"), "{reason}");
                saw_poison = true;
            }
        }
        assert!(saw_poison);
    }

    #[tokio::test]
    async fn stale_etag_pin_poisons_the_split() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"new content")]).await);
        let mut slice = listed(&store).await;
        slice[0].etag = Some("\"stale\"".into());
        let (tx, mut rx) = mpsc::channel(8);
        let params = FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: None,
            bytes_read: None,
            origin: Origin::Lister,
        };
        tokio::spawn(run_fetcher(params));
        let mut saw_poison = false;
        while let Some(m) = rx.recv().await {
            if let ChunkMsg::LaneFailed(failure) = m {
                assert!(
                    matches!(failure, SplitFailure::Poison(PoisonKind::EtagDrift, _)),
                    "an overwrite under the pin is split poison: {failure}"
                );
                saw_poison = true;
            }
        }
        assert!(saw_poison, "a failed precondition must poison the split");
    }

    #[tokio::test(start_paused = true)]
    async fn pause_halts_fetching_and_resume_continues() {
        let flaky = Arc::new(FlakyStore::new(seeded(&[("p/a", b"abcdefgh")]).await));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(1); // tiny: sends interleave with pauses
        let pause = Arc::new(AtomicBool::new(true));
        let params = FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 2,
            range_bytes: 64,
            tx,
            pause: Arc::clone(&pause),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: None,
            bytes_read: None,
            origin: Origin::Lister,
        };
        tokio::spawn(run_fetcher(params));
        // Paused before the first send: nothing arrives. On paused time the
        // sleep returns only once the fetcher is parked.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            rx.try_recv().is_err(),
            "paused fetcher must not deliver messages"
        );
        assert_eq!(flaky.gets.load(Ordering::Relaxed), 0, "nor read ahead");
        pause.store(false, Ordering::Relaxed);
        let mut msgs = Vec::new();
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        assert_eq!(
            assembled(&msgs),
            vec![("p/a".to_string(), b"abcdefgh".to_vec())]
        );
    }

    #[tokio::test]
    async fn dropping_the_lane_ends_the_fetcher() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", &[b'x'; 4096][..])]).await);
        let slice = listed(&store).await;
        let (tx, rx) = mpsc::channel(1);
        let params = FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 8,
            range_bytes: 4096,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: None,
            bytes_read: None,
            origin: Origin::Lister,
        };
        let task = tokio::spawn(run_fetcher(params));
        drop(rx);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("fetcher must end promptly when the lane is dropped")
            .unwrap();
    }

    #[tokio::test]
    async fn pinned_reads_walk_the_object_in_bounded_contiguous_windows() {
        // 20-byte object, 8-byte windows → 0..8, 8..16, 16..20.
        let flaky = Arc::new(FlakyStore::new(
            seeded(&[("p/a", b"0123456789abcdefghij")]).await,
        ));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        assert!(
            slice[0].etag.is_some(),
            "InMemory reports an ETag → pinned path"
        );
        let msgs = collect_fetch(Arc::clone(&store), slice, 0, 4, 8).await;
        assert_eq!(
            assembled(&msgs),
            vec![("p/a".to_string(), b"0123456789abcdefghij".to_vec())]
        );
        assert_eq!(
            flaky.recorded_ranges(),
            vec![
                RangeKind::Bounded(0, 8),
                RangeKind::Bounded(8, 16),
                RangeKind::Bounded(16, 20),
            ],
            "windows must be bounded, contiguous, and cover the object exactly"
        );
    }

    const DEPTH: usize = crate::lane_ctx::LANE_HANDOFF_CHUNKS;

    /// Fetcher parameters over a whole-object unit, unpaused, with no
    /// metrics.
    fn fetcher(
        store: Arc<dyn ObjectStore>,
        slice: Vec<ObjectEntry>,
        chunk_bytes: usize,
        range_bytes: usize,
        tx: mpsc::Sender<ChunkMsg>,
    ) -> FetcherParams {
        FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes,
            range_bytes,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: None,
            bytes_read: None,
            origin: Origin::Lister,
        }
    }

    /// Returns once every task is idle: on paused time the sleep fires only
    /// when nothing else can run.
    async fn park() {
        tokio::time::sleep(Duration::from_millis(80)).await;
    }

    async fn drain(rx: &mut mpsc::Receiver<ChunkMsg>) -> Vec<ChunkMsg> {
        let mut msgs = Vec::new();
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        msgs
    }

    fn debugged(msgs: &[ChunkMsg]) -> Vec<String> {
        msgs.iter().map(|m| format!("{m:?}")).collect()
    }

    /// Pins that a lane blocked on its hand-off holds the window it is
    /// handing on and the next one, each drained in full, and starts no
    /// third. Regression for #877.
    #[tokio::test(start_paused = true)]
    async fn a_blocked_lane_holds_two_windows_and_fetches_no_further() {
        let flaky = Arc::new(FlakyStore::new(seeded(&[("p/a", &[b'x'; 48][..])]).await));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        let (tx, _rx) = mpsc::channel(DEPTH);
        let task = tokio::spawn(run_fetcher(fetcher(store, slice, 2, 16, tx)));
        park().await;
        assert_eq!(
            flaky.recorded_ranges(),
            vec![RangeKind::Bounded(0, 16), RangeKind::Bounded(16, 32)],
            "the second window is read while the first is handed on"
        );
        // The drain marker fires only when a body is read to its end, so a
        // read-ahead that held its body open fails here.
        assert_eq!(flaky.bodies_drained(), 2);
        task.abort();
    }

    /// Pins that the next member's first window is read, under that
    /// member's own ETag, while the lane has not taken the last window of
    /// the member before it, and that the lane receives the same messages as
    /// a lane that never blocks. Regression for #877.
    #[tokio::test(start_paused = true)]
    async fn the_next_members_get_starts_while_the_last_window_is_queued() {
        let flaky = Arc::new(FlakyStore::new(
            seeded(&[("p/a", &[b'a'; 16][..]), ("p/b", &[b'b'; 12][..])]).await,
        ));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        assert_ne!(slice[0].etag, slice[1].etag);
        let (tx, mut rx) = mpsc::channel(DEPTH);
        let task = tokio::spawn(run_fetcher(fetcher(
            Arc::clone(&store),
            slice.clone(),
            2,
            16,
            tx,
        )));
        park().await;
        assert_eq!(
            flaky.requests(),
            vec![
                ("p/a".to_string(), slice[0].etag.clone()),
                ("p/b".to_string(), slice[1].etag.clone()),
            ]
        );
        let msgs = drain(&mut rx).await;
        task.await.unwrap();
        let unblocked = collect_fetch(store, slice, 0, 2, 16).await;
        assert_eq!(debugged(&msgs), debugged(&unblocked));
    }

    /// Pins that the read-ahead skips an empty member, which is read without
    /// a request, and reads the next member that has bytes. Its
    /// `ObjectStart`/`ObjectEnd` pair still reaches the lane in place.
    #[tokio::test(start_paused = true)]
    async fn a_zero_byte_member_is_skipped_by_the_read_ahead() {
        let flaky = Arc::new(FlakyStore::new(
            seeded(&[
                ("p/a", &[b'a'; 16][..]),
                ("p/b", b""),
                ("p/c", &[b'c'; 12][..]),
            ])
            .await,
        ));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(DEPTH);
        let task = tokio::spawn(run_fetcher(fetcher(
            Arc::clone(&store),
            slice.clone(),
            2,
            16,
            tx,
        )));
        park().await;
        let keys: Vec<String> = flaky.requests().into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["p/a", "p/c"]);
        let msgs = drain(&mut rx).await;
        task.await.unwrap();
        let unblocked = collect_fetch(store, slice, 0, 2, 16).await;
        assert_eq!(debugged(&msgs), debugged(&unblocked));
        assert_eq!(
            assembled(&msgs),
            vec![
                ("p/a".to_string(), vec![b'a'; 16]),
                ("p/b".to_string(), Vec::new()),
                ("p/c".to_string(), vec![b'c'; 12]),
            ]
        );
    }

    /// Pins that a fetcher paused mid-object finishes the one window it is
    /// reading ahead while it waits, and starts no other.
    #[tokio::test(start_paused = true)]
    async fn a_paused_lane_finishes_its_read_ahead_and_fetches_no_further() {
        let mut flaky = FlakyStore::new(seeded(&[("p/a", &[b'x'; 48][..])]).await);
        flaky.hold_on = 2;
        let flaky = Arc::new(flaky);
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(DEPTH);
        let params = fetcher(store, slice, 2, 16, tx);
        let pause = Arc::clone(&params.pause);
        let task = tokio::spawn(run_fetcher(params));
        park().await;
        assert!(matches!(
            rx.recv().await,
            Some(ChunkMsg::ObjectStart { .. })
        ));
        assert!(matches!(rx.recv().await, Some(ChunkMsg::Chunk(_))));
        pause.store(true, Ordering::Relaxed);
        park().await;
        assert_eq!(flaky.bodies_drained(), 1, "the read-ahead is still held");
        flaky.release.add_permits(1);
        park().await;
        assert_eq!(
            flaky.recorded_ranges(),
            vec![RangeKind::Bounded(0, 16), RangeKind::Bounded(16, 32)]
        );
        assert_eq!(
            flaky.bodies_drained(),
            2,
            "the read-ahead finished while the fetcher waited out the pause"
        );
        task.abort();
    }

    /// Pins that a byte-range read plans its next window only after the
    /// trim has seen the window in hand, so a read ahead never runs past the
    /// owned bytes.
    #[tokio::test(start_paused = true)]
    async fn a_blocked_ranged_read_stops_at_the_owned_bytes() {
        let flaky = Arc::new(FlakyStore::new(
            seeded(&[("p/a", b"aa\nbbbb\ncc\r\n \ndddddd\nee\nfff")]).await,
        ));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        // ObjectStart fills the one slot, so the fetcher blocks on the first
        // chunk of the first window.
        let (tx, mut rx) = mpsc::channel(1);
        let mut params = fetcher(store, slice, 2, 4, tx);
        params.range = Some(SplitRange::new(8, 21, b'\n'));
        let task = tokio::spawn(run_fetcher(params));
        park().await;
        assert_eq!(
            flaky.recorded_ranges(),
            vec![RangeKind::Bounded(7, 11), RangeKind::Bounded(11, 15)]
        );
        let msgs = drain(&mut rx).await;
        task.await.unwrap();
        assert_eq!(assembled(&msgs)[0].1, b"cc\r\n \ndddddd\n");
        assert_eq!(
            flaky.recorded_ranges(),
            vec![
                RangeKind::Bounded(7, 11),
                RangeKind::Bounded(11, 15),
                RangeKind::Bounded(15, 19),
                RangeKind::Bounded(19, 23),
            ]
        );
    }

    /// Pins the read-ahead's memory bound: the window queued chunks retain,
    /// the window being handed on and the window read ahead, and no fourth.
    #[tokio::test(start_paused = true)]
    async fn a_lane_holds_at_most_three_windows() {
        let mut flaky = FlakyStore::new(seeded(&[("p/a", &[b'x'; 80][..])]).await);
        flaky.count_live = true;
        let flaky = Arc::new(flaky);
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(DEPTH);
        let task = tokio::spawn(run_fetcher(fetcher(store, slice, 2, 16, tx)));
        loop {
            park().await;
            // Dropped at once, as a lane drops each chunk once it is framed.
            if rx.recv().await.is_none() {
                break;
            }
        }
        task.await.unwrap();
        assert_eq!(flaky.recorded_ranges().len(), 5);
        assert_eq!(flaky.live.load(Ordering::Relaxed), 0);
        assert_eq!(flaky.max_live.load(Ordering::Relaxed), 3);
    }

    /// Pins that a failed read ahead within an object reaches the lane only
    /// after every chunk of the window before it.
    #[tokio::test(start_paused = true)]
    async fn a_failed_read_ahead_reaches_the_lane_after_the_earlier_window() {
        for (origin, overwritten) in [(notified(None), true), (Origin::Lister, false)] {
            let (inner, entry) = thirty_bytes().await;
            let store = Arc::new(
                HeadStore::new(inner, 2).head_answers(vec![Ok(meta(Some("\"moved\""), 30))]),
            );
            let (tx, mut rx) = mpsc::channel(DEPTH);
            let mut params = fetcher(store, vec![entry], 2, 10, tx);
            params.origin = origin;
            let task = tokio::spawn(run_fetcher(params));
            park().await;
            let mut msgs = drain(&mut rx).await;
            task.await.unwrap();
            let failure = match msgs.pop() {
                Some(ChunkMsg::LaneFailed(failure)) => failure,
                other => panic!("the read did not fail: {other:?}"),
            };
            match failure {
                SplitFailure::Overwritten(_) => assert!(overwritten),
                SplitFailure::Poison(PoisonKind::EtagDrift, _) => assert!(!overwritten),
                other => panic!("unexpected failure: {other}"),
            }
            assert!(matches!(msgs[0], ChunkMsg::ObjectStart { .. }));
            assert_eq!(msgs.len(), 6, "ObjectStart and five chunks");
            assert_eq!(assembled(&msgs)[0].1, b"0123456789");
        }
    }

    /// Pins that a next member whose read ahead fails is reported after its
    /// own `ObjectStart`, where a read started at that point would report it.
    #[tokio::test(start_paused = true)]
    async fn a_failed_member_read_ahead_follows_that_members_start() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", &[b'a'; 16][..])]).await);
        let mut slice = listed(&store).await;
        slice.push(entry("p/gone", 12, Some("\"gone\"")));
        let (tx, mut rx) = mpsc::channel(DEPTH);
        let task = tokio::spawn(run_fetcher(fetcher(store, slice, 2, 16, tx)));
        park().await;
        let msgs = drain(&mut rx).await;
        task.await.unwrap();
        let tail: Vec<String> = debugged(&msgs[msgs.len() - 3..])
            .into_iter()
            .map(|m| m.split([' ', '(']).next().unwrap().to_owned())
            .collect();
        assert_eq!(tail, ["ObjectEnd", "ObjectStart", "LaneFailed"]);
        assert!(matches!(
            msgs.last(),
            Some(ChunkMsg::LaneFailed(SplitFailure::Poison(
                PoisonKind::NotFound,
                _
            )))
        ));
    }

    /// Pins that each window of an object retries on a budget of its own:
    /// failures of one window never count against the next.
    #[tokio::test(start_paused = true)]
    async fn each_window_gets_its_own_retry_budget() {
        // Window 2 (8..16) fails one attempt short of the budget, then one
        // attempt past it.
        for (window_two_failures, completes) in [(MAX_ATTEMPTS - 1, true), (MAX_ATTEMPTS, false)] {
            let mut flaky = FlakyStore::new(seeded(&[("p/a", b"0123456789abcdef")]).await);
            flaky.fail_gets = AtomicU32::new(MAX_ATTEMPTS - 1);
            flaky.fail_past = 8;
            flaky.fail_past_gets = AtomicU32::new(window_two_failures);
            let flaky = Arc::new(flaky);
            let store: Arc<dyn ObjectStore> = flaky.clone();
            let slice = listed(&store).await;
            let (tx, mut rx) = mpsc::channel(DEPTH);
            let task = tokio::spawn(run_fetcher(fetcher(store, slice, 2, 8, tx)));
            let msgs = drain(&mut rx).await;
            task.await.unwrap();
            if completes {
                assert_eq!(
                    assembled(&msgs),
                    vec![("p/a".to_string(), b"0123456789abcdef".to_vec())]
                );
                assert_eq!(flaky.gets.load(Ordering::Relaxed), 2 * MAX_ATTEMPTS);
            } else {
                assert!(matches!(
                    msgs.last(),
                    Some(ChunkMsg::LaneFailed(SplitFailure::Poison(
                        PoisonKind::RetriesExhausted,
                        _
                    )))
                ));
                assert_eq!(assembled(&msgs[..msgs.len() - 1])[0].1, b"01234567");
                assert_eq!(flaky.gets.load(Ordering::Relaxed), 2 * MAX_ATTEMPTS);
            }
        }
    }

    /// Pins that a lane dropped while a window is read ahead ends the
    /// fetcher and cancels that read.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_lane_mid_read_ahead_ends_the_fetcher() {
        for paused in [false, true] {
            let mut flaky = FlakyStore::new(seeded(&[("p/a", &[b'x'; 48][..])]).await);
            flaky.hold_on = 2;
            let flaky = Arc::new(flaky);
            let store: Arc<dyn ObjectStore> = flaky.clone();
            let slice = listed(&store).await;
            let (tx, mut rx) = mpsc::channel(DEPTH);
            let params = fetcher(store, slice, 2, 16, tx);
            params.pause.store(paused, Ordering::Relaxed);
            let pause = Arc::clone(&params.pause);
            let task = tokio::spawn(run_fetcher(params));
            park().await;
            if paused {
                assert_eq!(
                    flaky.held.load(Ordering::Relaxed),
                    0,
                    "paused before any GET"
                );
                pause.store(false, Ordering::Relaxed);
                park().await;
                // One slot frees, so the fetcher's next send waits out the
                // pause instead of the full hand-off.
                pause.store(true, Ordering::Relaxed);
                assert!(rx.recv().await.is_some());
                park().await;
            }
            assert_eq!(flaky.held.load(Ordering::Relaxed), 1);
            drop(rx);
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("the fetcher ends when its lane is dropped")
                .unwrap();
            assert_eq!(flaky.held_dropped.load(Ordering::Relaxed), 1);
        }
    }

    /// Pins that a bounded read answered with no bytes poisons the unit,
    /// where reading the window again would succeed.
    #[tokio::test(start_paused = true)]
    async fn an_empty_ranged_read_poisons_the_unit() {
        let mut flaky = FlakyStore::new(seeded(&[("p/a", b"0123456789")]).await);
        flaky.empty_on = 1;
        let store: Arc<dyn ObjectStore> = Arc::new(flaky);
        let slice = listed(&store).await;
        let msgs = collect_fetch(store, slice, 0, 4, 8).await;
        assert!(
            matches!(
                msgs.last(),
                Some(ChunkMsg::LaneFailed(SplitFailure::Poison(PoisonKind::Undecodable, reason)))
                    if reason.contains("returned no bytes")
            ),
            "{msgs:?}"
        );
    }

    /// Pins that a member the store reports no ETag for is never read
    /// ahead: its un-ranged body must not open before its turn.
    #[tokio::test(start_paused = true)]
    async fn a_member_without_an_etag_is_not_read_ahead() {
        let flaky = Arc::new(FlakyStore::new(
            seeded(&[("p/a", &[b'a'; 16][..]), ("p/b", &[b'b'; 12][..])]).await,
        ));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let mut slice = listed(&store).await;
        slice[1].etag = None;
        let (tx, mut rx) = mpsc::channel(DEPTH);
        let task = tokio::spawn(run_fetcher(fetcher(store, slice, 2, 16, tx)));
        park().await;
        assert_eq!(flaky.recorded_ranges(), vec![RangeKind::Bounded(0, 16)]);
        let msgs = drain(&mut rx).await;
        task.await.unwrap();
        assert_eq!(
            flaky.recorded_ranges(),
            vec![RangeKind::Bounded(0, 16), RangeKind::Full]
        );
        assert_eq!(assembled(&msgs)[1].1, vec![b'b'; 12]);
    }

    /// The messages a lane receives for whole members with these bodies,
    /// read in `range_bytes` windows and `chunk_bytes` chunks.
    fn expected_messages(
        slice: &[ObjectEntry],
        bodies: &[Vec<u8>],
        chunk_bytes: usize,
        range_bytes: usize,
    ) -> Vec<String> {
        let mut msgs = Vec::new();
        for (ordinal, (entry, body)) in slice.iter().zip(bodies).enumerate() {
            msgs.push(ChunkMsg::ObjectStart {
                ordinal: ordinal as u32,
                key: entry.key.clone(),
                last_modified_ms: entry.last_modified_ms,
            });
            for window in body.chunks(range_bytes) {
                for chunk in window.chunks(chunk_bytes) {
                    msgs.push(ChunkMsg::Chunk(Bytes::copy_from_slice(chunk)));
                }
            }
            msgs.push(ChunkMsg::ObjectEnd);
        }
        debugged(&msgs)
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        /// Pins that the messages a lane receives and the GETs a unit makes
        /// depend on neither the hand-off depth nor how fast the lane reads.
        #[test]
        fn a_unit_streams_its_members_in_order_under_any_windows(
            sizes in proptest::collection::vec(0usize..48, 1..7),
            range_bytes in 1usize..20,
            chunk_bytes in 1usize..8,
            depth in 1usize..6,
            park_every in 1usize..4,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();
            let bodies: Vec<Vec<u8>> = sizes
                .iter()
                .enumerate()
                .map(|(i, n)| (0..*n).map(|b| b'a' + ((b + i) % 26) as u8).collect())
                .collect();
            let (msgs, slice, ranges) = rt.block_on(async {
                let keys: Vec<String> = (0..bodies.len()).map(|i| format!("p/{i}")).collect();
                let objects: Vec<(&str, &[u8])> = keys
                    .iter()
                    .zip(&bodies)
                    .map(|(k, b)| (k.as_str(), b.as_slice()))
                    .collect();
                let flaky = Arc::new(FlakyStore::new(seeded(&objects).await));
                let store: Arc<dyn ObjectStore> = flaky.clone();
                let slice = listed(&store).await;
                let (tx, mut rx) = mpsc::channel(depth);
                let task = tokio::spawn(run_fetcher(fetcher(
                    store,
                    slice.clone(),
                    chunk_bytes,
                    range_bytes,
                    tx,
                )));
                let mut msgs = Vec::new();
                while let Some(m) = rx.recv().await {
                    msgs.push(m);
                    if msgs.len() % park_every == 0 {
                        park().await;
                    }
                }
                task.await.unwrap();
                (msgs, slice, flaky.recorded_ranges())
            });
            proptest::prop_assert_eq!(
                debugged(&msgs),
                expected_messages(&slice, &bodies, chunk_bytes, range_bytes)
            );
            let want: Vec<RangeKind> = bodies
                .iter()
                .flat_map(|body| {
                    let size = body.len() as u64;
                    (0..size)
                        .step_by(range_bytes)
                        .map(move |w| RangeKind::Bounded(w, (w + range_bytes as u64).min(size)))
                })
                .collect();
            proptest::prop_assert_eq!(ranges, want);
        }
    }

    /// Run a fetcher over one no-ETag object whose stream is cut after
    /// `cut_after` bytes, returning the terminal `LaneFailed` failure.
    async fn no_etag_mid_stream_failure(cut_fatal: bool) -> SplitFailure {
        no_etag_mid_stream_failure_with(cut_fatal, None).await
    }

    /// [`no_etag_mid_stream_failure`] with the cut's error text replaced.
    async fn no_etag_mid_stream_failure_with(
        cut_fatal: bool,
        error_text: Option<String>,
    ) -> SplitFailure {
        let flaky = FlakyStore {
            cut_after: 4,
            cut_fatal,
            error_text,
            ..FlakyStore::new(seeded(&[("p/a", b"0123456789")]).await)
        };
        flaky.cut_streams.store(1, Ordering::Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(flaky);
        let mut slice = listed(&store).await;
        slice[0].etag = None; // no pin → streaming fallback, no mid-object resume
        let msgs = collect_fetch(store, slice, 0, 64, 64).await;
        match msgs.into_iter().last() {
            Some(ChunkMsg::LaneFailed(failure)) => failure,
            other => panic!("a mid-stream break without an ETag must fail the lane: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mid_stream_permission_loss_without_etag_is_pipeline_fatal() {
        // Credentials/permission failures hold for every object on every
        // instance: no peer fares better, so poisoning (hand-back + retry
        // elsewhere) would serialize the same failure across the fleet.
        let failure = no_etag_mid_stream_failure(true).await;
        let SplitFailure::Fatal(e) = failure else {
            panic!("PermissionDenied mid-stream must be pipeline-fatal, got: {failure}");
        };
        assert!(e.to_string().contains("p/a"), "{e}");
    }

    #[tokio::test]
    async fn mid_stream_generic_failure_without_etag_poisons_the_split() {
        // A transport break after bytes were delivered cannot be resumed
        // without an ETag pin (a re-read could splice two versions):
        // object-level poison, never pipeline-fatal.
        let failure = no_etag_mid_stream_failure(false).await;
        let SplitFailure::Poison(kind, reason) = failure else {
            panic!("a generic mid-stream break is split poison, got: {failure}");
        };
        assert_eq!(kind, PoisonKind::Undecodable);
        assert!(reason.contains("no ETag"), "{reason}");
    }

    #[tokio::test]
    async fn an_object_without_an_etag_uses_the_streaming_fallback() {
        let flaky = Arc::new(FlakyStore::new(seeded(&[("p/a", b"hello world")]).await));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let mut slice = listed(&store).await;
        slice[0].etag = None; // store reports no ETag → one continuous stream
        let msgs = collect_fetch(Arc::clone(&store), slice, 0, 4, 4).await;
        assert_eq!(
            assembled(&msgs),
            vec![("p/a".to_string(), b"hello world".to_vec())]
        );
        assert_eq!(
            flaky.recorded_ranges(),
            vec![RangeKind::Full],
            "the fallback issues a single un-ranged GET, never a bounded window"
        );
    }

    /// A fetcher parked in the pause wait gives up when its lane drops the
    /// receiver, without ever being unpaused.
    #[tokio::test(start_paused = true)]
    async fn a_paused_fetcher_exits_when_its_lane_drops_the_receiver() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"aaaa")]).await);
        let slice = listed(&store).await;
        let (tx, rx) = mpsc::channel(8);
        let params = FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(true)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: None,
            bytes_read: None,
            origin: Origin::Lister,
        };
        let task = tokio::spawn(run_fetcher(params));
        // On paused time the sleep returns only once the fetcher is parked.
        tokio::time::sleep(Duration::from_millis(80)).await;
        drop(rx);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("a paused fetcher exits once the receiver is gone")
            .unwrap();
    }

    /// A read the store refuses with a TLS alert fails the lane as
    /// pipeline-fatal, and the error names the alert.
    #[tokio::test]
    async fn a_tls_rejection_on_a_read_is_fatal_and_named() {
        let addr =
            spate_test::tls_alert_server(b"", u8::from(rustls::AlertDescription::HandshakeFailure));
        let store: Arc<dyn ObjectStore> = Arc::new(crate::test_servers::tls_store_at(
            &format!("https://{addr}"),
            &crate::test_servers::TestCa::new("any"),
        ));
        let slice = vec![ObjectEntry {
            key: "p/a".to_owned(),
            size: 10,
            etag: Some("\"e\"".to_owned()),
            last_modified_ms: 0,
        }];
        let msgs = collect_fetch(store, slice, 0, 64, 64).await;
        let Some(ChunkMsg::LaneFailed(SplitFailure::Fatal(e))) = msgs.into_iter().last() else {
            panic!("expected a pipeline-fatal lane failure");
        };
        assert!(e.to_string().contains("HandshakeFailure"), "{e}");
    }

    /// A store error body of 64 KiB, as an `AccessDenied` or `NoSuchKey` XML
    /// document quotes it.
    fn large_body(code: &str) -> String {
        format!(
            "<Error><Code>{code}</Code><Message>{}</Message></Error>",
            "x".repeat(64 * 1024)
        )
    }

    /// The store's text in `reason` is cut: under 2 KiB, marked `…`.
    fn assert_cut(reason: &str) {
        assert!(reason.len() < 2048, "{} bytes", reason.len());
        assert!(reason.contains('…'), "{reason}");
    }

    /// One entry under `key` with an ETag, read from a local server answering
    /// every request with `status` and `body`; returns the lane's failure.
    async fn read_answered(status: &'static str, body: String, key: &str) -> SplitFailure {
        let url = crate::test_servers::sequence_server(vec![(status, body)]);
        let store: Arc<dyn ObjectStore> = Arc::new(crate::test_servers::store_at(&url));
        let slice = vec![entry(key, 10, Some("\"e\""))];
        match collect_fetch(store, slice, 0, 64, 64)
            .await
            .into_iter()
            .last()
        {
            Some(ChunkMsg::LaneFailed(failure)) => failure,
            other => panic!("a refused read must fail the lane: {other:?}"),
        }
    }

    /// A read refused 403 with a 64 KiB body is pipeline-fatal, and its
    /// reason carries the status and the store's text cut to the bound.
    /// Regression for #1305.
    #[tokio::test]
    async fn a_refused_read_with_a_large_body_is_fatal_and_cut() {
        let failure = read_answered("403 Forbidden", large_body("AccessDenied"), "p/a").await;
        let SplitFailure::Fatal(e) = failure else {
            panic!("a 403 read must be fatal, got: {failure}");
        };
        let reason = e.to_string();
        assert!(reason.contains("p/a"), "{reason}");
        assert!(reason.contains("403 Forbidden"), "{reason}");
        assert!(reason.contains("AccessDenied"), "{reason}");
        assert!(reason.ends_with('…'), "{reason}");
        assert_cut(&reason);
    }

    /// A read answered 404 with a 64 KiB body poisons the unit as
    /// `NotFound`, with the store's text cut to the bound.
    #[tokio::test]
    async fn a_missing_object_with_a_large_body_poisons_and_cut() {
        let failure = read_answered("404 Not Found", large_body("NoSuchKey"), "p/a").await;
        let SplitFailure::Poison(PoisonKind::NotFound, reason) = failure else {
            panic!("a 404 read must poison as not_found, got: {failure}");
        };
        assert!(reason.contains("p/a"), "{reason}");
        assert!(reason.contains("NoSuchKey"), "{reason}");
        assert!(reason.ends_with('…'), "{reason}");
        assert_cut(&reason);
    }

    /// A multibyte error body is cut on a character: 512 characters of the
    /// store's body and the `…`.
    #[tokio::test]
    async fn a_multibyte_error_body_is_cut_on_a_character() {
        let body = format!(
            "<Error><Message>{}</Message></Error>",
            "é".repeat(64 * 1024)
        );
        let failure = read_answered("403 Forbidden", body, "p/a").await;
        let SplitFailure::Fatal(e) = failure else {
            panic!("a 403 read must be fatal, got: {failure}");
        };
        let reason = e.to_string();
        let text = reason
            .split_once("403 Forbidden: ")
            .map(|(_, text)| text)
            .expect("the store's body follows the status");
        assert_eq!(text.chars().count(), 512 + 1, "{text}");
        assert!(text.ends_with("é…"), "{text}");
    }

    /// A refused read of a key longer than the bound names the key, the kind
    /// of failure and the status, with the store's body cut to the bound.
    #[tokio::test]
    async fn a_long_key_refusal_keeps_the_status() {
        let key = format!("p/{}", "k".repeat(1000));
        let failure = read_answered("403 Forbidden", large_body("AccessDenied"), &key).await;
        let SplitFailure::Fatal(e) = failure else {
            panic!("a 403 read must be fatal, got: {failure}");
        };
        let reason = e.to_string();
        assert!(reason.contains(&format!("\"{key}\"")), "{reason}");
        assert!(
            reason.contains("lacked the necessary privileges"),
            "{reason}"
        );
        assert!(reason.contains("403 Forbidden: <Error>"), "{reason}");
        assert!(reason.ends_with('…'), "{reason}");
        assert!(
            reason.len() < 2048 + 2 * key.len(),
            "{} bytes",
            reason.len()
        );
    }

    /// Pins that a TLS rejection on a read of a long key is still named.
    #[tokio::test]
    async fn a_tls_rejection_on_a_long_key_is_still_named() {
        let addr =
            spate_test::tls_alert_server(b"", u8::from(rustls::AlertDescription::HandshakeFailure));
        let store: Arc<dyn ObjectStore> = Arc::new(crate::test_servers::tls_store_at(
            &format!("https://{addr}"),
            &crate::test_servers::TestCa::new("any"),
        ));
        let slice = vec![entry(&format!("p/{}", "k".repeat(1000)), 10, Some("\"e\""))];
        let msgs = collect_fetch(store, slice, 0, 64, 64).await;
        let Some(ChunkMsg::LaneFailed(SplitFailure::Fatal(e))) = msgs.into_iter().last() else {
            panic!("expected a pipeline-fatal lane failure");
        };
        assert!(e.to_string().contains("HandshakeFailure"), "{e}");
    }

    /// A read failing past its attempt budget poisons the unit with the
    /// last error's text cut to the bound.
    #[tokio::test(start_paused = true)]
    async fn a_read_failing_past_the_budget_with_a_large_error_is_cut() {
        let flaky = FlakyStore {
            error_text: Some("x".repeat(64 * 1024)),
            ..FlakyStore::new(seeded(&[("p/a", b"0123456789")]).await)
        };
        flaky.fail_gets.store(MAX_ATTEMPTS, Ordering::Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(flaky);
        let slice = listed(&store).await;
        let msgs = collect_fetch(store, slice, 0, 64, 64).await;
        let Some(ChunkMsg::LaneFailed(SplitFailure::Poison(PoisonKind::RetriesExhausted, reason))) =
            msgs.into_iter().last()
        else {
            panic!("a read failing past its budget must poison as retries_exhausted");
        };
        assert!(reason.contains("attempts"), "{reason}");
        assert!(reason.contains("xxxx"), "{reason}");
        assert!(reason.ends_with('…'), "{reason}");
        assert_cut(&reason);
    }

    /// A read of a key that encodes past the bound, failing 503 past its
    /// attempt budget, names the status and the store's code.
    #[tokio::test(start_paused = true)]
    async fn a_read_of_a_long_key_past_the_budget_keeps_the_status() {
        let text = format!(
            "Error performing GET http://127.0.0.1/b/p/{} in 1ms - Server returned non-2xx \
             status code: 503 Service Unavailable: <Error><Code>SlowDown</Code><Message>{}",
            "%E6%95%B0".repeat(60),
            "x".repeat(64 * 1024)
        );
        let flaky = FlakyStore {
            error_text: Some(text),
            ..FlakyStore::new(seeded(&[("p/a", b"0123456789")]).await)
        };
        flaky.fail_gets.store(MAX_ATTEMPTS, Ordering::Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(flaky);
        let slice = listed(&store).await;
        let msgs = collect_fetch(store, slice, 0, 64, 64).await;
        let Some(ChunkMsg::LaneFailed(SplitFailure::Poison(PoisonKind::RetriesExhausted, reason))) =
            msgs.into_iter().last()
        else {
            panic!("a read failing past its budget must poison as retries_exhausted");
        };
        assert!(reason.contains("503 Service Unavailable"), "{reason}");
        assert!(reason.contains("SlowDown"), "{reason}");
        assert!(reason.ends_with('…'), "{reason}");
        assert_cut(&reason);
    }

    /// A mid-object failure without an ETag, fatal or poison, carries the
    /// store's text cut to the bound.
    #[tokio::test]
    async fn a_mid_object_failure_with_a_large_error_is_cut() {
        let text = Some("x".repeat(64 * 1024));
        let SplitFailure::Fatal(e) = no_etag_mid_stream_failure_with(true, text.clone()).await
        else {
            panic!("PermissionDenied mid-stream must be pipeline-fatal");
        };
        let reason = e.to_string();
        assert!(
            reason.contains("p/a") && reason.contains("mid-object"),
            "{reason}"
        );
        assert!(reason.ends_with('…'), "{reason}");
        assert_cut(&reason);
        let SplitFailure::Poison(PoisonKind::Undecodable, reason) =
            no_etag_mid_stream_failure_with(false, text).await
        else {
            panic!("a generic mid-stream break is split poison");
        };
        assert!(
            reason.contains("p/a") && reason.contains("mid-object"),
            "{reason}"
        );
        assert!(reason.ends_with('…'), "{reason}");
        assert_cut(&reason);
    }

    /// Each transient read failure's log line carries the store's text cut
    /// to the bound.
    #[test]
    fn a_retried_read_logs_its_store_error_cut() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            rt.block_on(async {
                let flaky = FlakyStore {
                    error_text: Some("x".repeat(64 * 1024)),
                    ..FlakyStore::new(seeded(&[("p/a", b"0123456789")]).await)
                };
                flaky.fail_gets.store(2, Ordering::Relaxed);
                let store: Arc<dyn ObjectStore> = Arc::new(flaky);
                let slice = listed(&store).await;
                let msgs = collect_fetch(store, slice, 0, 64, 64).await;
                assert_eq!(
                    assembled(&msgs),
                    vec![("p/a".to_string(), b"0123456789".to_vec())]
                );
            });
        });
        let retried: Vec<_> = lines
            .iter()
            .filter(|l| l.contains("transient object read failure"))
            .collect();
        assert_eq!(retried.len(), 2, "{lines:?}");
        for line in retried {
            assert!(line.contains("xxxx…"), "{line}");
            assert!(line.len() < 2048, "{} bytes", line.len());
        }
    }

    // ---------------------------------------------- ranged reads, end to end --

    /// Window sizes and injected failures for one [`read_through_lane`] run.
    #[derive(Clone, Copy, Debug)]
    struct ReadPlan {
        range_bytes: usize,
        chunk_bytes: usize,
        /// Retryable failures of the first GETs, which read the range's head.
        head_failures: u32,
        /// Retryable failures of the first GETs reaching past the range's end.
        tail_failures: u32,
        /// Cut the first GET's body after this many bytes.
        cut_after: Option<usize>,
    }

    impl ReadPlan {
        fn windows(range_bytes: usize, chunk_bytes: usize) -> ReadPlan {
            ReadPlan {
                range_bytes,
                chunk_bytes,
                head_failures: 0,
                tail_failures: 0,
                cut_after: None,
            }
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    /// Read `range` of `object` through a fetcher over a [`FlakyStore`] and a
    /// lane resuming `resume` records in, returning each emitted record's
    /// offset and bytes. Panics if the split poisons.
    fn read_through_lane(
        rt: &tokio::runtime::Runtime,
        object: &[u8],
        range: Option<SplitRange>,
        resume: u64,
        plan: ReadPlan,
    ) -> Vec<(i64, Vec<u8>)> {
        use crate::lane::S3Lane;
        use crate::lane_ctx::SplitTracker;
        use crate::offset::Position;
        use spate_core::source::{PayloadBatch as _, SourceLane as _};

        let flaky = FlakyStore {
            cut_after: plan.cut_after.unwrap_or(0),
            fail_past: range.map_or(u64::MAX, |r| r.end),
            ..FlakyStore::new(rt.block_on(seeded(&[("p/big.ndjson", object)])))
        };
        flaky.fail_gets.store(plan.head_failures, Ordering::Relaxed);
        flaky
            .fail_past_gets
            .store(plan.tail_failures, Ordering::Relaxed);
        flaky
            .cut_streams
            .store(u32::from(plan.cut_after.is_some()), Ordering::Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(flaky);
        let slice = rt.block_on(listed(&store));
        let (tx, rx) = mpsc::channel(4);
        rt.spawn(run_fetcher(FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: plan.chunk_bytes,
            range_bytes: plan.range_bytes,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range,
            bytes_read: None,
            origin: Origin::Lister,
        }));
        let tracker = Arc::new(SplitTracker::new());
        let (poison_tx, poison_rx) = std_mpsc::channel();
        let mut lane = S3Lane::new(
            spate_core::source::LaneId(0),
            spate_core::record::PartitionId(0),
            rx,
            rt.handle().clone(),
            spate_core::checkpoint::Checkpointer::new().handle(),
            crate::config::Compression::None,
            Arc::new(|| Box::new(crate::testutil::TestLineFramer::new(1 << 20))),
            Some(Position {
                ordinal: 0,
                record: resume,
            }),
            Arc::clone(&tracker),
            poison_tx,
            spate_core::feed::wake_channel().0,
            None,
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut out = Vec::new();
        loop {
            match lane
                .poll(1024, Duration::from_millis(20))
                .expect("the lane polls")
            {
                Some(mut batch) => {
                    while let Some(p) = batch.next_payload() {
                        out.push((p.offset, p.bytes.to_vec()));
                    }
                }
                None if tracker.terminal().is_some() => return out,
                None => {
                    if let Ok(report) = poison_rx.try_recv() {
                        panic!("the split poisoned: {}", report.reason);
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the lane never reached end of input"
                    );
                }
            }
        }
    }

    fn bytes_of(records: Vec<(i64, Vec<u8>)>) -> Vec<Vec<u8>> {
        records.into_iter().map(|(_, bytes)| bytes).collect()
    }

    /// The records of `object` framed whole, in one chunk.
    fn framed_whole(object: &[u8]) -> Vec<Vec<u8>> {
        let mut framer = crate::framer::ObjectFramer::new(Arc::new(|| {
            Box::new(crate::testutil::TestLineFramer::new(1 << 20))
        }));
        framer.begin_object(crate::framer::Codec::Plain).unwrap();
        framer.push_chunk(object).unwrap();
        framer.finish_object().unwrap();
        std::iter::from_fn(|| framer.pop_record()).collect()
    }

    /// Object bytes of short records, `\r\n` endings, whitespace-only lines,
    /// lines longer than a whole range, and an optional unterminated final
    /// line.
    fn arb_lines_object() -> impl proptest::strategy::Strategy<Value = Vec<u8>> {
        use proptest::prelude::*;
        (
            proptest::collection::vec(
                prop_oneof![
                    4 => "[a-z]{1,6}".prop_map(|s| format!("{s}\n")),
                    1 => "[a-z]{1,6}".prop_map(|s| format!("{s}\r\n")),
                    1 => "[ \t]{0,3}".prop_map(|s| format!("{s}\n")),
                    1 => "[a-z]{30,60}".prop_map(|s| format!("{s}\n")),
                ],
                1..30,
            ),
            proptest::option::of("[a-z]{1,6}"),
        )
            .prop_map(|(lines, last)| {
                let mut object = lines.concat();
                object.push_str(last.as_deref().unwrap_or_default());
                object.into_bytes()
            })
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(48))]

        /// An object tiled into ranges, each read through the fetcher and a
        /// lane with failures injected in the head and tail windows, yields
        /// the whole object's records exactly once and in order.
        #[test]
        fn ranges_tiling_an_object_emit_each_record_once(
            object in arb_lines_object(),
            pieces in 1u64..10,
            range_bytes in 1usize..32,
            chunk_bytes in 1usize..16,
            head_failures in 0u32..2,
            tail_failures in 0u32..3,
            cut_after in proptest::option::of(0usize..24),
        ) {
            let rt = runtime();
            let size = object.len() as u64;
            let n = pieces.min(size);
            let bound = |i: u64| (u128::from(i) * u128::from(size) / u128::from(n)) as u64;
            let plan = ReadPlan { range_bytes, chunk_bytes, head_failures, tail_failures, cut_after };
            let mut joined = Vec::new();
            for i in 0..n {
                let range = SplitRange::new(bound(i), bound(i + 1), b'\n');
                joined.extend(bytes_of(read_through_lane(&rt, &object, Some(range), 0, plan)));
            }
            proptest::prop_assert_eq!(joined, framed_whole(&object));
        }
    }

    /// A range read under one window layout, then resumed at every record
    /// count under others, emits exactly the records past the resume point at
    /// the same offsets. The ranges put a delimiter at `start - 1`, `end - 1`
    /// and `end`, include an empty range, one ending at the object's end and
    /// one whose head spans several windows, and the layouts put a window edge
    /// at `end - 1` and at the object's end.
    #[test]
    fn a_resumed_range_emits_the_same_tail_under_any_windows() {
        let rt = runtime();
        // Delimiters at 2, 7, 11, 13, 20 and 23; 27 bytes.
        let object = b"aa\nbbbb\ncc\r\n \ndddddd\nee\nfff";
        let ranges = [
            (0, 3),
            (3, 8),
            (3, 7),
            (5, 14),
            (8, 21),
            (15, 20),
            (14, 27),
            (16, 27),
        ];
        let reference = ReadPlan::windows(4, 2);
        let others = [
            ReadPlan {
                head_failures: 1,
                tail_failures: 1,
                ..ReadPlan::windows(5, 3)
            },
            // From byte 7, windows end at 20, `end - 1` of (8, 21).
            ReadPlan::windows(13, 1),
            ReadPlan::windows(64, 64),
        ];
        for (start, end) in ranges {
            let range = Some(SplitRange::new(start, end, b'\n'));
            let full = read_through_lane(&rt, object, range, 0, reference);
            for plan in others {
                for k in 0..=full.len() {
                    let resumed = read_through_lane(&rt, object, range, k as u64, plan);
                    assert_eq!(
                        resumed,
                        full[k..],
                        "range [{start}, {end}) resumed at {k} under {plan:?}"
                    );
                }
            }
        }
        // Spot-check the reference reads against the ownership rule.
        let read = |start, end| {
            bytes_of(read_through_lane(
                &rt,
                object,
                Some(SplitRange::new(start, end, b'\n')),
                0,
                reference,
            ))
        };
        assert_eq!(read(3, 7), vec![b"bbbb".to_vec()]);
        assert_eq!(read(5, 14), vec![b"cc".to_vec()]);
        assert_eq!(read(8, 21), vec![b"cc".to_vec(), b"dddddd".to_vec()]);
        assert!(read(15, 20).is_empty());
        assert_eq!(
            read(14, 27),
            vec![b"dddddd".to_vec(), b"ee".to_vec(), b"fff".to_vec()]
        );
        assert_eq!(read(16, 27), vec![b"ee".to_vec(), b"fff".to_vec()]);
    }

    /// A ranged read counts the bytes it fetches and trims into
    /// `bytes_read`, so with the forwarded bytes the lane counts, the total is
    /// every byte read from the store.
    #[tokio::test]
    async fn a_ranged_read_counts_the_bytes_it_trims() {
        let flaky = Arc::new(FlakyStore::new(
            seeded(&[("p/a", b"aa\nbbbb\ncc\r\n \ndddddd\nee\nfff")]).await,
        ));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        let trimmed = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::channel(64);
        let task = tokio::spawn(run_fetcher(FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 2,
            range_bytes: 4,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
            range: Some(SplitRange::new(8, 21, b'\n')),
            bytes_read: Some(spate_core::metrics::Counter::from_arc(Arc::clone(&trimmed))),
            origin: Origin::Lister,
        }));
        let mut msgs = Vec::new();
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        task.await.unwrap();
        let forwarded = assembled(&msgs);
        assert_eq!(forwarded[0].1, b"cc\r\n \ndddddd\n");
        let fetched: u64 = flaky
            .recorded_ranges()
            .iter()
            .map(|r| match r {
                RangeKind::Bounded(start, end) => end - start,
                other => panic!("a ranged read issues bounded windows, got {other:?}"),
            })
            .sum();
        assert_eq!(fetched, 16, "windows 7..11, 11..15, 15..19 and 19..23");
        assert_eq!(
            trimmed.load(Ordering::Relaxed) + forwarded[0].1.len() as u64,
            fetched
        );
    }

    // ------------------------------------------- notification reads --

    /// One request a [`HeadStore`] saw: `(if_match, version, head)`.
    type Seen = (Option<String>, Option<String>, bool);

    /// An in-memory store that records each request's `If-Match`, version
    /// and HEAD flag, fails one GET with a failed precondition, and answers
    /// HEADs from a script.
    #[derive(Debug)]
    struct HeadStore {
        inner: InMemory,
        /// The GET, counted from 1 and HEADs not counted, that answers 412,
        /// or 416 when `range_refused` is set.
        precondition_on: u32,
        range_refused: bool,
        gets: AtomicU32,
        /// Answers for the HEADs, in order; the inner store answers once
        /// the script is spent.
        heads: std::sync::Mutex<std::collections::VecDeque<object_store::Result<ObjectMeta>>>,
        seen: std::sync::Mutex<Vec<Seen>>,
    }

    use object_store::ObjectMeta;

    impl HeadStore {
        fn new(inner: InMemory, precondition_on: u32) -> HeadStore {
            HeadStore {
                inner,
                precondition_on,
                range_refused: false,
                gets: AtomicU32::new(0),
                heads: std::sync::Mutex::new(std::collections::VecDeque::new()),
                seen: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn refusing_the_range(mut self) -> HeadStore {
            self.range_refused = true;
            self
        }

        fn head_answers(self, answers: Vec<object_store::Result<ObjectMeta>>) -> HeadStore {
            *self.heads.lock().unwrap() = answers.into();
            self
        }

        fn seen(&self) -> Vec<Seen> {
            self.seen.lock().unwrap().clone()
        }

        fn heads(&self) -> usize {
            self.seen().iter().filter(|(_, _, head)| *head).count()
        }
    }

    impl fmt::Display for HeadStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "HeadStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for HeadStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
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
            self.seen.lock().unwrap().push((
                options.if_match.clone(),
                options.version.clone(),
                options.head,
            ));
            if options.head {
                let scripted = self.heads.lock().unwrap().pop_front();
                if let Some(answer) = scripted {
                    let meta = answer?;
                    return Ok(GetResult {
                        payload: GetResultPayload::Stream(Box::pin(futures_util::stream::empty())),
                        range: 0..meta.size,
                        meta,
                        attributes: Default::default(),
                        extensions: Default::default(),
                    });
                }
                return self.inner.get_opts(location, options).await;
            }
            if self.gets.fetch_add(1, Ordering::Relaxed) + 1 == self.precondition_on {
                if self.range_refused {
                    return Err(object_store::Error::Generic {
                        store: "head",
                        source: "Server returned non-2xx status code: 416 Range Not Satisfiable: "
                            .into(),
                    });
                }
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "overwritten".into(),
                });
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
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

    fn notified(version: Option<&str>) -> Origin {
        Origin::Notification {
            version: version.map(str::to_owned),
            re_enqueues: 0,
        }
    }

    fn meta(etag: Option<&str>, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from("k"),
            last_modified: chrono::DateTime::from_timestamp_millis(1_760_000_000_000).unwrap(),
            size,
            e_tag: etag.map(str::to_owned),
            version: None,
        }
    }

    /// A 30-byte object `k` and its listed entry, the ETag the store reports.
    async fn thirty_bytes() -> (InMemory, ObjectEntry) {
        let store = seeded(&[("k", b"0123456789abcdefghijABCDEFGHIJ")]).await;
        let listed = list_all(&(Arc::new(store.clone()) as Arc<dyn ObjectStore>), None)
            .await
            .unwrap();
        (store, listed[0].clone())
    }

    /// Drive a fetcher over the one `entry` of a unit from `origin`, in
    /// 10-byte windows, and collect its message stream.
    async fn fetch_from(
        store: Arc<HeadStore>,
        entry: ObjectEntry,
        origin: Origin,
        retries: Option<spate_core::metrics::Counter>,
    ) -> Vec<ChunkMsg> {
        let (tx, mut rx) = mpsc::channel(64);
        let task = tokio::spawn(run_fetcher(FetcherParams {
            split: "s3-test".into(),
            store,
            slice: Arc::new(vec![entry]),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 10,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries,
            range: None,
            bytes_read: None,
            origin,
        }));
        let mut msgs = Vec::new();
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        task.await.unwrap();
        msgs
    }

    fn last_failure(msgs: Vec<ChunkMsg>) -> SplitFailure {
        match msgs.into_iter().last() {
            Some(ChunkMsg::LaneFailed(failure)) => failure,
            other => panic!("the read did not fail: {other:?}"),
        }
    }

    /// Pins that a notification unit naming a version sends it on every
    /// window of a pinned read and on an unpinned read.
    #[tokio::test]
    async fn a_notification_unit_with_a_version_id_reads_that_version() {
        let (inner, entry) = thirty_bytes().await;
        let store = Arc::new(HeadStore::new(inner.clone(), 0));
        let msgs = fetch_from(
            Arc::clone(&store),
            entry.clone(),
            notified(Some("v1")),
            None,
        )
        .await;
        assert_eq!(assembled(&msgs)[0].1.len(), 30);
        let seen = store.seen();
        assert_eq!(seen.len(), 3, "{seen:?}");
        for (if_match, version, head) in seen {
            assert_eq!(if_match, entry.etag);
            assert_eq!(version.as_deref(), Some("v1"));
            assert!(!head);
        }

        let store = Arc::new(HeadStore::new(inner, 0));
        let unpinned = ObjectEntry {
            etag: None,
            ..entry
        };
        fetch_from(Arc::clone(&store), unpinned, notified(Some("v1")), None).await;
        assert_eq!(store.seen(), [(None, Some("v1".to_owned()), false)]);
    }

    /// Pins that a notification whose `versionId` is absent, null, `""` or
    /// `"null"` reads the key, as a lister's unit does.
    #[tokio::test]
    async fn a_null_or_absent_version_id_reads_by_key() {
        let (inner, entry) = thirty_bytes().await;
        let scope = crate::unit::Scope {
            bucket: Some("bucket".to_owned()),
            prefix: Path::from(""),
        };
        let packing = crate::split::Packing {
            target_bytes: 1 << 20,
            compression: crate::config::Compression::None,
            delimiter: Some(b'\n'),
        };
        let mut origins = vec![Origin::Lister];
        for version in [
            None,
            Some(serde_json::Value::Null),
            Some("".into()),
            Some("null".into()),
        ] {
            let mut object = serde_json::json!({ "key": "k", "size": 30 });
            if let Some(version) = version {
                object["versionId"] = version;
            }
            let body = serde_json::json!({ "Records": [{
                "eventName": "ObjectCreated:Put",
                "s3": { "bucket": { "name": "bucket" }, "object": object },
            }]});
            let crate::unit::Decoded::Read(cursors) =
                crate::unit::decode(&serde_json::to_vec(&body).unwrap(), &scope, packing)
            else {
                panic!("the notification is not read");
            };
            origins.extend(cursors.into_iter().flatten().map(|part| part.origin));
        }
        assert_eq!(origins.len(), 5);
        for origin in origins {
            let store = Arc::new(HeadStore::new(inner.clone(), 0));
            fetch_from(Arc::clone(&store), entry.clone(), origin.clone(), None).await;
            let seen = store.seen();
            assert!(!seen.is_empty());
            assert!(
                seen.iter().all(|(_, version, _)| version.is_none()),
                "{origin:?}: {seen:?}"
            );
        }
    }

    /// Pins that a failed precondition mid-read of a notification unit with
    /// no version HEADs the key once and ends the lane's stream with the
    /// overwrite at the content the HEAD reports, after the windows already
    /// read.
    #[tokio::test]
    async fn a_412_on_a_notification_read_heads_the_key_and_reports_the_overwrite() {
        let (inner, entry) = thirty_bytes().await;
        let store =
            Arc::new(HeadStore::new(inner, 2).head_answers(vec![Ok(meta(Some("new"), 99))]));
        let msgs = fetch_from(Arc::clone(&store), entry, notified(None), None).await;
        let chunks: Vec<&[u8]> = msgs
            .iter()
            .filter_map(|m| match m {
                ChunkMsg::Chunk(b) => Some(b.as_ref()),
                _ => None,
            })
            .collect();
        assert_eq!(chunks, [b"0123456789".as_slice()]);
        assert_eq!(store.heads(), 1);
        let SplitFailure::Overwritten(overwrite) = last_failure(msgs) else {
            panic!("not an overwrite");
        };
        assert_eq!(
            *overwrite,
            Overwrite {
                key: "k".to_owned(),
                etag: Some("\"new\"".to_owned()),
                size: 99,
                last_modified_ms: 1_760_000_000_000,
            }
        );
    }

    /// Pins that a failed precondition on a lister's unit or on a read that
    /// names a version poisons the unit as `EtagDrift` without a HEAD.
    #[tokio::test]
    async fn a_412_on_a_lister_or_versioned_read_poisons_without_a_head() {
        for origin in [Origin::Lister, notified(Some("v1"))] {
            let (inner, entry) = thirty_bytes().await;
            let store = Arc::new(HeadStore::new(inner, 1));
            let msgs = fetch_from(Arc::clone(&store), entry, origin.clone(), None).await;
            assert_eq!(store.heads(), 0, "{origin:?}");
            assert!(
                matches!(
                    last_failure(msgs),
                    SplitFailure::Poison(PoisonKind::EtagDrift, _)
                ),
                "{origin:?}"
            );
        }
    }

    /// Pins that a HEAD reporting the ETag the read was pinned to poisons
    /// the split as `EtagDrift`, since a unit at that ETag fails the same
    /// way.
    #[tokio::test]
    async fn a_head_reporting_the_pinned_etag_poisons_the_read() {
        let (inner, entry) = thirty_bytes().await;
        let pinned = entry.etag.clone().unwrap();
        let store = Arc::new(
            HeadStore::new(inner, 1)
                .head_answers(vec![Ok(meta(Some(pinned.trim_matches('"')), 30))]),
        );
        let entry = ObjectEntry {
            etag: Some(crate::unit::quoted(pinned.clone())),
            ..entry
        };
        let msgs = fetch_from(Arc::clone(&store), entry, notified(None), None).await;
        assert_eq!(store.heads(), 1);
        assert!(matches!(
            last_failure(msgs),
            SplitFailure::Poison(PoisonKind::EtagDrift, _)
        ));
    }

    /// Pins the text each split failure displays: a poison and an overwrite
    /// name the object, and a fatal failure shows its error.
    #[test]
    fn a_split_failure_displays_its_cause() {
        let poison = SplitFailure::Poison(PoisonKind::NotFound, "\"k\" is gone".into());
        assert_eq!(poison.to_string(), "split poisoned: \"k\" is gone");
        let overwrite = SplitFailure::Overwritten(Box::new(Overwrite {
            key: "data/k".into(),
            etag: None,
            size: 1,
            last_modified_ms: 0,
        }));
        assert_eq!(overwrite.to_string(), "\"data/k\" was overwritten");
        let fatal = SplitFailure::Fatal(SourceError::Client {
            class: ErrorClass::Fatal,
            reason: "denied".into(),
        });
        assert!(fatal.to_string().contains("denied"), "{fatal}");
    }

    /// Pins that a HEAD answering no ETag reports an overwrite that reads
    /// unpinned.
    #[tokio::test]
    async fn a_head_reporting_no_etag_reports_an_unpinned_overwrite() {
        let (inner, entry) = thirty_bytes().await;
        let store = Arc::new(HeadStore::new(inner, 1).head_answers(vec![Ok(meta(None, 7))]));
        let msgs = fetch_from(store, entry, notified(None), None).await;
        let SplitFailure::Overwritten(overwrite) = last_failure(msgs) else {
            panic!("not an overwrite");
        };
        assert_eq!((overwrite.etag, overwrite.size), (None, 7));
    }

    /// Pins that a HEAD after a 412 is classified as a read is: 404 poisons
    /// the split as `NotFound`, and 403 fails the pipeline, each reason
    /// naming the HEAD and the byte the read stopped at.
    #[tokio::test]
    async fn a_head_after_a_412_classifies_404_and_403_as_a_read_does() {
        let (inner, entry) = thirty_bytes().await;
        let store = Arc::new(HeadStore::new(inner.clone(), 2).head_answers(vec![Err(
            object_store::Error::NotFound {
                path: "k".into(),
                source: "gone".into(),
            },
        )]));
        let msgs = fetch_from(store, entry.clone(), notified(None), None).await;
        let SplitFailure::Poison(PoisonKind::NotFound, reason) = last_failure(msgs) else {
            panic!("a 404 on the HEAD must poison as not_found");
        };
        assert!(reason.contains("at byte 10"), "{reason}");
        assert!(reason.contains("the HEAD after"), "{reason}");
        let store = Arc::new(HeadStore::new(inner, 2).head_answers(vec![Err(
            object_store::Error::PermissionDenied {
                path: "k".into(),
                source: "denied".into(),
            },
        )]));
        let msgs = fetch_from(store, entry, notified(None), None).await;
        let SplitFailure::Fatal(e) = last_failure(msgs) else {
            panic!("a 403 on the HEAD must be fatal");
        };
        assert!(e.to_string().contains("the HEAD after"), "{e}");
    }

    /// A HEAD after a 412 that fails with a 64 KiB error, 404 or 403, keeps
    /// the HEAD's suffix behind the store's text cut to the bound.
    #[tokio::test]
    async fn a_failed_head_with_a_large_error_keeps_its_suffix() {
        let big = || "x".repeat(64 * 1024);
        let suffix = "(the HEAD after the store refused the pinned read)";
        let (inner, entry) = thirty_bytes().await;
        let store = Arc::new(HeadStore::new(inner.clone(), 2).head_answers(vec![Err(
            object_store::Error::NotFound {
                path: "k".into(),
                source: big().into(),
            },
        )]));
        let msgs = fetch_from(store, entry.clone(), notified(None), None).await;
        let SplitFailure::Poison(PoisonKind::NotFound, reason) = last_failure(msgs) else {
            panic!("a 404 on the HEAD must poison as not_found");
        };
        assert!(reason.ends_with(suffix), "{reason}");
        assert_cut(&reason);
        let store = Arc::new(HeadStore::new(inner, 2).head_answers(vec![Err(
            object_store::Error::PermissionDenied {
                path: "k".into(),
                source: big().into(),
            },
        )]));
        let msgs = fetch_from(store, entry, notified(None), None).await;
        let SplitFailure::Fatal(e) = last_failure(msgs) else {
            panic!("a 403 on the HEAD must be fatal");
        };
        let reason = e.to_string();
        assert!(reason.ends_with(suffix), "{reason}");
        assert_cut(&reason);
    }

    /// Pins that a ranged GET a notification unit's read is refused with 416,
    /// as RustFS answers past the end of a smaller new object, HEADs the key
    /// and reports the overwrite, without retrying the GET.
    #[tokio::test]
    async fn a_416_on_a_notification_read_heads_the_key_and_reports_the_overwrite() {
        let (inner, entry) = thirty_bytes().await;
        let store = Arc::new(
            HeadStore::new(inner, 2)
                .refusing_the_range()
                .head_answers(vec![Ok(meta(Some("new"), 5))]),
        );
        let msgs = fetch_from(Arc::clone(&store), entry, notified(None), None).await;
        let SplitFailure::Overwritten(overwrite) = last_failure(msgs) else {
            panic!("a 416 on a notification read must report an overwrite");
        };
        assert_eq!(overwrite.size, 5);
        assert_eq!(store.heads(), 1);
        assert_eq!(store.seen().len(), 3, "two GETs and the HEAD, no retry");
    }

    /// Pins that a transient HEAD failure after a 412 is retried and counted
    /// in `get_retries`, and that a HEAD failing past the attempt budget
    /// poisons the unit as `RetriesExhausted`.
    #[tokio::test(start_paused = true)]
    async fn a_retryable_head_after_a_412_is_retried() {
        let transient = || object_store::Error::Generic {
            store: "head",
            source: "503".into(),
        };
        let (inner, entry) = thirty_bytes().await;
        let retried = Arc::new(AtomicU64::new(0));
        let store = Arc::new(
            HeadStore::new(inner.clone(), 1)
                .head_answers(vec![Err(transient()), Ok(meta(Some("new"), 30))]),
        );
        let msgs = fetch_from(
            Arc::clone(&store),
            entry.clone(),
            notified(None),
            Some(spate_core::metrics::Counter::from_arc(Arc::clone(&retried))),
        )
        .await;
        assert!(matches!(last_failure(msgs), SplitFailure::Overwritten(_)));
        assert_eq!(store.heads(), 2);
        assert_eq!(retried.load(Ordering::Relaxed), 1);

        let failing = (0..MAX_ATTEMPTS).map(|_| Err(transient())).collect();
        let store = Arc::new(HeadStore::new(inner, 1).head_answers(failing));
        let msgs = fetch_from(Arc::clone(&store), entry, notified(None), None).await;
        assert!(matches!(
            last_failure(msgs),
            SplitFailure::Poison(PoisonKind::RetriesExhausted, _)
        ));
        assert_eq!(store.heads(), MAX_ATTEMPTS as usize);
    }
}
