//! The async edge: the shared listing helper and one fetcher task per
//! gained split, reading its member objects' bytes to the pipeline thread.
//!
//! Runs on the pipeline's I/O runtime. Every channel send `await`s, so a
//! fetcher never blocks a runtime worker and sink workers sharing the
//! runtime keep running while lanes are back-pressured.
//!
//! Each ETag-pinned object is read as a sequence of **bounded ranged GETs,
//! each drained fully into memory before any chunk is forwarded**. The
//! connection is released between windows, so a back-pressured lane parks on
//! buffered bytes, never an idle S3 body that the client's request timeout
//! (or an intermediary) would reset mid-stream. See [`stream_object`].
//!
//! # Determinism
//!
//! [`ObjectStore::list`](object_store::ObjectStore::list) guarantees no
//! ordering, so [`list_all`] collects the listing and sorts it by key
//! before the planner packs it. A fetcher never lists: it reads its
//! split's member objects in descriptor order, so every (ordinal, record
//! index) offset is minted against the immutable descriptor, identical
//! under every owner of the split.
//!
//! # Retries
//!
//! Transient GET failures are retried inside the fetcher with capped
//! exponential backoff, resuming from the last delivered byte with a
//! ranged GET **conditioned on the object's ETag**, so a splice of two
//! different object versions is impossible. Persistent failure (attempt
//! budget exhausted) and the object-level non-retryable classes (missing
//! key, failed precondition) **poison the split**, which is handed back for
//! a peer to retry and quarantined at the attempt cap, while
//! credentials/configuration failures stay fatal for the pipeline. An
//! object whose store returns no ETag cannot be resumed mid-stream safely
//! and poisons the split on a mid-object break instead.

use crate::error::classify;
use crate::split_ctx::PoisonKind;
use bytes::Bytes;
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};
use spate_core::coordination::SplitId;
use spate_core::error::{ErrorClass, SourceError};
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

/// How a split read failed. The classification decides whether one object
/// condemns the split (poison, handled by the coordinator) or the whole
/// pipeline (fatal).
#[derive(Debug)]
pub(crate) enum SplitFailure {
    /// Object-level failure: a member was deleted after planning, was
    /// overwritten under its ETag pin, is corrupt, or exhausted its read
    /// budget. The split is handed back for a peer to retry; it never
    /// fails the pipeline directly.
    Poison(PoisonKind, String),
    /// Instance- or configuration-level failure (credentials, permissions,
    /// client misconfiguration): terminal for the pipeline.
    Fatal(SourceError),
}

impl std::fmt::Display for SplitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SplitFailure::Poison(_, reason) => write!(f, "split poisoned: {reason}"),
            SplitFailure::Fatal(e) => write!(f, "{e}"),
        }
    }
}

/// Collect and sort the full listing under `prefix`. Memory is
/// O(number of keys), held for the duration of one plan run, which for an
/// open plan is every replan tick, not startup alone.
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
    pub(crate) split: SplitId,
    pub(crate) store: Arc<dyn ObjectStore>,
    /// The split's member objects, ordinal order.
    pub(crate) slice: Arc<Vec<ObjectEntry>>,
    /// First ordinal to fetch (the committed watermark's object).
    pub(crate) start_ordinal: u32,
    /// Committed ETag of the resume object, pinning its content.
    pub(crate) resume_etag: Option<String>,
    /// Upper bound on a single [`ChunkMsg::Chunk`].
    pub(crate) chunk_bytes: usize,
    /// Upper bound on one bounded ranged GET (the per-lane read-ahead window,
    /// the source's `prefetch_bytes`). Each window is drained fully into memory
    /// before any chunk is forwarded, so the S3 connection is released before a
    /// back-pressured hand-off; see [`stream_object`].
    pub(crate) range_bytes: usize,
    pub(crate) tx: mpsc::Sender<ChunkMsg>,
    /// Backpressure pause (set by `Source::pause`): checked between sends.
    pub(crate) pause: Arc<AtomicBool>,
    /// Cooperative-revocation stop (set by `SplitCtx::begin_revoke`): checked
    /// only at object boundaries, **never mid-object**. The
    /// *between-objects* pause wait checks it; the mid-object pause waits
    /// ignore it. On stop the fetcher returns, dropping `tx` at a boundary.
    /// That channel close is indistinguishable from a natural end of slice,
    /// so the lane takes its clean end-of-input path and never the
    /// mid-object Fatal path (see [`run_fetcher`]).
    pub(crate) stop: Arc<AtomicBool>,
    /// First backoff step (doubles per attempt, capped; tests shrink it).
    pub(crate) retry_base: Duration,
    /// `spate_s3_source_get_retries_total`, when metrics are attached.
    pub(crate) retries: Option<spate_core::metrics::Counter>,
}

/// How many attempts one object GET, and in the planner one prefix
/// listing, gets before escalating. Failing fast (a poisoned split, a
/// fatal plan) beats a backfill wedged invisibly in an endless retry
/// loop.
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
        stop,
        retry_base,
        retries,
    } = params;

    for (ordinal, entry) in slice.iter().enumerate().skip(start_ordinal as usize) {
        // Cooperative revocation: stop only ever takes effect here, at an object
        // boundary, before any `ObjectStart` for this object. Dropping `tx`
        // now closes the channel exactly as a natural end of slice would, so
        // the lane's end-of-input decision (`set_terminal` + waker) fires
        // unmodified and the mid-object Fatal path is never reachable.
        if stop.load(Ordering::Relaxed) {
            return;
        }
        // The planner's member cap bounds split sizes, so ordinals always
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
        if pause_gate(&pause, Some(&stop), &tx).await.is_err() {
            return;
        }
        let started = tx
            .send(ChunkMsg::ObjectStart {
                ordinal,
                key: entry.key.clone(),
                last_modified_ms: entry.last_modified_ms,
            })
            .await;
        if started.is_err() {
            return; // lane dropped: shutdown
        }
        match stream_object(
            &split,
            &store,
            entry,
            pinned_etag.as_deref(),
            range_bytes,
            chunk_bytes,
            &tx,
            &pause,
            retry_base,
            retries.as_ref(),
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
        if tx.send(ChunkMsg::ObjectEnd).await.is_err() {
            return;
        }
    }
    // Slice exhausted: dropping tx closes the channel, which is the lane's
    // end-of-slice signal.
}

/// Wait while paused. Fails when the lane is gone (channel closed), so a
/// paused fetcher still exits promptly on shutdown.
///
/// `stop` is passed `Some` only by the **between-objects** caller: a
/// cooperative revocation that lands while the fetcher is back-pressured must be
/// noticed promptly, and returning here is safe because no object is open. The
/// mid-object callers pass `None`. A stop must never end the wait between an
/// `ObjectStart` and its `ObjectEnd` (that would close the channel with the
/// lane's object still open, tripping its Fatal path); mid-object they wait
/// for unpause and let the boundary check act.
async fn pause_gate(
    pause: &AtomicBool,
    stop: Option<&AtomicBool>,
    tx: &mpsc::Sender<ChunkMsg>,
) -> Result<(), ()> {
    while pause.load(Ordering::Relaxed) {
        if tx.is_closed() {
            return Err(());
        }
        if let Some(stop) = stop
            && stop.load(Ordering::Relaxed)
        {
            return Err(());
        }
        tokio::time::sleep(PAUSE_POLL).await;
    }
    Ok(())
}

/// Stream one object's bytes as bounded [`ChunkMsg::Chunk`]s. `Ok(true)` =
/// complete, `Ok(false)` = receiver dropped (shutdown), `Err` = terminal
/// failure.
///
/// When the object's content is pinned by an ETag (S3 always supplies one, as
/// does `object_store`'s `LocalFileSystem`) the read is a sequence of **bounded
/// ranged GETs, each drained fully into memory before any chunk is forwarded**
/// ([`stream_object_ranged`]), so a lane parked on backpressure only holds
/// buffered bytes, never an idle GET body that S3 (or an intermediary)
/// could reset or time out mid-stream. Without an ETag a resumed ranged read
/// could splice two object versions, so such stores keep a single continuous
/// stream ([`stream_object_streaming`]).
#[expect(
    clippy::too_many_arguments,
    reason = "internal seam between run_fetcher and the read loops"
)]
async fn stream_object(
    split: &SplitId,
    store: &Arc<dyn ObjectStore>,
    entry: &ObjectEntry,
    pinned_etag: Option<&str>,
    range_bytes: usize,
    chunk_bytes: usize,
    tx: &mpsc::Sender<ChunkMsg>,
    pause: &AtomicBool,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
) -> Result<bool, SplitFailure> {
    match pinned_etag {
        Some(etag) => {
            stream_object_ranged(
                split,
                store,
                entry,
                etag,
                range_bytes,
                chunk_bytes,
                tx,
                pause,
                retry_base,
                retries,
            )
            .await
        }
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
            )
            .await
        }
    }
}

/// Pinned read path: walk the object in `range_bytes` windows, each a
/// short-lived ranged GET **fully buffered before the hand-off**. The
/// connection is released the moment a window is read, so a paused or
/// back-pressured lane holds only `range_bytes` of buffered read-ahead and
/// never an idle body left un-polled past the client's request timeout. The
/// ETag pin keeps the multi-GET read splice-safe: an overwrite between windows
/// trips the `if_match` precondition instead of blending versions.
#[expect(
    clippy::too_many_arguments,
    reason = "internal read loop, mirrors stream_object_streaming"
)]
async fn stream_object_ranged(
    split: &SplitId,
    store: &Arc<dyn ObjectStore>,
    entry: &ObjectEntry,
    etag: &str,
    range_bytes: usize,
    chunk_bytes: usize,
    tx: &mpsc::Sender<ChunkMsg>,
    pause: &AtomicBool,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
) -> Result<bool, SplitFailure> {
    let path = Path::from(entry.key.as_str());
    let window = range_bytes.max(1) as u64;
    let mut delivered: u64 = 0;
    let mut attempt: u32 = 0;

    while delivered < entry.size {
        // Saturating: `entry.size` is remote listing data and may be
        // adversarially close to u64::MAX.
        let end = delivered.saturating_add(window).min(entry.size);
        let options = GetOptions {
            if_match: Some(etag.to_owned()),
            range: Some(GetRange::Bounded(delivered..end)),
            ..Default::default()
        };
        // One bounded request, drained fully into memory at network speed. The
        // connection is released here, *before* the possibly-long hand-off
        // below, so backpressure never leaves an idle S3 body open.
        let buffered = match store.get_opts(&path, options).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => bytes,
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
                    continue;
                }
            },
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
                continue;
            }
        };
        let read = buffered.len() as u64;
        if read == 0 {
            // A conforming store never returns an empty bounded range; guard
            // anyway so a misbehaving one poisons the object rather than
            // spinning.
            return Err(SplitFailure::Poison(
                PoisonKind::Undecodable,
                format!(
                    "split {split}: ranged read of \"{}\" at byte {delivered} returned no bytes",
                    entry.key
                ),
            ));
        }
        // Forward the buffered window. No connection is open here, so a paused /
        // back-pressured lane only parks on bytes already in memory.
        let mut bytes = buffered;
        while !bytes.is_empty() {
            let take = bytes.len().min(chunk_bytes);
            let chunk = bytes.split_to(take);
            if pause_gate(pause, None, tx).await.is_err() {
                return Ok(false);
            }
            if tx.send(ChunkMsg::Chunk(chunk)).await.is_err() {
                return Ok(false);
            }
        }
        delivered += read;
        // A completed window is progress: reset the retry budget so an object
        // larger than one failure-free window still finishes.
        attempt = 0;
    }
    Ok(true)
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
    split: &SplitId,
    store: &Arc<dyn ObjectStore>,
    entry: &ObjectEntry,
    chunk_bytes: usize,
    tx: &mpsc::Sender<ChunkMsg>,
    pause: &AtomicBool,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
) -> Result<bool, SplitFailure> {
    let path = Path::from(entry.key.as_str());
    let mut delivered: u64 = 0;
    let mut attempt: u32 = 0;

    'attempts: loop {
        // In this fallback `delivered` is always 0: a mid-object stream error
        // is fatal below (no ETag to pin a resumed read), so a retry never
        // resumes partway. The guard therefore only fires for a zero-length
        // object, and every GET re-reads the whole object un-ranged.
        if delivered >= entry.size {
            return Ok(true);
        }
        let options = GetOptions::default();
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
                        if pause_gate(pause, None, tx).await.is_err() {
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
                                     {delivered}: {e}",
                                    entry.key
                                ),
                            }));
                        }
                        return Err(SplitFailure::Poison(
                            PoisonKind::Undecodable,
                            format!(
                                "split {split}: read of \"{}\" failed mid-object at byte \
                                 {delivered} and the store reports no ETag to pin a \
                                 resumed read to: {e}",
                                entry.key
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

/// Back off and bump the attempt counter for a retryable error; escalate
/// non-retryable classes and exhausted budgets.
///
/// Escalation splits by scope: credentials/config failures are pipeline
/// [`Fatal`](SplitFailure::Fatal). Everything else is object-level
/// [`Poison`](SplitFailure::Poison), handled by handing the split back: a
/// deleted or overwritten object (`NotFound`, a failed `if_match`
/// precondition), or a read that keeps failing past the attempt budget.
async fn retry_or_fail(
    split: &SplitId,
    entry: &ObjectEntry,
    e: &object_store::Error,
    delivered: u64,
    attempt: &mut u32,
    retry_base: Duration,
    retries: Option<&spate_core::metrics::Counter>,
) -> Result<(), SplitFailure> {
    if classify(e) != ErrorClass::Retryable {
        let reason = format!(
            "split {split}: reading \"{}\" failed at byte {delivered}: {e}",
            entry.key
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
             {MAX_ATTEMPTS} attempts: {e}",
                entry.key
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
        error = %e,
        "transient object read failure; backing off and resuming with a ranged get"
    );
    tokio::time::sleep(backoff).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{GetResult, GetResultPayload, ObjectStoreExt as _, PutPayload};
    use std::fmt;
    use std::sync::atomic::AtomicU32;

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
    /// retryable error, and cutting the first `cut_streams` result streams
    /// after `cut_after` bytes (with a retryable error, or `PermissionDenied`
    /// when `cut_fatal` is set). Records every requested range in `ranges`
    /// for read-path assertions.
    #[derive(Debug)]
    struct FlakyStore {
        inner: InMemory,
        fail_gets: AtomicU32,
        cut_streams: AtomicU32,
        cut_after: usize,
        cut_fatal: bool,
        gets: AtomicU32,
        ranges: std::sync::Mutex<Vec<RangeKind>>,
        /// Payload streams drained to completion (a cut or abandoned body
        /// never counts). This is the observable that separates "window
        /// fully buffered before hand-off" from "connection held open".
        bodies_drained: Arc<AtomicU32>,
    }

    impl FlakyStore {
        fn new(inner: InMemory) -> FlakyStore {
            FlakyStore {
                inner,
                fail_gets: AtomicU32::new(0),
                cut_streams: AtomicU32::new(0),
                cut_after: 0,
                cut_fatal: false,
                gets: AtomicU32::new(0),
                ranges: std::sync::Mutex::new(Vec::new()),
                bodies_drained: Arc::new(AtomicU32::new(0)),
            }
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

        fn cut_error(fatal: bool) -> object_store::Error {
            if fatal {
                object_store::Error::PermissionDenied {
                    path: "k".into(),
                    source: "injected permission loss".into(),
                }
            } else {
                Self::generic("injected stream cut")
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
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.ranges.lock().unwrap().push(range_kind(&options.range));
            if self
                .fail_gets
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(Self::generic("injected get failure"));
            }
            let result = self.inner.get_opts(location, options).await?;
            if self
                .cut_streams
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                let cut_after = self.cut_after;
                let cut_fatal = self.cut_fatal;
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
                                vec![Err(Self::cut_error(cut_fatal))]
                            } else {
                                let take = bytes.len().min(cut_after - sent);
                                sent += take;
                                let mut out = vec![Ok(bytes.slice(0..take))];
                                if sent >= cut_after {
                                    out.push(Err(Self::cut_error(cut_fatal)));
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
            split: SplitId::new("s3-test").unwrap(),
            store,
            slice: Arc::new(slice),
            start_ordinal,
            resume_etag: None,
            chunk_bytes,
            range_bytes,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
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
        let store: Arc<dyn ObjectStore> =
            Arc::new(seeded(&[("p/a", b"first"), ("p/b", b"second")]).await);
        let slice = listed(&store).await;
        let msgs = collect_fetch(store, slice, 1, 64, 64).await;
        assert_eq!(
            assembled(&msgs),
            vec![("p/b".to_string(), b"second".to_vec())]
        );
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
            split: SplitId::new("s3-test").unwrap(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
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
            split: SplitId::new("s3-test").unwrap(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
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

    #[tokio::test]
    async fn pause_halts_fetching_and_resume_continues() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"abcdefgh")]).await);
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(1); // tiny: sends interleave with pauses
        let pause = Arc::new(AtomicBool::new(true));
        let params = FetcherParams {
            split: SplitId::new("s3-test").unwrap(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 2,
            range_bytes: 64,
            tx,
            pause: Arc::clone(&pause),
            stop: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
        };
        tokio::spawn(run_fetcher(params));
        // Paused before the first send: nothing arrives.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            rx.try_recv().is_err(),
            "paused fetcher must not deliver messages"
        );
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
            split: SplitId::new("s3-test").unwrap(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 8,
            range_bytes: 4096,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
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

    #[tokio::test]
    async fn a_backpressured_lane_buffers_one_window_and_fetches_no_further() {
        // 16-byte object, 8-byte windows → two windows. With a capacity-1
        // channel that nothing drains, the fetcher must read the FIRST window
        // in full, proving the connection is released before the hand-off,
        // then block on the send, never starting the second window. That
        // bounds peak per-lane memory to one window under backpressure.
        let flaky = Arc::new(FlakyStore::new(
            seeded(&[("p/a", b"0123456789abcdef")]).await,
        ));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let slice = listed(&store).await;
        let (tx, _rx) = mpsc::channel(1); // ObjectStart fills the one slot; never drained
        let params = FetcherParams {
            split: SplitId::new("s3-test").unwrap(),
            store: Arc::clone(&store),
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 4,
            range_bytes: 8,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
        };
        let task = tokio::spawn(run_fetcher(params));
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            flaky.recorded_ranges(),
            vec![RangeKind::Bounded(0, 8)],
            "one window fetched: the second is not started while the \
             hand-off is blocked"
        );
        // Issuance alone can't distinguish "window fully buffered before
        // the hand-off" from "body held open while blocked on the send":
        // the drain marker fires only when the payload stream is read to
        // its natural end, so a regression to lazy streaming (connection
        // pinned during backpressure) fails here.
        assert_eq!(
            flaky.bodies_drained(),
            1,
            "the first window's body must be fully drained (connection \
             released) before the fetcher blocks on the hand-off"
        );
        task.abort();
    }

    /// Run a fetcher over one no-ETag object whose stream is cut after
    /// `cut_after` bytes, returning the terminal `LaneFailed` failure.
    async fn no_etag_mid_stream_failure(cut_fatal: bool) -> SplitFailure {
        let flaky = FlakyStore {
            cut_after: 4,
            cut_fatal,
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

    // ----------------------------------------- cooperative-revocation stop --

    #[tokio::test]
    async fn a_preset_stop_ends_the_fetcher_before_reading_anything() {
        // Stop set before the fetcher starts: the boundary check at the top
        // of the loop fires before any `ObjectStart`, so `tx` is dropped with
        // no object open. The lane reads a closed channel and takes its clean
        // end-of-input path; a revocation at a boundary is indistinguishable
        // from a natural end of slice.
        let store: Arc<dyn ObjectStore> =
            Arc::new(seeded(&[("p/a", b"aaaa"), ("p/b", b"bbbb")]).await);
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(64);
        let params = FetcherParams {
            split: SplitId::new("s3-test").unwrap(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(true)),
            retry_base: Duration::from_millis(1),
            retries: None,
        };
        let task = tokio::spawn(run_fetcher(params));
        let mut msgs = Vec::new();
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        task.await.unwrap();
        assert!(
            msgs.is_empty(),
            "a fetcher that starts stopped reads nothing: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn a_paused_fetcher_notices_the_stop_and_closes_cleanly() {
        // A back-pressured fetcher parked in the pause wait must give up its
        // split promptly when a revocation sets `stop`, closing the channel at
        // the (pre-first-object) boundary rather than waiting out the pause.
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"aaaa")]).await);
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(8);
        let pause = Arc::new(AtomicBool::new(true));
        let stop = Arc::new(AtomicBool::new(false));
        let params = FetcherParams {
            split: SplitId::new("s3-test").unwrap(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            range_bytes: 64,
            tx,
            pause: Arc::clone(&pause),
            stop: Arc::clone(&stop),
            retry_base: Duration::from_millis(1),
            retries: None,
        };
        let task = tokio::spawn(run_fetcher(params));
        // Parked on the pause: nothing arrives.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(rx.try_recv().is_err(), "a paused fetcher delivers nothing");
        // Stop while paused: the fetcher must exit without ever unpausing. A
        // timeout draining the channel would mean the stop was ignored.
        stop.store(true, Ordering::Relaxed);
        let mut msgs = Vec::new();
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(m) = rx.recv().await {
                msgs.push(m);
            }
        })
        .await;
        assert!(
            drained.is_ok(),
            "a paused fetcher must notice the stop promptly"
        );
        task.await.unwrap();
        assert!(msgs.is_empty(), "no object was ever started: {msgs:?}");
    }

    #[tokio::test]
    async fn a_stop_finishes_the_open_object_then_cuts_at_the_boundary() {
        // A stop observed mid-object must never close the channel there (that
        // trips the lane's mid-object Fatal path): the fetcher finishes the
        // open object and returns only at the next boundary, before starting
        // the next object.
        let store: Arc<dyn ObjectStore> =
            Arc::new(seeded(&[("p/a", b"0123456789"), ("p/b", b"bbbb")]).await);
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(1); // interleave sends with receipts
        let pause = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let params = FetcherParams {
            split: SplitId::new("s3-test").unwrap(),
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 2,
            range_bytes: 64,
            tx,
            pause: Arc::clone(&pause),
            stop: Arc::clone(&stop),
            retry_base: Duration::from_millis(1),
            retries: None,
        };
        let task = tokio::spawn(run_fetcher(params));
        // Take the first object's start and one data chunk, leaving the
        // fetcher mid-object, then pause and stop it.
        let m0 = rx.recv().await.unwrap();
        assert!(matches!(m0, ChunkMsg::ObjectStart { ordinal: 0, .. }));
        let m1 = rx.recv().await.unwrap();
        assert!(matches!(m1, ChunkMsg::Chunk(_)), "mid the first object");
        pause.store(true, Ordering::Relaxed);
        stop.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(40)).await;
        pause.store(false, Ordering::Relaxed); // let it drain to the boundary
        let mut msgs = vec![m0, m1];
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        task.await.unwrap();
        // No failure, and the first object completed intact while the second
        // never started: the stop took effect only at the boundary.
        assert!(
            !msgs.iter().any(|m| matches!(m, ChunkMsg::LaneFailed(_))),
            "a boundary stop is a clean channel close, never a failure"
        );
        assert_eq!(
            assembled(&msgs),
            vec![("p/a".to_string(), b"0123456789".to_vec())],
            "the open object finishes; the boundary stop precedes the next object"
        );
    }
}
