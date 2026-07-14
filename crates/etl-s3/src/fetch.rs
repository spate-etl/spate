//! The async edge: one startup listing, then one fetcher task per lane
//! streaming object bytes to the pipeline thread.
//!
//! Runs on the pipeline's I/O runtime. Every channel send `await`s — a
//! fetcher never blocks a runtime worker, so sink workers sharing the
//! runtime keep running while lanes are back-pressured.
//!
//! # Determinism
//!
//! [`ObjectStore::list`](object_store::ObjectStore::list) guarantees no
//! ordering, so the listing is collected once and sorted by key; the
//! sorted listing is dealt round-robin across lanes. Both steps are pure,
//! so a restart over an unchanged key set reproduces every lane slice —
//! and with it every (ordinal, record index) offset — exactly.
//!
//! # Retries
//!
//! Transient GET failures are retried inside the fetcher with capped
//! exponential backoff, resuming from the last delivered byte with a
//! ranged GET **conditioned on the object's ETag** — a splice of two
//! different object versions is impossible. An object whose store returns
//! no ETag cannot be resumed mid-stream safely and fails the lane instead.
//! Persistent failure (attempt budget exhausted) and non-retryable classes
//! (missing key, failed precondition, auth) fail the lane: the pipeline
//! restarts and replays from the committed watermark.

use crate::error::classify;
use crate::offset::{MAX_ORDINAL, Position};
use crate::store::{LaneState, Manifest, chain_hash};
use bytes::Bytes;
use etl_core::error::{ErrorClass, SourceError};
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};
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
    /// Last-modified time (ms since epoch) — the records' event time.
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
    /// The lane cannot continue; the error is terminal for the pipeline.
    LaneFailed(SourceError),
}

/// Collect and sort the full listing under `prefix`. Memory is
/// O(number of keys) — transient, at startup only.
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

/// Deal the sorted listing round-robin into `lanes` slices. Ordinals are
/// indexes into each slice.
pub(crate) fn assign_lanes(
    entries: Vec<ObjectEntry>,
    lanes: u32,
) -> Result<Vec<Vec<ObjectEntry>>, SourceError> {
    let mut slices: Vec<Vec<ObjectEntry>> = (0..lanes).map(|_| Vec::new()).collect();
    for (i, entry) in entries.into_iter().enumerate() {
        slices[i % lanes as usize].push(entry);
    }
    for (lane, slice) in slices.iter().enumerate() {
        if slice.len() > MAX_ORDINAL as usize + 1 {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!(
                    "lane {lane} would hold {} objects, above the composite-offset \
                     limit of {}; raise `lanes` to spread the listing wider",
                    slice.len(),
                    MAX_ORDINAL as u64 + 1,
                ),
            });
        }
    }
    Ok(slices)
}

/// Validate a loaded manifest against the fresh listing: every committed
/// position must still mean what it meant when it was written.
pub(crate) fn validate_resume(
    slices: &[Vec<ObjectEntry>],
    manifest: &Manifest,
) -> Result<(), SourceError> {
    for (&lane, state) in &manifest.lane_states {
        let slice = slices.get(lane as usize).ok_or_else(|| {
            drift_error(
                lane,
                format!(
                    "manifest has lane {lane} but only {} lanes exist",
                    slices.len()
                ),
            )
        })?;
        validate_lane_resume(lane, slice, state)?;
    }
    Ok(())
}

fn validate_lane_resume(
    lane: u32,
    slice: &[ObjectEntry],
    state: &LaneState,
) -> Result<(), SourceError> {
    let ordinal = state.ordinal as usize;
    let Some(entry) = slice.get(ordinal) else {
        return Err(drift_error(
            lane,
            format!(
                "committed ordinal {ordinal} but the lane's slice now holds only {} \
                 objects — keys were removed below the committed position",
                slice.len()
            ),
        ));
    };
    if entry.key != state.key {
        return Err(drift_error(
            lane,
            format!(
                "committed ordinal {ordinal} was \"{}\", the listing now has \"{}\" \
                 there — keys were added or removed below the committed position",
                state.key, entry.key
            ),
        ));
    }
    let mut hash = 0u64;
    for e in &slice[..=ordinal] {
        hash = chain_hash(hash, &e.key);
    }
    if hash != state.keys_hash {
        return Err(drift_error(
            lane,
            format!(
                "the key sequence below committed ordinal {ordinal} changed \
                 (rolling hash mismatch) even though the key at the ordinal matches"
            ),
        ));
    }
    if let (Some(listed), Some(committed)) = (&entry.etag, &state.etag)
        && listed != committed
    {
        return Err(drift_error(
            lane,
            format!(
                "object \"{}\" at committed ordinal {ordinal} was overwritten \
                 (etag {committed} committed, {listed} listed) — its record \
                 indexes are no longer meaningful",
                entry.key
            ),
        ));
    }
    // A mid-object watermark replays the object and discards the committed
    // record count, which is only sound if the content provably didn't
    // change. Without a committed ETag to pin the re-read to, a same-key
    // overwrite could silently skip records — refuse instead.
    if state.etag.is_none() && Position::decode(state.watermark).record > 0 {
        return Err(SourceError::Client {
            class: ErrorClass::Fatal,
            reason: format!(
                "lane {lane}: the committed position is mid-object (\"{}\", {} records \
                 committed) but the store reported no ETag at commit time, so the \
                 object's content cannot be verified on resume; start a fresh \
                 checkpoint to re-run over the current listing",
                entry.key,
                Position::decode(state.watermark).record
            ),
        });
    }
    Ok(())
}

fn drift_error(lane: u32, detail: String) -> SourceError {
    SourceError::Client {
        class: ErrorClass::Fatal,
        reason: format!(
            "lane {lane}: the object listing changed under the checkpoint ({detail}); \
             the backfill's key set must stay frozen — start a fresh checkpoint to \
             re-run over the current listing"
        ),
    }
}

/// Everything one fetcher task needs.
pub(crate) struct FetcherParams {
    pub(crate) lane: u32,
    pub(crate) store: Arc<dyn ObjectStore>,
    /// The lane's full slice, ordinal order.
    pub(crate) slice: Arc<Vec<ObjectEntry>>,
    /// First ordinal to fetch (the committed watermark's object).
    pub(crate) start_ordinal: u32,
    /// Committed ETag of the resume object, pinning its content.
    pub(crate) resume_etag: Option<String>,
    /// Upper bound on a single [`ChunkMsg::Chunk`].
    pub(crate) chunk_bytes: usize,
    pub(crate) tx: mpsc::Sender<ChunkMsg>,
    /// Backpressure pause (set by `Source::pause`): checked between sends.
    pub(crate) pause: Arc<AtomicBool>,
    /// First backoff step (doubles per attempt, capped; tests shrink it).
    pub(crate) retry_base: Duration,
    /// `etl_s3_source_get_retries_total`, when metrics are attached.
    pub(crate) retries: Option<etl_core::metrics::Counter>,
}

/// How many attempts one object GET — and, in the source, one startup
/// listing or manifest load — gets before failing. Failing fast (and
/// replaying from the watermark after a restart) beats a backfill wedged
/// invisibly in an endless retry loop.
pub(crate) const MAX_ATTEMPTS: u32 = 8;
pub(crate) const BACKOFF_CAP: Duration = Duration::from_secs(5);
const PAUSE_POLL: Duration = Duration::from_millis(25);

/// Stream the lane's slice. Ends by dropping `tx` (the lane observes a
/// closed channel = end of slice) or after a `LaneFailed`.
pub(crate) async fn run_fetcher(params: FetcherParams) {
    let FetcherParams {
        lane,
        store,
        slice,
        start_ordinal,
        resume_etag,
        chunk_bytes,
        tx,
        pause,
        retry_base,
        retries,
    } = params;

    for (ordinal, entry) in slice.iter().enumerate().skip(start_ordinal as usize) {
        // The listing guard bounds slice lengths, so ordinals always fit.
        let ordinal = ordinal as u32;
        // Pin the GET to the exact content the offsets were (or will be)
        // minted against: the committed ETag for the resume object, the
        // listed ETag otherwise.
        let pinned_etag = if ordinal == start_ordinal && resume_etag.is_some() {
            resume_etag.clone()
        } else {
            entry.etag.clone()
        };
        if pause_gate(&pause, &tx).await.is_err() {
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
            lane,
            &store,
            entry,
            pinned_etag.as_deref(),
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
async fn pause_gate(pause: &AtomicBool, tx: &mpsc::Sender<ChunkMsg>) -> Result<(), ()> {
    while pause.load(Ordering::Relaxed) {
        if tx.is_closed() {
            return Err(());
        }
        tokio::time::sleep(PAUSE_POLL).await;
    }
    Ok(())
}

/// Stream one object's bytes as bounded chunks. `Ok(true)` = complete,
/// `Ok(false)` = receiver dropped (shutdown), `Err` = terminal failure.
#[expect(
    clippy::too_many_arguments,
    reason = "internal seam between run_fetcher and the retry loop"
)]
async fn stream_object(
    lane: u32,
    store: &Arc<dyn ObjectStore>,
    entry: &ObjectEntry,
    pinned_etag: Option<&str>,
    chunk_bytes: usize,
    tx: &mpsc::Sender<ChunkMsg>,
    pause: &AtomicBool,
    retry_base: Duration,
    retries: Option<&etl_core::metrics::Counter>,
) -> Result<bool, SourceError> {
    let path = Path::from(entry.key.as_str());
    let mut delivered: u64 = 0;
    let mut attempt: u32 = 0;

    'attempts: loop {
        // Delivered everything the listing said the object holds — a rare
        // retry landing exactly at the end needs no further GET.
        if delivered >= entry.size {
            return Ok(true);
        }
        let options = GetOptions {
            if_match: pinned_etag.map(str::to_owned),
            range: (delivered > 0).then_some(GetRange::Offset(delivered)),
            ..Default::default()
        };
        let result = match store.get_opts(&path, options).await {
            Ok(r) => r,
            Err(e) => {
                retry_or_fail(
                    lane,
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
                    // Resuming mid-object requires the ETag pin; without
                    // one, a silent overwrite between attempts could
                    // splice two object versions.
                    if delivered > 0 && pinned_etag.is_none() {
                        return Err(SourceError::Client {
                            class: ErrorClass::Fatal,
                            reason: format!(
                                "lane {lane}: read of \"{}\" failed mid-object at byte \
                                 {delivered} and the store reports no ETag to pin a \
                                 resumed read to: {e}",
                                entry.key
                            ),
                        });
                    }
                    retry_or_fail(
                        lane,
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
async fn retry_or_fail(
    lane: u32,
    entry: &ObjectEntry,
    e: &object_store::Error,
    delivered: u64,
    attempt: &mut u32,
    retry_base: Duration,
    retries: Option<&etl_core::metrics::Counter>,
) -> Result<(), SourceError> {
    if classify(e) != ErrorClass::Retryable {
        return Err(SourceError::Client {
            class: ErrorClass::Fatal,
            reason: format!(
                "lane {lane}: reading \"{}\" failed at byte {delivered}: {e}",
                entry.key
            ),
        });
    }
    *attempt += 1;
    if let Some(c) = retries {
        c.increment(1);
    }
    if *attempt >= MAX_ATTEMPTS {
        return Err(SourceError::Client {
            class: ErrorClass::Fatal,
            reason: format!(
                "lane {lane}: reading \"{}\" still failing at byte {delivered} after \
                 {MAX_ATTEMPTS} attempts: {e}",
                entry.key
            ),
        });
    }
    let backoff = retry_base
        .saturating_mul(1 << (*attempt - 1).min(16))
        .min(BACKOFF_CAP);
    tracing::warn!(
        lane,
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

    #[test]
    fn round_robin_assignment_is_deterministic() {
        let entries: Vec<ObjectEntry> = (0..7).map(|i| entry(&format!("k{i}"), 1, None)).collect();
        let slices = assign_lanes(entries, 3).unwrap();
        let keys: Vec<Vec<&str>> = slices
            .iter()
            .map(|s| s.iter().map(|e| e.key.as_str()).collect())
            .collect();
        assert_eq!(keys[0], ["k0", "k3", "k6"]);
        assert_eq!(keys[1], ["k1", "k4"]);
        assert_eq!(keys[2], ["k2", "k5"]);
    }

    #[test]
    fn resume_validation_catches_each_drift_kind() {
        let slice = vec![
            entry("a", 1, Some("e-a")),
            entry("b", 1, Some("e-b")),
            entry("c", 1, Some("e-c")),
        ];
        let state = |ordinal: u32, key: &str, etag: &str, hash_keys: &[&str]| LaneState {
            watermark: 0,
            ordinal,
            key: key.into(),
            etag: Some(etag.into()),
            keys_hash: hash_keys.iter().fold(0, |h, k| chain_hash(h, k)),
        };
        let manifest_with = |st: LaneState| {
            let mut m = Manifest::new(
                1,
                crate::store::SourceIdentity {
                    url: "s3://b/p/".into(),
                    format: crate::config::Format::Ndjson,
                    compression: crate::config::Compression::Auto,
                },
            );
            m.lane_states.insert(0, st);
            m
        };

        // Healthy resume.
        let ok = manifest_with(state(1, "b", "e-b", &["a", "b"]));
        validate_resume(std::slice::from_ref(&slice), &ok).unwrap();

        // Ordinal beyond the listing.
        let short = manifest_with(state(9, "z", "e", &["a"]));
        let err = validate_resume(std::slice::from_ref(&slice), &short).unwrap_err();
        assert!(err.to_string().contains("removed"), "{err}");

        // Key mismatch at the ordinal.
        let moved = manifest_with(state(1, "was-here", "e-b", &["a", "was-here"]));
        let err = validate_resume(std::slice::from_ref(&slice), &moved).unwrap_err();
        assert!(err.to_string().contains("added or removed"), "{err}");

        // Compensating drift below the ordinal: key at ordinal matches,
        // prefix hash does not.
        let swapped = manifest_with(state(1, "b", "e-b", &["other", "b"]));
        let err = validate_resume(std::slice::from_ref(&slice), &swapped).unwrap_err();
        assert!(err.to_string().contains("rolling hash"), "{err}");

        // Same key overwritten (etag change).
        let rewritten = manifest_with(state(1, "b", "old-etag", &["a", "b"]));
        let err = validate_resume(std::slice::from_ref(&slice), &rewritten).unwrap_err();
        assert!(err.to_string().contains("overwritten"), "{err}");

        // Mid-object watermark with no committed ETag: the replayed
        // discard cannot be verified, so the resume must refuse rather
        // than risk silently skipping records.
        let mut unpinned = state(1, "b", "unused", &["a", "b"]);
        unpinned.etag = None;
        unpinned.watermark = Position {
            ordinal: 1,
            record: 3,
        }
        .encode()
        .unwrap();
        let err = validate_resume(&[slice], &manifest_with(unpinned)).unwrap_err();
        assert!(err.to_string().contains("no ETag"), "{err}");
    }

    // ---------------------------------------------------- fetcher tests --

    /// Wraps a store, failing the first `fail_gets` `get_opts` calls with a
    /// retryable error, and cutting the first `cut_streams` result streams
    /// after `cut_after` bytes with a retryable error.
    #[derive(Debug)]
    struct FlakyStore {
        inner: InMemory,
        fail_gets: AtomicU32,
        cut_streams: AtomicU32,
        cut_after: usize,
        gets: AtomicU32,
    }

    impl FlakyStore {
        fn new(inner: InMemory) -> FlakyStore {
            FlakyStore {
                inner,
                fail_gets: AtomicU32::new(0),
                cut_streams: AtomicU32::new(0),
                cut_after: 0,
                gets: AtomicU32::new(0),
            }
        }

        fn generic(what: &str) -> object_store::Error {
            object_store::Error::Generic {
                store: "flaky",
                source: what.to_owned().into(),
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
                                vec![Err(Self::generic("injected stream cut"))]
                            } else {
                                let take = bytes.len().min(cut_after - sent);
                                sent += take;
                                let mut out = vec![Ok(bytes.slice(0..take))];
                                if sent >= cut_after {
                                    out.push(Err(Self::generic("injected stream cut")));
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
            Ok(result)
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
    ) -> Vec<ChunkMsg> {
        let (tx, mut rx) = mpsc::channel(64);
        let params = FetcherParams {
            lane: 0,
            store,
            slice: Arc::new(slice),
            start_ordinal,
            resume_etag: None,
            chunk_bytes,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
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
        let msgs = collect_fetch(store, slice, 0, 4).await;
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
        let msgs = collect_fetch(store, slice, 1, 64).await;
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
        let msgs = collect_fetch(store, slice, 0, 64).await;
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
        let msgs = collect_fetch(store, slice, 0, 4).await;
        assert_eq!(
            assembled(&msgs),
            vec![("p/a".to_string(), body.to_vec())],
            "resumed read must splice exactly at the cut"
        );
    }

    #[tokio::test]
    async fn missing_object_fails_the_lane_fatally() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"x")]).await);
        // A slice naming a key that does not exist (drifted listing).
        let slice = vec![entry("p/ghost", 1, None)];
        let (tx, mut rx) = mpsc::channel(8);
        let params = FetcherParams {
            lane: 0,
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
        };
        tokio::spawn(run_fetcher(params));
        let mut saw_failure = false;
        while let Some(m) = rx.recv().await {
            if let ChunkMsg::LaneFailed(SourceError::Client { class, reason }) = m {
                assert_eq!(class, ErrorClass::Fatal, "{reason}");
                saw_failure = true;
            }
        }
        assert!(saw_failure);
    }

    #[tokio::test]
    async fn stale_etag_pin_fails_the_lane_fatally() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"new content")]).await);
        let mut slice = listed(&store).await;
        slice[0].etag = Some("\"stale\"".into());
        let (tx, mut rx) = mpsc::channel(8);
        let params = FetcherParams {
            lane: 0,
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 64,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
            retry_base: Duration::from_millis(1),
            retries: None,
        };
        tokio::spawn(run_fetcher(params));
        let mut saw_failure = false;
        while let Some(m) = rx.recv().await {
            if let ChunkMsg::LaneFailed(e) = m {
                assert!(e.to_string().contains("Fatal"), "{e}");
                saw_failure = true;
            }
        }
        assert!(saw_failure, "a failed precondition must fail the lane");
    }

    #[tokio::test]
    async fn pause_halts_fetching_and_resume_continues() {
        let store: Arc<dyn ObjectStore> = Arc::new(seeded(&[("p/a", b"abcdefgh")]).await);
        let slice = listed(&store).await;
        let (tx, mut rx) = mpsc::channel(1); // tiny: sends interleave with pauses
        let pause = Arc::new(AtomicBool::new(true));
        let params = FetcherParams {
            lane: 0,
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 2,
            tx,
            pause: Arc::clone(&pause),
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
            lane: 0,
            store,
            slice: Arc::new(slice),
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: 8,
            tx,
            pause: Arc::new(AtomicBool::new(false)),
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
}
