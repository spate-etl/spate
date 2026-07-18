//! The lane-assembly context: everything split materialization needs,
//! implementing the coordination driver's
//! [`SplitSource`](etl_core::coordination::driver::SplitSource) callbacks.
//!
//! [`SplitCtx`] lives beside the
//! [`CoordinationDriver`](etl_core::coordination::driver::CoordinationDriver)
//! as a sibling field of the source, so the driver and the context borrow
//! disjointly. It owns the per-split state table and the two seams a lane
//! (pipeline thread) shares with the control plane (controller thread):
//!
//! - **Completion** rides a [`SplitTracker`]: the lane records the terminal
//!   watermark `T` (one past the last record it emitted) exactly once, at
//!   its end-of-input decision. A split is complete when the **acked**
//!   watermark `W` reaches `T` — the lane knows "fully framed", the
//!   checkpoint tracker knows "fully acked", and [`SplitCtx::encode_commit`]
//!   / [`SplitCtx::sweep`] are where the two meet.
//! - **Poison** rides [`PoisonReport`]s: a lane may neither block nor
//!   return an error for anything that is not pipeline-fatal (a lane
//!   `poll` error is terminal for the whole job), so object-level failures
//!   travel this side channel, the lane goes quiescent, and the control
//!   plane hands the split back to the coordinator on its own thread.

use crate::config::Compression;
use crate::fetch::{FetcherParams, ObjectEntry, run_fetcher};
use crate::framer::FramerFactory;
use crate::lane::S3Lane;
use crate::metrics::S3Metrics;
use crate::offset::Position;
use crate::split::SplitDescriptor;
use etl_core::checkpoint::AckIssuer;
use etl_core::coordination::driver::{SplitOpening, SplitSource};
use etl_core::coordination::{SplitId, SplitProgress, SplitSpec};
use etl_core::error::{ErrorClass, SourceError};
use etl_core::source::LaneId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// Fetcher→lane channel depth, in chunks. The per-lane read-ahead is the
/// `prefetch_bytes` window each fetcher buffers in memory (see
/// [`fetch`](crate::fetch)), so this channel is only a small hand-off
/// buffer: its chunks are zero-copy views into that one window, not extra
/// copies. Keeping it shallow bounds peak per-lane read-ahead to ~one
/// window under sustained backpressure.
const LANE_HANDOFF_CHUNKS: usize = 4;

/// Sentinel for "the lane has not observed end-of-input yet".
const UNKNOWN: i64 = i64::MIN;

/// Version of the [`ProgressState`] wire encoding carried inside
/// [`SplitProgress::state`].
const PROGRESS_STATE_VERSION: u32 = 1;

/// Lane→control-plane completion accounting, lock-free.
///
/// `terminal` is one-shot (the end-of-input decision); `objects_done`
/// counts fully framed member objects so the control plane can settle the
/// `objects_remaining` gauge when a split closes mid-way.
#[derive(Debug)]
pub(crate) struct SplitTracker {
    terminal: AtomicI64,
    objects_done: AtomicU32,
}

impl SplitTracker {
    pub(crate) fn new() -> SplitTracker {
        SplitTracker {
            terminal: AtomicI64::new(UNKNOWN),
            objects_done: AtomicU32::new(0),
        }
    }

    /// Record the terminal watermark. Called once, from the lane, at its
    /// end-of-input decision point.
    pub(crate) fn set_terminal(&self, watermark: i64) {
        debug_assert_ne!(watermark, UNKNOWN, "terminal watermark is a real offset");
        let prev = self.terminal.swap(watermark, Ordering::Release);
        debug_assert!(
            prev == UNKNOWN || prev == watermark,
            "a lane decides end-of-input once; conflicting terminals {prev} vs {watermark}"
        );
    }

    /// The terminal watermark, once the lane has decided end-of-input.
    pub(crate) fn terminal(&self) -> Option<i64> {
        match self.terminal.load(Ordering::Acquire) {
            UNKNOWN => None,
            w => Some(w),
        }
    }

    /// Count one member object as fully framed.
    pub(crate) fn object_done(&self) {
        self.objects_done.fetch_add(1, Ordering::Relaxed);
    }

    /// Member objects fully framed under this tenancy.
    pub(crate) fn objects_done(&self) -> u32 {
        self.objects_done.load(Ordering::Relaxed)
    }
}

/// What kind of object-level poison a split hit — the bounded `reason`
/// label of `etl_s3_source_objects_failed_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PoisonKind {
    /// A planned object no longer exists (deleted after planning).
    NotFound,
    /// The object's content changed under its pin: a failed `If-Match`
    /// precondition, or content that no longer matches committed progress.
    EtagDrift,
    /// The object's content cannot be decoded (corrupt, truncated, over
    /// the per-object record limit, or unverifiable without an ETag).
    Undecodable,
    /// Reads kept failing past the attempt budget.
    RetriesExhausted,
}

impl PoisonKind {
    /// The metric label value; bounded and stable.
    pub(crate) fn reason_label(self) -> &'static str {
        match self {
            PoisonKind::NotFound => "not_found",
            PoisonKind::EtagDrift => "etag_drift",
            PoisonKind::Undecodable => "undecodable",
            PoisonKind::RetriesExhausted => "retries_exhausted",
        }
    }
}

/// A lane's report that its split hit object-level poison. The reporting
/// lane goes quiescent; the control plane drains these on the controller
/// thread and hands the split back to the coordinator, which retries it
/// elsewhere and quarantines it at the attempt cap.
#[derive(Debug)]
pub(crate) struct PoisonReport {
    /// The split the lane was reading.
    pub(crate) split: SplitId,
    /// Failure classification (bounded; drives the failure metric).
    pub(crate) kind: PoisonKind,
    /// Human-readable cause, naming the object.
    pub(crate) reason: String,
}

/// The opaque resume payload written into [`SplitProgress::state`]: the
/// key and ETag pin at the watermark's ordinal, so the next owner resumes
/// against provably-identical content and
/// [`validate_resume`](SplitCtx::validate_resume) can cross-check carried
/// progress against its descriptor.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ProgressState {
    v: u32,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    etag: Option<String>,
}

impl ProgressState {
    fn at(objects: &[ObjectEntry], watermark: i64) -> ProgressState {
        let ordinal = Position::decode(watermark).ordinal as usize;
        let entry = objects.get(ordinal);
        ProgressState {
            v: PROGRESS_STATE_VERSION,
            key: entry.map(|e| e.key.clone()),
            etag: entry.and_then(|e| e.etag.clone()),
        }
    }

    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("progress-state serialization is infallible")
    }

    /// Decode a carried payload. Empty bytes (an external seed with no
    /// pin) decode to an empty state; a versioned payload must match.
    fn decode(bytes: &[u8]) -> Result<ProgressState, String> {
        if bytes.is_empty() {
            return Ok(ProgressState {
                v: PROGRESS_STATE_VERSION,
                key: None,
                etag: None,
            });
        }
        let state: ProgressState = serde_json::from_slice(bytes)
            .map_err(|e| format!("progress state failed to decode: {e}"))?;
        if state.v != PROGRESS_STATE_VERSION {
            return Err(format!(
                "progress state version {} is not this release's version \
                 {PROGRESS_STATE_VERSION}",
                state.v
            ));
        }
        Ok(state)
    }
}

/// Per-held-split control-plane state.
struct SplitState {
    /// The descriptor's member objects; ordinals index into this.
    objects: Arc<Vec<ObjectEntry>>,
    /// Lane id of the current tenancy (pause/resume routing).
    lane: LaneId,
    /// Backpressure pause flag the fetcher honors between sends.
    pause: Arc<AtomicBool>,
    /// Completion accounting shared with the lane.
    tracker: Arc<SplitTracker>,
    /// Objects not yet complete when this tenancy opened (settles the
    /// `objects_remaining` gauge at close).
    remaining_at_open: u64,
    /// The watermark this tenancy resumed from (0 fresh) — the terminal
    /// watermark of a tenancy that emits nothing.
    resume_watermark: i64,
    /// The most recent watermark handed to `encode_commit`.
    last_committed: Option<i64>,
    /// This split's end-of-input was already reported through
    /// `take_finishing` (the commit-ready hint fires once per tenancy).
    hinted: bool,
}

/// The lane-assembly context (see the module docs).
pub(crate) struct SplitCtx {
    store: Arc<dyn object_store::ObjectStore>,
    handle: tokio::runtime::Handle,
    issuer: AckIssuer,
    make_framer: FramerFactory,
    compression: Compression,
    chunk_bytes: usize,
    range_bytes: usize,
    retry_base: Duration,
    metrics: Option<S3Metrics>,
    splits: BTreeMap<SplitId, SplitState>,
    poison_tx: std::sync::mpsc::Sender<PoisonReport>,
    poison_rx: std::sync::mpsc::Receiver<PoisonReport>,
}

impl std::fmt::Debug for SplitCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitCtx")
            .field("splits", &self.splits.len())
            .finish_non_exhaustive()
    }
}

impl SplitCtx {
    #[expect(
        clippy::too_many_arguments,
        reason = "assembled once by the source at open"
    )]
    pub(crate) fn new(
        store: Arc<dyn object_store::ObjectStore>,
        handle: tokio::runtime::Handle,
        issuer: AckIssuer,
        make_framer: FramerFactory,
        compression: Compression,
        chunk_bytes: usize,
        range_bytes: usize,
        retry_base: Duration,
        metrics: Option<S3Metrics>,
    ) -> SplitCtx {
        let (poison_tx, poison_rx) = std::sync::mpsc::channel();
        SplitCtx {
            store,
            handle,
            issuer,
            make_framer,
            compression,
            chunk_bytes,
            range_bytes,
            retry_base,
            metrics,
            splits: BTreeMap::new(),
            poison_tx,
            poison_rx,
        }
    }

    /// Drain pending poison reports (controller thread; never blocks).
    pub(crate) fn drain_poison(&mut self) -> Vec<PoisonReport> {
        let mut reports = Vec::new();
        while let Ok(report) = self.poison_rx.try_recv() {
            if let Some(m) = &self.metrics {
                m.objects_failed(report.kind).increment(1);
            }
            reports.push(report);
        }
        reports
    }

    /// Flip the pause flag of every split whose lane is named.
    pub(crate) fn set_paused(&self, lanes: &[LaneId], paused: bool) {
        for state in self.splits.values() {
            if lanes.contains(&state.lane) {
                state.pause.store(paused, Ordering::Relaxed);
            }
        }
    }
}

impl SplitSource for SplitCtx {
    type Lane = S3Lane;

    fn open_split(&mut self, opening: SplitOpening<'_>) -> Result<S3Lane, SourceError> {
        let split = opening.split.id.clone();
        // The spec was validated when planned, but decode defensively: a
        // corrupt store record must fail loudly, not panic.
        let descriptor = SplitDescriptor::decode(&opening.split.descriptor).map_err(|e| {
            SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!("split {split}: {}", e.reason),
            }
        })?;
        let objects: Arc<Vec<ObjectEntry>> = Arc::new(descriptor.to_entries());

        // Store-supplied data: a negative watermark must fail before
        // `Position::decode` (its debug_assert guards internal arithmetic,
        // not hostile records). `validate_resume` already rejects this
        // shape, but the driver seam does not force that ordering.
        if let Some(p) = opening.resume
            && p.watermark < 0
        {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!(
                    "split {split}: carried watermark {} is negative — no position in \
                     any descriptor",
                    p.watermark
                ),
            });
        }
        let resume = opening.resume.map(|p| Position::decode(p.watermark));
        let resume_watermark = opening.resume.map_or(0, |p| p.watermark);
        let start_ordinal = resume.map_or(0, |p| p.ordinal);
        // Pin the resume object to the content its committed records were
        // minted against: the carried pin, else the descriptor's ETag
        // (identical content — the descriptor is immutable and in the
        // split's identity).
        let resume_etag = match opening.resume {
            Some(p) => ProgressState::decode(&p.state)
                .map_err(|reason| SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!("split {split}: {reason}"),
                })?
                .etag
                .or_else(|| {
                    objects
                        .get(start_ordinal as usize)
                        .and_then(|e| e.etag.clone())
                }),
            None => None,
        };

        let tracker = Arc::new(SplitTracker::new());
        let pause = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(LANE_HANDOFF_CHUNKS);
        // Detached deliberately: the fetcher exits when the lane (its
        // receiver) drops; `close_split` must never abort it mid-drain.
        drop(self.handle.spawn(run_fetcher(FetcherParams {
            split: split.clone(),
            store: Arc::clone(&self.store),
            slice: Arc::clone(&objects),
            start_ordinal,
            resume_etag,
            chunk_bytes: self.chunk_bytes,
            range_bytes: self.range_bytes,
            tx,
            pause: Arc::clone(&pause),
            retry_base: self.retry_base,
            retries: self.metrics.as_ref().map(|m| m.get_retries.clone()),
        })));

        let remaining_at_open = (objects.len() as u64).saturating_sub(u64::from(start_ordinal));
        if let Some(m) = &self.metrics {
            m.objects_remaining.increment(remaining_at_open as f64);
        }
        let lane = S3Lane::new(
            opening.lane,
            opening.partition,
            rx,
            self.handle.clone(),
            self.issuer.clone(),
            self.compression,
            Arc::clone(&self.make_framer),
            resume,
            split.clone(),
            Arc::clone(&tracker),
            self.poison_tx.clone(),
            opening.waker.clone(),
            self.metrics.clone(),
        );
        self.splits.insert(
            split,
            SplitState {
                objects,
                lane: opening.lane,
                pause,
                tracker,
                remaining_at_open,
                resume_watermark,
                last_committed: None,
                hinted: false,
            },
        );
        Ok(lane)
    }

    fn validate_resume(
        &self,
        split: &SplitSpec,
        progress: &SplitProgress,
    ) -> Result<(), SourceError> {
        let refuse = |detail: String| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: format!(
                "split {}: carried progress no longer matches its descriptor ({detail}); \
                 this is unrecoverable divergence — requeue the split or start a fresh job",
                split.id
            ),
        };
        let descriptor =
            SplitDescriptor::decode(&split.descriptor).map_err(|e| SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!("split {}: {}", split.id, e.reason),
            })?;
        let state = ProgressState::decode(&progress.state).map_err(refuse)?;

        // Store-supplied data: reject a negative watermark before
        // `Position::decode`, whose debug_assert guards internal
        // arithmetic, not hostile records.
        if progress.watermark < 0 {
            return Err(refuse(format!(
                "watermark {} is negative — no position in any descriptor",
                progress.watermark
            )));
        }
        let pos = Position::decode(progress.watermark);
        let ordinal = pos.ordinal as usize;
        if descriptor.objects.is_empty() && progress.watermark == 0 {
            return Ok(()); // an empty split's only legal progress
        }
        let Some(entry) = descriptor.objects.get(ordinal) else {
            return Err(refuse(format!(
                "watermark ordinal {ordinal} is outside the {}-object descriptor",
                descriptor.objects.len()
            )));
        };
        if let Some(key) = &state.key
            && key != &entry.key
        {
            return Err(refuse(format!(
                "progress pins object \"{key}\" at ordinal {ordinal}, the descriptor has \
                 \"{}\" there",
                entry.key
            )));
        }
        if let (Some(pinned), Some(listed)) = (&state.etag, &entry.etag)
            && pinned != listed
        {
            return Err(refuse(format!(
                "progress pins ETag {pinned} at ordinal {ordinal}, the descriptor has \
                 {listed} — progress and descriptor were minted against different content"
            )));
        }
        // A mid-object watermark replays the object and discards the
        // committed record count, which is only sound against provably
        // identical content. Without any pin (neither carried nor in the
        // descriptor), a same-key overwrite could silently skip records.
        if pos.record > 0 && state.etag.is_none() && entry.etag.is_none() {
            return Err(refuse(format!(
                "the watermark is mid-object (\"{}\", {} records committed) but neither \
                 the progress nor the descriptor carries an ETag to pin the re-read to",
                entry.key, pos.record
            )));
        }
        Ok(())
    }

    fn encode_commit(
        &mut self,
        split: &SplitId,
        watermark: i64,
    ) -> Result<SplitProgress, SourceError> {
        let Some(state) = self.splits.get_mut(split) else {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!("commit for unheld split {split} — driver/context wiring bug"),
            });
        };
        // Tripwire: a committed watermark always decodes *inside* the
        // split's descriptor. Even one past the last emittable record stays
        // in its object: the emit guard caps indices at MAX_RECORD_INDEX,
        // so the watermark's `+ 1` lands on the reserved end-of-object
        // index and never carries into the next ordinal (see offset.rs,
        // `watermark_after_last_emittable_record_stays_in_its_object`).
        // The single legal at-or-past-`members` shape is the empty
        // descriptor's zero watermark (empty splits complete via sweep and
        // never reach here, but the boundary shape stays legal for safety).
        // Anything else is offset-accounting corruption; fail loudly now
        // instead of persisting a position validate_resume would reject.
        let pos = Position::decode(watermark);
        let members = state.objects.len();
        let legal_empty = members == 0 && watermark == 0;
        if pos.ordinal as usize >= members && !legal_empty {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!(
                    "split {split}: committed watermark {watermark} decodes to ordinal {} \
                     record {}, outside the {members}-member descriptor — offset accounting bug",
                    pos.ordinal, pos.record
                ),
            });
        }
        state.last_committed = Some(watermark);
        let payload = ProgressState::at(&state.objects, watermark).encode();
        // Complete iff the lane has decided end-of-input at exactly this
        // acked watermark: fully framed (T known) and fully acked (W == T).
        Ok(if state.tracker.terminal() == Some(watermark) {
            SplitProgress::completed(watermark, payload)
        } else {
            SplitProgress::new(watermark, payload)
        })
    }

    fn sweep(&mut self, split: &SplitId) -> Result<Option<SplitProgress>, SourceError> {
        let Some(state) = self.splits.get(split) else {
            return Ok(None);
        };
        let Some(terminal) = state.tracker.terminal() else {
            return Ok(None); // still reading
        };
        // Two shapes only a sweep can complete: the final ack landed on an
        // earlier tick, before the lane observed end-of-input (that commit
        // carried `completed: false` and no further watermark will ever
        // arrive); or the tenancy emitted nothing at all (an empty split,
        // or a resume exactly at end-of-input).
        let acked_all = state.last_committed == Some(terminal)
            || (state.last_committed.is_none() && state.resume_watermark == terminal);
        if !acked_all {
            return Ok(None); // data still in flight to the sink
        }
        let payload = ProgressState::at(&state.objects, terminal).encode();
        Ok(Some(SplitProgress::completed(terminal, payload)))
    }

    fn close_split(&mut self, split: &SplitId) {
        // End of the tenancy (the driver retires it first, so no commit
        // or sweep can arrive afterwards): drop the split's state and
        // settle the gauge. Never blocks and never aborts the fetcher —
        // it exits on its own when the (dropped) lane closes the channel.
        if let Some(state) = self.splits.remove(split)
            && let Some(m) = &self.metrics
        {
            let done = u64::from(state.tracker.objects_done());
            m.objects_remaining
                .decrement(state.remaining_at_open.saturating_sub(done) as f64);
        }
    }

    fn take_finishing(&mut self) -> Vec<SplitId> {
        // Cheap edge-detect: one atomic load per held split (≤ the
        // coordinator's working-set bound), on the controller thread.
        self.splits
            .iter_mut()
            .filter_map(|(id, state)| {
                if state.hinted || state.tracker.terminal().is_none() {
                    return None;
                }
                state.hinted = true;
                Some(id.clone())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_is_unknown_until_set_and_stable_after() {
        let t = SplitTracker::new();
        assert_eq!(t.terminal(), None);
        t.set_terminal(42);
        assert_eq!(t.terminal(), Some(42));
        t.set_terminal(42); // idempotent re-set of the same value is fine
        assert_eq!(t.terminal(), Some(42));
    }

    #[test]
    fn tracker_carries_zero_watermarks_and_counts_objects() {
        // 0 is a legitimate terminal (an empty split emitted nothing).
        let t = SplitTracker::new();
        t.set_terminal(0);
        assert_eq!(t.terminal(), Some(0));
        t.object_done();
        t.object_done();
        assert_eq!(t.objects_done(), 2);
    }

    #[test]
    fn progress_state_round_trips_and_pins_the_encoding() {
        let state = ProgressState {
            v: PROGRESS_STATE_VERSION,
            key: Some("exports/part-000.ndjson".into()),
            etag: Some("\"9b2cf5\"".into()),
        };
        let bytes = state.encode();
        // Persisted document: the field names and shape are wire format.
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            r#"{"v":1,"key":"exports/part-000.ndjson","etag":"\"9b2cf5\""}"#
        );
        assert_eq!(ProgressState::decode(&bytes).unwrap(), state);
    }

    #[test]
    fn empty_progress_state_decodes_as_unpinned() {
        let state = ProgressState::decode(b"").unwrap();
        assert_eq!(state.key, None);
        assert_eq!(state.etag, None);
    }

    #[test]
    fn unknown_progress_state_version_is_rejected() {
        let err = ProgressState::decode(br#"{"v":99}"#).unwrap_err();
        assert!(err.contains("version 99"), "{err}");
    }

    // ------------------------------------------- resume validation matrix --
    // Ported from the manifest era's `resume_validation_catches_each_drift
    // _kind`: carried progress must still mean what it meant when written.
    // Two historical rows have no coordinated analog by design: the rolling
    // keys_hash "compensating drift" check (descriptors are immutable and
    // their keys+etags are in the split id — covered by the digest
    // sensitivity tests), and cross-listing drift (workers never list; a
    // drifted store surfaces at fetch time as If-Match poison).

    fn test_ctx() -> (SplitCtx, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let ctx = SplitCtx::new(
            Arc::new(object_store::memory::InMemory::new()),
            rt.handle().clone(),
            etl_core::checkpoint::Checkpointer::new().handle(),
            Arc::new(|| Box::new(crate::testutil::TestLineFramer::new(1 << 20))),
            Compression::Auto,
            64,
            1024,
            Duration::from_millis(1),
            None,
        );
        (ctx, rt)
    }

    fn spec_of(objects: &[(&str, Option<&str>)]) -> SplitSpec {
        let entries: Vec<ObjectEntry> = objects
            .iter()
            .map(|(key, etag)| ObjectEntry {
                key: (*key).to_string(),
                size: 1,
                etag: etag.map(str::to_owned),
                last_modified_ms: 0,
            })
            .collect();
        let id = crate::split::split_id_for(objects.iter().copied()).unwrap();
        SplitSpec::new(
            id,
            SplitDescriptor::from_entries(&entries).encode().unwrap(),
        )
    }

    fn progress(ordinal: u32, record: u64, key: Option<&str>, etag: Option<&str>) -> SplitProgress {
        let watermark = Position { ordinal, record }.encode().unwrap();
        let state = ProgressState {
            v: PROGRESS_STATE_VERSION,
            key: key.map(str::to_owned),
            etag: etag.map(str::to_owned),
        };
        SplitProgress::new(watermark, state.encode())
    }

    #[test]
    fn validate_resume_rejects_each_drift_kind() {
        let (ctx, _rt) = test_ctx();
        let spec = spec_of(&[("a", Some("e-a")), ("b", Some("e-b")), ("c", Some("e-c"))]);

        // Healthy resume: mid-split, pins matching the descriptor.
        ctx.validate_resume(&spec, &progress(1, 0, Some("b"), Some("e-b")))
            .unwrap();
        // A carried payload with no pins at all is legal (external seed).
        ctx.validate_resume(&spec, &SplitProgress::new(0, Vec::new()))
            .unwrap();

        // Watermark ordinal beyond the descriptor's member count.
        let err = ctx
            .validate_resume(&spec, &progress(9, 0, Some("z"), None))
            .unwrap_err();
        assert!(err.to_string().contains("outside"), "{err}");

        // Key pinned at the ordinal does not match the descriptor.
        let err = ctx
            .validate_resume(&spec, &progress(1, 0, Some("was-here"), Some("e-b")))
            .unwrap_err();
        assert!(err.to_string().contains("was-here"), "{err}");

        // ETag pin does not match the descriptor: progress and descriptor
        // were minted against different content.
        let err = ctx
            .validate_resume(&spec, &progress(1, 0, Some("b"), Some("old-etag")))
            .unwrap_err();
        assert!(err.to_string().contains("different content"), "{err}");

        // Undecodable carried state.
        let err = ctx
            .validate_resume(&spec, &SplitProgress::new(0, b"garbage".to_vec()))
            .unwrap_err();
        assert!(err.to_string().contains("decode"), "{err}");

        // Mid-object watermark with no pin anywhere: the replayed discard
        // cannot be verified, so refuse rather than risk skipping records.
        let unpinned_spec = spec_of(&[("a", None), ("b", None)]);
        let err = ctx
            .validate_resume(&unpinned_spec, &progress(1, 3, Some("b"), None))
            .unwrap_err();
        assert!(err.to_string().contains("mid-object"), "{err}");
        // With the descriptor carrying a pin, the same shape is fine.
        ctx.validate_resume(&spec, &progress(1, 3, Some("b"), None))
            .unwrap();
    }

    #[test]
    fn negative_watermark_from_the_store_is_refused_without_panicking() {
        // Hostile/corrupt coordination-store data must be an Err, never a
        // debug-build panic inside Position::decode.
        let (ctx, _rt) = test_ctx();
        let spec = spec_of(&[("a", Some("e-a"))]);
        let err = ctx
            .validate_resume(&spec, &SplitProgress::new(-1, Vec::new()))
            .unwrap_err();
        assert!(err.to_string().contains("negative"), "{err}");
    }

    #[test]
    fn commit_beyond_the_descriptor_is_an_accounting_bug() {
        use etl_core::source::LaneId;

        let (mut ctx, _rt) = test_ctx();
        let spec = spec_of(&[("a", Some("e-a")), ("b", Some("e-b"))]);
        let id = spec.id.clone();
        // Seed the held-split state directly (SplitOpening is
        // framework-constructed); only the descriptor length matters here.
        let descriptor = SplitDescriptor::decode(&spec.descriptor).unwrap();
        ctx.splits.insert(
            id.clone(),
            SplitState {
                objects: Arc::new(descriptor.to_entries()),
                lane: LaneId(0),
                pause: Arc::new(AtomicBool::new(false)),
                tracker: Arc::new(SplitTracker::new()),
                remaining_at_open: 2,
                resume_watermark: 0,
                last_committed: None,
                hinted: false,
            },
        );

        // In-range commit: fine (and pinned to the descriptor).
        ctx.encode_commit(
            &id,
            Position {
                ordinal: 1,
                record: 3,
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
        // A watermark one past a maximally-full final member stays in its
        // ordinal at the reserved index — inside the descriptor.
        ctx.encode_commit(
            &id,
            Position {
                ordinal: 1,
                record: crate::offset::MAX_RECORD_INDEX + 1,
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
        // ordinal == members is unreachable as a real watermark (no carry
        // out of a member's record field) and validate_resume rejects it:
        // corruption, must fail loudly.
        let err = ctx
            .encode_commit(
                &id,
                Position {
                    ordinal: 2,
                    record: 0,
                }
                .encode()
                .unwrap(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("accounting"), "{err}");
        let err = ctx
            .encode_commit(
                &id,
                Position {
                    ordinal: 5,
                    record: 0,
                }
                .encode()
                .unwrap(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("accounting"), "{err}");
        let err = ctx
            .encode_commit(
                &id,
                Position {
                    ordinal: 2,
                    record: 1,
                }
                .encode()
                .unwrap(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("accounting"), "{err}");

        // The empty-descriptor boundary: watermark 0 against zero members
        // stays legal (empty splits complete via sweep; the shape must not
        // trip the tripwire if one ever lands here).
        let empty_id = crate::split::split_id_for([("empty-placeholder", None)]).unwrap();
        ctx.splits.insert(
            empty_id.clone(),
            SplitState {
                objects: Arc::new(Vec::new()),
                lane: LaneId(1),
                pause: Arc::new(AtomicBool::new(false)),
                tracker: Arc::new(SplitTracker::new()),
                remaining_at_open: 0,
                resume_watermark: 0,
                last_committed: None,
                hinted: false,
            },
        );
        ctx.encode_commit(&empty_id, 0).unwrap();
    }

    #[test]
    fn empty_split_progress_is_legal_at_zero_only() {
        let (ctx, _rt) = test_ctx();
        let entries: Vec<ObjectEntry> = Vec::new();
        let id = crate::split::split_id_for([("placeholder", None)]).unwrap();
        let spec = SplitSpec::new(
            id,
            SplitDescriptor::from_entries(&entries).encode().unwrap(),
        );
        ctx.validate_resume(&spec, &SplitProgress::new(0, Vec::new()))
            .unwrap();
        let err = ctx
            .validate_resume(&spec, &progress(0, 1, None, None))
            .unwrap_err();
        assert!(err.to_string().contains("outside"), "{err}");
    }
}
