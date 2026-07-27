//! The data plane: one [`S3Lane`] per lane, polled on a pipeline thread.
//!
//! A lane pulls byte chunks from its fetcher's channel, decompresses and
//! frames them on the pipeline thread (see [`framer`](crate::framer)),
//! assigns each record its composite offset, and hands out borrowed
//! payload batches with one [`AckRef`] each.
//!
//! Three rules here are load-bearing for correctness:
//!
//! - **End-of-input is only decided by a `poll` that returns `Ok(None)`**
//!   after observing the channel closed with nothing buffered. The
//!   driver's poll→push→poll sequencing on one thread then guarantees the
//!   lane's final batch was fully pushed downstream before that decision
//!   records the terminal watermark.
//! - **Blocking is bounded.** When idle the lane waits on the channel via
//!   the I/O runtime with the poll timeout applied — it never busy-spins
//!   and never parks longer than the driver allows.
//! - **Poison never surfaces as a poll error.** A lane `poll` error is
//!   terminal for the whole pipeline, so object-level failures (deleted,
//!   overwritten, corrupt, or unreadable objects) are reported through the
//!   [`PoisonReport`] side channel instead and the lane goes quiescent;
//!   only instance-level failures (credentials, wiring) return `Err`.

use crate::config::Compression;
use crate::fetch::{ChunkMsg, SplitFailure};
use crate::framer::{Codec, FramerFactory, ObjectFramer};
use crate::metrics::S3Metrics;
use crate::offset::{MAX_RECORD_INDEX, Position};
use crate::split_ctx::{PoisonKind, PoisonReport, SplitTracker};
use spate_core::checkpoint::{AckIssuer, AckRef};
use spate_core::coordination::{ControlWaker, SplitId};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::record::{PartitionId, RawPayload};
use spate_core::source::{LaneId, PayloadBatch, SourceLane};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

/// The object currently being framed.
struct CurrentObject {
    ordinal: u32,
    key: String,
    /// Record index the next emitted record gets.
    next_record: u64,
    /// Event time stamped on this object's records.
    event_time_ms: i64,
}

/// One framed record, owned by the lane across the batch's lifetime.
struct HeldRecord {
    offset: i64,
    event_time_ms: i64,
    bytes: Vec<u8>,
}

/// Data-plane pollable unit of the S3 source: owns one slice's chunk
/// stream and framing state.
pub struct S3Lane {
    id: LaneId,
    partition: PartitionId,
    rx: mpsc::Receiver<ChunkMsg>,
    handle: tokio::runtime::Handle,
    issuer: AckIssuer,
    compression: Compression,
    framer: ObjectFramer,
    /// Committed resume position; consumed at the first `ObjectStart`.
    resume: Option<Position>,
    /// Records of the resume object still to discard (replayed committed
    /// records).
    pending_discard: u64,
    current: Option<CurrentObject>,
    /// The object ended but framed records are still queued; finalized
    /// once the queue drains.
    pending_end: bool,
    held: Vec<HeldRecord>,
    /// The split this lane reads; names the work in poison reports.
    split: SplitId,
    /// Carries the terminal watermark to the control plane at the
    /// end-of-input decision.
    tracker: Arc<SplitTracker>,
    /// Side channel for object-level failures (see the module docs).
    poison_tx: std::sync::mpsc::Sender<PoisonReport>,
    /// Wakes the control plane. Signalled on the two edges the driver
    /// would otherwise only notice between waits: end-of-input, and
    /// poison. Never on the per-record path.
    waker: ControlWaker,
    /// The split hit poison: everything undelivered was discarded and
    /// every later poll idles with `Ok(None)` until the lane is retired.
    poisoned: bool,
    /// One past the last emitted record's offset — the terminal watermark
    /// `T` once end-of-input is observed. Starts at the resume watermark
    /// (0 fresh), so a tenancy that emits nothing terminates exactly where
    /// it began.
    watermark_candidate: i64,
    /// Sticky terminal failure: every later poll re-reports it.
    failed: Option<(ErrorClass, String)>,
    metrics: Option<S3Metrics>,
    /// Decoded bytes already counted into the metrics.
    decoded_reported: u64,
}

impl std::fmt::Debug for S3Lane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Lane")
            .field("id", &self.id)
            .field("partition", &self.partition)
            .finish()
    }
}

impl S3Lane {
    #[expect(
        clippy::too_many_arguments,
        reason = "assembled in one place by the source's lane builder"
    )]
    pub(crate) fn new(
        id: LaneId,
        partition: PartitionId,
        rx: mpsc::Receiver<ChunkMsg>,
        handle: tokio::runtime::Handle,
        issuer: AckIssuer,
        compression: Compression,
        make_framer: FramerFactory,
        resume: Option<Position>,
        split: SplitId,
        tracker: Arc<SplitTracker>,
        poison_tx: std::sync::mpsc::Sender<PoisonReport>,
        waker: ControlWaker,
        metrics: Option<S3Metrics>,
    ) -> S3Lane {
        let watermark_candidate = resume.map_or(0, |p| {
            p.encode()
                .expect("a decoded resume position always re-encodes")
        });
        S3Lane {
            id,
            partition,
            rx,
            handle,
            issuer,
            compression,
            framer: ObjectFramer::new(make_framer),
            resume,
            pending_discard: 0,
            current: None,
            pending_end: false,
            held: Vec::new(),
            split,
            tracker,
            poison_tx,
            waker,
            poisoned: false,
            watermark_candidate,
            failed: None,
            metrics,
            decoded_reported: 0,
        }
    }

    /// Record a terminal failure and return it. Later polls re-report it.
    fn fail(&mut self, class: ErrorClass, reason: String) -> SourceError {
        self.failed = Some((class, reason.clone()));
        SourceError::Client { class, reason }
    }

    /// Report object-level poison and go quiescent. Everything undelivered
    /// is discarded — none of it was acked, so replay by the split's next
    /// owner cannot lose data — and every later poll idles with
    /// `Ok(None)` until the control plane retires the lane.
    fn poison(&mut self, kind: PoisonKind, reason: String) {
        self.held.clear();
        self.current = None;
        self.pending_end = false;
        self.pending_discard = 0;
        self.poisoned = true;
        // The receiver disappearing (shutdown) makes the report moot.
        let _ = self.poison_tx.send(PoisonReport {
            split: self.split.clone(),
            kind,
            reason,
        });
        // The report is only read between control-plane waits; wake so the
        // split is handed back now rather than an idle timeout from now.
        self.waker.wake();
    }

    /// Move framed records into `held`, assigning composite offsets, until
    /// the framer queue is empty or the batch is full. May poison the
    /// split (an object over the per-object record limit); the caller
    /// checks `self.poisoned` after every call.
    fn drain_framer(&mut self, max_records: usize) -> Result<(), SourceError> {
        while self.held.len() < max_records {
            let Some(bytes) = self.framer.pop_record() else {
                break;
            };
            if self.pending_discard > 0 {
                self.pending_discard -= 1;
                continue;
            }
            let cur = self
                .current
                .as_mut()
                .expect("framed records only exist within an object");
            if cur.next_record > MAX_RECORD_INDEX {
                // A property of the object's content: it will overflow on
                // every owner, which is exactly what quarantine is for.
                let key = cur.key.clone();
                self.poison(
                    PoisonKind::Undecodable,
                    format!(
                        "object \"{key}\" holds more than {} records, the composite-offset \
                     limit per object",
                        MAX_RECORD_INDEX + 1
                    ),
                );
                return Ok(());
            }
            let pos = Position {
                ordinal: cur.ordinal,
                record: cur.next_record,
            };
            let offset = match pos.encode() {
                Ok(o) => o,
                Err(e) => return Err(self.fail(ErrorClass::Fatal, e.to_string())),
            };
            cur.next_record += 1;
            debug_assert!(
                offset >= self.watermark_candidate,
                "offsets must be monotonic"
            );
            self.watermark_candidate = offset + 1;
            self.held.push(HeldRecord {
                offset,
                event_time_ms: cur.event_time_ms,
                bytes,
            });
        }
        Ok(())
    }

    /// Complete a pending object end once its records have drained. May
    /// poison the split (content drift); the caller checks
    /// `self.poisoned`.
    fn finalize_object(&mut self) {
        debug_assert!(self.pending_end && self.framer.queued() == 0);
        let cur = self.current.take().expect("finalize without an object");
        self.pending_end = false;
        if self.pending_discard > 0 {
            // The object now frames fewer records than were committed
            // against it — its content changed underneath the pin.
            self.poison(
                PoisonKind::EtagDrift,
                format!(
                    "object \"{}\" ended {} records short of its committed position — \
                 its content changed under the checkpoint (the key set and object \
                 contents must stay frozen for the backfill's lifetime)",
                    cur.key, self.pending_discard
                ),
            );
            return;
        }
        self.tracker.object_done();
        if let Some(m) = &self.metrics {
            m.objects_completed.increment(1);
            m.objects_remaining.decrement(1.0);
        }
    }

    /// Apply one fetcher message to the framing state.
    fn on_msg(&mut self, msg: ChunkMsg) -> Result<(), SourceError> {
        match msg {
            ChunkMsg::ObjectStart {
                ordinal,
                key,
                last_modified_ms,
            } => {
                debug_assert!(
                    self.current.is_none() && !self.pending_end,
                    "ObjectStart while the previous object is open"
                );
                let codec = Codec::resolve(self.compression, &key);
                if let Err(e) = self.framer.begin_object(codec) {
                    self.poison(
                        PoisonKind::Undecodable,
                        format!("starting decode of \"{key}\": {e}"),
                    );
                    return Ok(());
                }
                // The committed watermark's record index is how many
                // records of the resume object are already committed:
                // replay them silently.
                let discard = match self.resume.take() {
                    Some(pos) if pos.ordinal == ordinal => pos.record,
                    Some(pos) => {
                        return Err(self.fail(
                            ErrorClass::Fatal,
                            format!(
                                "fetcher started at ordinal {ordinal} but the committed \
                                 resume position is ordinal {} — internal wiring bug",
                                pos.ordinal
                            ),
                        ));
                    }
                    None => 0,
                };
                self.pending_discard = discard;
                self.current = Some(CurrentObject {
                    ordinal,
                    key,
                    next_record: discard,
                    event_time_ms: last_modified_ms,
                });
                Ok(())
            }
            ChunkMsg::Chunk(bytes) => {
                if let Some(m) = &self.metrics {
                    m.bytes_read.increment(bytes.len() as u64);
                }
                if let Err(e) = self.framer.push_chunk(&bytes) {
                    let key = self
                        .current
                        .as_ref()
                        .map_or_else(String::new, |c| c.key.clone());
                    self.poison(
                        PoisonKind::Undecodable,
                        format!("decoding \"{key}\": {e} (corrupt or truncated object?)"),
                    );
                }
                Ok(())
            }
            ChunkMsg::ObjectEnd => {
                if let Err(e) = self.framer.finish_object() {
                    let key = self
                        .current
                        .as_ref()
                        .map_or_else(String::new, |c| c.key.clone());
                    self.poison(
                        PoisonKind::Undecodable,
                        format!("finishing decode of \"{key}\": {e} (truncated object?)"),
                    );
                    return Ok(());
                }
                self.pending_end = true;
                Ok(())
            }
            ChunkMsg::LaneFailed(SplitFailure::Poison(kind, reason)) => {
                self.poison(kind, reason);
                Ok(())
            }
            ChunkMsg::LaneFailed(SplitFailure::Fatal(e)) => {
                let (class, reason) = match e {
                    SourceError::Client { class, reason } => (class, reason),
                    other => (ErrorClass::Fatal, other.to_string()),
                };
                Err(self.fail(class, reason))
            }
        }
    }

    /// Report the decoded-bytes delta at a batch boundary.
    fn report_decoded(&mut self) {
        if let Some(m) = &self.metrics {
            let total = self.framer.decoded_bytes();
            m.bytes_decoded
                .increment(total.saturating_sub(self.decoded_reported));
            self.decoded_reported = total;
        }
    }
}

impl SourceLane for S3Lane {
    type Batch<'a> = S3Batch<'a>;

    fn id(&self) -> LaneId {
        self.id
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        max_records: usize,
        timeout: Duration,
    ) -> Result<Option<S3Batch<'_>>, SourceError> {
        if let Some((class, reason)) = &self.failed {
            return Err(SourceError::Client {
                class: *class,
                reason: reason.clone(),
            });
        }
        if self.poisoned {
            // Quiescent until the control plane retires the lane; bounded
            // idle, never a busy-spin, never an error (a poll error would
            // fail the pipeline — poison must not).
            std::thread::sleep(timeout);
            return Ok(None);
        }
        self.held.clear();
        let deadline = Instant::now() + timeout;
        // Whether this poll has already tried the channel. The deadline
        // must not short-circuit the *first* receive: with `timeout` zero
        // (the driver's head-of-line rotation) `now >= deadline` is true
        // immediately, and bailing before `try_recv` would report the lane
        // empty while a chunk sits ready in its channel — starving it for
        // as long as a sibling lane keeps the rotation fed.
        let mut recv_attempted = false;

        loop {
            self.drain_framer(max_records)?;
            if self.pending_end && !self.poisoned && self.framer.queued() == 0 {
                self.finalize_object();
            }
            if self.poisoned {
                return Ok(None);
            }
            if self.held.len() >= max_records {
                break;
            }
            // `drain_framer` returns early only on a full batch (broken
            // above) or an empty queue, and an empty queue finalizes a
            // pending object end — so no object can still be pending here.
            debug_assert!(!self.pending_end, "pending object end past the drain");
            // The deadline caps the whole poll, not just the idle wait: a
            // stream of chunks that frames no records (one enormous line,
            // whitespace floods) must still return control to the driver —
            // heartbeats and shutdown are processed between polls. Checked
            // only once a receive has been attempted, so a zero-timeout
            // poll still consumes one ready chunk (and the framing pass at
            // the top of the next iteration turns it into records) before
            // this bails.
            if recv_attempted && Instant::now() >= deadline {
                if self.held.is_empty() {
                    return Ok(None);
                }
                break;
            }
            recv_attempted = true;
            let msg = match self.rx.try_recv() {
                Ok(m) => Some(m),
                Err(TryRecvError::Empty) => {
                    if !self.held.is_empty() {
                        break; // hand over what we have instead of waiting
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Ok(None);
                    }
                    let rx = &mut self.rx;
                    // Constructed inside the runtime context (the timer
                    // registers with the ambient reactor); blocks this
                    // pipeline thread only, bounded by the poll timeout.
                    match self
                        .handle
                        .block_on(async { tokio::time::timeout(remaining, rx.recv()).await })
                    {
                        Ok(Some(m)) => Some(m),
                        Ok(None) => None,
                        Err(_) => return Ok(None), // timed out idle
                    }
                }
                Err(TryRecvError::Disconnected) => None,
            };
            match msg {
                Some(m) => {
                    self.on_msg(m)?;
                    if self.poisoned {
                        return Ok(None);
                    }
                }
                None => {
                    // Channel closed. Mid-object it means the fetcher died
                    // without reporting (it always sends LaneFailed on
                    // error), so only a clean end-of-input may pass.
                    if self.current.is_some() {
                        return Err(self.fail(
                            ErrorClass::Fatal,
                            "object stream ended mid-object: the fetcher terminated \
                             unexpectedly"
                                .into(),
                        ));
                    }
                    if self.framer.queued() > 0 {
                        continue; // drain the tail first
                    }
                    if !self.held.is_empty() {
                        break; // final batch now; the decision on the next poll
                    }
                    // Nothing buffered anywhere and no more input: this
                    // poll's Ok(None) is the end-of-input decision (see
                    // module docs) — everything emitted was already handed
                    // out, so `watermark_candidate` is the terminal
                    // watermark.
                    self.tracker.set_terminal(self.watermark_candidate);
                    // Completion is decided here, on a pipeline thread; the
                    // control plane reads it via `take_finishing` between
                    // waits. Wake so the split completes and frees its
                    // working-set slot in microseconds. Once per lane.
                    self.waker.wake();
                    return Ok(None);
                }
            }
        }

        self.report_decoded();
        let last = self
            .held
            .last()
            .expect("non-empty batch at emit time")
            .offset;
        let ack = self.issuer.issue(self.partition, last);
        Ok(Some(S3Batch {
            records: &self.held,
            idx: 0,
            partition: self.partition,
            ack,
        }))
    }
}

/// One poll's records, borrowed from the lane's held buffer.
pub struct S3Batch<'a> {
    records: &'a [HeldRecord],
    idx: usize,
    partition: PartitionId,
    ack: AckRef,
}

impl std::fmt::Debug for S3Batch<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Batch")
            .field("records", &self.records.len())
            .field("idx", &self.idx)
            .finish()
    }
}

impl<'a> PayloadBatch<'a> for S3Batch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let rec = self.records.get(self.idx)?;
        self.idx += 1;
        Some(RawPayload {
            bytes: &rec.bytes,
            key: None,
            partition: self.partition,
            offset: rec.offset,
            timestamp_ms: rec.event_time_ms,
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TestLineFramer;
    use spate_core::checkpoint::Checkpointer;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    struct LaneRig {
        lane: S3Lane,
        tx: Option<mpsc::Sender<ChunkMsg>>,
        tracker: Arc<SplitTracker>,
        poison_rx: std::sync::mpsc::Receiver<PoisonReport>,
        _rt: tokio::runtime::Runtime,
    }

    fn rig(resume: Option<Position>) -> LaneRig {
        let rt = runtime();
        let (tx, rx) = mpsc::channel(64);
        let checkpointer = Checkpointer::new();
        let tracker = Arc::new(SplitTracker::new());
        let (poison_tx, poison_rx) = std::sync::mpsc::channel();
        let lane = S3Lane::new(
            LaneId(0),
            PartitionId(0),
            rx,
            rt.handle().clone(),
            checkpointer.handle(),
            Compression::Auto,
            Arc::new(|| Box::new(TestLineFramer::new(1 << 20))),
            resume,
            SplitId::new("s3-test").unwrap(),
            Arc::clone(&tracker),
            poison_tx,
            ControlWaker::inert(),
            None,
        );
        LaneRig {
            lane,
            tx: Some(tx),
            tracker,
            poison_rx,
            _rt: rt,
        }
    }

    fn send(rig: &LaneRig, msg: ChunkMsg) {
        rig.tx.as_ref().unwrap().try_send(msg).unwrap();
    }

    fn start(rig: &LaneRig, ordinal: u32, key: &str) {
        send(
            rig,
            ChunkMsg::ObjectStart {
                ordinal,
                key: key.into(),
                last_modified_ms: 1_000,
            },
        );
    }

    /// Poll and collect `(offset, bytes)` pairs of the returned batch.
    fn poll_batch(lane: &mut S3Lane, max: usize) -> Option<Vec<(i64, Vec<u8>)>> {
        let batch = lane
            .poll(max, Duration::from_millis(50))
            .expect("poll must succeed");
        batch.map(|mut b| {
            let mut out = Vec::new();
            while let Some(p) = b.next_payload() {
                out.push((p.offset, p.bytes.to_vec()));
            }
            out
        })
    }

    #[test]
    fn frames_records_with_composite_offsets_across_objects() {
        let mut r = rig(None);
        start(&r, 0, "p/a.ndjson");
        send(&r, ChunkMsg::Chunk(bytes::Bytes::from_static(b"a1\na2\n")));
        send(&r, ChunkMsg::ObjectEnd);
        start(&r, 1, "p/b.ndjson");
        send(&r, ChunkMsg::Chunk(bytes::Bytes::from_static(b"b1\n")));
        send(&r, ChunkMsg::ObjectEnd);
        let records = poll_batch(&mut r.lane, 512).unwrap();
        let expect = |ord: u32, rec: u64| {
            Position {
                ordinal: ord,
                record: rec,
            }
            .encode()
            .unwrap()
        };
        assert_eq!(
            records,
            vec![
                (expect(0, 0), b"a1".to_vec()),
                (expect(0, 1), b"a2".to_vec()),
                (expect(1, 0), b"b1".to_vec()),
            ],
            "a batch may span objects; offsets stay monotonic"
        );
    }

    #[test]
    fn terminal_watermark_is_recorded_on_the_poll_after_the_final_batch() {
        let mut r = rig(None);
        start(&r, 0, "p/a.ndjson");
        send(&r, ChunkMsg::Chunk(bytes::Bytes::from_static(b"only\n")));
        send(&r, ChunkMsg::ObjectEnd);
        r.tx.take(); // close the channel: input exhausted
        let records = poll_batch(&mut r.lane, 512).unwrap();
        assert_eq!(records.len(), 1);
        assert!(
            r.tracker.terminal().is_none(),
            "no terminal while the final batch is being handed out"
        );
        assert!(poll_batch(&mut r.lane, 512).is_none());
        let expected = Position {
            ordinal: 0,
            record: 1,
        }
        .encode()
        .unwrap();
        assert_eq!(
            r.tracker.terminal(),
            Some(expected),
            "terminal = one past the last emitted record, decided by the None poll"
        );
    }

    #[test]
    fn empty_input_terminates_at_the_resume_watermark() {
        // A tenancy that emits nothing (resume exactly at end-of-input)
        // must terminate exactly where it began.
        let resume = Position {
            ordinal: 2,
            record: 3,
        };
        let mut r = rig(Some(resume));
        r.tx.take(); // nothing to read
        assert!(poll_batch(&mut r.lane, 512).is_none());
        assert_eq!(r.tracker.terminal(), Some(resume.encode().unwrap()));
    }

    #[test]
    fn resume_discards_the_committed_record_count() {
        // Watermark E(0, 2): two records of object 0 are committed.
        let mut r = rig(Some(Position {
            ordinal: 0,
            record: 2,
        }));
        start(&r, 0, "p/a.ndjson");
        send(
            &r,
            ChunkMsg::Chunk(bytes::Bytes::from_static(b"r0\nr1\nr2\nr3\n")),
        );
        send(&r, ChunkMsg::ObjectEnd);
        let records = poll_batch(&mut r.lane, 512).unwrap();
        let expect = |rec: u64| {
            Position {
                ordinal: 0,
                record: rec,
            }
            .encode()
            .unwrap()
        };
        assert_eq!(
            records,
            vec![(expect(2), b"r2".to_vec()), (expect(3), b"r3".to_vec())],
            "replayed committed records are discarded; indexes continue"
        );
    }

    #[test]
    fn resume_exactly_at_object_end_advances_cleanly() {
        // Watermark E(0, 2) with the object holding exactly two records.
        let mut r = rig(Some(Position {
            ordinal: 0,
            record: 2,
        }));
        start(&r, 0, "p/a.ndjson");
        send(&r, ChunkMsg::Chunk(bytes::Bytes::from_static(b"r0\nr1\n")));
        send(&r, ChunkMsg::ObjectEnd);
        start(&r, 1, "p/b.ndjson");
        send(&r, ChunkMsg::Chunk(bytes::Bytes::from_static(b"next\n")));
        send(&r, ChunkMsg::ObjectEnd);
        let records = poll_batch(&mut r.lane, 512).unwrap();
        assert_eq!(
            records,
            vec![(
                Position {
                    ordinal: 1,
                    record: 0
                }
                .encode()
                .unwrap(),
                b"next".to_vec()
            )]
        );
    }

    #[test]
    fn resume_object_shorter_than_committed_poisons_the_split() {
        // Watermark says 3 records are committed; the object now has 1 —
        // content drift. Object-level, so it must not error the poll.
        let mut r = rig(Some(Position {
            ordinal: 0,
            record: 3,
        }));
        start(&r, 0, "p/a.ndjson");
        send(&r, ChunkMsg::Chunk(bytes::Bytes::from_static(b"only\n")));
        send(&r, ChunkMsg::ObjectEnd);
        assert!(poll_batch(&mut r.lane, 512).is_none());
        let report = r.poison_rx.try_recv().expect("a poison report");
        assert!(report.reason.contains("short"), "{}", report.reason);
        assert_eq!(report.split.as_str(), "s3-test");
        // Quiescent thereafter: no error, no duplicate report, no
        // terminal watermark (the split did not finish).
        assert!(poll_batch(&mut r.lane, 512).is_none());
        assert!(r.poison_rx.try_recv().is_err(), "poison reports once");
        assert!(r.tracker.terminal().is_none());
    }

    #[test]
    fn fetcher_poison_goes_quiescent_and_discards_undelivered_records() {
        let mut r = rig(None);
        start(&r, 0, "p/a.ndjson");
        send(&r, ChunkMsg::Chunk(bytes::Bytes::from_static(b"a1\n")));
        send(
            &r,
            ChunkMsg::LaneFailed(SplitFailure::Poison(
                PoisonKind::NotFound,
                "object vanished".into(),
            )),
        );
        // The framed-but-unacked record must not be delivered past the
        // poison: replay by the next owner would then duplicate it
        // harmlessly, but delivering it here while reporting failure
        // would tangle the split's accounting.
        assert!(poll_batch(&mut r.lane, 512).is_none());
        let report = r.poison_rx.try_recv().expect("a poison report");
        assert!(report.reason.contains("vanished"), "{}", report.reason);
    }

    #[test]
    fn fetcher_fatal_failure_is_terminal() {
        let mut r = rig(None);
        send(
            &r,
            ChunkMsg::LaneFailed(SplitFailure::Fatal(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "access denied".into(),
            })),
        );
        let err = r.lane.poll(512, Duration::from_millis(50)).unwrap_err();
        assert!(err.to_string().contains("access denied"), "{err}");
        let again = r.lane.poll(512, Duration::from_millis(50)).unwrap_err();
        assert!(again.to_string().contains("access denied"), "sticky");
        assert!(r.poison_rx.try_recv().is_err(), "fatal is not poison");
    }

    #[test]
    fn mid_object_disconnect_is_fatal() {
        let mut r = rig(None);
        start(&r, 0, "p/a.ndjson");
        send(&r, ChunkMsg::Chunk(bytes::Bytes::from_static(b"partial")));
        r.tx.take(); // fetcher gone without ObjectEnd or LaneFailed
        // First poll returns the framed data? No — "partial" has no
        // newline and the object never ends, so nothing is emittable.
        let err = r.lane.poll(512, Duration::from_millis(50)).unwrap_err();
        assert!(err.to_string().contains("mid-object"), "{err}");
        assert!(
            r.tracker.terminal().is_none(),
            "a dead lane never terminates"
        );
    }

    #[test]
    fn batch_respects_max_records_and_continues() {
        let mut r = rig(None);
        start(&r, 0, "p/a.ndjson");
        send(
            &r,
            ChunkMsg::Chunk(bytes::Bytes::from_static(b"1\n2\n3\n4\n5\n")),
        );
        send(&r, ChunkMsg::ObjectEnd);
        let first = poll_batch(&mut r.lane, 2).unwrap();
        assert_eq!(first.len(), 2);
        let second = poll_batch(&mut r.lane, 2).unwrap();
        assert_eq!(second.len(), 2);
        let third = poll_batch(&mut r.lane, 2).unwrap();
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].1, b"5".to_vec());
    }

    #[test]
    fn idle_poll_times_out_with_none() {
        let mut r = rig(None);
        let started = Instant::now();
        let polled = r.lane.poll(512, Duration::from_millis(60)).unwrap();
        assert!(polled.is_none());
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "idle poll must block up to the timeout, not busy-spin"
        );
        assert!(r.tracker.terminal().is_none(), "idle is not end-of-input");
    }
}
