//! The pipeline-thread driver loop.
//!
//! One driver owns a set of source lanes, one erased operator chain, and a
//! backpressure controller. Everything here is synchronous; the loop never
//! blocks on a channel send (the backpressure invariant) and heartbeats the
//! liveness probe on every iteration, including while paused, while
//! retrying a blocked batch, and while draining.

use super::{DriverEvent, ThreadControl};
use crate::admin::HealthState;
use crate::backpressure::{InflightBudget, Transition, WatermarkController};
use crate::checkpoint::{AckRef, AdvanceCounter};
use crate::error::{ErrorClass, FatalError, SourceError};
use crate::metrics::{BackpressureMetrics, SourceMetrics};
use crate::ops::{BlockReason, PushOutcome, RunnableChain};
use crate::record::{PartitionId, RawPayload};
use crate::sink::ShardQueues;
use crate::source::{LaneId, PayloadBatch, SourceLane};
use crate::telemetry::RateLimit;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static POLL_ERROR_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));
static GATE_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));

/// Tuning for one driver thread. Constructed by the runtime; exposed to
/// tests.
#[derive(Clone, Debug)]
pub(crate) struct DriverParams {
    /// This thread's index (labels, heartbeats).
    pub thread: usize,
    /// Max payloads per lane poll.
    pub max_records: usize,
    /// Lane poll timeout; also the paused-loop sleep.
    pub poll_timeout: Duration,
    /// Flush the chain after this long without new data (drives partial
    /// encoder chunks out of idle pipelines).
    pub idle_flush: Duration,
    /// Sleep between retries of a blocked batch.
    pub blocked_retry: Duration,
    /// Queue fill ratio below which resume is allowed (mirrors the
    /// backpressure low watermark).
    pub queue_low_ratio: f64,
    /// Hard per-partition ceiling on registered-but-unadvanced batches
    /// (`checkpoint.max_pending_batches`). A lane whose partition is at the
    /// ceiling is skipped, not polled, so pending cannot exceed it.
    pub max_pending_batches: usize,
}

/// How the driver loop ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DriverExit {
    /// Shutdown barrier arrived at; thread done.
    Completed,
    /// A fatal error was reported; thread done.
    Failed,
}

/// Everything a driver thread owns for its lifetime.
pub(crate) struct DriverContext<L> {
    pub params: DriverParams,
    pub control: crossbeam_channel::Receiver<ThreadControl<L>>,
    pub events: crossbeam_channel::Sender<DriverEvent>,
    pub chain: Box<dyn RunnableChain>,
    pub bp: WatermarkController,
    pub budget: Arc<InflightBudget>,
    /// Introspection clones of every installed sink's shard queues; the
    /// backpressure resume gate requires *all* of them below the low ratio.
    pub queues: Vec<ShardQueues>,
    pub health: Arc<HealthState>,
    pub bp_metrics: BackpressureMetrics,
    pub source_metrics: SourceMetrics,
    /// The process-wide shutdown flag: checked inside long-running inner
    /// loops (blocked-batch retries) so a wedged batch cannot defer
    /// shutdown to the barrier timeout.
    pub shutdown: Arc<AtomicBool>,
}

/// Run one driver thread to completion.
pub(crate) fn run_driver<L: SourceLane>(ctx: DriverContext<L>) -> DriverExit {
    let DriverContext {
        params,
        control,
        events,
        mut chain,
        mut bp,
        budget,
        queues,
        health,
        bp_metrics,
        source_metrics,
        shutdown,
    } = ctx;

    let mut lanes: Vec<L> = Vec::new();
    let mut next_lane = 0usize;
    let mut last_data = Instant::now();
    let mut flushed_since_data = false;
    let mut pause_started: Option<Instant> = None;
    // Consecutive empty lane polls; resets on data or a lane-set change.
    let mut empty_polls: usize = 0;
    // A control message received while parked with no lanes (see the
    // lane-less wait below); handled by the drain at the top of the loop.
    let mut parked: Option<ThreadControl<L>> = None;
    // The pending-ceiling gate per owned partition: `issued` counts the
    // batches this thread registered, `advanced` is the controller's
    // retirement counter. A lane is skipped while the difference is at
    // `checkpoint.max_pending_batches`.
    let mut gates: HashMap<PartitionId, GateState> = HashMap::new();
    // Consecutive gated lanes; a full gated pass parks on the control channel.
    let mut gated_streak = 0usize;

    loop {
        health.heartbeat(params.thread);

        // 1. Control messages (never block).
        while let Some(msg) = parked.take().or_else(|| control.try_recv().ok()) {
            match msg {
                ThreadControl::AddLane { lane, gate } => {
                    if let Some(gate) = gate {
                        match gates.get(&lane.partition()) {
                            // A later epoch replaces the gate wholesale;
                            // the issued count restarts with the sequence.
                            Some(g) if g.epoch >= gate.epoch => {}
                            _ => {
                                gates.insert(
                                    lane.partition(),
                                    GateState {
                                        epoch: gate.epoch,
                                        advanced: gate.advanced,
                                        issued: 0,
                                        gated: false,
                                    },
                                );
                            }
                        }
                    }
                    lanes.push(lane);
                }
                ThreadControl::StopLanes {
                    lanes: stop,
                    barrier,
                    deadline,
                } => {
                    let mut stopped = 0usize;
                    lanes.retain(|l| {
                        let goes = stop.contains(&l.id());
                        stopped += usize::from(goes);
                        !goes
                    });
                    prune_gates(&mut gates, &lanes);
                    if stopped > 0 {
                        flush_until(
                            chain.as_mut(),
                            deadline,
                            &mut bp,
                            &events,
                            &health,
                            params.thread,
                        );
                    }
                    // One arrival per stopped lane; the source sized the
                    // barrier by lane count.
                    for _ in 0..stopped {
                        barrier.arrive();
                    }
                }
                ThreadControl::FlushNow => {
                    // Push the tail out now so the acks it holds can resolve
                    // instead of waiting out a full `idle_flush` lull. Held
                    // acks pin partition watermarks under sustained load.
                    match chain.flush() {
                        PushOutcome::Done => flushed_since_data = true,
                        PushOutcome::Blocked { .. } => bp.on_send_rejected(),
                        PushOutcome::Fatal(error) => {
                            let _ = events.send(DriverEvent::Fatal {
                                thread: params.thread,
                                error,
                            });
                        }
                    }
                }
                ThreadControl::DropLanes { lanes: drop } => {
                    // Committed-and-complete lanes are dropped without a flush;
                    // their records are already sink-durable, and flushing here
                    // emits a partial chunk once per completed unit.
                    lanes.retain(|l| !drop.contains(&l.id()));
                    prune_gates(&mut gates, &lanes);
                }
                ThreadControl::Shutdown { barrier, deadline } => {
                    flush_until(
                        chain.as_mut(),
                        deadline,
                        &mut bp,
                        &events,
                        &health,
                        params.thread,
                    );
                    lanes.clear();
                    // Dropping the chain closes this thread's shard-queue
                    // senders; the terminal stage fails the acks of any records
                    // it still parks (its documented Drop contract).
                    drop(chain);
                    barrier.arrive();
                    return DriverExit::Completed;
                }
            }
        }

        // 2. Backpressure transitions. Pause/resume are *requests*; only
        // the controller thread touches the Source.
        let queues_low = queues.iter().all(|q| q.all_below(params.queue_low_ratio));
        if let Some(t) = bp.tick(&budget, queues_low) {
            let owned: Vec<LaneId> = lanes.iter().map(SourceLane::id).collect();
            apply_transition(t, &owned, &events, &bp_metrics, &mut pause_started);
        }
        if bp.is_paused() {
            std::thread::sleep(params.poll_timeout);
            continue;
        }

        // 3. Poll one lane (round-robin), or idle.
        if lanes.is_empty() {
            // Wait on the control channel, not the clock. Sleeping out the full
            // poll timeout delays every control message by up to that long,
            // including `Shutdown` and the `AddLane` handing over the next unit.
            park_on_control(&control, &mut parked, params.poll_timeout);
            idle_flush(
                chain.as_mut(),
                &mut last_data,
                &mut flushed_since_data,
                params.idle_flush,
                &mut bp,
                &events,
                params.thread,
            );
            continue;
        }
        next_lane %= lanes.len();
        let lane_idx = next_lane;
        next_lane += 1;

        // Head-of-line guard: while any lane in the rotation is producing, poll
        // with a zero timeout so one empty lane (a fetcher cold start, a
        // starved partition queue) never parks the thread while sibling lanes
        // hold ready data. Only a full empty pass blocks for the real timeout.
        let lane_timeout = if empty_polls >= lanes.len() {
            params.poll_timeout
        } else {
            Duration::ZERO
        };

        // Pending-ceiling gate: skip a lane whose partition has the full ceiling
        // of batches registered and not yet advanced past. The check precedes
        // the poll and a poll issues one batch, so pending stays at or under the
        // ceiling. A fully gated pass parks on the control channel, where the
        // controller's next harvest reopens the gates.
        let partition = lanes[lane_idx].partition();
        let at_ceiling = match gates.get_mut(&partition) {
            Some(g) => {
                let pending = g.issued.saturating_sub(g.advanced.get());
                let at = pending >= params.max_pending_batches as u64;
                // Edge-triggered: one line when the gate closes, so a partition
                // riding the ceiling reads as a state change rather than a log
                // stream.
                if at && !g.gated {
                    g.gated = true;
                    crate::rate_limited_warn!(
                        GATE_WARN,
                        partition = partition.0,
                        pending,
                        limit = params.max_pending_batches,
                        "partition at the pending-batch ceiling; skipping its \
                         lanes until acknowledgments retire batches"
                    );
                } else if !at && g.gated {
                    g.gated = false;
                    tracing::debug!(partition = partition.0, "pending-batch ceiling reopened");
                }
                at
            }
            None => false,
        };
        if at_ceiling {
            gated_streak += 1;
            if gated_streak >= lanes.len() {
                gated_streak = 0;
                park_on_control(&control, &mut parked, params.poll_timeout);
                idle_flush(
                    chain.as_mut(),
                    &mut last_data,
                    &mut flushed_since_data,
                    params.idle_flush,
                    &mut bp,
                    &events,
                    params.thread,
                );
            }
            continue;
        }
        gated_streak = 0;

        let owned_ids: Vec<LaneId> = lanes.iter().map(SourceLane::id).collect();
        // The poll result borrows the lane's buffers, so the fatal is latched
        // here and acted on once this block ends.
        let fatal_reported = {
            let poll_started = Instant::now();
            let polled = lanes[lane_idx].poll(params.max_records, lane_timeout);
            source_metrics.poll_duration(poll_started.elapsed());

            let mut fatal_reported = false;
            match polled {
                Ok(Some(mut batch)) => {
                    empty_polls = 0;
                    last_data = Instant::now();
                    flushed_since_data = false;
                    if let Some(g) = gates.get_mut(&partition) {
                        let id = batch.ack().batch_id();
                        // A batch stamped with another epoch has its
                        // registration discarded; nothing retires it from the
                        // gate.
                        if id.epoch == g.epoch {
                            g.issued = g.issued.max(id.seq + 1);
                        }
                    }
                    let mut counting = CountingBatch::new(&mut batch);
                    let outcome = drive_batch(
                        chain.as_mut(),
                        &mut counting,
                        &mut bp,
                        &budget,
                        &queues,
                        &params,
                        &events,
                        &owned_ids,
                        &health,
                        &bp_metrics,
                        &mut pause_started,
                        &shutdown,
                    );
                    source_metrics.batch(counting.records, counting.bytes);
                    if let Err(error) = outcome {
                        let _ = events.send(DriverEvent::Fatal {
                            thread: params.thread,
                            error,
                        });
                        fatal_reported = true;
                    }
                }
                Ok(None) => {
                    empty_polls = empty_polls.saturating_add(1);
                    idle_flush(
                        chain.as_mut(),
                        &mut last_data,
                        &mut flushed_since_data,
                        params.idle_flush,
                        &mut bp,
                        &events,
                        params.thread,
                    );
                }
                Err(e) if is_fatal(&e) => {
                    let _ = events.send(DriverEvent::Fatal {
                        thread: params.thread,
                        error: FatalError {
                            component: format!("driver-{}", params.thread),
                            reason: format!("source poll failed: {e}"),
                        },
                    });
                    fatal_reported = true;
                }
                Err(e) => {
                    // Counts toward the empty pass: a lane looping on a
                    // retryable error degrades to the blocking cadence rather
                    // than spinning on zero-timeout polls.
                    empty_polls = empty_polls.saturating_add(1);
                    crate::rate_limited_warn!(
                        POLL_ERROR_WARN,
                        thread = params.thread,
                        error = %e,
                        "retryable source poll error"
                    );
                }
            }
            fatal_reported
        };
        if fatal_reported {
            // Dropping the chain closes this thread's shard-queue senders and
            // fails any parked acks (the terminal's Drop contract).
            drop(chain);
            return park_until_shutdown(&control, lanes, &health, params.thread);
        }
    }
}

/// Driver-side half of one partition's pending-ceiling gate.
struct GateState {
    /// Assignment epoch the gate belongs to; batches from other epochs are
    /// stale and uncounted.
    epoch: u32,
    /// Batches the controller has advanced past (shared counter).
    advanced: AdvanceCounter,
    /// Batches this thread has issued for the partition, derived from the
    /// contiguous ack sequence. The count is exact, since a partition has one
    /// issuing thread.
    issued: u64,
    /// Whether the partition is currently at the ceiling, so the WARN on
    /// closing (and the DEBUG on reopening) fire on the edge, not per poll.
    gated: bool,
}

/// Park on the control channel for up to `poll_timeout`. A thread with nothing
/// pollable (no lanes, or every lane gated) is waiting for a control message,
/// or, when gated, for the controller's next harvest. A received message is
/// stashed for the drain at the top of the loop.
fn park_on_control<L>(
    control: &crossbeam_channel::Receiver<ThreadControl<L>>,
    parked: &mut Option<ThreadControl<L>>,
    poll_timeout: Duration,
) {
    match control.recv_timeout(poll_timeout) {
        Ok(msg) => *parked = Some(msg),
        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
            // The controller is gone and no message can arrive; keep the
            // idle cadence rather than spinning hot.
            std::thread::sleep(poll_timeout);
        }
    }
}

/// Drop gates for partitions that no longer have a lane on this thread.
fn prune_gates<L: SourceLane>(gates: &mut HashMap<PartitionId, GateState>, lanes: &[L]) {
    gates.retain(|p, _| lanes.iter().any(|l| l.partition() == *p));
}

fn is_fatal(e: &SourceError) -> bool {
    let SourceError::Client { class, .. } = e;
    *class == ErrorClass::Fatal
}

/// A fatal thread must not vanish before the drain choreography. The
/// controller sizes its shutdown [`DrainBarrier`](crate::source::DrainBarrier)
/// by thread count, so a driver that returned early would force every
/// fatal-initiated shutdown to burn the full drain timeout waiting on an
/// arrival that can never come. Park here (chain already dropped, lanes
/// released on request) until `Shutdown` arrives, then join the barrier.
fn park_until_shutdown<L: SourceLane>(
    control: &crossbeam_channel::Receiver<ThreadControl<L>>,
    mut lanes: Vec<L>,
    health: &HealthState,
    thread: usize,
) -> DriverExit {
    loop {
        health.heartbeat(thread);
        match control.recv_timeout(Duration::from_millis(50)) {
            // A lane assigned in the fatal→shutdown race: accept and drop
            // it; the failure is already latched, nothing polls it again.
            Ok(ThreadControl::AddLane { lane, .. }) => drop(lane),
            Ok(ThreadControl::StopLanes {
                lanes: stop,
                barrier,
                ..
            }) => {
                let mut stopped = 0usize;
                lanes.retain(|l| {
                    let goes = stop.contains(&l.id());
                    stopped += usize::from(goes);
                    !goes
                });
                for _ in 0..stopped {
                    barrier.arrive();
                }
            }
            Ok(ThreadControl::DropLanes { lanes: drop }) => {
                lanes.retain(|l| !drop.contains(&l.id()));
            }
            // Chain already dropped here; nothing to flush.
            Ok(ThreadControl::FlushNow) => {}
            Ok(ThreadControl::Shutdown { barrier, .. }) => {
                lanes.clear();
                barrier.arrive();
                return DriverExit::Failed;
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return DriverExit::Failed,
        }
    }
}

/// Push one batch through the chain, retrying blocked pushes with the
/// resume cursor until the batch completes.
///
/// The batch borrows the lane's buffers, so the retry loop must hold it.
/// While blocked, the loop keeps ticking the backpressure controller (raising
/// a pause request the first time), heartbeats, and sleeps briefly. The
/// never-block invariant is about channel sends; deferring this thread's
/// *next* poll is what backpressure does.
#[expect(
    clippy::too_many_arguments,
    reason = "free function over disjoint driver-state borrows"
)]
fn drive_batch(
    chain: &mut dyn RunnableChain,
    batch: &mut dyn PayloadBatch<'_>,
    bp: &mut WatermarkController,
    budget: &InflightBudget,
    queues: &[ShardQueues],
    params: &DriverParams,
    events: &crossbeam_channel::Sender<DriverEvent>,
    owned: &[LaneId],
    health: &HealthState,
    bp_metrics: &BackpressureMetrics,
    pause_started: &mut Option<Instant>,
    shutdown: &AtomicBool,
) -> Result<(), FatalError> {
    // Cloned before the first push. If the chain panics the batch may be
    // in an arbitrary state, but this handle can still fail it.
    let ack: AckRef = batch.ack().clone();
    let mut from = 0usize;
    loop {
        health.heartbeat(params.thread);
        let pushed = std::panic::catch_unwind(AssertUnwindSafe(|| chain.push_batch(batch, from)));
        match pushed {
            Ok(PushOutcome::Done) => return Ok(()),
            Ok(PushOutcome::Blocked { resume_at, reason }) => {
                debug_assert!(resume_at >= from, "resume cursor must not go backwards");
                from = resume_at;
                // Abandon a batch that cannot unblock rather than hold shutdown
                // to the barrier deadline; failing its acknowledgment replays
                // the data after restart.
                if shutdown.load(Ordering::Relaxed) {
                    tracing::warn!(
                        thread = params.thread,
                        "shutdown during a blocked batch; abandoning it for replay"
                    );
                    ack.fail();
                    // Discard the chain's mid-batch cursor and not-ready stash;
                    // otherwise the Shutdown-triggered flush trips the resume
                    // asserts or replays the stale payload.
                    chain.abandon_batch();
                    return Ok(());
                }
                // Only sink pressure engages the backpressure controller; a
                // not-ready wait is counted by the chain and retried.
                if reason == BlockReason::Capacity {
                    bp.on_send_rejected();
                    let queues_low = queues.iter().all(|q| q.all_below(params.queue_low_ratio));
                    if let Some(t) = bp.tick(budget, queues_low) {
                        apply_transition(t, owned, events, bp_metrics, pause_started);
                    }
                }
                std::thread::sleep(params.blocked_retry);
            }
            Ok(PushOutcome::Fatal(error)) => {
                ack.fail();
                return Err(error);
            }
            Err(panic) => {
                ack.fail();
                return Err(FatalError {
                    component: format!("driver-{}", params.thread),
                    reason: format!("operator chain panicked: {}", panic_message(panic.as_ref())),
                });
            }
        }
    }
}

/// Wraps a lane batch to count payloads and payload bytes as the chain
/// consumes them, feeding `spate_source_records_total` and
/// `spate_source_bytes_total`. Counting happens per payload (not per derived
/// record), and each payload is yielded exactly once even across
/// blocked-batch retries, so the totals are exact.
struct CountingBatch<'a, 'buf> {
    inner: &'a mut dyn PayloadBatch<'buf>,
    records: u64,
    bytes: u64,
}

impl<'a, 'buf> CountingBatch<'a, 'buf> {
    fn new(inner: &'a mut dyn PayloadBatch<'buf>) -> Self {
        CountingBatch {
            inner,
            records: 0,
            bytes: 0,
        }
    }
}

impl<'buf> PayloadBatch<'buf> for CountingBatch<'_, 'buf> {
    fn next_payload(&mut self) -> Option<RawPayload<'buf>> {
        let payload = self.inner.next_payload()?;
        self.records += 1;
        self.bytes +=
            payload.bytes.len() as u64 + payload.key.map(<[u8]>::len).unwrap_or_default() as u64;
        Some(payload)
    }

    fn ack(&self) -> &AckRef {
        self.inner.ack()
    }
}

/// Best-effort chain flush with a deadline (revocation and shutdown).
/// A flush still blocked at the deadline is abandoned. The terminal
/// stage's Drop contract fails any parked acknowledgments, so abandoned
/// data replays instead of being committed.
fn flush_until(
    chain: &mut dyn RunnableChain,
    deadline: Instant,
    bp: &mut WatermarkController,
    events: &crossbeam_channel::Sender<DriverEvent>,
    health: &HealthState,
    thread: usize,
) {
    loop {
        health.heartbeat(thread);
        match chain.flush() {
            PushOutcome::Done => return,
            PushOutcome::Blocked { .. } => {
                bp.on_send_rejected();
                if Instant::now() >= deadline {
                    tracing::error!(
                        thread,
                        "drain deadline exceeded with the chain still blocked; \
                         abandoning parked records for replay"
                    );
                    return;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            PushOutcome::Fatal(error) => {
                let _ = events.send(DriverEvent::Fatal { thread, error });
                return;
            }
        }
    }
}

/// Flush partial terminal state once per data lull.
fn idle_flush(
    chain: &mut dyn RunnableChain,
    last_data: &mut Instant,
    flushed_since_data: &mut bool,
    after: Duration,
    bp: &mut WatermarkController,
    events: &crossbeam_channel::Sender<DriverEvent>,
    thread: usize,
) {
    if *flushed_since_data || last_data.elapsed() < after {
        return;
    }
    match chain.flush() {
        PushOutcome::Done => *flushed_since_data = true,
        PushOutcome::Blocked { .. } => {
            // Full queues while idle: note the rejection; the main-loop tick
            // raises the pause request and the flush retries next lull.
            bp.on_send_rejected();
        }
        PushOutcome::Fatal(error) => {
            let _ = events.send(DriverEvent::Fatal { thread, error });
        }
    }
}

fn apply_transition(
    t: Transition,
    owned: &[LaneId],
    events: &crossbeam_channel::Sender<DriverEvent>,
    bp_metrics: &BackpressureMetrics,
    pause_started: &mut Option<Instant>,
) {
    match t {
        Transition::Pause => {
            *pause_started = Some(Instant::now());
            bp_metrics.pause_started();
            let _ = events.send(DriverEvent::PauseLanes {
                lanes: owned.to_vec(),
            });
        }
        Transition::Resume => {
            let paused_for = pause_started
                .take()
                .map(|s| s.elapsed())
                .unwrap_or_default();
            bp_metrics.pause_ended(paused_for);
            let _ = events.send(DriverEvent::ResumeLanes {
                lanes: owned.to_vec(),
            });
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
