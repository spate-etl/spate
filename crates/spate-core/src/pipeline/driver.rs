//! The pipeline-thread driver loop.
//!
//! One driver owns a set of source lanes, one erased operator chain, and a
//! backpressure controller. Everything here is synchronous; the loop never
//! blocks on a channel send (the backpressure invariant) and heartbeats the
//! liveness probe on every iteration, including while paused, while
//! retrying a blocked batch, and while draining.

use super::runtime::RuntimeOptions;
use super::{DriverEvent, PauseCause, ThreadControl};
use crate::admin::HealthState;
use crate::backoff::Backoff;
use crate::backpressure::{BackpressureParams, InflightBudget, Transition, WatermarkController};
use crate::checkpoint::{AckRef, AdvanceCounter};
use crate::clock::{Clock, Stopwatch, SystemClock};
use crate::config::{BackpressureSection, CheckpointSection};
use crate::error::{ErrorClass, FatalError};
use crate::hash::FastMap;
use crate::metrics::{BackpressureMetrics, SourceMetrics};
use crate::ops::{BlockReason, PushOutcome, RunnableChain};
use crate::record::{PartitionId, RawPayload};
use crate::sink::ShardQueues;
use crate::source::{LaneId, PayloadBatch, SourceLane};
use crate::telemetry::RateLimit;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static POLL_ERROR_WARN: RateLimit = RateLimit::hot_path();
static GATE_WARN: RateLimit = RateLimit::hot_path();
static CEILING_PAUSE_WARN: RateLimit = RateLimit::hot_path();

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
    /// The longest wait between retries of a push or flush a full shard queue
    /// refused, and the wait before replaying a not-ready payload.
    pub blocked_retry: Duration,
    /// Queue fill ratio below which resume is allowed (mirrors the
    /// backpressure low watermark).
    pub queue_low_ratio: f64,
    /// Hard per-partition ceiling on registered-but-unadvanced batches
    /// (`checkpoint.max_pending_batches`). A lane whose partition is at the
    /// ceiling is skipped, not polled, so pending cannot exceed it.
    pub max_pending_batches: usize,
    /// How long a partition stays at the ceiling before its lanes are paused
    /// at the source (`backpressure.min_pause`).
    pub ceiling_pause_after: Duration,
}

impl DriverParams {
    /// Tuning for driver thread `thread` from the runtime options and the
    /// pipeline's backpressure and checkpoint settings.
    pub(crate) fn new(
        thread: usize,
        options: &RuntimeOptions,
        backpressure: &BackpressureSection,
        checkpoint: &CheckpointSection,
    ) -> Self {
        DriverParams {
            thread,
            max_records: options.max_records,
            poll_timeout: options.poll_timeout,
            idle_flush: options.idle_flush,
            blocked_retry: options.blocked_retry,
            queue_low_ratio: backpressure.low_ratio,
            max_pending_batches: checkpoint.max_pending_batches,
            ceiling_pause_after: backpressure.min_pause,
        }
    }
}

/// The watermark controller's parameters from the pipeline's backpressure
/// settings. A budget past `usize` saturates.
pub(crate) fn backpressure_params(section: &BackpressureSection) -> BackpressureParams {
    BackpressureParams::from_budget(
        usize::try_from(section.max_inflight_bytes.as_u64()).unwrap_or(usize::MAX),
        section.high_ratio,
        section.low_ratio,
        section.min_pause,
    )
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
    /// The drain's flag, set once the drain begins: checked inside
    /// long-running inner loops (blocked-batch retries) so a wedged batch
    /// cannot defer shutdown to the barrier timeout.
    pub shutdown: Arc<AtomicBool>,
    /// Drives the idle flush.
    pub clock: Arc<dyn Clock + Send + Sync>,
    /// Reads the drain deadlines, which the stop path cuts on this clock.
    pub stop_clock: Arc<dyn Clock + Send + Sync>,
}

/// The ends of a [`DriverContext::detached`] thread that its caller drives.
#[cfg(any(test, feature = "testing"))]
pub(crate) struct Detached<L> {
    pub control: crossbeam_channel::Sender<ThreadControl<L>>,
    pub events: crossbeam_channel::Receiver<DriverEvent>,
    pub budget: Arc<InflightBudget>,
    pub health: Arc<HealthState>,
}

#[cfg(any(test, feature = "testing"))]
impl<L> DriverContext<L> {
    /// Driver thread 0 with no runtime around it, on `clock`, and the ends its
    /// caller drives.
    ///
    /// It runs `chain` over `queues` with the given settings. The thread owns
    /// a fresh in-flight budget and health state, its metrics carry `labels`,
    /// and its shutdown flag stays unset.
    pub(crate) fn detached(
        options: &RuntimeOptions,
        backpressure: &BackpressureSection,
        checkpoint: &CheckpointSection,
        chain: Box<dyn RunnableChain>,
        queues: Vec<ShardQueues>,
        labels: &crate::metrics::ComponentLabels,
        clock: Arc<crate::clock::TestClock>,
    ) -> (Self, Detached<L>) {
        let (control_tx, control) = crossbeam_channel::unbounded();
        let (events, events_rx) = crossbeam_channel::unbounded();
        let budget = Arc::new(InflightBudget::new());
        let health =
            HealthState::with_clock(1, crate::admin::HealthThresholds::default(), clock.clone());
        let ctx = DriverContext {
            params: DriverParams::new(0, options, backpressure, checkpoint),
            control,
            events,
            chain,
            bp: WatermarkController::new(backpressure_params(backpressure)),
            budget: Arc::clone(&budget),
            queues,
            health: Arc::clone(&health),
            bp_metrics: BackpressureMetrics::new(labels),
            source_metrics: SourceMetrics::shadow(labels),
            shutdown: Arc::new(AtomicBool::new(false)),
            clock: clock.clone(),
            stop_clock: clock,
        };
        let detached = Detached {
            control: control_tx,
            events: events_rx,
            budget,
            health,
        };
        (ctx, detached)
    }
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
        clock,
        stop_clock,
    } = ctx;

    let mut lanes: Vec<L> = Vec::new();
    let mut next_lane = 0usize;
    let mut last_data = clock.now();
    let mut flushed_since_data = false;
    let mut pause_started: Option<Stopwatch> = None;
    // Consecutive empty or failed lane polls since the last batch; a skipped
    // lane does not count.
    let mut empty_polls: usize = 0;
    // A control message received while parked with no lanes (see the
    // lane-less wait below); handled by the drain at the top of the loop.
    let mut parked: Option<ThreadControl<L>> = None;
    // The pending-ceiling gate per owned partition: `issued` counts the
    // batches this thread registered, `advanced` is the controller's
    // retirement counter. A lane is skipped while the difference is at
    // `checkpoint.max_pending_batches`.
    let mut gates: FastMap<PartitionId, GateState> = FastMap::default();
    // Consecutive lanes skipped as gated or finished; a full skipped pass
    // parks on the control channel.
    let mut skipped_streak = 0usize;

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
                                        closed_at: None,
                                        paused: false,
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
                    // A streak counted against the old rotation would park
                    // the smaller one before it has seen every lane.
                    skipped_streak = 0;
                    if stopped > 0 {
                        flush_until(
                            chain.as_mut(),
                            deadline,
                            &*stop_clock,
                            &mut bp,
                            &budget,
                            &events,
                            &health,
                            params.thread,
                            params.blocked_retry,
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
                        PushOutcome::Done => {
                            flushed_since_data = true;
                            bp.on_push_done();
                        }
                        PushOutcome::Blocked { .. } => bp.on_queue_rejected(&budget),
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
                    skipped_streak = 0;
                }
                ThreadControl::Shutdown { barrier, deadline } => {
                    flush_until(
                        chain.as_mut(),
                        deadline,
                        &*stop_clock,
                        &mut bp,
                        &budget,
                        &events,
                        &health,
                        params.thread,
                        params.blocked_retry,
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
        let queues_low = || queues_below_low(&queues, params.queue_low_ratio);
        if let Some(t) = bp.tick(&budget, queues_low) {
            let owned = lanes.iter().map(SourceLane::id).collect();
            apply_transition(t, owned, &events, &bp_metrics, &mut pause_started);
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
                clock.now().saturating_duration_since(last_data),
                &mut flushed_since_data,
                params.idle_flush,
                &mut bp,
                &budget,
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
        // ceiling. A pass in which every lane is gated or finished parks on the
        // control channel, where the controller's next harvest reopens the
        // gates. A finished lane is never polled again, so its gate is not read.
        let finished = lanes[lane_idx].finished();
        let partition = lanes[lane_idx].partition();
        let at_ceiling = match gates.get_mut(&partition) {
            Some(g) if !finished => {
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
                // A gate that is open and was open on the last pass has
                // nothing to time and nothing to lift.
                if (at || g.closed_at.is_some())
                    && let Some(transition) =
                        g.source_request(at, &*clock, params.ceiling_pause_after)
                {
                    request_for_partition(transition, g.epoch, partition, &lanes, &events);
                }
                at
            }
            _ => false,
        };
        if finished || at_ceiling {
            skipped_streak += 1;
            if skipped_streak >= lanes.len() {
                skipped_streak = 0;
                park_on_control(&control, &mut parked, params.poll_timeout);
                idle_flush(
                    chain.as_mut(),
                    clock.now().saturating_duration_since(last_data),
                    &mut flushed_since_data,
                    params.idle_flush,
                    &mut bp,
                    &budget,
                    &events,
                    params.thread,
                );
            }
            continue;
        }
        skipped_streak = 0;

        // The poll result borrows the lane's buffers, so the fatal is latched
        // here and acted on once this block ends.
        let (fatal_reported, polled_batch) = {
            let (lane, owned) = split_for_poll(&mut lanes, lane_idx);
            let poll_started = Stopwatch::start(&SystemClock);
            let polled = lane.poll(params.max_records, lane_timeout);
            source_metrics.poll_duration(poll_started.elapsed(&SystemClock));

            let mut fatal_reported = false;
            let polled_batch = matches!(polled, Ok(Some(_)));
            match polled {
                Ok(Some(mut batch)) => {
                    empty_polls = 0;
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
                        &owned,
                        &health,
                        &bp_metrics,
                        &mut pause_started,
                        &shutdown,
                    );
                    source_metrics.batch(counting.records, counting.bytes);
                    // A batch with no payloads gives the chain nothing to hold,
                    // so it leaves the lull running that flushes what an
                    // earlier batch parked.
                    if counting.records > 0 {
                        last_data = clock.now();
                        flushed_since_data = false;
                    }
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
                        clock.now().saturating_duration_since(last_data),
                        &mut flushed_since_data,
                        params.idle_flush,
                        &mut bp,
                        &budget,
                        &events,
                        params.thread,
                    );
                }
                Err(e) if e.class() == ErrorClass::Fatal => {
                    let _ = events.send(DriverEvent::Fatal {
                        thread: params.thread,
                        error: FatalError {
                            component: format!("driver-{}", params.thread),
                            reason: format!("source poll failed: {}", crate::error::chain(&e)),
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
                        error = %crate::error::chain(&e),
                        "retryable source poll error"
                    );
                }
            }
            (fatal_reported, polled_batch)
        };
        if polled_batch {
            // The batch is finished, and the lane's next poll may be a long
            // way off behind a gate or a pause.
            lanes[lane_idx].release_batch();
        }
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
    /// When the pass that found the partition at the ceiling ran, for as long
    /// as it has stayed there.
    closed_at: Option<Instant>,
    /// Whether the source was asked to pause the partition's lanes for this
    /// stay at the ceiling.
    paused: bool,
}

impl GateState {
    /// Record a pass that found the partition at the ceiling (`at`) or below
    /// it, and return what to ask of the source: a pause once the partition
    /// has stayed at the ceiling for `pause_after`, and a resume when a paused
    /// one leaves it. The clock is read only at the ceiling.
    #[cold]
    #[inline(never)]
    fn source_request(
        &mut self,
        at: bool,
        clock: &dyn Clock,
        pause_after: Duration,
    ) -> Option<Transition> {
        if !at {
            self.closed_at = None;
            return std::mem::take(&mut self.paused).then_some(Transition::Resume);
        }
        if self.paused {
            return None;
        }
        let now = clock.now();
        let since = *self.closed_at.get_or_insert(now);
        if now.saturating_duration_since(since) < pause_after {
            return None;
        }
        self.paused = true;
        Some(Transition::Pause)
    }
}

/// Ask the controller to pause or resume this thread's lanes on `partition`
/// at the source, for the ceiling gate of assignment `epoch`.
#[cold]
#[inline(never)]
fn request_for_partition<L: SourceLane>(
    transition: Transition,
    epoch: u32,
    partition: PartitionId,
    lanes: &[L],
    events: &crossbeam_channel::Sender<DriverEvent>,
) {
    let lanes: Vec<LaneId> = lanes
        .iter()
        .filter(|l| l.partition() == partition)
        .map(SourceLane::id)
        .collect();
    let cause = PauseCause::Ceiling { epoch };
    let _ = events.send(match transition {
        Transition::Pause => {
            crate::rate_limited_warn!(
                CEILING_PAUSE_WARN,
                partition = partition.0,
                "partition held at the pending-batch ceiling; pausing its lanes at the source"
            );
            DriverEvent::PauseLanes { lanes, cause }
        }
        Transition::Resume => DriverEvent::ResumeLanes { lanes, cause },
    });
}

/// Park on the control channel for up to `poll_timeout`. A thread with nothing
/// pollable (no lanes, or every lane gated or finished) is waiting for a
/// control message, or, when gated, for the controller's next harvest. A
/// received message is stashed for the drain at the top of the loop.
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
fn prune_gates<L: SourceLane>(gates: &mut FastMap<PartitionId, GateState>, lanes: &[L]) {
    gates.retain(|p, _| lanes.iter().any(|l| l.partition() == *p));
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
/// While blocked, the loop records each rejection with the backpressure
/// controller and ticks it, heartbeats, and sleeps for [`retry_wait`]. A rejection
/// raises a pause request at once while a shard worker waits for a write
/// permit, and otherwise once rejections have lasted `min_pause`. The
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
    owned: &dyn Fn() -> Vec<LaneId>,
    health: &HealthState,
    bp_metrics: &BackpressureMetrics,
    pause_started: &mut Option<Stopwatch>,
    shutdown: &AtomicBool,
) -> Result<(), FatalError> {
    // Cloned before the first push. If the chain panics the batch may be
    // in an arbitrary state, but this handle can still fail it.
    let ack: AckRef = batch.ack().clone();
    let mut from = 0usize;
    let mut capacity = None;
    loop {
        health.heartbeat(params.thread);
        let pushed = std::panic::catch_unwind(AssertUnwindSafe(|| chain.push_batch(batch, from)));
        match pushed {
            Ok(PushOutcome::Done) => {
                bp.on_push_done();
                return Ok(());
            }
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
                    bp.on_queue_rejected(budget);
                    let queues_low = || queues_below_low(queues, params.queue_low_ratio);
                    if let Some(t) = bp.tick(budget, queues_low) {
                        apply_transition(t, owned(), events, bp_metrics, pause_started);
                    }
                }
                std::thread::sleep(retry_wait(reason, &mut capacity, params.blocked_retry));
            }
            Ok(PushOutcome::Fatal(error)) => {
                ack.fail();
                return Err(error);
            }
            Err(panic) => {
                ack.fail();
                return Err(FatalError {
                    component: format!("driver-{}", params.thread),
                    reason: format!(
                        "operator chain panicked: {}",
                        crate::task::panic_text(panic.as_ref())
                    ),
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

/// The first wait before retrying a push or flush that a full shard queue
/// refused.
const CAPACITY_RETRY_FIRST: Duration = Duration::from_micros(200);

/// The wait before retrying a blocked push or flush.
///
/// A capacity block waits [`CAPACITY_RETRY_FIRST`], or `blocked_retry` when
/// that is shorter, doubling on each further block that shares `capacity` up to
/// `blocked_retry`. A shard worker drains
/// its queue in passes far shorter than `blocked_retry`, so the first retries
/// usually find room. A not-ready block waits `blocked_retry`.
fn retry_wait(
    reason: BlockReason,
    capacity: &mut Option<Backoff>,
    blocked_retry: Duration,
) -> Duration {
    match reason {
        BlockReason::Capacity => capacity
            .get_or_insert_with(|| Backoff::new(CAPACITY_RETRY_FIRST, blocked_retry))
            .next_delay(),
        BlockReason::NotReady => blocked_retry,
    }
}

/// Best-effort chain flush with a deadline (revocation and shutdown).
/// A flush still blocked at the deadline is abandoned. The terminal
/// stage's Drop contract fails any parked acknowledgments, so abandoned
/// data replays instead of being committed.
#[expect(
    clippy::too_many_arguments,
    reason = "free function over disjoint driver-state borrows"
)]
fn flush_until(
    chain: &mut dyn RunnableChain,
    deadline: Instant,
    stop_clock: &dyn Clock,
    bp: &mut WatermarkController,
    budget: &InflightBudget,
    events: &crossbeam_channel::Sender<DriverEvent>,
    health: &HealthState,
    thread: usize,
    blocked_retry: Duration,
) {
    let mut capacity = None;
    loop {
        health.heartbeat(thread);
        match chain.flush() {
            PushOutcome::Done => {
                bp.on_push_done();
                return;
            }
            PushOutcome::Blocked { .. } => {
                bp.on_queue_rejected(budget);
                if stop_clock.now() >= deadline {
                    tracing::error!(
                        thread,
                        "drain deadline exceeded with the chain still blocked; \
                         abandoning parked records for replay"
                    );
                    return;
                }
                let wait = retry_wait(BlockReason::Capacity, &mut capacity, blocked_retry);
                // A wait never carries the drain past its deadline.
                std::thread::sleep(wait.min(deadline.saturating_duration_since(stop_clock.now())));
            }
            PushOutcome::Fatal(error) => {
                let _ = events.send(DriverEvent::Fatal { thread, error });
                return;
            }
        }
    }
}

/// Flush partial terminal state once per data lull, once `quiet_for` since
/// the last data reaches `after`.
#[expect(
    clippy::too_many_arguments,
    reason = "free function over disjoint driver-state borrows"
)]
fn idle_flush(
    chain: &mut dyn RunnableChain,
    quiet_for: Duration,
    flushed_since_data: &mut bool,
    after: Duration,
    bp: &mut WatermarkController,
    budget: &InflightBudget,
    events: &crossbeam_channel::Sender<DriverEvent>,
    thread: usize,
) {
    if *flushed_since_data || quiet_for < after {
        return;
    }
    match chain.flush() {
        PushOutcome::Done => {
            *flushed_since_data = true;
            bp.on_push_done();
        }
        PushOutcome::Blocked { .. } => {
            // Full queues while idle: record the rejection for the main-loop
            // tick, and retry the flush on the next pass.
            bp.on_queue_rejected(budget);
        }
        PushOutcome::Fatal(error) => {
            let _ = events.send(DriverEvent::Fatal { thread, error });
        }
    }
}

/// The lane at `idx`, borrowed for its poll, and a function that lists every
/// lane's ID in rotation order.
///
/// The poll's batch holds the lane mutably, so a transition raised while the
/// batch is pushed reads the other lanes through this split, and only when it
/// fires.
///
/// # Panics
///
/// When `idx` is not below `lanes.len()`.
fn split_for_poll<L: SourceLane>(
    lanes: &mut [L],
    idx: usize,
) -> (&mut L, impl Fn() -> Vec<LaneId> + '_) {
    let (before, rest) = lanes.split_at_mut(idx);
    let (lane, after) = rest.split_first_mut().expect("idx is below lanes.len()");
    let polled = lane.id();
    let (before, after): (&[L], &[L]) = (before, after);
    let ids = move || {
        before
            .iter()
            .map(SourceLane::id)
            .chain(std::iter::once(polled))
            .chain(after.iter().map(SourceLane::id))
            .collect()
    };
    (lane, ids)
}

/// Whether every shard queue of every installed sink is at or below `ratio`
/// of its capacity, the reading the backpressure controller takes before a
/// resume.
fn queues_below_low(queues: &[ShardQueues], ratio: f64) -> bool {
    queues.iter().all(|q| q.all_below(ratio))
}

fn apply_transition(
    t: Transition,
    owned: Vec<LaneId>,
    events: &crossbeam_channel::Sender<DriverEvent>,
    bp_metrics: &BackpressureMetrics,
    pause_started: &mut Option<Stopwatch>,
) {
    match t {
        Transition::Pause => {
            *pause_started = Some(Stopwatch::start(&SystemClock));
            bp_metrics.pause_started();
            let _ = events.send(DriverEvent::PauseLanes {
                lanes: owned,
                cause: PauseCause::Backpressure,
            });
        }
        Transition::Resume => {
            let paused_for = pause_started
                .take()
                .map(|s| s.elapsed(&SystemClock))
                .unwrap_or_default();
            bp_metrics.pause_ended(paused_for);
            let _ = events.send(DriverEvent::ResumeLanes {
                lanes: owned,
                cause: PauseCause::Backpressure,
            });
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::admin::HealthThresholds;
    use crate::clock::TestClock;
    use crate::metrics::ComponentLabels;
    use crate::pipeline::fakes::{ChainMode, ChainShared, FakeChain, SourceLog};
    use crate::source::{DefaultLane, DrainBarrier};
    use bytesize::ByteSize;
    use spate_test::{HANG, wait_until};
    use std::sync::Mutex;

    fn lanes(ids: &[u32]) -> Vec<DefaultLane> {
        ids.iter().map(|&id| DefaultLane(LaneId(id))).collect()
    }

    /// A paused thread stays paused while a sink queue is above the low ratio,
    /// with the budget already drained, and resumes once the queue is down to
    /// it.
    #[test]
    fn full_shard_queues_hold_a_pause() {
        const BUDGET: u64 = 1_000;
        let clock = TestClock::frozen();
        let options = RuntimeOptions {
            poll_timeout: Duration::from_millis(1),
            ..RuntimeOptions::default()
        };
        // No minimum pause, so only the budget and the queues decide a resume.
        let backpressure = BackpressureSection {
            max_inflight_bytes: ByteSize::b(BUDGET),
            high_ratio: 0.8,
            low_ratio: 0.5,
            min_pause: Duration::ZERO,
        };
        // Four slots at a ratio of 0.5: three queued is above it, two is at it.
        let (queues, mut receivers) = crate::sink::shard_queues(1, 4);
        for _ in 0..3 {
            queues.try_send(0, crate::sink::test_chunk()).unwrap();
        }
        let shared = Arc::new(ChainShared::default());
        let chain = FakeChain {
            shared: Arc::clone(&shared),
            log: Arc::new(Mutex::new(SourceLog::default())),
            mode: ChainMode::Ok,
            batches_seen: 0,
        };
        let (ctx, driver) = DriverContext::<DefaultLane>::detached(
            &options,
            &backpressure,
            &CheckpointSection::default(),
            Box::new(chain),
            vec![queues],
            &ComponentLabels::new("full-queues-hold-a-pause", "driver-0", "driver"),
            clock.clone(),
        );
        let thread = crate::task::spawn_thread("full-queues", move || run_driver(ctx)).unwrap();

        // A failed assertion is caught so the thread still gets its
        // `Shutdown`; the handle joins it on drop.
        let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            driver.budget.add(usize::try_from(BUDGET).unwrap());
            assert!(matches!(
                driver.events.recv_timeout(HANG),
                Ok(DriverEvent::PauseLanes { .. })
            ));
            driver.budget.sub(usize::try_from(BUDGET).unwrap());

            // The channel orders the release before the tick of the pass that
            // serves this flush.
            driver.control.send(ThreadControl::FlushNow).unwrap();
            wait_until(HANG, "the thread serves the flush", || {
                shared.flushes.load(Ordering::Relaxed) >= 1
            });
            // That pass heartbeat before it read the flush, so a heartbeat on the
            // moved clock comes from a later pass, after the tick.
            clock.advance(HealthThresholds::default().heartbeat_stale + Duration::from_millis(1));
            wait_until(HANG, "the thread starts another pass", || {
                driver.health.healthy()
            });
            assert!(
                driver.events.try_recv().is_err(),
                "resumed with three of four slots queued"
            );

            receivers[0].try_recv().unwrap();
            assert!(matches!(
                driver.events.recv_timeout(HANG),
                Ok(DriverEvent::ResumeLanes { .. })
            ));
        }));
        driver
            .control
            .send(ThreadControl::Shutdown {
                barrier: DrainBarrier::new(1),
                deadline: clock.now(),
            })
            .unwrap();
        let exit = thread.join().unwrap();
        if let Err(panic) = checked {
            std::panic::resume_unwind(panic);
        }
        assert_eq!(exit, DriverExit::Completed);
    }

    /// A lane whose first `batches` polls each return one payload.
    struct BatchLane {
        batches: usize,
        ack: crate::checkpoint::AckRef,
    }

    struct OnePayload<'a> {
        ack: &'a crate::checkpoint::AckRef,
        taken: bool,
    }

    impl<'a> PayloadBatch<'a> for OnePayload<'a> {
        fn next_payload(&mut self) -> Option<crate::record::RawPayload<'a>> {
            if std::mem::replace(&mut self.taken, true) {
                return None;
            }
            Some(crate::record::RawPayload {
                bytes: b"x",
                key: None,
                partition: PartitionId(0),
                offset: 0,
                timestamp_ms: 0,
            })
        }

        fn ack(&self) -> &crate::checkpoint::AckRef {
            self.ack
        }
    }

    impl SourceLane for BatchLane {
        type Batch<'a> = OnePayload<'a>;

        fn id(&self) -> LaneId {
            LaneId(0)
        }

        fn partition(&self) -> PartitionId {
            PartitionId(0)
        }

        fn poll(
            &mut self,
            _max_records: usize,
            _timeout: Duration,
        ) -> Result<Option<OnePayload<'_>>, crate::error::SourceError> {
            if self.batches == 0 {
                return Ok(None);
            }
            self.batches -= 1;
            Ok(Some(OnePayload {
                ack: &self.ack,
                taken: false,
            }))
        }
    }

    /// What a [`with_driver`] check drives and reads.
    struct Driving<'a> {
        driver: &'a Detached<BatchLane>,
        clock: &'a TestClock,
        shared: &'a ChainShared,
    }

    impl Driving<'_> {
        fn flushes(&self) -> usize {
            self.shared.flushes.load(Ordering::Relaxed)
        }

        fn wait_for_flushes(&self, n: usize) {
            wait_until(HANG, "the thread flushes", || self.flushes() >= n);
        }

        /// Return once a pass has started after this call. A heartbeat on the
        /// moved clock comes from such a pass.
        fn wait_for_a_later_pass(&self) {
            self.clock
                .advance(HealthThresholds::default().heartbeat_stale + Duration::from_millis(1));
            wait_until(HANG, "the thread starts another pass", || {
                self.driver.health.healthy()
            });
        }

        /// Move the clock past the idle-flush lull. The thread reads the start
        /// of its lull when it starts, so the clock moves only after a pass.
        fn start_the_lull(&self) {
            self.wait_for_a_later_pass();
            self.clock.advance(IDLE);
        }

        fn paused(&self) -> bool {
            match self.driver.events.try_recv() {
                Ok(DriverEvent::PauseLanes { .. }) => true,
                Ok(other) => panic!("unexpected event {other:?}"),
                Err(_) => false,
            }
        }
    }

    /// Run driver thread 0 over `mode`, with `lane` batches when set, until
    /// `check` returns, then shut it down.
    /// Run `check` against a driver thread. With `gated`, one shard is counted
    /// as waiting for a write permit before the thread starts, so its first
    /// push reads it; `check` takes it back with `ungate_shard`.
    fn with_driver(
        mode: ChainMode,
        min_pause: Duration,
        idle_flush: Duration,
        lane: Option<usize>,
        gated: bool,
        check: impl FnOnce(&Driving<'_>),
    ) {
        with_blocked_retry(
            Duration::from_millis(1),
            mode,
            min_pause,
            idle_flush,
            lane,
            gated,
            check,
        );
    }

    /// [`with_driver`] with the thread's `blocked_retry`. A failed `check`
    /// is raised without waiting for the thread, which may be asleep for a
    /// long `blocked_retry`, and leaves the thread running.
    fn with_blocked_retry(
        blocked_retry: Duration,
        mode: ChainMode,
        min_pause: Duration,
        idle_flush: Duration,
        lane: Option<usize>,
        gated: bool,
        check: impl FnOnce(&Driving<'_>),
    ) {
        let clock = TestClock::frozen();
        let options = RuntimeOptions {
            poll_timeout: Duration::from_millis(1),
            idle_flush,
            blocked_retry,
            ..RuntimeOptions::default()
        };
        let backpressure = BackpressureSection {
            min_pause,
            ..BackpressureSection::default()
        };
        let (queues, _receivers) = crate::sink::shard_queues(1, 4);
        let shared = Arc::new(ChainShared::default());
        let chain = FakeChain {
            shared: Arc::clone(&shared),
            log: Arc::new(Mutex::new(SourceLog::default())),
            mode,
            batches_seen: 0,
        };
        let (ctx, driver) = DriverContext::<BatchLane>::detached(
            &options,
            &backpressure,
            &CheckpointSection::default(),
            Box::new(chain),
            vec![queues],
            &ComponentLabels::new("test", spate_test::unique_name("driver"), "driver"),
            clock.clone(),
        );
        let (ack, _acks) = crate::checkpoint::AckRef::test_pair();
        if let Some(batches) = lane {
            driver
                .control
                .send(ThreadControl::AddLane {
                    lane: BatchLane { batches, ack },
                    gate: None,
                })
                .unwrap();
        }
        if gated {
            driver.budget.gate_shard();
        }
        let thread = crate::task::spawn_thread("driver-test", move || run_driver(ctx)).unwrap();
        let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            check(&Driving {
                driver: &driver,
                clock: &clock,
                shared: &shared,
            });
        }));
        if let Err(panic) = checked {
            // Detach the thread: a dropped handle would join it.
            let _ = thread.join_by(clock.now(), &*clock);
            std::panic::resume_unwind(panic);
        }
        driver
            .control
            .send(ThreadControl::Shutdown {
                barrier: DrainBarrier::new(1),
                deadline: clock.now(),
            })
            .unwrap();
        assert_eq!(thread.join().unwrap(), DriverExit::Completed);
    }

    const AN_HOUR: Duration = Duration::from_secs(3600);

    /// The lull after which a thread with no lanes flushes, on the test clock.
    const IDLE: Duration = Duration::from_millis(10);

    #[derive(Clone, Copy)]
    enum Site {
        DriveBatch,
        FlushNow,
        FlushUntil,
        IdleFlush,
    }

    /// Reject one push or flush at `site`, with `min_pause` an hour and a shard
    /// counted as waiting for a permit when `gated`, and report whether the
    /// thread asked for a pause.
    fn one_rejection_pauses(site: Site, gated: bool) -> bool {
        let (mode, idle_flush, lane) = match site {
            Site::DriveBatch => (
                ChainMode::BlockOnce(AtomicBool::new(false)),
                AN_HOUR,
                Some(1),
            ),
            Site::FlushNow => (
                ChainMode::BlockEveryOtherFlush(AtomicBool::new(false)),
                AN_HOUR,
                None,
            ),
            Site::FlushUntil => (
                ChainMode::BlockEveryOtherFlush(AtomicBool::new(false)),
                AN_HOUR,
                Some(0),
            ),
            Site::IdleFlush => (
                ChainMode::BlockEveryOtherFlush(AtomicBool::new(false)),
                IDLE,
                None,
            ),
        };
        let mut paused = false;
        with_driver(mode, AN_HOUR, idle_flush, lane, gated, |d| {
            match site {
                // The push ticks the controller before it retries, so a
                // consumed payload follows that tick.
                Site::DriveBatch => wait_until(HANG, "the batch is taken", || {
                    d.shared.consumed.load(Ordering::Relaxed) == 1
                }),
                // The flush is served before the tick of its pass.
                Site::FlushNow => {
                    d.driver.control.send(ThreadControl::FlushNow).unwrap();
                    d.wait_for_flushes(1);
                    d.wait_for_a_later_pass();
                }
                // A blocked flush, then a completed one, before the tick of
                // the pass that served the stop.
                Site::FlushUntil => {
                    d.driver
                        .control
                        .send(ThreadControl::StopLanes {
                            lanes: vec![LaneId(0)],
                            barrier: DrainBarrier::new(1),
                            deadline: d.clock.now() + AN_HOUR,
                        })
                        .unwrap();
                    d.wait_for_flushes(2);
                    d.wait_for_a_later_pass();
                }
                // The idle flush runs after the tick of its pass, so the tick
                // that reads it is in the pass after.
                Site::IdleFlush => {
                    d.start_the_lull();
                    d.wait_for_flushes(1);
                    d.wait_for_a_later_pass();
                    d.wait_for_a_later_pass();
                }
            }
            paused = d.paused();
            if gated {
                d.driver.budget.ungate_shard();
            }
        });
        paused
    }

    #[test]
    fn a_blocked_push_with_no_shard_gated_does_not_pause() {
        assert!(!one_rejection_pauses(Site::DriveBatch, false));
    }

    #[test]
    fn a_blocked_push_with_a_shard_gated_pauses() {
        assert!(one_rejection_pauses(Site::DriveBatch, true));
    }

    #[test]
    fn a_blocked_flush_now_with_no_shard_gated_does_not_pause() {
        assert!(!one_rejection_pauses(Site::FlushNow, false));
    }

    #[test]
    fn a_blocked_flush_now_with_a_shard_gated_pauses() {
        assert!(one_rejection_pauses(Site::FlushNow, true));
    }

    #[test]
    fn a_blocked_stop_flush_with_no_shard_gated_does_not_pause() {
        assert!(!one_rejection_pauses(Site::FlushUntil, false));
    }

    #[test]
    fn a_blocked_stop_flush_with_a_shard_gated_pauses() {
        assert!(one_rejection_pauses(Site::FlushUntil, true));
    }

    #[test]
    fn a_blocked_idle_flush_with_no_shard_gated_does_not_pause() {
        assert!(!one_rejection_pauses(Site::IdleFlush, false));
    }

    #[test]
    fn a_blocked_idle_flush_with_a_shard_gated_pauses() {
        assert!(one_rejection_pauses(Site::IdleFlush, true));
    }

    /// A push a full queue refused is retried long before `blocked_retry`,
    /// here an hour.
    #[test]
    fn a_capacity_block_retries_before_blocked_retry() {
        with_blocked_retry(
            AN_HOUR,
            ChainMode::BlockOnce(AtomicBool::new(false)),
            AN_HOUR,
            AN_HOUR,
            Some(1),
            false,
            |d| {
                wait_until(HANG, "the refused batch is retried", || {
                    d.shared.consumed.load(Ordering::Relaxed) == 1
                });
            },
        );
    }

    /// A drain flush that a full queue keeps refusing waits no longer than the
    /// time left before the drain's deadline, with `blocked_retry` an hour.
    #[test]
    fn a_blocked_drain_flush_never_waits_past_its_deadline() {
        with_blocked_retry(
            AN_HOUR,
            ChainMode::FlushBlocked,
            AN_HOUR,
            AN_HOUR,
            Some(0),
            false,
            |d| {
                d.driver
                    .control
                    .send(ThreadControl::StopLanes {
                        lanes: vec![LaneId(0)],
                        barrier: DrainBarrier::new(1),
                        deadline: d.clock.now() + Duration::from_millis(1),
                    })
                    .unwrap();
                // Waits doubling from 200 µs would take minutes to reach the
                // twentieth flush; waits clamped to the 1 ms left take 20 ms.
                d.wait_for_flushes(20);
                d.clock.advance(Duration::from_millis(1));
                d.wait_for_a_later_pass();
            },
        );
    }

    /// A drain flush that a full queue refused retries on the capacity
    /// schedule, long before an hour-long `blocked_retry` and deadline.
    #[test]
    fn a_blocked_drain_flush_retries_before_blocked_retry() {
        with_blocked_retry(
            AN_HOUR,
            ChainMode::FlushBlocked,
            AN_HOUR,
            AN_HOUR,
            Some(0),
            false,
            |d| {
                d.driver
                    .control
                    .send(ThreadControl::StopLanes {
                        lanes: vec![LaneId(0)],
                        barrier: DrainBarrier::new(1),
                        deadline: d.clock.now() + AN_HOUR,
                    })
                    .unwrap();
                d.wait_for_flushes(2);
                // Past the deadline, the drain gives up on its next retry.
                d.clock.advance(AN_HOUR);
                d.wait_for_a_later_pass();
            },
        );
    }

    /// A capacity block starts at `CAPACITY_RETRY_FIRST` and doubles to
    /// `blocked_retry`; a not-ready block waits `blocked_retry` and leaves the
    /// capacity schedule where it was.
    #[test]
    fn retry_wait_backs_off_a_capacity_block_to_blocked_retry() {
        let us = Duration::from_micros;
        let mut capacity = None;
        let mut waits = Vec::new();
        for reason in [
            BlockReason::Capacity,
            BlockReason::Capacity,
            BlockReason::NotReady,
            BlockReason::Capacity,
            BlockReason::Capacity,
            BlockReason::Capacity,
            BlockReason::Capacity,
        ] {
            waits.push(retry_wait(reason, &mut capacity, us(2_000)));
        }
        assert_eq!(
            waits,
            [
                us(200),
                us(400),
                us(2_000),
                us(800),
                us(1_600),
                us(2_000),
                us(2_000)
            ]
        );

        let mut capacity = None;
        let waits: Vec<_> = (0..4)
            .map(|_| retry_wait(BlockReason::Capacity, &mut capacity, us(1_000)))
            .collect();
        assert_eq!(waits, [us(200), us(400), us(800), us(1_000)]);
    }

    /// A pause a rejection raised while a shard waited for a permit ends once
    /// the shard gets one, with `min_pause` an hour.
    #[test]
    fn a_gated_pause_resumes_once_the_shard_gets_a_permit() {
        with_driver(
            ChainMode::BlockOnce(AtomicBool::new(false)),
            AN_HOUR,
            AN_HOUR,
            Some(1),
            true,
            |d| {
                wait_until(HANG, "the batch is taken", || {
                    d.shared.consumed.load(Ordering::Relaxed) == 1
                });
                assert!(d.paused());
                d.driver.budget.ungate_shard();
                wait_until(HANG, "the thread resumes", || {
                    match d.driver.events.try_recv() {
                        Ok(DriverEvent::ResumeLanes { .. }) => true,
                        Ok(other) => panic!("unexpected event {other:?}"),
                        Err(_) => false,
                    }
                });
            },
        );
    }

    const MIN_PAUSE: Duration = Duration::from_millis(5);

    /// Batches that are each rejected once and then taken, for longer than
    /// `min_pause`, never pause: each completed push ends the run.
    #[test]
    fn completed_pushes_between_rejections_do_not_pause() {
        with_driver(
            ChainMode::BlockEveryOtherPush(AtomicBool::new(false)),
            MIN_PAUSE,
            AN_HOUR,
            Some(usize::MAX),
            false,
            |d| {
                let start = std::time::Instant::now();
                wait_until(HANG, "batches over four minimum pauses", || {
                    start.elapsed() >= MIN_PAUSE * 4
                        && d.shared.consumed.load(Ordering::Relaxed) >= 4
                });
                assert!(!d.paused());
            },
        );
    }

    /// A flush at `site` is rejected and then completes, and a `FlushNow` is
    /// rejected more than `min_pause` after the first rejection. The completed
    /// flush ended the first run, so the thread does not pause.
    fn a_completed_flush_ends_the_run(site: Site) {
        let idle_flush = if matches!(site, Site::IdleFlush) {
            IDLE
        } else {
            AN_HOUR
        };
        let lane = matches!(site, Site::FlushUntil).then_some(0);
        with_driver(
            ChainMode::BlockEveryOtherFlush(AtomicBool::new(false)),
            MIN_PAUSE,
            idle_flush,
            lane,
            false,
            |d| {
                match site {
                    Site::FlushNow => {
                        d.driver.control.send(ThreadControl::FlushNow).unwrap();
                        d.wait_for_flushes(1);
                        d.driver.control.send(ThreadControl::FlushNow).unwrap();
                    }
                    Site::FlushUntil => {
                        d.driver
                            .control
                            .send(ThreadControl::StopLanes {
                                lanes: vec![LaneId(0)],
                                barrier: DrainBarrier::new(1),
                                deadline: d.clock.now() + AN_HOUR,
                            })
                            .unwrap();
                    }
                    // The next pass's idle flush completes.
                    Site::IdleFlush => d.start_the_lull(),
                    Site::DriveBatch => unreachable!("pushes have their own test"),
                }
                d.wait_for_flushes(2);
                let start = std::time::Instant::now();
                wait_until(HANG, "two minimum pauses pass", || {
                    start.elapsed() >= MIN_PAUSE * 2
                });
                d.driver.control.send(ThreadControl::FlushNow).unwrap();
                d.wait_for_flushes(3);
                d.wait_for_a_later_pass();
                assert!(!d.paused());
            },
        );
    }

    #[test]
    fn a_completed_flush_now_ends_the_run() {
        a_completed_flush_ends_the_run(Site::FlushNow);
    }

    #[test]
    fn a_completed_stop_flush_ends_the_run() {
        a_completed_flush_ends_the_run(Site::FlushUntil);
    }

    #[test]
    fn a_completed_idle_flush_ends_the_run() {
        a_completed_flush_ends_the_run(Site::IdleFlush);
    }

    /// A push that stays blocked with no shard gated pauses once its rejections
    /// span `min_pause`.
    #[test]
    fn a_push_that_stays_blocked_pauses() {
        let open = Arc::new(AtomicBool::new(false));
        with_driver(
            ChainMode::BlockUntil(Arc::clone(&open)),
            Duration::from_millis(10),
            AN_HOUR,
            Some(1),
            false,
            |d| {
                assert!(matches!(
                    d.driver.events.recv_timeout(HANG),
                    Ok(DriverEvent::PauseLanes { .. })
                ));
                // Let the batch through so the thread can serve the shutdown.
                open.store(true, Ordering::Relaxed);
            },
        );
    }

    /// The reading is true only when every queue of every sink is at or below
    /// the ratio, and true with no sink installed.
    #[test]
    fn queues_below_low_needs_every_sinks_queues_below_the_ratio() {
        let (first, _first_rx) = crate::sink::shard_queues(1, 2);
        let (second, _second_rx) = crate::sink::shard_queues(1, 2);
        let both = [first.clone(), second.clone()];
        assert!(queues_below_low(&[], 0.5), "no sink installed");
        assert!(queues_below_low(&both, 0.5), "both empty");
        second.try_send(0, crate::sink::test_chunk()).unwrap();
        assert!(queues_below_low(&both, 0.5), "the second is at the ratio");
        second.try_send(0, crate::sink::test_chunk()).unwrap();
        assert!(!queues_below_low(&both, 0.5), "the second is full");
    }

    /// Wherever the split falls, it lends the lane at that index and lists
    /// every lane's ID in rotation order.
    #[test]
    fn split_for_poll_lists_every_lane_at_every_split() {
        let mut held = lanes(&[0, 1, 2]);
        for idx in 0..held.len() {
            let (lane, ids) = split_for_poll(&mut held, idx);
            assert_eq!(lane.id(), LaneId(u32::try_from(idx).unwrap()));
            assert_eq!(
                ids(),
                vec![LaneId(0), LaneId(1), LaneId(2)],
                "split at {idx}"
            );
        }
    }

    fn gate() -> GateState {
        GateState {
            epoch: 4,
            advanced: AdvanceCounter::new(),
            issued: 0,
            gated: false,
            closed_at: None,
            paused: false,
        }
    }

    /// A partition is paused once it has stayed at the ceiling for the whole
    /// wait, and not a millisecond sooner. The pause is requested once.
    #[test]
    fn a_gate_asks_for_a_pause_once_it_has_stayed_closed_for_the_wait() {
        const WAIT: Duration = Duration::from_millis(500);
        let clock = TestClock::frozen();
        let mut gate = gate();
        assert_eq!(gate.source_request(true, &*clock, WAIT), None);
        clock.advance(WAIT - Duration::from_millis(1));
        assert_eq!(gate.source_request(true, &*clock, WAIT), None);
        clock.advance(Duration::from_millis(1));
        assert_eq!(
            gate.source_request(true, &*clock, WAIT),
            Some(Transition::Pause)
        );
        clock.advance(WAIT);
        assert_eq!(gate.source_request(true, &*clock, WAIT), None);
    }

    /// With no wait configured the pass that finds the partition at the
    /// ceiling asks for the pause.
    #[test]
    fn a_gate_with_no_wait_asks_for_a_pause_on_the_closing_pass() {
        let clock = TestClock::frozen();
        assert_eq!(
            gate().source_request(true, &*clock, Duration::ZERO),
            Some(Transition::Pause)
        );
    }

    /// A paused gate asks for one resume when it reopens, and one that
    /// reopened before its pause asks for nothing.
    #[test]
    fn a_gate_asks_for_a_resume_only_after_its_pause() {
        let clock = TestClock::frozen();
        let mut gate = gate();
        assert_eq!(
            gate.source_request(true, &*clock, Duration::ZERO),
            Some(Transition::Pause)
        );
        assert_eq!(
            gate.source_request(false, &*clock, Duration::ZERO),
            Some(Transition::Resume)
        );
        assert_eq!(gate.source_request(false, &*clock, Duration::ZERO), None);

        const WAIT: Duration = Duration::from_millis(500);
        assert_eq!(gate.source_request(true, &*clock, WAIT), None);
        assert_eq!(gate.source_request(false, &*clock, WAIT), None);
    }

    /// Each stay at the ceiling is timed from its own start: time spent at
    /// the ceiling before a reopening does not count toward the next pause.
    #[test]
    fn a_reopened_gate_times_its_next_closure_from_the_start() {
        const WAIT: Duration = Duration::from_millis(500);
        let clock = TestClock::frozen();
        let mut gate = gate();
        assert_eq!(gate.source_request(true, &*clock, WAIT), None);
        clock.advance(WAIT - Duration::from_millis(1));
        assert_eq!(gate.source_request(false, &*clock, WAIT), None);
        clock.advance(Duration::from_millis(1));
        assert_eq!(gate.source_request(true, &*clock, WAIT), None);
        clock.advance(WAIT - Duration::from_millis(1));
        assert_eq!(gate.source_request(true, &*clock, WAIT), None);
        clock.advance(Duration::from_millis(1));
        assert_eq!(
            gate.source_request(true, &*clock, WAIT),
            Some(Transition::Pause)
        );
    }

    struct PartitionLane(LaneId, PartitionId);

    impl SourceLane for PartitionLane {
        type Batch<'a> = crate::source::NoBatch;
        fn id(&self) -> LaneId {
            self.0
        }
        fn partition(&self) -> PartitionId {
            self.1
        }
        fn poll(
            &mut self,
            _max_records: usize,
            _timeout: Duration,
        ) -> Result<Option<crate::source::NoBatch>, crate::error::SourceError> {
            Ok(None)
        }
    }

    /// A ceiling request names the thread's lanes on that partition and no
    /// other lane, with the gate's epoch.
    #[test]
    fn a_ceiling_request_names_only_the_partitions_lanes() {
        let lanes = [
            PartitionLane(LaneId(0), PartitionId(7)),
            PartitionLane(LaneId(1), PartitionId(8)),
            PartitionLane(LaneId(2), PartitionId(7)),
        ];
        let (tx, rx) = crossbeam_channel::unbounded();
        request_for_partition(Transition::Pause, 4, PartitionId(7), &lanes, &tx);
        request_for_partition(Transition::Resume, 4, PartitionId(7), &lanes, &tx);
        let cause = PauseCause::Ceiling { epoch: 4 };
        let named = vec![LaneId(0), LaneId(2)];
        assert!(matches!(
            rx.try_recv(),
            Ok(DriverEvent::PauseLanes { lanes, cause: c }) if lanes == named && c == cause
        ));
        assert!(matches!(
            rx.try_recv(),
            Ok(DriverEvent::ResumeLanes { lanes, cause: c }) if lanes == named && c == cause
        ));
        assert!(rx.try_recv().is_err());
    }

    /// Each setting reaches the driver field the runtime reads it from. Every
    /// value differs, so a field read from the wrong setting fails.
    #[test]
    fn driver_params_take_each_setting_from_its_own_field() {
        let options = RuntimeOptions {
            max_records: 11,
            poll_timeout: Duration::from_millis(3),
            idle_flush: Duration::from_millis(5),
            blocked_retry: Duration::from_millis(7),
            ..RuntimeOptions::default()
        };
        let backpressure = BackpressureSection {
            max_inflight_bytes: ByteSize::b(1_000),
            high_ratio: 0.9,
            low_ratio: 0.3,
            min_pause: Duration::from_millis(13),
        };
        let checkpoint = CheckpointSection {
            max_pending_batches: 17,
            ..CheckpointSection::default()
        };

        let params = DriverParams::new(2, &options, &backpressure, &checkpoint);
        assert_eq!(params.thread, 2);
        assert_eq!(params.max_records, 11);
        assert_eq!(params.poll_timeout, Duration::from_millis(3));
        assert_eq!(params.idle_flush, Duration::from_millis(5));
        assert_eq!(params.blocked_retry, Duration::from_millis(7));
        assert!((params.queue_low_ratio - 0.3).abs() < f64::EPSILON);
        assert_eq!(params.max_pending_batches, 17);
        assert_eq!(params.ceiling_pause_after, Duration::from_millis(13));

        assert_eq!(
            backpressure_params(&backpressure),
            BackpressureParams {
                high_bytes: 900,
                low_bytes: 300,
                min_pause: Duration::from_millis(13),
            }
        );
    }
}
