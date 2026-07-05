//! The controller thread: owns the [`Source`] control plane and the
//! [`Checkpointer`].
//!
//! Everything that touches `&mut Source` happens here — pause/resume
//! requests from drivers, commits, and rebalance choreography — so the
//! source implementation never needs internal locking for the control
//! plane.

use super::{DriverEvent, ExitState, FatalErrorReport, ThreadControl};
use crate::admin::HealthState;
use crate::checkpoint::Checkpointer;
use crate::error::{ErrorClass, FatalError, SourceError};
use crate::metrics::{CheckpointMetrics, PipelineMetrics, PipelineState, SourceMetrics};
use crate::record::PartitionId;
use crate::source::{DrainBarrier, LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Signals the controller sends the runtime's main thread.
#[derive(Debug)]
pub(crate) enum ControllerSignal {
    /// Every driver drained its lanes; main may join the driver threads
    /// and drain the sink within the shared deadline.
    LanesDrained {
        /// Remaining shared drain deadline.
        sink_deadline: Instant,
    },
    /// The controller finished its final commit and exited.
    Finished(ControllerReport),
}

/// Final report from the controller.
#[derive(Debug)]
pub(crate) struct ControllerReport {
    pub state: ExitState,
    pub final_watermarks: Vec<(PartitionId, i64)>,
}

/// Tuning and wiring for the controller loop.
pub(crate) struct ControllerContext<S: Source> {
    pub source: S,
    pub checkpointer: Checkpointer,
    pub control_txs: Vec<crossbeam_channel::Sender<ThreadControl<S::Lane>>>,
    pub events_rx: crossbeam_channel::Receiver<DriverEvent>,
    pub to_main: crossbeam_channel::Sender<ControllerSignal>,
    pub sink_drained_rx: crossbeam_channel::Receiver<()>,
    pub shutdown: Arc<AtomicBool>,
    pub health: Arc<HealthState>,
    pub commit_interval: Duration,
    pub drain_timeout: Duration,
    pub event_poll_timeout: Duration,
    pub checkpoint_metrics: CheckpointMetrics,
    pub source_metrics: SourceMetrics,
    pub pipeline_metrics: PipelineMetrics,
}

pub(crate) fn run_controller<S: Source>(ctx: ControllerContext<S>) {
    let ControllerContext {
        mut source,
        mut checkpointer,
        control_txs,
        events_rx,
        to_main,
        sink_drained_rx,
        shutdown,
        health,
        commit_interval,
        drain_timeout,
        event_poll_timeout,
        checkpoint_metrics,
        source_metrics,
        pipeline_metrics,
    } = ctx;

    let mut state = State {
        assignment: HashMap::new(),
        thread_load: vec![0usize; control_txs.len()],
        paused: HashSet::new(),
        epoch: 0,
        pending_commit: BTreeMap::new(),
        committed: BTreeMap::new(),
        failure: None,
    };

    // Open the source with an issuer handle. A failure here is fatal
    // before any thread has data.
    if let Err(e) = source.open(SourceCtx::new(checkpointer.handle())) {
        state.failure = Some(FatalError {
            component: "source".into(),
            reason: format!("source open failed: {e}"),
        });
    }

    let mut last_commit = Instant::now();

    while state.failure.is_none() && !shutdown.load(Ordering::Relaxed) {
        // 1. Driver requests.
        while let Ok(event) = events_rx.try_recv() {
            handle_driver_event(event, &mut source, &mut state);
        }
        if state.failure.is_some() {
            break;
        }

        // 2. Commit tick.
        if last_commit.elapsed() >= commit_interval {
            last_commit = Instant::now();
            commit_cycle(
                &mut source,
                &mut checkpointer,
                &mut state,
                &checkpoint_metrics,
                &health,
            );
        }

        // 3. Source control-plane events.
        match source.poll_events(event_poll_timeout) {
            Ok(SourceEvent::LanesAssigned(lanes)) => {
                handle_assign(
                    lanes,
                    &mut source,
                    &mut checkpointer,
                    &mut state,
                    &control_txs,
                    &health,
                    &source_metrics,
                    &pipeline_metrics,
                    &checkpoint_metrics,
                    drain_timeout,
                );
            }
            Ok(SourceEvent::LanesRevoked { lanes, barrier }) => {
                handle_revoke(
                    lanes,
                    barrier,
                    &mut source,
                    &mut checkpointer,
                    &mut state,
                    &control_txs,
                    &source_metrics,
                    &checkpoint_metrics,
                    &health,
                    drain_timeout,
                );
            }
            Ok(SourceEvent::Idle) => {}
            Err(e) if is_fatal(&e) => {
                state.failure = Some(FatalError {
                    component: "source".into(),
                    reason: format!("poll_events failed: {e}"),
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "retryable source control-plane error");
            }
        }
    }

    // ---- Drain sequence (shutdown or failure; DESIGN.md § Shutdown) ----
    pipeline_metrics.set_state(if state.failure.is_some() {
        PipelineState::Failed
    } else {
        PipelineState::Draining
    });
    let deadline = Instant::now() + drain_timeout;

    // Step 1: every driver flushes its chain, drops its lanes, and arrives.
    let barrier = DrainBarrier::new(control_txs.len());
    for tx in &control_txs {
        let _ = tx.send(ThreadControl::Shutdown {
            barrier: barrier.clone(),
            deadline,
        });
    }
    if !barrier.wait(drain_timeout) {
        tracing::error!(
            remaining = barrier.remaining(),
            "drivers did not finish draining before the deadline"
        );
    }

    // Step 2: hand over to main, which joins the driver threads (dropping
    // their chains closes the shard queues) and drains the sink.
    let _ = to_main.send(ControllerSignal::LanesDrained {
        sink_deadline: deadline,
    });
    let sink_budget = deadline.saturating_duration_since(Instant::now()) + Duration::from_secs(2);
    if sink_drained_rx.recv_timeout(sink_budget).is_err() {
        tracing::error!("sink drain did not report before the deadline");
    }

    // Step 3: final acknowledgement drain and synchronous commit. Only
    // durably-written batches advanced watermarks; abandoned data replays.
    commit_cycle(
        &mut source,
        &mut checkpointer,
        &mut state,
        &checkpoint_metrics,
        &health,
    );
    if let Err(e) = source.flush_commits() {
        tracing::error!(error = %e, "final commit flush failed; offsets will replay");
    }

    let report = ControllerReport {
        state: match state.failure {
            Some(e) => ExitState::Failed(FatalErrorReport {
                component: e.component,
                reason: e.reason,
            }),
            None => ExitState::Completed,
        },
        final_watermarks: state.committed.into_iter().collect(),
    };
    let _ = to_main.send(ControllerSignal::Finished(report));
}

struct State {
    /// Lane → (partition, owning thread).
    assignment: HashMap<LaneId, (PartitionId, usize)>,
    /// Lanes owned per thread (assignment balancing).
    thread_load: Vec<usize>,
    /// Lanes currently paused at the source.
    paused: HashSet<LaneId>,
    /// Assignment epoch counter (strictly increasing).
    epoch: u32,
    /// Watermarks taken from the checkpointer but not yet successfully
    /// committed (merged by max; retried every tick).
    pending_commit: BTreeMap<PartitionId, i64>,
    /// Everything successfully committed (for the exit report).
    committed: BTreeMap<PartitionId, i64>,
    failure: Option<FatalError>,
}

fn is_fatal(e: &SourceError) -> bool {
    let SourceError::Client { class, .. } = e;
    *class == ErrorClass::Fatal
}

fn handle_driver_event<S: Source>(event: DriverEvent, source: &mut S, state: &mut State) {
    match event {
        DriverEvent::PauseLanes { lanes } => {
            let newly: Vec<LaneId> = lanes
                .into_iter()
                .filter(|l| !state.paused.contains(l) && state.assignment.contains_key(l))
                .collect();
            if newly.is_empty() {
                return;
            }
            match source.pause(&newly) {
                Ok(()) => state.paused.extend(newly),
                Err(e) => tracing::warn!(error = %e, "source pause failed"),
            }
        }
        DriverEvent::ResumeLanes { lanes } => {
            let resumable: Vec<LaneId> = lanes
                .into_iter()
                .filter(|l| state.paused.contains(l))
                .collect();
            if resumable.is_empty() {
                return;
            }
            match source.resume(&resumable) {
                Ok(()) => {
                    for l in &resumable {
                        state.paused.remove(l);
                    }
                }
                Err(e) => tracing::warn!(error = %e, "source resume failed"),
            }
        }
        DriverEvent::Fatal { thread, error } => {
            tracing::error!(thread, error = %error, "pipeline thread reported fatal");
            state.failure.get_or_insert(error);
        }
    }
}

/// Drain acknowledgements, publish checkpoint health, and commit what
/// advanced. Failed commits are retried next tick (positions merge by
/// max, so nothing regresses).
fn commit_cycle<S: Source>(
    source: &mut S,
    checkpointer: &mut Checkpointer,
    state: &mut State,
    metrics: &CheckpointMetrics,
    health: &HealthState,
) {
    let stats = checkpointer.drain();
    if stats.stale_epoch > 0 || stats.unknown > 0 {
        tracing::debug!(
            stale = stats.stale_epoch,
            unknown = stats.unknown,
            "discarded stale acknowledgements"
        );
    }
    for (p, offset) in checkpointer.take_watermarks() {
        let slot = state.pending_commit.entry(p).or_insert(offset);
        *slot = (*slot).max(offset);
    }

    metrics.set_pending_max(checkpointer.max_pending());
    let stalled = checkpointer.stalled_partitions();
    let age = stalled
        .iter()
        .map(|(_, since)| since.elapsed())
        .max()
        .unwrap_or(Duration::ZERO);
    metrics.set_watermark_age(age);
    health.report_watermark(age, checkpointer.max_pending() > 0);

    if state.pending_commit.is_empty() {
        return;
    }
    let positions: Vec<(PartitionId, i64)> =
        state.pending_commit.iter().map(|(&p, &o)| (p, o)).collect();
    let started = Instant::now();
    match source.commit(&positions) {
        Ok(()) => {
            metrics.commit(true, started.elapsed());
            state.committed.append(&mut state.pending_commit);
        }
        Err(e) if is_fatal(&e) => {
            metrics.commit(false, started.elapsed());
            state.failure.get_or_insert(FatalError {
                component: "source".into(),
                reason: format!("commit failed fatally: {e}"),
            });
        }
        Err(e) => {
            metrics.commit(false, started.elapsed());
            tracing::warn!(error = %e, "commit failed; retrying next tick");
        }
    }
}

/// Apply a new assignment. Assignments must describe the FULL new lane set
/// under eager-rebalance semantics (all previous lanes revoked first); if a
/// source hands us an assignment while lanes are still live, they are
/// drained and revoked here first — `Checkpointer::begin_epoch` replaces
/// all trackers, so committing anything in flight beforehand is what keeps
/// at-least-once intact.
#[expect(
    clippy::too_many_arguments,
    reason = "controller state is deliberately spread across owners"
)]
fn handle_assign<S: Source>(
    lanes: Vec<S::Lane>,
    source: &mut S,
    checkpointer: &mut Checkpointer,
    state: &mut State,
    control_txs: &[crossbeam_channel::Sender<ThreadControl<S::Lane>>],
    health: &Arc<HealthState>,
    source_metrics: &SourceMetrics,
    pipeline_metrics: &PipelineMetrics,
    checkpoint_metrics: &CheckpointMetrics,
    drain_timeout: Duration,
) {
    if !state.assignment.is_empty() {
        tracing::warn!(
            live_lanes = state.assignment.len(),
            "assignment received while lanes are live; draining and revoking \
             them first (sources should revoke before reassigning)"
        );
        let live: Vec<LaneId> = state.assignment.keys().copied().collect();
        let barrier = DrainBarrier::new(live.len());
        revoke_lanes(
            &live,
            barrier,
            source,
            checkpointer,
            state,
            control_txs,
            checkpoint_metrics,
            health,
            drain_timeout,
        );
    }

    state.epoch += 1;
    // Lanes of one partition must share a thread (per-partition sequence
    // counters are issuer-local); group by partition, then place groups on
    // the least-loaded thread.
    let mut groups: HashMap<PartitionId, Vec<S::Lane>> = HashMap::new();
    for lane in lanes {
        groups.entry(lane.partition()).or_default().push(lane);
    }
    let partitions: Vec<PartitionId> = groups.keys().copied().collect();
    checkpointer.begin_epoch(&partitions, state.epoch);

    for (partition, group) in groups {
        let thread = state
            .thread_load
            .iter()
            .enumerate()
            .min_by_key(|(_, load)| **load)
            .map(|(i, _)| i)
            .unwrap_or(0);
        for lane in group {
            let id = lane.id();
            state.assignment.insert(id, (partition, thread));
            state.thread_load[thread] += 1;
            let _ = control_txs[thread].send(ThreadControl::AddLane(lane));
        }
    }

    health.set_assignment_received(true);
    source_metrics.rebalance_assigned();
    source_metrics.set_lanes_active(state.assignment.len());
    pipeline_metrics.set_state(PipelineState::Running);
}

#[expect(
    clippy::too_many_arguments,
    reason = "controller state is deliberately spread across owners"
)]
fn handle_revoke<S: Source>(
    lanes: Vec<LaneId>,
    barrier: DrainBarrier,
    source: &mut S,
    checkpointer: &mut Checkpointer,
    state: &mut State,
    control_txs: &[crossbeam_channel::Sender<ThreadControl<S::Lane>>],
    source_metrics: &SourceMetrics,
    checkpoint_metrics: &CheckpointMetrics,
    health: &Arc<HealthState>,
    drain_timeout: Duration,
) {
    source_metrics.rebalance_revoked();
    revoke_lanes(
        &lanes,
        barrier,
        source,
        checkpointer,
        state,
        control_txs,
        checkpoint_metrics,
        health,
        drain_timeout,
    );
    source_metrics.set_lanes_active(state.assignment.len());
}

/// Shared revocation choreography: stop owning threads, wait for the
/// drain, commit what was acknowledged, then drop tracking for partitions
/// with no remaining lanes.
#[expect(
    clippy::too_many_arguments,
    reason = "controller state is deliberately spread across owners"
)]
fn revoke_lanes<S: Source>(
    lanes: &[LaneId],
    barrier: DrainBarrier,
    source: &mut S,
    checkpointer: &mut Checkpointer,
    state: &mut State,
    control_txs: &[crossbeam_channel::Sender<ThreadControl<S::Lane>>],
    checkpoint_metrics: &CheckpointMetrics,
    health: &Arc<HealthState>,
    drain_timeout: Duration,
) {
    let deadline = Instant::now() + drain_timeout;
    let mut by_thread: HashMap<usize, Vec<LaneId>> = HashMap::new();
    for lane in lanes {
        match state.assignment.get(lane) {
            Some(&(_, thread)) => by_thread.entry(thread).or_default().push(*lane),
            None => {
                // Unknown lane: nobody would arrive for it; do so on its
                // behalf so the barrier cannot hang.
                tracing::warn!(lane = lane.0, "revocation for an unassigned lane");
                barrier.arrive();
            }
        }
    }
    for (thread, subset) in by_thread {
        let _ = control_txs[thread].send(ThreadControl::StopLanes {
            lanes: subset,
            barrier: barrier.clone(),
            deadline,
        });
    }
    if !barrier.wait(drain_timeout) {
        tracing::error!(
            remaining = barrier.remaining(),
            "lane drain did not finish before the deadline; unflushed \
             records will replay"
        );
    }

    // Commit everything the drain acknowledged, then forget the partitions
    // that no longer have lanes.
    commit_cycle(source, checkpointer, state, checkpoint_metrics, health);
    if let Err(e) = source.flush_commits() {
        tracing::warn!(error = %e, "flush of stored commits failed during revocation");
    }

    let mut revoked_parts: HashSet<PartitionId> = lanes
        .iter()
        .filter_map(|l| state.assignment.get(l).map(|&(p, _)| p))
        .collect();
    for lane in lanes {
        if let Some((_, thread)) = state.assignment.remove(lane) {
            state.thread_load[thread] = state.thread_load[thread].saturating_sub(1);
        }
        state.paused.remove(lane);
    }
    // Drop tracking only for partitions with no remaining lanes; their
    // late acknowledgements are then discarded as stale.
    let live_partitions: HashSet<PartitionId> =
        state.assignment.values().map(|&(p, _)| p).collect();
    let to_revoke: Vec<PartitionId> = revoked_parts
        .drain()
        .filter(|p| !live_partitions.contains(p))
        .collect();
    checkpointer.revoke(&to_revoke);
}
