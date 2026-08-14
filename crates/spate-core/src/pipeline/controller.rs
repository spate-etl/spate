//! The controller thread: owns the [`Source`] control plane and the
//! [`Checkpointer`].
//!
//! Everything that touches `&mut Source` happens here (pause/resume requests
//! from drivers, commits, and rebalance choreography), so the source
//! implementation never needs internal locking for the control plane.

use super::{DriverEvent, ExitState, FatalErrorReport, ThreadControl};
use crate::admin::HealthState;
use crate::checkpoint::Checkpointer;
use crate::error::{ErrorClass, FatalError, SourceError};
use crate::metrics::{CheckpointMetrics, Meter, PipelineMetrics, PipelineState, SourceMetrics};
use crate::record::PartitionId;
use crate::source::{DrainBarrier, LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Poll cadence while fast-commit partitions are being chased
/// ([`SourceEvent::CommitReady`]). Tight enough that a finishing split's
/// final ack commits within ~a millisecond of resolving, bounded to one
/// commit interval by the chase deadline.
const FAST_COMMIT_POLL: Duration = Duration::from_millis(1);

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
    /// A partition watermark stalled behind a failed batch for longer than
    /// this fails the pipeline (permanent sink failures otherwise leave it
    /// running forever, committing nothing for that partition).
    pub stalled_fail_after: Duration,
    pub checkpoint_metrics: CheckpointMetrics,
    /// Shared with the source at `open` so it can publish consumer lag, which
    /// only the client can measure. The controller records everything else.
    pub source_metrics: Arc<SourceMetrics>,
    /// The source's custom-metrics scope (`spate_<component_type>_source_*`),
    /// handed to it at `open`. `None` unless the source declared a
    /// non-reserved `component_type`.
    pub source_meter: Option<Meter>,
    /// `metrics.per_partition_detail`, forwarded to the source so its own
    /// per-partition families honor the same cardinality gate.
    pub per_partition_detail: bool,
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
        stalled_fail_after,
        checkpoint_metrics,
        source_metrics,
        source_meter,
        per_partition_detail,
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
    if let Err(e) = source.open(
        SourceCtx::new(checkpointer.handle())
            .with_meter(source_meter)
            .with_stage_metrics(Some(Arc::clone(&source_metrics)))
            .with_partition_detail(per_partition_detail),
    ) {
        state.failure = Some(FatalError {
            component: "source".into(),
            reason: format!("source open failed: {e}"),
        });
    }

    let mut last_commit = Instant::now();
    // Fast-commit mode (`SourceEvent::CommitReady`): partitions whose final
    // acks are being chased with a tightened cadence, each with its OWN
    // deadline bounding its chase to one commit interval. A single shared
    // deadline would be re-armed by every new hint, so on a job where units
    // of work finish continuously a permanently stalled partition would never
    // age out and the controller would stay pinned at `FAST_COMMIT_POLL`
    // indefinitely.
    let mut fast_commit: BTreeMap<PartitionId, Instant> = BTreeMap::new();
    // Set when the source reports `SourceEvent::Drained`. The loop exits into
    // the ordinary drain sequence, and `Completed` is additionally required
    // to mean "everything acknowledged and committed" (see the backstop after
    // the final commit below).
    let mut drained_exit = false;

    while state.failure.is_none() && !shutdown.load(Ordering::Relaxed) {
        // Driver requests.
        while let Ok(event) = events_rx.try_recv() {
            handle_driver_event(event, &mut source, &mut state);
        }
        if state.failure.is_some() {
            break;
        }

        // Harvest acknowledgments every pass, not only on the commit tick.
        // Watermark advances retire batches from the drivers'
        // pending-ceiling gates, so harvesting at control cadence reopens a
        // gated lane in ~one `event_poll_timeout` instead of a full commit
        // interval. The commit itself stays on the tick.
        harvest(&mut checkpointer, &mut state);

        // The commit tick.
        if last_commit.elapsed() >= commit_interval {
            last_commit = Instant::now();
            // Seal partial chain buffers before harvesting acknowledgments.
            // A below-target chunk (a low-volume split branch under sustained
            // load) otherwise holds its records' acks, and with them the
            // partition watermark, until it happens to fill. `idle_flush`
            // needs an empty poll that a loaded pipeline never produces.
            // Best-effort like the CommitReady chase below; sealed chunks
            // settle through the sink and commit on a later tick, so watermark
            // staleness is bounded by ~two commit intervals plus the sink
            // linger instead of growing without bound.
            for tx in &control_txs {
                let _ = tx.send(ThreadControl::FlushNow);
            }
            commit_cycle(
                &mut source,
                &mut checkpointer,
                &mut state,
                &checkpoint_metrics,
                &health,
                None,
            );

            // A watermark stalled behind a failed batch is permanent; acks
            // only ever fail, never un-fail. If one has been stalled past the
            // limit, a sink leg is permanently broken (fatal write error,
            // dropped table); fail the pipeline so it restarts and replays
            // rather than running on committing nothing.
            for (partition, since) in checkpointer.stalled_partitions() {
                let age = since.elapsed();
                if age > stalled_fail_after {
                    state.failure.get_or_insert(FatalError {
                        component: "checkpoint".into(),
                        reason: format!(
                            "partition {} watermark stalled behind a failed batch for {age:?} \
                             (limit {stalled_fail_after:?}); a sink leg is permanently failing",
                            partition.0
                        ),
                    });
                    break;
                }
            }
        }
        if state.failure.is_some() {
            break;
        }

        // Source control-plane events. Fast-commit mode tightens the
        // wait so chased acks are committed within ~a millisecond of
        // resolving instead of on the next periodic tick.
        let poll_timeout = if fast_commit.is_empty() {
            event_poll_timeout
        } else {
            event_poll_timeout.min(FAST_COMMIT_POLL)
        };
        match source.poll_events(poll_timeout) {
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
            Ok(SourceEvent::LanesRetired { lanes }) => {
                handle_retired::<S>(
                    lanes,
                    &mut checkpointer,
                    &mut state,
                    &control_txs,
                    &source_metrics,
                );
            }
            Ok(SourceEvent::LanesAdded(lanes)) => {
                handle_added::<S>(
                    lanes,
                    &mut checkpointer,
                    &mut state,
                    &control_txs,
                    &source_metrics,
                );
            }
            Ok(SourceEvent::CommitReady { partitions }) => {
                // Chasing the commit is useless while the records are still
                // buffered in the chain. Flush the owning threads first so
                // the acks being chased can resolve. Without this the tail
                // sits until `idle_flush` elapses, and the unit of work costs
                // a full lull to complete.
                let mut threads: BTreeSet<usize> = BTreeSet::new();
                for p in &partitions {
                    threads.extend(
                        state
                            .assignment
                            .values()
                            .filter(|(part, _)| part == p)
                            .map(|&(_, thread)| thread),
                    );
                }
                for thread in threads {
                    let _ = control_txs[thread].send(ThreadControl::FlushNow);
                }
                // Each hinted partition gets its own window; re-hinting one
                // that is already being chased does not extend it.
                let until = Instant::now() + commit_interval;
                for p in partitions {
                    fast_commit.entry(p).or_insert(until);
                }
            }
            Ok(SourceEvent::Idle) => {}
            Ok(SourceEvent::Drained) => {
                tracing::info!("source drained; starting graceful completion drain");
                drained_exit = true;
                break;
            }
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

        // Chase fast-commit partitions: commit just them, standing down once
        // each is fully resolved and flushed, or at the deadline, so a
        // stalled batch falls back to the periodic tick instead of spinning
        // here.
        if !fast_commit.is_empty() && state.failure.is_none() {
            // Age out partitions past their own window first. A batch that
            // never resolves falls back to the periodic tick instead of
            // holding the tightened cadence open.
            let now = Instant::now();
            fast_commit.retain(|_, &mut until| now < until);
            if !fast_commit.is_empty() {
                let chasing: BTreeSet<PartitionId> = fast_commit.keys().copied().collect();
                commit_cycle(
                    &mut source,
                    &mut checkpointer,
                    &mut state,
                    &checkpoint_metrics,
                    &health,
                    Some(&chasing),
                );
                // Stand down when the unit of work is done, meaning its lane
                // left the assignment, not because its acks look quiet for an
                // instant. A momentary lull between the flush and the sink's
                // acknowledgment would otherwise end the chase early and
                // leave the final watermark to the periodic tick, costing a
                // full commit interval.
                fast_commit.retain(|&p, _| state.assignment.values().any(|&(part, _)| part == p));
            }
        }
    }

    // ---- Drain sequence (shutdown or failure; see graceful-shutdown.mdx) ----
    // Failure-initiated drains must set the process shutdown flag too.
    // Drivers wedged in the blocked-batch retry loop only observe that flag,
    // and main joins them without a timeout, so without this store a chain
    // failure elsewhere leaves a blocked driver spinning forever.
    shutdown.store(true, Ordering::Relaxed);
    pipeline_metrics.set_state(if state.failure.is_some() {
        PipelineState::Failed
    } else {
        PipelineState::Draining
    });
    let deadline = Instant::now() + drain_timeout;

    // Every driver flushes its chain, drops its lanes, and arrives.
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

    // Hand over to main, which joins the driver threads (dropping
    // their chains closes the shard queues) and drains the sink.
    let _ = to_main.send(ControllerSignal::LanesDrained {
        sink_deadline: deadline,
    });
    let sink_budget = deadline.saturating_duration_since(Instant::now()) + Duration::from_secs(2);
    if sink_drained_rx.recv_timeout(sink_budget).is_err() {
        tracing::error!("sink drain did not report before the deadline");
    }

    // The final acknowledgment drain and synchronous commit. Only
    // durably-written batches advanced watermarks; abandoned data replays.
    commit_cycle(
        &mut source,
        &mut checkpointer,
        &mut state,
        &checkpoint_metrics,
        &health,
        None,
    );
    let final_flush_failed = if let Err(e) = source.flush_commits() {
        tracing::error!(error = %e, "final commit flush failed; offsets will replay");
        true
    } else {
        false
    };

    // Drained-exit backstop: a bounded source's `Completed` is read as "the
    // job finished, every record durably committed", so both an
    // unacknowledged tail (a sink that wedged during the drain and had
    // batches abandoned at the deadline) and a final commit that did not
    // persist (there is no next tick to retry a retryable failure into)
    // must surface as a failure, not a clean exit. A healthy drain reaches
    // this point with nothing pending and nothing uncommitted;
    // signal-initiated shutdowns keep their existing semantics (replay on
    // next start).
    if drained_exit && state.failure.is_none() {
        let pending = checkpointer.max_pending();
        if pending > 0 {
            state.failure = Some(FatalError {
                component: "source".into(),
                reason: format!(
                    "source drained but unacknowledged batches remain (max {pending} on one \
                     partition); their data was not durably committed — rerun to complete"
                ),
            });
        } else if !state.pending_commit.is_empty() || final_flush_failed {
            state.failure = Some(FatalError {
                component: "source".into(),
                reason: format!(
                    "source drained and every batch was acknowledged, but the final \
                     watermark commit did not persist ({} partition(s) uncommitted{}); \
                     the checkpoint holds stale offsets — rerun to replay the tail and \
                     complete",
                    state.pending_commit.len(),
                    if final_flush_failed {
                        ", final flush failed"
                    } else {
                        ""
                    },
                ),
            });
        }
        if state.failure.is_some() {
            pipeline_metrics.set_state(PipelineState::Failed);
        }
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
    /// Lanes paused at the source by a driver's backpressure request.
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

/// The pending-ceiling gate for one partition's lanes. `None` (with a
/// warning) if the checkpointer does not track the partition, a controller
/// bug. The lane then runs ungated rather than not at all.
fn pending_gate(
    checkpointer: &Checkpointer,
    partition: PartitionId,
    epoch: u32,
) -> Option<crate::checkpoint::PendingGate> {
    let gate = checkpointer
        .advance_handle(partition)
        .map(|advanced| crate::checkpoint::PendingGate { epoch, advanced });
    if gate.is_none() {
        tracing::warn!(
            partition = partition.0,
            "no advance counter for an assigned partition; its lanes run ungated"
        );
    }
    gate
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

/// Drain registrations and acknowledgments into the trackers and fold the
/// watermarks that advanced into `state.pending_commit` (merged by max, so
/// a retried commit never regresses). Advancing also retires batches from
/// the drivers' pending-ceiling gates, so the controller calls this every
/// loop pass rather than only on the commit tick.
fn harvest(checkpointer: &mut Checkpointer, state: &mut State) {
    let stats = checkpointer.drain();
    if stats.stale_epoch > 0 || stats.unknown > 0 {
        tracing::debug!(
            stale = stats.stale_epoch,
            unknown = stats.unknown,
            "discarded stale acknowledgments"
        );
    }
    for (p, offset) in checkpointer.take_watermarks() {
        let slot = state.pending_commit.entry(p).or_insert(offset);
        *slot = (*slot).max(offset);
    }
}

/// Harvest, publish checkpoint health, and commit what advanced. Failed
/// commits are retried next tick (positions merge by max, so nothing
/// regresses).
fn commit_cycle<S: Source>(
    source: &mut S,
    checkpointer: &mut Checkpointer,
    state: &mut State,
    metrics: &CheckpointMetrics,
    health: &HealthState,
    only: Option<&BTreeSet<PartitionId>>,
) {
    harvest(checkpointer, state);

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
    // A fast-commit pass (`only`) sends just the chased partitions; the
    // rest keep the periodic cadence so a hint never amplifies commit
    // traffic for splits that are still flowing.
    let positions: Vec<(PartitionId, i64)> = state
        .pending_commit
        .iter()
        .filter(|(p, _)| only.is_none_or(|f| f.contains(p)))
        .map(|(&p, &o)| (p, o))
        .collect();
    if positions.is_empty() {
        return;
    }
    let started = Instant::now();
    match source.commit(&positions) {
        Ok(()) => {
            metrics.commit(true, started.elapsed());
            for &(p, o) in &positions {
                state.pending_commit.remove(&p);
                state.committed.insert(p, o);
            }
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
/// drained and revoked here first. `Checkpointer::begin_epoch` replaces all
/// trackers, so committing anything in flight beforehand keeps at-least-once
/// intact.
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
        let gate = pending_gate(checkpointer, partition, state.epoch);
        for lane in group {
            let id = lane.id();
            state.assignment.insert(id, (partition, thread));
            state.thread_load[thread] += 1;
            let _ = control_txs[thread].send(ThreadControl::AddLane {
                lane,
                gate: gate.clone(),
            });
        }
    }

    health.set_assignment_received(true);
    source_metrics.rebalance_assigned();
    source_metrics.set_lanes_active(state.assignment.len());
    pipeline_metrics.set_state(PipelineState::Running);
}

/// Merge additional lanes into the *current* assignment epoch
/// ([`SourceEvent::LanesAdded`]). Existing lanes and their in-flight acks
/// are untouched. The checkpointer's epoch is extended with the new
/// partitions before any new lane reaches a pipeline thread, the same
/// ordering contract as a full assignment.
fn handle_added<S: Source>(
    lanes: Vec<S::Lane>,
    checkpointer: &mut Checkpointer,
    state: &mut State,
    control_txs: &[crossbeam_channel::Sender<ThreadControl<S::Lane>>],
    source_metrics: &SourceMetrics,
) {
    if lanes.is_empty() {
        return;
    }
    let mut groups: HashMap<PartitionId, Vec<S::Lane>> = HashMap::new();
    for lane in lanes {
        groups.entry(lane.partition()).or_default().push(lane);
    }
    let partitions: Vec<PartitionId> = groups.keys().copied().collect();
    if let Err(e) = checkpointer.extend_epoch(&partitions) {
        state.failure.get_or_insert(e);
        return;
    }

    for (partition, group) in groups {
        let thread = state
            .thread_load
            .iter()
            .enumerate()
            .min_by_key(|(_, load)| **load)
            .map(|(i, _)| i)
            .unwrap_or(0);
        // Added lanes join the current epoch, so their gates carry it; the
        // per-partition ceiling makes a newcomer's pressure independent of
        // its siblings', so no controller-side re-pause is needed.
        let gate = pending_gate(checkpointer, partition, state.epoch);
        for lane in group {
            let id = lane.id();
            state.assignment.insert(id, (partition, thread));
            state.thread_load[thread] += 1;
            let _ = control_txs[thread].send(ThreadControl::AddLane {
                lane,
                gate: gate.clone(),
            });
        }
    }
    source_metrics.set_lanes_active(state.assignment.len());
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

/// Remove finished lanes ([`SourceEvent::LanesRetired`]). Their work is
/// fully delivered, acknowledged, and committed, so there is no drain
/// barrier to wait on, nothing to commit, and nothing to flush. The owning
/// threads get a [`ThreadControl::DropLanes`], which they service by
/// dropping the lanes and carrying straight on polling.
///
/// A hot path. A bounded backfill retires one lane per completed unit of
/// work (a coordinated split), so anything synchronous here is paid
/// O(splits) times. Routing it through `StopLanes` instead would run a full
/// `flush_until` per completed split, fragmenting sink batches and parking
/// the owning thread on a blocked chain.
fn handle_retired<S: Source>(
    lanes: Vec<LaneId>,
    checkpointer: &mut Checkpointer,
    state: &mut State,
    control_txs: &[crossbeam_channel::Sender<ThreadControl<S::Lane>>],
    source_metrics: &SourceMetrics,
) {
    let mut by_thread: HashMap<usize, Vec<LaneId>> = HashMap::new();
    for lane in &lanes {
        if let Some(&(_, thread)) = state.assignment.get(lane) {
            by_thread.entry(thread).or_default().push(*lane);
        } else {
            tracing::warn!(lane = lane.0, "retirement for an unassigned lane");
        }
    }
    for (thread, subset) in by_thread {
        let _ = control_txs[thread].send(ThreadControl::DropLanes { lanes: subset });
    }

    let mut retired_parts: HashSet<PartitionId> = lanes
        .iter()
        .filter_map(|l| state.assignment.get(l).map(|&(p, _)| p))
        .collect();
    for lane in &lanes {
        if let Some((_, thread)) = state.assignment.remove(lane) {
            state.thread_load[thread] = state.thread_load[thread].saturating_sub(1);
        }
        state.paused.remove(lane);
    }
    // Drop tracking only for partitions with no remaining lanes; their
    // late acknowledgments (none exist by contract) would be stale.
    let live_partitions: HashSet<PartitionId> =
        state.assignment.values().map(|&(p, _)| p).collect();
    let to_revoke: Vec<PartitionId> = retired_parts
        .drain()
        .filter(|p| !live_partitions.contains(p))
        .collect();
    checkpointer.revoke(&to_revoke);
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
                // Unknown lane: no thread would arrive for it; arrive on its
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
    commit_cycle(
        source,
        checkpointer,
        state,
        checkpoint_metrics,
        health,
        None,
    );
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
    // late acknowledgments are then discarded as stale.
    let live_partitions: HashSet<PartitionId> =
        state.assignment.values().map(|&(p, _)| p).collect();
    let to_revoke: Vec<PartitionId> = revoked_parts
        .drain()
        .filter(|p| !live_partitions.contains(p))
        .collect();
    checkpointer.revoke(&to_revoke);
}
