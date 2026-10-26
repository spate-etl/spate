//! The controller thread: owns the [`Source`] control plane and the
//! [`Checkpointer`].
//!
//! Everything that touches `&mut Source` happens here (pause/resume requests
//! from drivers, commits, and rebalance choreography), so the source
//! implementation never needs internal locking for the control plane.

use super::runtime::StopRequest;
use super::{DriverEvent, ExitState, FatalErrorReport, ThreadControl};
use crate::admin::HealthState;
use crate::backpressure::InflightBudget;
use crate::checkpoint::Checkpointer;
use crate::clock::Clock;
use crate::error::{ErrorClass, FatalError, SourceError};
use crate::feed::STOP_RESERVE;
use crate::metrics::{
    CheckpointMetrics, InflightBudgetMetrics, Meter, PipelineMetrics, PipelineState, SourceMetrics,
};
use crate::record::PartitionId;
use crate::sink::SinkFailures;
use crate::source::{DrainBarrier, LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Poll cadence while fast-commit partitions are being chased
/// ([`SourceEvent::CommitReady`]). Tight enough that a finishing lane's
/// final ack commits within ~a millisecond of resolving, bounded to one
/// commit interval by the chase deadline, after which the partition is
/// committed on the ordinary cadence.
pub(crate) const FAST_COMMIT_POLL: Duration = Duration::from_millis(1);

/// How long before the drain deadline the drivers and the sink drain end
/// when the pipeline's feed hands back, so the feed's close has this time
/// left inside `checkpoint.drain_timeout`.
pub(crate) const CLOSE_SHARE: Duration = Duration::from_secs(1);

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
    /// The drain deadline, read on the stop clock: the feed's close must end
    /// by then.
    pub close_by: Instant,
}

/// Tuning and wiring for the controller loop.
pub(crate) struct ControllerContext<S: Source> {
    pub source: S,
    pub checkpointer: Checkpointer,
    pub control_txs: Vec<crossbeam_channel::Sender<ThreadControl<S::Lane>>>,
    pub events_rx: crossbeam_channel::Receiver<DriverEvent>,
    pub to_main: crossbeam_channel::Sender<ControllerSignal>,
    pub sink_drained_rx: crossbeam_channel::Receiver<()>,
    /// Set when a graceful stop is asked for. The loop reads on until the
    /// source has nothing left to read or the reserve is reached.
    pub stop: Arc<StopRequest>,
    /// The drivers' flag, set when the drain begins. A driver abandons a
    /// blocked batch once it is set.
    pub shutdown: Arc<AtomicBool>,
    /// The clock a stop's reserve and deadlines are read on.
    pub stop_clock: Arc<dyn Clock + Send + Sync>,
    /// [`CLOSE_SHARE`] when the pipeline's feed hands back, else zero.
    pub close_share: Duration,
    pub health: Arc<HealthState>,
    pub commit_interval: Duration,
    /// How long a `CommitReady` partition is chased at [`FAST_COMMIT_POLL`].
    pub chase_window: Duration,
    pub drain_timeout: Duration,
    pub event_poll_timeout: Duration,
    /// A partition watermark stalled behind a failed batch for longer than
    /// this fails the pipeline (permanent sink failures otherwise leave it
    /// running forever, committing nothing for that partition).
    pub stalled_fail_after: Duration,
    /// Why each sink last abandoned a batch, appended to a failure that a
    /// failed batch caused.
    pub sink_failures: SinkFailures,
    pub checkpoint_metrics: CheckpointMetrics,
    /// The pipeline's in-flight byte budget, shared with every driver and
    /// sink. Read once per loop pass to publish `budget_metrics`.
    pub budget: Arc<InflightBudget>,
    /// The budget gauge's single owner.
    pub budget_metrics: InflightBudgetMetrics,
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
        stop,
        shutdown,
        stop_clock,
        close_share,
        health,
        commit_interval,
        chase_window,
        drain_timeout,
        event_poll_timeout,
        stalled_fail_after,
        sink_failures,
        checkpoint_metrics,
        budget,
        budget_metrics,
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
        revoked_committed: BTreeMap::new(),
        failure: None,
    };

    // Open the source with an issuer handle. A failure here is fatal
    // before any thread has data.
    let stop_signal = crate::source::StopSignal::new(stop.flag());
    if let Err(e) = source.open(
        SourceCtx::new(checkpointer.handle())
            .with_stop(stop_signal.clone())
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
    // deadline is re-armed by every new hint, pinning the controller at
    // `FAST_COMMIT_POLL` while any partition stays stalled.
    let mut fast_commit: BTreeMap<PartitionId, Instant> = BTreeMap::new();
    // Hinted partitions past their window. Each is committed on any pass that
    // harvested an advance for it, at the ordinary poll cadence, until its
    // lanes leave, so an ack resolving after the window skips the tick. The
    // value is the position last sent, so a commit that failed retryably is
    // retried on the next advance or the tick.
    let mut after_window: BTreeMap<PartitionId, Option<i64>> = BTreeMap::new();
    // Set when the source reports `SourceEvent::Drained`. The loop exits into
    // the ordinary drain sequence, and `Completed` is additionally required
    // to mean "everything acknowledged and committed" (see the backstop after
    // the final commit below).
    let mut drained_exit = false;
    let opened = state.failure.is_none();
    // Set on the first pass that sees the stop. The loop keeps running while
    // the source reads on what it holds, until the reserve.
    let mut stopping: Option<StopTimes> = None;

    while state.failure.is_none() {
        if stopping.is_none() && stop.is_set() {
            let trigger = stop.triggered_at().unwrap_or_else(|| stop_clock.now());
            stopping = Some(StopTimes::new(trigger, drain_timeout));
            pipeline_metrics.set_state(PipelineState::Draining);
        }
        if let Some(times) = &stopping
            && (stop_clock.now() >= times.reserve_at || !source.reading_on_stop())
        {
            break;
        }
        // A revocation during the stop's reading drains its lanes by the
        // reserve, which the hand-back, the drain and the close run in.
        let revoke_budget = stopping.as_ref().map_or(drain_timeout, |times| {
            stop_budget(stop_clock.now(), times.reserve_at, drain_timeout)
        });
        let watch = StopWatch {
            stop: &stop,
            shutdown: &shutdown,
            clock: stop_clock.as_ref(),
            drain_timeout,
        };

        // Driver requests.
        while let Ok(event) = events_rx.try_recv() {
            handle_driver_event(event, &mut source, &mut state);
        }
        if state.failure.is_some() {
            break;
        }

        // Sampled once per pass. This loop runs while the drivers are paused.
        budget_metrics.set_inflight_bytes(budget.usage());

        // Harvest acknowledgments every pass, not only on the commit tick.
        // Watermark advances retire batches from the drivers' pending-ceiling
        // gates, so harvesting at control cadence reopens a gated lane in ~one
        // `event_poll_timeout`. The commit itself stays on the tick.
        harvest(&mut checkpointer, &mut state);

        // The commit tick.
        if last_commit.elapsed() >= commit_interval {
            last_commit = Instant::now();
            // Seal partial chain buffers before harvesting acknowledgments. A
            // below-target chunk (a low-volume split branch under sustained
            // load) otherwise holds its records' acks, and with them the
            // partition watermark, until it happens to fill; `idle_flush` needs
            // an empty poll that a loaded pipeline never produces. Best-effort:
            // sealed chunks settle through the sink and commit on a later tick.
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
                false,
            );

            // A watermark stalled behind a failed batch is permanent; acks
            // only ever fail, never un-fail. If one has been stalled past the
            // limit, a sink leg is permanently broken (fatal write error,
            // dropped table); fail the pipeline so it restarts and replays.
            for (partition, since) in checkpointer.stalled_partitions() {
                let age = since.elapsed();
                if age > stalled_fail_after {
                    state.failure.get_or_insert(FatalError {
                        component: "checkpoint".into(),
                        reason: with_sink_failures(
                            format!(
                                "partition {} watermark stalled behind a failed batch for \
                                 {age:?} (limit {stalled_fail_after:?})",
                                partition.0
                            ),
                            &sink_failures,
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
                    revoke_budget,
                    watch,
                );
                // A new epoch may hand back a hinted partition id after
                // revoking its lanes above; its new lanes have not hinted.
                fast_commit.clear();
                after_window.clear();
                // An assignment marks the pipeline running; the stop is still
                // under way.
                if stopping.is_some() {
                    pipeline_metrics.set_state(PipelineState::Draining);
                }
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
                    revoke_budget,
                    watch,
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
                let until = Instant::now() + chase_window;
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
        // each lane leaves the assignment. Past its window a partition moves
        // to `after_window`, so a stalled batch does not spin here.
        if (!fast_commit.is_empty() || !after_window.is_empty()) && state.failure.is_none() {
            // A partition past its window leaves the 1 ms cadence and is
            // committed on ordinary passes.
            let now = Instant::now();
            fast_commit.retain(|&p, &mut until| {
                let open = now < until;
                if !open {
                    after_window.entry(p).or_insert(None);
                }
                open
            });
            // An ack that resolved while `poll_events` waited is not in this
            // pass's first harvest.
            if !after_window.is_empty() {
                harvest(&mut checkpointer, &mut state);
            }
            let late_advance = after_window.iter().any(|(p, sent)| {
                state
                    .pending_commit
                    .get(p)
                    .is_some_and(|&o| Some(o) != *sent)
            });
            if !fast_commit.is_empty() || late_advance {
                let chasing: BTreeSet<PartitionId> = fast_commit
                    .keys()
                    .chain(after_window.keys())
                    .copied()
                    .collect();
                commit_cycle(
                    &mut source,
                    &mut checkpointer,
                    &mut state,
                    &checkpoint_metrics,
                    &health,
                    Some(&chasing),
                    false,
                );
                for (p, sent) in &mut after_window {
                    if let Some(&o) = state.pending_commit.get(p) {
                        *sent = Some(o);
                    }
                }
            }
            // Stand down when the unit of work is done, meaning its lane
            // left the assignment, not because its acks look quiet for an
            // instant. A momentary lull between the flush and the sink's
            // acknowledgment would otherwise end the chase early and
            // leave the final watermark to the periodic tick, costing a
            // full commit interval.
            let assigned = |p: PartitionId| state.assignment.values().any(|&(part, _)| part == p);
            fast_commit.retain(|&p, _| assigned(p));
            after_window.retain(|&p, _| assigned(p));
        }
    }

    // ---- Drain sequence (shutdown or failure; see graceful-shutdown.mdx) ----
    // Every drain sets the drivers' flag here, a stop's only once its reading
    // ends, so a batch blocked during the reading keeps retrying. Drivers
    // wedged in the blocked-batch retry loop only observe that flag, and main
    // joins them without a timeout, so without this store a chain failure
    // elsewhere leaves a blocked driver spinning forever.
    shutdown.store(true, Ordering::Relaxed);
    // The drain's commits, the final one included, run with the signal clear.
    stop_signal.close();
    pipeline_metrics.set_state(if state.failure.is_some() {
        PipelineState::Failed
    } else {
        PipelineState::Draining
    });
    // Hand back what the stop's reading did not finish, before the drivers
    // drain, so a peer can take it while this instance drains its sink. A
    // failed run reads nothing more and hands back at once.
    if opened
        && !drained_exit
        && let Err(e) = source.stop_reading()
    {
        tracing::warn!(error = %e, "the source could not hand back the units it holds");
    }
    // A graceful stop's deadline counts from its trigger, so the reading
    // above spends from the same `drain_timeout`.
    let deadline =
        stopping.map_or_else(|| stop_clock.now() + drain_timeout, |times| times.deadline);
    let lanes_by = lanes_deadline(stop_clock.now(), deadline, close_share);

    // Every driver flushes its chain, drops its lanes, and arrives.
    let barrier = DrainBarrier::new(control_txs.len());
    for tx in &control_txs {
        let _ = tx.send(ThreadControl::Shutdown {
            barrier: barrier.clone(),
            deadline: lanes_by,
        });
    }
    if !barrier.wait(lanes_by.saturating_duration_since(stop_clock.now())) {
        tracing::error!(
            remaining = barrier.remaining(),
            "drivers did not finish draining before the deadline"
        );
    }

    // A driver's last flush can report a fatal, and the loop that reads
    // `events_rx` has already been left to send `Shutdown`. Take what the
    // drain produced, so a run that abandoned a batch exits non-zero. Only the
    // failure: every driver has dropped its lanes by now, so a pause request
    // queued behind it addresses an assignment nothing holds.
    while let Ok(event) = events_rx.try_recv() {
        if let DriverEvent::Fatal { thread, error } = event {
            tracing::error!(thread, error = %error, "pipeline thread reported fatal");
            state.failure.get_or_insert(error);
        }
    }
    if state.failure.is_some() {
        pipeline_metrics.set_state(PipelineState::Failed);
    }

    // Hand over to main, which joins the driver threads (dropping
    // their chains closes the shard queues) and drains the sink.
    let _ = to_main.send(ControllerSignal::LanesDrained {
        sink_deadline: lanes_by,
    });
    let sink_budget = lanes_by.saturating_duration_since(stop_clock.now()) + Duration::from_secs(2);
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
        true,
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
                reason: with_sink_failures(
                    format!(
                        "source drained but unacknowledged batches remain (max {pending} on \
                         one partition); their data was not durably committed — rerun to \
                         complete"
                    ),
                    &sink_failures,
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
        close_by: deadline,
    };
    let _ = to_main.send(ControllerSignal::Finished(report));
}

/// How often a revocation's wait looks for a stop.
const STOP_POLL: Duration = Duration::from_millis(10);

/// What a revocation needs to end its wait for a stop.
#[derive(Clone, Copy)]
struct StopWatch<'a> {
    stop: &'a StopRequest,
    shutdown: &'a AtomicBool,
    clock: &'a (dyn Clock + Send + Sync),
    drain_timeout: Duration,
}

impl StopWatch<'_> {
    /// Whether a stop is under way that the drivers must serve now: its
    /// source reads nothing on, or its reserve is reached.
    fn ends_the_wait<S: Source>(&self, source: &mut S) -> bool {
        if !self.stop.is_set() {
            return false;
        }
        let trigger = self.stop.triggered_at().unwrap_or_else(|| self.clock.now());
        let times = StopTimes::new(trigger, self.drain_timeout);
        self.clock.now() >= times.reserve_at || !source.reading_on_stop()
    }
}

/// Wait for a revocation's `barrier` until `deadline`. A stop that the
/// drivers must serve now sets their flag, as the drain does, so a driver
/// held by a blocked batch abandons it for replay and arrives.
fn wait_revocation<S: Source>(
    barrier: &DrainBarrier,
    deadline: Instant,
    source: &mut S,
    watch: StopWatch<'_>,
) -> bool {
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if barrier.wait(left.min(STOP_POLL)) {
            return true;
        }
        if left <= STOP_POLL {
            return false;
        }
        // The source gets no `poll_events` while the drain runs, so a lease
        // it holds would lapse unless it renews here.
        source.renew_held();
        if !watch.shutdown.load(Ordering::Relaxed) && watch.ends_the_wait(source) {
            watch.shutdown.store(true, Ordering::Relaxed);
        }
    }
}

/// The instants a graceful stop runs to, both counted from its trigger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StopTimes {
    /// When the source stops reading: [`STOP_RESERVE`] before the deadline,
    /// and never before the trigger.
    reserve_at: Instant,
    /// The drain deadline: the trigger plus `checkpoint.drain_timeout`.
    deadline: Instant,
}

impl StopTimes {
    fn new(trigger: Instant, drain_timeout: Duration) -> StopTimes {
        let deadline = trigger + drain_timeout;
        let reserve_at = deadline
            .checked_sub(STOP_RESERVE)
            .map_or(trigger, |at| at.max(trigger));
        StopTimes {
            reserve_at,
            deadline,
        }
    }
}

/// `fresh`, cut so that it ends by `deadline`.
fn stop_budget(now: Instant, deadline: Instant, fresh: Duration) -> Duration {
    fresh.min(deadline.saturating_duration_since(now))
}

/// The instant the drivers and the sink drain end by: `close_share` before
/// `deadline`, and never before `now`.
fn lanes_deadline(now: Instant, deadline: Instant, close_share: Duration) -> Instant {
    deadline
        .checked_sub(close_share)
        .map_or(now, |by| by.max(now))
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
    /// Last committed watermark of each partition still tracked (for the
    /// exit report).
    committed: BTreeMap<PartitionId, i64>,
    /// Exit-report entries of partitions revoked since the last assignment.
    /// The next assignment moves back the ones it hands back and drops the
    /// rest; a lane gain drops them all, since a revoked partition returns
    /// only in a new epoch.
    revoked_committed: BTreeMap<PartitionId, i64>,
    failure: Option<FatalError>,
}

/// `reason`, followed by each sink that abandoned a batch and why.
fn with_sink_failures(reason: String, sink_failures: &SinkFailures) -> String {
    match sink_failures.describe() {
        Some(sinks) => format!("{reason}; {sinks}"),
        None => reason,
    }
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

/// Harvest, publish checkpoint health, and commit what advanced. A failed
/// tick commit is retried next tick (positions merge by max, so nothing
/// regresses). The final pass has no next tick. Positions it does not commit
/// stay uncommitted and may replay.
fn commit_cycle<S: Source>(
    source: &mut S,
    checkpointer: &mut Checkpointer,
    state: &mut State,
    metrics: &CheckpointMetrics,
    health: &HealthState,
    only: Option<&BTreeSet<PartitionId>>,
    final_pass: bool,
) {
    harvest(checkpointer, state);

    let max_pending = checkpointer.max_pending();
    metrics.set_pending_max(max_pending);
    // Gathering the counts allocates, so the default configuration must not
    // pay for it. The chase pass is skipped for the same reason. It runs at
    // `FAST_COMMIT_POLL` through each hinted partition's window, then on
    // every advancing pass until the partition's lanes leave.
    // The periodic tick still fires during a chase, so the detail trails the
    // aggregate by at most one interval.
    if only.is_none() && metrics.publishes_partition_detail() {
        let pending = checkpointer.pending_by_partition();
        for &(partition, n) in &pending {
            metrics.set_partition_pending(partition, n);
        }
        // The tracker set is the live set. `begin_epoch` replaces it and
        // `revoke` removes from it, so retaining against it needs no hook in
        // the revocation paths.
        let live: Vec<PartitionId> = pending.iter().map(|&(p, _)| p).collect();
        metrics.retain_partitions(&live);
    }

    let stalled = checkpointer.stalled_partitions();
    let age = stalled
        .iter()
        .map(|(_, since)| since.elapsed())
        .max()
        .unwrap_or(Duration::ZERO);
    metrics.set_watermark_age(age);
    health.report_watermark(age, max_pending > 0);

    if state.pending_commit.is_empty() {
        return;
    }
    // A fast-commit pass (`only`) sends just the chased partitions; the
    // rest keep the periodic cadence so a hint never amplifies commit
    // traffic for lanes that are still flowing.
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
                // A retried commit can land after its partition left; the
                // exit report lists it only if an assignment hands it back.
                if checkpointer.tracks(p) {
                    state.committed.insert(p, o);
                } else {
                    state.revoked_committed.insert(p, o);
                }
            }
        }
        Err(e) if is_fatal(&e) => {
            metrics.commit(false, started.elapsed());
            state.failure.get_or_insert(FatalError {
                component: "source".into(),
                reason: format!("commit failed fatally: {e}"),
            });
        }
        Err(e) if final_pass => {
            metrics.commit(false, started.elapsed());
            tracing::warn!(error = %e, "final commit failed; uncommitted positions may replay");
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
    watch: StopWatch<'_>,
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
            watch,
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
    // An eager rebalance revokes every lane before handing most partitions
    // back; a partition handed back keeps its watermark in the exit report.
    for (p, o) in std::mem::take(&mut state.revoked_committed) {
        if groups.contains_key(&p) {
            state.committed.entry(p).or_insert(o);
        }
    }

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
    // A revoked partition cannot return in this epoch, so its exit-report
    // entry has nowhere to go.
    state.revoked_committed.clear();
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
    watch: StopWatch<'_>,
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
        watch,
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
/// work (a feed unit part), so anything synchronous here is paid
/// once per unit part. Routing it through `StopLanes` instead would run a full
/// `flush_until` per completed unit part, fragmenting sink batches and parking
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
    // Drop tracking and the exit-report entry only for partitions with no
    // remaining lanes; their late acknowledgments (none exist by contract)
    // would be stale.
    let live_partitions: HashSet<PartitionId> =
        state.assignment.values().map(|&(p, _)| p).collect();
    let to_revoke: Vec<PartitionId> = retired_parts
        .drain()
        .filter(|p| !live_partitions.contains(p))
        .collect();
    checkpointer.revoke(&to_revoke);
    for p in &to_revoke {
        state.committed.remove(p);
    }
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
    watch: StopWatch<'_>,
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
    if !wait_revocation(&barrier, deadline, source, watch) {
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
        false,
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
    // Drop tracking only for partitions with no remaining lanes; their late
    // acknowledgments are then discarded as stale. Their exit-report entries
    // wait for the next assignment, which may hand them back.
    let live_partitions: HashSet<PartitionId> =
        state.assignment.values().map(|&(p, _)| p).collect();
    let to_revoke: Vec<PartitionId> = revoked_parts
        .drain()
        .filter(|p| !live_partitions.contains(p))
        .collect();
    checkpointer.revoke(&to_revoke);
    for p in &to_revoke {
        if let Some(o) = state.committed.remove(p) {
            state.revoked_committed.insert(*p, o);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins that the reserve sits `STOP_RESERVE` before the deadline, and at
    /// the trigger when the drain timeout is shorter than the reserve.
    #[test]
    fn a_stop_reads_to_the_reserve_before_its_deadline() {
        let trigger = Instant::now();
        let times = StopTimes::new(trigger, Duration::from_secs(25));
        assert_eq!(times.deadline, trigger + Duration::from_secs(25));
        assert_eq!(times.reserve_at, trigger + Duration::from_secs(20));
        let short = StopTimes::new(trigger, Duration::from_secs(3));
        assert_eq!(short.reserve_at, trigger);
        assert_eq!(short.deadline, trigger + Duration::from_secs(3));
    }

    /// Pins that a budget is cut to what is left before the deadline, and is
    /// zero past it.
    #[test]
    fn a_stop_budget_ends_by_the_deadline() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(4);
        let fresh = Duration::from_secs(10);
        assert_eq!(stop_budget(now, deadline, fresh), Duration::from_secs(4));
        assert_eq!(stop_budget(now, now + fresh * 2, fresh), fresh);
        assert_eq!(
            stop_budget(deadline + fresh, deadline, fresh),
            Duration::ZERO
        );
    }

    /// Pins that the lanes end the close's share before the deadline, and
    /// never before now.
    #[test]
    fn the_lanes_leave_the_close_its_share() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);
        assert_eq!(
            lanes_deadline(now, deadline, CLOSE_SHARE),
            deadline - CLOSE_SHARE
        );
        assert_eq!(lanes_deadline(now, deadline, Duration::ZERO), deadline);
        let late = deadline - Duration::from_millis(500);
        assert_eq!(lanes_deadline(late, deadline, CLOSE_SHARE), late);
    }
}
