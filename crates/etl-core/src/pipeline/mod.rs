//! Pipeline runtime: pinned driver threads, the source controller, and
//! process assembly.
//!
//! Thread anatomy (see `docs/DESIGN.md` § Process anatomy):
//!
//! ```text
//! main (run())          controller (std thread)      driver 0..N (std threads)
//!   metrics/admin/io  ──  owns Source + Checkpointer ── own lanes + chain
//!   joins everything      poll_events / commit tick     poll → push_batch → route
//!                         pause/resume application      backpressure ticks
//! ```
//!
//! Communication: the controller sends [`ThreadControl`] messages to
//! drivers (lane assignment, drain barriers); drivers send [`DriverEvent`]
//! requests back (pause/resume — only the controller touches the
//! [`Source`](crate::source::Source) — and fatal reports). All channels are
//! unbounded crossbeam channels: control traffic is rare and must never
//! block a poll loop.
//!
//! Shutdown (also the full-revocation path, per DESIGN.md § Shutdown):
//! SIGTERM → controller stops event polling and sends `Shutdown` to every
//! driver → each driver flushes its chain, drops its lanes, and arrives at
//! the barrier → main joins driver threads (dropping chains closes the
//! shard queues) → the sink drains under the remaining deadline → the
//! controller runs a final drain + commit + `flush_commits` → the process
//! reports an [`ExitReport`]. A sink that cannot flush by the deadline is
//! abandoned loudly; unacknowledged offsets are never committed, so the
//! data replays after restart (at-least-once).

mod builder;
mod controller;
mod driver;
mod runtime;

pub use builder::{BuildError, ChainCtx, Pipeline, PipelineError, SinkOptions};
pub use runtime::{PipelineRuntime, RuntimeOptions, ShutdownHandle, StartError, metrics_settings};

use crate::error::FatalError;
use crate::record::PartitionId;
use crate::sink::ShardQueues;
use crate::source::{DrainBarrier, LaneId};
use std::time::Instant;

/// Control messages the controller sends to a driver thread.
pub(crate) enum ThreadControl<L> {
    /// Take ownership of a newly assigned lane.
    AddLane(L),
    /// Stop and drop the listed lanes (revocation): flush the chain, then
    /// arrive at `barrier` once per stopped lane before `deadline`.
    StopLanes {
        lanes: Vec<LaneId>,
        barrier: DrainBarrier,
        deadline: Instant,
    },
    /// Stop everything (shutdown): flush the chain, drop all lanes, arrive
    /// once at `barrier`, and exit the thread.
    Shutdown {
        barrier: DrainBarrier,
        deadline: Instant,
    },
}

/// Requests and reports a driver thread sends the controller.
#[derive(Debug)]
pub(crate) enum DriverEvent {
    /// Backpressure tripped: pause these lanes at the source.
    PauseLanes { lanes: Vec<LaneId> },
    /// Backpressure cleared: resume these lanes.
    ResumeLanes { lanes: Vec<LaneId> },
    /// The chain failed or panicked; the pipeline must stop.
    Fatal { thread: usize, error: FatalError },
}

/// What the sink reported when draining at shutdown — the sink layer's
/// [`DrainReport`](crate::sink::DrainReport), re-exported so assemblies
/// hand `SinkPool::drain`'s result straight through.
pub use crate::sink::DrainReport;

/// The sink half the runtime drives: the shared shard-queue handle plus a
/// drain hook invoked once at shutdown with the remaining drain budget.
///
/// Built by the sink layer (`SinkPool`) or by tests; the runtime is
/// deliberately ignorant of worker internals.
pub struct SinkRuntime {
    /// Sending side of the per-shard chunk queues (the runtime only uses
    /// capacity introspection; the chain's terminal stage holds clones).
    pub queues: ShardQueues,
    /// Drain the sink: flush what's pending within the budget, fail the
    /// acknowledgements of anything abandoned, and report.
    pub drain: SinkDrainFn,
    /// Optional connectivity probe (e.g. `SinkPool::probe_all`). The
    /// runtime probes at startup and then periodically, driving the
    /// sinks-connected half of `/readyz`. Without a probe the flag is set
    /// unconditionally.
    pub probe: Option<SinkProbeFn>,
}

impl std::fmt::Debug for SinkRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkRuntime")
            .field("queues", &self.queues)
            .finish_non_exhaustive()
    }
}

/// Boxed sink drain hook — defined next to the sink layer that produces
/// it, re-exported here where the runtime consumes it.
pub use crate::sink::{SinkDrainFn, SinkProbeFn};

/// Terminal state of a pipeline run.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExitState {
    /// Drained and committed cleanly (SIGTERM or programmatic shutdown).
    Completed,
    /// A fatal error stopped the pipeline; the process should exit
    /// non-zero.
    Failed(FatalErrorReport),
}

/// Owned copy of the fatal error carried in the exit report.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct FatalErrorReport {
    /// Component that failed.
    pub component: String,
    /// Human-readable cause.
    pub reason: String,
}

impl std::fmt::Display for FatalErrorReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pipeline failed in {}: {}", self.component, self.reason)
    }
}

impl std::error::Error for FatalErrorReport {}

/// Outcome of [`PipelineRuntime::run`].
#[derive(Debug)]
#[non_exhaustive]
pub struct ExitReport {
    /// How the run ended.
    pub state: ExitState,
    /// The sink's drain report (absent when the sink drain hook could not
    /// run, e.g. the I/O runtime was already gone).
    pub sink_drain: Option<DrainReport>,
    /// The last committed watermark per partition, as reported by the
    /// final commit.
    pub final_watermarks: Vec<(PartitionId, i64)>,
}

impl ExitReport {
    /// Log the outcome — state, sink drain, final watermarks — at the level
    /// matching it (`info` for a clean exit, `error` for a failure).
    pub fn log(&self) {
        match &self.state {
            ExitState::Completed => tracing::info!(
                state = ?self.state,
                drain = ?self.sink_drain,
                watermarks = ?self.final_watermarks,
                "pipeline finished"
            ),
            ExitState::Failed(failure) => tracing::error!(
                component = %failure.component,
                reason = %failure.reason,
                drain = ?self.sink_drain,
                watermarks = ?self.final_watermarks,
                "pipeline failed"
            ),
        }
    }

    /// The process exit code this outcome maps to: `0` for a clean exit,
    /// `1` for a failure.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self.state {
            ExitState::Completed => 0,
            ExitState::Failed(_) => 1,
        }
    }

    /// The report as a `Result`, so a `main` can `?` a failed run: a clean
    /// exit passes the report through, a failure returns the
    /// [`FatalErrorReport`] as the error.
    pub fn ok(self) -> Result<ExitReport, FatalErrorReport> {
        match &self.state {
            ExitState::Completed => Ok(self),
            ExitState::Failed(failure) => Err(failure.clone()),
        }
    }
}

#[cfg(all(test, not(loom)))]
pub(crate) mod fakes;
#[cfg(all(test, not(loom)))]
mod tests;
