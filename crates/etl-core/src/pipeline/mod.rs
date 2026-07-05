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

mod controller;
mod driver;
mod runtime;

pub use runtime::{PipelineRuntime, RuntimeOptions, ShutdownHandle, StartError};

use crate::error::FatalError;
use crate::record::PartitionId;
use crate::sink::ShardQueues;
use crate::source::{DrainBarrier, LaneId};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

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

/// What the sink reported when draining at shutdown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DrainReport {
    /// Batches flushed durably during the drain.
    pub flushed_batches: u64,
    /// Batches abandoned at the deadline (their offsets stay uncommitted
    /// and replay after restart; their acknowledgements must have been
    /// failed by the sink).
    pub abandoned_batches: u64,
}

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

/// Boxed sink drain hook: budget in, report out.
pub type SinkDrainFn =
    Box<dyn FnOnce(Duration) -> Pin<Box<dyn Future<Output = DrainReport> + Send>> + Send>;

/// Boxed, repeatable sink connectivity probe (readiness).
pub type SinkProbeFn = Box<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), crate::error::SinkError>> + Send>>
        + Send
        + Sync,
>;

/// Terminal state of a pipeline run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExitState {
    /// Drained and committed cleanly (SIGTERM or programmatic shutdown).
    Completed,
    /// A fatal error stopped the pipeline; the process should exit
    /// non-zero.
    Failed(FatalErrorReport),
}

/// Owned copy of the fatal error carried in the exit report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FatalErrorReport {
    /// Component that failed.
    pub component: String,
    /// Human-readable cause.
    pub reason: String,
}

/// Outcome of [`PipelineRuntime::run`].
#[derive(Debug)]
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

#[cfg(all(test, not(loom)))]
mod tests;
