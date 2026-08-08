//! Source abstraction: a control plane ([`Source`]) and a data plane
//! ([`SourceLane`]).
//!
//! A source is poll-based — no `futures::Stream`. The control plane surfaces
//! lane assignment and revocation as events and owns commits and
//! pause/resume; each lane is a pollable unit pinned to one pipeline thread
//! (for Kafka: a partition queue), yielding payloads that **borrow** the
//! source's buffers for the duration of one `push_batch` call (ADR-0003).
//! The obligations an implementor takes on are spelled out in
//! [the connector contracts][contracts].
//!
//! [contracts]: https://spate.kainth.dev/docs/user-guide/extending/contracts

mod barrier;

pub use barrier::DrainBarrier;

use crate::checkpoint::AckIssuer;
use crate::checkpoint::AckRef;
use crate::error::SourceError;
use crate::framing::FramingContract;
use crate::metrics::{Meter, SourceMetrics};
use crate::record::{PartitionId, RawPayload};
use std::sync::Arc;
use std::time::Duration;

/// Identifier of one source lane within an assignment (dense,
/// source-assigned; stable until the lane is revoked).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LaneId(pub u32);

/// One poll's worth of borrowed payloads. Streaming — payloads are handed
/// out one at a time, and every payload shares the batch lifetime `'buf`.
/// The batch carries exactly one [`AckRef`], issued by the lane through its
/// [`AckIssuer`]; records derived from these payloads clone it.
pub trait PayloadBatch<'buf> {
    /// The next payload, or `None` when the batch is exhausted.
    fn next_payload(&mut self) -> Option<RawPayload<'buf>>;

    /// The acknowledgment handle covering every payload in this batch.
    fn ack(&self) -> &AckRef;
}

/// Data-plane pollable unit of a source, owned by one pipeline thread.
///
/// Contract: payloads yielded by [`SourceLane::poll`] are valid only until
/// the returned batch is dropped, which happens before the next `poll` call
/// on the same lane — records must be consumed or encoded within that
/// window (the operator chain guarantees this by construction).
pub trait SourceLane: Send {
    /// The borrowed batch type (a GAT so payloads can borrow lane buffers).
    type Batch<'a>: PayloadBatch<'a>
    where
        Self: 'a;

    /// This lane's identity within the current assignment.
    fn id(&self) -> LaneId;

    /// The source partition this lane reads. Used for checkpoint issuing
    /// and shard routing fallback.
    fn partition(&self) -> PartitionId;

    /// Poll up to `max_records` payloads, waiting at most `timeout`.
    /// `Ok(None)` means nothing arrived — the driver treats it as idle.
    /// Implementations must not busy-spin when idle: block up to `timeout`.
    fn poll(
        &mut self,
        max_records: usize,
        timeout: Duration,
    ) -> Result<Option<Self::Batch<'_>>, SourceError>;
}

/// Control-plane event returned by [`Source::poll_events`].
#[derive(Debug)]
#[non_exhaustive]
pub enum SourceEvent<L> {
    /// New lanes were assigned; the runtime distributes them across
    /// pipeline threads. The source bumps its assignment epoch first.
    LanesAssigned(Vec<L>),
    /// Additional lanes join the *current* assignment epoch; existing
    /// lanes are untouched and their in-flight batches keep resolving.
    /// Coordinated sources emit this for incremental split gains so a
    /// routine gain never drains flowing lanes (contrast
    /// [`SourceEvent::LanesAssigned`], whose eager-rebalance contract
    /// replaces the full lane set). Lane ids must be new — never reuse an
    /// id from this source's lifetime — and each lane's partition must be
    /// fresh, not one revoked earlier in the epoch.
    LanesAdded(Vec<L>),
    /// Lanes are being revoked. The runtime trips the [`DrainBarrier`] for
    /// the owning threads, which stop the lanes, flush in-flight records,
    /// and arrive; the source completes the revocation (final synchronous
    /// commit included) only after [`DrainBarrier::wait`] returns.
    LanesRevoked {
        /// Which lanes to stop.
        lanes: Vec<LaneId>,
        /// Barrier the owning pipeline threads arrive at once drained.
        barrier: DrainBarrier,
    },
    /// Lanes whose work is finished are leaving the assignment: their
    /// input is fully delivered, acknowledged, *and committed* (e.g. a
    /// coordinated split whose terminal progress reached the store), so
    /// by contract nothing unflushed or uncommitted can exist behind
    /// them. The runtime removes them without a drain barrier — pure
    /// bookkeeping, no pipeline stall. Sources must use
    /// [`SourceEvent::LanesRevoked`] instead whenever any in-flight data
    /// or uncommitted acknowledgment may remain.
    LanesRetired {
        /// The finished lanes.
        lanes: Vec<LaneId>,
    },
    /// Nothing happened within the timeout.
    Idle,
    /// A hint that a commit outside the periodic tick is worthwhile *now*
    /// for the named partitions: their lanes have decided end-of-input,
    /// and the source cannot finalize their unit of work (e.g. complete a
    /// coordinated split, freeing its working-set slot) until the acked
    /// watermark reaches it through `Source::commit`. The runtime responds
    /// by briefly tightening its commit cadence *for those partitions
    /// only* — flowing partitions keep the periodic tick — until their
    /// acks quiesce or one commit interval elapses. Purely a latency
    /// optimization: correctness never depends on it, and sources that
    /// never emit it get the periodic cadence.
    CommitReady {
        /// Partitions whose final acks are worth chasing.
        partitions: Vec<PartitionId>,
    },
    /// The source has permanently exhausted its input: every lane has
    /// yielded its final batch and will only ever return `Ok(None)` again.
    /// Bounded sources (backfills) emit this to request a graceful drain;
    /// the runtime flushes chains, drains sinks, runs a final synchronous
    /// commit, and exits with [`ExitState::Completed`](crate::pipeline::ExitState).
    ///
    /// Contract: a source must not report `Drained` while any lane still
    /// holds unemitted data — a lane's exhaustion may only be decided by a
    /// `poll` that returned `Ok(None)` after its final batch was consumed
    /// (the poll→push→poll sequencing on the owning thread then guarantees
    /// the final batch was fully pushed downstream). Emitting `Drained` is
    /// idempotent: sources should keep returning it once drained.
    /// Unbounded sources never emit it.
    Drained,
}

/// Everything a source receives at [`Source::open`].
#[derive(Debug)]
#[non_exhaustive]
pub struct SourceCtx {
    /// Issuer for batch acknowledgment handles. Sources clone it into
    /// every lane they construct; each lane issues one [`AckRef`] per poll
    /// batch (`issue(partition, last_offset)`).
    pub issuer: AckIssuer,
    /// A [`Meter`] scoped `spate_<component_type>_source_*` for the source's own
    /// metric families (e.g. consumer lag, broker statistics), pre-labeled
    /// with the standard `pipeline`/`component`/`component_type`. `None` unless
    /// the source declared a [`Source::component_type`] that is a usable,
    /// non-reserved namespace — a reserved default (`"source"`) opts out
    /// silently, a malformed value is logged and also yields `None`. Resolve
    /// handles from it once here in `open`; never on the poll path.
    pub meter: Option<Meter>,
    /// The framework's own source-stage handles (`spate_source_*`), shared with
    /// the controller. A source that can observe its own consumer lag
    /// publishes it here — [`SourceMetrics::set_partition_lag`] and
    /// [`SourceMetrics::retain_partitions`] have no other caller, because the
    /// framework cannot compute lag without the client's view of the log end,
    /// nor tell which partitions the client still owns. `None` only when the
    /// source is driven outside a pipeline (tests, or a direct `open` call),
    /// in which case lag simply goes unpublished.
    ///
    /// Everything else on these handles — records, bytes, poll duration,
    /// rebalances, active lanes — is recorded by the runtime itself; a source
    /// must not touch those.
    pub stage_metrics: Option<Arc<SourceMetrics>>,
    /// Whether cardinality-sensitive per-partition series are enabled
    /// (`metrics.per_partition_detail`). Gates a connector's own per-partition
    /// families: when `false`, register and emit only aggregate
    /// (per-component or per-broker) series. It does **not** gate
    /// `spate_source_lag_records` — consumer lag has no aggregate series to fall
    /// back to, so it always publishes per partition.
    pub per_partition_detail: bool,
}

impl SourceCtx {
    /// Context wrapping the checkpointer's issuer. The custom-metrics
    /// [`meter`](Self::meter) is `None`; the runtime attaches one via
    /// [`with_meter`](Self::with_meter).
    #[must_use]
    pub fn new(issuer: AckIssuer) -> Self {
        SourceCtx {
            issuer,
            meter: None,
            stage_metrics: None,
            per_partition_detail: false,
        }
    }

    /// Attach the source's custom-metrics scope. Called by the runtime, which
    /// builds it from the source's `component_type`.
    #[must_use]
    pub fn with_meter(mut self, meter: Option<Meter>) -> Self {
        self.meter = meter;
        self
    }

    /// Share the framework's source-stage handles so the source can publish
    /// the one series only it can measure: consumer lag. Called by the
    /// runtime with the same instance the controller records against.
    #[must_use]
    pub fn with_stage_metrics(mut self, metrics: Option<Arc<SourceMetrics>>) -> Self {
        self.stage_metrics = metrics;
        self
    }

    /// Enable cardinality-sensitive per-partition series. Called by the
    /// runtime from `metrics.per_partition_detail`.
    #[must_use]
    pub fn with_partition_detail(mut self, enabled: bool) -> Self {
        self.per_partition_detail = enabled;
        self
    }
}

/// Control plane of a source. Driven by the runtime's controller from a
/// single thread; lanes run on pipeline threads.
pub trait Source: Send {
    /// The lane type this source produces.
    type Lane: SourceLane;

    /// The `component_type` metric label for this source (e.g. `"kafka"`),
    /// mirroring [`SinkParts::with_component_type`](crate::sink::SinkParts::with_component_type)
    /// on the sink side. It is also the namespace of the source's custom-metrics
    /// [`Meter`](SourceCtx::meter): declaring `"kafka"` scopes the source's own
    /// families under `spate_kafka_source_*`. The default `"source"` is a reserved
    /// root, so a source that does not override this gets no custom `Meter`
    /// (its framework stage metrics are unaffected).
    fn component_type(&self) -> &str {
        "source"
    }

    /// How the payloads this source emits are framed, so the framework can
    /// pair it with a deserializer without the two being coordinated by hand
    /// (see [`FramingContract`]). A source that splits its own bytes into one
    /// record per payload returns [`FramingContract::PerRecord`]; the default
    /// is [`FramingContract::WholePayload`] — the source emits whole payloads
    /// and the deserializer owns framing (Kafka, and any source that does not
    /// frame).
    fn framing_contract(&self) -> FramingContract {
        FramingContract::WholePayload
    }

    /// Connect and prepare. Called once before any other method.
    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError>;

    /// Service control-plane work (rebalance callbacks, statistics) and
    /// return the next event, waiting at most `timeout`. Must be called
    /// regularly regardless of backpressure state.
    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<Self::Lane>, SourceError>;

    /// Store per-partition committable positions (each is the offset one
    /// past the last acknowledged record). Positions are durable per the
    /// source's own policy (e.g. interval auto-commit of stored offsets).
    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError>;

    /// Synchronously flush stored positions (shutdown, revocation).
    fn flush_commits(&mut self) -> Result<(), SourceError> {
        Ok(())
    }

    /// Stop fetching for `lanes` (backpressure). Optional capability:
    /// sources that cannot pause rely on bounded-queue pushback alone.
    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        let _ = lanes;
        Ok(())
    }

    /// Resume fetching for `lanes`.
    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        let _ = lanes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SourceCtx;
    use crate::checkpoint::Checkpointer;

    #[test]
    fn source_ctx_partition_detail_defaults_off_and_round_trips() {
        let cp = Checkpointer::new();
        let ctx = SourceCtx::new(cp.handle());
        assert!(!ctx.per_partition_detail);
        assert!(ctx.meter.is_none());
        let ctx = SourceCtx::new(cp.handle()).with_partition_detail(true);
        assert!(ctx.per_partition_detail);
    }
}
