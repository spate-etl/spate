//! Source abstraction: a control plane ([`Source`]) and a data plane
//! ([`SourceLane`]).
//!
//! A source is poll-based — no `futures::Stream`. The control plane surfaces
//! lane assignment and revocation as events and owns commits and
//! pause/resume; each lane is a pollable unit pinned to one pipeline thread
//! (for Kafka: a partition queue), yielding payloads that **borrow** the
//! source's buffers for the duration of one `push_batch` call. See
//! `docs/DESIGN.md` (§ Source abstraction, § Frozen v1 contracts).

mod barrier;

pub use barrier::DrainBarrier;

use crate::checkpoint::AckIssuer;
use crate::checkpoint::AckRef;
use crate::error::SourceError;
use crate::record::{PartitionId, RawPayload};
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

    /// The acknowledgement handle covering every payload in this batch.
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
    /// Nothing happened within the timeout.
    Idle,
}

/// Everything a source receives at [`Source::open`].
#[derive(Debug)]
#[non_exhaustive]
pub struct SourceCtx {
    /// Issuer for batch acknowledgement handles. Sources clone it into
    /// every lane they construct; each lane issues one [`AckRef`] per poll
    /// batch (`issue(partition, last_offset)`).
    pub issuer: AckIssuer,
}

impl SourceCtx {
    /// Context wrapping the checkpointer's issuer.
    #[must_use]
    pub fn new(issuer: AckIssuer) -> Self {
        SourceCtx { issuer }
    }
}

/// Control plane of a source. Driven by the runtime's controller from a
/// single thread; lanes run on pipeline threads.
pub trait Source: Send {
    /// The lane type this source produces.
    type Lane: SourceLane;

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
