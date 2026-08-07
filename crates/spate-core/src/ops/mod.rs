//! Operator chain: statically composed push stages behind one type-erasure
//! boundary per batch.
//!
//! Stages compose via [`Collector`] (monomorphized — a whole chain compiles
//! to one loop); the only virtual call on the data path is
//! [`RunnableChain::push_batch`], once per poll batch. Records are born
//! (deserialized) and die (encoded into shard frames, filtered, or skipped)
//! inside a single `push_batch` call, so borrowed payloads never cross or
//! outlive the boundary (ADR-0013).

mod builder;
mod chain;
mod handoff;
mod split;
#[cfg(test)]
mod tests;

pub use builder::{
    Assemble, ChainBuilder, ChainFactory, FilterPart, FlatMapPart, InspectPart, MapPart, Root,
    RoutedSplit, SinkedChain, SplitBuilder, TryMapPart, chain, chain_owned,
};
pub use chain::{Emitter, Filter, FlatMap, Inspect, Map, StageLifecycle, TryMap, TypedChain};
pub use handoff::{ChunkConfig, SinkHandoff};
pub use split::{Sink, SinkCtx, SplitEmitter, SplitTerminal};

use crate::deser::RecFamily;
use crate::error::FatalError;
use crate::record::{Flow, Record};
use crate::source::PayloadBatch;

/// Why a batch could not complete yet. Both cases are retried with the
/// resume cursor, but only [`BlockReason::Capacity`] engages the driver's
/// backpressure controller — a not-ready wait is an upstream dependency
/// (e.g. a schema fetch), not sink pressure, and pausing the source for it
/// would misreport the pipeline's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlockReason {
    /// The terminal stage could not accept more output (a shard queue is
    /// full): genuine backpressure.
    Capacity,
    /// A deserializer reported
    /// [`DeserError::NotReady`](crate::error::DeserError::NotReady): the
    /// payload replays once its dependency arrives. Counted on
    /// `spate_deser_not_ready_total`.
    NotReady,
}

/// Result of pushing one batch (or a resumed suffix of one) through a
/// chain.
#[derive(Debug)]
#[non_exhaustive]
pub enum PushOutcome {
    /// Every payload was fully processed (records may have been filtered
    /// or skipped by policy along the way).
    Done,
    /// The batch could not complete yet. Payloads with index `< resume_at`
    /// are fully processed; the driver later re-pushes the same batch with
    /// `from = resume_at`. Any partially-emitted payload's already-emitted
    /// records are parked inside the terminal stage and drain first on
    /// resume — operators never re-run for them.
    Blocked {
        /// Index of the first payload not yet fully processed.
        resume_at: usize,
        /// What the batch is waiting for.
        reason: BlockReason,
    },
    /// A `Fail`-policy stage tripped or an invariant broke. The batch's
    /// [`AckRef`](crate::checkpoint::AckRef) must be failed by the driver;
    /// the pipeline stops.
    Fatal(FatalError),
}

/// THE SEAM — the one erasure boundary between a pipeline thread's driver
/// loop and a typed chain. The methods are generic over the buffer lifetime
/// only, so `Box<dyn RunnableChain>` is legal.
pub trait RunnableChain: Send {
    /// Push payloads `from..` of `batch` through the chain.
    fn push_batch<'buf>(&mut self, batch: &mut dyn PayloadBatch<'buf>, from: usize) -> PushOutcome;

    /// Flush terminal-stage state (parked records, partial encoder
    /// buffers) downstream. Called by the driver on drain, on linger
    /// deadlines, and before commit ticks.
    fn flush(&mut self) -> PushOutcome;

    /// Discard any per-batch replay/resume state after the driver failed the
    /// current batch's acknowledgment (a shutdown-time abandonment of a
    /// batch blocked mid-push). Terminal parked chunks — which carry their
    /// own acks — are unaffected; only the chain's own mid-batch cursor and
    /// any stashed not-ready payload are cleared, so the next `push_batch` of
    /// a fresh batch starts clean instead of tripping the resume-cursor
    /// asserts or replaying the stale payload under the new batch's ack.
    ///
    /// The default is a no-op for chains that keep no cross-call batch state.
    fn abandon_batch(&mut self) {}
}

/// Push-model stage: receives one record, forwards 0..N downstream.
///
/// Composed statically — `Map<F, Filter<P, Term>>` monomorphizes into a
/// single inlined loop body.
pub trait Collector<T> {
    /// Push one record. [`Flow::Blocked`] propagates up to the boundary.
    fn push(&mut self, rec: Record<T>) -> Flow;
}

/// Family-erased collector: accepts the family's record type at *any*
/// buffer lifetime through a lifetime-generic method, which keeps it
/// dyn-compatible. This is what lets `flat_map` closures hold a plain
/// `&mut Emitter<'_, OutF>` without naming the downstream stack type.
pub trait CollectorFor<F: RecFamily> {
    /// Push one record of the family at any lifetime.
    fn push_rec<'buf>(&mut self, rec: Record<F::Rec<'buf>>) -> Flow;
}

impl<F, C> CollectorFor<F> for C
where
    F: RecFamily,
    C: for<'buf> Collector<<F as RecFamily>::Rec<'buf>>,
{
    #[inline(always)]
    fn push_rec<'buf>(&mut self, rec: Record<F::Rec<'buf>>) -> Flow {
        self.push(rec)
    }
}
