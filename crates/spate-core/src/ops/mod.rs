//! Operator chain: statically composed push stages behind one type-erasure
//! boundary per batch.
//!
//! Stages compose via [`Collector`] (monomorphized, so a whole chain compiles
//! to one loop); the only virtual call on the data path is
//! [`RunnableChain::push_batch`], once per poll batch. Records are born
//! (deserialized) and die (encoded into shard frames, filtered, or skipped)
//! inside a single `push_batch` call, so borrowed payloads never cross or
//! outlive the boundary (ADR-0013).
//!
//! # Owned vs borrowed record families
//!
//! For owned families ([`Owned<T>`](crate::deser::Owned)) the builder
//! offers [`ChainBuilder::map`] / [`ChainBuilder::try_map`] with plain
//! closure bounds; bare closures infer. For **borrowing** families a
//! `rustc` limitation (E0582: a higher-ranked lifetime may not appear only
//! in associated-type positions) rules out `FnMut`-with-projection-output
//! bounds at the definition site; use [`ChainBuilder::map_rec`] /
//! [`ChainBuilder::try_map_rec`], whose bound goes through [`MapFn`] /
//! [`TryMapFn`]. Pass a **`fn` item** where you can: it satisfies a
//! higher-ranked bound by construction, where a closure satisfies it only
//! when the compiler infers a higher-ranked signature for it.
//!
//! A stage over a borrowing family, written as a `fn` item:
//!
//! ```
//! # use spate_core::checkpoint::AckRef;
//! # use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
//! # use spate_core::error::DeserError;
//! # use spate_core::ops::chain;
//! # use spate_core::record::RawPayload;
//! # struct LogEvent<'buf> {
//! #     key: &'buf str,
//! # }
//! # struct LogF;
//! # impl RecFamily for LogF {
//! #     type Rec<'buf> = LogEvent<'buf>;
//! # }
//! # #[derive(Clone, Default)]
//! # struct LogDeser;
//! # impl Deserializer<LogF> for LogDeser {
//! #     fn deserialize<'buf>(
//! #         &mut self,
//! #         raw: &RawPayload<'buf>,
//! #         ack: &AckRef,
//! #         out: &mut dyn EmitRecord<'buf, LogEvent<'buf>>,
//! #     ) -> Result<(), DeserError> {
//! #         let _ = (raw, ack, out);
//! #         Ok(())
//! #     }
//! # }
//! # let log_deser = LogDeser;
//! struct Compact<'buf> {
//!     key: &'buf str,
//! }
//! struct CompactF;
//! impl RecFamily for CompactF {
//!     type Rec<'buf> = Compact<'buf>;
//! }
//!
//! fn shrink<'a>(e: LogEvent<'a>) -> Compact<'a> {
//!     Compact { key: e.key }
//! }
//! let stage = chain(log_deser).map_rec::<CompactF, _>(shrink);
//! # let _ = stage;
//! ```
//!
//! [`ChainBuilder::filter`], [`ChainBuilder::inspect`], and
//! [`ChainBuilder::flat_map`] have no output binding, so a single generic
//! method serves both kinds of family.
//!
//! ```
//! use spate_core::backpressure::InflightBudget;
//! use spate_core::deser::{BytesPassthrough, Owned};
//! use spate_core::error::ErrorPolicy;
//! use spate_core::ops::{ChunkConfig, chain};
//! use spate_core::record::Record;
//! use spate_core::sink::{KeyHashRouter, RowEncoder, shard_queues};
//! use std::sync::Arc;
//!
//! // A trivial encoder writing `<u32 len><bytes>` rows.
//! #[derive(Clone)]
//! struct LenPrefix;
//! impl RowEncoder<Owned<Vec<u8>>> for LenPrefix {
//!     fn encode<'buf>(
//!         &mut self,
//!         rec: &Record<Vec<u8>>,
//!         buf: &mut bytes::BytesMut,
//!     ) -> Result<(), spate_core::error::SinkError> {
//!         buf.extend_from_slice(&(rec.payload.len() as u32).to_le_bytes());
//!         buf.extend_from_slice(&rec.payload);
//!         Ok(())
//!     }
//! }
//!
//! let (queues, _rx) = shard_queues(2, 64);
//! let budget = Arc::new(InflightBudget::new());
//!
//! let mut pipeline_chain = chain(BytesPassthrough)
//!     .map(|mut bytes: Vec<u8>| {
//!         bytes.make_ascii_uppercase();
//!         bytes
//!     })
//!     .filter(|bytes: &Vec<u8>| !bytes.is_empty())
//!     .try_map(
//!         |bytes: Vec<u8>| String::from_utf8(bytes).map(String::into_bytes),
//!         ErrorPolicy::Skip,
//!     )
//!     .sink(LenPrefix, KeyHashRouter, ChunkConfig::default(), queues, budget)
//!     .build();
//! # let _ = &mut pipeline_chain;
//! ```

mod builder;
mod chain;
mod handoff;
mod split;
#[cfg(test)]
mod tests;

pub use builder::{
    Assemble, ChainBuilder, ChainFactory, FilterPart, FlatMapPart, InspectPart, MapFn, MapPart,
    Root, RoutedSplit, SinkedChain, SplitBuilder, TryMapFn, TryMapPart, chain, chain_owned,
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
/// backpressure controller. A not-ready wait is an upstream dependency
/// (e.g. a schema fetch), not sink pressure, and pausing the source for it
/// would misreport the pipeline's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlockReason {
    /// The terminal stage could not accept more output (a shard queue is
    /// full). This is sink backpressure.
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
    /// resume; operators never re-run for them.
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

/// The one erasure boundary between a pipeline thread's driver loop and a
/// typed chain. The methods are generic over the buffer lifetime
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
    /// batch blocked mid-push). Terminal parked chunks (which carry their
    /// own acks) are unaffected; only the chain's own mid-batch cursor and
    /// any stashed not-ready payload are cleared, so the next `push_batch` of
    /// a fresh batch starts clean instead of tripping the resume-cursor
    /// asserts or replaying the stale payload under the new batch's ack.
    ///
    /// The default is a no-op for chains that keep no cross-call batch state.
    fn abandon_batch(&mut self) {}
}

/// Push-model stage: receives one record, forwards 0..N downstream.
///
/// Composed statically; `Map<F, Filter<P, Term>>` monomorphizes into a
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
