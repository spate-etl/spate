//! The split terminal: route each record to exactly one of N typed sink
//! branches, each with its own schema, encoder, router, and shard queues.
//!
//! Where [`SinkHandoff`](super::handoff::SinkHandoff) is one table, the split
//! terminal fans a heterogeneously-typed stream out across many. The user
//! writes a single `match` (classify + extract in the same arm) and dispatches
//! with [`SplitEmitter::emit`]; a record that reaches no branch follows the
//! configured [`ErrorPolicy`] (`Fail` — the default — stops the pipeline;
//! `Skip` drops it and counts `etl_operator_records_dropped_total{reason="unrouted"}`).
//!
//! # How the typed dispatch stays cheap and object-safe
//!
//! Each branch is a [`SinkHandoff<F, BoxedEncoder<F>, BoxedRouter<F>>`](super::handoff::SinkHandoff)
//! — its encoder and router are erased so the branch's concrete type depends
//! only on the destination family `F`. A [`Sink<F>`] handle (a plain index plus
//! `F`) therefore names the exact concrete type, so [`SplitEmitter::emit`]
//! recovers it with one `Any` downcast, then routes and encodes through the
//! branch's boxed router/encoder — one virtual call each per record over the
//! single-sink path's concrete types — straight into that branch's per-shard
//! buffer.
//! The at-least-once machinery is inherited unchanged: each branch clones the
//! poll batch's [`AckRef`](crate::checkpoint::AckRef) into its own fail-on-drop
//! `AckSet`, so the source watermark holds until *every* branch that received a
//! derived record has durably written, and any branch's failure stalls it.

use super::Collector;
use super::chain::{FatalSlot, OpMeterSlot, StageLifecycle};
use super::handoff::{ChunkConfig, SinkHandoff};
use crate::backpressure::InflightBudget;
use crate::checkpoint::AckRef;
use crate::deser::RecFamily;
use crate::error::{ErrorPolicy, FatalError, SinkError};
use crate::record::{Flow, Record, RecordMeta};
use crate::sink::{RecordRouter, RowEncoder, ShardQueues};
use bytes::BytesMut;
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

/// The per-sink handles a split branch needs, resolved by name from the
/// chain factory's [`ChainCtx`](crate::pipeline::ChainCtx) via
/// [`ChainCtx::sink`](crate::pipeline::ChainCtx::sink). Bundling the name in
/// keeps [`SplitBuilder::add`](super::ChainBuilder) from repeating it.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SinkCtx {
    pub(crate) name: String,
    pub(crate) queues: ShardQueues,
    pub(crate) budget: Arc<InflightBudget>,
    /// This branch's resolved terminal-stage chunking (its per-sink YAML
    /// `chunk:` block, `SinkOptions::with_chunk`, or the default), applied by
    /// [`SplitBuilder::add`](super::ChainBuilder).
    pub(crate) chunk: ChunkConfig,
}

impl SinkCtx {
    /// Bundle a named sink's queues and the shared in-flight budget. Chunking
    /// starts at [`ChunkConfig::default`]; override it with
    /// [`with_chunk`](Self::with_chunk). (Builder pipelines never call this —
    /// [`ChainCtx::sink`](crate::pipeline::ChainCtx::sink) hands out a fully
    /// resolved `SinkCtx`.)
    #[must_use]
    pub fn new(name: String, queues: ShardQueues, budget: Arc<InflightBudget>) -> Self {
        SinkCtx {
            name,
            queues,
            budget,
            chunk: ChunkConfig::default(),
        }
    }

    /// Set this branch's terminal-stage chunking — the manual-assembly
    /// counterpart to the per-sink YAML `chunk:` block.
    #[must_use]
    pub fn with_chunk(mut self, chunk: ChunkConfig) -> Self {
        self.chunk = chunk;
        self
    }
}

/// A typed, `Copy` handle to one split branch — a branch index plus the
/// destination family, so [`SplitEmitter::emit`] both type-checks the row and
/// recovers the branch with zero per-call lookup. Minted by
/// [`SplitBuilder::add`](super::ChainBuilder).
pub struct Sink<F: RecFamily> {
    idx: usize,
    _f: PhantomData<fn() -> F>,
}

impl<F: RecFamily> Sink<F> {
    pub(crate) fn new(idx: usize) -> Self {
        Sink {
            idx,
            _f: PhantomData,
        }
    }
}

impl<F: RecFamily> Clone for Sink<F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F: RecFamily> Copy for Sink<F> {}

impl<F: RecFamily> std::fmt::Debug for Sink<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sink").field("idx", &self.idx).finish()
    }
}

// ── Type-erased encoder/router so a branch's concrete type keys on `F` ──────

/// A [`RowEncoder`] that can clone itself into a box, so a boxed encoder is
/// still `Clone` (the terminal stage mints one encoder per shard).
trait EncoderClone<F: RecFamily>: RowEncoder<F> {
    /// Clone into a fresh box.
    fn clone_box(&self) -> Box<dyn EncoderClone<F>>;
}

impl<F: RecFamily, T> EncoderClone<F> for T
where
    T: RowEncoder<F> + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn EncoderClone<F>> {
        Box::new(self.clone())
    }
}

/// A branch's encoder, erased to depend only on the destination family.
type BoxedEncoder<F> = Box<dyn EncoderClone<F>>;

impl<F: RecFamily> Clone for BoxedEncoder<F> {
    fn clone(&self) -> Self {
        // Dispatch through the trait object to the *concrete* encoder's
        // `clone_box`. `self.clone_box()` would re-select the blanket impl
        // (which also covers `Box<dyn EncoderClone>`) and recurse into this
        // very `clone` — infinitely. `(**self)` pins it to the vtable.
        (**self).clone_box()
    }
}

impl<F: RecFamily> RowEncoder<F> for BoxedEncoder<F> {
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        (**self).encode(rec, buf)
    }

    fn buffered_bytes(&self) -> usize {
        (**self).buffered_bytes()
    }

    fn finish_chunk(&mut self, buf: &mut BytesMut) -> Result<(), SinkError> {
        (**self).finish_chunk(buf)
    }
}

/// A branch's router, erased to depend only on the destination family.
type BoxedRouter<F> = Box<dyn RecordRouter<F>>;

impl<F: RecFamily> RecordRouter<F> for BoxedRouter<F> {
    fn route_record<'buf>(&self, rec: &Record<F::Rec<'buf>>, num_shards: usize) -> usize {
        (**self).route_record(rec, num_shards)
    }
}

/// A branch's concrete type: a [`SinkHandoff`] over the erased encoder/router,
/// determined by the destination family alone.
type Branch<F> = SinkHandoff<F, BoxedEncoder<F>, BoxedRouter<F>>;

// ── Object-safe branch storage ──────────────────────────────────────────────

/// The lifecycle a split branch exposes to the terminal, plus `Any` recovery
/// for the typed emit path. Object-safe (no destination-family type appears in
/// the signatures), so branches of different families live in one `Vec`.
pub(crate) trait ErasedBranch: Send {
    fn relieve(&mut self) -> Flow;
    fn flush_terminal(&mut self) -> Flow;
    fn take_fatal(&mut self) -> Option<FatalError>;
    fn on_batch_end(&mut self, elapsed: Duration);
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<F, E, R> ErasedBranch for SinkHandoff<F, E, R>
where
    F: RecFamily + 'static,
    E: RowEncoder<F> + Clone + 'static,
    R: RecordRouter<F> + 'static,
{
    fn relieve(&mut self) -> Flow {
        StageLifecycle::relieve(self)
    }

    fn flush_terminal(&mut self) -> Flow {
        StageLifecycle::flush_terminal(self)
    }

    fn take_fatal(&mut self) -> Option<FatalError> {
        StageLifecycle::take_fatal(self)
    }

    fn on_batch_end(&mut self, elapsed: Duration) {
        StageLifecycle::on_batch_end(self, elapsed);
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Build one erased branch from a concrete encoder/router pair.
pub(crate) fn new_branch<F, E, R>(
    encoder: E,
    router: R,
    queues: ShardQueues,
    budget: Arc<InflightBudget>,
    cfg: ChunkConfig,
    meter: OpMeterSlot,
    component: Arc<str>,
) -> Box<dyn ErasedBranch>
where
    F: RecFamily + 'static,
    E: RowEncoder<F> + Clone + Send + 'static,
    R: RecordRouter<F> + 'static,
{
    let encoder: BoxedEncoder<F> = Box::new(encoder);
    let router: BoxedRouter<F> = Box::new(router);
    let handoff: Branch<F> =
        SinkHandoff::new(encoder, router, queues, budget, cfg, meter, component);
    Box::new(handoff)
}

// ── The stack-borrowed emitter ──────────────────────────────────────────────

/// Stack-borrowed emitter handed to a [`route`](super::ChainBuilder) closure.
/// [`emit`](Self::emit) routes one derived record to the branch named by a
/// [`Sink<F>`] handle; a record that emits to no branch triggers the split's
/// `unmatched` policy. One `Any` downcast per emit, no per-call name lookup.
pub struct SplitEmitter<'a> {
    branches: &'a mut [Box<dyn ErasedBranch>],
    meta: RecordMeta,
    ack: &'a AckRef,
    emitted: u32,
    flow: Flow,
}

impl std::fmt::Debug for SplitEmitter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitEmitter")
            .field("emitted", &self.emitted)
            .field("flow", &self.flow)
            .finish_non_exhaustive()
    }
}

impl SplitEmitter<'_> {
    /// Route one derived record to `handle`'s branch, inheriting the parent's
    /// metadata and acknowledgement handle. Emitting to no branch (returning
    /// from the closure without any `emit`) invokes the `unmatched` policy.
    ///
    /// # Panics
    ///
    /// Panics if `handle` does not name a branch of this split — a handle
    /// minted by a different split's builder, or one whose record family
    /// does not match the branch at its index.
    #[inline]
    pub fn emit<'buf, F: RecFamily + 'static>(&mut self, handle: Sink<F>, row: F::Rec<'buf>) {
        let branch = self
            .branches
            .get_mut(handle.idx)
            .and_then(|b| b.as_any_mut().downcast_mut::<Branch<F>>())
            .expect(
                "split branch/handle mismatch: this Sink<F> handle does not name a \
                 branch of this split (a handle from another split, or the wrong \
                 record family)",
            );
        let flow = branch.push(Record {
            payload: row,
            meta: self.meta,
            ack: self.ack.clone(),
        });
        self.emitted += 1;
        if self.flow != Flow::Blocked {
            self.flow = flow;
        }
    }

    /// The parent record's metadata.
    #[must_use]
    pub fn meta(&self) -> RecordMeta {
        self.meta
    }
}

// ── The terminal stage ──────────────────────────────────────────────────────

/// The chain's split terminal. Runs the route closure over each record and
/// aggregates the branches' lifecycle (relieve/flush/fatal). See the
/// [module docs](self).
pub struct SplitTerminal<SrcF: RecFamily, G> {
    route: G,
    branches: Vec<Box<dyn ErasedBranch>>,
    unmatched: ErrorPolicy,
    meter: OpMeterSlot,
    fatal: FatalSlot,
    component: Arc<str>,
    _family: PhantomData<fn() -> SrcF>,
}

impl<SrcF: RecFamily, G> SplitTerminal<SrcF, G> {
    pub(crate) fn new(
        route: G,
        branches: Vec<Box<dyn ErasedBranch>>,
        unmatched: ErrorPolicy,
        meter: OpMeterSlot,
        component: Arc<str>,
    ) -> Self {
        SplitTerminal {
            route,
            branches,
            unmatched,
            meter,
            fatal: FatalSlot(None),
            component,
            _family: PhantomData,
        }
    }
}

impl<SrcF: RecFamily, G> std::fmt::Debug for SplitTerminal<SrcF, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitTerminal")
            .field("branches", &self.branches.len())
            .field("unmatched", &self.unmatched)
            .finish_non_exhaustive()
    }
}

impl<'buf, SrcF, G> Collector<<SrcF as RecFamily>::Rec<'buf>> for SplitTerminal<SrcF, G>
where
    SrcF: RecFamily,
    G: for<'b> FnMut(SrcF::Rec<'b>, &mut SplitEmitter<'_>),
{
    fn push(&mut self, rec: Record<SrcF::Rec<'buf>>) -> Flow {
        self.meter.0.seen();
        // A latched fatal short-circuits the rest of the batch, just like
        // `SinkHandoff` — the chain drains it via `take_fatal`.
        if self.fatal.0.is_some() {
            return Flow::Continue;
        }
        let Record {
            payload, meta, ack, ..
        } = rec;
        let mut em = SplitEmitter {
            branches: &mut self.branches,
            meta,
            ack: &ack,
            emitted: 0,
            flow: Flow::Continue,
        };
        (self.route)(payload, &mut em);
        let (emitted, flow) = (em.emitted, em.flow);
        if emitted == 0 {
            match self.unmatched {
                // Drop-and-count: the record's ack share releases as success
                // when `ack` drops here, exactly like a `filter` drop.
                ErrorPolicy::Skip => self.meter.0.unrouted(),
                // Stop the pipeline: the driver fails the batch's ack.
                _ => {
                    self.fatal.0 = Some(FatalError {
                        component: self.component.to_string(),
                        reason: "record matched no split branch".into(),
                    });
                }
            }
        } else {
            self.meter.0.out_n(u64::from(emitted));
        }
        flow
    }
}

impl<SrcF: RecFamily, G> StageLifecycle for SplitTerminal<SrcF, G> {
    fn on_batch_end(&mut self, elapsed: Duration) {
        self.meter.0.flush(elapsed);
        for branch in &mut self.branches {
            branch.on_batch_end(elapsed);
        }
    }

    fn take_fatal(&mut self) -> Option<FatalError> {
        if let Some(fatal) = self.fatal.0.take() {
            return Some(fatal);
        }
        for branch in &mut self.branches {
            if let Some(fatal) = branch.take_fatal() {
                return Some(fatal);
            }
        }
        None
    }

    fn relieve(&mut self) -> Flow {
        // Relieve every branch (make progress on all), block if any is backed
        // up — a branch that stays blocked keeps the chain from new payloads.
        let mut flow = Flow::Continue;
        for branch in &mut self.branches {
            if branch.relieve() == Flow::Blocked {
                flow = Flow::Blocked;
            }
        }
        flow
    }

    fn flush_terminal(&mut self) -> Flow {
        let mut flow = Flow::Continue;
        for branch in &mut self.branches {
            if branch.flush_terminal() == Flow::Blocked {
                flow = Flow::Blocked;
            }
        }
        flow
    }
}
