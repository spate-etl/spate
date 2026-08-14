//! The fluent, type-safe chain builder.
//!
//! Stages are recorded as lightweight *parts* and assembled into the
//! statically composed collector stack when the chain is built. The same
//! parts can assemble any number of identical chains (one per pipeline
//! thread) through [`ChainFactory`].
//!
//! # Owned vs borrowed record families
//!
//! For owned families ([`Owned<T>`](crate::deser::Owned)) the builder
//! offers [`ChainBuilder::map`] / [`ChainBuilder::try_map`] with plain
//! closure bounds; bare closures infer. For **borrowing** families a
//! `rustc` limitation (E0582: a higher-ranked lifetime may not appear only
//! in associated-type positions) rules out `FnMut`-with-projection-output
//! bounds at the definition site; use [`ChainBuilder::map_rec`] /
//! [`ChainBuilder::try_map_rec`] and pass **`fn` items**, which are
//! naturally higher-ranked:
//!
//! ```ignore
//! fn shrink<'a>(e: LogEvent<'a>) -> Compact<'a> { /* ... */ }
//! chain(log_deser).map_rec::<CompactF, _>(shrink)
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

use super::chain::{
    FatalSlot, Filter, FlatMap, Inspect, Map, OpMeter, OpMeterSlot, StageLifecycle, TryMap,
    TypedChain,
};
use super::handoff::{ChunkConfig, SinkHandoff};
use super::split::{ErasedBranch, Sink, SinkCtx, SplitEmitter, SplitTerminal, new_branch};
use super::{Collector, Emitter, RunnableChain};
use crate::backpressure::InflightBudget;
use crate::deser::{Deserializer, Owned, RecFamily};
use crate::error::ErrorPolicy;
use crate::metrics::{ComponentLabels, DeserMetrics, OperatorMetrics};
use crate::sink::{RecordRouter, RowEncoder, ShardQueues};
use std::marker::PhantomData;
use std::sync::Arc;

/// A record-to-record transform between families. Implemented for every
/// `FnMut(In) -> Out`; expressed as an independent two-parameter trait so
/// higher-ranked builder bounds stay legal for borrowing families (see the
/// module docs on E0582). `fn` items satisfy it at every lifetime.
pub trait MapFn<In, Out>: FnMut(In) -> Out {}
impl<G, In, Out> MapFn<In, Out> for G where G: FnMut(In) -> Out {}

/// Fallible variant of [`MapFn`].
pub trait TryMapFn<In, Out, Err>: FnMut(In) -> Result<Out, Err> {}
impl<G, In, Out, Err> TryMapFn<In, Out, Err> for G where G: FnMut(In) -> Result<Out, Err> {}

/// Assembles recorded parts into the concrete collector stack, given the
/// terminal stage. Takes `&self` so one set of parts can assemble many
/// identical chains. Stage closures must therefore be `Clone` (plain closures
/// and closures over `Clone`/`Arc` state are).
pub trait Assemble<Term> {
    /// The assembled collector stack.
    type Out;
    /// Build the stack around `term`.
    fn assemble(&self, term: Term) -> Self::Out;
}

/// The empty stage list.
#[derive(Clone, Copy, Debug, Default)]
pub struct Root;

impl<T> Assemble<T> for Root {
    type Out = T;
    fn assemble(&self, term: T) -> T {
        term
    }
}

/// Recorded `map`/`map_rec` stage.
#[derive(Clone, Debug)]
pub struct MapPart<Prev, G> {
    prev: Prev,
    f: G,
    meter: OpMeterSlot,
}

impl<Prev, G: Clone, Term> Assemble<Term> for MapPart<Prev, G>
where
    Prev: Assemble<Map<G, Term>>,
{
    type Out = Prev::Out;
    fn assemble(&self, term: Term) -> Self::Out {
        self.prev.assemble(Map {
            f: self.f.clone(),
            next: term,
            meter: self.meter.clone(),
        })
    }
}

/// Recorded `filter` stage.
#[derive(Clone, Debug)]
pub struct FilterPart<Prev, P> {
    prev: Prev,
    p: P,
    meter: OpMeterSlot,
}

impl<Prev, P: Clone, Term> Assemble<Term> for FilterPart<Prev, P>
where
    Prev: Assemble<Filter<P, Term>>,
{
    type Out = Prev::Out;
    fn assemble(&self, term: Term) -> Self::Out {
        self.prev.assemble(Filter {
            p: self.p.clone(),
            next: term,
            meter: self.meter.clone(),
        })
    }
}

/// Recorded `inspect` stage.
#[derive(Clone, Debug)]
pub struct InspectPart<Prev, G> {
    prev: Prev,
    f: G,
}

impl<Prev, G: Clone, Term> Assemble<Term> for InspectPart<Prev, G>
where
    Prev: Assemble<Inspect<G, Term>>,
{
    type Out = Prev::Out;
    fn assemble(&self, term: Term) -> Self::Out {
        self.prev.assemble(Inspect {
            f: self.f.clone(),
            next: term,
        })
    }
}

/// Recorded `try_map`/`try_map_rec` stage.
#[derive(Clone, Debug)]
pub struct TryMapPart<Prev, G> {
    prev: Prev,
    f: G,
    policy: ErrorPolicy,
    component: Arc<str>,
    meter: OpMeterSlot,
}

impl<Prev, G: Clone, Term> Assemble<Term> for TryMapPart<Prev, G>
where
    Prev: Assemble<TryMap<G, Term>>,
{
    type Out = Prev::Out;
    fn assemble(&self, term: Term) -> Self::Out {
        self.prev.assemble(TryMap {
            f: self.f.clone(),
            next: term,
            policy: self.policy,
            component: Arc::clone(&self.component),
            meter: self.meter.clone(),
            fatal: FatalSlot(None),
        })
    }
}

/// Recorded `flat_map` stage.
#[derive(Clone, Debug)]
pub struct FlatMapPart<OutF: RecFamily, Prev, G> {
    prev: Prev,
    g: G,
    meter: OpMeterSlot,
    _out: PhantomData<fn() -> OutF>,
}

impl<OutF: RecFamily, Prev, G: Clone, Term> Assemble<Term> for FlatMapPart<OutF, Prev, G>
where
    Prev: Assemble<FlatMap<OutF, G, Term>>,
{
    type Out = Prev::Out;
    fn assemble(&self, term: Term) -> Self::Out {
        self.prev.assemble(FlatMap {
            g: self.g.clone(),
            next: term,
            meter: self.meter.clone(),
            _out: PhantomData,
        })
    }
}

#[derive(Clone, Debug)]
struct MetricsSpec {
    pipeline: String,
    component: String,
    deser: Arc<DeserMetrics>,
}

impl MetricsSpec {
    fn op_handle(&self, idx: usize, kind: &'static str) -> Arc<OperatorMetrics> {
        let labels = ComponentLabels::new(
            self.pipeline.clone(),
            format!("{}.{idx}_{kind}", self.component),
            kind,
        );
        Arc::new(OperatorMetrics::new(&labels))
    }
}

fn meter_for(metrics: &Option<MetricsSpec>, idx: usize, kind: &'static str) -> OpMeterSlot {
    OpMeterSlot(OpMeter::new(
        metrics.as_ref().map(|m| m.op_handle(idx, kind)),
    ))
}

/// Fluent builder for one pipeline's operator chain. `DF` is the
/// deserializer's record family; `CurF` the family at the current end of
/// the chain (changed by `map_rec` and `flat_map`, and by `map` for owned
/// payloads).
#[derive(Clone, Debug)]
pub struct ChainBuilder<DF: RecFamily, CurF: RecFamily, D, P> {
    deser: D,
    parts: P,
    deser_policy: ErrorPolicy,
    metrics: Option<MetricsSpec>,
    stage_idx: usize,
    _fam: PhantomData<fn() -> (DF, CurF)>,
}

/// Start a chain from a deserializer producing family `F`.
pub fn chain<F: RecFamily, D: Deserializer<F>>(deser: D) -> ChainBuilder<F, F, D, Root> {
    ChainBuilder {
        deser,
        parts: Root,
        deser_policy: ErrorPolicy::Skip,
        metrics: None,
        stage_idx: 0,
        _fam: PhantomData,
    }
}

/// Start a chain from a deserializer producing owned records `T`.
pub fn chain_owned<T, D>(deser: D) -> ChainBuilder<Owned<T>, Owned<T>, D, Root>
where
    T: Send + 'static,
    D: Deserializer<Owned<T>>,
{
    chain(deser)
}

impl<DF: RecFamily, CurF: RecFamily, D, P> ChainBuilder<DF, CurF, D, P> {
    /// Enable framework metrics for every stage of this chain. Must be
    /// called before any stage is added so all stages get handles.
    ///
    /// # Panics
    ///
    /// Panics if stages were already added.
    #[must_use]
    pub fn with_metrics(
        mut self,
        pipeline: impl Into<String>,
        component: impl Into<String>,
    ) -> Self {
        assert_eq!(
            self.stage_idx, 0,
            "with_metrics must be called before stages are added"
        );
        let pipeline = pipeline.into();
        let component = component.into();
        let deser_labels = ComponentLabels::new(
            pipeline.clone(),
            format!("{component}.deserializer"),
            "deserializer",
        );
        self.metrics = Some(MetricsSpec {
            pipeline,
            component,
            deser: Arc::new(DeserMetrics::new(&deser_labels)),
        });
        self
    }

    /// Error policy for the deserializer stage (default: `Skip`).
    #[must_use]
    pub fn deser_error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.deser_policy = policy;
        self
    }

    /// Transform each record into family `NF`. For borrowing families pass
    /// a `fn` item (see the module docs); for owned payloads
    /// [`ChainBuilder::map`] is more ergonomic.
    #[must_use]
    pub fn map_rec<NF, G>(self, f: G) -> ChainBuilder<DF, NF, D, MapPart<P, G>>
    where
        NF: RecFamily,
        G: for<'buf> MapFn<CurF::Rec<'buf>, NF::Rec<'buf>>,
    {
        let Self {
            deser,
            parts,
            deser_policy,
            metrics,
            stage_idx,
            _fam,
        } = self;
        let meter = meter_for(&metrics, stage_idx, "map");
        ChainBuilder {
            deser,
            parts: MapPart {
                prev: parts,
                f,
                meter,
            },
            deser_policy,
            metrics,
            stage_idx: stage_idx + 1,
            _fam: PhantomData,
        }
    }

    /// Fallibly transform each record into family `NF` with a per-stage
    /// [`ErrorPolicy`]. For borrowing families pass a `fn` item.
    #[must_use]
    pub fn try_map_rec<NF, G, E>(
        self,
        f: G,
        policy: ErrorPolicy,
    ) -> ChainBuilder<DF, NF, D, TryMapPart<P, G>>
    where
        NF: RecFamily,
        G: for<'buf> TryMapFn<CurF::Rec<'buf>, NF::Rec<'buf>, E>,
        E: std::fmt::Display,
    {
        let Self {
            deser,
            parts,
            deser_policy,
            metrics,
            stage_idx,
            _fam,
        } = self;
        let meter = meter_for(&metrics, stage_idx, "try_map");
        ChainBuilder {
            deser,
            parts: TryMapPart {
                prev: parts,
                f,
                policy,
                component: Arc::from(format!("try_map_{stage_idx}")),
                meter,
            },
            deser_policy,
            metrics,
            stage_idx: stage_idx + 1,
            _fam: PhantomData,
        }
    }

    /// Keep only records whose payload satisfies the predicate.
    #[must_use]
    pub fn filter<Pr>(self, p: Pr) -> ChainBuilder<DF, CurF, D, FilterPart<P, Pr>>
    where
        Pr: for<'buf> FnMut(&CurF::Rec<'buf>) -> bool,
    {
        let Self {
            deser,
            parts,
            deser_policy,
            metrics,
            stage_idx,
            _fam,
        } = self;
        let meter = meter_for(&metrics, stage_idx, "filter");
        ChainBuilder {
            deser,
            parts: FilterPart {
                prev: parts,
                p,
                meter,
            },
            deser_policy,
            metrics,
            stage_idx: stage_idx + 1,
            _fam: PhantomData,
        }
    }

    /// Observe each record's payload without transforming it.
    #[must_use]
    pub fn inspect<G>(self, f: G) -> ChainBuilder<DF, CurF, D, InspectPart<P, G>>
    where
        G: for<'buf> FnMut(&CurF::Rec<'buf>),
    {
        let Self {
            deser,
            parts,
            deser_policy,
            metrics,
            stage_idx,
            _fam,
        } = self;
        ChainBuilder {
            deser,
            parts: InspectPart { prev: parts, f },
            deser_policy,
            metrics,
            stage_idx: stage_idx + 1,
            _fam: PhantomData,
        }
    }

    /// Expand each record into 0..N records of family `OutF` through a
    /// stack-borrowed [`Emitter`].
    #[must_use]
    pub fn flat_map<OutF, G>(self, g: G) -> ChainBuilder<DF, OutF, D, FlatMapPart<OutF, P, G>>
    where
        OutF: RecFamily,
        G: for<'buf> FnMut(CurF::Rec<'buf>, &mut Emitter<'_, OutF>),
    {
        let Self {
            deser,
            parts,
            deser_policy,
            metrics,
            stage_idx,
            _fam,
        } = self;
        let meter = meter_for(&metrics, stage_idx, "flat_map");
        ChainBuilder {
            deser,
            parts: FlatMapPart {
                prev: parts,
                g,
                meter,
                _out: PhantomData,
            },
            deser_policy,
            metrics,
            stage_idx: stage_idx + 1,
            _fam: PhantomData,
        }
    }

    /// Terminate the chain into a sink. Records are routed by `router`,
    /// encoded by `encoder` on the pipeline thread, and handed to the sink
    /// workers through `queues`. `router` may be any meta-only
    /// [`ShardRouter`](crate::sink::ShardRouter) (bridged automatically,
    /// [`KeyHashRouter`](crate::sink::KeyHashRouter) being the default
    /// choice) or a record-aware
    /// [`RecordRouter`](crate::sink::RecordRouter).
    #[must_use]
    pub fn sink<E, R>(
        self,
        encoder: E,
        router: R,
        cfg: ChunkConfig,
        queues: ShardQueues,
        budget: Arc<InflightBudget>,
    ) -> SinkedChain<DF, CurF, D, P, E, R> {
        let handoff_meter = meter_for(&self.metrics, self.stage_idx, "sink_handoff");
        SinkedChain {
            builder: self,
            encoder,
            router,
            cfg,
            queues,
            budget,
            handoff_meter,
        }
    }

    /// Terminate the chain into a **split sink**. Each record is routed to
    /// exactly one of several typed sink branches (each its own
    /// table/schema/encoder), declared with [`SplitBuilder::add`] and
    /// dispatched by the closure passed to [`SplitBuilder::route`]. Each
    /// branch's chunking comes from its own [`SinkCtx`](crate::ops::SinkCtx)
    /// (resolved from that sink's per-name YAML `chunk:` block, or
    /// `SinkOptions::with_chunk`, or the default), so a split tunes each
    /// destination independently. `unmatched` is the policy for a record that
    /// reaches no branch. [`ErrorPolicy::Fail`] (the operator default) stops
    /// the pipeline; [`ErrorPolicy::Skip`] drops it and counts
    /// `spate_operator_records_dropped_total{reason="unrouted"}`.
    #[must_use]
    pub fn split(self, unmatched: ErrorPolicy) -> SplitBuilder<DF, CurF, D, P> {
        SplitBuilder {
            builder: self,
            unmatched,
            branches: Vec::new(),
            next_idx: 0,
        }
    }
}

/// Closure-friendly transforms for chains whose current records are owned.
impl<DF: RecFamily, T: Send + 'static, D, P> ChainBuilder<DF, Owned<T>, D, P> {
    /// Transform each record's payload. Bare closures infer; the output
    /// type may differ (the chain's family becomes `Owned<U>`).
    #[must_use]
    pub fn map<U, G>(self, f: G) -> ChainBuilder<DF, Owned<U>, D, MapPart<P, G>>
    where
        U: Send + 'static,
        G: FnMut(T) -> U,
    {
        let Self {
            deser,
            parts,
            deser_policy,
            metrics,
            stage_idx,
            _fam,
        } = self;
        let meter = meter_for(&metrics, stage_idx, "map");
        ChainBuilder {
            deser,
            parts: MapPart {
                prev: parts,
                f,
                meter,
            },
            deser_policy,
            metrics,
            stage_idx: stage_idx + 1,
            _fam: PhantomData,
        }
    }

    /// Fallibly transform each record's payload with a per-stage
    /// [`ErrorPolicy`].
    #[must_use]
    pub fn try_map<U, G, E>(
        self,
        f: G,
        policy: ErrorPolicy,
    ) -> ChainBuilder<DF, Owned<U>, D, TryMapPart<P, G>>
    where
        U: Send + 'static,
        G: FnMut(T) -> Result<U, E>,
        E: std::fmt::Display,
    {
        let Self {
            deser,
            parts,
            deser_policy,
            metrics,
            stage_idx,
            _fam,
        } = self;
        let meter = meter_for(&metrics, stage_idx, "try_map");
        ChainBuilder {
            deser,
            parts: TryMapPart {
                prev: parts,
                f,
                policy,
                component: Arc::from(format!("try_map_{stage_idx}")),
                meter,
            },
            deser_policy,
            metrics,
            stage_idx: stage_idx + 1,
            _fam: PhantomData,
        }
    }
}

/// A fully specified chain, ready to build, or to stamp out one instance per
/// pipeline thread via [`SinkedChain::build_factory`].
#[derive(Clone, Debug)]
pub struct SinkedChain<DF: RecFamily, CurF: RecFamily, D, P, E, R> {
    builder: ChainBuilder<DF, CurF, D, P>,
    encoder: E,
    router: R,
    cfg: ChunkConfig,
    queues: ShardQueues,
    budget: Arc<InflightBudget>,
    handoff_meter: OpMeterSlot,
}

impl<DF, CurF, D, P, E, R> SinkedChain<DF, CurF, D, P, E, R>
where
    DF: RecFamily,
    CurF: RecFamily,
    D: Deserializer<DF> + 'static,
    P: Assemble<SinkHandoff<CurF, E, R>>,
    P::Out: for<'buf> Collector<<DF as RecFamily>::Rec<'buf>> + StageLifecycle + Send + 'static,
    E: RowEncoder<CurF> + Clone + 'static,
    R: RecordRouter<CurF> + Send + 'static,
{
    /// Build one chain instance. Consumes the specification; the deserializer
    /// and router need no `Clone`, but the encoder does. The terminal stage
    /// mints one encoder per shard (columnar encoders hold per-block state
    /// that cannot be shared), and every in-tree encoder is `Clone`.
    #[must_use]
    pub fn build(self) -> Box<dyn RunnableChain> {
        let SinkedChain {
            builder,
            encoder,
            router,
            cfg,
            queues,
            budget,
            handoff_meter,
        } = self;
        let term = SinkHandoff::new(
            encoder,
            router,
            queues,
            budget,
            cfg,
            handoff_meter,
            Arc::from("sink_handoff"),
        );
        let ops = builder.parts.assemble(term);
        Box::new(TypedChain::<DF, D, _>::new(
            builder.deser,
            ops,
            builder.deser_policy,
            builder.metrics.as_ref().map(|m| Arc::clone(&m.deser)),
        ))
    }

    /// Turn the specification into a factory producing one identical chain
    /// per pipeline thread.
    #[must_use]
    pub fn build_factory(self) -> ChainFactory<DF, CurF, D, P, E, R>
    where
        D: Clone,
        E: Clone,
        R: Clone,
    {
        ChainFactory { spec: self }
    }
}

/// Stamps out identical chains, one per pipeline thread. `Send + Sync`
/// when the deserializer, stage closures, encoder, and router are.
#[derive(Clone, Debug)]
pub struct ChainFactory<DF: RecFamily, CurF: RecFamily, D, P, E, R> {
    spec: SinkedChain<DF, CurF, D, P, E, R>,
}

impl<DF, CurF, D, P, E, R> ChainFactory<DF, CurF, D, P, E, R>
where
    DF: RecFamily,
    CurF: RecFamily,
    D: Deserializer<DF> + Clone + 'static,
    P: Assemble<SinkHandoff<CurF, E, R>>,
    P::Out: for<'buf> Collector<<DF as RecFamily>::Rec<'buf>> + StageLifecycle + Send + 'static,
    E: RowEncoder<CurF> + Clone + 'static,
    R: RecordRouter<CurF> + Clone + Send + 'static,
{
    /// Build one more identical chain.
    #[must_use]
    pub fn make(&self) -> Box<dyn RunnableChain> {
        let spec = &self.spec;
        let term = SinkHandoff::new(
            spec.encoder.clone(),
            spec.router.clone(),
            spec.queues.clone(),
            Arc::clone(&spec.budget),
            spec.cfg,
            spec.handoff_meter.clone(),
            Arc::from("sink_handoff"),
        );
        let ops = spec.builder.parts.assemble(term);
        Box::new(TypedChain::<DF, D, _>::new(
            spec.builder.deser.clone(),
            ops,
            spec.builder.deser_policy,
            spec.builder.metrics.as_ref().map(|m| Arc::clone(&m.deser)),
        ))
    }
}

/// Accumulates split-sink branches before the routing closure is supplied.
/// Built by [`ChainBuilder::split`]; each [`add`](Self::add) declares one
/// destination and hands back a typed handle, then [`route`](Self::route)
/// takes the closure that dispatches to them.
pub struct SplitBuilder<DF: RecFamily, CurF: RecFamily, D, P> {
    builder: ChainBuilder<DF, CurF, D, P>,
    unmatched: ErrorPolicy,
    branches: Vec<Box<dyn ErasedBranch>>,
    next_idx: usize,
}

impl<DF: RecFamily, CurF: RecFamily, D, P> std::fmt::Debug for SplitBuilder<DF, CurF, D, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitBuilder")
            .field("branches", &self.branches.len())
            .field("unmatched", &self.unmatched)
            .finish_non_exhaustive()
    }
}

impl<DF: RecFamily, CurF: RecFamily, D, P> SplitBuilder<DF, CurF, D, P> {
    /// Declare one destination branch of family `F` and return its typed,
    /// `Copy` [`Sink<F>`] handle. `sink` comes from
    /// [`ChainCtx::sink`](crate::pipeline::ChainCtx::sink); `encoder`/`router`
    /// are that table's, exactly as for [`ChainBuilder::sink`]. The declaration
    /// order fixes the handle indices; call once per destination, then
    /// [`route`](Self::route).
    #[must_use = "a dropped Sink<F> handle leaves its branch permanently unreachable"]
    pub fn add<F, E, R>(&mut self, encoder: E, router: R, sink: SinkCtx) -> Sink<F>
    where
        F: RecFamily + 'static,
        E: RowEncoder<F> + Clone + Send + 'static,
        R: RecordRouter<F> + 'static,
    {
        let idx = self.next_idx;
        self.next_idx += 1;
        // Per-branch operator meter, labeled by the sink name so each branch's
        // `spate_operator_*` series is distinct.
        let meter = OpMeterSlot(OpMeter::new(self.builder.metrics.as_ref().map(|m| {
            let labels = ComponentLabels::new(
                m.pipeline.clone(),
                format!("{}.sink.{}", m.component, sink.name),
                "sink_handoff",
            );
            Arc::new(OperatorMetrics::new(&labels))
        })));
        let component: Arc<str> = Arc::from(format!("sink.{}", sink.name));
        let branch = new_branch::<F, E, R>(
            encoder,
            router,
            sink.queues,
            sink.budget,
            sink.chunk,
            meter,
            component,
        );
        self.branches.push(branch);
        Sink::new(idx)
    }

    /// Supply the routing closure (classify + extract per record in one
    /// `match`) and finish the split. The closure emits each record to exactly
    /// one branch via [`SplitEmitter::emit`]; emitting to none invokes the
    /// `unmatched` policy from [`ChainBuilder::split`].
    #[must_use]
    pub fn route<G>(self, route: G) -> RoutedSplit<DF, CurF, D, P, G>
    where
        G: for<'buf> FnMut(CurF::Rec<'buf>, &mut SplitEmitter<'_>) + Send + 'static,
    {
        let handoff_meter = meter_for(&self.builder.metrics, self.builder.stage_idx, "split");
        RoutedSplit {
            builder: self.builder,
            unmatched: self.unmatched,
            branches: self.branches,
            route,
            handoff_meter,
        }
    }
}

/// A split-terminated chain, ready to [`build`](Self::build). Not `Clone`; the
/// branches are built once. Stamp identical per-thread chains by re-running
/// the `chains` factory closure (as the pipeline builder does).
pub struct RoutedSplit<DF: RecFamily, CurF: RecFamily, D, P, G> {
    builder: ChainBuilder<DF, CurF, D, P>,
    unmatched: ErrorPolicy,
    branches: Vec<Box<dyn ErasedBranch>>,
    route: G,
    handoff_meter: OpMeterSlot,
}

impl<DF: RecFamily, CurF: RecFamily, D, P, G> std::fmt::Debug for RoutedSplit<DF, CurF, D, P, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutedSplit")
            .field("branches", &self.branches.len())
            .field("unmatched", &self.unmatched)
            .finish_non_exhaustive()
    }
}

impl<DF, CurF, D, P, G> RoutedSplit<DF, CurF, D, P, G>
where
    DF: RecFamily,
    CurF: RecFamily,
    D: Deserializer<DF> + 'static,
    G: for<'buf> FnMut(CurF::Rec<'buf>, &mut SplitEmitter<'_>) + Send + 'static,
    P: Assemble<SplitTerminal<CurF, G>>,
    P::Out: for<'buf> Collector<<DF as RecFamily>::Rec<'buf>> + StageLifecycle + Send + 'static,
{
    /// Build one chain instance whose terminal is the split sink.
    #[must_use]
    pub fn build(self) -> Box<dyn RunnableChain> {
        let RoutedSplit {
            builder,
            unmatched,
            branches,
            route,
            handoff_meter,
        } = self;
        let term = SplitTerminal::new(
            route,
            branches,
            unmatched,
            handoff_meter,
            Arc::from("split"),
        );
        let ops = builder.parts.assemble(term);
        Box::new(TypedChain::<DF, D, _>::new(
            builder.deser,
            ops,
            builder.deser_policy,
            builder.metrics.as_ref().map(|m| Arc::clone(&m.deser)),
        ))
    }
}
