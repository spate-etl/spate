//! Chain combinators and the typed chain behind the erasure boundary.
//!
//! Stages compose statically; `Map<G, Filter<P, Term>>` monomorphizes into
//! one loop body. Beyond [`Collector`](super::Collector), every stage
//! implements [`StageLifecycle`]: the cascade the boundary uses to flush
//! per-batch metric accumulators, surface fatal errors, and reach the
//! terminal stage's buffers without any stage knowing its downstream's
//! concrete type.
//!
//! Pressure discipline: the terminal stage never rejects a record
//! mid-payload. It parks sealed chunks internally and reports pressure
//! through [`StageLifecycle::relieve`], which the chain checks *between*
//! payloads. Operators therefore never re-run for an already-pushed record,
//! and no lifetime-bearing state is ever stored across the boundary.

use super::{BlockReason, Collector, CollectorFor, PushOutcome, RunnableChain};
use crate::checkpoint::AckRef;
use crate::deser::{Deserializer, EmitRecord, RecFamily};
use crate::error::{DeserError, ErrorClass, ErrorPolicy, FatalError};
use crate::metrics::{DeserMetrics, OperatorMetrics};
use crate::record::{Flow, RawPayload, Record, RecordMeta};
use crate::source::PayloadBatch;
use crate::telemetry::RateLimit;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-batch lifecycle cascade implemented by every stage. Combinators
/// handle their own concern and delegate downstream; the terminal stage
/// anchors the recursion.
pub trait StageLifecycle {
    /// Flush per-batch metric accumulators. `elapsed` is the chain-level
    /// batch duration: per-stage attribution inside one inlined loop is
    /// not measurable without per-record clocks, so every stage reports
    /// the chain figure, keeping the histograms comparable.
    fn on_batch_end(&mut self, elapsed: Duration);

    /// Take the first fatal error recorded by any stage.
    fn take_fatal(&mut self) -> Option<FatalError>;

    /// Let the terminal stage drain parked output. [`Flow::Blocked`] means
    /// it is still backed up and the chain must not accept new payloads.
    fn relieve(&mut self) -> Flow;

    /// Seal and hand off all terminal buffers, partial chunks included.
    /// [`Flow::Blocked`] means not everything could be sent; retry later.
    fn flush_terminal(&mut self) -> Flow;
}

/// Local accumulators around a shared [`OperatorMetrics`] handle: stages
/// count into plain fields during a batch and flush once at batch end.
#[derive(Debug, Default)]
pub(crate) struct OpMeter {
    handle: Option<Arc<OperatorMetrics>>,
    records_in: u64,
    records_out: u64,
    filtered: u64,
    skipped: u64,
    unrouted: u64,
    record_errors: u64,
}

impl OpMeter {
    pub(crate) fn new(handle: Option<Arc<OperatorMetrics>>) -> Self {
        OpMeter {
            handle,
            ..Default::default()
        }
    }

    #[inline(always)]
    pub(crate) fn seen(&mut self) {
        self.records_in += 1;
    }

    #[inline(always)]
    pub(crate) fn out(&mut self) {
        self.records_out += 1;
    }

    #[inline(always)]
    pub(crate) fn out_n(&mut self, n: u64) {
        self.records_out += n;
    }

    #[inline(always)]
    pub(crate) fn filtered(&mut self) {
        self.filtered += 1;
    }

    #[inline(always)]
    pub(crate) fn skipped(&mut self) {
        self.skipped += 1;
    }

    #[inline(always)]
    pub(crate) fn unrouted(&mut self) {
        self.unrouted += 1;
    }

    #[inline(always)]
    pub(crate) fn record_error(&mut self) {
        self.record_errors += 1;
    }

    pub(crate) fn flush(&mut self, elapsed: Duration) {
        if let Some(h) = &self.handle {
            h.batch(self.records_in, self.records_out, elapsed);
            if self.filtered > 0 {
                h.filtered(self.filtered);
            }
            if self.skipped > 0 {
                h.skipped(self.skipped);
            }
            if self.unrouted > 0 {
                h.unrouted(self.unrouted);
            }
            if self.record_errors > 0 {
                h.errors(ErrorClass::RecordLevel, self.record_errors);
            }
        }
        self.records_in = 0;
        self.records_out = 0;
        self.filtered = 0;
        self.skipped = 0;
        self.unrouted = 0;
        self.record_errors = 0;
    }
}

/// Cloneable meter slot: chain factories clone stage structs per pipeline
/// thread; the shared handle is kept, the local accumulators reset.
#[derive(Debug, Default)]
pub(crate) struct OpMeterSlot(pub(crate) OpMeter);

impl Clone for OpMeterSlot {
    fn clone(&self) -> Self {
        OpMeterSlot(OpMeter::new(self.0.handle.clone()))
    }
}

/// Cloneable fatal-error slot (clears on clone, like the meter slot).
#[derive(Debug, Default)]
pub(crate) struct FatalSlot(pub(crate) Option<FatalError>);

impl Clone for FatalSlot {
    fn clone(&self) -> Self {
        FatalSlot(None)
    }
}

/// `map`: transform the payload.
#[derive(Clone, Debug)]
pub struct Map<G, N> {
    pub(crate) f: G,
    pub(crate) next: N,
    pub(crate) meter: OpMeterSlot,
}

impl<In, Out, G, N> Collector<In> for Map<G, N>
where
    G: FnMut(In) -> Out,
    N: Collector<Out>,
{
    #[inline(always)]
    fn push(&mut self, rec: Record<In>) -> Flow {
        self.meter.0.seen();
        self.meter.0.out();
        self.next.push(rec.map(&mut self.f))
    }
}

/// `filter`: drop records failing the predicate. A drop releases the
/// record's ack share, so it counts as success for the batch.
#[derive(Clone, Debug)]
pub struct Filter<P, N> {
    pub(crate) p: P,
    pub(crate) next: N,
    pub(crate) meter: OpMeterSlot,
}

impl<T, P, N> Collector<T> for Filter<P, N>
where
    P: FnMut(&T) -> bool,
    N: Collector<T>,
{
    #[inline(always)]
    fn push(&mut self, rec: Record<T>) -> Flow {
        self.meter.0.seen();
        if (self.p)(&rec.payload) {
            self.meter.0.out();
            self.next.push(rec)
        } else {
            self.meter.0.filtered();
            Flow::Continue
        }
    }
}

/// `inspect`: observe without transforming (no metrics of its own).
#[derive(Clone, Debug)]
pub struct Inspect<G, N> {
    pub(crate) f: G,
    pub(crate) next: N,
}

impl<T, G, N> Collector<T> for Inspect<G, N>
where
    G: FnMut(&T),
    N: Collector<T>,
{
    #[inline(always)]
    fn push(&mut self, rec: Record<T>) -> Flow {
        (self.f)(&rec.payload);
        self.next.push(rec)
    }
}

static TRY_MAP_SKIP_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));

/// `try_map`: fallible transform with a per-stage [`ErrorPolicy`].
/// `Skip` drops the record (releasing its ack share) and counts it;
/// `Fail` records a fatal error; the batch aborts and the pipeline stops.
#[derive(Clone, Debug)]
pub struct TryMap<G, N> {
    pub(crate) f: G,
    pub(crate) next: N,
    pub(crate) policy: ErrorPolicy,
    pub(crate) component: Arc<str>,
    pub(crate) meter: OpMeterSlot,
    pub(crate) fatal: FatalSlot,
}

impl<In, Out, E, G, N> Collector<In> for TryMap<G, N>
where
    G: FnMut(In) -> Result<Out, E>,
    E: std::fmt::Display,
    N: Collector<Out>,
{
    #[inline(always)]
    fn push(&mut self, rec: Record<In>) -> Flow {
        self.meter.0.seen();
        if self.fatal.0.is_some() {
            // Fatal pending: swallow the rest of the batch. The boundary
            // fails the batch's ack, so nothing is lost.
            return Flow::Continue;
        }
        let Record { payload, meta, ack } = rec;
        match (self.f)(payload) {
            Ok(out) => {
                self.meter.0.out();
                self.next.push(Record {
                    payload: out,
                    meta,
                    ack,
                })
            }
            Err(e) => {
                match self.policy {
                    ErrorPolicy::Skip => {
                        self.meter.0.skipped();
                        self.meter.0.record_error();
                        crate::rate_limited_warn!(
                            TRY_MAP_SKIP_WARN,
                            component = &*self.component,
                            error = %e,
                            "record skipped by error policy"
                        );
                    }
                    _ => {
                        self.fatal.0 = Some(FatalError {
                            component: self.component.to_string(),
                            reason: e.to_string(),
                        });
                    }
                }
                Flow::Continue
            }
        }
    }
}

/// Stack-borrowed emitter handed to `flat_map` closures. Parameterized by
/// the output *family* (a `'static` tag), so user closures never name the
/// concrete downstream stack type or the buffer lifetime. Each `emit` is
/// one virtual call, confined to flat_map stages.
pub struct Emitter<'a, OutF: RecFamily> {
    next: &'a mut dyn CollectorFor<OutF>,
    meta: RecordMeta,
    ack: &'a AckRef,
    emitted: u64,
    flow: Flow,
}

impl<OutF: RecFamily> std::fmt::Debug for Emitter<'_, OutF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Emitter")
            .field("emitted", &self.emitted)
            .field("flow", &self.flow)
            .finish_non_exhaustive()
    }
}

impl<OutF: RecFamily> Emitter<'_, OutF> {
    /// Emit one derived record, inheriting the parent's metadata and ack
    /// handle.
    #[inline(always)]
    pub fn emit<'buf>(&mut self, payload: OutF::Rec<'buf>) -> Flow {
        let flow = self.next.push_rec(Record {
            payload,
            meta: self.meta,
            ack: self.ack.clone(),
        });
        self.emitted += 1;
        if self.flow != Flow::Blocked {
            self.flow = flow;
        }
        flow
    }

    /// The parent record's metadata.
    #[must_use]
    pub fn meta(&self) -> RecordMeta {
        self.meta
    }
}

/// `flat_map`: one record in, 0..N out via a stack-borrowed [`Emitter`].
/// Carries the output family as a type parameter so the impl is fully
/// constrained (closure argument types are not associated bindings).
#[derive(Clone, Debug)]
pub struct FlatMap<OutF: RecFamily, G, N> {
    pub(crate) g: G,
    pub(crate) next: N,
    pub(crate) meter: OpMeterSlot,
    pub(crate) _out: PhantomData<fn() -> OutF>,
}

impl<In, OutF, G, N> Collector<In> for FlatMap<OutF, G, N>
where
    OutF: RecFamily,
    G: FnMut(In, &mut Emitter<'_, OutF>),
    N: for<'buf> Collector<<OutF as RecFamily>::Rec<'buf>>,
{
    #[inline(always)]
    fn push(&mut self, rec: Record<In>) -> Flow {
        self.meter.0.seen();
        let Record { payload, meta, ack } = rec;
        let mut em = Emitter {
            next: &mut self.next,
            meta,
            ack: &ack,
            emitted: 0,
            flow: Flow::Continue,
        };
        (self.g)(payload, &mut em);
        let (emitted, flow) = (em.emitted, em.flow);
        self.meter.0.out_n(emitted);
        flow
    }
}

// ---- lifecycle cascade ---------------------------------------------------

impl<G, N: StageLifecycle> StageLifecycle for Map<G, N> {
    fn on_batch_end(&mut self, elapsed: Duration) {
        self.meter.0.flush(elapsed);
        self.next.on_batch_end(elapsed);
    }
    fn take_fatal(&mut self) -> Option<FatalError> {
        self.next.take_fatal()
    }
    fn relieve(&mut self) -> Flow {
        self.next.relieve()
    }
    fn flush_terminal(&mut self) -> Flow {
        self.next.flush_terminal()
    }
}

impl<P, N: StageLifecycle> StageLifecycle for Filter<P, N> {
    fn on_batch_end(&mut self, elapsed: Duration) {
        self.meter.0.flush(elapsed);
        self.next.on_batch_end(elapsed);
    }
    fn take_fatal(&mut self) -> Option<FatalError> {
        self.next.take_fatal()
    }
    fn relieve(&mut self) -> Flow {
        self.next.relieve()
    }
    fn flush_terminal(&mut self) -> Flow {
        self.next.flush_terminal()
    }
}

impl<G, N: StageLifecycle> StageLifecycle for Inspect<G, N> {
    fn on_batch_end(&mut self, elapsed: Duration) {
        self.next.on_batch_end(elapsed);
    }
    fn take_fatal(&mut self) -> Option<FatalError> {
        self.next.take_fatal()
    }
    fn relieve(&mut self) -> Flow {
        self.next.relieve()
    }
    fn flush_terminal(&mut self) -> Flow {
        self.next.flush_terminal()
    }
}

impl<G, N: StageLifecycle> StageLifecycle for TryMap<G, N> {
    fn on_batch_end(&mut self, elapsed: Duration) {
        self.meter.0.flush(elapsed);
        self.next.on_batch_end(elapsed);
    }
    fn take_fatal(&mut self) -> Option<FatalError> {
        self.fatal.0.take().or_else(|| self.next.take_fatal())
    }
    fn relieve(&mut self) -> Flow {
        self.next.relieve()
    }
    fn flush_terminal(&mut self) -> Flow {
        self.next.flush_terminal()
    }
}

impl<OutF: RecFamily, G, N: StageLifecycle> StageLifecycle for FlatMap<OutF, G, N> {
    fn on_batch_end(&mut self, elapsed: Duration) {
        self.meter.0.flush(elapsed);
        self.next.on_batch_end(elapsed);
    }
    fn take_fatal(&mut self) -> Option<FatalError> {
        self.next.take_fatal()
    }
    fn relieve(&mut self) -> Flow {
        self.next.relieve()
    }
    fn flush_terminal(&mut self) -> Flow {
        self.next.flush_terminal()
    }
}

// ---- the typed chain behind the boundary ----------------------------------

/// Adapter exposing the ops stack as the deserializer's emission target,
/// counting emissions and latching downstream pressure.
struct OpsEmit<'a, Ops> {
    ops: &'a mut Ops,
    emitted: &'a mut u64,
    flow: &'a mut Flow,
}

impl<'buf, T, Ops> EmitRecord<'buf, T> for OpsEmit<'_, Ops>
where
    Ops: Collector<T>,
{
    #[inline(always)]
    fn emit(&mut self, rec: Record<T>) -> Flow {
        *self.emitted += 1;
        let flow = self.ops.push(rec);
        if *self.flow != Flow::Blocked {
            *self.flow = flow;
        }
        flow
    }
}

static DESER_SKIP_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));

/// An owned copy of a payload whose deserialization returned
/// [`DeserError::NotReady`], replayed on the next push. Owned because the
/// chain is `'static`-erased and cannot hold `'buf` data across calls; the
/// copy happens only on the rare not-ready path, which already implies an
/// asynchronous round-trip (e.g. a schema-registry fetch).
#[derive(Debug)]
struct PendingPayload {
    bytes: Vec<u8>,
    key: Option<Vec<u8>>,
    partition: crate::record::PartitionId,
    offset: i64,
    timestamp_ms: i64,
}

impl PendingPayload {
    fn from_raw(raw: &RawPayload<'_>) -> Self {
        PendingPayload {
            bytes: raw.bytes.to_vec(),
            key: raw.key.map(<[u8]>::to_vec),
            partition: raw.partition,
            offset: raw.offset,
            timestamp_ms: raw.timestamp_ms,
        }
    }

    fn as_raw(&self) -> RawPayload<'_> {
        RawPayload {
            bytes: &self.bytes,
            key: self.key.as_deref(),
            partition: self.partition,
            offset: self.offset,
            timestamp_ms: self.timestamp_ms,
        }
    }
}

/// Outcome of pushing one payload through deserializer + operator stack.
enum Step {
    /// Payload fully processed; keep going.
    Continue,
    /// Payload fully processed (output parked as needed); downstream is
    /// backed up, so stop accepting further payloads.
    Backpressure,
    /// Payload untouched: deserialization reported
    /// [`DeserError::NotReady`]; replay it later.
    NotReady,
    /// A stage failed fatally.
    Fatal(FatalError),
}

/// A concrete chain: deserializer + statically composed operator stack,
/// erased behind [`RunnableChain`]. `Ops` must accept the family's record
/// type at every buffer lifetime; the HRTB is what makes borrowed records
/// legal behind the erased boundary.
pub struct TypedChain<F: RecFamily, D, Ops> {
    deser: D,
    ops: Ops,
    deser_policy: ErrorPolicy,
    deser_metrics: Option<Arc<DeserMetrics>>,
    /// Payloads fully processed from the batch currently in progress.
    cursor: usize,
    mid_batch: bool,
    /// Not-ready payload awaiting replay (always belongs to the batch in
    /// progress; carries no ack, since the batch's handle covers it).
    pending: Option<PendingPayload>,
    _family: PhantomData<fn() -> F>,
}

impl<F: RecFamily, D, Ops> TypedChain<F, D, Ops> {
    pub(crate) fn new(
        deser: D,
        ops: Ops,
        deser_policy: ErrorPolicy,
        deser_metrics: Option<Arc<DeserMetrics>>,
    ) -> Self {
        TypedChain {
            deser,
            ops,
            deser_policy,
            deser_metrics,
            cursor: 0,
            mid_batch: false,
            pending: None,
            _family: PhantomData,
        }
    }
}

impl<F, D, Ops> TypedChain<F, D, Ops>
where
    F: RecFamily,
    D: Deserializer<F>,
    Ops: for<'buf> Collector<<F as RecFamily>::Rec<'buf>> + StageLifecycle + Send,
{
    /// Push one payload through the deserializer and operator stack.
    fn deser_step(
        &mut self,
        raw: &RawPayload<'_>,
        ack: &AckRef,
        ok: &mut u64,
        errors: &mut u64,
    ) -> Step {
        let mut flow = Flow::Continue;
        let mut emitted = 0u64;
        let result = self.deser.deserialize(
            raw,
            ack,
            &mut OpsEmit {
                ops: &mut self.ops,
                emitted: &mut emitted,
                flow: &mut flow,
            },
        );
        *ok += emitted;
        match result {
            Err(DeserError::NotReady { .. }) => {
                debug_assert_eq!(
                    emitted, 0,
                    "NotReady after emitting records would duplicate them on replay"
                );
                return Step::NotReady;
            }
            Err(e) => {
                *errors += 1;
                match self.deser_policy {
                    ErrorPolicy::Skip => {
                        crate::rate_limited_warn!(
                            DESER_SKIP_WARN,
                            partition = raw.partition.0,
                            offset = raw.offset,
                            error = %e,
                            "payload skipped by deserializer error policy"
                        );
                    }
                    _ => {
                        return Step::Fatal(FatalError {
                            component: "deserializer".to_string(),
                            reason: e.to_string(),
                        });
                    }
                }
            }
            Ok(()) => {}
        }
        if let Some(fatal) = self.ops.take_fatal() {
            return Step::Fatal(fatal);
        }
        // The terminal never rejects mid-payload; pressure surfaces
        // between payloads, so operators never re-run for a record.
        if flow == Flow::Blocked || self.ops.relieve() == Flow::Blocked {
            return Step::Backpressure;
        }
        Step::Continue
    }
}

impl<F: RecFamily, D, Ops> std::fmt::Debug for TypedChain<F, D, Ops> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedChain")
            .field("cursor", &self.cursor)
            .field("mid_batch", &self.mid_batch)
            .finish_non_exhaustive()
    }
}

impl<F, D, Ops> RunnableChain for TypedChain<F, D, Ops>
where
    F: RecFamily,
    D: Deserializer<F>,
    Ops: for<'buf> Collector<<F as RecFamily>::Rec<'buf>> + StageLifecycle + Send,
{
    fn push_batch<'buf>(&mut self, batch: &mut dyn PayloadBatch<'buf>, from: usize) -> PushOutcome {
        if self.mid_batch {
            debug_assert_eq!(
                from, self.cursor,
                "resume must continue where Blocked stopped"
            );
        } else {
            debug_assert_eq!(from, 0, "fresh batches start at payload 0");
            debug_assert!(
                self.pending.is_none(),
                "a not-ready payload must not survive its batch"
            );
            self.cursor = 0;
            self.pending = None;
            self.mid_batch = true;
        }

        // A backed-up terminal would grow without bound if we kept
        // encoding into it: drain first, stay blocked if that fails.
        if self.ops.relieve() == Flow::Blocked {
            return PushOutcome::Blocked {
                resume_at: self.cursor,
                reason: BlockReason::Capacity,
            };
        }

        let started = Instant::now();
        let mut ok: u64 = 0;
        let mut errors: u64 = 0;
        let mut not_ready: u64 = 0;
        let mut outcome: Option<PushOutcome> = None;

        // Replay a stashed not-ready payload before pulling new ones. Its
        // index is `cursor`; the batch iterator is already past it.
        if let Some(p) = self.pending.take() {
            let raw = p.as_raw();
            match self.deser_step(&raw, batch.ack(), &mut ok, &mut errors) {
                Step::Continue => self.cursor += 1,
                Step::Backpressure => {
                    self.cursor += 1;
                    outcome = Some(PushOutcome::Blocked {
                        resume_at: self.cursor,
                        reason: BlockReason::Capacity,
                    });
                }
                Step::NotReady => {
                    not_ready += 1;
                    self.pending = Some(p);
                    outcome = Some(PushOutcome::Blocked {
                        resume_at: self.cursor,
                        reason: BlockReason::NotReady,
                    });
                }
                Step::Fatal(f) => outcome = Some(PushOutcome::Fatal(f)),
            }
        }

        while outcome.is_none() {
            let Some(raw) = batch.next_payload() else {
                break;
            };
            match self.deser_step(&raw, batch.ack(), &mut ok, &mut errors) {
                Step::Continue => self.cursor += 1,
                Step::Backpressure => {
                    self.cursor += 1;
                    outcome = Some(PushOutcome::Blocked {
                        resume_at: self.cursor,
                        reason: BlockReason::Capacity,
                    });
                }
                Step::NotReady => {
                    not_ready += 1;
                    self.pending = Some(PendingPayload::from_raw(&raw));
                    outcome = Some(PushOutcome::Blocked {
                        resume_at: self.cursor,
                        reason: BlockReason::NotReady,
                    });
                }
                Step::Fatal(f) => outcome = Some(PushOutcome::Fatal(f)),
            }
        }

        let elapsed = started.elapsed();
        if let Some(m) = &self.deser_metrics {
            m.batch(ok, errors, elapsed);
            if errors > 0 && matches!(self.deser_policy, ErrorPolicy::Skip) {
                m.dropped(errors);
            }
            if not_ready > 0 {
                m.not_ready(not_ready);
            }
        }
        self.ops.on_batch_end(elapsed);

        match outcome {
            Some(PushOutcome::Fatal(f)) => {
                batch.ack().fail();
                self.mid_batch = false;
                self.pending = None;
                PushOutcome::Fatal(f)
            }
            Some(blocked) => blocked,
            None => {
                self.mid_batch = false;
                PushOutcome::Done
            }
        }
    }

    fn flush(&mut self) -> PushOutcome {
        if let Some(fatal) = self.ops.take_fatal() {
            return PushOutcome::Fatal(fatal);
        }
        match self.ops.flush_terminal() {
            Flow::Continue => PushOutcome::Done,
            Flow::Blocked => PushOutcome::Blocked {
                resume_at: self.cursor,
                reason: BlockReason::Capacity,
            },
        }
    }

    fn abandon_batch(&mut self) {
        // Drop the mid-batch cursor and any stashed not-ready payload. The
        // terminal stage's parked chunks (with their own acks) are left
        // alone; only this chain's per-batch bookkeeping is reset so the
        // next fresh push_batch neither asserts nor replays the stale
        // payload under a new batch's ack.
        self.mid_batch = false;
        self.cursor = 0;
        self.pending = None;
    }
}
