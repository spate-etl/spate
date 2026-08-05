//! The decode rig both bench tiers drive: one deserializer, the bytes it
//! decodes, and the sink that counts what came out.
//!
//! Included with `#[path]` rather than imported: a bench target is its own
//! crate, so two targets can only agree on what they measure by compiling the
//! same source. The corpora already work that way (`orders.rs`, `shapes.rs`);
//! this is the other half. `decode_gungraun.rs` counts instructions inside
//! [`decode_run`] and `decode_wall.rs` times the same function, so a counted
//! regression and a wall-clock one are statements about one region rather than
//! about two hand-copied ones that have since drifted apart.
//!
//! What stays in each target is the case list — which corpus, under which
//! settings, expecting how many records. That is the part the two tiers are
//! entitled to differ on: the counted tier can afford a case the wall tier
//! would spend a minute of a person's afternoon on.
//!
//! Nothing here mentions a corpus. [`batch_rig`] is generic over the record
//! type and [`shape_rig`] decodes into a value, so both take payloads as bare
//! bytes and no corpus type crosses this boundary.

// Each includer compiles this module separately and uses a different subset of
// it — the wall tier has no use for `decode_once`, which exists for the four
// reference cases gungraun calls exactly once. So an item is legitimately dead
// in one target while live in another, which is a module-wide `allow` rather
// than per-item `expect`: an `expect` would itself go unfulfilled in whichever
// target does use the item.
#![allow(dead_code, reason = "each bench target uses a different subset")]

use serde::de::DeserializeOwned;
use serde_json::Value;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, Owned, RecFamily};
use spate_core::record::{Flow, PartitionId, RawPayload, Record};
use spate_json::{JsonDeserializerBuilder, JsonFraming, JsonSettings, OnError};
use std::hint::black_box;

/// A sink that counts records and nothing else.
///
/// Generic over the record type so one sink serves the typed and the
/// dynamically-typed arms. `Flow::Continue` unconditionally: the deserializer
/// discards what `emit` returns — backpressure is handled between payloads,
/// not inside one — so a sink that blocked would change nothing except this
/// file.
pub(crate) struct Sink(pub(crate) u64);

impl<T> EmitRecord<'_, T> for Sink {
    fn emit(&mut self, _rec: Record<T>) -> Flow {
        self.0 += 1;
        Flow::Continue
    }
}

/// One deserializer with the bytes it decodes.
///
/// The payload is owned rather than pre-wrapped in a `RawPayload`, because a
/// `RawPayload` borrows it and a benchmark argument has to be a single owned
/// value. Building the wrapper is a handful of stores inside the measured
/// region; it is identical across the cases, so it cancels in any comparison.
pub(crate) struct Rig<D> {
    pub(crate) deser: D,
    pub(crate) payload: Vec<u8>,
    pub(crate) ack: AckRef,
    pub(crate) sink: Sink,
    /// How many records this payload must yield. Asserted rather than
    /// returned: a framing that silently stopped splitting would otherwise
    /// read as a large improvement.
    pub(crate) expect: u64,
}

pub(crate) fn raw_payload(bytes: &[u8]) -> RawPayload<'_> {
    RawPayload {
        bytes,
        key: None,
        partition: PartitionId(0),
        offset: 1,
        timestamp_ms: 0,
    }
}

/// The measured work: one payload through one deserializer.
pub(crate) fn decode_once<F, D>(rig: &mut Rig<D>)
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let raw = raw_payload(&rig.payload);
    rig.sink.0 = 0;
    rig.deser
        .deserialize(black_box(&raw), &rig.ack, &mut rig.sink)
        .unwrap();
    assert_eq!(rig.sink.0, rig.expect, "framing emitted a different count");
}

/// The measured work for the cases added after the reference four: one
/// payload through one deserializer, returning how many records it emitted.
///
/// `#[inline(never)]` is load-bearing, not stylistic, and removing it does not
/// fail anything — it silently empties the measurement. Callgrind toggles
/// collection on the benchmark function's module, and a toggle flips
/// collection rather than forcing it on, so work the optimiser reshapes across
/// that boundary leaves the region holding whatever else was running — usually
/// the allocator tearing down the corpus. `deserialize` is generic over the
/// record family and monomorphises into this crate, which makes it an ordinary
/// inlining candidate; a named frame the optimiser may not erase is what keeps
/// the decode inside the region.
///
/// The wall tier does not need the attribute and is not harmed by it: it times
/// a region it opens and closes itself. Keeping one function for both tiers is
/// worth more than saving a call, and it means neither tier can be reshaped
/// without the other seeing it.
///
/// Resetting the sink is what makes the function repeatable. The counted tier
/// drives a rig once and discards it, so nothing there needs to survive a
/// second call; the wall harness drives one rig thousands of times, and a
/// counter that accumulated would fail the assertion on the second iteration.
///
/// Returning the emitted count is what keeps the call alive: the records are
/// otherwise unobserved, and without a use the optimiser is free to delete the
/// decode this exists to count. The caller asserts it, so a fixture that
/// silently stopped emitting — or started decoding cleanly — cannot pass as a
/// fast one.
#[inline(never)]
pub(crate) fn decode_run<F, D>(rig: &mut Rig<D>) -> u64
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let raw = raw_payload(&rig.payload);
    rig.sink.0 = 0;
    rig.deser
        .deserialize(black_box(&raw), &rig.ack, &mut rig.sink)
        .expect("the fixture decodes under its error policy");
    rig.sink.0
}

/// The measured work for a case whose payload must fail.
///
/// Separate from [`decode_run`] rather than folded into it with a flag: the
/// two differ in what they assert, and an `Ok` from an `on_error: fail`
/// fixture is a fixture that stopped being broken — which would leave the case
/// quietly measuring the happy path under an error-path name.
#[inline(never)]
pub(crate) fn decode_run_err<F, D>(rig: &mut Rig<D>) -> u64
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let raw = raw_payload(&rig.payload);
    rig.sink.0 = 0;
    let res = rig
        .deser
        .deserialize(black_box(&raw), &rig.ack, &mut rig.sink);
    assert!(
        black_box(res).is_err(),
        "the fixture decoded cleanly under on_error: fail"
    );
    rig.sink.0
}

/// Install a real recorder, once per process.
///
/// Without one the `metrics` facade hands back no-op handles, so the drop
/// counters `Skip` increments cost nothing to increment and a case claiming to
/// measure the skip path would be measuring it with half of it missing. A
/// Prometheus recorder is what a deployed pipeline installs, and an increment
/// against it is the atomic add production pays.
///
/// Metric handles are resolved at build time (INV-8), so this has to run
/// before the builder is asked for them. Only counters are registered here, so
/// nothing claims a gauge series and INV-10 has nothing to arbitrate.
pub(crate) fn install_recorder() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        // Dropped rather than unwrapped: `Once` already rules out a second
        // install from here, and a host that somehow arrives with a global
        // recorder should degrade to no-op handles rather than abort the run.
        let _ = metrics::set_global_recorder(recorder);
    });
}

/// A rig whose decode backend, allocator and log limiter are already in the
/// state a running pipeline's would be in.
///
/// The warm pass is the single most load-bearing line in this file for the
/// `simd` arm. That backend keeps its mutable scratch buffer and the parser's
/// own reusable buffers in a thread-local, allocated lazily on first use —
/// which, because gungraun calls the benchmark function exactly once, would
/// otherwise be allocated *inside* the collected region and charged to the
/// case as if it were per-record cost. The heap counts say so directly:
/// compiling this pass out takes `large_string` from 7 allocated blocks to 15
/// and from 525 KB to 2.4 MB, and every other counted case gains between eight
/// and twenty-one blocks of the same scratch. The counted tier's reference
/// cases still carry that charge and are the control that makes it visible —
/// `simd` allocates 19 blocks against `serde_json`'s 8 on `single_typed`, and
/// reads between 12% and 22% above it depending on the architecture, where the
/// warmed cases differ by a single block. The block counts are a property of
/// the backend and hold everywhere; the percentages are not, and are quoted
/// only to say the charge is large relative to a small case.
///
/// Warming with the case's own payload rather than a token document is what
/// makes it complete: both the scratch copy and the parser's buffers are sized
/// to the input, so a small warm-up would leave the measured pass growing them
/// — a smaller version of the same defect.
///
/// The wall harness runs its own unmeasured warm-up before every region, so
/// the thread-local would come warm there whether or not this ran. Keeping the
/// pass is what makes the two tiers warm identically — and the harness calls
/// `setup` on the thread that later runs the routine, so the thread-local this
/// touches is the one the measured pass reads.
///
/// Two other things come warm with it, and both are wanted. The allocator has
/// already served and been returned the case's working set, so the measured
/// pass allocates against populated free lists rather than off the top of a
/// heap nothing has used, which is the state a pipeline decoding its millionth
/// payload is in. That is not automatically the cheaper of the two — for the
/// allocation-heavy cases a virgin heap is measurably cheaper, because a bump
/// off the top costs less than a bin lookup — which is the point: the
/// alternative is not a neutral measurement, it is the *first* payload a
/// process ever decodes, and no pipeline spends its life there.
///
/// And the rate limiter behind the skip warning has already seen whatever
/// poison the payload carries. It allows five events per window, so what the
/// warm pass leaves for the measured one depends on how much poison there is.
/// Under the counted tier, which decodes once more, both regimes appear: a
/// corpus dropping more than five records exhausts the window in the warm pass
/// and suppresses every drop on the lock-free fast path, and a corpus dropping
/// one spends one of the five and leaves the measured drop taking the mutex
/// and emitting an event. Under the wall tier every case converges on the
/// first of those within the warm-up, because a region running thousands of
/// iterations exhausts any window in its first few — which is the steady state
/// a pipeline is in, and is why a wall case and its counted twin can differ
/// here without either being wrong.
///
/// `labels` names the pipeline and component the drop counters carry. One
/// bench binary runs many cases in one process, so they are per-case rather
/// than per-binary: distinct label sets keep one case's counters from being
/// summed into another's.
pub(crate) fn warm_rig<F, D>(
    settings: JsonSettings,
    payload: Vec<u8>,
    expect: u64,
    labels: (&'static str, &'static str),
    build: impl FnOnce(&JsonDeserializerBuilder) -> D,
) -> Rig<D>
where
    F: RecFamily,
    D: Deserializer<F>,
{
    install_recorder();
    let builder = JsonDeserializerBuilder::from_settings(settings).with_metrics(labels.0, labels.1);
    let deser = build(&builder);
    // The receiver is dropped here, so when the last `AckRef` goes with the rig
    // the batch resolves into a disconnected channel and the send is discarded.
    // That happens in teardown, outside the collected region.
    let (ack, _ack_rx) = AckRef::test_pair();
    let mut rig = Rig {
        deser,
        payload,
        ack,
        sink: Sink(0),
        expect,
    };
    let raw = raw_payload(&rig.payload);
    // The result is deliberately ignored: an `on_error: fail` fixture returns
    // here the very error its case is about, and the warm pass is not where
    // that is asserted.
    let _ = rig.deser.deserialize(&raw, &rig.ack, &mut rig.sink);
    rig.sink.0 = 0;
    rig
}

/// Settings for a case that varies all three knobs rather than only the
/// framing.
pub(crate) fn full_settings(
    framing: JsonFraming,
    on_error: OnError,
    reject_duplicate_keys: bool,
) -> JsonSettings {
    JsonSettings {
        framing,
        on_error,
        reject_duplicate_keys,
    }
}

pub(crate) fn settings(framing: JsonFraming) -> JsonSettings {
    JsonSettings {
        framing,
        // Skip is the shipped default, so this is the path production takes.
        // It is not the cheaper of the two by construction: on valid input
        // Skip's error bookkeeping never runs, while ndjson under Fail buffers
        // every decoded payload before emitting any of them. Measuring the
        // default is the point; a Fail case would be a second shape, not a
        // stricter version of this one.
        on_error: OnError::Skip,
        // Off: the structural check parses each document a second time, which
        // would measure the guard rather than the decode.
        reject_duplicate_keys: false,
    }
}

/// An unwarmed rig with no metrics, for a case whose point is the decode
/// alone.
pub(crate) fn rig<D>(
    framing: JsonFraming,
    payload: Vec<u8>,
    expect: u64,
    build: impl FnOnce(&JsonDeserializerBuilder) -> D,
) -> Rig<D> {
    let builder = JsonDeserializerBuilder::from_settings(settings(framing));
    let deser = build(&builder);
    // The receiver is dropped here, so when the last `AckRef` goes with the rig
    // the batch resolves into a disconnected channel and the send is discarded.
    // That happens in teardown, outside the collected region, which is why the
    // ack path costs the measurement nothing beyond the per-record clone.
    let (ack, _ack_rx) = AckRef::test_pair();
    Rig {
        deser,
        payload,
        ack,
        sink: Sink(0),
        expect,
    }
}

/// A warmed rig decoding records of one type under one framing and policy.
pub(crate) fn batch_rig<P>(
    framing: JsonFraming,
    on_error: OnError,
    payload: Vec<u8>,
    expect: u64,
    labels: (&'static str, &'static str),
) -> Rig<spate_json::JsonSerdeDeserializer<P>>
where
    P: DeserializeOwned + Send + 'static,
{
    warm_rig::<Owned<P>, _>(
        full_settings(framing, on_error, false),
        payload,
        expect,
        labels,
        |b| b.build_serde::<P>(),
    )
}

/// A warmed rig decoding one document of an arbitrary shape into a value,
/// optionally through the duplicate-key guard.
pub(crate) fn shape_rig(
    payload: Vec<u8>,
    guard: bool,
    expect: u64,
    labels: (&'static str, &'static str),
) -> Rig<spate_json::JsonValueDeserializer> {
    warm_rig::<Owned<Value>, _>(
        full_settings(JsonFraming::Single, OnError::Skip, guard),
        payload,
        expect,
        labels,
        JsonDeserializerBuilder::build_value,
    )
}
