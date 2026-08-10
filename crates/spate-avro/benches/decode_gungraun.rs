//! Instruction counts for the Avro decode paths (gungraun).
//!
//! A subset of what `benches/decode.rs` times, counted instead of timed:
//! callgrind's instruction count is deterministic, so it compares across the
//! shared CI runners where a wall-clock number is only noise. DHAT runs
//! alongside callgrind in the same invocation, so every case also reports
//! deterministic heap counts — allocated blocks, bytes, and the t-gmax peak.
//! Not the full criterion matrix — callgrind runs the workload under
//! emulation, and the published comparison corpus (200 batches, 20,000 events
//! per iteration) is far too large to emulate.
//!
//! # `decode` — one payload, three backends
//!
//! - `flat_record` × `value` / `serde_typed` / `datum_typed` — the three
//!   decode paths over one 15-field record. This is the comparison the crate
//!   exists to settle, and instructions attribute it without a timer; the
//!   heap counts attribute the `Value`-tree allocations that separate the
//!   dynamically-typed path from the single-pass one.
//! - `batch50` × `value` / `datum_typed` — the same two ends of that spread
//!   over an array-shaped record, where the dynamically-typed path's
//!   per-element allocation concentrates. The serde arm is left out because
//!   it sits between the two and moves with them.
//! - `flat_record_malformed` × `value` — the flat record truncated
//!   mid-field. Skip and Fail both pay decode-until-error plus the `Err`
//!   for every bad record (INV-7 admits no other policy), so this count is
//!   the per-record price of a poison-pill storm.
//!
//! # `confluent`, `evolution`, `shapes`, `errors` — one poll batch
//!
//! Those six cases decode a single payload. The four groups below decode a
//! batch of them (`corpora::BATCH`), because what they measure is a
//! *steady state* rather than a first touch: the schema memo, the reader
//! schema and the compiled decode spec are all state the deserializer carries
//! across a poll batch, and a one-payload region only ever measures the walk
//! that populates them.
//!
//! - **`confluent`** — the production default framing, parameterized by what
//!   the schema cache answers. `cached_schema` is the steady state: the memo
//!   holds the id, so `SchemaCache::lookup` never touches the shared lock —
//!   the memo exists precisely because taking a read lock per payload
//!   ping-ponged that lock's cache line across pinned pipeline threads.
//!   `unknown_schema_id` and `poisoned_schema_id` are the two states that do
//!   *not* memo-hit, so each payload refreshes the snapshot under the read
//!   lock and clones the map's `Arc`: they are the cold-lookup regime, and
//!   they are also what a storm of unregistered or unusable ids costs.
//!   Read `unknown_schema_id`'s **heap** numbers with one correction: its
//!   fetch requests queue on a channel whose fetcher never runs, so the
//!   queue's blocks accumulate for the whole corpus where a live pipeline's
//!   fetcher would be draining them. The instruction count is unaffected —
//!   the send is the same send either way — but the DHAT peak is an
//!   overstatement, and only for that case.
//! - **`evolution`** — the only path that applies a `reader_schema`, over one
//!   writer schema and three readers that isolate one resolution rule each.
//!   `writer_schema_only` is the same corpus with no reader schema at all, so
//!   the resolution term is the difference between it and the other three
//!   rather than an absolute anybody has to interpret alone. There is no
//!   alias case: the resolution the two-pass path delegates to matches
//!   fields by name and never consults a reader field's aliases, which
//!   `tests/bench_fixtures.rs` pins. The three resolving readers are also the
//!   cases whose counts are not bit-reproducible across processes — see
//!   `corpora`'s note on what a deterministic corpus does not pin.
//! - **`shapes`** — decode shapes with bespoke handling in the single-pass
//!   path and none of which any other case reaches: a map, an enum, a fixed,
//!   both decimal backings, uuid, date and the two timestamp precisions; then
//!   a recursive named reference, which is the only shape that makes the walk
//!   resolve a `Schema::Ref` and the only one that drives the depth guard
//!   past a couple of levels.
//! - **`errors`** — the malformed-datum fixture through the two paths the
//!   `decode` group does not drive it through, plus a stale single-object
//!   fingerprint. Under Skip or Fail this is the steady-state cost of a
//!   poison-pill storm, and the three differ in where the payload dies:
//!   inside the single-pass walk's bounds checks, inside the two-pass
//!   `Value` build, and before either, at the framing.
//!
//! Needs valgrind and a same-version `gungraun-runner`, neither of which
//! exists on every developer machine: run it with `make bench-gungraun`.

// `library_benchmark` and `library_benchmark_group` expand to public modules,
// functions and constants of their own, none of which carry documentation, so
// the workspace's `missing_docs` lint has nothing to bite on here.
#![expect(missing_docs, reason = "items are generated by gungraun macros")]

use gungraun::{Dhat, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main};
use spate_avro::{
    AvroDatumDeserializer, AvroDeserializerBuilder, AvroMode, AvroSerdeDeserializer, AvroSettings,
    AvroValueDeserializer, RegistrySection, SchemaSource,
};
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, Owned, RecFamily};
use spate_core::record::{PartitionId, RawPayload};
use std::hint::black_box;
use std::time::Duration;

#[path = "support/corpora.rs"]
mod corpora;
#[path = "support/orders.rs"]
mod orders;
#[path = "support/registry_stub.rs"]
mod registry_stub;

use orders::Sink;
use registry_stub::{StubRegistry, Warm};

/// The negative-cache TTL every Confluent rig is built with.
///
/// Long enough that a negative entry cannot expire part-way through an
/// emulated walk of the corpus. If it did, the payloads after the expiry
/// would take the `Missing` arm instead of the `Failed` one and the count
/// would depend on how long valgrind took — which is the one thing an
/// instruction count is supposed to be free of.
const NEGATIVE_TTL: Duration = Duration::from_secs(3_600);

/// One deserializer with the bytes it decodes.
///
/// The payload is owned rather than pre-wrapped in a `RawPayload`, because a
/// `RawPayload` borrows it and a benchmark argument has to be a single owned
/// value. Building the wrapper is a handful of stores inside the measured
/// region; it is identical across the cases, so it cancels in any comparison.
struct Rig<D> {
    deser: D,
    payload: Vec<u8>,
    ack: AckRef,
    sink: Sink,
    // The builder keeps this runtime's handle for the registry fetcher. Raw
    // mode never uses it, but the handle must not outlive the runtime.
    _rt: tokio::runtime::Runtime,
}

/// The batch equivalent: one deserializer with a whole poll batch's worth of
/// payloads, and whether the corpus is one every payload decodes or one every
/// payload fails.
struct BatchRig<D> {
    deser: D,
    payloads: Vec<Vec<u8>>,
    decodes: bool,
    ack: AckRef,
    sink: Sink,
    _rt: tokio::runtime::Runtime,
}

/// A free function rather than a `Rig` method: borrowing the whole rig would
/// collide with the mutable borrows of `deser` and `sink`.
fn raw_payload(bytes: &[u8]) -> RawPayload<'_> {
    RawPayload {
        bytes,
        key: None,
        partition: PartitionId(0),
        offset: 1,
        timestamp_ms: 0,
    }
}

/// The measured work: one payload through one deserializer.
fn decode_once<F, D>(rig: &mut Rig<D>)
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let raw = raw_payload(&rig.payload);
    rig.deser
        .deserialize(black_box(&raw), &rig.ack, &mut rig.sink)
        .unwrap();
}

/// The measured error path: one malformed payload through one deserializer.
/// The stage applies the `ErrorPolicy` after `deserialize` returns, so
/// decode-until-error plus building the `Err` is the cost shared by Skip and
/// Fail. The assert mirrors `decode_once`'s `unwrap`: if the fixture ever
/// decodes cleanly, the bench panics instead of quietly counting the happy
/// path.
fn decode_once_err<F, D>(rig: &mut Rig<D>)
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let raw = raw_payload(&rig.payload);
    let res = rig
        .deser
        .deserialize(black_box(&raw), &rig.ack, &mut rig.sink);
    assert!(black_box(res).is_err(), "malformed fixture decoded cleanly");
}

/// The measured work for every batch case: the whole corpus through one
/// deserializer.
///
/// **`#[inline(never)]` is a measurement property, not a style choice.**
/// Collection is bounded by a callgrind toggle on the glob
/// `*::__gungraun_wrapper_mod::*`, which the macro wraps each benchmark
/// function in — and a toggle *flips* collection rather than forcing it on.
/// A loop written inline in the benchmark function can be reshaped by the
/// optimizer into a symbol under that same module path, and entering it turns
/// collection back **off**: the case then reports a plausible number that is
/// pure allocator bookkeeping and does not move when the corpus size does.
/// A named, never-inlined function at the crate root cannot be outlined
/// under the wrapper's path, so the walk is always inside the region.
///
/// The check that settles it for a new case is arithmetic, not inspection:
/// halve [`corpora::BATCH`], re-run, and confirm the count halves. A count
/// that does not move with the corpus is measuring nothing, however plausible
/// it looks.
///
/// `tests/bench_fixtures.rs` pins every corpus, and the assert below pins the
/// outcome: a fixture that started decoding when it used to fail (or the
/// reverse) fails the bench rather than silently re-baselining it.
#[inline(never)]
fn decode_batch<F, D>(rig: &mut BatchRig<D>)
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let BatchRig {
        deser,
        payloads,
        decodes,
        ack,
        sink,
        ..
    } = rig;
    let mut decoded = 0usize;
    for payload in payloads.iter() {
        let raw = raw_payload(payload);
        if deser.deserialize(black_box(&raw), ack, sink).is_ok() {
            decoded += 1;
        }
    }
    assert_eq!(
        decoded,
        if *decodes { payloads.len() } else { 0 },
        "the corpus no longer resolves the way this case measures"
    );
}

/// Build a builder over a fixed inline schema in raw mode — no registry, so
/// nothing in the measured path touches the network.
fn rig<D>(
    schema: &str,
    payload: Vec<u8>,
    build: impl FnOnce(&AvroDeserializerBuilder) -> D,
) -> Rig<D> {
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(schema)),
        ..AvroSettings::default()
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    let deser = build(&builder);
    // The rig holds the only `AckRef`, so the batch never resolves and no
    // message is ever sent — the receiver has nothing to keep alive.
    let (ack, _ack_rx) = AckRef::test_pair();
    Rig {
        deser,
        payload,
        ack,
        sink: Sink(0),
        _rt: rt,
    }
}

/// The batch equivalent, taking whole settings so a case can choose its mode
/// and its reader schema. The runtime is never driven, so a Confluent rig
/// built this way never performs a fetch.
fn batch_rig<D>(
    settings: &AvroSettings,
    payloads: Vec<Vec<u8>>,
    decodes: bool,
    build: impl FnOnce(&AvroDeserializerBuilder) -> D,
) -> BatchRig<D> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(settings, rt.handle()).unwrap();
    let deser = build(&builder);
    let (ack, _ack_rx) = AckRef::test_pair();
    BatchRig {
        deser,
        payloads,
        decodes,
        ack,
        sink: Sink(0),
        _rt: rt,
    }
}

/// Raw-mode settings over one inline schema, optionally resolved into a
/// reader schema.
fn raw_settings(schema: &str, reader: Option<&str>) -> AvroSettings {
    AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(schema)),
        reader_schema: reader.map(SchemaSource::inline),
        ..AvroSettings::default()
    }
}

fn flat_value_rig() -> Rig<AvroValueDeserializer> {
    rig(orders::SCHEMA, orders::order_datum(), |b| {
        b.build_value().expect("value builder")
    })
}

fn flat_serde_rig() -> Rig<AvroSerdeDeserializer<orders::Order>> {
    rig(orders::SCHEMA, orders::order_datum(), |b| {
        b.build_serde::<orders::Order>().expect("serde builder")
    })
}

fn flat_datum_rig() -> Rig<AvroDatumDeserializer<Owned<orders::Order>>> {
    rig(orders::SCHEMA, orders::order_datum(), |b| {
        b.build_serde_datum::<orders::Order>()
            .expect("datum builder")
    })
}

fn flat_value_malformed_rig() -> Rig<AvroValueDeserializer> {
    rig(orders::SCHEMA, orders::malformed_datum(), |b| {
        b.build_value().expect("value builder")
    })
}

fn batch_value_rig() -> Rig<AvroValueDeserializer> {
    rig(orders::BATCH_SCHEMA, orders::batch_datum(), |b| {
        b.build_value().expect("value builder")
    })
}

fn batch_datum_rig() -> Rig<AvroDatumDeserializer<Owned<orders::PlacedOrder>>> {
    rig(orders::BATCH_SCHEMA, orders::batch_datum(), |b| {
        b.build_serde_datum::<orders::PlacedOrder>()
            .expect("datum builder")
    })
}

// ---------------------------------------------------------------------------
// Confluent framing and schema-cache lookup
// ---------------------------------------------------------------------------

fn confluent_settings(url: String) -> AvroSettings {
    AvroSettings {
        mode: AvroMode::Confluent,
        registry: Some(RegistrySection {
            url,
            username: None,
            password: None,
        }),
        negative_cache_ttl: NEGATIVE_TTL,
        ..AvroSettings::default()
    }
}

/// A Confluent rig whose cache the stub registry has already answered for.
/// `want` decides which answer: the schema itself, or the 404 that
/// negative-caches the id.
fn warmed_confluent_rig(id: u32, want: Warm) -> BatchRig<AvroValueDeserializer> {
    let stub = StubRegistry::start(corpora::READY_ID, orders::SCHEMA);
    let settings = confluent_settings(stub.url());
    // `enable_all`: this runtime does drive the fetcher, but only inside the
    // warm-up below — never again once the rig is handed over.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    let mut deser = builder.build_value().expect("value builder");
    let payloads = corpora::confluent_orders(id);
    let (ack, _ack_rx) = AckRef::test_pair();
    let mut sink = Sink(0);
    registry_stub::warm(&mut deser, &rt, &ack, &mut sink, &payloads[0], want);
    stub.shutdown();
    BatchRig {
        deser,
        payloads,
        decodes: matches!(want, Warm::Ready),
        ack,
        // A fresh counter: the warm-up emitted a record the measured walk
        // did not.
        sink: Sink(0),
        _rt: rt,
    }
}

fn confluent_cached_rig() -> BatchRig<AvroValueDeserializer> {
    warmed_confluent_rig(corpora::READY_ID, Warm::Ready)
}

fn confluent_poisoned_rig() -> BatchRig<AvroValueDeserializer> {
    warmed_confluent_rig(corpora::POISON_ID, Warm::Poisoned)
}

/// The `Lookup::Missing` corpus, and the one Confluent rig that needs no
/// stub at all: its runtime is never driven, so the fetcher task never polls,
/// the request it queues is never read, and the id stays missing for every
/// payload. Nothing here — setup included — opens a socket.
fn confluent_unknown_rig() -> BatchRig<AvroValueDeserializer> {
    let settings = confluent_settings("http://127.0.0.1:1".to_owned());
    batch_rig(
        &settings,
        corpora::confluent_orders(corpora::UNKNOWN_ID),
        false,
        |b| b.build_value().expect("value builder"),
    )
}

// ---------------------------------------------------------------------------
// Schema evolution
// ---------------------------------------------------------------------------

fn evolution_rig(reader: Option<&str>) -> BatchRig<AvroSerdeDeserializer<corpora::Evolved>> {
    let settings = raw_settings(corpora::EVENT_WRITER, reader);
    batch_rig(&settings, corpora::event_datums(), true, |b| {
        b.build_serde::<corpora::Evolved>().expect("serde builder")
    })
}

// ---------------------------------------------------------------------------
// Schema shapes
// ---------------------------------------------------------------------------

fn shapes_rig() -> BatchRig<AvroDatumDeserializer<Owned<corpora::Shapes>>> {
    let settings = raw_settings(&corpora::shapes_schema(), None);
    batch_rig(&settings, corpora::shapes_datums(), true, |b| {
        b.build_serde_datum::<corpora::Shapes>()
            .expect("datum builder")
    })
}

fn recursive_rig() -> BatchRig<AvroDatumDeserializer<Owned<corpora::LongList>>> {
    let settings = raw_settings(corpora::LONG_LIST, None);
    batch_rig(&settings, corpora::long_list_datums(), true, |b| {
        b.build_serde_datum::<corpora::LongList>()
            .expect("datum builder")
    })
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

fn truncated_datum_rig() -> BatchRig<AvroDatumDeserializer<Owned<orders::Order>>> {
    let settings = raw_settings(orders::SCHEMA, None);
    batch_rig(&settings, corpora::truncated_order_datums(), false, |b| {
        b.build_serde_datum::<orders::Order>()
            .expect("datum builder")
    })
}

fn truncated_serde_rig() -> BatchRig<AvroSerdeDeserializer<orders::Order>> {
    let settings = raw_settings(orders::SCHEMA, None);
    batch_rig(&settings, corpora::truncated_order_datums(), false, |b| {
        b.build_serde::<orders::Order>().expect("serde builder")
    })
}

fn stale_fingerprint_rig() -> BatchRig<AvroValueDeserializer> {
    let settings = AvroSettings {
        mode: AvroMode::SingleObject,
        schema: Some(SchemaSource::inline(orders::SCHEMA)),
        ..AvroSettings::default()
    };
    batch_rig(&settings, corpora::stale_single_object(), false, |b| {
        b.build_value().expect("value builder")
    })
}

// Each case returns its rig rather than dropping it: a value moved into the
// benchmark function is dropped inside the collected region, and tearing down
// a runtime would swamp the decode it is meant to measure. A `///` comment is
// a `#[doc]` attribute, which `#[library_benchmark]` rejects.
#[library_benchmark]
#[bench::flat_record(flat_value_rig())]
#[bench::batch50(batch_value_rig())]
fn decode_value(mut rig: Rig<AvroValueDeserializer>) -> Rig<AvroValueDeserializer> {
    decode_once(&mut rig);
    rig
}

#[library_benchmark]
#[bench::flat_record(flat_serde_rig())]
fn decode_serde_typed(
    mut rig: Rig<AvroSerdeDeserializer<orders::Order>>,
) -> Rig<AvroSerdeDeserializer<orders::Order>> {
    decode_once(&mut rig);
    rig
}

#[library_benchmark]
#[bench::flat_record(flat_datum_rig())]
fn decode_datum_typed(
    mut rig: Rig<AvroDatumDeserializer<Owned<orders::Order>>>,
) -> Rig<AvroDatumDeserializer<Owned<orders::Order>>> {
    decode_once(&mut rig);
    rig
}

#[library_benchmark]
#[bench::flat_record_malformed(flat_value_malformed_rig())]
fn decode_value_malformed(mut rig: Rig<AvroValueDeserializer>) -> Rig<AvroValueDeserializer> {
    decode_once_err(&mut rig);
    rig
}

#[library_benchmark]
#[bench::batch50(batch_datum_rig())]
fn decode_batch_datum_typed(
    mut rig: Rig<AvroDatumDeserializer<Owned<orders::PlacedOrder>>>,
) -> Rig<AvroDatumDeserializer<Owned<orders::PlacedOrder>>> {
    decode_once(&mut rig);
    rig
}

#[library_benchmark]
#[bench::cached_schema(confluent_cached_rig())]
#[bench::unknown_schema_id(confluent_unknown_rig())]
#[bench::poisoned_schema_id(confluent_poisoned_rig())]
fn decode_confluent(mut rig: BatchRig<AvroValueDeserializer>) -> BatchRig<AvroValueDeserializer> {
    decode_batch(&mut rig);
    rig
}

#[library_benchmark]
#[bench::writer_schema_only(evolution_rig(None))]
#[bench::reordered_fields(evolution_rig(Some(corpora::EVENT_REORDERED)))]
#[bench::promoted_numerics(evolution_rig(Some(corpora::EVENT_PROMOTED)))]
#[bench::added_default_field(evolution_rig(Some(corpora::EVENT_DEFAULTED)))]
fn decode_resolved(
    mut rig: BatchRig<AvroSerdeDeserializer<corpora::Evolved>>,
) -> BatchRig<AvroSerdeDeserializer<corpora::Evolved>> {
    decode_batch(&mut rig);
    rig
}

#[library_benchmark]
#[bench::logical_types(shapes_rig())]
fn decode_shapes(
    mut rig: BatchRig<AvroDatumDeserializer<Owned<corpora::Shapes>>>,
) -> BatchRig<AvroDatumDeserializer<Owned<corpora::Shapes>>> {
    decode_batch(&mut rig);
    rig
}

#[library_benchmark]
#[bench::recursive_refs(recursive_rig())]
fn decode_recursive(
    mut rig: BatchRig<AvroDatumDeserializer<Owned<corpora::LongList>>>,
) -> BatchRig<AvroDatumDeserializer<Owned<corpora::LongList>>> {
    decode_batch(&mut rig);
    rig
}

#[library_benchmark]
#[bench::truncated_datum(truncated_datum_rig())]
fn decode_malformed_datum_typed(
    mut rig: BatchRig<AvroDatumDeserializer<Owned<orders::Order>>>,
) -> BatchRig<AvroDatumDeserializer<Owned<orders::Order>>> {
    decode_batch(&mut rig);
    rig
}

#[library_benchmark]
#[bench::truncated_datum(truncated_serde_rig())]
fn decode_malformed_serde_typed(
    mut rig: BatchRig<AvroSerdeDeserializer<orders::Order>>,
) -> BatchRig<AvroSerdeDeserializer<orders::Order>> {
    decode_batch(&mut rig);
    rig
}

#[library_benchmark]
#[bench::stale_fingerprint(stale_fingerprint_rig())]
fn decode_single_object(
    mut rig: BatchRig<AvroValueDeserializer>,
) -> BatchRig<AvroValueDeserializer> {
    decode_batch(&mut rig);
    rig
}

library_benchmark_group!(
    name = decode;
    benchmarks =
        decode_value,
        decode_serde_typed,
        decode_datum_typed,
        decode_value_malformed,
        decode_batch_datum_typed
);

library_benchmark_group!(name = confluent; benchmarks = decode_confluent);

library_benchmark_group!(name = evolution; benchmarks = decode_resolved);

library_benchmark_group!(name = shapes; benchmarks = decode_shapes, decode_recursive);

library_benchmark_group!(
    name = errors;
    benchmarks =
        decode_malformed_datum_typed,
        decode_malformed_serde_typed,
        decode_single_object
);

// DHAT is scoped as an extra tool rather than a callgrind argument: the
// callgrind invocation — and so every `Ir` baseline — is bit-identical with
// and without it. `--num-callers=500` (the maximum) keeps allocation stacks
// deep enough that heap blocks attribute to the decode under measurement
// rather than to whichever frame the default depth of 4 happens to cut at.
main!(
    config = LibraryBenchmarkConfig::default().tool(Dhat::with_args(["--num-callers=500"])),
    // Bracketed: with a `config`, `main!` takes more than one group only as
    // an array — the bare comma-separated form is a single-group spelling and
    // is rejected outright rather than silently measuring the first.
    library_benchmark_groups = [decode, confluent, evolution, shapes, errors]
);
