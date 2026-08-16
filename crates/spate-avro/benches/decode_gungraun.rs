//! Instruction counts for the Avro decode paths (gungraun).
//!
//! A subset of the cases `benches/decode_paths_wall.rs` times, over the same
//! regions in `support/decode_rig.rs`. The batch corpus is left out: 20,000
//! lines per iteration is more than this tier runs in reasonable time. DHAT
//! reports alongside, so each case also carries heap counts.
//!
//! # `decode`: one payload, three backends
//!
//! - `flat_record` × `value` / `serde_typed` / `datum_typed` — the three
//!   decode paths over one 15-field record. This is the comparison the crate
//!   is built to settle, and instructions attribute it without a timer; the
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
//! # `confluent`, `evolution`, `shapes`, `errors`: one poll batch
//!
//! Those six cases decode a single payload. The four groups below decode a
//! batch of them (`corpora::BATCH`), because what they measure is a
//! *steady state* rather than a first touch: the schema memo, the reader
//! schema, the held datum reader and the compiled decode spec are all state
//! the deserializer carries across a poll batch, and a one-payload region only
//! ever measures the walk that populates them.
//!
//! A single-payload region on the value or serde path builds a held reader and
//! decodes through it once, so it prices the build. The wall tier builds one
//! during warm-up, outside the recorded region, and walks a whole corpus per
//! iteration, so it prices only the steady state that build buys. Holding a
//! reader, or ceasing to, therefore moves the two tiers in opposite
//! directions; a change to what a build costs moves this tier alone, and a
//! change to what a decode costs moves both.
//!
//! - **`confluent`** — the production default framing, parameterized by what
//!   the schema cache answers. `cached_schema` is the steady state: the memo
//!   holds the id, so `SchemaCache::lookup` never takes the shared lock.
//!   `unknown_schema_id` and `poisoned_schema_id` are the two states that do
//!   *not* memo-hit, so each payload refreshes the snapshot under the read
//!   lock and clones the map's `Arc`: they are the cold-lookup regime, and
//!   they are also what a storm of unregistered or unusable ids costs.
//!   Read `unknown_schema_id`'s **heap** numbers with one correction: its
//!   fetch requests queue on a channel whose fetcher never runs, so the
//!   queue's blocks accumulate for the whole corpus where a live pipeline's
//!   fetcher would be draining them. The instruction count is unaffected,
//!   since the send is the same send either way, but the DHAT peak is an
//!   overstatement, and only for that case.
//! - **`evolution`** — the only path that applies a `reader_schema`, over one
//!   writer schema and three readers that isolate one resolution rule each.
//!   `writer_schema_only` is the same corpus with no reader schema at all, so
//!   the resolution term is the difference between it and the other three
//!   rather than an absolute anybody has to interpret alone. There is no
//!   alias case: the resolution the two-pass path delegates to matches
//!   fields by name and never consults a reader field's aliases, which
//!   `tests/bench_fixtures.rs` pins. The three resolving readers are also the
//!   cases whose counts are not bit-reproducible across processes; see
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
//! Run it with `make bench-gungraun`; the runner must match the `gungraun`
//! version in `Cargo.toml`.

// `library_benchmark` and `library_benchmark_group` expand to public modules,
// functions and constants of their own, none of which carry documentation, so
// the workspace's `missing_docs` lint has nothing to bite on here.
#![expect(missing_docs, reason = "items are generated by gungraun macros")]

use gungraun::{Dhat, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main};
use spate_avro::{AvroDatumDeserializer, AvroSerdeDeserializer, AvroValueDeserializer};
use spate_core::deser::Owned;

#[path = "support/batches.rs"]
mod batches;
#[path = "support/corpora.rs"]
mod corpora;
#[path = "support/decode_rig.rs"]
mod decode_rig;
#[path = "support/orders.rs"]
mod orders;
#[path = "support/registry_stub.rs"]
mod registry_stub;

use decode_rig::{
    BatchRig, Rig, batch_datum_rig, batch_value_rig, confluent_cached_rig, confluent_poisoned_rig,
    confluent_unknown_rig, decode_batch, decode_once, decode_once_err, evolution_rig,
    flat_datum_rig, flat_serde_rig, flat_value_malformed_rig, flat_value_rig, recursive_rig,
    shapes_rig, stale_fingerprint_rig, truncated_datum_rig, truncated_serde_rig,
};

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
// callgrind invocation, and so every `Ir` baseline, is bit-identical with
// and without it. `--num-callers=500` (the maximum) keeps allocation stacks
// deep enough that heap blocks attribute to the decode under measurement
// rather than to whichever frame the default depth of 4 happens to cut at.
main!(
    config = LibraryBenchmarkConfig::default().tool(Dhat::with_args(["--num-callers=500"])),
    // Bracketed: with a `config`, `main!` takes more than one group only as
    // an array; the bare comma-separated form is a single-group spelling and
    // is rejected outright rather than silently measuring the first.
    library_benchmark_groups = [decode, confluent, evolution, shapes, errors]
);
