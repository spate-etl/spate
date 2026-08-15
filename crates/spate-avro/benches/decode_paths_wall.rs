//! Wall time for this crate's decode paths, and for the parser underneath
//! them.
//!
//! What a case measures is the whole of `Deserializer::deserialize` over a
//! poll batch's worth of payloads: the framing, the schema lookup, the decode,
//! the record emission, and the per-record error isolation that comes with it.
//! The `lib_*` cases call `apache-avro` directly over the same bytes.
//!
//! The framework cases reach their work through `decode_batch` or
//! `decode_and_flatten` in `support/decode_rig.rs`, which `decode_gungraun.rs`
//! also drives. Where both tiers cover one rig, a counted regression and a
//! wall-clock one describe one region. The rigs they cover overlap in part: the
//! counted tier decodes a single payload where these cases walk a corpus, and
//! it drives none of the batch, mixed-id, single-object or flatten rigs. Some
//! cases here have no counted twin.
//!
//! ## Groups
//!
//! Case ids carry a group prefix. `--filter` matches a substring against the
//! case ids of every wall target in the workspace (issue #237), so each prefix
//! below is one no other crate's ids contain:
//!
//! | Prefix | What a change to it moves |
//! |---|---|
//! | `raw_` | the decode paths in raw mode: three over a flat record, and over a batch those three plus the borrowed path and two decode-and-flatten arms |
//! | `mode_` | the wire framings a deployed pipeline runs, and for the Confluent pair the schema cache behind them |
//! | `resolved_` | reader-schema resolution, the only path that applies a reader schema |
//! | `shapes_` | logical types, a map, an enum, a fixed, and a recursive named reference |
//! | `err_` | the failure paths, which a stream carrying schema drift runs constantly |
//! | `lib_` | `apache-avro` over the same bytes |
//!
//! ```sh
//! make bench-ab REF=main FILTER=raw_
//! ```
//!
//! ## What each corpus is sized for
//!
//! One iteration walks a whole corpus: [`corpora::BATCH`] records for the
//! per-record corpora, [`batches::LINES`] lines for the batch ones. A single
//! Avro record decodes in a few hundred nanoseconds, too little to time alone.
//!
//! ## Which cases carry a floor
//!
//! A floor is the library call a framework case makes, over the same bytes, so
//! that a regression in the dependency is not read as one in the framework. It
//! applies where the framework case reaches `apache-avro` to decode:
//!
//! | Framework case | Floor |
//! |---|---|
//! | `raw_flat15_value` | `lib_flat15_value_held` |
//! | `raw_batch_value` | `lib_batch_value_held` |
//! | `resolved_reordered` | `lib_resolved_held` |
//! | `raw_flat15_serde`, `raw_batch_serde` | none; the library offers no held-reader route that also reads a `T` out of the `Value` |
//! | `raw_flat15_datum`, `raw_batch_datum`, `raw_batch_datum_borrowed` | none; see below |
//! | `raw_batch_value_flatten`, `raw_batch_datum_flatten` | none; read each against its decode-only partner above, the same corpus without the flatten |
//! | `resolved_writer_only`, `resolved_promoted`, `resolved_defaulted` | none; read against `lib_resolved` and `resolved_reordered`, one reader schema at a time |
//! | `mode_*`, `shapes_*`, `err_*` | none; they price this crate's framing, schema cache and error isolation, which the library has no counterpart for. Read them against `raw_flat15_value` over the same records |
//!
//! The `*_held` floors are the partners, not the free-function ones: the crate
//! holds a reader per writer schema id, so a floor calling `from_avro_datum`
//! resolves per payload where its partner does not, and would read as the
//! framework beating its own floor.
//!
//! Each floor runs a near-subset of its partner, which keeps the margin
//! between them small and a relative reading of that margin noisy. A library
//! regression moves both. A framework regression moves the framework case
//! alone.
//!
//! ## The `lib_*` cases that are not floors
//!
//! The `raw_*_datum` cases have no floor. `AvroDatumDeserializer` walks the
//! datum itself and calls no library decoder, so a regression inside
//! `apache-avro`'s decoder cannot move them.
//!
//! `lib_flat15_read_deser` and `lib_batch_read_deser` are a **competitor
//! baseline** for those cases rather than a floor: the library's own
//! single-pass route to a `T`, over the same bytes, read against the crate's.
//! Both amortize schema resolution — the crate resolves once per compiled
//! schema, these once per corpus — so the margin between them is decode.
//!
//! Read them against `lib_*_typed` only with that in mind: the two-pass route
//! resolves per call, so a margin taken there mixes decode with resolution.
//! The `*_held` pairs below price the resolution separately.
//!
//! `read_deser` takes `T: DeserializeOwned` from an `impl Read`, so it
//! allocates every string and bytes field and cannot borrow into the payload.
//! `raw_batch_datum_borrowed` has no counterpart for that reason.
//!
//! ## The `*_held` cases
//!
//! `lib_flat15_value_held`, `lib_batch_value_held` and `lib_resolved_held`
//! run the same work as `lib_flat15_value`, `lib_batch_value` and
//! `lib_resolved` through a `GenericDatumReader` built in setup rather than
//! the free function that resolves the writer schema's named types on every
//! call. Each pair prices that resolution. The `*_held` half is what this
//! crate does; the free-function half is what it did before it held a reader
//! per schema id.
//!
//! ## Two metrics with a narrower meaning than their names
//!
//! The `wire_*` cases in `wire_wall.rs` and the three non-decoding `err_` cases
//! return before reading most of the corpus, so their `bytes_per_s` is a rate
//! of disposal.
//!
//! `err_unknown_schema_id` queues a registry fetch per payload on a channel
//! whose fetcher is never polled, so its `peak_rss_bytes` covers that queue as
//! well as the error path. Both legs queue alike, so a comparison holds.

#![expect(
    deprecated,
    reason = "the lib_ floors measure the library's own datum functions"
)]

use std::cell::RefCell;

use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use spate_avro::{AvroDatumDeserializer, AvroSerdeDeserializer, AvroValue, AvroValueDeserializer};
use spate_bench::{Corpus, Suite, bench_main};
use spate_core::deser::{Deserializer, Owned, RecFamily};

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
    BatchRig, FlattenRig, TypedFlattenSink, ValueFlattenSink, confluent_cached_rig,
    confluent_mixed_rig, confluent_poisoned_rig, confluent_unknown_rig, decode_and_flatten,
    decode_batch, evolution_rig, flat_corpus_datum_rig, flat_corpus_serde_rig,
    flat_corpus_value_rig, order_batch_borrowed_rig, order_batch_datum_rig, order_batch_serde_rig,
    order_batch_value_rig, recursive_rig, shapes_rig, single_object_rig, stale_fingerprint_rig,
    truncated_datum_rig, truncated_serde_rig, truncated_value_rig,
};

/// Records in the per-record corpora, the extent those cases declare.
const RECORDS: u64 = corpora::BATCH as u64;

fn suite() -> Suite {
    let s = spate_bench::suite("spate-avro");
    let s = raw_decode_cases(s);
    let s = batch_cases(s);
    let s = mode_cases(s);
    let s = resolved_cases(s);
    let s = shapes_cases(s);
    let s = error_cases(s);
    floor_cases(s)
}

bench_main!(suite);

// ---------------------------------------------------------------------------
// Case builders
// ---------------------------------------------------------------------------

/// Absorb every byte the region reads, so the digest proves both legs decoded
/// the same corpus.
fn absorb(corpus: &mut Corpus, payloads: &[Vec<u8>]) {
    for payload in payloads {
        corpus.absorb("payload", payload);
    }
}

fn corpus_bytes(payloads: &[Vec<u8>]) -> u64 {
    payloads.iter().map(|p| p.len() as u64).sum()
}

/// A case that walks a whole corpus through one deserializer.
fn batch_case<F, D>(suite: Suite, id: &'static str, items: u64, make: fn() -> BatchRig<D>) -> Suite
where
    F: RecFamily + 'static,
    D: Deserializer<F> + 'static,
{
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = make();
                absorb(corpus, &rig.payloads);
                RefCell::new(rig)
            },
            move |b, rig: &RefCell<BatchRig<D>>| {
                b.iter(|| decode_batch::<F, D>(&mut rig.borrow_mut()));
            },
        )
        .items(items)
        .bytes_of(|rig: &RefCell<BatchRig<D>>| corpus_bytes(&rig.borrow().payloads))
        .done()
}

/// A case that decodes and then flattens, the `flat_map` stage minus the
/// engine around it.
fn flatten_case<F, D, S>(suite: Suite, id: &'static str, make: fn() -> FlattenRig<D, S>) -> Suite
where
    F: RecFamily + 'static,
    D: Deserializer<F> + 'static,
    S: for<'buf> spate_core::deser::EmitRecord<'buf, F::Rec<'buf>> + decode_rig::Rows + 'static,
{
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = make();
                absorb(corpus, &rig.payloads);
                RefCell::new(rig)
            },
            move |b, rig: &RefCell<FlattenRig<D, S>>| {
                b.iter(|| decode_and_flatten::<F, D, S>(&mut rig.borrow_mut()));
            },
        )
        .items(batches::LINES)
        .bytes_of(|rig: &RefCell<FlattenRig<D, S>>| corpus_bytes(&rig.borrow().payloads))
        .done()
}

// ---------------------------------------------------------------------------
// `raw_`: the three decode paths in raw mode
// ---------------------------------------------------------------------------

fn raw_decode_cases(suite: Suite) -> Suite {
    let s = batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        suite,
        "raw_flat15_value",
        RECORDS,
        flat_corpus_value_rig,
    );
    let s = batch_case::<Owned<orders::Order>, AvroSerdeDeserializer<orders::Order>>(
        s,
        "raw_flat15_serde",
        RECORDS,
        flat_corpus_serde_rig,
    );
    batch_case::<Owned<orders::Order>, AvroDatumDeserializer<Owned<orders::Order>>>(
        s,
        "raw_flat15_datum",
        RECORDS,
        flat_corpus_datum_rig,
    )
}

// ---------------------------------------------------------------------------
// `raw_batch_`: one datum carrying an array of lines
// ---------------------------------------------------------------------------

fn batch_cases(suite: Suite) -> Suite {
    let s = batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        suite,
        "raw_batch_value",
        batches::LINES,
        order_batch_value_rig,
    );
    let s = batch_case::<Owned<batches::OrderPlaced>, AvroSerdeDeserializer<batches::OrderPlaced>>(
        s,
        "raw_batch_serde",
        batches::LINES,
        order_batch_serde_rig,
    );
    let s = batch_case::<
        Owned<batches::OrderPlaced>,
        AvroDatumDeserializer<Owned<batches::OrderPlaced>>,
    >(s, "raw_batch_datum", batches::LINES, order_batch_datum_rig);
    let s = batch_case::<batches::BatchRefFam, AvroDatumDeserializer<batches::BatchRefFam>>(
        s,
        "raw_batch_datum_borrowed",
        batches::LINES,
        order_batch_borrowed_rig,
    );
    let s = flatten_case::<Owned<AvroValue>, AvroValueDeserializer, ValueFlattenSink>(
        s,
        "raw_batch_value_flatten",
        decode_rig::value_flatten_rig,
    );
    flatten_case::<
        batches::BatchRefFam,
        AvroDatumDeserializer<batches::BatchRefFam>,
        TypedFlattenSink,
    >(s, "raw_batch_datum_flatten", decode_rig::typed_flatten_rig)
}

// ---------------------------------------------------------------------------
// `mode_`: the wire framings, and the schema cache behind them
// ---------------------------------------------------------------------------

fn mode_cases(suite: Suite) -> Suite {
    let s = batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        suite,
        "mode_confluent_warm",
        RECORDS,
        confluent_cached_rig,
    );
    let s = batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        s,
        "mode_confluent_mixed_ids",
        RECORDS,
        confluent_mixed_rig,
    );
    batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        s,
        "mode_single_object",
        RECORDS,
        single_object_rig,
    )
}

// ---------------------------------------------------------------------------
// `resolved_`: reader-schema resolution
// ---------------------------------------------------------------------------

fn resolved_cases(suite: Suite) -> Suite {
    type Rig = AvroSerdeDeserializer<corpora::Evolved>;
    type Fam = Owned<corpora::Evolved>;

    let s = batch_case::<Fam, Rig>(suite, "resolved_writer_only", RECORDS, || {
        evolution_rig(None)
    });
    let s = batch_case::<Fam, Rig>(s, "resolved_reordered", RECORDS, || {
        evolution_rig(Some(corpora::EVENT_REORDERED))
    });
    let s = batch_case::<Fam, Rig>(s, "resolved_promoted", RECORDS, || {
        evolution_rig(Some(corpora::EVENT_PROMOTED))
    });
    batch_case::<Fam, Rig>(s, "resolved_defaulted", RECORDS, || {
        evolution_rig(Some(corpora::EVENT_DEFAULTED))
    })
}

// ---------------------------------------------------------------------------
// `shapes_`: decode shapes no other case reaches
// ---------------------------------------------------------------------------

fn shapes_cases(suite: Suite) -> Suite {
    let s = batch_case::<Owned<corpora::Shapes>, AvroDatumDeserializer<Owned<corpora::Shapes>>>(
        suite,
        "shapes_logical_types",
        RECORDS,
        shapes_rig,
    );
    batch_case::<Owned<corpora::LongList>, AvroDatumDeserializer<Owned<corpora::LongList>>>(
        s,
        "shapes_recursive_refs",
        RECORDS,
        recursive_rig,
    )
}

// ---------------------------------------------------------------------------
// `err_`: the failure paths
// ---------------------------------------------------------------------------

fn error_cases(suite: Suite) -> Suite {
    let s = batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        suite,
        "err_truncated_value",
        RECORDS,
        truncated_value_rig,
    );
    let s = batch_case::<Owned<orders::Order>, AvroSerdeDeserializer<orders::Order>>(
        s,
        "err_truncated_serde",
        RECORDS,
        truncated_serde_rig,
    );
    let s = batch_case::<Owned<orders::Order>, AvroDatumDeserializer<Owned<orders::Order>>>(
        s,
        "err_truncated_datum",
        RECORDS,
        truncated_datum_rig,
    );
    let s = batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        s,
        "err_unknown_schema_id",
        RECORDS,
        confluent_unknown_rig,
    );
    let s = batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        s,
        "err_poisoned_schema_id",
        RECORDS,
        confluent_poisoned_rig,
    );
    batch_case::<Owned<AvroValue>, AvroValueDeserializer>(
        s,
        "err_stale_fingerprint",
        RECORDS,
        stale_fingerprint_rig,
    )
}

// ---------------------------------------------------------------------------
// `lib_`: apache-avro over the same bytes
// ---------------------------------------------------------------------------

/// A parsed writer schema, an optional reader schema, the corpus, and a
/// [`GenericDatumReader`] over the pair.
///
/// The schemas are leaked so the reader can borrow them for `'static` and be
/// built in setup, outside every measured region. A leak per case is bounded
/// by the number of floor cases, since setup runs once each.
struct Floor {
    writer: &'static Schema,
    reader: Option<&'static Schema>,
    held: GenericDatumReader<'static>,
    payloads: Vec<Vec<u8>>,
}

impl Floor {
    fn new(writer_json: &str, reader_json: Option<&str>, payloads: Vec<Vec<u8>>) -> Floor {
        let writer: &'static Schema = Box::leak(Box::new(
            Schema::parse_str(writer_json).expect("the writer schema parses"),
        ));
        let reader: Option<&'static Schema> = reader_json.map(|json| {
            &*Box::leak(Box::new(
                Schema::parse_str(json).expect("the reader schema parses"),
            ))
        });
        let held = GenericDatumReader::builder(writer)
            .maybe_reader_schema(reader)
            .build()
            .expect("the fixture schema resolves");
        Floor {
            writer,
            reader,
            held,
            payloads,
        }
    }
}

/// The library's route to a `Value`. The decoded value is returned rather than
/// discarded, so it is dropped inside the region, which is parity with the
/// crate's path, where the record reaches a sink and is dropped there.
fn floor_values(f: &Floor) -> usize {
    let mut decoded = 0usize;
    for payload in &f.payloads {
        let value = apache_avro::from_avro_datum(f.writer, &mut payload.as_slice(), f.reader)
            .expect("the fixture decodes");
        std::hint::black_box(&value);
        decoded += 1;
    }
    decoded
}

/// The library's two-pass route from a datum to a `T`: build the `Value`, then
/// read the target out of it. The only route that applies a reader schema.
fn floor_typed<T: serde::de::DeserializeOwned>(f: &Floor) -> usize {
    let mut decoded = 0usize;
    for payload in &f.payloads {
        let value = apache_avro::from_avro_datum(f.writer, &mut payload.as_slice(), f.reader)
            .expect("the fixture decodes");
        let target: T = apache_avro::from_value(&value).expect("the fixture matches the target");
        std::hint::black_box(&target);
        decoded += 1;
    }
    decoded
}

/// The library's route to a `Value` through [`Floor::held`], which resolves
/// the writer schema's named types once for the corpus where the free function
/// above resolves them on every call.
fn floor_values_held(f: &Floor) -> usize {
    let mut decoded = 0usize;
    for payload in &f.payloads {
        let value = f
            .held
            .read_value(&mut payload.as_slice())
            .expect("the fixture decodes");
        std::hint::black_box(&value);
        decoded += 1;
    }
    decoded
}

/// The library's two-pass route to a `T` through [`Floor::held`].
fn floor_typed_held<T: serde::de::DeserializeOwned>(f: &Floor) -> usize {
    let mut decoded = 0usize;
    for payload in &f.payloads {
        let value = f
            .held
            .read_value(&mut payload.as_slice())
            .expect("the fixture decodes");
        let target: T = apache_avro::from_value(&value).expect("the fixture matches the target");
        std::hint::black_box(&target);
        decoded += 1;
    }
    decoded
}

/// The library's single-pass route from a datum to a `T`, through
/// [`Floor::held`].
///
/// `T: DeserializeOwned` and the reader is `impl Read`, so every string and
/// bytes field is a fresh allocation: there is no counterpart here to the
/// crate's borrowed family. Panics if a reader schema is configured, so this
/// route carries no `resolved_*` case.
fn floor_read_deser<T: serde::de::DeserializeOwned>(f: &Floor) -> usize {
    let mut decoded = 0usize;
    for payload in &f.payloads {
        let target: T = f
            .held
            .read_deser(&mut payload.as_slice())
            .expect("the fixture decodes");
        std::hint::black_box(&target);
        decoded += 1;
    }
    decoded
}

fn floor_case<T: 'static>(
    suite: Suite,
    id: &'static str,
    items: u64,
    make: fn() -> Floor,
    routine: fn(&Floor) -> T,
) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let floor = make();
                absorb(corpus, &floor.payloads);
                floor
            },
            move |b, floor: &Floor| {
                b.iter(|| routine(floor));
            },
        )
        .items(items)
        .bytes_of(|floor: &Floor| corpus_bytes(&floor.payloads))
        .done()
}

fn flat_floor() -> Floor {
    Floor::new(orders::SCHEMA, None, corpora::order_datums())
}

fn batch_floor() -> Floor {
    Floor::new(batches::BATCH_SCHEMA, None, batches::order_batches())
}

fn resolved_floor() -> Floor {
    Floor::new(
        corpora::EVENT_WRITER,
        Some(corpora::EVENT_REORDERED),
        corpora::event_datums(),
    )
}

fn floor_cases(suite: Suite) -> Suite {
    let s = floor_case(suite, "lib_flat15_value", RECORDS, flat_floor, floor_values);
    let s = floor_case(
        s,
        "lib_flat15_typed",
        RECORDS,
        flat_floor,
        floor_typed::<orders::Order>,
    );
    let s = floor_case(
        s,
        "lib_batch_value",
        batches::LINES,
        batch_floor,
        floor_values,
    );
    let s = floor_case(
        s,
        "lib_batch_typed",
        batches::LINES,
        batch_floor,
        floor_typed::<batches::OrderPlaced>,
    );
    let s = floor_case(
        s,
        "lib_resolved",
        RECORDS,
        resolved_floor,
        floor_typed::<corpora::Evolved>,
    );
    let s = floor_case(
        s,
        "lib_flat15_read_deser",
        RECORDS,
        flat_floor,
        floor_read_deser::<orders::Order>,
    );
    let s = floor_case(
        s,
        "lib_batch_read_deser",
        batches::LINES,
        batch_floor,
        floor_read_deser::<batches::OrderPlaced>,
    );
    let s = floor_case(
        s,
        "lib_flat15_value_held",
        RECORDS,
        flat_floor,
        floor_values_held,
    );
    let s = floor_case(
        s,
        "lib_batch_value_held",
        batches::LINES,
        batch_floor,
        floor_values_held,
    );
    floor_case(
        s,
        "lib_resolved_held",
        RECORDS,
        resolved_floor,
        floor_typed_held::<corpora::Evolved>,
    )
}
