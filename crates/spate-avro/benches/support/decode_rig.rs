//! The decode rigs both bench tiers drive: a deserializer, the bytes it
//! decodes, and the ack and sink around it.
//!
//! `benches/decode_paths_wall.rs` times these regions and
//! `benches/decode_gungraun.rs` counts them, so a wall-clock regression and a
//! counted one are statements about one region rather than two that have
//! drifted apart.
//!
//! Included with `#[path]` rather than imported: a bench target is its own
//! crate. A target including this module also includes `corpora`, `orders`,
//! `batches` and `registry_stub` at its crate root, which the rig constructors
//! below reach through `crate::`.
//!
//! # Every measured function carries `#[inline(never)]`
//!
//! Callgrind collection is bounded by a toggle on the glob
//! `*::__gungraun_wrapper_mod::*`, the module the `#[library_benchmark]` macro
//! wraps each benchmark function in, and a toggle flips collection rather than
//! forcing it on. A body the optimizer places under that module path is entered
//! with collection already on, so entering it turns collection off, and the
//! case counts a fraction of the work. `#[inline(never)]` keeps the body a
//! symbol of its own, outside the glob.
//!
//! This module is not any bench target's crate root, so [`decode_once`],
//! [`decode_once_err`], [`decode_batch`] and [`decode_and_flatten`] each need
//! the attribute. Removing one fails no build and no test; it drops that case's
//! count silently.

// Each bench target compiles this module separately and drives a different
// subset of the rigs.
#![allow(dead_code, reason = "each bench target uses a different subset")]

use spate_avro::{
    AvroDatumDeserializer, AvroDeserializerBuilder, AvroMode, AvroSerdeDeserializer, AvroSettings,
    AvroValueDeserializer, RegistrySection, SchemaSource,
};
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, Owned, RecFamily};
use spate_core::record::{Flow, PartitionId, RawPayload, Record};
use std::hint::black_box;
use std::time::Duration;

use crate::registry_stub::{StubRegistry, Warm};
use crate::{batches, corpora, orders};

/// The negative-cache TTL every Confluent rig is built with.
///
/// Long enough that a negative entry cannot expire part-way through a walk of
/// the corpus. If it did, the payloads after the expiry would take the
/// `Missing` arm instead of the `Failed` one and the count would depend on how
/// long the walk took.
pub(crate) const NEGATIVE_TTL: Duration = Duration::from_secs(3_600);

/// One deserializer with the bytes it decodes.
///
/// The payload is owned; each case builds the `RawPayload` wrapper inside the
/// measured region. That cost is identical across cases, so it cancels in any
/// comparison.
pub(crate) struct Rig<D> {
    pub(crate) deser: D,
    pub(crate) payload: Vec<u8>,
    pub(crate) ack: AckRef,
    pub(crate) sink: orders::Sink,
    // The builder keeps this runtime's handle for the registry fetcher. Raw
    // mode never uses it, but the handle must not outlive the runtime.
    pub(crate) _rt: tokio::runtime::Runtime,
}

/// The batch equivalent: one deserializer with a whole poll batch's worth of
/// payloads, and whether the corpus is one every payload decodes or one every
/// payload fails.
pub(crate) struct BatchRig<D> {
    pub(crate) deser: D,
    pub(crate) payloads: Vec<Vec<u8>>,
    pub(crate) decodes: bool,
    pub(crate) ack: AckRef,
    pub(crate) sink: orders::Sink,
    pub(crate) _rt: tokio::runtime::Runtime,
}

/// Wrap payload bytes for one deserialize call.
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
#[inline(never)]
pub(crate) fn decode_once<F, D>(rig: &mut Rig<D>)
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
/// decodes cleanly, the bench panics instead of silently counting the happy
/// path.
#[inline(never)]
pub(crate) fn decode_once_err<F, D>(rig: &mut Rig<D>)
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
/// deserializer. Returns the number of payloads that decoded, which is what
/// reaches the wall tier's `black_box`.
///
/// To check that a new case measures anything, halve its corpus, re-run, and
/// confirm the count halves. A count that does not move with the corpus is
/// measuring nothing, however plausible it looks.
///
/// `tests/bench_fixtures.rs` pins every corpus, and the assert below pins the
/// outcome: a fixture that started decoding when it used to fail (or the
/// reverse) fails the bench rather than silently re-baselining it.
#[inline(never)]
pub(crate) fn decode_batch<F, D>(rig: &mut BatchRig<D>) -> usize
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
    decoded
}

// ---------------------------------------------------------------------------
// Rig construction
// ---------------------------------------------------------------------------

/// Build a builder over a fixed inline schema in raw mode, with no registry,
/// so nothing in the measured path touches the network.
pub(crate) fn rig<D>(
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
    // message is ever sent, so the receiver has nothing to keep alive.
    let (ack, _ack_rx) = AckRef::test_pair();
    Rig {
        deser,
        payload,
        ack,
        sink: orders::Sink(0),
        _rt: rt,
    }
}

/// The batch equivalent, taking whole settings so a case can choose its mode
/// and its reader schema. The runtime is never driven, so a Confluent rig
/// built this way never performs a fetch.
pub(crate) fn batch_rig<D>(
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
        sink: orders::Sink(0),
        _rt: rt,
    }
}

/// Raw-mode settings over one inline schema, optionally resolved into a
/// reader schema.
pub(crate) fn raw_settings(schema: &str, reader: Option<&str>) -> AvroSettings {
    AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(schema)),
        reader_schema: reader.map(SchemaSource::inline),
        ..AvroSettings::default()
    }
}

// ---------------------------------------------------------------------------
// The flat 15-field record
// ---------------------------------------------------------------------------

pub(crate) fn flat_value_rig() -> Rig<AvroValueDeserializer> {
    rig(orders::SCHEMA, orders::order_datum(), |b| {
        b.build_value().expect("value builder")
    })
}

pub(crate) fn flat_serde_rig() -> Rig<AvroSerdeDeserializer<orders::Order>> {
    rig(orders::SCHEMA, orders::order_datum(), |b| {
        b.build_serde::<orders::Order>().expect("serde builder")
    })
}

pub(crate) fn flat_datum_rig() -> Rig<AvroDatumDeserializer<Owned<orders::Order>>> {
    rig(orders::SCHEMA, orders::order_datum(), |b| {
        b.build_serde_datum::<orders::Order>()
            .expect("datum builder")
    })
}

pub(crate) fn flat_value_malformed_rig() -> Rig<AvroValueDeserializer> {
    rig(orders::SCHEMA, orders::malformed_datum(), |b| {
        b.build_value().expect("value builder")
    })
}

pub(crate) fn batch_value_rig() -> Rig<AvroValueDeserializer> {
    rig(orders::BATCH_SCHEMA, orders::batch_datum(), |b| {
        b.build_value().expect("value builder")
    })
}

pub(crate) fn batch_datum_rig() -> Rig<AvroDatumDeserializer<Owned<orders::PlacedOrder>>> {
    rig(orders::BATCH_SCHEMA, orders::batch_datum(), |b| {
        b.build_serde_datum::<orders::PlacedOrder>()
            .expect("datum builder")
    })
}

/// The whole flat-record corpus, one deserializer per decode path.
pub(crate) fn flat_corpus_value_rig() -> BatchRig<AvroValueDeserializer> {
    let settings = raw_settings(orders::SCHEMA, None);
    batch_rig(&settings, corpora::order_datums(), true, |b| {
        b.build_value().expect("value builder")
    })
}

pub(crate) fn flat_corpus_serde_rig() -> BatchRig<AvroSerdeDeserializer<orders::Order>> {
    let settings = raw_settings(orders::SCHEMA, None);
    batch_rig(&settings, corpora::order_datums(), true, |b| {
        b.build_serde::<orders::Order>().expect("serde builder")
    })
}

pub(crate) fn flat_corpus_datum_rig() -> BatchRig<AvroDatumDeserializer<Owned<orders::Order>>> {
    let settings = raw_settings(orders::SCHEMA, None);
    batch_rig(&settings, corpora::order_datums(), true, |b| {
        b.build_serde_datum::<orders::Order>()
            .expect("datum builder")
    })
}

// ---------------------------------------------------------------------------
// The order-batch corpus, where one datum carries an array of lines
// ---------------------------------------------------------------------------

pub(crate) fn order_batch_value_rig() -> BatchRig<AvroValueDeserializer> {
    let settings = raw_settings(batches::BATCH_SCHEMA, None);
    batch_rig(&settings, batches::order_batches(), true, |b| {
        b.build_value().expect("value builder")
    })
}

pub(crate) fn order_batch_serde_rig() -> BatchRig<AvroSerdeDeserializer<batches::OrderPlaced>> {
    let settings = raw_settings(batches::BATCH_SCHEMA, None);
    batch_rig(&settings, batches::order_batches(), true, |b| {
        b.build_serde::<batches::OrderPlaced>()
            .expect("serde builder")
    })
}

pub(crate) fn order_batch_datum_rig() -> BatchRig<AvroDatumDeserializer<Owned<batches::OrderPlaced>>>
{
    let settings = raw_settings(batches::BATCH_SCHEMA, None);
    batch_rig(&settings, batches::order_batches(), true, |b| {
        b.build_serde_datum::<batches::OrderPlaced>()
            .expect("datum builder")
    })
}

pub(crate) fn order_batch_borrowed_rig() -> BatchRig<AvroDatumDeserializer<batches::BatchRefFam>> {
    let settings = raw_settings(batches::BATCH_SCHEMA, None);
    batch_rig(&settings, batches::order_batches(), true, |b| {
        b.build_datum::<batches::BatchRefFam>()
            .expect("datum builder")
    })
}

// ---------------------------------------------------------------------------
// Decode plus flatten: the `flat_map` stage minus the engine around it
// ---------------------------------------------------------------------------

/// Counts the rows a decoded batch flattens into, over the `AvroValue` tree.
pub(crate) struct ValueFlattenSink(pub(crate) u64);

impl EmitRecord<'_, spate_avro::AvroValue> for ValueFlattenSink {
    fn emit(&mut self, rec: Record<spate_avro::AvroValue>) -> Flow {
        let mut rows = 0u64;
        batches::flatten_value(&rec.payload, |row| {
            black_box(&row);
            rows += 1;
        });
        self.0 += rows;
        Flow::Continue
    }
}

/// The borrowed-path equivalent, over the typed record.
pub(crate) struct TypedFlattenSink(pub(crate) u64);

impl<'buf> EmitRecord<'buf, batches::OrderPlacedRef<'buf>> for TypedFlattenSink {
    fn emit(&mut self, rec: Record<batches::OrderPlacedRef<'buf>>) -> Flow {
        let mut rows = 0u64;
        batches::flatten_typed(&rec.payload, |row| {
            black_box(&row);
            rows += 1;
        });
        self.0 += rows;
        Flow::Continue
    }
}

/// A batch rig whose sink flattens rather than counting, and the corpus it
/// walks. Separate from [`BatchRig`] because the sink type is the subject.
pub(crate) struct FlattenRig<D, S> {
    pub(crate) deser: D,
    pub(crate) payloads: Vec<Vec<u8>>,
    pub(crate) ack: AckRef,
    pub(crate) sink: S,
    pub(crate) _rt: tokio::runtime::Runtime,
}

/// The rows a flatten sink has emitted so far. [`decode_and_flatten`] returns
/// it, so the value the harness holds is one the flatten decides.
pub(crate) trait Rows {
    fn rows(&self) -> u64;
}

impl Rows for ValueFlattenSink {
    fn rows(&self) -> u64 {
        self.0
    }
}

impl Rows for TypedFlattenSink {
    fn rows(&self) -> u64 {
        self.0
    }
}

/// Decode every payload and flatten each decoded record, returning the rows the
/// corpus has produced.
#[inline(never)]
pub(crate) fn decode_and_flatten<F, D, S>(rig: &mut FlattenRig<D, S>) -> u64
where
    F: RecFamily,
    D: Deserializer<F>,
    S: for<'buf> EmitRecord<'buf, F::Rec<'buf>> + Rows,
{
    let FlattenRig {
        deser,
        payloads,
        ack,
        sink,
        ..
    } = rig;
    for payload in payloads.iter() {
        let raw = raw_payload(payload);
        deser
            .deserialize(black_box(&raw), ack, sink)
            .expect("the batch corpus decodes");
    }
    sink.rows()
}

fn flatten_rig<D, S>(
    payloads: Vec<Vec<u8>>,
    sink: S,
    build: impl FnOnce(&AvroDeserializerBuilder) -> D,
) -> FlattenRig<D, S> {
    let settings = raw_settings(batches::BATCH_SCHEMA, None);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    let deser = build(&builder);
    let (ack, _ack_rx) = AckRef::test_pair();
    FlattenRig {
        deser,
        payloads,
        ack,
        sink,
        _rt: rt,
    }
}

pub(crate) fn value_flatten_rig() -> FlattenRig<AvroValueDeserializer, ValueFlattenSink> {
    flatten_rig(batches::order_batches(), ValueFlattenSink(0), |b| {
        b.build_value().expect("value builder")
    })
}

pub(crate) fn typed_flatten_rig()
-> FlattenRig<AvroDatumDeserializer<batches::BatchRefFam>, TypedFlattenSink> {
    flatten_rig(batches::order_batches(), TypedFlattenSink(0), |b| {
        b.build_datum::<batches::BatchRefFam>()
            .expect("datum builder")
    })
}

// ---------------------------------------------------------------------------
// Confluent framing and schema-cache lookup
// ---------------------------------------------------------------------------

pub(crate) fn confluent_settings(url: String) -> AvroSettings {
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
pub(crate) fn warmed_confluent_rig(id: u32, want: Warm) -> BatchRig<AvroValueDeserializer> {
    let stub = StubRegistry::start(&[(corpora::READY_ID, orders::SCHEMA)]);
    let settings = confluent_settings(stub.url());
    // `enable_all`: this runtime does drive the fetcher, but only inside the
    // warm-up below, and never again once the rig is handed over.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    let mut deser = builder.build_value().expect("value builder");
    let payloads = corpora::confluent_orders(id);
    let (ack, _ack_rx) = AckRef::test_pair();
    let mut sink = orders::Sink(0);
    crate::registry_stub::warm(&mut deser, &rt, &ack, &mut sink, &payloads[0], want);
    stub.shutdown();
    BatchRig {
        deser,
        payloads,
        decodes: matches!(want, Warm::Ready),
        ack,
        // A fresh counter: the warm-up emitted a record the measured walk
        // did not.
        sink: orders::Sink(0),
        _rt: rt,
    }
}

pub(crate) fn confluent_cached_rig() -> BatchRig<AvroValueDeserializer> {
    warmed_confluent_rig(corpora::READY_ID, Warm::Ready)
}

/// A Confluent rig whose memo holds every id in [`corpora::MIXED_IDS`], over a
/// corpus that rotates through them. Read against
/// [`confluent_cached_rig`]: the datum bodies are the same, so the difference
/// between the two is the schema lookup on eight keys rather than one.
pub(crate) fn confluent_mixed_rig() -> BatchRig<AvroValueDeserializer> {
    let routes: Vec<(u32, &str)> = corpora::MIXED_IDS
        .iter()
        .map(|id| (*id, orders::SCHEMA))
        .collect();
    let stub = StubRegistry::start(&routes);
    let settings = confluent_settings(stub.url());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    let mut deser = builder.build_value().expect("value builder");
    let (ack, _ack_rx) = AckRef::test_pair();
    let mut sink = orders::Sink(0);
    // One payload per id, so every id is Ready in the memo before the walk.
    let seeds: Vec<Vec<u8>> = corpora::MIXED_IDS
        .iter()
        .map(|id| corpora::confluent(*id, &corpora::order_datums()[0]))
        .collect();
    crate::registry_stub::warm_each(&mut deser, &rt, &ack, &mut sink, &seeds);
    stub.shutdown();
    BatchRig {
        deser,
        payloads: corpora::confluent_mixed_orders(),
        decodes: true,
        ack,
        sink: orders::Sink(0),
        _rt: rt,
    }
}

pub(crate) fn confluent_poisoned_rig() -> BatchRig<AvroValueDeserializer> {
    warmed_confluent_rig(corpora::POISON_ID, Warm::Poisoned)
}

/// The `Lookup::Missing` corpus, and the one Confluent rig that needs no
/// stub at all: its runtime is never driven, so the fetcher task never polls,
/// the request it queues is never read, and the id stays missing for every
/// payload. Nothing here, setup included, opens a socket.
pub(crate) fn confluent_unknown_rig() -> BatchRig<AvroValueDeserializer> {
    let settings = confluent_settings("http://127.0.0.1:1".to_owned());
    batch_rig(
        &settings,
        corpora::confluent_orders(corpora::UNKNOWN_ID),
        false,
        |b| b.build_value().expect("value builder"),
    )
}

/// Single-object framing whose fingerprint matches the configured schema, so
/// every payload resolves and decodes.
pub(crate) fn single_object_rig() -> BatchRig<AvroValueDeserializer> {
    let settings = AvroSettings {
        mode: AvroMode::SingleObject,
        schema: Some(SchemaSource::inline(orders::SCHEMA)),
        ..AvroSettings::default()
    };
    batch_rig(&settings, corpora::matching_single_object(), true, |b| {
        b.build_value().expect("value builder")
    })
}

// ---------------------------------------------------------------------------
// Schema evolution
// ---------------------------------------------------------------------------

pub(crate) fn evolution_rig(
    reader: Option<&str>,
) -> BatchRig<AvroSerdeDeserializer<corpora::Evolved>> {
    let settings = raw_settings(corpora::EVENT_WRITER, reader);
    batch_rig(&settings, corpora::event_datums(), true, |b| {
        b.build_serde::<corpora::Evolved>().expect("serde builder")
    })
}

// ---------------------------------------------------------------------------
// Schema shapes
// ---------------------------------------------------------------------------

pub(crate) fn shapes_rig() -> BatchRig<AvroDatumDeserializer<Owned<corpora::Shapes>>> {
    let settings = raw_settings(&corpora::shapes_schema(), None);
    batch_rig(&settings, corpora::shapes_datums(), true, |b| {
        b.build_serde_datum::<corpora::Shapes>()
            .expect("datum builder")
    })
}

pub(crate) fn recursive_rig() -> BatchRig<AvroDatumDeserializer<Owned<corpora::LongList>>> {
    let settings = raw_settings(corpora::LONG_LIST, None);
    batch_rig(&settings, corpora::long_list_datums(), true, |b| {
        b.build_serde_datum::<corpora::LongList>()
            .expect("datum builder")
    })
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

pub(crate) fn truncated_value_rig() -> BatchRig<AvroValueDeserializer> {
    let settings = raw_settings(orders::SCHEMA, None);
    batch_rig(&settings, corpora::truncated_order_datums(), false, |b| {
        b.build_value().expect("value builder")
    })
}

pub(crate) fn truncated_datum_rig() -> BatchRig<AvroDatumDeserializer<Owned<orders::Order>>> {
    let settings = raw_settings(orders::SCHEMA, None);
    batch_rig(&settings, corpora::truncated_order_datums(), false, |b| {
        b.build_serde_datum::<orders::Order>()
            .expect("datum builder")
    })
}

pub(crate) fn truncated_serde_rig() -> BatchRig<AvroSerdeDeserializer<orders::Order>> {
    let settings = raw_settings(orders::SCHEMA, None);
    batch_rig(&settings, corpora::truncated_order_datums(), false, |b| {
        b.build_serde::<orders::Order>().expect("serde builder")
    })
}

pub(crate) fn stale_fingerprint_rig() -> BatchRig<AvroValueDeserializer> {
    let settings = AvroSettings {
        mode: AvroMode::SingleObject,
        schema: Some(SchemaSource::inline(orders::SCHEMA)),
        ..AvroSettings::default()
    };
    batch_rig(&settings, corpora::stale_single_object(), false, |b| {
        b.build_value().expect("value builder")
    })
}
