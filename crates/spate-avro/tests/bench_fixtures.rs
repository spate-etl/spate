//! The bench corpora are reproducible, and decode the way the benches claim.
//!
//! An instruction count only means something if both legs of a comparison ran
//! on byte-identical input, so "the corpus is a pure function of the index" is
//! a property worth a test rather than an assumption. The benches themselves
//! cannot carry it: they need Linux, valgrind and a matching runner. This runs
//! everywhere `cargo test` does.
//!
//! It also carries the fixtures' semantics. Every batch case asserts that its
//! whole corpus decodes, or that none of it does; a corpus that silently
//! flipped would re-baseline the case rather than fail it, and here that
//! failure arrives on the pull request instead of after the fact. The
//! hand-framed map and the reader schemas get the same treatment: each is
//! decoded through the public API and checked against what the schema says,
//! so neither the framing nor a resolution rule can silently stop doing what
//! its case is named for, including the alias rule, which does not work at
//! all and is pinned as such.

use spate_avro::{
    AvroDeserializerBuilder, AvroMode, AvroSettings, AvroValue, RegistrySection, SchemaSource,
};
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
use spate_core::record::{Flow, PartitionId, RawPayload, Record};
use std::time::Duration;

#[path = "../benches/support/corpora.rs"]
mod corpora;
#[path = "../benches/support/orders.rs"]
mod orders;
#[path = "../benches/support/registry_stub.rs"]
mod registry_stub;

use registry_stub::{StubRegistry, Warm};

/// A runtime that can drive a registry fetch. Only the stub test needs one.
fn io_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// The runtime a raw-mode builder is handed: it hosts a fetcher that raw mode
/// never uses, so it needs no drivers. Built without them because these
/// helpers make one per call, and a driver-enabled runtime leaves a blocking
/// pool winding down behind every one of them.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

fn raw(bytes: &[u8]) -> RawPayload<'_> {
    RawPayload {
        bytes,
        key: None,
        partition: PartitionId(0),
        offset: 1,
        timestamp_ms: 0,
    }
}

fn builder(settings: &AvroSettings, rt: &tokio::runtime::Runtime) -> AvroDeserializerBuilder {
    AvroDeserializerBuilder::from_settings(settings, rt.handle()).unwrap()
}

fn raw_settings(schema: &str, reader: Option<&str>) -> AvroSettings {
    AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(schema)),
        reader_schema: reader.map(SchemaSource::inline),
        ..AvroSettings::default()
    }
}

/// How many of a corpus decode, and how many fail, the pair every batch
/// case's `decodes` flag claims.
fn outcomes<F, D>(mut deser: D, payloads: &[Vec<u8>]) -> (usize, usize)
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let (ack, _rx) = AckRef::test_pair();
    let mut sink = orders::Sink(0);
    let mut ok = 0;
    let mut err = 0;
    for payload in payloads {
        if deser.deserialize(&raw(payload), &ack, &mut sink).is_ok() {
            ok += 1;
        } else {
            err += 1;
        }
    }
    (ok, err)
}

struct Collected<T>(Vec<Record<T>>);
impl<'buf, T> EmitRecord<'buf, T> for Collected<T> {
    fn emit(&mut self, rec: Record<T>) -> Flow {
        self.0.push(rec);
        Flow::Continue
    }
}

/// Decode one payload into `T` through the single-pass path.
fn decode_one_datum<T>(schema: &str, payload: &[u8]) -> T
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    let rt = runtime();
    let mut deser = builder(&raw_settings(schema, None), &rt)
        .build_serde_datum::<T>()
        .unwrap();
    let (ack, _rx) = AckRef::test_pair();
    let mut out = Collected::<T>(Vec::new());
    deser.deserialize(&raw(payload), &ack, &mut out).unwrap();
    assert_eq!(out.0.len(), 1);
    out.0.pop().unwrap().payload
}

/// Decode one payload into an [`AvroValue`] through the two-pass path,
/// optionally resolved into a reader schema.
fn decode_one_value(schema: &str, reader: Option<&str>, payload: &[u8]) -> AvroValue {
    let rt = runtime();
    let mut deser = builder(&raw_settings(schema, reader), &rt)
        .build_value()
        .unwrap();
    let (ack, _rx) = AckRef::test_pair();
    let mut out = Collected::<AvroValue>(Vec::new());
    deser.deserialize(&raw(payload), &ack, &mut out).unwrap();
    assert_eq!(out.0.len(), 1);
    out.0.pop().unwrap().payload
}

fn field<'a>(value: &'a AvroValue, name: &str) -> &'a AvroValue {
    let AvroValue::Record(fields) = value else {
        panic!("expected a record, got {value:?}");
    };
    &fields
        .iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("no field {name} in {fields:?}"))
        .1
}

// ---------------------------------------------------------------------------
// Reproducibility
// ---------------------------------------------------------------------------

#[test]
fn the_corpora_are_reproducible() {
    assert_eq!(
        corpora::digest(&corpora::order_datums()),
        corpora::digest(&corpora::order_datums())
    );
    assert_eq!(
        corpora::digest(&corpora::shapes_datums()),
        corpora::digest(&corpora::shapes_datums())
    );
}

/// Two calls in one process only prove the generator is pure. The property
/// the benches need is stronger: that the corpus is the same *across
/// revisions*, since a merge-base leg and a head leg run different builds.
/// The digest is the witness: any edit to a derivation, a schema, a batch
/// size or an encoder moves it, and moving it silently would re-baseline
/// every recorded count for the case it feeds.
#[test]
fn the_corpora_are_pinned_across_revisions() {
    let mut drifted: Vec<String> = Vec::new();
    for (name, digest, want) in [
        (
            "order_datums",
            corpora::digest(&corpora::order_datums()),
            0x38c9_8ced_fdf4_47b1u64,
        ),
        (
            "truncated_order_datums",
            corpora::digest(&corpora::truncated_order_datums()),
            0x95ba_6447_27d2_47e3,
        ),
        (
            "confluent_orders",
            corpora::digest(&corpora::confluent_orders(corpora::READY_ID)),
            0xb017_5c6b_afbe_2fff,
        ),
        (
            "stale_single_object",
            corpora::digest(&corpora::stale_single_object()),
            0xefd4_d6fa_5ab3_e7a1,
        ),
        (
            "event_datums",
            corpora::digest(&corpora::event_datums()),
            0x0a08_f98e_1525_2b3c,
        ),
        (
            "shapes_datums",
            corpora::digest(&corpora::shapes_datums()),
            0x8f3e_6856_dd5e_c3b2,
        ),
        (
            "long_list_datums",
            corpora::digest(&corpora::long_list_datums()),
            0x77ba_abca_3240_c9a5,
        ),
    ] {
        // Collected rather than asserted one at a time: a change to a shared
        // derivation moves several corpora at once, and re-running the test
        // per corpus to discover that is a waste of a maintainer's afternoon.
        if digest != want {
            drifted.push(format!("{name}: {digest:#018x} (pinned {want:#018x})"));
        }
    }
    assert!(
        drifted.is_empty(),
        "corpora drifted from their pinned digests — if that is intended, \
         every recorded count for the cases they feed is against a different \
         corpus:\n  {}",
        drifted.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// The corpora decode the way their cases assert
// ---------------------------------------------------------------------------

#[test]
fn the_decoding_corpora_decode_in_full() {
    let rt = runtime();

    let events = corpora::event_datums();
    for reader in [
        None,
        Some(corpora::EVENT_REORDERED),
        Some(corpora::EVENT_PROMOTED),
        Some(corpora::EVENT_DEFAULTED),
    ] {
        let deser = builder(&raw_settings(corpora::EVENT_WRITER, reader), &rt)
            .build_serde::<corpora::Evolved>()
            .unwrap();
        assert_eq!(
            outcomes(deser, &events),
            (corpora::BATCH, 0),
            "the evolution corpus stopped resolving into {reader:?}"
        );
    }

    let shapes_schema = corpora::shapes_schema();
    let deser = builder(&raw_settings(&shapes_schema, None), &rt)
        .build_serde_datum::<corpora::Shapes>()
        .unwrap();
    assert_eq!(
        outcomes(deser, &corpora::shapes_datums()),
        (corpora::BATCH, 0)
    );

    let deser = builder(&raw_settings(corpora::LONG_LIST, None), &rt)
        .build_serde_datum::<corpora::LongList>()
        .unwrap();
    assert_eq!(
        outcomes(deser, &corpora::long_list_datums()),
        (corpora::BATCH, 0),
        "a {}-node list no longer decodes; the datum path's depth guard is \
         the first thing to check",
        corpora::LIST_NODES
    );
}

#[test]
fn the_poison_corpora_fail_on_every_payload() {
    let rt = runtime();
    let truncated = corpora::truncated_order_datums();

    let deser = builder(&raw_settings(orders::SCHEMA, None), &rt)
        .build_serde_datum::<orders::Order>()
        .unwrap();
    assert_eq!(outcomes(deser, &truncated), (0, corpora::BATCH));

    let deser = builder(&raw_settings(orders::SCHEMA, None), &rt)
        .build_serde::<orders::Order>()
        .unwrap();
    assert_eq!(outcomes(deser, &truncated), (0, corpora::BATCH));

    let single_object = AvroSettings {
        mode: AvroMode::SingleObject,
        schema: Some(SchemaSource::inline(orders::SCHEMA)),
        ..AvroSettings::default()
    };
    let deser = builder(&single_object, &rt).build_value().unwrap();
    assert_eq!(
        outcomes(deser, &corpora::stale_single_object()),
        (0, corpora::BATCH),
        "the stale fingerprint now matches the configured schema, so the case \
         measures a successful decode instead of the mismatch"
    );
}

/// The single-payload corpora `benches/support/orders.rs` has carried since
/// the bench was written, pinned the same way.
#[test]
fn the_single_payload_fixtures_still_decode_and_fail_as_documented() {
    let rt = runtime();
    let b = builder(&raw_settings(orders::SCHEMA, None), &rt);
    assert_eq!(
        outcomes(b.build_value().unwrap(), &[orders::order_datum()]),
        (1, 0)
    );
    assert_eq!(
        outcomes(b.build_value().unwrap(), &[orders::malformed_datum()]),
        (0, 1),
        "the malformed fixture decodes cleanly, so the error-path case is \
         counting the happy path"
    );

    let batch = decode_one_value(orders::BATCH_SCHEMA, None, &orders::batch_datum());
    let AvroValue::Array(lines) = field(&batch, "lines") else {
        panic!("lines is not an array");
    };
    assert_eq!(lines.len() as u64, orders::BATCH_LINES);

    // The array's element *types* are the workload the counted tier compares
    // against, so pin them rather than only their count. A rename may move the
    // field names freely; changing a `long` to an `int` here would move the
    // instruction baseline with nothing else to say it had.
    let AvroValue::Record(fields) = &lines[0] else {
        panic!("a line is not a record");
    };
    let shape: Vec<&str> = fields
        .iter()
        .map(|(_, v)| match v {
            AvroValue::String(_) => "string",
            AvroValue::Long(_) => "long",
            AvroValue::Int(_) => "int",
            other => panic!("unexpected line field {other:?}"),
        })
        .collect();
    assert_eq!(
        shape,
        ["string", "long", "string"],
        "the batch fixture's element types are the measured workload"
    );
}

// ---------------------------------------------------------------------------
// The fixtures mean what their names say
// ---------------------------------------------------------------------------

/// `Value::Map` is a `HashMap`, so a multi-entry map encoded through
/// `to_avro_datum` produces different bytes on every run. The corpus frames
/// the map itself; this is what proves the framing agrees with the schema.
#[test]
fn the_hand_framed_map_decodes_to_its_entries() {
    let schema = corpora::shapes_schema();
    let datums = corpora::shapes_datums();
    for i in [0usize, 1, 2, 3, 17, corpora::BATCH - 1] {
        let decoded: corpora::Shapes = decode_one_datum(&schema, &datums[i]);
        let want: std::collections::HashMap<String, i64> =
            corpora::shape_tags(i).into_iter().collect();
        assert_eq!(decoded.tags, want, "record {i}'s map framing drifted");
    }
    // Not every record carries the same number of entries, or the map's
    // block loop is a constant.
    let sizes: std::collections::HashSet<usize> =
        (0..8).map(|i| corpora::shape_tags(i).len()).collect();
    assert!(sizes.len() > 1, "every map has the same entry count");
}

#[test]
fn the_logical_type_fixture_carries_every_shape_it_names() {
    let schema = corpora::shapes_schema();
    let decoded: corpora::Shapes = decode_one_datum(&schema, &corpora::shapes_datums()[1]);
    assert_eq!(decoded.digest.0.len(), 8, "the fixed field lost its width");
    assert_eq!(
        decoded.balance.0.len(),
        6,
        "the fixed decimal lost its width"
    );
    assert!(!decoded.price.0.is_empty(), "the bytes decimal is empty");
    assert_eq!(
        decoded.uuid.len(),
        36,
        "the uuid did not decode to its canonical form: {}",
        decoded.uuid
    );
    assert!(decoded.created_us > decoded.created_ms);
}

#[test]
fn the_recursive_fixture_is_a_chain_not_a_pair() {
    let decoded: corpora::LongList =
        decode_one_datum(corpora::LONG_LIST, &corpora::long_list_datums()[0]);
    let mut nodes = 1;
    let mut node = &decoded;
    while let Some(next) = &node.next {
        node = next;
        nodes += 1;
    }
    assert_eq!(nodes, corpora::LIST_NODES);
}

/// Each reader schema has to apply its rule. An alias that silently
/// stopped resolving, or a default that stopped being filled, would leave the
/// case measuring plain identity resolution under an evolution name.
#[test]
fn each_reader_schema_resolves_its_rule() {
    let datum = &corpora::event_datums()[3];
    let writer = corpora::EVENT_WRITER;

    let plain = decode_one_value(writer, None, datum);
    let AvroValue::Record(fields) = &plain else {
        panic!("expected a record");
    };
    let order: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(order, ["id", "name", "amount", "region", "ts_ms"]);

    let reordered = decode_one_value(writer, Some(corpora::EVENT_REORDERED), datum);
    let AvroValue::Record(fields) = &reordered else {
        panic!("expected a record");
    };
    let order: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        order,
        ["ts_ms", "region", "amount", "name", "id"],
        "the reader schema no longer reorders the resolved record"
    );

    let promoted = decode_one_value(writer, Some(corpora::EVENT_PROMOTED), datum);
    assert!(
        matches!(field(&promoted, "id"), AvroValue::Long(_)),
        "int did not promote to long: {:?}",
        field(&promoted, "id")
    );
    assert!(
        matches!(field(&promoted, "amount"), AvroValue::Double(_)),
        "float did not promote to double"
    );

    let defaulted = decode_one_value(writer, Some(corpora::EVENT_DEFAULTED), datum);
    assert_eq!(
        field(&defaulted, "channel"),
        &AvroValue::String("web".to_owned()),
        "the added field was not filled from its default"
    );
}

/// Why the evolution group has three readers and not four.
///
/// The two-pass path resolves through `apache_avro::types::Value::resolve`.
/// Its `resolve_record` matches a writer field to a reader field by the
/// reader field's *name* alone: a reader field alias is registered in the
/// record's lookup table when the schema is parsed, and then never consulted
/// during resolution. A reader that renames a field and carries the old name
/// as an alias therefore rejects every payload, rather than resolving it.
///
/// This is pinned rather than worked around because the crate's own
/// documentation lists aliases among the resolution rules a `reader_schema`
/// applies (issue #74). If the dependency starts honoring them, this test
/// fails, and the bench gains the case it is missing.
#[test]
fn a_reader_field_alias_does_not_resolve() {
    let rt = runtime();
    let datum = &corpora::event_datums()[3];
    let mut deser = builder(
        &raw_settings(corpora::EVENT_WRITER, Some(corpora::EVENT_ALIASED)),
        &rt,
    )
    .build_value()
    .unwrap();
    let (ack, _rx) = AckRef::test_pair();
    let mut out = Collected::<AvroValue>(Vec::new());
    let err = deser
        .deserialize(&raw(datum), &ack, &mut out)
        .expect_err("a reader field alias resolves after all");
    assert!(
        err.to_string().contains("label"),
        "expected the renamed field to be reported missing, got {err}"
    );
}

#[test]
fn the_confluent_corpus_carries_the_wire_header() {
    let framed = corpora::confluent_orders(corpora::READY_ID);
    let (id, datum) = spate_avro::parse_confluent(&framed[7]).unwrap();
    assert_eq!(id, corpora::READY_ID);
    assert_eq!(datum, corpora::order_datums()[7].as_slice());
}

// ---------------------------------------------------------------------------
// The Confluent warm-up
// ---------------------------------------------------------------------------

/// The Confluent cases reach a warm cache through the public API: a stub
/// registry on a loopback socket, driven once in setup. Everything below runs
/// in the bench's `#[bench]` argument expression, where a failure surfaces as
/// a bench that dies under valgrind on a machine most contributors do not
/// have, so it is exercised here, under plain `cargo test`, first.
#[test]
fn the_stub_registry_warms_a_ready_and_a_poisoned_id() {
    let stub = StubRegistry::start(corpora::READY_ID, orders::SCHEMA);
    let settings = AvroSettings {
        mode: AvroMode::Confluent,
        registry: Some(RegistrySection {
            url: stub.url(),
            username: None,
            password: None,
        }),
        negative_cache_ttl: Duration::from_secs(3_600),
        ..AvroSettings::default()
    };
    let rt = io_runtime();
    let b = builder(&settings, &rt);
    let (ack, _rx) = AckRef::test_pair();

    let ready = corpora::confluent_orders(corpora::READY_ID);
    let mut deser = b.build_value().unwrap();
    let mut sink = orders::Sink(0);
    registry_stub::warm(&mut deser, &rt, &ack, &mut sink, &ready[0], Warm::Ready);
    // Warmed: the whole corpus now decodes without the runtime running again.
    assert_eq!(outcomes(deser, &ready[..64]), (64, 0));

    let poisoned = corpora::confluent_orders(corpora::POISON_ID);
    let mut deser = b.build_value().unwrap();
    registry_stub::warm(
        &mut deser,
        &rt,
        &ack,
        &mut sink,
        &poisoned[0],
        Warm::Poisoned,
    );
    assert_eq!(outcomes(deser, &poisoned[..64]), (0, 64));

    stub.shutdown();
}

/// The `unknown_schema_id` rig drives an undriven runtime on purpose: the
/// fetcher never polls, so the id stays missing and nothing opens a socket.
/// The URL points at a port nothing listens on, which is only safe *because*
/// no fetch is ever attempted.
#[test]
fn an_undriven_runtime_leaves_every_id_missing() {
    let settings = AvroSettings {
        mode: AvroMode::Confluent,
        registry: Some(RegistrySection {
            url: "http://127.0.0.1:1".to_owned(),
            username: None,
            password: None,
        }),
        negative_cache_ttl: Duration::from_secs(3_600),
        ..AvroSettings::default()
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let deser = builder(&settings, &rt).build_value().unwrap();
    let payloads = corpora::confluent_orders(corpora::UNKNOWN_ID);
    assert_eq!(outcomes(deser, &payloads[..64]), (0, 64));
}
