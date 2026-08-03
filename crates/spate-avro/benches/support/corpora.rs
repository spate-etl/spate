//! Batch-sized decode corpora: Confluent-framed payloads, one writer schema
//! with the reader schemas it evolves into, the logical-type and recursive
//! schemas, and the poison payloads.
//!
//! Included with `#[path]` by `benches/decode_gungraun.rs` and by
//! `tests/bench_fixtures.rs`, which pins every corpus below. A bench target
//! is its own crate, so two targets can only agree on a workload by
//! compiling the same source.
//!
//! # Why a batch
//!
//! [`BATCH`] payloads, not one. A source hands the chain a poll batch and the
//! deserializer walks it with its schema memo, its reader schema and its
//! compiled decode spec already hot; one payload measures the first of those
//! walks, which is the least representative one. The size is chosen so
//! per-payload decode dominates the once-per-batch memo refresh, and so every
//! case — from the cheapest error return to the most expensive resolved
//! decode — lands inside the counter tier's useful range rather than in the
//! noise below it.
//!
//! # Determinism
//!
//! Every corpus is a pure function of the record index: an instruction count
//! only means something if the merge-base leg and the head leg encoded
//! byte-identical input, and those are different builds in different
//! processes. That rules out `rand` and `DefaultHasher`, and it also rules
//! out **`apache_avro`'s map encoder**: `Value::Map` is a `HashMap`, whose
//! iteration order is seeded per process, so a multi-entry map encoded
//! through `to_avro_datum` produces different bytes on every run. The map
//! field in [`shapes_schema`] is therefore framed here by
//! [`encode_map_of_long`] and appended to a head record that carries every
//! other field — `tests/bench_fixtures.rs` decodes the result through the
//! public API, so the hand-framing cannot drift from what the schema says.
//!
//! ## What a deterministic corpus still does not pin
//!
//! Identical input bytes do not make every count bit-identical, and it is
//! worth knowing exactly which cases are not. A `HashMap`'s seed is drawn per
//! process, so where one is *built during the decode* the probe sequence — not
//! the hash itself — differs between the merge-base leg and the head leg with
//! no code change at all. Two places do that: `apache-avro`'s `resolve_record`
//! builds one per record whenever a reader schema is applied, and the
//! logical-type target holds a `HashMap` field.
//!
//! Running the same binary twice moves exactly those four cases — the three
//! resolving readers and `logical_types` — and nothing else: the writer-only
//! reader, the recursive datum, the Confluent cases and every pre-existing
//! case come back bit-identical. That controlled comparison bounds the seed's
//! own contribution at roughly a thousandth of a percent. Across two *builds*
//! the same four are still the only cases that move, by up to a twentieth of
//! a percent — the difference is ordinary codegen jitter, which every case is
//! exposed to and which these four cannot be separated from.
//!
//! So the caveat is narrow: on those four, do not read a near-zero delta as
//! exactly zero. It is not a reason to change the fixtures. Removing the
//! nondeterminism would mean giving up either the reader schema or a
//! realistic map target, which is the whole of what those cases measure.

use apache_avro::types::Value;
use apache_avro::{Decimal, Schema, Uuid, to_avro_datum};
use std::collections::HashMap;

/// Payloads in one corpus — a poll batch's worth. See the module docs.
///
/// The floor is set by the cheapest case, not the most expensive one: the
/// Confluent cases that resolve to a missing schema id return before any
/// decode happens, and at half this size that case counts below the range
/// where a counter comparison is worth reading. The ceiling is the recursive
/// datum, which is two orders above it and still well inside.
pub(crate) const BATCH: usize = 2_000;

/// Epoch milliseconds every timestamp derivation counts from.
const BASE_MS: i64 = 1_772_000_000_000;

// ---------------------------------------------------------------------------
// Confluent framing
// ---------------------------------------------------------------------------

/// The schema id the stub registry serves, and the one the warm-memo corpus
/// is framed with.
pub(crate) const READY_ID: u32 = 4_211;

/// A schema id the registry answers `404` for, so it negative-caches and
/// every payload carrying it resolves to `Lookup::Failed`.
pub(crate) const POISON_ID: u32 = 4_212;

/// A schema id that is never fetched at all — the deserializer's runtime is
/// never driven, so the fetcher task never runs and the id stays
/// `Lookup::Missing` for the whole corpus.
pub(crate) const UNKNOWN_ID: u32 = 4_213;

/// Wrap a bare datum in the Confluent wire format: magic `0x00`, the schema
/// id big-endian, then the datum.
pub(crate) fn confluent(id: u32, datum: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(5 + datum.len());
    framed.push(0x00);
    framed.extend_from_slice(&id.to_be_bytes());
    framed.extend_from_slice(datum);
    framed
}

/// [`BATCH`] flat-order datums framed under one schema id.
pub(crate) fn confluent_orders(id: u32) -> Vec<Vec<u8>> {
    order_datums()
        .iter()
        .map(|datum| confluent(id, datum))
        .collect()
}

/// A single-object framing whose fingerprint is deliberately not the
/// configured schema's: magic `0xC3 0x01`, the fingerprint little-endian,
/// then the datum. Every payload resolves to `SchemaUnavailable`, which is
/// what a consumer sees when the producer moves to a schema its fixed
/// configuration no longer matches.
pub(crate) const STALE_FINGERPRINT: u64 = 0xDEAD_BEEF_DEAD_BEEF;

/// [`BATCH`] flat-order datums framed single-object under
/// [`STALE_FINGERPRINT`].
pub(crate) fn stale_single_object() -> Vec<Vec<u8>> {
    order_datums()
        .iter()
        .map(|datum| {
            let mut framed = Vec::with_capacity(10 + datum.len());
            framed.extend_from_slice(&[0xC3, 0x01]);
            framed.extend_from_slice(&STALE_FINGERPRINT.to_le_bytes());
            framed.extend_from_slice(datum);
            framed
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The flat-order corpus
// ---------------------------------------------------------------------------

/// [`BATCH`] bare datums of `orders::SCHEMA`, one per index.
///
/// The fields vary with the index rather than repeating one record: both
/// union branches occur, the tag array runs empty through three elements, and
/// the string lengths move, so the walk is not a single wire shape decoded
/// two thousand times.
pub(crate) fn order_datums() -> Vec<Vec<u8>> {
    let schema = Schema::parse_str(crate::orders::SCHEMA).expect("order schema parses");
    (0..BATCH).map(|i| order_datum(&schema, i)).collect()
}

/// The same corpus truncated mid-record — the poison-pill storm's payload.
/// Truncation, not emptiness: an empty payload is a tombstone and decodes to
/// nothing without touching the error path at all.
pub(crate) fn truncated_order_datums() -> Vec<Vec<u8>> {
    order_datums()
        .into_iter()
        .map(|mut datum| {
            datum.truncate(datum.len() / 2);
            datum
        })
        .collect()
}

fn order_datum(schema: &Schema, i: usize) -> Vec<u8> {
    let n = i as i64;
    let mut rec = apache_avro::types::Record::new(schema).expect("order schema is a record");
    rec.put("id", 900_000 + n);
    rec.put("user_id", 70_000 + (n * 7) % 5_000);
    rec.put("sku", format!("SKU-{:04}-{}", i % 10_000, size_of(i)));
    rec.put("quantity", (i % 5) as i32 + 1);
    #[expect(
        clippy::cast_precision_loss,
        reason = "a residue mod 9_999 is exactly representable"
    )]
    rec.put("unit_price", ((i % 9_999) as f64) / 100.0);
    rec.put("currency", ["GBP", "USD", "EUR"][i % 3]);
    rec.put("region", ["emea", "amer", "apac"][i % 3]);
    rec.put("channel", ["web", "app", "pos"][i % 3]);
    rec.put("created_ms", BASE_MS + n);
    rec.put("updated_ms", BASE_MS + n + 60_000);
    // Both union branches, on different periods so they do not move together.
    rec.put(
        "discount",
        if i.is_multiple_of(3) {
            Value::Union(0, Box::new(Value::Null))
        } else {
            #[expect(
                clippy::cast_precision_loss,
                reason = "a residue mod 50 is exactly representable"
            )]
            Value::Union(1, Box::new(Value::Double((i % 50) as f64 / 100.0)))
        },
    );
    rec.put(
        "coupon",
        if i.is_multiple_of(2) {
            Value::Union(0, Box::new(Value::Null))
        } else {
            Value::Union(1, Box::new(Value::String(format!("CPN-{}", i % 97))))
        },
    );
    rec.put(
        "tags",
        Value::Array(
            (0..i % 4)
                .map(|j| Value::String(format!("tag-{}", (i + j) % 16)))
                .collect(),
        ),
    );
    rec.put("priority", (i % 4) as i32);
    rec.put("note", NOTES[i % NOTES.len()]);
    to_avro_datum(schema, rec).expect("order datum encodes")
}

const NOTES: [&str; 4] = [
    "leave at the door",
    "signature required",
    "",
    "ring the bell twice and wait by the side gate",
];

fn size_of(i: usize) -> &'static str {
    ["S", "M", "L", "XL"][i % 4]
}

// ---------------------------------------------------------------------------
// Schema evolution: one writer, four readers
// ---------------------------------------------------------------------------

/// The writer schema every evolution case decodes from.
pub(crate) const EVENT_WRITER: &str = r#"{"type":"record","name":"Event","fields":[
  {"name":"id","type":"int"},
  {"name":"name","type":"string"},
  {"name":"amount","type":"float"},
  {"name":"region","type":"string"},
  {"name":"ts_ms","type":"long"}]}"#;

/// Reader: the same five fields, declared in a different order. Resolution
/// has to match each writer field to a reader field by name and reorder the
/// result.
pub(crate) const EVENT_REORDERED: &str = r#"{"type":"record","name":"Event","fields":[
  {"name":"ts_ms","type":"long"},
  {"name":"region","type":"string"},
  {"name":"amount","type":"float"},
  {"name":"name","type":"string"},
  {"name":"id","type":"int"}]}"#;

/// Reader: `int`→`long` and `float`→`double`, Avro's two numeric widenings.
pub(crate) const EVENT_PROMOTED: &str = r#"{"type":"record","name":"Event","fields":[
  {"name":"id","type":"long"},
  {"name":"name","type":"string"},
  {"name":"amount","type":"double"},
  {"name":"region","type":"string"},
  {"name":"ts_ms","type":"long"}]}"#;

/// Reader: one added field the writer never wrote, filled from its default.
pub(crate) const EVENT_DEFAULTED: &str = r#"{"type":"record","name":"Event","fields":[
  {"name":"id","type":"int"},
  {"name":"name","type":"string"},
  {"name":"amount","type":"float"},
  {"name":"region","type":"string"},
  {"name":"ts_ms","type":"long"},
  {"name":"channel","type":"string","default":"web"}]}"#;

/// Reader: a renamed field carrying the old name as an alias.
///
/// **Not a bench case.** The two-pass path resolves through
/// `apache_avro::types::Value::resolve`, whose `resolve_record` matches
/// writer fields to reader fields by name only — a reader field alias is
/// registered when the schema is parsed and then never consulted, so this
/// reader rejects every payload the other four resolve.
/// `tests/bench_fixtures.rs` pins that, so the case can be added the day the
/// dependency starts honouring it — which is also why this reader is `allow`ed
/// rather than deleted: it is the input that pin needs.
#[allow(dead_code, reason = "used only by the fixtures test's alias pin")]
pub(crate) const EVENT_ALIASED: &str = r#"{"type":"record","name":"Event","fields":[
  {"name":"id","type":"int"},
  {"name":"label","type":"string","aliases":["name"]},
  {"name":"amount","type":"float"},
  {"name":"region","type":"string"},
  {"name":"ts_ms","type":"long"}]}"#;

/// The one decode target all five evolution cases share.
///
/// A superset of every reader shape, so the only thing that differs between
/// the five counts is the resolution rule under test — a per-reader target
/// would fold each struct's own field count into the comparison. The widths
/// are the promoted ones and serde's integer and float visitors accept the
/// narrower wire values; the two renamed halves and the defaulted field carry
/// `#[serde(default)]` because each is absent from four of the five readers.
#[derive(Debug, serde::Deserialize)]
#[expect(dead_code, reason = "deserialization target only")]
pub(crate) struct Evolved {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    label: String,
    amount: f64,
    region: String,
    ts_ms: i64,
    #[serde(default)]
    channel: String,
}

/// [`BATCH`] bare datums of [`EVENT_WRITER`], one per index.
pub(crate) fn event_datums() -> Vec<Vec<u8>> {
    let schema = Schema::parse_str(EVENT_WRITER).expect("event writer schema parses");
    (0..BATCH)
        .map(|i| {
            let mut rec = apache_avro::types::Record::new(&schema).expect("event is a record");
            rec.put("id", (i % 100_000) as i32);
            rec.put("name", format!("event-{}", i % 512));
            #[expect(
                clippy::cast_precision_loss,
                reason = "a residue mod 1_000 is exactly representable"
            )]
            rec.put("amount", (i % 1_000) as f32 / 8.0);
            rec.put("region", ["emea", "amer", "apac"][i % 3]);
            rec.put("ts_ms", BASE_MS + i as i64);
            to_avro_datum(&schema, rec).expect("event datum encodes")
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Schema shapes: logical types, and a recursive named reference
// ---------------------------------------------------------------------------

/// Every field of the logical-type record except the trailing map, which is
/// framed separately (see the module docs on determinism).
const SHAPES_HEAD_FIELDS: &str = r#"
  {"name":"colour","type":{"type":"enum","name":"Colour","symbols":["RED","GREEN","BLUE"]}},
  {"name":"digest","type":{"type":"fixed","name":"F8","size":8}},
  {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
  {"name":"balance","type":{"type":"fixed","name":"F6","size":6,"logicalType":"decimal",
    "precision":10,"scale":2}},
  {"name":"uuid","type":{"type":"string","logicalType":"uuid"}},
  {"name":"day","type":{"type":"int","logicalType":"date"}},
  {"name":"created_ms","type":{"type":"long","logicalType":"timestamp-millis"}},
  {"name":"created_us","type":{"type":"long","logicalType":"timestamp-micros"}}"#;

/// The trailing `map<long>` field.
const SHAPES_MAP_FIELD: &str = r#"{"name":"tags","type":{"type":"map","values":"long"}}"#;

fn shapes_record(fields: &str) -> String {
    format!(r#"{{"type":"record","name":"Shapes","fields":[{fields}]}}"#)
}

/// The full logical-type schema the bench decodes against.
pub(crate) fn shapes_schema() -> String {
    shapes_record(&format!("{SHAPES_HEAD_FIELDS},{SHAPES_MAP_FIELD}"))
}

/// A bytes target: a `Vec<u8>` field would route through `deserialize_seq`
/// and fail on both decode paths.
///
/// `allow` rather than `expect`: the wrapped bytes are read by
/// `tests/bench_fixtures.rs` and by nothing in the bench, and the two targets
/// compile this same source, so an `expect` would fire as unfulfilled in the
/// one that does read it.
#[derive(Debug)]
#[allow(dead_code, reason = "read only by the fixtures test")]
pub(crate) struct Blob(pub(crate) Vec<u8>);

impl<'de> serde::Deserialize<'de> for Blob {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = Blob;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("bytes")
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Blob, E> {
                Ok(Blob(v.to_vec()))
            }
        }
        d.deserialize_bytes(V)
    }
}

/// The enum target — decoded from the symbol name on both paths.
#[derive(Debug, serde::Deserialize)]
pub(crate) enum Colour {
    #[serde(rename = "RED")]
    Red,
    #[serde(rename = "GREEN")]
    Green,
    #[serde(rename = "BLUE")]
    Blue,
}

/// The logical-type decode target.
#[derive(Debug, serde::Deserialize)]
#[expect(dead_code, reason = "deserialization target only")]
pub(crate) struct Shapes {
    pub(crate) colour: Colour,
    pub(crate) digest: Blob,
    pub(crate) price: Blob,
    pub(crate) balance: Blob,
    pub(crate) uuid: String,
    pub(crate) day: i32,
    pub(crate) created_ms: i64,
    pub(crate) created_us: i64,
    pub(crate) tags: HashMap<String, i64>,
}

/// Map entries for record `i`: one through four, so the block loop runs more
/// than once and the entry count varies across the corpus.
pub(crate) fn shape_tags(i: usize) -> Vec<(String, i64)> {
    (0..=i % 4)
        .map(|j| (format!("k{}", (i + j) % 23), ((i * 31 + j) % 5_000) as i64))
        .collect()
}

/// [`BATCH`] bare datums of [`shapes_schema`], one per index.
pub(crate) fn shapes_datums() -> Vec<Vec<u8>> {
    let head = Schema::parse_str(&shapes_record(SHAPES_HEAD_FIELDS)).expect("shapes head parses");
    (0..BATCH)
        .map(|i| {
            let mut datum =
                to_avro_datum(&head, shapes_head_value(i)).expect("shapes head encodes");
            encode_map_of_long(&mut datum, &shape_tags(i));
            datum
        })
        .collect()
}

fn shapes_head_value(i: usize) -> Value {
    let n = i as i64;
    Value::Record(vec![
        (
            "colour".to_owned(),
            Value::Enum((i % 3) as u32, ["RED", "GREEN", "BLUE"][i % 3].to_owned()),
        ),
        (
            "digest".to_owned(),
            Value::Fixed(8, (n as u64).to_be_bytes().to_vec()),
        ),
        (
            "price".to_owned(),
            // Two's-complement big-endian, as a `bytes` decimal is written.
            Value::Decimal(Decimal::from(((i % 30_000) as i16).to_be_bytes().to_vec())),
        ),
        (
            "balance".to_owned(),
            // Six bytes, the fixed backing's exact width, sign-extended.
            Value::Decimal(Decimal::from(
                i64::from(-((i % 30_000) as i32)).to_be_bytes()[2..].to_vec(),
            )),
        ),
        (
            "uuid".to_owned(),
            Value::Uuid(Uuid::from_u128(
                0x0123_4567_89AB_CDEF_0000_0000_0000_0000 | i as u128,
            )),
        ),
        ("day".to_owned(), Value::Date(19_000 + (i % 365) as i32)),
        ("created_ms".to_owned(), Value::TimestampMillis(BASE_MS + n)),
        (
            "created_us".to_owned(),
            Value::TimestampMicros(BASE_MS * 1_000 + n),
        ),
    ])
}

/// Avro's binary map encoding: one block of `entries.len()` (key, value)
/// pairs, then the terminating zero count.
fn encode_map_of_long(out: &mut Vec<u8>, entries: &[(String, i64)]) {
    zigzag(entries.len() as i64, out);
    for (key, value) in entries {
        zigzag(key.len() as i64, out);
        out.extend_from_slice(key.as_bytes());
        zigzag(*value, out);
    }
    zigzag(0, out);
}

/// Avro's zig-zag varint.
fn zigzag(value: i64, out: &mut Vec<u8>) {
    let mut n = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        if n & !0x7F == 0 {
            out.push(n as u8);
            return;
        }
        out.push((n as u8 & 0x7F) | 0x80);
        n >>= 7;
    }
}

/// A schema whose `next` field refers to the record by name, so every level
/// of the datum costs the decode spec's named-type lookup.
pub(crate) const LONG_LIST: &str = r#"{"type":"record","name":"LongList","fields":[
  {"name":"value","type":"long"},
  {"name":"next","type":["null","LongList"]}]}"#;

/// Nodes in each [`LONG_LIST`] datum. A node costs two levels of the datum
/// path's depth guard (the union, then the record it wraps), so this stays
/// comfortably inside the guard's limit while still walking a chain rather
/// than a pair.
pub(crate) const LIST_NODES: usize = 32;

/// The recursive decode target.
#[derive(Debug, serde::Deserialize)]
#[expect(dead_code, reason = "deserialization target only")]
pub(crate) struct LongList {
    pub(crate) value: i64,
    pub(crate) next: Option<Box<LongList>>,
}

/// [`BATCH`] bare datums of [`LONG_LIST`], each [`LIST_NODES`] long.
pub(crate) fn long_list_datums() -> Vec<Vec<u8>> {
    let schema = Schema::parse_str(LONG_LIST).expect("long list schema parses");
    (0..BATCH)
        .map(|i| {
            let mut node = Value::Record(vec![
                ("value".to_owned(), Value::Long((i * LIST_NODES) as i64)),
                ("next".to_owned(), Value::Union(0, Box::new(Value::Null))),
            ]);
            for depth in 1..LIST_NODES {
                node = Value::Record(vec![
                    (
                        "value".to_owned(),
                        Value::Long((i * LIST_NODES + depth) as i64),
                    ),
                    ("next".to_owned(), Value::Union(1, Box::new(node))),
                ]);
            }
            to_avro_datum(&schema, node).expect("long list datum encodes")
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Corpus digest
// ---------------------------------------------------------------------------

/// FNV-1a over every byte of a corpus, in order, with the payload lengths
/// folded in. `tests/bench_fixtures.rs` pins the result: two calls in one
/// process only prove the generator is pure, and the property the benches
/// need is that the corpus is identical *across revisions*, because the
/// merge-base leg and the head leg run different builds.
///
/// `allow` rather than `expect`, for the reason given on [`Blob`].
#[allow(dead_code, reason = "called only by the fixtures test")]
pub(crate) fn digest(corpus: &[Vec<u8>]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut fold = |byte: u8| {
        h ^= u64::from(byte);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    };
    for payload in corpus {
        for byte in (payload.len() as u64).to_le_bytes() {
            fold(byte);
        }
        for byte in payload {
            fold(*byte);
        }
    }
    h
}
