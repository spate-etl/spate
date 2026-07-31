//! Differential parity suite: every payload the two-pass path
//! (`build_serde`: `from_avro_datum` → `from_value`) decodes, the
//! single-pass path (`build_serde_datum`) must decode to the **same
//! value**. Payloads are encoded with apache-avro itself, so the reference
//! semantics come from the reference implementation, not from this suite's
//! opinion of the spec.
//!
//! The known, documented divergences (strict truncation; the per-datum
//! collection-item budget; skipped-field content validation; the uniform
//! acceptance superset) are pinned by their own tests at the bottom.

use apache_avro::types::Value;
use apache_avro::{Schema, to_avro_datum};
use proptest::prelude::*;
use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord};
use spate_core::record::{Flow, PartitionId, RawPayload, Record};
use std::collections::HashMap;
use std::fmt::Debug;

struct Collected<T>(Vec<Record<T>>);
impl<'buf, T> EmitRecord<'buf, T> for Collected<T> {
    fn emit(&mut self, rec: Record<T>) -> Flow {
        self.0.push(rec);
        Flow::Continue
    }
}

fn raw(bytes: &[u8]) -> RawPayload<'_> {
    RawPayload {
        bytes,
        key: None,
        partition: PartitionId(0),
        offset: 7,
        timestamp_ms: 0,
    }
}

fn builder(schema_json: &str) -> AvroDeserializerBuilder {
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(schema_json)),
        ..AvroSettings::default()
    };
    let rt = Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    ));
    AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap()
}

/// Decode `datum` through both typed paths and assert byte-identical
/// results (via `PartialEq`, so float fields should use [`Bits`]).
fn assert_parity<T>(schema_json: &str, datum: &[u8]) -> T
where
    T: serde::de::DeserializeOwned + Send + PartialEq + Debug + 'static,
{
    let b = builder(schema_json);
    let (ack, _rx) = AckRef::test_pair();

    let mut two_pass = Collected::<T>(Vec::new());
    b.build_serde::<T>()
        .unwrap()
        .deserialize(&raw(datum), &ack, &mut two_pass)
        .expect("two-pass decode");
    let mut single_pass = Collected::<T>(Vec::new());
    b.build_serde_datum::<T>()
        .unwrap()
        .deserialize(&raw(datum), &ack, &mut single_pass)
        .expect("single-pass decode");

    assert_eq!(two_pass.0.len(), 1);
    assert_eq!(single_pass.0.len(), 1);
    let reference = two_pass.0.pop().unwrap().payload;
    let got = single_pass.0.pop().unwrap().payload;
    assert_eq!(got, reference, "single-pass diverged from two-pass");
    got
}

/// An f64 compared by bit pattern, so NaN payloads still assert equality.
#[derive(Debug, serde::Deserialize)]
#[serde(transparent)]
struct Bits(f64);
impl PartialEq for Bits {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

/// A bytes target that accepts serde's bytes visitors (a `Vec<u8>` field
/// would go through `deserialize_seq` and fail on both paths).
#[derive(Debug, PartialEq)]
struct Blob(Vec<u8>);
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

// ---------------------------------------------------------------------------
// The published comparison workload shape
// ---------------------------------------------------------------------------

const SENSOR_BATCH: &str = r#"{"type":"record","name":"SensorBatch","namespace":"rs.etl.bench","fields":[
    {"name":"batch_id","type":"long"},
    {"name":"sensor","type":"string"},
    {"name":"region","type":["null","string"],"default":null},
    {"name":"batch_ts_ms","type":"long"},
    {"name":"send_ts_us","type":"long"},
    {"name":"events","type":{"type":"array","items":{"type":"record","name":"Event","fields":[
        {"name":"seq","type":"int"},
        {"name":"name","type":"string"},
        {"name":"unit","type":"string"},
        {"name":"value","type":"long"},
        {"name":"quality","type":["null","double"],"default":null},
        {"name":"tags","type":{"type":"array","items":"string"}}]}}}]}"#;

#[derive(Debug, serde::Deserialize, PartialEq)]
struct SensorBatch {
    batch_id: i64,
    sensor: String,
    region: Option<String>,
    batch_ts_ms: i64,
    send_ts_us: i64,
    events: Vec<Event>,
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct Event {
    seq: i32,
    name: String,
    unit: String,
    value: i64,
    quality: Option<Bits>,
    tags: Vec<String>,
}

/// One generated event: (seq, name, unit, value, quality, tags).
type EventTuple<'a> = (i32, &'a str, &'a str, i64, Option<f64>, Vec<String>);

fn sensor_batch_datum(batch_id: i64, region: Option<&str>, events: &[EventTuple<'_>]) -> Vec<u8> {
    let schema = Schema::parse_str(SENSOR_BATCH).unwrap();
    let union = |idx: u32, v: Value| Value::Union(idx, Box::new(v));
    let record = Value::Record(vec![
        ("batch_id".into(), Value::Long(batch_id)),
        ("sensor".into(), Value::String(format!("sensor-{batch_id}"))),
        (
            "region".into(),
            match region {
                None => union(0, Value::Null),
                Some(r) => union(1, Value::String(r.into())),
            },
        ),
        ("batch_ts_ms".into(), Value::Long(1_772_000_000_000)),
        ("send_ts_us".into(), Value::Long(1_772_000_000_000_000)),
        (
            "events".into(),
            Value::Array(
                events
                    .iter()
                    .map(|(seq, name, unit, value, quality, tags)| {
                        Value::Record(vec![
                            ("seq".into(), Value::Int(*seq)),
                            ("name".into(), Value::String((*name).into())),
                            ("unit".into(), Value::String((*unit).into())),
                            ("value".into(), Value::Long(*value)),
                            (
                                "quality".into(),
                                match quality {
                                    None => union(0, Value::Null),
                                    Some(q) => union(1, Value::Double(*q)),
                                },
                            ),
                            (
                                "tags".into(),
                                Value::Array(
                                    tags.iter().map(|t| Value::String(t.clone())).collect(),
                                ),
                            ),
                        ])
                    })
                    .collect(),
            ),
        ),
    ]);
    to_avro_datum(&schema, record).unwrap()
}

#[test]
fn sensor_batch_shapes_are_identical() {
    let cases: &[(i64, Option<&str>, Vec<EventTuple<'_>>)] = &[
        (0, None, vec![(0, "metric_0", "count", 1, None, vec![])]),
        (
            1,
            Some("region-1"),
            vec![
                (0, "metric_31", "drop", 5, Some(0.13), vec!["tag-1".into()]),
                (
                    1,
                    "metric_0",
                    "bytes",
                    2_147_483_646,
                    Some(0.99),
                    vec!["tag-2".into(), "tag-3".into(), "tag-4".into()],
                ),
            ],
        ),
        (7, Some("region-0"), vec![]),
    ];
    for (batch_id, region, events) in cases {
        let datum = sensor_batch_datum(*batch_id, *region, events);
        let got: SensorBatch = assert_parity(SENSOR_BATCH, &datum);
        assert_eq!(got.batch_id, *batch_id);
        assert_eq!(got.events.len(), events.len());
    }
}

proptest! {
    #[test]
    fn sensor_batch_parity_holds_for_generated_values(
        batch_id in any::<i64>(),
        region in proptest::option::of("[\\PC]{0,12}"),
        events in proptest::collection::vec(
            (
                any::<i32>(),
                "[\\PC]{0,8}",
                "[\\PC]{0,8}",
                any::<i64>(),
                proptest::option::of(any::<f64>()),
                proptest::collection::vec("[\\PC]{0,6}", 0..4),
            ),
            0..6,
        ),
    ) {
        let events: Vec<EventTuple<'_>> = events
            .iter()
            .map(|(seq, name, unit, value, quality, tags)| {
                (*seq, name.as_str(), unit.as_str(), *value, *quality, tags.clone())
            })
            .collect();
        let datum = sensor_batch_datum(batch_id, region.as_deref(), &events);
        let _: SensorBatch = assert_parity(SENSOR_BATCH, &datum);
    }
}

// ---------------------------------------------------------------------------
// Kitchen sink: every schema node the single-pass path dispatches on
// ---------------------------------------------------------------------------

#[test]
fn scalars_fixed_enum_and_logical_types_are_identical() {
    const SCH: &str = r#"{"type":"record","name":"K","fields":[
        {"name":"b","type":"boolean"},
        {"name":"i","type":"int"},
        {"name":"l","type":"long"},
        {"name":"f","type":"float"},
        {"name":"d","type":"double"},
        {"name":"s","type":"string"},
        {"name":"by","type":"bytes"},
        {"name":"fx","type":{"type":"fixed","name":"F8","size":8}},
        {"name":"e","type":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}},
        {"name":"date","type":{"type":"int","logicalType":"date"}},
        {"name":"tms","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"tus","type":{"type":"long","logicalType":"timestamp-micros"}}]}"#;

    #[derive(Debug, serde::Deserialize, PartialEq)]
    enum Color {
        #[serde(rename = "RED")]
        Red,
        #[serde(rename = "GREEN")]
        Green,
        #[serde(rename = "BLUE")]
        Blue,
    }
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct K {
        b: bool,
        i: i32,
        l: i64,
        f: f32,
        d: Bits,
        s: String,
        by: Blob,
        fx: Blob,
        e: Color,
        date: i32,
        tms: i64,
        tus: i64,
    }

    let schema = Schema::parse_str(SCH).unwrap();
    let record = Value::Record(vec![
        ("b".into(), Value::Boolean(true)),
        ("i".into(), Value::Int(-42)),
        ("l".into(), Value::Long(i64::MIN)),
        ("f".into(), Value::Float(std::f32::consts::PI)),
        ("d".into(), Value::Double(f64::NAN)),
        ("s".into(), Value::String("héllo → wörld".into())),
        ("by".into(), Value::Bytes(vec![0, 255, 128])),
        ("fx".into(), Value::Fixed(8, vec![1, 2, 3, 4, 5, 6, 7, 8])),
        ("e".into(), Value::Enum(1, "GREEN".into())),
        ("date".into(), Value::Date(19_000)),
        ("tms".into(), Value::TimestampMillis(-1_000)), // pre-epoch
        ("tus".into(), Value::TimestampMicros(1_772_000_000_000_000)),
    ]);
    let datum = to_avro_datum(&schema, record).unwrap();
    let got: K = assert_parity(SCH, &datum);
    assert_eq!(got.e, Color::Green);
    assert!(got.d.0.is_nan());
    assert_eq!(got.tms, -1_000);
}

#[test]
fn uuid_decodes_to_the_canonical_lowercase_form() {
    const SCH: &str = r#"{"type":"record","name":"U","fields":[
        {"name":"u","type":{"type":"string","logicalType":"uuid"}}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct U {
        u: String,
    }
    let schema = Schema::parse_str(SCH).unwrap();
    let uuid = apache_avro::Uuid::parse_str("F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6").unwrap();
    let record = Value::Record(vec![("u".into(), Value::Uuid(uuid))]);
    let datum = to_avro_datum(&schema, record).unwrap();
    let got: U = assert_parity(SCH, &datum);
    // Canonicalized: the uppercase input comes out lowercase on both paths.
    assert_eq!(got.u, "f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
}

#[test]
fn decimal_bytes_and_fixed_backing_are_identical() {
    // `from_value` emits `Decimal::to_vec()`, which sign-extends the value
    // back to its original wire length — i.e. the bytes exactly as
    // written, including a fixed-backed decimal's sign padding.
    const SCH: &str = r#"{"type":"record","name":"D","fields":[
        {"name":"db","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
        {"name":"df","type":{"type":"fixed","name":"F6","size":6,"logicalType":"decimal","precision":10,"scale":2}}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct D {
        db: Blob,
        df: Blob,
    }
    let schema = Schema::parse_str(SCH).unwrap();
    let record = Value::Record(vec![
        (
            "db".into(),
            Value::Decimal(apache_avro::Decimal::from(vec![0xFB, 0x2E])), // -1234
        ),
        (
            "df".into(),
            Value::Decimal(apache_avro::Decimal::from(vec![
                0xFF, 0xFF, 0xFF, 0xFF, 0xFB, 0x2E,
            ])),
        ),
    ]);
    let datum = to_avro_datum(&schema, record).unwrap();
    let got: D = assert_parity(SCH, &datum);
    assert_eq!(got.db, Blob(vec![0xFB, 0x2E]));
    assert_eq!(
        got.df,
        Blob(vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFB, 0x2E]),
        "fixed sign padding is preserved, as Decimal::to_vec sign-extends"
    );
}

#[test]
fn maps_and_record_into_map_are_identical() {
    const SCH: &str = r#"{"type":"record","name":"M","fields":[
        {"name":"m","type":{"type":"map","values":"long"}},
        {"name":"flat","type":{"type":"record","name":"Inner","fields":[
            {"name":"x","type":"long"},{"name":"y","type":"long"}]}}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct M {
        m: HashMap<String, i64>,
        // A record decoded into a map target — `#[serde(flatten)]` relies
        // on this acceptance.
        flat: HashMap<String, i64>,
    }
    let schema = Schema::parse_str(SCH).unwrap();
    let mut m = HashMap::new();
    m.insert("a".to_string(), Value::Long(1));
    m.insert("κλειδί".to_string(), Value::Long(-2));
    let record = Value::Record(vec![
        ("m".into(), Value::Map(m)),
        (
            "flat".into(),
            Value::Record(vec![
                ("x".into(), Value::Long(10)),
                ("y".into(), Value::Long(20)),
            ]),
        ),
    ]);
    let datum = to_avro_datum(&schema, record).unwrap();
    let got: M = assert_parity(SCH, &datum);
    assert_eq!(got.m.len(), 2);
    assert_eq!(got.flat["x"], 10);
}

#[test]
fn multi_branch_union_maps_to_a_positional_rust_enum() {
    const SCH: &str = r#"{"type":"record","name":"W","fields":[
        {"name":"v","type":["long","string","boolean"]}]}"#;
    // Variant order mirrors the union declaration — selection is
    // positional by wire index on both paths.
    #[derive(Debug, serde::Deserialize, PartialEq)]
    enum V {
        L(i64),
        S(String),
        B(bool),
    }
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct W {
        v: V,
    }
    let schema = Schema::parse_str(SCH).unwrap();
    for (value, expected) in [
        (Value::Union(0, Box::new(Value::Long(9))), V::L(9)),
        (
            Value::Union(1, Box::new(Value::String("s".into()))),
            V::S("s".into()),
        ),
        (Value::Union(2, Box::new(Value::Boolean(true))), V::B(true)),
    ] {
        let record = Value::Record(vec![("v".into(), value)]);
        let datum = to_avro_datum(&schema, record).unwrap();
        let got: W = assert_parity(SCH, &datum);
        assert_eq!(got.v, expected);
    }
}

#[test]
fn union_null_into_a_seq_target_reads_as_empty() {
    const SCH: &str = r#"{"type":"record","name":"N","fields":[
        {"name":"xs","type":["null",{"type":"array","items":"long"}]}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct N {
        // Not an Option: `from_value` maps Union(null) to an empty Vec and
        // the single-pass path must reproduce that.
        xs: Vec<i64>,
    }
    let schema = Schema::parse_str(SCH).unwrap();
    let record = Value::Record(vec![("xs".into(), Value::Union(0, Box::new(Value::Null)))]);
    let datum = to_avro_datum(&schema, record).unwrap();
    let got: N = assert_parity(SCH, &datum);
    assert_eq!(got.xs, Vec::<i64>::new());
}

#[test]
fn nested_option_and_defaults_are_identical() {
    const SCH: &str = r#"{"type":"record","name":"O","fields":[
        {"name":"a","type":["null","long"]},
        {"name":"b","type":["null","string"]},
        {"name":"extra_in_schema","type":"string"}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct O {
        a: Option<i64>,
        b: Option<String>,
        #[serde(default)]
        missing_in_schema: Option<i64>,
    }
    let schema = Schema::parse_str(SCH).unwrap();
    let record = Value::Record(vec![
        ("a".into(), Value::Union(1, Box::new(Value::Long(5)))),
        ("b".into(), Value::Union(0, Box::new(Value::Null))),
        ("extra_in_schema".into(), Value::String("skipped".into())),
    ]);
    let datum = to_avro_datum(&schema, record).unwrap();
    let got: O = assert_parity(SCH, &datum);
    assert_eq!(
        got,
        O {
            a: Some(5),
            b: None,
            missing_in_schema: None
        }
    );
}

#[test]
fn recursive_named_refs_are_identical() {
    const SCH: &str = r#"{"type":"record","name":"LongList","fields":[
        {"name":"value","type":"long"},
        {"name":"next","type":["null","LongList"]}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct LongList {
        value: i64,
        next: Option<Box<LongList>>,
    }
    let schema = Schema::parse_str(SCH).unwrap();
    let record = Value::Record(vec![
        ("value".into(), Value::Long(1)),
        (
            "next".into(),
            Value::Union(
                1,
                Box::new(Value::Record(vec![
                    ("value".into(), Value::Long(2)),
                    ("next".into(), Value::Union(0, Box::new(Value::Null))),
                ])),
            ),
        ),
    ]);
    let datum = to_avro_datum(&schema, record).unwrap();
    let got: LongList = assert_parity(SCH, &datum);
    assert_eq!(got.value, 1);
    assert_eq!(got.next.unwrap().value, 2);
}

proptest! {
    #[test]
    fn scalar_extremes_are_identical(
        l in any::<i64>(),
        i in any::<i32>(),
        f in any::<f32>(),
        d in any::<f64>(),
        s in "[\\PC]{0,32}",
    ) {
        const SCH: &str = r#"{"type":"record","name":"S","fields":[
            {"name":"l","type":"long"},
            {"name":"i","type":"int"},
            {"name":"f","type":"float"},
            {"name":"d","type":"double"},
            {"name":"s","type":"string"}]}"#;
        #[derive(Debug, serde::Deserialize)]
        struct S {
            l: i64,
            i: i32,
            f: f32,
            d: Bits,
            s: String,
        }
        impl PartialEq for S {
            fn eq(&self, other: &Self) -> bool {
                self.l == other.l
                    && self.i == other.i
                    && self.f.to_bits() == other.f.to_bits()
                    && self.d == other.d
                    && self.s == other.s
            }
        }
        let schema = Schema::parse_str(SCH).unwrap();
        let record = Value::Record(vec![
            ("l".into(), Value::Long(l)),
            ("i".into(), Value::Int(i)),
            ("f".into(), Value::Float(f)),
            ("d".into(), Value::Double(d)),
            ("s".into(), Value::String(s)),
        ]);
        let datum = to_avro_datum(&schema, record).unwrap();
        let _: S = assert_parity(SCH, &datum);
    }
}

// ---------------------------------------------------------------------------
// Documented divergences, pinned
// ---------------------------------------------------------------------------

#[test]
fn truncated_trailing_option_diverges_by_design() {
    // apache-avro's decoder maps EOF at a union index to `Union(0, Null)`,
    // so the two-pass path silently decodes a truncated trailing Option as
    // None. The single-pass path treats any truncation as Malformed.
    const SCH: &str = r#"{"type":"record","name":"T","fields":[
        {"name":"a","type":"long"},
        {"name":"b","type":["null","long"]}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct T {
        a: i64,
        b: Option<i64>,
    }
    // Only field `a` is present; `b`'s union index is missing entirely.
    let truncated =
        to_avro_datum(&Schema::parse_str(r#""long""#).unwrap(), Value::Long(9)).unwrap();

    let b = builder(SCH);
    let (ack, _rx) = AckRef::test_pair();

    let mut two_pass = Collected::<T>(Vec::new());
    b.build_serde::<T>()
        .unwrap()
        .deserialize(&raw(&truncated), &ack, &mut two_pass)
        .expect("the two-pass path lenient-decodes the truncation");
    assert_eq!(two_pass.0[0].payload, T { a: 9, b: None });

    let mut single_pass = Collected::<T>(Vec::new());
    let err = b
        .build_serde_datum::<T>()
        .unwrap()
        .deserialize(&raw(&truncated), &ack, &mut single_pass)
        .unwrap_err();
    assert!(
        matches!(err, spate_core::error::DeserError::Malformed { .. }),
        "{err}"
    );
    assert!(single_pass.0.is_empty());
}

#[test]
fn string_into_unit_enum_matches_two_pass() {
    // `from_value` feeds a plain string value into a unit enum by variant
    // name (`EnumUnitDeserializer`); the single-pass path mirrors it.
    const SCH: &str = r#"{"type":"record","name":"T","fields":[
        {"name":"e","type":"string"}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    enum E {
        #[serde(rename = "on")]
        On,
        #[serde(rename = "off")]
        Off,
    }
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct T {
        e: E,
    }
    let datum = to_avro_datum(
        &Schema::parse_str(SCH).unwrap(),
        Value::Record(vec![("e".into(), Value::String("off".into()))]),
    )
    .unwrap();
    let got: T = assert_parity(SCH, &datum);
    assert_eq!(got, T { e: E::Off });
    let _ = E::On;
}

#[test]
fn invalid_utf8_in_a_skipped_field_diverges_by_design() {
    // decode_internal UTF-8-validates every string; the single-pass path
    // validates skipped fields structurally only.
    const SCH: &str = r#"{"type":"record","name":"T","fields":[
        {"name":"skip","type":"string"},
        {"name":"keep","type":"long"}]}"#;
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct T {
        keep: i64,
    }
    // skip: a 2-byte string holding invalid UTF-8; keep: 5.
    let datum = [0x04, 0xFF, 0xFE, 0x0A];

    let b = builder(SCH);
    let (ack, _rx) = AckRef::test_pair();

    let mut two_pass = Collected::<T>(Vec::new());
    b.build_serde::<T>()
        .unwrap()
        .deserialize(&raw(&datum), &ack, &mut two_pass)
        .unwrap_err();
    assert!(two_pass.0.is_empty());

    let mut single_pass = Collected::<T>(Vec::new());
    b.build_serde_datum::<T>()
        .unwrap()
        .deserialize(&raw(&datum), &ack, &mut single_pass)
        .expect("the single-pass path skips the field structurally");
    assert_eq!(single_pass.0[0].payload, T { keep: 5 });
}

#[test]
fn zero_width_item_bomb_diverges_by_design() {
    // A four-byte payload claiming 100 000 null items: the two-pass path
    // walks it into 100 000 values, the single-pass path errors on the
    // per-datum collection-item budget.
    const SCH: &str = r#"{"type":"array","items":"null"}"#;
    let datum = to_avro_datum(
        &Schema::parse_str(SCH).unwrap(),
        Value::Array(vec![Value::Null; 100_000]),
    )
    .unwrap();

    let b = builder(SCH);
    let (ack, _rx) = AckRef::test_pair();

    let mut two_pass = Collected::<Vec<()>>(Vec::new());
    b.build_serde::<Vec<()>>()
        .unwrap()
        .deserialize(&raw(&datum), &ack, &mut two_pass)
        .expect("the two-pass path walks the bomb");
    assert_eq!(two_pass.0[0].payload.len(), 100_000);

    let mut single_pass = Collected::<Vec<()>>(Vec::new());
    let err = b
        .build_serde_datum::<Vec<()>>()
        .unwrap()
        .deserialize(&raw(&datum), &ack, &mut single_pass)
        .unwrap_err();
    assert!(
        matches!(err, spate_core::error::DeserError::Malformed { .. }),
        "{err}"
    );
    assert!(single_pass.0.is_empty());
}

#[test]
fn union_into_map_target_diverges_by_design() {
    // The uniform-superset direction: `from_value`'s `deserialize_map`
    // refuses a union value where every single-pass method unwraps the
    // branch, so this shape decodes only on the single-pass path.
    const SCH: &str = r#"["null",{"type":"map","values":"long"}]"#;
    let datum = to_avro_datum(
        &Schema::parse_str(SCH).unwrap(),
        Value::Union(
            1,
            Box::new(Value::Map(HashMap::from([("a".into(), Value::Long(1))]))),
        ),
    )
    .unwrap();

    let b = builder(SCH);
    let (ack, _rx) = AckRef::test_pair();

    let mut two_pass = Collected::<HashMap<String, i64>>(Vec::new());
    b.build_serde::<HashMap<String, i64>>()
        .unwrap()
        .deserialize(&raw(&datum), &ack, &mut two_pass)
        .unwrap_err();

    let mut single_pass = Collected::<HashMap<String, i64>>(Vec::new());
    b.build_serde_datum::<HashMap<String, i64>>()
        .unwrap()
        .deserialize(&raw(&datum), &ack, &mut single_pass)
        .expect("the single-pass path unwraps the union");
    assert_eq!(
        single_pass.0[0].payload,
        HashMap::from([("a".to_owned(), 1)])
    );
}

#[test]
fn garbage_errors_on_both_paths() {
    let b = builder(SENSOR_BATCH);
    let (ack, _rx) = AckRef::test_pair();
    let garbage = [0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
    let mut out = Collected::<SensorBatch>(Vec::new());
    b.build_serde::<SensorBatch>()
        .unwrap()
        .deserialize(&raw(&garbage), &ack, &mut out)
        .unwrap_err();
    b.build_serde_datum::<SensorBatch>()
        .unwrap()
        .deserialize(&raw(&garbage), &ack, &mut out)
        .unwrap_err();
    assert!(out.0.is_empty());
}
