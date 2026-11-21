//! The bounds the value and serde paths check before apache-avro decodes a
//! datum: 32 levels of nesting, the per-datum collection-item budget, and
//! lengths within the bytes present. The datum path keeps its own limits.

#![expect(deprecated, reason = "fixtures encode with to_avro_datum")]

use apache_avro::types::Value;
use apache_avro::{BigDecimal, Days, Decimal, Duration, Millis, Months, Schema, to_avro_datum};
use spate_avro::{AvroDeserializerBuilder, AvroDeserializerConfig, SchemaSource};
use spate_core::deser::{Deserializer, Owned};
use spate_core::error::DeserError;
use spate_test::{EmitCollector, raw_payload, test_ack};

fn builder(schema: &str, reader: Option<&str>) -> AvroDeserializerBuilder {
    let mut settings = AvroDeserializerConfig::raw(SchemaSource::inline(schema));
    settings.reader_schema = reader.map(SchemaSource::inline);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    AvroDeserializerBuilder::new(settings, rt.handle()).unwrap()
}

/// Decode one payload, returning the records or the error.
fn decode<T: Send + 'static>(
    deser: &mut dyn Deserializer<Owned<T>>,
    datum: &[u8],
) -> Result<Vec<T>, DeserError> {
    let mut out = EmitCollector::<T>::new();
    deser.deserialize(&raw_payload(datum), &test_ack(), &mut out)?;
    Ok(out.records.into_iter().map(|r| r.payload).collect())
}

/// The refusal's reason, which must be a malformed-record error.
fn refusal<T: Send + 'static + std::fmt::Debug>(
    deser: &mut dyn Deserializer<Owned<T>>,
    datum: &[u8],
) -> String {
    match decode(deser, datum) {
        Err(DeserError::Malformed { reason }) => reason,
        other => panic!("expected a malformed-record error, got {other:?}"),
    }
}

/// Avro's zigzag varint encoding of a long.
fn zig(n: i64) -> Vec<u8> {
    let mut z = ((n << 1) ^ (n >> 63)).cast_unsigned();
    let mut out = Vec::new();
    while z > 0x7F {
        out.push(0x80 | (z & 0x7F) as u8);
        z >>= 7;
    }
    out.push(z as u8);
    out
}

/// `arrays` arrays nested around `leaf`, a schema in JSON.
fn nested(arrays: usize, leaf: &str) -> String {
    let mut schema = leaf.to_owned();
    for _ in 0..arrays {
        schema = format!(r#"{{"type":"array","items":{schema}}}"#);
    }
    schema
}

/// `value` inside `arrays` arrays of one item each, encoded under `schema`.
fn nested_datum(schema: &str, arrays: usize, value: Value) -> Vec<u8> {
    let mut value = value;
    for _ in 0..arrays {
        value = Value::Array(vec![value]);
    }
    to_avro_datum(&Schema::parse_str(schema).unwrap(), value).unwrap()
}

const DEPTH_REFUSAL: &str = "datum nesting exceeds the depth limit of 32";

/// A linked list: an ordinary recursive schema.
const LIST: &str = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"]}]}"#;

/// The list with a defaulted field added, so a read resolves the whole tree.
const LIST_READER: &str = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"tag","type":"string","default":""},{"name":"next","type":["null","Node"]}]}"#;

#[derive(Debug, serde::Deserialize)]
struct Node {
    #[expect(dead_code, reason = "decoded for its depth")]
    value: i64,
    next: Option<Box<Node>>,
}

impl Node {
    fn len(&self) -> usize {
        1 + self.next.as_ref().map_or(0, |n| n.len())
    }
}

/// A list of `nodes` nodes: each a zero long and the union index of `Node`,
/// then the trailing `null` branch.
/// A linked list whose nodes also hold bytes.
const PADDED: &str = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"pad","type":"bytes"},{"name":"next","type":["null","Node"]}]}"#;

/// The padded list with a defaulted field added, so a read resolves the
/// whole tree.
const PADDED_READER: &str = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"pad","type":"bytes"},{"name":"tag","type":"string","default":""},{"name":"next","type":["null","Node"]}]}"#;

/// `nodes` nodes of [`PADDED`], the first holding `head` bytes of pad.
fn padded(nodes: usize, head: usize) -> Vec<u8> {
    fn node(nodes: usize, pad: usize) -> Value {
        let next = match nodes {
            1 => Value::Union(0, Box::new(Value::Null)),
            _ => Value::Union(1, Box::new(node(nodes - 1, 0))),
        };
        Value::Record(vec![
            ("value".into(), Value::Long(0)),
            ("pad".into(), Value::Bytes(vec![0; pad])),
            ("next".into(), next),
        ])
    }
    to_avro_datum(&Schema::parse_str(PADDED).unwrap(), node(nodes, head)).unwrap()
}

fn list(nodes: usize) -> Vec<u8> {
    let mut datum = Vec::new();
    for _ in 1..nodes {
        datum.extend([0x00, 0x02]);
    }
    datum.extend([0x00, 0x00]);
    datum
}

#[test]
fn thirty_two_levels_decode_and_thirty_three_are_refused() {
    for (levels, refused) in [(31, false), (32, true)] {
        let arrays = nested(levels, r#""long""#);
        let array_datum = nested_datum(&arrays, levels, Value::Long(7));
        let maps = nested_maps(levels);
        let mut map_datum = Vec::new();
        for _ in 0..levels {
            map_datum.extend([0x02, 0x02, b'k']);
        }
        map_datum.push(0x0e);
        map_datum.resize(map_datum.len() + levels, 0x00);
        for (schema, datum) in [(arrays, array_datum), (maps, map_datum)] {
            let mut deser = builder(&schema, None).build_value();
            if refused {
                assert!(
                    refusal(&mut deser, &datum).contains(DEPTH_REFUSAL),
                    "{schema}"
                );
            } else {
                assert_eq!(decode(&mut deser, &datum).unwrap().len(), 1, "{schema}");
            }
        }
    }
}

/// `maps` maps nested around `long`, a schema in JSON.
fn nested_maps(maps: usize) -> String {
    let mut schema = r#""long""#.to_owned();
    for _ in 0..maps {
        schema = format!(r#"{{"type":"map","values":{schema}}}"#);
    }
    schema
}

#[test]
fn a_ten_node_list_decodes_and_eleven_are_refused() {
    // A node is three levels (record, union, named reference), so 32 levels
    // hold ten nodes.
    let mut value = builder(LIST, None).build_value();
    let mut serde = builder(LIST, None).build_serde::<Node>();
    assert_eq!(decode(&mut value, &list(10)).unwrap().len(), 1);
    assert_eq!(decode(&mut serde, &list(10)).unwrap()[0].len(), 10);
    assert!(refusal(&mut value, &list(11)).contains(DEPTH_REFUSAL));
    assert!(refusal(&mut serde, &list(11)).contains(DEPTH_REFUSAL));
}

#[test]
fn decimal_and_big_decimal_count_a_level_and_uuid_does_not() {
    // apache-avro decodes a decimal's and a big-decimal's bytes in a nested
    // decode, and reads a uuid in place.
    let decimal = r#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}"#;
    let big = r#"{"type":"bytes","logicalType":"big-decimal"}"#;
    let uuid = r#"{"type":"string","logicalType":"uuid"}"#;
    let one: BigDecimal = "1.5".parse().unwrap();
    let cases = [
        (decimal, Value::Decimal(Decimal::from(vec![1u8])), 30, false),
        (decimal, Value::Decimal(Decimal::from(vec![1u8])), 31, true),
        (big, Value::BigDecimal(one.clone()), 30, false),
        (big, Value::BigDecimal(one), 31, true),
        (
            uuid,
            Value::Uuid(apache_avro::Uuid::from_u128(7)),
            31,
            false,
        ),
    ];
    for (leaf, value, arrays, refused) in cases {
        let schema = nested(arrays, leaf);
        let datum = nested_datum(&schema, arrays, value);
        let mut deser = builder(&schema, None).build_value();
        if refused {
            assert!(
                refusal(&mut deser, &datum).contains(DEPTH_REFUSAL),
                "{leaf} under {arrays} arrays"
            );
        } else {
            assert_eq!(
                decode(&mut deser, &datum).unwrap().len(),
                1,
                "{leaf} under {arrays} arrays"
            );
        }
    }
}

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the test joins the thread it spawns"
)]
fn the_deepest_datum_decodes_on_a_two_mib_stack() {
    // Pipeline threads have std's default 2 MiB stack.
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(|| {
            let mut value = builder(LIST, None).build_value();
            let mut resolved = builder(LIST, Some(LIST_READER)).build_value();
            let mut serde = builder(LIST, None).build_serde::<Node>();
            assert_eq!(decode(&mut value, &list(10)).unwrap().len(), 1);
            // The deepest resolve is ten nodes, past the resolution limit's
            // nine unless the datum's content buys the doubling: 64 KiB on
            // the head node does.
            assert!(
                refusal(&mut resolved, &list(10)).contains("exceeds the resolution limit"),
                "{}",
                refusal(&mut resolved, &list(10))
            );
            let mut padded_reader = builder(PADDED, Some(PADDED_READER)).build_value();
            assert_eq!(
                decode(&mut padded_reader, &padded(10, 64 << 10))
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(decode(&mut serde, &list(10)).unwrap()[0].len(), 10);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn a_size_prefixed_block_is_walked_item_by_item() {
    // A negative count is followed by the block's byte size, which the
    // decode ignores, so the check walks the items too.
    let mut nulls = builder(r#"{"type":"array","items":"null"}"#, None).build_value();
    let claim = [zig(-(1 << 20)), zig(0), vec![0x00]].concat();
    assert!(refusal(&mut nulls, &claim).contains("item budget"));

    let inner = nested(31, r#""long""#);
    let schema = format!(r#"{{"type":"array","items":{inner}}}"#);
    let item = nested_datum(&inner, 31, Value::Long(7));
    let sized = [zig(-1), zig(item.len() as i64), item, vec![0x00]].concat();
    let mut deep = builder(&schema, None).build_value();
    assert!(refusal(&mut deep, &sized).contains(DEPTH_REFUSAL));
}

#[test]
fn the_item_budget_admits_its_floor_and_refuses_one_more() {
    let mut nulls = builder(r#"{"type":"array","items":"null"}"#, None).build_value();
    let at = [zig(1 << 16), vec![0x00]].concat();
    assert_eq!(
        decode(&mut nulls, &at).unwrap(),
        [Value::Array(vec![Value::Null; 1 << 16])]
    );
    let over = [zig((1 << 16) + 1), vec![0x00]].concat();
    assert!(refusal(&mut nulls, &over).contains("item budget"));

    let mut map = builder(r#"{"type":"map","values":"null"}"#, None).build_value();
    let claim = [zig(1 << 20), vec![0x00]].concat();
    assert!(refusal(&mut map, &claim).contains("item budget"));
}

#[test]
fn a_declared_length_past_the_datum_is_refused_before_the_decode() {
    // apache-avro allocates a declared string length before it reads the
    // bytes, so the check refuses it first.
    let mut deser = builder(
        r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#,
        None,
    )
    .build_value();
    let reason = refusal(&mut deser, &zig(400 << 20));
    assert!(reason.contains("needed 419430400 bytes"), "{reason}");
}

#[test]
fn duration_and_big_decimal_decode_with_the_fields_after_them() {
    let schema = r#"{"type":"record","name":"R","fields":[
        {"name":"d","type":{"type":"fixed","name":"D","size":12,"logicalType":"duration"}},
        {"name":"b","type":{"type":"bytes","logicalType":"big-decimal"}},
        {"name":"after","type":"long"}]}"#;
    let value = Value::Record(vec![
        (
            "d".into(),
            Value::Duration(Duration::new(Months::new(1), Days::new(2), Millis::new(3))),
        ),
        ("b".into(), Value::BigDecimal("12.75".parse().unwrap())),
        ("after".into(), Value::Long(7)),
    ]);
    let datum = to_avro_datum(&Schema::parse_str(schema).unwrap(), value.clone()).unwrap();
    let mut deser = builder(schema, None).build_value();
    assert_eq!(decode(&mut deser, &datum).unwrap(), [value]);
}

#[test]
fn the_item_budget_holds_after_a_duration() {
    // A walk that reads a `duration` short lands inside it and misses the claim.
    let schema = r#"{"type":"record","name":"R","fields":[
        {"name":"d","type":{"type":"fixed","name":"D","size":12,"logicalType":"duration"}},
        {"name":"a","type":{"type":"array","items":"null"}}]}"#;
    let datum = [vec![0; 12], zig(1 << 20), vec![0x00]].concat();
    let mut deser = builder(schema, None).build_value();
    assert!(refusal(&mut deser, &datum).contains("item budget"));
}

/// The decoded-size budget of a datum under 64 KiB: 1,024 bytes for each
/// byte of the item budget's 65,536 floor.
const BUILT_FLOOR: u64 = 65_536 * 1024;

/// What the check charges for one value, and for a record field's or map
/// key's `String`, before its bytes.
const VALUE: u64 = 56;
const STRING: u64 = 24;

/// An array of records, each one `null` field whose name is `name_len`
/// long.
fn wide(name_len: usize) -> String {
    format!(
        r#"{{"type":"array","items":{{"type":"record","name":"W","fields":[{{"name":"{}","type":"null"}}]}}}}"#,
        "f".repeat(name_len)
    )
}

/// A datum of `items` items for [`wide`], 4 bytes on the wire, and what the
/// check charges for it: the array, then per item the record, its field and
/// the `null`.
fn wide_datum(name_len: usize, items: u64) -> (Vec<u8>, u64) {
    let built = VALUE + items * (VALUE + STRING + name_len as u64 + VALUE);
    ([zig(items as i64), vec![0x00]].concat(), built)
}

fn over_budget(reason: &str) -> bool {
    reason.contains(&format!("decodes to more than {BUILT_FLOOR} bytes"))
}

#[test]
fn the_byte_budget_admits_its_floor_and_refuses_one_more() {
    // 887 and 888 put the datum either side of the floor.
    let (datum, built) = wide_datum(887, 65_536);
    assert!(built <= BUILT_FLOOR);
    let mut deser = builder(&wide(887), None).build_value();
    assert_eq!(decode(&mut deser, &datum).unwrap().len(), 1);

    let (datum, built) = wide_datum(888, 65_536);
    assert!(built > BUILT_FLOOR);
    let mut deser = builder(&wide(888), None).build_value();
    assert!(over_budget(&refusal(&mut deser, &datum)));
    let mut serde = builder(&wide(888), None).build_serde::<Vec<serde::de::IgnoredAny>>();
    assert!(over_budget(&refusal(&mut serde, &datum)));
}

#[test]
fn the_byte_budget_scales_with_the_payload_past_its_floor() {
    // 70,000 one-byte booleans: a 70,004-byte datum, so the budget is 1,024
    // times that, above the floor. A field name of 888 fits it, and 889 does
    // not.
    let schema = |n: usize| {
        format!(
            r#"{{"type":"array","items":{{"type":"record","name":"W","fields":[{{"name":"{}","type":"boolean"}}]}}}}"#,
            "f".repeat(n)
        )
    };
    let items = 70_000_u64;
    let datum = [zig(items as i64), vec![0x00; items as usize], vec![0x00]].concat();
    let budget = datum.len() as u64 * 1024;
    let built = |n: u64| VALUE + items * (VALUE + STRING + n + VALUE);
    assert!(BUILT_FLOOR < built(888) && built(888) <= budget && budget < built(889));

    let mut deser = builder(&schema(888), None).build_value();
    assert_eq!(decode(&mut deser, &datum).unwrap().len(), 1);
    let mut deser = builder(&schema(889), None).build_value();
    let reason = refusal(&mut deser, &datum);
    assert!(
        reason.contains(&format!("decodes to more than {budget} bytes")),
        "{reason}"
    );
}

#[test]
fn each_decoded_byte_counts_against_the_byte_budget() {
    // A record of `pad`, one item short of the item budget, then `x`. The
    // budget left for `x`'s value is `slack`, and each case's `x` costs
    // `base + n`: `n` at the slack is admitted, and one more is refused.
    let pad = wide(888);
    let (pad_datum, pad_built) = wide_datum(888, 65_535);
    let slack = BUILT_FLOOR - (VALUE + STRING + 3 + pad_built + STRING + 1);
    type Case = (&'static str, u64, fn(usize) -> (String, Vec<u8>));
    let cases: [Case; 6] = [
        ("string", VALUE, |n| {
            (
                r#""string""#.into(),
                [zig(n as i64), vec![b'a'; n]].concat(),
            )
        }),
        ("fixed", VALUE, |n| {
            (
                format!(r#"{{"type":"fixed","name":"F","size":{n}}}"#),
                vec![0; n],
            )
        }),
        ("decimal", VALUE, |n| {
            (
                r#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}"#.into(),
                [zig(n as i64), vec![0; n]].concat(),
            )
        }),
        ("big-decimal", VALUE, |n| {
            (
                r#"{"type":"bytes","logicalType":"big-decimal"}"#.into(),
                [zig(n as i64), vec![0; n]].concat(),
            )
        }),
        ("enum symbol", VALUE, |n| {
            (
                format!(
                    r#"{{"type":"enum","name":"E","symbols":["{}"]}}"#,
                    "s".repeat(n)
                ),
                zig(0),
            )
        }),
        ("map key", VALUE + STRING + VALUE, |n| {
            (
                r#"{"type":"map","values":"null"}"#.into(),
                [zig(1), zig(n as i64), vec![b'k'; n], vec![0x00]].concat(),
            )
        }),
    ];
    for (label, base, case) in cases {
        let n = usize::try_from(slack - base).unwrap();
        for (n, refused) in [(n, false), (n + 1, true)] {
            let (x, x_datum) = case(n);
            let schema = format!(
                r#"{{"type":"record","name":"R","fields":[{{"name":"pad","type":{pad}}},{{"name":"x","type":{x}}}]}}"#
            );
            let datum = [pad_datum.clone(), x_datum].concat();
            assert!(datum.len() <= 65_536, "{label}: the floor sets the budget");
            let mut deser = builder(&schema, None).build_value();
            let got = match decode(&mut deser, &datum) {
                Ok(_) => false,
                Err(DeserError::Malformed { reason }) => over_budget(&reason),
                Err(e) => panic!("{label}: {e}"),
            };
            assert_eq!(got, refused, "{label} of {n} bytes");
        }
    }
}

/// A reader-schema case: the writer schema, the reader schema for `n`, one
/// item's bytes, what the datum charges outside its 65,536 items, and what
/// each item charges besides `n`.
struct ReaderCase {
    label: &'static str,
    writer: String,
    reader: fn(usize) -> String,
    item: &'static [u8],
    fixed: u64,
    base: u64,
}

/// The writer item every case but two uses, and what it charges: the
/// record, its field and the `null`.
const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"f","type":"null"}]}"#;
const W_ITEM: u64 = VALUE + STRING + 1 + VALUE;

/// Reader record `W`, adding a string field with an `n`-byte default: its
/// two field names, and the default. `f` has a default too, which the writer
/// supplies, so it is not charged.
fn w_with_default(n: usize) -> String {
    format!(
        r#"{{"type":"record","name":"W","fields":[{{"name":"f","type":"null","default":null}},{{"name":"s","type":"string","default":"{}"}}]}}"#,
        "a".repeat(n)
    )
}
const W_DEFAULT: u64 = STRING + 1 + STRING + 1 + VALUE;

/// Reader record `W`, adding a field `s` of `schema` with the JSON `default`.
fn w_with(schema: &str, default: &str) -> String {
    format!(
        r#"{{"type":"record","name":"W","fields":[{{"name":"f","type":"null","default":null}},{{"name":"s","type":{schema},"default":{default}}}]}}"#
    )
}

/// A named record wider than any case's `W`, which a writer `W` resolves into
/// when it comes first in a union.
const BIG: &str =
    r#"{"type":"record","name":"Big","fields":[{"name":"x","type":"string","default":"BIG"}]}"#;

fn big() -> String {
    BIG.replace("BIG", &"x".repeat(5000))
}

fn array_of(items: &str) -> String {
    format!(r#"{{"type":"array","items":{items}}}"#)
}

fn reader_cases() -> Vec<ReaderCase> {
    vec![
        ReaderCase {
            label: "string default",
            writer: array_of(W),
            reader: |n| array_of(&w_with_default(n)),
            item: &[],
            fixed: VALUE,
            base: W_ITEM + W_DEFAULT,
        },
        ReaderCase {
            label: "null union default",
            writer: array_of(W),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"f","type":"null"}},{{"name":"{}","type":["null","string"],"default":null}}]}}"#,
                    "u".repeat(n)
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + STRING + 1 + STRING + VALUE + VALUE,
        },
        ReaderCase {
            label: "array default",
            writer: array_of(W),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"f","type":"null"}},{{"name":"a","type":{{"type":"array","items":"string"}},"default":["{}"]}}]}}"#,
                    "a".repeat(n)
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + STRING + 1 + STRING + 1 + VALUE + VALUE,
        },
        ReaderCase {
            label: "record default",
            writer: array_of(W),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"f","type":"null"}},{{"name":"r","type":{{"type":"record","name":"D","fields":[{{"name":"k","type":"string"}}]}},"default":{{"k":"{}"}}}}]}}"#,
                    "a".repeat(n)
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + STRING + 1 + STRING + 1 + VALUE + STRING + 1 + VALUE,
        },
        ReaderCase {
            label: "field wrapped in a union",
            writer: array_of(
                r#"{"type":"record","name":"W","fields":[{"name":"f","type":"long"}]}"#,
            ),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"f","type":["null","long"]}},{{"name":"s","type":"string","default":"{}"}}]}}"#,
                    "a".repeat(n)
                ))
            },
            item: &[0x00],
            fixed: VALUE,
            base: VALUE + STRING + 1 + VALUE + VALUE + W_DEFAULT,
        },
        ReaderCase {
            label: "enum symbols",
            writer: array_of(
                r#"{"type":"record","name":"W","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}}]}"#,
            ),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"e","type":{{"type":"enum","name":"E","symbols":["A","{}"]}}}}]}}"#,
                    "B".repeat(n)
                ))
            },
            item: &[0x00],
            fixed: VALUE,
            base: VALUE + STRING + 1 + VALUE + 1 + STRING + 1,
        },
        ReaderCase {
            label: "the only record of its kind",
            writer: array_of(W),
            reader: |n| {
                array_of(&format!(
                    r#"["null",{{"type":"record","name":"V","fields":[{{"name":"f","type":"null"}},{{"name":"s","type":"string","default":"{}"}},{{"name":"b","type":["null",{}],"default":null}}]}}]"#,
                    "a".repeat(n),
                    big()
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + VALUE + W_DEFAULT + STRING + 1 + VALUE + VALUE,
        },
        ReaderCase {
            label: "named reference",
            writer: format!(
                r#"{{"type":"record","name":"T","fields":[{{"name":"a","type":{}}}]}}"#,
                array_of(W)
            ),
            reader: |n| {
                format!(
                    r#"{{"type":"record","name":"T","fields":[{{"name":"w0","type":["null",{}],"default":null}},{{"name":"b","type":["null",{}],"default":null}},{{"name":"a","type":{{"type":"array","items":"W"}}}}]}}"#,
                    w_with_default(n),
                    big()
                )
            },
            item: &[],
            // The writer's `T`, its field and the array; the reader's three
            // field names and its two `null` defaults.
            fixed: VALUE
                + STRING
                + 1
                + VALUE
                + (STRING + 2)
                + (STRING + 1)
                + (STRING + 1)
                + 2 * (VALUE + VALUE),
            base: W_ITEM + W_DEFAULT,
        },
        ReaderCase {
            label: "map values",
            writer: format!(r#"{{"type":"map","values":{W}}}"#),
            reader: |n| format!(r#"{{"type":"map","values":{}}}"#, w_with_default(n)),
            // An empty key per entry.
            item: &[0x00],
            fixed: VALUE,
            base: STRING + W_ITEM + W_DEFAULT,
        },
        ReaderCase {
            label: "map into a record",
            writer: array_of(r#"{"type":"map","values":"null"}"#),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"R","fields":[{{"name":"s","type":"string","default":"{}"}}]}}"#,
                    "a".repeat(n)
                ))
            },
            // An empty map.
            item: &[0x00],
            fixed: VALUE,
            base: VALUE + STRING + 1 + VALUE,
        },
        ReaderCase {
            label: "string into an enum's default symbol",
            writer: array_of(r#""string""#),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"enum","name":"E","symbols":["A","{0}"],"default":"{0}"}}"#,
                    "B".repeat(n)
                ))
            },
            // An empty string.
            item: &[0x00],
            fixed: VALUE,
            base: VALUE,
        },
        ReaderCase {
            label: "nested record default",
            writer: array_of(W),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"f","type":"null"}},{{"name":"r","type":{{"type":"record","name":"D","fields":[{{"name":"s","type":"string","default":"{}"}}]}},"default":{{}}}}]}}"#,
                    "a".repeat(n)
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + STRING + 1 + STRING + 1 + VALUE + STRING + 1 + VALUE,
        },
        ReaderCase {
            label: "union default of its first variant",
            writer: array_of(W),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"f","type":"null"}},{{"name":"u","type":["string","null"],"default":"{}"}}]}}"#,
                    "a".repeat(n)
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + STRING + 1 + STRING + 1 + VALUE + VALUE,
        },
        ReaderCase {
            label: "bytes default",
            writer: array_of(W),
            reader: |n| array_of(&w_with(r#""bytes""#, &format!(r#""{}""#, "a".repeat(n)))),
            item: &[],
            fixed: VALUE,
            base: W_ITEM + W_DEFAULT,
        },
        ReaderCase {
            label: "fixed default",
            writer: array_of(W),
            reader: |n| {
                array_of(&w_with(
                    &format!(r#"{{"type":"fixed","name":"X","size":{n}}}"#),
                    &format!(r#""{}""#, "a".repeat(n)),
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + W_DEFAULT,
        },
        ReaderCase {
            label: "decimal default",
            writer: array_of(W),
            reader: |n| {
                array_of(&w_with(
                    r#"{"type":"bytes","logicalType":"decimal","precision":1,"scale":0}"#,
                    &format!(r#""{}""#, "a".repeat(n)),
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + W_DEFAULT,
        },
        ReaderCase {
            label: "map default",
            writer: array_of(W),
            reader: |n| {
                array_of(&w_with(
                    r#"{"type":"map","values":"null"}"#,
                    &format!(r#"{{"{}":null}}"#, "k".repeat(n)),
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + W_DEFAULT + STRING + VALUE,
        },
        ReaderCase {
            label: "map into a union's only record",
            writer: format!(
                r#"{{"type":"record","name":"T","fields":[{{"name":"a","type":{}}}]}}"#,
                array_of(r#"{"type":"map","values":"null"}"#)
            ),
            reader: |n| {
                format!(
                    r#"{{"type":"record","name":"T","fields":[{{"name":"a","type":{}}},{{"name":"b","type":["null",{}],"default":null}}]}}"#,
                    array_of(&format!(
                        r#"["null",{{"type":"record","name":"R","fields":[{{"name":"s","type":"string","default":"{}"}}]}}]"#,
                        "a".repeat(n)
                    )),
                    big()
                )
            },
            item: &[0x00],
            // The writer's `T`, its field and the array; the reader's two
            // field names and the `null` default.
            fixed: VALUE + STRING + 1 + VALUE + (STRING + 1) + (STRING + 1) + 2 * VALUE,
            base: VALUE + VALUE + STRING + 1 + VALUE,
        },
        ReaderCase {
            label: "string into a union's enum",
            writer: array_of(r#""string""#),
            reader: |n| {
                array_of(&format!(
                    r#"["null",{{"type":"enum","name":"E","symbols":["A","{0}"],"default":"{0}"}}]"#,
                    "B".repeat(n)
                ))
            },
            item: &[0x00],
            fixed: VALUE,
            base: VALUE + VALUE,
        },
        ReaderCase {
            label: "the enum's own symbols",
            writer: array_of(
                r#"{"type":"record","name":"W","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}}]}"#,
            ),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"e","type":{{"type":"enum","name":"E","symbols":["A","{}"]}}}},{{"name":"g","type":["null",{{"type":"enum","name":"G","symbols":["{}"]}}],"default":null}}]}}"#,
                    "B".repeat(n),
                    "C".repeat(5000)
                ))
            },
            item: &[0x00],
            fixed: VALUE,
            base: VALUE + STRING + 1 + VALUE + 1 + STRING + 1 + STRING + 1 + 2 * VALUE,
        },
        ReaderCase {
            label: "a writer field named as a reader alias",
            writer: array_of(W),
            reader: |n| {
                array_of(&format!(
                    r#"{{"type":"record","name":"W","fields":[{{"name":"s","type":"string","default":"{}","aliases":["f"]}}]}}"#,
                    "a".repeat(n)
                ))
            },
            item: &[],
            fixed: VALUE,
            base: W_ITEM + STRING + 1 + VALUE,
        },
        ReaderCase {
            label: "writer union",
            writer: array_of(&format!(r#"["null",{W}]"#)),
            reader: |n| array_of(&format!(r#"["null",{}]"#, w_with_default(n))),
            item: &[0x02],
            fixed: VALUE,
            base: VALUE + W_ITEM + W_DEFAULT,
        },
    ]
}

/// What one decoded `Value` takes: the value, its string and byte contents,
/// and its field names and map keys.
fn value_bytes(value: &Value) -> u64 {
    VALUE
        + match value {
            Value::String(s) | Value::Enum(_, s) => s.len() as u64,
            Value::Bytes(b) | Value::Fixed(_, b) => b.len() as u64,
            Value::Array(items) => items.iter().map(value_bytes).sum(),
            Value::Record(fields) => fields
                .iter()
                .map(|(k, v)| STRING + k.len() as u64 + value_bytes(v))
                .sum(),
            Value::Map(entries) => entries
                .iter()
                .map(|(k, v)| STRING + k.len() as u64 + value_bytes(v))
                .sum(),
            Value::Union(_, inner) => value_bytes(inner),
            _ => 0,
        }
}

#[test]
fn a_reader_schema_charges_at_least_what_one_item_resolves_to() {
    // The check charges an item its writer value and what resolution adds,
    // so apache-avro's resolved item must not take more.
    for case in reader_cases() {
        let n = 100;
        let datum = [zig(1), case.item.to_vec(), vec![0x00]].concat();
        let mut deser = builder(&case.writer, Some(&(case.reader)(n))).build_value();
        let out = decode(&mut deser, &datum).unwrap_or_else(|e| panic!("{}: {e}", case.label));
        let item = match &out[0] {
            Value::Record(fields) => &fields.iter().find(|(k, _)| k == "a").unwrap().1,
            other => other,
        };
        let one = match item {
            Value::Array(items) => &items[0],
            Value::Map(entries) => entries.values().next().unwrap(),
            other => panic!("{}: {other:?}", case.label),
        };
        let charged = case.base + n as u64;
        assert!(
            value_bytes(one) <= charged,
            "{}: resolved {} bytes, charged {charged}: {one:?}",
            case.label,
            value_bytes(one)
        );
    }
}

#[test]
fn a_reader_schema_counts_what_resolution_builds() {
    // 65,536 items of each case, with `n` at the budget and one past it.
    for case in reader_cases() {
        let mut datum = zig(1 << 16);
        for _ in 0..1 << 16 {
            datum.extend_from_slice(case.item);
        }
        datum.push(0x00);
        let budget = (datum.len() as u64).max(1 << 16) * 1024;
        let n = (budget - case.fixed) / 65_536 - case.base;
        let n = usize::try_from(n).unwrap();
        for (n, refused) in [(n, false), (n + 1, true)] {
            let reader = (case.reader)(n);
            let mut deser = builder(&case.writer, Some(&reader)).build_value();
            let got = match decode(&mut deser, &datum) {
                Ok(_) => false,
                Err(DeserError::Malformed { reason }) => {
                    reason.contains(&format!("decodes to more than {budget} bytes"))
                }
                Err(e) => panic!("{}: {e}", case.label),
            };
            assert_eq!(got, refused, "{} with n = {n}", case.label);
        }
    }
}

#[test]
fn a_writer_with_a_field_and_its_reader_alias_decodes() {
    // apache-avro matches `s` by name and drops `f`, so the alias matches
    // nothing.
    let writer = r#"{"type":"record","name":"W","fields":[{"name":"f","type":"string"},{"name":"s","type":"string"}]}"#;
    let reader = r#"{"type":"record","name":"W","fields":[{"name":"s","type":"string","default":"d","aliases":["f"]}]}"#;
    let mut deser = builder(writer, Some(reader)).build_value();
    let out = decode(&mut deser, &[0x04, b'h', b'i', 0x04, b'y', b'o']).unwrap();
    assert_eq!(
        out,
        [Value::Record(vec![(
            "s".into(),
            Value::String("yo".into())
        )])]
    );
}

#[test]
fn a_reader_union_of_two_records_is_charged_as_its_widest() {
    // apache-avro resolves a record into the first named variant it fits,
    // whatever its name, so `W` resolves into `Big` and takes its default.
    let reader = array_of(&format!(
        r#"[{},{{"type":"record","name":"W","fields":[{{"name":"f","type":"null"}}]}}]"#,
        BIG.replace("BIG", &"x".repeat(1000))
    ));
    let mut deser = builder(&array_of(W), Some(&reader)).build_value();
    let one = decode(&mut deser, &[zig(1), vec![0x00]].concat()).unwrap();
    let Value::Array(items) = &one[0] else {
        panic!("{one:?}")
    };
    let Value::Union(0, record) = &items[0] else {
        panic!("{items:?}")
    };
    let Value::Record(fields) = record.as_ref() else {
        panic!("{record:?}")
    };
    assert_eq!(fields[0].0, "x");

    // Charged as `W`, 65,536 items would fit the floor. Charged as `Big`, they
    // do not.
    let datum = [zig(1 << 16), vec![0x00]].concat();
    let as_w = VALUE + 65_536 * (W_ITEM + VALUE + STRING + 1);
    assert!(as_w <= BUILT_FLOOR);
    assert!(over_budget(&refusal(&mut deser, &datum)));
}

#[test]
fn a_duration_schema_resolves_its_named_references() {
    // The datum path refuses a `duration` schema and keeps no name index for
    // it, so the check builds its own.
    let schema = r#"{"type":"record","name":"N","fields":[
        {"name":"d","type":{"type":"fixed","name":"D","size":12,"logicalType":"duration"}},
        {"name":"next","type":["null","N"]}]}"#;
    let datum = [vec![0; 12], vec![0x02], vec![0; 12], vec![0x00]].concat();
    let mut deser = builder(schema, None).build_value();
    assert_eq!(decode(&mut deser, &datum).unwrap().len(), 1);
}

#[derive(Debug, serde::Deserialize)]
struct Keep {
    keep: i64,
}

/// A record whose `deep` field the [`Keep`] target ignores.
fn ignored(deep: &str) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"keep","type":"long"}},{{"name":"deep","type":{deep}}}]}}"#
    )
}

#[test]
fn the_datum_path_keeps_its_own_limits() {
    // Forty levels in an ignored field: refused on the value path, skipped on
    // the datum path.
    let deep = nested(40, r#""long""#);
    let schema = ignored(&deep);
    let mut datum = zig(5);
    datum.extend(nested_datum(&deep, 40, Value::Long(7)));
    let mut value = builder(&schema, None).build_value();
    assert!(refusal(&mut value, &datum).contains(DEPTH_REFUSAL));
    let mut skip = builder(&schema, None).build_serde_datum::<Keep>().unwrap();
    assert_eq!(decode(&mut skip, &datum).unwrap()[0].keep, 5);

    // The datum path skips a size-prefixed block by its byte size.
    let schema = ignored(r#"{"type":"array","items":"null"}"#);
    let datum = [zig(5), zig(-(1 << 20)), zig(0), vec![0x00]].concat();
    let mut skip = builder(&schema, None).build_serde_datum::<Keep>().unwrap();
    assert_eq!(decode(&mut skip, &datum).unwrap()[0].keep, 5);

    // A 64-node list decodes there in full, and 65 nodes pass its limit.
    let mut datum_path = builder(LIST, None).build_serde_datum::<Node>().unwrap();
    assert_eq!(decode(&mut datum_path, &list(64)).unwrap()[0].len(), 64);
    assert!(refusal(&mut datum_path, &list(65)).contains("depth limit of 128"));

    // The datum path builds no value tree, so a skipped field charges no
    // decoded size: 65,536 records of 20 `null` fields would be 77 MB as
    // values.
    let nulls: Vec<String> = (0..20)
        .map(|i| format!(r#"{{"name":"n{i}","type":"null"}}"#))
        .collect();
    let schema = ignored(&format!(
        r#"{{"type":"array","items":{{"type":"record","name":"Z","fields":[{}]}}}}"#,
        nulls.join(",")
    ));
    let datum = [zig(5), zig(1 << 16), vec![0x00]].concat();
    let mut skip = builder(&schema, None).build_serde_datum::<Keep>().unwrap();
    assert_eq!(decode(&mut skip, &datum).unwrap()[0].keep, 5);

    // Skipped, the list counts its named-type references too: 42 nodes pass
    // and 43 do not.
    let mut skip = builder(&ignored(LIST), None)
        .build_serde_datum::<Keep>()
        .unwrap();
    let skipped = |nodes| [zig(5), list(nodes)].concat();
    assert_eq!(decode(&mut skip, &skipped(42)).unwrap()[0].keep, 5);
    assert!(refusal(&mut skip, &skipped(43)).contains("depth limit of 128"));
}

/// `nodes` nested `Deep` records ending in `leaf`, which sits under `pad`
/// arrays: a field the [`Keep`] target ignores, and its datum after `keep`.
fn deep_ignored(leaf: &str, value: &Value, pad: usize, nodes: usize) -> (String, Vec<u8>) {
    let leaf = nested(pad, leaf);
    let deep = format!(
        r#"{{"type":"record","name":"Deep","fields":[{{"name":"d","type":[{leaf},"Deep"]}}]}}"#
    );
    let mut datum = zig(5);
    datum.resize(datum.len() + nodes - 1, 0x02);
    datum.push(0x00);
    datum.extend(nested_datum(&leaf, pad, value.clone()));
    (ignored(&deep), datum)
}

#[test]
fn a_skipped_decimal_takes_no_extra_level_on_the_datum_path() {
    // The deepest an ignored `long` skips on the datum path, a decimal skips
    // too. Each pad puts the leaf at a different level, so one of them puts
    // it at the last level the limit allows.
    let skips = |leaf: &str, value: &Value, pad: usize, nodes: usize| {
        let (schema, datum) = deep_ignored(leaf, value, pad, nodes);
        let mut skip = builder(&schema, None).build_serde_datum::<Keep>().unwrap();
        decode(&mut skip, &datum).is_ok()
    };
    let decimal = r#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}"#;
    for pad in 0..3 {
        let deepest = (1..100)
            .take_while(|&n| skips(r#""long""#, &Value::Long(7), pad, n))
            .last()
            .expect("one node skips");
        assert!(deepest < 99, "the datum path has a depth limit");
        assert!(
            skips(
                decimal,
                &Value::Decimal(Decimal::from(vec![1u8])),
                pad,
                deepest
            ),
            "a decimal under {pad} arrays in {deepest} nodes"
        );
    }
}
