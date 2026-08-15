//! The batch corpus: one datum is an order carrying an array of lines, so a
//! decode is measured per line and a `flat_map` has something to fan out.
//!
//! Shared by `benches/decode_paths_wall.rs` (wall time) and
//! `benches/decode_gungraun.rs` (instruction counts). Included with `#[path]`
//! rather than imported: a bench target is its own crate, so two targets can
//! only agree on a workload by compiling the same source.
//!
//! The record names, and the fields a reader will recognize, come from the
//! storefront model `crates/spate/examples/README.md` describes and
//! `crates/spate-datagen/src/events.rs` defines: `OrderPlaced` carrying
//! `order_id`, `customer_id`, `region`, `placed_at`, and a `lines` array of
//! `OrderLine` with `sku` and `qty`. The namespace here is `spate.bench`; the
//! published model's is `spate.datagen`.
//!
//! The fields this fixture adds to that model, and what each one exercises:
//!
//! - `region` and `discount` are nullable unions, one at each nesting level, so
//!   every decode path walks a union.
//! - `tags`, an array of strings inside the line record, is a second nesting
//!   level, so no path through the decoder takes a flat-struct fast case.
//! - `channel` is a low-cardinality string on the parent, denormalized onto
//!   every flattened row, so the flatten pays a per-row string clone.
//! - `received_at_us` is a second timestamp, at microsecond scale.
//! - `seq` carries a line's position, so a flatten that reorders is detectable.
//! - `unit` is a packaging unit, taken from `LineItem` in
//!   `crates/spate-json/benches/support/orders.rs`. One of its values is the
//!   sentinel [`flatten_typed`] and [`flatten_value`] drop a line on. The
//!   model's `unit_cents` is a price; this fixture carries no price field.
//!
//! Corpora and the flatten they feed. The rig that decodes them is
//! `decode_rig.rs`.

#![expect(deprecated, reason = "fixtures call the datum free functions directly")]
// Each bench target compiles this module separately and uses a different
// subset of it: the counted tier measures cases the wall tier leaves out.
#![allow(dead_code, reason = "each bench target uses a different subset")]

use apache_avro::types::Value as AvroValue;
use apache_avro::{Schema, to_avro_datum};

/// One order with its lines. The module docs say where each field comes from.
pub(crate) const BATCH_SCHEMA: &str = r#"{
  "type": "record",
  "name": "OrderPlaced",
  "namespace": "spate.bench",
  "fields": [
    {"name": "order_id", "type": "long"},
    {"name": "customer_id", "type": "int"},
    {"name": "channel", "type": "string"},
    {"name": "region", "type": ["null", "string"], "default": null},
    {"name": "placed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "received_at_us", "type": "long"},
    {"name": "lines", "type": {"type": "array", "items":
      {"type": "record", "name": "OrderLine", "fields": [
        {"name": "seq", "type": "int"},
        {"name": "sku", "type": "string"},
        {"name": "unit", "type": "string"},
        {"name": "qty", "type": "long"},
        {"name": "discount", "type": ["null", "double"], "default": null},
        {"name": "tags", "type": {"type": "array", "items": "string"}}]}}}
  ]
}"#;

/// Orders in the corpus. A case covers all of them in one iteration.
pub(crate) const BATCHES: u64 = 200;

/// Lines in one order.
pub(crate) const LINES_PER_BATCH: u32 = 100;

/// Lines in the whole corpus, the extent a case declares with `.items()`.
pub(crate) const LINES: u64 = BATCHES * LINES_PER_BATCH as u64;

/// The `unit` value [`flatten_typed`] and [`flatten_value`] drop a line on.
pub(crate) const VOID_UNIT: &str = "void";

/// A line whose `discount` is present and below this is dropped.
pub(crate) const DISCOUNT_FLOOR: f64 = 0.2;

const CUSTOMERS: u64 = 1024;
const SKUS: u64 = 32;
const TAGS: u64 = 16;
const CHANNELS: [&str; 4] = ["web", "mobile", "partner", "phone"];
const UNITS: [&str; 8] = [
    "each", "case", "pallet", "void", "box", "crate", "drum", "roll",
];
const BASE_TS_MS: i64 = 1_772_000_000_000;

fn customer_of(order_id: u64) -> i32 {
    i32::try_from(order_id % CUSTOMERS).expect("customer index fits i32")
}

fn channel_of(order_id: u64) -> &'static str {
    CHANNELS[usize::try_from(order_id % 4).expect("index fits usize")]
}

fn region_of(order_id: u64) -> Option<String> {
    if order_id.is_multiple_of(10) {
        None
    } else {
        Some(format!("region-{}", order_id % 7))
    }
}

fn sku_of(order_id: u64, seq: u32) -> String {
    format!("sku_{}", (order_id * 31 + u64::from(seq)) % SKUS)
}

fn unit_of(order_id: u64, seq: u32) -> &'static str {
    UNITS[usize::try_from((order_id * 7 + u64::from(seq)) % 8).expect("index fits usize")]
}

fn qty_of(order_id: u64, seq: u32) -> i64 {
    let v = (order_id.wrapping_mul(1_000_003) + u64::from(seq) * 97) % 2_147_483_647;
    i64::try_from(v).expect("value below 2^31")
}

fn discount_of(order_id: u64, seq: u32) -> Option<f64> {
    let s = u64::from(seq);
    if (order_id + s).is_multiple_of(5) {
        None
    } else {
        #[expect(
            clippy::cast_precision_loss,
            reason = "the numerator is a residue mod 100, exactly representable"
        )]
        Some(((order_id * 13 + s * 7) % 100) as f64 / 100.0)
    }
}

fn tags_of(order_id: u64, seq: u32) -> Vec<String> {
    let s = u64::from(seq);
    (0..((order_id + s) % 4))
        .map(|j| format!("tag-{}", (order_id + s + j) % TAGS))
        .collect()
}

/// Encode one order as a bare Avro datum.
pub(crate) fn encode_batch(schema: &Schema, order_id: u64) -> Vec<u8> {
    let region = match region_of(order_id) {
        // Branch indices follow the schema's declared union order,
        // `["null","string"]`.
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        Some(r) => AvroValue::Union(1, Box::new(AvroValue::String(r))),
    };
    let lines = (0..LINES_PER_BATCH)
        .map(|seq| {
            let discount = match discount_of(order_id, seq) {
                None => AvroValue::Union(0, Box::new(AvroValue::Null)),
                Some(d) => AvroValue::Union(1, Box::new(AvroValue::Double(d))),
            };
            let tags = AvroValue::Array(
                tags_of(order_id, seq)
                    .into_iter()
                    .map(AvroValue::String)
                    .collect(),
            );
            AvroValue::Record(vec![
                (
                    "seq".to_owned(),
                    AvroValue::Int(i32::try_from(seq).expect("seq fits i32")),
                ),
                ("sku".to_owned(), AvroValue::String(sku_of(order_id, seq))),
                (
                    "unit".to_owned(),
                    AvroValue::String(unit_of(order_id, seq).to_owned()),
                ),
                ("qty".to_owned(), AvroValue::Long(qty_of(order_id, seq))),
                ("discount".to_owned(), discount),
                ("tags".to_owned(), tags),
            ])
        })
        .collect();
    let id = i64::try_from(order_id).expect("order_id fits i64");
    let record = AvroValue::Record(vec![
        ("order_id".to_owned(), AvroValue::Long(id)),
        (
            "customer_id".to_owned(),
            AvroValue::Int(customer_of(order_id)),
        ),
        (
            "channel".to_owned(),
            AvroValue::String(channel_of(order_id).to_owned()),
        ),
        ("region".to_owned(), region),
        (
            "placed_at".to_owned(),
            AvroValue::TimestampMillis(BASE_TS_MS + id),
        ),
        (
            "received_at_us".to_owned(),
            AvroValue::Long(BASE_TS_MS * 1000 + id),
        ),
        ("lines".to_owned(), AvroValue::Array(lines)),
    ]);
    to_avro_datum(schema, record).expect("encode an order datum")
}

/// The whole corpus: [`BATCHES`] orders as bare Avro datums.
pub(crate) fn order_batches() -> Vec<Vec<u8>> {
    let schema = Schema::parse_str(BATCH_SCHEMA).expect("parse the batch schema");
    (0..BATCHES).map(|id| encode_batch(&schema, id)).collect()
}

/// The owned decode target, for the value and two-pass typed paths.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct OrderPlaced {
    pub(crate) order_id: i64,
    pub(crate) customer_id: i32,
    pub(crate) channel: String,
    pub(crate) region: Option<String>,
    pub(crate) placed_at: i64,
    pub(crate) received_at_us: i64,
    pub(crate) lines: Vec<OrderLine>,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct OrderLine {
    pub(crate) seq: i32,
    pub(crate) sku: String,
    pub(crate) unit: String,
    pub(crate) qty: i64,
    pub(crate) discount: Option<f64>,
    pub(crate) tags: Vec<String>,
}

/// The borrowed decode target for the single-pass path: the same shape with
/// string contents pointing into the payload buffer.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct OrderPlacedRef<'a> {
    pub(crate) order_id: i64,
    pub(crate) customer_id: i32,
    #[serde(borrow)]
    pub(crate) channel: &'a str,
    #[serde(borrow)]
    pub(crate) region: Option<&'a str>,
    pub(crate) placed_at: i64,
    pub(crate) received_at_us: i64,
    pub(crate) lines: Vec<OrderLineRef<'a>>,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct OrderLineRef<'a> {
    pub(crate) seq: i32,
    #[serde(borrow)]
    pub(crate) sku: &'a str,
    #[serde(borrow)]
    pub(crate) unit: &'a str,
    pub(crate) qty: i64,
    pub(crate) discount: Option<f64>,
    #[serde(borrow)]
    pub(crate) tags: Vec<&'a str>,
}

pub(crate) struct BatchRefFam;
impl spate_core::deser::RecFamily for BatchRefFam {
    type Rec<'buf> = OrderPlacedRef<'buf>;
}

/// A timestamp in milliseconds. A stand-in for the sink crate's newtype, which
/// wraps an `i64` the same way, so this module pulls in no sink crate.
pub(crate) struct DateTime64Millis(pub(crate) i64);

/// A timestamp in microseconds.
pub(crate) struct DateTime64Micros(pub(crate) i64);

/// One output row of the flatten: an order line with its order's fields
/// denormalized onto it.
pub(crate) struct OrderLineRow {
    pub(crate) order_id: u64,
    pub(crate) line_seq: u16,
    pub(crate) channel: String,
    pub(crate) region: String,
    pub(crate) sku_upper: String,
    pub(crate) unit: String,
    pub(crate) qty: i64,
    pub(crate) qty_scaled: i64,
    pub(crate) discount: Option<f64>,
    pub(crate) tags: Vec<String>,
    pub(crate) placed_at: DateTime64Millis,
    pub(crate) received_at: DateTime64Micros,
}

/// The flatten over the borrowed typed record: coalesce a null region, drop a
/// voided line, drop a line discounted below the floor, upper-case the SKU, and
/// scale the quantity by the line's position.
pub(crate) fn flatten_typed<F: FnMut(OrderLineRow)>(batch: &OrderPlacedRef<'_>, mut emit: F) {
    let region = batch.region.unwrap_or("");
    for line in &batch.lines {
        if line.unit == VOID_UNIT {
            continue;
        }
        if matches!(line.discount, Some(d) if d < DISCOUNT_FLOOR) {
            continue;
        }
        emit(OrderLineRow {
            order_id: u64::try_from(batch.order_id).expect("order_id non-negative"),
            line_seq: u16::try_from(line.seq).expect("seq fits u16"),
            channel: batch.channel.to_owned(),
            region: region.to_owned(),
            sku_upper: line.sku.to_ascii_uppercase(),
            unit: line.unit.to_owned(),
            qty: line.qty,
            qty_scaled: line.qty * 1000 / (i64::from(line.seq) + 1),
            discount: line.discount,
            tags: line.tags.iter().map(|t| (*t).to_string()).collect(),
            placed_at: DateTime64Millis(batch.placed_at),
            received_at: DateTime64Micros(batch.received_at_us),
        });
    }
}

/// The same flatten over the decoded `AvroValue` tree, reached positionally.
pub(crate) fn flatten_value<F: FnMut(OrderLineRow)>(v: &AvroValue, mut emit: F) {
    let rec = as_record(v);
    let order_id = u64::try_from(as_long(&rec[0].1)).expect("order_id non-negative");
    let channel = as_str(&rec[2].1);
    let region = as_union(&rec[3].1).map_or_else(String::new, |r| as_str(r).to_owned());
    let placed_at = as_long(&rec[4].1);
    let received_at_us = as_long(&rec[5].1);
    let AvroValue::Array(lines) = &rec[6].1 else {
        panic!("lines is not an array")
    };
    for item in lines {
        let line = as_record(item);
        let unit = as_str(&line[2].1);
        if unit == VOID_UNIT {
            continue;
        }
        let discount = as_union(&line[4].1).map(|d| match d {
            AvroValue::Double(d) => *d,
            other => panic!("expected an Avro double, got {other:?}"),
        });
        if matches!(discount, Some(d) if d < DISCOUNT_FLOOR) {
            continue;
        }
        let seq_raw = as_long(&line[0].1);
        let seq = u32::try_from(seq_raw).expect("seq non-negative");
        let qty = as_long(&line[3].1);
        emit(OrderLineRow {
            order_id,
            line_seq: u16::try_from(seq_raw).expect("seq fits u16"),
            channel: channel.to_owned(),
            region: region.clone(),
            sku_upper: as_str(&line[1].1).to_ascii_uppercase(),
            unit: unit.to_owned(),
            qty,
            qty_scaled: qty * 1000 / i64::from(seq + 1),
            discount,
            tags: as_tags(&line[5].1),
            placed_at: DateTime64Millis(placed_at),
            received_at: DateTime64Micros(received_at_us),
        });
    }
}

fn as_record(v: &AvroValue) -> &[(String, AvroValue)] {
    match v {
        AvroValue::Record(fields) => fields,
        other => panic!("expected an Avro record, got {other:?}"),
    }
}

fn as_long(v: &AvroValue) -> i64 {
    match v {
        AvroValue::Long(n) | AvroValue::TimestampMillis(n) => *n,
        AvroValue::Int(n) => i64::from(*n),
        other => panic!("expected an Avro long, got {other:?}"),
    }
}

fn as_str(v: &AvroValue) -> &str {
    match v {
        AvroValue::String(s) => s,
        other => panic!("expected an Avro string, got {other:?}"),
    }
}

fn as_union(v: &AvroValue) -> Option<&AvroValue> {
    match v {
        AvroValue::Union(_, inner) => match inner.as_ref() {
            AvroValue::Null => None,
            present => Some(present),
        },
        AvroValue::Null => None,
        other => Some(other),
    }
}

fn as_tags(v: &AvroValue) -> Vec<String> {
    match v {
        AvroValue::Array(items) => items.iter().map(|t| as_str(t).to_owned()).collect(),
        other => panic!("expected an Avro array, got {other:?}"),
    }
}
