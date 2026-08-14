//! Decode fixtures shared by `benches/decode.rs` (wall time) and
//! `benches/decode_gungraun.rs` (instruction counts): a flat 15-field record,
//! an array-shaped one, and a truncated copy of the flat record for the
//! error path.
//!
//! Included with `#[path]` rather than imported: a bench target is its own
//! crate, so two targets can only agree on a workload by compiling the same
//! source. If the two ever encoded different datums, a wall-clock result and
//! an instruction count would not be talking about the same bytes.
//!
//! The published comparison corpus is not here. It lives in `decode.rs`
//! with the golden self-check that pins it to the benchmark repository.

use apache_avro::{Schema, to_avro_datum};
use spate_core::record::{Flow, Record};

/// A realistic flat record: 15 fields, two nullable unions, one string array.
pub(crate) const SCHEMA: &str = r#"{"type":"record","name":"Order","fields":[
  {"name":"id","type":"long"},
  {"name":"user_id","type":"long"},
  {"name":"sku","type":"string"},
  {"name":"quantity","type":"int"},
  {"name":"unit_price","type":"double"},
  {"name":"currency","type":"string"},
  {"name":"region","type":"string"},
  {"name":"channel","type":"string"},
  {"name":"created_ms","type":"long"},
  {"name":"updated_ms","type":"long"},
  {"name":"discount","type":["null","double"],"default":null},
  {"name":"coupon","type":["null","string"],"default":null},
  {"name":"tags","type":{"type":"array","items":"string"}},
  {"name":"priority","type":"int"},
  {"name":"note","type":"string"}]}"#;

#[derive(Debug, serde::Deserialize)]
#[expect(dead_code, reason = "deserialization target only")]
pub(crate) struct Order {
    id: i64,
    user_id: i64,
    sku: String,
    quantity: i32,
    unit_price: f64,
    currency: String,
    region: String,
    channel: String,
    created_ms: i64,
    updated_ms: i64,
    discount: Option<f64>,
    coupon: Option<String>,
    tags: Vec<String>,
    priority: i32,
    note: String,
}

/// Counts emitted records and drops them, so a decode bench measures the
/// decode rather than whatever a downstream operator would do with the result.
pub(crate) struct Sink(pub(crate) u64);

impl<T> spate_core::deser::EmitRecord<'_, T> for Sink {
    fn emit(&mut self, _rec: Record<T>) -> Flow {
        self.0 += 1;
        Flow::Continue
    }
}

/// One `Order` as a bare Avro datum.
pub(crate) fn order_datum() -> Vec<u8> {
    use apache_avro::types::Value;

    let schema = Schema::parse_str(SCHEMA).unwrap();
    let mut rec = apache_avro::types::Record::new(&schema).unwrap();
    rec.put("id", 902_144i64);
    rec.put("user_id", 71_002i64);
    rec.put("sku", "SKU-4477-XL");
    rec.put("quantity", 3);
    rec.put("unit_price", 24.99f64);
    rec.put("currency", "GBP");
    rec.put("region", "emea");
    rec.put("channel", "web");
    rec.put("created_ms", 1_772_000_000_000i64);
    rec.put("updated_ms", 1_772_000_060_000i64);
    rec.put("discount", Value::Union(0, Box::new(Value::Null)));
    rec.put("coupon", Value::Union(0, Box::new(Value::Null)));
    rec.put(
        "tags",
        Value::Array(vec![
            Value::String("gift".into()),
            Value::String("prio".into()),
        ]),
    );
    rec.put("priority", 2);
    rec.put("note", "leave at the door");
    to_avro_datum(&schema, rec).unwrap()
}

/// One [`SCHEMA`] datum cut off mid-record: the front half of
/// [`order_datum`]'s bytes, ending inside a field. Decoding it must fail on
/// every path. Truncation rather than emptiness, because an empty payload is
/// a tombstone and decodes to nothing without touching the error path.
pub(crate) fn malformed_datum() -> Vec<u8> {
    let mut datum = order_datum();
    datum.truncate(datum.len() / 2);
    datum
}

/// The batch shape: one datum is an array of lines, so throughput is
/// measured per line. Tracks the `flat_map` use case.
///
/// Do not change the field *types*: one string, then an array of (string,
/// long, string). They are the workload the counted tier compares against, and
/// `tests/bench_fixtures.rs` pins them. The names are free.
pub(crate) const BATCH_SCHEMA: &str = r#"{"type":"record","name":"PlacedOrder","fields":[
  {"name":"region","type":"string"},
  {"name":"lines","type":{"type":"array","items":
    {"type":"record","name":"OrderLine","fields":[
      {"name":"sku","type":"string"},
      {"name":"qty","type":"long"},
      {"name":"unit","type":"string"}]}}}]}"#;

/// Lines in one [`BATCH_SCHEMA`] datum.
pub(crate) const BATCH_LINES: u64 = 50;

#[derive(Debug, serde::Deserialize)]
#[expect(dead_code, reason = "deserialization target only")]
pub(crate) struct PlacedOrder {
    region: String,
    lines: Vec<OrderLine>,
}

#[derive(Debug, serde::Deserialize)]
#[expect(dead_code, reason = "deserialization target only")]
pub(crate) struct OrderLine {
    sku: String,
    qty: i64,
    unit: String,
}

/// One [`BATCH_SCHEMA`] datum holding [`BATCH_LINES`] lines.
pub(crate) fn batch_datum() -> Vec<u8> {
    use apache_avro::types::Value;

    let schema = Schema::parse_str(BATCH_SCHEMA).unwrap();
    let mut rec = apache_avro::types::Record::new(&schema).unwrap();
    rec.put("region", "eu-west");
    rec.put(
        "lines",
        Value::Array(
            (0..BATCH_LINES)
                .map(|i| {
                    Value::Record(vec![
                        ("sku".into(), Value::String(format!("SKU-{i:04}"))),
                        ("qty".into(), Value::Long(i as i64 * 37)),
                        ("unit".into(), Value::String("each".into())),
                    ])
                })
                .collect(),
        ),
    );
    to_avro_datum(&schema, rec).unwrap()
}
