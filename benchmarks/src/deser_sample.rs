//! Shared sample shapes for the cross-format deserialization rig
//! (`bin/deser_formats`).
//!
//! The same logical data is encoded as **both** Avro and JSON so the decode
//! throughput of each format is measured on identical input. Two workloads:
//!
//! - [`Order`] — a single flat 15-field record (mixed scalar types).
//! - [`SensorBatch`] of [`Reading`] — a nested batch of `n` richer events
//!   (bool, float, and a small nested string array), encoded for the batch in
//!   every JSON framing (one nested document, a top-level array, and NDJSON)
//!   plus the equivalent single Avro datum.
//!
//! Own the shapes here (rather than reusing `avro_batch`) so the rig can pick a
//! representative complexity without disturbing the existing Avro rigs.

use apache_avro::types::{Record as AvroRecord, Value as AvroValue};
use apache_avro::{Schema, to_avro_datum};
use serde::{Deserialize, Serialize};

// ---- order: a single flat 15-field record ----------------------------------

/// Avro writer schema for [`Order`].
pub const ORDER_SCHEMA: &str = r#"{"type":"record","name":"Order","fields":[
  {"name":"id","type":"long"},
  {"name":"sku","type":"string"},
  {"name":"customer","type":"string"},
  {"name":"qty","type":"int"},
  {"name":"price","type":"double"},
  {"name":"currency","type":"string"},
  {"name":"ts_ms","type":"long"},
  {"name":"region","type":"string"},
  {"name":"priority","type":"int"},
  {"name":"discount","type":"double"},
  {"name":"notes","type":"string"},
  {"name":"paid","type":"boolean"},
  {"name":"channel","type":"string"},
  {"name":"warehouse","type":"string"},
  {"name":"coupon","type":"string"}]}"#;

/// A flat 15-field order record with mixed scalar types.
#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    /// Order id.
    pub id: i64,
    /// Stock-keeping unit.
    pub sku: String,
    /// Customer id.
    pub customer: String,
    /// Quantity ordered.
    pub qty: i32,
    /// Unit price.
    pub price: f64,
    /// ISO currency code.
    pub currency: String,
    /// Order timestamp, epoch milliseconds.
    pub ts_ms: i64,
    /// Fulfilment region.
    pub region: String,
    /// Fulfilment priority.
    pub priority: i32,
    /// Applied discount fraction.
    pub discount: f64,
    /// Free-text notes.
    pub notes: String,
    /// Whether the order is paid.
    pub paid: bool,
    /// Sales channel.
    pub channel: String,
    /// Warehouse id.
    pub warehouse: String,
    /// Coupon code (empty when none).
    pub coupon: String,
}

/// A representative [`Order`] sample.
pub fn sample_order() -> Order {
    Order {
        id: 918_273_645,
        sku: "SKU-000917".to_owned(),
        customer: "cust-4471".to_owned(),
        qty: 7,
        price: 1499.95,
        currency: "USD".to_owned(),
        ts_ms: 1_772_000_000_000,
        region: "emea".to_owned(),
        priority: 2,
        discount: 0.15,
        notes: "expedited; gift wrap".to_owned(),
        paid: true,
        channel: "web".to_owned(),
        warehouse: "wh-eu-3".to_owned(),
        coupon: "SAVE15".to_owned(),
    }
}

/// One bare Avro datum for the [`Order`] sample.
pub fn avro_order() -> Vec<u8> {
    let o = sample_order();
    let schema = Schema::parse_str(ORDER_SCHEMA).expect("order schema");
    let mut rec = AvroRecord::new(&schema).expect("order record");
    rec.put("id", o.id);
    rec.put("sku", o.sku.as_str());
    rec.put("customer", o.customer.as_str());
    rec.put("qty", o.qty);
    rec.put("price", o.price);
    rec.put("currency", o.currency.as_str());
    rec.put("ts_ms", o.ts_ms);
    rec.put("region", o.region.as_str());
    rec.put("priority", o.priority);
    rec.put("discount", o.discount);
    rec.put("notes", o.notes.as_str());
    rec.put("paid", o.paid);
    rec.put("channel", o.channel.as_str());
    rec.put("warehouse", o.warehouse.as_str());
    rec.put("coupon", o.coupon.as_str());
    to_avro_datum(&schema, rec).expect("order datum")
}

/// The [`Order`] sample as one JSON document.
pub fn json_order() -> Vec<u8> {
    serde_json::to_vec(&sample_order()).expect("order json")
}

// ---- batch: a nested n-reading batch ---------------------------------------

/// Avro writer schema for [`SensorBatch`].
pub const BATCH_SCHEMA: &str = r#"{"type":"record","name":"SensorBatch","fields":[
  {"name":"sensor","type":"string"},
  {"name":"batch_ts_ms","type":"long"},
  {"name":"region","type":"string"},
  {"name":"readings","type":{"type":"array","items":
    {"type":"record","name":"Reading","fields":[
      {"name":"name","type":"string"},
      {"name":"value","type":"long"},
      {"name":"unit","type":"string"},
      {"name":"ts_ms","type":"long"},
      {"name":"ok","type":"boolean"},
      {"name":"ratio","type":"double"},
      {"name":"tags","type":{"type":"array","items":"string"}}]}}}]}"#;

/// One reading within a [`SensorBatch`] — richer than a flat scalar row: a
/// bool, a float, and a small nested string array.
#[derive(Debug, Serialize, Deserialize)]
pub struct Reading {
    /// Metric name.
    pub name: String,
    /// Metric value.
    pub value: i64,
    /// Metric unit.
    pub unit: String,
    /// Reading timestamp, epoch milliseconds.
    pub ts_ms: i64,
    /// Whether the reading passed validation.
    pub ok: bool,
    /// A normalized ratio.
    pub ratio: f64,
    /// Free-form tags.
    pub tags: Vec<String>,
}

/// A batch envelope carrying `n` [`Reading`]s.
#[derive(Debug, Serialize, Deserialize)]
pub struct SensorBatch {
    /// Sensor id.
    pub sensor: String,
    /// Batch timestamp, epoch milliseconds.
    pub batch_ts_ms: i64,
    /// Sensor region.
    pub region: String,
    /// The readings carried in this batch.
    pub readings: Vec<Reading>,
}

/// `n` representative readings.
pub fn sample_readings(n: u64) -> Vec<Reading> {
    (0..n)
        .map(|i| Reading {
            name: format!("metric_{i}"),
            value: i as i64 * 37,
            unit: "count".to_owned(),
            ts_ms: 1_772_000_000_000 + i as i64,
            ok: i % 5 != 0,
            ratio: i as f64 / 100.0,
            tags: vec![
                "prod".to_owned(),
                "eu".to_owned(),
                format!("rack-{}", i % 8),
            ],
        })
        .collect()
}

/// A [`SensorBatch`] of `n` readings.
pub fn sample_batch(n: u64) -> SensorBatch {
    SensorBatch {
        sensor: "sensor-7".to_owned(),
        batch_ts_ms: 1_772_000_000_000,
        region: "emea".to_owned(),
        readings: sample_readings(n),
    }
}

/// One bare Avro datum for a batch of `n` readings (the nested-array shape).
pub fn avro_batch(n: u64) -> Vec<u8> {
    let batch = sample_batch(n);
    let schema = Schema::parse_str(BATCH_SCHEMA).expect("batch schema");
    let mut rec = AvroRecord::new(&schema).expect("batch record");
    rec.put("sensor", batch.sensor.as_str());
    rec.put("batch_ts_ms", batch.batch_ts_ms);
    rec.put("region", batch.region.as_str());
    rec.put(
        "readings",
        AvroValue::Array(
            batch
                .readings
                .iter()
                .map(|r| {
                    AvroValue::Record(vec![
                        ("name".into(), AvroValue::String(r.name.clone())),
                        ("value".into(), AvroValue::Long(r.value)),
                        ("unit".into(), AvroValue::String(r.unit.clone())),
                        ("ts_ms".into(), AvroValue::Long(r.ts_ms)),
                        ("ok".into(), AvroValue::Boolean(r.ok)),
                        ("ratio".into(), AvroValue::Double(r.ratio)),
                        (
                            "tags".into(),
                            AvroValue::Array(
                                r.tags
                                    .iter()
                                    .map(|t| AvroValue::String(t.clone()))
                                    .collect(),
                            ),
                        ),
                    ])
                })
                .collect(),
        ),
    );
    to_avro_datum(&schema, rec).expect("batch datum")
}

/// The batch of `n` readings as one nested JSON document (framing `single`).
pub fn json_batch_document(n: u64) -> Vec<u8> {
    serde_json::to_vec(&sample_batch(n)).expect("batch document")
}

/// The `n` readings as a bare top-level JSON array (framing `array`).
pub fn json_batch_array(n: u64) -> Vec<u8> {
    serde_json::to_vec(&sample_readings(n)).expect("batch array")
}

/// The `n` readings as NDJSON — one reading per `\n`-separated line (framing
/// `ndjson`).
pub fn json_batch_ndjson(n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    for r in &sample_readings(n) {
        out.extend_from_slice(&serde_json::to_vec(r).expect("reading line"));
        out.push(b'\n');
    }
    out
}
