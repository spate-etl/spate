//! Decode fixtures shared by `benches/decode.rs` (wall time) and
//! `benches/decode_gungraun.rs` (instruction counts): a flat 15-field record,
//! a nested sensor batch, and the three payload framings the deserializer
//! supports.
//!
//! Included with `#[path]` rather than imported: a bench target is its own
//! crate, so two targets can only agree on a workload by compiling the same
//! source. If the two ever encoded different documents, a wall-clock result
//! and an instruction count would not be talking about the same bytes.

// Each bench target compiles this module separately and uses a different
// subset of it — the wall-clock bench decodes a nested document the counted
// one does not, and only the counted one needs a record sink. So an item is
// legitimately dead in one target while live in the other, which is a
// module-wide `allow` rather than per-item `expect`: an `expect` would itself
// go unfulfilled in whichever target does use the item.
#![allow(dead_code, reason = "each bench target uses a different subset")]

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct Order {
    pub(crate) id: i64,
    pub(crate) sku: String,
    pub(crate) customer: String,
    pub(crate) qty: i32,
    pub(crate) price: f64,
    pub(crate) currency: String,
    pub(crate) ts_ms: i64,
    pub(crate) region: String,
    pub(crate) priority: i32,
    pub(crate) discount: f64,
    pub(crate) notes: String,
    pub(crate) paid: bool,
    pub(crate) channel: String,
    pub(crate) warehouse: String,
    pub(crate) coupon: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Reading {
    pub(crate) name: String,
    pub(crate) value: i64,
    pub(crate) unit: String,
    pub(crate) ts_ms: i64,
    pub(crate) ok: bool,
    pub(crate) ratio: f64,
    pub(crate) tags: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SensorBatch {
    pub(crate) sensor: String,
    pub(crate) batch_ts_ms: i64,
    pub(crate) region: String,
    pub(crate) readings: Vec<Reading>,
}

pub(crate) fn sample_order() -> Order {
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
        coupon: Some("SAVE15".to_owned()),
    }
}

pub(crate) fn sample_readings(n: u64) -> Vec<Reading> {
    (0..n)
        .map(|i| Reading {
            name: format!("metric_{i}"),
            value: (i as i64) * 37,
            unit: "count".to_owned(),
            ts_ms: 1_772_000_000_000 + i as i64,
            ok: i % 5 != 0,
            ratio: (i as f64) / 100.0,
            tags: vec![
                "prod".to_owned(),
                "eu".to_owned(),
                format!("rack-{}", i % 8),
            ],
        })
        .collect()
}

pub(crate) fn sample_batch(n: u64) -> SensorBatch {
    SensorBatch {
        sensor: "sensor-7".to_owned(),
        batch_ts_ms: 1_772_000_000_000,
        region: "emea".to_owned(),
        readings: sample_readings(n),
    }
}

pub(crate) fn ndjson(readings: &[Reading]) -> Vec<u8> {
    let mut out = Vec::new();
    for r in readings {
        out.extend_from_slice(&serde_json::to_vec(r).unwrap());
        out.push(b'\n');
    }
    out
}

/// One flat record as a single JSON document — the `single` framing's payload.
pub(crate) fn order_document() -> Vec<u8> {
    serde_json::to_vec(&sample_order()).expect("encode an order")
}

/// A nested document holding `n` readings. Not a framing the deserializer
/// splits: one payload, one record, whose cost is the nested decode.
pub(crate) fn batch_document(n: u64) -> Vec<u8> {
    serde_json::to_vec(&sample_batch(n)).expect("encode a batch")
}

/// `n` readings as a top-level JSON array — the `array` framing's payload.
pub(crate) fn readings_array(n: u64) -> Vec<u8> {
    serde_json::to_vec(&sample_readings(n)).expect("encode an array")
}

/// `n` readings newline-delimited — the `ndjson` framing's payload.
pub(crate) fn readings_ndjson(n: u64) -> Vec<u8> {
    ndjson(&sample_readings(n))
}

/// Counts emitted records, so the decode's output cannot be optimised away
/// and a case that silently stops emitting is visible as a changed count.
pub(crate) struct Sink(pub(crate) u64);

impl<T> spate_core::deser::EmitRecord<'_, T> for Sink {
    fn emit(&mut self, _rec: spate_core::record::Record<T>) -> spate_core::record::Flow {
        self.0 += 1;
        spate_core::record::Flow::Continue
    }
}
