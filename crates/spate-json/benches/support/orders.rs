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

pub(crate) fn sample_reading(i: u64) -> Reading {
    Reading {
        name: format!("metric_{i}"),
        value: (i as i64) * 37,
        unit: "count".to_owned(),
        ts_ms: 1_772_000_000_000 + i as i64,
        ok: !i.is_multiple_of(5),
        ratio: (i as f64) / 100.0,
        tags: vec![
            "prod".to_owned(),
            "eu".to_owned(),
            format!("rack-{}", i % 8),
        ],
    }
}

pub(crate) fn sample_readings(n: u64) -> Vec<Reading> {
    (0..n).map(sample_reading).collect()
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

// ---------------------------------------------------------------------------
// Poison-pill corpora.
//
// The deserializer's whole error-isolation story — drop a record and keep
// going, or fail the payload without emitting a prefix — runs only on input
// that does not decode, and every corpus above is valid by construction. These
// build the same records with a known fraction of them broken, in a known way,
// at a known position.
// ---------------------------------------------------------------------------

/// Records in the error-policy corpora.
///
/// Forty times the fifty-record batch the reference cases carry, from this
/// same builder, which puts those cases in the 10^6–10^8 instruction band the
/// rest of this workspace's counter benches sit in while leaving the clean
/// case an exact scale-up of the reference one rather than a different
/// workload at a different size.
///
/// Shared with the test that pins the corpora: two copies of this would let
/// the bench's corpus drift while the test went on passing against its own.
pub(crate) const RECORDS: u64 = 2_000;

/// One record in this many is broken in the partial-poison corpora.
///
/// Ten per cent is a rate a real stream reaches — a schema drift on one
/// producer of ten — and it is far enough from both ends that the count is
/// neither the clean path with a rounding error on top nor the all-poison case
/// under another name.
pub(crate) const BAD_EVERY: u64 = 10;

/// How a poison record is broken.
///
/// The two kinds reach the failure from opposite ends of the parser, which is
/// why both are worth a case: one stops the parser before it has a value, the
/// other hands `serde` a value it cannot use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Corruption {
    /// The document with its closing brace removed. Every field parses, and
    /// then the input ends — a *syntax* error (`is_data` false), reached after
    /// the parser has done the whole document's work.
    Syntax,
    /// A well-formed document whose `name` is a number where [`Reading`] wants
    /// a string. The parse succeeds and the *mapping* fails — a data error,
    /// and the one a schema drift produces rather than a truncated write.
    TypeMismatch,
}

/// One record, encoded and then broken.
pub(crate) fn bad_reading(i: u64, how: Corruption) -> Vec<u8> {
    let r = sample_reading(i);
    match how {
        Corruption::Syntax => {
            let mut line = serde_json::to_vec(&r).expect("encode a reading");
            let brace = line.pop();
            assert_eq!(brace, Some(b'}'), "a record does not end in a brace");
            line
        }
        Corruption::TypeMismatch => serde_json::to_vec(&serde_json::json!({
            "name": i,
            "value": r.value,
            "unit": r.unit,
            "ts_ms": r.ts_ms,
            "ok": r.ok,
            "ratio": r.ratio,
            "tags": r.tags,
        }))
        .expect("encode a mismatched reading"),
    }
}

/// `records` readings newline-delimited, with every `bad_every`-th record
/// broken (`bad_every` of 0 breaks none, 1 breaks all).
pub(crate) fn readings_ndjson_bad_every(records: u64, bad_every: u64, how: Corruption) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..records {
        if bad_every != 0 && (i + 1).is_multiple_of(bad_every) {
            out.extend_from_slice(&bad_reading(i, how));
        } else {
            out.extend_from_slice(&serde_json::to_vec(&sample_reading(i)).unwrap());
        }
        out.push(b'\n');
    }
    out
}

/// How many records a corpus built by [`readings_ndjson_bad_every`] must
/// yield. Derived from the same arithmetic the builder uses, so the two cannot
/// disagree about which lines are broken.
pub(crate) fn good_lines(records: u64, bad_every: u64) -> u64 {
    if bad_every == 0 {
        return records;
    }
    records
        - (1..=records)
            .filter(|i| i.is_multiple_of(bad_every))
            .count() as u64
}

/// `records` readings newline-delimited with only the **last** one broken.
///
/// Late on purpose. Under `on_error: fail` the ndjson path decodes every line
/// into a holding buffer before emitting any of them, so a payload that fails
/// on its last line is the one that pays for the whole decode and then throws
/// all of it away — the atomic path's worst case, and the one that says what
/// atomicity costs.
pub(crate) fn readings_ndjson_bad_last(records: u64, how: Corruption) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..records - 1 {
        out.extend_from_slice(&serde_json::to_vec(&sample_reading(i)).unwrap());
        out.push(b'\n');
    }
    out.extend_from_slice(&bad_reading(records - 1, how));
    out.push(b'\n');
    out
}

/// `records` readings as a top-level array with only the **last** element
/// broken — the array framing's counterpart to
/// [`readings_ndjson_bad_last`], and broken the same way at the same position
/// so the pair differs in framing and nothing else.
///
/// A syntax error here would end the array parse early rather than late, so
/// this takes the [`TypeMismatch`](Corruption::TypeMismatch) kind: the parser
/// builds every element before `serde` rejects the last, and the whole vector
/// is discarded.
pub(crate) fn readings_array_bad_last(records: u64) -> Vec<u8> {
    let mut out = Vec::from(b"[");
    for i in 0..records - 1 {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(&serde_json::to_vec(&sample_reading(i)).unwrap());
    }
    out.push(b',');
    out.extend_from_slice(&bad_reading(records - 1, Corruption::TypeMismatch));
    out.push(b']');
    out
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
