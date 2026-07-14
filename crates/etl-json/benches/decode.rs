//! Criterion micro-benchmarks for the JSON decode primitives.
//!
//! `json_decode` measures a single flat 15-field record; `json_decode_batch50`
//! measures a 50-element batch in each framing (one nested document, a
//! top-level array, and NDJSON), reported per element so the framings compare
//! directly. Cross-format comparison against Avro lives in the `deser_formats`
//! rig under `benchmarks/`.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
struct Order {
    id: i64,
    sku: String,
    customer: String,
    qty: i32,
    price: f64,
    currency: String,
    ts_ms: i64,
    region: String,
    priority: i32,
    discount: f64,
    notes: String,
    paid: bool,
    channel: String,
    warehouse: String,
    coupon: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Reading {
    name: String,
    value: i64,
    unit: String,
    ts_ms: i64,
    ok: bool,
    ratio: f64,
    tags: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct SensorBatch {
    sensor: String,
    batch_ts_ms: i64,
    region: String,
    readings: Vec<Reading>,
}

fn sample_order() -> Order {
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

fn sample_readings(n: u64) -> Vec<Reading> {
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

fn sample_batch(n: u64) -> SensorBatch {
    SensorBatch {
        sensor: "sensor-7".to_owned(),
        batch_ts_ms: 1_772_000_000_000,
        region: "emea".to_owned(),
        readings: sample_readings(n),
    }
}

fn ndjson(readings: &[Reading]) -> Vec<u8> {
    let mut out = Vec::new();
    for r in readings {
        out.extend_from_slice(&serde_json::to_vec(r).unwrap());
        out.push(b'\n');
    }
    out
}

fn json_decode(c: &mut Criterion) {
    let bytes = serde_json::to_vec(&sample_order()).unwrap();
    let mut g = c.benchmark_group("json_decode");
    g.bench_function("serde_typed", |b| {
        b.iter(|| {
            let _: Order = serde_json::from_slice(std::hint::black_box(&bytes)).unwrap();
        });
    });
    g.bench_function("serde_value", |b| {
        b.iter(|| {
            let _: Value = serde_json::from_slice(std::hint::black_box(&bytes)).unwrap();
        });
    });
    g.finish();
}

fn json_decode_batch50(c: &mut Criterion) {
    const N: u64 = 50;
    let batch = sample_batch(N);
    let document = serde_json::to_vec(&batch).unwrap();
    let array = serde_json::to_vec(&batch.readings).unwrap();
    let lines = ndjson(&batch.readings);

    let mut g = c.benchmark_group("json_decode_batch50");
    g.throughput(Throughput::Elements(N));
    g.bench_function("document", |b| {
        b.iter(|| {
            let _: SensorBatch = serde_json::from_slice(std::hint::black_box(&document)).unwrap();
        });
    });
    g.bench_function("array", |b| {
        b.iter(|| {
            let _: Vec<Reading> = serde_json::from_slice(std::hint::black_box(&array)).unwrap();
        });
    });
    g.bench_function("ndjson", |b| {
        b.iter(|| {
            for line in std::hint::black_box(&lines).split(|&c| c == b'\n') {
                if !line.is_empty() {
                    let _: Reading = serde_json::from_slice(line).unwrap();
                }
            }
        });
    });
    g.finish();
}

criterion_group!(benches, json_decode, json_decode_batch50);
criterion_main!(benches);
