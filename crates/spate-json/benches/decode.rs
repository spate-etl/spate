//! Criterion micro-benchmarks for the JSON decode primitives.
//!
//! `json_decode` measures a single flat 15-field record; `json_decode_batch50`
//! measures a 50-element batch in each framing (one nested document, a
//! top-level array, and NDJSON), reported per element so the framings compare
//! directly. Cross-format comparison against Avro lives in the `deser_formats`
//! rig under `benchmarks/`.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde_json::Value;

#[path = "support/orders.rs"]
mod orders;

use orders::{Order, Reading, SensorBatch};

fn json_decode(c: &mut Criterion) {
    let bytes = orders::order_document();
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
    let document = orders::batch_document(N);
    let array = orders::readings_array(N);
    let lines = orders::readings_ndjson(N);

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
