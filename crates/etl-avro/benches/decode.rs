//! Datum-decoding throughput: the dynamically-typed Value path vs the
//! serde-typed path, on a realistic 15-field record — plus (feature `fast`)
//! the fast backend's owned and borrowed variants, and a batch shape (one
//! datum = an array of 50 events, per-event throughput) tracking the
//! flagship `flat_map` use case.

use apache_avro::{Schema, to_avro_datum};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use etl_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
use etl_core::checkpoint::AckRef;
#[cfg(feature = "fast")]
use etl_core::deser::RecFamily;
use etl_core::deser::{Deserializer, EmitRecord};
use etl_core::record::{Flow, PartitionId, RawPayload, Record};
use std::hint::black_box;

const SCHEMA: &str = r#"{"type":"record","name":"Order","fields":[
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
struct Order {
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

/// Borrowed twin of [`Order`]: string-ish fields point into the payload.
#[cfg(feature = "fast")]
#[derive(Debug, serde::Deserialize)]
#[expect(dead_code, reason = "deserialization target only")]
struct OrderRef<'a> {
    id: i64,
    user_id: i64,
    sku: &'a str,
    quantity: i32,
    unit_price: f64,
    currency: &'a str,
    region: &'a str,
    channel: &'a str,
    created_ms: i64,
    updated_ms: i64,
    discount: Option<f64>,
    coupon: Option<&'a str>,
    tags: Vec<&'a str>,
    priority: i32,
    note: &'a str,
}

#[cfg(feature = "fast")]
struct OrderRefFam;
#[cfg(feature = "fast")]
impl RecFamily for OrderRefFam {
    type Rec<'buf> = OrderRef<'buf>;
}

struct Sink(u64);
impl<T> EmitRecord<'_, T> for Sink {
    fn emit(&mut self, _rec: Record<T>) -> Flow {
        self.0 += 1;
        Flow::Continue
    }
}

fn datum() -> Vec<u8> {
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
    rec.put(
        "discount",
        apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
    );
    rec.put(
        "coupon",
        apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
    );
    rec.put(
        "tags",
        apache_avro::types::Value::Array(vec![
            apache_avro::types::Value::String("gift".into()),
            apache_avro::types::Value::String("prio".into()),
        ]),
    );
    rec.put("priority", 2);
    rec.put("note", "leave at the door");
    to_avro_datum(&schema, rec).unwrap()
}

fn bench(c: &mut Criterion) {
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(SCHEMA)),
        ..AvroSettings::default()
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    let payload = datum();
    let raw = RawPayload {
        bytes: &payload,
        key: None,
        partition: PartitionId(0),
        offset: 1,
        timestamp_ms: 0,
    };
    let (ack, _rx) = AckRef::test_pair();

    let mut group = c.benchmark_group("avro_decode");
    group.throughput(Throughput::Elements(1));
    group.bench_function("value", |b| {
        let mut deser = builder.build_value().expect("apache builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.bench_function("serde_typed", |b| {
        let mut deser = builder.build_serde::<Order>().expect("apache builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    #[cfg(feature = "fast")]
    group.bench_function("serde_fast", |b| {
        let mut deser = builder.build_serde_fast::<Order>().unwrap();
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    #[cfg(feature = "fast")]
    group.bench_function("serde_fast_borrowed", |b| {
        let mut deser = builder.build_fast::<OrderRefFam>().unwrap();
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.finish();

    bench_batch(c);
}

/// The flagship batch shape: one datum = one sensor batch holding an array
/// of 50 events, throughput measured **per event** — the fast backend's
/// headline `flat_map` use case, regression-tracked inside the workspace.
fn bench_batch(c: &mut Criterion) {
    const BATCH_SCHEMA: &str = r#"{"type":"record","name":"SensorBatch","fields":[
      {"name":"sensor","type":"string"},
      {"name":"events","type":{"type":"array","items":
        {"type":"record","name":"Event","fields":[
          {"name":"name","type":"string"},
          {"name":"value","type":"long"},
          {"name":"unit","type":"string"}]}}}]}"#;
    const EVENTS: u64 = 50;

    #[derive(Debug, serde::Deserialize)]
    #[expect(dead_code, reason = "deserialization target only")]
    struct SensorBatch {
        sensor: String,
        events: Vec<Event>,
    }
    #[derive(Debug, serde::Deserialize)]
    #[expect(dead_code, reason = "deserialization target only")]
    struct Event {
        name: String,
        value: i64,
        unit: String,
    }

    #[cfg(feature = "fast")]
    #[derive(Debug, serde::Deserialize)]
    #[expect(dead_code, reason = "deserialization target only")]
    struct SensorBatchRef<'a> {
        sensor: &'a str,
        events: Vec<EventRef<'a>>,
    }
    #[cfg(feature = "fast")]
    #[derive(Debug, serde::Deserialize)]
    #[expect(dead_code, reason = "deserialization target only")]
    struct EventRef<'a> {
        name: &'a str,
        value: i64,
        unit: &'a str,
    }
    #[cfg(feature = "fast")]
    struct BatchRefFam;
    #[cfg(feature = "fast")]
    impl RecFamily for BatchRefFam {
        type Rec<'buf> = SensorBatchRef<'buf>;
    }

    let schema = Schema::parse_str(BATCH_SCHEMA).unwrap();
    let mut rec = apache_avro::types::Record::new(&schema).unwrap();
    rec.put("sensor", "sensor-7");
    rec.put(
        "events",
        apache_avro::types::Value::Array(
            (0..EVENTS)
                .map(|i| {
                    apache_avro::types::Value::Record(vec![
                        (
                            "name".into(),
                            apache_avro::types::Value::String(format!("metric_{i}")),
                        ),
                        (
                            "value".into(),
                            apache_avro::types::Value::Long(i as i64 * 37),
                        ),
                        (
                            "unit".into(),
                            apache_avro::types::Value::String("count".into()),
                        ),
                    ])
                })
                .collect(),
        ),
    );
    let payload = to_avro_datum(&schema, rec).unwrap();

    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(BATCH_SCHEMA)),
        ..AvroSettings::default()
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    let raw = RawPayload {
        bytes: &payload,
        key: None,
        partition: PartitionId(0),
        offset: 1,
        timestamp_ms: 0,
    };
    let (ack, _rx) = AckRef::test_pair();

    let mut group = c.benchmark_group("avro_decode_batch50");
    group.throughput(Throughput::Elements(EVENTS));
    group.bench_function("serde_typed", |b| {
        let mut deser = builder
            .build_serde::<SensorBatch>()
            .expect("apache builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    #[cfg(feature = "fast")]
    group.bench_function("serde_fast", |b| {
        let mut deser = builder.build_serde_fast::<SensorBatch>().unwrap();
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    #[cfg(feature = "fast")]
    group.bench_function("serde_fast_borrowed", |b| {
        let mut deser = builder.build_fast::<BatchRefFam>().unwrap();
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
