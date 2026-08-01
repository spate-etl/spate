//! Datum-decoding throughput across the three decode paths — the
//! dynamically-typed Value path, the two-pass serde-typed path, and the
//! single-pass datum path (owned and borrowed) — on a realistic 15-field
//! record, a batch shape (one datum = an array of 50 events, per-event
//! throughput) tracking the flagship `flat_map` use case, and the
//! sensor-batch attribution corpus with its decode-plus-flatten arms.
//!
//! The flat-record and batch fixtures live in `benches/support/orders.rs`,
//! shared with the instruction-count bench in `benches/decode_gungraun.rs`.

use apache_avro::Schema;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord};
use spate_core::record::{Flow, PartitionId, RawPayload, Record};
use std::hint::black_box;

#[path = "support/orders.rs"]
mod orders;

use orders::{Order, SCHEMA, Sink};

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
    let payload = orders::order_datum();
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
        let mut deser = builder.build_value().expect("value builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.bench_function("serde_typed", |b| {
        let mut deser = builder.build_serde::<Order>().expect("serde builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.bench_function("datum_typed", |b| {
        let mut deser = builder.build_serde_datum::<Order>().expect("datum builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.finish();

    bench_batch(c);
    bench_sensor_batch(c);
}

/// The flagship batch shape: one datum = one sensor batch holding an array
/// of 50 events, throughput measured **per event** — the `flat_map` use
/// case, regression-tracked inside the workspace.
fn bench_batch(c: &mut Criterion) {
    use orders::{BATCH_EVENTS as EVENTS, BATCH_SCHEMA, SensorBatch};

    let payload = orders::batch_datum();

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
    group.bench_function("value", |b| {
        let mut deser = builder.build_value().expect("value builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.bench_function("serde_typed", |b| {
        let mut deser = builder.build_serde::<SensorBatch>().expect("serde builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.bench_function("datum_typed", |b| {
        let mut deser = builder
            .build_serde_datum::<SensorBatch>()
            .expect("datum builder");
        let mut sink = Sink(0);
        b.iter(|| {
            deser.deserialize(black_box(&raw), &ack, &mut sink).unwrap();
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// The published comparison workload (`sensor_batch`)
//
// Everything below reproduces the spate-benchmark corpus byte-for-byte so the
// numbers here attribute the CPU that the published Kafka→Avro→ClickHouse
// comparison actually spends. Three things are restated rather than imported
// (the benchmark repo is a separate workspace pinning spate-avro from
// crates.io): the schema, the field derivations, and the entrant's flatten.
// `golden_self_check` proves the restated generator identical to the corpus,
// so the restatement cannot drift silently.
// ---------------------------------------------------------------------------

/// Byte-identical copy of `workload/schema/sensor_batch.avsc` in the
/// spate-benchmark repository (dataset `d2-60d7e5bb2a82`). Every framework in
/// that comparison decodes this exact schema.
const COMPARISON_SCHEMA: &str = r#"{
  "type": "record",
  "name": "SensorBatch",
  "namespace": "rs.etl.bench",
  "doc": "One Kafka message. Registered under subject `sensor-batches-value` and produced Confluent-framed (0x00 + BE u32 schema id + Avro datum). Every framework in the comparison decodes THIS file — nobody re-declares the schema inline.",
  "fields": [
    {
      "name": "batch_id",
      "type": "long",
      "doc": "Dense, monotonic from 0. With `event_seq` this forms the row identity the correctness gate counts, so it must survive the pipeline unmodified."
    },
    {
      "name": "sensor",
      "type": "string",
      "doc": "Low-cardinality by construction (1024 distinct values), so it maps onto LowCardinality(String) and is a sane sharding/ordering key."
    },
    {
      "name": "region",
      "type": ["null", "string"],
      "default": null,
      "doc": "Deliberately a nullable union, and deliberately null ~10% of the time: it forces every implementation through a real union-decode path, and the transform must coalesce null to the empty string. The ClickHouse column is LowCardinality(String), NOT LowCardinality(Nullable(String)) — our Native encoder rejects a non-String inner, and that trap is the reason the coalesce is part of the specified work rather than an incidental detail."
    },
    {
      "name": "batch_ts_ms",
      "type": "long",
      "doc": "Event timestamp, epoch milliseconds. Lands in a DateTime64(3) column."
    },
    {
      "name": "send_ts_us",
      "type": "long",
      "doc": "Epoch MICROSECONDS, and specifically the producer's INTENDED send time on its fixed open-loop schedule, never the actual send time. This is the whole coordinated-omission defence: if the generator falls behind, the delay it caused appears in the latency percentiles instead of vanishing from them. Lands in a DateTime64(6) column; end-to-end latency is computed server-side as ingest_ts - send_ts."
    },
    {
      "name": "events",
      "doc": "The fan-out. One message becomes `events.length` rows, so every implementation must perform a real flat_map/flatMap rather than a pass-through — which is the point: a 1:1 pipe would be a benchmark of nothing.",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Event",
          "fields": [
            {
              "name": "seq",
              "type": "int",
              "doc": "Position within `events`, 0-based, assigned by the producer. Carried explicitly rather than derived from array position so that an implementation which reorders during flatten cannot silently pass the row-identity gate."
            },
            {
              "name": "name",
              "type": "string",
              "doc": "Low-cardinality metric name (32 distinct values)."
            },
            {
              "name": "unit",
              "type": "string",
              "doc": "Low-cardinality unit (8 distinct values). One value, `\"drop\"`, is the transform's filter sentinel."
            },
            {
              "name": "value",
              "type": "long",
              "doc": "Deterministic function of (batch_id, seq) — see methodology/. Because it is reproducible from the row identity alone, the driver can compute the expected checksum in closed form and prove that two frameworks did the same arithmetic, not merely that they moved the same number of rows."
            },
            {
              "name": "quality",
              "type": ["null", "double"],
              "default": null,
              "doc": "Second nullable union. The transform's filter drops rows where this is non-null and below a threshold."
            },
            {
              "name": "tags",
              "type": {
                "type": "array",
                "items": "string"
              },
              "doc": "Inner array-of-string, 0-3 elements. This is the second nesting level; it maps to Array(LowCardinality(String)) and exists so the decode path cannot be a flat-struct fast case."
            }
          ]
        }
      }
    }
  ]
}"#;

// Field derivations restated from spate-benchmark `harness/src/corpus.rs`
// (the `dataset-version` region). The derivations are periodic in `batch_id`
// with period 200 (lcm of the mod-8 unit, mod-5/mod-100 quality, mod-10
// region residues), so 200 consecutive batches reproduce the corpus's exact
// value distribution and its 73.5% filter pass rate (14,700 of 20,000 events).
mod comparison_corpus {
    use apache_avro::Schema;
    use apache_avro::to_avro_datum;
    use apache_avro::types::Value as AvroValue;

    pub(crate) const EVENTS_PER_BATCH: u32 = 100;
    const SENSORS: u64 = 1024;
    const NAMES: u64 = 32;
    const TAGS: u64 = 16;
    const UNITS: [&str; 8] = [
        "count", "bytes", "ms", "drop", "ratio", "celsius", "volts", "rpm",
    ];
    const BASE_TS_MS: i64 = 1_772_000_000_000;

    fn sensor_of(batch_id: u64) -> String {
        format!("sensor-{}", batch_id % SENSORS)
    }

    fn region_of(batch_id: u64) -> Option<String> {
        if batch_id.is_multiple_of(10) {
            None
        } else {
            Some(format!("region-{}", batch_id % 7))
        }
    }

    fn name_of(batch_id: u64, seq: u32) -> String {
        format!("metric_{}", (batch_id * 31 + u64::from(seq)) % NAMES)
    }

    fn unit_of(batch_id: u64, seq: u32) -> &'static str {
        UNITS[usize::try_from((batch_id * 7 + u64::from(seq)) % 8).expect("index fits usize")]
    }

    fn value_of(batch_id: u64, seq: u32) -> i64 {
        let v = (batch_id.wrapping_mul(1_000_003) + u64::from(seq) * 97) % 2_147_483_647;
        i64::try_from(v).expect("value below 2^31")
    }

    fn quality_of(batch_id: u64, seq: u32) -> Option<f64> {
        let s = u64::from(seq);
        if (batch_id + s).is_multiple_of(5) {
            None
        } else {
            #[expect(
                clippy::cast_precision_loss,
                reason = "the numerator is a residue mod 100, exactly representable"
            )]
            Some(((batch_id * 13 + s * 7) % 100) as f64 / 100.0)
        }
    }

    fn tags_of(batch_id: u64, seq: u32) -> Vec<String> {
        let s = u64::from(seq);
        (0..((batch_id + s) % 4))
            .map(|j| format!("tag-{}", (batch_id + s + j) % TAGS))
            .collect()
    }

    /// Encode one batch as a bare Avro datum, with the prefill `send_ts_us`
    /// derivation (`BASE_TS_MS * 1000 + batch_id`) the drain corpus uses.
    pub(crate) fn encode_batch(schema: &Schema, batch_id: u64) -> Vec<u8> {
        let region = match region_of(batch_id) {
            // Branch indices follow the schema's declared union order,
            // `["null","string"]`.
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
            Some(r) => AvroValue::Union(1, Box::new(AvroValue::String(r))),
        };
        let events = (0..EVENTS_PER_BATCH)
            .map(|seq| {
                let quality = match quality_of(batch_id, seq) {
                    None => AvroValue::Union(0, Box::new(AvroValue::Null)),
                    Some(q) => AvroValue::Union(1, Box::new(AvroValue::Double(q))),
                };
                let tags = AvroValue::Array(
                    tags_of(batch_id, seq)
                        .into_iter()
                        .map(AvroValue::String)
                        .collect(),
                );
                AvroValue::Record(vec![
                    (
                        "seq".to_owned(),
                        AvroValue::Int(i32::try_from(seq).expect("seq fits i32")),
                    ),
                    ("name".to_owned(), AvroValue::String(name_of(batch_id, seq))),
                    (
                        "unit".to_owned(),
                        AvroValue::String(unit_of(batch_id, seq).to_owned()),
                    ),
                    ("value".to_owned(), AvroValue::Long(value_of(batch_id, seq))),
                    ("quality".to_owned(), quality),
                    ("tags".to_owned(), tags),
                ])
            })
            .collect();
        let record = AvroValue::Record(vec![
            (
                "batch_id".to_owned(),
                AvroValue::Long(i64::try_from(batch_id).expect("batch_id fits i64")),
            ),
            ("sensor".to_owned(), AvroValue::String(sensor_of(batch_id))),
            ("region".to_owned(), region),
            (
                "batch_ts_ms".to_owned(),
                AvroValue::Long(BASE_TS_MS + i64::try_from(batch_id).expect("batch_id fits i64")),
            ),
            (
                "send_ts_us".to_owned(),
                AvroValue::Long(
                    BASE_TS_MS * 1000 + i64::try_from(batch_id).expect("batch_id fits i64"),
                ),
            ),
            ("events".to_owned(), AvroValue::Array(events)),
        ]);
        to_avro_datum(schema, record).expect("encode sensor batch datum")
    }

    /// The corpus's own byte-level pin: FNV-1a over the datums for batches
    /// 0..1000 (spate-benchmark `harness/tests/golden_corpus.rs`). If the
    /// restated derivations drift from the published corpus in any byte, the
    /// bench refuses to run rather than measuring the wrong workload.
    pub(crate) fn golden_self_check(schema: &Schema) {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut total = 0usize;
        for id in 0..1000 {
            let b = encode_batch(schema, id);
            total += b.len();
            for byte in &b {
                h ^= u64::from(*byte);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        assert_eq!(
            format!("{h:016x}"),
            "5c66e4254fe9b472",
            "restated corpus derivations no longer match the published corpus"
        );
        assert_eq!(total, 4_051_124, "restated corpus changed size");
    }
}

// The comparison entrant's flatten, restated from spate-benchmark
// `entrants/spate/src/rows.rs`: positional traversal of the decoded
// `AvroValue`, the two filters, and a `Row` carrying 4 owned Strings plus a
// `Vec<String>` of tags per surviving event. `DateTime64Millis`/`Micros` are
// stand-ins for the spate-clickhouse newtypes (plain i64 wrappers there too),
// so this file does not pull a sink crate into spate-avro's dev-deps.
mod comparison_flatten {
    use apache_avro::types::Value as AvroValue;

    const DROP_UNIT: &str = "drop";
    const QUALITY_FLOOR: f64 = 0.2;

    #[expect(
        dead_code,
        reason = "wire-shape stand-in; the wrapped value is never read"
    )]
    pub(crate) struct DateTime64Millis(pub(crate) i64);
    #[expect(
        dead_code,
        reason = "wire-shape stand-in; the wrapped value is never read"
    )]
    pub(crate) struct DateTime64Micros(pub(crate) i64);

    /// The output row, matching the comparison's `sensor_events` table.
    #[expect(dead_code, reason = "flatten output shape; fields are never read")]
    pub(crate) struct Row {
        pub(crate) batch_id: u64,
        pub(crate) event_seq: u16,
        pub(crate) sensor: String,
        pub(crate) region: String,
        pub(crate) name_upper: String,
        pub(crate) unit: String,
        pub(crate) value: i64,
        pub(crate) value_scaled: i64,
        pub(crate) quality: Option<f64>,
        pub(crate) tags: Vec<String>,
        pub(crate) batch_ts: DateTime64Millis,
        pub(crate) send_ts: DateTime64Micros,
    }

    fn as_record(v: &AvroValue) -> &[(String, AvroValue)] {
        match v {
            AvroValue::Record(fields) => fields,
            other => panic!("expected an Avro record, got {other:?}"),
        }
    }

    fn as_long(v: &AvroValue) -> i64 {
        match v {
            AvroValue::Long(n) => *n,
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

    /// The borrowed decode target for the single-pass path: the same
    /// message shape with string contents pointing into the payload
    /// buffer (mirrors the harness `SensorBatch`/`Event`, borrowed).
    #[derive(Debug, serde::Deserialize)]
    pub(crate) struct SensorBatchRef<'a> {
        pub(crate) batch_id: i64,
        #[serde(borrow)]
        pub(crate) sensor: &'a str,
        #[serde(borrow)]
        pub(crate) region: Option<&'a str>,
        pub(crate) batch_ts_ms: i64,
        pub(crate) send_ts_us: i64,
        pub(crate) events: Vec<EventRef<'a>>,
    }

    #[derive(Debug, serde::Deserialize)]
    pub(crate) struct EventRef<'a> {
        pub(crate) seq: i32,
        #[serde(borrow)]
        pub(crate) name: &'a str,
        #[serde(borrow)]
        pub(crate) unit: &'a str,
        pub(crate) value: i64,
        pub(crate) quality: Option<f64>,
        #[serde(borrow)]
        pub(crate) tags: Vec<&'a str>,
    }

    pub(crate) struct BatchRefFam;
    impl spate_core::deser::RecFamily for BatchRefFam {
        type Rec<'buf> = SensorBatchRef<'buf>;
    }

    /// The entrant's flatten over the borrowed typed record instead of the
    /// `AvroValue` tree: same filters, same `Row`, no positional
    /// tree-walking — what the comparison entrant's `flat_map` becomes on
    /// the single-pass path.
    pub(crate) fn flatten_typed<F: FnMut(Row)>(batch: &SensorBatchRef<'_>, mut emit: F) {
        let region = batch.region.unwrap_or("");
        for e in &batch.events {
            if e.unit == DROP_UNIT {
                continue;
            }
            if matches!(e.quality, Some(q) if q < QUALITY_FLOOR) {
                continue;
            }
            emit(Row {
                batch_id: u64::try_from(batch.batch_id).expect("batch_id non-negative"),
                event_seq: u16::try_from(e.seq).expect("seq fits u16"),
                sensor: batch.sensor.to_owned(),
                region: region.to_owned(),
                name_upper: e.name.to_ascii_uppercase(),
                unit: e.unit.to_owned(),
                value: e.value,
                value_scaled: e.value * 1000 / (i64::from(e.seq) + 1),
                quality: e.quality,
                tags: e.tags.iter().map(|t| (*t).to_string()).collect(),
                batch_ts: DateTime64Millis(batch.batch_ts_ms),
                send_ts: DateTime64Micros(batch.send_ts_us),
            });
        }
    }

    pub(crate) fn flatten_value<F: FnMut(Row)>(v: &AvroValue, mut emit: F) {
        let rec = as_record(v);
        let batch_id = u64::try_from(as_long(&rec[0].1)).expect("batch_id non-negative");
        let sensor = as_str(&rec[1].1);
        let region = as_union(&rec[2].1).map_or_else(String::new, |r| as_str(r).to_owned());
        let batch_ts_ms = as_long(&rec[3].1);
        let send_ts_us = as_long(&rec[4].1);
        let AvroValue::Array(events) = &rec[5].1 else {
            panic!("events is not an array")
        };
        for ev in events {
            let e = as_record(ev);
            let unit = as_str(&e[2].1);
            if unit == DROP_UNIT {
                continue;
            }
            let quality = as_union(&e[4].1).map(|q| match q {
                AvroValue::Double(d) => *d,
                other => panic!("expected an Avro double, got {other:?}"),
            });
            if matches!(quality, Some(q) if q < QUALITY_FLOOR) {
                continue;
            }
            let seq_raw = as_long(&e[0].1);
            let seq = u32::try_from(seq_raw).expect("seq non-negative");
            let value = as_long(&e[3].1);
            emit(Row {
                batch_id,
                event_seq: u16::try_from(seq_raw).expect("seq fits u16"),
                sensor: sensor.to_owned(),
                region: region.clone(),
                name_upper: as_str(&e[1].1).to_ascii_uppercase(),
                unit: unit.to_owned(),
                value,
                value_scaled: value * 1000 / i64::from(seq + 1),
                quality,
                tags: as_tags(&e[5].1),
                batch_ts: DateTime64Millis(batch_ts_ms),
                send_ts: DateTime64Micros(send_ts_us),
            });
        }
    }
}

/// Attribution over the published comparison workload: one full 200-batch
/// period per iteration (20,000 events, 14,700 surviving rows), decoded and
/// flattened the way the comparison's Spate entrant does. Throughput is per
/// **event**; divide the per-event time by 0.735 for per-output-row cost.
fn bench_sensor_batch(c: &mut Criterion) {
    /// Batches per iteration — one full period of the corpus derivations.
    const PERIOD: u64 = 200;
    /// Events per iteration.
    const EVENTS: u64 = PERIOD * comparison_corpus::EVENTS_PER_BATCH as u64;

    // Mirrors the decode target committed in the benchmark harness
    // (`harness/src/corpus.rs`, `SensorBatch`/`Event`).
    #[derive(Debug, serde::Deserialize)]
    #[expect(dead_code, reason = "deserialization target only")]
    struct SensorBatch {
        batch_id: i64,
        sensor: String,
        region: Option<String>,
        batch_ts_ms: i64,
        send_ts_us: i64,
        events: Vec<Event>,
    }
    #[derive(Debug, serde::Deserialize)]
    #[expect(dead_code, reason = "deserialization target only")]
    struct Event {
        seq: i32,
        name: String,
        unit: String,
        value: i64,
        quality: Option<f64>,
        tags: Vec<String>,
    }

    /// Emits the decoded batch straight into the restated flatten — the
    /// entrant's `flat_map` stage, minus the engine around it.
    struct FlattenSink(u64);
    impl EmitRecord<'_, spate_avro::AvroValue> for FlattenSink {
        fn emit(&mut self, rec: Record<spate_avro::AvroValue>) -> Flow {
            let mut rows = 0u64;
            comparison_flatten::flatten_value(&rec.payload, |row| {
                black_box(&row);
                rows += 1;
            });
            self.0 += rows;
            Flow::Continue
        }
    }

    /// The borrowed-path equivalent: decoded typed batch straight into the
    /// typed flatten.
    struct TypedFlattenSink(u64);
    impl<'buf> EmitRecord<'buf, comparison_flatten::SensorBatchRef<'buf>> for TypedFlattenSink {
        fn emit(&mut self, rec: Record<comparison_flatten::SensorBatchRef<'buf>>) -> Flow {
            let mut rows = 0u64;
            comparison_flatten::flatten_typed(&rec.payload, |row| {
                black_box(&row);
                rows += 1;
            });
            self.0 += rows;
            Flow::Continue
        }
    }

    let schema = Schema::parse_str(COMPARISON_SCHEMA).unwrap();
    comparison_corpus::golden_self_check(&schema);

    let payloads: Vec<Vec<u8>> = (0..PERIOD)
        .map(|id| comparison_corpus::encode_batch(&schema, id))
        .collect();
    let raws: Vec<RawPayload<'_>> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| RawPayload {
            bytes: p,
            key: None,
            partition: PartitionId(0),
            offset: i as i64,
            timestamp_ms: 0,
        })
        .collect();
    let values: Vec<apache_avro::types::Value> = payloads
        .iter()
        .map(|p| apache_avro::from_avro_datum(&schema, &mut p.as_slice(), None).unwrap())
        .collect();

    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(COMPARISON_SCHEMA)),
        ..AvroSettings::default()
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    let (ack, _rx) = AckRef::test_pair();

    let mut group = c.benchmark_group("sensor_batch");
    group.throughput(Throughput::Elements(EVENTS));
    group.bench_function("value_decode", |b| {
        let mut deser = builder.build_value().expect("value builder");
        let mut sink = Sink(0);
        b.iter(|| {
            for raw in &raws {
                deser.deserialize(black_box(raw), &ack, &mut sink).unwrap();
            }
        });
    });
    group.bench_function("serde_typed_decode", |b| {
        let mut deser = builder.build_serde::<SensorBatch>().expect("serde builder");
        let mut sink = Sink(0);
        b.iter(|| {
            for raw in &raws {
                deser.deserialize(black_box(raw), &ack, &mut sink).unwrap();
            }
        });
    });
    group.bench_function("flatten_value", |b| {
        b.iter(|| {
            let mut rows = 0u64;
            for v in &values {
                comparison_flatten::flatten_value(black_box(v), |row| {
                    black_box(&row);
                    rows += 1;
                });
            }
            black_box(rows)
        });
    });
    group.bench_function("decode_plus_flatten", |b| {
        let mut deser = builder.build_value().expect("value builder");
        let mut sink = FlattenSink(0);
        b.iter(|| {
            for raw in &raws {
                deser.deserialize(black_box(raw), &ack, &mut sink).unwrap();
            }
        });
    });
    group.bench_function("datum_typed_decode", |b| {
        let mut deser = builder
            .build_serde_datum::<SensorBatch>()
            .expect("datum builder");
        let mut sink = Sink(0);
        b.iter(|| {
            for raw in &raws {
                deser.deserialize(black_box(raw), &ack, &mut sink).unwrap();
            }
        });
    });
    group.bench_function("datum_borrowed_decode", |b| {
        let mut deser = builder
            .build_datum::<comparison_flatten::BatchRefFam>()
            .expect("datum builder");
        let mut sink = Sink(0);
        b.iter(|| {
            for raw in &raws {
                deser.deserialize(black_box(raw), &ack, &mut sink).unwrap();
            }
        });
    });
    group.bench_function("datum_borrowed_plus_flatten", |b| {
        let mut deser = builder
            .build_datum::<comparison_flatten::BatchRefFam>()
            .expect("datum builder");
        let mut sink = TypedFlattenSink(0);
        b.iter(|| {
            for raw in &raws {
                deser.deserialize(black_box(raw), &ack, &mut sink).unwrap();
            }
        });
    });
    group.bench_function("memcpy_baseline", |b| {
        let mut scratch: Vec<u8> = Vec::with_capacity(8192);
        b.iter(|| {
            for p in &payloads {
                scratch.clear();
                scratch.extend_from_slice(black_box(p));
                black_box(scratch.as_slice());
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
