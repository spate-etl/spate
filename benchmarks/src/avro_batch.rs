//! Shared sensor-batch Avro shapes for the `e2e_kafka_clickhouse` rig.
//!
//! One datum is a *sensor batch*: a `sensor` string, a `batch_ts_ms`
//! timestamp, and an array of `{name, value, unit}` events. The chain
//! explodes the array with `flat_map` into one ClickHouse row per event.

use apache_avro::types::{Record as AvroRecord, Value as AvroValue};
use apache_avro::{Schema, to_avro_datum};
use serde::{Deserialize, Serialize};
use spate_clickhouse::NativeSchema;
use spate_core::deser::Owned;
use spate_core::ops::Emitter;
use std::sync::Arc;

/// The writer schema replayed on every payload. Neutral "sensor batch"
/// naming, matching the `spate-avro` decode microbench, with a `batch_ts_ms`
/// timestamp field added.
pub const BATCH_SCHEMA: &str = r#"{"type":"record","name":"SensorBatch","fields":[
  {"name":"sensor","type":"string"},
  {"name":"batch_ts_ms","type":"long"},
  {"name":"events","type":{"type":"array","items":
    {"type":"record","name":"Event","fields":[
      {"name":"name","type":"string"},
      {"name":"value","type":"long"},
      {"name":"unit","type":"string"}]}}}]}"#;

/// The exploded event row's ClickHouse columns, in positional order — the
/// Native encoder maps struct fields to these by position, and the DDL and
/// the sink `columns` list are generated from them.
pub const EVENT_COLUMNS: &[(&str, &str)] = &[
    ("sensor", "LowCardinality(String)"),
    ("batch_ts_ms", "Int64"),
    ("name", "LowCardinality(String)"),
    ("value", "Int64"),
    ("unit", "LowCardinality(String)"),
];

// ---- owned family ----------------------------------------------------------

/// Owned sensor batch (the `apache_owned` decode target).
/// `Clone` satisfies the serde deserializer's (spurious, `PhantomData`-driven)
/// `Clone` bound so a per-thread clone is legal.
#[derive(Debug, Deserialize, Clone)]
pub struct SensorBatchOwned {
    /// The sensor identifier.
    pub sensor: String,
    /// Batch timestamp in epoch milliseconds.
    pub batch_ts_ms: i64,
    /// The events carried in this batch.
    pub events: Vec<EventOwned>,
}

/// Owned inner event.
#[derive(Debug, Deserialize, Clone)]
pub struct EventOwned {
    /// Metric name.
    pub name: String,
    /// Metric value.
    pub value: i64,
    /// Metric unit.
    pub unit: String,
}

/// Owned `flat_map` output row.
#[derive(Debug, Serialize)]
pub struct SensorEventOwned {
    /// The parent batch's sensor.
    pub sensor: String,
    /// The parent batch's timestamp.
    pub batch_ts_ms: i64,
    /// This event's metric name.
    pub name: String,
    /// This event's value.
    pub value: i64,
    /// This event's unit.
    pub unit: String,
}

// ---- operator stages (fn items: naturally higher-ranked) -------------------

/// Explode an owned batch into one owned [`SensorEventOwned`] per event.
pub fn explode_owned(b: SensorBatchOwned, out: &mut Emitter<'_, Owned<SensorEventOwned>>) {
    let SensorBatchOwned {
        sensor,
        batch_ts_ms,
        events,
    } = b;
    for e in events {
        out.emit(SensorEventOwned {
            sensor: sensor.clone(),
            batch_ts_ms,
            name: e.name,
            value: e.value,
            unit: e.unit,
        });
    }
}

/// Keep events with a non-negative value (exercises the filter stage).
pub fn keep_owned(e: &SensorEventOwned) -> bool {
    e.value >= 0
}

// ---- helpers ---------------------------------------------------------------

/// Build the Native column schema for the exploded event row, server-free.
pub fn native_schema() -> Arc<NativeSchema> {
    NativeSchema::from_columns(EVENT_COLUMNS).expect("native schema")
}

/// Encode one `SensorBatch` datum holding `events` events (bare Avro datum,
/// `raw` framing — no Confluent prefix, no registry).
pub fn encode_batch(events: u64) -> Vec<u8> {
    let schema = Schema::parse_str(BATCH_SCHEMA).expect("schema");
    let mut rec = AvroRecord::new(&schema).expect("record");
    rec.put("sensor", "sensor-7");
    rec.put("batch_ts_ms", 1_772_000_000_000i64);
    rec.put(
        "events",
        AvroValue::Array(
            (0..events)
                .map(|i| {
                    AvroValue::Record(vec![
                        ("name".into(), AvroValue::String(format!("metric_{i}"))),
                        ("value".into(), AvroValue::Long(i as i64 * 37)),
                        ("unit".into(), AvroValue::String("count".into())),
                    ])
                })
                .collect(),
        ),
    );
    to_avro_datum(&schema, rec).expect("datum")
}
