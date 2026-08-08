//! Fan-out pipeline: Kafka → Avro → flat_map → ClickHouse **Native** (sharded).
//!
//! The columnar twin of `kafka_avro_to_clickhouse.rs`. Where that example
//! writes one row per message through RowBinary, this one decodes a nested
//! `SensorBatch`, explodes its event array into one row per event with
//! `flat_map`, encodes the rows **columnar** with the ClickHouse Native
//! encoder, and routes each row to the shard a `Distributed` table would pick.
//!
//! Three things it shows that the sibling example does not:
//!
//! - **`flat_map` fan-out** — one Kafka message becomes N rows, each carrying
//!   its parent's ack so the watermark still cannot outrun unacknowledged data.
//! - **Native columnar encoding** — fields are written into per-column buffers
//!   rather than row-at-a-time, which is what ClickHouse ingests most cheaply.
//! - **Record-aware shard routing** — flat_map children share their parent's
//!   metadata, so the default meta-only router would colocate every event of a
//!   batch; a [`DistributedRouter`](spate::clickhouse::DistributedRouter) keyed
//!   on each row's own `sensor` places them the way `xxHash64(sensor)` would.
//!
//! # What the builder desugars to
//!
//! Same six-step assembly as the sibling example (see the
//! `spate::pipeline::Pipeline` module docs for the full mapping); the only
//! differences are the nested record shape and the columnar encoder:
//!
//! 1. `Pipeline::from_path` — telemetry, metrics exporter (before any handle),
//!    the shared I/O runtime, and the inflight budget.
//! 2. `KafkaSource::from_component_config` — the `source: { kafka: ... }` section.
//! 3. `AvroDeserializerBuilder::build_serde::<SensorBatch>()` — the typed
//!    decoder. The YAML uses `mode: raw` with an inline writer schema, so no
//!    registry is needed.
//! 4. `sink.native_schema()` — fetches `system.columns` and builds the columnar
//!    template; `NativeEncoder::new` mints one encoder per shard on `.clone()`.
//! 5. `.flat_map` fans out the event array; `.filter` drops negatives. Native
//!    column mapping is **positional** — the `SensorEvent` field order must
//!    equal the YAML `columns` order — with a first-record field-name check
//!    off the hot path.
//! 6. `sink.router::<Owned<SensorEvent>>(sensor_key)` — a record-aware
//!    [`DistributedRouter`](spate::clickhouse::DistributedRouter): each exploded
//!    event routes by **its own** `sensor` field (flat_map children share
//!    their parent's metadata, so the default meta-only `KeyHashRouter`
//!    would colocate them), placing every sensor on the shard a ClickHouse
//!    `Distributed` table with sharding key `xxHash64(sensor)` would pick.
//!    With the YAML's single shard it routes identically to the default —
//!    scaling out is a YAML change (see the `shards:` comment there).
//! 7. `.run(source)` — the runtime, reusing the builder's I/O runtime.
//!
//! # Run it
//!
//! Needs Kafka and ClickHouse (`KAFKA_BROKERS`, `CLICKHOUSE_URL`), a topic of
//! bare-datum Avro `SensorBatch` messages (`mode: raw`, no registry), and the
//! target table. `batch_ts_ms` is epoch milliseconds and lands in a real
//! `DateTime64(3)` column: the row declares that scale with the
//! [`DateTime64Millis`] wrapper, which encodes as the raw little-endian
//! `Int64` (exactly the epoch-millis wire value, zero cost), and the column
//! name matches the struct field so the positional check passes.
//!
//! Caveat: the Native leaf writer does not rescale to the column's declared
//! precision — pointed at a `DateTime64(6)` column, these milli-scaled values
//! would land as 1970-era timestamps. The wrapper is what makes that
//! checkable: under the YAML's `validate_schema: full` a wrapper/precision
//! mismatch fails fatally on the first record, before anything is inserted
//! (a plain `i64` field declares no scale, so nothing could validate it).
//!
//! ```sql
//! CREATE TABLE sensor_events (
//!     sensor       LowCardinality(String),
//!     batch_ts_ms  DateTime64(3),
//!     name         LowCardinality(String),
//!     value        Int64,
//!     unit         LowCardinality(String)
//! ) ENGINE = MergeTree ORDER BY (sensor, batch_ts_ms);
//!
//! -- Sharded deployments add a Distributed table for SELECTs whose sharding
//! -- key matches the router (inserts stay direct-to-local); with
//! -- optimize_skip_unused_shards=1, sensor-filtered queries touch one shard:
//! -- CREATE TABLE sensor_events_dist AS sensor_events
//! --     ENGINE = Distributed(<cluster>, <db>, sensor_events, xxHash64(sensor));
//! ```
//!
//! ```sh
//! cargo run --release -p spate --features full \
//!   --example kafka_avro_flatmap_clickhouse
//! ```
//!
//! SIGTERM drains gracefully; probes: `curl localhost:9090/readyz`.

// The examples index renders these fields; see scripts/examples-index.sh.
// INDEX-TIER:  production
// INDEX-GOAL:  fan a nested batch into one row per event and shard them by sensor
// INDEX-TECH:  Kafka, Avro and ClickHouse Native
// INDEX-NEEDS: Kafka and ClickHouse

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use serde::{Deserialize, Serialize};
use spate::avro::AvroDeserializerBuilder;
use spate::clickhouse::{DateTime64Millis, NativeEncoder, ShardKey};
use spate::kafka::KafkaSource;
use spate::prelude::*;
use std::path::Path;

// ANCHOR: record
/// One Kafka datum: a sensor's batch of readings. The nested event array is
/// exploded downstream by `flat_map`.
#[derive(Debug, Deserialize)]
struct SensorBatch {
    sensor: String,
    batch_ts_ms: i64,
    events: Vec<Event>,
}

/// One inner reading.
#[derive(Debug, Deserialize)]
struct Event {
    name: String,
    value: i64,
    unit: String,
}

/// The `flat_map` output = one ClickHouse row. **Field order must match the
/// `columns` list in the YAML** — Native maps fields to columns positionally.
/// [`DateTime64Millis`] declares the timestamp's scale so `validate_schema:
/// full` can check it against the column's declared precision (it still
/// encodes as the raw `Int64`).
#[derive(Debug, Serialize)]
struct SensorEvent {
    sensor: String,
    batch_ts_ms: DateTime64Millis,
    name: String,
    value: i64,
    unit: String,
}
// ANCHOR_END: record

// ANCHOR: shard_key
/// Sharding key: the `sensor` column — one sensor always lands on one shard,
/// matching a `Distributed` DDL of `xxHash64(sensor)`. A named fn item: the
/// extractor is a fn pointer, so it cannot capture.
fn sensor_key(row: &SensorEvent) -> ShardKey<'_> {
    ShardKey::Str(&row.sensor)
}
// ANCHOR_END: shard_key

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Constructor owns init: logs, the metrics exporter (installed before any
    // handle can exist), and the shared I/O runtime.
    let config_path = std::env::var("SPATE_CONFIG")
        .unwrap_or_else(|_| "crates/spate/examples/kafka_avro_flatmap_clickhouse.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: Kafka ───────────────────────────────────────────────────
    let source = KafkaSource::from_component_config(&pipeline.config().source)?;

    // ── Deserializer: Avro, typed ───────────────────────────────────────
    // `raw` mode (inline writer schema) avoids a registry.
    let deser_section = pipeline
        .config()
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?;
    let deserializer =
        AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())?
            .build_serde::<SensorBatch>()?;

    // ── Sink: ClickHouse Native, sharded by sensor ──────────────────────
    // `format: native` fetches `system.columns` and hands the encoder the
    // real column types (so `batch_ts_ms`'s `DateTime64(3)` is laid out as an
    // Int64). The encoder is `Clone`: the terminal stage mints one per shard.
    // ANCHOR: router
    let sink = spate::clickhouse::config::from_component_config(
        pipeline.config().sink_config("default")?,
    )?;
    // No-op unless the YAML opts into `distributed_check`; with it, startup
    // fails fast if the sink topology drifts from the cluster + DDL.
    pipeline.block_on(sink.validate_distributed())?;
    // Weights come from the validated YAML — router and endpoints can't
    // drift. With a single shard this routes identically to the default
    // (everything to shard 0); with N it matches `xxHash64(sensor)`.
    let router = sink.router::<Owned<SensorEvent>>(sensor_key);
    // ANCHOR_END: router
    // ANCHOR: encoder
    let native = pipeline.block_on(sink.native_schema())?;
    let encoder = NativeEncoder::<Owned<SensorEvent>>::new(native);
    // ANCHOR_END: encoder

    // ── The chain, and run ──────────────────────────────────────────────
    // `flat_map` explodes each batch's event array into one row per event;
    // `filter` drops negative readings. `NativeEncoder::encode` then writes
    // each field into its per-column buffer on the pipeline thread, inside
    // the terminal sink-handoff stage.
    let report = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            let chunk_cfg = ctx.chunk();
            chain::<Owned<SensorBatch>, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                .flat_map::<Owned<SensorEvent>, _>(|batch, out| {
                    let (sensor, batch_ts_ms) = (batch.sensor, batch.batch_ts_ms);
                    for event in batch.events {
                        out.emit(SensorEvent {
                            sensor: sensor.clone(),
                            batch_ts_ms: DateTime64Millis(batch_ts_ms),
                            name: event.name,
                            value: event.value,
                            unit: event.unit,
                        });
                    }
                })
                .filter(|event: &SensorEvent| event.value >= 0)
                .sink(
                    encoder.clone(),
                    router.clone(), // Clone, not Copy: one router per chain lane
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .run(source)?;

    report.log();
    std::process::exit(report.exit_code());
}
