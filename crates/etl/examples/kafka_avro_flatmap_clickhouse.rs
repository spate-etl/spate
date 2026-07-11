//! Zero-copy pipeline: Kafka → Avro (borrowed decode) → flat_map → ClickHouse Native.
//!
//! The high-performance twin of `kafka_avro_to_clickhouse.rs`. Where that
//! example owns every field (double-decode, RowBinary), this one never copies
//! the payload's `&str` fields until the columnar encode: `avro-fast` decodes
//! a Kafka datum into a **borrowed** `SensorBatch<'buf>` whose strings point
//! straight into the payload buffer, `flat_map` explodes the nested event
//! array into one row per event (still borrowing), and the ClickHouse **Native**
//! encoder copies each field into its column buffer.
//!
//! # Where the borrow ends
//!
//! `SensorBatch<'buf>` and the `SensorEvent<'buf>` rows it fans out borrow the
//! lane's payload buffer for the whole chain — deserialize, `flat_map`, and
//! `filter` all run without copying a byte of `sensor`/`name`/`unit`. The
//! borrow ends inside [`NativeEncoder`](etl::clickhouse::NativeEncoder)'s
//! `RowEncoder::encode`: it is the first stage that *keeps* the bytes, copying
//! each field into its per-column buffer. That copy runs on the pipeline
//! thread inside `push_batch` (the terminal sink-handoff stage), before the
//! payload buffer is recycled — so the `'buf` lifetime never outlives the copy.
//!
//! # What the builder desugars to
//!
//! Same six-step assembly as the sibling example (see the
//! `etl::pipeline::Pipeline` module docs for the full mapping); the only
//! differences are the borrowed record family and the columnar encoder:
//!
//! 1. `Pipeline::from_path` — telemetry, metrics exporter (before any handle),
//!    the shared I/O runtime, and the inflight budget.
//! 2. `KafkaSource::from_component_config` — the `source: { kafka: ... }` section.
//! 3. `AvroDeserializerBuilder::build_fast::<BatchFam>()` — the single-pass
//!    borrowed decoder. `build_fast` rejects a configured `reader_schema`
//!    (evolution is `#[serde(default)]`/`#[serde(alias)]`), so the YAML uses
//!    `mode: raw` with an inline writer schema.
//! 4. `sink.native_schema()` — fetches `system.columns` and builds the columnar
//!    template; `NativeEncoder::new` mints one encoder per shard on `.clone()`.
//! 5. `.flat_map` fans out the event array; `.filter` drops negatives. Native
//!    column mapping is **positional** — the `SensorEvent` field order must
//!    equal the YAML `columns` order — with a first-record field-name check
//!    off the hot path.
//! 6. `sink.router::<EventFam>(sensor_key)` — a record-aware
//!    [`DistributedRouter`](etl::clickhouse::DistributedRouter): each exploded
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
//! cargo run --release -p etl --features full,avro-fast \
//!   --example kafka_avro_flatmap_clickhouse
//! ```
//!
//! `avro-fast` is deliberately excluded from `full` (a `serde_avro_fast`
//! license-metadata caveat — see the `etl-avro` docs), so it is named
//! explicitly. SIGTERM drains gracefully; probes: `curl localhost:9090/readyz`.

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use etl::avro::AvroDeserializerBuilder;
use etl::clickhouse::{DateTime64Millis, NativeEncoder, ShardKey};
use etl::kafka::KafkaSource;
use etl::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// One Kafka datum, decoded **borrowed**: the string fields point into the
/// payload buffer (`avro-fast` never copies them), the event array is exploded
/// downstream by `flat_map`.
#[derive(Debug, Deserialize)]
struct SensorBatch<'a> {
    #[serde(borrow)]
    sensor: &'a str,
    batch_ts_ms: i64,
    #[serde(borrow)]
    events: Vec<Event<'a>>,
}

/// One inner reading, still borrowing the payload.
#[derive(Debug, Deserialize)]
struct Event<'a> {
    name: &'a str,
    value: i64,
    unit: &'a str,
}

/// The `flat_map` output = one ClickHouse row. **Field order must match the
/// `columns` list in the YAML** — Native maps fields to columns positionally.
/// The `&str` fields still borrow the payload; the copy happens in the Native
/// encoder's column buffers. [`DateTime64Millis`] declares the timestamp's
/// scale so `validate_schema: full` can check it against the column's
/// declared precision (it still encodes as the raw `Int64`).
#[derive(Debug, Serialize)]
struct SensorEvent<'a> {
    sensor: &'a str,
    batch_ts_ms: DateTime64Millis,
    name: &'a str,
    value: i64,
    unit: &'a str,
}

/// Record family for the borrowed [`SensorBatch`] (the deserializer output).
#[derive(Debug)]
struct BatchFam;
impl RecFamily for BatchFam {
    type Rec<'buf> = SensorBatch<'buf>;
}

/// Record family for the borrowed [`SensorEvent`] (the `flat_map` output and
/// the ClickHouse row).
#[derive(Debug)]
struct EventFam;
impl RecFamily for EventFam {
    type Rec<'buf> = SensorEvent<'buf>;
}

/// Sharding key: the `sensor` column — one sensor always lands on one shard,
/// matching a `Distributed` DDL of `xxHash64(sensor)`. A fn item, not a
/// closure: the extractor is higher-ranked over the payload lifetime (the
/// same rule as `map_rec` on a borrowing family).
fn sensor_key<'a>(row: &'a SensorEvent<'_>) -> ShardKey<'a> {
    ShardKey::Str(row.sensor)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Constructor owns init: logs, the metrics exporter (installed before any
    // handle can exist), and the shared I/O runtime.
    let config_path = std::env::var("ETL_CONFIG")
        .unwrap_or_else(|_| "crates/etl/examples/kafka_avro_flatmap_clickhouse.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: Kafka ───────────────────────────────────────────────────
    let source = KafkaSource::from_component_config(&pipeline.config().source)?;

    // ── Deserializer: Avro, borrowed (zero-copy) ────────────────────────
    // `build_fast` decodes each datum directly into `SensorBatch<'buf>`; its
    // `&str` fields alias the payload buffer. `raw` mode (inline writer schema)
    // avoids a registry; `build_fast` rejects any `reader_schema`.
    let deser_section = pipeline
        .config()
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?;
    let deserializer =
        AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())?
            .build_fast::<BatchFam>()?;

    // ── Sink: ClickHouse Native, sharded by sensor ──────────────────────
    // `format: native` fetches `system.columns` and hands the encoder the
    // real column types (so `batch_ts_ms`'s `DateTime64(3)` is laid out as an
    // Int64). The encoder is `Clone`: the terminal stage mints one per shard.
    let sink = etl::clickhouse::config::from_component_config(&pipeline.config().sink)?;
    // No-op unless the YAML opts into `distributed_check`; with it, startup
    // fails fast if the sink topology drifts from the cluster + DDL.
    pipeline.block_on(sink.validate_distributed())?;
    // Weights come from the validated YAML — router and endpoints can't
    // drift. With a single shard this routes identically to the default
    // (everything to shard 0); with N it matches `xxHash64(sensor)`.
    let router = sink.router::<EventFam>(sensor_key);
    let native = pipeline.block_on(sink.native_schema())?;
    let encoder = NativeEncoder::<EventFam>::new(native);

    // ── The chain, and run ──────────────────────────────────────────────
    // `flat_map` explodes each batch's event array into one borrowed row per
    // event; `filter` drops negative readings. Both take plain closures —
    // only `map_rec`/`try_map_rec` on a borrowing family need `fn` items. The
    // borrow lives to the sink handoff, where `NativeEncoder::encode` copies
    // each field into its column buffer on the pipeline thread.
    let report = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            chain::<BatchFam, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                .flat_map::<EventFam, _>(|batch, out| {
                    let (sensor, batch_ts_ms) = (batch.sensor, batch.batch_ts_ms);
                    for event in batch.events {
                        out.emit(SensorEvent {
                            sensor,
                            batch_ts_ms: DateTime64Millis(batch_ts_ms),
                            name: event.name,
                            value: event.value,
                            unit: event.unit,
                        });
                    }
                })
                .filter(|event: &SensorEvent<'_>| event.value >= 0)
                .sink(
                    encoder.clone(),
                    router.clone(), // Clone, not Copy: one router per chain lane
                    ChunkConfig::default(),
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .run(source)?;

    report.log();
    std::process::exit(report.exit_code());
}
