//! The flagship pipeline: Kafka → Avro → chain → sharded ClickHouse.
//!
//! This example is the assembly reference — a production binary in full.
//! [`Pipeline`] owns the process plumbing (telemetry, metrics exporter,
//! the shared I/O runtime, shard queues, sink workers, probes); the code
//! here is only what is genuinely this pipeline's: connector construction,
//! schema validation, and the operator chain. The YAML next to it
//! (`kafka_avro_to_clickhouse.yaml`) carries all tuning; point `ETL_CONFIG`
//! elsewhere to reconfigure without recompiling.
//!
//! # What the builder desugars to
//!
//! Every step is a thin composition of public primitives — assemblies can
//! drop down to them at any point (see the `etl::pipeline::Pipeline`
//! module docs for the full mapping):
//!
//! - `Pipeline::from_path` — `telemetry::init` → `metrics::install`
//!   (exporter before any handle, so nothing records into the void) →
//!   the `etl-io` tokio runtime → `InflightBudget::new`.
//! - `.sink(sink)` — `SinkBundle::into_parts` → `shard_queues` →
//!   per-shard `SinkShardMetrics` → `SinkPool::spawn` → drain + probe
//!   wiring (the probe uses its own client set).
//! - `.chains(..)` — the per-thread chain factory, with queues/budget/name
//!   delivered through [`ChainCtx`].
//! - `.run(source)` — `PipelineRuntime::new(...).run()`, reusing the
//!   builder's I/O runtime.
//!
//! # Run it
//!
//! Needs Kafka, a Confluent-compatible schema registry, and ClickHouse
//! (set `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `CLICKHOUSE_URL`), plus a
//! target table — note the deduplication window, without which retry
//! idempotency silently does nothing on plain MergeTree:
//!
//! ```sql
//! CREATE TABLE orders (
//!     id           UInt64,
//!     customer     String,
//!     amount_cents Int64,
//!     ts_ms        Int64
//! ) ENGINE = MergeTree ORDER BY id
//!   SETTINGS non_replicated_deduplication_window = 100;
//! ```
//!
//! ```sh
//! cargo run --release -p etl --example kafka_avro_to_clickhouse --features full
//! ```
//!
//! SIGTERM drains gracefully: lanes stop, chains flush, sink batches
//! complete (bounded by `checkpoint.drain_timeout`), offsets commit —
//! at-least-once end to end. Probes: `curl localhost:9090/readyz`.

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use etl::avro::AvroDeserializerBuilder;
use etl::clickhouse::ClickHouseEncoder;
use etl::kafka::KafkaSource;
use etl::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// One record, end to end: `Deserialize` reads it from Avro (field names
/// match the writer schema), `Serialize` writes it as RowBinary — where
/// **field order must match the `columns` list in the YAML** (RowBinary
/// carries no names; order is the wire contract).
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
    customer: String,
    amount_cents: i64,
    ts_ms: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Constructor owns init: JSON logs (RUST_LOG overrides the filter; call
    // `etl::telemetry::init` first to customize), the metrics exporter —
    // installed before any handle can exist — and the shared I/O runtime.
    let config_path = std::env::var("ETL_CONFIG")
        .unwrap_or_else(|_| "crates/etl/examples/kafka_avro_to_clickhouse.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: Kafka ───────────────────────────────────────────────────
    // One consumer per process; partitions become lanes fanned across
    // pipeline threads. The `source: { kafka: ... }` section is the
    // connector's own schema.
    let source = KafkaSource::from_component_config(&pipeline.config().source)?;

    // ── Deserializer: Confluent-framed Avro ─────────────────────────────
    // Schemas come from the registry via an async fetcher on the I/O
    // runtime; a cache miss never blocks a pipeline thread — the batch
    // retries once the schema lands.
    let deser_section = pipeline
        .config()
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?;
    let deserializer =
        AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())?
            .build_serde::<Order>();

    // ── Sink: sharded ClickHouse ────────────────────────────────────────
    // The connector turns its section into everything the builder needs:
    // writer, per-shard replica endpoints, pool tuning, readiness probe.
    let sink = etl::clickhouse::config::from_component_config(&pipeline.config().sink)?;

    // Opt-in fail-fast schema validation (`validate_schema: names|full` in
    // the YAML): checks the configured columns against every replica's
    // live table NOW — before any thread spawns — and hands the encoder
    // the expected schema so the row struct is checked against it on the
    // first record. `off` (the default) returns None and issues no queries.
    let encoder = match pipeline.block_on(sink.validate_schema())? {
        Some(schema) => ClickHouseEncoder::<Owned<Order>>::with_schema(schema),
        None => ClickHouseEncoder::<Owned<Order>>::new(),
    };

    // ── The chain, and run ──────────────────────────────────────────────
    // One identical chain per pipeline thread, fully monomorphized; the
    // ChainCtx delivers this thread's queue/budget plumbing. Record-level
    // failures follow the per-stage policy: Skip counts and continues;
    // Fail stops the pipeline. Blocks until SIGTERM/SIGINT (drain) or a
    // fatal error.
    let report = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            chain_owned::<Order, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                .try_map(
                    |order: Order| {
                        if order.amount_cents >= 0 {
                            Ok(order)
                        } else {
                            Err("negative amount")
                        }
                    },
                    ErrorPolicy::Skip,
                )
                .sink(
                    encoder.clone(),
                    KeyHashRouter,
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
