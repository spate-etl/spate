//! The flagship pipeline: Kafka → Avro → chain → sharded ClickHouse.
//!
//! This example is the assembly reference — a production binary in full.
//! [`Pipeline`] owns the process plumbing (telemetry, metrics exporter,
//! the shared I/O runtime, shard queues, sink workers, probes); the code
//! here is only what is genuinely this pipeline's: connector construction,
//! schema validation, and the operator chain. The YAML next to it
//! (`kafka_avro_to_clickhouse.yaml`) carries all tuning; point `SPATE_CONFIG`
//! elsewhere to reconfigure without recompiling.
//!
//! # What the builder desugars to
//!
//! Every step is a thin composition of public primitives — assemblies can
//! drop down to them at any point (see the `spate::pipeline::Pipeline`
//! module docs for the full mapping):
//!
//! - `Pipeline::from_path` — `telemetry::init` → `metrics::install`
//!   (exporter before any handle, so nothing records into the void) →
//!   the `spate-io` tokio runtime → `InflightBudget::new`.
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
//! cargo run --release -p spate --example kafka_avro_to_clickhouse --features full
//! ```
//!
//! SIGTERM drains gracefully: lanes stop, chains flush, sink batches
//! complete (bounded by `checkpoint.drain_timeout`), offsets commit —
//! at-least-once end to end. Probes: `curl localhost:9090/readyz`.
//!
//! The `ANCHOR` comments below mark the regions the site renders. They are
//! stripped from what it shows, and they nest; see `docs/STYLE.md` § 10.

// The examples index renders these fields; see scripts/examples-index.sh.
// INDEX-TIER:  production
// INDEX-GOAL:  load an Avro order stream from Kafka into ClickHouse
// INDEX-TECH:  Kafka, Avro and ClickHouse
// INDEX-NEEDS: Kafka, a schema registry and ClickHouse

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

// ANCHOR: imports
use serde::{Deserialize, Serialize};
use spate::avro::AvroDeserializerBuilder;
use spate::clickhouse::ClickHouseEncoder;
use spate::kafka::KafkaSource;
use spate::prelude::*;
use std::path::Path;
// ANCHOR_END: imports

/// One record, end to end: `Deserialize` reads it from Avro (field names
/// match the writer schema), `Serialize` writes it as RowBinary — where
/// **field order must match the `columns` list in the YAML** (RowBinary
/// carries no names; order is the wire contract).
// ANCHOR: record
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
    customer: String,
    amount_cents: i64,
    ts_ms: i64,
}
// ANCHOR_END: record

// ANCHOR: assembly
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Constructor owns init: JSON logs (RUST_LOG overrides the filter; call
    // `spate::telemetry::init` first to customize), the metrics exporter —
    // installed before any handle can exist — and the shared I/O runtime.
    let config_path = std::env::var("SPATE_CONFIG")
        .unwrap_or_else(|_| "crates/spate/examples/kafka_avro_to_clickhouse.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: Kafka ───────────────────────────────────────────────────
    // One consumer per process; partitions become lanes fanned across
    // pipeline threads. The `source: { kafka: ... }` section is the
    // connector's own schema.
    // ANCHOR: source
    let source = KafkaSource::from_component_config(&pipeline.config().source)?;
    // ANCHOR_END: source

    // ── Deserializer: Confluent-framed Avro ─────────────────────────────
    // Schemas come from the registry via an async fetcher on the I/O
    // runtime; a cache miss never blocks a pipeline thread — the batch
    // retries once the schema lands.
    // ANCHOR: deserializer
    let deser_section = pipeline
        .config()
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?;
    let deserializer =
        AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())?
            .build_serde::<Order>()?;
    // ANCHOR_END: deserializer

    // ── Sink: sharded ClickHouse ────────────────────────────────────────
    // The connector turns its section into everything the builder needs:
    // writer, per-shard replica endpoints, pool tuning, readiness probe.
    // ANCHOR: sink
    let sink = spate::clickhouse::config::from_component_config(
        pipeline.config().sink_config("default")?,
    )?;
    // ANCHOR_END: sink

    // Opt-in fail-fast schema validation (`validate_schema: names|full` in
    // the YAML): checks the configured columns against every replica's
    // live table NOW — before any thread spawns — and hands the encoder
    // the expected schema so the row struct is checked against it on the
    // first record. `off` (the default) returns None and issues no queries.
    // ANCHOR: encoder
    let encoder = match pipeline.block_on(sink.validate_schema())? {
        Some(schema) => ClickHouseEncoder::<Owned<Order>>::with_schema(schema),
        None => ClickHouseEncoder::<Owned<Order>>::new(),
    };
    // ANCHOR_END: encoder

    // ── The chain, and run ──────────────────────────────────────────────
    // One identical chain per pipeline thread, fully monomorphized; the
    // ChainCtx delivers this thread's queue/budget plumbing. Record-level
    // failures follow the per-stage policy: Skip counts and continues;
    // Fail stops the pipeline. Blocks until SIGTERM/SIGINT (drain) or a
    // fatal error.
    let report = pipeline
        .sink(sink)?
        // ANCHOR: chain
        .chains(move |ctx| {
            // The sink's chunking — per-sink `chunk:` in the YAML, or the
            // default — bound before `with_metrics` moves `ctx.pipeline`.
            let chunk_cfg = ctx.chunk();
            chain_owned::<Order, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                // ANCHOR: validate
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
                // ANCHOR_END: validate
                .sink(
                    encoder.clone(),
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        // ANCHOR_END: chain
        .run(source)?;

    report.log();
    std::process::exit(report.exit_code());
}
// ANCHOR_END: assembly
