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
//!     order_id    UInt64,
//!     customer_id UInt32,
//!     region      LowCardinality(String),
//!     placed_at   DateTime64(3),
//!     total_cents UInt64
//! ) ENGINE = MergeTree ORDER BY order_id
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

// The `ANCHOR` comments below mark the regions the site renders. They are
// stripped from what it shows, and they nest; see docs/STYLE.md § 10.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  production
// INDEX-GOAL:  load an Avro order stream from Kafka into ClickHouse
// INDEX-TECH:  Kafka, Avro and ClickHouse
// INDEX-NEEDS: Kafka, a schema registry and ClickHouse

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

// ANCHOR: imports
use serde::{Deserialize, Serialize};
use spate::avro::AvroDeserializerBuilder;
use spate::clickhouse::{ClickHouseEncoder, DateTime64Millis};
use spate::kafka::KafkaSource;
use spate::prelude::*;
use std::path::Path;
// ANCHOR_END: imports

/// The two ends of the pipeline, and why they are two types. `Deserialize`
/// reads [`OrderPlaced`] from Avro, so its fields match the writer schema —
/// including the nested `lines` array. `Serialize` writes [`OrderRow`] as
/// RowBinary, where **field order must match the `columns` list in the YAML**
/// (RowBinary carries no names; order is the wire contract). The chain's
/// `try_map` is what turns one into the other.
// ANCHOR: record
#[derive(Debug, Deserialize)]
struct OrderPlaced {
    order_id: u64,
    customer_id: u32,
    region: String,
    placed_at: i64,
    lines: Vec<OrderLine>,
}

/// Only what the total needs: a target type declares the fields it reads, not
/// the whole record, so the writer schema's `sku` needs no field here.
///
/// That is a convenience, not a saving. `build_serde` decodes the datum into
/// an intermediate value and then reads the target out of it by name, so an
/// undeclared field is still decoded before it is discarded.
/// `build_serde_datum` is the path that skips it without materializing it.
#[derive(Debug, Deserialize)]
struct OrderLine {
    qty: u32,
    unit_cents: u32,
}

/// One ClickHouse row. [`DateTime64Millis`] declares the timestamp's scale so
/// `validate_schema: full` can check it against the column's declared
/// precision (it still encodes as the raw `Int64`).
#[derive(Debug, Serialize)]
struct OrderRow {
    order_id: u64,
    customer_id: u32,
    region: String,
    placed_at: DateTime64Millis,
    total_cents: u64,
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
            .build_serde::<OrderPlaced>()?;
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
        Some(schema) => ClickHouseEncoder::<Owned<OrderRow>>::with_schema(schema),
        None => ClickHouseEncoder::<Owned<OrderRow>>::new(),
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
            chain_owned::<OrderPlaced, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                // ANCHOR: validate
                // Total an order's lines into its row, and reject one whose
                // total cannot be stated: an order with no lines to total,
                // and one whose total does not fit the column, are both
                // malformed rather than zero.
                .try_map(
                    |order: OrderPlaced| {
                        if order.lines.is_empty() {
                            return Err("order has no lines");
                        }
                        let total_cents = order
                            .lines
                            .iter()
                            .try_fold(0u64, |total, line| {
                                u64::from(line.qty)
                                    .checked_mul(u64::from(line.unit_cents))
                                    .and_then(|amount| total.checked_add(amount))
                            })
                            .ok_or("order total overflows the column")?;
                        Ok(OrderRow {
                            order_id: order.order_id,
                            customer_id: order.customer_id,
                            region: order.region,
                            placed_at: DateTime64Millis(order.placed_at),
                            total_cents,
                        })
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
