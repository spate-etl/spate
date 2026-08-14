//! Fan-out pipeline: Kafka → Avro → flat_map → ClickHouse **Native** (sharded).
//!
//! The columnar twin of `kafka_avro_to_clickhouse.rs`. Where that example
//! totals a placed order into one row through RowBinary, this one keeps the
//! lines: it explodes the order's line array into one row per line with
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
//!   metadata, so the default meta-only router would colocate every line of an
//!   order anyway; a [`DistributedRouter`](spate::clickhouse::DistributedRouter)
//!   keyed on each row's own `order_id` places them the way
//!   `xxHash64(order_id)` would, which is what a `Distributed` read needs.
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
//! 3. `AvroDeserializerBuilder::build_serde::<OrderPlaced>()` — the typed
//!    decoder. The YAML uses `mode: raw` with an inline writer schema, so no
//!    registry is needed.
//! 4. `sink.native_schema()` — fetches `system.columns` and builds the columnar
//!    template; `NativeEncoder::new` mints one encoder per shard on `.clone()`.
//! 5. `.flat_map` fans out the line array; `.filter` drops a line ordering no
//!    units. Native column mapping is **positional** (the `OrderLineRow`
//!    field order must equal the YAML `columns` order), with a first-record
//!    field-name check off the hot path.
//! 6. `sink.router::<Owned<OrderLineRow>>(order_key)` — a record-aware
//!    [`DistributedRouter`](spate::clickhouse::DistributedRouter): each exploded
//!    line routes by **its own** `order_id` field, placing every order's lines
//!    on the shard a ClickHouse `Distributed` table with sharding key
//!    `xxHash64(order_id)` would pick. With the YAML's single shard it routes
//!    identically to the default; scaling out is a YAML change (see the
//!    `shards:` comment there).
//! 7. `.run(source)` — the runtime, reusing the builder's I/O runtime.
//!
//! # Run it
//!
//! Needs Kafka and ClickHouse (`KAFKA_BROKERS`, `CLICKHOUSE_URL`), a topic of
//! bare-datum Avro `OrderPlaced` messages (`mode: raw`, no registry), and the
//! target table. `placed_at` is epoch milliseconds and lands in a real
//! `DateTime64(3)` column: the row declares that scale with the
//! [`DateTime64Millis`] wrapper, which encodes as the raw little-endian
//! `Int64` (exactly the epoch-millis wire value, zero cost), and the column
//! name matches the struct field so the positional check passes.
//!
//! Caveat: the Native leaf writer does not rescale to the column's declared
//! precision. Pointed at a `DateTime64(6)` column, these milli-scaled values
//! would land as 1970-era timestamps. The wrapper makes that
//! checkable: under the YAML's `validate_schema: full` a wrapper/precision
//! mismatch fails fatally on the first record, before anything is inserted
//! (a plain `i64` field declares no scale, so nothing could validate it).
//!
//! ```sql
//! CREATE TABLE order_lines (
//!     order_id    UInt64,
//!     placed_at   DateTime64(3),
//!     sku         LowCardinality(String),
//!     qty         UInt32,
//!     unit_cents  UInt32
//! ) ENGINE = MergeTree ORDER BY (order_id, sku);
//!
//! -- Sharded deployments add a Distributed table for SELECTs whose sharding
//! -- key matches the router (inserts stay direct-to-local); with
//! -- optimize_skip_unused_shards=1, order-filtered queries touch one shard:
//! -- CREATE TABLE order_lines_dist AS order_lines
//! --     ENGINE = Distributed(<cluster>, <db>, order_lines, xxHash64(order_id));
//! ```
//!
//! ```sh
//! cargo run --release -p spate --features full \
//!   --example kafka_avro_flatmap_clickhouse
//! ```
//!
//! SIGTERM drains gracefully; probes: `curl localhost:9090/readyz`.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  production
// INDEX-GOAL:  fan an order's lines into a row each and shard them by order
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
/// One Kafka datum: a placed order, whose nested line array is exploded
/// downstream by `flat_map`. The writer schema carries the order's customer
/// and region too; a target type declares the fields it reads and the rest are
/// discarded.
///
/// `build_serde` decodes the datum into an intermediate value and then reads
/// the target out of it by name, so an undeclared field is decoded and then
/// discarded rather than stepped over, and still costs what decoding it costs.
/// [`build_serde_datum`](spate::avro::AvroDeserializerBuilder::build_serde_datum)
/// steps over it without materializing it.
#[derive(Debug, Deserialize)]
struct OrderPlaced {
    order_id: u64,
    placed_at: i64,
    lines: Vec<OrderLine>,
}

/// One line of an order.
#[derive(Debug, Deserialize)]
struct OrderLine {
    sku: String,
    qty: u32,
    unit_cents: u32,
}

/// The `flat_map` output = one ClickHouse row. **Field order must match the
/// `columns` list in the YAML**, because Native maps fields positionally.
/// [`DateTime64Millis`] declares the timestamp's scale so `validate_schema:
/// full` can check it against the column's declared precision (it still
/// encodes as the raw `Int64`).
#[derive(Debug, Serialize)]
struct OrderLineRow {
    order_id: u64,
    placed_at: DateTime64Millis,
    sku: String,
    qty: u32,
    unit_cents: u32,
}
// ANCHOR_END: record

// ANCHOR: shard_key
/// Sharding key: the `order_id` column, so an order's lines land together,
/// matching a `Distributed` DDL of `xxHash64(order_id)`. A named fn item,
/// because the extractor is a fn pointer and cannot capture.
///
/// `ShardKey::U64` hashes eight little-endian bytes, which is what ClickHouse
/// hashes for a `UInt64` column. The variant has to match the column's
/// declared width; `U32` over the same value hashes differently.
fn order_key(row: &OrderLineRow) -> ShardKey<'_> {
    ShardKey::U64(row.order_id)
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
            .build_serde::<OrderPlaced>()?;

    // ── Sink: ClickHouse Native, sharded by order ───────────────────────
    // `format: native` fetches `system.columns` and hands the encoder the
    // real column types (so `placed_at`'s `DateTime64(3)` is laid out as an
    // Int64). The encoder is `Clone`: the terminal stage mints one per shard.
    // ANCHOR: router
    let sink = spate::clickhouse::config::from_component_config(
        pipeline.config().sink_config("default")?,
    )?;
    // No-op unless the YAML opts into `distributed_check`; with it, startup
    // fails fast if the sink topology drifts from the cluster + DDL.
    pipeline.block_on(sink.validate_distributed())?;
    // Weights come from the validated YAML, so router and endpoints cannot
    // drift. With a single shard this routes identically to the default
    // (everything to shard 0); with N it matches `xxHash64(order_id)`.
    let router = sink.router::<Owned<OrderLineRow>>(order_key);
    // ANCHOR_END: router
    // ANCHOR: encoder
    let native = pipeline.block_on(sink.native_schema())?;
    let encoder = NativeEncoder::<Owned<OrderLineRow>>::new(native);
    // ANCHOR_END: encoder

    // ── The chain, and run ──────────────────────────────────────────────
    // `flat_map` explodes each order's line array into one row per line;
    // `filter` drops a line that orders nothing. It is the only bad quantity
    // the filter can see: `qty` is unsigned, so a negative one fails to decode
    // and takes the whole order with it, one stage earlier. `NativeEncoder::
    // encode` then writes each field into its per-column buffer on the
    // pipeline thread, inside the terminal sink-handoff stage.
    let report = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            let chunk_cfg = ctx.chunk();
            chain::<Owned<OrderPlaced>, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                .flat_map::<Owned<OrderLineRow>, _>(|order, out| {
                    let (order_id, placed_at) = (order.order_id, order.placed_at);
                    for line in order.lines {
                        out.emit(OrderLineRow {
                            order_id,
                            placed_at: DateTime64Millis(placed_at),
                            sku: line.sku,
                            qty: line.qty,
                            unit_cents: line.unit_cents,
                        });
                    }
                })
                .filter(|line: &OrderLineRow| line.qty > 0)
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
