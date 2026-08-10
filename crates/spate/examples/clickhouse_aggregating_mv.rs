//! Raw orders → Null landing table → Materialized View → AggregatingMergeTree.
//!
//! ClickHouse `AggregateFunction` columns store opaque, version-dependent
//! aggregate *states*, so the sink never writes them directly. Instead it
//! INSERTs plain order rows into an `ENGINE = Null` table, and a
//! `MATERIALIZED VIEW` computes the states (`minState`/`maxState`/
//! `sumMapState`) into the target `AggregatingMergeTree` — a per-region
//! rollup of when orders were placed and how many units of each SKU they
//! carried. ClickHouse owns the state construction and its versioning; the
//! framework just ships rows.
//!
//! This example uses `spate-test`'s in-memory source, so it runs against
//! nothing but ClickHouse.
//!
//! # Run it
//!
//! Needs only ClickHouse (`CLICKHOUSE_URL`, default `http://localhost:8123`).
//! Create the three objects first — target ClickHouse >= 26.1 so insert
//! deduplication reaches the view (exactly-once under at-least-once retries):
//!
//! ```sql
//! -- 1. Target: the fixed-schema AggregatingMergeTree (already exists in prod).
//! CREATE TABLE orders_agg (
//!     region            String,
//!     first_placed_at   AggregateFunction(min, DateTime),
//!     last_placed_at    AggregateFunction(max, DateTime),
//!     qty_by_sku        AggregateFunction(sumMap, Map(String, UInt64))
//! ) ENGINE = AggregatingMergeTree ORDER BY region
//!   SETTINGS non_replicated_deduplication_window = 100;  -- dedup window
//!
//! -- 2. Landing table: plain columns, stores nothing (the sink writes here).
//! CREATE TABLE orders_null (
//!     region     String,
//!     placed_at  DateTime,
//!     qty_by_sku Map(String, UInt64)
//! ) ENGINE = Null;
//!
//! -- 3. MV: raw orders -> aggregate states -> target.
//! CREATE MATERIALIZED VIEW orders_mv TO orders_agg AS
//! SELECT region, minState(placed_at) AS first_placed_at,
//!        maxState(placed_at) AS last_placed_at,
//!        sumMapState(qty_by_sku) AS qty_by_sku
//! FROM orders_null GROUP BY region;
//! ```
//!
//! ```sh
//! cargo run -p spate --example clickhouse_aggregating_mv --features full
//! ```
//!
//! Read the finalized values back with the `-Merge` combinators — the stored
//! columns stay `AggregateFunction`, and `FINAL` alone does not finalize them:
//!
//! ```sql
//! SELECT region, minMerge(first_placed_at), maxMerge(last_placed_at),
//!        sumMapMerge(qty_by_sku)
//! FROM orders_agg GROUP BY region;
//! ```
//!
//! An alternative row shape carries one `(sku, qty)` pair per row; the view
//! would then use `sumMapState(map(sku, qty))`. The whole-`Map` shape used
//! here — an order's lines already collapsed to a per-SKU total — exercises
//! the sink's `Map(String, UInt64)` encoding.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  production
// INDEX-GOAL:  roll orders up per region through a Null landing table into an AggregatingMergeTree
// INDEX-TECH:  ClickHouse
// INDEX-NEEDS: ClickHouse

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use serde::Serialize;
use spate::clickhouse::ClickHouseEncoder;
use spate::prelude::*;
use spate::source::LaneId;
use spate_test::{TestDeserializer, memory_source};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

/// One placed order, already collapsed to a per-SKU quantity. `Serialize`
/// writes it as RowBinary into the Null landing table, where **field order
/// must match the `columns` list in the YAML** (RowBinary carries no names;
/// order is the wire contract).
#[derive(Clone, Debug, Serialize)]
struct OrderRollup {
    region: String,
    placed_at: u32,                    // epoch seconds -> DateTime
    qty_by_sku: BTreeMap<String, u64>, // -> Map(String, UInt64)
}

/// Parse a demo payload `region|placed_at|SKU=qty,SKU=qty` into an
/// [`OrderRollup`]. In a real pipeline this is the deserializer's job (Avro,
/// JSON, ...); here a tiny hand-parser keeps the example self-contained.
fn parse_order(line: &[u8]) -> Option<OrderRollup> {
    let line = std::str::from_utf8(line).ok()?;
    let mut parts = line.split('|');
    let region = parts.next()?.to_string();
    let placed_at: u32 = parts.next()?.parse().ok()?;
    let mut qty_by_sku = BTreeMap::new();
    for kv in parts.next()?.split(',').filter(|s| !s.is_empty()) {
        let (sku, qty) = kv.split_once('=')?;
        qty_by_sku.insert(sku.to_string(), qty.parse().ok()?);
    }
    Some(OrderRollup {
        region,
        placed_at,
        qty_by_sku,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Demos want pretty logs: init telemetry before the builder's JSON
    // default becomes a no-op (first init wins).
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    let config_path = std::env::var("SPATE_CONFIG")
        .unwrap_or_else(|_| "crates/spate/examples/clickhouse_aggregating_mv.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: in-memory (a Kafka source would supply this from a broker) ─
    let (source, handle) = memory_source();

    // ── Sink: ClickHouse, pointed at the Null landing table ────────────────
    let sink = spate::clickhouse::config::from_component_config(
        pipeline.config().sink_config("default")?,
    )?;

    // Fail-fast validation against the *Null* table (plain columns). If this
    // sink is ever repointed at the AggregatingMergeTree, validation fails
    // here with an actionable "insert into a Null table + MV" error — the
    // sink cannot write aggregate states directly.
    let encoder = match pipeline.block_on(sink.validate_schema())? {
        Some(schema) => ClickHouseEncoder::<Owned<OrderRollup>>::with_schema(schema),
        None => ClickHouseEncoder::<Owned<OrderRollup>>::new(),
    };

    // ── Chain: bytes -> OrderRollup -> RowBinary -> Null table ─────────────
    let runtime = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::split_on(b'\n'))
                .with_metrics(ctx.pipeline, "main")
                .try_map(
                    |line: Vec<u8>| parse_order(&line).ok_or("malformed order line"),
                    ErrorPolicy::Skip,
                )
                .sink(
                    encoder.clone(),
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false, // the demo triggers shutdown itself
            ..RuntimeOptions::default()
        })
        .into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    // Feed a handful of raw orders across two regions (newline-framed).
    // Timestamps are epoch seconds on 2026-01-01, deliberately out of order:
    // the view's min/max states are what put them back in it.
    let p0 = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let mut last = 0;
    for payload in [
        &b"eu-west|1767225600|KBD-01=1,MSE-01=2"[..],
        b"eu-west|1767229200|KBD-01=2,MON-01=3",
        b"eu-west|1767227400|MSE-01=1",
        b"us-east|1767238800|CBL-01=10",
        b"us-east|1767235200|CBL-01=5,DCK-01=7",
    ] {
        last = handle.push(p0, Some(b"demo"), payload);
    }

    // Watermarks advance once the sink acknowledges durably — wait for the
    // commit covering the last event, then drain gracefully.
    let deadline = Instant::now() + Duration::from_secs(10);
    while handle.last_committed(p0) != Some(last + 1) {
        assert!(Instant::now() < deadline, "commit not observed in time");
        std::thread::sleep(Duration::from_millis(20));
    }
    shutdown.trigger();
    let report = join.join().expect("pipeline thread")?;

    report.log();
    println!(
        "\nRaw orders landed in orders_null; the MV built min/max/sumMap states \
         into orders_agg. Read back the aggregates with:\n  \
         SELECT region, minMerge(first_placed_at), maxMerge(last_placed_at), \
         sumMapMerge(qty_by_sku) FROM orders_agg GROUP BY region;"
    );
    std::process::exit(report.exit_code());
}
