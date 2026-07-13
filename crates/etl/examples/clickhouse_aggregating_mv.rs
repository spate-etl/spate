//! Raw events → Null landing table → Materialized View → AggregatingMergeTree.
//!
//! ClickHouse `AggregateFunction` columns store opaque, version-dependent
//! aggregate *states*, so the sink never writes them directly. Instead it
//! INSERTs plain event rows into an `ENGINE = Null` table, and a
//! `MATERIALIZED VIEW` computes the states (`minState`/`maxState`/
//! `sumMapState`) into the target `AggregatingMergeTree`. ClickHouse owns the
//! state construction and its versioning; the framework just ships rows.
//!
//! This example uses `etl-test`'s in-memory source, so it runs against
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
//! CREATE TABLE events_agg (
//!     bucket String,
//!     dt_min AggregateFunction(min, DateTime),
//!     dt_max AggregateFunction(max, DateTime),
//!     counts AggregateFunction(sumMap, Map(String, UInt64))
//! ) ENGINE = AggregatingMergeTree ORDER BY bucket
//!   SETTINGS non_replicated_deduplication_window = 100;  -- dedup window
//!
//! -- 2. Landing table: plain columns, stores nothing (the sink writes here).
//! CREATE TABLE events_null (
//!     bucket String,
//!     dt     DateTime,
//!     counts Map(String, UInt64)
//! ) ENGINE = Null;
//!
//! -- 3. MV: raw events -> aggregate states -> target.
//! CREATE MATERIALIZED VIEW events_mv TO events_agg AS
//! SELECT bucket, minState(dt) AS dt_min, maxState(dt) AS dt_max,
//!        sumMapState(counts) AS counts
//! FROM events_null GROUP BY bucket;
//! ```
//!
//! ```sh
//! cargo run -p etl --example clickhouse_aggregating_mv --features full
//! ```
//!
//! Read the finalized values back with the `-Merge` combinators — the stored
//! columns stay `AggregateFunction`, and `FINAL` alone does not finalize them:
//!
//! ```sql
//! SELECT bucket, minMerge(dt_min), maxMerge(dt_max), sumMapMerge(counts)
//! FROM events_agg GROUP BY bucket;
//! ```
//!
//! An alternative event shape carries one `(metric, value)` pair per row; the
//! view would then use `sumMapState(map(metric, value))`. The whole-`Map`
//! shape used here exercises the sink's `Map(String, UInt64)` encoding.

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use etl::clickhouse::ClickHouseEncoder;
use etl::prelude::*;
use etl::source::LaneId;
use etl_test::{TestDeserializer, memory_source};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

/// One raw event. `Serialize` writes it as RowBinary into the Null landing
/// table, where **field order must match the `columns` list in the YAML**
/// (RowBinary carries no names; order is the wire contract).
#[derive(Clone, Debug, Serialize)]
struct Event {
    bucket: String,
    dt: u32,                       // epoch seconds -> DateTime
    counts: BTreeMap<String, u64>, // -> Map(String, UInt64)
}

/// Parse a demo payload `bucket|dt|k1=v1,k2=v2` into an [`Event`]. In a real
/// pipeline this is the deserializer's job (Avro, JSON, ...); here a tiny
/// hand-parser keeps the example self-contained.
fn parse_event(line: &[u8]) -> Option<Event> {
    let line = std::str::from_utf8(line).ok()?;
    let mut parts = line.split('|');
    let bucket = parts.next()?.to_string();
    let dt: u32 = parts.next()?.parse().ok()?;
    let mut counts = BTreeMap::new();
    for kv in parts.next()?.split(',').filter(|s| !s.is_empty()) {
        let (k, v) = kv.split_once('=')?;
        counts.insert(k.to_string(), v.parse().ok()?);
    }
    Some(Event { bucket, dt, counts })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Demos want pretty logs: init telemetry before the builder's JSON
    // default becomes a no-op (first init wins).
    etl::telemetry::init(etl::telemetry::LogFormat::Pretty, "info");

    let config_path = std::env::var("ETL_CONFIG")
        .unwrap_or_else(|_| "crates/etl/examples/clickhouse_aggregating_mv.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: in-memory (a Kafka source would supply this from a broker) ─
    let (source, handle) = memory_source();

    // ── Sink: ClickHouse, pointed at the Null landing table ────────────────
    let sink =
        etl::clickhouse::config::from_component_config(pipeline.config().sink_config("default")?)?;

    // Fail-fast validation against the *Null* table (plain columns). If this
    // sink is ever repointed at the AggregatingMergeTree, validation fails
    // here with an actionable "insert into a Null table + MV" error — the
    // sink cannot write aggregate states directly.
    let encoder = match pipeline.block_on(sink.validate_schema())? {
        Some(schema) => ClickHouseEncoder::<Owned<Event>>::with_schema(schema),
        None => ClickHouseEncoder::<Owned<Event>>::new(),
    };

    // ── Chain: bytes -> Event -> RowBinary -> Null table ───────────────────
    let runtime = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            chain_owned::<Vec<u8>, _>(TestDeserializer::split_on(b'\n'))
                .with_metrics(ctx.pipeline, "main")
                .try_map(
                    |line: Vec<u8>| parse_event(&line).ok_or("malformed event line"),
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
        .runtime_options(RuntimeOptions {
            handle_signals: false, // the demo triggers shutdown itself
            ..RuntimeOptions::default()
        })
        .into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    // Feed a handful of raw events across two buckets (newline-framed).
    let p0 = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let mut last = 0;
    for payload in [
        &b"a|1000|x=1,y=2"[..],
        b"a|2000|x=2,z=3",
        b"a|1500|y=1",
        b"b|5000|p=10",
        b"b|4000|p=5,q=7",
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
        "\nRaw events landed in events_null; the MV built min/max/sumMap states \
         into events_agg. Read back the aggregates with:\n  \
         SELECT bucket, minMerge(dt_min), maxMerge(dt_max), sumMapMerge(counts) \
         FROM events_agg GROUP BY bucket;"
    );
    std::process::exit(report.exit_code());
}
