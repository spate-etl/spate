//! Multi-sink split: Kafka → Avro (borrowed) → `flat_map` → **split** into one
//! ClickHouse table per record kind.
//!
//! The zero-copy chain of `kafka_avro_flatmap_clickhouse.rs`, extended with a
//! [`split`](etl::ops::ChainBuilder) terminal: one interleaved stream of typed
//! metric readings fans out to **N tables, each with its own schema, encoder,
//! and batch/linger tuning**, instead of one wide table. A `gauge` reading
//! carries a number, a `text` reading a string — genuinely different columns —
//! so each lands in a table indexed for its own shape.
//!
//! # The routing the user writes
//!
//! Each destination is declared once with [`SplitBuilder::add`](etl::ops::SplitBuilder),
//! which returns a `Copy` typed handle. The `route` closure then does one
//! `match` — classify **and** extract in the same arm — and dispatches with
//! `out.emit(handle, row)`; a reading whose `kind` matches no branch follows
//! the `unmatched` policy (`Skip` here: dropped and counted on
//! `etl_operator_records_dropped_total{reason="unrouted"}`).
//!
//! # At-least-once across tables
//!
//! Each branch clones the source batch's ack, so a Kafka batch's offsets commit
//! only after **every** table its readings landed in has durably written; a
//! failed write to any one table stalls the batch and replays it. This falls
//! straight out of the shared ack handle — the split terminal adds no new
//! delivery machinery.
//!
//! ```sql
//! CREATE TABLE metrics_gauge (
//!     host   LowCardinality(String),
//!     ts_ms  DateTime64(3),
//!     name   LowCardinality(String),
//!     value  Int64
//! ) ENGINE = MergeTree ORDER BY (host, name, ts_ms);
//!
//! CREATE TABLE metrics_text (
//!     host   LowCardinality(String),
//!     ts_ms  DateTime64(3),
//!     name   LowCardinality(String),
//!     text   String
//! ) ENGINE = MergeTree ORDER BY (host, name, ts_ms);
//! ```
//!
//! ```sh
//! cargo run --release -p etl --features full,avro-fast \
//!   --example multi_table_split
//! ```
//!
//! Needs Kafka and ClickHouse (`KAFKA_BROKERS`, `CLICKHOUSE_URL`), a topic of
//! bare-datum Avro `MetricBatch` messages, and both target tables. SIGTERM
//! drains gracefully; probes: `curl localhost:9090/readyz`.

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use etl::avro::AvroDeserializerBuilder;
use etl::clickhouse::{DateTime64Millis, NativeEncoder, ShardKey};
use etl::kafka::KafkaSource;
use etl::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// One Kafka datum: a host's batch of typed readings, decoded **borrowed**.
#[derive(Debug, Deserialize)]
struct MetricBatch<'a> {
    #[serde(borrow)]
    host: &'a str,
    ts_ms: i64,
    #[serde(borrow)]
    readings: Vec<Reading<'a>>,
}

/// One reading. `kind` selects the destination table; a gauge uses `value`, a
/// text metric uses `text` — the fields the two tables do not share.
#[derive(Debug, Deserialize)]
struct Reading<'a> {
    kind: &'a str,
    name: &'a str,
    value: i64,
    text: &'a str,
}

/// The `flat_map` output: a reading enriched with its batch's host and time,
/// still borrowing the payload. This is the family the split classifies.
#[derive(Debug)]
struct Sample<'a> {
    host: &'a str,
    ts_ms: i64,
    kind: &'a str,
    name: &'a str,
    value: i64,
    text: &'a str,
}

/// The gauge table's row (numeric `value`, no `text` column). Field order
/// matches the YAML `columns` — Native maps positionally.
#[derive(Debug, Serialize)]
struct GaugeRow<'a> {
    host: &'a str,
    ts_ms: DateTime64Millis,
    name: &'a str,
    value: i64,
}

/// The text table's row (string `text`, no `value` column).
#[derive(Debug, Serialize)]
struct TextRow<'a> {
    host: &'a str,
    ts_ms: DateTime64Millis,
    name: &'a str,
    text: &'a str,
}

#[derive(Debug)]
struct BatchFam;
impl RecFamily for BatchFam {
    type Rec<'buf> = MetricBatch<'buf>;
}

#[derive(Debug)]
struct SampleFam;
impl RecFamily for SampleFam {
    type Rec<'buf> = Sample<'buf>;
}

#[derive(Debug)]
struct GaugeFam;
impl RecFamily for GaugeFam {
    type Rec<'buf> = GaugeRow<'buf>;
}

#[derive(Debug)]
struct TextFam;
impl RecFamily for TextFam {
    type Rec<'buf> = TextRow<'buf>;
}

/// Shard each table by `host`, matching a `Distributed` DDL of
/// `xxHash64(host)`. `fn` items: higher-ranked over the payload lifetime.
fn gauge_key<'a>(row: &'a GaugeRow<'_>) -> ShardKey<'a> {
    ShardKey::Str(row.host)
}
fn text_key<'a>(row: &'a TextRow<'_>) -> ShardKey<'a> {
    ShardKey::Str(row.host)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::var("ETL_CONFIG")
        .unwrap_or_else(|_| "crates/etl/examples/multi_table_split.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: Kafka ───────────────────────────────────────────────────
    let source = KafkaSource::from_component_config(&pipeline.config().source)?;

    // ── Deserializer: Avro, borrowed (zero-copy) ────────────────────────
    let deser_section = pipeline
        .config()
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?;
    let deserializer =
        AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())?
            .build_fast::<BatchFam>()?;

    // ── Sinks: one ClickHouse table per kind, from the `sinks:` map ──────
    // Each sink mints its own Native encoder (its table's column types) and a
    // host-sharded router, exactly as a single-sink pipeline would — just N of
    // them. Built before `add_sink` moves each sink into its worker pool.
    let gauge_sink =
        etl::clickhouse::config::from_component_config(pipeline.config().sink_config("gauge")?)?;
    let text_sink =
        etl::clickhouse::config::from_component_config(pipeline.config().sink_config("text")?)?;
    let gauge_router = gauge_sink.router::<GaugeFam>(gauge_key);
    let text_router = text_sink.router::<TextFam>(text_key);
    let gauge_enc = NativeEncoder::<GaugeFam>::new(pipeline.block_on(gauge_sink.native_schema())?);
    let text_enc = NativeEncoder::<TextFam>::new(pipeline.block_on(text_sink.native_schema())?);

    // ── The chain, and run ──────────────────────────────────────────────
    let report = pipeline
        .add_sink("gauge", gauge_sink)?
        .add_sink("text", text_sink)?
        .chains(move |ctx| {
            // deserialize → explode readings (enriched with host/time) → split
            // by `kind` into the two tables. `ErrorPolicy::Skip`: an unknown
            // kind is dropped and counted, not fatal.
            let mut split = chain::<BatchFam, _>(deserializer.clone())
                // Clone: `ctx.sink(...)` below borrows `ctx`, so `ctx.pipeline`
                // must not be moved out of it.
                .with_metrics(ctx.pipeline.clone(), "main")
                .flat_map::<SampleFam, _>(|batch, out| {
                    let (host, ts_ms) = (batch.host, batch.ts_ms);
                    for r in batch.readings {
                        out.emit(Sample {
                            host,
                            ts_ms,
                            kind: r.kind,
                            name: r.name,
                            value: r.value,
                            text: r.text,
                        });
                    }
                })
                .split(ChunkConfig::default(), ErrorPolicy::Skip);

            // Declare the branches; each `add` returns a Copy, typed handle.
            let gauge = split.add::<GaugeFam, _, _>(
                gauge_enc.clone(),
                gauge_router.clone(),
                ctx.sink("gauge"),
            );
            let text =
                split.add::<TextFam, _, _>(text_enc.clone(), text_router.clone(), ctx.sink("text"));

            // The routing logic: one match, O(1) dispatch, type-checked per arm.
            split
                .route(move |s: Sample<'_>, out| match s.kind {
                    "gauge" => out.emit(
                        gauge,
                        GaugeRow {
                            host: s.host,
                            ts_ms: DateTime64Millis(s.ts_ms),
                            name: s.name,
                            value: s.value,
                        },
                    ),
                    "text" => out.emit(
                        text,
                        TextRow {
                            host: s.host,
                            ts_ms: DateTime64Millis(s.ts_ms),
                            name: s.name,
                            text: s.text,
                        },
                    ),
                    // Any other kind matches no branch → `unmatched` (Skip).
                    _ => {}
                })
                .build()
        })
        .run(source)?;

    report.log();
    std::process::exit(report.exit_code());
}
