//! Load run: datagen → Avro → flat_map → ClickHouse **Native**, for measuring
//! how fast a sink takes rows.
//!
//! The chain follows `kafka_avro_flatmap_clickhouse.rs` without its
//! `with_metrics` stage and `qty > 0` filter. It decodes the whole event
//! union and keeps placed orders, and the broker is taken out.
//! [`DatagenSource`] generates Avro storefront events in-process,
//! unthrottled, on 32 partitions, and the chain explodes each placed order
//! into one row per line. The sink is sized like a production one: 32 shards,
//! four inserts in flight per shard, batches of up to 1,048,576 rows. With no
//! broker to read from, the pipeline threads and the sink set the rate.
//!
//! # Measuring with it
//!
//! Rows are counted on the server, and the hot path carries no counter of its
//! own. Read `SELECT count()` from the table at two instants and divide by the
//! time between them; this keeps warm-up and the end of the run out of the
//! rate.
//!
//! The seed is fixed, so two runs with the same `SPATE_LOAD_COUNT` write the
//! same rows. Two builds of this example therefore measure the same work.
//! `count` is in events; each placed order becomes one to five rows, and
//! payments and refunds become none.
//!
//! On exit the example prints one JSON line:
//!
//! ```json
//! {"available_parallelism":32,"compression":"lz4","io_threads":2,"state":"completed","threads":null,"wall_ms":8123}
//! ```
//!
//! `wall_ms` runs from the start of `run` to its return. `threads` is
//! `null` when the config leaves it to the runtime to derive. The run ends on
//! its own once the source has generated `count` events, or drains on
//! SIGTERM.
//!
//! # Run it
//!
//! Needs ClickHouse (`CLICKHOUSE_URL`, `CLICKHOUSE_PASSWORD`) and the target
//! table:
//!
//! ```sql
//! CREATE TABLE order_lines (
//!     order_id    UInt64,
//!     placed_at   DateTime64(3),
//!     sku         LowCardinality(String),
//!     qty         UInt32,
//!     unit_cents  UInt32
//! ) ENGINE = MergeTree ORDER BY (order_id, sku)
//! SETTINGS non_replicated_deduplication_window = 100;
//! ```
//!
//! ```sh
//! SPATE_LOAD_COUNT=50000000 cargo run --release -p spate \
//!   --features datagen-avro,avro,clickhouse --example clickhouse_load
//! ```

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  operating
// INDEX-GOAL:  measure how fast a sharded ClickHouse sink takes generated rows
// INDEX-TECH:  datagen, Avro and ClickHouse Native
// INDEX-NEEDS: ClickHouse

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use serde::{Deserialize, Serialize};
use spate::avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
use spate::clickhouse::{
    self, ClickHouseRow, ClickHouseSinkConfig, Compression, DateTime64Millis, NativeEncoder,
    ShardKey,
};
use spate::datagen::{DatagenSource, EVENT_SCHEMA_JSON};
use spate::prelude::*;
use std::path::Path;
use std::time::Instant;

/// One generated event. The writer schema is a top-level Avro union, whose
/// branch index selects the variant **positionally**, so the variants stay in
/// the union's declaration order. Only placed orders produce rows.
#[derive(Debug, Deserialize)]
enum StorefrontEvent {
    OrderPlaced(OrderPlaced),
    PaymentCaptured(serde::de::IgnoredAny),
    RefundIssued(serde::de::IgnoredAny),
}

/// The fields of a placed order the rows need.
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

/// One ClickHouse row per order line. Native maps columns by field
/// declaration order.
#[derive(Debug, Serialize, ClickHouseRow)]
struct OrderLineRow {
    order_id: u64,
    placed_at: DateTime64Millis,
    sku: String,
    qty: u32,
    unit_cents: u32,
}

/// Shard by `order_id`, as a `Distributed` table keyed on `xxHash64(order_id)`
/// would.
fn order_key(row: &OrderLineRow) -> ShardKey<'_> {
    ShardKey::U64(row.order_id)
}

/// The `compression` value as the YAML spells it.
fn compression_name(compression: Compression) -> String {
    match compression {
        Compression::None => "off".to_owned(),
        Compression::Lz4 => "lz4".to_owned(),
        Compression::Zstd(level) => format!("zstd:{level}"),
        other => format!("{other:?}"),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::var("SPATE_CONFIG")
        .unwrap_or_else(|_| "crates/spate/examples/clickhouse_load.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;
    let threads = pipeline.config().pipeline.threads;
    let io_threads = pipeline.config().pipeline.io_threads;

    let source = DatagenSource::from_component_config(&pipeline.config().source)?;

    // The decoder reads against the schema the source writes, taken from the
    // same constant, so the two cannot drift.
    let avro = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(EVENT_SCHEMA_JSON)),
        ..AvroSettings::default()
    };
    let deserializer = AvroDeserializerBuilder::from_settings(&avro, &pipeline.io_handle())?
        .build_serde::<StorefrontEvent>()?;

    let sink_config: ClickHouseSinkConfig = pipeline
        .config()
        .sink_config("default")?
        .deserialize_into()?;
    let compression = compression_name(sink_config.compression);
    let sink = pipeline
        .block_on(clickhouse::config::build(sink_config)?.with_row::<Owned<OrderLineRow>>())?;
    let router = sink.router::<Owned<OrderLineRow>>(order_key);
    let encoder = NativeEncoder::<Owned<OrderLineRow>>::new(sink.native_schema()?);

    let started = Instant::now();
    let report = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            let chunk_cfg = ctx.chunk();
            chain::<Owned<StorefrontEvent>, _>(deserializer.clone())
                .flat_map::<Owned<OrderLineRow>, _>(|event, out| {
                    let StorefrontEvent::OrderPlaced(order) = event else {
                        return;
                    };
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
                .sink(
                    encoder.clone(),
                    router.clone(),
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .run(source)?;
    let wall_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

    report.log();
    let state = match report.state {
        ExitState::Completed => "completed",
        _ => "failed",
    };
    let summary = serde_json::json!({
        "wall_ms": wall_ms,
        "state": state,
        "available_parallelism": std::thread::available_parallelism().map_or(0, usize::from),
        "threads": threads,
        "io_threads": io_threads,
        "compression": compression,
    });
    println!("{summary}");
    std::process::exit(report.exit_code());
}
