//! Multi-sink split: Kafka → Avro → **split** into one ClickHouse table per
//! event kind.
//!
//! One interleaved stream of storefront events fans out to **N tables, each
//! with its own schema, encoder, and batch/linger tuning**, instead of one
//! wide table, through a [`split`](spate::ops::ChainBuilder) terminal. A
//! captured payment carries an amount; a refund carries an amount and a
//! reason, so the two need different columns and each lands in a table shaped
//! for it.
//!
//! The stream arrives as a **top-level Avro union**, the idiomatic spelling of
//! a sum type, which decodes straight into a Rust enum: the wire branch index
//! selects the variant positionally. Classification is therefore a `match` on
//! a decoded variant rather than a string compare on a discriminator field.
//!
//! # The routing the user writes
//!
//! Each destination is declared once with [`SplitBuilder::add`](spate::ops::SplitBuilder),
//! which returns a `Copy` typed handle. The `route` closure then does one
//! `match`, classifying **and** extracting in the same arm, and dispatches
//! with `out.emit(handle, row)`; an event kind with no branch follows the
//! `unmatched` policy (`Skip` here: dropped and counted on
//! `spate_operator_records_dropped_total{reason="unrouted"}`). A placed order
//! is that kind: this pipeline settles money and has no table for it.
//!
//! # At-least-once across tables
//!
//! Each branch clones the source batch's ack, so a Kafka batch's offsets commit
//! only after **every** table its events landed in has durably written; a
//! failed write to any one table stalls the batch and replays it. This falls
//! out of the shared ack handle; the split terminal adds no new delivery
//! machinery.
//!
//! ```sql
//! CREATE TABLE payments (
//!     order_id     UInt64,
//!     amount_cents UInt64
//! ) ENGINE = MergeTree ORDER BY order_id;
//!
//! CREATE TABLE refunds (
//!     order_id     UInt64,
//!     amount_cents UInt64,
//!     reason       LowCardinality(String)
//! ) ENGINE = MergeTree ORDER BY order_id;
//! ```
//!
//! ```sh
//! cargo run --release -p spate --features full \
//!   --example multi_table_split
//! ```
//!
//! Needs Kafka and ClickHouse (`KAFKA_BROKERS`, `CLICKHOUSE_URL`), a topic of
//! bare-datum Avro storefront-event messages, and both target tables. SIGTERM
//! drains gracefully; probes: `curl localhost:9090/readyz`.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  production
// INDEX-GOAL:  route payments and refunds to a table each from one event stream
// INDEX-TECH:  Kafka and ClickHouse
// INDEX-NEEDS: Kafka and ClickHouse

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use serde::{Deserialize, Serialize};
use spate::avro::AvroDeserializerBuilder;
use spate::clickhouse::{NativeEncoder, ShardKey};
use spate::kafka::KafkaSource;
use spate::prelude::*;
use std::path::Path;

/// One Kafka datum: whichever of the three storefront events it happens to
/// be. The writer schema is a top-level Avro union, the idiomatic spelling
/// of a sum type, and the branch is selected **positionally**, so these
/// variants must stay in the union's declaration order.
#[derive(Debug, Deserialize)]
enum StorefrontEvent {
    /// Routed nowhere by this pipeline, and decoded as
    /// [`IgnoredAny`](serde::de::IgnoredAny) to say so at the type level:
    /// no arm reads a placed order, so no field is declared for one. It
    /// exercises the `unmatched` policy below.
    ///
    /// The branch is still decoded. `build_serde` materializes the datum and
    /// then reads the target out of it, so `IgnoredAny` discards a value that
    /// already exists rather than stepping over the bytes.
    /// [`build_serde_datum`](spate::avro::AvroDeserializerBuilder::build_serde_datum)
    /// skips it without materializing it, and this pipeline declares no
    /// `reader_schema`, so it could take that path.
    OrderPlaced(serde::de::IgnoredAny),
    PaymentCaptured(PaymentRow),
    RefundIssued(RefundRow),
}

/// The payments table's row, and the decoded shape of the event that fills
/// it. One type serves both, because the event is the row here. Field order
/// matches the YAML `columns`; Native maps positionally.
#[derive(Debug, Deserialize, Serialize)]
struct PaymentRow {
    order_id: u64,
    amount_cents: u64,
}

/// The refunds table's row, which carries a `reason` a payment has no column
/// for. That is why there are two tables rather than one wide one.
#[derive(Debug, Deserialize, Serialize)]
struct RefundRow {
    order_id: u64,
    amount_cents: u64,
    reason: String,
}

/// Shard both tables by `order_id`, matching a `Distributed` DDL of
/// `xxHash64(order_id)`. It is the only field the three events share, so it
/// is the only key that can colocate an order's payment and its refund on one
/// shard. Named fn items: the extractor is a fn pointer, so it cannot
/// capture.
fn payment_key(row: &PaymentRow) -> ShardKey<'_> {
    ShardKey::U64(row.order_id)
}
fn refund_key(row: &RefundRow) -> ShardKey<'_> {
    ShardKey::U64(row.order_id)
}

// ANCHOR: assembly
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::var("SPATE_CONFIG")
        .unwrap_or_else(|_| "crates/spate/examples/multi_table_split.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: Kafka ───────────────────────────────────────────────────
    let source = KafkaSource::from_component_config(&pipeline.config().source)?;

    // ── Deserializer: Avro, typed ───────────────────────────────────────
    let deser_section = pipeline
        .config()
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?;
    let deserializer =
        AvroDeserializerBuilder::from_component(deser_section, &pipeline.io_handle())?
            .build_serde::<StorefrontEvent>()?;

    // ── Sinks: one ClickHouse table per event kind, from the `sinks:` map ─
    // Each sink mints its own Native encoder (its table's column types) and an
    // order-sharded router, as a single-sink pipeline does, with N of them
    // here. Built before `add_sink` moves each sink into its worker pool.
    let payments_sink = spate::clickhouse::config::from_component_config(
        pipeline.config().sink_config("payments")?,
    )?;
    let refunds_sink = spate::clickhouse::config::from_component_config(
        pipeline.config().sink_config("refunds")?,
    )?;
    let payments_router = payments_sink.router::<Owned<PaymentRow>>(payment_key);
    let refunds_router = refunds_sink.router::<Owned<RefundRow>>(refund_key);
    let payments_enc =
        NativeEncoder::<Owned<PaymentRow>>::new(pipeline.block_on(payments_sink.native_schema())?);
    let refunds_enc =
        NativeEncoder::<Owned<RefundRow>>::new(pipeline.block_on(refunds_sink.native_schema())?);

    // ── The chain, and run ──────────────────────────────────────────────
    // ANCHOR: install_sinks
    let report = pipeline
        .add_sink("payments", payments_sink)?
        .add_sink("refunds", refunds_sink)?
        // ANCHOR_END: install_sinks
        .chains(move |ctx| {
            // deserialize → split by event kind into the two tables.
            // `ErrorPolicy::Skip`: a kind with no branch is dropped and
            // counted, not fatal.
            // ANCHOR: split
            let mut split = chain::<Owned<StorefrontEvent>, _>(deserializer.clone())
                // Clone: `ctx.sink(...)` below borrows `ctx`, so `ctx.pipeline`
                // must not be moved out of it.
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ErrorPolicy::Skip);

            // Declare the branches; each `add` returns a Copy, typed handle.
            let payments = split.add::<Owned<PaymentRow>, _, _>(
                payments_enc.clone(),
                payments_router.clone(),
                ctx.sink("payments"),
            );
            let refunds = split.add::<Owned<RefundRow>, _, _>(
                refunds_enc.clone(),
                refunds_router.clone(),
                ctx.sink("refunds"),
            );

            // The routing logic: one match, O(1) dispatch, type-checked per
            // arm. Classify and extract in the same arm, because the
            // variant's payload is already the row its table takes.
            split
                .route(move |event: StorefrontEvent, out| match event {
                    StorefrontEvent::PaymentCaptured(row) => out.emit(payments, row),
                    StorefrontEvent::RefundIssued(row) => out.emit(refunds, row),
                    // A placed order matches no branch → `unmatched` (Skip).
                    StorefrontEvent::OrderPlaced(_) => {}
                })
                .build()
            // ANCHOR_END: split
        })
        .run(source)?;

    report.log();
    std::process::exit(report.exit_code());
}
// ANCHOR_END: assembly
