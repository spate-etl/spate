//! A realistic pipeline that needs nothing installed, and the only
//! infrastructure-free example that ends the way production does.
//!
//! The flagship examples show the shape a deployment takes, and every one of
//! them wants a broker, a schema registry or a database first. This one has
//! the same shape (a live event stream, a JSON deserializer, a filter, a
//! normalizing map, a split terminal fanning out to three destinations, the
//! admin server up) over [`spate_datagen`], which manufactures the stream
//! in-process:
//!
//! ```sh
//! cargo run -p spate --example storefront_pipeline
//! ```
//!
//! # How it stops
//!
//! The shipped config sets `count:` on the source, so the generator exhausts
//! its budget, reports `Drained`, and the pipeline exits
//! [`ExitState::Completed`] on its own, which lets this file also be a
//! test.
//!
//! **Delete `count:` and the stream never ends.** `SIGTERM` (or `Ctrl-C`) is
//! then the only way out, and it is the reason `handle_signals` is left at its
//! default here while every other infrastructure-free example turns it off:
//! this is the one place the production shutdown path is demonstrable without
//! a broker. Both routes run the *same* drain: stop the source, finish the
//! records already in flight, flush every sink, commit the final watermarks,
//! then exit. What you watch here under `Ctrl-C` is what a pod does when
//! Kubernetes evicts it.
//!
//! # What it demonstrates
//!
//! - **A source with no prerequisites.** `DatagenSource::from_component_config`
//!   reads the same opaque `source:` section a Kafka or S3 source would.
//! - **Referential integrity that survives the pipeline.** A payment always
//!   names an order the same partition placed earlier, so the assertions at the
//!   foot can check every payment against the orders that reached the sink.
//! - **Key-hash colocation across a split.** The generator keys each payload by
//!   order id; [`KeyHashRouter`] hashes that key, so an order and its payment
//!   land on the same shard index even though they land in different sinks.
//! - **The admin server.** `/metrics`, `/healthz` and `/readyz` are printed
//!   before the run starts. Curl them while it is going, or delete `count:`
//!   and take your time.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-RANK:  15
// INDEX-TIER:  getting-started
// INDEX-GOAL:  run a realistic order stream to completion or to SIGTERM
// INDEX-TECH:  no infrastructure
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::prelude::*;
use spate::record::stable_key_hash;
use spate_datagen::storefront::StorefrontEvent;
use spate_datagen::{DatagenSource, DatagenSourceConfig};
use spate_json::JsonDeserializerBuilder;
use spate_test::{TestEncoder, capture_sink, decode_rows};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

/// Shards per sink. Above one, so the colocation checks at the foot have two
/// outcomes to tell apart.
const SHARDS: usize = 2;

/// The refund reason this ledger does not book: a duplicate order's refund
/// reverses a mistake rather than returning goods.
const UNBOOKED_REASON: &str = "duplicate_order";

/// This is the one example that serves the admin endpoints, and `main`
/// prints their URLs to curl, so it names an address where the others ask
/// for `none`. Port 0 rather than the `0.0.0.0:9090` default, because several
/// examples run concurrently under `cargo test`. `pipeline.name` is unique to
/// this file because a gauge series has one live owner per process
/// (INV-10). Everything else is ordinary framework tuning.
const CONFIG: &str = r#"
pipeline: { name: storefront-demo, threads: 2 }
admin: { listen: "127.0.0.1:0" }
checkpoint: { interval: 200ms }
metrics: { exporter: prometheus }

# 4 lanes x 5 events every 20ms = 1000 events/s, for two seconds. Drop the
# `count` line and the stream runs until SIGTERM.
source:
  datagen:
    partitions: 4
    events_per_tick: 5
    tick_interval: 20ms
    count: 2000
    seed: 7

deserializer:
  json:
    on_error: skip

# One destination per event kind. Each is an ordinary connector section keyed
# by the name the split resolves through `ctx.sink("<name>")`; a real pipeline
# puts a table or a topic here instead of a capturing mock.
sinks:
  orders:   { capture: {} }
  payments: { capture: {} }
  refunds:  { capture: {} }
"#;

/// The map stage's output: one ledger entry per event, whatever its kind.
///
/// The normalization belongs at the edge: `order_placed` carries its value
/// spread over `lines`, the other two carry it directly, and a downstream
/// balance check should not have to know that. The split matches on `kind`.
#[derive(Debug)]
struct Ledger {
    kind: &'static str,
    order_id: u64,
    amount_cents: u64,
    /// Region for an order, refund reason for a refund, empty for a payment.
    detail: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    let mut config = PipelineConfig::from_str(CONFIG)?;
    // The runtime binds the admin server inside `run`, which is past the last
    // chance to print where it landed. Resolve port 0 to a concrete port here
    // instead, so the URLs below are ones a reader can paste.
    let admin = resolve_port(
        config
            .admin
            .listen
            .expect("this example serves the admin endpoints"),
    )?;
    config.admin.listen = Some(admin);

    let pipeline = Pipeline::from_config(config)?;

    // The source reads the opaque `source:` section, as a broker-backed one
    // does. Nothing above this line knows the stream is manufactured.
    let source = DatagenSource::from_component_config(&pipeline.config().source)?;
    // The same section, read again for the one number the assertions need. A
    // constant here would be free to drift from the YAML above.
    let budget = DatagenSourceConfig::from_component_config(&pipeline.config().source)?.count;
    let deser_section = pipeline
        .config()
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?
        .clone();

    // Three capturing sinks, one per event kind, each `SHARDS` wide.
    let (orders_sink, orders) = capture_sink(SHARDS, 1);
    let (payments_sink, payments) = capture_sink(SHARDS, 1);
    let (refunds_sink, refunds) = capture_sink(SHARDS, 1);
    let quick = |sink: spate_test::CaptureSink| {
        sink.with_pool_config({
            let mut cfg = SinkPoolConfig::default();
            cfg.batch.linger = Duration::from_millis(50); // flush quickly for the demo
            cfg
        })
    };

    println!("admin server:");
    for probe in ["/metrics", "/healthz", "/readyz"] {
        println!("  curl http://{admin}{probe}");
    }
    println!("running; SIGTERM (Ctrl-C) drains gracefully\n");

    let pipeline = pipeline
        .add_sink("orders", quick(orders_sink))?
        .add_sink("payments", quick(payments_sink))?
        .add_sink("refunds", quick(refunds_sink))?
        .chains(move |ctx| {
            // The generator frames one event per payload, so `for_source_framing`
            // derives `single` from the source and rejects a `framing:` that
            // would frame the payload a second time.
            let deser = JsonDeserializerBuilder::from_component(&deser_section)
                .and_then(|b| b.for_source_framing(ctx.source_framing))
                .expect("deserializer config")
                .with_metrics(ctx.pipeline.clone(), "main")
                .build_serde::<StorefrontEvent>();

            let mut split = chain_owned::<StorefrontEvent, _>(deser)
                // Clone: `ctx.sink(...)` below borrows `ctx`, so `ctx.pipeline`
                // must not be moved out of it.
                .with_metrics(ctx.pipeline.clone(), "main")
                // A duplicate order's refund is bookkeeping, not a return;
                // counted on `spate_operator_records_dropped_total`.
                .filter(|event: &StorefrontEvent| match event {
                    StorefrontEvent::RefundIssued(r) => r.reason != UNBOOKED_REASON,
                    _ => true,
                })
                .map(to_ledger)
                .split(ErrorPolicy::Skip);

            // Each `add` returns a `Copy` typed handle into the branch. All
            // three carry byte rows because the capturing sink stores bytes;
            // against a real sink each branch would bring its own row struct
            // and its own encoder, which is the point of the split.
            let to_orders =
                split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("orders"));
            let to_payments =
                split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("payments"));
            let to_refunds =
                split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("refunds"));

            // One match: classify and project in the same arm. `KeyHashRouter`
            // shards on the payload key the generator set, the order id, so
            // every event about one order takes the same shard index in
            // whichever sink it lands in.
            split
                .route(move |entry: Ledger, out| {
                    let row = format!("{},{},{}", entry.order_id, entry.amount_cents, entry.detail)
                        .into_bytes();
                    match entry.kind {
                        "order_placed" => out.emit(to_orders, row),
                        "payment_captured" => out.emit(to_payments, row),
                        "refund_issued" => out.emit(to_refunds, row),
                        // Unreachable today; a new event kind would land here
                        // and follow the `unmatched` policy (Skip) instead of
                        // silently joining somebody else's table.
                        _ => {}
                    }
                })
                .build()
        });

    // ANCHOR: run
    // `RuntimeOptions::default()` leaves `handle_signals` on, so SIGTERM and
    // Ctrl-C run the same drain a source that exhausts its input runs.
    let report = pipeline.run(source)?;
    report.log();
    // ANCHOR_END: run

    // ── What the three sinks captured ───────────────────────────────────
    let (order_rows, order_shards) = captured(&orders);
    let (payment_rows, payment_shards) = captured(&payments);
    let (refund_rows, refund_shards) = captured(&refunds);
    println!(
        "\ncaptured: {} orders, {} payments, {} refunds",
        order_rows.len(),
        payment_rows.len(),
        refund_rows.len()
    );

    // Both of these hold however the run ended: the exit state because both
    // routes out are the same drain, the filter because it is per record.
    assert_eq!(
        report.state,
        ExitState::Completed,
        "both routes out of this pipeline are a clean drain"
    );
    assert!(
        !refund_rows.iter().any(|r| r.ends_with(UNBOOKED_REASON)),
        "a `{UNBOOKED_REASON}` refund reached the sink"
    );

    // Everything below is about *volume*, and a signalled run stops wherever
    // the signal lands, so the totals are whatever prefix of the stream it
    // caught. The final watermarks sum to the configured budget exactly when
    // the source exhausted it instead.
    let released: i64 = report.final_watermarks.iter().map(|(_, at)| at).sum();
    if budget != Some(released.unsigned_abs()) {
        println!("stopped early at {released} events; skipping the volume checks");
        return Ok(());
    }

    assert!(
        !order_rows.is_empty() && !payment_rows.is_empty() && !refund_rows.is_empty(),
        "every branch of the split must have received traffic"
    );
    assert!(
        order_rows.len() > payment_rows.len() && payment_rows.len() > refund_rows.len(),
        "a capture needs an earlier placement and a refund an earlier capture, \
         so the three kinds thin out in that order"
    );

    // Referential integrity, end to end: a payment names an order this run
    // already wrote, and a refund names a payment it already wrote. Compared
    // as sets rather than counts, because at-least-once permits a replayed
    // duplicate after a retry.
    let placed: HashSet<u64> = order_shards.keys().copied().collect();
    for id in payment_shards.keys() {
        assert!(placed.contains(id), "payment for unplaced order {id}");
    }
    let paid: HashSet<u64> = payment_shards.keys().copied().collect();
    for id in refund_shards.keys() {
        assert!(paid.contains(id), "refund against uncaptured order {id}");
    }

    // Colocation: the same key hashes to the same shard index in every sink.
    // Naming the shard the key hashes to makes this a test of the key rather
    // than of determinism. `KeyHashRouter` falls back to the source partition
    // for a keyless record, and all three events of an order come from one
    // partition, so the two sinks would agree either way.
    for (id, shard) in &payment_shards {
        let by_key = (stable_key_hash(id.to_string().as_bytes()) % SHARDS as u64) as usize;
        assert_eq!(
            *shard, by_key,
            "the payment for order {id} did not land on its key's shard"
        );
        assert_eq!(
            order_shards.get(id),
            Some(shard),
            "order {id} and its payment landed on different shards"
        );
    }
    Ok(())
}

/// Normalize an event into a ledger entry: every kind ends up carrying the
/// amount it settles, which for an order means summing its lines.
fn to_ledger(event: StorefrontEvent) -> Ledger {
    match event {
        StorefrontEvent::OrderPlaced(o) => Ledger {
            kind: "order_placed",
            order_id: o.order_id,
            amount_cents: o
                .lines
                .iter()
                .map(|l| u64::from(l.qty) * u64::from(l.unit_cents))
                .sum(),
            detail: o.region.into_owned(),
        },
        StorefrontEvent::PaymentCaptured(p) => Ledger {
            kind: "payment_captured",
            order_id: p.order_id,
            amount_cents: p.amount_cents,
            detail: String::new(),
        },
        StorefrontEvent::RefundIssued(r) => Ledger {
            kind: "refund_issued",
            order_id: r.order_id,
            amount_cents: r.amount_cents,
            detail: r.reason.into_owned(),
        },
        // `StorefrontEvent` is `#[non_exhaustive]`: a kind added upstream
        // reaches this arm rather than failing to compile a downstream crate.
        other => Ledger {
            kind: "unknown",
            order_id: other.order_id(),
            amount_cents: 0,
            detail: String::new(),
        },
    }
}

/// Every row one sink recorded, plus the shard each order id landed on.
fn captured(script: &spate_test::SinkScript) -> (Vec<String>, HashMap<u64, usize>) {
    let mut rows = Vec::new();
    let mut shards = HashMap::new();
    for write in script.writes() {
        for row in decode_rows(&write.payload) {
            let row = String::from_utf8_lossy(&row).into_owned();
            if let Some(id) = row.split(',').next().and_then(|f| f.parse::<u64>().ok()) {
                shards.insert(id, write.shard);
            }
            rows.push(row);
        }
    }
    (rows, shards)
}

/// Turn a port-0 listen address into the concrete one the runtime will bind.
///
/// Binding and releasing leaves a window in which something else could take
/// the port; a demo trades that for a URL a reader can paste, and a real
/// deployment names a fixed port in its config and needs none of this.
fn resolve_port(listen: SocketAddr) -> std::io::Result<SocketAddr> {
    if listen.port() != 0 {
        return Ok(listen);
    }
    std::net::TcpListener::bind(listen)?.local_addr()
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
