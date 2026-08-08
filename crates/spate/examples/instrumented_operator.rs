//! Counting what an operator *you wrote* is doing.
//!
//! The engine measures the stages it owns — records in, bytes out, flush
//! durations — but it cannot see what your business logic means: how many of
//! these records are refunds, how many line items an order carried. That is
//! what [`ChainCtx::meter`] is for. It hands the chain factory a
//! [`Meter`](spate::metrics::Meter) already labeled with this pipeline and the
//! component you name, so the series you mint from it join the framework's in
//! a query instead of sitting in a namespace of their own.
//!
//! The shape to copy is **mint at build time, close the handle over the
//! closure**. Operator closures are pure — `.map` and `.inspect` take a bare
//! function, there is no per-record context to look a metric up from — and
//! that is deliberate: it makes the only workable pattern the correct one
//! (INV-8). This example is that pattern end to end, and it finishes by
//! reading the rendered Prometheus exposition, so it proves the series and
//! their labels arrive rather than that a family registered:
//!
//! ```sh
//! cargo run -p spate --example instrumented_operator
//! ```
//!
//! For custom metrics *outside* a pipeline — the raw facade, a standalone
//! `Meter`, a framework stage handle — see `custom_metrics.rs`.
//!
//! [`ChainCtx::meter`]: spate::pipeline::ChainCtx::meter

// The examples index renders these four fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  operating
// INDEX-GOAL:  count what an operator you wrote is doing
// INDEX-TECH:  the Meter API
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::prelude::*;
use spate::source::LaneId;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, memory_source, wait_until};
use std::time::Duration;

/// The `pipeline.name` here is also the `pipeline` label on every series the
/// `Meter` mints, so it is worth keeping unique to this example: a gauge series
/// has exactly one live owner per process (INV-10), and examples share a
/// process when the suite runs them. Counters and histograms sum rather than
/// claim ownership, which is the other reason this file uses only those.
const CONFIG: &str = r#"
pipeline: { name: storefront-orders, threads: 1 }
checkpoint: { interval: 200ms }
metrics: { listen: "127.0.0.1:0" }
source: { memory: {} }
sink: { capture: {} }
"#;

/// One storefront event: `kind,order_id,line_items,amount`.
struct Event<'a> {
    kind: &'a str,
    order_id: &'a str,
    line_items: u64,
    amount: f64,
}

/// Parsing is the operator's own business — nothing here is framework API.
/// Unparseable input yields `None`; the chain drops it below.
fn parse(record: &[u8]) -> Option<Event<'_>> {
    let mut fields = std::str::from_utf8(record).ok()?.split(',');
    Some(Event {
        kind: fields.next()?,
        order_id: fields.next()?,
        line_items: fields.next()?.parse().ok()?,
        amount: fields.next()?.parse().ok()?,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;

    // `from_config` installed the exporter — before any handle can exist, which
    // is the ordering that makes the handles below record into something. The
    // handle is cheap to clone; keep one, because the builder is consumed by
    // assembly and the assertions at the end render from it.
    let exposition = pipeline.metrics().clone();

    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config({
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50); // flush quickly for the demo
        cfg
    });

    let runtime = pipeline
        .sink(sink)?
        // ANCHOR: chain
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk(); // bind before `with_metrics` moves `ctx.pipeline`

            // ── 1. A Meter scoped to this pipeline and this component ────────
            // `ctx.meter` fills in the pipeline name from the config; you name
            // the component instance ("checkout") and its type ("inspect").
            // Those three become the standard labels on everything minted
            // below, so these series join the engine's own on a dashboard.
            // Names land under the `spate_custom_` bucket — the namespace
            // reserved for pipeline authors — and you pass the LOCAL name, so
            // there is no prefix to typo and no way to collide with a
            // framework family.
            let meter = ctx.meter("checkout", "inspect");

            // ── 2. Handles minted HERE, at chain-build time (INV-8) ──────────
            // This factory runs once per pipeline thread, before a single
            // record flows. Resolving a name or building a label set on the
            // per-record path is the thing not to do: it is a registry lookup
            // per record where this is a lookup per thread. What crosses into
            // the closure is the resolved handle, which is `Arc`-backed — so
            // when the runtime runs several pipeline threads, each thread mints
            // its own handle for the same series and their counts sum.
            //
            // Per-instance identity belongs in a label, not in the name:
            // `channel` here keeps `orders_total` low-cardinality and still
            // lets a query break the count down.
            let orders = meter.counter("orders_total", &[("channel", "web".into())]);
            let refunds = meter.counter("refunds_total", &[("channel", "web".into())]);
            let line_items = meter.counter("line_items_total", &[]);
            // Not a `*_duration_seconds` name, so the exporter renders this one
            // as a summary (quantiles plus `_sum`/`_count`) rather than with
            // its default duration buckets — which is what you want for a value
            // that is not a latency.
            let order_value = meter.histogram("order_value_dollars", &[]);

            chain_owned::<Vec<u8>, _>(TestDeserializer::split_on(b'\n'))
                .with_metrics(ctx.pipeline, "checkout")
                // ── 3. `.inspect` — the home for an observation-only stage ───
                // It sees `&record` and returns nothing, so it cannot change
                // what flows downstream: the compiler enforces that this stage
                // is measurement and nothing else. A `.map` that counted on the
                // side would hide the same work inside a transformation.
                .inspect(move |record: &Vec<u8>| {
                    let Some(event) = parse(record) else { return };
                    match event.kind {
                        "refund" => refunds.increment(1),
                        _ => {
                            orders.increment(1);
                            line_items.increment(event.line_items);
                            order_value.record(event.amount);
                        }
                    }
                })
                // Refunds are settled elsewhere and unparseable lines are
                // noise; only orders reach this sink. The counters above still
                // saw the refund — that is the point of measuring before the
                // filter rather than after it.
                .filter(|record: &Vec<u8>| parse(record).is_some_and(|event| event.kind == "order"))
                .map(|record: Vec<u8>| {
                    let event = parse(&record).expect("filtered to parseable orders");
                    format!("{}={:.2}", event.order_id, event.amount).into_bytes()
                })
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        // ANCHOR_END: chain
        .runtime_options(RuntimeOptions {
            handle_signals: false, // the demo triggers shutdown itself
            ..RuntimeOptions::default()
        })
        .into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    // One payload, five newline-separated events: three orders carrying six
    // line items between them, one refund, and one line the parser rejects.
    let p0 = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let last = handle.push(
        p0,
        Some(b"storefront"),
        concat!(
            "order,1042,3,129.99\n",
            "order,1043,1,19.50\n",
            "refund,1042,1,19.50\n",
            "order,1044,2,64.00\n",
            "heartbeat"
        )
        .as_bytes(),
    );

    // Bounded on purpose: an unbounded wait turns a broken pipeline into a hung
    // process rather than a failing one.
    wait_until(Duration::from_secs(10), "the offset to commit", || {
        handle.last_committed(p0) == Some(last + 1)
    });
    shutdown.trigger();
    let report = join.join().expect("pipeline thread")?;

    let rows: Vec<String> = script
        .writes()
        .iter()
        .flat_map(|w| spate_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8_lossy(&r).into_owned())
        .collect();
    assert_eq!(
        rows.len(),
        3,
        "three orders; the refund and the junk line went"
    );

    // ── 4. Read the exposition, and assert the wiring ───────────────────────
    // This is what a scrape of `/metrics` returns. Asserting on the rendered
    // text — the full series, labels and value — is the assertion worth making:
    // a test that only checks the family name passes just as happily against a
    // handle nothing ever increments.
    let scraped = exposition.render();
    let labels = r#"pipeline="storefront-orders",component="checkout",component_type="inspect""#;
    for series in [
        format!(r#"spate_custom_orders_total{{{labels},channel="web"}} 3"#),
        format!(r#"spate_custom_refunds_total{{{labels},channel="web"}} 1"#),
        format!(r#"spate_custom_line_items_total{{{labels}}} 6"#),
        format!(r#"spate_custom_order_value_dollars_count{{{labels}}} 3"#),
    ] {
        assert!(
            scraped.contains(&series),
            "missing from the exposition: {series}"
        );
    }

    for line in scraped.lines().filter(|l| l.starts_with("spate_custom_")) {
        println!("{line}");
    }
    println!("\npipeline exit: {:?}", report.state);
    println!("rows written ({}): {rows:?}", rows.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller, so the assertions above stop being decorative.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
