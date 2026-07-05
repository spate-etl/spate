//! Registering your own metrics next to the framework's.
//!
//! The framework's instrumentation API **is** the [`metrics`] facade: any
//! counter/gauge/histogram you record is exported by the same Prometheus
//! endpoint as `etl_*` metrics — no extra registry to learn. This example
//! installs the exporter (in a real pipeline [`PipelineRuntime::run`] does
//! that from your YAML), records custom metrics the recommended way
//! (pre-registered handles, per-batch counting), and prints the rendered
//! exposition:
//!
//! ```sh
//! cargo run -p etl --example custom_metrics
//! ```
//!
//! [`PipelineRuntime::run`]: etl::pipeline::PipelineRuntime::run

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use etl::metrics::{ComponentLabels, DeserMetrics, MetricsSettings, install};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In a real pipeline the runtime installs this from the YAML `metrics`
    // section and mounts /metrics on the admin server.
    let handle = install(&MetricsSettings::default())?;

    // ── Your metrics ────────────────────────────────────────────────────
    // Pre-register handles once, at build time — never resolve names or
    // labels on the per-record path (that is the framework convention its
    // own stages follow).
    let orders_enriched = metrics::counter!("myapp_orders_enriched_total", "region" => "eu");
    let enrich_seconds = metrics::histogram!("myapp_enrich_duration_seconds");

    // Hot loop: touch only the handles, count per batch.
    let batch_size = 512;
    orders_enriched.increment(batch_size);
    enrich_seconds.record(0.012);

    // ── Framework handles, side by side ────────────────────────────────
    // The same pattern the engine uses internally, available to custom
    // components: a deserializer-stage handle counting one batch.
    let deser = DeserMetrics::new(&ComponentLabels::new(
        "metrics-demo",
        "main.deserializer",
        "deserializer",
    ));
    deser.batch(batch_size, 0, Duration::from_micros(850));

    // Render what Prometheus would scrape.
    let exposition = handle.render();
    println!("{exposition}");
    assert!(exposition.contains("myapp_orders_enriched_total"));
    assert!(exposition.contains("etl_deser_records_total"));
    Ok(())
}
