//! Registering your own metrics next to the framework's.
//!
//! The framework's instrumentation API **is** the [`metrics`] facade: any
//! counter/gauge/histogram you record is exported by the same Prometheus
//! endpoint as `spate_*` metrics — no extra registry to learn. This example
//! installs the exporter by hand (in a real pipeline
//! [`Pipeline::from_config`] does that from your YAML *before* you can
//! build any handle — that ordering guarantee is the point of the
//! constructor), then records custom metrics three ways — the raw facade
//! macros, a [`Meter`](spate::metrics::Meter)-owned family that inherits the
//! three standard labels, and a framework stage handle — and prints the
//! rendered exposition:
//!
//! ```sh
//! cargo run -p spate --example custom_metrics
//! ```
//!
//! [`Pipeline::from_config`]: spate::pipeline::Pipeline::from_config

// The examples index renders these four fields; see scripts/examples-index.sh.
// INDEX-TIER:  operating
// INDEX-GOAL:  register your own metrics beside the framework's
// INDEX-TECH:  the Meter API
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::metrics::{DeserMetrics, Meter, MetricsSettings, install};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In a real pipeline Pipeline::from_config installs this from the YAML
    // `metrics` section and the runtime mounts /metrics on the admin server.
    let handle = install(&MetricsSettings::default())?;

    let batch_size = 512;

    // ── Raw facade: quick, but you label everything by hand ─────────────
    // Pre-register handles once, at build time — never resolve names or
    // labels on the per-record path (that is the framework convention its
    // own stages follow). These series carry ONLY the labels typed here.
    let orders_enriched = metrics::counter!("myapp_orders_enriched_total", "region" => "eu");
    let enrich_seconds = metrics::histogram!("myapp_enrich_duration_seconds");

    // Hot loop: touch only the handles, count per batch.
    orders_enriched.increment(batch_size);
    enrich_seconds.record(0.012);

    // ── A Meter-owned family: standard labels + the spate_ umbrella ───────
    // A `Meter` is a scope bound to one component's pipeline/component/
    // component_type. Every handle it mints carries those three labels AND is
    // auto-prefixed `spate_<namespace>_`, so the family sits under the same
    // `spate_` root as the framework's and joins cleanly in a query. You pass
    // the LOCAL name — the Meter adds the prefix. A pipeline author uses the
    // default `custom` namespace (`Meter::new` / `ChainCtx::meter`); a
    // connector claims its own via `Meter::with_namespace("kafka", ...)`.
    let meter = Meter::new("metrics-demo", "enricher", "map");
    let schema_fetches = meter.counter("schema_fetches_total", &[("registry", "prod".into())]);
    let fetch_seconds = meter.histogram("schema_fetch_duration_seconds", &[]);
    schema_fetches.increment(3);
    fetch_seconds.record(0.004);

    // ── Framework handles, side by side ────────────────────────────────
    // The same pattern the engine uses internally, available to custom
    // components: a deserializer-stage handle counting one batch, built from
    // the same Meter's label set so it shares the standard labels.
    let deser = DeserMetrics::new(meter.labels());
    deser.batch(batch_size, 0, Duration::from_micros(850));

    // Render what Prometheus would scrape.
    let exposition = handle.render();
    println!("{exposition}");
    assert!(exposition.contains("myapp_orders_enriched_total"));
    // The Meter family is auto-prefixed `spate_custom_` and carries the standard
    // labels the raw macro above omits.
    assert!(exposition.contains(
        r#"spate_custom_schema_fetches_total{pipeline="metrics-demo",component="enricher",component_type="map",registry="prod"}"#
    ));
    assert!(exposition.contains("spate_deser_records_total"));
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
