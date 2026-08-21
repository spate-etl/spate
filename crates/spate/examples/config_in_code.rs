//! A pipeline configured in code, with no YAML file anywhere.
//!
//! A program embedding Spate inside a larger service usually holds its
//! settings already, from its own configuration system or from flags, and has
//! nothing to hand [`Pipeline::from_path`]. It builds a [`PipelineConfig`]
//! through the constructors instead and hands that to
//! [`Pipeline::from_config`]:
//!
//! ```sh
//! cargo run -p spate --example config_in_code
//! ```
//!
//! Every framework section is `#[non_exhaustive]`, so a struct literal for one
//! does not compile outside `spate-core`. A section starts at `new` where a
//! field has no default and at `default()` where every field has one, and the
//! fields you are setting are assignments after that. A key added in a later
//! release reaches you as a new default rather than as a compile error.
//!
//! `memory_pipeline.rs` loads a configuration of the same shape from a string,
//! so the two files read side by side show both forms.

// The `ANCHOR` comments below mark the region the configuration guide renders.
// They are stripped from what that page shows; see docs/STYLE.md § 10.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-RANK:  25
// INDEX-TIER:  getting-started
// INDEX-GOAL:  configure a pipeline in code instead of loading a file
// INDEX-TECH:  no infrastructure
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::config::{ComponentConfig, ConfigError, MetricsExporter, PipelineSection, YamlValue};
use spate::prelude::*;
use spate::source::LaneId;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, memory_source};
use std::time::{Duration, Instant};

// ANCHOR: config
/// The configuration a file would otherwise carry.
///
/// `validate` runs the cross-field checks the YAML loaders run.
/// `Pipeline::from_config` does not call it, so a config built here is
/// unchecked until this line.
fn config() -> Result<PipelineConfig, ConfigError> {
    // `name` is the one field of the `pipeline` section with no default.
    let mut pipeline = PipelineSection::new("config-in-code");
    pipeline.threads = Some(1);

    // The `source`, `deserializer` and `sink` bodies are opaque: a connector's
    // factory deserializes its own config out of one. The in-memory components
    // below are built programmatically, so these bodies are empty and their
    // tags are informational.
    let mut cfg = PipelineConfig::new(
        pipeline,
        ComponentConfig::new("memory", empty_body()),
        ComponentConfig::new("capture", empty_body()),
    )
    .with_deserializer(ComponentConfig::new("split", empty_body()));

    // The optional sections start at their defaults, so setting one is an
    // assignment. The demo wants neither an admin server nor an exporter.
    cfg.checkpoint.interval = Duration::from_millis(200);
    cfg.admin.listen = None;
    cfg.metrics.exporter = MetricsExporter::None;

    cfg.validate()?;
    Ok(cfg)
}

/// A component body with no settings in it. A real connector's body is the
/// YAML mapping its own config type deserializes from.
fn empty_body() -> YamlValue {
    YamlValue::Mapping(Default::default())
}
// ANCHOR_END: config

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(LogFormat::Pretty, "info");

    let cfg = config()?;
    println!("pipeline: {}", cfg.pipeline.name);
    println!("sinks: {:?}", cfg.sink_names());

    let pipeline = Pipeline::from_config(cfg)?;

    // The in-memory source and the capturing sink stand in for the connectors
    // a real embedding builds from its own settings.
    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config({
        let mut pool = SinkPoolConfig::default();
        pool.batch.linger = Duration::from_millis(50); // flush quickly for the demo
        pool
    });

    let pipeline = pipeline
        .sink(sink)?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::split_on(b','))
                .with_metrics(ctx.pipeline, "main")
                .map(|word: Vec<u8>| word.to_ascii_uppercase())
                .sink(
                    TestEncoder,
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
        });

    let runtime = pipeline.into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    let p0 = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let last = handle.push(p0, Some(b"demo"), &b"alpha,beta,gamma"[..]);

    // The deadline bounds the wait. Without it a broken pipeline hangs the
    // process instead of failing it.
    let deadline = Instant::now() + Duration::from_secs(10);
    while handle.last_committed(p0) != Some(last + 1) {
        assert!(Instant::now() < deadline, "commit not observed in time");
        std::thread::sleep(Duration::from_millis(20));
    }
    shutdown.trigger();
    let report = join.join().expect("pipeline thread")?;

    let rows: Vec<String> = script
        .writes()
        .iter()
        .flat_map(|w| spate_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8_lossy(&r).into_owned())
        .collect();

    assert_eq!(rows, ["ALPHA", "BETA", "GAMMA"]);

    println!("pipeline exit: {:?}", report.state);
    println!("rows written ({}): {rows:?}", rows.len());
    Ok(())
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
