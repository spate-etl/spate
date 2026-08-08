//! Bounded S3 backfill, runnable without any infrastructure.
//!
//! Point the S3 source at a bucket prefix and it streams every object's
//! NDJSON records through the pipeline and **terminates the pipeline
//! itself** once the prefix is exhausted — no shutdown trigger. Solo (no
//! coordinator injected) the source keeps its progress in an in-process
//! store, so a second run replays the whole prefix — at-least-once, safe
//! but wasteful, and the startup WARN says so. Durable resume and
//! multi-instance sharing come from injecting a coordinator over a
//! durable backend via `with_coordinator` — see the
//! `s3_coordinated_backfill` example.
//!
//! Object storage here is a local directory (`file://`); against real S3
//! swap the URLs (`s3://bucket/prefix/`) and pass credentials/region
//! through the `store` map. The S3 source is format-agnostic: we wire the
//! format's framer into it in code — `spate-json`'s [`NdjsonFramer`] — so it
//! hands each NDJSON line to the chain as one payload, and
//! `for_source_framing` then derives the JSON deserializer's `single`
//! granularity from the source instead of it being hand-set.
//!
//! ```sh
//! cargo run -p spate --features s3,json --example s3_backfill
//! ```

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use serde::Deserialize;
use spate::json::{JsonDeserializerBuilder, NdjsonFramer};
use spate::prelude::*;
use spate::s3::S3Source;
use spate_test::{TestEncoder, capture_sink};
use std::io::Write as _;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct Reading {
    sensor: String,
    value: f64,
}

fn config_yaml(root: &std::path::Path) -> String {
    format!(
        r#"
pipeline: {{ name: s3-backfill-demo, threads: 1 }}
checkpoint: {{ interval: 200ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "file://{data}/"
deserializer:
  json: {{}}
sink: {{ capture: {{}} }}
"#,
        data = root.join("data").display(),
    )
}

/// One full pipeline run; returns the rows the sink durably wrote.
fn run_once(yaml: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(yaml)?)?;
    // The S3 source is a transport; the format supplies the framer. Wire in
    // `spate-json`'s NDJSON framer (bounded at 64 MiB per record) so each line
    // becomes one payload. The `deserializer: { json: {} }` section carries
    // only decode knobs — no `framing:` — and the framework derives the
    // granularity from the source.
    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(64 << 20)));
    let deser_section = pipeline
        .config()
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?
        .clone();
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config({
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50);
        cfg
    });

    let report = pipeline
        .sink(sink)?
        .chains(move |ctx| {
            let chunk_cfg = ctx.chunk();
            // The S3 lane already frames NDJSON (one line = one payload), so
            // `for_source_framing` derives `single` from the source and would
            // reject a `framing:` that double-frames it.
            let deser = JsonDeserializerBuilder::from_component(&deser_section)
                .and_then(|b| b.for_source_framing(ctx.source_framing))
                .expect("deserializer config")
                .with_metrics(ctx.pipeline.clone(), "main")
                .build_serde::<Reading>();
            chain_owned::<Reading, _>(deser)
                .with_metrics(ctx.pipeline, "main")
                .filter(|r: &Reading| r.value.is_finite())
                .map(|r: Reading| format!("{}={}", r.sensor, r.value).into_bytes())
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
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        // The bounded source completes the pipeline itself: `run` returns
        // once the prefix is exhausted, committed, and drained.
        .run(source)?;

    println!("pipeline exit: {:?}", report.state);
    assert_eq!(report.state, ExitState::Completed);
    Ok(script
        .writes()
        .iter()
        .flat_map(|w| spate_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8_lossy(&r).into_owned())
        .collect())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    // Stage a tiny "bucket": two NDJSON objects, one of them gzipped
    // (codec picked per object by extension).
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("data"))?;
    std::fs::write(
        root.path().join("data/2026-07-13.ndjson"),
        concat!(
            "{\"sensor\":\"kitchen\",\"value\":21.5}\n",
            "{\"sensor\":\"attic\",\"value\":31.0}\n",
        ),
    )?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(
        concat!(
            "{\"sensor\":\"cellar\",\"value\":12.25}\n",
            "{\"sensor\":\"hall\",\"value\":19.75}\n",
        )
        .as_bytes(),
    )?;
    std::fs::write(root.path().join("data/2026-07-14.ndjson.gz"), gz.finish()?)?;

    let yaml = config_yaml(root.path());

    println!("── run 1: fresh backfill ──");
    let mut rows = run_once(&yaml)?;
    rows.sort();
    println!("rows written ({}): {rows:?}", rows.len());
    assert_eq!(rows.len(), 4);

    println!("\n── run 2: rerun on an ephemeral (solo) store ──");
    let mut rows = run_once(&yaml)?;
    rows.sort();
    println!(
        "rows written ({}): solo progress died with run 1, so the rerun replayed \
         the prefix — inject a coordinator over a durable backend for real resume",
        rows.len()
    );
    assert_eq!(rows.len(), 4);
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
