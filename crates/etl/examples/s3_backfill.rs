//! Bounded S3 backfill, runnable without any infrastructure.
//!
//! Point the S3 source at a bucket prefix and it streams every object's
//! NDJSON records through the pipeline, checkpoints to a manifest object,
//! and **terminates the pipeline itself** once the prefix is exhausted —
//! no shutdown trigger. A second run resumes from the manifest and, with
//! nothing new to read, exits immediately with zero records: that is the
//! at-least-once resume contract in miniature.
//!
//! Object storage here is a local directory (`file://`); against real S3
//! swap the URLs (`s3://bucket/prefix/`) and pass credentials/region
//! through the `store` map. The source hands each NDJSON line to the
//! chain as one payload, so the JSON deserializer runs `single` framing.
//!
//! ```sh
//! cargo run -p etl --features s3,json --example s3_backfill
//! ```

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use etl::json::{JsonDeserializerBuilder, JsonFraming, JsonSettings, OnError};
use etl::prelude::*;
use etl::s3::S3Source;
use etl_test::{TestEncoder, capture_sink};
use serde::Deserialize;
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
    lanes: 2
    checkpoint:
      url: "file://{state}/manifest.json"
sink: {{ capture: {{}} }}
"#,
        data = root.join("data").display(),
        state = root.join("state").display(),
    )
}

/// One full pipeline run; returns the rows the sink durably wrote.
fn run_once(yaml: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(yaml)?)?;
    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?;
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config({
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50);
        cfg
    });

    let report = pipeline
        .sink(sink)?
        .chains(|ctx| {
            // The S3 lane frames NDJSON: one line = one payload → `single`.
            let deser = JsonDeserializerBuilder::from_settings(JsonSettings {
                framing: JsonFraming::Single,
                on_error: OnError::Skip,
                reject_duplicate_keys: false,
            })
            .with_metrics(ctx.pipeline.clone(), "main")
            .build_serde::<Reading>();
            chain_owned::<Reading, _>(deser)
                .with_metrics(ctx.pipeline, "main")
                .filter(|r: &Reading| r.value.is_finite())
                .map(|r: Reading| format!("{}={}", r.sensor, r.value).into_bytes())
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    ChunkConfig::default(),
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
        .flat_map(|w| etl_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8_lossy(&r).into_owned())
        .collect())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    etl::telemetry::init(etl::telemetry::LogFormat::Pretty, "info");

    // Stage a tiny "bucket": two NDJSON objects, one of them gzipped
    // (codec picked per object by extension).
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("data"))?;
    std::fs::create_dir_all(root.path().join("state"))?;
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

    println!("\n── run 2: resume of a finished backfill ──");
    let rows = run_once(&yaml)?;
    println!(
        "rows written ({}): everything was already committed in the manifest",
        rows.len()
    );
    assert!(rows.is_empty());

    println!(
        "\nmanifest at {}:",
        root.path().join("state/manifest.json").display()
    );
    println!(
        "{}",
        std::fs::read_to_string(root.path().join("state/manifest.json"))?
    );
    Ok(())
}
