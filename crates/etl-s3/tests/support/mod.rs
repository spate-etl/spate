//! Shared pipeline-assembly helpers for the etl-s3 integration suites.
// Each test binary compiles this module independently and uses a subset.
#![allow(dead_code)]

use etl_core::config::PipelineConfig;
use etl_core::ops::{ChunkConfig, chain_owned};
use etl_core::pipeline::{Pipeline, RuntimeOptions, ShutdownHandle};
use etl_core::sink::KeyHashRouter;
use etl_s3::S3Source;
use etl_test::{BytesPassthrough, PipelineRun, SinkScript, TestEncoder, capture_sink};

/// A pipeline running in the background plus its observation handles.
pub(crate) struct Launched {
    pub(crate) run: PipelineRun,
    pub(crate) script: SinkScript,
    pub(crate) shutdown: ShutdownHandle,
}

/// Assemble and launch a byte-passthrough pipeline over the YAML config's
/// `source: { s3: ... }` section. `pre` runs against the sink script
/// before the pipeline starts (write scripting must be in place before
/// data can flow).
pub(crate) fn launch_scripted(
    yaml: &str,
    options: RuntimeOptions,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(yaml).expect("config parses"))
        .expect("pipeline builds");
    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())
        .expect("source config");
    let (sink, script) = capture_sink(1, 1);
    pre(&script);
    let runtime = pipeline
        .sink(sink)
        .expect("sink installs")
        .chains(|ctx| {
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    ChunkConfig::default(),
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(options)
        .into_runtime(source)
        .expect("runtime assembles");
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());
    Launched {
        run,
        script,
        shutdown,
    }
}

pub(crate) fn launch(yaml: &str, options: RuntimeOptions) -> Launched {
    launch_scripted(yaml, options, |_| {})
}

pub(crate) fn test_options() -> RuntimeOptions {
    RuntimeOptions {
        handle_signals: false,
        ..RuntimeOptions::default()
    }
}

/// Every row the capture sink durably wrote, decoded to strings.
pub(crate) fn captured_rows(script: &SinkScript) -> Vec<String> {
    script
        .writes()
        .iter()
        .flat_map(|w| etl_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8(r).unwrap())
        .collect()
}

pub(crate) fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// `n` one-line JSON records tagged with `prefix`.
pub(crate) fn recs(prefix: &str, n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("{{\"k\":\"{prefix}-{i}\"}}"))
        .collect()
}

/// Join lines with trailing newlines into object bytes.
pub(crate) fn lines_bytes(lines: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    for l in lines {
        out.extend_from_slice(l.as_bytes());
        out.push(b'\n');
    }
    out
}
