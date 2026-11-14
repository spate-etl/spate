//! The SIMD features line a process logs when its first pipeline starts.
//!
//! One test in its own binary, because the capture below is a process-wide
//! global subscriber, the line is logged once per process, and `cargo test`
//! shares one process across a binary.

use spate_core::config::PipelineConfig;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::telemetry::cpu::{compiled_simd, detected_simd};
use std::env::consts::ARCH;

use spate_test::{
    HANG, LogCapture, PipelineRun, TestEncoder, capture_sink, memory_source, passthrough_runtime,
};

/// Start a pipeline named `name`, stop it, and wait for its clean exit.
fn run_once(name: &str) {
    let config = format!(
        r#"
pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
source: {{ memory: {{}} }}
sink: {{ capture: {{}} }}
"#
    );
    let (source, _handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(&config).expect("config")).expect("builder");
    let runtime = passthrough_runtime(
        pipeline,
        source,
        sink,
        TestEncoder,
        RuntimeOptions::default(),
    );
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());
    shutdown.trigger();
    let report = run.wait_exit(HANG).expect("the run exits").expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");
}

/// Two pipelines started one after the other in one process log one line,
/// carrying the architecture and the two lists the table reports.
#[test]
fn the_first_start_in_a_process_logs_the_cpu_features_once() {
    // `info` is the level a deployment runs at, and the level
    // `spate_core::telemetry` assigns to a startup milestone.
    let capture = LogCapture::install_global(tracing::Level::INFO);

    run_once("cpu-features-first");
    run_once("cpu-features-second");

    let lines: Vec<String> = capture
        .lines()
        .into_iter()
        .filter(|l| l.contains("cpu features"))
        .collect();
    let expected = format!(
        " INFO spate_core::telemetry::cpu: cpu features arch={} compiled={} detected={}",
        ARCH,
        compiled_simd().join(","),
        detected_simd().join(","),
    );
    assert_eq!(lines, vec![expected]);
}
