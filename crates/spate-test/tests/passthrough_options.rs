//! `passthrough_runtime` applies the options it is given. Its own test binary
//! because it installs the process-global Prometheus recorder.

use spate_core::config::PipelineConfig;
use spate_core::metrics::{Exporter, MetricsSettings, install};
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_test::{
    PipelineRun, TestEncoder, capture_sink, memory_source, metric_value, passthrough_runtime,
};
use std::time::Duration;

const CONFIG: &str = r#"
pipeline: { name: options-apply, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: prometheus }
source: { memory: {} }
sink: { capture: {} }
"#;

/// The options' `version` is the one the run publishes, so what a caller sets
/// in them reaches the runtime.
#[test]
fn the_options_given_are_the_options_the_run_uses() {
    let (source, _handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config")).expect("builder");
    let runtime = passthrough_runtime(
        pipeline,
        source,
        sink,
        TestEncoder,
        RuntimeOptions {
            version: "from-the-options".to_owned(),
            ..RuntimeOptions::default()
        },
    );
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());
    shutdown.trigger();
    let report = run
        .wait_exit(Duration::from_secs(20))
        .expect("the run exits")
        .expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");

    let rendered = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("reuse installed exporter")
    .render();
    assert_eq!(
        metric_value(
            &rendered,
            "spate_pipeline_info",
            &[("version", "from-the-options")]
        ),
        Some(1.0),
        "rendered:\n{rendered}"
    );
}
