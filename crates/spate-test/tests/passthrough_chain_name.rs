//! `passthrough_runtime` names its chain `main`, the prefix of the component
//! label on the chain's series. Its own test binary because it installs the
//! process-global Prometheus recorder.

use spate_core::config::PipelineConfig;
use spate_core::metrics::{Exporter, MetricsSettings, install};
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::source::LaneId;
use spate_test::{
    PipelineRun, TestEncoder, capture_sink, memory_source, metric_value, passthrough_runtime,
};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(10);

const CONFIG: &str = r#"
pipeline: { name: chain-name, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: prometheus }
checkpoint: { interval: 100ms }
source: { memory: {} }
sink: { capture: {} }
"#;

/// The deserializer's series carries `component="main.deserializer"`.
#[test]
fn the_chain_is_named_main() {
    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config")).expect("builder");
    let runtime = passthrough_runtime(
        pipeline,
        source,
        sink,
        TestEncoder,
        RuntimeOptions::default(),
    );
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let last = handle.push(p, None, b"row");
    assert!(handle.wait_committed(p, last + 1, DEADLINE), "commit");
    shutdown.trigger();
    let report = run
        .wait_exit(DEADLINE)
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
            "spate_deser_records_total",
            &[("component", "main.deserializer"), ("outcome", "ok")]
        ),
        Some(1.0),
        "rendered:\n{rendered}"
    );
}
