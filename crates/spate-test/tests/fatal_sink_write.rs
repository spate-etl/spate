//! A fatal sink write fails the pipeline through the stalled watermark, and
//! the failure names the sink and the write's error.

use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{
    BytesPassthrough, PipelineRun, TestEncoder, WriteOutcome, capture_sink, memory_source,
};
use std::time::Duration;

const CONFIG: &str = r#"
pipeline: { name: fatal-sink-write, threads: 1, io_threads: 1 }
admin: { listen: none }
metrics: { exporter: none }
checkpoint: { interval: 100ms, drain_timeout: 2s, stalled_fail_after: 200ms }
source: { memory: {} }
sinks:
  a: { capture: {} }
  b: { capture: {} }
"#;

/// With two sinks, the stall failure carries the error of the one that
/// abandoned the batch and names only that sink. Regression for #710.
#[test]
fn the_stall_failure_names_the_sink_and_its_error() {
    let (source, handle) = memory_source();
    let (sink_a, _script_a) = capture_sink(1, 1);
    let (sink_b, script_b) = capture_sink(1, 1);
    script_b.enqueue_global(WriteOutcome::fatal("certificate rejected"));

    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
        .expect("builder")
        .add_sink("a", sink_a)
        .expect("sink a")
        .add_sink("b", sink_b)
        .expect("sink b")
        .chains(|ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ErrorPolicy::Skip);
            let a = split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("a"));
            let b = split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("b"));
            split
                .route(move |row: Vec<u8>, out| match row.first() {
                    Some(b'a') => out.emit(a, row),
                    _ => out.emit(b, row),
                })
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .expect("into_runtime");

    let run = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    handle.push(p, None, b"apple");
    handle.push(p, None, b"banana");

    let report = run
        .wait_exit(Duration::from_secs(10))
        .expect("pipeline did not stop on its own after a fatal sink write")
        .expect("run");
    let ExitState::Failed(failure) = report.state else {
        panic!(
            "a fatal sink write must fail the pipeline (got {:?})",
            report.state
        );
    };
    assert_eq!(failure.component, "checkpoint");
    assert!(
        failure.reason.contains("sink `b`") && failure.reason.contains("certificate rejected"),
        "{}",
        failure.reason
    );
    assert!(!failure.reason.contains("sink `a`"), "{}", failure.reason);
}
