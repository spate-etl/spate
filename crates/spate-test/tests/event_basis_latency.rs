//! Under `metrics.e2e_basis: event`, a record's event time never stops the
//! pipeline, however far in the past it is.

use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{BytesPassthrough, HANG, PipelineRun, TestEncoder, capture_sink, memory_source};

const CONFIG: &str = r"
pipeline: { name: event-basis-latency, threads: 1, io_threads: 1 }
admin: { listen: none }
metrics: { exporter: none, e2e_basis: event }
checkpoint: { interval: 100ms, drain_timeout: 2s }
source: { memory: {} }
sinks:
  a: { capture: {} }
";

/// A record whose event time is `i64::MIN` has an age no `i64` holds. The
/// pipeline writes it, commits past it, takes the next record and stops
/// cleanly.
#[test]
fn a_record_at_the_bottom_of_the_event_time_range_is_committed() {
    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
        .expect("builder")
        .add_sink("a", sink)
        .expect("sink a")
        .chains(|ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ErrorPolicy::Skip);
            let a = split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("a"));
            split
                .route(move |row: Vec<u8>, out| out.emit(a, row))
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .expect("into_runtime");
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    handle.push_at(p, None, b"oldest", Some(i64::MIN));
    assert!(
        handle.wait_committed(p, 1, HANG),
        "the watermark never passed the record"
    );
    handle.push_at(p, None, b"next", Some(i64::MIN));
    assert!(
        handle.wait_committed(p, 2, HANG),
        "the watermark never passed the next record"
    );

    shutdown.trigger();
    let report = run
        .wait_exit(HANG)
        .expect("the pipeline did not stop")
        .expect("run");
    assert!(
        matches!(report.state, ExitState::Completed),
        "{:?}",
        report.state
    );
    assert_eq!(script.writes().len(), 2);
}
