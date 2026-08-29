//! Which declared metrics a running pipeline actually writes.
//!
//! Its own test binary because it installs the process-global recorder, and it
//! must be the recorder present before the pipeline builds: metric handles are
//! pre-registered at build time (INV-8) and bind to whatever recorder exists
//! then.
//!
//! `spate-core`'s own test binary already claims the global recorder, and its
//! pipeline fakes are `#[cfg(test)]` and so invisible from outside the crate,
//! which is why this lives here, on `spate-test`'s public mocks.

mod metric_witness;

use metric_witness::{Witness, WitnessRecorder};
use metrics_exporter_prometheus::PrometheusBuilder;
use spate_core::config::PipelineConfig;
use spate_core::metrics::names;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{BytesPassthrough, TestEncoder, capture_sink, decode_rows, memory_source};
use std::time::Duration;

const CONFIG: &str = r#"
pipeline: { name: coverage, threads: 2, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: prometheus }
checkpoint: { interval: 100ms }
source: { memory: {} }
sink: { capture: {} }
"#;

/// Run a scripted pipeline to a clean drain and report what it wrote.
fn run_scenario() -> Witness {
    // Installed before anything builds. `spate-core`'s own `install` then finds
    // the global slot taken, warns, and runs against this recorder with a
    // detached render handle.
    let recorder = WitnessRecorder::new(PrometheusBuilder::new().build_recorder());
    let witness = recorder.witness();
    metrics::set_global_recorder(recorder).expect("the witness owns the global recorder");

    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);

    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
        .expect("builder")
        .sink(sink)
        .expect("sink")
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline, "main")
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
        .into_runtime(source)
        .expect("into_runtime");

    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let mut last = 0;
    for payload in [&b"alpha"[..], b"beta", b"gamma"] {
        last = handle.push(p, Some(b"key"), payload);
    }
    assert!(
        handle.wait_committed(p, last + 1, Duration::from_secs(10)),
        "timed out waiting for commit (last: {:?})",
        handle.last_committed(p)
    );
    shutdown.trigger();
    let report = join.join().expect("join").expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");

    // Records flowed, so the run is a real one and the witness describes a
    // pipeline that did work.
    let rows: Vec<Vec<u8>> = script
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .collect();
    assert_eq!(
        rows,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
    );

    witness
}

/// The witness separates "something wrote this" from "this exists", which is
/// the distinction a rendered exposition cannot express.
///
/// `spate_backpressure_inflight_bytes` is the worked example. Before the fix
/// for #332 it was registered by every driver and written by nothing, and it
/// rendered `0` exactly as an idle pipeline's would.
#[test]
fn the_witness_separates_written_series_from_registered_ones() {
    let witness = run_scenario();
    let written = witness.written();
    let registered = witness.registered();

    assert!(
        !written.is_empty(),
        "the witness saw no writes at all, so it is not observing the pipeline"
    );

    // A counter on the per-record path, incremented at batch boundaries.
    assert!(
        written.contains(names::OPERATOR_RECORDS_IN_TOTAL),
        "the witness must see writes from the driver threads; wrote:\n{written:#?}"
    );
    // A gauge on the control plane, written by the controller thread.
    assert!(
        written.contains(names::BACKPRESSURE_INFLIGHT_BYTES),
        "the witness must see writes from the controller thread; wrote:\n{written:#?}"
    );

    // Every written name was registered first, so the two sets are consistent
    // and `written` is a subset rather than a separate accounting.
    for name in &written {
        assert!(
            registered.contains(name),
            "`{name}` was written without being registered"
        );
    }

    // Registration alone is not coverage: the scenario has no coordinator, so
    // nothing in that family can have been written.
    assert!(
        !written.contains(names::COORDINATION_LEADER),
        "the scenario runs no coordinator, so this cannot have been written"
    );
}
