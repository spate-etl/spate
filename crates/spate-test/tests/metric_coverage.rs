//! Which declared metrics a running pipeline writes.
//!
//! Its own test binary. The witness must be the global recorder, installed
//! before the pipeline builds, because handles are pre-registered at build
//! time (INV-8).

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
use std::collections::BTreeSet;
use std::time::Duration;

const CONFIG: &str = r#"
pipeline: { name: coverage, threads: 2, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: prometheus }
checkpoint: { interval: 100ms }
source: { memory: {} }
sink: { capture: {} }
"#;

/// What one scripted pipeline run wrote, split by phase.
struct Coverage {
    /// Written during assembly, which is where a handle struct's constructor
    /// publishes an initial value.
    at_build: BTreeSet<String>,
    /// Written after assembly, with the pipeline running.
    while_running: BTreeSet<String>,
    witness: Witness,
}

/// Run a scripted pipeline to a clean drain and report what it wrote.
fn run_scenario() -> Coverage {
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

    // Assembly is over, so the next reading covers the run alone.
    let at_build = witness.written();
    witness.reset();

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

    // The run moved records, so the witness describes a working pipeline.
    let rows: Vec<Vec<u8>> = script
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .collect();
    assert_eq!(
        rows,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
    );

    Coverage {
        at_build,
        while_running: witness.written(),
        witness,
    }
}

/// The witness reports what was written, and sees the driver and controller
/// threads.
#[test]
fn the_witness_separates_written_series_from_registered_ones() {
    let coverage = run_scenario();
    let written = &coverage.while_running;
    let registered = coverage.witness.registered();

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

    // `written` is a subset of `registered`.
    for name in written {
        assert!(
            registered.contains(name),
            "`{name}` was written without being registered"
        );
    }

    // The scenario runs no coordinator, so this family stays unwritten.
    assert!(
        !written.contains(names::COORDINATION_LEADER),
        "the scenario runs no coordinator, so this cannot have been written"
    );
}

/// The framework series this scenario reaches while the pipeline runs.
///
/// The scenario is one pipeline over `spate-test`'s mocks: a memory source, a
/// passthrough chain and a capture sink, run to a clean drain. It has no
/// coordinator, no Kafka consumer and no failures.
///
/// Series outside that path are asserted where they occur. Coordination is
/// covered in `crates/spate-coordination/tests/revocation_metrics.rs`,
/// consumer lag in `crates/spate-kafka/tests/mock_cluster.rs`, and the failure
/// paths in the Docker-gated `crates/spate/tests/e2e_*.rs`.
const EXERCISED_WHILE_RUNNING: &[&str] = &[
    names::BACKPRESSURE_INFLIGHT_BYTES,
    names::CHECKPOINT_COMMITS_TOTAL,
    names::CHECKPOINT_COMMIT_DURATION_SECONDS,
    names::CHECKPOINT_PENDING_BATCHES,
    names::CHECKPOINT_WATERMARK_AGE_SECONDS,
    names::DESER_BATCH_DURATION_SECONDS,
    names::DESER_RECORDS_TOTAL,
    names::E2E_LATENCY_SECONDS,
    names::OPERATOR_BATCH_DURATION_SECONDS,
    names::OPERATOR_RECORDS_IN_TOTAL,
    names::OPERATOR_RECORDS_OUT_TOTAL,
    names::PIPELINE_INFO,
    names::PIPELINE_STATE,
    names::PIPELINE_THREADS,
    names::QUEUE_DEPTH,
    names::SINK_BATCH_BYTES,
    names::SINK_BATCH_ROWS,
    names::SINK_BYTES_TOTAL,
    names::SINK_FLUSHES_TOTAL,
    names::SINK_FLUSH_DURATION_SECONDS,
    names::SINK_INFLIGHT_BATCHES,
    names::SINK_PERMIT_WAIT_DURATION_SECONDS,
    names::SINK_RECORDS_TOTAL,
    names::SINK_REPLICA_HEALTHY,
    names::SINK_SHARD_HEALTHY,
    names::SINK_WRITE_DURATION_SECONDS,
    names::SOURCE_BYTES_TOTAL,
    names::SOURCE_LANES_ACTIVE,
    names::SOURCE_POLL_DURATION_SECONDS,
    names::SOURCE_RECORDS_TOTAL,
];

/// Series whose only writer is a handle struct's constructor.
const PUBLISHED_AT_BUILD: &[&str] = &[names::QUEUE_CAPACITY];

#[test]
fn the_happy_path_writes_every_series_it_covers() {
    let coverage = run_scenario();

    let mut silent: Vec<&str> = EXERCISED_WHILE_RUNNING
        .iter()
        .filter(|n| !coverage.while_running.contains(**n))
        .copied()
        .collect();
    silent.sort_unstable();
    assert!(
        silent.is_empty(),
        "registered and never written while the pipeline ran: {silent:?}"
    );

    let mut missing: Vec<&str> = PUBLISHED_AT_BUILD
        .iter()
        .filter(|n| !coverage.at_build.contains(**n))
        .copied()
        .collect();
    missing.sort_unstable();
    assert!(
        missing.is_empty(),
        "expected to be published by a constructor during assembly: {missing:?}"
    );

    // An entry the pipeline never registers has to be removed from the list,
    // not left to pass as a no-op.
    for name in EXERCISED_WHILE_RUNNING.iter().chain(PUBLISHED_AT_BUILD) {
        assert!(
            coverage.witness.registered().contains(*name),
            "`{name}` is listed here but the pipeline never registered it"
        );
    }
}
