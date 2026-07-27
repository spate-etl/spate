//! Shutdown must terminate while the sink is down (#83), asserted through a
//! whole `PipelineRuntime` rather than at `SinkPool` level.
//!
//! The unit tests in `spate-core` cover `SinkPool::drain` in isolation, and the
//! `spate` facade covers the real thing against a paused ClickHouse — but that
//! suite is Docker-gated and does not run on a pull request. This is the
//! docker-free gate for the same regression, and it exercises the leg the
//! `SinkPool` tests cannot: `PipelineRuntime::run`'s
//! `io.block_on(drain(budget))`, where a wedged drain wedges the process.
//!
//! The sink here is not hung, merely *down*: every write fails retryably and
//! `retry.max_attempts` defaults to 0 (unbounded), so each write task retries
//! for the length of the outage and never releases its in-flight permit. That
//! is the routine reachability of #83 — no exotic client behaviour needed.

use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::ErrorPolicy;
use spate_core::ops::{ChunkConfig, chain_owned};
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions, SinkOptions};

use spate_core::record::PartitionId;
use spate_core::sink::{BatchConfig, InflightConfig, KeyHashRouter, SinkPoolConfig};
use spate_core::source::LaneId;
use spate_test::{
    BytesPassthrough, PipelineRun, TestEncoder, WriteOutcome, capture_sink, memory_source,
};
use std::time::{Duration, Instant};

const CONFIG: &str = r#"
pipeline: { name: drain-wedge, threads: 1, io_threads: 1 }
metrics: { exporter: none, listen: "127.0.0.1:0" }
checkpoint: { interval: 200ms, drain_timeout: 2s, stalled_fail_after: 60s }
source: { memory: {} }
sinks:
  a: { capture: {} }
"#;

#[test]
fn shutdown_terminates_while_every_permit_is_held_by_a_failing_sink() {
    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    // One permit, and batches that seal on a single row: the second row's
    // batch has nowhere to go the moment the first write starts retrying.
    let sink = sink.with_pool_config(SinkPoolConfig {
        batch: BatchConfig {
            max_rows: 1,
            max_bytes: u64::MAX,
            linger: Duration::from_millis(50),
        },
        inflight: InflightConfig { max_per_shard: 1 },
        ..SinkPoolConfig::default()
    });

    // Scripts fall back to success once exhausted, which would heal the sink
    // mid-drain and pass this test for the wrong reason. Enqueue far more
    // failures than the drain budget can consume.
    for _ in 0..1024 {
        script.enqueue_global(WriteOutcome::retryable("sink is down"));
    }

    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
        .expect("builder")
        .add_sink_with(
            "a",
            sink,
            SinkOptions::default()
                // A shallow queue so the chain blocks behind the stalled
                // worker rather than buffering the whole push.
                .with_queue_capacity(2)
                // One chunk per row. At the default 64KiB the driver hands
                // the whole push over as a single chunk, the worker seals
                // exactly one batch, and the in-flight window is never
                // contended — the test would then pass on the unfixed worker
                // too, which it must not.
                .with_chunk(ChunkConfig {
                    target_bytes: 1,
                    ..ChunkConfig::default()
                }),
        )
        .expect("sink a")
        .chains(|ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ErrorPolicy::Fail);
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
    for i in 0..64u8 {
        handle.push(p, None, &[i]);
    }

    // Wait for the sink to actually be retrying, so the in-flight window is
    // provably saturated before shutdown rather than by assumption.
    spate_test::wait_until(
        Duration::from_secs(10),
        "the failing sink to start writing",
        || !script.writes().is_empty(),
    );

    let started = Instant::now();
    shutdown.trigger();
    // `wait_exit` bounds this: a bare `join` would hang the suite on a
    // regression instead of failing it. Well above the 2s drain_timeout, so a
    // deadline-honouring drain is never a flake and a wedge is unambiguous.
    let report = run
        .wait_exit(Duration::from_secs(30))
        .expect("pipeline did not exit while its sink was down (#83)")
        .expect("run");
    let exited_in = started.elapsed();

    assert!(
        exited_in < Duration::from_secs(15),
        "exit took {exited_in:?} against a 2s drain_timeout"
    );
    // Abandoning is the correct outcome, and asserting it proves the deadline
    // sweep actually ran rather than the sink having quietly healed.
    let drain = report.sink_drain.expect("a sink drain report");
    assert!(
        drain.abandoned > 0,
        "a sink that never accepted a write must abandon, got {drain:?}"
    );
    assert!(
        matches!(report.state, ExitState::Completed),
        "an abandoned batch is a completed drain, not a failure: {report:?}"
    );
}
