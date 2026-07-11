//! A multi-sink **split** assembly through the `Pipeline` builder over this
//! crate's mocks: memory source in, two capturing sinks out, routed by
//! payload — the first-class path a framework user takes to test a split
//! pipeline (routing, the `unmatched` policy, and at-least-once across sinks).

use etl_core::config::PipelineConfig;
use etl_core::deser::Owned;
use etl_core::error::ErrorPolicy;
use etl_core::ops::{ChunkConfig, chain_owned};
use etl_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use etl_core::record::PartitionId;
use etl_core::sink::KeyHashRouter;
use etl_core::source::LaneId;
use etl_test::{BytesPassthrough, TestEncoder, capture_sink, decode_rows, memory_source};
use std::time::{Duration, Instant};

const CONFIG: &str = r#"
pipeline: { name: split-test, threads: 1, io_threads: 1 }
metrics: { exporter: none, listen: "127.0.0.1:0" }
source: { memory: {} }
sinks:
  a: { capture: {} }
  b: { capture: {} }
"#;

#[test]
fn split_routes_to_named_sinks_and_commits_at_least_once() {
    let (source, handle) = memory_source();
    let (sink_a, script_a) = capture_sink(1, 1);
    let (sink_b, script_b) = capture_sink(1, 1);

    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
        .expect("builder")
        .add_sink("a", sink_a)
        .expect("sink a")
        .add_sink("b", sink_b)
        .expect("sink b")
        .chains(|ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ChunkConfig::default(), ErrorPolicy::Skip);
            // Two branches over the same owned family, different capture sinks.
            let a = split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("a"));
            let b = split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("b"));
            split
                .route(move |row: Vec<u8>, out| match row.first() {
                    Some(b'a') => out.emit(a, row),
                    Some(b'b') => out.emit(b, row),
                    // Anything else matches no branch → dropped (Skip policy).
                    _ => {}
                })
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
    for payload in [&b"apple"[..], b"banana", b"cherry", b"avocado", b"berry"] {
        last = handle.push(p, None, payload);
    }

    // At-least-once: the watermark reaches one-past-last only after every
    // record — routed to either sink, or Skipped — is accounted for.
    let deadline = Instant::now() + Duration::from_secs(10);
    while handle.last_committed(p) != Some(last + 1) {
        assert!(
            Instant::now() < deadline,
            "commit timeout (last committed: {:?})",
            handle.last_committed(p)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    shutdown.trigger();
    let report = join.join().expect("join").expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");

    let rows_a: Vec<Vec<u8>> = script_a
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .collect();
    let rows_b: Vec<Vec<u8>> = script_b
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .collect();
    assert_eq!(
        rows_a,
        vec![b"apple".to_vec(), b"avocado".to_vec()],
        "a-prefixed rows routed to sink a, in order"
    );
    assert_eq!(
        rows_b,
        vec![b"banana".to_vec(), b"berry".to_vec()],
        "b-prefixed rows routed to sink b, in order"
    );
    // "cherry" matched no branch and was Skipped — it is in neither sink.
}

#[test]
fn split_unmatched_fail_stops_the_pipeline() {
    let (source, handle) = memory_source();
    let (sink_a, _script_a) = capture_sink(1, 1);

    const ONE_SINK: &str = r#"
pipeline: { name: split-fail, threads: 1, io_threads: 1 }
metrics: { exporter: none, listen: "127.0.0.1:0" }
checkpoint: { interval: 200ms, drain_timeout: 2s, stalled_fail_after: 3s }
source: { memory: {} }
sinks:
  a: { capture: {} }
"#;

    let runtime = Pipeline::from_config(PipelineConfig::from_str(ONE_SINK).expect("config"))
        .expect("builder")
        .add_sink("a", sink_a)
        .expect("sink a")
        .chains(|ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ChunkConfig::default(), ErrorPolicy::Fail);
            let a = split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("a"));
            split
                .route(move |row: Vec<u8>, out| {
                    if row.first() == Some(&b'a') {
                        out.emit(a, row);
                    }
                    // else: unmatched under Fail policy → stop the pipeline.
                })
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .expect("into_runtime");

    let join = std::thread::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    handle.push(p, None, b"apple"); // routes to a
    handle.push(p, None, b"zebra"); // unmatched → Fail records a fatal

    // The Fail-policy trip must stop the pipeline on its own — no external
    // shutdown trigger. The deadline is far above the configured 2s
    // drain_timeout so a regression to timeout-bound (or never) exits fails
    // here rather than being masked by a manual drain.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !join.is_finished() {
        assert!(
            Instant::now() < deadline,
            "pipeline did not stop on its own after a Fail-policy fatal"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let report = join.join().expect("join").expect("run");
    assert!(
        matches!(report.state, ExitState::Failed(_)),
        "unmatched record under Fail policy must fail the pipeline (got {:?})",
        report.state
    );
    assert_eq!(report.exit_code(), 1);
}
