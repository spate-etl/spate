//! A whole assembly through the `Pipeline` builder over this crate's
//! mocks: memory source in, capturing sink bundle out — the first-class
//! path framework users take to test their own pipelines.

use etl_core::config::PipelineConfig;
use etl_core::ops::chain_owned;
use etl_core::pipeline::{Pipeline, RuntimeOptions};
use etl_core::record::PartitionId;
use etl_core::sink::KeyHashRouter;
use etl_core::source::LaneId;
use etl_test::{BytesPassthrough, TestEncoder, capture_sink, decode_rows, memory_source};
use std::time::Duration;

const CONFIG: &str = r#"
pipeline: { name: bundle-test, threads: 1, io_threads: 1 }
metrics: { exporter: none }
source: { memory: {} }
sink: { capture: {} }
"#;

#[test]
fn builder_pipeline_over_mocks_delivers_and_commits() {
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

    // At-least-once: the watermark (one-past-last) only reaches past
    // `last` once the capture writer durably acknowledged every record.
    assert!(
        handle.wait_committed(p, last + 1, Duration::from_secs(10)),
        "timed out waiting for the commit (last: {:?})",
        handle.last_committed(p)
    );
    shutdown.trigger();
    let report = join.join().expect("join").expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");

    let rows: Vec<Vec<u8>> = script
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .collect();
    assert_eq!(
        rows,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()],
        "every record captured, in order"
    );
}
