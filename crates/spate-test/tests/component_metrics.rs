//! The component-metrics seam end-to-end: the runtime hands a `Source` and a
//! sink `ShardWriter` each a role-scoped [`Meter`], and their custom families
//! render under the `spate_<component_type>_<role>_*` umbrella.
//!
//! Its own test binary because it installs the process-global Prometheus
//! recorder (once-per-process); it renders through the reused install handle
//! and asserts the mocks' families appear with the standard labels.

use spate_core::config::PipelineConfig;
use spate_core::metrics::{Exporter, MetricsSettings, install};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{BytesPassthrough, TestEncoder, capture_sink, decode_rows, memory_source};
use std::time::Duration;

// Prometheus exporter so the mocks' families are recorded; port 0 lets the OS
// pick a free admin port (no collision with a real deployment).
const CONFIG: &str = r#"
pipeline: { name: seam-test, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: prometheus }
source: { memory: {} }
sink: { capture: {} }
"#;

#[test]
fn source_and_sink_receive_role_scoped_meters() {
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
    for payload in [&b"alpha"[..], b"beta"] {
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

    // Records still flowed end-to-end.
    let rows: Vec<Vec<u8>> = script
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .collect();
    assert_eq!(rows, vec![b"alpha".to_vec(), b"beta".to_vec()]);

    // `install` is idempotent, so this reuses the pipeline's global handle to
    // render.
    let rendered = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("reuse installed exporter")
    .render();

    // The source's family: namespace = its `component_type()` ("memory"),
    // role = source (from wiring position), standard labels attached.
    assert!(
        rendered.contains(
            r#"spate_memory_source_opens_total{pipeline="seam-test",component="source",component_type="memory"}"#
        ),
        "source family missing:\n{rendered}"
    );
    // The sink's family: namespace = SinkParts component_type ("capture"),
    // role = sink; the default single sink keeps component="sink".
    assert!(
        rendered.contains(
            r#"spate_capture_sink_attaches_total{pipeline="seam-test",component="sink",component_type="capture"}"#
        ),
        "sink family missing:\n{rendered}"
    );
}
