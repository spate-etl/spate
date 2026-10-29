//! Event-basis latency uses the oldest known event time in a sink batch.

use spate_core::config::PipelineConfig;
use spate_core::metrics::{E2eBasis, Exporter, MetricsSettings, install};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::{PartitionId, RecordMeta};
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{
    BytesPassthrough, TestEncoder, capture_sink, decode_rows, memory_source, wait_until,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONFIG: &str = r#"
pipeline: { name: e2e-event-time, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: prometheus, e2e_basis: event }
source: { memory: {} }
sink: { capture: {} }
"#;

fn histogram_value(rendered: &str, suffix: &str) -> f64 {
    let line = rendered
        .lines()
        .find(|line| line.starts_with(&format!("spate_e2e_latency_seconds_{suffix}{{")))
        .unwrap_or_else(|| panic!("no end-to-end {suffix}:\n{rendered}"));
    line.rsplit(' ')
        .next()
        .expect("histogram value")
        .parse()
        .expect("numeric histogram value")
}

/// An absent event time does not hide a known event time in the same batch.
/// Regression for #1285.
#[test]
fn mixed_batch_uses_its_oldest_present_event_time() {
    let metrics = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        e2e_basis: E2eBasis::Event,
        ..MetricsSettings::default()
    })
    .expect("install Prometheus recorder");

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

    let event_time_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("current time fits event timestamp")
        - 60_000;
    let partition = PartitionId(0);
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());
    wait_until(Duration::from_secs(10), "memory source to open", || {
        handle.is_open()
    });
    handle.push_at(partition, None, b"present", Some(event_time_ms));
    let last = handle.push_at(partition, None, b"absent", Some(RecordMeta::NO_EVENT_TIME));
    handle.assign_lanes(&[(LaneId(0), partition)]);
    assert!(
        handle.wait_committed(partition, last + 1, Duration::from_secs(10)),
        "timed out waiting for commit (last: {:?})",
        handle.last_committed(partition)
    );
    shutdown.trigger();
    let report = join.join().expect("join").expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");

    let rows: Vec<Vec<u8>> = script
        .writes()
        .iter()
        .flat_map(|write| decode_rows(&write.payload))
        .collect();
    assert_eq!(rows, vec![b"present".to_vec(), b"absent".to_vec()]);

    let rendered = metrics.render();
    let sum = histogram_value(&rendered, "sum");
    let count = histogram_value(&rendered, "count");
    assert_eq!(count, 1.0, "both records must share one batch:\n{rendered}");
    assert!(
        (30.0..120.0).contains(&sum),
        "the batch should use the present timestamp about one minute ago:\n{rendered}"
    );
}
