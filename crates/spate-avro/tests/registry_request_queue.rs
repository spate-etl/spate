//! Pins that a pipeline whose schema registry accepts connections and never
//! answers holds flat memory while its records wait on schema fetches.

use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, AvroValue, RegistrySection};
use spate_core::config::PipelineConfig;
use spate_core::metrics::{Exporter, MetricsSettings, install};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{
    HANG, PipelineRun, TestEncoder, capture_sink, confluent_frame, memory_source, metric_sum,
    unique_name, wait_until,
};
use spate_test_support::{resident_kib, silent_server};
use std::time::Duration;

/// Not-ready replays at the first memory reading; the second is at ten times
/// as many.
const REPLAYS: u64 = 200_000;
/// Allowed growth in resident memory between the two readings.
const TOLERANCE_KIB: u64 = 2 * 1024;
const THREADS: u32 = 8;

/// Eight pipeline threads each hold a payload naming its own unknown schema
/// id, so more ids wait than the fetcher fetches at once. Every replay asks
/// for its id again, and resident memory at ten times the replays stays
/// within [`TOLERANCE_KIB`] of the first reading.
#[test]
fn a_stalled_registry_holds_flat_memory() {
    let registry = silent_server();
    let name = unique_name("avro-request-queue");
    let metrics = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");
    let config = format!(
        "pipeline: {{ name: {name}, threads: {THREADS} }}\n\
         admin: {{ listen: none }}\n\
         metrics: {{ exporter: prometheus }}\n\
         source: {{ memory: {{}} }}\n\
         sink: {{ capture: {{}} }}\n"
    );
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(&config).expect("config")).expect("builder");
    let settings = AvroSettings {
        mode: AvroMode::Confluent,
        registry: Some(RegistrySection::new(format!("http://{registry}"))),
        ..AvroSettings::default()
    };
    let deserializer = AvroDeserializerBuilder::from_settings(&settings, &pipeline.io_handle())
        .expect("builder")
        .build_value()
        .expect("deserializer");

    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let runtime = pipeline
        .sink(sink)
        .expect("sink")
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            chain_owned::<AvroValue, _>(deserializer.clone())
                .with_metrics(ctx.pipeline, "main")
                .map(|_: AvroValue| b"decoded".to_vec())
                .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            // A short retry reaches the replay counts in seconds.
            blocked_retry: Duration::from_micros(50),
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .expect("into_runtime");
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let lanes: Vec<(LaneId, PartitionId)> =
        (0..THREADS).map(|i| (LaneId(i), PartitionId(i))).collect();
    for i in 0..THREADS {
        handle.push(PartitionId(i), None, &confluent_frame(i + 1, &[0x02]));
    }
    handle.assign_lanes(&lanes);

    let replays = || {
        metric_sum(
            &metrics.render(),
            "spate_deser_not_ready_total",
            &[("pipeline", &name)],
        )
        .unwrap_or(0.0)
    };
    let hang = HANG.saturating_mul(10);
    wait_until(hang, "the first replay count", || {
        replays() >= REPLAYS as f64
    });
    let first = resident_kib();
    wait_until(hang, "ten times the replays", || {
        replays() >= (10 * REPLAYS) as f64
    });
    let last = resident_kib();

    shutdown.trigger();
    run.wait_exit(HANG)
        .expect("the pipeline exits after shutdown")
        .expect("a clean exit");
    assert!(
        last <= first + TOLERANCE_KIB,
        "resident memory grew from {first} KiB at {REPLAYS} replays to {last} KiB at {} \
         (tolerance {TOLERANCE_KIB} KiB)",
        10 * REPLAYS
    );
}
