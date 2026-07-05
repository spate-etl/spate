//! The "hello world" pipeline — everything in memory, no external systems.
//!
//! Demonstrates the full assembly every real pipeline follows:
//! source → deserializer → operator chain → sharded sink → checkpointed
//! commits, driven by [`PipelineRuntime`] with backpressure and graceful
//! shutdown — using `etl-test`'s in-memory source and capturing sink, so it
//! runs anywhere:
//!
//! ```sh
//! cargo run -p etl --example memory_pipeline
//! ```

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use etl::backpressure::InflightBudget;
use etl::config::PipelineConfig;
use etl::metrics::{ComponentLabels, SinkShardMetrics};
use etl::ops::{ChunkConfig, chain_owned};
use etl::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl::record::PartitionId;
use etl::sink::{KeyHashRouter, SinkPool, SinkPoolConfig, shard_queues};
use etl::source::LaneId;
use etl_test::{ReplicaTag, TestDeserializer, TestEncoder, capture_writer, memory_source};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Framework tuning comes from YAML; the `source`/`sink` sections are
/// opaque bags each connector's assembler reads. Here we assemble the
/// in-memory pieces by hand, so the tags are informational.
const CONFIG: &str = r#"
pipeline: { name: memory-demo, threads: 1 }
checkpoint: { interval: 200ms }
source: { memory: {} }
sink: { capture: {} }
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    etl::telemetry::init(etl::telemetry::LogFormat::Pretty, "info");
    let config = PipelineConfig::from_str(CONFIG)?;

    // ── Source ──────────────────────────────────────────────────────────
    // The in-memory source pairs with a handle that scripts assignments
    // and pushes records; a Kafka source gets all of this from the broker.
    let (source, handle) = memory_source();

    // ── Sink ────────────────────────────────────────────────────────────
    // One shard with one replica, captured in memory. Sink workers run on
    // a tokio runtime — the same runtime real connectors share.
    let (writer, script) = capture_writer();
    let endpoints = vec![vec![ReplicaTag {
        shard: 0,
        replica: 0,
    }]];
    let (queues, receivers) = shard_queues(1, 8);
    let budget = Arc::new(InflightBudget::new());
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50); // flush quickly for the demo
        cfg
    };
    let sink_labels = ComponentLabels::new("memory-demo", "sink", "capture");
    let pool = SinkPool::spawn(
        Arc::new(writer),
        endpoints,
        receivers,
        pool_cfg,
        Arc::clone(&budget),
        vec![SinkShardMetrics::new(
            &sink_labels,
            0,
            &["memory-0".to_string()],
        )],
        "memory-demo",
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: queues.clone(),
        drain: Box::new(move |deadline| {
            Box::pin(async move {
                let r = pool.drain(deadline).await;
                etl::pipeline::DrainReport {
                    flushed_batches: r.flushed,
                    abandoned_batches: r.abandoned,
                }
            })
        }),
        probe: None, // in-memory sink: nothing to probe
    };

    // ── Chain ───────────────────────────────────────────────────────────
    // Payloads are comma-separated words; the deserializer splits them
    // (one payload → N records), the chain filters and transforms, the
    // terminal stage encodes and routes to shard queues. One chain per
    // pipeline thread.
    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| {
        chain_owned::<Vec<u8>, _>(TestDeserializer::split_on(b','))
            .with_metrics("memory-demo", "main")
            .filter(|word: &Vec<u8>| !word.is_empty())
            .map(|word: Vec<u8>| word.to_ascii_uppercase())
            .sink(
                TestEncoder,
                KeyHashRouter,
                ChunkConfig::default(),
                chain_queues.clone(),
                Arc::clone(&chain_budget),
            )
            .build()
    };

    // ── Run ─────────────────────────────────────────────────────────────
    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false, // the demo triggers shutdown itself
            ..RuntimeOptions::default()
        });
    let shutdown = runtime.shutdown_handle();
    let pipeline = std::thread::spawn(move || runtime.run());

    // Feed it: one lane on partition 0, three payloads, nine words total
    // (one is empty and gets filtered).
    let p0 = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let mut last = 0;
    for payload in [
        &b"alpha,beta,gamma"[..],
        b"delta,,epsilon",
        b"zeta,eta,theta",
    ] {
        last = handle.push(p0, Some(b"demo"), payload);
    }

    // Watermarks advance once the sink acknowledges durably — wait for the
    // commit covering the last offset, then drain gracefully.
    let deadline = Instant::now() + Duration::from_secs(10);
    while handle.last_committed(p0) != Some(last + 1) {
        assert!(Instant::now() < deadline, "commit not observed in time");
        std::thread::sleep(Duration::from_millis(20));
    }
    shutdown.trigger();
    let report = pipeline.join().expect("pipeline thread")?;

    // What the sink captured, decoded back to rows.
    let rows: Vec<String> = script
        .writes()
        .iter()
        .flat_map(|w| etl_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8_lossy(&r).into_owned())
        .collect();

    println!("\npipeline exit: {:?}", report.state);
    println!("final watermarks: {:?}", report.final_watermarks);
    println!("rows written ({}): {rows:?}", rows.len());
    assert_eq!(rows.len(), 8, "nine words minus one filtered empty");
    assert!(rows.contains(&"ALPHA".to_string()));
    Ok(())
}
