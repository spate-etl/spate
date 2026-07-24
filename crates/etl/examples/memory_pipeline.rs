//! The "hello world" pipeline — everything in memory, no external systems.
//!
//! Demonstrates the full assembly every real pipeline follows:
//! source → deserializer → operator chain → sharded sink → checkpointed
//! commits, assembled with [`Pipeline`] and driven with backpressure and
//! graceful shutdown — using `etl-test`'s in-memory source and capturing
//! sink bundle, so it runs anywhere:
//!
//! ```sh
//! cargo run -p etl --example memory_pipeline
//! ```
//!
//! This is also the pattern for **testing your own pipelines**: build with
//! [`Pipeline::into_runtime`] (instead of `run`) to get the shutdown
//! handle, spawn the run, drive records through the source handle, and
//! assert on what the sink captured.

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use etl::prelude::*;
use etl::source::LaneId;
use etl_test::{TestDeserializer, TestEncoder, capture_sink, memory_source};
use std::time::{Duration, Instant};

/// Framework tuning comes from YAML; the `source`/`sink` sections are
/// opaque bags each connector's factory reads. The in-memory pieces are
/// built programmatically, so the tags are informational.
const CONFIG: &str = r#"
pipeline: { name: memory-demo, threads: 1 }
checkpoint: { interval: 200ms }
source: { memory: {} }
sink: { capture: {} }
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Demos want pretty logs: init telemetry BEFORE the builder and its
    // JSON default becomes a no-op (first init wins).
    etl::telemetry::init(etl::telemetry::LogFormat::Pretty, "info");

    // The builder owns the rest of process init: the metrics exporter
    // (before any handle can exist) and the shared I/O runtime the sink
    // workers run on — the same runtime real connectors share.
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;

    // ── Source ──────────────────────────────────────────────────────────
    // The in-memory source pairs with a handle that scripts assignments
    // and pushes records; a Kafka source gets all of this from the broker.
    let (source, handle) = memory_source();

    // ── Sink ────────────────────────────────────────────────────────────
    // One shard with one replica, captured in memory; the script handle
    // reads back every durable write.
    let (sink, script) = capture_sink(1, 1);
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50); // flush quickly for the demo
        cfg
    };
    let sink = sink.with_pool_config(pool_cfg);

    // ── Chain, assembled and spawned ────────────────────────────────────
    // Payloads are comma-separated words; the deserializer splits them
    // (one payload → N records), the chain filters and transforms, the
    // terminal stage encodes and routes to shard queues. One chain per
    // pipeline thread; ChainCtx carries that thread's plumbing.
    let runtime = pipeline
        .sink(sink)?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::split_on(b','))
                .with_metrics(ctx.pipeline, "main")
                .filter(|word: &Vec<u8>| !word.is_empty())
                .map(|word: Vec<u8>| word.to_ascii_uppercase())
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
            handle_signals: false, // the demo triggers shutdown itself
            ..RuntimeOptions::default()
        })
        .into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

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
    let report = join.join().expect("pipeline thread")?;

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
