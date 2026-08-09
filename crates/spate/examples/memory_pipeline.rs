//! The "hello world" pipeline — everything in memory, no external systems.
//!
//! Demonstrates the full assembly every real pipeline follows:
//! source → deserializer → operator chain → sharded sink → checkpointed
//! commits, assembled with [`Pipeline`] and driven with backpressure and
//! graceful shutdown — using `spate-test`'s in-memory source and capturing
//! sink bundle, so it runs anywhere:
//!
//! ```sh
//! cargo run -p spate --example memory_pipeline
//! ```
//!
//! This is also the pattern for **testing your own pipelines**: build with
//! [`Pipeline::into_runtime`] (instead of `run`) to get the shutdown
//! handle, spawn the run, drive records through the source handle, and
//! assert on what the sink captured.

// The `ANCHOR` comments below mark the regions the quickstart and the how-to
// guides render, and nest where a page shows part of a step. They are stripped
// from what those pages show; see docs/STYLE.md § 10.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-RANK:  10
// INDEX-TIER:  getting-started
// INDEX-GOAL:  build, drive and assert on a whole pipeline
// INDEX-TECH:  no infrastructure
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

// ANCHOR: imports
use spate::prelude::*;
use spate::source::LaneId;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, memory_source};
use std::time::{Duration, Instant};
// ANCHOR_END: imports

/// Framework tuning comes from YAML; the `source`/`sink` sections are
/// opaque bags each connector's factory reads. The in-memory pieces are
/// built programmatically, so the tags are informational.
// ANCHOR: config
const CONFIG: &str = r#"
pipeline: { name: memory-demo, threads: 1 }
checkpoint: { interval: 200ms }
source: { memory: {} }
sink: { capture: {} }
"#;
// ANCHOR_END: config

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Demos want pretty logs: init telemetry BEFORE the builder and its
    // JSON default becomes a no-op (first init wins). The builder owns the
    // rest of process init: the metrics exporter (before any handle can
    // exist) and the shared I/O runtime the sink workers run on — the same
    // runtime real connectors share.
    // ANCHOR: init
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;
    // ANCHOR_END: init

    // The in-memory source pairs with a handle that scripts assignments and
    // pushes records; a Kafka source gets all of this from the broker. The
    // sink is one shard with one replica, captured in memory, and its script
    // handle reads back every durable write.
    // ANCHOR: mocks
    let (source, handle) = memory_source();

    let (sink, script) = capture_sink(1, 1);
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50); // flush quickly for the demo
        cfg
    };
    let sink = sink.with_pool_config(pool_cfg);
    // ANCHOR_END: mocks

    // Payloads are comma-separated words; the deserializer splits them
    // (one payload → N records), the chain filters and transforms, the
    // terminal stage encodes and routes to shard queues. One chain per
    // pipeline thread; ChainCtx carries that thread's plumbing.
    // ANCHOR: chain
    let pipeline = pipeline
        .sink(sink)?
        .chains(|ctx| {
            // Resolve the sink's chunking up front — the per-sink YAML `chunk:`
            // block, or the 64 KiB default. Bind it before `with_metrics` moves
            // `ctx.pipeline`, so the later `ctx.chunk()` doesn't borrow a moved ctx.
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
        // ANCHOR: options
        .runtime_options(RuntimeOptions {
            handle_signals: false, // the demo triggers shutdown itself
            ..RuntimeOptions::default()
        });
    // ANCHOR_END: options

    // ANCHOR: handoff
    let runtime = pipeline.into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());
    // ANCHOR_END: handoff
    // ANCHOR_END: chain

    // Feed it: one lane on partition 0, three payloads, nine words total
    // (one is empty and gets filtered).
    // ANCHOR: drive
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

    // Bounded on purpose: an unbounded wait turns a broken pipeline into a
    // hung process rather than a failing one.
    let deadline = Instant::now() + Duration::from_secs(10);
    while handle.last_committed(p0) != Some(last + 1) {
        assert!(Instant::now() < deadline, "commit not observed in time");
        std::thread::sleep(Duration::from_millis(20));
    }
    shutdown.trigger();
    let report = join.join().expect("pipeline thread")?;
    // ANCHOR_END: drive

    // What the sink captured, decoded back to rows.
    // ANCHOR: capture
    let rows: Vec<String> = script
        .writes()
        .iter()
        .flat_map(|w| spate_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8_lossy(&r).into_owned())
        .collect();

    assert_eq!(rows.len(), 8, "nine words minus one filtered empty");
    assert!(rows.contains(&"ALPHA".to_string()));
    // ANCHOR_END: capture

    println!("\npipeline exit: {:?}", report.state);
    println!("final watermarks: {:?}", report.final_watermarks);
    println!("rows written ({}): {rows:?}", rows.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller, so the assertions above stop being decorative.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
