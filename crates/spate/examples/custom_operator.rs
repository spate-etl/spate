//! Writing custom operators.
//!
//! Operators in `spate` are **stateful closures** over the push model: `map`,
//! `filter`, `flat_map`, and `try_map` compose statically into one loop, and
//! a closure capturing its own state (here: a dedup set) is a full-blown
//! custom operator — no trait to implement. (The underlying `Collector` /
//! `StageLifecycle` traits in [`spate::ops`] are public for advanced stages,
//! but closures are the intended API.)
//!
//! This example drives a chain by hand — poll a batch, push it through,
//! flush — which is exactly what a pipeline thread does in production. It
//! deliberately bypasses the runtime; for a full assembly around a chain
//! like this one, see `memory_pipeline.rs` and `spate::pipeline::Pipeline`:
//!
//! ```sh
//! cargo run -p spate --example custom_operator
//! ```

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::backpressure::InflightBudget;
use spate::checkpoint::Checkpointer;
use spate::deser::Owned;
use spate::error::ErrorPolicy;
use spate::ops::{ChunkConfig, PushOutcome, chain_owned};
use spate::record::PartitionId;
use spate::sink::{KeyHashRouter, shard_queues};
use spate::source::{LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use spate_test::{TestDeserializer, TestEncoder, memory_source};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    // A chain: dedup (stateful flat_map) → validate (try_map with a
    // per-stage error policy) → uppercase (map).
    let (queues, mut receivers) = shard_queues(1, 16);
    let budget = Arc::new(InflightBudget::new());

    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut chain = chain_owned::<Vec<u8>, _>(TestDeserializer::passthrough())
        // Custom operator #1: stateful deduplication. `flat_map` emits
        // 0..N records; emitting zero drops the duplicate (its ack share
        // is released — dropping counts as success, not loss).
        .flat_map::<Owned<Vec<u8>>, _>(move |word, out| {
            if seen.insert(word.clone()) {
                out.emit(word);
            }
        })
        // Custom operator #2: record-level validation. `Skip` drops bad
        // records and counts them in metrics; `Fail` would stop the
        // pipeline instead.
        .try_map(
            |word: Vec<u8>| {
                if word.iter().all(u8::is_ascii_alphabetic) {
                    Ok(word)
                } else {
                    Err("non-alphabetic word")
                }
            },
            ErrorPolicy::Skip,
        )
        .map(|word: Vec<u8>| word.to_ascii_uppercase())
        .sink(
            TestEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            budget,
        )
        .build();

    // Drive it exactly like a pipeline thread: poll a batch from a lane,
    // push it through the chain, flush the terminal buffers.
    let mut cp = Checkpointer::new();
    let (mut source, handle) = memory_source();
    source.open(SourceCtx::new(cp.handle()))?;
    let p0 = PartitionId(0);
    cp.begin_epoch(&[p0], 1);
    handle.assign_lanes(&[(LaneId(0), p0)]);
    let SourceEvent::LanesAssigned(mut lanes) = source.poll_events(Duration::from_millis(100))?
    else {
        panic!("expected assignment");
    };

    for word in ["hello", "world", "hello", "rust", "n0pe", "world"] {
        handle.push(p0, None, word.as_bytes());
    }

    let mut batch = lanes[0]
        .poll(64, Duration::from_millis(200))?
        .expect("records queued");
    assert!(matches!(chain.push_batch(&mut batch, 0), PushOutcome::Done));
    drop(batch);
    assert!(matches!(chain.flush(), PushOutcome::Done));

    // What came out the other end: duplicates and the invalid word gone.
    let mut rows = Vec::new();
    while let Ok(chunk) = receivers[0].try_recv() {
        rows.extend(
            spate_test::decode_rows(&chunk.frame)
                .into_iter()
                .map(|r| String::from_utf8_lossy(&r).into_owned()),
        );
    }
    println!("deduped + validated + uppercased: {rows:?}");
    assert_eq!(rows, ["HELLO", "WORLD", "RUST"]);

    // The checkpoint side saw everything resolve (drops included).
    drop(chain); // releases the acks parked in received-but-undropped chunks
    cp.drain();
    println!("committable watermarks: {:?}", cp.take_watermarks());
    Ok(())
}
