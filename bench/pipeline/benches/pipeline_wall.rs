//! Wall-clock cases for one whole bounded pipeline on event-log lines: S3
//! objects of newline-delimited JSON, decoded into a typed struct, flattened
//! and encoded as ClickHouse Native into a sink that discards the batches.
//!
//! Every case reads 64 objects of 16,384 lines from a local directory, about
//! 627 bytes a line and 10 MiB an object uncompressed. A line nests two levels
//! and is mostly strings, with an array of strings and a string map
//! (`spate_test_support::events`). The run is a bounded backfill: the
//! in-process lister lists the directory and the pipeline exits by itself once
//! every unit is acknowledged. The split target is 25 MiB, so a unit packs two
//! uncompressed objects or sixteen zstd ones, and the setup refuses a corpus
//! whose unit count the case's thread count does not divide: a thread takes
//! whole units, so an uneven share would leave threads idle at the end. The
//! sink's linger is 5 ms (the default is 1 s).
//!
//! The figures are lines and uncompressed bytes per second. Decompression,
//! framing, decoding, flattening and encoding all run on the pipeline threads,
//! so lines per second per pipeline core is the lines figure divided by the
//! case's thread count.
//! The GET is a local file read on the I/O runtime.
//!
//! Run: `cargo xtask bench ab --ref main --package spate-pipeline-bench --filter pipeline/`
//!
//! # What the figure prices
//!
//! The CPU a line costs from the stored object to an encoded block, at one and
//! at four pipeline threads. It does not price a network read or a ClickHouse
//! insert.
//!
//! # Cases
//!
//! - `pipeline/ndjson_zst/native/threads_{1,4}`: zstd level 3 objects.
//! - `pipeline/ndjson/native/threads_{1,4}`: the same lines uncompressed. libzstd
//!   is C, built outside rustc, so a build setting that reaches only Rust code
//!   shows here without the zstd share diluting it.
//!
//! # What the measured region carries
//!
//! One whole pipeline run: thread start-up, the listing, the reads, the sink
//! and the drain. The pipeline and its sink are built before the region by
//! `iter_batched`, one per iteration, and each case pins one iteration.
//! `tests/pipeline_bench_fixture.rs` pins that a second run from one setup
//! writes every line again.

use spate_bench::{Suite, bench_main};
use std::time::Duration;

#[path = "support/pipeline_bench.rs"]
mod pipeline_bench;

use pipeline_bench::{Rig, Shape, Stored};

/// Objects in the corpus.
const OBJECTS: usize = 64;

/// Lines in every object.
const LINES: usize = 16_384;

/// How long one run may take before it panics. It sits far above the slowest
/// case, so a run that reaches it has hung.
const DEADLINE: Duration = Duration::from_secs(600);

fn case(suite: Suite, stored: Stored, threads: usize) -> Suite {
    let shape = Shape {
        objects: OBJECTS,
        lines: LINES,
        stored,
        threads,
    };
    let id = format!("pipeline/{}/native/threads_{threads}", stored.name());
    suite
        .case(
            &id,
            move |corpus, _seed| {
                let rig = Rig::new(shape, DEADLINE, |object| corpus.absorb("object", object));
                let units = rig.units();
                assert_eq!(
                    units % threads as u64,
                    0,
                    "{units} units do not share evenly over {threads} threads"
                );
                corpus.absorb(
                    "run",
                    format!("stored={} threads={threads}", stored.name()).as_bytes(),
                );
                corpus.declare("backend", spate_json::BACKEND_ID.as_bytes());
                rig
            },
            move |b, rig: &Rig| {
                b.iter_batched(
                    |_| rig.make(),
                    |run| {
                        let rows = run.drive().rows;
                        assert_eq!(rows, shape.total_lines(), "every line reaches the sink");
                        rows
                    },
                );
            },
        )
        .items(shape.total_lines())
        .bytes_of(Rig::bytes)
        .iters(1)
        .done()
}

fn suite() -> Suite {
    let mut suite = spate_bench::suite("spate-pipeline-bench");
    for stored in [Stored::Zstd, Stored::Plain] {
        for threads in [1, 4] {
            suite = case(suite, stored, threads);
        }
    }
    suite
}

bench_main!(suite);
