//! Wall-clock cases for the feed read path on many small objects at a read
//! parallelism of 8.
//!
//! Every case reads 4,608 uncompressed newline-delimited objects of 64,000
//! bytes (1,000 lines of 64 bytes) from a local directory, through a store
//! wrapper that adds a fixed latency before every GET, into one sink that
//! counts rows and discards them. The figure is objects per second.
//!
//! Each run publishes one one-object unit per object to a fresh in-process
//! feed. The run uses 4 pipeline threads and leaves `units_held` unset, so the
//! reader holds two units per thread, 8, and its read parallelism is 8. It uses
//! the default runtime options and a sink linger of 5 ms, the linger of the measured cloud runs (the default is
//! 1 s). Each unit carries the size and ETag the store reports, so the read
//! goes through the pinned GET path.
//!
//! Run: `cargo xtask bench ab --ref main --filter small_objects`
//!
//! # What the figure prices
//!
//! The per-unit control-plane cost of the feed path against the in-process
//! feed, the cheapest one. It does not predict throughput against a cloud
//! object store or a networked feed.
//!
//! # Expected floors at 20 ms
//!
//! 4,608 GETs over 8 readers make an 11.5 s floor of GET latency alone; over
//! 96 readers, 960 ms. A unit holds its slot until the sink has written its
//! last rows, and a wave of small units never fills a batch by size, so every
//! wave also waits up to one linger: 576 waves at 8 units held (about 14.4 s,
//! 320 objects/s) and 48 waves at 96 (about 1.2 s, 3,840 objects/s).
//!
//! # Cases
//!
//! - `small_objects/feed/get_{0,20}ms`. The 0 ms case prices the path with no
//!   GET latency to hide behind.
//!
//! # What the measured region carries
//!
//! One whole pipeline run: thread start-up, the reads, the sink and the
//! drain. The pipeline, its feed and the store wrapper are built before the
//! region by `iter_batched`, one per iteration, and each case pins one
//! iteration. A run ends when its last unit is acknowledged: the rig triggers
//! shutdown and wakes the reader at once, so no poll timeout enters the region
//! after that. `tests/feed_bench_fixture.rs` pins that a second run from one
//! setup reads every row again.

use spate_bench::{Suite, bench_main};
use std::time::Duration;

#[path = "support/feed_bench.rs"]
mod feed_bench;

use feed_bench::{LINES, OBJECTS, Rig, SHORT_LINGER};

/// How long one run may take before it panics. It sits far above the slowest
/// case, so a run that reaches it has hung.
const DEADLINE: Duration = Duration::from_secs(3_600);

fn case(suite: Suite, id: &str, hold_ms: u64, linger: Duration) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                for object in 0..OBJECTS {
                    corpus.absorb("object", &feed_bench::object_bytes(object));
                }
                corpus.absorb(
                    "run",
                    format!("hold={hold_ms}ms linger={linger:?}").as_bytes(),
                );
                Rig::new(OBJECTS, Duration::from_millis(hold_ms), linger, DEADLINE)
            },
            |b, rig: &Rig| {
                b.iter_batched(
                    |_| rig.make(),
                    |run| {
                        let rows = run.drive().rows;
                        assert_eq!(rows, (OBJECTS * LINES) as u64, "every row reaches the sink");
                        rows
                    },
                );
            },
        )
        .items(OBJECTS as u64)
        .iters(1)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-s3");
    let suite = case(suite, "small_objects/feed/get_0ms", 0, SHORT_LINGER);
    case(suite, "small_objects/feed/get_20ms", 20, SHORT_LINGER)
}

bench_main!(suite);
