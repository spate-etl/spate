//! Wall-clock cases for the feed read path on many small objects, against the
//! coordinated solo path at a read parallelism of 96.
//!
//! Every case reads 4,608 uncompressed newline-delimited objects of 64,000
//! bytes (1,000 lines of 64 bytes) from a local directory, through a store
//! wrapper that adds a fixed latency before every GET, into one sink that
//! counts rows and discards them. The figure is objects per second.
//!
//! - **`feed`** publishes one one-object unit per object to a fresh in-process
//!   feed. The reader holds up to `UNITS_HELD` units, so its read parallelism
//!   is that bound.
//! - **`coordinated`** runs the coordinated solo path over a fresh in-process
//!   store with `max_in_flight: 96`. Under a 1 MiB split target the objects
//!   pack 16 to a split: 288 splits, three waves of 96.
//!
//! Both arms run on 4 pipeline threads, the default runtime options and a sink
//! linger of 5 ms, the linger of the measured cloud runs (the default is 1 s).
//! Each feed unit carries the size and ETag the store reports, so both arms read
//! through the same pinned GET path.
//!
//! Run: `cargo xtask bench ab --ref main --filter small_objects`
//!
//! # What the figure prices
//!
//! The per-unit control-plane cost of the feed path, against the in-process
//! coordinated backend, the cheapest one. It does not predict throughput against a
//! cloud object store, and it is not the comparison against a networked
//! coordination store that the work feed was accepted on.
//!
//! # Expected floors at 20 ms
//!
//! The coordinated arm makes 4,608 GETs over 96 readers, a 960 ms floor of GET
//! latency alone. The feed arm makes them over `UNITS_HELD` readers: the same
//! floor at 96 held, 11.5 s at 8. They differ in how often a reader waits for
//! the sink.
//!
//! - A feed unit holds its slot until the sink has written its last rows, and
//!   a wave of small units never fills a batch by size, so every wave also
//!   waits up to one linger: 48 waves at 96 units held (about 1.2 s, 3,840
//!   objects/s) and 576 waves at 8 (about 14.4 s, 320 objects/s).
//! - A coordinated split reads its 16 objects back to back and waits for the
//!   sink once, three waves in all (about 0.975 s, 4,726 objects/s).
//!
//! At 96 held the feed arm's ceiling is about 19% below the coordinated one
//! (3,840 against 4,726 objects/s) before any control-plane cost.
//!
//! # Cases
//!
//! - `small_objects/{feed,coordinated}/get_{0,20}ms`. The 0 ms pair prices the
//!   path with no GET latency to hide behind.
//!
//! The coordinated cases go with the coordination removal in v0.4.0.
//!
//! # What the measured region carries
//!
//! One whole pipeline run: thread start-up, the reads, the sink and the
//! drain. The pipeline, its feed or coordination section and the store wrapper
//! are built before the region by `iter_batched`, one per iteration, and each
//! case pins one iteration. The feed arm ends when its last unit is
//! acknowledged: the rig triggers shutdown and wakes the reader at once, so
//! no poll timeout enters the region after that. The coordinated arm ends when
//! its plan completes. `tests/feed_bench_fixture.rs` pins that a second run
//! from one setup reads every row again.

use spate_bench::{Suite, bench_main};
use std::time::Duration;

#[path = "support/feed_bench.rs"]
mod feed_bench;

use feed_bench::{Arm, LINES, OBJECTS, Rig, SHORT_LINGER};

/// How long one run may take before it panics. It sits far above the slowest
/// case, so a run that reaches it has hung.
const DEADLINE: Duration = Duration::from_secs(3_600);

fn case(suite: Suite, id: &str, arm: Arm, hold_ms: u64, linger: Duration) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                for object in 0..OBJECTS {
                    corpus.absorb("object", &feed_bench::object_bytes(object));
                }
                corpus.absorb(
                    "run",
                    format!("{arm:?} hold={hold_ms}ms linger={linger:?}").as_bytes(),
                );
                Rig::new(
                    arm,
                    OBJECTS,
                    Duration::from_millis(hold_ms),
                    linger,
                    DEADLINE,
                )
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
    let suite = case(
        suite,
        "small_objects/feed/get_0ms",
        Arm::Feed,
        0,
        SHORT_LINGER,
    );
    let suite = case(
        suite,
        "small_objects/coordinated/get_0ms",
        Arm::Coordinated,
        0,
        SHORT_LINGER,
    );
    let suite = case(
        suite,
        "small_objects/feed/get_20ms",
        Arm::Feed,
        20,
        SHORT_LINGER,
    );
    case(
        suite,
        "small_objects/coordinated/get_20ms",
        Arm::Coordinated,
        20,
        SHORT_LINGER,
    )
}

bench_main!(suite);
