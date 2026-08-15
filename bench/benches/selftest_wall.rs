//! The harness benching itself.
//!
//! Two jobs. It is the worked example a bench author copies, with four cases
//! covering the shapes the builder supports, and it is what the acceptance run
//! drives: `make bench-ab REF=HEAD REPS=6 PACKAGE=spate-bench` compares this target against itself
//! and must flag nothing. A flag there means the harness is measuring its own
//! noise rather than the code under test.
//!
//! It also proves the feature split. This file compiles against `spate-bench`'s
//! **default** features, so a crate adding a `benches/*_wall.rs` of its own
//! never builds the CLI, clap, or the comparator.
//!
//! Run: `cargo bench -p spate-bench --bench selftest_wall`

use spate_bench::rng::SplitMix64;
use spate_bench::{Suite, bench_main};

fn suite() -> Suite {
    spate_bench::suite("spate-bench")
        // Clone-and-sort: one allocation per iteration, inside the region. This
        // is the case that gives the acceptance run a *flaggable* allocation
        // comparison. `alloc_churn` below allocates far more, but it is
        // erratic and can never reach the significant table.
        .case(
            "sort_u64_16k",
            |corpus, seed| {
                let mut rng = SplitMix64::new(seed);
                let values: Vec<u64> = (0..16_384).map(|_| rng.next_u64()).collect();
                let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
                corpus.absorb("values", &bytes);
                values
            },
            |b, values| {
                b.iter(|| {
                    let mut scratch = values.clone();
                    scratch.sort_unstable();
                    scratch
                });
            },
        )
        .items(16_384)
        .bytes(16_384 * 8)
        .done()
        // The same workload through `iter_batched`, which builds every input
        // before the region so only the sort is timed. It reports no resident
        // set: the harness holds one prebuilt input per iteration, and the
        // high-water mark would be about that rather than about the sort. One
        // input is 128 KiB, so the count is pinned rather than calibrated.
        .case(
            "sort_batched_16k",
            |corpus, seed| {
                let mut rng = SplitMix64::new(seed);
                let values: Vec<u64> = (0..16_384).map(|_| rng.next_u64()).collect();
                let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
                corpus.absorb("values", &bytes);
                values
            },
            |b, values| {
                b.iter_batched(
                    |_| values.clone(),
                    |mut scratch| {
                        scratch.sort_unstable();
                        scratch
                    },
                );
            },
        )
        .items(16_384)
        .bytes(16_384 * 8)
        .iters(64)
        .done()
        // A scan over borrowed bytes: no allocation at all, so its allocation
        // metrics are a true zero rather than an absent one, the distinction
        // the record schema turns on. Its resident set is legitimately
        // unattributable: the corpus is already resident when the region opens
        // and nothing else grows, so none is emitted and the record says why.
        .case(
            "utf8_scan_1mib",
            |corpus, seed| {
                let mut rng = SplitMix64::new(seed);
                let text = rng.ascii(1 << 20);
                corpus.absorb("text", &text);
                text
            },
            |b, text| {
                b.iter(|| {
                    text.iter()
                        .filter(|byte| byte.is_ascii_alphabetic())
                        .count()
                });
            },
        )
        .items(1 << 20)
        .bytes(1 << 20)
        .done()
        // Deliberately noisy: a churn of short-lived allocations of varying size
        // is decided by the allocator's free lists as much as by the code. It is
        // here so the report has an informational row to render, and so the
        // `erratic` path is exercised by every run rather than only by a unit
        // test.
        .case(
            "alloc_churn",
            |corpus, seed| {
                let mut rng = SplitMix64::new(seed);
                let sizes: Vec<usize> = (0..512)
                    .map(|_| 16 + usize::try_from(rng.below(496)).unwrap_or(0))
                    .collect();
                let bytes: Vec<u8> = sizes
                    .iter()
                    .flat_map(|s| (*s as u64).to_le_bytes())
                    .collect();
                corpus.absorb("sizes", &bytes);
                sizes
            },
            |b, sizes| {
                b.iter(|| {
                    let mut total = 0usize;
                    for size in sizes {
                        let block: Vec<u8> = vec![7u8; *size];
                        total += block.len();
                    }
                    total
                });
            },
        )
        .items(512)
        .erratic("a churn of short-lived allocations is decided by the allocator's free lists")
        .done()
}

bench_main!(suite);
