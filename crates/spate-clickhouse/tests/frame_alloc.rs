//! Steady-state allocation of the sink's frame codec.
//!
//! One test per binary, because `Counting` counts every thread in the process
//! while a case runs.

use spate_clickhouse::Compression;
use spate_clickhouse::testing::frame_codec;
use spate_test_support::alloc::{Counting, measure};

#[global_allocator]
static ALLOC: Counting = Counting;

/// Pins that a warm lz4 codec allocates nothing for a frame, on either side
/// of the 65,535-byte hash table boundary. Regression for #1433.
#[test]
fn a_warm_lz4_frame_allocates_nothing() {
    let mut codec = frame_codec(Compression::Lz4)
        .expect("a compressing codec")
        .make();
    for len in [40_000_usize, 70_000] {
        let chunk: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let counts = measure(|| {
            codec.encode_frame(&chunk).expect("lz4 codes");
        })
        .expect("Counting is the global allocator");
        assert_eq!((counts.allocs, counts.bytes), (0, 0), "{len}-byte chunk");
    }
}
