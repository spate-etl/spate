//! Allocation snapshot of this crate's RowBinary and Native encoders.
//!
//! Each case drives the same rig and the same `encode_chunk` region the
//! counted tier in `encode_gungraun.rs` measures, under the same case names,
//! and prints its allocation, reallocation and byte counts for
//! `cargo xtask bench alloc` to compare with `benches/alloc/`. Harness-free, so
//! every case runs on the main thread and nothing else allocates while one is
//! counted. Each case is counted once after one uncounted chunk, so the Native
//! encoder's columns have been reset by `finish_chunk` and keep their
//! capacity. The RowBinary rig's encoder skips the first-record check;
//! `tests/encoder_alloc.rs` pins that a checked encoder runs it once.

use spate_test_support::alloc::{Counting, main_with};

#[path = "support/encode_rig.rs"]
mod encode_rig;
#[path = "support/rows.rs"]
mod rows;

use encode_rig::{
    encode_chunk, native_events, native_exotic, native_metrics, rowbinary_events, rowbinary_metrics,
};

#[global_allocator]
static ALLOC: Counting = Counting;

fn main() {
    main_with(|report| {
        let mut r = native_events();
        report.case("native_events", || encode_chunk(&mut r));
        let mut r = native_metrics();
        report.case("native_metrics", || encode_chunk(&mut r));
        let mut r = native_exotic();
        report.case("native_exotic", || encode_chunk(&mut r));
        let mut r = rowbinary_events();
        report.case("rowbinary_events", || encode_chunk(&mut r));
        let mut r = rowbinary_metrics();
        report.case("rowbinary_metrics", || encode_chunk(&mut r));
    });
}
