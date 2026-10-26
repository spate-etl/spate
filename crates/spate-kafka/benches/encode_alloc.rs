//! Allocation snapshot of this crate's sink encode path.
//!
//! Each case drives the same rig and the same `encode_run` region the counted
//! tier in `encode_gungraun.rs` measures, under the same case names, and
//! prints its allocation, reallocation and byte counts for
//! `cargo xtask bench alloc` to compare with `benches/alloc/`. Harness-free, so
//! every case runs on the main thread and nothing else allocates while one is
//! counted. Each case is counted once after one uncounted run, so the
//! encoder's scratch is already sized.

use spate_test_support::alloc::{Counting, main_with};

#[path = "support/encode_rig.rs"]
mod encode_rig;
#[path = "support/messages.rs"]
mod messages;

use encode_rig::{
    bytes_keyed, bytes_keyless, encode_run, json_typed, stamped_headers, stamped_over_limit,
};
use messages::RECORDS;

#[global_allocator]
static ALLOC: Counting = Counting;

fn main() {
    main_with(|report| {
        let mut r = bytes_keyless();
        report.case("bytes_keyless", || {
            assert_eq!(encode_run(&mut r), RECORDS, "a record failed to encode");
        });
        let mut r = bytes_keyed();
        report.case("bytes_keyed", || {
            assert_eq!(encode_run(&mut r), RECORDS, "a record failed to encode");
        });
        let mut r = stamped_headers();
        report.case("stamped_headers", || {
            assert_eq!(encode_run(&mut r), RECORDS, "a record failed to encode");
        });
        let mut r = stamped_over_limit();
        report.case("stamped_over_limit", || {
            assert_eq!(encode_run(&mut r), 0, "the size guard accepted a record");
        });
        let mut r = json_typed();
        report.case("json_typed", || {
            assert_eq!(encode_run(&mut r), RECORDS, "a record failed to encode");
        });
    });
}
