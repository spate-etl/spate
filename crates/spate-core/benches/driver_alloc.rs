//! Allocation snapshot of a pipeline thread's poll loop.
//!
//! Each case runs the same thread the counted tier in `driver_gungraun.rs`
//! measures, under the same case names, and prints its allocation,
//! reallocation and byte counts for `cargo xtask bench alloc` to compare with
//! `benches/alloc/`. Harness-free, so every case runs on the main thread and
//! nothing else allocates while one is counted.
//!
//! The thread is built before its case opens the window and freed after it
//! closes, so the counts are the loop's, plus the handling of the lane
//! assignments and the stop that the loop does itself. The loop reads the
//! system clock around every poll for its poll-duration histogram; no branch
//! takes the reading, so it cannot move a count.

use spate_test_support::alloc::{Counting, main_with};
use std::any::Any;

#[path = "support/driver_rig.rs"]
mod driver_rig;

#[global_allocator]
static ALLOC: Counting = Counting;

fn main() {
    main_with(|report| {
        for case in driver_rig::CASES {
            // `report.case` runs once uncounted, then once counted; each run
            // takes a thread built ahead and keeps what it returns.
            let mut runs = vec![case.build(), case.build()];
            let mut kept: Vec<Box<dyn Any + Send>> = Vec::with_capacity(runs.len());
            report.case(case.name, || {
                let run = runs.pop().expect("one run built per pass");
                let (counts, state) = run();
                assert_eq!(counts, case.expected(), "{}: counts moved", case.name);
                kept.push(state);
            });
        }
    });
}
