//! `measure` with `Counting` as the global allocator.
//!
//! One test in its own binary. nextest runs each test in a process of its
//! own, where the only other thread is the harness's, blocked on the test's
//! result, so the counted figures are the closure's alone.

use std::hint::black_box;

use spate_test_support::alloc::{Counting, Counts, measure};

#[global_allocator]
static ALLOC: Counting = Counting;

/// A known allocation pattern is counted exactly, after one uncounted pass,
/// and a second window starts from zero.
#[test]
fn measure_counts_the_counted_pass_only() {
    let mut runs = 0;
    let counts = measure(|| {
        runs += 1;
        let mut v: Vec<u8> = Vec::with_capacity(100);
        v.reserve_exact(250);
        black_box(&v);
    })
    .expect("`Counting` is this binary's global allocator");
    assert_eq!(runs, 2, "one uncounted pass, then the counted one");
    assert_eq!(
        counts,
        Counts {
            allocs: 1,
            reallocs: 1,
            bytes: 250,
        }
    );

    // A second window starts from zero, whatever earlier windows counted.
    let counts = measure(|| {
        black_box(Vec::<u8>::with_capacity(10));
    })
    .expect("`Counting` is this binary's global allocator");
    assert_eq!(
        counts,
        Counts {
            allocs: 1,
            reallocs: 0,
            bytes: 10,
        }
    );
}
