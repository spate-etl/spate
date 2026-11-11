//! `measure` with `Counting` as the global allocator.
//!
//! Its own binary, because the global allocator is the binary's.

use std::hint::black_box;
use std::sync::Barrier;

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

/// A window counts the thread that opened it: what another thread allocates
/// while it is open moves no figure. Regression for #1861.
#[test]
fn another_threads_allocations_are_not_counted() {
    let (go, done) = (Barrier::new(2), Barrier::new(2));
    std::thread::scope(|s| {
        // One allocation per pass of `measure`, each made while the measuring
        // thread waits inside its closure.
        s.spawn(|| {
            for _ in 0..2 {
                go.wait();
                black_box(Vec::<u8>::with_capacity(4096));
                done.wait();
            }
        });
        let counts = measure(|| {
            go.wait();
            done.wait();
        })
        .expect("`Counting` is this binary's global allocator");
        assert_eq!(counts, Counts::default());
    });
}
