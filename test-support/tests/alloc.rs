//! `measure` with `Counting` as the global allocator.
//!
//! Its own binary, because the global allocator is the binary's.

use std::hint::black_box;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::time::{Duration, Instant};

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

/// How long a wait in this file lasts before it fails as a hang.
const HANG: Duration = Duration::from_secs(60);

/// Spin until `reached` holds. It allocates nothing, so it can run inside a
/// counted pass.
fn spin_until(reached: impl Fn() -> bool) {
    let start = Instant::now();
    while !reached() {
        assert!(start.elapsed() < HANG, "hung waiting on the other thread");
        std::thread::yield_now();
    }
}

/// A window counts the thread that opened it: what another thread allocates
/// while it is open moves no figure. Regression for #1861.
#[test]
fn another_threads_allocations_are_not_counted() {
    // `asked` and `made` count the allocations the measuring thread has asked
    // for and the other thread has made.
    let (asked, made, stop) = (AtomicU32::new(0), AtomicU32::new(0), AtomicBool::new(false));
    let counts = std::thread::scope(|s| {
        s.spawn(|| {
            let mut seen = 0;
            loop {
                spin_until(|| asked.load(SeqCst) > seen || stop.load(SeqCst));
                if stop.load(SeqCst) {
                    return;
                }
                seen += 1;
                black_box(Vec::<u8>::with_capacity(4096));
                made.store(seen, SeqCst);
            }
        });
        // One allocation per pass of `measure`, each made while the measuring
        // thread waits inside its closure.
        let counts = measure(|| {
            let want = asked.fetch_add(1, SeqCst) + 1;
            spin_until(|| made.load(SeqCst) == want);
        });
        // Before the result is read, so a failure ends the other thread and
        // the scope, and is reported.
        stop.store(true, SeqCst);
        counts
    });
    assert_eq!(made.load(SeqCst), 2, "one allocation per pass");
    assert_eq!(
        counts.expect("`Counting` is this binary's global allocator"),
        Counts::default()
    );
}
