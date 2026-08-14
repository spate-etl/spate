//! The counting global allocator.
//!
//! Allocation totals are the least noisy thing this harness measures: for most
//! code they are deterministic, which is why their significance floor is one
//! percent where wall time's is five. Getting them costs two relaxed
//! `fetch_add`s on the success path of every allocation.
//!
//! # Why it is not behind a feature
//!
//! Gating it would mean the wall numbers and the allocation numbers come from
//! two different binaries, so a build that allocates more, and is therefore
//! slower, reports the extra allocations from one build and the timing from
//! another. It would also double the build cost of an A/B run, which is already
//! two full compilations. The counter perturbs both legs identically, and the
//! comparison is between legs, so the cost is inside the baseline rather than
//! in the difference.
//!
//! # How a `realloc` is charged
//!
//! A growing reallocation is counted as its *growth*, and a shrinking one is not
//! counted at all. So a doubling push-loop does not report quadratic bytes,
//! but a build that grows a buffer by repeated appends instead of reserving it
//! once reports roughly the same byte total as one that reserves.
//! `alloc_count_per_iter` is what catches that one, which is the reason both
//! figures are reported rather than only the bytes.
//!
//! # Why `installed` allocates rather than reading a flag
//!
//! A hand-written `fn main` that does not use [`crate::bench_main!`] gets no
//! allocator, and a flag set by the macro would then be the only thing saying
//! so. Watching the counter move across a real allocation asks the question
//! that matters, *is this process's allocator the one that counts*, and
//! cannot be answered wrongly by a macro that was never invoked. A case whose
//! allocator is absent reports no allocation metrics and says why in a note;
//! it never reports zero, which would compare as a real change.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

static BYTES: AtomicU64 = AtomicU64::new(0);
static COUNT: AtomicU64 = AtomicU64::new(0);

/// A `System` allocator that counts what it hands out.
///
/// Install it with [`crate::bench_main!`], which is the only supported way:
/// a process may have exactly one global allocator, and the macro is where
/// that decision belongs.
#[derive(Debug, Default, Clone, Copy)]
pub struct Counting;

// SAFETY: every method forwards to `System`, which is a valid `GlobalAlloc`,
// with the same layout and pointer arguments it was given. The counters are
// plain atomics and are touched only after a successful call, so they add no
// requirement of their own; an allocator that counts is still the allocator it
// wraps.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is forwarded unchanged to the system allocator,
        // which is the contract this method is being called under.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as `alloc`.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was allocated by this allocator, hence by `System`,
        // with `layout`, which is what `System::dealloc` requires.
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `ptr`/`layout` come from a previous allocation by this
        // allocator and `new_size` satisfies the caller's obligations, all of
        // which are forwarded unchanged.
        let out = unsafe { System.realloc(ptr, layout, new_size) };
        if !out.is_null() && new_size > layout.size() {
            // Counted as the *growth* only, and not counted at all when the
            // block shrank or stayed the same. A realloc that grows a `Vec` is
            // one allocation the program asked for; charging it the full new
            // size would make a doubling push-loop report quadratic bytes and
            // swamp every other figure in the case. And counting a shrink at
            // all, which a `record(0)` still does since the count is
            // unconditional, would report a head that added a per-iteration
            // `shrink_to_fit` as an allocation-count regression against a 1%
            // floor while it allocated nothing new.
            record(new_size - layout.size());
        }
        out
    }
}

fn record(size: usize) {
    // Relaxed on both. What is needed here is only that the additions do not
    // get lost, which `fetch_add` gives at any ordering; the ordering that
    // makes a *snapshot* see another thread's additions has to come from
    // somewhere else, and does.
    //
    // For a single-threaded case that somewhere is one thread doing both the
    // allocating and the reading. For a case whose measured region spans several
    // threads, the region cannot close until those threads have reported
    // through whatever synchronisation the case uses to bound its own work,
    // and that release/acquire pair carries every allocation before it. A case
    // that let a thread keep allocating past the end of its region would have
    // no defined allocation total to report at any ordering, so this is a
    // property such a case must have regardless.
    // `crates/spate-core/benches/support/ack_traffic.rs` is the worked example:
    // its workers allocate inside the region and are joined only when the rig
    // drops, long after the snapshot, and its gate is what closes the gap.
    BYTES.fetch_add(size as u64, Ordering::Relaxed);
    COUNT.fetch_add(1, Ordering::Relaxed);
}

/// The counters at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    /// Bytes handed out since the process started.
    pub bytes: u64,
    /// Allocations made since the process started.
    pub count: u64,
}

impl Snapshot {
    /// What was allocated between `self` and a later snapshot.
    ///
    /// Saturating rather than wrapping: an underflow would mean the later
    /// snapshot was taken first, and reporting a huge number is worse than
    /// reporting none of the growth.
    #[must_use]
    pub const fn since(self, earlier: Self) -> Self {
        Self {
            bytes: self.bytes.saturating_sub(earlier.bytes),
            count: self.count.saturating_sub(earlier.count),
        }
    }
}

/// Reads both counters.
#[must_use]
pub fn snapshot() -> Snapshot {
    Snapshot {
        bytes: BYTES.load(Ordering::Relaxed),
        count: COUNT.load(Ordering::Relaxed),
    }
}

/// Whether this process's global allocator is the counting one.
///
/// Answered by allocating and watching the counter, not by a flag; see the
/// module documentation. There is no false positive to worry about: if another
/// thread's allocation is what moved the counter, the counting allocator is
/// installed either way.
#[must_use]
pub fn installed() -> bool {
    let before = snapshot();
    // `with_capacity` rather than a literal, so nothing can constant-fold the
    // allocation away, and `black_box` so the vector is observably used.
    let probe: Vec<u8> = Vec::with_capacity(64);
    std::hint::black_box(&probe);
    let after = snapshot();
    drop(probe);
    after.count > before.count
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout};

    use super::{Counting, installed, snapshot};

    /// The allocator itself, called directly. The unit tests run under a
    /// harness that does not install it, so the only way to exercise `Counting`
    /// is to be its caller, and without this the realloc rule below has no
    /// coverage anywhere.
    #[test]
    fn the_allocator_counts_what_it_hands_out() {
        let layout = Layout::from_size_align(4096, 8).expect("valid layout");
        let before = snapshot();

        // SAFETY: `layout` has a non-zero size and a power-of-two alignment,
        // and every pointer below is freed exactly once with the layout it was
        // last allocated or reallocated under.
        unsafe {
            let ptr = Counting.alloc(layout);
            assert!(!ptr.is_null());
            let after_alloc = snapshot();
            assert_eq!(after_alloc.count, before.count + 1);
            assert_eq!(after_alloc.bytes, before.bytes + 4096);

            // Growth is charged as the growth, not as the new size: a doubling
            // push-loop would otherwise report quadratic bytes.
            let grown = Counting.realloc(ptr, layout, 8192);
            assert!(!grown.is_null());
            let after_grow = snapshot();
            assert_eq!(after_grow.count, after_alloc.count + 1);
            assert_eq!(after_grow.bytes, after_alloc.bytes + 4096);

            // A shrink is not an allocation at all. Counting it would report a
            // head that added a per-iteration `shrink_to_fit` as a 100%
            // allocation-count regression against a 1% floor.
            let grown_layout = Layout::from_size_align(8192, 8).expect("valid layout");
            let shrunk = Counting.realloc(grown, grown_layout, 2048);
            assert!(!shrunk.is_null());
            assert_eq!(snapshot(), after_grow);

            Counting.dealloc(
                shrunk,
                Layout::from_size_align(2048, 8).expect("valid layout"),
            );
        }

        // Freeing is not counted either. These are totals handed out, not a
        // live figure.
        assert!(snapshot().count > before.count);
    }

    /// The unit tests run under this crate's own `bench_main!`-free harness, so
    /// the counting allocator is *not* the global one here, so `installed()` is
    /// therefore asserted only for self-consistency with what the global
    /// counters do. Its behaviour with the allocator in place is proved end to
    /// end by the self-test bench, whose records carry non-zero allocation
    /// metrics.
    #[test]
    fn the_probe_agrees_with_the_global_counters() {
        let counting = installed();
        let before = snapshot();
        let ballast: Vec<u8> = Vec::with_capacity(4096);
        std::hint::black_box(&ballast);
        let after = snapshot();
        drop(ballast);

        if counting {
            assert!(
                after.count > before.count && after.bytes > before.bytes,
                "the allocator reports itself installed but the counters did not move"
            );
        }
    }

    #[test]
    fn a_delta_never_underflows() {
        let big = super::Snapshot {
            bytes: 10,
            count: 2,
        };
        let small = super::Snapshot {
            bytes: 100,
            count: 20,
        };
        assert_eq!(big.since(small), super::Snapshot { bytes: 0, count: 0 });
    }
}
