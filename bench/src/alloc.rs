//! The counting global allocator.
//!
//! Allocation totals are the least noisy thing this harness measures: for most
//! code they are deterministic, which is why their significance floor is one
//! percent where wall time's is five. Getting them costs two relaxed
//! `fetch_add`s on the success path of every allocation, on a cache line the
//! allocating thread has to itself.
//!
//! # Why it is not behind a feature
//!
//! Gating it would mean the wall numbers and the allocation numbers come from
//! two different binaries, so a build that allocates more, and is therefore
//! slower, reports the extra allocations from one build and the timing from
//! another. The counter perturbs both legs identically, and the
//! comparison is between legs, so the cost is inside the baseline rather than
//! in the difference.
//!
//! # Why each thread counts on its own line
//!
//! Counters that every thread writes share one cache line, and a case that
//! allocates from several threads then measures that line moving between
//! cores, which depends on how often the threads allocate at the same moment.
//! So the counters are split into [`SHARDS`] padded shards, a thread adds to
//! its own, and a snapshot sums them all.
//!
//! A thread takes the next shard round robin at its first allocation and keeps
//! it until it exits. Shards are never handed back, so two threads share a line
//! when more than [`SHARDS`] allocating threads are alive at once, or when a
//! thread whose turn came after the count wrapped meets an older thread still
//! alive on the same shard. The totals stay exact either way; only the cost of
//! counting changes.
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
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::Padded;

/// How many counter shards there are.
pub const SHARDS: usize = 64;

/// One thread's share of the counters.
struct Shard {
    bytes: AtomicU64,
    count: AtomicU64,
}

static COUNTERS: [Padded<Shard>; SHARDS] = [const {
    Padded(Shard {
        bytes: AtomicU64::new(0),
        count: AtomicU64::new(0),
    })
}; SHARDS];

/// How many threads have taken a shard so far.
static TAKEN: AtomicUsize = AtomicUsize::new(0);

/// A slot no thread has taken yet.
const UNSET: usize = usize::MAX;

/// The type of [`SLOT`], named so the assertion below checks the real one.
type Slot = Cell<usize>;

thread_local! {
    /// The shard this thread adds to, or [`UNSET`] before its first allocation.
    static SLOT: Slot = const { Cell::new(UNSET) };
}

// The allocator reads `SLOT` on every call, so it must have no destructor to
// register: registering one can allocate, which would re-enter the allocator.
const _: () = assert!(!std::mem::needs_drop::<Slot>());

/// The calling thread's shard, taking the next one on its first call.
fn shard() -> &'static Shard {
    let index = SLOT
        .try_with(|slot| {
            if slot.get() == UNSET {
                slot.set(TAKEN.fetch_add(1, Ordering::Relaxed) % SHARDS);
            }
            slot.get()
        })
        // A thread whose locals are gone still allocates while it exits.
        .unwrap_or(0);
    &COUNTERS[index].0
}

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
// wraps. The shard index is a const-initialized thread-local with no
// destructor, so reading it does not call back into this allocator.
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
            // block shrank or stayed the same.
            record(new_size - layout.size());
        }
        out
    }
}

fn record(size: usize) {
    // Relaxed on both: the additions cannot be lost at any ordering. A
    // *snapshot* sees another thread's additions only through the case's own
    // synchronisation, which any case with a defined total already has; its
    // measured region cannot close until the threads it spans have reported.
    // `crates/spate-core/benches/support/ack_traffic.rs` is the worked example.
    let shard = shard();
    shard.bytes.fetch_add(size as u64, Ordering::Relaxed);
    shard.count.fetch_add(1, Ordering::Relaxed);
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

/// Sums both counters over every shard.
#[must_use]
pub fn snapshot() -> Snapshot {
    COUNTERS
        .iter()
        .fold(Snapshot { bytes: 0, count: 0 }, |sum, shard| Snapshot {
            bytes: sum.bytes + shard.0.bytes.load(Ordering::Relaxed),
            count: sum.count + shard.0.count.load(Ordering::Relaxed),
        })
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
    use std::sync::{Barrier, Mutex, MutexGuard, PoisonError};

    use super::{Counting, SHARDS, Snapshot, installed, shard, snapshot};

    /// Held by every test that calls `Counting` directly. Their assertions are
    /// exact deltas on process-wide counters, which `cargo test` shares between
    /// the tests of one binary.
    static DIRECT: Mutex<()> = Mutex::new(());

    fn direct() -> MutexGuard<'static, ()> {
        DIRECT.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The allocator itself, called directly. The unit tests run under a
    /// harness that does not install it, so the only way to exercise `Counting`
    /// is to be its caller, and without this the realloc rule below has no
    /// coverage anywhere.
    #[test]
    fn the_allocator_counts_what_it_hands_out() {
        let _direct = direct();
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
    /// counters do. Its behavior with the allocator in place is proved end to
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

    /// Threads alive at once count on different cache lines, and each keeps
    /// its line from one allocation to the next.
    #[test]
    fn concurrent_threads_keep_separate_padded_shards() {
        let _direct = direct();
        let threads = 4;
        let barrier = Barrier::new(threads);
        let lines: Vec<(usize, usize)> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    scope.spawn(|| {
                        let first = std::ptr::from_ref(shard()) as usize;
                        barrier.wait();
                        (first, std::ptr::from_ref(shard()) as usize)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("the thread finished"))
                .collect()
        });

        for &(first, second) in &lines {
            assert_eq!(first, second, "a thread moved to another shard");
            assert_eq!(first % 128, 0, "a shard is not on a line of its own");
        }
        let mut addresses: Vec<usize> = lines.iter().map(|&(first, _)| first).collect();
        addresses.sort_unstable();
        addresses.dedup();
        assert_eq!(addresses.len(), threads, "two live threads share a shard");
    }

    /// The totals stay exact when more threads have counted than there are
    /// shards, so some of them share one.
    #[test]
    fn totals_stay_exact_across_more_threads_than_shards() {
        let _direct = direct();
        let threads = SHARDS + 1;
        let calls = 100;
        let layout = Layout::from_size_align(48, 8).expect("valid layout");
        let before = snapshot();

        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| {
                    for _ in 0..calls {
                        // SAFETY: `layout` has a non-zero size and a
                        // power-of-two alignment, and the pointer is freed once
                        // with the layout it was allocated under.
                        unsafe {
                            let ptr = Counting.alloc(layout);
                            assert!(!ptr.is_null());
                            Counting.dealloc(ptr, layout);
                        }
                    }
                });
            }
        });

        let total = (threads * calls) as u64;
        assert_eq!(
            snapshot().since(before),
            Snapshot {
                bytes: total * 48,
                count: total,
            }
        );
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
