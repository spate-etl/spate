//! The counting allocator behind the allocation snapshots.
//!
//! A snapshot target is a harness-free `[[bench]]` binary named
//! `benches/<name>_alloc.rs` that installs [`Counting`] as its global
//! allocator and hands its cases to [`main_with`]. `cargo xtask bench alloc`
//! runs it in the release profile and compares what it prints with the
//! committed file.
//!
//! # What the figures count
//!
//! - `allocs` is each successful `alloc` and `alloc_zeroed`.
//! - `reallocs` is each successful `realloc`, whether it grows, keeps or
//!   shrinks the block.
//! - `bytes` is the requested size of each `alloc` and `alloc_zeroed`, plus
//!   `new_size - old_size` of each growing `realloc`.
//!
//! Freeing is not counted. Sizes are the requested `Layout` sizes, so the
//! platform `malloc` changes no figure. Only the Rust global allocator is seen:
//! what C code allocates through `malloc` directly, as libzstd does, is absent.
//!
//! Counting is gated by one process-wide flag. The binaries run every case on
//! the main thread with no harness threads, so nothing else allocates while a
//! window is open.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static WINDOW: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

/// A `System` allocator that counts what it hands out while a window is open.
///
/// Install it in a snapshot target with
/// `#[global_allocator] static ALLOC: Counting = Counting;`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Counting;

// SAFETY: every method forwards to `System`, a valid `GlobalAlloc`, with the
// layout and pointer arguments it was given. The counters are plain atomics
// touched only after a successful call, so they add no requirement of their
// own.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is forwarded unchanged, under the contract this
        // method is called with.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as `alloc`.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was allocated by this allocator, hence by `System`,
        // with `layout`.
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `ptr` and `layout` come from an allocation by this allocator
        // and `new_size` meets the caller's obligations, all forwarded
        // unchanged.
        let out = unsafe { System.realloc(ptr, layout, new_size) };
        if !out.is_null() && WINDOW.load(Ordering::Relaxed) {
            REALLOCS.fetch_add(1, Ordering::Relaxed);
            if new_size > layout.size() {
                BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            }
        }
        out
    }
}

fn record_alloc(size: usize) {
    if WINDOW.load(Ordering::Relaxed) {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}

/// What one case allocated in its counted pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    /// Successful `alloc` and `alloc_zeroed` calls.
    pub allocs: u64,
    /// Successful `realloc` calls, growing, equal or shrinking.
    pub reallocs: u64,
    /// Requested bytes, with a growing `realloc` charged its growth.
    pub bytes: u64,
}

impl Counts {
    fn read() -> Self {
        Self {
            allocs: ALLOCS.load(Ordering::Relaxed),
            reallocs: REALLOCS.load(Ordering::Relaxed),
            bytes: BYTES.load(Ordering::Relaxed),
        }
    }

    const fn since(self, earlier: Self) -> Self {
        Self {
            allocs: self.allocs - earlier.allocs,
            reallocs: self.reallocs - earlier.reallocs,
            bytes: self.bytes - earlier.bytes,
        }
    }
}

/// The process's global allocator is not [`Counting`], so a window counts
/// nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotInstalled;

/// Runs `f` once uncounted, then once counted, and returns what the second
/// pass allocated.
///
/// The first pass absorbs first-touch state such as lazily built scratch
/// buffers, so the figures are the steady state.
///
/// # Errors
///
/// [`NotInstalled`] when an allocation inside an open window leaves the
/// counters unmoved.
pub fn measure(mut f: impl FnMut()) -> Result<Counts, NotInstalled> {
    f();
    if !installed() {
        return Err(NotInstalled);
    }
    let before = Counts::read();
    WINDOW.store(true, Ordering::SeqCst);
    f();
    WINDOW.store(false, Ordering::SeqCst);
    Ok(Counts::read().since(before))
}

/// Whether an allocation in an open window moves the counters.
fn installed() -> bool {
    let before = Counts::read();
    WINDOW.store(true, Ordering::SeqCst);
    let probe: Vec<u8> = Vec::with_capacity(64);
    std::hint::black_box(&probe);
    WINDOW.store(false, Ordering::SeqCst);
    drop(probe);
    Counts::read().allocs > before.allocs
}

/// The header keys and case figures one snapshot target prints.
#[derive(Debug, Default)]
pub struct Report {
    keys: Vec<(String, String)>,
    rows: Vec<(String, Counts)>,
}

impl Report {
    /// Adds a `# key: value` header line, for a property of the build that
    /// moves the counts, such as a backend chosen at compile or run time.
    pub fn key(&mut self, key: &str, value: impl std::fmt::Display) {
        self.keys.push((key.to_owned(), value.to_string()));
    }

    /// Measures one case with [`measure`] and records it under `name`.
    ///
    /// # Panics
    ///
    /// Panics when [`Counting`] is not the global allocator.
    pub fn case(&mut self, name: &str, f: impl FnMut()) {
        let counts = measure(f).unwrap_or_else(|NotInstalled| {
            panic!(
                "the global allocator is not `Counting`; declare \
                 `#[global_allocator] static ALLOC: Counting = Counting;`"
            )
        });
        self.rows.push((name.to_owned(), counts));
    }

    /// The header lines, then a column line, then one tab-separated line per
    /// case, sorted by case name.
    fn render(&self) -> String {
        let mut rows: Vec<_> = self.rows.iter().collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out = String::new();
        for (k, v) in &self.keys {
            out.push_str(&format!("# {k}: {v}\n"));
        }
        out.push_str("case\tallocs\treallocs\tbytes\n");
        for (name, c) in rows {
            out.push_str(&format!(
                "{name}\t{}\t{}\t{}\n",
                c.allocs, c.reallocs, c.bytes
            ));
        }
        out
    }
}

/// The body of a snapshot target's `main`: collects its cases and prints the
/// report to stdout.
///
/// # Panics
///
/// Panics in a debug build, whose allocations differ from the release build
/// the committed files hold, when a case panics, and when stdout cannot be
/// written, as when its reader has closed the pipe.
pub fn main_with(f: impl FnOnce(&mut Report)) {
    if cfg!(debug_assertions) {
        panic!("allocation snapshots are taken in release; run `cargo xtask bench alloc`");
    }
    let mut report = Report::default();
    f(&mut report);
    std::io::stdout()
        .write_all(report.render().as_bytes())
        .expect("write the report to stdout");
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    use super::{Counting, Counts, NotInstalled, Report, WINDOW, main_with, measure};

    /// The window and counters are process-wide and libtest runs tests on
    /// several threads, so each test that opens a window holds this.
    static SERIAL: Mutex<()> = Mutex::new(());

    fn layout(size: usize) -> Layout {
        Layout::from_size_align(size, 8).expect("valid layout")
    }

    /// Each kind of call moves the figures its rule names, and nothing moves
    /// with the window closed. The unit-test binary does not install
    /// `Counting`, so the test is its direct caller.
    #[test]
    fn the_snapshot_allocator_counts_each_kind() {
        let _serial = SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let start = Counts::read();
        let at = |allocs, reallocs, bytes| Counts {
            allocs: start.allocs + allocs,
            reallocs: start.reallocs + reallocs,
            bytes: start.bytes + bytes,
        };

        // SAFETY: every layout has a non-zero size and a power-of-two
        // alignment, and each pointer is freed once with the layout it was
        // last allocated or reallocated under.
        unsafe {
            WINDOW.store(true, Ordering::SeqCst);
            let a = Counting.alloc(layout(100));
            assert_eq!(Counts::read(), at(1, 0, 100));
            let z = Counting.alloc_zeroed(layout(50));
            assert_eq!(Counts::read(), at(2, 0, 150));
            let grown = Counting.realloc(a, layout(100), 160);
            assert_eq!(Counts::read(), at(2, 1, 210));
            let same = Counting.realloc(grown, layout(160), 160);
            assert_eq!(Counts::read(), at(2, 2, 210));
            let shrunk = Counting.realloc(same, layout(160), 40);
            assert_eq!(Counts::read(), at(2, 3, 210));
            Counting.dealloc(shrunk, layout(40));
            Counting.dealloc(z, layout(50));
            assert_eq!(Counts::read(), at(2, 3, 210));

            WINDOW.store(false, Ordering::SeqCst);
            let closed = Counting.alloc(layout(100));
            let closed = Counting.realloc(closed, layout(100), 200);
            Counting.dealloc(closed, layout(200));
            assert_eq!(Counts::read(), at(2, 3, 210));
        }
    }

    /// The unit-test binary's global allocator is `System`, so the probe
    /// allocation counts nothing.
    #[test]
    fn measure_refuses_an_allocator_that_is_not_installed() {
        let _serial = SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut runs = 0;
        assert_eq!(measure(|| runs += 1), Err(NotInstalled));
        assert!(!WINDOW.load(Ordering::SeqCst));
        assert_eq!(runs, 1);
    }

    #[test]
    #[should_panic(expected = "the global allocator is not `Counting`")]
    fn a_case_refuses_an_allocator_that_is_not_installed() {
        let _serial = SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Report::default().case("a", || {});
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "allocation snapshots are taken in release")]
    fn main_with_refuses_a_debug_build() {
        main_with(|_| {});
    }

    #[test]
    fn the_report_sorts_cases_under_its_keys() {
        let mut report = Report::default();
        report.key("backend", "serde_json");
        report.rows.push((
            "b".to_owned(),
            Counts {
                allocs: 1,
                reallocs: 2,
                bytes: 3,
            },
        ));
        report.rows.push(("a".to_owned(), Counts::default()));
        assert_eq!(
            report.render(),
            "# backend: serde_json\ncase\tallocs\treallocs\tbytes\na\t0\t0\t0\nb\t1\t2\t3\n"
        );
    }
}
