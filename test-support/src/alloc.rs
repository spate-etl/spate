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
//! A window belongs to the thread that opens it, and counts what that thread
//! allocates. Another thread allocating meanwhile, such as a test harness's,
//! moves no figure.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::Write as _;

thread_local! {
    /// Whether this thread's window is open.
    static WINDOW: Cell<bool> = const { Cell::new(false) };
    /// What this thread's windows have counted.
    static COUNTED: Cell<Counts> = const { Cell::new(Counts::ZERO) };
}

/// Adds to this thread's figures while its window is open.
fn record(allocs: u64, reallocs: u64, bytes: u64) {
    // A thread allocates and frees after its locals are gone, and the
    // allocator must not panic there.
    let _ = WINDOW.try_with(|window| {
        if window.get() {
            let _ = COUNTED.try_with(|counted| {
                let c = counted.get();
                counted.set(Counts {
                    allocs: c.allocs + allocs,
                    reallocs: c.reallocs + reallocs,
                    bytes: c.bytes + bytes,
                });
            });
        }
    });
}

fn set_window(open: bool) {
    WINDOW.with(|window| window.set(open));
}

/// A `System` allocator that counts what it hands out while a window is open.
///
/// Install it in a snapshot target with
/// `#[global_allocator] static ALLOC: Counting = Counting;`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Counting;

// SAFETY: every method forwards to `System`, a valid `GlobalAlloc`, with the
// layout and pointer arguments it was given. The counters are const-initialized
// thread-local cells with no destructor, so reading them allocates nothing,
// and they are touched only after a successful call.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is forwarded unchanged, under the contract this
        // method is called with.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(1, 0, layout.size() as u64);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as `alloc`.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(1, 0, layout.size() as u64);
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
        if !out.is_null() {
            record(0, 1, new_size.saturating_sub(layout.size()) as u64);
        }
        out
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
    const ZERO: Self = Self {
        allocs: 0,
        reallocs: 0,
        bytes: 0,
    };

    /// What the calling thread's windows have counted so far.
    fn read() -> Self {
        COUNTED.with(Cell::get)
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
/// pass allocated on the calling thread.
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
    set_window(true);
    f();
    set_window(false);
    Ok(Counts::read().since(before))
}

/// Whether an allocation in an open window moves the counters.
fn installed() -> bool {
    let before = Counts::read();
    set_window(true);
    let probe: Vec<u8> = Vec::with_capacity(64);
    std::hint::black_box(&probe);
    set_window(false);
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

    use super::{Counting, Counts, NotInstalled, Report, WINDOW, main_with, measure, set_window};

    fn layout(size: usize) -> Layout {
        Layout::from_size_align(size, 8).expect("valid layout")
    }

    /// Each kind of call moves the figures its rule names, and nothing moves
    /// with the window closed. The unit-test binary does not install
    /// `Counting`, so the test is its direct caller.
    #[test]
    fn the_snapshot_allocator_counts_each_kind() {
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
            set_window(true);
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

            set_window(false);
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
        let mut runs = 0;
        assert_eq!(measure(|| runs += 1), Err(NotInstalled));
        assert!(!WINDOW.with(std::cell::Cell::get));
        assert_eq!(runs, 1);
    }

    #[test]
    #[should_panic(expected = "the global allocator is not `Counting`")]
    fn a_case_refuses_an_allocator_that_is_not_installed() {
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
