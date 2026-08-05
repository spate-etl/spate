//! The wall-clock A/B benchmark harness.
//!
//! **Not published.** This crate exists inside the Spate repository to measure
//! it; it is `publish = false` and is not installable from crates.io.
//!
//! Benchmarks here answer one question — *did this change move it* — by
//! measuring two builds against each other in one sitting. Nothing is stored:
//! a comparison is produced, read, and thrown away, so there is no archive to
//! keep honest and no threshold learned from history.
//!
//! # The two halves
//!
//! **The author surface** (default features) is what a `benches/*_wall.rs`
//! target uses: [`suite`] to declare cases, [`Bencher`] to mark the measured
//! region, [`Corpus`] to prove both legs saw the same bytes, and
//! [`bench_main!`] to install the counting allocator and the runner protocol.
//!
//! **The driver** (`driver` feature) is the `bench` CLI: it discovers targets
//! through `cargo metadata`, builds both legs, drives each binary over the
//! [`protocol`], and renders a [`compare::Comparison`].
//!
//! The split is load-bearing. A crate's bench target must not drag clap, git
//! worktrees and the comparator into `cargo bench --no-run`, so no bench target
//! ever enables `driver`. A workspace-wide `--all-features` build does — that
//! is what `--all-features` means — and it is why the CLI is still linted and
//! compile-checked on every pull request.
//!
//! # A minimal target
//!
//! ```no_run
//! use spate_bench::{Suite, bench_main};
//!
//! fn suite() -> Suite {
//!     spate_bench::suite("spate-bench")
//!         .case(
//!             "sum_1k",
//!             |corpus, seed| {
//!                 let mut rng = spate_bench::rng::SplitMix64::new(seed);
//!                 let data: Vec<u64> = (0..1024).map(|_| rng.next_u64()).collect();
//!                 corpus.absorb("data", &data.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>());
//!                 data
//!             },
//!             |b, data| b.iter(|| data.iter().fold(0u64, |a, v| a.wrapping_add(*v))),
//!         )
//!         .items(1024)
//!         .done()
//! }
//!
//! bench_main!(suite);
//! ```
//!
//! `docs/user-guide/07-reference/benchmarking.mdx` is the prose version, and
//! `benches/selftest_wall.rs` is a worked example that this crate's own
//! acceptance run drives.

pub mod alloc;
pub mod case;
pub mod clock;
pub mod corpus;
pub mod fingerprint;
pub mod protocol;
pub mod record;
pub mod rng;
pub mod rusage;

#[cfg(feature = "driver")]
pub mod cargo;
#[cfg(feature = "driver")]
pub mod compare;
#[cfg(feature = "driver")]
pub mod render;
#[cfg(feature = "driver")]
pub mod runner;
#[cfg(feature = "driver")]
pub mod stats;
#[cfg(feature = "driver")]
pub mod worktree;
// `ab` sits last because it is the only module that composes all of the others.
#[cfg(feature = "driver")]
pub mod ab;

pub use case::{Bencher, Case, CaseBuilder, Suite, suite};
pub use corpus::Corpus;
