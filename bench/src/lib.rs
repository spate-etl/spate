//! The wall-clock A/B benchmark harness.
//!
//! **Not published.** This crate exists inside the Spate repository to measure
//! it; it is `publish = false` and is not installable from crates.io.
//!
//! Benchmarks here answer one question — *did this change move it* — by
//! measuring two builds against each other in one sitting, on one machine.
//! Nothing is stored: a comparison is produced, read, and thrown away, so there
//! is no archive to keep honest and no threshold learned from history. That is
//! a deliberate limit rather than a gap. A stored figure is only comparable
//! with a later one when the machine, the toolchain and the corpus were all the
//! same, and none of those hold across time; two builds measured minutes apart
//! need none of it.
//!
//! Wall time gates nothing. No pull request passes or fails on a number from
//! this tier, and no job runs it. The instruction-count tier counts rather than
//! times, which is what makes it comparable across the shared machines CI uses.
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
//! # The smallest complete target
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
//! That is the whole of the minimum. `benches/selftest_wall.rs` carries every
//! shape the builder supports — an allocating workload, the same workload
//! through `iter_batched`, a non-allocating one and an erratic one — and is
//! also what the A/A acceptance run drives: `make bench-ab REF=HEAD REPS=6`
//! compares it against itself and must flag nothing, because a flag there means
//! the harness is measuring its own noise rather than the code under test.
//!
//! # Declaring a target
//!
//! A wall-clock target is a bench target whose name ends in `_wall`, in any
//! workspace member — conventionally `crates/<pkg>/benches/<name>_wall.rs`. The
//! suffix is what keeps the tiers apart: `_gungraun` belongs to the
//! instruction-count tier, and neither discovery can see the other's targets.
//! The name has to be a valid Rust identifier, because the record carries the
//! crate name cargo compiles the target under and a hyphen becomes an
//! underscore there.
//!
//! The package needs a dev-dependency on this crate and a `[[bench]]` stanza
//! with `harness = false`. The dev-dependency is versionless, so cargo strips it
//! when the crate is packaged and an unpublished harness never appears in what a
//! consumer resolves. `harness = false` is required: without it cargo runs the
//! target under libtest, which rejects the runner protocol's arguments before
//! the target's own `main` is reached. The driver detects that and says so, but
//! only once both legs have been built — so `make ci-lint` checks the manifest
//! first and names the file.
//!
//! The name passed to [`suite`] must be the package cargo compiles the target
//! in. The target refuses to start otherwise, rather than emitting records that
//! intersect with nothing on the other leg.
//!
//! # The rules a case must satisfy
//!
//! - **Build the input in setup, not in the routine.** Only the routine is
//!   measured, so neither the time nor the allocations of building a corpus are
//!   attributed to the code under test.
//! - **Return the routine's result.** It is what reaches `black_box`. A routine
//!   written `|| { let _ = f(x); }` hands it `()`, and the call can be optimised
//!   away. A case whose per-iteration cost does not clear twice an empty loop's
//!   is refused rather than reported, but the fix is to return the value.
//! - **Absorb everything built into the [`Corpus`].** That digest is what proves
//!   both legs saw the same bytes; a case that absorbs nothing compares equal to
//!   anything, including a case that changed.
//! - **Build byte-identical input on both legs.** They are two checkouts that
//!   may straddle a dependency bump, and a generator whose output stream changed
//!   between them makes every number incomparable while looking healthy.
//!   [`rng::SplitMix64`] is the answer where a corpus wants pseudo-random bytes
//!   — a few lines held to a known-answer test, so it cannot drift the way a
//!   general-purpose generator's does. A corpus that is a pure function of the
//!   record index satisfies the rule at least as strongly, and ignores the seed
//!   argument entirely. What the rule rules out is anything environmental: a
//!   hash map's iteration order, a clock, a file on disk.
//! - **Declare `.items()` or `.bytes()` where an iteration covers a known
//!   amount.** That is what turns a duration into a throughput.
//!
//! `.iters(n)` pins the iteration count for a case that cannot be calibrated
//! meaningfully — `iter_batched` builds one input per iteration, so a case with
//! a large input should pin rather than let calibration choose. `.erratic(why)`
//! marks a case whose numbers are decided by something other than the code; it
//! is measured and reported like any other and can never reach the
//! significant-changes table.
//!
//! [`bench_main!`] installs the counting allocator and the runner protocol. A
//! target with a hand-written `main` still runs, and reports absent allocation
//! metrics with a note saying why — the harness detects the allocator by
//! watching it rather than by trusting a flag.
//!
//! # The traps
//!
//! Four failures that produce a plausible number rather than an error, which is
//! what makes them worth stating:
//!
//! - **A rig driven through `&mut self` has to be held in a `RefCell`.** A
//!   routine receives its state by shared reference.
//! - **A fold's parameters have to stay opaque.** A parameter written as a
//!   literal is a constant the optimiser sees through, so a routine taking a
//!   shard count that way has its division folded into a mask and reports a
//!   figure several times cheaper than the code it stands in for. `black_box`
//!   around the parameter holds the routine to the shape production runs.
//! - **Whatever a routine mutates has to be put back.** A routine runs thousands
//!   of times against one piece of state, so a rig that moves a budget or a
//!   watermark by relative amounts leaves each drive starting where the last one
//!   finished; within a few, the case exercises a different branch of the state
//!   machine than its name claims — and returns a stable number throughout. A
//!   rig borrowed from the instruction-count tier is where this bites, because
//!   that tier builds one and drives it once. A case whose subject keeps state
//!   is worth driving twice in a test before it is worth measuring.
//! - **A feature-selected subject has to be absorbed into the corpus.** The
//!   guarded `features` field records what was passed to cargo, which is a
//!   different question: a change moving a feature into the default set leaves
//!   both legs agreeing on it while the compiled code diverges. A crate that
//!   selects a backend that way exposes the compiled backend's identity as a
//!   constant and folds it into every case, so the two tripwires stay
//!   independent.
//!
//! Where a crate wraps a third-party library, that library's own floor belongs
//! beside the crate's path over the same bytes, so a regression in the
//! dependency is not read as one in the framework. A floor set that covers the
//! entry points unevenly is worse than none: a case read against the *other*
//! parser's floor reports the framework as faster than the library it calls,
//! which is a well-formed number saying something impossible.
//!
//! # Where the rest is
//!
//! Each rule sits with the code that enforces it.
//!
//! - [`case`] — the metrics and when each is present, the measured region, and
//!   what the driver decides rather than the author.
//! - [`record`] — why an absent metric is absent rather than zero, and why a leg
//!   needs no index or manifest.
//! - [`alloc`] — the counting allocator, and how a `realloc` is charged.
//! - [`rusage`] — the resident-set validity gate, and the limits of the kernel's
//!   CPU accounting.
//! - [`fingerprint`] — the guarded fields, and what makes two legs the same
//!   build.
//! - `stats` — the decision rule, the per-metric floors, and the bootstrap.
//! - `compare` — pairing two legs, and the refusals that stop a well-formed
//!   table being drawn off records that should never have met.
//! - `ab` — the order a run happens in, and why each step is where it is.
//!
//! # What this tier does not do
//!
//! **No latency percentiles.** They need rate control and a correction for
//! coordinated omission, and a percentile taken without either describes the
//! harness rather than the code.
//!
//! **No absolute claims.** Every number is one machine on one afternoon. A
//! figure only reaches the documentation carrying how it was established.

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
