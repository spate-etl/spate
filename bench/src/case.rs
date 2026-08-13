//! The author surface: declaring cases, and the loop that measures one.
//!
//! A bench target declares a [`Suite`] of cases. Each case has a *setup* that
//! builds its input from a seed and absorbs it into a [`Corpus`], and a
//! *routine* that exercises the code under test. The split is what makes the
//! measurement mean anything: setup runs outside the measured region, so
//! building a corpus never lands in the timing, and its allocations never land
//! in the allocation totals.
//!
//! ```
//! # use spate_bench::Suite;
//! fn suite() -> Suite {
//!     spate_bench::suite("spate-bench")
//!         .case(
//!             "reverse_1k",
//!             |corpus, seed| {
//!                 let mut rng = spate_bench::rng::SplitMix64::new(seed);
//!                 let text = rng.ascii(1024);
//!                 corpus.absorb("text", &text);
//!                 text
//!             },
//!             |b, text| b.iter(|| text.iter().rev().copied().collect::<Vec<u8>>()),
//!         )
//!         .bytes(1024)
//!         .done()
//! }
//! # assert_eq!(suite().cases().len(), 1);
//! ```
//!
//! # What the driver decides, and why
//!
//! The iteration count is **not** chosen here. The driver calibrates it once,
//! on the base leg, and pins it for both — a self-calibrating leg would make
//! the resident-set and allocation totals incomparable between the legs, and
//! would let a slowdown hide as fewer iterations rather than showing up as a
//! longer one. `.iters(n)` pins it for a case that cannot be calibrated
//! meaningfully; everything else takes the driver's number.
//!
//! The seed is the driver's too, and it seeds the *corpus only*. Replicates
//! deliberately share it: a per-replicate corpus would inject variance into the
//! very quantity the paired bootstrap exists to resolve.
//!
//! # What comes out
//!
//! | Metric | Unit | Better | Present |
//! |---|---|---|---|
//! | `wall_ns_per_iter` | ns | lower | always |
//! | `cpu_ns_per_iter` | ns | lower | when the kernel reports the process's CPU accounting |
//! | `records_per_s` | records/s | higher | when the case declared how many items an iteration covers |
//! | `bytes_per_s` | bytes/s | higher | when the case declared how many bytes an iteration covers |
//! | `peak_rss_bytes` | bytes | lower | when running the case took the process above what building its corpus left resident, and the case did not batch its inputs |
//! | `alloc_bytes_per_iter` | bytes | lower | when the counting allocator is installed |
//! | `alloc_count_per_iter` | allocations | lower | when the counting allocator is installed |
//!
//! Only wall time is unconditional, and an absent metric is absent rather than
//! zero — see [`crate::record`] for why that distinction is load-bearing.
//!
//! A case using `iter_batched` reports no resident set at all. The harness holds
//! one prebuilt input per iteration, so the process's high-water mark would be
//! about the batching rather than about the routine, and a figure that is mostly
//! harness is worse than none.
//!
//! # The refusal
//!
//! A case whose per-iteration cost does not clear twice an empty loop's is
//! refused rather than reported. That is the failure this surface has to be
//! built against: a routine whose result is discarded gives `black_box` nothing
//! to hold, the optimiser deletes the call, and what comes back is a well-formed
//! record of the loop. The routine returning its result is what prevents it.

use std::collections::BTreeMap;
use std::rc::Rc;

use crate::alloc;
use crate::clock::Stopwatch;
use crate::corpus::Corpus;
use crate::record::{
    ALLOC_BYTES_PER_ITER, ALLOC_COUNT_PER_ITER, BYTES_PER_S, CPU_NS_PER_ITER, Metric,
    PEAK_RSS_BYTES, RECORDS_PER_S, WALL_NS_PER_ITER,
};
use crate::rusage::PeakWatch;

/// The largest iteration count calibration will settle on.
///
/// A ceiling rather than a target: [`Bencher::iter_batched`] pre-builds one
/// input per iteration, so an unbounded count is an unbounded allocation. Ten
/// million iterations of a one-nanosecond routine is already a ten-millisecond
/// region, which is more signal than the noise floor of a laptop can use.
pub const MAX_ITERS: u64 = 10_000_000;

/// How far above an empty loop a case has to be before its number means
/// anything.
///
/// Twice, which is deliberately generous. A case at the empty loop's own cost
/// is measuring the loop; a case at twice it is measuring the loop and a little
/// else, and neither is a benchmark.
const DEGENERATE_FACTOR: f64 = 2.0;

/// How many reference passes the floor is taken as the minimum of.
///
/// A stall during the reference loop inflates the floor and could refuse a
/// legitimate case — and the refusal aborts the whole run, since a leg with a
/// missing replicate is not a leg. Taking the smallest of three passes makes
/// that need three consecutive stalls.
const DEGENERATE_PASSES: u32 = 3;

/// How much longer the floor probe runs each time the clock cannot resolve it.
///
/// Eight, so a count the clock misses by a little clears it in one step and one
/// it misses by a lot still arrives in a handful — the whole search costs three
/// empty loops per step, and only on the path that would otherwise refuse the
/// case outright.
const DEGENERATE_PROBE_GROWTH: u64 = 8;

/// The longest empty loop the floor probe will time.
///
/// About four million iterations, a few milliseconds. Past that the probe would
/// cost more than the region it guards, and a clock that cannot resolve it is
/// not one this tier can measure against.
const DEGENERATE_PROBE_MAX: u64 = 1 << 22;

/// Nanoseconds per iteration of a loop that does nothing but keep its counter.
///
/// The guard that catches a bench reporting a plausible number while measuring
/// nothing — the same failure the instruction-count tier has
/// `scripts/gungraun-collected-region.sh` for.
///
/// It is reachable by accident. `Bencher::iter` passes the routine's *return
/// value* to `black_box`, so a routine written `|| { let _ = decode(&input); }`
/// hands it `()`, which constrains nothing: the optimiser is then free to
/// delete the call, and the case reports a per-iteration cost at the empty
/// loop's floor with a valid corpus digest and no complaint. Measured on a
/// scratch target, the difference between the two forms was 232 ns and
/// 0.24 ns.
///
/// Measured here rather than assumed, and at the case's own iteration count
/// where the clock can resolve one, so it costs what the case's own loop
/// overhead costs and moves with the machine.
///
/// # When the count is too small to time
///
/// A case whose routine is expensive calibrates to very few iterations — one
/// costing a millisecond and a half lands on about thirty at the default
/// `--target-ms`. An empty loop of thirty iterations takes tens of nanoseconds,
/// against a clock whose tick is also tens, so every pass can read zero and the
/// floor is unmeasurable through no fault of the case. That refuses a
/// legitimate case, and refusing one aborts the whole run.
///
/// So the probe grows the loop, by [`DEGENERATE_PROBE_GROWTH`] a step up to
/// [`DEGENERATE_PROBE_MAX`] iterations, until the clock resolves it, and divides
/// by what it actually ran. A per-iteration cost is what the floor is either
/// way; the multiple only buys enough elapsed time to see it.
///
/// The growth never applies to a count the clock already resolves, so no case
/// that passes this guard today sees a different floor because of it.
fn empty_loop_ns_per_iter(iters: u64) -> Option<f64> {
    let mut count = iters.max(1);
    loop {
        // Timed with the same stopwatch as the case, so the clock's own
        // overhead sits in both numerators and cancels. That is what lets the
        // guard hold at a pinned `.iters(64)` as well as at ten million — the
        // count a case is pinned to is exactly where an author is most likely
        // to be measuring something expensive, and where a threshold would have
        // switched the guard off.
        let mut floor: Option<f64> = None;
        for _ in 0..DEGENERATE_PASSES {
            let clock = Stopwatch::start();
            for i in 0..count {
                std::hint::black_box(i);
            }
            let elapsed = clock.elapsed_ns();
            // A pass the clock read as zero is not a floor of zero — it is the
            // clock declining to resolve the loop, and taking it as a minimum
            // would set the floor to nothing and switch the guard off for the
            // one case it exists to catch.
            if elapsed > 0 {
                let per_iter = elapsed as f64 / count as f64;
                floor = Some(floor.map_or(per_iter, |best: f64| best.min(per_iter)));
            }
        }
        if floor.is_some() {
            return floor;
        }
        // Every pass read zero. Grow, unless there is nowhere left to grow to —
        // a clock that cannot resolve four million empty iterations is not a
        // clock this tier can measure against at all, and saying so is better
        // than returning a floor nothing established.
        if count >= DEGENERATE_PROBE_MAX {
            return None;
        }
        count = count
            .saturating_mul(DEGENERATE_PROBE_GROWTH)
            .min(DEGENERATE_PROBE_MAX);
    }
}

/// Starts a suite for `krate`.
///
/// The name must be the package name cargo knows, because the driver intersects
/// the two legs' case lists by it. [`crate::protocol`] checks it against
/// `CARGO_PKG_NAME` at start-up rather than letting a typo become a leg that
/// pairs with nothing.
#[must_use]
pub fn suite(krate: &str) -> Suite {
    Suite {
        krate: krate.to_owned(),
        cases: Vec::new(),
    }
}

/// A bench target's cases.
#[derive(Debug)]
pub struct Suite {
    krate: String,
    cases: Vec<Case>,
}

impl Suite {
    /// Adds a case.
    ///
    /// `setup` builds the case's input from the driver's seed and absorbs
    /// everything it built into the [`Corpus`]; `routine` marks the measured
    /// region with [`Bencher::iter`] or [`Bencher::iter_batched`].
    #[must_use]
    pub fn case<S: 'static>(
        self,
        id: &str,
        setup: impl Fn(&mut Corpus, u64) -> S + 'static,
        routine: impl Fn(&mut Bencher, &S) + 'static,
    ) -> CaseBuilder<S> {
        CaseBuilder {
            suite: self,
            id: id.to_owned(),
            setup: Box::new(setup),
            routine: Rc::new(routine),
            items: None,
            bytes: None,
            iters: None,
            erratic: None,
        }
    }

    /// The package these cases belong to.
    #[must_use]
    pub fn krate(&self) -> &str {
        &self.krate
    }

    /// Every case, in declaration order.
    #[must_use]
    pub fn cases(&self) -> &[Case] {
        &self.cases
    }

    /// The case with this id.
    #[must_use]
    pub fn find(&self, id: &str) -> Option<&Case> {
        self.cases.iter().find(|c| c.id == id)
    }
}

/// A case's setup: seed and corpus in, the routine's state out.
type Setup<S> = Box<dyn Fn(&mut Corpus, u64) -> S>;
/// A case's routine. Shared rather than owned, because `done` moves it into a
/// closure the suite keeps while the builder is consumed.
type Routine<S> = Rc<dyn Fn(&mut Bencher, &S)>;
/// How many items or bytes one iteration covers, read off the setup's state.
type Extent<S> = Option<Box<dyn Fn(&S) -> u64>>;
/// A case with its state type erased: seed and corpus in, a ready routine out.
type Prepare = Box<dyn Fn(u64, &mut Corpus) -> Prepared>;

/// A case under construction.
///
/// Finish it with [`CaseBuilder::done`], which returns the suite so cases
/// chain.
pub struct CaseBuilder<S> {
    suite: Suite,
    id: String,
    setup: Setup<S>,
    routine: Routine<S>,
    items: Extent<S>,
    bytes: Extent<S>,
    iters: Option<u64>,
    erratic: Option<String>,
}

impl<S> std::fmt::Debug for CaseBuilder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaseBuilder")
            .field("id", &self.id)
            .field("iters", &self.iters)
            .field("erratic", &self.erratic)
            .finish_non_exhaustive()
    }
}

impl<S: 'static> CaseBuilder<S> {
    /// How many items one iteration of the routine processes.
    ///
    /// Turns the wall time into a `records_per_s` metric as well.
    #[must_use]
    pub fn items(self, items: u64) -> Self {
        self.items_of(move |_| items)
    }

    /// How many items one iteration processes, computed from the setup's state.
    #[must_use]
    pub fn items_of(mut self, items: impl Fn(&S) -> u64 + 'static) -> Self {
        self.items = Some(Box::new(items));
        self
    }

    /// How many bytes one iteration of the routine processes.
    #[must_use]
    pub fn bytes(self, bytes: u64) -> Self {
        self.bytes_of(move |_| bytes)
    }

    /// How many bytes one iteration processes, computed from the setup's state.
    #[must_use]
    pub fn bytes_of(mut self, bytes: impl Fn(&S) -> u64 + 'static) -> Self {
        self.bytes = Some(Box::new(bytes));
        self
    }

    /// Pins the iteration count, skipping calibration.
    ///
    /// For a case whose cost per iteration is not the thing being measured — a
    /// routine that builds internal state on the first call, or one whose
    /// memory grows with the count.
    #[must_use]
    pub fn iters(mut self, iters: u64) -> Self {
        self.iters = Some(iters.max(1));
        self
    }

    /// Marks the case as known-noisy, with the reason.
    ///
    /// An erratic case is measured and reported like any other, but it can
    /// never reach the significant-changes table — its numbers are
    /// informational. The reason is rendered beside it, so a reader is told why
    /// rather than left to wonder.
    #[must_use]
    pub fn erratic(mut self, why: &str) -> Self {
        self.erratic = Some(why.to_owned());
        self
    }

    /// Adds the case to its suite.
    #[must_use]
    pub fn done(self) -> Suite {
        let Self {
            mut suite,
            id,
            setup,
            routine,
            items,
            bytes,
            iters,
            erratic,
        } = self;

        let prepare = Box::new(move |seed: u64, corpus: &mut Corpus| {
            let state = setup(corpus, seed);
            let items = items.as_ref().map(|f| f(&state));
            let bytes = bytes.as_ref().map(|f| f(&state));
            let routine = Rc::clone(&routine);
            Prepared {
                items,
                bytes,
                exercise: Box::new(move |b| routine(b, &state)),
            }
        });

        suite.cases.push(Case {
            id,
            iters_hint: iters,
            erratic,
            prepare,
        });
        suite
    }
}

/// One declared case.
pub struct Case {
    id: String,
    iters_hint: Option<u64>,
    erratic: Option<String>,
    prepare: Prepare,
}

impl std::fmt::Debug for Case {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Case")
            .field("id", &self.id)
            .field("iters_hint", &self.iters_hint)
            .field("erratic", &self.erratic)
            .finish_non_exhaustive()
    }
}

/// A case's setup, run: its state, and how many items and bytes an iteration
/// covers.
struct Prepared {
    items: Option<u64>,
    bytes: Option<u64>,
    exercise: Box<dyn Fn(&mut Bencher)>,
}

/// What one measured process is asked to do.
///
/// Crate-internal: the only supported entry point is [`crate::bench_main!`],
/// which also installs the counting allocator. A hand-rolled driver calling
/// [`Case::measure`] would silently produce records with no allocation metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunOptions {
    /// Seeds the corpus. Shared across replicates by design.
    pub seed: u64,
    /// Iterations inside the measured region, as calibrated by the driver.
    pub iters: u64,
    /// Milliseconds of unmeasured warm-up before the region opens.
    pub warmup_ms: u64,
}

/// What one measured process produced.
#[derive(Debug, Clone)]
pub(crate) struct Outcome {
    /// The metrics, keyed by the constants in [`crate::record`].
    pub metrics: BTreeMap<String, Metric>,
    /// Why anything is missing.
    pub notes: Vec<String>,
    /// Digest of everything setup absorbed.
    pub corpus_digest: String,
    /// Digest of everything setup declared, absent when it declared nothing.
    pub build_digest: Option<String>,
}

impl Case {
    /// The case's id, unique within its target.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Why the case declared itself noisy, if it did.
    #[must_use]
    pub fn erratic(&self) -> Option<&str> {
        self.erratic.as_deref()
    }

    /// The iteration count the case pinned, if it pinned one.
    #[must_use]
    pub fn iters_hint(&self) -> Option<u64> {
        self.iters_hint
    }

    /// Finds an iteration count whose measured region runs for about
    /// `target_ms`.
    ///
    /// # Errors
    ///
    /// When the routine does not mark a measured region exactly once.
    pub(crate) fn calibrate(&self, seed: u64, target_ms: u64) -> Result<u64, String> {
        if let Some(pinned) = self.iters_hint {
            return Ok(pinned);
        }

        let mut corpus = Corpus::new();
        let prepared = (self.prepare)(seed, &mut corpus);
        let target_ns = target_ms.max(1).saturating_mul(1_000_000);

        let mut iters: u64 = 1;
        let mut wall_ns: u64;
        loop {
            wall_ns = self.time_once(&prepared, iters)?.max(1);
            // A tenth of the target is enough to extrapolate from without
            // paying for a full-length probe at every step.
            if wall_ns.saturating_mul(10) >= target_ns || iters >= MAX_ITERS {
                break;
            }
            // Grow towards the target rather than doubling blindly: a routine
            // eleven orders of magnitude short of the target would otherwise
            // need forty probes to get there. Capped at 20× a step so one
            // unusually fast probe cannot overshoot into a minute-long region.
            let factor = (target_ns / wall_ns).clamp(2, 20);
            iters = iters.saturating_mul(factor).min(MAX_ITERS);
        }

        // Nanosecond durations and iteration counts are both far below 2^53, so
        // the round trip through f64 is exact for every value this can see.
        let per_iter = wall_ns as f64 / iters as f64;
        let wanted = target_ns as f64 / per_iter.max(f64::MIN_POSITIVE);
        Ok(wanted.clamp(1.0, MAX_ITERS as f64) as u64)
    }

    /// Runs one measured replicate.
    ///
    /// # Errors
    ///
    /// When the routine does not mark a measured region exactly once, when the
    /// clock cannot resolve a loop of this length, or when the case turns out
    /// to be measuring that loop rather than its routine.
    pub(crate) fn measure(&self, opts: &RunOptions) -> Result<Outcome, String> {
        let mut corpus = Corpus::new();
        let prepared = (self.prepare)(opts.seed, &mut corpus);
        let iters = opts.iters.max(1);

        // `installed()` allocates, so it is asked before anything is watched.
        let counting = alloc::installed();

        // The resident-set baseline is taken *before* the warm-up rather than
        // after it. `ru_maxrss` is monotonic, and the warm-up runs the same
        // routine, so a baseline taken afterwards would find the mark already
        // set by the routine's own first pass and report nothing — measurably:
        // a 200 ms warm-up suppressed this metric entirely, and the default
        // 50 ms one let it survive only on allocator creep. What the gate is
        // for is excluding a case whose *corpus building* set the mark, and
        // starting here still does that. Wall, CPU and allocation totals are
        // strictly region-only; they are taken inside `Bencher`.
        let watch = PeakWatch::start();
        self.warm_up(&prepared, iters, opts.warmup_ms)?;

        let mut bencher = Bencher::recording(iters);
        (prepared.exercise)(&mut bencher);
        let region = bencher.finish_recording(&self.id)?;

        let wall_ns = region.wall_ns;
        let cpu_ns = region.cpu_ns;
        let peak_rss = watch.peak_rss_bytes();

        // The guard that catches a case measuring nothing. See
        // `empty_loop_ns_per_iter`.
        let per_iter = wall_ns as f64 / iters as f64;
        let Some(floor) = empty_loop_ns_per_iter(iters) else {
            return Err(format!(
                "case '{}': the clock could not resolve a loop of {iters} iterations, so \
                 nothing measured over one is a measurement. Raise the iteration count.",
                self.id
            ));
        };
        if per_iter <= floor * DEGENERATE_FACTOR {
            return Err(format!(
                "case '{}' took {per_iter:.3} ns per iteration against an empty loop's \
                 {floor:.3} ns, so it is measuring the loop rather than the routine. Either \
                 the routine was optimised away — return its result instead of discarding \
                 it, so black_box has something to hold on to — or one iteration is too \
                 little work to time, in which case fold more of it into each one.",
                self.id
            ));
        }

        let mut metrics = BTreeMap::new();
        let mut notes = Vec::new();

        // Every quantity below is a count or a nanosecond duration over one
        // region, so all of them are far below 2^53 and exact in f64.
        let iters_f = iters as f64;
        let wall_f = wall_ns as f64;

        metrics.insert(
            WALL_NS_PER_ITER.to_owned(),
            Metric::minimize(per_iter, "ns"),
        );

        match cpu_ns {
            Some(cpu) => {
                metrics.insert(
                    CPU_NS_PER_ITER.to_owned(),
                    Metric::minimize(cpu as f64 / iters_f, "ns"),
                );
            }
            None => notes.push(
                "cpu_ns_per_iter absent: the kernel declined to report this process's \
                 CPU accounting"
                    .to_owned(),
            ),
        }

        let seconds = wall_f / 1e9;
        if seconds > 0.0 {
            if let Some(items) = prepared.items {
                metrics.insert(
                    RECORDS_PER_S.to_owned(),
                    Metric::maximize(items as f64 * iters_f / seconds, "records/s"),
                );
            }
            if let Some(bytes) = prepared.bytes {
                metrics.insert(
                    BYTES_PER_S.to_owned(),
                    Metric::maximize(bytes as f64 * iters_f / seconds, "bytes/s"),
                );
            }
        } else if prepared.items.is_some() || prepared.bytes.is_some() {
            // Unreachable while the degenerate guard above holds, and noted
            // rather than dropped anyway: every other conditional metric says
            // why it is missing, and a throughput that vanishes silently is the
            // one shape this schema does not allow.
            notes.push(
                "throughput metrics absent: the measured region reported zero elapsed time"
                    .to_owned(),
            );
        }

        match peak_rss.filter(|_| !region.batched) {
            Some(peak) => {
                metrics.insert(
                    PEAK_RSS_BYTES.to_owned(),
                    Metric::minimize(peak as f64, "bytes"),
                );
            }
            None if region.batched => notes.push(
                "peak_rss_bytes absent: this case uses iter_batched, so the harness holds \
                 one prebuilt input per iteration and its own buffers dominate the \
                 process's high-water mark — the figure would be about the batching \
                 rather than about the routine"
                    .to_owned(),
            ),
            None if crate::rusage::peak_rss_bytes().is_none() => notes.push(
                "peak_rss_bytes absent: this platform does not report a peak resident set \
                 in a unit this harness knows"
                    .to_owned(),
            ),
            None => notes.push(
                "peak_rss_bytes absent: running the case never took this process above \
                 what building its corpus had already made resident, so the only figure \
                 available describes the setup"
                    .to_owned(),
            ),
        }

        if counting {
            let used = region.alloc;
            metrics.insert(
                ALLOC_BYTES_PER_ITER.to_owned(),
                Metric::minimize(used.bytes as f64 / iters_f, "bytes"),
            );
            metrics.insert(
                ALLOC_COUNT_PER_ITER.to_owned(),
                Metric::minimize(used.count as f64 / iters_f, "allocations"),
            );
        } else {
            notes.push(
                "allocation metrics absent: this process's global allocator is not the \
                 counting one, which means the binary does not use bench_main!"
                    .to_owned(),
            );
        }

        Ok(Outcome {
            metrics,
            notes,
            corpus_digest: corpus.digest_hex(),
            build_digest: corpus.build_digest_hex(),
        })
    }

    /// Times one pass without producing metrics — the calibration probe.
    fn time_once(&self, prepared: &Prepared, iters: u64) -> Result<u64, String> {
        let mut bencher = Bencher::recording(iters);
        (prepared.exercise)(&mut bencher);
        Ok(bencher.finish_recording(&self.id)?.wall_ns)
    }

    /// Runs the routine untimed until `warmup_ms` have passed.
    ///
    /// In chunks rather than full passes: a calibrated pass is by design about
    /// as long as the whole warm-up budget, so one pass would either overshoot
    /// it or be the only one that ran.
    fn warm_up(&self, prepared: &Prepared, iters: u64, warmup_ms: u64) -> Result<(), String> {
        if warmup_ms == 0 {
            return Ok(());
        }
        let chunk = (iters / 8).max(1);
        let clock = Stopwatch::start();
        let budget_ns = warmup_ms.saturating_mul(1_000_000);
        loop {
            let mut bencher = Bencher::warming(chunk);
            (prepared.exercise)(&mut bencher);
            bencher.finish_warm_up(&self.id)?;
            if clock.elapsed_ns() >= budget_ns {
                return Ok(());
            }
        }
    }
}

/// The handle a routine uses to mark its measured region.
///
/// A routine must call exactly one of [`Bencher::iter`] or
/// [`Bencher::iter_batched`], exactly once. Two regions in one routine would
/// mean two answers to a question with one slot in the record, and none would
/// mean a record with no measurement in it — both are errors rather than
/// silently-plausible numbers.
#[derive(Debug)]
pub struct Bencher {
    iters: u64,
    recording: bool,
    regions: u32,
    batched: bool,
    region: Option<Region>,
}

/// What one measured region cost.
#[derive(Debug, Clone, Copy)]
struct Region {
    wall_ns: u64,
    cpu_ns: Option<u64>,
    alloc: alloc::Snapshot,
    /// Whether the routine used [`Bencher::iter_batched`], whose prebuilt
    /// inputs sit inside the resident-set window even though they are outside
    /// the timed region.
    batched: bool,
}

impl Bencher {
    fn recording(iters: u64) -> Self {
        Self {
            iters,
            recording: true,
            regions: 0,
            batched: false,
            region: None,
        }
    }

    fn warming(iters: u64) -> Self {
        Self {
            iters,
            recording: false,
            regions: 0,
            batched: false,
            region: None,
        }
    }

    /// How many iterations the routine is being asked for.
    ///
    /// Useful for a routine that has to size something by the count; the loop
    /// itself is [`Bencher::iter`]'s job.
    #[must_use]
    pub const fn iters(&self) -> u64 {
        self.iters
    }

    /// Runs `routine` for the requested number of iterations, inside the
    /// measured region.
    ///
    /// **Return the routine's result.** It is what is passed through
    /// [`std::hint::black_box`], so a routine written `|| { let _ = f(x); }`
    /// hands the black box `()` and leaves the optimiser free to delete the
    /// call. That failure is caught — [`empty_loop_ns_per_iter`] refuses a case
    /// whose per-iteration cost sits at an empty loop's — but the fix is here.
    ///
    /// The result is dropped inside the region: a routine whose result is
    /// expensive to drop should return something cheap and keep the expensive
    /// part in [`Bencher::iter_batched`].
    pub fn iter<T>(&mut self, mut routine: impl FnMut() -> T) {
        self.regions += 1;
        let iters = self.iters;
        self.region(move || {
            for _ in 0..iters {
                std::hint::black_box(routine());
            }
        });
    }

    /// Runs `routine` once per iteration over inputs built beforehand.
    ///
    /// `make` is called for every iteration *before* the region opens, so
    /// building the input is never part of the measurement, and the outputs are
    /// collected into a vector reserved beforehand so dropping them is not
    /// either. The input itself is moved into `routine` and dropped inside the
    /// region, which is where it belongs: consuming it is part of the work.
    ///
    /// One input per iteration means the memory this holds is proportional to
    /// the iteration count. A case whose input is large should pin
    /// [`CaseBuilder::iters`] rather than let calibration choose.
    pub fn iter_batched<I, T>(
        &mut self,
        mut make: impl FnMut(u64) -> I,
        mut routine: impl FnMut(I) -> T,
    ) {
        self.regions += 1;
        self.batched = true;
        let count = usize::try_from(self.iters).unwrap_or(usize::MAX);
        let inputs: Vec<I> = (0..self.iters).map(&mut make).collect();
        let mut outputs: Vec<T> = Vec::with_capacity(count);

        self.region(|| {
            for input in inputs {
                outputs.push(routine(input));
            }
        });

        std::hint::black_box(&outputs);
        drop(outputs);
    }

    /// Opens the measured region around `body`, or runs it bare during warm-up.
    ///
    /// Every instrument starts and stops here rather than around the routine
    /// call, which is what keeps [`Bencher::iter_batched`]'s input building
    /// outside the measurement: the routine has already built its inputs by the
    /// time this is reached.
    fn region(&mut self, body: impl FnOnce()) {
        if !self.recording {
            body();
            return;
        }

        let alloc_before = alloc::snapshot();
        let cpu_before = crate::rusage::cpu_ns();
        let clock = Stopwatch::start();

        body();

        let wall_ns = clock.elapsed_ns();
        let cpu_after = crate::rusage::cpu_ns();
        let alloc_after = alloc::snapshot();

        self.region = Some(Region {
            wall_ns,
            cpu_ns: match (cpu_before, cpu_after) {
                (Some(before), Some(after)) => Some(after.saturating_sub(before)),
                _ => None,
            },
            alloc: alloc_after.since(alloc_before),
            batched: self.batched,
        });
    }

    /// Consumes a recording bencher, yielding what its one region cost.
    fn finish_recording(self, case: &str) -> Result<Region, String> {
        self.check_regions(case)?;
        self.region
            .ok_or_else(|| format!("case '{case}' marked a region that produced no measurement"))
    }

    /// Consumes a warm-up bencher, checking it exercised the same one region.
    fn finish_warm_up(self, case: &str) -> Result<(), String> {
        self.check_regions(case)
    }

    /// Exactly one region, or an error naming which way it went wrong.
    fn check_regions(&self, case: &str) -> Result<(), String> {
        match self.regions {
            1 => Ok(()),
            0 => Err(format!(
                "case '{case}' never called Bencher::iter or Bencher::iter_batched, \
                 so nothing was measured"
            )),
            n => Err(format!(
                "case '{case}' marked {n} measured regions; a case has one measurement, \
                 so split it into {n} cases"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_ITERS, RunOptions, suite};
    use crate::record::{ALLOC_BYTES_PER_ITER, CPU_NS_PER_ITER, RECORDS_PER_S, WALL_NS_PER_ITER};

    fn trivial() -> super::Suite {
        suite("spate-bench")
            .case(
                "sum",
                |corpus, seed| {
                    let mut rng = crate::rng::SplitMix64::new(seed);
                    let data: Vec<u64> = (0..256).map(|_| rng.next_u64()).collect();
                    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
                    corpus.absorb("data", &bytes);
                    data
                },
                |b, data| b.iter(|| data.iter().fold(0u64, |a, v| a.wrapping_add(*v))),
            )
            .items(256)
            .done()
    }

    #[test]
    fn a_measured_case_reports_wall_time_and_throughput() {
        let suite = trivial();
        let case = suite.find("sum").expect("declared");
        let outcome = case
            .measure(&RunOptions {
                seed: 7,
                iters: 64,
                warmup_ms: 0,
            })
            .expect("measures");

        assert!(outcome.metrics.contains_key(WALL_NS_PER_ITER));
        assert!(outcome.metrics.contains_key(RECORDS_PER_S));
        assert!(outcome.metrics.contains_key(CPU_NS_PER_ITER));
        assert_eq!(outcome.corpus_digest.len(), 16);

        // The allocator is not installed under the test harness, so the
        // allocation metrics must be absent *and* explained.
        assert!(!outcome.metrics.contains_key(ALLOC_BYTES_PER_ITER));
        assert!(
            outcome.notes.iter().any(|n| n.contains("bench_main")),
            "{:?}",
            outcome.notes
        );
    }

    #[test]
    fn the_corpus_digest_is_the_same_for_the_same_seed_and_differs_otherwise() {
        let suite = trivial();
        let case = suite.find("sum").expect("declared");
        let opts = RunOptions {
            seed: 7,
            iters: 8,
            warmup_ms: 0,
        };
        let first = case.measure(&opts).expect("measures").corpus_digest;
        let again = case.measure(&opts).expect("measures").corpus_digest;
        assert_eq!(first, again);

        let other = case
            .measure(&RunOptions { seed: 8, ..opts })
            .expect("measures")
            .corpus_digest;
        assert_ne!(first, other);
    }

    #[test]
    fn calibration_lands_inside_the_ceiling_and_a_pinned_count_is_returned_verbatim() {
        let suite = trivial();
        let settled = suite
            .find("sum")
            .expect("declared")
            .calibrate(1, 5)
            .expect("calibrates");
        assert!((1..=MAX_ITERS).contains(&settled), "{settled}");

        let pinned = suite
            .case("fixed", |_, _| (), |b, ()| b.iter(|| 1u8))
            .iters(99)
            .done();
        assert_eq!(
            pinned.find("fixed").expect("declared").calibrate(1, 5000),
            Ok(99)
        );
    }

    /// A routine that measures nothing, or measures twice, is an error. Both
    /// would otherwise produce a well-formed record: the first with no wall
    /// time at all, the second with whichever region happened to be timed.
    #[test]
    fn a_routine_must_mark_exactly_one_region() {
        let opts = RunOptions {
            seed: 1,
            iters: 4,
            warmup_ms: 0,
        };

        let none = suite("spate-bench")
            .case("none", |_, _| (), |_b, ()| {})
            .done();
        let err = none
            .find("none")
            .expect("declared")
            .measure(&opts)
            .expect_err("no region");
        assert!(err.contains("never called"), "{err}");

        let twice = suite("spate-bench")
            .case(
                "twice",
                |_, _| (),
                |b, ()| {
                    b.iter(|| 1u8);
                    b.iter(|| 2u8);
                },
            )
            .done();
        let err = twice
            .find("twice")
            .expect("declared")
            .measure(&opts)
            .expect_err("two regions");
        assert!(err.contains("2 measured regions"), "{err}");
    }

    /// The failure that produces a plausible number rather than an error: a
    /// routine returning `()` gives `black_box` nothing to hold, the call is
    /// deleted, and the case reports the cost of an empty loop as if it were a
    /// measurement.
    /// A routine whose body the optimiser deleted is indistinguishable from one
    /// that never had a body, so the guard is asserted against the latter —
    /// which behaves the same way in every profile, where the deletion only
    /// happens in an optimised build.
    #[test]
    fn a_routine_that_measures_nothing_is_refused() {
        let opts = RunOptions {
            seed: 1,
            iters: 200_000,
            warmup_ms: 0,
        };

        let empty = suite("spate-bench")
            .case("empty", |_, _| (), |b, ()| b.iter(|| {}))
            .done();
        let err = empty
            .find("empty")
            .expect("declared")
            .measure(&opts)
            .expect_err("a routine at the empty loop's cost must not report a number");
        assert!(err.contains("measuring the loop"), "{err}");

        // A routine that does something is measured.
        let real = suite("spate-bench")
            .case(
                "real",
                |_, _| vec![1u64; 256],
                |b, data| b.iter(|| data.iter().sum::<u64>()),
            )
            .done();
        assert!(real.find("real").expect("declared").measure(&opts).is_ok());

        // The guard applies at every iteration count — there is no threshold
        // below which it stands aside, which matters because a pinned
        // `.iters(n)` is exactly where an `iter_batched` case lives.
        let few = RunOptions {
            iters: 4096,
            ..opts
        };
        assert!(
            empty
                .find("empty")
                .expect("declared")
                .measure(&few)
                .is_err()
        );
        assert!(real.find("real").expect("declared").measure(&few).is_ok());
    }

    /// The floor is a measurement, so it has to say when it is not one. A pass
    /// the clock read as zero must not become a floor of zero — that would
    /// switch the guard off for the one case it exists to catch.
    #[test]
    fn the_reference_floor_reports_a_number_or_nothing() {
        let floor = super::empty_loop_ns_per_iter(1_000_000).expect("a million iterations resolve");
        assert!(floor > 0.0 && floor.is_finite(), "{floor}");
    }

    /// A count far below the clock's resolution still yields a floor.
    ///
    /// An expensive routine calibrates to a few dozen iterations, and an empty
    /// loop of a few dozen is tens of nanoseconds against a clock that ticks in
    /// tens — so every pass can read zero and the case is refused for a reason
    /// that is about the clock rather than about the case. It is intermittent,
    /// which is worse: `frame_lf_split_chunks` calibrates to about thirty and
    /// failed roughly one A/A run in five before the probe grew itself.
    ///
    /// A flaky guard cannot be tested by running it once, so this asserts the
    /// property that makes it not flaky: the probe reports a number at every
    /// count, down to one.
    #[test]
    fn the_reference_floor_resolves_a_count_the_clock_cannot_time_directly() {
        for iters in [1, 2, 31, 64, 1024] {
            let floor = super::empty_loop_ns_per_iter(iters)
                .unwrap_or_else(|| panic!("{iters} iteration(s) produced no floor"));
            assert!(floor > 0.0 && floor.is_finite(), "{iters}: {floor}");
        }
    }

    /// Once the loop is long enough to dominate the two clock reads around it,
    /// the floor is a per-iteration cost — an empty iteration is a counter and
    /// a `black_box`, which is nanoseconds rather than hundreds of them.
    ///
    /// Asserted only above a handful of iterations. Below that the stopwatch's
    /// own overhead lands on very few iterations and inflates the figure, which
    /// is deliberate rather than a defect: the case's region is timed with the
    /// same stopwatch and carries the same overhead, so where it is large
    /// enough to matter it sits in both numerators and cancels. It is also why
    /// the probe grows only when the clock resolves *nothing* — a count it does
    /// resolve has to keep the floor it has always had.
    #[test]
    fn the_reference_floor_is_a_per_iteration_cost_once_the_loop_dominates() {
        for iters in [31, 64, 1024] {
            let floor = super::empty_loop_ns_per_iter(iters).expect("resolves");
            assert!(
                floor < 100.0,
                "{iters}: {floor} ns/iter is a total rather than one iteration"
            );
        }
    }

    /// A batched case's resident-set figure would be about the harness's own
    /// prebuilt inputs, so it is not reported at all.
    #[test]
    fn a_batched_case_reports_no_resident_set() {
        let suite = suite("spate-bench")
            .case(
                "batched",
                |_, _| (),
                |b, ()| {
                    b.iter_batched(|i| vec![i; 4096], |input| input.iter().sum::<u64>());
                },
            )
            .done();

        let outcome = suite
            .find("batched")
            .expect("declared")
            .measure(&RunOptions {
                seed: 1,
                iters: 512,
                warmup_ms: 0,
            })
            .expect("measures");
        assert!(!outcome.metrics.contains_key(crate::record::PEAK_RSS_BYTES));
        assert!(
            outcome.notes.iter().any(|n| n.contains("iter_batched")),
            "{:?}",
            outcome.notes
        );
    }

    #[test]
    fn erratic_and_iters_hints_survive_onto_the_case() {
        let suite = suite("spate-bench")
            .case("noisy", |_, _| (), |b, ()| b.iter(|| 1u8))
            .erratic("depends on the allocator's mood")
            .iters(3)
            .done();
        let case = suite.find("noisy").expect("declared");
        assert_eq!(case.erratic(), Some("depends on the allocator's mood"));
        assert_eq!(case.iters_hint(), Some(3));
        assert_eq!(suite.krate(), "spate-bench");
    }

    /// Ordering, not counts. A lazy implementation that built each input as it
    /// ran it — the exact bug this case is named for — produces the same two
    /// counts, so what is recorded is the *sequence*: every build must precede
    /// every run.
    #[test]
    fn iter_batched_builds_every_input_before_the_region() {
        use std::cell::RefCell;
        use std::rc::Rc;

        #[derive(Debug, PartialEq, Eq)]
        enum Event {
            Built(u64),
            Ran(u64),
        }

        let log: Rc<RefCell<Vec<Event>>> = Rc::new(RefCell::new(Vec::new()));
        let setup_log = Rc::clone(&log);

        let suite = suite("spate-bench")
            .case(
                "batched",
                move |_, _| Rc::clone(&setup_log),
                |b, log| {
                    let make_log = Rc::clone(log);
                    let run_log = Rc::clone(log);
                    b.iter_batched(
                        move |i| {
                            make_log.borrow_mut().push(Event::Built(i));
                            vec![i; 4]
                        },
                        move |input| {
                            run_log.borrow_mut().push(Event::Ran(input[0]));
                            input.len()
                        },
                    );
                },
            )
            .done();

        suite
            .find("batched")
            .expect("declared")
            .measure(&RunOptions {
                seed: 1,
                iters: 10,
                warmup_ms: 0,
            })
            .expect("measures");

        let events = log.borrow();
        let expected: Vec<Event> = (0..10)
            .map(Event::Built)
            .chain((0..10).map(Event::Ran))
            .collect();
        assert_eq!(*events, expected);
    }
}
