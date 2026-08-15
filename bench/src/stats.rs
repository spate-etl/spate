//! The decision rule: when a difference between two legs is a finding.
//!
//! Two parts, both of which must hold before anything is flagged:
//!
//! 1. a bootstrapped 90% confidence interval on the mean relative difference
//!    excludes zero, and
//! 2. the mean relative difference is at least the metric's floor in magnitude.
//!
//! The first asks whether the difference is distinguishable from the noise in
//! the replicates. The second asks whether it is worth telling anybody. A rule
//! with only the first flags a reliably-measured 0.3% on a quiet machine; a
//! rule with only the second flags whatever the machine happened to do.
//!
//! # The floors, and why they differ
//!
//! | Metric | Floor |
//! |---|---|
//! | Wall time, CPU time, throughput | 5% |
//! | Peak resident set | 10% |
//! | Allocation totals | 1% |
//!
//! The resident set gets a wider floor because it moves for allocator reasons
//! unrelated to the change. Allocation totals get a much narrower one because
//! they are near-deterministic, and a five-percent floor there would suppress a
//! real regression.
//!
//! # A derived metric and the metric it comes from
//!
//! `records_per_s` and `bytes_per_s` are not measurements. A case declares how
//! many items or bytes an iteration covers, and the rate is that constant over
//! the wall time with the iteration count pinned across both legs, so each is
//! `const / wall_ns_per_iter` on every replicate.
//!
//! A floor applied to both separately asks two different questions about one
//! measurement. A wall difference `d` reaches a rate as `-d / (1 + d)`, and
//! `|d| >= f` and `|-d / (1 + d)| >= f` are not the same condition: either side
//! of the floor lies a band where the rows describing one timing event
//! disagree. A derived row therefore carries the verdict of the quantity that
//! was measured, and keeps its own difference, interval and floor.
//! [`derived_from`] names that quantity, and
//! [`crate::compare::Row::is_finding`] keeps the derived row out of the
//! significant-changes table, so one timing event is one finding.
//!
//! # When the rule is not applied
//!
//! Two conditions stop it before it starts. Both print the difference with no
//! verdict beside it, and name the replicate count that case would need.
//!
//! Fewer than five paired replicates. Below that, the tail of a bootstrap is
//! decided by which single pair was drawn least often, so an interval would be
//! an assertion about the resampler rather than about the code. The count named
//! there is projected from an interval this module does not otherwise trust,
//! and is a starting point.
//!
//! An interval that straddles the metric's floor. A floor separates a
//! difference worth telling anybody about from one that is not, and can only
//! answer that while it is wider than the interval. Where the interval is
//! wider, a case's own spread reaches the floor unaided and which case clears
//! it is decided by the run. [`replicates_for`] names the count that brings the
//! interval back inside the floor.
//!
//! An interval lying *wholly* past the floor is a third case, and is judged.
//! The floor has separated that difference whatever the interval's width, and
//! declining it would hide the largest movements a run can find.
//!
//! The count a case needs is set by its own spread and by the machine it runs
//! on, and is answered per case. A count short of what a case needs costs
//! verdicts on that case alone.
//!
//! # Pairing, and why it is not a ratio of means
//!
//! An A/B run interleaves the legs, so replicate *k* of each ran adjacent in
//! time and shared whatever the machine was doing at that moment. The
//! statistic is therefore computed *per pair*, as `d_k = (head_k − base_k) /
//! base_k`, and averaged. Dividing the mean of one leg by the mean of the
//! other throws that away: it is the same arithmetic on drift-free data and a
//! strictly worse estimate on real data, where the whole reason for
//! interleaving is that the machine drifts.
//!
//! # Why a bootstrap
//!
//! The per-pair differences are not normal, there are ten of them, and their
//! distribution is decided by the machine rather than by anything modellable.
//! The percentile bootstrap asks the data what its own spread is, which needs
//! no distributional assumption and no history, and there is no history here
//! by design.
//!
//! Its cuts are then widened for the sample size, by [`widening`]. Resampling
//! spreads the means by the plug-in standard deviation and cuts them at normal
//! critical values, both of which understate what a small sample supports: at
//! five pairs the interval is 31% narrower than its stated coverage. The
//! widening is where this module assumes the mean of the differences is roughly
//! normal. The interval's shape, skew included, comes from the resampling.
//!
//! The resampling is seeded from the case and metric names, so re-rendering the
//! same two legs produces the same interval to the last digit. A report whose
//! numbers move when it is regenerated is a report no one can quote. It draws
//! 10 000 resamples.
//!
//! # No multiplicity correction
//!
//! A run comparing twenty cases across seven metrics computes over a hundred
//! intervals, and at 90% coverage a few will exclude zero by chance. The floors
//! absorb most of that, and correcting properly would need a family definition
//! this tier has no basis to pick. The consequence belongs to whoever reads the
//! report: a single flagged row on an otherwise quiet one is a reason to re-run,
//! not a result.

use std::hash::Hasher as _;

use twox_hash::XxHash64;

use crate::record::{
    ALLOC_BYTES_PER_ITER, ALLOC_COUNT_PER_ITER, BYTES_PER_S, PEAK_RSS_BYTES, RECORDS_PER_S,
    WALL_NS_PER_ITER,
};
use crate::rng::SplitMix64;

/// Below this many paired replicates, a difference is printed without a
/// verdict.
///
/// Five is where a percentile bootstrap starts to have anything to resample:
/// with four pairs the 5th percentile of the resampled means is decided by
/// which single pair was drawn least often.
pub const MIN_REPLICATES: usize = 5;

/// The smallest count that is both judgeable and balanced: [`MIN_REPLICATES`]
/// rounded up to an even number.
///
/// An A/B run alternates which leg goes first, so an odd count leaves one leg
/// second more often than the other. [`replicates_for`] recommends a count a
/// two-leg run will accept.
pub const MIN_BALANCED_REPLICATES: usize = MIN_REPLICATES + MIN_REPLICATES % 2;

/// The largest count [`replicates_for`] will name.
///
/// Past this, the answer a report carries is that the case is not measurable on
/// this machine rather than a count.
pub const MAX_SUGGESTED_REPLICATES: usize = 200;

/// Resamples per bootstrap. Enough that the interval's own Monte-Carlo error is
/// far below the floors below, and cheap enough to run for every metric of
/// every case.
pub const BOOTSTRAP_RESAMPLES: usize = 10_000;

/// The interval's coverage.
pub const CONFIDENCE: f64 = 0.90;

/// The normal critical value the resampled percentiles sit at for
/// [`CONFIDENCE`]: `z(0.95)`.
const Z: f64 = 1.644_853_626_951_472_2;

/// `t(0.95, v)` for `v` degrees of freedom, indexed by `v - 1`.
///
/// Tabulated over the range where the correction is largest and an
/// approximation is worst: at four degrees of freedom the expansion
/// [`t_quantile`] falls back on is 5% low.
const T_95: [f64; 30] = [
    6.313_751_515,
    2.919_985_580,
    2.353_363_435,
    2.131_846_786,
    2.015_048_373,
    1.943_180_281,
    1.894_578_605,
    1.859_548_038,
    1.833_112_933,
    1.812_461_123,
    1.795_884_819,
    1.782_287_556,
    1.770_933_396,
    1.761_310_136,
    1.753_050_356,
    1.745_883_676,
    1.739_606_726,
    1.734_063_607,
    1.729_132_812,
    1.724_718_243,
    1.720_742_903,
    1.717_144_374,
    1.713_871_528,
    1.710_882_080,
    1.708_140_761,
    1.705_617_920,
    1.703_288_446,
    1.701_130_934,
    1.699_127_027,
    1.697_260_887,
];

/// `t(0.95, v)`, the one-sided critical value each tail of the interval sits at.
///
/// [`T_95`] up to thirty degrees of freedom, and the Cornish-Fisher expansion of
/// the quantile beyond it, which is within 0.002% of exact from there on and
/// converges to [`Z`].
fn t_quantile(freedom: usize) -> f64 {
    // One degree of freedom is the floor. The table starts there and the
    // expansion divides by the count, so zero would otherwise reach the
    // expansion as an index off the end of the table and return an infinity
    // that the interval carries all the way to a report.
    let freedom = freedom.max(1);
    match T_95.get(freedom - 1) {
        Some(exact) => *exact,
        None => {
            let v = freedom as f64;
            Z + (Z.powi(3) + Z) / (4.0 * v)
                + (5.0 * Z.powi(5) + 16.0 * Z.powi(3) + 3.0 * Z) / (96.0 * v * v)
        }
    }
}

/// How much wider than its percentile cuts an interval on `n` pairs has to be
/// for its stated coverage to be its coverage.
///
/// Two corrections, which multiply. The resampled means are spread by the
/// plug-in standard deviation, which divides by `n`, where the spread a sample
/// of `n` supports divides by `n - 1`. And the cuts sit at a normal critical
/// value, where a mean estimated from `n` pairs is a *t* one on `n - 1` degrees
/// of freedom. The factor is 1.449 at five pairs, 1.079 at twenty, and falls to
/// one as the sample grows.
///
/// Panics in debug on fewer than two pairs, which has no spread to widen.
#[must_use]
pub fn widening(n: usize) -> f64 {
    debug_assert!(n >= 2, "widening needs at least two pairs");
    let count = n as f64;
    t_quantile(n - 1) / Z * (count / (count - 1.0)).sqrt()
}

/// The default significance floor: a difference smaller than this is not
/// reported however tight its interval.
pub const DEFAULT_FLOOR: f64 = 0.05;

/// The floor for the peak resident set, which moves for allocator reasons that
/// have nothing to do with the change.
pub const RSS_FLOOR: f64 = 0.10;

/// The floor for the allocation totals.
///
/// One percent rather than five, because these are near-deterministic: a change
/// that allocates two percent more really does allocate two percent more, and a
/// five-percent floor would suppress it.
pub const ALLOC_FLOOR: f64 = 0.01;

/// The metric a derived one is computed from, or `None` when it is measured.
///
/// A throughput is the case's declared item or byte count over the wall time,
/// so it carries no information the wall time does not. A caller holding a
/// case's rows uses this to give the derived row the measured row's verdict;
/// judging the two against a floor apiece makes them contradict each other in a
/// band either side of it.
#[must_use]
pub fn derived_from(metric: &str) -> Option<&'static str> {
    match metric {
        RECORDS_PER_S | BYTES_PER_S => Some(WALL_NS_PER_ITER),
        _ => None,
    }
}

/// The floor this metric is judged against, as a fraction of the base mean.
#[must_use]
pub fn floor_for(metric: &str) -> f64 {
    match metric {
        PEAK_RSS_BYTES => RSS_FLOOR,
        ALLOC_BYTES_PER_ITER | ALLOC_COUNT_PER_ITER => ALLOC_FLOOR,
        _ => DEFAULT_FLOOR,
    }
}

/// What the rule concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The metric moved in the direction that is better, past both parts of the
    /// rule.
    Improved,
    /// The metric moved in the direction that is worse, past both parts.
    Regressed,
    /// Either the interval includes zero or the difference is below the floor.
    NoChange,
    /// The rule was not applied. The difference is still printed, and
    /// [`Analysis::replicates_needed`] says what it would take to apply it.
    ///
    /// Two causes, stated in the module header: fewer than [`MIN_REPLICATES`]
    /// paired replicates, or an interval no narrower than the metric's floor.
    NoVerdict,
}

impl Verdict {
    /// Whether this verdict belongs in the significant-changes table.
    #[must_use]
    pub const fn is_significant(self) -> bool {
        matches!(self, Self::Improved | Self::Regressed)
    }

    /// Whether the rule reached a conclusion at all.
    ///
    /// False only for [`Verdict::NoVerdict`], where the rule was never applied
    /// rather than applied and not met. A report that conflates the two claims
    /// the opposite of what happened.
    #[must_use]
    pub const fn is_judged(self) -> bool {
        !matches!(self, Self::NoVerdict)
    }

    /// A one-word rendering.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Improved => "improved",
            Self::Regressed => "regressed",
            Self::NoChange => "no change",
            Self::NoVerdict => "no verdict",
        }
    }

    /// The machine token, as `--format json` carries it.
    ///
    /// Snake case throughout, including where [`Verdict::label`] renders a
    /// space: a field a script matches on and a phrase a reader reads are two
    /// strings with different rules. Stable for a report schema version; see
    /// [`crate::render::REPORT_SCHEMA_VERSION`].
    #[must_use]
    pub const fn token(self) -> &'static str {
        match self {
            Self::Improved => "improved",
            Self::Regressed => "regressed",
            Self::NoChange => "no_change",
            Self::NoVerdict => "no_verdict",
        }
    }
}

/// One metric of one case, decided.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Analysis {
    /// Paired replicates behind the numbers.
    pub replicates: usize,
    /// Mean of the base leg's values.
    pub base_mean: f64,
    /// Mean of the head leg's values.
    pub head_mean: f64,
    /// Mean per-pair relative difference, as a fraction.
    pub delta: f64,
    /// Lower bound of the bootstrapped interval, as a fraction.
    pub ci_low: f64,
    /// Upper bound of the bootstrapped interval, as a fraction.
    pub ci_high: f64,
    /// The floor this metric was judged against, as a fraction.
    pub floor: f64,
    /// The conclusion.
    pub verdict: Verdict,
    /// Paired replicates this metric would need before the rule can be applied
    /// to it, and `None` whenever it was applied.
    ///
    /// Set for every [`Verdict::NoVerdict`]. `None` past
    /// [`MAX_SUGGESTED_REPLICATES`], where the answer is that this case is not
    /// measurable here rather than a count.
    pub replicates_needed: Option<usize>,
}

/// Compares one metric's paired samples.
///
/// `base` and `head` are already paired by replicate index, so element *k* of
/// each is replicate *k*. Pairing is the caller's job, because only the caller
/// knows which records went missing.
///
/// # Errors
///
/// When the samples are not the same length (the caller failed to pair them),
/// when there are none, when one is not finite, or when a base value is zero
/// while its partner is not. A change from nothing has no relative size, and
/// reporting one as infinite would put a nonsense row at the top of the table.
/// Zero on *both* legs is not an error: it is no change, and a non-allocating
/// case reports it every replicate.
pub fn analyse(
    metric: &str,
    higher_is_better: bool,
    base: &[f64],
    head: &[f64],
    seed: u64,
) -> Result<Analysis, String> {
    if base.len() != head.len() {
        return Err(format!(
            "{metric}: {} base samples against {} head samples — these were not paired",
            base.len(),
            head.len()
        ));
    }
    if base.is_empty() {
        return Err(format!("{metric}: no paired samples"));
    }

    let mut deltas = Vec::with_capacity(base.len());
    for (k, (b, h)) in base.iter().zip(head).enumerate() {
        if !b.is_finite() || !h.is_finite() {
            return Err(format!(
                "{metric}: replicate {k} is {b} on the base leg and {h} on the head leg, \
                 and a value that is not a finite number is not a measurement"
            ));
        }
        deltas.push(match (*b, *h) {
            // Zero on both legs is a common answer, since a non-allocating
            // case reports zero allocations every replicate, and it is *no
            // change* rather than an undefined ratio. Judged per pair rather than over
            // the whole sample, because one non-zero replicate elsewhere must
            // not turn every zero pair into an error.
            (0.0, 0.0) => 0.0,
            // Zero on the base and something on the head is a change from
            // nothing, which has no relative size. Reported as incomparable,
            // naming both values so a reader can see it is a finding rather
            // than a gap.
            (0.0, _) => {
                return Err(format!(
                    "{metric}: replicate {k} is {b} on the base leg and {h} on the head leg, \
                     so there is no relative difference to state — the change is from nothing"
                ));
            }
            (base_value, head_value) => (head_value - base_value) / base_value,
        });
    }

    let n = deltas.len();
    let delta = mean(&deltas);
    let (ci_low, ci_high) = bootstrap_ci(&deltas, seed);
    let floor = floor_for(metric);

    // The direction of *goodness*, not the sign: a throughput that rose is an
    // improvement, a latency that rose is a regression.
    let cleared = if (delta > 0.0) == higher_is_better {
        Verdict::Improved
    } else {
        Verdict::Regressed
    };

    let half_width = (ci_high - ci_low) / 2.0;
    let (verdict, replicates_needed) = if n < MIN_REPLICATES {
        (Verdict::NoVerdict, replicates_for(half_width, n, floor))
    } else if ci_low > floor || ci_high < -floor {
        // The whole interval is past the floor, so the floor has separated this
        // difference however wide the interval is. Asked before the width, since
        // a difference this size is exactly what a wide interval must not hide.
        (cleared, None)
    } else if half_width >= floor {
        // The floor asks whether a difference is worth telling anybody, and can
        // only answer that while it is wider than the interval. An interval that
        // straddles the floor leaves the question open, and answering it from a
        // point estimate reports the machine.
        (Verdict::NoVerdict, replicates_for(half_width, n, floor))
    } else if (ci_low > 0.0 || ci_high < 0.0) && delta.abs() >= floor {
        // Both halves still asked. A widened interval has two arms of different
        // lengths, so a difference past the floor with a half-width inside it
        // does not by itself put the whole interval one side of zero.
        (cleared, None)
    } else {
        (Verdict::NoChange, None)
    };

    Ok(Analysis {
        replicates: n,
        base_mean: mean(base),
        head_mean: mean(head),
        delta,
        ci_low,
        ci_high,
        floor,
        verdict,
        replicates_needed,
    })
}

/// The smallest count whose interval this case's spread would fit inside the
/// floor, given a `half_width` measured over `n` pairs.
///
/// An interval's width falls as `t(0.95, m - 1) / sqrt(m)`, and the spread it
/// is built from is a property of the case rather than of the count, so
/// `half(m) = half(n) * t(0.95, m - 1) / t(0.95, n - 1) * sqrt(n / m)`. Searched
/// rather than solved: the numerator is tabulated, and the answer has to be an
/// even number a two-leg run will accept.
///
/// `None` past [`MAX_SUGGESTED_REPLICATES`].
#[must_use]
pub fn replicates_for(half_width: f64, n: usize, floor: f64) -> Option<usize> {
    let measured = t_quantile(n.saturating_sub(1));
    (MIN_BALANCED_REPLICATES..=MAX_SUGGESTED_REPLICATES)
        .step_by(2)
        .find(|m| {
            let scaled = (n as f64 / *m as f64).sqrt();
            half_width * t_quantile(m - 1) / measured * scaled < floor
        })
}

/// The bootstrap's seed for one (case, metric) pair.
///
/// Derived from the names rather than from a clock, so the same two legs
/// rendered twice give the same interval, and so two metrics of one case do
/// not share a resampling pattern.
#[must_use]
pub fn seed_for(case: &str, metric: &str) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(case.as_bytes());
    hasher.write_u8(0);
    hasher.write(metric.as_bytes());
    hasher.finish()
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// The percentile bootstrap on the mean of `deltas`, widened for the sample
/// size.
///
/// The cuts alone state a coverage they do not have on a small sample; see
/// [`widening`], which is the factor applied here.
fn bootstrap_ci(deltas: &[f64], seed: u64) -> (f64, f64) {
    let n = deltas.len();
    if n == 1 {
        // One pair has no spread to resample: every resample is that pair, so
        // the interval is the point itself. Reported rather than special-cased
        // away; `MIN_REPLICATES` is what stops it becoming a verdict.
        return (deltas[0], deltas[0]);
    }

    let mut rng = SplitMix64::new(seed);
    let mut means = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let mut total = 0.0;
        for _ in 0..n {
            total += deltas[rng.below(n as u64) as usize];
        }
        means.push(total / n as f64);
    }
    means.sort_by(f64::total_cmp);

    let tail = (1.0 - CONFIDENCE) / 2.0;
    let low = percentile(&means, tail);
    let high = percentile(&means, 1.0 - tail);

    // Widened about the estimate rather than about the interval's own centre,
    // so a percentile interval that came out lopsided stays lopsided.
    let centre = mean(deltas);
    let factor = widening(n);
    (
        centre - factor * (centre - low),
        centre + factor * (high - centre),
    )
}

/// Nearest-rank percentile of a sorted slice.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    debug_assert!(!sorted.is_empty(), "percentile of an empty sample");
    let last = sorted.len() - 1;
    let idx = (p * last as f64).round().clamp(0.0, last as f64) as usize;
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::{
        ALLOC_FLOOR, Analysis, DEFAULT_FLOOR, MAX_SUGGESTED_REPLICATES, MIN_BALANCED_REPLICATES,
        MIN_REPLICATES, RSS_FLOOR, Verdict, analyse, floor_for, replicates_for, seed_for, widening,
    };
    use crate::record::{ALLOC_BYTES_PER_ITER, PEAK_RSS_BYTES, WALL_NS_PER_ITER};

    /// Ten replicates with a little jitter, so the interval has something to
    /// resample rather than collapsing onto a point.
    fn jittered(centre: f64, n: usize) -> Vec<f64> {
        (0..n)
            .map(|k| centre * (1.0 + ((k % 5) as f64 - 2.0) * 0.002))
            .collect()
    }

    fn wall(base: &[f64], head: &[f64]) -> Analysis {
        analyse(
            WALL_NS_PER_ITER,
            false,
            base,
            head,
            seed_for("case", WALL_NS_PER_ITER),
        )
        .expect("analyses")
    }

    /// The acceptance property in miniature: identical legs flag nothing. If
    /// this can fail, the harness is measuring itself.
    #[test]
    fn identical_legs_are_no_change() {
        let samples = jittered(1000.0, 10);
        let out = wall(&samples, &samples);
        assert_eq!(out.verdict, Verdict::NoChange);
        assert!(out.delta.abs() < 1e-12, "{}", out.delta);
        assert!(out.ci_low <= 0.0 && out.ci_high >= 0.0);
    }

    #[test]
    fn a_shift_past_the_floor_is_flagged_in_the_right_direction() {
        let base = jittered(1000.0, 10);
        let slower: Vec<f64> = base.iter().map(|v| v * 1.2).collect();
        let faster: Vec<f64> = base.iter().map(|v| v * 0.8).collect();

        let regressed = wall(&base, &slower);
        assert_eq!(regressed.verdict, Verdict::Regressed);
        assert!((regressed.delta - 0.2).abs() < 1e-9, "{}", regressed.delta);
        assert!(regressed.ci_low > 0.0);

        let improved = wall(&base, &faster);
        assert_eq!(improved.verdict, Verdict::Improved);
        assert!(improved.ci_high < 0.0);

        // The same shift on a higher-is-better metric reverses the verdict, and
        // nothing else about it changes.
        let throughput = analyse("records_per_s", true, &base, &slower, 1).expect("analyses");
        assert_eq!(throughput.verdict, Verdict::Improved);
        assert!((throughput.delta - regressed.delta).abs() < 1e-12);
    }

    /// A difference that is real but small. The interval excludes zero and the
    /// rule still declines, which is the second half doing its job.
    #[test]
    fn a_tight_interval_below_the_floor_is_not_a_finding() {
        let base = jittered(1000.0, 10);
        let head: Vec<f64> = base.iter().map(|v| v * 1.02).collect();
        let out = wall(&base, &head);
        assert!(out.ci_low > 0.0, "the interval should exclude zero");
        assert!(out.delta.abs() < DEFAULT_FLOOR);
        assert_eq!(out.verdict, Verdict::NoChange);
    }

    /// And the mirror image: a large difference the replicates cannot resolve.
    /// The interval is wider than the floor, so the rule declines rather than
    /// calling it no change, and says what resolving it would take.
    #[test]
    fn a_large_difference_with_an_interval_spanning_zero_is_not_a_finding() {
        let base = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0];
        let head = vec![84.0, 94.0, 103.0, 113.0, 122.0, 132.0];
        let out = wall(&base, &head);
        assert!(out.delta.abs() > DEFAULT_FLOOR);
        assert!(out.ci_low < 0.0 && out.ci_high > 0.0);
        assert_eq!(out.verdict, Verdict::NoVerdict);
        assert!(out.replicates_needed.is_some(), "{out:?}");
    }

    #[test]
    fn too_few_replicates_report_a_delta_and_no_verdict() {
        let base = jittered(1000.0, MIN_REPLICATES - 1);
        let head: Vec<f64> = base.iter().map(|v| v * 2.0).collect();
        let out = wall(&base, &head);
        assert_eq!(out.replicates, MIN_REPLICATES - 1);
        assert_eq!(out.verdict, Verdict::NoVerdict);
        assert!((out.delta - 1.0).abs() < 1e-9, "{}", out.delta);
        // Even, so the count it asks for is one a two-leg run accepts.
        assert_eq!(out.replicates_needed, Some(MIN_BALANCED_REPLICATES));
        assert_eq!(MIN_BALANCED_REPLICATES % 2, 0);
    }

    /// The case from the A/A report, as a spread rather than as its records:
    /// five pairs, a +9.02% mean difference and an interval half again as wide
    /// as the floor. Judged, it reads as a 9% regression; both A/A runs
    /// produced one of these and neither produced the same one.
    #[test]
    fn a_spread_the_floor_cannot_suppress_is_declined_with_a_count() {
        // Mean zero, sample standard deviation one, so the deltas below carry
        // exactly the spread named.
        let unit = [-1.264_911, -0.632_456, 0.0, 0.632_456, 1.264_911];
        let base = vec![1000.0; unit.len()];
        let head: Vec<f64> = unit
            .iter()
            .map(|z| 1000.0 * (1.0 + 0.0902 + 0.0834 * z))
            .collect();

        let out = wall(&base, &head);
        assert_eq!(out.replicates, 5);
        assert!(out.delta > DEFAULT_FLOOR, "{}", out.delta);
        assert!(
            out.ci_low > 0.0,
            "the interval excludes zero: {}",
            out.ci_low
        );
        assert_eq!(
            out.verdict,
            Verdict::NoVerdict,
            "an interval wider than the floor is not a verdict"
        );

        // The count it names is even, larger than what was run, and enough:
        // re-measuring that many times puts the interval inside the floor.
        let needed = out.replicates_needed.expect("a count");
        assert_eq!(needed % 2, 0, "{needed}");
        assert!(needed > out.replicates, "{needed}");
        let half = (out.ci_high - out.ci_low) / 2.0;
        assert!(super::replicates_for(half, out.replicates, out.floor) == Some(needed));

        // And one step below it is not enough, so the count is the smallest
        // that works rather than a round number.
        assert!(
            needed == MIN_BALANCED_REPLICATES || {
                let shorter = needed - 2;
                super::t_quantile(shorter - 1) / super::t_quantile(out.replicates - 1)
                    * (out.replicates as f64 / shorter as f64).sqrt()
                    * half
                    >= out.floor
            }
        );
    }

    /// `replicates_for` is public and takes the count it measured as an
    /// argument, so it answers for counts `analyse` never hands it.
    #[test]
    fn a_count_the_rule_never_produces_is_still_answered() {
        for n in [0, 1, 2, MIN_REPLICATES, MAX_SUGGESTED_REPLICATES] {
            let answer = replicates_for(0.2, n, DEFAULT_FLOOR);
            assert!(
                answer.is_none_or(|m| m % 2 == 0 && m <= MAX_SUGGESTED_REPLICATES),
                "at {n}: {answer:?}"
            );
        }
        // A spread already inside the floor asks for the smallest count that is
        // judgeable and balanced, rather than for the count it was measured at.
        assert_eq!(
            replicates_for(0.0, 40, DEFAULT_FLOOR),
            Some(MIN_BALANCED_REPLICATES)
        );
    }

    /// Samples with a given mean and sample standard deviation, so a fixture
    /// can name the interval it wants rather than being tuned to it.
    fn shaped(n: usize, mean_delta: f64, sd: f64) -> (Vec<f64>, Vec<f64>) {
        let count = n as f64;
        let centred: Vec<f64> = (0..n).map(|k| (k as f64) - (count - 1.0) / 2.0).collect();
        let scale = (centred.iter().map(|z| z * z).sum::<f64>() / (count - 1.0)).sqrt();
        let head = centred
            .iter()
            .map(|z| 1000.0 * (1.0 + mean_delta + sd * z / scale))
            .collect();
        (vec![1000.0; n], head)
    }

    /// A difference the floor has separated is judged however wide the interval
    /// is. A width test applied first declines a 40% regression whose interval
    /// runs from +34% to +45%, which is the largest movement a run can find.
    #[test]
    fn an_interval_wholly_past_the_floor_is_judged_at_any_width() {
        for (n, delta, sd) in [(20, 0.40, 0.145), (6, 0.15, 0.070), (20, -0.40, 0.145)] {
            let (base, head) = shaped(n, delta, sd);
            let out = wall(&base, &head);
            let half = (out.ci_high - out.ci_low) / 2.0;
            assert!(
                half >= out.floor,
                "the fixture must be wider than the floor: {half} at n={n}"
            );
            assert!(
                out.ci_low > out.floor || out.ci_high < -out.floor,
                "and wholly past it: [{}, {}]",
                out.ci_low,
                out.ci_high
            );
            let expected = if delta > 0.0 {
                Verdict::Regressed
            } else {
                Verdict::Improved
            };
            assert_eq!(out.verdict, expected, "at n={n} delta={delta}");
            assert_eq!(out.replicates_needed, None);
        }
    }

    /// And the measured rows that made two A/A runs of one commit disagree stay
    /// declined. Each straddles the floor rather than clearing it, so the rule
    /// above does not reach them.
    ///
    /// Given as a difference and a spread rather than as the intervals the
    /// report carried, because those were cut before [`widening`] existed. The
    /// spread behind a reported half-width `h` at five pairs is `h * 2.5 / z`.
    #[test]
    fn the_spreads_that_made_two_a_a_runs_disagree_are_still_declined() {
        for (case, delta, sd) in [
            ("err_truncated_value · wall_ns_per_iter", 0.0666, 0.0791),
            ("raw_batch_serde · wall_ns_per_iter", 0.0902, 0.0834),
            ("resolved_writer_only · wall_ns_per_iter", 0.0853, 0.0753),
        ] {
            let (base, head) = shaped(5, delta, sd);
            let out = wall(&base, &head);
            assert_eq!(
                out.verdict,
                Verdict::NoVerdict,
                "{case}: [{}, {}]",
                out.ci_low,
                out.ci_high
            );
            assert!(out.replicates_needed.is_some(), "{case}");
        }
    }

    /// A spread nothing on this machine will resolve names no count at all,
    /// rather than a number nobody will run.
    #[test]
    fn a_spread_beyond_reach_names_no_count() {
        let base = vec![100.0; 8];
        let head = vec![5.0, 400.0, 8.0, 350.0, 12.0, 380.0, 6.0, 420.0];
        let out = wall(&base, &head);
        assert_eq!(out.verdict, Verdict::NoVerdict);
        assert_eq!(out.replicates_needed, None);
    }

    /// A difference well past the floor, measured precisely, is still a
    /// finding: the gate is about the interval, not about the difference.
    #[test]
    fn a_precise_measurement_is_still_judged() {
        let base = jittered(1000.0, 12);
        let head: Vec<f64> = base.iter().map(|v| v * 1.09).collect();
        let out = wall(&base, &head);
        assert!((out.ci_high - out.ci_low) / 2.0 < out.floor);
        assert_eq!(out.verdict, Verdict::Regressed);
        assert_eq!(out.replicates_needed, None);
    }

    /// The report must be reproducible from the same two legs, or a number
    /// no one can quote twice ends up in a pull request comment.
    #[test]
    fn the_bootstrap_is_bit_identical_across_runs() {
        let base = jittered(1000.0, 9);
        let head: Vec<f64> = base.iter().map(|v| v * 1.07).collect();
        let first = wall(&base, &head);
        let again = wall(&base, &head);
        assert_eq!(first.ci_low.to_bits(), again.ci_low.to_bits());
        assert_eq!(first.ci_high.to_bits(), again.ci_high.to_bits());
        assert_eq!(first.delta.to_bits(), again.delta.to_bits());
    }

    /// The factor at the two counts the rule is read at, against `t(0.95, n-1)`
    /// and `z(0.95)` computed elsewhere. A table entry mistyped by a digit
    /// changes an interval on every report and nothing else notices.
    #[test]
    fn the_widening_matches_the_critical_values_it_is_built_from() {
        assert!((widening(5) - 1.449_051).abs() < 1e-6, "{}", widening(5));
        assert!((widening(20) - 1.078_548).abs() < 1e-6, "{}", widening(20));
        assert!((widening(2) - 5.428_442).abs() < 1e-6, "{}", widening(2));

        // Strictly decreasing, always widening, and gone in the limit. The
        // expansion takes over past thirty degrees of freedom and must not step
        // when it does.
        let mut previous = f64::INFINITY;
        for n in 2..=2000 {
            let factor = widening(n);
            assert!(factor > 1.0, "at {n}: {factor}");
            assert!(factor < previous, "at {n}: {factor} then {previous}");
            previous = factor;
        }
        assert!(widening(2000) < 1.001, "{}", widening(2000));

        // The seam is a join rather than a step: the last tabulated value and
        // the first computed one differ by less than the gap before them.
        let step_before = widening(30) - widening(31);
        let step_across = widening(31) - widening(32);
        assert!(step_across < step_before, "{step_across} {step_before}");

        // Every count produces a finite factor. An infinity here reaches a
        // report as an interval spanning everything, and the table lookup is one
        // index away from returning one.
        for n in [2, 3, 5, 30, 31, 200, usize::MAX] {
            assert!(widening(n).is_finite(), "at {n}");
        }
        assert!(super::t_quantile(0).is_finite());
    }

    /// The interval a sample supports, end to end. Twelve deltas of known
    /// spread, against `t(0.95, 11) * s / sqrt(12)` computed by hand: the
    /// bootstrap's own Monte-Carlo error is what the tolerance covers.
    #[test]
    fn the_interval_is_the_width_the_sample_supports() {
        let deltas: Vec<f64> = (0..12).map(|k| 0.10 * (f64::from(k) - 5.5)).collect();
        let base = vec![1000.0; deltas.len()];
        let head: Vec<f64> = deltas.iter().map(|d| 1000.0 * (1.0 + d)).collect();
        let out = wall(&base, &head);

        let count = deltas.len() as f64;
        let mean_delta = deltas.iter().sum::<f64>() / count;
        let variance = deltas.iter().map(|d| (d - mean_delta).powi(2)).sum::<f64>() / (count - 1.0);
        let expected = 1.795_884_819 * variance.sqrt() / count.sqrt();

        let half = (out.ci_high - out.ci_low) / 2.0;
        assert!(
            (half - expected).abs() < 0.06 * expected,
            "half {half} against {expected}"
        );
    }

    /// The estimate stays inside its own interval, and the interval stays put
    /// when the differences carry no spread at all.
    #[test]
    fn widening_moves_the_ends_and_not_the_estimate() {
        let base = jittered(1000.0, 8);
        let head: Vec<f64> = base.iter().map(|v| v * 1.03).collect();
        let out = wall(&base, &head);
        assert!(out.ci_low < out.delta && out.delta < out.ci_high);

        let flat = vec![500.0; 8];
        let still = wall(&flat, &flat);
        assert!(still.ci_low.abs() < f64::EPSILON && still.ci_high.abs() < f64::EPSILON);
    }

    #[test]
    fn the_floors_are_per_metric() {
        assert!((floor_for(WALL_NS_PER_ITER) - DEFAULT_FLOOR).abs() < f64::EPSILON);
        assert!((floor_for(PEAK_RSS_BYTES) - RSS_FLOOR).abs() < f64::EPSILON);
        assert!((floor_for(ALLOC_BYTES_PER_ITER) - ALLOC_FLOOR).abs() < f64::EPSILON);

        // A three-percent allocation change is a finding; the same change in
        // wall time is not.
        let base = jittered(1000.0, 10);
        let head: Vec<f64> = base.iter().map(|v| v * 1.03).collect();
        let alloc = analyse(ALLOC_BYTES_PER_ITER, false, &base, &head, 5).expect("analyses");
        assert_eq!(alloc.verdict, Verdict::Regressed);
        assert_eq!(wall(&base, &head).verdict, Verdict::NoChange);
    }

    #[test]
    fn unpaired_or_degenerate_samples_are_errors_rather_than_numbers() {
        assert!(analyse(WALL_NS_PER_ITER, false, &[1.0, 2.0], &[1.0], 1).is_err());
        assert!(analyse(WALL_NS_PER_ITER, false, &[], &[], 1).is_err());
        let err = analyse(WALL_NS_PER_ITER, false, &[1.0], &[f64::NAN], 1).expect_err("not finite");
        assert!(err.contains("head leg"), "{err}");

        // A change from nothing has no relative size, and the message has to
        // say which way round it was.
        let err =
            analyse(ALLOC_BYTES_PER_ITER, false, &[0.0; 6], &[1.0; 6], 1).expect_err("undefined");
        assert!(err.contains("from nothing"), "{err}");
    }

    /// A non-allocating case reports zero allocations on both legs, every
    /// replicate. That is no change, not an undefined ratio and not a line of
    /// noise in every report.
    #[test]
    fn zero_on_both_legs_is_no_change() {
        let out = analyse(ALLOC_BYTES_PER_ITER, false, &[0.0; 8], &[0.0; 8], 1).expect("analyses");
        assert_eq!(out.verdict, Verdict::NoChange);
        assert!((out.delta).abs() < f64::EPSILON);
        assert!((out.ci_low).abs() < f64::EPSILON && (out.ci_high).abs() < f64::EPSILON);
        assert_eq!(out.replicates, 8);
    }

    /// The pair, not the sample, decides. One replicate that allocated must not
    /// turn the seven that did not into an error, which is what a
    /// whole-sample "all zero" test does, and it is reachable whenever a case
    /// allocates on some replicates only.
    #[test]
    fn a_zero_pair_beside_a_non_zero_one_is_still_no_change_for_that_pair() {
        let mut base = vec![0.0; 8];
        let mut head = vec![0.0; 8];
        base[3] = 100.0;
        head[3] = 100.0;
        let out = analyse(ALLOC_BYTES_PER_ITER, false, &base, &head, 1).expect("analyses");
        assert_eq!(out.verdict, Verdict::NoChange);
        assert!(out.delta.abs() < f64::EPSILON);

        // And the asymmetric pair is still the error it should be.
        head[5] = 1.0;
        assert!(analyse(ALLOC_BYTES_PER_ITER, false, &base, &head, 1).is_err());
    }

    #[test]
    fn seeds_differ_by_case_and_by_metric() {
        assert_ne!(seed_for("a", "wall"), seed_for("b", "wall"));
        assert_ne!(seed_for("a", "wall"), seed_for("a", "cpu"));
        assert_eq!(seed_for("a", "wall"), seed_for("a", "wall"));
        // The separator matters: without it, ("ab","c") and ("a","bc") collide.
        assert_ne!(seed_for("ab", "c"), seed_for("a", "bc"));
    }

    #[test]
    fn verdict_labels_and_significance_agree() {
        assert!(Verdict::Improved.is_significant());
        assert!(Verdict::Regressed.is_significant());
        assert!(!Verdict::NoChange.is_significant());
        assert!(!Verdict::NoVerdict.is_significant());
        assert_eq!(Verdict::NoVerdict.label(), "no verdict");
    }

    /// The two renderings are deliberately different strings. A token that
    /// drifts back to its label would go on parsing until a consumer split it.
    #[test]
    fn a_verdict_reads_one_way_for_humans_and_another_for_scripts() {
        assert_eq!(Verdict::Improved.token(), "improved");
        assert_eq!(Verdict::Regressed.token(), "regressed");
        assert_eq!(Verdict::NoChange.token(), "no_change");
        assert_eq!(Verdict::NoVerdict.token(), "no_verdict");

        for verdict in [
            Verdict::Improved,
            Verdict::Regressed,
            Verdict::NoChange,
            Verdict::NoVerdict,
        ] {
            assert!(!verdict.token().contains(' '), "{}", verdict.token());
        }
        assert!(Verdict::NoChange.label().contains(' '));
        assert!(Verdict::NoVerdict.label().contains(' '));
    }
}
