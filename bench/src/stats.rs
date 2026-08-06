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
//! Fewer than five paired replicates prints the difference and declines to judge
//! it. Below that, the tail of a bootstrap is decided by which single pair was
//! drawn least often, so an interval would be an assertion about the resampler
//! rather than about the code. The default is ten.
//!
//! # Pairing, and why it is not a ratio of means
//!
//! An A/B run interleaves the legs, so replicate *k* of each ran adjacent in
//! time and shared whatever the machine was doing at that moment. The
//! statistic is therefore computed *per pair* — `d_k = (head_k − base_k) /
//! base_k` — and averaged. Dividing the mean of one leg by the mean of the
//! other throws that away: it is the same arithmetic on drift-free data and a
//! strictly worse estimate on real data, where the whole reason for
//! interleaving is that the machine drifts.
//!
//! # Why a bootstrap
//!
//! The per-pair differences are not normal, there are ten of them, and their
//! distribution is decided by the machine rather than by anything modellable.
//! The percentile bootstrap asks the data what its own spread is, which needs
//! no distributional assumption and no history — and there is no history here
//! by design.
//!
//! The resampling is seeded from the case and metric names, so re-rendering the
//! same two legs produces the same interval to the last digit. A report whose
//! numbers move when it is regenerated is a report nobody can quote. It draws
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

use crate::record::{ALLOC_BYTES_PER_ITER, ALLOC_COUNT_PER_ITER, PEAK_RSS_BYTES};
use crate::rng::SplitMix64;

/// Below this many paired replicates, a difference is printed without a
/// verdict.
///
/// Five is where a percentile bootstrap starts to have anything to resample:
/// with four pairs the 5th percentile of the resampled means is decided by
/// which single pair was drawn least often.
pub const MIN_REPLICATES: usize = 5;

/// Resamples per bootstrap. Enough that the interval's own Monte-Carlo error is
/// far below the floors below, and cheap enough to run for every metric of
/// every case.
pub const BOOTSTRAP_RESAMPLES: usize = 10_000;

/// The interval's coverage.
pub const CONFIDENCE: f64 = 0.90;

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
    /// Too few paired replicates to judge. The difference is still printed.
    NoVerdict,
}

impl Verdict {
    /// Whether this verdict belongs in the significant-changes table.
    #[must_use]
    pub const fn is_significant(self) -> bool {
        matches!(self, Self::Improved | Self::Regressed)
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
}

/// Compares one metric's paired samples.
///
/// `base` and `head` are already paired by replicate index — element *k* of
/// each is replicate *k* — which is the caller's job, because only the caller
/// knows which records went missing.
///
/// # Errors
///
/// When the samples are not the same length (the caller failed to pair them),
/// when there are none, when one is not finite, or when a base value is zero
/// while its partner is not — a change from nothing has no relative size, and
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
            // Zero on both legs is a real and common answer — a non-allocating
            // case reports zero allocations every replicate — and it is *no
            // change*, not an undefined ratio. Judged per pair rather than over
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

    let verdict = if n < MIN_REPLICATES {
        Verdict::NoVerdict
    } else if (ci_low > 0.0 || ci_high < 0.0) && delta.abs() >= floor {
        // The direction of *goodness*, not the sign: a throughput that rose is
        // an improvement, a latency that rose is a regression.
        if (delta > 0.0) == higher_is_better {
            Verdict::Improved
        } else {
            Verdict::Regressed
        }
    } else {
        Verdict::NoChange
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
    })
}

/// The bootstrap's seed for one (case, metric) pair.
///
/// Derived from the names rather than from a clock, so the same two legs
/// rendered twice give the same interval — and so two metrics of one case do
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

/// The percentile bootstrap on the mean of `deltas`.
fn bootstrap_ci(deltas: &[f64], seed: u64) -> (f64, f64) {
    let n = deltas.len();
    if n == 1 {
        // One pair has no spread to resample: every resample is that pair, so
        // the interval is the point itself. Reported rather than special-cased
        // away — `MIN_REPLICATES` is what stops it becoming a verdict.
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
    (percentile(&means, tail), percentile(&means, 1.0 - tail))
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
        ALLOC_FLOOR, Analysis, DEFAULT_FLOOR, MIN_REPLICATES, RSS_FLOOR, Verdict, analyse,
        floor_for, seed_for,
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
    #[test]
    fn a_large_difference_with_an_interval_spanning_zero_is_not_a_finding() {
        let base = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0];
        let head = vec![40.0, 190.0, 60.0, 170.0, 50.0, 180.0];
        let out = wall(&base, &head);
        assert!(out.delta.abs() > DEFAULT_FLOOR);
        assert!(out.ci_low < 0.0 && out.ci_high > 0.0);
        assert_eq!(out.verdict, Verdict::NoChange);
    }

    #[test]
    fn too_few_replicates_report_a_delta_and_no_verdict() {
        let base = jittered(1000.0, MIN_REPLICATES - 1);
        let head: Vec<f64> = base.iter().map(|v| v * 2.0).collect();
        let out = wall(&base, &head);
        assert_eq!(out.replicates, MIN_REPLICATES - 1);
        assert_eq!(out.verdict, Verdict::NoVerdict);
        assert!((out.delta - 1.0).abs() < 1e-9, "{}", out.delta);
    }

    /// The report must be reproducible from the same two legs, or a number
    /// nobody can quote twice ends up in a pull request comment.
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
    /// replicate. That is no change — not an undefined ratio, and not a line of
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
    /// turn the seven that did not into an error — which is what a
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
}
