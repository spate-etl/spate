//! Summarising a set of repetitions into one number and an interval.
//!
//! Lifted out of the `deser_formats` rig so every rig that repeats a
//! measurement summarises it the same way. Two rigs with two definitions of
//! "the number" is how a comparison ends up measuring the summary rather than
//! the code.
//!
//! The interval is Student-t rather than the normal approximation, because a
//! benchmark repeats five or ten times, not a thousand: at those sizes 1.96 is
//! optimistic by a wide margin (t(4) is 2.776), and an interval that is too
//! narrow is the kind of error that makes noise look like a result.

/// Student-t critical value t(df, 0.975).
///
/// Tabulated to 30 degrees of freedom, then t(30) = 2.042 for anything larger.
/// The table does **not** converge to 1.96 anywhere near its end — t(16) is
/// still 2.120 — so falling back to the normal value would emit an interval
/// 6–8% too narrow at twenty repetitions, which is exactly the failure this
/// module exists to avoid. 2.042 is above the true value for every df past 30
/// and approaches 1.96 from there, so the fallback errs wide: a benchmark that
/// repeats more than thirty times gets a slightly conservative interval rather
/// than a slightly false one.
fn t_975(df: usize) -> f64 {
    const TABLE: [f64; 30] = [
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
        2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
        2.052, 2.048, 2.045, 2.042,
    ];
    match df {
        0 => 0.0,
        d if d <= TABLE.len() => TABLE[d - 1],
        _ => TABLE[TABLE.len() - 1],
    }
}

/// `(mean, ci95_low, ci95_high)` over `xs`.
///
/// A single sample reports itself with a zero-width interval. That is the
/// arithmetic truth — one observation has no spread to report — but it is not
/// a *confidence* interval, and a consumer that draws `ci95` without also
/// reading `n` renders it as false precision. Callers should attach `n`
/// alongside, and omit the interval entirely when `n < 2`; the rigs here do.
#[must_use]
pub fn stats(xs: &[f64]) -> (f64, f64, f64) {
    let n = xs.len();
    if n == 0 {
        return (0.0, 0.0, 0.0);
    }
    let nf = n as f64;
    let mean = xs.iter().sum::<f64>() / nf;
    if n < 2 {
        return (mean, mean, mean);
    }
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (nf - 1.0);
    let sem = (var / nf).sqrt();
    let t = t_975(n - 1);
    (mean, mean - t * sem, mean + t * sem)
}

/// The median of `xs`, averaging the two middle values for an even count.
///
/// # Panics
///
/// If any sample is not comparable — a NaN timing is a bug in the rig that
/// produced it, not something to average over.
#[must_use]
pub fn median(xs: &[f64]) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let n = v.len();
    if n == 0 {
        0.0
    } else if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

#[cfg(test)]
mod tests {
    use super::{median, stats};

    /// The fallback is the tail of the table, not the normal approximation:
    /// at twenty repetitions the difference is a real 6–8% of the interval's
    /// width, in the direction that makes noise look like a result.
    #[test]
    fn the_fallback_stays_above_the_normal_value() {
        let xs: Vec<f64> = (0..40).map(|i| 10.0 + f64::from(i % 3)).collect();
        let (mean, _, hi) = stats(&xs);
        let n = xs.len() as f64;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let normal = 1.96 * (var / n).sqrt();
        assert!(
            (hi - mean) > normal,
            "df past the table must not fall back to 1.96"
        );
    }

    #[test]
    fn a_single_sample_is_its_own_interval() {
        let (mean, lo, hi) = stats(&[4.0]);
        assert!((mean - 4.0).abs() < f64::EPSILON);
        assert!((lo - 4.0).abs() < f64::EPSILON);
        assert!((hi - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn identical_samples_have_a_zero_width_interval() {
        let (mean, lo, hi) = stats(&[7.0; 5]);
        assert!((mean - 7.0).abs() < 1e-12);
        assert!((hi - lo).abs() < 1e-12);
    }

    /// The reason this module exists rather than a normal approximation: at
    /// five repetitions the Student-t interval is materially wider, and a
    /// too-narrow interval is what turns noise into an apparent result.
    #[test]
    fn the_interval_is_wider_than_a_normal_approximation() {
        let xs = [10.0, 11.0, 9.0, 10.5, 9.5];
        let (mean, lo, hi) = stats(&xs);
        let n = xs.len() as f64;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let normal_half_width = 1.96 * (var / n).sqrt();
        assert!(
            (hi - mean) > normal_half_width,
            "t(4)=2.776 should exceed 1.96"
        );
        assert!(
            (mean - lo - (hi - mean)).abs() < 1e-12,
            "interval is symmetric"
        );
    }

    #[test]
    fn the_median_averages_the_middle_pair_for_an_even_count() {
        assert!((median(&[1.0, 2.0, 3.0, 4.0]) - 2.5).abs() < f64::EPSILON);
        assert!((median(&[3.0, 1.0, 2.0]) - 2.0).abs() < f64::EPSILON);
    }
}
