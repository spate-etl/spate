//! Capped exponential backoff with deterministic pseudo-jitter, for retry
//! loops in the engine and in connectors.

use crate::rng::SplitMix64;
use std::time::Duration;

/// A capped exponential sequence of retry delays.
///
/// [`new`](Backoff::new) starts at `initial` and doubles each step up to
/// `max`. [`with_multiplier`](Backoff::with_multiplier) sets another growth
/// factor. [`with_jitter`](Backoff::with_jitter) shortens each delay by a
/// random part of up to `fraction`, so a delay lies in
/// `[(1 - fraction) × step, step]` and is never shortened to zero.
///
/// Jitter draws from a xorshift state set by
/// [`with_seed`](Backoff::with_seed), with no dependency on a random-number
/// crate. Instances seeded alike retry together, and instances seeded apart
/// spread out.
///
/// The constructor and the setters are `const`, so a schedule can be declared
/// once as a `const` item. Each use of the item starts a fresh sequence.
///
/// # Examples
///
/// ```
/// use spate_core::backoff::Backoff;
/// use std::time::Duration;
///
/// const SCHEDULE: Backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(5));
///
/// let mut backoff = SCHEDULE;
/// assert_eq!(backoff.next_delay(), Duration::from_millis(200));
/// assert_eq!(backoff.next_delay(), Duration::from_millis(400));
/// backoff.reset();
/// assert_eq!(backoff.next_delay(), Duration::from_millis(200));
///
/// let mut jittered = SCHEDULE.with_jitter(0.2).with_seed(7);
/// let delay = jittered.next_delay();
/// assert!(Duration::from_millis(160) <= delay && delay <= Duration::from_millis(200));
/// ```
// No `Copy`: an implicit copy would fork the sequence's state.
#[derive(Clone, Debug)]
pub struct Backoff {
    start: Duration,
    max: Duration,
    multiplier: f64,
    jitter: f64,
    current: Duration,
    rng: u64,
}

/// The xorshift state for `seed`: the first SplitMix64 draw, which avalanches
/// adjacent seeds, so seed `n` and seed `n + 1` start from unrelated states.
const fn rng_state(seed: u64) -> u64 {
    // Seeds are often consecutive sequence numbers. XOR-ing the seed into the
    // state instead of mixing it leaves every seed's first draw inside the
    // same thousandth of the jitter window, and relocates xorshift's
    // absorbing zero state rather than removing it.
    match SplitMix64::new(seed).next_u64() {
        0 => 0x9E37_79B9_7F4A_7C15,
        mixed => mixed,
    }
}

impl Backoff {
    /// A backoff starting at `initial` and doubling up to `max`, without
    /// jitter.
    ///
    /// An `initial` above `max` starts at `max`.
    #[must_use]
    pub const fn new(initial: Duration, max: Duration) -> Self {
        // `Duration`'s ordering is not usable in a `const fn`.
        let start = if initial.as_nanos() <= max.as_nanos() {
            initial
        } else {
            max
        };
        Backoff {
            start,
            max,
            multiplier: 2.0,
            jitter: 0.0,
            current: start,
            rng: rng_state(0),
        }
    }

    /// This backoff, growing each step by `multiplier`.
    ///
    /// A multiplier below `1.0`, or NaN, reaches `max` on the second delay.
    #[must_use]
    pub const fn with_multiplier(self, multiplier: f64) -> Self {
        Backoff { multiplier, ..self }
    }

    /// This backoff, shortening each delay by a random part of up to
    /// `fraction` of it.
    ///
    /// `1.0` is full jitter. A fraction above `1.0` acts as `1.0`, and a
    /// negative or non-finite one as no jitter. Instances draw alike unless
    /// each takes its own [`with_seed`](Backoff::with_seed).
    #[must_use]
    pub const fn with_jitter(self, fraction: f64) -> Self {
        Backoff {
            jitter: fraction,
            ..self
        }
    }

    /// This backoff, drawing its jitter from `seed`.
    ///
    /// An instance given no seed draws as seed `0`.
    #[must_use]
    pub const fn with_seed(self, seed: u64) -> Self {
        Backoff {
            rng: rng_state(seed),
            ..self
        }
    }

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    /// The delay to wait before the next attempt.
    ///
    /// Total for any input to the constructor and the setters, with
    /// [`reset`](Backoff::reset) called at any point:
    ///
    /// * the delay never exceeds `max`;
    /// * it is never zero unless `min(initial, max)` is itself zero, so it
    ///   cannot degenerate into a zero-delay hot-retry loop against a failing
    ///   backend;
    /// * it never panics.
    ///
    /// `max` may lie past the range of an `Instant`, so a caller that adds
    /// the delay to one uses `checked_add`.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.current;
        let cap = self.max;
        // Grow in `f64` seconds and clamp *before* rebuilding the `Duration`:
        // `Duration::mul_f64` panics on an overflowing, non-finite or negative
        // result. A multiplier outside the growing range jumps straight to the
        // ceiling, NaN included, for which the comparison is false.
        let grown = if self.multiplier >= 1.0 {
            (base.as_secs_f64() * self.multiplier).min(cap.as_secs_f64())
        } else {
            cap.as_secs_f64()
        };
        // `cap.as_secs_f64()` can itself round up past `Duration::MAX`, which
        // the trailing `min` absorbs.
        self.current = Duration::try_from_secs_f64(grown).unwrap_or(cap).min(cap);

        // Bound the *fraction*, not the factor computed from it: clamping the
        // factor lets an out-of-range `jitter` land on exactly `0.0` and erase
        // the delay outright. `f64::clamp` propagates NaN instead of clamping
        // it, hence the explicit finite check.
        let jitter = if self.jitter.is_finite() {
            self.jitter.clamp(0.0, 1.0)
        } else {
            0.0
        };
        if jitter <= 0.0 {
            return base;
        }
        let unit = (self.next_rand() >> 11) as f64 / (1u64 << 53) as f64;
        let jittered = base.as_secs_f64() * (1.0 - jitter * unit);
        // Round-tripping a near-`Duration::MAX` base through `f64` rounds *up*
        // past `Duration::MAX`, even at a factor of exactly 1.0, so `base` is
        // both the fallback and the ceiling. The floor keeps a sub-nanosecond
        // product from rounding down to a zero delay.
        let floor = base.min(Duration::from_nanos(1));
        Duration::try_from_secs_f64(jittered)
            .unwrap_or(base)
            .clamp(floor, base)
    }

    /// Starts the schedule again from `initial`, or from `max` when `initial`
    /// is above it.
    ///
    /// The jitter draw continues from its current state.
    pub fn reset(&mut self) {
        self.current = self.start;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::RetryConfig;
    use proptest::num;
    use proptest::prelude::*;

    fn backoff(jitter: f64, seed: u64) -> Backoff {
        Backoff::new(Duration::from_millis(100), Duration::from_millis(400))
            .with_jitter(jitter)
            .with_seed(seed)
    }

    fn policy(initial: Duration, max: Duration, multiplier: f64, jitter: f64) -> Backoff {
        Backoff::new(initial, max)
            .with_multiplier(multiplier)
            .with_jitter(jitter)
    }

    fn millis(b: &mut Backoff, steps: usize) -> Vec<u128> {
        (0..steps).map(|_| b.next_delay().as_millis()).collect()
    }

    #[test]
    fn grows_exponentially_and_caps_without_jitter() {
        let mut b = backoff(0.0, 7);
        assert_eq!(b.next_delay(), Duration::from_millis(100));
        assert_eq!(b.next_delay(), Duration::from_millis(200));
        assert_eq!(b.next_delay(), Duration::from_millis(400));
        assert_eq!(b.next_delay(), Duration::from_millis(400), "capped");
    }

    #[test]
    fn doubling_from_200ms_holds_at_the_5s_cap() {
        let mut b = Backoff::new(Duration::from_millis(200), Duration::from_secs(5));
        assert_eq!(millis(&mut b, 7), [200, 400, 800, 1600, 3200, 5000, 5000]);
        let mut b = Backoff::new(Duration::from_millis(10), Duration::from_millis(250));
        assert_eq!(millis(&mut b, 7), [10, 20, 40, 80, 160, 250, 250]);
    }

    #[test]
    fn reset_restarts_the_schedule_below_the_ceiling() {
        let mut b = backoff(0.0, 7);
        assert_eq!(millis(&mut b, 3), [100, 200, 400]);
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_millis(100));

        let mut b = Backoff::new(Duration::from_secs(10), Duration::from_secs(1));
        b.next_delay();
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_secs(1), "clamped to max");

        let mut b = backoff(0.2, 7);
        millis(&mut b, 3);
        b.reset();
        let d = b.next_delay();
        assert!(
            Duration::from_millis(80) <= d && d <= Duration::from_millis(100),
            "{d:?}"
        );
    }

    #[test]
    fn a_zero_initial_returns_zero_without_panicking() {
        for jitter in [0.0, 0.5] {
            let mut b = Backoff::new(Duration::ZERO, Duration::from_secs(5)).with_jitter(jitter);
            assert_eq!(b.next_delay(), Duration::ZERO, "{jitter}");
            b.reset();
            assert_eq!(b.next_delay(), Duration::ZERO, "{jitter}");
        }
    }

    #[test]
    fn an_unseeded_backoff_draws_as_seed_zero() {
        let full = Duration::from_millis(100);
        let mut unseeded = Backoff::new(full, full).with_jitter(1.0);
        let mut zero = Backoff::new(full, full).with_jitter(1.0).with_seed(0);
        let drawn = millis(&mut unseeded, 8);
        assert_eq!(drawn, millis(&mut zero, 8));
        assert!(drawn.iter().any(|&d| d < 100), "no draw shortened");
    }

    #[test]
    fn the_seed_splitmix64_maps_to_zero_still_jitters() {
        // This seed's first SplitMix64 draw is 0, xorshift's absorbing state.
        let full = Duration::from_millis(100);
        let mut b = Backoff::new(full, full)
            .with_jitter(1.0)
            .with_seed(0x61C8_8646_80B5_83EB);
        assert!((0..8).any(|_| b.next_delay() < full), "no draw shortened");
    }

    /// The constructor and every setter evaluate in a `const` item.
    const FULLY_CONFIGURED: Backoff =
        Backoff::new(Duration::from_millis(200), Duration::from_secs(5))
            .with_multiplier(3.0)
            .with_jitter(0.2)
            .with_seed(7);

    #[test]
    fn a_fully_configured_schedule_is_a_const_item() {
        let mut b = FULLY_CONFIGURED;
        let d = b.next_delay();
        assert!(Duration::from_millis(160) <= d && d <= Duration::from_millis(200));
        b.reset();
        b.next_delay();
        let d = b.next_delay();
        assert!(Duration::from_millis(480) <= d && d <= Duration::from_millis(600));
    }

    #[test]
    fn jitter_stays_within_the_configured_fraction() {
        let mut b = backoff(0.2, 42);
        for expected_ms in [100u64, 200, 400, 400] {
            let d = b.next_delay();
            let full = Duration::from_millis(expected_ms);
            assert!(d <= full, "jitter only shortens: {d:?} > {full:?}");
            assert!(
                d >= full.mul_f64(0.8),
                "at most 20% removed: {d:?} < 0.8 * {full:?}"
            );
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = backoff(0.5, 1);
        let mut b = backoff(0.5, 2);
        let same = (0..8).filter(|_| a.next_delay() == b.next_delay()).count();
        assert!(same < 8, "two seeds should not produce identical jitter");
    }

    #[test]
    fn extreme_max_and_multiplier_do_not_overflow() {
        // A policy both sink validators accept; without the clamp, growth past
        // `max` overflows `Duration` and panics the write task.
        let max = Duration::from_secs(315_576_000_000);
        let mut b = policy(Duration::from_secs(1), max, 1e9, 0.0).with_seed(3);
        for _ in 0..8 {
            assert!(b.next_delay() <= max);
        }
    }

    #[test]
    fn a_near_max_duration_ceiling_saturates() {
        // `Duration::MAX.as_secs_f64()` rounds *up* past `Duration::MAX`, so
        // the clamped f64 is not necessarily a representable ceiling.
        let mut b = policy(Duration::from_secs(1), Duration::MAX, 1e9, 0.5).with_seed(11);
        for _ in 0..8 {
            assert!(b.next_delay() <= Duration::MAX);
        }
    }

    #[test]
    fn a_pathological_multiplier_lands_on_the_ceiling() {
        // `0.0`, `-0.0` and subnormals are the collapsing cases, pinning the
        // delay at 0ns forever; `-0.0` reaches that through
        // `try_from_secs_f64(-0.0) == Ok(0ns)`, not a failed conversion.
        let (initial, max) = (Duration::from_millis(100), Duration::from_secs(10));
        for multiplier in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -1.0,
            0.0,
            -0.0,
            f64::MIN_POSITIVE,
            0.5,
        ] {
            let mut b = policy(initial, max, multiplier, 0.0).with_seed(5);
            assert_eq!(b.next_delay(), initial, "{multiplier}");
            assert_eq!(b.next_delay(), max, "{multiplier}");
            assert_eq!(b.next_delay(), max, "{multiplier}");
        }
    }

    #[test]
    fn an_out_of_range_jitter_never_lengthens_the_delay() {
        for jitter in [1.5, -0.5, f64::NAN, f64::INFINITY] {
            let mut b = policy(
                Duration::from_millis(100),
                Duration::from_secs(10),
                2.0,
                jitter,
            )
            .with_seed(5);
            for expected in [Duration::from_millis(100), Duration::from_millis(200)] {
                assert!(b.next_delay() <= expected, "{jitter}");
            }
        }
    }

    #[test]
    fn jitter_never_erases_the_delay() {
        // Clamping the *factor* to `[0.0, 1.0]` lets a `jitter` above 1.0 land
        // on exactly 0.0, turning roughly 40% of sleeps into zero-delay
        // retries. The last case needs no invalid input: `initial == max == 1ns`
        // with `jitter: 1.0` passes `validate` and rounds most delays to zero.
        for (initial, max, jitter) in [
            (Duration::from_millis(100), Duration::from_secs(10), 1.5),
            (Duration::from_millis(100), Duration::from_secs(10), 1e300),
            (
                Duration::from_millis(100),
                Duration::from_secs(10),
                f64::INFINITY,
            ),
            (Duration::from_nanos(1), Duration::from_nanos(1), 1.0),
        ] {
            for seed in 0..256 {
                let mut b = policy(initial, max, 2.0, jitter).with_seed(seed);
                for _ in 0..8 {
                    let d = b.next_delay();
                    assert!(!d.is_zero(), "zero delay: jitter {jitter}, seed {seed}");
                    assert!(d <= max, "{d:?} exceeds {max:?}");
                }
            }
        }
    }

    #[test]
    fn an_initial_above_the_ceiling_starts_at_the_ceiling() {
        // Without the clamp the first delay is `initial`, ten times the ceiling.
        let max = Duration::from_secs(1);
        let mut b = Backoff::new(Duration::from_secs(10), max).with_seed(5);
        for _ in 0..4 {
            assert_eq!(b.next_delay(), max);
        }
    }

    #[test]
    fn sequential_seeds_spread_the_first_retry() {
        // With the seed only XOR-ed into the state, the first draw of a million
        // sequential seeds falls inside [0.8597, 0.8598]: every batch's first
        // retry lands on the same 82.804118ms, in lockstep. Spread across the
        // window is the property; "not exactly equal" does not catch it.
        let (full, window) = (0.1_f64, 0.02_f64); // 100ms delay, 20% jittered away
        let mut deciles = [0usize; 10];
        for seq in 0..512 {
            let d = backoff(0.2, seq).next_delay();
            let frac = (d.as_secs_f64() - (full - window)) / window;
            deciles[((frac * 10.0) as usize).min(9)] += 1;
        }
        assert!(
            deciles.iter().all(|&n| n > 0),
            "first-retry jitter is not spread across the window: {deciles:?}"
        );
    }

    /// Log-uniform over the whole `Duration` range: `magnitude` picks the
    /// bit-width, so nanoseconds, milliseconds, days and the
    /// `Duration::MAX` region are all sampled about equally often.
    ///
    /// Drawing seconds uniformly from `u64`, the obvious generator, is
    /// degenerate here. It puts every draw above 10^18 seconds, so no
    /// realistic policy is ever built, and under 0.1% of them pass
    /// [`RetryConfig::validate`].
    fn any_duration() -> impl Strategy<Value = Duration> {
        (0u32..128, num::u128::ANY).prop_map(|(magnitude, raw)| {
            let nanos = raw >> (127 - magnitude);
            Duration::new(
                (nanos / 1_000_000_000).min(u64::MAX as u128) as u64,
                (nanos % 1_000_000_000) as u32,
            )
        })
    }

    proptest! {
        /// `Backoff` is total for every input to its constructor and setters,
        /// with `reset` interleaved. It never panics, never exceeds `max`, and
        /// never returns a zero delay unless `min(initial, max)` leaves it no
        /// non-zero delay to return. An upper bound alone does not catch
        /// this, because an implementation returning `Duration::ZERO`
        /// unconditionally satisfies it.
        #[test]
        fn any_policy_saturates_without_collapsing(
            initial in any_duration(),
            max in any_duration(),
            multiplier in num::f64::ANY,
            jitter in num::f64::ANY,
            seed in num::u64::ANY,
            reset_at in 0usize..16,
        ) {
            let must_be_zero = initial.min(max).is_zero();
            let mut b = policy(initial, max, multiplier, jitter).with_seed(seed);
            for step in 0..16 {
                if step == reset_at {
                    b.reset();
                }
                let d = b.next_delay();
                prop_assert!(d <= max, "{d:?} exceeds the {max:?} ceiling");
                prop_assert!(
                    must_be_zero || !d.is_zero(),
                    "zero delay from a policy with a non-zero floor"
                );
            }
        }

        /// Over the space the sink validators admit, the sequence grows
        /// monotonically towards `max` and every delay is a real sleep.
        /// Bounded to two days so the assertions stay exact: above 2^23
        /// seconds a `Duration` no longer round-trips through `f64`, and
        /// growth can jitter by a nanosecond in either direction.
        #[test]
        fn a_validated_config_grows_towards_its_ceiling(
            initial_nanos in 1u64..=86_400_000_000_000,
            span_nanos in 0u64..=86_400_000_000_000,
            multiplier in 1.0f64..=1e9,
            jitter in 0.0f64..=1.0,
            seed in num::u64::ANY,
        ) {
            let initial = Duration::from_nanos(initial_nanos);
            let cfg = RetryConfig {
                initial,
                max: initial + Duration::from_nanos(span_nanos),
                multiplier,
                jitter,
                max_attempts: 0,
            };
            prop_assert_eq!(cfg.validate(), Ok(()), "{:?}", cfg);
            let mut b = policy(cfg.initial, cfg.max, cfg.multiplier, cfg.jitter).with_seed(seed);
            let mut previous = Duration::ZERO;
            for _ in 0..24 {
                let d = b.next_delay();
                prop_assert!(!d.is_zero(), "a validated policy produced a zero delay");
                prop_assert!(d <= cfg.max);
                prop_assert!(b.current >= previous, "growth went backwards");
                previous = b.current;
            }
        }
    }
}
