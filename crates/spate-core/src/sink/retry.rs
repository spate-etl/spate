//! Capped exponential backoff with deterministic pseudo-jitter.

use super::config::RetryConfig;
use std::time::Duration;

/// Backoff sequence for one batch's retry loop.
///
/// Jitter is pseudo-random from a xorshift state seeded per batch (no
/// dependency on a random-number crate; batches seed differently, so
/// replicas of a struggling shard don't retry in lockstep).
#[derive(Debug)]
pub(crate) struct Backoff {
    cfg: RetryConfig,
    current: Duration,
    rng: u64,
}

/// SplitMix64 finalizer. Avalanches adjacent inputs, so batch `n` and batch
/// `n + 1` start from unrelated states.
fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Backoff {
    pub(crate) fn new(cfg: RetryConfig, seed: u64) -> Self {
        Backoff {
            cfg,
            // Keeps the ceiling unconditional for an `initial` above `max`.
            current: cfg.initial.min(cfg.max),
            // Seeds are consecutive batch sequence numbers. XOR-ing the seed
            // into the state instead of mixing it leaves every batch's first
            // retry inside the same thousandth of the jitter window, and
            // relocates xorshift's absorbing zero state rather than removing it.
            rng: match splitmix64(seed) {
                0 => 0x9E37_79B9_7F4A_7C15,
                mixed => mixed,
            },
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

    /// The delay to sleep before the next attempt.
    ///
    /// Total for *any* [`RetryConfig`]. The connector validators reject
    /// nonsensical policies, but the framework struct is public and a
    /// programmatic caller can still build one. These properties hold
    /// unconditionally, and the proptests below enforce them:
    ///
    /// * the delay never exceeds `max`;
    /// * it is never zero unless `min(initial, max)` is itself zero, so it
    ///   cannot degenerate into a zero-delay hot-retry loop against a failing
    ///   replica;
    /// * it never panics.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let base = self.current;
        let cap = self.cfg.max;
        // Grow in `f64` seconds and clamp *before* rebuilding the `Duration`:
        // `Duration::mul_f64` panics on an overflowing, non-finite or negative
        // result. A multiplier outside the growing range jumps straight to the
        // ceiling, NaN included, for which the comparison is false.
        let grown = if self.cfg.multiplier >= 1.0 {
            (base.as_secs_f64() * self.cfg.multiplier).min(cap.as_secs_f64())
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
        let jitter = if self.cfg.jitter.is_finite() {
            self.cfg.jitter.clamp(0.0, 1.0)
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
        // product, which `validate` admits, from rounding down to a zero delay.
        let floor = base.min(Duration::from_nanos(1));
        Duration::try_from_secs_f64(jittered)
            .unwrap_or(base)
            .clamp(floor, base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn cfg(jitter: f64) -> RetryConfig {
        RetryConfig {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(400),
            multiplier: 2.0,
            jitter,
            max_attempts: 0,
        }
    }

    #[test]
    fn grows_exponentially_and_caps_without_jitter() {
        let mut b = Backoff::new(cfg(0.0), 7);
        assert_eq!(b.next_delay(), Duration::from_millis(100));
        assert_eq!(b.next_delay(), Duration::from_millis(200));
        assert_eq!(b.next_delay(), Duration::from_millis(400));
        assert_eq!(b.next_delay(), Duration::from_millis(400), "capped");
    }

    #[test]
    fn jitter_stays_within_the_configured_fraction() {
        let mut b = Backoff::new(cfg(0.2), 42);
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
        let mut a = Backoff::new(cfg(0.5), 1);
        let mut b = Backoff::new(cfg(0.5), 2);
        let same = (0..8).filter(|_| a.next_delay() == b.next_delay()).count();
        assert!(same < 8, "two seeds should not produce identical jitter");
    }

    #[test]
    fn extreme_max_and_multiplier_do_not_overflow() {
        // A policy both sink validators accept; without the clamp, growth past
        // `max` overflows `Duration` and panics the write task.
        let cfg = RetryConfig {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(315_576_000_000),
            multiplier: 1e9,
            jitter: 0.0,
            max_attempts: 0,
        };
        let mut b = Backoff::new(cfg, 3);
        for _ in 0..8 {
            assert!(b.next_delay() <= cfg.max);
        }
    }

    #[test]
    fn a_near_max_duration_ceiling_saturates() {
        // `Duration::MAX.as_secs_f64()` rounds *up* past `Duration::MAX`, so
        // the clamped f64 is not necessarily a representable ceiling.
        let cfg = RetryConfig {
            initial: Duration::from_secs(1),
            max: Duration::MAX,
            multiplier: 1e9,
            jitter: 0.5,
            max_attempts: 0,
        };
        let mut b = Backoff::new(cfg, 11);
        for _ in 0..8 {
            assert!(b.next_delay() <= cfg.max);
        }
    }

    #[test]
    fn a_pathological_multiplier_lands_on_the_ceiling() {
        // `0.0`, `-0.0` and subnormals are the collapsing cases, pinning the
        // delay at 0ns forever; `-0.0` reaches that through
        // `try_from_secs_f64(-0.0) == Ok(0ns)`, not a failed conversion.
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
            let cfg = RetryConfig {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(10),
                multiplier,
                jitter: 0.0,
                max_attempts: 0,
            };
            let mut b = Backoff::new(cfg, 5);
            assert_eq!(b.next_delay(), cfg.initial, "{multiplier}");
            assert_eq!(b.next_delay(), cfg.max, "{multiplier}");
            assert_eq!(b.next_delay(), cfg.max, "{multiplier}");
        }
    }

    #[test]
    fn an_out_of_range_jitter_never_lengthens_the_delay() {
        for jitter in [1.5, -0.5, f64::NAN, f64::INFINITY] {
            let cfg = RetryConfig {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(10),
                multiplier: 2.0,
                jitter,
                max_attempts: 0,
            };
            let mut b = Backoff::new(cfg, 5);
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
            let cfg = RetryConfig {
                initial,
                max,
                multiplier: 2.0,
                jitter,
                max_attempts: 0,
            };
            for seed in 0..256 {
                let mut b = Backoff::new(cfg, seed);
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
        let cfg = RetryConfig {
            initial: Duration::from_secs(10),
            max: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: 0.0,
            max_attempts: 0,
        };
        let mut b = Backoff::new(cfg, 5);
        for _ in 0..4 {
            assert_eq!(b.next_delay(), cfg.max);
        }
    }

    #[test]
    fn sequential_seeds_spread_the_first_retry() {
        // With the seed only XOR-ed into the state, the first draw of a million
        // sequential seeds falls inside [0.8597, 0.8598]: every batch's first
        // retry lands on the same 82.804118ms, in lockstep. Spread across the
        // window is the property; "not exactly equal" does not catch it.
        let cfg = cfg(0.2);
        let (full, window) = (0.1_f64, 0.02_f64); // 100ms delay, 20% jittered away
        let mut deciles = [0usize; 10];
        for seq in 0..512 {
            let d = Backoff::new(cfg, seq).next_delay();
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
        (0u32..128, proptest::num::u128::ANY).prop_map(|(magnitude, raw)| {
            let nanos = raw >> (127 - magnitude);
            Duration::new(
                (nanos / 1_000_000_000).min(u64::MAX as u128) as u64,
                (nanos % 1_000_000_000) as u32,
            )
        })
    }

    proptest! {
        /// `Backoff` is total for every `RetryConfig`, including the ones the
        /// sink validators reject; a programmatic caller can still build one.
        /// It never panics, never exceeds `max`, and never returns a zero
        /// delay unless the policy leaves it no non-zero delay to return. An
        /// upper bound alone does not catch this, because an implementation
        /// returning `Duration::ZERO` unconditionally satisfies it.
        #[test]
        fn any_config_saturates_without_collapsing(
            initial in any_duration(),
            max in any_duration(),
            multiplier in proptest::num::f64::ANY,
            jitter in proptest::num::f64::ANY,
            seed in proptest::num::u64::ANY,
        ) {
            let cfg = RetryConfig { initial, max, multiplier, jitter, max_attempts: 0 };
            // The only policy with no non-zero delay available to it.
            let must_be_zero = cfg.initial.min(cfg.max).is_zero();
            let mut b = Backoff::new(cfg, seed);
            for _ in 0..16 {
                let d = b.next_delay();
                prop_assert!(d <= cfg.max, "{d:?} exceeds the {:?} ceiling", cfg.max);
                prop_assert!(
                    must_be_zero || !d.is_zero(),
                    "zero delay from a policy with a non-zero floor"
                );
            }
        }

        /// Over the space the validators admit, the sequence grows
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
            seed in proptest::num::u64::ANY,
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
            let mut b = Backoff::new(cfg, seed);
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
