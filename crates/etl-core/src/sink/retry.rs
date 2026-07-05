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

impl Backoff {
    pub(crate) fn new(cfg: RetryConfig, seed: u64) -> Self {
        Backoff {
            cfg,
            current: cfg.initial,
            // Xorshift must not start at zero; fold in a constant.
            rng: seed ^ 0x9E37_79B9_7F4A_7C15,
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
    pub(crate) fn next_delay(&mut self) -> Duration {
        let base = self.current;
        let grown = base.mul_f64(self.cfg.multiplier);
        self.current = grown.min(self.cfg.max);

        if self.cfg.jitter <= 0.0 {
            return base;
        }
        // Subtract up to `jitter` of the delay: full delay down to
        // (1 - jitter) * delay.
        let unit = (self.next_rand() >> 11) as f64 / (1u64 << 53) as f64;
        base.mul_f64(1.0 - self.cfg.jitter * unit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
