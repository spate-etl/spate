//! Sink worker-pool tuning knobs.
//!
//! These are framework-level structs, and they are also the wire format: the
//! `batch`, `inflight`, `retry` and `breaker` sub-sections of every sink's
//! YAML deserialize straight into them. The keys and defaults are meant to be
//! identical across connectors, so they are literally one type rather than a
//! per-connector mirror that has to be kept in step by hand.

use bytesize::ByteSize;
use serde::{Deserialize, Deserializer};
use std::time::Duration;

/// Accept `128MiB`-style sizes on the wire while keeping the field a plain
/// `u64` — `ByteSize` is a parsing convenience, not part of the batching API.
fn de_byte_size<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    ByteSize::deserialize(d).map(|b| b.as_u64())
}

/// Batch sealing thresholds for one shard worker. A batch seals as soon as
/// **any** threshold trips; since chunks arrive whole, a sealed batch may
/// overshoot `max_rows`/`max_bytes` by at most one chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BatchConfig {
    /// Seal at this many rows (for a message-oriented sink, messages).
    pub max_rows: u64,
    /// Seal at this many encoded, uncompressed bytes.
    #[serde(deserialize_with = "de_byte_size")]
    pub max_bytes: u64,
    /// Seal a non-empty batch this long after its first chunk arrived,
    /// bounding latency at low throughput.
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        BatchConfig {
            max_rows: 500_000,
            max_bytes: 128 * 1024 * 1024,
            linger: Duration::from_secs(1),
        }
    }
}

/// In-flight write limits for one shard worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct InflightConfig {
    /// Concurrent sealed batches per shard (for a replicated sink, writes to
    /// different replicas). While all permits are taken the worker stops
    /// consuming its queue, which fills and surfaces as backpressure.
    pub max_per_shard: usize,
}

impl Default for InflightConfig {
    fn default() -> Self {
        InflightConfig { max_per_shard: 2 }
    }
}

/// Retry policy for batch writes. Retries rotate across healthy replicas;
/// the sealed batch and its deduplication token are reused unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RetryConfig {
    /// First backoff delay.
    #[serde(with = "humantime_serde")]
    pub initial: Duration,
    /// Backoff cap.
    #[serde(with = "humantime_serde")]
    pub max: Duration,
    /// Backoff growth factor per attempt.
    pub multiplier: f64,
    /// Fraction of the delay randomized away (`0.0..=1.0`).
    pub jitter: f64,
    /// Total write attempts before the batch is abandoned (acknowledgements
    /// failed, watermark stalls). `0` means unbounded — retry until the
    /// drain deadline, the at-least-once default.
    pub max_attempts: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: 0.2,
            max_attempts: 0,
        }
    }
}

/// Why a [`RetryConfig`] was rejected.
///
/// Each message names the offending key relative to the sink's `retry`
/// section; connectors prepend their own config path when converting it into
/// their `ConfigError`.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RetryConfigError {
    /// `multiplier` is not a finite number in `[1.0, 1e9]`. Below `1.0` the
    /// delay shrinks instead of backing off.
    #[error("retry.multiplier must be a finite number in [1.0, 1e9] (got {0})")]
    Multiplier(f64),
    /// `jitter` is not a finite fraction in `[0.0, 1.0]`.
    #[error("retry.jitter must be a finite fraction in [0.0, 1.0] (got {0})")]
    Jitter(f64),
    /// `initial` or `max` is zero, leaving no delay to sleep at all.
    #[error("retry.initial and retry.max must be non-zero")]
    ZeroDelay,
    /// `initial` is larger than the ceiling it grows towards.
    #[error("retry.initial ({initial:?}) must not exceed retry.max ({max:?})")]
    InitialExceedsMax {
        /// The configured first delay.
        initial: Duration,
        /// The configured ceiling.
        max: Duration,
    },
}

impl RetryConfig {
    /// Reject a retry policy that would misbehave at runtime. Connectors call
    /// this from their config validation and prepend their own config path to
    /// the message, so the rules stay in one place instead of being mirrored
    /// per connector.
    ///
    /// This is about intent, not safety. [`Backoff`](super::retry) is total
    /// for *any* `RetryConfig` — it saturates at `max` and never returns a
    /// zero delay — so nothing here is load-bearing for the write loop. What
    /// it catches is a policy no operator means: a sub-`1.0` multiplier
    /// shrinks the delay instead of backing off, a zero delay is not a
    /// backoff at all, and both are worth failing at load rather than at 3am.
    ///
    /// The bounds are deliberately generous and do **not** guarantee a
    /// *sensible* policy — `initial: 1ns, max: 1ns` passes. They rule out the
    /// nonsensical, not the merely aggressive.
    ///
    /// # Errors
    ///
    /// [`RetryConfigError`], naming the offending key.
    ///
    /// ```
    /// use etl_core::sink::{RetryConfig, RetryConfigError};
    ///
    /// assert!(RetryConfig::default().validate().is_ok());
    ///
    /// let hot_loop = RetryConfig { multiplier: 0.5, ..RetryConfig::default() };
    /// assert_eq!(hot_loop.validate(), Err(RetryConfigError::Multiplier(0.5)));
    /// ```
    pub fn validate(&self) -> Result<(), RetryConfigError> {
        if !self.multiplier.is_finite() || !(1.0..=1e9).contains(&self.multiplier) {
            return Err(RetryConfigError::Multiplier(self.multiplier));
        }
        if !self.jitter.is_finite() || !(0.0..=1.0).contains(&self.jitter) {
            return Err(RetryConfigError::Jitter(self.jitter));
        }
        if self.initial.is_zero() || self.max.is_zero() {
            return Err(RetryConfigError::ZeroDelay);
        }
        if self.initial > self.max {
            return Err(RetryConfigError::InitialExceedsMax {
                initial: self.initial,
                max: self.max,
            });
        }
        Ok(())
    }

    /// Whether this policy lets a shard sleep indefinitely without ever
    /// giving up on the batch.
    ///
    /// It takes *both* halves: unbounded attempts, so the batch is never
    /// abandoned, and a ceiling long enough that a sleeping shard is
    /// indistinguishable from a wedged one. With a finite `max_attempts` the
    /// batch is abandoned and the stall is bounded; with a short ceiling the
    /// retries keep visibly ticking. Only the combination goes quiet.
    ///
    /// Deliberately not a [`validate`](Self::validate) rule. Nothing here is
    /// unsafe — the drain deadline still aborts the sleep at shutdown, so
    /// at-least-once holds — and a sink fronting an expensive or rate-limited
    /// destination may well mean it. The threshold is a heuristic, which a
    /// warning may use and a rejection may not.
    pub(crate) fn stalls_indefinitely(&self) -> bool {
        /// Past this, an unbounded policy stops reading as a backoff.
        const CEILING: Duration = Duration::from_secs(300);
        self.max_attempts == 0 && self.max > CEILING
    }
}

/// Per-replica circuit breaker thresholds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BreakerConfig {
    /// Consecutive failures that open the breaker, quarantining that endpoint.
    pub failure_threshold: u32,
    /// How long an open breaker rejects a replica before probing again.
    #[serde(with = "humantime_serde")]
    pub open_for: Duration,
    /// Concurrent probe writes allowed while half-open.
    pub half_open_probes: u32,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        BreakerConfig {
            failure_threshold: 3,
            open_for: Duration::from_secs(5),
            half_open_probes: 1,
        }
    }
}

/// Complete sink worker-pool configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SinkPoolConfig {
    /// Batch sealing thresholds.
    pub batch: BatchConfig,
    /// In-flight limits.
    pub inflight: InflightConfig,
    /// Write retry policy.
    pub retry: RetryConfig,
    /// Replica circuit breaker.
    pub breaker: BreakerConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry(mutate: impl FnOnce(&mut RetryConfig)) -> RetryConfig {
        let mut cfg = RetryConfig::default();
        mutate(&mut cfg);
        cfg
    }

    #[test]
    fn validate_rejects_policies_that_misbehave_at_runtime() {
        let cases = [
            (retry(|c| c.multiplier = 0.5), "multiplier"),
            (retry(|c| c.multiplier = -2.0), "multiplier"),
            (retry(|c| c.multiplier = f64::NAN), "multiplier"),
            (retry(|c| c.multiplier = f64::INFINITY), "multiplier"),
            (retry(|c| c.multiplier = 1e9 + 1.0), "multiplier"),
            (retry(|c| c.jitter = 1.5), "jitter"),
            (retry(|c| c.jitter = -0.1), "jitter"),
            (retry(|c| c.jitter = f64::NAN), "jitter"),
            (retry(|c| c.initial = Duration::ZERO), "non-zero"),
            (retry(|c| c.max = Duration::ZERO), "non-zero"),
            (
                retry(|c| {
                    c.initial = Duration::from_secs(10);
                    c.max = Duration::from_secs(1);
                }),
                "must not exceed",
            ),
        ];
        for (cfg, needle) in cases {
            let err = cfg
                .validate()
                .expect_err(&format!("{cfg:?} must fail"))
                .to_string();
            assert!(err.contains(needle), "expected `{needle}` in `{err}`");
        }
    }

    #[test]
    fn the_error_messages_survived_the_move_into_the_framework() {
        // Both sinks mirrored these strings before the rules moved here, and
        // their config tests still assert on them under their own prefix.
        assert_eq!(
            RetryConfigError::Multiplier(0.5).to_string(),
            "retry.multiplier must be a finite number in [1.0, 1e9] (got 0.5)"
        );
        assert_eq!(
            RetryConfigError::Jitter(1.5).to_string(),
            "retry.jitter must be a finite fraction in [0.0, 1.0] (got 1.5)"
        );
        assert_eq!(
            RetryConfigError::ZeroDelay.to_string(),
            "retry.initial and retry.max must be non-zero"
        );
        assert_eq!(
            RetryConfigError::InitialExceedsMax {
                initial: Duration::from_secs(10),
                max: Duration::from_secs(1),
            }
            .to_string(),
            "retry.initial (10s) must not exceed retry.max (1s)"
        );
    }

    #[test]
    fn only_unbounded_attempts_with_a_long_ceiling_count_as_a_stall() {
        let long = Duration::from_secs(3600);
        // Both halves, the only combination that goes quiet.
        assert!(retry(|c| c.max = long).stalls_indefinitely());
        // A finite attempt cap abandons the batch; the stall is bounded.
        assert!(
            !retry(|c| {
                c.max = long;
                c.max_attempts = 5;
            })
            .stalls_indefinitely()
        );
        // A short ceiling keeps the retries visibly ticking.
        assert!(!RetryConfig::default().stalls_indefinitely());
        // Right at the threshold is still fine; it is an upper bound.
        assert!(!retry(|c| c.max = Duration::from_secs(300)).stalls_indefinitely());
        assert!(retry(|c| c.max = Duration::from_secs(301)).stalls_indefinitely());
    }

    #[test]
    fn validate_accepts_the_default_and_the_boundaries() {
        assert!(RetryConfig::default().validate().is_ok());
        let boundary = RetryConfig {
            initial: Duration::from_nanos(1),
            max: Duration::from_nanos(1),
            multiplier: 1.0,
            jitter: 0.0,
            max_attempts: 0,
        };
        assert!(boundary.validate().is_ok(), "{boundary:?}");
        let upper = RetryConfig {
            multiplier: 1e9,
            jitter: 1.0,
            ..RetryConfig::default()
        };
        assert!(upper.validate().is_ok(), "{upper:?}");
    }
}
