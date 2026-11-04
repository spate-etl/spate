//! Sink worker-pool tuning knobs.
//!
//! These are framework-level structs, and they are also the wire format: the
//! `batch`, `inflight`, `retry` and `breaker` sub-sections of every sink's
//! YAML deserialize straight into them. The keys and defaults are meant to be
//! identical across connectors, so they are literally one type rather than a
//! per-connector mirror that has to be kept in step by hand.
//!
//! Every struct here is `#[non_exhaustive]`. Construct one with `default()`
//! (or [`SinkPoolConfig::new`]) and assign the fields you are tuning; a knob
//! added later arrives as a new default and existing code keeps compiling.

use bytesize::ByteSize;
use serde::{Deserialize, Deserializer};
use std::time::Duration;

/// Accept `128MiB`-style sizes on the wire while keeping the field a plain
/// `u64`. `ByteSize` is a parsing convenience, not part of the batching API.
fn de_byte_size<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    ByteSize::deserialize(d).map(|b| b.as_u64())
}

/// Batch sealing thresholds for one shard worker. A batch seals as soon as
/// **any** threshold trips; since chunks arrive whole, a sealed batch may
/// overshoot `max_rows`/`max_bytes` by at most one chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct BatchConfig {
    /// Seal at this many rows (for a message-oriented sink, messages).
    ///
    /// Must be at least 1; [`SinkPoolConfig::validate`] rejects `0`.
    pub max_rows: u64,
    /// Seal at this many encoded, uncompressed bytes.
    ///
    /// Must be at least 1; [`SinkPoolConfig::validate`] rejects `0`.
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
#[non_exhaustive]
pub struct InflightConfig {
    /// Concurrent sealed batches per shard (for a replicated sink, writes to
    /// different replicas). While all permits are taken the worker stops
    /// consuming its queue, which fills and surfaces as backpressure.
    ///
    /// Must be at least 1; [`SinkPoolConfig::validate`] rejects `0`.
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
#[non_exhaustive]
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
    /// Total write attempts before the batch is abandoned (acknowledgments
    /// failed, watermark stalls). `0` means unbounded, retrying until the drain
    /// deadline, at which point the attempt in flight is aborted and the batch
    /// abandoned. The at-least-once default.
    ///
    /// An unbounded policy holds its in-flight slot
    /// ([`InflightConfig::max_per_shard`]) for the whole outage, since a slot
    /// frees only when its write task ends. That is how a down sink
    /// back-pressures the source rather than buffering, and it means a shard
    /// talking to a dead sink runs at zero in-flight capacity until either the
    /// sink recovers or the drain deadline arrives.
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
    /// `multiplier` is not a finite number in `[1.0, 1e9]`. Below `1.0` it
    /// asks for a shrinking delay, which the backoff holds at `max` from the
    /// second delay on.
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
    /// Reject a retry policy that would misbehave at runtime.
    /// [`SinkPoolConfig::validate`] runs it with the rules of the other
    /// sections.
    ///
    /// This is about intent, not safety.
    /// [`Backoff`](crate::backoff::Backoff) never panics for *any*
    /// `RetryConfig` and always saturates at `max`. It
    /// returns a zero delay only for a policy this rejects (`initial` or
    /// `max` of zero), so "never zero" is a property of a **validated**
    /// policy, not of the type. What it catches is a policy no operator
    /// means: a sub-`1.0` multiplier asks for a shrinking delay, which the
    /// backoff holds at `max` from the second delay on, a zero delay is not a
    /// backoff at all, and both are worth failing at load rather than at 3am.
    ///
    /// The bounds are generous and do **not** guarantee a *sensible* policy.
    /// `initial: 1ns, max: 1ns` passes. They rule out the nonsensical, not
    /// the aggressive.
    ///
    /// # Errors
    ///
    /// [`RetryConfigError`], naming the offending key.
    ///
    /// ```
    /// use spate_core::sink::{RetryConfig, RetryConfigError};
    ///
    /// assert!(RetryConfig::default().validate().is_ok());
    ///
    /// let mut hot_loop = RetryConfig::default();
    /// hot_loop.multiplier = 0.5;
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
    /// Not a [`validate`](Self::validate) rule. Nothing here is unsafe; the
    /// drain deadline still aborts the sleep at shutdown, so at-least-once
    /// holds. A sink fronting an expensive or rate-limited destination may
    /// well mean it. The threshold is a heuristic, which a warning may use
    /// and a rejection may not.
    pub(crate) fn stalls_indefinitely(&self) -> bool {
        /// Past this, an unbounded policy stops reading as a backoff.
        const CEILING: Duration = Duration::from_secs(300);
        self.max_attempts == 0 && self.max > CEILING
    }
}

/// Per-replica circuit breaker thresholds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct BreakerConfig {
    /// Consecutive failures that open the breaker, quarantining that endpoint.
    ///
    /// Must be at least 1; [`validate`](Self::validate) rejects `0`. At the
    /// point of use `0` opens on the first failure, the same as `1`, so the
    /// rule catches a value no operator means.
    pub failure_threshold: u32,
    /// How long an open breaker rejects a replica before probing again.
    ///
    /// Also bounds how long a batch parked behind a fully-probing shard waits
    /// before re-checking of its own accord (clamped to `[100ms, 30s]`).
    /// Capped at a year on the way in: it is stamped into a deadline, and
    /// `Instant + Duration` panics rather than saturating.
    #[serde(with = "humantime_serde")]
    pub open_for: Duration,
    /// Concurrent probe writes allowed while half-open.
    ///
    /// Must be at least 1; [`validate`](Self::validate) rejects `0`, which
    /// taken literally would mean the replica never recovers. The breaker also
    /// floors it at 1 at the point of use, so a config built programmatically
    /// rather than loaded cannot wedge a replica either.
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

impl BreakerConfig {
    /// Ceiling on [`open_for`](Self::open_for).
    ///
    /// It is stamped into a deadline, and `Instant + Duration` panics rather
    /// than saturating. A year already means "never probe again", so anything
    /// beyond it is a typo rather than a policy.
    pub const MAX_OPEN_FOR: Duration = Duration::from_secs(365 * 24 * 60 * 60);

    /// Reject breaker thresholds that would misbehave at runtime.
    ///
    /// It checks `failure_threshold`, `half_open_probes` and `open_for`, in
    /// that order. [`SinkPoolConfig::validate`] runs it with the rules of the
    /// other sections.
    ///
    /// # Errors
    ///
    /// [`BreakerConfigError`], naming the offending key.
    ///
    /// ```
    /// use spate_core::sink::{BreakerConfig, BreakerConfigError};
    ///
    /// assert!(BreakerConfig::default().validate().is_ok());
    ///
    /// let mut wedged = BreakerConfig::default();
    /// wedged.half_open_probes = 0;
    /// assert_eq!(wedged.validate(), Err(BreakerConfigError::ZeroHalfOpenProbes));
    /// ```
    pub fn validate(&self) -> Result<(), BreakerConfigError> {
        if self.failure_threshold == 0 {
            return Err(BreakerConfigError::ZeroFailureThreshold);
        }
        if self.half_open_probes == 0 {
            return Err(BreakerConfigError::ZeroHalfOpenProbes);
        }
        if self.open_for.is_zero() {
            return Err(BreakerConfigError::ZeroOpenFor);
        }
        if self.open_for > Self::MAX_OPEN_FOR {
            return Err(BreakerConfigError::OpenForTooLong(self.open_for));
        }
        Ok(())
    }
}

/// Why a [`BreakerConfig`] was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BreakerConfigError {
    /// `failure_threshold` is zero, which opens the breaker on the first
    /// failure as `1` does.
    #[error("breaker.failure_threshold must be at least 1")]
    ZeroFailureThreshold,
    /// `half_open_probes` is zero, so no probe budget exists to recover with.
    #[error("breaker.half_open_probes must be at least 1")]
    ZeroHalfOpenProbes,
    /// `open_for` is zero, which is not a quarantine at all.
    #[error("breaker.open_for must be non-zero")]
    ZeroOpenFor,
    /// `open_for` exceeds [`BreakerConfig::MAX_OPEN_FOR`].
    #[error("breaker.open_for must not exceed a year (got {0:?})")]
    OpenForTooLong(Duration),
}

/// Complete sink worker-pool configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[non_exhaustive]
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

impl SinkPoolConfig {
    /// All four sections at once, as a connector's factory assembles them
    /// from its config.
    ///
    /// ```
    /// use spate_core::sink::{BatchConfig, BreakerConfig, InflightConfig, RetryConfig, SinkPoolConfig};
    ///
    /// let mut batch = BatchConfig::default();
    /// batch.max_rows = 1_000;
    /// let pool = SinkPoolConfig::new(
    ///     batch,
    ///     InflightConfig::default(),
    ///     RetryConfig::default(),
    ///     BreakerConfig::default(),
    /// );
    /// assert_eq!(pool.batch.max_rows, 1_000);
    /// ```
    #[must_use]
    pub fn new(
        batch: BatchConfig,
        inflight: InflightConfig,
        retry: RetryConfig,
        breaker: BreakerConfig,
    ) -> SinkPoolConfig {
        SinkPoolConfig {
            batch,
            inflight,
            retry,
            breaker,
        }
    }

    /// Reject a pool configuration that would misbehave at runtime.
    ///
    /// It checks the `batch` and `inflight` keys, then runs
    /// [`RetryConfig::validate`] and [`BreakerConfig::validate`], and reports
    /// the first section that fails. Like those, it catches values no
    /// operator means. The pipeline builder runs it when it installs a sink.
    ///
    /// # Errors
    ///
    /// [`SinkPoolConfigError`], naming the offending key.
    ///
    /// ```
    /// use spate_core::sink::{SinkPoolConfig, SinkPoolConfigError};
    ///
    /// assert!(SinkPoolConfig::default().validate().is_ok());
    ///
    /// let mut pool = SinkPoolConfig::default();
    /// pool.batch.max_rows = 0;
    /// assert_eq!(pool.validate(), Err(SinkPoolConfigError::ZeroMaxRows));
    /// ```
    pub fn validate(&self) -> Result<(), SinkPoolConfigError> {
        self.batch.check()?;
        self.inflight.check()?;
        self.retry.validate()?;
        self.breaker.validate()?;
        Ok(())
    }
}

impl BatchConfig {
    fn check(&self) -> Result<(), SinkPoolConfigError> {
        if self.max_rows == 0 {
            return Err(SinkPoolConfigError::ZeroMaxRows);
        }
        if self.max_bytes == 0 {
            return Err(SinkPoolConfigError::ZeroMaxBytes);
        }
        Ok(())
    }
}

impl InflightConfig {
    /// The `inflight` rule alone, for the spawn guard.
    pub(crate) fn check(&self) -> Result<(), SinkPoolConfigError> {
        if self.max_per_shard == 0 {
            return Err(SinkPoolConfigError::ZeroMaxPerShard);
        }
        Ok(())
    }
}

/// Why a [`SinkPoolConfig`] was rejected.
///
/// Each message names the offending key relative to the sink's section. The
/// pipeline builder prefixes the sink's name when it returns one as
/// [`BuildError::Config`](crate::pipeline::BuildError::Config).
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum SinkPoolConfigError {
    /// `batch.max_rows` is zero, which seals every chunk as its own batch.
    #[error("batch.max_rows must be at least 1")]
    ZeroMaxRows,
    /// `batch.max_bytes` is zero, which seals every chunk as its own batch.
    #[error("batch.max_bytes must be at least 1")]
    ZeroMaxBytes,
    /// `inflight.max_per_shard` is zero, which parks every sealed batch.
    #[error("inflight.max_per_shard must be at least 1")]
    ZeroMaxPerShard,
    /// The `retry` section failed [`RetryConfig::validate`].
    #[error(transparent)]
    Retry(#[from] RetryConfigError),
    /// The `breaker` section failed [`BreakerConfig::validate`].
    #[error(transparent)]
    Breaker(#[from] BreakerConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A default config with `mutate` applied.
    fn tuned<T: Default>(mutate: impl FnOnce(&mut T)) -> T {
        let mut cfg = T::default();
        mutate(&mut cfg);
        cfg
    }

    fn retry(mutate: impl FnOnce(&mut RetryConfig)) -> RetryConfig {
        tuned(mutate)
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

    fn breaker(mutate: impl FnOnce(&mut BreakerConfig)) -> BreakerConfig {
        tuned(mutate)
    }

    fn pool(mutate: impl FnOnce(&mut SinkPoolConfig)) -> SinkPoolConfig {
        tuned(mutate)
    }

    #[test]
    fn breaker_validate_rejects_a_budget_no_replica_could_recover_from() {
        assert_eq!(
            breaker(|c| c.failure_threshold = 0).validate(),
            Err(BreakerConfigError::ZeroFailureThreshold)
        );
        // The threshold is checked first, so a config with both zero names it.
        assert_eq!(
            breaker(|c| {
                c.failure_threshold = 0;
                c.half_open_probes = 0;
            })
            .validate(),
            Err(BreakerConfigError::ZeroFailureThreshold)
        );
        assert_eq!(
            breaker(|c| c.half_open_probes = 0).validate(),
            Err(BreakerConfigError::ZeroHalfOpenProbes)
        );
        assert_eq!(
            breaker(|c| c.open_for = Duration::ZERO).validate(),
            Err(BreakerConfigError::ZeroOpenFor)
        );
        let too_long = BreakerConfig::MAX_OPEN_FOR + Duration::from_secs(1);
        assert_eq!(
            breaker(|c| c.open_for = too_long).validate(),
            Err(BreakerConfigError::OpenForTooLong(too_long))
        );
    }

    #[test]
    fn breaker_validate_accepts_the_default_and_the_boundaries() {
        assert!(BreakerConfig::default().validate().is_ok());
        // Both ends of every bound, so tightening one shows up here rather
        // than in a connector's load path.
        assert!(breaker(|c| c.half_open_probes = 1).validate().is_ok());
        assert!(breaker(|c| c.failure_threshold = 1).validate().is_ok());
        assert!(
            breaker(|c| c.failure_threshold = u32::MAX)
                .validate()
                .is_ok()
        );
        assert!(
            breaker(|c| c.open_for = Duration::from_nanos(1))
                .validate()
                .is_ok()
        );
        assert!(
            breaker(|c| c.open_for = BreakerConfig::MAX_OPEN_FOR)
                .validate()
                .is_ok()
        );
    }

    /// The cap exists because `on_failure` stamps `now + open_for` and
    /// `Instant + Duration` panics on overflow. Validation rejects the value,
    /// but `BreakerConfig` is a public `Copy` struct, so the breaker floors and
    /// caps at the point of use too. This pins that the two agree on where
    /// the line is.
    #[test]
    fn breaker_cap_matches_what_the_breaker_applies() {
        assert_eq!(
            BreakerConfig::MAX_OPEN_FOR,
            Duration::from_secs(365 * 24 * 60 * 60)
        );
    }

    #[test]
    fn pool_validate_rejects_each_zero_section_key() {
        let cases = [
            (
                pool(|c| c.batch.max_rows = 0),
                SinkPoolConfigError::ZeroMaxRows,
            ),
            (
                pool(|c| c.batch.max_bytes = 0),
                SinkPoolConfigError::ZeroMaxBytes,
            ),
            (
                pool(|c| c.inflight.max_per_shard = 0),
                SinkPoolConfigError::ZeroMaxPerShard,
            ),
            (
                pool(|c| c.retry.initial = Duration::ZERO),
                SinkPoolConfigError::Retry(RetryConfigError::ZeroDelay),
            ),
            (
                pool(|c| c.breaker.failure_threshold = 0),
                SinkPoolConfigError::Breaker(BreakerConfigError::ZeroFailureThreshold),
            ),
            (
                pool(|c| c.breaker.half_open_probes = 0),
                SinkPoolConfigError::Breaker(BreakerConfigError::ZeroHalfOpenProbes),
            ),
        ];
        for (cfg, want) in cases {
            assert_eq!(cfg.validate(), Err(want), "{cfg:?}");
        }
    }

    #[test]
    fn pool_validate_accepts_the_default_and_the_boundaries() {
        assert_eq!(SinkPoolConfig::default().validate(), Ok(()));
        let ones = pool(|c| {
            c.batch.max_rows = 1;
            c.batch.max_bytes = 1;
            c.inflight.max_per_shard = 1;
            c.breaker.failure_threshold = 1;
        });
        assert_eq!(ones.validate(), Ok(()), "{ones:?}");
        let upper = pool(|c| {
            c.batch.max_rows = u64::MAX;
            c.batch.max_bytes = u64::MAX;
            c.inflight.max_per_shard = usize::MAX;
            c.breaker.failure_threshold = u32::MAX;
        });
        assert_eq!(upper.validate(), Ok(()), "{upper:?}");
        // Unbounded attempts with a long ceiling is a warning, never a rule.
        let unbounded = pool(|c| {
            c.retry.max_attempts = 0;
            c.retry.max = Duration::from_secs(3600);
        });
        assert_eq!(unbounded.validate(), Ok(()), "{unbounded:?}");
    }

    /// Pins the section order: batch, inflight, retry, breaker.
    #[test]
    fn pool_validate_reports_the_first_invalid_section() {
        let mut cfg = pool(|c| {
            c.batch.max_rows = 0;
            c.batch.max_bytes = 0;
            c.inflight.max_per_shard = 0;
            c.retry.initial = Duration::ZERO;
            c.breaker.failure_threshold = 0;
        });
        assert_eq!(cfg.validate(), Err(SinkPoolConfigError::ZeroMaxRows));
        cfg.batch.max_rows = 1;
        assert_eq!(cfg.validate(), Err(SinkPoolConfigError::ZeroMaxBytes));
        cfg.batch.max_bytes = 1;
        assert_eq!(cfg.validate(), Err(SinkPoolConfigError::ZeroMaxPerShard));
        cfg.inflight.max_per_shard = 1;
        assert_eq!(
            cfg.validate(),
            Err(SinkPoolConfigError::Retry(RetryConfigError::ZeroDelay))
        );
        cfg.retry.initial = Duration::from_millis(1);
        assert_eq!(
            cfg.validate(),
            Err(SinkPoolConfigError::Breaker(
                BreakerConfigError::ZeroFailureThreshold
            ))
        );
    }

    /// Pins each pool rule's message, and that a wrapped retry or breaker
    /// error prints the inner message with its key.
    #[test]
    fn each_pool_rule_prints_its_message() {
        let cases = [
            (
                SinkPoolConfigError::ZeroMaxRows,
                "batch.max_rows must be at least 1",
            ),
            (
                SinkPoolConfigError::ZeroMaxBytes,
                "batch.max_bytes must be at least 1",
            ),
            (
                SinkPoolConfigError::ZeroMaxPerShard,
                "inflight.max_per_shard must be at least 1",
            ),
            (
                SinkPoolConfigError::Breaker(BreakerConfigError::ZeroFailureThreshold),
                "breaker.failure_threshold must be at least 1",
            ),
            (
                SinkPoolConfigError::Breaker(BreakerConfigError::ZeroOpenFor),
                "breaker.open_for must be non-zero",
            ),
            (
                SinkPoolConfigError::Retry(RetryConfigError::Multiplier(0.5)),
                "retry.multiplier must be a finite number in [1.0, 1e9] (got 0.5)",
            ),
        ];
        for (err, want) in cases {
            assert_eq!(err.to_string(), want);
        }
    }
}
