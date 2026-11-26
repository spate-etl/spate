//! Sink worker-pool tuning knobs.
//!
//! These are framework-level structs, and they are also the wire format: the
//! `queue`, `batch`, `inflight`, `retry` and `breaker` blocks beside a sink's
//! backend key deserialize straight into them. The keys are identical across
//! connectors, so they are one type rather than a per-connector mirror that
//! has to be kept in step by hand. A connector supplies the base each pool
//! block overlays, as [`SinkParts::pool`](super::SinkParts::pool).
//!
//! Every struct here is `#[non_exhaustive]`. Construct one with `default()`
//! (or [`SinkPoolConfig::new`]) and assign the fields you are tuning; a knob
//! added later arrives as a new default and existing code keeps compiling.

use crate::clock::LONGEST_DEADLINE;
use crate::config::{
    ConfigError, DurationBounds, DurationBoundsError, SectionPath, check_duration,
    ser_byte_size_exact,
};
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::Semaphore;

/// How much encoded data each shard's chunk queue holds, the `queue:` block
/// beside a sink's backend key.
///
/// The queue counts chunks, so `max_bytes` sets a slot count:
/// `max_bytes / chunk.target_bytes`, rounded up. Queued bytes can sit below
/// `max_bytes`, because a partial chunk seals on every commit tick, or above
/// it, because a chunk seals once its frame reaches the target. Without the
/// block each queue holds 8 chunks.
///
/// Queued chunks are charged to the in-flight byte budget, so every shard's
/// queue has to fit under the budget's low watermark beside the pending
/// writes. [The backpressure page][bp] carries the sizing rule.
///
/// [bp]: https://spate.kainth.dev/docs/user-guide/concepts/backpressure
///
/// ```
/// use spate_core::sink::QueueSection;
///
/// let queue = QueueSection::new(8 * 1024 * 1024);
/// assert_eq!(queue.max_bytes.as_u64(), 8 * 1024 * 1024);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct QueueSection {
    /// Encoded bytes per shard queue, as `8MiB`-style sizes or a plain byte
    /// count. Above zero, and at most [`Semaphore::MAX_PERMITS`] chunks.
    pub max_bytes: ByteSize,
}

impl QueueSection {
    /// A queue holding `max_bytes` bytes per shard.
    #[must_use]
    pub fn new(max_bytes: u64) -> QueueSection {
        QueueSection {
            max_bytes: ByteSize::b(max_bytes),
        }
    }

    /// The slot count for chunks of `chunk_bytes`, which must be above zero.
    /// `at` is the path of the sink section, such as `sinks.eu`.
    pub(crate) fn capacity(
        &self,
        chunk_bytes: usize,
        at: &SectionPath,
    ) -> Result<usize, ConfigError> {
        let max_bytes = self.max_bytes.as_u64();
        if max_bytes == 0 {
            return Err(at.invalid("queue.max_bytes", "must be greater than zero"));
        }
        // `usize` on a 32-bit target is narrower than the `u64` wire value.
        let max_bytes = usize::try_from(max_bytes).map_err(|_| {
            at.invalid(
                "queue.max_bytes",
                format!("({max_bytes}) exceeds the addressable range on this platform"),
            )
        })?;
        let slots = max_bytes.div_ceil(chunk_bytes);
        // tokio's channel panics above this many slots.
        if slots > Semaphore::MAX_PERMITS {
            return Err(at.invalid(
                "queue.max_bytes",
                format!(
                    "makes {slots} chunks of {chunk_bytes} bytes, above the {} a queue holds",
                    Semaphore::MAX_PERMITS
                ),
            ));
        }
        Ok(slots)
    }
}

/// Batch sealing thresholds for one shard worker. A batch seals as soon as
/// **any** threshold trips; since chunks arrive whole, a sealed batch may
/// overshoot `max_rows`/`max_bytes` by at most one chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct BatchSection {
    /// Seal at this many rows (for a message-oriented sink, messages).
    ///
    /// Must be at least 1; [`SinkPoolConfig::validate`] rejects `0`.
    pub max_rows: u64,
    /// Seal at this many encoded, uncompressed bytes.
    ///
    /// Must be greater than zero; [`SinkPoolConfig::validate`] rejects `0`.
    #[serde(serialize_with = "ser_byte_size_exact")]
    pub max_bytes: ByteSize,
    /// Seal a non-empty batch this long after its first chunk arrived,
    /// bounding latency at low throughput.
    ///
    /// At most a year ([`LONGEST_DEADLINE`]); [`SinkPoolConfig::validate`]
    /// rejects more, and a pool spawned without it seals at a year.
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
}

impl Default for BatchSection {
    fn default() -> Self {
        BatchSection {
            max_rows: 500_000,
            max_bytes: ByteSize::mib(128),
            linger: Duration::from_secs(1),
        }
    }
}

/// In-flight write limits for one shard worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct InflightSection {
    /// Concurrent sealed batches per shard (for a replicated sink, writes to
    /// different replicas). While all permits are taken the worker stops
    /// consuming its queue, which fills and surfaces as backpressure.
    ///
    /// Must be at least 1 and at most tokio's [`Semaphore::MAX_PERMITS`];
    /// [`SinkPoolConfig::validate`] rejects anything else, and
    /// [`SinkPool::spawn`](super::SinkPool::spawn) panics on it.
    pub max_per_shard: usize,
}

impl Default for InflightSection {
    fn default() -> Self {
        InflightSection { max_per_shard: 2 }
    }
}

/// Retry policy for batch writes. Retries rotate across healthy replicas;
/// the sealed batch and its deduplication token are reused unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct RetrySection {
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
    /// ([`InflightSection::max_per_shard`]) for the whole outage, since a slot
    /// frees only when its write task ends. That is how a down sink
    /// back-pressures the source rather than buffering, and it means a shard
    /// talking to a dead sink runs at zero in-flight capacity until either the
    /// sink recovers or the drain deadline arrives.
    pub max_attempts: u32,
}

impl Default for RetrySection {
    fn default() -> Self {
        RetrySection {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: crate::backoff::DEFAULT_JITTER,
            max_attempts: 0,
        }
    }
}

/// Why a [`RetrySection`] was rejected.
///
/// Each message names the offending key relative to the sink's `retry`
/// section; connectors prepend their own config path when converting it into
/// their `ConfigError`.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RetrySectionError {
    /// `multiplier` is not a finite number in `[1.0, 1e9]`. Below `1.0` it
    /// asks for a shrinking delay, which the backoff holds at `max` from the
    /// second delay on.
    #[error("retry.multiplier: must be a finite number in [1.0, 1e9] (got {0})")]
    MultiplierOutOfRange(f64),
    /// `jitter` is not a finite fraction in `[0.0, 1.0]`.
    #[error("retry.jitter: must be a finite fraction in [0.0, 1.0] (got {0})")]
    JitterOutOfRange(f64),
    /// `initial` is zero, leaving no delay to sleep at all.
    #[error("retry.initial: must be greater than zero")]
    ZeroInitial,
    /// `max` is zero, leaving no delay to sleep at all.
    #[error("retry.max: must be greater than zero")]
    ZeroMax,
    /// `initial` is larger than the ceiling it grows towards.
    #[error("retry.initial: must not exceed `retry.max` ({max:?}); found {initial:?}")]
    InitialExceedsMax {
        /// The configured first delay.
        initial: Duration,
        /// The configured ceiling.
        max: Duration,
    },
}

impl RetrySection {
    /// Reject a retry policy that would misbehave at runtime.
    /// [`SinkPoolConfig::validate`] runs it with the rules of the other
    /// sections.
    ///
    /// This is about intent, not safety.
    /// [`Backoff`](crate::backoff::Backoff) never panics for *any*
    /// `RetrySection` and always saturates at `max`. It
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
    /// [`RetrySectionError`], naming the offending key.
    ///
    /// ```
    /// use spate_core::sink::{RetrySection, RetrySectionError};
    ///
    /// assert!(RetrySection::default().validate().is_ok());
    ///
    /// let mut hot_loop = RetrySection::default();
    /// hot_loop.multiplier = 0.5;
    /// assert_eq!(hot_loop.validate(), Err(RetrySectionError::MultiplierOutOfRange(0.5)));
    /// ```
    pub fn validate(&self) -> Result<(), RetrySectionError> {
        if !self.multiplier.is_finite() || !(1.0..=1e9).contains(&self.multiplier) {
            return Err(RetrySectionError::MultiplierOutOfRange(self.multiplier));
        }
        if !self.jitter.is_finite() || !(0.0..=1.0).contains(&self.jitter) {
            return Err(RetrySectionError::JitterOutOfRange(self.jitter));
        }
        if self.initial.is_zero() {
            return Err(RetrySectionError::ZeroInitial);
        }
        if self.max.is_zero() {
            return Err(RetrySectionError::ZeroMax);
        }
        if self.initial > self.max {
            return Err(RetrySectionError::InitialExceedsMax {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct BreakerSection {
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
    /// At most [`MAX_OPEN_FOR`](Self::MAX_OPEN_FOR); a breaker built without
    /// [`validate`](Self::validate) re-probes after a year.
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

impl Default for BreakerSection {
    fn default() -> Self {
        BreakerSection {
            failure_threshold: 3,
            open_for: Duration::from_secs(5),
            half_open_probes: 1,
        }
    }
}

impl BreakerSection {
    /// Ceiling on [`open_for`](Self::open_for).
    ///
    /// It is stamped into a deadline, and `Instant + Duration` panics rather
    /// than saturating. A year already means "never probe again", so anything
    /// beyond it is a typo rather than a policy.
    pub const MAX_OPEN_FOR: Duration = LONGEST_DEADLINE;

    /// Reject breaker thresholds that would misbehave at runtime.
    ///
    /// It checks `failure_threshold`, `half_open_probes` and `open_for`, in
    /// that order. [`SinkPoolConfig::validate`] runs it with the rules of the
    /// other sections.
    ///
    /// # Errors
    ///
    /// [`BreakerSectionError`], naming the offending key.
    ///
    /// ```
    /// use spate_core::sink::{BreakerSection, BreakerSectionError};
    ///
    /// assert!(BreakerSection::default().validate().is_ok());
    ///
    /// let mut wedged = BreakerSection::default();
    /// wedged.half_open_probes = 0;
    /// assert_eq!(wedged.validate(), Err(BreakerSectionError::ZeroHalfOpenProbes));
    /// ```
    pub fn validate(&self) -> Result<(), BreakerSectionError> {
        if self.failure_threshold == 0 {
            return Err(BreakerSectionError::ZeroFailureThreshold);
        }
        if self.half_open_probes == 0 {
            return Err(BreakerSectionError::ZeroHalfOpenProbes);
        }
        const OPEN_FOR: DurationBounds =
            DurationBounds::at_least(Duration::from_nanos(1)).at_most(BreakerSection::MAX_OPEN_FOR);
        check_duration(self.open_for, OPEN_FOR).map_err(BreakerSectionError::OpenForOutOfRange)
    }
}

/// Why a [`BreakerSection`] was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BreakerSectionError {
    /// `failure_threshold` is zero, which opens the breaker on the first
    /// failure as `1` does.
    #[error("breaker.failure_threshold: must be at least 1")]
    ZeroFailureThreshold,
    /// `half_open_probes` is zero, so no probe budget exists to recover with.
    #[error("breaker.half_open_probes: must be at least 1")]
    ZeroHalfOpenProbes,
    /// `open_for` is zero, which is not a quarantine at all, or exceeds
    /// [`BreakerSection::MAX_OPEN_FOR`].
    #[error("breaker.open_for: {0}")]
    OpenForOutOfRange(DurationBoundsError),
}

/// Complete sink worker-pool configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct SinkPoolConfig {
    /// Batch sealing thresholds.
    pub batch: BatchSection,
    /// In-flight limits.
    pub inflight: InflightSection,
    /// Write retry policy.
    pub retry: RetrySection,
    /// Replica circuit breaker.
    pub breaker: BreakerSection,
}

impl SinkPoolConfig {
    /// All four sections at once, as a connector's factory assembles them
    /// from its config.
    ///
    /// ```
    /// use spate_core::sink::{BatchSection, BreakerSection, InflightSection, RetrySection, SinkPoolConfig};
    ///
    /// let mut batch = BatchSection::default();
    /// batch.max_rows = 1_000;
    /// let pool = SinkPoolConfig::new(
    ///     batch,
    ///     InflightSection::default(),
    ///     RetrySection::default(),
    ///     BreakerSection::default(),
    /// );
    /// assert_eq!(pool.batch.max_rows, 1_000);
    /// ```
    #[must_use]
    pub fn new(
        batch: BatchSection,
        inflight: InflightSection,
        retry: RetrySection,
        breaker: BreakerSection,
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
    /// [`RetrySection::validate`] and [`BreakerSection::validate`], and reports
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

impl BatchSection {
    fn check(&self) -> Result<(), SinkPoolConfigError> {
        if self.max_rows == 0 {
            return Err(SinkPoolConfigError::ZeroMaxRows);
        }
        if self.max_bytes.as_u64() == 0 {
            return Err(SinkPoolConfigError::ZeroMaxBytes);
        }
        const LINGER: DurationBounds =
            DurationBounds::at_least(Duration::ZERO).at_most(LONGEST_DEADLINE);
        check_duration(self.linger, LINGER).map_err(SinkPoolConfigError::LingerTooLong)?;
        Ok(())
    }
}

impl InflightSection {
    /// The `inflight` rule alone, for the spawn guard.
    pub(crate) fn check(&self) -> Result<(), SinkPoolConfigError> {
        if self.max_per_shard == 0 {
            return Err(SinkPoolConfigError::ZeroMaxPerShard);
        }
        if self.max_per_shard > Semaphore::MAX_PERMITS {
            return Err(SinkPoolConfigError::MaxPerShardTooLarge(self.max_per_shard));
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
    #[error("batch.max_rows: must be at least 1")]
    ZeroMaxRows,
    /// `batch.max_bytes` is zero, which seals every chunk as its own batch.
    #[error("batch.max_bytes: must be greater than zero")]
    ZeroMaxBytes,
    /// `batch.linger` exceeds a year, past the longest deadline the worker
    /// arms.
    #[error("batch.linger: {0}")]
    LingerTooLong(DurationBoundsError),
    /// `inflight.max_per_shard` is zero, which parks every sealed batch.
    #[error("inflight.max_per_shard: must be at least 1")]
    ZeroMaxPerShard,
    /// `inflight.max_per_shard` exceeds tokio's [`Semaphore::MAX_PERMITS`],
    /// which the worker's in-flight semaphore cannot hold.
    #[error("inflight.max_per_shard: must not exceed {max} (got {0})", max = Semaphore::MAX_PERMITS)]
    MaxPerShardTooLarge(usize),
    /// The `retry` section failed [`RetrySection::validate`].
    #[error(transparent)]
    Retry(#[from] RetrySectionError),
    /// The `breaker` section failed [`BreakerSection::validate`].
    #[error(transparent)]
    Breaker(#[from] BreakerSectionError),
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

    fn retry(mutate: impl FnOnce(&mut RetrySection)) -> RetrySection {
        tuned(mutate)
    }

    fn slots(max_bytes: u64, chunk_bytes: usize) -> Result<usize, String> {
        QueueSection::new(max_bytes)
            .capacity(chunk_bytes, &SectionPath::sink("fake"))
            .map_err(|e| e.to_string())
    }

    /// A queue's bytes become whole chunks, rounded up, with tokio's channel
    /// bound the largest count accepted.
    #[test]
    fn a_queue_size_becomes_a_slot_count() {
        assert_eq!(slots(1024 * 1024, 64 * 1024), Ok(16));
        assert_eq!(slots(1024 * 1024 + 1, 64 * 1024), Ok(17));
        assert_eq!(slots(1, 64 * 1024), Ok(1));
        let most = Semaphore::MAX_PERMITS as u64;
        assert_eq!(slots(most, 1), Ok(Semaphore::MAX_PERMITS));
        let err = slots(most + 1, 1).unwrap_err();
        assert!(
            err.starts_with("sink.fake.queue.max_bytes: makes "),
            "{err}"
        );
        assert_eq!(
            slots(0, 64 * 1024),
            Err("sink.fake.queue.max_bytes: must be greater than zero".to_owned())
        );
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
            (retry(|c| c.initial = Duration::ZERO), "retry.initial: "),
            (retry(|c| c.max = Duration::ZERO), "retry.max: "),
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
        // Pins each retry rule's whole message, led by its key.
        assert_eq!(
            RetrySectionError::MultiplierOutOfRange(0.5).to_string(),
            "retry.multiplier: must be a finite number in [1.0, 1e9] (got 0.5)"
        );
        assert_eq!(
            RetrySectionError::JitterOutOfRange(1.5).to_string(),
            "retry.jitter: must be a finite fraction in [0.0, 1.0] (got 1.5)"
        );
        assert_eq!(
            RetrySectionError::ZeroInitial.to_string(),
            "retry.initial: must be greater than zero"
        );
        assert_eq!(
            RetrySectionError::ZeroMax.to_string(),
            "retry.max: must be greater than zero"
        );
        assert_eq!(
            RetrySectionError::InitialExceedsMax {
                initial: Duration::from_secs(10),
                max: Duration::from_secs(1),
            }
            .to_string(),
            "retry.initial: must not exceed `retry.max` (1s); found 10s"
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
        assert!(!RetrySection::default().stalls_indefinitely());
        // Right at the threshold is still fine; it is an upper bound.
        assert!(!retry(|c| c.max = Duration::from_secs(300)).stalls_indefinitely());
        assert!(retry(|c| c.max = Duration::from_secs(301)).stalls_indefinitely());
    }

    #[test]
    fn validate_accepts_the_default_and_the_boundaries() {
        assert!(RetrySection::default().validate().is_ok());
        let boundary = RetrySection {
            initial: Duration::from_nanos(1),
            max: Duration::from_nanos(1),
            multiplier: 1.0,
            jitter: 0.0,
            max_attempts: 0,
        };
        assert!(boundary.validate().is_ok(), "{boundary:?}");
        let upper = RetrySection {
            multiplier: 1e9,
            jitter: 1.0,
            ..RetrySection::default()
        };
        assert!(upper.validate().is_ok(), "{upper:?}");
    }

    fn breaker(mutate: impl FnOnce(&mut BreakerSection)) -> BreakerSection {
        tuned(mutate)
    }

    fn pool(mutate: impl FnOnce(&mut SinkPoolConfig)) -> SinkPoolConfig {
        tuned(mutate)
    }

    #[test]
    fn breaker_validate_rejects_a_budget_no_replica_could_recover_from() {
        assert_eq!(
            breaker(|c| c.failure_threshold = 0).validate(),
            Err(BreakerSectionError::ZeroFailureThreshold)
        );
        // The threshold is checked first, so a config with both zero names it.
        assert_eq!(
            breaker(|c| {
                c.failure_threshold = 0;
                c.half_open_probes = 0;
            })
            .validate(),
            Err(BreakerSectionError::ZeroFailureThreshold)
        );
        assert_eq!(
            breaker(|c| c.half_open_probes = 0).validate(),
            Err(BreakerSectionError::ZeroHalfOpenProbes)
        );
        let refused = |open_for| {
            breaker(|c| c.open_for = open_for)
                .validate()
                .expect_err("out of range")
                .to_string()
        };
        assert_eq!(
            refused(Duration::ZERO),
            "breaker.open_for: must be from 1ns to 1year; found 0s"
        );
        assert_eq!(
            refused(BreakerSection::MAX_OPEN_FOR + Duration::from_nanos(1)),
            "breaker.open_for: must be from 1ns to 1year; found 1year 1ns"
        );
    }

    #[test]
    fn breaker_validate_accepts_the_default_and_the_boundaries() {
        assert!(BreakerSection::default().validate().is_ok());
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
            breaker(|c| c.open_for = BreakerSection::MAX_OPEN_FOR)
                .validate()
                .is_ok()
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
                pool(|c| c.batch.max_bytes = ByteSize::b(0)),
                SinkPoolConfigError::ZeroMaxBytes,
            ),
            (
                pool(|c| c.inflight.max_per_shard = 0),
                SinkPoolConfigError::ZeroMaxPerShard,
            ),
            (
                pool(|c| c.retry.initial = Duration::ZERO),
                SinkPoolConfigError::Retry(RetrySectionError::ZeroInitial),
            ),
            (
                pool(|c| c.retry.max = Duration::ZERO),
                SinkPoolConfigError::Retry(RetrySectionError::ZeroMax),
            ),
            (
                pool(|c| c.breaker.failure_threshold = 0),
                SinkPoolConfigError::Breaker(BreakerSectionError::ZeroFailureThreshold),
            ),
            (
                pool(|c| c.breaker.half_open_probes = 0),
                SinkPoolConfigError::Breaker(BreakerSectionError::ZeroHalfOpenProbes),
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
            c.batch.max_bytes = ByteSize::b(1);
            c.inflight.max_per_shard = 1;
            c.breaker.failure_threshold = 1;
        });
        assert_eq!(ones.validate(), Ok(()), "{ones:?}");
        let upper = pool(|c| {
            c.batch.max_rows = u64::MAX;
            c.batch.max_bytes = ByteSize::b(u64::MAX);
            c.batch.linger = LONGEST_DEADLINE;
            c.inflight.max_per_shard = Semaphore::MAX_PERMITS;
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

    #[test]
    fn pool_validate_rejects_values_past_their_ceilings() {
        let linger = LONGEST_DEADLINE + Duration::from_nanos(1);
        let err = pool(|c| c.batch.linger = linger).validate().unwrap_err();
        assert!(
            matches!(err, SinkPoolConfigError::LingerTooLong(_)),
            "{err:?}"
        );
        let permits = Semaphore::MAX_PERMITS + 1;
        assert_eq!(
            pool(|c| c.inflight.max_per_shard = permits).validate(),
            Err(SinkPoolConfigError::MaxPerShardTooLarge(permits))
        );
    }

    /// Pins that a `linger` the YAML loader parses but no deadline can hold
    /// is refused naming the key, and that `1y` itself passes.
    #[test]
    fn a_yaml_linger_past_a_year_is_refused() {
        let batch: BatchSection = serde_yaml::from_str("linger: 300000000000y").expect("parses");
        let err = pool(|c| c.batch = batch).validate().unwrap_err();
        assert!(
            matches!(err, SinkPoolConfigError::LingerTooLong(_)),
            "{err:?}"
        );
        assert!(err.to_string().starts_with("batch.linger: "), "{err}");

        let year: BatchSection = serde_yaml::from_str("linger: 1y").expect("parses");
        assert_eq!(pool(|c| c.batch = year).validate(), Ok(()));
        let two: BatchSection = serde_yaml::from_str("linger: 2y").expect("parses");
        assert_eq!(
            pool(|c| c.batch = two).validate().unwrap_err().to_string(),
            "batch.linger: must be from 0s to 1year; found 2years"
        );
    }

    /// Pins that `breaker.open_for: 1y`, refused by v0.4.0, now loads.
    #[test]
    fn a_yaml_open_for_of_a_year_validates() {
        let breaker: BreakerSection = serde_yaml::from_str("open_for: 1y").expect("parses");
        assert_eq!(breaker.validate(), Ok(()));
    }

    /// Pins the section order: batch, inflight, retry, breaker.
    #[test]
    fn pool_validate_reports_the_first_invalid_section() {
        let mut cfg = pool(|c| {
            c.batch.max_rows = 0;
            c.batch.max_bytes = ByteSize::b(0);
            c.inflight.max_per_shard = 0;
            c.retry.initial = Duration::ZERO;
            c.breaker.failure_threshold = 0;
        });
        assert_eq!(cfg.validate(), Err(SinkPoolConfigError::ZeroMaxRows));
        cfg.batch.max_rows = 1;
        assert_eq!(cfg.validate(), Err(SinkPoolConfigError::ZeroMaxBytes));
        cfg.batch.max_bytes = ByteSize::b(1);
        assert_eq!(cfg.validate(), Err(SinkPoolConfigError::ZeroMaxPerShard));
        cfg.inflight.max_per_shard = 1;
        assert_eq!(
            cfg.validate(),
            Err(SinkPoolConfigError::Retry(RetrySectionError::ZeroInitial))
        );
        cfg.retry.initial = Duration::from_millis(1);
        assert_eq!(
            cfg.validate(),
            Err(SinkPoolConfigError::Breaker(
                BreakerSectionError::ZeroFailureThreshold
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
                "batch.max_rows: must be at least 1",
            ),
            (
                SinkPoolConfigError::ZeroMaxBytes,
                "batch.max_bytes: must be greater than zero",
            ),
            (
                SinkPoolConfigError::ZeroMaxPerShard,
                "inflight.max_per_shard: must be at least 1",
            ),
            (
                SinkPoolConfigError::Breaker(BreakerSectionError::ZeroFailureThreshold),
                "breaker.failure_threshold: must be at least 1",
            ),
            (
                SinkPoolConfigError::Breaker(BreakerSectionError::ZeroHalfOpenProbes),
                "breaker.half_open_probes: must be at least 1",
            ),
            (
                SinkPoolConfigError::MaxPerShardTooLarge(Semaphore::MAX_PERMITS + 1),
                &format!(
                    "inflight.max_per_shard: must not exceed {} (got {})",
                    Semaphore::MAX_PERMITS,
                    Semaphore::MAX_PERMITS + 1
                ),
            ),
            (
                SinkPoolConfigError::Retry(RetrySectionError::MultiplierOutOfRange(0.5)),
                "retry.multiplier: must be a finite number in [1.0, 1e9] (got 0.5)",
            ),
        ];
        for (err, want) in cases {
            assert_eq!(err.to_string(), want);
        }
    }
}
