//! Sink worker-pool tuning knobs.
//!
//! These are framework-level structs; the pipeline runtime maps the user's
//! YAML sink section onto them.

use std::time::Duration;

/// Batch sealing thresholds for one shard worker. A batch seals as soon as
/// **any** threshold trips; since chunks arrive whole, a sealed batch may
/// overshoot `max_rows`/`max_bytes` by at most one chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchConfig {
    /// Seal at this many rows.
    pub max_rows: u64,
    /// Seal at this many encoded bytes.
    pub max_bytes: u64,
    /// Seal a non-empty batch this long after its first chunk arrived,
    /// bounding latency at low throughput.
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InflightConfig {
    /// Concurrent sealed batches per shard. While all permits are taken the
    /// worker stops consuming its queue, which fills and surfaces as
    /// backpressure.
    pub max_per_shard: usize,
}

impl Default for InflightConfig {
    fn default() -> Self {
        InflightConfig { max_per_shard: 2 }
    }
}

/// Retry policy for batch writes. Retries rotate across healthy replicas;
/// the sealed batch and its deduplication token are reused unchanged.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RetryConfig {
    /// First backoff delay.
    pub initial: Duration,
    /// Backoff cap.
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

/// Per-replica circuit breaker thresholds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BreakerConfig {
    /// Consecutive failures that open the breaker.
    pub failure_threshold: u32,
    /// How long an open breaker rejects a replica before probing again.
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
