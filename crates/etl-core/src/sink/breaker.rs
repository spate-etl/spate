//! Per-replica circuit breakers for one shard.
//!
//! State machine per replica: `Closed` (counting consecutive failures) →
//! `Open` (rejecting until a deadline) → `HalfOpen` (a bounded number of
//! probe writes) → `Closed` on probe success or back to `Open` on failure.
//! The set is shared between the shard worker (replica selection) and its
//! in-flight write tasks (outcome reporting) behind a mutex — a handful of
//! uncontended lockings per batch, never on the record path.

use super::config::BreakerConfig;
use crate::metrics::SinkShardMetrics;
use std::sync::Arc;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Closed { consecutive_failures: u32 },
    Open { until: Instant },
    HalfOpen { probes_in_flight: u32 },
}

/// Breaker state for every replica of one shard, plus the round-robin
/// rotation cursor.
#[derive(Debug)]
pub(crate) struct BreakerSet {
    cfg: BreakerConfig,
    states: Vec<State>,
    cursor: usize,
    metrics: Arc<SinkShardMetrics>,
}

impl BreakerSet {
    pub(crate) fn new(replicas: usize, cfg: BreakerConfig, metrics: Arc<SinkShardMetrics>) -> Self {
        assert!(replicas > 0, "a shard needs at least one replica");
        for r in 0..replicas {
            metrics.set_replica_healthy(r, true);
        }
        BreakerSet {
            cfg,
            states: vec![
                State::Closed {
                    consecutive_failures: 0
                };
                replicas
            ],
            cursor: 0,
            metrics,
        }
    }

    /// Pick the next usable replica, rotating round-robin and skipping open
    /// breakers (open breakers past their deadline transition to half-open
    /// and become usable as probes). `None` when every replica is open.
    pub(crate) fn next_replica(&mut self, now: Instant) -> Option<usize> {
        let n = self.states.len();
        for step in 0..n {
            let idx = (self.cursor + step) % n;
            match self.states[idx] {
                State::Closed { .. } => {
                    self.cursor = (idx + 1) % n;
                    return Some(idx);
                }
                State::Open { until } if now >= until => {
                    self.states[idx] = State::HalfOpen {
                        probes_in_flight: 1,
                    };
                    self.cursor = (idx + 1) % n;
                    return Some(idx);
                }
                State::HalfOpen { probes_in_flight }
                    if probes_in_flight < self.cfg.half_open_probes =>
                {
                    self.states[idx] = State::HalfOpen {
                        probes_in_flight: probes_in_flight + 1,
                    };
                    self.cursor = (idx + 1) % n;
                    return Some(idx);
                }
                _ => {}
            }
        }
        None
    }

    /// Earliest instant at which an open breaker becomes half-open — how
    /// long a fully-open shard should wait before re-picking.
    pub(crate) fn next_probe_at(&self, now: Instant) -> Option<Instant> {
        self.states
            .iter()
            .filter_map(|s| match s {
                State::Open { until } => Some(*until),
                _ => None,
            })
            .min()
            .map(|t| t.max(now))
    }

    /// Record a successful write on `replica`.
    pub(crate) fn on_success(&mut self, replica: usize) {
        let was_unhealthy = !matches!(self.states[replica], State::Closed { .. });
        self.states[replica] = State::Closed {
            consecutive_failures: 0,
        };
        if was_unhealthy {
            self.metrics.set_replica_healthy(replica, true);
        }
    }

    /// Record a failed write on `replica`.
    pub(crate) fn on_failure(&mut self, replica: usize, now: Instant) {
        let next = match self.states[replica] {
            State::Closed {
                consecutive_failures,
            } => {
                let failures = consecutive_failures + 1;
                if failures >= self.cfg.failure_threshold {
                    State::Open {
                        until: now + self.cfg.open_for,
                    }
                } else {
                    State::Closed {
                        consecutive_failures: failures,
                    }
                }
            }
            // A failed half-open probe re-opens immediately.
            State::HalfOpen { .. } => State::Open {
                until: now + self.cfg.open_for,
            },
            State::Open { until } => State::Open { until },
        };
        let newly_open = matches!(next, State::Open { .. })
            && !matches!(self.states[replica], State::Open { .. });
        self.states[replica] = next;
        if newly_open {
            self.metrics.set_replica_healthy(replica, false);
            self.metrics.breaker_opened(replica);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_open(&self, replica: usize) -> bool {
        matches!(self.states[replica], State::Open { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::ComponentLabels;
    use std::time::Duration;

    fn set(replicas: usize, threshold: u32) -> BreakerSet {
        let labels = ComponentLabels::new("p", "sink", "test");
        let names: Vec<String> = (0..replicas).map(|r| format!("r{r}")).collect();
        BreakerSet::new(
            replicas,
            BreakerConfig {
                failure_threshold: threshold,
                open_for: Duration::from_secs(5),
                half_open_probes: 1,
            },
            Arc::new(SinkShardMetrics::new(&labels, 0, &names)),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn rotates_round_robin_over_healthy_replicas() {
        let mut b = set(3, 3);
        let now = Instant::now();
        assert_eq!(b.next_replica(now), Some(0));
        assert_eq!(b.next_replica(now), Some(1));
        assert_eq!(b.next_replica(now), Some(2));
        assert_eq!(b.next_replica(now), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn opens_after_threshold_and_skips_while_open() {
        let mut b = set(2, 2);
        let now = Instant::now();
        b.on_failure(0, now);
        assert!(!b.is_open(0), "below threshold");
        b.on_failure(0, now);
        assert!(b.is_open(0));
        // Rotation only yields replica 1 while 0 is open.
        assert_eq!(b.next_replica(now), Some(1));
        assert_eq!(b.next_replica(now), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn success_resets_the_failure_streak() {
        let mut b = set(1, 3);
        let now = Instant::now();
        b.on_failure(0, now);
        b.on_failure(0, now);
        b.on_success(0);
        b.on_failure(0, now);
        b.on_failure(0, now);
        assert!(!b.is_open(0), "streak was reset by the success");
    }

    #[tokio::test(start_paused = true)]
    async fn half_open_probe_recovers_or_reopens() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        b.on_failure(0, t0);
        assert!(b.is_open(0));
        assert_eq!(b.next_replica(t0), None, "open: no replica");
        assert_eq!(b.next_probe_at(t0), Some(t0 + Duration::from_secs(5)));

        // Past the deadline: half-open allows exactly one probe.
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(b.next_replica(t1), Some(0));
        assert_eq!(b.next_replica(t1), None, "probe budget exhausted");

        // Probe failure re-opens; probe success closes.
        b.on_failure(0, t1);
        assert!(b.is_open(0));
        let t2 = t1 + Duration::from_secs(6);
        assert_eq!(b.next_replica(t2), Some(0));
        b.on_success(0);
        assert_eq!(b.next_replica(t2), Some(0), "closed again");
    }
}
