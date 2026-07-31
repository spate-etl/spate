//! Per-replica circuit breakers for one shard.
//!
//! State machine per replica: `Closed` (counting consecutive failures) →
//! `Open` (rejecting until a deadline) → `HalfOpen` (a bounded number of
//! probe writes) → `Closed` on probe success or back to `Open` on failure.
//! The set is shared between the shard worker (replica selection) and its
//! in-flight write tasks (outcome reporting) behind a mutex — a handful of
//! uncontended lockings per batch, never on the record path.
//!
//! "Nothing is pickable" is two conditions, not one, and they end
//! differently. An `Open` breaker is released by a *deadline* the caller can
//! compute for itself ([`BreakerSet::next_probe_at`]). A set that is entirely
//! `HalfOpen` with its probe budget spent has no deadline at all — only
//! *leaving* `HalfOpen` frees a slot — and is released by an *event*: the
//! in-flight probe reporting. [`BreakerSet::subscribe`] is that event.

use super::config::BreakerConfig;
use crate::metrics::SinkShardMetrics;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Closed { consecutive_failures: u32 },
    Open { until: Instant },
    HalfOpen { probes_in_flight: u32 },
}

/// One replica handed out by [`BreakerSet::next_replica`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Pick {
    /// Index into the shard's endpoint list.
    pub(crate) replica: usize,
    /// This pick consumed a half-open probe slot, so the caller must release
    /// it if the attempt never reports an outcome. See
    /// [`BreakerSet::release_probe`].
    pub(crate) probe: bool,
}

/// Breaker state for every replica of one shard, plus the round-robin
/// rotation cursor.
#[derive(Debug)]
pub(crate) struct BreakerSet {
    cfg: BreakerConfig,
    states: Vec<State>,
    cursor: usize,
    metrics: Arc<SinkShardMetrics>,
    /// Last *reported* shard health (≥1 replica circuit-closed), so the
    /// caller's log fires on transitions only. The gauge is written on every
    /// outcome regardless — see [`BreakerSet::refresh_shard_health`].
    shard_healthy: bool,
    /// Bumped whenever a replica may have become pickable, releasing write
    /// tasks parked on a set that offered them nothing. See the module
    /// header for why a deadline cannot cover that state.
    ///
    /// The sender lives here, under the mutex, for two reasons. It cannot be
    /// forgotten at a call site: the methods that can make a replica usable
    /// are the methods that publish. And as a field of the set every waiter
    /// holds an `Arc` to, it outlives every receiver, so
    /// [`watch::Receiver::changed`] can never enter the permanently-`Err`
    /// state that the drain watch needs a liveness flag to guard against.
    ///
    /// Not a breach of the "no subscriber under this mutex" rule that
    /// [`ShardHealthTransition`] exists for: that rule is about `tracing`,
    /// which runs arbitrary user-installed code. `send_modify` runs a version
    /// bump and waker wakes — the same class of side effect as the gauge
    /// writes already made here.
    wake_tx: watch::Sender<u64>,
}

impl BreakerSet {
    pub(crate) fn new(replicas: usize, cfg: BreakerConfig, metrics: Arc<SinkShardMetrics>) -> Self {
        assert!(replicas > 0, "a shard needs at least one replica");
        for r in 0..replicas {
            metrics.set_replica_healthy(r, true);
        }
        // Every replica starts circuit-closed, so the shard starts healthy.
        metrics.set_shard_healthy(true);
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
            shard_healthy: true,
            wake_tx: watch::Sender::new(0),
        }
    }

    /// A receiver for the wake signal, marked as having seen the present.
    /// Every write task takes one before it can park on a set that offers it
    /// nothing.
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.wake_tx.subscribe()
    }

    /// Release every parked write task so it re-picks against the state this
    /// outcome just produced.
    ///
    /// A failure that leaves nothing pickable still publishes: a probe
    /// failing turns `HalfOpen` into `Open { until }`, which is a *deadline*
    /// where a moment ago there was none, and a waiter that computed its wake
    /// from the old state has to recompute it.
    ///
    /// Ordering, which is the whole point: a waiter snapshots its receiver's
    /// version *before* it reads the states, and this bump happens *after*
    /// the mutation and under the same mutex. The two critical sections
    /// serialize, so a waiter that read the pre-outcome states is necessarily
    /// behind this version and its next `changed()` returns without ever
    /// suspending. There is no interleaving in which a waiter observes the
    /// old state and also misses the wake.
    fn publish_wake(&self) {
        self.wake_tx.send_modify(|g| *g = g.wrapping_add(1));
    }

    /// Pick the next usable replica, rotating round-robin and skipping open
    /// breakers (open breakers past their deadline transition to half-open
    /// and become usable as probes). `None` when every replica is open.
    pub(crate) fn next_replica(&mut self, now: Instant) -> Option<Pick> {
        let n = self.states.len();
        for step in 0..n {
            let idx = (self.cursor + step) % n;
            match self.states[idx] {
                State::Closed { .. } => {
                    self.cursor = (idx + 1) % n;
                    return Some(Pick {
                        replica: idx,
                        probe: false,
                    });
                }
                State::Open { until } if now >= until => {
                    self.states[idx] = State::HalfOpen {
                        probes_in_flight: 1,
                    };
                    self.cursor = (idx + 1) % n;
                    return Some(Pick {
                        replica: idx,
                        probe: true,
                    });
                }
                State::HalfOpen { probes_in_flight }
                    if probes_in_flight < self.cfg.half_open_probes =>
                {
                    self.states[idx] = State::HalfOpen {
                        probes_in_flight: probes_in_flight + 1,
                    };
                    self.cursor = (idx + 1) % n;
                    return Some(Pick {
                        replica: idx,
                        probe: true,
                    });
                }
                _ => {}
            }
        }
        None
    }

    /// Hand back a half-open probe slot whose attempt never reported an
    /// outcome, and wake anyone parked waiting for one.
    ///
    /// `probes_in_flight` is otherwise cleared only by *leaving* `HalfOpen`,
    /// which is what [`on_success`](Self::on_success) and
    /// [`on_failure`](Self::on_failure) do. A write task that dies without
    /// reporting — a writer panic, whose `JoinError` carries no replica — would
    /// therefore consume a slot for good, and with the default
    /// `half_open_probes: 1` that pins the replica in `HalfOpen` forever:
    /// `next_replica` never offers it again and the shard is unwritable for
    /// the life of the process. No timer recovers that, because re-picking
    /// finds the same exhausted budget.
    ///
    /// `HalfOpen { probes_in_flight: 0 }` is the correct resting state after
    /// a release — the probe never happened, so the next caller should be
    /// allowed to take it.
    ///
    /// Only meaningful while the replica is still `HalfOpen`; a replica that
    /// has since moved on had its budget cleared wholesale, so this is a
    /// no-op. Callers must still disarm rather than rely on that check — see
    /// `ProbeGuard` in the worker.
    pub(crate) fn release_probe(&mut self, replica: usize) {
        if let State::HalfOpen { probes_in_flight } = self.states[replica] {
            self.states[replica] = State::HalfOpen {
                probes_in_flight: probes_in_flight.saturating_sub(1),
            };
        }
        self.publish_wake();
    }

    /// Earliest instant at which an open breaker becomes half-open.
    ///
    /// Only `Open` folds in, because it is the only state with a *scheduled*
    /// transition. A set that is entirely `HalfOpen` with its probe budget
    /// spent returns `None` from here *and* from
    /// [`next_replica`](Self::next_replica): nothing is pickable and no clock
    /// will change that. What will is the in-flight probe reporting — see
    /// [`subscribe`](Self::subscribe). So this is not, on its own, "how long
    /// a shard should wait"; the worker takes the min of it and a bounded
    /// heartbeat, and waits on the wake signal alongside both.
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

    /// Record a successful write on `replica`. Returns the shard-health
    /// transition, if any, for the caller to log outside the lock.
    pub(crate) fn on_success(&mut self, replica: usize) -> Option<ShardHealthTransition> {
        self.states[replica] = State::Closed {
            consecutive_failures: 0,
        };
        self.publish_replica_health(replica);
        self.publish_wake();
        self.refresh_shard_health()
    }

    /// Record a failed write on `replica`. Returns the shard-health
    /// transition, if any, for the caller to log outside the lock.
    pub(crate) fn on_failure(
        &mut self,
        replica: usize,
        now: Instant,
    ) -> Option<ShardHealthTransition> {
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
        self.publish_replica_health(replica);
        // The counter stays edge-triggered: it counts transitions, not state.
        if newly_open {
            self.metrics.breaker_opened(replica);
        }
        self.publish_wake();
        self.refresh_shard_health()
    }

    /// Republish one replica's health gauge from its current state.
    ///
    /// Level-driven, not edge-triggered: writing only on a transition means a
    /// reading that was wrong when it was written — clobbered by another
    /// handle set, or lost across a restart of whatever scraped it — stands
    /// until the *next* transition, which for a quarantined replica may never
    /// come. Every write outcome refreshes it instead, so a stale reading
    /// self-corrects within one probe cycle. `HalfOpen` reads `0`: a replica
    /// being probed is not yet usable, which is the same rule shard health
    /// uses.
    fn publish_replica_health(&self, replica: usize) {
        let healthy = matches!(self.states[replica], State::Closed { .. });
        self.metrics.set_replica_healthy(replica, healthy);
    }

    /// Recompute shard health (≥1 replica circuit-closed), republish the
    /// gauge, and report a transition for the caller to log — the log only on
    /// an edge, the gauge every time. `next_replica`'s Open→HalfOpen promotion
    /// neither adds nor removes a `Closed` state, so only the failure/success
    /// paths can move this signal.
    ///
    /// The gauge write is unconditional so that a reading that no longer
    /// matches the breakers cannot persist: an edge-triggered writer that has
    /// already published `0` never publishes `0` again, so a shard whose every
    /// replica is quarantined — the state that most needs to be visible —
    /// would keep serving whatever value it was left at. Rewriting it on each
    /// outcome bounds that to one probe cycle. `Gauge::set` is an atomic
    /// store; this runs per write attempt, never per record.
    fn refresh_shard_health(&mut self) -> Option<ShardHealthTransition> {
        let up = self
            .states
            .iter()
            .any(|s| matches!(s, State::Closed { .. }));
        self.metrics.set_shard_healthy(up);
        if up == self.shard_healthy {
            return None;
        }
        self.shard_healthy = up;
        Some(if up {
            ShardHealthTransition::Recovered
        } else {
            ShardHealthTransition::AllQuarantined
        })
    }

    /// Just the replica index, for the many assertions that do not care
    /// whether the pick consumed a probe slot. `Pick::probe` is asserted
    /// directly where it is the point.
    #[cfg(test)]
    fn next_replica_idx(&mut self, now: Instant) -> Option<usize> {
        self.next_replica(now).map(|p| p.replica)
    }

    #[cfg(test)]
    pub(crate) fn is_open(&self, replica: usize) -> bool {
        matches!(self.states[replica], State::Open { .. })
    }

    #[cfg(test)]
    pub(crate) fn shard_healthy(&self) -> bool {
        self.shard_healthy
    }
}

/// A shard-health edge reported by [`BreakerSet`], logged by the write task
/// after the breaker lock is released — tracing subscribers must never run
/// under that mutex.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShardHealthTransition {
    Recovered,
    AllQuarantined,
}

impl ShardHealthTransition {
    pub(crate) fn log(self, shard: u32) {
        match self {
            ShardHealthTransition::Recovered => {
                tracing::info!(shard, "shard recovered a healthy replica");
            }
            ShardHealthTransition::AllQuarantined => {
                tracing::error!(
                    shard,
                    "all replicas quarantined; sink is back-pressuring the source"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::ComponentLabels;
    use std::time::Duration;

    /// A component name unique to each `set()` call. `SinkShardMetrics` owns
    /// its gauge series (one live owner per process); these tests run
    /// concurrently under `cargo test`, and while most only read the cached
    /// `shard_healthy()` bool — unaffected by shadowing — the level-drive test
    /// below reads the rendered gauge, so it must own the series it inspects.
    fn breaker_component() -> String {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        format!(
            "breaker-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    fn set(replicas: usize, threshold: u32) -> BreakerSet {
        let labels = ComponentLabels::new("p", breaker_component(), "test");
        let names: Vec<String> = (0..replicas).map(|r| format!("r{r}")).collect();
        BreakerSet::new(
            replicas,
            BreakerConfig {
                failure_threshold: threshold,
                open_for: Duration::from_secs(5),
                half_open_probes: 1,
            },
            Arc::new(SinkShardMetrics::new(
                &labels,
                0,
                &names,
                crate::metrics::E2eBasis::Ingest,
            )),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn rotates_round_robin_over_healthy_replicas() {
        let mut b = set(3, 3);
        let now = Instant::now();
        assert_eq!(b.next_replica_idx(now), Some(0));
        assert_eq!(b.next_replica_idx(now), Some(1));
        assert_eq!(b.next_replica_idx(now), Some(2));
        assert_eq!(b.next_replica_idx(now), Some(0));
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
        assert_eq!(b.next_replica_idx(now), Some(1));
        assert_eq!(b.next_replica_idx(now), Some(1));
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
        assert_eq!(b.next_replica_idx(t0), None, "open: no replica");
        assert_eq!(b.next_probe_at(t0), Some(t0 + Duration::from_secs(5)));

        // Past the deadline: half-open allows exactly one probe.
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t1), Some(0));
        assert_eq!(b.next_replica_idx(t1), None, "probe budget exhausted");

        // Probe failure re-opens; probe success closes.
        b.on_failure(0, t1);
        assert!(b.is_open(0));
        let t2 = t1 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t2), Some(0));
        b.on_success(0);
        assert_eq!(b.next_replica_idx(t2), Some(0), "closed again");
    }

    /// The fact that forces the wait on a fully-quarantined shard to be
    /// event-driven: once every replica is half-open with its probe budget
    /// spent, *both* selectors return `None` and no clock will change that.
    /// There is no deadline to sleep until, only an outcome to wait for.
    #[tokio::test(start_paused = true)]
    async fn a_fully_probing_set_offers_neither_a_replica_nor_a_deadline() {
        let mut b = set(2, 1);
        let t0 = Instant::now();
        b.on_failure(0, t0);
        b.on_failure(1, t0);

        // Past both deadlines: each replica yields its single probe, and the
        // set is then entirely half-open with nothing left to hand out.
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t1), Some(0));
        assert_eq!(b.next_replica_idx(t1), Some(1));
        assert_eq!(b.next_replica_idx(t1), None, "both probe budgets spent");
        assert_eq!(
            b.next_probe_at(t1),
            None,
            "half-open carries no scheduled transition, so there is no \
             instant to wait until"
        );
    }

    /// `Pick::probe` is what tells the write task whether it is holding a
    /// budgeted slot it must hand back. A pick off a closed breaker is not.
    #[tokio::test(start_paused = true)]
    async fn only_a_half_open_pick_reports_itself_as_a_probe() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        assert_eq!(
            b.next_replica(t0),
            Some(Pick {
                replica: 0,
                probe: false
            }),
            "a closed breaker consumes no budget"
        );

        b.on_failure(0, t0);
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(
            b.next_replica(t1),
            Some(Pick {
                replica: 0,
                probe: true
            }),
            "the open→half-open promotion spends the first probe"
        );
    }

    /// Without this, a write task that dies without reporting an outcome —
    /// a writer panic — consumes the probe slot for good, and with the
    /// default budget of one that pins the replica in half-open forever.
    #[tokio::test(start_paused = true)]
    async fn a_released_probe_slot_becomes_pickable_again() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        b.on_failure(0, t0);
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t1), Some(0));
        assert_eq!(b.next_replica_idx(t1), None, "budget spent");

        b.release_probe(0);
        assert_eq!(
            b.next_replica_idx(t1),
            Some(0),
            "the probe never happened, so the next caller may take it"
        );
    }

    /// A guard that drops after its outcome was already reported must not
    /// hand back a slot the reported outcome already cleared — another batch
    /// may have re-promoted the replica since. The worker disarms rather than
    /// relying on this, but the state check is the second line of defence.
    #[tokio::test(start_paused = true)]
    async fn releasing_a_replica_that_moved_on_does_not_grant_extra_probes() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        b.on_failure(0, t0);
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t1), Some(0));

        // The probe succeeded: the replica is closed and the budget is gone
        // wholesale. A late release must not resurrect it as a half-open slot.
        b.on_success(0);
        b.release_probe(0);
        assert!(!b.is_open(0), "still closed");

        // Re-open it and confirm the budget is the configured one, not one
        // inflated by the stale release.
        b.on_failure(0, t1);
        let t2 = t1 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t2), Some(0));
        assert_eq!(b.next_replica_idx(t2), None, "still exactly one probe");
    }

    #[tokio::test(start_paused = true)]
    async fn shard_healthy_until_the_last_replica_opens() {
        let mut b = set(2, 1);
        let now = Instant::now();
        assert!(b.shard_healthy(), "all replicas start closed");
        // One replica quarantined: the shard still has a healthy replica.
        assert_eq!(b.on_failure(0, now), None, "no shard-level transition");
        assert!(b.is_open(0));
        assert!(b.shard_healthy(), "replica 1 is still closed");
        // Both replicas quarantined: the shard is fully unhealthy.
        assert_eq!(
            b.on_failure(1, now),
            Some(ShardHealthTransition::AllQuarantined)
        );
        assert!(b.is_open(1));
        assert!(!b.shard_healthy(), "every replica is open");
    }

    #[tokio::test(start_paused = true)]
    async fn shard_health_recovers_only_on_probe_success() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        assert_eq!(
            b.on_failure(0, t0),
            Some(ShardHealthTransition::AllQuarantined)
        );
        assert!(!b.shard_healthy(), "the only replica is open");

        // A half-open probe does not restore shard health by itself — the
        // probed replica is HalfOpen, not Closed.
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t1), Some(0));
        assert!(!b.shard_healthy(), "probing, not yet confirmed healthy");
        // Only a successful probe (→ Closed) flips it back.
        assert_eq!(b.on_success(0), Some(ShardHealthTransition::Recovered));
        assert!(b.shard_healthy(), "probe closed the breaker");
    }

    #[tokio::test(start_paused = true)]
    async fn shard_health_does_not_flap_across_probe_failures() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        assert_eq!(
            b.on_failure(0, t0),
            Some(ShardHealthTransition::AllQuarantined)
        );
        assert!(!b.shard_healthy());
        // Probe, fail, probe, fail: the shard must stay unhealthy throughout
        // — no spurious "recovered" transition on each half-open cycle.
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t1), Some(0));
        assert!(!b.shard_healthy());
        assert_eq!(b.on_failure(0, t1), None, "re-open is not a transition");
        assert!(!b.shard_healthy());
        let t2 = t1 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t2), Some(0));
        assert!(!b.shard_healthy());
        assert_eq!(b.on_failure(0, t2), None, "still quarantined, no edge");
        assert!(!b.shard_healthy());
    }

    /// The health gauges are level-driven: every write outcome republishes
    /// them, not only the ones that flip the cached state. An edge-triggered
    /// writer that has already published `0` never publishes `0` again, so a
    /// reading knocked off the truth — by another handle set, or lost across a
    /// restart of whatever scraped it — would stand until the next transition,
    /// which for a wedged shard may be never.
    ///
    /// The test clobbers both gauges to a healthy-looking `1` directly through
    /// the facade, then drives a failure that is deliberately *not* a
    /// transition (the shard is already down), and asserts the exposition is
    /// back to `0`. Against an edge-triggered writer the clobbered `1` would
    /// survive.
    #[tokio::test(start_paused = true)]
    async fn health_gauges_are_republished_on_every_outcome_not_just_transitions() {
        use crate::metrics::names;

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let labels = ComponentLabels::new("p", breaker_component(), "test");
        let gauge = |name: &str| {
            handle
                .render()
                .lines()
                .find(|l| l.starts_with(name))
                .and_then(|l| l.rsplit(' ').next())
                .and_then(|v| v.parse::<f64>().ok())
        };

        metrics::with_local_recorder(&recorder, || {
            // Keep a handle to the same metrics the breaker writes through, so
            // the clobber below hits the very series the breaker republishes.
            let metrics = Arc::new(SinkShardMetrics::new(
                &labels,
                0,
                &["r0".into(), "r1".into()],
                crate::metrics::E2eBasis::Ingest,
            ));
            let mut b = BreakerSet::new(
                2,
                BreakerConfig {
                    failure_threshold: 1,
                    open_for: Duration::from_secs(5),
                    half_open_probes: 1,
                },
                Arc::clone(&metrics),
            );

            // Quarantine both replicas: the shard is down and stays down.
            b.on_failure(0, Instant::now());
            b.on_failure(1, Instant::now());
            assert!(!b.shard_healthy());
            assert_eq!(gauge(names::SINK_SHARD_HEALTHY), Some(0.0));
            assert_eq!(gauge(names::SINK_REPLICA_HEALTHY), Some(0.0));

            // A scrape target restarts, or a rogue writer intervenes: the
            // gauges now read a healthy shard that is in fact fully down.
            metrics.set_shard_healthy(true);
            metrics.set_replica_healthy(0, true);
            assert_eq!(gauge(names::SINK_SHARD_HEALTHY), Some(1.0), "clobbered");

            // One more failed outcome — not a transition, the shard was
            // already down — must put the truth back.
            assert_eq!(
                b.on_failure(0, Instant::now()),
                None,
                "already quarantined: no edge"
            );
            assert_eq!(
                gauge(names::SINK_SHARD_HEALTHY),
                Some(0.0),
                "a non-transition outcome must still republish shard health"
            );
            assert_eq!(
                gauge(names::SINK_REPLICA_HEALTHY),
                Some(0.0),
                "and per-replica health with it"
            );
        });
    }
}
