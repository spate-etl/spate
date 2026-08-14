//! Per-replica circuit breakers for one shard.
//!
//! State machine per replica: `Closed` (counting consecutive failures) →
//! `Open` (rejecting until a deadline) → `HalfOpen` (a bounded number of
//! probe writes) → `Closed` on probe success or back to `Open` on failure.
//! The set is shared between the shard worker (replica selection) and its
//! in-flight write tasks (outcome reporting) behind a mutex, taken a handful
//! of uncontended times per batch and never on the record path.
//!
//! "Nothing is pickable" covers two conditions, and they end differently. An
//! `Open` breaker is released by a *deadline* the caller can compute for
//! itself ([`BreakerSet::next_probe_at`]). A set that is entirely `HalfOpen`
//! with its probe budget spent has no deadline at all, because only *leaving*
//! `HalfOpen` frees a slot; it is released by an *event*, the in-flight probe
//! reporting. [`BreakerSet::subscribe`] is that event.

use super::config::BreakerConfig;
use crate::metrics::SinkShardMetrics;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Closed {
        consecutive_failures: u32,
    },
    Open {
        until: Instant,
    },
    HalfOpen {
        probes_in_flight: u32,
        /// Which half-open *episode* this is. A replica can enter half-open,
        /// re-open, and enter it again while a probe slot from the first
        /// episode is still unaccounted for; without this, handing that stale
        /// slot back would credit the later episode and let it run one more
        /// concurrent probe than `half_open_probes` allows.
        episode: u64,
    },
}

/// One replica handed out by [`BreakerSet::next_replica`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Pick {
    /// Index into the shard's endpoint list.
    pub(crate) replica: usize,
    /// `Some(episode)` when this pick spent a half-open probe slot, which the
    /// caller must hand back if the attempt never reports an outcome. The
    /// episode identifies *which* half-open run the slot came from, so a
    /// release that arrives after the replica has re-opened and re-probed is
    /// discarded rather than credited to the wrong one. See
    /// [`BreakerSet::release_probe`].
    pub(crate) probe: Option<u64>,
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
    /// outcome regardless; see [`BreakerSet::refresh_shard_health`].
    shard_healthy: bool,
    /// Bumped whenever a replica may have become pickable, releasing write
    /// tasks parked on a set that offered them nothing. See the module
    /// header for why a deadline cannot cover that state.
    ///
    /// The sender lives here, under the mutex. The methods that can make a
    /// replica usable are the methods that publish, so a call site cannot
    /// forget it. As a field of the set every waiter holds an `Arc` to, it
    /// outlives every receiver, so [`watch::Receiver::changed`] cannot enter
    /// the permanently-`Err` state that the drain watch needs a liveness flag
    /// to guard against.
    ///
    /// The "no subscriber under this mutex" rule that
    /// [`ShardHealthTransition`] serves is about `tracing`, which runs
    /// arbitrary user-installed code. `send_modify` runs a version bump and
    /// waker wakes, the same class of side effect as the gauge writes already
    /// made here.
    wake_tx: watch::Sender<u64>,
    /// Source of half-open episode numbers. Monotonic across the whole set;
    /// the numbers distinguish one episode from another and are not dense per
    /// replica.
    next_episode: u64,
}

impl BreakerSet {
    pub(crate) fn new(
        replicas: usize,
        mut cfg: BreakerConfig,
        metrics: Arc<SinkShardMetrics>,
    ) -> Self {
        assert!(replicas > 0, "a shard needs at least one replica");
        // `BreakerConfig::validate` rejects both of these at load, and every
        // sink calls it. `BreakerConfig` is a public `Copy` struct, so
        // `SinkParts` and `spate-test` can build one that never went through a
        // loader; these clamps cover that path.
        //
        // `half_open_probes: 0` wedges the replica shut. The open→half-open
        // promotion admits its first probe unconditionally, and once that slot
        // is handed back the replica sits in `HalfOpen { 0 }`, which no budget
        // of `0` can re-admit and no deadline covers, because half-open
        // schedules nothing.
        cfg.half_open_probes = cfg.half_open_probes.max(1);
        // `on_failure` stamps `now + open_for`, and `Instant + Duration`
        // *panics* on overflow rather than saturating. The heartbeat's own
        // clamp does not cover this; it bounds how long a parked task waits,
        // not what gets written into the state.
        cfg.open_for = cfg.open_for.min(BreakerConfig::MAX_OPEN_FOR);
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
            next_episode: 0,
        }
    }

    /// A receiver for the wake signal, whose cursor is the version of the
    /// state as of this call.
    ///
    /// Take this **inside the same critical section** that found nothing
    /// pickable; that is what rules out a missed wake. Every publisher runs in
    /// a strictly later critical section, so its bump is strictly later than
    /// this cursor, and the waiter's `changed()` returns without suspending. A
    /// receiver held across picks pushes that guarantee out into wherever the
    /// caller chooses to re-arm.
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.wake_tx.subscribe()
    }

    /// Release every parked write task so it re-picks against the state this
    /// outcome just produced.
    ///
    /// A failure that leaves nothing pickable still publishes. A probe failing
    /// turns `HalfOpen` into `Open { until }`, a *deadline* where a moment ago
    /// there was none, and a waiter that computed its wake from the old state
    /// has to recompute it.
    ///
    /// Ordering rests on the mutex. A waiter takes its receiver from
    /// [`subscribe`](Self::subscribe) inside the critical section that read
    /// the states, and this bump happens after the mutation inside another
    /// one. The two serialize, so a waiter holding a cursor from the
    /// pre-outcome state is behind this version and its `changed()` returns
    /// without suspending.
    fn publish_wake(&self) {
        self.wake_tx.send_modify(|g| *g = g.wrapping_add(1));
    }

    /// Pick the next usable replica, rotating round-robin and skipping
    /// unavailable ones (an open breaker past its deadline transitions to
    /// half-open and becomes usable as a probe).
    ///
    /// `None` when nothing is pickable, which covers **two** states: every
    /// replica open and still inside its deadline, or every replica half-open
    /// with its probe budget spent. Only the first has an instant to wait for;
    /// see [`next_probe_at`](Self::next_probe_at) and the module header.
    pub(crate) fn next_replica(&mut self, now: Instant) -> Option<Pick> {
        let n = self.states.len();
        for step in 0..n {
            let idx = (self.cursor + step) % n;
            match self.states[idx] {
                State::Closed { .. } => {
                    self.cursor = (idx + 1) % n;
                    return Some(Pick {
                        replica: idx,
                        probe: None,
                    });
                }
                State::Open { until } if now >= until => {
                    // A fresh half-open run, so a probe slot still outstanding
                    // from the previous one cannot be handed back into it.
                    self.next_episode += 1;
                    let episode = self.next_episode;
                    self.states[idx] = State::HalfOpen {
                        probes_in_flight: 1,
                        episode,
                    };
                    self.cursor = (idx + 1) % n;
                    return Some(Pick {
                        replica: idx,
                        probe: Some(episode),
                    });
                }
                State::HalfOpen {
                    probes_in_flight,
                    episode,
                } if probes_in_flight < self.cfg.half_open_probes => {
                    self.states[idx] = State::HalfOpen {
                        probes_in_flight: probes_in_flight + 1,
                        episode,
                    };
                    self.cursor = (idx + 1) % n;
                    return Some(Pick {
                        replica: idx,
                        probe: Some(episode),
                    });
                }
                _ => {}
            }
        }
        None
    }

    /// Hand back a half-open probe slot whose attempt never reported an
    /// outcome, and wake any task parked waiting for one.
    ///
    /// `probes_in_flight` is otherwise cleared only by *leaving* `HalfOpen`,
    /// which is what [`on_success`](Self::on_success) and
    /// [`on_failure`](Self::on_failure) do. A write task that dies without
    /// reporting (a writer panic, whose `JoinError` carries no replica) would
    /// therefore consume a slot for good. With the default
    /// `half_open_probes: 1` that pins the replica in `HalfOpen` forever;
    /// `next_replica` never offers it again and the shard is unwritable for
    /// the life of the process. No timer recovers that, because re-picking
    /// finds the same exhausted budget.
    ///
    /// `HalfOpen { probes_in_flight: 0 }` is the resting state after a
    /// release. The probe never happened, so the next caller may take it.
    ///
    /// `episode` is what makes this safe to call late. A replica can leave
    /// half-open and enter it again while a slot from the earlier run is still
    /// unaccounted for, and "is this replica half-open *now*" cannot tell the
    /// two runs apart. Crediting the later one would let it run
    /// `half_open_probes + 1` concurrent probes against the endpoint the
    /// breaker is shielding. A release naming a run that has ended is dropped
    /// and wakes no waiter, because it changed nothing.
    ///
    /// This bounds accounting, not overlap. Two runs can still have probes in
    /// flight at once, because ending a run does not abort the writes it
    /// started. See the note on [`ProbeGuard`](super::worker).
    pub(crate) fn release_probe(&mut self, replica: usize, episode: u64) {
        let State::HalfOpen {
            probes_in_flight,
            episode: current,
        } = self.states[replica]
        else {
            return;
        };
        if current != episode {
            return;
        }
        self.states[replica] = State::HalfOpen {
            probes_in_flight: probes_in_flight.saturating_sub(1),
            episode: current,
        };
        self.publish_wake();
    }

    /// Earliest instant at which an open breaker becomes half-open.
    ///
    /// Only `Open` folds in, because it is the only state with a *scheduled*
    /// transition. A set that is entirely `HalfOpen` with its probe budget
    /// spent returns `None` from here *and* from
    /// [`next_replica`](Self::next_replica). Nothing is pickable and no clock
    /// will change that; the in-flight probe reporting will, via
    /// [`subscribe`](Self::subscribe). On its own this is not "how long a
    /// shard should wait". The worker takes the min of it and a bounded
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
        // The counter is edge-triggered; it counts transitions, not state.
        if newly_open {
            self.metrics.breaker_opened(replica);
        }
        self.publish_wake();
        self.refresh_shard_health()
    }

    /// Republish one replica's health gauge from its current state.
    ///
    /// Level-driven rather than edge-triggered. Writing only on a transition
    /// leaves a reading that was wrong when it was written (clobbered by
    /// another handle set, or lost across a restart of whatever scraped it)
    /// standing until the *next* transition, which for a quarantined replica
    /// may never come. Every write outcome refreshes it instead, so a stale
    /// reading self-corrects within one probe cycle. `HalfOpen` reads `0`; a
    /// replica being probed is not yet usable, and shard health uses the same
    /// rule.
    fn publish_replica_health(&self, replica: usize) {
        let healthy = matches!(self.states[replica], State::Closed { .. });
        self.metrics.set_replica_healthy(replica, healthy);
    }

    /// Recompute shard health (≥1 replica circuit-closed), republish the
    /// gauge, and report a transition for the caller to log. The log fires
    /// only on an edge, the gauge every time. `next_replica`'s Open→HalfOpen
    /// promotion neither adds nor removes a `Closed` state, so only the
    /// failure/success paths can move this signal.
    ///
    /// The gauge write is unconditional, so a reading that no longer matches
    /// the breakers cannot persist. An edge-triggered writer that has already
    /// published `0` never publishes `0` again, so a shard whose every replica
    /// is quarantined would keep serving whatever value it was left at.
    /// Rewriting it on each outcome bounds that to one probe cycle.
    /// `Gauge::set` is an atomic store; this runs per write attempt, never per
    /// record.
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

    /// Just the replica index, for the assertions that do not care whether the
    /// pick consumed a probe slot. Assertions on `Pick::probe` call
    /// [`next_replica`](Self::next_replica) instead.
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
/// after the breaker lock is released. Tracing subscribers must never run
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
    /// concurrently under `cargo test`, and while most read only the cached
    /// `shard_healthy()` bool, which shadowing does not affect, the
    /// level-drive test below reads the rendered gauge, so it must own the
    /// series it inspects.
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

    /// Once every replica is half-open with its probe budget spent, *both*
    /// selectors return `None` and no clock will change that. There is no
    /// deadline to sleep until, only an outcome to wait for, so the wait on a
    /// fully-quarantined shard is event-driven.
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
    /// budgeted slot it must hand back, and which half-open run it came from.
    /// A pick off a closed breaker holds nothing.
    #[tokio::test(start_paused = true)]
    async fn only_a_half_open_pick_reports_itself_as_a_probe() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        assert_eq!(
            b.next_replica(t0),
            Some(Pick {
                replica: 0,
                probe: None
            }),
            "a closed breaker consumes no budget"
        );

        b.on_failure(0, t0);
        let t1 = t0 + Duration::from_secs(6);
        let pick = b.next_replica(t1).expect("promoted to half-open");
        assert_eq!(pick.replica, 0);
        assert!(
            pick.probe.is_some(),
            "the open→half-open promotion spends the first probe"
        );
    }

    /// Without this, a write task that dies without reporting an outcome (a
    /// writer panic) consumes the probe slot for good, and with the default
    /// budget of one that pins the replica in half-open forever.
    #[tokio::test(start_paused = true)]
    async fn a_released_probe_slot_becomes_pickable_again() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        b.on_failure(0, t0);
        let t1 = t0 + Duration::from_secs(6);
        let episode = b.next_replica(t1).expect("probe").probe.expect("budgeted");
        assert_eq!(b.next_replica_idx(t1), None, "budget spent");

        b.release_probe(0, episode);
        assert_eq!(
            b.next_replica_idx(t1),
            Some(0),
            "the probe never happened, so the next caller may take it"
        );
    }

    /// A slot handed back after its half-open run has *ended* must not be
    /// credited to whatever run is current.
    ///
    /// "Is this replica half-open now" cannot tell the two apart. A replica
    /// can go half-open, re-open, and go half-open again while the first run's
    /// slot is still unaccounted for. Crediting the later run lets it write
    /// `half_open_probes + 1` concurrent probes at the endpoint the breaker is
    /// quarantining.
    #[tokio::test(start_paused = true)]
    async fn a_slot_released_from_an_ended_run_does_not_credit_the_current_one() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        b.on_failure(0, t0);

        // Run one: X takes the only probe, then a straggler failure re-opens
        // the replica while X is still in flight.
        let t1 = t0 + Duration::from_secs(6);
        let stale = b.next_replica(t1).expect("probe").probe.expect("budgeted");
        b.on_failure(0, t1);
        assert!(b.is_open(0), "re-opened under X");

        // Run two: C takes its only probe, and the budget is spent.
        let t2 = t1 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t2), Some(0));
        assert_eq!(b.next_replica_idx(t2), None, "run two's budget is spent");

        // X finally dies and hands back a slot belonging to run one.
        b.release_probe(0, stale);
        assert_eq!(
            b.next_replica_idx(t2),
            None,
            "a slot from an ended run must not become a second concurrent \
             probe in the current one"
        );
    }

    /// The same rule for a replica that has left half-open altogether. The
    /// reported outcome already cleared the budget wholesale, so a late
    /// release must not resurrect a slot.
    #[tokio::test(start_paused = true)]
    async fn releasing_a_replica_that_moved_on_does_not_grant_extra_probes() {
        let mut b = set(1, 1);
        let t0 = Instant::now();
        b.on_failure(0, t0);
        let t1 = t0 + Duration::from_secs(6);
        let episode = b.next_replica(t1).expect("probe").probe.expect("budgeted");

        // The probe succeeded, so the replica is closed.
        b.on_success(0);
        b.release_probe(0, episode);
        assert!(
            matches!(b.states[0], State::Closed { .. }),
            "a release must not drag a closed replica back into half-open"
        );

        // Re-open it and confirm the budget is the configured one, not one
        // inflated by the stale release.
        b.on_failure(0, t1);
        let t2 = t1 + Duration::from_secs(6);
        assert_eq!(b.next_replica_idx(t2), Some(0));
        assert_eq!(b.next_replica_idx(t2), None, "still exactly one probe");
    }

    /// `half_open_probes: 0` is expressible (`BreakerConfig` has no validator)
    /// and taken literally it means "never recover". The promotion admits its
    /// first probe regardless, and once that slot is handed back the replica
    /// rests in half-open with a budget nothing can satisfy and no deadline to
    /// wait on, so the shard is unwritable for good.
    #[tokio::test(start_paused = true)]
    async fn a_zero_probe_budget_cannot_wedge_a_replica_shut() {
        let labels = ComponentLabels::new("p", breaker_component(), "test");
        let mut b = BreakerSet::new(
            1,
            BreakerConfig {
                failure_threshold: 1,
                open_for: Duration::from_secs(5),
                half_open_probes: 0,
            },
            Arc::new(SinkShardMetrics::new(
                &labels,
                0,
                &["r0".to_string()],
                crate::metrics::E2eBasis::Ingest,
            )),
        );
        let t0 = Instant::now();
        b.on_failure(0, t0);
        let t1 = t0 + Duration::from_secs(6);
        let episode = b.next_replica(t1).expect("probe").probe.expect("budgeted");

        b.release_probe(0, episode);
        assert_eq!(
            b.next_replica_idx(t1),
            Some(0),
            "a released slot must be re-takeable, or the replica is shut for \
             the life of the process"
        );
    }

    /// `open_for` is unvalidated, and `Instant + Duration` panics on overflow
    /// rather than saturating, so an absurd value takes the shard worker down
    /// at the first failure.
    #[tokio::test(start_paused = true)]
    async fn an_absurd_open_for_does_not_panic_the_state_machine() {
        let labels = ComponentLabels::new("p", breaker_component(), "test");
        let mut b = BreakerSet::new(
            1,
            BreakerConfig {
                failure_threshold: 1,
                open_for: Duration::MAX,
                half_open_probes: 1,
            },
            Arc::new(SinkShardMetrics::new(
                &labels,
                0,
                &["r0".to_string()],
                crate::metrics::E2eBasis::Ingest,
            )),
        );
        let t0 = Instant::now();
        b.on_failure(0, t0);
        assert!(b.is_open(0));
        assert!(
            b.next_probe_at(t0).is_some(),
            "an open breaker still names a deadline, however distant"
        );
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

        // A half-open probe does not restore shard health by itself; the
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
        // Probe, fail, probe, fail: the shard must stay unhealthy throughout,
        // with no spurious "recovered" transition on each half-open cycle.
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

    /// The health gauges are level-driven. Every write outcome republishes
    /// them, not only the ones that flip the cached state. An edge-triggered
    /// writer that has already published `0` never publishes `0` again, so a
    /// reading knocked off the truth (by another handle set, or lost across a
    /// restart of whatever scraped it) would stand until the next transition,
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
            // gauges now read a healthy shard that is fully down.
            metrics.set_shard_healthy(true);
            metrics.set_replica_healthy(0, true);
            assert_eq!(gauge(names::SINK_SHARD_HEALTHY), Some(1.0), "clobbered");

            // One more failed outcome (not a transition, the shard was already
            // down) must put the truth back.
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
