//! Coordination handles (`etl_coordination_*`).

use super::labels::ComponentLabels;
use super::names;
use metrics::{Counter, Gauge, Histogram};
use std::time::Duration;

/// Why a split lease was acquired (the `reason` label on
/// `etl_coordination_acquisitions_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AcquireReason {
    /// First claim of a runnable split no one has held.
    Create,
    /// Instant claim of a voluntarily released split.
    Released,
    /// Fast reclaim of a split this worker (by stable id) still held.
    Reclaimed,
    /// Takeover of a split whose lease expired unrenewed.
    Expired,
    /// Steal from an over-loaded live owner, for balance.
    Stolen,
    /// Cooperative handoff from a live owner that drained and released the
    /// split first — the resume point covers everything it emitted, so the
    /// claim is replay-free (unlike [`Stolen`](AcquireReason::Stolen)).
    Handoff,
}

/// Outcome of one cooperative split handoff (the `outcome` label on
/// `etl_coordination_handoffs_total`), the consent-first live-owner
/// transfer that replaces a replaying steal when the owner is responsive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandoffOutcome {
    /// A requester wrote a handoff request naming an over-loaded victim.
    Requested,
    /// A victim drained a split, committed its tail, and released it — a
    /// replay-free transfer (the requester claims it as
    /// [`AcquireReason::Handoff`]).
    Granted,
    /// A request went unanswered for the full round budget and fell back
    /// to a replaying steal (the dead/stuck-owner path). Counted on the
    /// requester; a late grant may still land afterwards, so one request
    /// can count both `Timeout` and (on the victim) `Granted`.
    Timeout,
    /// A victim's granted drain ended without the full-commit release: it
    /// was fenced mid-drain (a fallback steal or expiry took the split)
    /// or the release write failed. Counted on the victim. A request the
    /// *requester* abandons (withdrawn as unjustified, superseded, or
    /// claimable work reappearing) ends with no terminal outcome at all —
    /// the outcomes are per-side events, not a partition of requests.
    Aborted,
}

/// Which term of time-to-balance a handoff duration measures (the `phase`
/// label on `etl_coordination_handoff_duration_seconds`).
///
/// They are observed on opposite sides of a move and **must not be added**:
/// `Request` starts before the victim has even been asked and stops when
/// the split is claimed, so it strictly *contains* the `Drain` it was
/// waiting on. Time-to-balance for one move is the `Request` alone;
/// `Request - Drain` is roughly what admission and the claim cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandoffPhase {
    /// Requester-side: from going under target with a victim worth asking,
    /// to claiming a granted split. Deliberately spans withdrawn,
    /// superseded and re-targeted requests rather than restarting per
    /// request — it answers "how long was this worker short of its share?",
    /// so request-admission pacing cannot hide in the gaps between attempts.
    Request,
    /// Victim-side: from annotating the grant to the release landing —
    /// stopping intake at a safe boundary, committing the drained tail, and
    /// giving the split up. This is the term concurrent grants overlap.
    Drain,
}

/// Why a split lease was lost involuntarily (the `reason` label on
/// `etl_coordination_revocations_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SplitLossReason {
    /// A write was rejected — a peer holds a higher lease epoch.
    Fenced,
    /// Self-fenced: no successful lease write for a full lease duration.
    Starved,
}

/// Outcome of one split-record write (the `outcome` label on
/// `etl_coordination_writes_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteOutcome {
    /// Durable.
    Ok,
    /// Lost a compare-and-swap race (fencing or claim contention).
    Conflict,
    /// Failed for any other reason (timeout, transport, service error).
    Error,
}

/// Outcome of one planner run while leader (the `outcome` label on
/// `etl_coordination_replans_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplanOutcome {
    /// The plan advanced: new splits were written or finality changed.
    Ok,
    /// The planner or the plan write failed.
    Error,
    /// The enumeration produced nothing new.
    Noop,
}

/// Store primitive (the `op` label on
/// `etl_coordination_store_op_duration_seconds`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StoreOp {
    /// Point read.
    Get,
    /// Create or CAS update.
    Put,
    /// Delete (graceful release).
    Delete,
    /// Reconcile listing.
    List,
    /// Watch (re)establishment.
    Watch,
}

/// Coordination handles (`etl_coordination_*`), pre-registered at build
/// time and handed to the coordination backend at construction.
/// Cloning is cheap — the fields are shared recorder handles.
#[derive(Clone, Debug)]
pub struct CoordinationMetrics {
    splits_owned: Gauge,
    splits_completed: Gauge,
    splits_quarantined: Gauge,
    live_workers: Gauge,
    leader: Gauge,
    idle: Gauge,
    handoffs_in_flight: Gauge,
    acquired_create: Counter,
    acquired_released: Counter,
    acquired_reclaimed: Counter,
    acquired_expired: Counter,
    acquired_stolen: Counter,
    acquired_handoff: Counter,
    lost_fenced: Counter,
    lost_starved: Counter,
    releases: Counter,
    handoffs_requested: Counter,
    handoffs_granted: Counter,
    handoffs_timeout: Counter,
    handoffs_aborted: Counter,
    splits_planned: Counter,
    replans_ok: Counter,
    replans_error: Counter,
    replans_noop: Counter,
    split_failures: Counter,
    quarantines: Counter,
    writes_ok: Counter,
    writes_conflict: Counter,
    writes_error: Counter,
    write_duration: Histogram,
    replan_duration: Histogram,
    reconcile_duration: Histogram,
    store_op_get: Histogram,
    store_op_put: Histogram,
    store_op_delete: Histogram,
    store_op_list: Histogram,
    store_op_watch: Histogram,
    handoff_request_duration: Histogram,
    handoff_drain_duration: Histogram,
}

impl CoordinationMetrics {
    /// Resolve all coordination handles.
    pub fn new(labels: &ComponentLabels) -> Self {
        let acquired = |reason| {
            labels.counter1(
                names::COORDINATION_ACQUISITIONS_TOTAL,
                names::L_REASON,
                reason,
            )
        };
        let lost = |reason| {
            labels.counter1(
                names::COORDINATION_REVOCATIONS_TOTAL,
                names::L_REASON,
                reason,
            )
        };
        let replans =
            |outcome| labels.counter1(names::COORDINATION_REPLANS_TOTAL, names::L_OUTCOME, outcome);
        let handoffs = |outcome| {
            labels.counter1(
                names::COORDINATION_HANDOFFS_TOTAL,
                names::L_OUTCOME,
                outcome,
            )
        };
        let writes =
            |outcome| labels.counter1(names::COORDINATION_WRITES_TOTAL, names::L_OUTCOME, outcome);
        let store_op = |op| {
            labels.histogram1(
                names::COORDINATION_STORE_OP_DURATION_SECONDS,
                names::L_OP,
                op,
            )
        };
        let handoff_phase = |phase| {
            labels.histogram1(
                names::COORDINATION_HANDOFF_DURATION_SECONDS,
                names::L_PHASE,
                phase,
            )
        };
        CoordinationMetrics {
            splits_owned: labels.gauge(names::COORDINATION_SPLITS_OWNED),
            splits_completed: labels.gauge(names::COORDINATION_SPLITS_COMPLETED),
            splits_quarantined: labels.gauge(names::COORDINATION_SPLITS_QUARANTINED),
            live_workers: labels.gauge(names::COORDINATION_LIVE_WORKERS),
            leader: labels.gauge(names::COORDINATION_LEADER),
            idle: labels.gauge(names::COORDINATION_IDLE),
            handoffs_in_flight: labels.gauge(names::COORDINATION_HANDOFFS_IN_FLIGHT),
            acquired_create: acquired("create"),
            acquired_released: acquired("released"),
            acquired_reclaimed: acquired("reclaimed"),
            acquired_expired: acquired("expired"),
            acquired_stolen: acquired("stolen"),
            acquired_handoff: acquired("handoff"),
            lost_fenced: lost("fenced"),
            lost_starved: lost("starved"),
            releases: labels.counter(names::COORDINATION_RELEASES_TOTAL),
            handoffs_requested: handoffs("requested"),
            handoffs_granted: handoffs("granted"),
            handoffs_timeout: handoffs("timeout"),
            handoffs_aborted: handoffs("aborted"),
            splits_planned: labels.counter(names::COORDINATION_SPLITS_PLANNED_TOTAL),
            replans_ok: replans("ok"),
            replans_error: replans("error"),
            replans_noop: replans("noop"),
            split_failures: labels.counter(names::COORDINATION_SPLIT_FAILURES_TOTAL),
            quarantines: labels.counter(names::COORDINATION_QUARANTINES_TOTAL),
            writes_ok: writes("ok"),
            writes_conflict: writes("conflict"),
            writes_error: writes("error"),
            write_duration: labels.histogram(names::COORDINATION_WRITE_DURATION_SECONDS),
            replan_duration: labels.histogram(names::COORDINATION_REPLAN_DURATION_SECONDS),
            reconcile_duration: labels.histogram(names::COORDINATION_RECONCILE_DURATION_SECONDS),
            store_op_get: store_op("get"),
            store_op_put: store_op("put"),
            store_op_delete: store_op("delete"),
            store_op_list: store_op("list"),
            store_op_watch: store_op("watch"),
            handoff_request_duration: handoff_phase("request"),
            handoff_drain_duration: handoff_phase("drain"),
        }
    }

    /// Set the number of splits this worker currently leases.
    pub fn set_splits_owned(&self, owned: usize) {
        self.splits_owned.set(owned as f64);
    }

    /// Set the observed count of completed splits across the fleet.
    pub fn set_splits_completed(&self, completed: usize) {
        self.splits_completed.set(completed as f64);
    }

    /// Set the observed count of quarantined splits across the fleet.
    pub fn set_splits_quarantined(&self, quarantined: usize) {
        self.splits_quarantined.set(quarantined as f64);
    }

    /// Set the observed count of distinct live workers, including self.
    pub fn set_live_workers(&self, workers: usize) {
        self.live_workers.set(workers as f64);
    }

    /// Flag whether this worker currently holds planner leadership.
    pub fn set_leader(&self, leader: bool) {
        self.leader.set(if leader { 1.0 } else { 0.0 });
    }

    /// Flag whether this worker is a zero-split standby observer.
    pub fn set_idle(&self, idle: bool) {
        self.idle.set(if idle { 1.0 } else { 0.0 });
    }

    /// Record one split acquisition.
    pub fn acquired(&self, reason: AcquireReason) {
        match reason {
            AcquireReason::Create => self.acquired_create.increment(1),
            AcquireReason::Released => self.acquired_released.increment(1),
            AcquireReason::Reclaimed => self.acquired_reclaimed.increment(1),
            AcquireReason::Expired => self.acquired_expired.increment(1),
            AcquireReason::Stolen => self.acquired_stolen.increment(1),
            AcquireReason::Handoff => self.acquired_handoff.increment(1),
        }
    }

    /// Record one cooperative-handoff outcome.
    pub fn handoff(&self, outcome: HandoffOutcome) {
        match outcome {
            HandoffOutcome::Requested => self.handoffs_requested.increment(1),
            HandoffOutcome::Granted => self.handoffs_granted.increment(1),
            HandoffOutcome::Timeout => self.handoffs_timeout.increment(1),
            HandoffOutcome::Aborted => self.handoffs_aborted.increment(1),
        }
    }

    /// Record one cooperative-handoff phase duration.
    pub fn handoff_duration(&self, phase: HandoffPhase, d: Duration) {
        match phase {
            HandoffPhase::Request => self.handoff_request_duration.record(d.as_secs_f64()),
            HandoffPhase::Drain => self.handoff_drain_duration.record(d.as_secs_f64()),
        }
    }

    /// Set the number of grants this worker is currently draining away.
    pub fn set_handoffs_in_flight(&self, in_flight: usize) {
        self.handoffs_in_flight.set(in_flight as f64);
    }

    /// Record one involuntary split loss.
    pub fn lost(&self, reason: SplitLossReason) {
        match reason {
            SplitLossReason::Fenced => self.lost_fenced.increment(1),
            SplitLossReason::Starved => self.lost_starved.increment(1),
        }
    }

    /// Record voluntarily released splits.
    pub fn released(&self, splits: u64) {
        self.releases.increment(splits);
    }

    /// Record splits newly written into the plan while leader.
    pub fn planned(&self, splits: u64) {
        self.splits_planned.increment(splits);
    }

    /// Record one planner run while leader and its duration.
    pub fn replan(&self, outcome: ReplanOutcome, d: Duration) {
        match outcome {
            ReplanOutcome::Ok => self.replans_ok.increment(1),
            ReplanOutcome::Error => self.replans_error.increment(1),
            ReplanOutcome::Noop => self.replans_noop.increment(1),
        }
        self.replan_duration.record(d.as_secs_f64());
    }

    /// Record one explicit split failure report.
    pub fn failed(&self) {
        self.split_failures.increment(1);
    }

    /// Record one split parked in quarantine.
    pub fn quarantined(&self) {
        self.quarantines.increment(1);
    }

    /// Record one split-record write and its round-trip time.
    pub fn write(&self, outcome: WriteOutcome, d: Duration) {
        match outcome {
            WriteOutcome::Ok => self.writes_ok.increment(1),
            WriteOutcome::Conflict => self.writes_conflict.increment(1),
            WriteOutcome::Error => self.writes_error.increment(1),
        }
        self.write_duration.record(d.as_secs_f64());
    }

    /// Record one full reconcile listing (the watch-loss backstop).
    pub fn reconcile(&self, d: Duration) {
        self.reconcile_duration.record(d.as_secs_f64());
    }

    /// Record one store primitive's round-trip time.
    pub fn store_op(&self, op: StoreOp, d: Duration) {
        let h = match op {
            StoreOp::Get => &self.store_op_get,
            StoreOp::Put => &self.store_op_put,
            StoreOp::Delete => &self.store_op_delete,
            StoreOp::List => &self.store_op_list,
            StoreOp::Watch => &self.store_op_watch,
        };
        h.record(d.as_secs_f64());
    }
}
