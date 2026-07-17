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
    acquired_create: Counter,
    acquired_released: Counter,
    acquired_reclaimed: Counter,
    acquired_expired: Counter,
    acquired_stolen: Counter,
    lost_fenced: Counter,
    lost_starved: Counter,
    releases: Counter,
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
        let writes =
            |outcome| labels.counter1(names::COORDINATION_WRITES_TOTAL, names::L_OUTCOME, outcome);
        let store_op = |op| {
            labels.histogram1(
                names::COORDINATION_STORE_OP_DURATION_SECONDS,
                names::L_OP,
                op,
            )
        };
        CoordinationMetrics {
            splits_owned: labels.gauge(names::COORDINATION_SPLITS_OWNED),
            splits_completed: labels.gauge(names::COORDINATION_SPLITS_COMPLETED),
            splits_quarantined: labels.gauge(names::COORDINATION_SPLITS_QUARANTINED),
            live_workers: labels.gauge(names::COORDINATION_LIVE_WORKERS),
            leader: labels.gauge(names::COORDINATION_LEADER),
            idle: labels.gauge(names::COORDINATION_IDLE),
            acquired_create: acquired("create"),
            acquired_released: acquired("released"),
            acquired_reclaimed: acquired("reclaimed"),
            acquired_expired: acquired("expired"),
            acquired_stolen: acquired("stolen"),
            lost_fenced: lost("fenced"),
            lost_starved: lost("starved"),
            releases: labels.counter(names::COORDINATION_RELEASES_TOTAL),
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
        }
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
