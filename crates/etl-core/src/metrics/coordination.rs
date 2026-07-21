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
    /// Fast reclaim of a split this worker (by stable id) still held.
    Reclaimed,
    /// Takeover of a split whose lease expired unrenewed.
    Expired,
    /// Claim of a split whose previous owner released it cleanly — a
    /// drained revocation, or a shutdown/scale-down hand-back. Either way
    /// the owner cleared the record before letting go, so the resume point
    /// covers everything it emitted and the claim is replay-free; a drained
    /// revocation additionally counts [`RevocationOutcome::Drained`] on the
    /// releasing side. Contrast [`Expired`](AcquireReason::Expired), where
    /// a dead owner's uncommitted tail replays.
    ///
    /// The two clean cases are one reason on purpose: a claiming worker
    /// cannot tell them apart (both present as a cleared owner and a
    /// vanished lease), and a label it cannot populate correctly is a
    /// series that reads zero forever.
    Reassigned,
}

/// Outcome of one split revocation (the `outcome` label on
/// `etl_coordination_revocations_total`) — the leader moving a split away
/// from a live owner by dropping it from that owner's assignment.
///
/// All three count on the **releasing** worker, so they read as one
/// lifecycle rather than as two sides of a negotiation: `Requested` is the
/// denominator, and every revocation that leaves it terminates in exactly
/// one of `Drained` or `Forced` — including the paths that do not look
/// like a revocation ending at all, where the split completes or is
/// `fail`ed mid-drain, or the process departs while draining.
/// `requested - drained - forced` is therefore the drains still in flight,
/// which `etl_coordination_splits_draining` reports directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RevocationOutcome {
    /// The leader stopped naming this split in the worker's assignment, so
    /// the cooperative drain began: stop intake at a safe boundary, chase
    /// the tail to a final fenced commit, release.
    Requested,
    /// The drain finished cooperatively: the tail committed and the release
    /// landed, so the next owner resumes past everything this worker
    /// emitted and replays nothing. The outcome the cooperative path exists
    /// to produce; the gaining side counts
    /// [`AcquireReason::Reassigned`]. A split that *completes* mid-drain
    /// counts here too — its tail is committed and nothing replays, which
    /// is the same outcome even though nobody took it over.
    Drained,
    /// The cooperative path did not finish, so the release was forced: the
    /// source declined to stop at a safe boundary, the drain outran
    /// `drain_deadline`, or the split was fenced away before the release
    /// landed. The uncommitted tail replays under the next owner. A decline
    /// and an elapsed deadline are one outcome on purpose — the leader's
    /// revocation is a decision, not a request, so both end the same way
    /// and differ only in how long the fleet waited to find out.
    Forced,
}

/// Why a split lease was lost involuntarily (the `reason` label on
/// `etl_coordination_split_losses_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SplitLossReason {
    /// A write was rejected — a peer holds a higher lease epoch.
    Fenced,
    /// Self-fenced: no successful lease write for a full lease duration.
    Starved,
    /// The leader stopped assigning the split and the cooperative drain
    /// never completed — the source declined it, or it outran
    /// `drain_deadline` — so this worker forced its own release. The
    /// split's uncommitted tail replays under its next owner: the
    /// bounded-replay outcome the cooperative path exists to avoid, and
    /// therefore a signal that `drain_deadline` is too tight for this
    /// source's commit interval, or that a lane is wedged.
    ///
    /// Narrower than [`RevocationOutcome::Forced`], which also covers a
    /// revocation whose split was fenced away mid-drain; that one is lost
    /// as [`Fenced`](SplitLossReason::Fenced), because a peer — not this
    /// worker — ended the tenancy.
    Revoked,
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
    splits_draining: Gauge,
    acquired_create: Counter,
    acquired_reclaimed: Counter,
    acquired_expired: Counter,
    acquired_reassigned: Counter,
    lost_fenced: Counter,
    lost_starved: Counter,
    lost_revoked: Counter,
    releases: Counter,
    revocations_requested: Counter,
    revocations_drained: Counter,
    revocations_forced: Counter,
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
    drain_duration: Histogram,
    assignment_latency: Histogram,
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
                names::COORDINATION_SPLIT_LOSSES_TOTAL,
                names::L_REASON,
                reason,
            )
        };
        let replans =
            |outcome| labels.counter1(names::COORDINATION_REPLANS_TOTAL, names::L_OUTCOME, outcome);
        let revocations = |outcome| {
            labels.counter1(
                names::COORDINATION_REVOCATIONS_TOTAL,
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
        CoordinationMetrics {
            splits_owned: labels.gauge(names::COORDINATION_SPLITS_OWNED),
            splits_completed: labels.gauge(names::COORDINATION_SPLITS_COMPLETED),
            splits_quarantined: labels.gauge(names::COORDINATION_SPLITS_QUARANTINED),
            live_workers: labels.gauge(names::COORDINATION_LIVE_WORKERS),
            leader: labels.gauge(names::COORDINATION_LEADER),
            idle: labels.gauge(names::COORDINATION_IDLE),
            splits_draining: labels.gauge(names::COORDINATION_SPLITS_DRAINING),
            acquired_create: acquired("create"),
            acquired_reclaimed: acquired("reclaimed"),
            acquired_expired: acquired("expired"),
            acquired_reassigned: acquired("reassigned"),
            lost_fenced: lost("fenced"),
            lost_starved: lost("starved"),
            lost_revoked: lost("revoked"),
            releases: labels.counter(names::COORDINATION_RELEASES_TOTAL),
            revocations_requested: revocations("requested"),
            revocations_drained: revocations("drained"),
            revocations_forced: revocations("forced"),
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
            drain_duration: labels.histogram(names::COORDINATION_DRAIN_DURATION_SECONDS),
            assignment_latency: labels.histogram(names::COORDINATION_ASSIGNMENT_LATENCY_SECONDS),
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
            AcquireReason::Reclaimed => self.acquired_reclaimed.increment(1),
            AcquireReason::Expired => self.acquired_expired.increment(1),
            AcquireReason::Reassigned => self.acquired_reassigned.increment(1),
        }
    }

    /// Record one revocation event.
    pub fn revocation(&self, outcome: RevocationOutcome) {
        match outcome {
            RevocationOutcome::Requested => self.revocations_requested.increment(1),
            RevocationOutcome::Drained => self.revocations_drained.increment(1),
            RevocationOutcome::Forced => self.revocations_forced.increment(1),
        }
    }

    // The two timings below were one `_duration_seconds` family split by a
    // `phase` label back when a move was a negotiation: the requester's
    // wait strictly *contained* the victim's drain, so the two were nested
    // terms of a single time-to-balance figure and belonged on one family.
    //
    // Leader-assigned reconciliation destroyed that relationship. Nothing
    // now spans both: a drain starts when the leader removes a split from
    // one worker's assignment, an assignment wait starts when the leader
    // adds a split to another's, and neither worker observes the other's
    // clock. They also have different denominators — every assigned split
    // is waited for, including brand-new splits and dead owners' work that
    // no revocation ever touched, so the assignment wait is not a
    // revocation measurement at all.
    //
    // Two families, therefore, not one with a label. A shared family would
    // assert a composition that no longer exists, and it would leave
    // `histogram_quantile` over `sum by (le)` — aggregating away `phase` —
    // spelled exactly like a reasonable query while silently mixing two
    // populations. Separate names make the meaningless aggregate
    // unwritable rather than merely discouraged.

    /// Record one cooperative drain: revocation requested to the release
    /// landing, on the **releasing** worker.
    ///
    /// Observed only when the drain completes cooperatively
    /// ([`RevocationOutcome::Drained`]) — a forced release is a failure of
    /// the drain, and timing it would mix `drain_deadline` into the
    /// distribution of how long draining actually takes.
    pub fn drain_duration(&self, d: Duration) {
        self.drain_duration.record(d.as_secs_f64());
    }

    /// Record one assignment wait: a split appearing in this worker's
    /// assignment to this worker holding its lease, on the **gaining**
    /// worker.
    ///
    /// This is the fleet's time-to-balance as an operator experiences it —
    /// how long work the leader has already decided this worker should be
    /// doing sat undone. It spans whatever stood in the way, including the
    /// previous owner's drain, so it never flatters itself by timing only
    /// the final claim.
    pub fn assignment_latency(&self, d: Duration) {
        self.assignment_latency.record(d.as_secs_f64());
    }

    /// Set the number of splits this worker is currently draining away
    /// under revocation.
    pub fn set_splits_draining(&self, draining: usize) {
        self.splits_draining.set(draining as f64);
    }

    /// Record one involuntary split loss.
    pub fn lost(&self, reason: SplitLossReason) {
        match reason {
            SplitLossReason::Fenced => self.lost_fenced.increment(1),
            SplitLossReason::Starved => self.lost_starved.increment(1),
            SplitLossReason::Revoked => self.lost_revoked.increment(1),
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
