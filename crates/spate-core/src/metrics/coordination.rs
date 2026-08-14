//! Coordination handles (`spate_coordination_*`).

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use metrics::{Counter, Histogram};
use std::sync::Arc;
use std::time::Duration;

/// Why a split lease was acquired (the `reason` label on
/// `spate_coordination_acquisitions_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AcquireReason {
    /// First claim of a runnable split no one has held.
    Create,
    /// Fast reclaim of a split this worker (by stable id) still held.
    Reclaimed,
    /// Takeover of a split whose lease expired unrenewed.
    Expired,
    /// Claim of a split whose previous owner released it cleanly, either a
    /// drained revocation or a shutdown/scale-down hand-back. Either way
    /// the owner cleared the record before letting go, so the resume point
    /// covers everything it emitted and the claim is replay-free; a drained
    /// revocation additionally counts [`RevocationOutcome::Drained`] on the
    /// releasing side. Contrast [`Expired`](AcquireReason::Expired), where
    /// a dead owner's uncommitted tail replays.
    ///
    /// One clean release counts here with *no* matching `drained`. That is
    /// the drain left behind by a [`RevocationOutcome::Cancelled`]
    /// revocation, which hands the split back to the worker that already had
    /// it. Counting it `drained` would claim a move that never happened.
    ///
    /// The two clean cases share one reason because a claiming worker cannot
    /// tell them apart; both present as a cleared owner and a vanished lease.
    /// A label it cannot populate correctly is a series that reads zero
    /// forever.
    Reassigned,
}

/// Outcome of one split revocation (the `outcome` label on
/// `spate_coordination_revocations_total`). A revocation is the leader moving
/// a split away from a live owner by dropping it from that owner's assignment.
///
/// All four count on the **releasing** worker, so they read as one lifecycle.
/// `Requested` is the denominator, and every revocation that leaves it
/// terminates in exactly one of `Drained`, `Forced`, or `Cancelled`. That
/// includes the paths that do not look like a revocation ending at all, where
/// the split completes or is `fail`ed mid-drain, or the process departs while
/// draining. `requested - drained - forced - cancelled` is therefore the
/// **revocations** still in flight.
///
/// That is not the same number as `spate_coordination_splits_draining`, which
/// counts **drains**. `Cancelled` ends a revocation while leaving its drain
/// running, so the gauge sits one higher than the counter arithmetic for as
/// long as that drain takes. The counter answers how much the leader is still
/// trying to move; the gauge answers how many splits are winding down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RevocationOutcome {
    /// The leader stopped naming this split in the worker's assignment, so
    /// the cooperative drain began: stop intake at a safe boundary, chase
    /// the tail to a final fenced commit, release.
    Requested,
    /// The drain finished cooperatively. The tail committed and the release
    /// landed, so the next owner resumes past everything this worker
    /// emitted and replays nothing. The gaining side counts
    /// [`AcquireReason::Reassigned`]. A split that *completes* mid-drain
    /// counts here too; its tail is committed and nothing replays, even
    /// though no worker took it over.
    Drained,
    /// The cooperative path did not finish, so the release was forced: the
    /// source declined to stop at a safe boundary, the drain outran
    /// `drain_deadline`, or the split was fenced away before the release
    /// landed. The uncommitted tail replays under the next owner. A decline
    /// and an elapsed deadline are one outcome. The leader's revocation is a
    /// decision rather than a request, so both end the same way and differ
    /// only in how long the fleet waited to find out.
    Forced,
    /// The leader took the revocation back by naming the split for this
    /// worker again while this worker still held it, so the pending forced
    /// release was dropped. Nothing is waiting for the split any more, so a
    /// drain slower than `drain_deadline` finishes cleanly instead of being
    /// charged a replay for a move no worker is waiting on. That is the whole
    /// of what cancelling buys; a drain that finishes inside the deadline
    /// would not have been forced.
    ///
    /// This counts the *revocation* ending, not the drain, and the two then
    /// diverge:
    ///
    /// - If the source had already stopped intake, the drain runs on
    ///   (resuming stopped intake is a seam sources do not have). It ends by
    ///   handing the split back, and this worker re-claims it
    ///   ([`AcquireReason::Reassigned`]) replay-free, at the cost of one lane
    ///   teardown and re-open. It counts under neither `drained` nor
    ///   `drain_duration_seconds`, because by then it is not a revocation
    ///   ending. `splits_draining` stays up until it lands.
    /// - If the source declined, or was never asked, nothing stopped and
    ///   nothing leaves; the split stays, still being read.
    ///
    /// A cancelled drain is still bounded, by silence rather than by the
    /// deadline. If it commits nothing for `drain_deadline` the split is
    /// released anyway and re-claimed with a fresh lane; a drain that never
    /// finishes would otherwise leave it owned, leased, and unread. That
    /// release counts a [`SplitLossReason::Revoked`] and no second revocation
    /// outcome.
    ///
    /// Sustained `cancelled` means the fleet's membership is flapping faster
    /// than a drain takes. Look at pod churn and at `drain_deadline`.
    Cancelled,
}

/// Why a split lease was lost involuntarily (the `reason` label on
/// `spate_coordination_split_losses_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SplitLossReason {
    /// A write was rejected because a peer holds a higher lease epoch.
    Fenced,
    /// Self-fenced: no successful lease write for a full lease duration.
    Starved,
    /// A cooperative drain that never completed, so this worker forced its
    /// own release. Either the leader had stopped assigning the split and
    /// the source declined or outran `drain_deadline`, or the leader took
    /// the revocation back ([`RevocationOutcome::Cancelled`]) and the drain
    /// it left behind then went a full `drain_deadline` without committing
    /// anything. A stalled drain releases too, or the split would stay
    /// owned with nothing reading it.
    ///
    /// The split's uncommitted tail replays under its next owner (for the
    /// cancelled case, usually this same worker). That is the bounded replay
    /// the cooperative path avoids, so it signals that `drain_deadline` is
    /// too tight for this source's commit interval, or that a lane is
    /// wedged.
    ///
    /// Narrower than [`RevocationOutcome::Forced`], which also covers a
    /// revocation whose split was fenced away mid-drain; that one is lost
    /// as [`Fenced`](SplitLossReason::Fenced), because a peer rather than
    /// this worker ended the tenancy.
    Revoked,
}

/// Outcome of one split-record write (the `outcome` label on
/// `spate_coordination_writes_total`).
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
/// `spate_coordination_replans_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplanOutcome {
    /// The plan advanced; new splits were written or finality changed.
    Ok,
    /// The planner or the plan write failed.
    Error,
    /// The enumeration produced nothing new.
    Noop,
}

/// Store primitive (the `op` label on
/// `spate_coordination_store_op_duration_seconds`).
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

/// Coordination handles (`spate_coordination_*`), pre-registered at build
/// time and handed to the coordination backend at construction.
/// Cloning is cheap; the fields are shared recorder handles.
#[derive(Clone, Debug)]
pub struct CoordinationMetrics {
    splits_owned: OwnedGauge,
    splits_completed: OwnedGauge,
    splits_quarantined: OwnedGauge,
    live_workers: OwnedGauge,
    leader: OwnedGauge,
    idle: OwnedGauge,
    splits_draining: OwnedGauge,
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
    revocations_cancelled: Counter,
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
    /// Shared so `Clone` hands out co-owners rather than duplicate claimants.
    /// The series is released when the last clone drops.
    _claim: Option<Arc<SeriesClaim>>,
}

impl CoordinationMetrics {
    /// Resolve all coordination handles.
    ///
    /// Claims the `spate_coordination_*` series for these labels; a second live
    /// handle set logs and becomes a shadow, counting but publishing no gauge.
    /// Cloning this struct shares the claim; clones are co-owners.
    pub fn new(labels: &ComponentLabels) -> Self {
        let claim = SeriesClaim::claim_or_shadow(Self::key(labels));
        Self::build(labels, claim.map(Arc::new))
    }

    /// Resolve all coordination handles, failing when another live handle set
    /// already owns the series.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub fn try_new(labels: &ComponentLabels) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(Self::key(labels))?;
        Ok(Self::build(labels, Some(Arc::new(claim))))
    }

    fn key(labels: &ComponentLabels) -> String {
        series_key("coordination", labels, "")
    }

    fn build(labels: &ComponentLabels, claim: Option<Arc<SeriesClaim>>) -> Self {
        let owned = claim.is_some();
        let gauge = |name| OwnedGauge::new(labels.gauge(name), owned);
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
            splits_owned: gauge(names::COORDINATION_SPLITS_OWNED),
            splits_completed: gauge(names::COORDINATION_SPLITS_COMPLETED),
            splits_quarantined: gauge(names::COORDINATION_SPLITS_QUARANTINED),
            live_workers: gauge(names::COORDINATION_LIVE_WORKERS),
            leader: gauge(names::COORDINATION_LEADER),
            idle: gauge(names::COORDINATION_IDLE),
            splits_draining: gauge(names::COORDINATION_SPLITS_DRAINING),
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
            revocations_cancelled: revocations("cancelled"),
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
            _claim: claim,
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
            RevocationOutcome::Cancelled => self.revocations_cancelled.increment(1),
        }
    }

    // The two timings below are separate families, not one split by a `phase`
    // label. Nothing spans both. A drain starts when the leader removes a
    // split from one worker's assignment, an assignment wait starts when the
    // leader adds a split to another's, and neither worker observes the
    // other's clock. The denominators differ too. Every assigned split is
    // waited for, including brand-new splits and dead owners' work that no
    // revocation touched, so the assignment wait is not a revocation
    // measurement.
    //
    // A shared family would leave `histogram_quantile` over `sum by (le)`,
    // aggregating away `phase`, spelled exactly like a reasonable query while
    // silently mixing two populations. Separate names make that aggregate
    // unwritable.

    /// Record one cooperative drain: revocation requested to the release
    /// landing, on the **releasing** worker.
    ///
    /// Observed only when the drain completes cooperatively
    /// ([`RevocationOutcome::Drained`]). A forced release is a failure of
    /// the drain, and timing it would mix `drain_deadline` into the
    /// distribution of how long draining takes.
    pub fn drain_duration(&self, d: Duration) {
        self.drain_duration.record(d.as_secs_f64());
    }

    /// Record one assignment wait: a split appearing in this worker's
    /// assignment to this worker holding its lease, on the **gaining**
    /// worker.
    ///
    /// This is the fleet's time-to-balance as an operator experiences it, the
    /// time work the leader has already decided this worker should be doing
    /// sat undone. It spans whatever stood in the way, including the previous
    /// owner's drain, rather than timing only the final claim.
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
