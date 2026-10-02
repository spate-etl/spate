//! Revoking a split by asking its source to give it up, and settling the
//! outcome.

use super::{ReleaseOutcome, Revoking, Task};
use crate::store::CoordinationStore;
use spate_core::coordination::{CoordinationError, CoordinationEvent, SplitId};
use spate_core::metrics::{RevocationOutcome, SplitLossReason};

impl<S: CoordinationStore + Clone> Task<S> {
    /// Ask the source to give a split up gracefully. Idempotent: the
    /// driver treats a repeated request for a split it is already draining
    /// as a no-op, so re-emitting on a later step is harmless.
    pub(super) fn begin_revoke(&mut self, id: &str) {
        let Ok(split) = SplitId::new(id.to_string()) else {
            return;
        };
        let now = self.clock.now();
        match self.revoking.get_mut(id) {
            // Re-revoking a drain this worker had cancelled. The drain
            // never stopped, so `started` still anchors the deadline and
            // one already past it is forced on the spot.
            Some(entry) => entry.cancelled = false,
            None => {
                self.revoking.insert(
                    id.to_string(),
                    Revoking {
                        started: now,
                        last_progress: now,
                        cancelled: false,
                    },
                );
                // Only a drain that is starting says so; the arm above
                // would put two starts against one finish.
                tracing::debug!(split = %id, "drain started");
            }
        }
        // The denominator of the revocation lifecycle, counted once per
        // revocation: `requested - drained - forced - cancelled` is the
        // revocations in flight, which is *not* `splits_draining`.
        self.metrics(|m| m.revocation(RevocationOutcome::Requested));
        self.emit(CoordinationEvent::RevokeRequested { split });
    }

    /// Enforce the drain deadline, against two different clocks.
    ///
    /// A cooperative revocation that completes releases through
    /// [`Task::release_splits`] and clears itself from `revoking`. What is
    /// left is bounded one of two ways, because the deadline is protecting
    /// two different things:
    ///
    /// - **A live revocation** holds a rebalance open, so it gets one
    ///   absolute `drain_deadline` from the request. Past it the drain
    ///   declined or is too slow to wait for, and it is forced.
    /// - **A cancelled revocation** ([`Task::reconcile_assignment`] takes
    ///   the decision back, and runs first in every step) has no rebalance
    ///   waiting on it, so slowness costs nothing and is not forced. What
    ///   it still owes is *liveness*: the source stopped intake at a safe
    ///   boundary and cannot be asked to resume, so a drain that never
    ///   finishes leaves the split owned, leased, assigned, and unread for
    ///   the life of the process, and a bounded job that contains it can
    ///   never complete. It is therefore bounded by
    ///   silence: `drain_deadline` with no commit landing at all, which for
    ///   a live drain cannot happen (its tail acks as it flushes) and for a
    ///   wedged one always does.
    ///
    /// Forcing is a *release*, not an abandonment: the owner field is
    /// cleared so the next claimant sees `Released` rather than `Expired`
    /// and spends no delivery attempt. Being revoked is not poison
    /// evidence. A stalled cancelled drain is
    /// re-claimed by this same worker (the leader still names it here), so
    /// forcing it costs one lane teardown and a bounded replay, and gets a
    /// reading split back.
    pub(super) async fn service_revocations(&mut self) -> Result<(), CoordinationError> {
        // An entry whose split left `owned` by a route that did not
        // settle it (a terminal commit, or an explicit `fail`) is still a
        // revocation that ended. Count it, or the in-flight count drifts.
        let orphans: Vec<String> = self
            .revoking
            .keys()
            .filter(|id| !self.owned.contains_key(*id))
            .cloned()
            .collect();
        for id in orphans {
            self.settle_revocation(&id, RevocationOutcome::Forced);
        }
        let deadline = self.config.drain_deadline;
        let now = self.clock.now();
        let overdue: Vec<(String, bool)> = self
            .revoking
            .iter()
            .filter(|(_, r)| {
                let anchor = if r.cancelled {
                    r.last_progress
                } else {
                    r.started
                };
                now.duration_since(anchor) >= deadline
            })
            .map(|(id, r)| (id.clone(), r.cancelled))
            .collect();
        for (id, cancelled) in overdue {
            if cancelled {
                tracing::warn!(
                    split = %id,
                    ?deadline,
                    "a cancelled revocation's drain has committed nothing for a full drain \
                     deadline; releasing the split so it can be re-claimed and read again \
                     (its uncommitted tail replays)"
                );
            } else {
                tracing::warn!(
                    split = %id,
                    ?deadline,
                    "drain deadline exceeded; forcing the revocation (its uncommitted tail replays)"
                );
            }
            self.force_revocation(&id).await?;
        }
        Ok(())
    }

    /// Retire one `revoking` entry under a terminal outcome, exactly once.
    /// A no-op for a split that is not being revoked, which is what lets
    /// every path that can end a tenancy call it unconditionally.
    ///
    /// A **cancelled** entry retires silently. Its revocation already
    /// terminated under [`RevocationOutcome::Cancelled`] and the entry
    /// outlived it only as a watchdog over the drain; counting a second
    /// outcome here would break `requested = drained + forced + cancelled`.
    /// The drain that finishes after a cancellation is therefore invisible
    /// to both the counter and the duration histogram. By then it is a
    /// split going nowhere, not a revocation ending.
    pub(super) fn settle_revocation(&mut self, id: &str, outcome: RevocationOutcome) {
        let Some(entry) = self.revoking.remove(id) else {
            return;
        };
        if entry.cancelled {
            return;
        }
        let drained_for = self.clock.now().duration_since(entry.started);
        self.metrics(|m| {
            m.revocation(outcome);
            // A forced release measures `drain_deadline`, not draining.
            if outcome == RevocationOutcome::Drained {
                m.drain_duration(drained_for);
            }
        });
        tracing::debug!(
            split = %id,
            ?outcome,
            drained_for_ms = drained_for.as_millis(),
            "drain finished"
        );
    }

    /// End a revocation the expensive way: give the split back without
    /// waiting for the drain, so its uncommitted tail replays under the
    /// next owner.
    ///
    /// Also the exit from a cancelled revocation's stalled drain, where
    /// "the next owner" is usually this worker again. The split is still
    /// assigned here, so it is re-claimed with a fresh lane and starts
    /// reading. That path counts no `Forced` (the revocation ended as
    /// `Cancelled`) and reports the release and its `Lost`.
    ///
    /// The release runs before the fence. The release CAS needs the lease
    /// revision, which lives in `owned`, and a fence would take it away.
    /// The loss is reported exactly once: if the release returned `Fenced`,
    /// `drop_owned` already counted `fenced` and emitted `Lost`, and adding
    /// a `revoked` on top would count one tenancy end twice under two
    /// different reasons.
    pub(super) async fn force_revocation(&mut self, id: &str) -> Result<(), CoordinationError> {
        self.settle_revocation(id, RevocationOutcome::Forced);
        let Ok(split) = SplitId::new(id.to_string()) else {
            return Ok(());
        };
        if self.release_one(&split).await? == ReleaseOutcome::Fenced {
            return Ok(()); // `drop_owned` already reported it
        }
        self.metrics(|m| m.lost(SplitLossReason::Revoked));
        self.emit(CoordinationEvent::Lost { split });
        Ok(())
    }
}
