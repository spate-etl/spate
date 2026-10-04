//! The leader's assignment publishing and a worker's response to its own
//! assignment.

use super::Task;
use crate::error::fatal_only;
use crate::protocol::{self, ClaimAction};
use crate::records::{self, AssignmentVal};
use crate::store::{CasOutcome, CoordinationStore, Keyspace, Revision};
use spate_core::coordination::CoordinationError;
use spate_core::metrics::RevocationOutcome;
use std::collections::BTreeSet;
use tokio::time::Instant;

impl<S: CoordinationStore + Clone> Task<S> {
    /// Move this worker toward the assignment it was given: claim what is
    /// named and not held, drain away what is held and not named, and cancel
    /// the drain of anything the leader has named again.
    ///
    /// **Absence of an assignment is not an instruction to hold nothing.**
    /// Until a record for this instance has been observed the worker keeps
    /// what it has and claims nothing new, so a leader gap costs
    /// rebalancing but never work. Once a record exists its omissions are
    /// meaningful, and a split missing from it is one to give up.
    ///
    /// Quarantine decisions run regardless of any of that: a fleet that has
    /// been told to hold nothing must still be able to reach the `Stalled`
    /// verdict, or a bounded job with a poison split would idle instead of
    /// finishing.
    pub(super) async fn reconcile_assignment(&mut self) -> Result<(), CoordinationError> {
        let quarantine_scan = std::mem::take(&mut self.quarantine_scan);
        let cap = self.config.max_in_flight as usize;
        // Scanning every split is the expensive half of a step. At the
        // lane budget the only reason to scan is a quarantine decision.
        if self.owned.len() < cap || quarantine_scan {
            let candidates = protocol::claim_candidates(
                &self.splits,
                |id| self.owned.contains_key(id),
                &self.instance,
                self.config.max_attempts,
            );
            for (id, action) in candidates {
                match action {
                    ClaimAction::Quarantine(kind) => self.try_quarantine(&id, kind).await?,
                    ClaimAction::Claim(kind) => {
                        // A claimable split that was not assigned to us
                        // belongs to another worker, or to the queue.
                        if !self.assigned.contains(&id) || self.owned.len() >= cap {
                            continue;
                        }
                        self.try_claim(&id, kind).await?;
                    }
                }
            }
        }
        // The leader can take a revocation back: `desired_assignment` is
        // sticky on the current owner, and a draining split still holds
        // its lease. This ends the revocation, not the drain, so the
        // entry stays, flagged and re-anchored on a slower bound. A
        // source that already stopped intake keeps draining
        // (`SplitSource` has no seam to resume it), and a drain that
        // never finishes strands the split; `service_revocations` bounds
        // it.
        let restored: Vec<String> = self
            .revoking
            .iter()
            .filter(|(id, r)| {
                !r.cancelled && self.assigned.contains(*id) && self.owned.contains_key(*id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        let now = self.clock.now();
        for id in restored {
            tracing::debug!(split = %id, "revocation cancelled: the leader assigned it back");
            if let Some(entry) = self.revoking.get_mut(&id) {
                entry.cancelled = true;
                // Re-anchor: the absolute clock stops here and the
                // silence clock starts.
                entry.last_progress = now;
            }
            self.metrics(|m| m.revocation(RevocationOutcome::Cancelled));
        }
        if !self.assignment_seen {
            return Ok(());
        }
        // A cancelled entry is a drain, not a revocation, so the leader
        // dropping the split again must re-request.
        let stale: Vec<String> = self
            .owned
            .keys()
            .filter(|id| {
                !self.assigned.contains(*id) && self.revoking.get(*id).is_none_or(|r| r.cancelled)
            })
            .cloned()
            .collect();
        for id in stale {
            self.begin_revoke(&id);
        }
        Ok(())
    }

    /// Leader side: compute the desired assignment and publish the records
    /// that changed.
    ///
    /// Gated on `assign_dirty`, which membership, split status, ownership,
    /// spec arrival, a grace window elapsing and a deleted assignment record
    /// set. A put of another writer's assignment record sets nothing; it
    /// reaches the decision at the next publish something else triggers, the
    /// reconcile backstop at the latest.
    /// [`protocol::desired_assignment`] is a fixpoint, so
    /// recomputing on a clean fleet would publish nothing; it is skipped
    /// anyway because the recompute itself is an O(members x splits) scan
    /// and `step` runs after every batch of watch events.
    ///
    /// Publishing is best-effort per instance. A failed or lost write
    /// leaves that instance on its previous assignment, which is stale but
    /// never unsafe, and the next step retries. There is no barrier and no
    /// acknowledgment protocol: the leader learns that a revocation
    /// completed by watching the split's lease disappear.
    pub(super) async fn publish_assignments(&mut self) -> Result<(), CoordinationError> {
        if !self.caught_up || !std::mem::take(&mut self.assign_dirty) {
            return Ok(());
        }
        let generation = match &self.plan {
            Some((plan, _)) => plan.generation,
            None => return Ok(()), // nothing planned yet: nothing to assign
        };
        let members: BTreeSet<String> = self.presence.keys().cloned().collect();
        if members.is_empty() {
            return Ok(());
        }
        let reserved = self.reserved_splits();
        // Ownership cannot stand in for this: a claim reaches the leader's
        // view some time after the assignment, and a graceful release clears
        // `owner` before dropping presence.
        let previous = protocol::last_assignees(&self.assignments, &members);
        // The tie-break seed is the job fingerprint, NOT `self.seed`,
        // which mixes in a per-run nonce: a leader-specific seed makes
        // every failover re-break every tie and churn the fleet.
        let desired = protocol::desired_assignment(
            &members,
            &self.splits,
            &reserved,
            &previous,
            &self.member_caps,
            self.config.max_in_flight,
            self.fp,
        );
        // A split named for the first time is work being handed out, not
        // a move. This counts what was *published*, not what landed, so a
        // write that fails below has its move counted again on each
        // publish until a later one rewrites its record.
        let moved = desired
            .iter()
            .flat_map(|(instance, splits)| {
                splits
                    .iter()
                    .map(move |id| (id.as_str(), instance.as_str()))
            })
            .filter(|(id, instance)| previous.get(id).is_some_and(|prev| prev != instance))
            .count();
        let mut published = 0usize;
        for (instance, splits) in desired {
            let current = self.assignments.get(&instance);
            if current.is_some_and(|(val, _)| val.splits == splits && val.generation == generation)
            {
                continue; // unchanged: the common case, and free
            }
            let val = AssignmentVal {
                schema: records::SCHEMA,
                generation,
                splits,
            };
            let key = records::assign_key(&instance);
            let bytes = records::encode_val(&val);
            let outcome = match current {
                Some((_, rev)) => {
                    self.store
                        .update(Keyspace::Durable, &key, bytes, *rev)
                        .await
                }
                None => self.store.create(Keyspace::Durable, &key, bytes).await,
            };
            match outcome {
                // Adopt our own write rather than just recording it: the
                // watch echo arrives at the revision just stored, which
                // the stale-echo guard drops.
                Ok(CasOutcome::Won(rev)) => {
                    published += 1;
                    self.apply_assignment(&instance, val, rev);
                }
                // Someone else wrote it, or the key is gone and the cached
                // revision is a ghost. Read what is there, so the next step
                // updates it or creates it instead of losing again.
                Ok(CasOutcome::Lost) => {
                    self.assign_dirty = true;
                    match self.store.get(Keyspace::Durable, &key).await {
                        Ok(Some(entry)) => self.apply_state_put(&entry)?,
                        Ok(None) => {
                            self.assignments.remove(&instance);
                        }
                        Err(e) => {
                            self.assignments.remove(&instance);
                            fatal_only("re-reading an assignment", &e)?;
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(%instance, error = %e, "assignment publish failed; retrying");
                    self.assign_dirty = true;
                    fatal_only("publishing an assignment", &e)?;
                }
            }
        }
        if published > 0 {
            // One line per rebalance. Both arms are needed: a member
            // joining a fleet whose lanes are full moves nothing, and a
            // grace window expiring moves splits with the fleet the same
            // size. Neither admits a split completing.
            if members != self.announced_members || moved > 0 {
                tracing::info!(
                    members = members.len(),
                    moved,
                    generation,
                    "assignment published"
                );
            }
            self.announced_members = members;
        }
        // Drop assignments for instances gone past their grace window.
        let stale: Vec<String> = self
            .assignments
            .keys()
            .filter(|i| !self.presence.contains_key(*i) && !self.departed.contains_key(*i))
            .cloned()
            .collect();
        for instance in stale {
            let key = records::assign_key(&instance);
            let Some((_, rev)) = self.assignments.get(&instance) else {
                continue;
            };
            match self.store.delete(Keyspace::Durable, &key, Some(*rev)).await {
                Ok(CasOutcome::Won(_)) => {
                    self.note_deleted(Keyspace::Durable, &key);
                    self.assignments.remove(&instance);
                }
                Ok(CasOutcome::Lost) => {}
                Err(e) => fatal_only("deleting a departed instance's assignment", &e)?,
            }
        }
        Ok(())
    }

    /// Force the next leader step to recompute and republish.
    pub(crate) fn mark_assignment_dirty(&mut self) {
        self.assign_dirty = true;
    }

    /// Expire the grace windows of departed instances.
    ///
    /// Runs on **every** worker's step, not just the leader's: every worker
    /// records departures (leadership can move between a departure and the
    /// next publish), so every worker has to expire them too, or a process
    /// that is never elected accumulates one entry per historical peer for
    /// its whole life.
    ///
    /// A zero delay short-circuits to "withhold nothing" rather than
    /// falling through the same code path with a zero comparison. That is
    /// deliberate and it is the point of this function's shape: a zero that
    /// flows through a general delay path as just another value is how
    /// "withhold nothing" turns into "withhold indefinitely". Making it a
    /// case of its own means that bug is not expressible here.
    pub(super) fn prune_departed(&mut self) {
        // A returning instance cancels its own grace window immediately.
        self.departed.retain(|i, _| !self.presence.contains_key(i));
        if self.config.rebalance_delay.is_zero() {
            self.departed.clear();
            return;
        }
        let delay = self.config.rebalance_delay;
        let now = self.clock.now();
        let before = self.departed.len();
        self.departed
            .retain(|_, since| now.duration_since(*since) < delay);
        // An elapsed window frees its splits for the next publish.
        if self.departed.len() < before {
            self.assign_dirty = true;
        }
    }

    /// Splits withheld from assignment because their owner departed less
    /// than `rebalance_delay` ago.
    fn reserved_splits(&mut self) -> BTreeSet<String> {
        self.prune_departed();
        if self.config.rebalance_delay.is_zero() || self.departed.is_empty() {
            return BTreeSet::new();
        }
        self.splits
            .iter()
            .filter(|(_, state)| {
                state
                    .progress
                    .owner
                    .as_deref()
                    .is_some_and(|o| self.departed.contains_key(o))
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Adopt an `assign.{instance}` record seen on the durable watch.
    pub(super) fn apply_assignment(&mut self, instance: &str, val: AssignmentVal, rev: Revision) {
        if instance == self.instance {
            // A deposed leader's late write is not an instruction, but its
            // revision is cached so our next publish to the key wins.
            if val.generation < self.assign_generation {
                tracing::debug!(
                    generation = val.generation,
                    seen = self.assign_generation,
                    "ignoring an assignment from a superseded generation"
                );
            } else {
                self.adopt_own_assignment(&val);
            }
        }
        self.assignments.insert(instance.to_string(), (val, rev));
    }

    fn adopt_own_assignment(&mut self, val: &AssignmentVal) {
        self.assign_generation = val.generation;
        let now: BTreeSet<String> = val.splits.iter().cloned().collect();
        // Start the acquisition clock for newly-named splits, and
        // stop it for anything no longer expected. Not for one
        // already held: that reports a drain as a reassignment.
        for id in now.difference(&self.assigned) {
            if self.owned.contains_key(id) {
                continue;
            }
            self.awaiting.insert(id.clone(), Instant::now());
        }
        self.awaiting.retain(|id, _| now.contains(id));
        self.assigned = now;
        self.assignment_seen = true;
    }
}
