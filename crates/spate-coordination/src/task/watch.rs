//! Folding watch and listing entries into the task's view of leases, splits and
//! progress.

use super::Task;
use crate::error::fatal;
use crate::protocol::SplitState;
use crate::records::{
    self, AssignmentVal, LeaderVal, LeaseVal, PlanRecord, SplitProgressRecord, SplitSpecRecord,
    SplitStatus, WorkerVal,
};
use crate::store::{CoordinationStore, Entry, Keyspace, Revision, WatchEvent};
use spate_core::coordination::{CoordinationError, CoordinationEvent, SplitId};
use spate_core::metrics::{RevocationOutcome, SplitLossReason};

impl<S: CoordinationStore + Clone> Task<S> {
    pub(super) fn apply_lease_event(&mut self, event: WatchEvent) -> Result<(), CoordinationError> {
        match event {
            WatchEvent::Put(entry) => self.apply_lease_put(&entry),
            WatchEvent::Delete { key, revision } => {
                self.apply_lease_delete(&key, Some(revision));
                Ok(())
            }
            WatchEvent::SnapshotDone => Ok(()),
        }
    }

    pub(super) fn apply_lease_put(&mut self, entry: &Entry) -> Result<(), CoordinationError> {
        if entry.key == records::LEADER_KEY {
            // A put at or below a revision already held is a stale echo.
            let held = self
                .leader_observed
                .as_ref()
                .map(|(_, rev)| *rev)
                .max(self.leadership);
            if held.is_some_and(|rev| rev >= entry.revision) {
                return Ok(());
            }
            let leader: LeaderVal = records::parse_val(&entry.key, &entry.value)?;
            if self.leadership.is_some() && leader.nonce != self.nonce {
                // Deposed: someone else won the key after our lease
                // lapsed. The generation fence rejects our plan writes.
                tracing::warn!(new_leader = %leader.owner, "leadership lost");
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
            }
            self.leader_observed = Some((leader, entry.revision));
            return Ok(());
        }
        if let Some(instance) = records::parse_worker_key(&entry.key) {
            if self
                .presence
                .get(instance)
                .is_some_and(|rev| *rev >= entry.revision)
            {
                return Ok(());
            }
            if self
                .presence
                .insert(instance.to_string(), entry.revision)
                .is_none()
            {
                self.assign_dirty = true; // membership grew
            }
            // An unreadable presence value costs balance, never safety:
            // the member keeps the leader's own budget.
            match records::parse_val::<WorkerVal>(&entry.key, &entry.value) {
                Ok(worker) if worker.max_in_flight > 0 => {
                    if self
                        .member_caps
                        .insert(instance.to_string(), worker.max_in_flight)
                        != Some(worker.max_in_flight)
                    {
                        self.assign_dirty = true;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(key = %entry.key, error = %e, "unreadable presence value");
                }
            }
            return Ok(());
        }
        if let Some(id) = records::parse_split_key(&entry.key) {
            // A put at or below the revision the view already holds is a
            // stale echo and must not be applied.
            if let Some(state) = self.splits.get(id)
                && state
                    .lease
                    .as_ref()
                    .is_some_and(|(_, rev)| *rev >= entry.revision)
            {
                return Ok(());
            }
            if self
                .pending_leases
                .get(id)
                .is_some_and(|(_, rev)| *rev >= entry.revision)
            {
                return Ok(());
            }
            let lease: LeaseVal = records::parse_val(&entry.key, &entry.value)?;
            if self.owned.contains_key(id) {
                // Someone rewrote the lease of a split we hold. Our own
                // heartbeat echoes match our nonce; anything else fenced us.
                if lease.nonce != self.nonce {
                    if lease.owner == self.instance {
                        return Err(fatal(format!(
                            "two live workers share instance_id {:?} (foreign nonce on our \
                             lease for split {id}); instance ids must be unique per live \
                             worker — use the pod name, not a constant",
                            self.instance
                        )));
                    }
                    tracing::warn!(split = %id, thief = %lease.owner, "lease taken; split lost");
                    self.drop_owned(id, SplitLossReason::Fenced);
                }
            }
            match self.splits.get_mut(id) {
                Some(state) => {
                    if state
                        .lease
                        .as_ref()
                        .is_none_or(|(l, _)| l.owner != lease.owner)
                    {
                        self.assign_dirty = true;
                    }
                    state.lease = Some((lease, entry.revision));
                }
                None => {
                    self.pending_leases
                        .insert(id.to_string(), (lease, entry.revision));
                }
            }
        }
        Ok(())
    }

    /// Apply a lease-key deletion. `revision` is the deletion's own
    /// revision when it came from a watch (used to discard stale echoes
    /// of deletes the key has since been rewritten past); `None` means
    /// authoritative absence from a reconcile listing.
    pub(super) fn apply_lease_delete(&mut self, key: &str, revision: Option<Revision>) {
        self.note_deleted(Keyspace::Ephemeral, key);
        let newer_than = |current: Revision| revision.is_none_or(|rev| rev > current);
        if key == records::LEADER_KEY {
            if !self
                .leader_observed
                .as_ref()
                .is_none_or(|(_, rev)| newer_than(*rev))
            {
                return; // stale echo: the key was rewritten after this delete
            }
            self.leader_observed = None;
            if let Some(rev) = self.leadership
                && newer_than(rev)
            {
                tracing::warn!("leadership lease expired");
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
            }
            return;
        }
        if let Some(instance) = records::parse_worker_key(key) {
            if self.presence.get(instance).copied().is_none_or(newer_than) {
                self.presence.remove(instance);
                self.member_caps.remove(instance);
                self.assign_dirty = true;
                // Start this instance's grace window. Every worker tracks
                // it: leadership can move between the departure and the
                // next publish.
                if instance != self.instance {
                    let now = self.clock.now();
                    self.departed.entry(instance.to_string()).or_insert(now);
                }
            }
            return;
        }
        if let Some(id) = records::parse_split_key(key) {
            if let Some(owned) = self.owned.get(id) {
                if !newer_than(owned.lease_rev) {
                    return; // stale echo of a delete our claim already replaced
                }
                // Heartbeats have been failing for a full TTL.
                self.drop_owned(id, SplitLossReason::Starved);
            }
            // No revocation bookkeeping here. Lease deletes also happen
            // for fails, completions, departures, and expiry; the
            // leader's `assign.{instance}` record starts and ends one.
            if let Some(state) = self.splits.get_mut(id)
                && state.lease.as_ref().is_none_or(|(_, rev)| newer_than(*rev))
            {
                if state.lease.is_some() {
                    self.assign_dirty = true;
                }
                state.lease = None;
                // An expired owner may sit at the attempts cap: give the
                // next pass a chance to park it even at target.
                if state.progress.status == SplitStatus::Runnable
                    && state.progress.attempts + 1 >= self.config.max_attempts
                {
                    self.quarantine_scan = true;
                }
            }
            if self
                .pending_leases
                .get(id)
                .is_some_and(|(_, rev)| newer_than(*rev))
            {
                self.pending_leases.remove(id);
            }
        }
    }

    pub(super) fn apply_state_event(&mut self, event: WatchEvent) -> Result<(), CoordinationError> {
        match event {
            WatchEvent::Put(entry) => self.apply_state_put(&entry),
            WatchEvent::Delete { key, .. } => {
                self.note_deleted(Keyspace::Durable, &key);
                // The protocol deletes two durable keys: assignment
                // records for departed instances, and startup probe keys.
                if let Some(instance) = records::parse_assign_key(&key) {
                    self.assignments.remove(instance);
                    // A leader republishes it if the instance is a member.
                    self.assign_dirty = true;
                    if instance == self.instance {
                        // An absent record means "nothing has been
                        // decided", never "release everything".
                        self.assignment_seen = false;
                        self.assigned.clear();
                        self.awaiting.clear();
                    }
                } else if let Some(instance) = records::parse_probe_key(&key) {
                    tracing::debug!(instance = %instance, "startup probe key cleared");
                } else {
                    tracing::warn!(
                        key = %key,
                        "durable record deleted externally; reconcile treats it as absent"
                    );
                }
                Ok(())
            }
            WatchEvent::SnapshotDone => Ok(()),
        }
    }

    pub(crate) fn apply_state_put(&mut self, entry: &Entry) -> Result<(), CoordinationError> {
        if let Some(instance) = records::parse_assign_key(&entry.key) {
            // Stale-echo guard, as on every other watched key.
            if self
                .assignments
                .get(instance)
                .is_some_and(|(_, rev)| *rev >= entry.revision)
            {
                return Ok(());
            }
            // An unreadable assignment carries no ownership: ignoring it
            // costs balance and this worker keeps what it holds.
            let val: AssignmentVal = match records::parse_val(&entry.key, &entry.value) {
                Ok(val) => val,
                Err(e) => {
                    tracing::warn!(key = %entry.key, error = %e, "unreadable assignment ignored");
                    return Ok(());
                }
            };
            self.apply_assignment(instance, val, entry.revision);
            return Ok(());
        }
        if entry.key == records::VERDICT_KEY {
            if !self.verdict_seen {
                self.verdict_seen = true;
                self.verdict_listing_allowed = true;
            }
            self.verdict_written = true;
            return Ok(());
        }
        if entry.key == records::PLAN_KEY {
            if entry.revision.0 <= self.plan_rev_seen {
                return Ok(());
            }
            self.plan_rev_seen = entry.revision.0;
            let plan = PlanRecord::parse(&entry.value, &self.fingerprint)?;
            if self.leadership.is_some()
                && let Some((current, _)) = &self.plan
                && plan.generation > current.generation
            {
                tracing::warn!(
                    generation = plan.generation,
                    "deposed by a newer plan generation"
                );
                self.leadership = None;
                self.metrics(|m| m.set_leader(false));
            }
            self.plan = Some((plan, entry.revision));
            return Ok(());
        }
        if let Some(id) = records::parse_spec_key(&entry.key) {
            let record = SplitSpecRecord::parse(&entry.key, &entry.value, self.fp)?;
            self.attach_spec(id, record);
            return Ok(());
        }
        if let Some(id) = records::parse_split_key(&entry.key) {
            let record = SplitProgressRecord::parse(&entry.key, &entry.value, self.fp)?;
            self.upsert_progress(id, record, entry.revision)?;
        }
        Ok(())
    }

    /// Attach an observed spec record (immutable, so re-deliveries are
    /// echoes) to its split, or buffer it until the progress record lands.
    pub(crate) fn attach_spec(&mut self, id: &str, record: SplitSpecRecord) {
        match self.splits.get_mut(id) {
            Some(state) => {
                if state.spec.is_none() {
                    state.spec = Some(record);
                    // A split becomes assignable once its spec is observed.
                    self.assign_dirty = true;
                }
            }
            None => {
                self.pending_specs.entry(id.to_string()).or_insert(record);
            }
        }
    }

    /// Fold a progress record into the view, from a watch event, a
    /// reconcile listing, or our own successful write. This is the ONLY
    /// place progress state changes: it keeps the status tallies exact,
    /// fences our ownership when a peer's higher epoch arrives, and emits
    /// the `Quarantined` transition exactly once.
    pub(crate) fn upsert_progress(
        &mut self,
        id: &str,
        record: SplitProgressRecord,
        rev: Revision,
    ) -> Result<(), CoordinationError> {
        let (previous_status, current_epoch) = match self.splits.get(id) {
            Some(state) => {
                if state.progress_rev >= rev {
                    return Ok(()); // stale, or our own echoed write
                }
                if state.progress.owner != record.owner {
                    // An owner-clear moves the sticky pass.
                    self.assign_dirty = true;
                }
                (Some(state.progress.status), Some(state.progress.epoch))
            }
            None => (None, None),
        };
        // A foreign `owner` on a split we are awaiting is the normal
        // mid-revocation state, and the wait is what `awaiting` times.
        // Clearing the timer here reads every assignment latency as ~0;
        // it is retired on a claim, or when the leader stops assigning.
        if let Some(current_epoch) = current_epoch
            && self.owned.contains_key(id)
            && record.epoch > current_epoch
        {
            if self.sent_quarantine_report(id, &record) {
                // This worker's own quarantining report ends the tenancy with
                // no loss; the next heartbeat deletes the lease.
                self.owned.remove(id);
                self.settle_revocation(id, RevocationOutcome::Forced);
                self.owed_leases.insert(id.to_string(), current_epoch);
            } else {
                // A claimant CASed the record past our tenancy.
                self.drop_owned(id, SplitLossReason::Fenced);
            }
        }
        if previous_status != Some(record.status) {
            match previous_status {
                Some(SplitStatus::Runnable) => self.runnable_count -= 1,
                Some(SplitStatus::Completed) => self.completed_count -= 1,
                Some(SplitStatus::Quarantined) => self.quarantined_count -= 1,
                None => {}
            }
            match record.status {
                SplitStatus::Runnable => self.runnable_count += 1,
                SplitStatus::Completed => self.completed_count += 1,
                SplitStatus::Quarantined => self.quarantined_count += 1,
            }
            if record.status == SplitStatus::Quarantined {
                self.metrics(|m| m.quarantined());
                self.emit(CoordinationEvent::Quarantined {
                    split: SplitId::new(id.to_string())?,
                    attempts: record.attempts,
                });
            }
        }
        // A runnable split whose next takeover would hit the cap needs a
        // quarantine decision, even at this worker's working-set target.
        if record.status == SplitStatus::Runnable && record.attempts + 1 >= self.config.max_attempts
        {
            self.quarantine_scan = true;
        }
        if previous_status != Some(record.status) || previous_status.is_none() {
            self.assign_dirty = true; // the assignable pool moved
        }
        match self.splits.get_mut(id) {
            Some(state) => {
                state.progress = record;
                state.progress_rev = rev;
            }
            None => {
                // A lease or spec observed before its progress record attaches now.
                let lease = self.pending_leases.remove(id);
                let spec = self.pending_specs.remove(id);
                self.splits.insert(
                    id.to_string(),
                    SplitState {
                        progress: record,
                        progress_rev: rev,
                        spec,
                        lease,
                    },
                );
            }
        }
        Ok(())
    }
}
