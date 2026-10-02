//! Claiming, quarantining and dropping splits through the lease and progress
//! records.

use super::{OwnedSplit, Task};
use crate::error::fatal_only;
use crate::protocol::ClaimKind;
use crate::records::{self, LeaseVal, SplitProgressRecord, SplitStatus};
use crate::store::{CasOutcome, CoordinationStore, Keyspace, Revision};
use spate_core::coordination::{CoordinationError, CoordinationEvent, LeaseEpoch, SplitId};
use spate_core::metrics::{AcquireReason, RevocationOutcome, SplitLossReason, WriteOutcome};
use tokio::time::Instant;

impl<S: CoordinationStore + Clone> Task<S> {
    /// The two-key claim: lease first (create, or CAS-update for a fast
    /// reclaim), then the progress-record CAS that transfers ownership.
    pub(super) async fn try_claim(
        &mut self,
        id: &str,
        mut kind: ClaimKind,
    ) -> Result<(), CoordinationError> {
        // A cooperative release is two writes on two watch streams, and a
        // fast claimant can see the lease vanish before the owner-clear
        // arrives, misreading a completed revocation as a death takeover
        // and burning a delivery attempt. One durable read settles it:
        // an already-cleared owner means Released.
        if kind == ClaimKind::Expired {
            let key = records::split_key_str(id);
            if let Ok(Some(entry)) = self.store.get(Keyspace::Durable, &key).await
                && let Ok(record) = SplitProgressRecord::parse(&key, &entry.value, self.fp)
            {
                // Honor the downgrade only from a read at least as fresh
                // as the view: a lagging replica's stale owner-`None`
                // lets a poison split cycle past its quarantine cap.
                let fresh = self
                    .splits
                    .get(id)
                    .is_none_or(|state| entry.revision >= state.progress_rev);
                if record.owner.is_none() && fresh {
                    kind = ClaimKind::Released;
                }
                self.upsert_progress(id, record, entry.revision)?;
            }
        }
        let Some(state) = self.splits.get(id) else {
            return Ok(());
        };
        // The refreshed record can show the split finished, parked or out
        // of attempts since the candidate was chosen.
        if state.progress.status != SplitStatus::Runnable {
            return Ok(());
        }
        if kind.consumes_attempt() && state.progress.attempts + 1 >= self.config.max_attempts {
            self.quarantine_scan = true;
            return Ok(());
        }
        let next_epoch = state.progress.epoch + 1;
        let lease_key = records::split_key_str(id);
        let lease_val = records::encode_val(&LeaseVal {
            schema: records::SCHEMA,
            owner: self.instance.clone(),
            nonce: self.nonce.clone(),
            epoch: next_epoch,
        });
        let started = Instant::now();
        let lease_outcome = match (kind, &state.lease) {
            (ClaimKind::Reclaim, Some((_, rev))) => {
                self.store
                    .update(Keyspace::Ephemeral, &lease_key, lease_val, *rev)
                    .await
            }
            _ => {
                self.store
                    .create(Keyspace::Ephemeral, &lease_key, lease_val)
                    .await
            }
        };
        let lease_rev = match lease_outcome {
            Ok(CasOutcome::Won(rev)) => rev,
            Ok(CasOutcome::Lost) => return Ok(()), // a peer won; watch updates the view
            Err(e) => {
                tracing::warn!(split = %id, error = %e, "lease write failed; next tick retries");
                self.metrics(|m| m.write(WriteOutcome::Error, started.elapsed()));
                return fatal_only("writing a claim's lease", &e);
            }
        };
        let reason = match kind {
            ClaimKind::Create => AcquireReason::Create,
            // A split whose previous owner cleared the owner field left
            // cleanly, so this claim replays nothing. Expired never
            // reaches here; the guard above downgraded it.
            ClaimKind::Released => AcquireReason::Reassigned,
            ClaimKind::Reclaim => AcquireReason::Reclaimed,
            ClaimKind::Expired => AcquireReason::Expired,
        };
        self.record_claim(id, kind, reason, next_epoch, lease_rev, started)
            .await?;
        // Assignment-to-acquisition. Observed on the write that
        // transferred ownership, so a rising median means slow reassignments
        // are *succeeding*, not that they got slower.
        if self.owned.contains_key(id)
            && let Some(since) = self.awaiting.remove(id)
        {
            self.metrics(|m| m.assignment_latency(since.elapsed()));
        }
        Ok(())
    }

    /// The progress-record CAS after a won lease. On a lost CAS, adopt a
    /// zombie's late commit (legal, since it was still the owner) and retry
    /// once. The acquisition metric counts here, on the write that
    /// transfers ownership, under the caller's reason.
    async fn record_claim(
        &mut self,
        id: &str,
        kind: ClaimKind,
        reason: AcquireReason,
        next_epoch: u64,
        lease_rev: Revision,
        started: Instant,
    ) -> Result<(), CoordinationError> {
        let key = records::split_key_str(id);
        for _ in 0..2 {
            let Some(state) = self.splits.get(id) else {
                self.release_lease_key(id, lease_rev).await?;
                return Ok(());
            };
            let Some(spec_record) = &state.spec else {
                // The spec put is in flight; the source needs the descriptor.
                self.release_lease_key(id, lease_rev).await?;
                return Ok(());
            };
            let split = spec_record.spec()?;
            // A takeover consumes an attempt only while the record it
            // replaces still names an owner. A release costs none, and a
            // failure report has counted its own.
            let consumes = kind.consumes_attempt() && state.progress.owner.is_some();
            let mut record = state.progress.clone();
            record.epoch = next_epoch;
            record.owner = Some(self.instance.clone());
            record.attempts += u32::from(consumes);
            record.written_at_ms = records::now_ms();
            let expected = state.progress_rev;
            let outcome = self
                .store
                .update(Keyspace::Durable, &key, record.encode(), expected)
                .await;
            match outcome {
                Ok(CasOutcome::Won(rev)) => {
                    self.metrics(|m| {
                        m.write(WriteOutcome::Ok, started.elapsed());
                        m.acquired(reason);
                    });
                    tracing::debug!(split = %id, ?reason, epoch = next_epoch, "split claimed");
                    let progress = record.progress()?;
                    self.record_own_write(id, record, rev, lease_rev)?;
                    self.emit(CoordinationEvent::Gained {
                        split,
                        epoch: LeaseEpoch(next_epoch),
                        progress,
                    });
                    return Ok(());
                }
                Ok(CasOutcome::Lost) => {
                    self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                    // Refresh and decide: a zombie's late commit is
                    // adopted (retry); a terminal record means walk away.
                    match self.store.get(Keyspace::Durable, &key).await {
                        Ok(Some(entry)) => {
                            self.apply_state_put(&entry)?;
                            let fresh = &self.splits[id];
                            let capped = kind.consumes_attempt()
                                && fresh.progress.owner.is_some()
                                && fresh.progress.attempts + 1 >= self.config.max_attempts;
                            if fresh.progress.status != SplitStatus::Runnable
                                || fresh.progress.epoch >= next_epoch
                                || capped
                            {
                                // Terminal, another claimant beat us between
                                // our lease write and record CAS, or the
                                // split is now due for quarantine.
                                self.quarantine_scan |= capped;
                                self.release_lease_key(id, lease_rev).await?;
                                return Ok(());
                            }
                            // else: adopted progress; retry the CAS once.
                        }
                        Ok(None) => {
                            self.release_lease_key(id, lease_rev).await?;
                            return Ok(());
                        }
                        Err(e) => {
                            self.release_lease_key(id, lease_rev).await?;
                            return fatal_only("re-reading a claimed record", &e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(split = %id, error = %e, "claim record write failed");
                    self.metrics(|m| m.write(WriteOutcome::Error, started.elapsed()));
                    self.release_lease_key(id, lease_rev).await?;
                    return fatal_only("writing a claim", &e);
                }
            }
        }
        self.release_lease_key(id, lease_rev).await?;
        Ok(())
    }

    /// Park an out-of-attempts split: the record CAS applies the same
    /// fence a claim would (epoch bump), so the dead owner's zombie can
    /// never write again.
    pub(super) async fn try_quarantine(
        &mut self,
        id: &str,
        kind: ClaimKind,
    ) -> Result<(), CoordinationError> {
        let Some(state) = self.splits.get(id) else {
            return Ok(());
        };
        let mut record = state.progress.clone();
        record.epoch += 1;
        record.status = SplitStatus::Quarantined;
        record.owner = None;
        record.attempts += u32::from(kind.consumes_attempt());
        record.written_at_ms = records::now_ms();
        let attempts = record.attempts;
        let key = records::split_key_str(id);
        let expected = state.progress_rev;
        let started = Instant::now();
        match self
            .store
            .update(Keyspace::Durable, &key, record.encode(), expected)
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                self.metrics(|m| m.write(WriteOutcome::Ok, started.elapsed()));
                tracing::warn!(split = %id, attempts, "split quarantined: out of delivery attempts");
                // upsert emits the Quarantined event and counts the metric
                // on the status transition.
                self.upsert_progress(id, record, rev)?;
                // Clear the dead owner's stale lease key so listings stay tidy.
                if kind == ClaimKind::Reclaim
                    && let Some(state) = self.splits.get(id)
                    && let Some((_, lease_rev)) = state.lease
                {
                    self.release_lease_key(id, lease_rev).await?;
                }
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                // Someone else moved it. Read it, so the next step decides
                // on the record rather than losing the same CAS again.
                match self.store.get(Keyspace::Durable, &key).await {
                    Ok(Some(entry)) => self.apply_state_put(&entry),
                    Ok(None) => Ok(()),
                    Err(e) => fatal_only("re-reading a split record", &e),
                }
            }
            Err(e) => {
                tracing::warn!(split = %id, error = %e, "quarantine write failed; next tick retries");
                self.metrics(|m| m.write(WriteOutcome::Error, started.elapsed()));
                fatal_only("quarantining a split", &e)?;
                // Re-arm the scan: `reconcile_assignment` takes the flag
                // before deciding whether to scan, so a worker at its
                // lane budget never re-derives this candidate and the
                // bounded job hangs instead of stalling.
                self.quarantine_scan = true;
                Ok(())
            }
        }
    }

    /// Best-effort removal of a lease key we hold (guarded by revision).
    /// A lost delete reads the key back once and deletes it at the read
    /// revision while it carries this worker's owner and nonce; a read that
    /// predates the latest renewal leaves the lease to expire. Only a fatal
    /// store error is returned.
    pub(super) async fn release_lease_key(
        &mut self,
        id: &str,
        lease_rev: Revision,
    ) -> Result<(), CoordinationError> {
        let key = records::split_key_str(id);
        self.note_deleted(Keyspace::Ephemeral, &key);
        let mut reread = None;
        match self
            .store
            .delete(Keyspace::Ephemeral, &key, Some(lease_rev))
            .await
        {
            Ok(CasOutcome::Won(_)) => {}
            Ok(CasOutcome::Lost) => match self.store.get(Keyspace::Ephemeral, &key).await {
                Ok(Some(entry))
                    if serde_json::from_slice::<LeaseVal>(&entry.value)
                        .is_ok_and(|v| v.owner == self.instance && v.nonce == self.nonce) =>
                {
                    match self
                        .store
                        .delete(Keyspace::Ephemeral, &key, Some(entry.revision))
                        .await
                    {
                        Ok(CasOutcome::Won(_)) => reread = Some(entry.revision),
                        Ok(CasOutcome::Lost) => {}
                        Err(e) => {
                            tracing::debug!(split = %id, error = %e, "lease cleanup failed; it will expire");
                            fatal_only("deleting a lease", &e)?;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!(split = %id, error = %e, "lease cleanup failed; it will expire");
                    fatal_only("re-reading a lease", &e)?;
                }
            },
            Err(e) => {
                tracing::debug!(split = %id, error = %e, "lease cleanup failed; it will expire");
                fatal_only("deleting a lease", &e)?;
            }
        }
        if let Some(state) = self.splits.get_mut(id)
            && state
                .lease
                .as_ref()
                .is_some_and(|(_, rev)| *rev == lease_rev || Some(*rev) == reread)
        {
            state.lease = None;
        }
        Ok(())
    }

    /// Fold our own successful claim into the view so later decisions see
    /// it (the watch echo arrives with a revision we already know and is
    /// skipped).
    fn record_own_write(
        &mut self,
        id: &str,
        record: SplitProgressRecord,
        rev: Revision,
        lease_rev: Revision,
    ) -> Result<(), CoordinationError> {
        let epoch = record.epoch;
        self.upsert_progress(id, record, rev)?;
        if let Some(state) = self.splits.get_mut(id) {
            state.lease = Some((
                LeaseVal {
                    schema: records::SCHEMA,
                    owner: self.instance.clone(),
                    nonce: self.nonce.clone(),
                    epoch,
                },
                lease_rev,
            ));
        }
        self.owned.insert(
            id.to_string(),
            OwnedSplit {
                lease_rev,
                last_ok_write: self.clock.now(),
            },
        );
        Ok(())
    }

    /// Stop tracking an owned split and tell the source.
    pub(super) fn drop_owned(&mut self, id: &str, reason: SplitLossReason) {
        if self.owned.remove(id).is_none() {
            return;
        }
        // A split fenced or starved mid-drain ends that revocation the
        // expensive way: whatever was uncommitted replays.
        self.settle_revocation(id, RevocationOutcome::Forced);
        self.metrics(|m| m.lost(reason));
        if let Ok(split) = SplitId::new(id.to_string()) {
            self.emit(CoordinationEvent::Lost { split });
        }
    }
}
