//! Serving control-thread commands for commits, completions, failures and
//! releases.

use super::{Command, ReleaseOutcome, Task};
use crate::error::{fatal, fatal_only, retryable, store_error};
use crate::records::{self, SplitProgressRecord, SplitStatus};
use crate::store::{CasOutcome, CoordinationStore, Keyspace, Revision};
use spate_core::coordination::{CoordinationError, CoordinationErrorKind, SplitId, SplitProgress};
use spate_core::metrics::{RevocationOutcome, SplitLossReason, WriteOutcome};
use tokio::time::Instant;

impl<S: CoordinationStore + Clone> Task<S> {
    pub(super) async fn handle_command(
        &mut self,
        command: Command,
    ) -> Result<(), CoordinationError> {
        let (result, reply) = match command {
            Command::Depart { deadline, reply } => {
                let shortfall = Box::pin(self.depart(Instant::from_std(deadline))).await;
                self.stopping = true;
                let fatal_error = shortfall.fatal.as_ref().map(|e| fatal(e.reason.clone()));
                let _ = reply.try_send(shortfall.into_reply());
                return fatal_error.map_or(Ok(()), Err);
            }
            Command::Commit {
                split,
                progress,
                reply,
            } => (self.commit(&split, &progress).await, reply),
            Command::Fail {
                split,
                reason,
                reply,
            } => (self.fail_split(&split, &reason).await, reply),
            Command::Release {
                splits,
                departure,
                reply,
            } => (self.release_splits(&splits, departure).await, reply),
            Command::DeclineRevoke { split, reply } => {
                let id = split.as_str().to_string();
                // A decline says the source never stopped intake, so
                // there is no drain. A live revocation is forced; under a
                // cancelled one the entry is dropped, watchdog and all.
                let cancelled = self.revoking.get(&id).is_some_and(|r| r.cancelled);
                let result = if cancelled {
                    self.revoking.remove(&id);
                    Ok(())
                } else if self.revoking.contains_key(&id) {
                    self.force_revocation(&id).await
                } else {
                    Ok(()) // never offered, or already settled
                };
                (result, reply)
            }
        };
        // A fatal failure is answered, then stops the task.
        let fatal_error = result
            .as_ref()
            .err()
            .filter(|e| e.kind == CoordinationErrorKind::Fatal)
            .map(|e| fatal(e.reason.clone()));
        let _ = reply.try_send(result);
        match fatal_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The fenced commit: one CAS on the durable progress record. The
    /// record is small by schema (the descriptor lives in the immutable
    /// spec record), so commit cost is independent of descriptor size.
    ///
    /// A lost CAS reads the record back. A newer runnable record of this
    /// tenancy is written again on top. This tenancy's own completed record
    /// ends the tenancy as a completing commit does, with no `Lost`. The caller
    /// gets `Ok` if this commit repeats the completing commit that landed, and
    /// `Fenced` otherwise. Another writer's record means a peer owns the split:
    /// nothing was written, the caller gets `Fenced`, and `Lost` follows. A
    /// read older than the write that won is `Retryable`, a failed read keeps
    /// its store error's class, and either keeps the split held.
    async fn commit(
        &mut self,
        split: &SplitId,
        progress: &SplitProgress,
    ) -> Result<(), CoordinationError> {
        let id = split.as_str();
        if !self.owned.contains_key(id) {
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fenced,
                format!("split {split} is not held by this worker; nothing was written"),
            ));
        }
        let owned_epoch = self
            .splits
            .get(id)
            .expect("owned splits are in the view")
            .progress
            .epoch;
        let key = records::split_key_str(id);
        loop {
            let state = self.splits.get(id).expect("owned splits are in the view");
            if let Some(previous) = state.progress.watermark
                && progress.watermark < previous
            {
                return Err(fatal(format!(
                    "split {split}: watermark would regress {previous} -> {} — this is a \
                     source bug (watermarks are one past the last acknowledged record and \
                     never move backwards)",
                    progress.watermark
                )));
            }
            let mut record = state.progress.clone();
            record.watermark = Some(progress.watermark);
            record.state = Some(records::b64_encode(&progress.state));
            record.completed = progress.completed;
            if progress.completed {
                record.status = SplitStatus::Completed;
            }
            record.written_at_ms = records::now_ms();
            let expected = state.progress_rev;
            let started = Instant::now();
            match self
                .store
                .update(Keyspace::Durable, &key, record.encode(), expected)
                .await
            {
                Ok(CasOutcome::Won(rev)) => {
                    self.metrics(|m| m.write(WriteOutcome::Ok, started.elapsed()));
                    // A landed commit is the only liveness signal a draining
                    // split gives the task, and what a cancelled revocation's
                    // watchdog is armed against.
                    let now = self.clock.now();
                    if let Some(entry) = self.revoking.get_mut(id) {
                        entry.last_progress = now;
                    }
                    self.upsert_progress(id, record, rev)?;
                    if progress.completed {
                        self.finish_completed(id).await?;
                    }
                    return Ok(());
                }
                Ok(CasOutcome::Lost) => {
                    self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                }
                Err(e) => {
                    self.metrics(|m| m.write(WriteOutcome::Error, started.elapsed()));
                    return Err(store_error(&format!("committing split {split}"), &e));
                }
            }
            let Some((fresh, rev)) = self.reread_record(split, &key, expected).await? else {
                self.drop_owned(id, SplitLossReason::Fenced);
                return Err(CoordinationError::new(
                    CoordinationErrorKind::Fenced,
                    format!("split {split} is owned by a peer; nothing was written"),
                ));
            };
            let ours = fresh.owner.as_deref() == Some(self.instance.as_str())
                && fresh.epoch == owned_epoch;
            if ours
                && fresh.watermark == Some(progress.watermark)
                && fresh.completed == progress.completed
            {
                // The winning write is this commit, landed with its reply lost.
                self.upsert_progress(id, fresh, rev)?;
                if progress.completed {
                    self.finish_completed(id).await?;
                }
                return Ok(());
            }
            let status = fresh.status;
            self.upsert_progress(id, fresh, rev)?;
            match status {
                SplitStatus::Runnable if ours => continue,
                SplitStatus::Completed if ours => {
                    self.finish_completed(id).await?;
                    return Err(CoordinationError::new(
                        CoordinationErrorKind::Fenced,
                        format!(
                            "split {split} was already completed by this worker; nothing was \
                             written"
                        ),
                    ));
                }
                _ => {}
            }
            self.drop_owned(id, SplitLossReason::Fenced);
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fenced,
                format!("split {split} is owned by a peer; nothing was written"),
            ));
        }
    }

    /// The split record and its revision, read back after a CAS at
    /// `expected` lost; `None` when the record is gone.
    async fn reread_record(
        &mut self,
        split: &SplitId,
        key: &str,
        expected: Revision,
    ) -> Result<Option<(SplitProgressRecord, Revision)>, CoordinationError> {
        let entry = match self.store.get(Keyspace::Durable, key).await {
            Ok(Some(entry)) => entry,
            Ok(None) => return Ok(None),
            Err(e) => return Err(store_error(&format!("re-reading split {split}"), &e)),
        };
        if entry.revision <= expected {
            return Err(retryable(format!(
                "split {split}: the record read back is older than the write that won"
            )));
        }
        let fresh = SplitProgressRecord::parse(key, &entry.value, self.fp)?;
        Ok(Some((fresh, entry.revision)))
    }

    /// Terminal commit bookkeeping: hand the lease back, stop tracking.
    async fn finish_completed(&mut self, id: &str) -> Result<(), CoordinationError> {
        let lease_rev = self.owned.get(id).map(|o| o.lease_rev);
        self.owned.remove(id);
        // A split that finishes mid-revocation ends it: its tail is
        // committed and nothing replays.
        self.settle_revocation(id, RevocationOutcome::Drained);
        if let Some(lease_rev) = lease_rev {
            self.release_lease_key(id, lease_rev).await?;
        }
        Ok(())
    }

    /// Explicit failure report: consumes an attempt, ends this tenancy
    /// gracefully-for-the-lease but non-gracefully for the attempt
    /// accounting, and quarantines at the cap. A lost CAS on this tenancy's
    /// own newer runnable record writes the report again on top of it; any
    /// other record, or a read-back that lags or fails `Retryable`, returns
    /// `Fenced` and emits `Lost`.
    async fn fail_split(&mut self, split: &SplitId, reason: &str) -> Result<(), CoordinationError> {
        let id = split.as_str();
        let Some(owned) = self.owned.get(id) else {
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fenced,
                format!("split {split} is not held by this worker"),
            ));
        };
        let lease_rev = owned.lease_rev;
        let state = self.splits.get(id).expect("owned splits are in the view");
        let owned_epoch = state.progress.epoch;
        let attempts = state.progress.attempts + 1;
        let quarantining = attempts >= self.config.max_attempts;
        self.metrics(|m| m.failed());
        tracing::warn!(split = %id, reason, attempts, quarantining, "split failed by the source");
        let key = records::split_key_str(id);
        loop {
            let state = self.splits.get(id).expect("owned splits are in the view");
            let mut record = state.progress.clone();
            record.attempts += 1;
            record.owner = None;
            if record.attempts >= self.config.max_attempts {
                record.status = SplitStatus::Quarantined;
                record.epoch += 1;
            }
            record.written_at_ms = records::now_ms();
            let expected = state.progress_rev;
            match self
                .store
                .update(Keyspace::Durable, &key, record.encode(), expected)
                .await
            {
                Ok(CasOutcome::Won(rev)) => {
                    // This tenancy ends by request: no Lost event, no loss
                    // metric. Remove from `owned` before folding the write so
                    // the epoch bump cannot read as a peer's fence.
                    self.owned.remove(id);
                    // A failure mid-revocation leaves an uncommitted tail to replay.
                    self.settle_revocation(id, RevocationOutcome::Forced);
                    self.upsert_progress(id, record, rev)?;
                    self.release_lease_key(id, lease_rev).await?;
                    return Ok(());
                }
                Ok(CasOutcome::Lost) => {}
                Err(e) => return Err(store_error(&format!("failing split {split}"), &e)),
            }
            let reread = match self.reread_record(split, &key, expected).await {
                Err(e) if e.kind == CoordinationErrorKind::Retryable => None,
                reread => reread?,
            };
            if let Some((fresh, rev)) = reread {
                let ours = fresh.owner.as_deref() == Some(self.instance.as_str())
                    && fresh.epoch == owned_epoch
                    && fresh.status == SplitStatus::Runnable;
                self.upsert_progress(id, fresh, rev)?;
                if ours {
                    continue;
                }
            }
            self.drop_owned(id, SplitLossReason::Fenced);
            return Err(CoordinationError::new(
                CoordinationErrorKind::Fenced,
                format!("split {split} is owned by a peer"),
            ));
        }
    }

    /// Graceful hand-back: clear `owner` on the record (so the next claim
    /// consumes no attempt), then drop the lease key (so peers claim
    /// instantly instead of after the TTL).
    ///
    /// `departure` distinguishes an embedder's own release, which retires
    /// this worker once its working set empties, from a revocation's
    /// hand-back, which never leaves the fleet even when it gives up the
    /// last split. Shutdown sends `Depart`.
    async fn release_splits(
        &mut self,
        splits: &[SplitId],
        departure: bool,
    ) -> Result<(), CoordinationError> {
        let mut released = 0u64;
        for split in splits {
            if self.release_one(split).await? == ReleaseOutcome::Released {
                released += 1;
            }
        }
        self.metrics(|m| m.released(released));
        if !departure {
            return Ok(());
        }
        // Releasing the last held split is how a worker leaves the fleet.
        // Stop claiming (or the releaser re-claims its own hand-backs),
        // hand leadership back, drop the presence key. The gate is on
        // what was ASKED, not on what the store acknowledged: a departing
        // worker that keeps claiming strands splits when the process exits.
        if self.owned.is_empty() && !splits.is_empty() {
            self.parting = true;
            self.demote().await?;
            let key = records::worker_key(&self.instance);
            if let Err(e) = self.store.delete(Keyspace::Ephemeral, &key, None).await {
                fatal_only("deleting presence", &e)?;
            }
            self.presence.remove(&self.instance);
        }
        Ok(())
    }
}
