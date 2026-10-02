//! Departure: releasing each owned split and deleting the worker's keys before
//! the task stops.

use super::{ReleaseOutcome, Shortfall, Task};
use crate::error::fatal_only;
use crate::records::{self, LeaderVal, LeaseVal, SplitProgressRecord};
use crate::store::{CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError};
use spate_core::coordination::{CoordinationError, SplitId};
use spate_core::metrics::{RevocationOutcome, SplitLossReason};
use std::time::Duration;
use tokio::time::Instant;

/// Pause before a departure retries a write the store refused.
const DEPART_RETRY: Duration = Duration::from_millis(50);

/// Run `op` until it succeeds, fails Fatal, or `deadline` passes, which
/// also cuts off an attempt in flight.
async fn until_deadline<T, F>(deadline: Instant, op: impl Fn() -> F) -> Result<T, StoreError>
where
    F: std::future::Future<Output = Result<T, StoreError>>,
{
    loop {
        let Ok(out) = tokio::time::timeout_at(deadline, op()).await else {
            return Err(StoreError::Retryable(
                "the departure's deadline passed".into(),
            ));
        };
        match out {
            Err(StoreError::Retryable(_)) if Instant::now() + DEPART_RETRY < deadline => {
                tokio::time::sleep(DEPART_RETRY).await;
            }
            out => return out,
        }
    }
}

/// Delete `key` while it is still this worker's: at `rev`, then, each time
/// that loses, at the revision a read finds while `ours` holds for its
/// value. A renewal that applied unseen leaves `rev` behind the store.
async fn delete_own<S: CoordinationStore>(
    store: &S,
    deadline: Instant,
    ks: Keyspace,
    key: &str,
    mut rev: Revision,
    ours: impl Fn(&[u8]) -> bool,
) -> Result<(), StoreError> {
    loop {
        if until_deadline(deadline, || store.delete(ks, key, Some(rev))).await? == CasOutcome::Lost
            && let Some(entry) = read_past(store, deadline, ks, key, rev).await?
            && ours(&entry.value)
        {
            rev = entry.revision;
            continue;
        }
        return Ok(());
    }
}

/// Read `key` after a write at `past` lost its CAS, until the read shows
/// the key gone or at a later revision. A replica may answer a read before
/// it has applied the write that won.
async fn read_past<S: CoordinationStore>(
    store: &S,
    deadline: Instant,
    ks: Keyspace,
    key: &str,
    past: Revision,
) -> Result<Option<Entry>, StoreError> {
    loop {
        match until_deadline(deadline, || store.get(ks, key)).await? {
            Some(entry) if entry.revision <= past => {
                if Instant::now() + DEPART_RETRY >= deadline {
                    return Err(StoreError::Retryable(
                        "reads still lagged the store at the departure's deadline".into(),
                    ));
                }
                tokio::time::sleep(DEPART_RETRY).await;
            }
            read => return Ok(read),
        }
    }
}

impl<S: CoordinationStore + Clone> Task<S> {
    /// Leave the job: hand back every owned split, write a verdict marker
    /// still owed, give up leadership and delete the presence key, retrying
    /// each write the store refuses until `deadline`. Every step runs.
    ///
    /// Presence goes after every owner clear has been tried: the leader
    /// withholds a split whose owner has no presence key.
    pub(super) async fn depart(&mut self, deadline: Instant) -> Shortfall {
        self.parting = true;
        let mut shortfall = Shortfall::default();
        let mut released = 0u64;
        let ids: Vec<String> = self.owned.keys().cloned().collect();
        for id in ids {
            if self.depart_split(&id, deadline, &mut shortfall).await {
                released += 1;
            }
        }
        self.metrics(|m| m.released(released));

        let store = self.store.clone();
        if self.terminal_reported && self.polled.is_some() && !self.verdict_written {
            let val = records::encode_val(&records::VerdictVal {
                schema: records::SCHEMA,
                reporter: self.instance.clone(),
            });
            // `Lost` means a marker exists, from a peer or an earlier attempt.
            match until_deadline(deadline, || {
                store.create(Keyspace::Durable, records::VERDICT_KEY, val.clone())
            })
            .await
            {
                Ok(_) => self.verdict_written = true,
                Err(e) => shortfall.note("writing the verdict marker".into(), &e),
            }
        }
        // A leader-key write that applied unseen can leave the key ours with
        // no leadership revision held; `leader_observed` still shows it.
        let observed_own = self
            .leader_observed
            .as_ref()
            .filter(|(v, _)| v.owner == self.instance && v.nonce == self.nonce)
            .map(|(_, rev)| *rev);
        if let Some(rev) = self.leadership.take().or(observed_own) {
            self.metrics(|m| m.set_leader(false));
            let ours = |value: &[u8]| {
                serde_json::from_slice::<LeaderVal>(value)
                    .is_ok_and(|v| v.owner == self.instance && v.nonce == self.nonce)
            };
            if let Err(e) = delete_own(
                &store,
                deadline,
                Keyspace::Ephemeral,
                records::LEADER_KEY,
                rev,
                ours,
            )
            .await
            {
                shortfall.note("deleting the leader key".into(), &e);
            }
        }
        let key = records::worker_key(&self.instance);
        if let Err(e) =
            until_deadline(deadline, || store.delete(Keyspace::Ephemeral, &key, None)).await
        {
            shortfall.note("deleting the presence key".into(), &e);
        }
        self.presence.remove(&self.instance);
        shortfall
    }

    /// Hand back one owned split during a departure: clear its owner, then
    /// delete its lease. Returns whether the owner was cleared.
    ///
    /// A write whose reply was lost may have applied, so a lost CAS reads
    /// the record back. Cleared at this tenancy's epoch, it is done; still
    /// naming this worker at that epoch, the clear goes again at the read
    /// revision. Any other record belongs to a later tenancy.
    async fn depart_split(
        &mut self,
        id: &str,
        deadline: Instant,
        shortfall: &mut Shortfall,
    ) -> bool {
        let (Some(owned), Some(state)) = (self.owned.get(id), self.splits.get(id)) else {
            return false;
        };
        let lease_rev = owned.lease_rev;
        let mut expected = state.progress_rev;
        let mut record = state.progress.clone();
        let epoch = record.epoch;
        let key = records::split_key_str(id);
        let store = self.store.clone();

        let cleared = loop {
            if Instant::now() >= deadline {
                shortfall.undone.push(format!("releasing split {id}"));
                break None;
            }
            record.owner = None;
            record.written_at_ms = records::now_ms();
            let value = record.encode();
            match until_deadline(deadline, || {
                store.update(Keyspace::Durable, &key, value.clone(), expected)
            })
            .await
            {
                Ok(CasOutcome::Won(rev)) => break Some((record, rev)),
                Ok(CasOutcome::Lost) => {
                    match read_past(&store, deadline, Keyspace::Durable, &key, expected).await {
                        Ok(Some(entry)) => {
                            match SplitProgressRecord::parse(&key, &entry.value, self.fp) {
                                Ok(fresh) if fresh.epoch == epoch && fresh.owner.is_none() => {
                                    break Some((fresh, entry.revision));
                                }
                                Ok(fresh)
                                    if fresh.epoch == epoch
                                        && fresh.owner.as_deref()
                                            == Some(self.instance.as_str()) =>
                                {
                                    record = fresh;
                                    expected = entry.revision;
                                }
                                Ok(_) => {
                                    self.drop_owned(id, SplitLossReason::Fenced);
                                    return false;
                                }
                                Err(e) => {
                                    shortfall.fatal(e);
                                    break None;
                                }
                            }
                        }
                        Ok(None) => {
                            self.drop_owned(id, SplitLossReason::Fenced);
                            return false;
                        }
                        Err(e) => {
                            shortfall.note(format!("reading split {id}"), &e);
                            break None;
                        }
                    }
                }
                Err(e) => {
                    shortfall.note(format!("releasing split {id}"), &e);
                    break None;
                }
            }
        };
        let released = cleared.is_some();
        if let Some((record, rev)) = cleared
            && let Err(e) = self.upsert_progress(id, record, rev)
        {
            shortfall.fatal(e);
        }
        let mut left_behind = !released;
        // The lease goes even when the owner stays set: peers then take the
        // split over as expired without waiting out the lease.
        self.owned.remove(id);
        self.note_deleted(Keyspace::Ephemeral, &key);
        let ours = |value: &[u8]| {
            serde_json::from_slice::<LeaseVal>(value)
                .is_ok_and(|v| v.owner == self.instance && v.nonce == self.nonce)
        };
        if let Err(e) =
            delete_own(&store, deadline, Keyspace::Ephemeral, &key, lease_rev, ours).await
        {
            shortfall.note(format!("deleting the lease of split {id}"), &e);
            left_behind = true;
        }
        if left_behind && let Ok(split) = SplitId::new(id.to_string()) {
            shortfall.unreleased.push((split, epoch));
        }
        self.settle_revocation(
            id,
            if released {
                RevocationOutcome::Drained
            } else {
                RevocationOutcome::Forced
            },
        );
        released
    }

    /// Release one held split, reporting how the tenancy ended so
    /// the caller can decide whether it still owes the source a `Lost`.
    ///
    /// A split under revocation settles here whichever command drove the
    /// release; a shutdown that happens to release a draining split still
    /// ended that revocation. Scoping the count to `revoking` keeps a bulk
    /// hand-back from reading as a fleet of revocations, so no `departure`
    /// flag is needed to tell them apart.
    ///
    /// A write whose reply was lost may have applied, so a lost CAS reads
    /// the record back. Still naming this worker at this tenancy's epoch and
    /// a newer revision, the clear goes again on top of the stored record.
    /// Cleared at that epoch, this worker's own failure report ended the
    /// tenancy. A read at or below the lost revision, or a failed read, is
    /// handled as a failed write; any other record as a fence.
    pub(super) async fn release_one(
        &mut self,
        split: &SplitId,
    ) -> Result<ReleaseOutcome, CoordinationError> {
        let id = split.as_str();
        let Some(owned) = self.owned.get(id) else {
            return Ok(ReleaseOutcome::Missing); // released/lost/completed
        };
        let lease_rev = owned.lease_rev;
        let Some(state) = self.splits.get(id) else {
            return Ok(ReleaseOutcome::Missing);
        };
        let mut record = state.progress.clone();
        let mut expected = state.progress_rev;
        let epoch = record.epoch;
        let key = records::split_key_str(id);
        let unconfirmed = loop {
            record.owner = None;
            record.written_at_ms = records::now_ms();
            match self
                .store
                .update(Keyspace::Durable, &key, record.encode(), expected)
                .await
            {
                Ok(CasOutcome::Won(rev)) => {
                    self.owned.remove(id);
                    self.upsert_progress(id, record, rev)?;
                    self.release_lease_key(id, lease_rev).await?;
                    // The cooperative outcome: the tail is committed and the
                    // owner cleared, so the next owner replays nothing.
                    self.settle_revocation(id, RevocationOutcome::Drained);
                    return Ok(ReleaseOutcome::Released);
                }
                Ok(CasOutcome::Lost) => {}
                Err(e) => {
                    tracing::warn!(split = %id, error = %e, "release write failed; lease will expire");
                    break Some(("releasing a split", e));
                }
            }
            let entry = match self.store.get(Keyspace::Durable, &key).await {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    self.drop_owned(id, SplitLossReason::Fenced);
                    return Ok(ReleaseOutcome::Fenced);
                }
                Err(e) => {
                    tracing::warn!(split = %id, error = %e, "release read-back failed; the owner stays set");
                    break Some(("re-reading a released split", e));
                }
            };
            let fresh = SplitProgressRecord::parse(&key, &entry.value, self.fp)?;
            if fresh.epoch == epoch
                && fresh.owner.as_deref() == Some(self.instance.as_str())
                && entry.revision > expected
            {
                record = fresh;
                expected = entry.revision;
                continue;
            }
            if fresh.epoch == epoch && fresh.owner.is_none() {
                // This worker's failure report ended the tenancy and counted it.
                self.owned.remove(id);
                self.upsert_progress(id, fresh, entry.revision)?;
                self.release_lease_key(id, lease_rev).await?;
                self.settle_revocation(id, RevocationOutcome::Forced);
                return Ok(ReleaseOutcome::Missing);
            }
            if entry.revision <= expected {
                tracing::warn!(split = %id, "release read-back lagged the store; the owner stays set");
                break None;
            }
            // Fenced: the split is someone else's problem now, which
            // is what a release wanted. `drop_owned` settles it.
            self.drop_owned(id, SplitLossReason::Fenced);
            return Ok(ReleaseOutcome::Fenced);
        };
        // Still drop the lease key best-effort: the attempt
        // accounting is conservative (counts as non-graceful).
        self.owned.remove(id);
        self.release_lease_key(id, lease_rev).await?;
        self.settle_revocation(id, RevocationOutcome::Forced);
        if let Some((doing, e)) = unconfirmed {
            fatal_only(doing, &e)?;
        }
        Ok(ReleaseOutcome::WriteFailed)
    }

    // ------------------------------------------------------------------
    // Terminal detection.
}
