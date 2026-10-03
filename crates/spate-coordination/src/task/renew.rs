//! Heartbeat renewal of presence, leadership and owned split leases.

use super::Task;
use crate::error::{fatal, fatal_only};
use crate::records::{self, LeaderVal, LeaseVal};
use crate::store::{CasOutcome, CoordinationStore, Keyspace};
use spate_core::coordination::CoordinationError;
use spate_core::metrics::{SplitLossReason, WriteOutcome};
use tokio::time::Instant;

impl<S: CoordinationStore + Clone> Task<S> {
    pub(super) async fn heartbeat(&mut self) -> Result<(), CoordinationError> {
        // Presence first: membership must outlive lease hiccups. A parting
        // worker's key stays deleted, or expires if the delete failed.
        if !self.parting {
            self.renew_presence().await?;
        }
        if self.leadership.is_some() {
            self.renew_leadership().await?;
        }
        let ids: Vec<String> = self.owned.keys().cloned().collect();
        for id in ids {
            self.renew_split(&id).await?;
        }
        self.settle_owed_leases().await?;
        // Starvation self-fence: any owned split without a successful
        // write for a full lease is dropped. Reads `clock`, the source
        // that stamps `last_ok_write`, so a test fences only what it
        // advances the clock past, and must step by no more than a
        // renew-interval while the worker is alive, or it expires the
        // lease and the renewal that would have saved it at once.
        let now = self.clock.now();
        let starved: Vec<String> = self
            .owned
            .iter()
            .filter(|(_, o)| now.duration_since(o.last_ok_write) >= self.config.lease_duration)
            .map(|(id, _)| id.clone())
            .collect();
        for id in starved {
            tracing::warn!(split = %id, "self-fencing: no successful lease write for a full lease");
            self.drop_owned(&id, SplitLossReason::Starved);
        }
        Ok(())
    }

    async fn renew_presence(&mut self) -> Result<(), CoordinationError> {
        let key = records::worker_key(&self.instance);
        let val = records::encode_val(&self.worker_val());
        // Retryable failures are tolerated: presence tunes fair-share, and
        // correctness does not depend on it.
        let written = match self.store.get(Keyspace::Ephemeral, &key).await {
            Ok(Some(entry)) => {
                self.store
                    .update(Keyspace::Ephemeral, &key, val, entry.revision)
                    .await
            }
            Ok(None) => self.store.create(Keyspace::Ephemeral, &key, val).await,
            Err(e) => Err(e),
        };
        if let Err(e) = written {
            tracing::debug!(error = %e, "presence renewal failed; next beat retries");
            fatal_only("renewing presence", &e)?;
        }
        Ok(())
    }

    async fn renew_leadership(&mut self) -> Result<(), CoordinationError> {
        let Some(rev) = self.leadership else {
            return Ok(());
        };
        let generation = self.plan.as_ref().map_or(0, |(p, _)| p.generation);
        let val = records::encode_val(&LeaderVal {
            schema: records::SCHEMA,
            owner: self.instance.clone(),
            nonce: self.nonce.clone(),
            generation,
        });
        match self
            .store
            .update(Keyspace::Ephemeral, records::LEADER_KEY, val.clone(), rev)
            .await
        {
            Ok(CasOutcome::Won(new_rev)) => {
                self.leadership = Some(new_rev);
                Ok(())
            }
            Ok(CasOutcome::Lost) => self.reread_leadership(val).await,
            Err(e) => {
                tracing::warn!(error = %e, "leadership renewal failed; next beat retries");
                fatal_only("renewing leadership", &e)
            }
        }
    }

    /// Settles a lost leadership renewal by reading the leader key back.
    /// A key that still carries this worker's owner and nonce is adopted and
    /// renewed with `val` in the same beat; any other key, or none, demotes.
    async fn reread_leadership(&mut self, val: Vec<u8>) -> Result<(), CoordinationError> {
        let entry = match self
            .store
            .get(Keyspace::Ephemeral, records::LEADER_KEY)
            .await
        {
            Ok(entry) => entry,
            Err(e) => {
                tracing::warn!(error = %e, "re-reading the leader key failed; next beat retries");
                return fatal_only("re-reading the leader key", &e);
            }
        };
        let ours = entry.filter(|entry| self.holds_leader_val(&entry.value));
        let Some(entry) = ours else {
            tracing::warn!("leadership renewal fenced; demoting");
            self.leadership = None;
            self.metrics(|m| m.set_leader(false));
            return Ok(());
        };
        self.leadership = Some(entry.revision);
        match self
            .store
            .update(
                Keyspace::Ephemeral,
                records::LEADER_KEY,
                val,
                entry.revision,
            )
            .await
        {
            Ok(CasOutcome::Won(new_rev)) => {
                self.leadership = Some(new_rev);
                Ok(())
            }
            // A read behind the write that won; the next beat reads again.
            Ok(CasOutcome::Lost) => Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "leadership renewal failed; next beat retries");
                fatal_only("renewing leadership", &e)
            }
        }
    }

    async fn renew_split(&mut self, id: &str) -> Result<(), CoordinationError> {
        let Some(owned) = self.owned.get(id) else {
            return Ok(());
        };
        // Cadence gate: skip if we renewed within the last interval. Read
        // from `clock`, the same source that stamps `last_ok_write`.
        // Under a frozen test clock nothing renews until the test
        // advances; `TestClock::advance_stepped` gives a live worker its
        // renewal inside every step.
        if self.clock.now().duration_since(owned.last_ok_write) < self.config.renew_interval() {
            return Ok(());
        }
        let lease_rev = owned.lease_rev;
        let epoch = self
            .splits
            .get(id)
            .map(|s| s.progress.epoch)
            .unwrap_or_default();
        let val = records::encode_val(&LeaseVal {
            schema: records::SCHEMA,
            owner: self.instance.clone(),
            nonce: self.nonce.clone(),
            epoch,
        });
        let key = records::split_key_str(id);
        let started = Instant::now();
        match self
            .store
            .update(Keyspace::Ephemeral, &key, val, lease_rev)
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                if let Some(owned) = self.owned.get_mut(id) {
                    owned.lease_rev = rev;
                    owned.last_ok_write = self.clock.now();
                }
                if let Some(state) = self.splits.get_mut(id)
                    && let Some((_, lease_rev)) = &mut state.lease
                {
                    *lease_rev = rev;
                }
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                self.metrics(|m| m.write(WriteOutcome::Conflict, started.elapsed()));
                match self.store.get(Keyspace::Ephemeral, &key).await {
                    Ok(Some(entry)) => {
                        let lease: LeaseVal = records::parse_val(&key, &entry.value)?;
                        if lease.owner == self.instance && lease.nonce == self.nonce {
                            // Maybe-landed: a previous renewal reported
                            // an error but wrote. Adopt its revision;
                            // dropping costs a delivery attempt.
                            if let Some(owned) = self.owned.get_mut(id) {
                                owned.lease_rev = entry.revision;
                                owned.last_ok_write = self.clock.now();
                            }
                            if let Some(state) = self.splits.get_mut(id) {
                                state.lease = Some((lease, entry.revision));
                            }
                            return Ok(());
                        }
                        if lease.owner == self.instance && lease.nonce != self.nonce {
                            return Err(fatal(format!(
                                "two live workers share instance_id {:?} (foreign nonce on \
                                 our lease for split {id}); instance ids must be unique per \
                                 live worker — use the pod name, not a constant",
                                self.instance
                            )));
                        }
                        // A thief CASed our lease: surrender.
                        self.drop_owned(id, SplitLossReason::Fenced);
                        Ok(())
                    }
                    Ok(None) => {
                        self.drop_owned(id, SplitLossReason::Starved);
                        Ok(())
                    }
                    Err(e) => {
                        // Cannot tell; the next beat decides.
                        fatal_only("re-reading a lease", &e)
                    }
                }
            }
            Err(e) => {
                tracing::warn!(split = %id, error = %e, "lease renewal failed; next beat retries");
                fatal_only("renewing a lease", &e)
            }
        }
    }
}
