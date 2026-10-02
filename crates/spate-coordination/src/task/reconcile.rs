//! The reconcile backstop and the background record reads that keep the view
//! current.

use super::{Listed, READ_CONCURRENCY, Reads, ReconcileRun, SinceListing, Task};
use crate::error::fatal_only;
use crate::records::{self, SplitStatus};
use crate::store::{CoordinationStore, Entry, Keyspace, StoreError};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use spate_core::coordination::CoordinationError;
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::Instant;

impl<S: CoordinationStore + Clone> Task<S> {
    /// Begin the reconcile backstop: list both keyspaces beside the task
    /// loop, and record the view's revisions now so the result, older than
    /// anything the view learns meanwhile, changes only what it can see.
    pub(super) fn start_reconcile(&mut self) -> Option<ReconcileRun> {
        if self.polled.is_some() {
            return self.start_polled_reconcile();
        }
        let store = self.store.clone();
        let listings = async move {
            let leases = store.list(Keyspace::Ephemeral, "").await;
            let records = store.list(Keyspace::Durable, "").await;
            (leases, records)
        }
        .boxed();
        self.since_listing = Some(SinceListing::default());
        Some(ReconcileRun {
            listings,
            started: Instant::now(),
            leader: self.leader_observed.as_ref().map(|(_, rev)| *rev),
            presence: self.presence.clone(),
            leases: self
                .splits
                .iter()
                .filter_map(|(id, state)| state.lease.as_ref().map(|(_, rev)| (id.clone(), *rev)))
                .collect(),
            assignments: self
                .assignments
                .iter()
                .map(|(instance, (_, rev))| (instance.clone(), *rev))
                .collect(),
        })
    }

    /// A polled watch is itself a listing of what it covers, so on a polled
    /// store only the leader reconciles, and only the split records, which
    /// no watch carries there. The run judges no absence.
    fn start_polled_reconcile(&mut self) -> Option<ReconcileRun> {
        self.leadership?;
        let store = self.store.clone();
        let listings = async move {
            let records = store.list(Keyspace::Durable, records::SPLIT_PREFIX).await;
            (Ok(Vec::new()), records)
        }
        .boxed();
        Some(ReconcileRun {
            listings,
            started: Instant::now(),
            leader: None,
            presence: BTreeMap::new(),
            leases: BTreeMap::new(),
            assignments: BTreeMap::new(),
        })
    }

    /// Apply a reconcile's listings like fresh snapshots: a key the view
    /// believes live but the listing omits is treated as deleted. Only keys
    /// the view has not changed since the listing began are judged. A key
    /// deleted meanwhile is not restored from it, and after a lease-watch
    /// rebuild none of its lease puts apply. Watches whose streams
    /// died silently get re-established by their select arms.
    pub(super) fn finish_reconcile(
        &mut self,
        run: &ReconcileRun,
        (leases, records): (Listed, Listed),
    ) -> Result<(), CoordinationError> {
        let since = self.since_listing.take().unwrap_or_default();
        let leases = match leases {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(error = %e, "reconcile listing failed; next tick retries");
                return fatal_only("listing the ephemeral keyspace", &e);
            }
        };
        let live: BTreeSet<&str> = leases.iter().map(|e| e.key.as_str()).collect();
        if let Some((_, rev)) = &self.leader_observed
            && run.leader == Some(*rev)
            && !live.contains(records::LEADER_KEY)
        {
            self.apply_lease_delete(records::LEADER_KEY, None);
        }
        let gone_workers: Vec<String> = self
            .presence
            .iter()
            .filter(|(i, rev)| {
                run.presence.get(*i) == Some(*rev)
                    && !live.contains(records::worker_key(i).as_str())
            })
            .map(|(i, _)| i.clone())
            .collect();
        for instance in gone_workers {
            self.apply_lease_delete(&records::worker_key(&instance), None);
        }
        let gone_leases: Vec<String> = self
            .splits
            .iter()
            .filter(|(id, s)| {
                s.lease
                    .as_ref()
                    .is_some_and(|(_, rev)| run.leases.get(*id) == Some(rev))
                    && !live.contains(records::split_key_str(id).as_str())
            })
            .map(|(id, _)| records::split_key_str(id))
            .collect();
        for key in gone_leases {
            self.apply_lease_delete(&key, None);
        }
        if !since.leases_rebuilt {
            for entry in leases.iter().filter(|e| !since.leases.contains(&e.key)) {
                self.apply_lease_put(entry)?;
            }
        }
        match records {
            Ok(entries) => {
                // Assignment records are the one durable key this
                // protocol deletes, and watch snapshots drop delete
                // markers. A missed deletion leaves a cached revision
                // whose instance never receives another assignment.
                let live: BTreeSet<&str> = entries.iter().map(|e| e.key.as_str()).collect();
                let gone: Vec<String> = self
                    .assignments
                    .iter()
                    .filter(|(i, (_, rev))| {
                        run.assignments.get(*i) == Some(rev)
                            && !live.contains(records::assign_key(i).as_str())
                    })
                    .map(|(i, _)| i.clone())
                    .collect();
                for instance in gone {
                    self.assignments.remove(&instance);
                    self.assign_dirty = true;
                    if instance == self.instance {
                        // An absent record means "nothing has been decided".
                        self.assignment_seen = false;
                        self.assigned.clear();
                        self.awaiting.clear();
                    }
                }
                for entry in entries.iter().filter(|e| !since.records.contains(&e.key)) {
                    self.apply_state_put(entry)?;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "durable reconcile listing failed; next tick retries");
                fatal_only("listing the durable keyspace", &e)?;
            }
        }
        // A backstop for any input whose change set no flag.
        self.assign_dirty = true;
        self.metrics(|m| m.reconcile(run.started.elapsed()));
        Ok(())
    }

    /// The durable keys a polled store's watch may never deliver and the
    /// view needs: the records of splits this worker was assigned, the
    /// specs of splits in the leader's view, and on a refresh tick the
    /// records of the leader's assigned splits that show no lease. Empty on
    /// a store that pushes changes.
    pub(super) fn wanted_reads(&mut self) -> Vec<String> {
        if self.polled.is_none() {
            return Vec::new();
        }
        let refresh = std::mem::take(&mut self.refresh_due);
        if refresh {
            self.parked_reads.clear();
        }
        let mut wanted = BTreeSet::new();
        for id in &self.assigned {
            match self.splits.get(id) {
                None => {
                    wanted.insert(records::split_key_str(id));
                }
                Some(state) if state.spec.is_some() => continue,
                Some(_) => {}
            }
            if !self.pending_specs.contains_key(id) {
                wanted.insert(records::spec_key_str(id));
            }
        }
        if self.leadership.is_some() {
            // A split the leader has seen without its spec cannot be
            // assigned until the spec is read.
            for (id, state) in &self.splits {
                if state.spec.is_none() {
                    wanted.insert(records::spec_key_str(id));
                }
            }
        }
        if refresh && self.leadership.is_some() {
            for (val, _) in self.assignments.values() {
                for id in &val.splits {
                    let unleased = self.splits.get(id).is_none_or(|state| {
                        state.lease.is_none() && state.progress.status == SplitStatus::Runnable
                    });
                    if unleased {
                        wanted.insert(records::split_key_str(id));
                    }
                }
            }
        }
        wanted
            .into_iter()
            .filter(|key| !self.parked_reads.contains(key))
            .collect()
    }

    pub(super) fn start_reads(&self, keys: Vec<String>) -> Reads {
        let store = self.store.clone();
        async move {
            futures_util::stream::iter(keys)
                .map(|key| {
                    let store = store.clone();
                    async move {
                        let read = store.get(Keyspace::Durable, &key).await;
                        (key, read)
                    }
                })
                .buffer_unordered(READ_CONCURRENCY)
                .collect()
                .await
        }
        .boxed()
    }

    /// Fold a read run's results into the view. A key found absent or not
    /// read waits for the next refresh tick.
    pub(super) fn finish_reads(
        &mut self,
        results: Vec<(String, Result<Option<Entry>, StoreError>)>,
    ) -> Result<(), CoordinationError> {
        for (key, read) in results {
            match read {
                Ok(Some(entry)) => self.apply_state_put(&entry)?,
                Ok(None) => {
                    self.parked_reads.insert(key);
                }
                Err(e) => {
                    tracing::warn!(key = %key, error = %e, "record read failed; next refresh retries");
                    self.parked_reads.insert(key);
                    fatal_only("reading a durable record", &e)?;
                }
            }
        }
        Ok(())
    }

    /// Remember a key deleted from the view while a reconcile listing is in
    /// flight, so the listing does not restore it.
    pub(super) fn note_deleted(&mut self, ks: Keyspace, key: &str) {
        if let Some(since) = &mut self.since_listing {
            match ks {
                Keyspace::Ephemeral => since.leases.insert(key.to_string()),
                Keyspace::Durable => since.records.insert(key.to_string()),
            };
        }
    }

    // ------------------------------------------------------------------
    // The engine step: election → planning → claims → revocations →
    // terminal.

    /// Fold a new leader's listing of split and spec records into the view,
    /// and open planning and publishing.
    pub(super) fn finish_catch_up(&mut self, listed: Listed) -> Result<(), CoordinationError> {
        match listed {
            Ok(entries) => {
                for entry in &entries {
                    self.apply_state_put(entry)?;
                }
                self.caught_up = true;
                self.assign_dirty = true;
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "leader catch-up listing failed; next refresh retries");
                fatal_only("listing records for a new leader", &e)
            }
        }
    }
}
