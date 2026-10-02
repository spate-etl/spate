//! Task startup, and the re-establishment of a watch.

use super::Task;
use crate::error::{fatal, store_error};
use crate::records::{self, PlanRecord, WorkerVal};
use crate::store::{CasOutcome, CoordinationStore, Keyspace, StoreError, WatchEvent, WatchStream};
use futures_util::StreamExt as _;
use spate_core::coordination::{CoordinationError, CoordinationErrorKind};
use std::time::Duration;

impl<S: CoordinationStore + Clone> Task<S> {
    pub(super) async fn startup(&mut self) -> Result<(), CoordinationError> {
        self.budgeted("store probe", Self::probe).await?;
        self.budgeted("joining the job", Self::join_job).await?;
        self.budgeted("announcing presence", Self::announce).await?;
        Ok(())
    }

    /// Startup-budgeted retry: capped exponential backoff, fatal after
    /// the configured attempts. Steady-state operations are NOT budgeted —
    /// they retry on later ticks and escalate through lease expiry. The
    /// backoff sleeps on real time because it paces store I/O.
    async fn budgeted<F>(&mut self, what: &str, op: F) -> Result<(), CoordinationError>
    where
        F: AsyncFn(&mut Self) -> Result<(), CoordinationError>,
    {
        let mut delay = Duration::from_millis(200);
        for attempt in 1..=self.config.startup_max_attempts {
            match op(self).await {
                Ok(()) => return Ok(()),
                Err(e) if e.kind == CoordinationErrorKind::Retryable => {
                    tracing::warn!(attempt, error = %e, "{what} failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
                Err(e) => return Err(e),
            }
        }
        Err(fatal(format!(
            "{what} did not succeed within {} attempts",
            self.config.startup_max_attempts
        )))
    }

    /// Verify the store's conditional semantics in both keyspaces before
    /// trusting fencing to them: create wins, a duplicate create loses, and
    /// an update or delete wins at the current revision and loses at a stale
    /// one.
    async fn probe(&mut self) -> Result<(), CoordinationError> {
        for ks in [Keyspace::Durable, Keyspace::Ephemeral] {
            let key = records::probe_key(&self.instance, &self.nonce);
            let ctx = "store probe";
            let rev = match self
                .store
                .create(ks, &key, b"probe".to_vec())
                .await
                .map_err(|e| store_error(ctx, &e))?
            {
                CasOutcome::Won(rev) => rev,
                CasOutcome::Lost => {
                    // Left by an earlier attempt that failed mid-probe.
                    let _ = self
                        .store
                        .delete(ks, &key, None)
                        .await
                        .map_err(|e| store_error(ctx, &e))?;
                    return Err(crate::error::retryable(
                        "probe key left by an earlier attempt; cleared, retrying",
                    ));
                }
            };
            if self
                .store
                .create(ks, &key, b"dup".to_vec())
                .await
                .map_err(|e| store_error(ctx, &e))?
                != CasOutcome::Lost
            {
                return Err(fatal(
                    "store accepted a duplicate create: create-if-absent is not enforced; \
                     this store cannot host coordination",
                ));
            }
            let rev2 = match self
                .store
                .update(ks, &key, b"update".to_vec(), rev)
                .await
                .map_err(|e| store_error(ctx, &e))?
            {
                CasOutcome::Won(rev2) => rev2,
                CasOutcome::Lost => {
                    return Err(fatal(
                        "store rejected a matched-revision update: CAS is broken; this \
                         store cannot host coordination",
                    ));
                }
            };
            if self
                .store
                .update(ks, &key, b"stale".to_vec(), rev)
                .await
                .map_err(|e| store_error(ctx, &e))?
                != CasOutcome::Lost
            {
                return Err(fatal(
                    "store accepted a stale-revision update: compare-and-swap is not \
                     enforced; fencing would corrupt silently; this store cannot host \
                     coordination",
                ));
            }
            if self
                .store
                .delete(ks, &key, Some(rev))
                .await
                .map_err(|e| store_error(ctx, &e))?
                != CasOutcome::Lost
            {
                return Err(fatal(
                    "store accepted a stale-revision delete: guarded delete is not \
                     enforced; this store cannot host coordination",
                ));
            }
            // The rejected delete must have left the key at `rev2`. Checked
            // with a CAS because a store may serve a `get` from a lagging
            // replica.
            let rev3 = match self
                .store
                .update(ks, &key, b"probe".to_vec(), rev2)
                .await
                .map_err(|e| store_error(ctx, &e))?
            {
                CasOutcome::Won(rev3) => rev3,
                CasOutcome::Lost => {
                    return Err(fatal(
                        "store removed a key on a stale-revision delete it reported as \
                         lost: guarded delete is not enforced; this store cannot host \
                         coordination",
                    ));
                }
            };
            if self
                .store
                .delete(ks, &key, Some(rev3))
                .await
                .map_err(|e| store_error(ctx, &e))?
                == CasOutcome::Lost
            {
                return Err(fatal(
                    "store rejected a matched-revision delete: guarded delete is broken; \
                     this store cannot host coordination",
                ));
            }
        }
        Ok(())
    }

    /// Read or create the plan record; the fingerprint check rejects a
    /// divergently-configured worker before it can touch anything.
    async fn join_job(&mut self) -> Result<(), CoordinationError> {
        let ctx = "reading the plan record";
        if let Some(entry) = self
            .store
            .get(Keyspace::Durable, records::PLAN_KEY)
            .await
            .map_err(|e| store_error(ctx, &e))?
        {
            let plan = PlanRecord::parse(&entry.value, &self.fingerprint)?;
            self.plan_rev_seen = entry.revision.0;
            self.plan = Some((plan, entry.revision));
            return Ok(());
        }
        let fresh = PlanRecord::new(self.fingerprint.clone());
        match self
            .store
            .create(Keyspace::Durable, records::PLAN_KEY, fresh.encode())
            .await
            .map_err(|e| store_error("creating the plan record", &e))?
        {
            CasOutcome::Won(rev) => {
                self.plan_rev_seen = rev.0;
                self.plan = Some((fresh, rev));
                Ok(())
            }
            CasOutcome::Lost => Err(crate::error::retryable(
                "lost the plan-creation race; re-reading",
            )),
        }
    }

    /// This worker's presence value. It advertises the lane budget so the
    /// leader balances against each member's own `max_in_flight` rather
    /// than assuming the fleet is homogeneous.
    pub(super) fn worker_val(&self) -> WorkerVal {
        WorkerVal {
            schema: records::SCHEMA,
            nonce: self.nonce.clone(),
            max_in_flight: self.config.max_in_flight,
        }
    }

    /// Write the worker presence key (taking over a dead predecessor's).
    async fn announce(&mut self) -> Result<(), CoordinationError> {
        let key = records::worker_key(&self.instance);
        let val = records::encode_val(&self.worker_val());
        let ctx = "announcing presence";
        match self
            .store
            .create(Keyspace::Ephemeral, &key, val.clone())
            .await
            .map_err(|e| store_error(ctx, &e))?
        {
            CasOutcome::Won(_) => Ok(()),
            CasOutcome::Lost => {
                // A presence key under our id: a predecessor not yet
                // expired, or a live twin that lease fencing catches.
                let entry = self
                    .store
                    .get(Keyspace::Ephemeral, &key)
                    .await
                    .map_err(|e| store_error(ctx, &e))?;
                match entry {
                    None => Err(crate::error::retryable("presence key vanished; retrying")),
                    Some(entry) => {
                        match self
                            .store
                            .update(Keyspace::Ephemeral, &key, val, entry.revision)
                            .await
                            .map_err(|e| store_error(ctx, &e))?
                        {
                            CasOutcome::Won(_) => Ok(()),
                            CasOutcome::Lost => {
                                Err(crate::error::retryable("presence key contended; retrying"))
                            }
                        }
                    }
                }
            }
        }
    }

    /// (Re-)establish a watch: drain its snapshot into a rebuilt view,
    /// return the live tail. Unbudgeted; retries until the store answers.
    /// While it retries, queued commands are refused as Retryable so the
    /// controller's bounded waits fail fast instead of backing up behind
    /// an unreachable store and wedging the control thread. The retry
    /// sleeps on real time because it paces store I/O.
    pub(super) async fn rewatch(&mut self, ks: Keyspace) -> Result<WatchStream, CoordinationError> {
        loop {
            match self.try_rewatch(ks).await {
                Ok(stream) => return Ok(stream),
                Err(e) if e.kind == CoordinationErrorKind::Retryable => {
                    tracing::warn!(error = %e, "watch establishment failed; retrying");
                    while let Ok(command) = self.commands.try_recv() {
                        let reason = format!(
                            "store unreachable while re-establishing watches: {}",
                            e.reason
                        );
                        command.refuse(CoordinationError::new(
                            CoordinationErrorKind::Retryable,
                            reason,
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_rewatch(&mut self, ks: Keyspace) -> Result<WatchStream, CoordinationError> {
        // On a polled store the durable watch covers only the keys every
        // worker must learn of promptly; split records arrive through reads.
        let prefixes: &[&str] = match (ks, self.polled) {
            (Keyspace::Durable, Some(_)) => &[
                records::ASSIGN_PREFIX,
                records::PLAN_KEY,
                records::VERDICT_KEY,
            ],
            _ => &[""],
        };
        let mut tails = Vec::with_capacity(prefixes.len());
        let mut snapshot = Vec::new();
        for prefix in prefixes {
            let mut stream = self
                .store
                .watch(ks, prefix)
                .await
                .map_err(|e| store_error("establishing watch", &e))?;
            loop {
                match stream.next().await {
                    Some(Ok(WatchEvent::SnapshotDone)) => break,
                    Some(Ok(WatchEvent::Put(entry))) => {
                        self.probe_applied(ks, entry.revision);
                        snapshot.push(entry);
                    }
                    Some(Ok(WatchEvent::Delete { .. })) => {}
                    Some(Err(e)) => return Err(store_error("watch snapshot", &e)),
                    None => {
                        return Err(crate::error::retryable(
                            "watch stream ended during snapshot",
                        ));
                    }
                }
            }
            tails.push(stream);
        }
        let stream = match tails.len() {
            1 => tails.pop().expect("one tail"),
            // A merged stream ends only when every tail has, so a tail that
            // ends reports it as an error, which re-establishes them all.
            _ => futures_util::stream::select_all(tails.into_iter().map(|tail| {
                tail.chain(futures_util::stream::once(async {
                    Err(StoreError::Retryable("a merged watch tail ended".into()))
                }))
                .boxed()
            }))
            .boxed(),
        };
        // The snapshot is authoritative for its keyspace: rebuild.
        match ks {
            Keyspace::Ephemeral => {
                // A listing in flight is older than this snapshot, which
                // carries no deletes: its lease puts could restore a key
                // deleted while the watch was down, and every live key
                // arrives through the new watch.
                if let Some(since) = &mut self.since_listing {
                    since.leases_rebuilt = true;
                }
                self.presence.clear();
                self.member_caps.clear();
                self.assign_dirty = true;
                self.leader_observed = None;
                self.pending_leases.clear();
                for state in self.splits.values_mut() {
                    state.lease = None;
                }
                for entry in snapshot {
                    self.apply_lease_put(&entry)?;
                }
            }
            Keyspace::Durable => {
                for entry in snapshot {
                    self.apply_state_put(&entry)?;
                }
            }
        }
        Ok(stream)
    }
}
