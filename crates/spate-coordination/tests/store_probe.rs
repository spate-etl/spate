//! The startup store probe rejects a store that does not enforce guarded
//! delete, in either keyspace.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream,
};
use spate_coordination::{CoordinationErrorKind, SplitCoordinator, StoreCoordinator};
use std::time::{Duration, Instant};
use support::{DEADLINE, POLL_INTERVAL, PhasedPlanner, config, runtime};

/// How [`BrokenDeleteStore`] mishandles a delete that carries a revision.
#[derive(Clone, Copy)]
enum Fault {
    /// Deletes whatever revision the key is at.
    IgnoresRevision,
    /// Deletes whatever revision the key is at, then reports `Lost`.
    RemovesButLoses,
    /// Reports `Lost` and leaves the key.
    AlwaysLoses,
}

/// A [`MemoryStore`] whose guarded delete is broken in one keyspace.
#[derive(Clone)]
struct BrokenDeleteStore {
    inner: MemoryStore,
    broken: Keyspace,
    fault: Fault,
}

impl CoordinationStore for BrokenDeleteStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        if ks != self.broken || expected.is_none() {
            return self.inner.delete(ks, key, expected).await;
        }
        match self.fault {
            Fault::IgnoresRevision => self.inner.delete(ks, key, None).await,
            Fault::RemovesButLoses => {
                let _ = self.inner.delete(ks, key, None).await?;
                Ok(CasOutcome::Lost)
            }
            Fault::AlwaysLoses => Ok(CasOutcome::Lost),
        }
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// Start a coordinator over a store broken by `fault` in `broken` and
/// assert startup fails fatally with a reason containing `expected`.
fn assert_probe_rejects(fault: Fault, broken: Keyspace, expected: &str) {
    let rt = runtime();
    let store = BrokenDeleteStore {
        inner: MemoryStore::new(support::LEASE),
        broken,
        fault,
    };
    let mut worker = StoreCoordinator::new(store, config(Some("solo")), rt.handle().clone(), None)
        .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("probe:v1", &["p0"])))
        .expect("start");

    let deadline = Instant::now() + DEADLINE;
    let error = loop {
        assert!(
            Instant::now() < deadline,
            "the probe accepted a store whose guarded delete is broken in {broken:?}"
        );
        if let Err(e) = worker.poll() {
            break e;
        }
        std::thread::sleep(POLL_INTERVAL);
    };
    assert_eq!(error.kind, CoordinationErrorKind::Fatal, "{error}");
    let reason = error.to_string();
    assert!(reason.contains("guarded delete"), "{reason}");
    assert!(reason.contains(expected), "{reason}");
}

/// A delete at a stale revision that wins fails startup. Regression for #642.
#[test]
fn a_stale_delete_that_wins_is_rejected_in_the_durable_keyspace() {
    assert_probe_rejects(
        Fault::IgnoresRevision,
        Keyspace::Durable,
        "accepted a stale-revision delete",
    );
}

/// A delete at a stale revision that wins fails startup. Regression for #642.
#[test]
fn a_stale_delete_that_wins_is_rejected_in_the_ephemeral_keyspace() {
    assert_probe_rejects(
        Fault::IgnoresRevision,
        Keyspace::Ephemeral,
        "accepted a stale-revision delete",
    );
}

/// A delete at a stale revision that reports `Lost` but removes the key
/// fails startup. Regression for #642.
#[test]
fn a_stale_delete_that_removes_the_key_is_rejected_in_the_durable_keyspace() {
    assert_probe_rejects(
        Fault::RemovesButLoses,
        Keyspace::Durable,
        "reported as lost",
    );
}

/// A delete at a stale revision that reports `Lost` but removes the key
/// fails startup. Regression for #642.
#[test]
fn a_stale_delete_that_removes_the_key_is_rejected_in_the_ephemeral_keyspace() {
    assert_probe_rejects(
        Fault::RemovesButLoses,
        Keyspace::Ephemeral,
        "reported as lost",
    );
}

/// A delete at the current revision that loses fails startup. Regression
/// for #642.
#[test]
fn a_matched_delete_that_loses_is_rejected_in_the_durable_keyspace() {
    assert_probe_rejects(
        Fault::AlwaysLoses,
        Keyspace::Durable,
        "rejected a matched-revision delete",
    );
}

/// A delete at the current revision that loses fails startup. Regression
/// for #642.
#[test]
fn a_matched_delete_that_loses_is_rejected_in_the_ephemeral_keyspace() {
    assert_probe_rejects(
        Fault::AlwaysLoses,
        Keyspace::Ephemeral,
        "rejected a matched-revision delete",
    );
}
