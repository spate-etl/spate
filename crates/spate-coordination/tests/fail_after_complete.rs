//! A failure report after this worker's own completing commit whose reply was
//! lost.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchMode, WatchStream,
};
use spate_coordination::{
    CoordinationEvent, LeaseEpoch, SplitCoordinator, SplitProgress, StoreCoordinator,
};
use spate_core::clock::tokio::Clock;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::polled::PolledStore;
use support::{Held, LEASE, PhasedPlanner, TestClock, config, runtime, split_id};

/// Wraps `S`; the next armed update on an armed key applies, then returns
/// Retryable.
#[derive(Clone)]
struct ReplyLost<S> {
    inner: S,
    armed: Arc<Mutex<Vec<(Keyspace, String)>>>,
}

impl<S> ReplyLost<S> {
    fn new(inner: S) -> ReplyLost<S> {
        ReplyLost {
            inner,
            armed: Arc::default(),
        }
    }

    fn arm(&self, ks: Keyspace, key: &str) {
        self.armed.lock().unwrap().push((ks, key.to_string()));
    }

    fn armed(&self) -> bool {
        !self.armed.lock().unwrap().is_empty()
    }

    fn fires(&self, ks: Keyspace, key: &str) -> bool {
        let mut armed = self.armed.lock().unwrap();
        match armed.iter().position(|(k, name)| *k == ks && name == key) {
            Some(i) => {
                armed.remove(i);
                true
            }
            None => false,
        }
    }
}

impl<S: CoordinationStore + Clone> CoordinationStore for ReplyLost<S> {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }

    fn op_timeout(&self) -> Option<Duration> {
        self.inner.op_timeout()
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
        let outcome = self.inner.update(ks, key, value, expected).await?;
        if matches!(outcome, CasOutcome::Won(_)) && self.fires(ks, key) {
            return Err(StoreError::Retryable(
                "injected: reply lost after the write applied".into(),
            ));
        }
        Ok(outcome)
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
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// A failure report after this worker's completing commit landed with its
/// reply lost ends the tenancy as the completion does: the lease is deleted
/// and no `Lost` follows.
#[test]
fn a_failure_report_after_an_unseen_completing_commit_hands_back_the_lease() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(LEASE, clock.clone());
    let lossy = ReplyLost::new(inner.clone());
    let mut worker = StoreCoordinator::with_clock(
        PolledStore::new(lossy.clone(), LEASE / 10),
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("complete:v1", &["r0"])))
        .unwrap();
    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    let epoch = LeaseEpoch(held.splits["r0"].0);
    lossy.arm(Keyspace::Durable, "split.r0");
    let commit = worker.commit(&split_id("r0"), &SplitProgress::completed(5, Vec::new()));
    assert!(
        commit.is_err(),
        "the injected reply loss surfaces: {commit:?}"
    );
    assert!(!lossy.armed(), "the commit took the fault");

    let report = worker.fail(&split_id("r0"), epoch, "poison");
    let entry = rt
        .block_on(inner.get(Keyspace::Durable, "split.r0"))
        .unwrap()
        .expect("split record");
    let record: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    let lease = rt
        .block_on(inner.get(Keyspace::Ephemeral, "split.r0"))
        .unwrap()
        .map(|e| String::from_utf8_lossy(&e.value).into_owned());
    let lost = worker
        .poll()
        .expect("poll")
        .into_iter()
        .any(|e| matches!(e, CoordinationEvent::Lost { ref split } if split.as_str() == "r0"));
    let mut beats = 0;
    support::drive_clocked(&mut worker, &clock, &mut held, "half a lease", |_| {
        beats += 1;
        beats > 6
    });
    let lease_later = rt
        .block_on(inner.get(Keyspace::Ephemeral, "split.r0"))
        .unwrap()
        .map(|e| String::from_utf8_lossy(&e.value).into_owned());
    assert!(
        lease.is_none() && !lost,
        "fail returned {report:?}; Lost queued: {lost}; lease {lease:?}; half a lease later \
         {lease_later:?}; record {record}"
    );
}
