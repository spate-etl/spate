//! A generation bump whose plan write applies while its reply is lost.

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_coordination::{SplitCoordinator as _, StoreCoordinator};
use spate_core::clock::tokio::Clock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::{Held, LEASE, PhasedPlanner, TestClock, config, runtime};

/// What lands in place of the next plan write before its reply is lost.
#[derive(Clone, Copy, Debug)]
enum Lands {
    /// The write as sent.
    AsSent,
    /// The write with another nonce under this worker's instance id.
    OtherNonce,
    /// The write with this worker's nonce under another instance id.
    OtherOwner,
    /// The write with no elector, as a build without the field writes it.
    NoElector,
    /// The write as sent, one generation lower.
    EarlierGeneration,
}

/// Wraps `S`; the next plan update lands as armed, then returns Retryable.
#[derive(Clone)]
struct ReplyLost<S> {
    inner: S,
    armed: Arc<Mutex<Option<Lands>>>,
    /// Whether the armed write also fails the next plan read, and the watch
    /// hides plan writes.
    refuse_reread: bool,
    reread_refused: Arc<AtomicBool>,
    refusing: Arc<AtomicBool>,
    /// Leader-key creates, one per election.
    elections: Arc<std::sync::atomic::AtomicUsize>,
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
        if ks == Keyspace::Ephemeral && key == "leader" {
            self.elections.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        let lands = (ks == Keyspace::Durable && key == "plan")
            .then(|| self.armed.lock().unwrap().take())
            .flatten();
        let Some(lands) = lands else {
            return self.inner.update(ks, key, value, expected).await;
        };
        let mut record: serde_json::Value = serde_json::from_slice(&value).unwrap();
        match lands {
            Lands::AsSent => {}
            Lands::OtherNonce => {
                record["elector"]["nonce"] = uuid::Uuid::new_v4().simple().to_string().into();
            }
            Lands::OtherOwner => record["elector"]["owner"] = "peer".into(),
            Lands::NoElector => {
                record.as_object_mut().unwrap().remove("elector");
            }
            Lands::EarlierGeneration => {
                record["generation"] = (record["generation"].as_u64().unwrap() - 1).into();
            }
        }
        let outcome = self
            .inner
            .update(ks, key, serde_json::to_vec(&record).unwrap(), expected)
            .await?;
        assert!(
            matches!(outcome, CasOutcome::Won(_)),
            "the armed write lost"
        );
        self.refusing.store(self.refuse_reread, Ordering::SeqCst);
        Err(StoreError::Retryable(
            "injected: reply lost after the write applied".into(),
        ))
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        if ks == Keyspace::Durable && key == "plan" && self.refusing.swap(false, Ordering::SeqCst) {
            self.reread_refused.store(true, Ordering::SeqCst);
            return Err(StoreError::Retryable("injected: re-read failed".into()));
        }
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
        let inner = self.inner.watch(ks, prefix).await?;
        if !self.refuse_reread {
            return Ok(inner);
        }
        // A watch that showed the bump would move the next election past it.
        Ok(inner
            .filter(|e| {
                std::future::ready(!matches!(e, Ok(WatchEvent::Put(entry)) if entry.key == "plan"))
            })
            .boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// Runs one worker whose first generation bump lands as `lands` with its
/// reply lost, until it holds the job's only split; returns the plan record.
fn elect(lands: Lands) -> serde_json::Value {
    elect_over(lands, |inner| inner)
}

/// [`elect`] over the store `wrap` builds from the memory store.
fn elect_over<S: CoordinationStore + Clone + Send + Sync + 'static>(
    lands: Lands,
    wrap: impl FnOnce(MemoryStore) -> S,
) -> serde_json::Value {
    elect_with(lands, wrap, false)
}

/// [`elect_over`]; with `refuse_reread`, the plan read after the armed write
/// also fails and the watch hides plan writes.
fn elect_with<S: CoordinationStore + Clone + Send + Sync + 'static>(
    lands: Lands,
    wrap: impl FnOnce(MemoryStore) -> S,
    refuse_reread: bool,
) -> serde_json::Value {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(LEASE, clock.clone());
    let store = ReplyLost {
        inner: wrap(inner.clone()),
        armed: Arc::new(Mutex::new(Some(lands))),
        refuse_reread,
        reread_refused: Arc::new(AtomicBool::new(false)),
        refusing: Arc::new(AtomicBool::new(false)),
        elections: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let mut worker = StoreCoordinator::with_clock(
        store.clone(),
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("bump:v1", &["g0"])))
        .unwrap();
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "holding the split", |h| {
        h.splits.len() == 1
    });
    assert!(
        store.armed.lock().unwrap().is_none(),
        "the bump took no fault"
    );
    assert_eq!(
        store.reread_refused.load(Ordering::SeqCst),
        refuse_reread,
        "the re-read fault"
    );
    if matches!(lands, Lands::AsSent) {
        assert_eq!(
            store.elections.load(Ordering::SeqCst),
            1 + usize::from(refuse_reread),
            "elections"
        );
    }
    let entry = rt
        .block_on(inner.get(Keyspace::Durable, "plan"))
        .unwrap()
        .expect("plan record");
    serde_json::from_slice(&entry.value).unwrap()
}

/// A bump that applied unseen is this worker's own and keeps its election.
/// Regression for #895.
#[test]
fn an_unseen_generation_bump_keeps_the_election() {
    let plan = elect(Lands::AsSent);
    assert_eq!(plan["generation"], 1, "{plan}");
    assert_eq!(plan["finality"], "final", "{plan}");
    assert_eq!(plan["elector"]["owner"], "solo", "{plan}");
}

/// On a store whose watch is polled, a bump that applied unseen keeps the
/// election.
#[test]
fn an_unseen_generation_bump_keeps_the_election_on_a_polled_store() {
    let plan = elect_over(Lands::AsSent, |inner| {
        support::polled::PolledStore::new(inner, LEASE / 10)
    });
    assert_eq!(plan["generation"], 1, "{plan}");
    assert_eq!(plan["finality"], "final", "{plan}");
    assert_eq!(plan["elector"]["owner"], "solo", "{plan}");
}

/// A bump at the same generation by an earlier process with this instance id
/// demotes this worker.
#[test]
fn a_predecessor_bump_at_the_same_generation_demotes() {
    let plan = elect(Lands::OtherNonce);
    assert_eq!(plan["generation"], 2, "{plan}");
    assert_eq!(plan["elector"]["owner"], "solo", "{plan}");
}

/// A bump at the same generation by another instance demotes this worker.
#[test]
fn a_peer_bump_at_the_same_generation_demotes() {
    let plan = elect(Lands::OtherOwner);
    assert_eq!(plan["generation"], 2, "{plan}");
    assert_eq!(plan["elector"]["owner"], "solo", "{plan}");
}

/// A bump at the same generation that names no elector demotes this worker.
#[test]
fn a_bump_at_the_same_generation_without_an_elector_demotes() {
    let plan = elect(Lands::NoElector);
    assert_eq!(plan["generation"], 2, "{plan}");
    assert_eq!(plan["elector"]["owner"], "solo", "{plan}");
}

/// A record naming this process at an earlier generation does not stand in
/// for the bump.
#[test]
fn an_own_record_at_an_earlier_generation_is_bumped_past() {
    let plan = elect(Lands::EarlierGeneration);
    assert_eq!(plan["generation"], 1, "{plan}");
    assert_eq!(plan["finality"], "final", "{plan}");
    assert_eq!(plan["elector"]["owner"], "solo", "{plan}");
}

/// A bump that applied unseen, given back after its re-read failed, is kept
/// by this process's next election at the same generation, when no watch
/// event showed it.
#[test]
fn an_unseen_bump_given_back_is_kept_by_the_next_election() {
    let plan = elect_with(Lands::AsSent, |inner| inner, true);
    assert_eq!(plan["generation"], 1, "{plan}");
    assert_eq!(plan["finality"], "final", "{plan}");
    assert_eq!(plan["elector"]["owner"], "solo", "{plan}");
}
