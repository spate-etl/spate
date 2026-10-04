//! An election that adopts its own earlier bump from a plan re-read served
//! by a lagging replica.

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_coordination::{SplitCoordinator as _, StoreCoordinator};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::{Held, PhasedPlanner, config, drive, runtime, store};

#[derive(Default)]
struct Script {
    step: u32,
    /// The plan record as this worker's own unseen bump wrote it.
    own: Option<Entry>,
    /// The revision a racing leader's bump wrote.
    successor: Option<Entry>,
    /// Outcome of the deposed successor's publish, attempted when this
    /// worker first publishes.
    zombie: Option<CasOutcome>,
    /// Whether the write confirming the stale re-read fails, unapplied.
    refuse_confirm: bool,
    confirm_refused: bool,
}

/// Memory store whose watch never shows the plan record, scripted:
/// 1. the first plan update applies and returns Retryable;
/// 2. the first plan get returns Retryable, and meanwhile another leader
///    bumps the record one generation further;
/// 3. the second plan get answers with the worker's own record from step 1
///    (a lagging replica), and with `refuse_confirm` the write confirming it
///    returns Retryable unapplied;
/// 4. on the worker's first publish (finality final), the other, deposed
///    leader publishes at the revision its bump won.
#[derive(Clone)]
struct Scripted {
    inner: MemoryStore,
    s: Arc<Mutex<Script>>,
}

impl CoordinationStore for Scripted {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }
    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }
    async fn create(&self, ks: Keyspace, key: &str, v: Vec<u8>) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, v).await
    }
    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        if ks != Keyspace::Durable || key != "plan" {
            return self.inner.update(ks, key, value, expected).await;
        }
        let rec: serde_json::Value = serde_json::from_slice(&value).unwrap();
        let step = self.s.lock().unwrap().step;
        if step == 3 {
            let mut s = self.s.lock().unwrap();
            let own = s.own.clone().unwrap();
            if s.refuse_confirm && expected == own.revision && value == own.value {
                s.confirm_refused = true;
                return Err(StoreError::Retryable("injected: confirm failed".into()));
            }
        }
        if step == 0 {
            let out = self.inner.update(ks, key, value, expected).await?;
            assert!(matches!(out, CasOutcome::Won(_)));
            let own = self.inner.get(ks, key).await?.unwrap();
            let mut s = self.s.lock().unwrap();
            s.own = Some(own);
            s.step = 1;
            return Err(StoreError::Retryable("injected: reply lost".into()));
        }
        if step >= 3 && rec["finality"] == "final" && self.s.lock().unwrap().zombie.is_none() {
            let succ = self.s.lock().unwrap().successor.clone().unwrap();
            let out = self
                .inner
                .update(ks, key, succ.value.clone(), succ.revision)
                .await?;
            self.s.lock().unwrap().zombie = Some(out);
        }
        self.inner.update(ks, key, value, expected).await
    }
    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        if ks == Keyspace::Durable && key == "plan" {
            let step = self.s.lock().unwrap().step;
            if step == 1 {
                let own = self.s.lock().unwrap().own.clone().unwrap();
                let mut rec: serde_json::Value = serde_json::from_slice(&own.value).unwrap();
                rec["generation"] = (rec["generation"].as_u64().unwrap() + 1).into();
                rec.as_object_mut().unwrap().remove("elector");
                let out = self
                    .inner
                    .update(ks, key, serde_json::to_vec(&rec).unwrap(), own.revision)
                    .await?;
                let CasOutcome::Won(rev) = out else {
                    panic!("successor bump lost")
                };
                let mut s = self.s.lock().unwrap();
                s.successor = Some(Entry {
                    key: key.into(),
                    value: serde_json::to_vec(&rec).unwrap(),
                    revision: rev,
                });
                s.step = 2;
                return Err(StoreError::Retryable("injected: re-read failed".into()));
            }
            if step == 2 {
                let own = self.s.lock().unwrap().own.clone();
                self.s.lock().unwrap().step = 3;
                return Ok(own);
            }
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

/// Runs one worker through the script; returns the deposed leader's publish
/// outcome.
fn zombie_publish(refuse_confirm: bool) -> Option<CasOutcome> {
    let rt = runtime();
    let s = Arc::new(Mutex::new(Script {
        refuse_confirm,
        ..Script::default()
    }));
    let st = Scripted {
        inner: store(),
        s: s.clone(),
    };
    let mut cfg = config(Some("worker-a"));
    cfg.reconcile_interval = Duration::from_secs(600);
    let mut a = StoreCoordinator::new(st, cfg, rt.handle().clone(), None).expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("stale:v1", &["x0"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "the worker publishing", |_| {
        s.lock().unwrap().zombie.is_some()
    });
    let s = s.lock().unwrap();
    assert_eq!(s.confirm_refused, refuse_confirm, "the confirm fault");
    s.zombie
}

/// A deposed leader's publish at the revision its bump won loses once this
/// worker leads, though this worker's re-read was served by a lagging replica.
#[test]
fn a_stale_own_record_on_reread_does_not_fence() {
    assert_eq!(
        zombie_publish(false),
        Some(CasOutcome::Lost),
        "a deposed leader's publish won while this worker led"
    );
}

/// As above, when the write confirming the stale re-read fails retryably.
#[test]
fn a_stale_own_record_whose_confirm_fails_does_not_fence() {
    assert_eq!(
        zombie_publish(true),
        Some(CasOutcome::Lost),
        "a deposed leader's publish won while this worker led"
    );
}
