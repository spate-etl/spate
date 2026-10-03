//! Conditional leader-key cleanup through the public release path.
mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchMode, WatchStream,
};
use spate_coordination::{CoordinationErrorKind, SplitCoordinator, StoreCoordinator};
use spate_core::clock::tokio::Clock;
use spate_core::metrics::CoordinationMetrics;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::{Held, LEASE, PhasedPlanner, TestClock, config, runtime, split_id};

enum Fault {
    Retry,
    Applied,
    Fatal,
    Replace(&'static str),
    BumpLost,
    StaleLost,
    Pending,
}

#[derive(Default)]
struct Script {
    armed: bool,
    faults: VecDeque<Fault>,
    attempts: Vec<Option<Revision>>,
    reads: usize,
    elections: usize,
    stale: Option<Entry>,
}

#[derive(Clone)]
struct Scripted {
    inner: MemoryStore,
    script: Arc<Mutex<Script>>,
}

impl CoordinationStore for Scripted {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }
    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }
    fn op_timeout(&self) -> Option<Duration> {
        self.inner.op_timeout()
    }
    fn attach_metrics(&self, metrics: &CoordinationMetrics) {
        self.inner.attach_metrics(metrics);
    }
    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        if ks == Keyspace::Ephemeral && key == "leader" {
            let mut script = self.script.lock().expect("script");
            if script.armed {
                script.elections += 1;
            }
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
        self.inner.update(ks, key, value, expected).await
    }
    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        if ks == Keyspace::Ephemeral && key == "leader" {
            let mut script = self.script.lock().expect("script");
            if script.armed {
                script.reads += 1;
                if let Some(stale) = &script.stale {
                    return Ok(Some(stale.clone()));
                }
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
        let fault = if ks == Keyspace::Ephemeral && key == "leader" {
            let mut script = self.script.lock().expect("script");
            if script.armed {
                script.attempts.push(expected);
                script.faults.pop_front()
            } else {
                None
            }
        } else {
            None
        };
        match fault {
            Some(Fault::Retry) => return Err(StoreError::Retryable("unapplied delete".into())),
            Some(Fault::Applied) => {
                let _ = self.inner.delete(ks, key, expected).await?;
                return Err(StoreError::Retryable("lost delete reply".into()));
            }
            Some(Fault::Fatal) => return Err(StoreError::Fatal("delete refused".into())),
            Some(Fault::Pending) => return std::future::pending().await,
            Some(Fault::Replace(field)) => {
                let entry = self.inner.get(ks, key).await?.expect("leader");
                let mut value: serde_json::Value =
                    serde_json::from_slice(&entry.value).expect("leader JSON");
                value[field] = serde_json::Value::String("successor".into());
                let outcome = self
                    .inner
                    .update(
                        ks,
                        key,
                        serde_json::to_vec(&value).expect("JSON"),
                        entry.revision,
                    )
                    .await?;
                assert!(matches!(outcome, CasOutcome::Won(_)));
                return Err(StoreError::Retryable("replacement after lost reply".into()));
            }
            Some(fault @ (Fault::BumpLost | Fault::StaleLost)) => {
                let entry = self.inner.get(ks, key).await?.expect("leader");
                if matches!(fault, Fault::StaleLost) {
                    self.script.lock().expect("script").stale = Some(entry.clone());
                }
                let outcome = self
                    .inner
                    .update(ks, key, entry.value, entry.revision)
                    .await?;
                assert!(matches!(outcome, CasOutcome::Won(_)));
                return Ok(CasOutcome::Lost);
            }
            None => {}
        }
        self.inner.delete(ks, key, expected).await
    }
    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }
    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

fn releasing(
    faults: Vec<Fault>,
    stale: bool,
    check: impl FnOnce(
        &Scripted,
        &Entry,
        Result<(), spate_coordination::CoordinationError>,
        &tokio::runtime::Runtime,
        &mut StoreCoordinator<Scripted>,
    ),
) {
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = Scripted {
        inner: MemoryStore::with_clock(LEASE, clock.clone()),
        script: Arc::default(),
    };
    let mut worker = StoreCoordinator::with_clock(
        store.clone(),
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("demote:v1", &["r0"])))
        .expect("start");
    let mut held = Held::default();
    spate_test::wait_until(Duration::from_secs(5), "claiming the split", || {
        held.fold(worker.poll().expect("poll"));
        held.splits.len() == 1
    });
    let before = rt
        .block_on(store.inner.get(Keyspace::Ephemeral, "leader"))
        .expect("get")
        .expect("leader");
    let value: serde_json::Value = serde_json::from_slice(&before.value).expect("JSON");
    assert_eq!(value["owner"], "solo");
    {
        let mut script = store.script.lock().expect("script");
        script.armed = true;
        script.faults = faults.into();
        script.stale = stale.then(|| before.clone());
    }
    let released = worker.release(&[split_id("r0")]);
    check(&store, &before, released, &rt, &mut worker);
    store.script.lock().expect("script").armed = false;
}

fn leader(store: &Scripted, rt: &tokio::runtime::Runtime) -> Option<Entry> {
    rt.block_on(store.inner.get(Keyspace::Ephemeral, "leader"))
        .expect("get")
}

/// An unapplied transient deletion is retried at the original revision.
/// Regression for #912.
#[test]
fn a_leader_key_delete_that_fails_unapplied_is_retried() {
    releasing(
        vec![Fault::Retry],
        false,
        |store, before, released, rt, _| {
            assert!(released.is_ok(), "{released:?}");
            assert!(leader(store, rt).is_none());
            assert_eq!(
                store.script.lock().expect("script").attempts,
                [Some(before.revision); 2]
            );
        },
    );
}

/// A transient deletion failure after read-back is retried at its newer revision.
/// Regression for #912.
#[test]
fn a_retryable_second_leader_delete_is_retried() {
    releasing(
        vec![Fault::BumpLost, Fault::Retry],
        false,
        |store, before, released, rt, _| {
            assert!(released.is_ok(), "{released:?}");
            assert!(leader(store, rt).is_none());
            let script = store.script.lock().expect("script");
            assert_eq!(script.attempts.len(), 3);
            assert_eq!(script.reads, 1);
            assert_eq!(script.attempts[0], Some(before.revision));
            assert!(script.attempts[1] > script.attempts[0]);
            assert_eq!(script.attempts[1], script.attempts[2]);
        },
    );
}

/// An applied deletion with a lost reply is retried conditionally without re-election.
/// Regression for #912.
#[test]
fn an_applied_leader_delete_with_a_lost_reply_is_reconciled() {
    releasing(
        vec![Fault::Applied],
        false,
        |store, before, released, rt, _| {
            assert!(released.is_ok(), "{released:?}");
            assert!(leader(store, rt).is_none());
            assert_eq!(
                store.script.lock().expect("script").attempts,
                [Some(before.revision); 2]
            );
            assert!(
                rt.block_on(store.inner.list(Keyspace::Ephemeral, "worker."))
                    .expect("presence")
                    .is_empty()
            );
        },
    );
}

fn replacement(field: &'static str) {
    releasing(
        vec![Fault::Replace(field)],
        false,
        |store, before, released, rt, _| {
            assert!(released.is_ok(), "{released:?}");
            let mut expected: serde_json::Value =
                serde_json::from_slice(&before.value).expect("JSON");
            expected[field] = serde_json::Value::String("successor".into());
            assert_eq!(
                leader(store, rt).expect("replacement").value,
                serde_json::to_vec(&expected).expect("JSON")
            );
            let script = store.script.lock().expect("script");
            assert_eq!(script.attempts, [Some(before.revision); 2]);
            assert_eq!(script.reads, 1);
        },
    );
}

/// A successor differing only in owner survives demotion read-back.
/// Regression for #912.
#[test]
fn retrying_a_leader_delete_keeps_a_peers_key() {
    replacement("owner");
}

/// A successor differing only in nonce survives demotion read-back.
/// Regression for #912.
#[test]
fn retrying_a_leader_delete_keeps_a_namesakes_key() {
    replacement("nonce");
}

/// Persistent transient deletions stop after three total conditional attempts.
/// Regression for #912.
#[test]
fn persistent_retryable_leader_deletes_stop_at_the_attempt_budget() {
    releasing(
        vec![Fault::Retry, Fault::Retry, Fault::Retry, Fault::Fatal],
        false,
        |store, before, released, rt, _| {
            assert!(released.is_ok(), "{released:?}");
            assert_eq!(leader(store, rt), Some(before.clone()));
            assert_eq!(
                store.script.lock().expect("script").attempts,
                [Some(before.revision); 3]
            );
            assert!(
                rt.block_on(store.inner.list(Keyspace::Ephemeral, "worker."))
                    .expect("presence")
                    .is_empty()
            );
        },
    );
}

/// A fatal deletion after a transient failure stops the coordination task.
/// Regression for #912.
#[test]
fn a_fatal_leader_delete_after_a_retry_stops_the_task() {
    releasing(
        vec![Fault::Retry, Fault::Fatal],
        false,
        |store, _, released, _, worker| {
            assert!(released.is_ok(), "{released:?}");
            spate_test::wait_until(Duration::from_secs(5), "the fatal demotion error", || {
                match worker.poll() {
                    Err(error) => {
                        assert_eq!(error.kind, CoordinationErrorKind::Fatal);
                        assert!(
                            error
                                .reason
                                .contains("deleting the leader key: delete refused"),
                            "{error}"
                        );
                        true
                    }
                    Ok(_) => false,
                }
            });
            assert_eq!(store.script.lock().expect("script").attempts.len(), 2);
        },
    );
}

/// A pending retry permits release to finish and remove presence within its command wait.
/// Regression for #912.
#[test]
fn demote_cleanup_has_one_operation_timeout_budget() {
    releasing(
        vec![Fault::Retry, Fault::Pending],
        false,
        |store, _, released, rt, _| {
            assert!(released.is_ok(), "{released:?}");
            assert_eq!(store.script.lock().expect("script").attempts.len(), 2);
            assert!(
                rt.block_on(store.inner.list(Keyspace::Ephemeral, "worker."))
                    .expect("presence")
                    .is_empty()
            );
        },
    );
}

/// Demotion shares its read allowance across repeated conditional losses.
/// Regression for #912.
#[test]
fn repeated_losses_share_the_read_and_delete_budgets() {
    releasing(
        vec![Fault::BumpLost, Fault::BumpLost, Fault::BumpLost],
        false,
        |store, _, released, _, _| {
            assert!(released.is_ok(), "{released:?}");
            let script = store.script.lock().expect("script");
            assert_eq!(script.attempts.len(), 3);
            assert!(script.reads <= 3);
            assert!(script.attempts.windows(2).all(|pair| pair[0] < pair[1]));
        },
    );
}

/// Stale reads after a second loss consume the remaining shared read allowance.
/// Regression for #912.
#[test]
fn stale_read_back_exhausts_the_total_read_budget() {
    releasing(
        vec![Fault::BumpLost, Fault::StaleLost],
        false,
        |store, before, released, rt, _| {
            assert!(released.is_ok(), "{released:?}");
            assert!(leader(store, rt).expect("leader").revision > before.revision);
            let script = store.script.lock().expect("script");
            assert_eq!(script.attempts.len(), 2);
            assert_eq!(script.attempts[0], Some(before.revision));
            assert_eq!(script.reads, 3);
        },
    );
}
