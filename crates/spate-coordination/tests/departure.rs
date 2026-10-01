//! `StoreCoordinator::depart` against a store that fails part-way: an
//! outage that breaks the watches, a store that stops answering, and fatal
//! write errors.

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_coordination::{CoordinationConfig, SplitCoordinator, StoreCoordinator};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::{Held, LEASE, PhasedPlanner, config_for, drive, runtime};
use tokio::sync::mpsc;

type Breaker = mpsc::UnboundedSender<Result<WatchEvent, StoreError>>;

/// Which primitive a [`FaultStore`] fault applies to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Create,
    Update,
    Get,
    Delete,
    List,
}

/// A [`MemoryStore`] a test can take down, wedge, slow, or fail on chosen
/// keys.
///
/// Down: every call fails Retryable; `go_down` also breaks every live
/// watch. Wedged: every call but `watch` pends. Latency: every such call first waits that
/// many milliseconds. Fatal: the next listed primitive on a listed key
/// fails Fatal. Ambiguous: a listed key's next update applies, then fails
/// Retryable. Plan lost: the plan record's update loses its CAS and its read
/// fails Retryable.
#[derive(Clone)]
struct FaultStore {
    inner: MemoryStore,
    down: Arc<AtomicBool>,
    wedged: Arc<AtomicBool>,
    latency_ms: Arc<AtomicU64>,
    fatal: Arc<Mutex<Vec<(Op, Keyspace, String)>>>,
    ambiguous: Arc<Mutex<Vec<(Keyspace, String)>>>,
    plan_lost: Arc<AtomicBool>,
    plan_refused: Arc<AtomicU64>,
    refused_watches: Arc<AtomicU64>,
    breakers: Arc<Mutex<Vec<Breaker>>>,
}

impl FaultStore {
    fn new(lease: Duration) -> FaultStore {
        FaultStore {
            inner: MemoryStore::new(lease),
            down: Arc::default(),
            wedged: Arc::default(),
            latency_ms: Arc::default(),
            fatal: Arc::default(),
            ambiguous: Arc::default(),
            plan_lost: Arc::default(),
            plan_refused: Arc::default(),
            refused_watches: Arc::default(),
            breakers: Arc::default(),
        }
    }

    fn unreachable() -> StoreError {
        StoreError::Retryable("injected: store unreachable".into())
    }

    fn plan_lost(&self, ks: Keyspace, key: &str) -> bool {
        ks == Keyspace::Durable && key == "plan" && self.plan_lost.load(Ordering::SeqCst)
    }

    async fn gate(&self, op: Op, ks: Keyspace, key: &str) -> Result<(), StoreError> {
        if self.wedged.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let latency = self.latency_ms.load(Ordering::SeqCst);
        if latency > 0 {
            tokio::time::sleep(Duration::from_millis(latency)).await;
        }
        if self.down.load(Ordering::SeqCst) {
            return Err(Self::unreachable());
        }
        let mut fatal = self.fatal.lock().unwrap();
        if let Some(i) = fatal
            .iter()
            .position(|(o, k, name)| *o == op && *k == ks && name == key)
        {
            fatal.remove(i);
            return Err(StoreError::Fatal(format!("injected: {key} refused")));
        }
        Ok(())
    }

    fn go_down(&self) {
        self.down.store(true, Ordering::SeqCst);
        for tx in self.breakers.lock().unwrap().drain(..) {
            let _ = tx.send(Err(Self::unreachable()));
        }
    }

    /// Split records, out of `keys`, that still name an owner.
    fn owned<'a>(&self, rt: &tokio::runtime::Runtime, keys: &[&'a str]) -> Vec<&'a str> {
        keys.iter()
            .copied()
            .filter(|key| {
                let entry = rt
                    .block_on(self.inner.get(Keyspace::Durable, key))
                    .expect("read the store")
                    .expect("the split record stays");
                let record: serde_json::Value =
                    serde_json::from_slice(&entry.value).expect("a JSON split record");
                !record["owner"].is_null()
            })
            .collect()
    }

    /// Keys of `ks` that still exist, out of `keys`.
    fn present<'a>(
        &self,
        rt: &tokio::runtime::Runtime,
        ks: Keyspace,
        keys: &[&'a str],
    ) -> Vec<&'a str> {
        keys.iter()
            .copied()
            .filter(|key| {
                rt.block_on(self.inner.get(ks, key))
                    .expect("read the store")
                    .is_some()
            })
            .collect()
    }
}

impl CoordinationStore for FaultStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.gate(Op::Create, ks, key).await?;
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.gate(Op::Update, ks, key).await?;
        if self.plan_lost(ks, key) {
            return Ok(CasOutcome::Lost);
        }
        let outcome = self.inner.update(ks, key, value, expected).await?;
        let mut ambiguous = self.ambiguous.lock().unwrap();
        if let Some(i) = ambiguous
            .iter()
            .position(|(k, name)| *k == ks && name == key)
        {
            ambiguous.remove(i);
            return Err(StoreError::Retryable("injected: reply lost".into()));
        }
        Ok(outcome)
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.gate(Op::Get, ks, key).await?;
        if self.plan_lost(ks, key) {
            self.plan_refused.fetch_add(1, Ordering::SeqCst);
            return Err(Self::unreachable());
        }
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.gate(Op::Delete, ks, key).await?;
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        if self.down.load(Ordering::SeqCst) {
            self.refused_watches.fetch_add(1, Ordering::SeqCst);
            return Err(Self::unreachable());
        }
        let inner = self.inner.watch(ks, prefix).await?;
        let (tx, rx) = mpsc::unbounded_channel();
        self.breakers.lock().unwrap().push(tx);
        let broken = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        });
        Ok(futures_util::stream::select(inner, broken).boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.gate(Op::List, ks, prefix).await?;
        self.inner.list(ks, prefix).await
    }
}

/// A started worker holding every split of `ids`.
fn holding(
    rt: &tokio::runtime::Runtime,
    store: &FaultStore,
    config: CoordinationConfig,
    ids: &[&str],
) -> StoreCoordinator<FaultStore> {
    let mut w = StoreCoordinator::new(store.clone(), config, rt.handle().clone(), None)
        .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("departure:v1", ids)))
        .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming every split", |h| {
        h.splits.len() == ids.len()
    });
    w
}

/// A departure sent while the task re-establishes watches broken by a store
/// outage shorter than `op_timeout` still hands everything back.
#[test]
fn a_departure_during_a_short_store_outage_hands_back_everything() {
    // A 15s lease scales `op_timeout` to 2s.
    let lease = Duration::from_secs(15);
    let rt = runtime();
    let store = FaultStore::new(lease);
    let mut a = holding(
        &rt,
        &store,
        config_for(lease, Some("worker-a")),
        &["p0", "p1"],
    );

    store.go_down();
    let deadline = Instant::now() + support::DEADLINE;
    while store.refused_watches.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "the task never re-watched");
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let back = Arc::clone(&store.down);
    let comeback = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(600));
        back.store(false, Ordering::SeqCst);
    });
    let result = a.depart(&[]);
    comeback.join().unwrap();

    let keys = ["leader", "worker.worker-a", "split.p0", "split.p1"];
    let left = store.present(&rt, Keyspace::Ephemeral, &keys);
    assert!(
        left.is_empty(),
        "depart returned {result:?}, leaving {left:?}"
    );
}

/// A departure whose task is gone hands its held splits back by direct
/// writes.
#[test]
fn a_departure_without_its_task_releases_directly() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["d0"]);

    drop(rt);
    assert!(a.depart(&[]).is_err(), "the task is gone");

    let reader = runtime();
    assert!(
        store
            .present(&reader, Keyspace::Ephemeral, &["split.d0"])
            .is_empty()
    );
    let record = reader
        .block_on(store.inner.get(Keyspace::Durable, "split.d0"))
        .unwrap()
        .expect("the split record stays");
    let record: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
    assert!(
        record["owner"].is_null(),
        "the owner was not cleared: {record}"
    );
}

/// A departure over a store that stops answering returns within one
/// `op_timeout`.
#[test]
fn a_departure_over_a_wedged_store_returns_within_op_timeout() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let config = config_for(LEASE, Some("worker-a"));
    let budget = config.op_timeout;
    let mut a = holding(&rt, &store, config, &["w0", "w1"]);

    store.wedged.store(true, Ordering::SeqCst);
    let started = Instant::now();
    let result = a.depart(&[]);
    let took = started.elapsed();
    assert!(result.is_err(), "nothing could be handed back");
    assert!(
        took < budget * 2,
        "depart took {took:?} against a budget of {budget:?}"
    );
}

/// A fatal error part-way through a departure stops none of the steps after
/// it: the presence key goes even when a split update and the leader delete
/// fail.
#[test]
fn a_departure_runs_every_step_past_a_fatal_error() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(
        &rt,
        &store,
        config_for(LEASE, Some("worker-a")),
        &["f0", "f1"],
    );

    store.fatal.lock().unwrap().extend([
        (Op::Update, Keyspace::Durable, "split.f0".to_string()),
        (Op::Delete, Keyspace::Ephemeral, "leader".to_string()),
    ]);
    assert!(a.depart(&[]).is_err(), "the injected errors are fatal");

    let left = store.present(&rt, Keyspace::Ephemeral, &["worker.worker-a", "split.f1"]);
    assert!(
        left.is_empty(),
        "the departure stopped early, leaving {left:?}"
    );
}

/// Store calls that fail Retryable for a moment, with no watch broken, still
/// let the departure hand everything back.
#[test]
fn a_departure_over_briefly_failing_writes_hands_back_everything() {
    // A 15s lease scales `op_timeout` to 2s.
    let lease = Duration::from_secs(15);
    let rt = runtime();
    let store = FaultStore::new(lease);
    let mut a = holding(
        &rt,
        &store,
        config_for(lease, Some("worker-a")),
        &["p0", "p1"],
    );

    store.down.store(true, Ordering::SeqCst);
    let back = Arc::clone(&store.down);
    let comeback = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        back.store(false, Ordering::SeqCst);
    });
    let result = a.depart(&[]);
    comeback.join().unwrap();

    assert!(result.is_ok(), "{result:?}");
    let keys = ["leader", "worker.worker-a", "split.p0", "split.p1"];
    let left = store.present(&rt, Keyspace::Ephemeral, &keys);
    assert!(left.is_empty(), "depart left {left:?}");
    assert!(store.owned(&rt, &["split.p0", "split.p1"]).is_empty());
}

/// A departure over a store that fails for longer than the departure's
/// budget reports the failure.
#[test]
fn a_departure_the_store_keeps_failing_reports_it() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["k0"]);

    store.down.store(true, Ordering::SeqCst);
    let result = a.depart(&[]);
    assert!(
        result.is_err(),
        "the store never answered, yet depart returned Ok"
    );
}

/// A departure whose task already stopped on a Retryable error hands its
/// split back by direct writes.
#[test]
fn a_departure_after_a_retryable_task_death_releases_directly() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["r0"]);

    // A lost generation bump whose plan re-read fails Retryable stops the
    // task.
    store.plan_lost.store(true, Ordering::SeqCst);
    let _: CasOutcome = rt
        .block_on(store.inner.delete(Keyspace::Ephemeral, "leader", None))
        .unwrap();
    let deadline = Instant::now() + support::DEADLINE;
    while store.plan_refused.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "the worker never re-elected");
        std::thread::sleep(support::POLL_INTERVAL);
    }
    store.latency_ms.store(10, Ordering::SeqCst);
    let result = a.depart(&[]);

    let record = rt
        .block_on(store.inner.get(Keyspace::Durable, "split.r0"))
        .unwrap()
        .expect("the split record stays");
    let record: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
    assert!(
        record["owner"].is_null(),
        "depart returned {result:?}; owner {}",
        record["owner"]
    );
    assert!(
        store
            .present(&rt, Keyspace::Ephemeral, &["split.r0"])
            .is_empty()
    );
}

/// Dropping a coordinator over a store that stops answering returns within
/// one `op_timeout`.
#[test]
fn a_drop_over_a_wedged_store_returns_within_op_timeout() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let config = config_for(LEASE, Some("worker-a"));
    let budget = config.op_timeout;
    let a = holding(&rt, &store, config, &["w0", "w1"]);

    store.wedged.store(true, Ordering::SeqCst);
    let started = Instant::now();
    drop(a);
    let took = started.elapsed();
    assert!(
        took < budget * 2,
        "drop took {took:?} against a budget of {budget:?}"
    );
}

/// A release write that applies but loses its reply still leaves no lease
/// and no owner: the retry loses its CAS, and the record read back shows
/// the owner already cleared.
#[test]
fn an_ambiguous_release_write_leaves_no_lease() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["a0"]);

    store
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.a0".to_string()));
    let result = a.depart(&[]);

    assert!(result.is_ok(), "{result:?}");
    let left = store.present(&rt, Keyspace::Ephemeral, &["split.a0", "worker.worker-a"]);
    assert!(left.is_empty(), "depart left {left:?}");
    assert!(store.owned(&rt, &["split.a0"]).is_empty());
}

/// A departure the task could not finish hands the rest back by direct
/// writes: the task's owner clear fails, and only the handle's direct
/// release, which reads the record first, can clear it afterwards.
#[test]
fn an_incomplete_departure_releases_directly() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["i0"]);

    store
        .fatal
        .lock()
        .unwrap()
        .push((Op::Update, Keyspace::Durable, "split.i0".to_string()));
    assert!(a.depart(&[]).is_err(), "the task could not clear the owner");

    assert!(store.owned(&rt, &["split.i0"]).is_empty());
    assert!(
        store
            .present(&rt, Keyspace::Ephemeral, &["split.i0"])
            .is_empty()
    );
}

/// A store that answers every call in a few tens of milliseconds still lets
/// a departure of several splits hand everything back.
#[test]
fn a_departure_over_a_slow_store_hands_back_everything() {
    // A 15s lease scales `op_timeout` to 2s.
    let lease = Duration::from_secs(15);
    let rt = runtime();
    let store = FaultStore::new(lease);
    let ids = ["s0", "s1", "s2", "s3", "s4", "s5", "s6"];
    let mut a = holding(&rt, &store, config_for(lease, Some("worker-a")), &ids);

    store.latency_ms.store(80, Ordering::SeqCst);
    let result = a.depart(&[]);
    store.latency_ms.store(0, Ordering::SeqCst);

    assert!(result.is_ok(), "{result:?}");
    let keys = ["leader", "worker.worker-a", "split.s0", "split.s6"];
    let left = store.present(&rt, Keyspace::Ephemeral, &keys);
    assert!(left.is_empty(), "depart left {left:?}");
}
