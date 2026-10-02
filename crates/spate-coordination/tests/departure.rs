//! Split commits, failure reports, releases and `StoreCoordinator::depart`
//! against a store that fails part-way: an outage that breaks the watches, a
//! store that stops answering, fatal store errors, and writes whose reply is
//! lost.

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_coordination::{
    CoordinationConfig, CoordinationErrorKind, CoordinationEvent, SplitCoordinator, SplitProgress,
    StoreCoordinator,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::{Held, LEASE, PhasedPlanner, TestClock, config_for, drive, runtime};
use tokio::sync::mpsc;

type Breaker = mpsc::UnboundedSender<Result<WatchEvent, StoreError>>;

/// Reconcile first runs at a point in this interval drawn per coordinator, so it
/// re-reads a split record during a test only if that point falls inside the test.
const NO_RECONCILE: Duration = Duration::from_secs(600);

/// A key and the peer's value that replaces it.
type PeerTake = (Keyspace, String, Vec<u8>);

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
/// watch. Wedged: every call but `watch` pends. Latency: every such call
/// first waits that many milliseconds. Fatal: the next listed primitive on
/// a listed key fails Fatal. Ambiguous: a listed key's next update applies,
/// then fails Retryable. Peer takes: a listed key is replaced by a peer's
/// value just before its next delete. Plan lost: the plan record's update
/// loses its CAS.
#[derive(Clone)]
struct FaultStore {
    inner: MemoryStore,
    down: Arc<AtomicBool>,
    wedged: Arc<AtomicBool>,
    latency_ms: Arc<AtomicU64>,
    fatal: Arc<Mutex<Vec<(Op, Keyspace, String)>>>,
    ambiguous: Arc<Mutex<Vec<(Keyspace, String)>>>,
    peer_takes: Arc<Mutex<Vec<PeerTake>>>,
    update_log: Arc<Mutex<Vec<(Keyspace, String)>>>,
    plan_lost: Arc<AtomicBool>,
    refused_watches: Arc<AtomicU64>,
    breakers: Arc<Mutex<Vec<Breaker>>>,
}

impl FaultStore {
    fn new(lease: Duration) -> FaultStore {
        FaultStore::over(MemoryStore::new(lease))
    }

    /// Like [`new`](Self::new), with lease expiry on `clock`.
    fn with_clock(lease: Duration, clock: Arc<TestClock>) -> FaultStore {
        FaultStore::over(MemoryStore::with_clock(lease, clock))
    }

    fn over(inner: MemoryStore) -> FaultStore {
        FaultStore {
            inner,
            down: Arc::default(),
            wedged: Arc::default(),
            latency_ms: Arc::default(),
            fatal: Arc::default(),
            ambiguous: Arc::default(),
            peer_takes: Arc::default(),
            update_log: Arc::default(),
            plan_lost: Arc::default(),
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

    /// How many updates of `key` have passed the gate.
    fn updates(&self, ks: Keyspace, key: &str) -> usize {
        self.update_log
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, name)| *k == ks && name == key)
            .count()
    }

    /// Split records, out of `keys`, that exist and name an owner.
    fn owned<'a>(&self, rt: &tokio::runtime::Runtime, keys: &[&'a str]) -> Vec<&'a str> {
        keys.iter()
            .copied()
            .filter(|key| {
                rt.block_on(self.inner.get(Keyspace::Durable, key))
                    .expect("read the store")
                    .is_some_and(|entry| {
                        let record: serde_json::Value =
                            serde_json::from_slice(&entry.value).expect("a JSON split record");
                        !record["owner"].is_null()
                    })
            })
            .collect()
    }

    /// The split record at `key`, which must exist.
    fn record(&self, rt: &tokio::runtime::Runtime, key: &str) -> serde_json::Value {
        let entry = rt
            .block_on(self.inner.get(Keyspace::Durable, key))
            .expect("read the store")
            .expect("the split record");
        serde_json::from_slice(&entry.value).expect("a JSON split record")
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
        self.update_log.lock().unwrap().push((ks, key.to_string()));
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
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.gate(Op::Delete, ks, key).await?;
        let take = {
            let mut takes = self.peer_takes.lock().unwrap();
            takes
                .iter()
                .position(|(k, name, _)| *k == ks && name == key)
                .map(|i| takes.remove(i))
        };
        if let Some((_, _, value)) = take {
            let _: CasOutcome = self.inner.delete(ks, key, None).await?;
            let _: CasOutcome = self.inner.create(ks, key, value).await?;
        }
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

/// A started worker over `inner` behind a polled watch, holding every split
/// of `ids`.
fn holding_polled<S: CoordinationStore + Clone>(
    rt: &tokio::runtime::Runtime,
    inner: S,
    ids: &[&str],
) -> StoreCoordinator<support::polled::PolledStore<S>> {
    let store = support::polled::PolledStore::new(inner, LEASE / 10);
    let mut w = StoreCoordinator::new(
        store,
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("departure:v1", ids)))
        .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming every split", |h| {
        h.splits.len() == ids.len()
    });
    w
}

/// Like [`holding_polled`], on `clock`, which must also drive `inner`'s lease
/// expiry.
fn holding_polled_clocked<S: CoordinationStore + Clone>(
    rt: &tokio::runtime::Runtime,
    inner: S,
    clock: &Arc<TestClock>,
    ids: &[&str],
) -> StoreCoordinator<support::polled::PolledStore<S>> {
    let store = support::polled::PolledStore::new(inner, LEASE / 10);
    let mut w = StoreCoordinator::with_clock(
        store,
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("departure:v1", ids)))
        .unwrap();
    let mut held = Held::default();
    support::drive_clocked(&mut w, clock, &mut held, "claiming every split", |h| {
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

/// A departure whose task already stopped on a store error hands its split
/// back by direct writes.
#[test]
fn a_departure_after_its_task_stopped_releases_directly() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["r0"]);

    // A lost generation bump whose plan re-read fails Fatal stops the task.
    store.plan_lost.store(true, Ordering::SeqCst);
    store
        .fatal
        .lock()
        .unwrap()
        .push((Op::Get, Keyspace::Durable, "plan".to_string()));
    let _: CasOutcome = rt
        .block_on(store.inner.delete(Keyspace::Ephemeral, "leader", None))
        .unwrap();
    let deadline = Instant::now() + support::DEADLINE;
    while !store.fatal.lock().unwrap().is_empty() {
        assert!(Instant::now() < deadline, "the worker never re-elected");
        std::thread::sleep(support::POLL_INTERVAL);
    }
    store.latency_ms.store(10, Ordering::SeqCst);
    let result = a.depart(&[]);
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.reason.contains("re-reading the plan record")),
        "{result:?}"
    );

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

/// A store slow enough that a departure of several splits takes most of
/// the budget still lets it hand everything back: the task writes until
/// seven eighths of `op_timeout`.
#[test]
fn a_departure_over_a_slow_store_hands_back_everything() {
    // A 30s lease scales `op_timeout` to 4s, so the task stops at 3.5s.
    // Sixteen writes at 200ms take 3.2s.
    let lease = Duration::from_secs(30);
    let rt = runtime();
    let store = FaultStore::new(lease);
    let ids = ["s0", "s1", "s2", "s3", "s4", "s5", "s6"];
    let mut a = holding(&rt, &store, config_for(lease, Some("worker-a")), &ids);

    store.latency_ms.store(200, Ordering::SeqCst);
    let result = a.depart(&[]);
    store.latency_ms.store(0, Ordering::SeqCst);

    assert!(result.is_ok(), "{result:?}");
    let keys = ["leader", "worker.worker-a", "split.s0", "split.s6"];
    let left = store.present(&rt, Keyspace::Ephemeral, &keys);
    assert!(left.is_empty(), "depart left {left:?}");
}

/// A worker whose last commit the store applied but answered Retryable,
/// on a polled store whose view lags that write, still has its owner
/// cleared and its lease deleted by the departure.
#[test]
fn a_departure_after_an_ambiguous_commit_hands_the_split_back() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let store = support::polled::PolledStore::new(fault.clone(), LEASE / 10);
    let mut a = StoreCoordinator::new(
        store,
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["c0"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.c0".to_string()));
    let committed = a.commit(
        &support::split_id("c0"),
        &spate_coordination::SplitProgress::new(7, vec![]),
    );
    assert!(committed.is_err(), "the injected reply loss surfaces");
    let result = a.depart(&[]);

    let owned = fault.owned(&rt, &["split.c0"]);
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    assert!(
        result.is_ok() && owned.is_empty() && lease.is_empty(),
        "depart returned {result:?}; still owned: {owned:?}; lease left: {lease:?}"
    );
}

/// Waits until every armed `ambiguous` fault has fired.
fn wait_ambiguous_drained(store: &FaultStore) {
    let deadline = Instant::now() + support::DEADLINE;
    while !store.ambiguous.lock().unwrap().is_empty() {
        assert!(Instant::now() < deadline, "the write never ran");
        std::thread::sleep(support::POLL_INTERVAL);
    }
}

/// A leadership renewal the store applied but answered Retryable leaves
/// the cached leader revision behind; the departure still deletes the key.
#[test]
fn a_departure_after_an_ambiguous_leader_renewal_deletes_the_leader_key() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["l0"]);

    store
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Ephemeral, "leader".to_string()));
    wait_ambiguous_drained(&store);
    let result = a.depart(&[]);

    let left = store.present(
        &rt,
        Keyspace::Ephemeral,
        &["leader", "worker.worker-a", "split.l0"],
    );
    assert!(left.is_empty(), "depart returned {result:?}; left {left:?}");
}

/// A lease renewal the store applied but answered Retryable leaves the
/// cached lease revision behind; the departure still deletes the lease.
#[test]
fn a_departure_after_an_ambiguous_lease_renewal_deletes_the_lease() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["l0"]);

    store
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Ephemeral, "split.l0".to_string()));
    wait_ambiguous_drained(&store);
    let result = a.depart(&[]);

    let owned = store.owned(&rt, &["split.l0"]);
    let lease = store.present(&rt, Keyspace::Ephemeral, &["split.l0"]);
    assert!(
        owned.is_empty() && lease.is_empty(),
        "depart returned {result:?}; still owned: {owned:?}; lease left: {lease:?}"
    );
}

/// A store that breaks the watches and comes back after the first send's
/// task deadline, inside the handle's budget, still takes the departure.
#[test]
fn a_store_back_after_the_task_deadline_takes_the_departure() {
    // A 15s lease scales `op_timeout` to 2s; the first task deadline is 1.75s.
    let lease = Duration::from_secs(15);
    let rt = runtime();
    let store = FaultStore::new(lease);
    let mut a = holding(
        &rt,
        &store,
        config_for(lease, Some("worker-a")),
        &["q0", "q1"],
    );

    // A networked store's call does not complete on its first poll.
    store.latency_ms.store(5, Ordering::SeqCst);
    store.go_down();
    let deadline = Instant::now() + support::DEADLINE;
    while store.refused_watches.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "the task never re-watched");
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let back = Arc::clone(&store.down);
    let comeback = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1780));
        back.store(false, Ordering::SeqCst);
    });
    let result = a.depart(&[]);
    comeback.join().unwrap();
    store.latency_ms.store(0, Ordering::SeqCst);

    let keys = ["leader", "worker.worker-a", "split.q0", "split.q1"];
    let left = store.present(&rt, Keyspace::Ephemeral, &keys);
    assert!(
        left.is_empty() && result.is_ok(),
        "depart returned {result:?}, leaving {left:?}"
    );
}

/// A gain the handle never polled, whose owner clear the task cannot
/// finish, is still handed back by the direct release.
#[test]
fn an_unpolled_gain_with_a_failed_clear_is_handed_back() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = StoreCoordinator::new(
        store.clone(),
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["u0"])))
        .unwrap();
    // Never polled: the handle holds nothing it could release on its own.
    let deadline = Instant::now() + support::DEADLINE;
    while store.owned(&rt, &["split.u0"]).is_empty() {
        assert!(Instant::now() < deadline, "the split was never claimed");
        std::thread::sleep(support::POLL_INTERVAL);
    }

    store
        .fatal
        .lock()
        .unwrap()
        .push((Op::Update, Keyspace::Durable, "split.u0".to_string()));
    let result = a.depart(&[]);

    let owned = store.owned(&rt, &["split.u0"]);
    assert!(owned.is_empty(), "owned {owned:?} after {result:?}");
}

/// Wraps a store. While `stale` is above zero, a read of the watched key
/// answers with the entry as it stood `depth` applied writes earlier, as a
/// replica that has not applied the latest writes answers a direct get.
#[derive(Clone)]
struct LaggingReads<S> {
    inner: S,
    watched: (Keyspace, String),
    history: Arc<Mutex<Vec<Option<Entry>>>>,
    depth: usize,
    stale: Arc<Mutex<u64>>,
}

impl<S: CoordinationStore + Clone> LaggingReads<S> {
    fn new(inner: S, ks: Keyspace, key: &str, depth: usize) -> Self {
        LaggingReads {
            inner,
            watched: (ks, key.to_string()),
            history: Arc::default(),
            depth,
            stale: Arc::default(),
        }
    }

    fn is_watched(&self, ks: Keyspace, key: &str) -> bool {
        self.watched.0 == ks && self.watched.1 == key
    }

    /// Run `write`, keeping the entry it replaced when it changed the key.
    async fn record(
        &self,
        ks: Keyspace,
        key: &str,
        write: impl std::future::Future<Output = Result<CasOutcome, StoreError>>,
    ) -> Result<CasOutcome, StoreError> {
        if !self.is_watched(ks, key) {
            return write.await;
        }
        let before = self.inner.get(ks, key).await?;
        let out = write.await;
        let after = self.inner.get(ks, key).await?;
        if before.as_ref().map(|e| e.revision) != after.as_ref().map(|e| e.revision) {
            self.history.lock().unwrap().push(before);
        }
        out
    }
}

impl<S: CoordinationStore + Clone> CoordinationStore for LaggingReads<S> {
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
        self.record(ks, key, self.inner.create(ks, key, value))
            .await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.record(ks, key, self.inner.update(ks, key, value, expected))
            .await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        let stale = self.is_watched(ks, key) && {
            let mut left = self.stale.lock().unwrap();
            let stale = *left > 0;
            *left = left.saturating_sub(1);
            stale
        };
        if stale {
            let history = self.history.lock().unwrap();
            if history.len() >= self.depth {
                return Ok(history[history.len() - self.depth].clone());
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
        self.record(ks, key, self.inner.delete(ks, key, expected))
            .await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// A renewal of `key` that applied unseen, then one read of the key that
/// lags it: the departure still deletes the key.
fn renewal_then_lagging_read(key: &'static str) {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let store = LaggingReads::new(fault.clone(), Keyspace::Ephemeral, key, 1);
    let mut a = StoreCoordinator::new(
        store.clone(),
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["l0"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Ephemeral, key.to_string()));
    wait_ambiguous_drained(&fault);
    *store.stale.lock().unwrap() = 1;
    let result = a.depart(&[]);

    let left = fault.present(&rt, Keyspace::Ephemeral, &[key]);
    assert!(
        left.is_empty(),
        "depart returned {result:?} with {left:?} left"
    );
}

/// A lease read back from a replica behind an unseen renewal is read again.
#[test]
fn a_lagging_read_after_an_ambiguous_lease_renewal_is_read_again() {
    renewal_then_lagging_read("split.l0");
}

/// A leader key read back from a replica behind an unseen renewal is read
/// again.
#[test]
fn a_lagging_read_after_an_ambiguous_leader_renewal_is_read_again() {
    renewal_then_lagging_read("leader");
}

/// An ambiguous final commit on a polled store, then one read of the split
/// record from before this tenancy's claim: the departure still clears the
/// owner and deletes the lease.
#[test]
fn a_lagging_read_after_an_ambiguous_commit_is_read_again() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let lagging = LaggingReads::new(fault.clone(), Keyspace::Durable, "split.c0", 2);
    let store = support::polled::PolledStore::new(lagging.clone(), LEASE / 10);
    let mut a = StoreCoordinator::new(
        store,
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["c0"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.c0".to_string()));
    let committed = a.commit(
        &support::split_id("c0"),
        &spate_coordination::SplitProgress::new(7, vec![]),
    );
    assert!(committed.is_err(), "the injected reply loss surfaces");
    *lagging.stale.lock().unwrap() = 1;
    let result = a.depart(&[]);

    let owned = fault.owned(&rt, &["split.c0"]);
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    assert!(
        result.is_ok() && owned.is_empty() && lease.is_empty(),
        "depart returned {result:?}; still owned: {owned:?}; lease left: {lease:?}"
    );
}

/// A departure after an unseen leadership renewal and the heartbeat that
/// follows it deletes the leader key.
#[test]
fn a_departure_after_an_unseen_renewal_and_the_next_beat_deletes_the_leader_key() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["d0"]);

    store
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Ephemeral, "leader".to_string()));
    wait_ambiguous_drained(&store);
    let renewed = store.updates(Keyspace::Ephemeral, "leader");
    let deadline = Instant::now() + support::DEADLINE;
    while store.updates(Keyspace::Ephemeral, "leader") == renewed {
        assert!(Instant::now() < deadline, "the next renewal never ran");
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let result = a.depart(&[]);

    let left = store.present(&rt, Keyspace::Ephemeral, &["leader"]);
    assert!(left.is_empty(), "depart returned {result:?}; left {left:?}");
}

/// A gain whose owner clear lands but whose lease delete fails, polled or
/// not, has its lease removed by the departure.
fn lease_delete_fails(poll: bool) {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = StoreCoordinator::new(
        store.clone(),
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["u0"])))
        .unwrap();
    if poll {
        let mut held = Held::default();
        drive(&mut a, &mut held, "claiming the split", |h| {
            h.splits.len() == 1
        });
    } else {
        let deadline = Instant::now() + support::DEADLINE;
        while store.owned(&rt, &["split.u0"]).is_empty()
            || store
                .present(&rt, Keyspace::Ephemeral, &["split.u0"])
                .is_empty()
        {
            assert!(Instant::now() < deadline, "the split was never claimed");
            std::thread::sleep(support::POLL_INTERVAL);
        }
    }

    store
        .fatal
        .lock()
        .unwrap()
        .push((Op::Delete, Keyspace::Ephemeral, "split.u0".to_string()));
    let result = a.depart(&[]);

    let lease = store.present(&rt, Keyspace::Ephemeral, &["split.u0"]);
    assert!(lease.is_empty(), "depart returned {result:?}; lease left");
}

/// The task's failed lease delete is finished by the direct release for a
/// gain the handle never polled.
#[test]
fn an_unpolled_gain_whose_lease_delete_fails_loses_its_lease() {
    lease_delete_fails(false);
}

/// The task's failed lease delete is finished by the direct release for a
/// gain the handle polled.
#[test]
fn a_polled_gain_whose_lease_delete_fails_loses_its_lease() {
    lease_delete_fails(true);
}

/// A peer that takes the lease between our cached revision and our delete
/// keeps its lease through our departure.
#[test]
fn a_peers_lease_survives_the_departure() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["t0"]);
    let ours = rt
        .block_on(store.inner.get(Keyspace::Ephemeral, "split.t0"))
        .unwrap()
        .expect("our lease");
    let mut peer: serde_json::Value = serde_json::from_slice(&ours.value).unwrap();
    peer["owner"] = "worker-b".into();
    peer["nonce"] = "peer-nonce".into();
    peer["epoch"] = (peer["epoch"].as_u64().unwrap() + 1).into();
    let peer = serde_json::to_vec(&peer).unwrap();

    store.peer_takes.lock().unwrap().push((
        Keyspace::Ephemeral,
        "split.t0".to_string(),
        peer.clone(),
    ));
    let result = a.depart(&[]);

    let left = rt
        .block_on(store.inner.get(Keyspace::Ephemeral, "split.t0"))
        .unwrap();
    assert!(
        left.as_ref().is_some_and(|e| e.value == peer),
        "depart returned {result:?}; the peer's lease is gone"
    );
}

/// A record a same-named later tenancy holds at a higher epoch keeps its
/// owner when the earlier tenancy departs.
#[test]
fn a_later_tenancy_of_the_same_name_keeps_its_owner() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let store = support::polled::PolledStore::new(fault.clone(), LEASE / 10);
    let mut a = StoreCoordinator::new(
        store,
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["j0"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    // A restart under the same id claims it again, unseen by the poller.
    let entry = rt
        .block_on(fault.inner.get(Keyspace::Durable, "split.j0"))
        .unwrap()
        .expect("record");
    let mut record: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    let epoch = record["epoch"].as_u64().unwrap() + 1;
    record["epoch"] = epoch.into();
    let won = rt
        .block_on(fault.inner.update(
            Keyspace::Durable,
            "split.j0",
            serde_json::to_vec(&record).unwrap(),
            entry.revision,
        ))
        .unwrap();
    assert!(matches!(won, CasOutcome::Won(_)));
    let result = a.depart(&[]);

    let after = rt
        .block_on(fault.inner.get(Keyspace::Durable, "split.j0"))
        .unwrap()
        .expect("record");
    let after: serde_json::Value = serde_json::from_slice(&after.value).unwrap();
    assert!(
        after["owner"] == "worker-a" && after["epoch"] == epoch,
        "depart returned {result:?}; record {after}"
    );
}

/// The handle polled a split at one epoch; the task lost it, claimed it
/// again at a higher one and cannot clear it. The direct release uses the
/// task's epoch.
#[test]
fn a_regained_split_is_released_at_the_task_epoch() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["e0"]);
    let record = |store: &FaultStore| -> serde_json::Value {
        let entry = rt
            .block_on(store.inner.get(Keyspace::Durable, "split.e0"))
            .unwrap()
            .expect("record");
        serde_json::from_slice(&entry.value).unwrap()
    };
    let first = record(&store)["epoch"].as_u64().unwrap();

    // The lease vanishes: the task drops the split and claims it again.
    let _: CasOutcome = rt
        .block_on(store.inner.delete(Keyspace::Ephemeral, "split.e0", None))
        .unwrap();
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        let now = record(&store);
        if now["epoch"].as_u64().unwrap() > first
            && now["owner"] == "worker-a"
            && !store
                .present(&rt, Keyspace::Ephemeral, &["split.e0"])
                .is_empty()
        {
            break;
        }
        assert!(Instant::now() < deadline, "never regained: {now}");
        std::thread::sleep(support::POLL_INTERVAL);
    }

    store
        .fatal
        .lock()
        .unwrap()
        .push((Op::Update, Keyspace::Durable, "split.e0".to_string()));
    let result = a.depart(&[]);

    let after = record(&store);
    assert!(
        after["owner"].is_null(),
        "depart returned {result:?}; record {after}"
    );
}

/// A worker that never led leaves the leader's key through its departure.
#[test]
fn a_departure_by_a_follower_keeps_the_leaders_key() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut b = holding(
        &rt,
        &store,
        config_for(LEASE, Some("worker-b")),
        &["f0", "f1"],
    );
    let mut a = StoreCoordinator::new(
        store.clone(),
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final(
        "departure:v1",
        &["f0", "f1"],
    )))
    .unwrap();
    let (mut held_a, mut held_b) = (Held::default(), Held::default());
    support::drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "worker-a joining",
        |a, _| a.splits.len() == 1,
    );
    let result = a.depart(&[]);

    let leader = rt
        .block_on(store.inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap()
        .map(|e| serde_json::from_slice::<serde_json::Value>(&e.value).unwrap());
    assert!(
        leader.as_ref().is_some_and(|v| v["owner"] == "worker-b"),
        "depart returned {result:?}; leader key {leader:?}"
    );
}

/// A lease read that lags an unseen renewal for the whole departure is read
/// again at a pause until the task's deadline, which reports the lease
/// delete undone.
#[test]
fn a_lease_read_that_keeps_lagging_is_reported_at_the_deadline() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let store = LaggingReads::new(fault.clone(), Keyspace::Ephemeral, "split.l0", 1);
    let mut a = StoreCoordinator::new(
        store.clone(),
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["l0"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Ephemeral, "split.l0".to_string()));
    wait_ambiguous_drained(&fault);
    *store.stale.lock().unwrap() = u64::MAX;
    let result = a.depart(&[]);

    // A 200ms `op_timeout` holds four 50ms pauses.
    let reads = u64::MAX - *store.stale.lock().unwrap();
    assert!(
        reads < 20
            && result
                .as_ref()
                .is_err_and(|e| e.to_string().contains("deleting the lease of split l0")),
        "{reads} lagging reads; depart returned {result:?}"
    );
}

/// A release after a commit that applied with its reply lost clears the
/// owner on top of the commit and deletes the lease.
/// Regression for #865.
#[test]
fn a_release_after_an_ambiguous_commit_hands_the_split_back() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let mut a = holding_polled(&rt, fault.clone(), &["c0", "c1"]);

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.c0".to_string()));
    let committed = a.commit(
        &support::split_id("c0"),
        &spate_coordination::SplitProgress::new(7, vec![]),
    );
    assert!(committed.is_err(), "the injected reply loss surfaces");
    let result = a.release(&[support::split_id("c0"), support::split_id("c1")]);

    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    assert!(
        result.is_ok() && record["owner"].is_null() && lease.is_empty(),
        "release returned {result:?}; record {record}; lease left: {lease:?}"
    );
    assert_eq!(record["watermark"], 7, "the committed watermark");
}

/// A release after an ambiguous commit whose read-back keeps answering from
/// before the commit still deletes the lease.
/// Regression for #865.
#[test]
fn a_release_whose_read_back_keeps_lagging_deletes_the_lease() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let lagging = LaggingReads::new(fault.clone(), Keyspace::Durable, "split.c0", 1);
    let mut a = holding_polled(&rt, lagging.clone(), &["c0", "c1"]);

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.c0".to_string()));
    let committed = a.commit(
        &support::split_id("c0"),
        &spate_coordination::SplitProgress::new(7, vec![]),
    );
    assert!(committed.is_err(), "the injected reply loss surfaces");
    *lagging.stale.lock().unwrap() = u64::MAX;
    let result = a.release(&[support::split_id("c0"), support::split_id("c1")]);

    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    assert!(
        result.is_ok() && lease.is_empty(),
        "release returned {result:?}; lease left: {lease:?}"
    );
}

/// A release after a failure report that applied with its reply lost, and
/// did not quarantine the split, deletes the lease.
/// Regression for #865.
#[test]
fn a_release_after_an_ambiguous_failure_report_deletes_the_lease() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let mut a = holding_polled(&rt, fault.clone(), &["r0", "r1"]);

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.r0".to_string()));
    let failed = a.fail(&support::split_id("r0"), "injected");
    assert!(failed.is_err(), "the injected reply loss surfaces");
    let result = a.release(&[support::split_id("r0"), support::split_id("r1")]);

    let record = fault.record(&rt, "split.r0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.r0"]);
    assert!(
        result.is_ok() && lease.is_empty() && record["owner"].is_null(),
        "release returned {result:?}; record {record}; lease left: {lease:?}"
    );
    assert_eq!(record["attempts"], 1, "one failure report");
}

/// A release after this worker's failure report applied with its reply lost
/// counts neither a release nor a fenced loss; one after a peer's unseen
/// quarantine counts a fenced loss.
/// Regression for #865.
#[test]
fn a_release_counts_an_unseen_failure_report_apart_from_a_fence() {
    let handle = spate_core::metrics::install(&spate_core::metrics::MetricsSettings {
        exporter: spate_core::metrics::Exporter::Prometheus,
        ..spate_core::metrics::MetricsSettings::default()
    })
    .expect("install the exporter");
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let labels = spate_core::metrics::ComponentLabels::new("departure", "release-accounting", "s3");
    let mut config = config_for(LEASE, Some("worker-a"));
    config.reconcile_interval = NO_RECONCILE;
    let mut a = StoreCoordinator::new(
        support::polled::PolledStore::new(fault.clone(), LEASE / 10),
        config,
        rt.handle().clone(),
        Some(spate_core::metrics::CoordinationMetrics::new(&labels)),
    )
    .expect("coordinator");
    let ids = ["r0", "q0", "h0"];
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &ids)))
        .unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "claiming every split", |h| {
        h.splits.len() == 3
    });

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.r0".to_string()));
    assert!(a.fail(&support::split_id("r0"), "injected").is_err());

    // A peer quarantines q0, unseen by the poller.
    let entry = rt
        .block_on(fault.inner.get(Keyspace::Durable, "split.q0"))
        .unwrap()
        .expect("record");
    let mut record: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    record["epoch"] = (record["epoch"].as_u64().unwrap() + 1).into();
    record["owner"] = serde_json::Value::Null;
    record["status"] = "quarantined".into();
    let won = rt
        .block_on(fault.inner.update(
            Keyspace::Durable,
            "split.q0",
            serde_json::to_vec(&record).unwrap(),
            entry.revision,
        ))
        .unwrap();
    assert!(matches!(won, CasOutcome::Won(_)));

    a.release(&ids.map(support::split_id)).expect("release");

    let text = handle.render();
    let component = [("component", "release-accounting")];
    let releases = spate_test::metric_sum(&text, "spate_coordination_releases_total", &component);
    let fenced = spate_test::metric_sum(
        &text,
        "spate_coordination_split_losses_total",
        &[("component", "release-accounting"), ("reason", "fenced")],
    );
    assert_eq!(
        (releases, fenced),
        (Some(1.0), Some(1.0)),
        "releases (h0 only) and fenced losses (q0 only)"
    );
}

/// A release after a completing commit that applied with its reply lost
/// leaves the split completed at the committed watermark, with no owner and
/// no lease.
/// Regression for #865.
#[test]
fn a_release_after_an_ambiguous_completing_commit_hands_the_split_back() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let mut a = holding_polled(&rt, fault.clone(), &["k0", "k1"]);

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.k0".to_string()));
    let committed = a.commit(
        &support::split_id("k0"),
        &spate_coordination::SplitProgress::completed(9, vec![]),
    );
    assert!(committed.is_err(), "the injected reply loss surfaces");
    let result = a.release(&[support::split_id("k0"), support::split_id("k1")]);

    let record = fault.record(&rt, "split.k0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.k0"]);
    assert!(
        result.is_ok()
            && record["owner"].is_null()
            && record["status"] == "completed"
            && record["watermark"] == 9
            && lease.is_empty(),
        "release returned {result:?}; record {record}; lease left: {lease:?}"
    );
}

/// A peer that takes the lease between our cached revision and the
/// release's delete keeps its lease.
#[test]
fn a_release_leaves_a_peer_lease_in_place() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(
        &rt,
        &store,
        config_for(LEASE, Some("worker-a")),
        &["t0", "t1"],
    );
    let ours = rt
        .block_on(store.inner.get(Keyspace::Ephemeral, "split.t0"))
        .unwrap()
        .expect("our lease");
    let mut peer: serde_json::Value = serde_json::from_slice(&ours.value).unwrap();
    peer["owner"] = "worker-b".into();
    peer["nonce"] = "peer-nonce".into();
    peer["epoch"] = (peer["epoch"].as_u64().unwrap() + 1).into();
    let peer = serde_json::to_vec(&peer).unwrap();

    store.peer_takes.lock().unwrap().push((
        Keyspace::Ephemeral,
        "split.t0".to_string(),
        peer.clone(),
    ));
    let result = a.release(&[support::split_id("t0"), support::split_id("t1")]);

    let left = rt
        .block_on(store.inner.get(Keyspace::Ephemeral, "split.t0"))
        .unwrap();
    assert!(
        left.as_ref().is_some_and(|e| e.value == peer),
        "release returned {result:?}; the peer's lease is gone"
    );
}

/// A peer that takes the leader key between our cached revision and the
/// departing release's delete keeps the key.
#[test]
fn a_release_keeps_a_peers_leader_key() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["p0"]);
    let peer = serde_json::to_vec(&serde_json::json!({
        "schema": 3, "owner": "worker-b", "nonce": "peer-nonce", "generation": 1
    }))
    .unwrap();
    store.peer_takes.lock().unwrap().push((
        Keyspace::Ephemeral,
        "leader".to_string(),
        peer.clone(),
    ));
    let result = a.release(&[support::split_id("p0")]);

    let left = rt
        .block_on(store.inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap();
    assert!(
        left.as_ref().is_some_and(|e| e.value == peer),
        "release returned {result:?}; the peer's leader key is gone"
    );
}

/// A record a same-named later tenancy holds at a higher epoch keeps its
/// owner when the earlier tenancy releases the split.
#[test]
fn a_release_keeps_a_later_tenancys_owner() {
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let mut a = holding_polled(&rt, fault.clone(), &["j0", "j1"]);

    // A restart under the same id claims it again, unseen by the poller.
    let entry = rt
        .block_on(fault.inner.get(Keyspace::Durable, "split.j0"))
        .unwrap()
        .expect("record");
    let mut record: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    let epoch = record["epoch"].as_u64().unwrap() + 1;
    record["epoch"] = epoch.into();
    let won = rt
        .block_on(fault.inner.update(
            Keyspace::Durable,
            "split.j0",
            serde_json::to_vec(&record).unwrap(),
            entry.revision,
        ))
        .unwrap();
    assert!(matches!(won, CasOutcome::Won(_)));
    let result = a.release(&[support::split_id("j0"), support::split_id("j1")]);

    let after = fault.record(&rt, "split.j0");
    assert!(
        after["owner"] == "worker-a" && after["epoch"] == epoch,
        "release returned {result:?}; record {after}"
    );
}

/// A revoked split whose failure report applied with its reply lost, handed
/// back by `hand_back`. Returns the rendered metrics and whether `Lost` was
/// emitted for it.
fn revoked_after_unseen_failure(
    component: &'static str,
    hand_back: impl FnOnce(
        &mut StoreCoordinator<support::polled::PolledStore<FaultStore>>,
        &spate_coordination::SplitId,
    ),
) -> (String, bool) {
    use spate_core::coordination::CoordinationEvent;
    let handle = spate_core::metrics::install(&spate_core::metrics::MetricsSettings {
        exporter: spate_core::metrics::Exporter::Prometheus,
        ..spate_core::metrics::MetricsSettings::default()
    })
    .expect("install the exporter");
    let rt = runtime();
    let fault = FaultStore::new(LEASE);
    let labels = spate_core::metrics::ComponentLabels::new("departure", component, "s3");
    let ids = ["f0", "f1", "f2", "f3"];
    let mut a_config = config_for(LEASE, Some("worker-a"));
    a_config.reconcile_interval = NO_RECONCILE;
    a_config.drain_deadline = LEASE * 20;
    let mut a = StoreCoordinator::new(
        support::polled::PolledStore::new(fault.clone(), LEASE / 10),
        a_config,
        rt.handle().clone(),
        Some(spate_core::metrics::CoordinationMetrics::new(&labels)),
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &ids)))
        .unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });

    let b_config = config_for(LEASE, Some("worker-b"));
    let mut b = StoreCoordinator::new(
        support::polled::PolledStore::new(fault.clone(), LEASE / 10),
        b_config,
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    b.start(Box::new(PhasedPlanner::one_final("departure:v1", &ids)))
        .unwrap();
    let mut held_b = Held::default();

    let deadline = Instant::now() + support::DEADLINE;
    let asked = loop {
        assert!(Instant::now() < deadline, "no revocation was requested");
        let mut asked = None;
        for event in a.poll().unwrap() {
            if let CoordinationEvent::RevokeRequested { split } = &event {
                asked = Some(split.clone());
            }
            held_a.fold(vec![event]);
        }
        held_b.fold(b.poll().unwrap());
        if let Some(split) = asked {
            break split;
        }
        std::thread::sleep(Duration::from_millis(5));
    };

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, format!("split.{}", asked.as_str())));
    assert!(
        a.fail(&asked, "injected").is_err(),
        "the injected reply loss surfaces"
    );
    hand_back(&mut a, &asked);

    let lost = a
        .poll()
        .unwrap()
        .into_iter()
        .any(|e| matches!(&e, CoordinationEvent::Lost { split } if *split == asked));
    (handle.render(), lost)
}

/// A revocation forced after this worker's failure report applied with its
/// reply lost counts a `revoked` loss and emits `Lost`.
/// Regression for #865.
#[test]
fn a_forced_revocation_after_an_unseen_failure_report_counts_revoked() {
    let (text, lost) = revoked_after_unseen_failure("forced-after-failure", |a, split| {
        a.decline_revoke(split).expect("decline");
    });
    let losses = |reason| {
        spate_test::metric_sum(
            &text,
            "spate_coordination_split_losses_total",
            &[("component", "forced-after-failure"), ("reason", reason)],
        )
    };
    assert!(lost, "no Lost event");
    assert_eq!(
        (losses("revoked"), losses("fenced").unwrap_or(0.0)),
        (Some(1.0), 0.0)
    );
}

/// A drained hand-back after this worker's failure report applied with its
/// reply lost ends the revocation as `forced`.
/// Regression for #865.
#[test]
fn a_hand_back_after_an_unseen_failure_report_ends_the_revocation_forced() {
    let (text, _) = revoked_after_unseen_failure("drained-after-failure", |a, split| {
        a.release_drained(std::slice::from_ref(split))
            .expect("release");
    });
    let outcome = |outcome| {
        spate_test::metric_sum(
            &text,
            "spate_coordination_revocations_total",
            &[("component", "drained-after-failure"), ("outcome", outcome)],
        )
    };
    assert_eq!(
        (outcome("forced"), outcome("drained").unwrap_or(0.0)),
        (Some(1.0), 0.0)
    );
}

/// Sets the next update of the split record `key` to apply and lose its reply.
fn arm_ambiguous(store: &FaultStore, key: &str) {
    store
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, key.to_string()));
}

/// Whether the events queued for `w` include `Lost` for `id`.
fn lost_queued(w: &mut impl SplitCoordinator, id: &str) -> bool {
    w.poll()
        .expect("poll")
        .iter()
        .any(|e| matches!(e, CoordinationEvent::Lost { split } if split.as_str() == id))
}

/// Whether `result` is an error of `kind`.
fn is_kind(
    result: &Result<(), spate_coordination::CoordinationError>,
    kind: CoordinationErrorKind,
) -> bool {
    matches!(result, Err(e) if e.kind == kind)
}

/// A commit after a commit that applied with its reply lost writes on top of
/// it and keeps the split.
#[test]
fn a_commit_after_an_ambiguous_commit_lands() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);

    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    let next = a.commit(&support::split_id("c0"), &SplitProgress::new(8, vec![]));

    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    assert!(
        next.is_ok()
            && record["watermark"] == 8
            && record["owner"] == "worker-a"
            && !lease.is_empty(),
        "commit returned {next:?}; record {record}; lease: {lease:?}"
    );
}

/// A commit whose read-back answers from before the write that won returns
/// Retryable and keeps the split; the next commit lands.
#[test]
fn a_commit_whose_read_back_lags_is_retried() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let lagging = LaggingReads::new(fault.clone(), Keyspace::Durable, "split.c0", 1);
    let mut a = holding_polled_clocked(&rt, lagging.clone(), &clock, &["c0"]);

    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    *lagging.stale.lock().unwrap() = 1;
    let lagged = a.commit(&support::split_id("c0"), &SplitProgress::new(8, vec![]));
    assert!(
        is_kind(&lagged, CoordinationErrorKind::Retryable),
        "{lagged:?}"
    );
    assert_eq!(fault.record(&rt, "split.c0")["watermark"], 7);

    let next = a.commit(&support::split_id("c0"), &SplitProgress::new(8, vec![]));
    let record = fault.record(&rt, "split.c0");
    assert!(
        next.is_ok() && record["watermark"] == 8,
        "commit returned {next:?}; record {record}"
    );
}

/// A commit whose read-back fails Retryable returns Retryable and keeps the
/// split; the next commit lands.
#[test]
fn a_commit_whose_read_back_fails_is_retried() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let tap = support::tap::TapStore::new(fault.clone());
    let mut a = holding_polled_clocked(&rt, tap.clone(), &clock, &["c0"]);

    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    let armed = Arc::new(AtomicBool::new(true));
    let fires = Arc::clone(&armed);
    tap.on_get(move |ks, key| {
        (ks == Keyspace::Durable && key == "split.c0" && fires.swap(false, Ordering::SeqCst))
            .then(|| StoreError::Retryable("injected: read failed".into()))
    });
    let failed = a.commit(&support::split_id("c0"), &SplitProgress::new(8, vec![]));
    assert!(
        is_kind(&failed, CoordinationErrorKind::Retryable) && !armed.load(Ordering::SeqCst),
        "{failed:?}"
    );

    let next = a.commit(&support::split_id("c0"), &SplitProgress::new(8, vec![]));
    let record = fault.record(&rt, "split.c0");
    assert!(
        next.is_ok() && record["watermark"] == 8,
        "commit returned {next:?}; record {record}"
    );
}

/// A commit after this tenancy's completing commit applied with its reply
/// lost returns Fenced, deletes the lease and emits no `Lost`.
#[test]
fn a_commit_after_an_ambiguous_completing_commit_ends_the_tenancy() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);

    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, vec![]),
    );
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    let next = a.commit(&support::split_id("c0"), &SplitProgress::new(8, vec![]));

    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    let lost = lost_queued(&mut a, "c0");
    assert!(
        is_kind(&next, CoordinationErrorKind::Fenced)
            && record["status"] == "completed"
            && record["watermark"] == 7
            && !lost
            && lease.is_empty(),
        "commit returned {next:?}; record {record}; Lost: {lost}; lease: {lease:?}"
    );
}

/// A failure report after a commit that applied with its reply lost ends the
/// tenancy: one attempt charged, the committed watermark kept, the lease gone
/// and no `Lost`.
#[test]
fn a_failure_report_after_an_ambiguous_commit_hands_the_split_back() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["r0"]);

    arm_ambiguous(&fault, "split.r0");
    let first = a.commit(&support::split_id("r0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    let failed = a.fail(&support::split_id("r0"), "injected");

    // The worker may claim the handed-back split again at a later epoch.
    let record = fault.record(&rt, "split.r0");
    let lease_epoch = rt
        .block_on(fault.inner.get(Keyspace::Ephemeral, "split.r0"))
        .expect("read the lease")
        .map(|entry| {
            let lease: serde_json::Value =
                serde_json::from_slice(&entry.value).expect("a JSON lease");
            lease["epoch"].as_u64().expect("a lease epoch")
        });
    let lost = lost_queued(&mut a, "r0");
    let epoch = record["epoch"].as_u64().expect("an epoch");
    assert!(
        failed.is_ok()
            && ((epoch == 1 && record["owner"].is_null()) || epoch >= 2)
            && record["attempts"] == 1
            && record["watermark"] == 7
            && lease_epoch != Some(1)
            && !lost,
        "fail returned {failed:?}; record {record}; lease epoch {lease_epoch:?}; Lost: {lost}"
    );
}

/// A failure report whose read-back, after a lost CAS, answers from before the
/// write that won is reported as fenced and emits `Lost`.
#[test]
fn a_failure_report_whose_read_back_lags_is_fenced() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let lagging = LaggingReads::new(fault.clone(), Keyspace::Durable, "split.c0", 1);
    let mut a = holding_polled_clocked(&rt, lagging.clone(), &clock, &["c0"]);

    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    *lagging.stale.lock().unwrap() = 1;
    let failed = a.fail(&support::split_id("c0"), "injected");

    let lost = lost_queued(&mut a, "c0");
    assert!(
        is_kind(&failed, CoordinationErrorKind::Fenced) && lost,
        "fail returned {failed:?}; Lost: {lost}"
    );
}

/// A failure report whose read-back, after a lost CAS, fails Retryable is
/// reported as fenced and emits `Lost`.
#[test]
fn a_failure_report_whose_read_back_fails_is_fenced() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let tap = support::tap::TapStore::new(fault.clone());
    let mut a = holding_polled_clocked(&rt, tap.clone(), &clock, &["c0"]);

    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    let armed = Arc::new(AtomicBool::new(true));
    let fires = Arc::clone(&armed);
    tap.on_get(move |ks, key| {
        (ks == Keyspace::Durable && key == "split.c0" && fires.swap(false, Ordering::SeqCst))
            .then(|| StoreError::Retryable("injected: read failed".into()))
    });
    let failed = a.fail(&support::split_id("c0"), "injected");

    let lost = lost_queued(&mut a, "c0");
    assert!(
        is_kind(&failed, CoordinationErrorKind::Fenced) && lost && !armed.load(Ordering::SeqCst),
        "fail returned {failed:?}; Lost: {lost}"
    );
}

/// Rewrites the split record `key` in the inner store through `edit`, as another
/// writer's update would.
fn rewrite_record(
    rt: &tokio::runtime::Runtime,
    fault: &FaultStore,
    key: &str,
    edit: impl FnOnce(&mut serde_json::Value),
) {
    let entry = rt
        .block_on(fault.inner.get(Keyspace::Durable, key))
        .expect("read the store")
        .expect("the split record");
    let mut record: serde_json::Value =
        serde_json::from_slice(&entry.value).expect("a JSON split record");
    edit(&mut record);
    let out = rt
        .block_on(fault.inner.update(
            Keyspace::Durable,
            key,
            serde_json::to_vec(&record).expect("encode"),
            entry.revision,
        ))
        .expect("write the store");
    assert!(matches!(out, CasOutcome::Won(_)), "the rewrite lost");
}

/// A failure report that loses its CAS to a peer's record returns Fenced and
/// emits `Lost`.
#[test]
fn a_failure_report_lost_to_a_peer_is_fenced_and_writes_nothing() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["r0"]);
    rewrite_record(&rt, &fault, "split.r0", |r| {
        r["owner"] = "worker-b".into();
        r["epoch"] = 2.into();
    });

    let failed = a.fail(&support::split_id("r0"), "injected");

    let record = fault.record(&rt, "split.r0");
    let lost = lost_queued(&mut a, "r0");
    assert!(
        is_kind(&failed, CoordinationErrorKind::Fenced) && lost,
        "fail returned {failed:?}; record {record}; Lost: {lost}"
    );
}

/// A commit or failure report that loses its CAS to a later tenancy of the same
/// instance id returns Fenced and leaves that tenancy's watermark.
#[test]
fn a_write_lost_to_a_later_tenancy_of_the_same_instance_is_fenced() {
    for fail in [false, true] {
        let rt = runtime();
        let clock = TestClock::frozen();
        let fault = FaultStore::with_clock(LEASE, clock.clone());
        let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["r0"]);
        rewrite_record(&rt, &fault, "split.r0", |r| {
            r["epoch"] = 2.into();
            r["watermark"] = 12.into();
        });

        let result = if fail {
            a.fail(&support::split_id("r0"), "injected")
        } else {
            a.commit(&support::split_id("r0"), &SplitProgress::new(13, vec![]))
        };

        let record = fault.record(&rt, "split.r0");
        assert!(
            is_kind(&result, CoordinationErrorKind::Fenced) && record["watermark"] == 12,
            "fail={fail}: returned {result:?}; record {record}"
        );
    }
}

/// A commit that loses its CAS to a peer's record returns Fenced and emits
/// `Lost`, even when that record holds the committed watermark.
#[test]
fn a_commit_lost_to_a_peer_at_the_same_watermark_is_fenced() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);
    rewrite_record(&rt, &fault, "split.c0", |r| {
        r["owner"] = "worker-b".into();
        r["epoch"] = 2.into();
        r["watermark"] = 7.into();
    });

    let result = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));

    let lost = lost_queued(&mut a, "c0");
    assert!(
        is_kind(&result, CoordinationErrorKind::Fenced) && lost,
        "commit returned {result:?}; Lost: {lost}"
    );
}

/// A failure report after this tenancy's completing commit applied with its
/// reply lost returns Fenced and leaves the completed record unwritten.
#[test]
fn a_failure_report_after_an_ambiguous_completing_commit_leaves_the_record() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["r0"]);
    arm_ambiguous(&fault, "split.r0");
    let first = a.commit(
        &support::split_id("r0"),
        &SplitProgress::completed(7, vec![]),
    );
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let failed = a.fail(&support::split_id("r0"), "injected");

    let record = fault.record(&rt, "split.r0");
    assert!(
        is_kind(&failed, CoordinationErrorKind::Fenced)
            && record["status"] == "completed"
            && record["watermark"] == 7
            && record["attempts"] == 0
            && record["owner"] == "worker-a",
        "fail returned {failed:?}; record {record}"
    );
}

/// A commit with a lower watermark than one a lost reply hid returns Fatal and
/// leaves the stored watermark.
#[test]
fn a_regressing_commit_after_an_ambiguous_commit_is_fatal() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);
    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let regressed = a.commit(&support::split_id("c0"), &SplitProgress::new(5, vec![]));

    let record = fault.record(&rt, "split.c0");
    assert!(
        is_kind(&regressed, CoordinationErrorKind::Fatal) && record["watermark"] == 7,
        "commit returned {regressed:?}; record {record}"
    );
}

/// The same completing commit sent again after its reply was lost is adopted:
/// the caller gets `Ok` and the lease is released.
#[test]
fn a_repeated_completing_commit_after_an_ambiguous_one_is_adopted() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);
    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, vec![]),
    );
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let again = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, vec![]),
    );

    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    assert!(
        again.is_ok()
            && record["status"] == "completed"
            && record["watermark"] == 7
            && lease.is_empty(),
        "commit returned {again:?}; record {record}; lease: {lease:?}"
    );
}

/// A failure report sent again after one that applied with its reply lost
/// returns Fenced and charges one attempt.
#[test]
fn a_failure_report_sent_again_after_an_ambiguous_one_is_fenced() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["r0"]);
    arm_ambiguous(&fault, "split.r0");
    let first = a.fail(&support::split_id("r0"), "injected");
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let second = a.fail(&support::split_id("r0"), "injected");

    let record = fault.record(&rt, "split.r0");
    assert!(
        is_kind(&second, CoordinationErrorKind::Fenced) && record["attempts"] == 1,
        "second {second:?}; record {record}"
    );
}

/// A commit after a failure report that applied with its reply lost returns
/// Fenced and writes nothing.
#[test]
fn a_commit_after_an_ambiguous_failure_report_is_fenced() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["r0"]);
    arm_ambiguous(&fault, "split.r0");
    let first = a.fail(&support::split_id("r0"), "injected");
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let next = a.commit(&support::split_id("r0"), &SplitProgress::new(9, vec![]));

    let record = fault.record(&rt, "split.r0");
    assert!(
        is_kind(&next, CoordinationErrorKind::Fenced) && record["watermark"].is_null(),
        "commit returned {next:?}; record {record}"
    );
}

/// A completing commit after a commit at the same watermark that applied with
/// its reply lost writes the completion.
#[test]
fn a_completing_commit_after_an_ambiguous_commit_at_the_same_watermark_completes() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);
    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let next = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, vec![]),
    );

    let record = fault.record(&rt, "split.c0");
    assert!(
        next.is_ok() && record["status"] == "completed" && record["completed"] == true,
        "commit returned {next:?}; record {record}"
    );
}

/// A read-back that fails Fatal after a lost CAS returns Fatal from a commit
/// and from a failure report.
#[test]
fn a_read_back_that_fails_fatally_is_fatal() {
    for commit in [true, false] {
        let rt = runtime();
        let clock = TestClock::frozen();
        let fault = FaultStore::with_clock(LEASE, clock.clone());
        let tap = support::tap::TapStore::new(fault.clone());
        let mut a = holding_polled_clocked(&rt, tap.clone(), &clock, &["c0"]);
        arm_ambiguous(&fault, "split.c0");
        let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
        assert!(
            is_kind(&first, CoordinationErrorKind::Retryable),
            "{first:?}"
        );
        let armed = Arc::new(AtomicBool::new(true));
        let fires = Arc::clone(&armed);
        tap.on_get(move |ks, key| {
            (ks == Keyspace::Durable && key == "split.c0" && fires.swap(false, Ordering::SeqCst))
                .then(|| StoreError::Fatal("injected: read refused".into()))
        });

        let failed = if commit {
            a.commit(&support::split_id("c0"), &SplitProgress::new(8, vec![]))
        } else {
            a.fail(&support::split_id("c0"), "injected")
        };

        assert!(
            is_kind(&failed, CoordinationErrorKind::Fatal) && !armed.load(Ordering::SeqCst),
            "commit={commit}: {failed:?}"
        );
    }
}
