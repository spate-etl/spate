//! Split commits, failure reports, claims, releases and
//! `StoreCoordinator::depart` against a store that fails part-way: an outage
//! that breaks the watches, a store that stops answering, fatal store errors,
//! and writes whose reply is lost.

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_coordination::{
    CoordinationConfig, CoordinationErrorKind, CoordinationEvent, LeaseEpoch, SplitCoordinator,
    SplitProgress, StoreCoordinator,
};
use spate_core::source::StopSignal;
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
/// then fails Retryable; `ambiguous_creates` does the same for a create that
/// wins. Peer takes: a listed key is replaced by a peer's
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
    ambiguous_creates: Arc<Mutex<Vec<(Keyspace, String)>>>,
    peer_takes: Arc<Mutex<Vec<PeerTake>>>,
    update_log: Arc<Mutex<Vec<(Keyspace, String)>>>,
    plan_lost: Arc<AtomicBool>,
    refused_watches: Arc<AtomicU64>,
    breakers: Arc<Mutex<Vec<Breaker>>>,
    hold: Arc<Mutex<Option<Hold>>>,
}

/// One durable update of `key` that sets `stop` on arrival and waits for
/// `release` before it reaches the store.
struct Hold {
    key: String,
    stop: Arc<AtomicBool>,
    release: Arc<tokio::sync::Notify>,
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
            ambiguous_creates: Arc::default(),
            peer_takes: Arc::default(),
            update_log: Arc::default(),
            plan_lost: Arc::default(),
            refused_watches: Arc::default(),
            breakers: Arc::default(),
            hold: Arc::default(),
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

    /// The epoch of the split record at `key`, which must exist.
    fn epoch(&self, rt: &tokio::runtime::Runtime, key: &str) -> LeaseEpoch {
        LeaseEpoch(self.record(rt, key)["epoch"].as_u64().expect("an epoch"))
    }

    /// Hold the next durable update of `key` until the returned notify fires,
    /// setting `stop` once the update arrives.
    fn hold(&self, key: &str, stop: &Arc<AtomicBool>) -> Arc<tokio::sync::Notify> {
        let release = Arc::new(tokio::sync::Notify::new());
        *self.hold.lock().unwrap() = Some(Hold {
            key: key.to_string(),
            stop: Arc::clone(stop),
            release: Arc::clone(&release),
        });
        release
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
        let outcome = self.inner.create(ks, key, value).await?;
        if matches!(outcome, CasOutcome::Won(_)) {
            let mut ambiguous = self.ambiguous_creates.lock().unwrap();
            if let Some(i) = ambiguous
                .iter()
                .position(|(k, name)| *k == ks && name == key)
            {
                ambiguous.remove(i);
                return Err(StoreError::Retryable("injected: reply lost".into()));
            }
        }
        Ok(outcome)
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        let held = {
            let mut hold = self.hold.lock().unwrap();
            if ks == Keyspace::Durable && hold.as_ref().is_some_and(|h| h.key == key) {
                hold.take()
            } else {
                None
            }
        };
        if let Some(hold) = held {
            hold.stop.store(true, Ordering::SeqCst);
            hold.release.notified().await;
        }
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

const FINAL_IDS: [&str; 6] = ["c0", "c1", "c2", "c3", "c4", "c5"];
const FINAL_KEYS: [&str; 6] = [
    "split.c0", "split.c1", "split.c2", "split.c3", "split.c4", "split.c5",
];

/// A final commit over a store that stops answering returns within its one
/// `op_timeout` budget, and nothing past the budget reaches the store.
#[test]
fn commit_final_over_a_wedged_store_sends_nothing_after_its_budget() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let config = config_for(LEASE, Some("worker-a"));
    let budget = config.op_timeout;
    let mut a = holding(&rt, &store, config, &FINAL_IDS);
    let before: Vec<(usize, serde_json::Value)> = FINAL_KEYS
        .iter()
        .map(|key| {
            let watermark = store.record(&rt, key)["watermark"].clone();
            (store.updates(Keyspace::Durable, key), watermark)
        })
        .collect();

    store.wedged.store(true, Ordering::SeqCst);
    let commits: Vec<_> = FINAL_IDS
        .iter()
        .map(|id| (support::split_id(id), SplitProgress::new(5, vec![])))
        .collect();
    let started = Instant::now();
    let results = a.commit_final(&commits);
    let took = started.elapsed();
    assert!(
        took < budget * 2,
        "commit_final took {took:?} against a budget of {budget:?}"
    );
    assert_eq!(results.len(), FINAL_IDS.len());
    let sent = results
        .iter()
        .filter(|r| !matches!(r, Err(e) if e.to_string().contains("nothing was sent")))
        .count();
    assert!(sent <= 1, "{results:?}");

    // Commands are served in order, so once this commit is answered every
    // command sent before it has been served.
    store.wedged.store(false, Ordering::SeqCst);
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        let reply = a.commit(&support::split_id("c0"), &SplitProgress::new(8, vec![]));
        let timed_out = matches!(&reply, Err(e) if e.kind == CoordinationErrorKind::Retryable
            && (e.to_string().contains("timed out") || e.to_string().contains("queue is full")));
        if !timed_out {
            break;
        }
        assert!(Instant::now() < deadline, "the barrier commit never landed");
    }

    for (key, (updates, watermark)) in FINAL_KEYS.iter().zip(&before).skip(1) {
        assert_eq!(store.updates(Keyspace::Durable, key), *updates, "{key}");
        assert_eq!(&store.record(&rt, key)["watermark"], watermark, "{key}");
    }
    assert_eq!(store.record(&rt, "split.c0")["watermark"], 8);
}

/// A final commit over a healthy store lands every split.
#[test]
fn commit_final_over_a_healthy_store_lands_every_split() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &FINAL_IDS);
    let commits: Vec<_> = FINAL_IDS
        .iter()
        .zip(10..)
        .map(|(id, w)| (support::split_id(id), SplitProgress::new(w, vec![])))
        .collect();

    let results = a.commit_final(&commits);

    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(results.len(), FINAL_IDS.len());
    for (key, (_, progress)) in FINAL_KEYS.iter().zip(&commits) {
        assert_eq!(
            store.record(&rt, key)["watermark"],
            progress.watermark,
            "{key}"
        );
    }
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
    let epoch = fault.epoch(&rt, "split.r0");

    fault
        .ambiguous
        .lock()
        .unwrap()
        .push((Keyspace::Durable, "split.r0".to_string()));
    let failed = a.fail(&support::split_id("r0"), epoch, "injected");
    assert!(
        is_kind(&failed, CoordinationErrorKind::Retryable),
        "the injected reply loss surfaces: {failed:?}"
    );
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
    let failed = a.fail(
        &support::split_id("r0"),
        LeaseEpoch(held.splits["r0"].0),
        "injected",
    );
    assert!(
        is_kind(&failed, CoordinationErrorKind::Retryable),
        "{failed:?}"
    );

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
    let failed = a.fail(
        &asked,
        LeaseEpoch(held_a.splits[asked.as_str()].0),
        "injected",
    );
    assert!(
        is_kind(&failed, CoordinationErrorKind::Retryable),
        "the injected reply loss surfaces: {failed:?}"
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

/// A commit at the same watermark with new state, after a commit that applied
/// with its reply lost, returns `Ok` and stores its own state.
/// Regression for #910.
#[test]
fn a_commit_with_new_state_after_an_ambiguous_commit_at_the_same_watermark_lands() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);

    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(
        &support::split_id("c0"),
        &SplitProgress::new(7, b"s1".to_vec()),
    );
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    let next = a.commit(
        &support::split_id("c0"),
        &SplitProgress::new(7, b"s2".to_vec()),
    );

    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    assert!(
        next.is_ok()
            && record["state"] == "czI="
            && record["watermark"] == 7
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
    let epoch = fault.epoch(&rt, "split.r0");

    arm_ambiguous(&fault, "split.r0");
    let first = a.commit(&support::split_id("r0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    let failed = a.fail(&support::split_id("r0"), epoch, "injected");

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
    let epoch = fault.epoch(&rt, "split.c0");

    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(&support::split_id("c0"), &SplitProgress::new(7, vec![]));
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    *lagging.stale.lock().unwrap() = 1;
    let failed = a.fail(&support::split_id("c0"), epoch, "injected");

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
    let epoch = fault.epoch(&rt, "split.c0");

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
    let failed = a.fail(&support::split_id("c0"), epoch, "injected");

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
    let epoch = fault.epoch(&rt, "split.r0");
    rewrite_record(&rt, &fault, "split.r0", |r| {
        r["owner"] = "worker-b".into();
        r["epoch"] = 2.into();
    });

    let updates = fault.updates(Keyspace::Durable, "split.r0");
    let failed = a.fail(&support::split_id("r0"), epoch, "injected");
    // The worker may re-claim the split after the fence, which adds a write.
    let attempted = fault.updates(Keyspace::Durable, "split.r0") - updates;

    let record = fault.record(&rt, "split.r0");
    let lost = lost_queued(&mut a, "r0");
    assert!(
        is_kind(&failed, CoordinationErrorKind::Fenced) && lost && attempted >= 1,
        "fail returned {failed:?}; record {record}; Lost: {lost}; CAS attempts: {attempted}"
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
        let epoch = fault.epoch(&rt, "split.r0");
        rewrite_record(&rt, &fault, "split.r0", |r| {
            r["epoch"] = 2.into();
            r["watermark"] = 12.into();
        });

        let updates = fault.updates(Keyspace::Durable, "split.r0");
        let result = if fail {
            a.fail(&support::split_id("r0"), epoch, "injected")
        } else {
            a.commit(&support::split_id("r0"), &SplitProgress::new(13, vec![]))
        };
        // The worker may re-claim the split after the fence, which adds a write.
        let attempted = fault.updates(Keyspace::Durable, "split.r0") - updates;
        let record = fault.record(&rt, "split.r0");
        assert!(
            is_kind(&result, CoordinationErrorKind::Fenced)
                && record["watermark"] == 12
                && attempted >= 1,
            "fail={fail}: returned {result:?}; record {record}; CAS attempts: {attempted}"
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
    let epoch = fault.epoch(&rt, "split.r0");
    arm_ambiguous(&fault, "split.r0");
    let first = a.commit(
        &support::split_id("r0"),
        &SplitProgress::completed(7, vec![]),
    );
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let updates = fault.updates(Keyspace::Durable, "split.r0");
    let failed = a.fail(&support::split_id("r0"), epoch, "injected");

    let record = fault.record(&rt, "split.r0");
    let attempted = fault.updates(Keyspace::Durable, "split.r0") - updates;
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.r0"]);
    assert!(
        is_kind(&failed, CoordinationErrorKind::Fenced)
            && record["status"] == "completed"
            && record["watermark"] == 7
            && record["attempts"] == 0
            && record["owner"] == "worker-a"
            && attempted == 1
            && lease.is_empty(),
        "fail returned {failed:?}; record {record}; CAS attempts: {attempted}; lease: {lease:?}"
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

/// The same completing commit sent again after its reply was lost is adopted
/// without a write on top: the caller gets `Ok` and the lease is released.
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
    let before = fault.updates(Keyspace::Durable, "split.c0");

    let again = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, vec![]),
    );

    let updates = fault.updates(Keyspace::Durable, "split.c0") - before;
    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    assert!(
        again.is_ok()
            && updates == 1
            && record["status"] == "completed"
            && record["watermark"] == 7
            && lease.is_empty(),
        "commit returned {again:?} after {updates} updates; record {record}; lease: {lease:?}"
    );
}

/// A completing commit with new state, after a completing commit at the same
/// watermark that applied with its reply lost, is written on top: `Ok`, the
/// lease deleted and no `Lost`.
/// Regression for #910.
#[test]
fn a_completing_commit_with_new_state_after_an_ambiguous_one_lands() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);
    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, b"s1".to_vec()),
    );
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let next = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, b"s2".to_vec()),
    );

    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    let lost = lost_queued(&mut a, "c0");
    assert!(
        next.is_ok()
            && record["status"] == "completed"
            && record["state"] == "czI="
            && record["watermark"] == 7
            && lease.is_empty()
            && !lost,
        "commit returned {next:?}; record {record}; Lost: {lost}; lease: {lease:?}"
    );
}

/// A commit that does not complete, at the watermark of this tenancy's
/// completing commit that applied with its reply lost, returns Fenced, keeps
/// the completed record, deletes the lease and emits no `Lost`.
#[test]
fn a_commit_at_the_landed_watermark_after_an_ambiguous_completing_commit_is_fenced() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);
    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, b"s1".to_vec()),
    );
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let next = a.commit(
        &support::split_id("c0"),
        &SplitProgress::new(7, b"s2".to_vec()),
    );

    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    let lost = lost_queued(&mut a, "c0");
    assert!(
        is_kind(&next, CoordinationErrorKind::Fenced)
            && record["status"] == "completed"
            && record["state"] == "czE="
            && lease.is_empty()
            && !lost,
        "commit returned {next:?}; record {record}; Lost: {lost}; lease: {lease:?}"
    );
}

/// A completing commit at a new watermark, after this tenancy's completing
/// commit applied with its reply lost, returns Fenced, keeps the completed
/// record, deletes the lease and emits no `Lost`.
#[test]
fn a_completing_commit_at_a_new_watermark_after_an_ambiguous_one_is_fenced() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);
    arm_ambiguous(&fault, "split.c0");
    let first = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, b"s1".to_vec()),
    );
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );

    let next = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(8, b"s2".to_vec()),
    );

    let record = fault.record(&rt, "split.c0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.c0"]);
    let lost = lost_queued(&mut a, "c0");
    assert!(
        is_kind(&next, CoordinationErrorKind::Fenced)
            && record["watermark"] == 7
            && record["state"] == "czE="
            && lease.is_empty()
            && !lost,
        "commit returned {next:?}; record {record}; Lost: {lost}; lease: {lease:?}"
    );
}

/// A completing commit at the watermark of a peer's unseen completed record
/// returns Fenced, emits `Lost` and leaves the peer's record unchanged.
#[test]
fn a_completing_commit_at_a_peers_completed_watermark_is_fenced() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut a = holding_polled_clocked(&rt, fault.clone(), &clock, &["c0"]);
    let ours = rt
        .block_on(fault.inner.get(Keyspace::Durable, "split.c0"))
        .unwrap()
        .expect("split record");
    let mut peer: serde_json::Value = serde_json::from_slice(&ours.value).unwrap();
    peer["owner"] = "worker-b".into();
    peer["epoch"] = (peer["epoch"].as_u64().unwrap() + 1).into();
    peer["watermark"] = 7.into();
    peer["state"] = "cGVlcg==".into();
    peer["completed"] = true.into();
    peer["status"] = "completed".into();
    let peer = serde_json::to_vec(&peer).unwrap();
    let won = rt
        .block_on(
            fault
                .inner
                .update(Keyspace::Durable, "split.c0", peer.clone(), ours.revision),
        )
        .unwrap();
    assert!(matches!(won, CasOutcome::Won(_)), "{won:?}");

    let next = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(7, b"s2".to_vec()),
    );

    let left = rt
        .block_on(fault.inner.get(Keyspace::Durable, "split.c0"))
        .unwrap()
        .expect("split record");
    let lost = lost_queued(&mut a, "c0");
    assert!(
        is_kind(&next, CoordinationErrorKind::Fenced) && left.value == peer && lost,
        "commit returned {next:?}; record {}; Lost: {lost}",
        String::from_utf8_lossy(&left.value)
    );
}

/// A failure report sent again after one that applied with its reply lost
/// ends the tenancy: one attempt charged, the lease deleted and no `Lost`.
/// Regression for #913.
#[test]
fn a_failure_report_sent_again_after_an_ambiguous_one_hands_the_split_back() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let tap = support::tap::TapStore::new(fault.clone());
    let mut a = holding_polled_clocked(&rt, tap.clone(), &clock, &["r0"]);
    let epoch = fault.epoch(&rt, "split.r0");
    arm_ambiguous(&fault, "split.r0");
    let first = a.fail(&support::split_id("r0"), epoch, "injected");
    assert!(
        is_kind(&first, CoordinationErrorKind::Retryable),
        "{first:?}"
    );
    let log = log_lease_writes(&tap, "split.r0");
    let reads = count_durable_reads(&tap, "split.r0");

    let second = a.fail(&support::split_id("r0"), epoch, "injected");

    assert_resent_report_ended(&rt, &fault, &mut a, &second, &log);
    assert!(
        reads.load(Ordering::SeqCst) > 0,
        "the second report read the record back"
    );
}

/// A failure report sent again after one that applied with its reply lost and
/// that the worker has since seen writes nothing: one attempt charged, the
/// split still runnable, the lease deleted and no `Lost`.
/// Regression for #913.
#[test]
fn a_failure_report_sent_again_after_a_seen_ambiguous_one_charges_one_attempt() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let tap = support::tap::TapStore::new(fault.clone());
    let mut cfg = config_for(LEASE, Some("worker-a"));
    cfg.max_attempts = 2;
    let mut a =
        StoreCoordinator::with_clock(tap.clone(), cfg, rt.handle().clone(), None, clock.clone())
            .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["r0"])))
        .unwrap();
    let mut fleet = support::Fleet::new(&fault.inner, rt.handle());
    fleet.join(&a);
    let mut held = Held::default();
    support::drive_clocked(&mut a, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });
    let epoch = LeaseEpoch(held.splits["r0"].0);
    seen_failure_report(&mut a, &fault, &fleet, &clock, epoch);
    let log = log_lease_writes(&tap, "split.r0");

    let second = a.fail(&support::split_id("r0"), epoch, "injected");

    assert_resent_report_ended(&rt, &fault, &mut a, &second, &log);
    let record = fault.record(&rt, "split.r0");
    assert!(record["status"] == "runnable", "record {record}");
}

/// Records every write to the ephemeral key `key` that reaches `tap` from now on.
fn log_lease_writes(
    tap: &support::tap::TapStore<FaultStore>,
    key: &'static str,
) -> Arc<Mutex<Vec<support::tap::Op>>> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&log);
    tap.on_write(move |w| {
        if w.ks == Keyspace::Ephemeral && w.key == key {
            sink.lock().unwrap().push(w.op);
        }
        None
    });
    log
}

/// Counts the durable reads of `key` that reach `tap` from now on.
fn count_durable_reads(
    tap: &support::tap::TapStore<FaultStore>,
    key: &'static str,
) -> Arc<AtomicU64> {
    let reads = Arc::new(AtomicU64::new(0));
    let count = Arc::clone(&reads);
    tap.on_get(move |ks, k| {
        if ks == Keyspace::Durable && k == key {
            count.fetch_add(1, Ordering::SeqCst);
        }
        None
    });
    reads
}

/// Asserts that a failure report sent again ended the tenancy of `r0` with one
/// attempt charged: `Ok`, no `Lost`, and a lease whose first write after the
/// first report is its delete.
fn assert_resent_report_ended(
    rt: &tokio::runtime::Runtime,
    fault: &FaultStore,
    a: &mut impl SplitCoordinator,
    second: &Result<(), spate_coordination::CoordinationError>,
    log: &Mutex<Vec<support::tap::Op>>,
) {
    // The worker may claim the handed-back split again at a later epoch.
    let record = fault.record(rt, "split.r0");
    let lease_epoch = rt
        .block_on(fault.inner.get(Keyspace::Ephemeral, "split.r0"))
        .expect("read the lease")
        .map(|entry| {
            let lease: serde_json::Value =
                serde_json::from_slice(&entry.value).expect("a JSON lease");
            lease["epoch"].as_u64().expect("a lease epoch")
        });
    let lost = lost_queued(a, "r0");
    let log = log.lock().unwrap().clone();
    let epoch = record["epoch"].as_u64().expect("an epoch");
    assert!(
        second.is_ok()
            && record["attempts"] == 1
            && ((epoch == 1 && record["owner"].is_null()) || epoch >= 2)
            && lease_epoch != Some(1)
            && !lost
            && log.first() == Some(&support::tap::Op::Delete),
        "second {second:?}; record {record}; lease epoch {lease_epoch:?}; Lost: {lost}; \
         lease writes {log:?}"
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
    let epoch = fault.epoch(&rt, "split.r0");
    arm_ambiguous(&fault, "split.r0");
    let first = a.fail(&support::split_id("r0"), epoch, "injected");
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
        let epoch = fault.epoch(&rt, "split.c0");
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
            a.fail(&support::split_id("c0"), epoch, "injected")
        };

        assert!(
            is_kind(&failed, CoordinationErrorKind::Fatal) && !armed.load(Ordering::SeqCst),
            "commit={commit}: {failed:?}"
        );
    }
}

/// A started worker named `instance` on `clock`, which must also drive `store`'s
/// lease expiry, once it holds the one split of `ids`.
fn claimed_clocked<S: CoordinationStore + Clone>(
    rt: &tokio::runtime::Runtime,
    store: S,
    clock: &Arc<TestClock>,
    instance: &str,
    ids: &[&str],
) -> (StoreCoordinator<S>, Held) {
    let mut w = StoreCoordinator::with_clock(
        store,
        config_for(LEASE, Some(instance)),
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("departure:v1", ids)))
        .unwrap();
    let mut held = Held::default();
    support::drive_clocked(&mut w, clock, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });
    (w, held)
}

/// Fails the next update of the split record `key` Retryable, with nothing
/// written. The returned flag is true until the fault fires.
fn refuse_next_update<S>(tap: &support::tap::TapStore<S>, key: &'static str) -> Arc<AtomicBool> {
    let armed = Arc::new(AtomicBool::new(true));
    let fires = Arc::clone(&armed);
    tap.on_write(move |w| {
        (matches!(w.op, support::tap::Op::Update)
            && w.ks == Keyspace::Durable
            && w.key == key
            && fires.swap(false, Ordering::SeqCst))
        .then(|| StoreError::Retryable("injected: write refused".into()))
    });
    armed
}

/// Fails the next delete of the lease `key` Retryable, with nothing deleted.
/// The returned flag is true until the fault fires.
fn refuse_next_delete<S>(tap: &support::tap::TapStore<S>, key: &'static str) -> Arc<AtomicBool> {
    let armed = Arc::new(AtomicBool::new(true));
    let fires = Arc::clone(&armed);
    tap.on_write(move |w| {
        (matches!(w.op, support::tap::Op::Delete)
            && w.ks == Keyspace::Ephemeral
            && w.key == key
            && fires.swap(false, Ordering::SeqCst))
        .then(|| StoreError::Retryable("injected: delete refused".into()))
    });
    armed
}

/// A first claim whose record write applied with its reply lost holds the split
/// at the epoch it wrote and charges no delivery attempt.
#[test]
fn a_claim_whose_reply_was_lost_costs_no_attempt() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    // The seed is a create, so the first update of the record is the claim.
    arm_ambiguous(&fault, "split.r0");

    let (_a, held) = claimed_clocked(&rt, fault.clone(), &clock, "worker-a", &["r0"]);

    let fired = fault.ambiguous.lock().unwrap().is_empty();
    let record = fault.record(&rt, "split.r0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.r0"]);
    let epoch = held.splits.get("r0").map(|(epoch, _)| *epoch);
    assert!(
        fired
            && record["attempts"] == 0
            && record["epoch"] == 1
            && record["owner"] == "worker-a"
            && epoch == Some(1)
            && !lease.is_empty(),
        "fault fired: {fired}; record {record}; held at {epoch:?}; lease: {lease:?}"
    );
}

/// A first claim whose record write failed with nothing written is claimed again
/// at the same epoch, and the record names the claimant.
#[test]
fn a_claim_write_that_did_not_apply_is_not_adopted() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let tap = support::tap::TapStore::new(fault.clone());
    let armed = refuse_next_update(&tap, "split.r0");

    let (_a, held) = claimed_clocked(&rt, tap, &clock, "worker-a", &["r0"]);

    let fired = !armed.load(Ordering::SeqCst);
    let record = fault.record(&rt, "split.r0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.r0"]);
    let epoch = held.splits.get("r0").map(|(epoch, _)| *epoch);
    assert!(
        fired
            && record["attempts"] == 0
            && record["epoch"] == 1
            && record["owner"] == "worker-a"
            && epoch == Some(1)
            && !lease.is_empty(),
        "fault fired: {fired}; record {record}; held at {epoch:?}; lease: {lease:?}"
    );
}

/// A worker restarted under the same instance id, whose claim write failed with
/// nothing written, leaves its predecessor's record and claims the split at the
/// next epoch.
#[test]
fn a_restarts_claim_write_that_did_not_apply_is_not_adopted() {
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let tap = support::tap::TapStore::new(fault.clone());
    let first = runtime();
    let (predecessor, _) = claimed_clocked(&first, tap.clone(), &clock, "worker-a", &["r0"]);
    support::crash(first, predecessor);

    let rt = runtime();
    clock.advance(LEASE * 2);
    let _: Vec<Entry> = rt
        .block_on(fault.inner.list(Keyspace::Ephemeral, ""))
        .expect("expire the predecessor's keys");
    let before = fault.record(&rt, "split.r0");
    assert!(
        before["epoch"] == 1 && before["owner"] == "worker-a",
        "predecessor's record {before}"
    );
    let armed = refuse_next_update(&tap, "split.r0");

    let (_a, held) = claimed_clocked(&rt, tap, &clock, "worker-a", &["r0"]);

    let fired = !armed.load(Ordering::SeqCst);
    let record = fault.record(&rt, "split.r0");
    let epoch = held.splits.get("r0").map(|(epoch, _)| *epoch);
    assert!(
        fired
            && record["epoch"] == 2
            && record["owner"] == "worker-a"
            && record["attempts"] == 1
            && epoch == Some(2),
        "fault fired: {fired}; record {record}; held at {epoch:?}"
    );
}

/// A claim of a released split whose record write applied with its reply lost
/// holds the split at the epoch it wrote and charges no delivery attempt.
#[test]
fn a_claim_of_a_released_split_whose_reply_was_lost_costs_no_attempt() {
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let mut a = holding(&rt, &store, config_for(LEASE, Some("worker-a")), &["r0"]);
    let result = a.depart(&[]);
    assert!(result.is_ok(), "{result:?}");
    let before = store.record(&rt, "split.r0");
    assert!(
        before["epoch"] == 1 && before["owner"].is_null(),
        "{before}"
    );
    arm_ambiguous(&store, "split.r0");

    let mut b = StoreCoordinator::new(
        store.clone(),
        config_for(LEASE, Some("worker-b")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    b.start(Box::new(PhasedPlanner::one_final("departure:v1", &["r0"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut b, &mut held, "claiming r0", |h| h.splits.len() == 1);

    let fired = store.ambiguous.lock().unwrap().is_empty();
    let record = store.record(&rt, "split.r0");
    let epoch = held.splits.get("r0").map(|(epoch, _)| *epoch);
    assert!(
        fired
            && record["attempts"] == 0
            && record["epoch"] == 2
            && record["owner"] == "worker-b"
            && epoch == Some(2),
        "fault fired: {fired}; record {record}; held at {epoch:?}"
    );
}

/// A claim whose record write applied with its reply lost, and whose read-back
/// fails Fatal, deletes its lease and stops the task.
#[test]
fn a_claim_read_back_that_fails_fatally_is_fatal() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    arm_ambiguous(&fault, "split.r0");
    fault
        .fatal
        .lock()
        .unwrap()
        .push((Op::Get, Keyspace::Durable, "split.r0".to_string()));
    let mut a = StoreCoordinator::with_clock(
        fault.clone(),
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["r0"])))
        .unwrap();

    let mut held = Held::default();
    let mut stopped = None;
    spate_test::wait_until(support::DEADLINE, "the task to stop or hold r0", || {
        clock.advance(LEASE / 12);
        match a.poll() {
            Ok(events) => held.fold(events),
            Err(e) => stopped = Some(e),
        }
        stopped.is_some() || !held.splits.is_empty()
    });

    let fired =
        fault.ambiguous.lock().unwrap().is_empty() && fault.fatal.lock().unwrap().is_empty();
    let gained = !held.splits.is_empty();
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.r0"]);
    assert!(
        fired
            && !gained
            && lease.is_empty()
            && stopped.as_ref().is_some_and(|e| {
                e.kind == CoordinationErrorKind::Fatal
                    && e.reason.contains("re-reading a claimed record")
            }),
        "faults fired: {fired}; gained r0: {gained}; lease: {lease:?}; poll returned {stopped:?}"
    );
}

/// A worker named `instance` that reconciles only outside the test.
fn unreconciled(instance: &str) -> CoordinationConfig {
    let mut config = config_for(LEASE, Some(instance));
    config.reconcile_interval = NO_RECONCILE;
    config
}

/// The value a [`LateWrite`] lands, computed from the record's current value.
type Late = Box<dyn FnOnce(&[u8]) -> Vec<u8> + Send>;

/// A [`MemoryStore`] whose first durable update of `key` lands `late` applied to
/// the record's current value, then fails Retryable with nothing of its own
/// written.
#[derive(Clone)]
struct LateWrite {
    inner: MemoryStore,
    key: &'static str,
    late: Arc<Mutex<Option<Late>>>,
}

impl LateWrite {
    fn new(inner: MemoryStore, key: &'static str, late: Late) -> LateWrite {
        LateWrite {
            inner,
            key,
            late: Arc::new(Mutex::new(Some(late))),
        }
    }

    fn fired(&self) -> bool {
        self.late.lock().unwrap().is_none()
    }
}

impl CoordinationStore for LateWrite {
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
        if ks == Keyspace::Durable && key == self.key {
            let late = self.late.lock().unwrap().take();
            if let Some(late) = late {
                let entry = self.inner.get(ks, key).await?.expect("record");
                let landed = self
                    .inner
                    .update(ks, key, late(&entry.value), entry.revision)
                    .await?;
                assert!(matches!(landed, CasOutcome::Won(_)), "the late write");
                return Err(StoreError::Retryable("injected: reply lost".into()));
            }
        }
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
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// Starts `worker-a` over `inner`, crashes it once it holds `x`, and returns
/// the record its claim wrote.
fn crashed_claim(inner: &MemoryStore, planner: &str) -> Entry {
    let rt = runtime();
    let mut a = StoreCoordinator::new(
        inner.clone(),
        unreconciled("worker-a"),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final(planner, &["x"])))
        .unwrap();
    drive(&mut a, &mut Held::default(), "worker-a claiming x", |h| {
        h.splits.len() == 1
    });
    support::crash(rt, a);
    let reader = runtime();
    reader
        .block_on(inner.get(Keyspace::Durable, "split.x"))
        .unwrap()
        .expect("record")
}

/// Replaces the record at `split.x`, read at `current`, with `value`.
fn rewind(
    rt: &tokio::runtime::Runtime,
    inner: &MemoryStore,
    current: &Entry,
    value: &serde_json::Value,
) {
    let rewound = rt
        .block_on(inner.update(
            Keyspace::Durable,
            "split.x",
            serde_json::to_vec(value).unwrap(),
            current.revision,
        ))
        .unwrap();
    assert!(matches!(rewound, CasOutcome::Won(_)));
}

/// A restarted worker whose view shows the record from before its predecessor's
/// claim, and whose own claim write fails with nothing written, does not adopt
/// the predecessor's record and takes the split at the next epoch.
#[test]
fn a_restart_with_a_lagging_view_does_not_adopt_its_predecessors_claim() {
    let inner = support::store();
    let claimed = crashed_claim(&inner, "lagging-restart:v1");

    let rt = runtime();
    let mut seed: serde_json::Value = serde_json::from_slice(&claimed.value).unwrap();
    assert!(
        seed["epoch"] == 1 && seed["owner"] == "worker-a",
        "predecessor's record {seed}"
    );
    seed["epoch"] = 0.into();
    seed["owner"] = serde_json::Value::Null;
    rewind(&rt, &inner, &claimed, &seed);

    // Dated before any write of the restart, which could otherwise share its
    // millisecond and write a byte-equal record.
    let mut predecessors: serde_json::Value = serde_json::from_slice(&claimed.value).unwrap();
    predecessors["written_at_ms"] = (predecessors["written_at_ms"].as_i64().unwrap() - 1).into();
    let predecessors = serde_json::to_vec(&predecessors).unwrap();
    let late = LateWrite::new(inner.clone(), "split.x", Box::new(move |_| predecessors));
    let mut b = StoreCoordinator::new(
        late.clone(),
        unreconciled("worker-a"),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    b.start(Box::new(PhasedPlanner::one_final(
        "lagging-restart:v1",
        &["x"],
    )))
    .unwrap();
    let mut held = Held::default();
    drive(&mut b, &mut held, "the restart claiming x", |h| {
        h.splits.len() == 1
    });

    let fired = late.fired();
    let record: serde_json::Value = serde_json::from_slice(
        &rt.block_on(inner.get(Keyspace::Durable, "split.x"))
            .unwrap()
            .expect("record")
            .value,
    )
    .unwrap();
    let epoch = held.splits.get("x").map(|(epoch, _)| *epoch);
    assert!(
        fired && epoch == Some(2) && record["epoch"] == 2 && record["attempts"] == 1,
        "fault fired: {fired}; record {record}; held at {epoch:?}"
    );
}

/// A restarted worker whose view lags its predecessor's completing commit, and
/// whose claim write fails with nothing written, gains nothing.
#[test]
fn a_restart_with_a_lagging_view_does_not_gain_a_completed_split() {
    let inner = support::store();
    let claimed = crashed_claim(&inner, "lagging-complete:v1");

    let rt = runtime();
    let mut finished: serde_json::Value = serde_json::from_slice(&claimed.value).unwrap();
    finished["status"] = "completed".into();
    finished["completed"] = true.into();
    finished["watermark"] = 9.into();
    let mut seed = finished.clone();
    seed["epoch"] = 0.into();
    seed["owner"] = serde_json::Value::Null;
    seed["status"] = "runnable".into();
    seed["completed"] = false.into();
    seed["watermark"] = serde_json::Value::Null;
    rewind(&rt, &inner, &claimed, &seed);

    let finished = serde_json::to_vec(&finished).unwrap();
    let late = LateWrite::new(inner.clone(), "split.x", Box::new(move |_| finished));
    let mut b = StoreCoordinator::new(
        late.clone(),
        unreconciled("worker-a"),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    b.start(Box::new(PhasedPlanner::one_final(
        "lagging-complete:v1",
        &["x"],
    )))
    .unwrap();
    let mut held = Held::default();
    drive(&mut b, &mut held, "the restart settling x", |h| {
        h.all_complete || !h.splits.is_empty()
    });

    let fired = late.fired();
    assert!(
        fired && held.splits.is_empty(),
        "fault fired: {fired}; held {:?}",
        held.splits
    );
}

/// A claim whose write fails with nothing written, after a peer's claim at the
/// same epoch landed unseen, does not adopt the peer's record.
#[test]
fn a_claim_write_that_did_not_apply_does_not_adopt_a_peers_claim() {
    let inner = support::store();
    let rt = runtime();
    let late = LateWrite::new(
        inner.clone(),
        "split.x",
        Box::new(|current| {
            let mut peer: serde_json::Value = serde_json::from_slice(current).unwrap();
            peer["epoch"] = 1.into();
            peer["owner"] = "worker-z".into();
            serde_json::to_vec(&peer).unwrap()
        }),
    );
    let mut b = StoreCoordinator::new(
        late.clone(),
        unreconciled("worker-b"),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    b.start(Box::new(PhasedPlanner::one_final("peer-claim:v1", &["x"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut b, &mut held, "worker-b claiming x", |h| {
        h.splits.len() == 1
    });

    let fired = late.fired();
    let record: serde_json::Value = serde_json::from_slice(
        &rt.block_on(inner.get(Keyspace::Durable, "split.x"))
            .unwrap()
            .expect("record")
            .value,
    )
    .unwrap();
    let epoch = held.splits.get("x").map(|(epoch, _)| *epoch);
    assert!(
        fired && record["owner"] == "worker-b" && epoch == Some(2),
        "fault fired: {fired}; record {record}; held at {epoch:?}"
    );
}

/// Installs the Prometheus exporter once per process and returns its handle.
fn exporter() -> spate_core::metrics::MetricsHandle {
    spate_core::metrics::install(&spate_core::metrics::MetricsSettings {
        exporter: spate_core::metrics::Exporter::Prometheus,
        ..spate_core::metrics::MetricsSettings::default()
    })
    .expect("install the exporter")
}

/// Starts a metered `worker-a` labelled `component`, holding `r0` and `h0`
/// over `store` on `clock`, runs `prepare` and `act`, and returns
/// `releases_total` for `component`. `store` sits over `inner`, whose lease
/// expiry `clock` drives.
fn releases_after<S: CoordinationStore + Clone>(
    store: S,
    inner: &MemoryStore,
    clock: &Arc<TestClock>,
    component: &'static str,
    prepare: impl FnOnce(&mut StoreCoordinator<S>, &support::Fleet, LeaseEpoch),
    act: impl FnOnce(&mut StoreCoordinator<S>),
) -> f64 {
    let handle = exporter();
    let rt = runtime();
    let labels = spate_core::metrics::ComponentLabels::new("departure", component, "s3");
    let mut a = StoreCoordinator::with_clock(
        store,
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        Some(spate_core::metrics::CoordinationMetrics::new(&labels)),
        clock.clone(),
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final(
        "departure:v1",
        &["r0", "h0"],
    )))
    .unwrap();
    let mut fleet = support::Fleet::new(inner, rt.handle());
    fleet.join(&a);
    let mut held = Held::default();
    support::drive_clocked(&mut a, clock, &mut held, "claiming every split", |h| {
        h.splits.len() == 2
    });
    prepare(&mut a, &fleet, LeaseEpoch(held.splits["r0"].0));
    act(&mut a);
    spate_test::metric_sum(
        &handle.render(),
        "spate_coordination_releases_total",
        &[("component", component)],
    )
    .unwrap_or(0.0)
}

/// Fails `r0` with the report's reply lost, then settles so the worker has
/// folded the report.
fn seen_failure_report(
    a: &mut impl SplitCoordinator,
    fault: &FaultStore,
    fleet: &support::Fleet,
    clock: &TestClock,
    epoch: LeaseEpoch,
) {
    fleet.settle(clock);
    arm_ambiguous(fault, "split.r0");
    let failed = a.fail(&support::split_id("r0"), epoch, "injected");
    assert!(
        is_kind(&failed, CoordinationErrorKind::Retryable),
        "the injected reply loss surfaces: {failed:?}"
    );
    assert!(
        fault.ambiguous.lock().unwrap().is_empty(),
        "the report took the fault"
    );
    fleet.settle(clock);
}

/// The two splits [`releases_after`] holds.
fn r0_and_h0() -> [spate_coordination::SplitId; 2] {
    [support::split_id("r0"), support::split_id("h0")]
}

/// A release after this worker's own failure report, once the worker has
/// seen the report, counts no release for the reported split and writes
/// nothing over the report.
/// Regression for #886.
#[test]
fn a_release_after_a_seen_failure_report_counts_no_release() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut reported_at = 0;

    let releases = releases_after(
        fault.clone(),
        &fault.inner,
        &clock,
        "release-after-seen-report",
        |a, fleet, epoch| {
            seen_failure_report(a, &fault, fleet, &clock, epoch);
            reported_at = fault.updates(Keyspace::Durable, "split.r0");
        },
        |a| a.release(&r0_and_h0()).expect("release"),
    );

    let record = fault.record(&rt, "split.r0");
    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.r0"]);
    let updates = fault.updates(Keyspace::Durable, "split.r0") - reported_at;
    assert!(
        releases == 1.0
            && record["epoch"] == 1
            && record["owner"].is_null()
            && record["attempts"] == 1
            && updates == 0
            && lease.is_empty(),
        "releases_total {releases} (h0 only); record {record}; \
         updates of split.r0 since the report: {updates}; lease: {lease:?}"
    );
}

/// A departure after this worker's own failure report, once the worker has
/// seen the report, counts no release for the reported split and writes
/// nothing over the report.
/// Regression for #886.
#[test]
fn a_departure_after_a_seen_failure_report_counts_no_release() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut reported_at = 0;

    let releases = releases_after(
        fault.clone(),
        &fault.inner,
        &clock,
        "depart-after-seen-report",
        |a, fleet, epoch| {
            seen_failure_report(a, &fault, fleet, &clock, epoch);
            reported_at = fault.updates(Keyspace::Durable, "split.r0");
        },
        |a| a.depart(&r0_and_h0()).expect("depart"),
    );

    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.r0"]);
    let updates = fault.updates(Keyspace::Durable, "split.r0") - reported_at;
    assert!(
        releases == 1.0 && updates == 0 && lease.is_empty(),
        "releases_total {releases} (h0 only); \
         updates of split.r0 since the report: {updates}; lease: {lease:?}"
    );
}

/// A departure after this worker's own failure report, which a polled view
/// has not seen, counts no release for the reported split.
/// Regression for #886.
#[test]
fn a_departure_after_an_unseen_failure_report_counts_no_release() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());

    let releases = releases_after(
        support::polled::PolledStore::new(fault.clone(), LEASE / 10),
        &fault.inner,
        &clock,
        "depart-after-unseen-report",
        |a, _, epoch| {
            arm_ambiguous(&fault, "split.r0");
            let failed = a.fail(&support::split_id("r0"), epoch, "injected");
            assert!(
                is_kind(&failed, CoordinationErrorKind::Retryable),
                "the injected reply loss surfaces: {failed:?}"
            );
            assert!(
                fault.ambiguous.lock().unwrap().is_empty(),
                "the report took the fault"
            );
        },
        |a| a.depart(&r0_and_h0()).expect("depart"),
    );

    let lease = fault.present(&rt, Keyspace::Ephemeral, &["split.r0"]);
    assert!(
        releases == 1.0 && lease.is_empty(),
        "releases_total {releases} (h0 only); lease: {lease:?}"
    );
}

/// A departure whose own owner clear applied with its reply lost, and whose
/// resend then lost its CAS, still counts the release.
#[test]
fn a_departure_whose_owner_clear_reply_was_lost_counts_the_release() {
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut armed_at = 0;

    let releases = releases_after(
        fault.clone(),
        &fault.inner,
        &clock,
        "depart-owner-clear-lost",
        |_, fleet, _| {
            fleet.settle(&clock);
            armed_at = fault.updates(Keyspace::Durable, "split.r0");
            arm_ambiguous(&fault, "split.r0");
        },
        |a| a.depart(&r0_and_h0()).expect("depart"),
    );

    let updates = fault.updates(Keyspace::Durable, "split.r0") - armed_at;
    assert!(
        releases == 2.0 && updates == 2,
        "releases_total {releases}; updates of split.r0 since arming: {updates}"
    );
}

/// A departure whose own owner clear applied with its reply lost, in a
/// tenancy that began after an earlier failure report, counts the release.
#[test]
fn a_departure_after_an_earlier_report_whose_owner_clear_reply_was_lost_counts_the_release() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut armed_at = 0;
    let mut begun = serde_json::Value::Null;

    let releases = releases_after(
        fault.clone(),
        &fault.inner,
        &clock,
        "depart-owner-clear-lost-after-report",
        |a, fleet, epoch| {
            a.fail(&support::split_id("r0"), epoch, "injected")
                .expect("fail");
            let mut held = Held::default();
            support::drive_clocked(a, &clock, &mut held, "re-claiming r0", |h| {
                h.splits.get("r0").is_some_and(|(epoch, _)| *epoch == 2)
            });
            fleet.settle(&clock);
            begun = fault.record(&rt, "split.r0");
            armed_at = fault.updates(Keyspace::Durable, "split.r0");
            arm_ambiguous(&fault, "split.r0");
        },
        |a| a.depart(&r0_and_h0()).expect("depart"),
    );

    let updates = fault.updates(Keyspace::Durable, "split.r0") - armed_at;
    assert!(
        begun["owner"] == "worker-a"
            && begun["epoch"] == 2
            && begun["attempts"] == 1
            && releases == 2.0
            && updates == 2,
        "re-claimed record {begun}; releases_total {releases}; \
         updates of split.r0 since arming: {updates}"
    );
}

/// Revokes a split from a metered `worker-a` labelled `component`, fails it
/// with the report's reply lost, settles so the worker has folded the report,
/// runs `hand_back` on the revoked split, and returns `revocations_total` for
/// `forced` and `drained` and the drain durations observed.
fn revocation_after_seen_report(
    component: &'static str,
    hand_back: impl FnOnce(&mut StoreCoordinator<FaultStore>, &spate_coordination::SplitId),
) -> (f64, f64, f64) {
    let handle = exporter();
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let ids = ["x0", "x1", "x2", "x3"];
    let planner = || Box::new(PhasedPlanner::one_final("departure:v1", &ids));
    let labels = spate_core::metrics::ComponentLabels::new("departure", component, "s3");
    let mut a = StoreCoordinator::with_clock(
        fault.clone(),
        config_for(LEASE, Some("worker-a")),
        rt.handle().clone(),
        Some(spate_core::metrics::CoordinationMetrics::new(&labels)),
        clock.clone(),
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    support::drive_clocked(
        &mut a,
        &clock,
        &mut held_a,
        "worker-a takes the plan",
        |h| h.splits.len() == ids.len(),
    );
    support::commit_held(&mut a, &held_a);

    // worker-b has its own wrapper, so its writes never take worker-a's fault.
    let rt_b = runtime();
    let mut b = StoreCoordinator::with_clock(
        FaultStore::over(fault.inner.clone()),
        config_for(LEASE, Some("worker-b")),
        rt_b.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    let deadline = Instant::now() + support::DEADLINE;
    let revoked = loop {
        assert!(
            Instant::now() < deadline,
            "the leader never revoked anything from worker-a"
        );
        clock.advance(LEASE / 12);
        std::thread::sleep(support::POLL_INTERVAL);
        let mut asked = None;
        for event in a.poll().expect("poll a") {
            if let CoordinationEvent::RevokeRequested { split } = &event {
                asked = Some(split.clone());
            }
            held_a.fold(vec![event]);
        }
        held_b.fold(b.poll().expect("poll b"));
        if let Some(split) = asked {
            break split;
        }
        support::commit_held(&mut a, &held_a);
    };
    let mut fleet = support::Fleet::new(&fault.inner, rt.handle());
    fleet.join(&a);
    fleet.join(&b);
    fleet.settle(&clock);
    let epoch = LeaseEpoch(held_a.splits[revoked.as_str()].0);
    arm_ambiguous(&fault, &format!("split.{}", revoked.as_str()));
    let failed = a.fail(&revoked, epoch, "injected");
    assert!(
        is_kind(&failed, CoordinationErrorKind::Retryable),
        "the injected reply loss surfaces: {failed:?}"
    );
    assert!(
        fault.ambiguous.lock().unwrap().is_empty(),
        "the report took the fault"
    );
    fleet.settle(&clock);
    hand_back(&mut a, &revoked);

    let text = handle.render();
    let outcome = |o| {
        spate_test::metric_sum(
            &text,
            "spate_coordination_revocations_total",
            &[("component", component), ("outcome", o)],
        )
        .unwrap_or(0.0)
    };
    let drains = spate_test::metric_sum(
        &text,
        "spate_coordination_drain_duration_seconds_count",
        &[("component", component)],
    )
    .unwrap_or(0.0);
    (outcome("forced"), outcome("drained"), drains)
}

/// A drained release of a revoked split after this worker's own failure
/// report, once the worker has seen the report, ends the revocation forced
/// and observes no drain duration.
/// Regression for #886.
#[test]
fn a_drained_release_after_a_seen_failure_report_is_forced() {
    let outcomes = revocation_after_seen_report("drained-after-seen-report", |a, revoked| {
        a.release_drained(std::slice::from_ref(revoked))
            .expect("release_drained");
    });
    assert_eq!(
        outcomes,
        (1.0, 0.0, 0.0),
        "(revocations forced, revocations drained, drain durations observed)"
    );
}

/// A departure holding a revoked split after this worker's own failure
/// report, once the worker has seen the report, ends that revocation forced.
/// Regression for #886.
#[test]
fn a_departure_after_a_seen_failure_report_ends_its_revocation_forced() {
    let outcomes =
        revocation_after_seen_report("depart-after-seen-report-revoked", |a, revoked| {
            a.depart(std::slice::from_ref(revoked)).expect("depart");
        });
    // The departure hands back every held split; other revocations in
    // progress end drained.
    assert_eq!(
        outcomes.0, 1.0,
        "(revocations forced, revocations drained, drain durations observed) = {outcomes:?}"
    );
}

/// `reason`'s count on `spate_coordination_acquisitions_total` for `component`.
fn acquisitions(handle: &spate_core::metrics::MetricsHandle, component: &str, reason: &str) -> f64 {
    spate_test::metric_sum(
        &handle.render(),
        "spate_coordination_acquisitions_total",
        &[("component", component), ("reason", reason)],
    )
    .unwrap_or(0.0)
}

/// A metered worker named `worker-a` and labelled `component`, over `store`,
/// whose lease expiry `clock` drives.
fn metered_clocked<S: CoordinationStore + Clone>(
    rt: &tokio::runtime::Runtime,
    store: S,
    clock: &Arc<TestClock>,
    component: &'static str,
    config: CoordinationConfig,
    ids: &[&str],
) -> StoreCoordinator<S> {
    let labels = spate_core::metrics::ComponentLabels::new("departure", component, "s3");
    let mut w = StoreCoordinator::with_clock(
        store,
        config,
        rt.handle().clone(),
        Some(spate_core::metrics::CoordinationMetrics::new(&labels)),
        clock.clone(),
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("departure:v1", ids)))
        .unwrap();
    w
}

/// Advances `clock` half a lease in steps, settling `fleet` and folding `w`'s
/// events into `held` after each.
fn run_half_a_lease(
    w: &mut impl SplitCoordinator,
    clock: &TestClock,
    fleet: &support::Fleet,
    held: &mut Held,
) {
    clock.advance_stepped(LEASE / 2, LEASE / 24, || {
        fleet.settle(clock);
        held.fold(w.poll().expect("poll"));
    });
}

/// What a worker with two delivery attempts left behind after failing `x`
/// once with a fault armed and running half a lease.
struct AfterReport {
    fired: bool,
    lease_after_report: bool,
    held: Held,
    record: serde_json::Value,
    reassigned: f64,
    reclaimed: f64,
}

/// Fails `x` once on a metered `worker-a` with `max_attempts: 2`, with the
/// fault `arm` sets up, then runs half a lease. `arm` returns whether its fault
/// has fired.
fn report_with_fault(
    component: &'static str,
    arm: impl FnOnce(&FaultStore, &support::tap::TapStore<FaultStore>) -> Box<dyn Fn() -> bool>,
) -> AfterReport {
    let handle = exporter();
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let tap = support::tap::TapStore::new(fault.clone());
    let mut config = config_for(LEASE, Some("worker-a"));
    config.max_attempts = 2;
    config.reconcile_interval = LEASE / 12;
    let mut a = metered_clocked(&rt, tap.clone(), &clock, component, config, &["x"]);
    let mut fleet = support::Fleet::new(&fault.inner, rt.handle());
    fleet.join(&a);
    let mut held = Held::default();
    support::drive_clocked(&mut a, &clock, &mut held, "claiming x", |h| {
        h.splits.len() == 1
    });
    let epoch = LeaseEpoch(held.splits["x"].0);
    held.splits.clear();

    let fired = arm(&fault, &tap);
    a.fail(&support::split_id("x"), epoch, "injected").unwrap();
    let lease_after_report = !fault
        .present(&rt, Keyspace::Ephemeral, &["split.x"])
        .is_empty();
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);

    AfterReport {
        fired: fired(),
        lease_after_report,
        held,
        record: fault.record(&rt, "split.x"),
        reassigned: acquisitions(&handle, component, "reassigned"),
        reclaimed: acquisitions(&handle, component, "reclaimed"),
    }
}

/// Asserts that `run` claimed `x` again on its last attempt as a reassignment.
fn assert_last_attempt_claimed(run: &AfterReport) {
    let epoch = run.held.splits.get("x").map(|(epoch, _)| *epoch);
    let record = &run.record;
    assert!(
        run.fired
            && run.held.quarantined.is_empty()
            && epoch == Some(2)
            && record["attempts"] == 1
            && record["epoch"] == 2
            && record["status"] == "runnable"
            && record["owner"] == "worker-a"
            && run.reassigned == 1.0
            && run.reclaimed == 0.0,
        "fault fired: {}; quarantined {:?}; held at {epoch:?}; record {record}; \
         reassigned {}; reclaimed {}",
        run.fired,
        run.held.quarantined,
        run.reassigned,
        run.reclaimed
    );
}

/// A failure report followed by a claim whose lease create applied with its
/// reply lost claims the split on its last attempt, counted as reassigned.
/// Regression for #894.
#[test]
fn a_lost_lease_create_reply_after_a_failure_report_keeps_the_last_attempt() {
    let run = report_with_fault("lost-create-after-report", |fault, _| {
        fault
            .ambiguous_creates
            .lock()
            .unwrap()
            .push((Keyspace::Ephemeral, "split.x".to_string()));
        let fault = fault.clone();
        Box::new(move || fault.ambiguous_creates.lock().unwrap().is_empty())
    });
    assert_last_attempt_claimed(&run);
}

/// A failure report whose lease delete fails claims the split on its last
/// attempt, counted as reassigned. Regression for #894.
#[test]
fn a_failed_lease_delete_after_a_failure_report_keeps_the_last_attempt() {
    let run = report_with_fault("failed-delete-after-report", |_, tap| {
        let armed = refuse_next_delete(tap, "split.x");
        Box::new(move || !armed.load(Ordering::SeqCst))
    });
    assert!(run.lease_after_report, "the report deleted the lease");
    assert_last_attempt_claimed(&run);
}

/// A first claim whose lease create applied with its reply lost holds the split
/// with `max_attempts: 1` and counts as a create. Regression for #894.
#[test]
fn a_lost_lease_create_reply_on_a_first_claim_does_not_quarantine_at_one_attempt() {
    let component = "lost-create-first-claim";
    let handle = exporter();
    let rt = runtime();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    fault
        .ambiguous_creates
        .lock()
        .unwrap()
        .push((Keyspace::Ephemeral, "split.x".to_string()));
    let mut config = config_for(LEASE, Some("worker-a"));
    config.max_attempts = 1;
    let mut a = metered_clocked(&rt, fault.clone(), &clock, component, config, &["x"]);
    let mut fleet = support::Fleet::new(&fault.inner, rt.handle());
    fleet.join(&a);
    let mut held = Held::default();

    run_half_a_lease(&mut a, &clock, &fleet, &mut held);

    let fired = fault.ambiguous_creates.lock().unwrap().is_empty();
    let record = fault.record(&rt, "split.x");
    let epoch = held.splits.get("x").map(|(epoch, _)| *epoch);
    let created = acquisitions(&handle, component, "create");
    assert!(
        fired
            && held.quarantined.is_empty()
            && epoch == Some(1)
            && record["attempts"] == 0
            && record["epoch"] == 1
            && record["owner"] == "worker-a"
            && created == 1.0,
        "fault fired: {fired}; quarantined {:?}; held at {epoch:?}; record {record}; \
         created {created}",
        held.quarantined
    );
}

/// A worker restarted under its predecessor's id while the predecessor's lease
/// is live reclaims the split, charges one attempt and counts it as reclaimed.
#[test]
fn a_restart_inside_the_lease_reclaims_and_charges_one_attempt() {
    let component = "restart-inside-lease";
    let handle = exporter();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let first = runtime();
    let (predecessor, _) = claimed_clocked(&first, fault.clone(), &clock, "worker-a", &["r0"]);
    support::crash(first, predecessor);

    let rt = runtime();
    let mut a = metered_clocked(
        &rt,
        fault.clone(),
        &clock,
        component,
        config_for(LEASE, Some("worker-a")),
        &["r0"],
    );
    let mut fleet = support::Fleet::new(&fault.inner, rt.handle());
    fleet.join(&a);
    let mut held = Held::default();
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);

    let record = fault.record(&rt, "split.r0");
    let epoch = held.splits.get("r0").map(|(epoch, _)| *epoch);
    let reclaimed = acquisitions(&handle, component, "reclaimed");
    assert!(
        epoch == Some(2)
            && record["attempts"] == 1
            && record["epoch"] == 2
            && record["owner"] == "worker-a"
            && reclaimed == 1.0,
        "held at {epoch:?}; record {record}; reclaimed {reclaimed}"
    );
}

/// A second live worker under the same id, over a split on its last attempt,
/// stops on the shared id and leaves the split runnable under the first.
#[test]
fn a_twin_over_a_split_on_its_last_attempt_stops_on_the_shared_id() {
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let mut config = config_for(LEASE, Some("worker-a"));
    config.max_attempts = 1;
    let rt = runtime();
    let mut first = StoreCoordinator::with_clock(
        fault.clone(),
        config.clone(),
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    first
        .start(Box::new(PhasedPlanner::one_final("departure:v1", &["r0"])))
        .unwrap();
    let mut held = Held::default();
    support::drive_clocked(&mut first, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });
    let mut solo = support::Fleet::new(&fault.inner, rt.handle());
    solo.join(&first);
    solo.settle(&clock);

    let twin_rt = runtime();
    let mut twin = StoreCoordinator::with_clock(
        fault.clone(),
        config,
        twin_rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    twin.start(Box::new(PhasedPlanner::one_final("departure:v1", &["r0"])))
        .unwrap();
    let mut both = support::Fleet::new(&fault.inner, rt.handle());
    both.join(&first);
    both.join(&twin);
    both.settle(&clock);

    let record = fault.record(&rt, "split.r0");
    assert!(
        record["status"] == "runnable" && record["owner"] == "worker-a" && record["attempts"] == 0,
        "record after the twin's first step: {record}"
    );

    clock.advance_stepped(LEASE / 2, LEASE / 24, || {
        solo.settle(&clock);
        held.fold(first.poll().expect("poll the first worker"));
    });
    let mut stopped = None;
    spate_test::wait_until(support::DEADLINE, "the twin to stop", || {
        match twin.poll() {
            Ok(_) => false,
            Err(e) => {
                stopped = Some(e);
                true
            }
        }
    });

    let after = fault.record(&rt, "split.r0");
    assert!(
        stopped.as_ref().is_some_and(|e| {
            e.kind == CoordinationErrorKind::Fatal && e.reason.contains("share instance_id")
        }) && after == record
            && held.splits.contains_key("r0"),
        "twin returned {stopped:?}; record {after}; first holds {:?}",
        held.splits.keys().collect::<Vec<_>>()
    );
}

/// A started `worker-a` with `max_attempts: 1` on `rt`, restarted after a
/// crashed predecessor that held `r0`, in a one-member fleet.
fn last_attempt_successor(
    rt: &tokio::runtime::Runtime,
    fault: &FaultStore,
    clock: &Arc<TestClock>,
) -> (StoreCoordinator<FaultStore>, support::Fleet) {
    let first = runtime();
    let (predecessor, _) = claimed_clocked(&first, fault.clone(), clock, "worker-a", &["r0"]);
    support::crash(first, predecessor);
    let mut config = config_for(LEASE, Some("worker-a"));
    config.max_attempts = 1;
    let mut a = StoreCoordinator::with_clock(
        fault.clone(),
        config,
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("departure:v1", &["r0"])))
        .unwrap();
    let mut fleet = support::Fleet::new(&fault.inner, rt.handle());
    fleet.join(&a);
    (a, fleet)
}

fn assert_quarantined_on_expiry(rt: &tokio::runtime::Runtime, fault: &FaultStore, held: &Held) {
    let record = fault.record(rt, "split.r0");
    assert!(
        record["status"] == "quarantined"
            && record["attempts"] == 1
            && record["owner"].is_null()
            && held.quarantined.iter().any(|(id, _)| id == "r0"),
        "record {record}; quarantined {:?}",
        held.quarantined
    );
}

/// A worker restarted after a crash on a split's last attempt quarantines it
/// once the predecessor's lease expires, with no error.
#[test]
fn a_restart_on_a_splits_last_attempt_quarantines_once_the_lease_expires() {
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let rt = runtime();
    let (mut a, fleet) = last_attempt_successor(&rt, &fault, &clock);
    let mut held = Held::default();
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);
    let record = fault.record(&rt, "split.r0");
    assert!(
        record["status"] == "runnable" && held.quarantined.is_empty(),
        "record inside the predecessor's lease: {record}"
    );
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);
    assert_quarantined_on_expiry(&rt, &fault, &held);
}

/// As the restart control, with every watch broken and re-established inside
/// the predecessor's lease: the rewatch replay of that lease is not a twin.
#[test]
fn a_rewatch_during_a_last_attempt_restart_still_quarantines_on_expiry() {
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let rt = runtime();
    let (mut a, fleet) = last_attempt_successor(&rt, &fault, &clock);
    let mut held = Held::default();
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);
    fault.go_down();
    fault.down.store(false, Ordering::SeqCst);
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);
    assert_quarantined_on_expiry(&rt, &fault, &held);
}

/// A worker restarted under its predecessor's id, after a failure report that
/// cleared the owner but left the lease, claims the split on its last attempt,
/// counted as reassigned. Regression for #894.
#[test]
fn a_restart_over_a_reported_splits_leftover_lease_keeps_the_last_attempt() {
    let component = "restart-over-reported-lease";
    let handle = exporter();
    let clock = TestClock::frozen();
    let fault = FaultStore::with_clock(LEASE, clock.clone());
    let tap = support::tap::TapStore::new(fault.clone());
    let first = runtime();
    let mut config = config_for(LEASE, Some("worker-a"));
    config.max_attempts = 2;
    let mut predecessor = StoreCoordinator::with_clock(
        tap.clone(),
        config.clone(),
        first.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    predecessor
        .start(Box::new(PhasedPlanner::one_final("departure:v1", &["x"])))
        .unwrap();
    let mut held = Held::default();
    support::drive_clocked(&mut predecessor, &clock, &mut held, "claiming x", |h| {
        h.splits.len() == 1
    });
    let armed = refuse_next_delete(&tap, "split.x");
    predecessor
        .fail(
            &support::split_id("x"),
            LeaseEpoch(held.splits["x"].0),
            "injected",
        )
        .unwrap();
    let fired = !armed.load(Ordering::SeqCst);
    support::crash(first, predecessor);
    let probe = runtime();
    let before = fault.record(&probe, "split.x");
    let lease_left = !fault
        .present(&probe, Keyspace::Ephemeral, &["split.x"])
        .is_empty();
    assert!(
        fired && lease_left && before["owner"].is_null() && before["attempts"] == 1,
        "setup: fired {fired}; lease left {lease_left}; record {before}"
    );

    let rt = runtime();
    let mut a = metered_clocked(&rt, fault.clone(), &clock, component, config, &["x"]);
    let mut fleet = support::Fleet::new(&fault.inner, rt.handle());
    fleet.join(&a);
    let mut held = Held::default();
    run_half_a_lease(&mut a, &clock, &fleet, &mut held);

    let record = fault.record(&rt, "split.x");
    let epoch = held.splits.get("x").map(|(epoch, _)| *epoch);
    let reassigned = acquisitions(&handle, component, "reassigned");
    assert!(
        held.quarantined.is_empty()
            && epoch == Some(2)
            && record["attempts"] == 1
            && record["status"] == "runnable"
            && record["owner"] == "worker-a"
            && reassigned == 1.0,
        "quarantined {:?}; held at {epoch:?}; record {record}; reassigned {reassigned}",
        held.quarantined
    );
}

/// A task still mid-poll when `support::crash` begins has finished, and every
/// thread of its runtime has exited, before `crash` returns. Regression for #967.
#[test]
fn a_crashed_workers_mid_poll_task_ends_before_crash_returns() {
    let clock = TestClock::frozen();
    let started = Arc::new(AtomicU64::new(0));
    let stopped = Arc::new(AtomicU64::new(0));
    // `support::runtime()` with thread hooks; the test needs its two workers.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start({
            let started = started.clone();
            move || {
                started.fetch_add(1, Ordering::SeqCst);
            }
        })
        .on_thread_stop({
            let stopped = stopped.clone();
            move || {
                stopped.fetch_add(1, Ordering::SeqCst);
            }
        })
        .build()
        .expect("test runtime");
    let (worker, _) = claimed_clocked(&rt, support::store(), &clock, "worker-a", &["r0"]);
    let crashed = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (saw_tx, saw_rx) = std::sync::mpsc::channel();
    // Holds its worker thread inside one poll until shutdown drops the task below.
    rt.spawn({
        let crashed = crashed.clone();
        async move {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv();
            saw_tx.send(crashed.load(Ordering::SeqCst)).unwrap();
        }
    });
    rt.spawn(async move {
        let _release = release_tx;
        std::future::pending::<()>().await;
    });
    entered_rx
        .recv_timeout(support::DEADLINE)
        .expect("the held task to start");

    support::crash(rt, worker);
    let live = started.load(Ordering::SeqCst) - stopped.load(Ordering::SeqCst);
    crashed.store(true, Ordering::SeqCst);
    let ran_after_crash = saw_rx
        .recv_timeout(support::DEADLINE)
        .expect("the held task to report");
    assert!(
        live == 0 && !ran_after_crash,
        "after crash returned: {live} runtime threads live; held task ran on {ran_after_crash}"
    );
}

/// Leases long enough that no renewal runs during a stop test; `op_timeout`
/// is two seconds, so a command waits up to six.
const STOP_LEASE: Duration = Duration::from_secs(15);

/// A stopping worker holding `c0` and `c1` under `config`, with its stop flag
/// clear.
fn stopping_worker_with(
    rt: &tokio::runtime::Runtime,
    store: &FaultStore,
    config: CoordinationConfig,
) -> (StoreCoordinator<FaultStore>, Arc<AtomicBool>) {
    let mut a = holding(rt, store, config, &["c0", "c1"]);
    let flag = Arc::new(AtomicBool::new(false));
    a.set_stop(StopSignal::new(Arc::clone(&flag)));
    (a, flag)
}

fn stopping_worker(
    rt: &tokio::runtime::Runtime,
    store: &FaultStore,
) -> (StoreCoordinator<FaultStore>, Arc<AtomicBool>) {
    stopping_worker_with(rt, store, config_for(STOP_LEASE, Some("worker-a")))
}

/// Commit `split` until it is answered, so every command sent before it has
/// been served.
fn barrier(a: &mut StoreCoordinator<FaultStore>, split: &str) {
    let deadline = Instant::now() + support::DEADLINE;
    while a
        .commit(&support::split_id(split), &SplitProgress::new(1, vec![]))
        .is_err_and(|e| e.kind == CoordinationErrorKind::Retryable)
    {
        assert!(Instant::now() < deadline, "the barrier commit never landed");
    }
}

fn is_stopping(r: &Result<(), spate_coordination::CoordinationError>, outcome: &str) -> bool {
    matches!(r, Err(e) if e.kind == CoordinationErrorKind::Retryable
        && e.to_string().contains(&format!("the run is stopping; {outcome}")))
}

/// A stop that begins while a commit waits on the store ends the wait with a
/// `Retryable` answer naming the stop. Regression for #962.
#[test]
fn a_stop_cancels_a_commit_waiting_on_the_store() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let release = store.hold("split.c0", &flag);

    let started = Instant::now();
    let r = a.commit(&support::split_id("c0"), &SplitProgress::new(5, vec![]));
    let took = started.elapsed();

    assert!(is_stopping(&r, "this command may still land"), "{r:?}");
    assert!(took < Duration::from_secs(1), "took {took:?}");
    release.notify_one();
}

/// A commit made while the stop is set reaches no store.
#[test]
fn a_commit_at_the_stop_sends_nothing() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let before = store.updates(Keyspace::Durable, "split.c0");
    let watermark = store.record(&rt, "split.c0")["watermark"].clone();

    flag.store(true, Ordering::SeqCst);
    let r = a.commit(&support::split_id("c0"), &SplitProgress::new(5, vec![]));
    flag.store(false, Ordering::SeqCst);
    barrier(&mut a, "c1");

    assert!(is_stopping(&r, "nothing was sent"), "{r:?}");
    assert_eq!(store.updates(Keyspace::Durable, "split.c0"), before);
    assert_eq!(store.record(&rt, "split.c0")["watermark"], watermark);
}

/// A commit cut short by the stop and landing afterwards is served before the
/// final commit, which ignores the stop and stores the same watermark again.
#[test]
fn a_commit_cut_short_by_the_stop_lands_before_the_final_commit() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let release = store.hold("split.c0", &flag);
    let before = store.updates(Keyspace::Durable, "split.c0");

    let r = a.commit(&support::split_id("c0"), &SplitProgress::new(5, vec![]));
    assert!(is_stopping(&r, "this command may still land"), "{r:?}");
    release.notify_one();
    let results = a.commit_final(&[
        (support::split_id("c0"), SplitProgress::new(5, vec![])),
        (support::split_id("c1"), SplitProgress::new(9, vec![])),
    ]);

    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(store.record(&rt, "split.c0")["watermark"], 5);
    assert_eq!(store.record(&rt, "split.c1")["watermark"], 9);
    assert_eq!(store.updates(Keyspace::Durable, "split.c0"), before + 2);
}

/// A completing commit cut short by the stop that lands afterwards ends the
/// tenancy: the final commit for that split is `Fenced` and the store keeps
/// the completed record.
#[test]
fn a_completing_commit_cut_short_by_the_stop_fences_the_final_commit() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let release = store.hold("split.c0", &flag);

    let r = a.commit(
        &support::split_id("c0"),
        &SplitProgress::completed(5, vec![]),
    );
    assert!(is_stopping(&r, "this command may still land"), "{r:?}");
    release.notify_one();
    let results = a.commit_final(&[(support::split_id("c0"), SplitProgress::new(5, vec![]))]);

    assert!(
        matches!(&results[..], [Err(e)] if e.kind == CoordinationErrorKind::Fenced),
        "{results:?}"
    );
    let record = store.record(&rt, "split.c0");
    assert_eq!(record["watermark"], 5);
    assert_eq!(record["completed"], true);
}

/// A failure report made while the stop is set writes nothing and consumes no
/// delivery attempt; the departure hands the split back.
#[test]
fn a_failure_report_at_the_stop_sends_nothing() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let epoch = store.epoch(&rt, "split.c0");
    let before = store.updates(Keyspace::Durable, "split.c0");
    let attempts = store.record(&rt, "split.c0")["attempts"].clone();

    flag.store(true, Ordering::SeqCst);
    let r = a.fail(&support::split_id("c0"), epoch, "poison");
    flag.store(false, Ordering::SeqCst);
    barrier(&mut a, "c1");

    assert!(is_stopping(&r, "nothing was sent"), "{r:?}");
    assert_eq!(store.updates(Keyspace::Durable, "split.c0"), before);
    a.depart(&[]).unwrap();
    assert!(store.owned(&rt, &["split.c0", "split.c1"]).is_empty());
    assert_eq!(store.record(&rt, "split.c0")["attempts"], attempts);
}

/// A failure report cut short by the stop that lands afterwards consumes the
/// last attempt and quarantines the split, so the final commit for it is
/// `Fenced`.
#[test]
fn a_quarantining_failure_report_cut_short_by_the_stop_fences_the_final_commit() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let mut config = config_for(STOP_LEASE, Some("worker-a"));
    config.max_attempts = 1;
    let (mut a, flag) = stopping_worker_with(&rt, &store, config);
    let epoch = store.epoch(&rt, "split.c0");
    let attempts = store.record(&rt, "split.c0")["attempts"].as_u64().unwrap();
    let release = store.hold("split.c0", &flag);

    let r = a.fail(&support::split_id("c0"), epoch, "poison");
    assert!(is_stopping(&r, "this command may still land"), "{r:?}");
    release.notify_one();
    let results = a.commit_final(&[(support::split_id("c0"), SplitProgress::new(5, vec![]))]);

    assert!(
        matches!(&results[..], [Err(e)] if e.kind == CoordinationErrorKind::Fenced),
        "{results:?}"
    );
    let record = store.record(&rt, "split.c0");
    assert_eq!(record["attempts"], attempts + 1, "{record}");
    assert_eq!(record["status"], "quarantined", "{record}");
}

/// A failure report cut short by the stop that lands afterwards, with attempts
/// left, lets this worker claim the split again; a final commit made before the
/// new claim is polled is `Fenced` and writes nothing, as is one made with no
/// tenancy on record, and one made after the claim is polled lands. Regression
/// for #1014.
#[test]
fn a_final_commit_after_a_cut_short_failure_report_and_a_claim_again_is_fenced() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let epoch = store.epoch(&rt, "split.c0");
    let attempts = store.record(&rt, "split.c0")["attempts"].as_u64().unwrap();
    let release = store.hold("split.c0", &flag);

    let r = a.fail(&support::split_id("c0"), epoch, "poison");
    assert!(is_stopping(&r, "this command may still land"), "{r:?}");
    release.notify_one();
    spate_test::wait_until(support::DEADLINE, "worker-a to claim c0 again", || {
        let record = store.record(&rt, "split.c0");
        record["owner"] == "worker-a" && record["epoch"].as_u64() > Some(epoch.0)
    });
    let claimed = store.record(&rt, "split.c0");
    let results = a.commit_final(&[(support::split_id("c0"), SplitProgress::new(5, vec![]))]);

    assert!(
        matches!(&results[..], [Err(e)] if e.kind == CoordinationErrorKind::Fenced),
        "{results:?}"
    );
    let record = store.record(&rt, "split.c0");
    assert_eq!(record["watermark"], claimed["watermark"], "{record}");
    assert_eq!(record["owner"], "worker-a", "{record}");
    assert_eq!(record["epoch"], claimed["epoch"], "{record}");
    assert_eq!(record["attempts"], attempts + 1, "{record}");

    let updates = store.updates(Keyspace::Durable, "split.c0");
    let results = a.commit_final(&[(support::split_id("c0"), SplitProgress::new(5, vec![]))]);
    assert!(
        matches!(&results[..], [Err(e)] if e.kind == CoordinationErrorKind::Fenced),
        "{results:?}"
    );
    assert_eq!(store.updates(Keyspace::Durable, "split.c0"), updates);

    let mut events = Vec::new();
    spate_test::wait_until(
        support::DEADLINE,
        "the new claim of c0 to be polled",
        || {
            events.extend(a.poll().expect("poll"));
            events.iter().any(|e| {
                matches!(e, CoordinationEvent::Gained { split, epoch: e, .. }
                if split.id.as_str() == "c0" && e.0 > epoch.0)
            })
        },
    );
    let results = a.commit_final(&[(support::split_id("c0"), SplitProgress::new(5, vec![]))]);
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(store.record(&rt, "split.c0")["watermark"], 5);
}

/// A claim again after a failure report cut short by the stop, polled before
/// the final commit with no `Lost` between, moves the commit to the new tenancy
/// and the commit lands.
#[test]
fn a_final_commit_after_a_claim_again_is_polled_lands() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let epoch = store.epoch(&rt, "split.c0");
    let release = store.hold("split.c0", &flag);

    let r = a.fail(&support::split_id("c0"), epoch, "poison");
    assert!(is_stopping(&r, "this command may still land"), "{r:?}");
    release.notify_one();
    let mut events = Vec::new();
    spate_test::wait_until(
        support::DEADLINE,
        "the new claim of c0 to be polled",
        || {
            events.extend(a.poll().expect("poll"));
            events.iter().any(|e| {
                matches!(e, CoordinationEvent::Gained { split, epoch: e, .. }
                if split.id.as_str() == "c0" && e.0 > epoch.0)
            })
        },
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, CoordinationEvent::Lost { split } if split.as_str() == "c0")),
        "{events:?}"
    );
    let results = a.commit_final(&[(support::split_id("c0"), SplitProgress::new(5, vec![]))]);

    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(store.record(&rt, "split.c0")["watermark"], 5);
}

/// A final commit batch carries each split's own tenancy: with `c0` claimed
/// again at a later epoch and `c1` still at its first, both commits land.
#[test]
fn a_final_commit_batch_stamps_each_split_with_its_own_tenancy() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let epoch = store.epoch(&rt, "split.c0");
    let release = store.hold("split.c0", &flag);

    let r = a.fail(&support::split_id("c0"), epoch, "poison");
    assert!(is_stopping(&r, "this command may still land"), "{r:?}");
    release.notify_one();
    let mut events = Vec::new();
    spate_test::wait_until(
        support::DEADLINE,
        "the new claim of c0 to be polled",
        || {
            events.extend(a.poll().expect("poll"));
            events.iter().any(|e| {
                matches!(e, CoordinationEvent::Gained { split, epoch: e, .. }
                if split.id.as_str() == "c0" && e.0 > epoch.0)
            })
        },
    );
    assert_ne!(store.epoch(&rt, "split.c1"), store.epoch(&rt, "split.c0"));
    let results = a.commit_final(&[
        (support::split_id("c1"), SplitProgress::new(3, vec![])),
        (support::split_id("c0"), SplitProgress::new(5, vec![])),
    ]);

    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(store.record(&rt, "split.c1")["watermark"], 3);
    assert_eq!(store.record(&rt, "split.c0")["watermark"], 5);
}

/// A drain release made while the stop is set sends nothing: the split keeps
/// its record until the departure hands it back.
#[test]
fn a_drain_release_at_the_stop_sends_nothing() {
    let rt = runtime();
    let store = FaultStore::new(STOP_LEASE);
    let (mut a, flag) = stopping_worker(&rt, &store);
    let before = store.updates(Keyspace::Durable, "split.c0");
    let epoch = store.epoch(&rt, "split.c0");

    flag.store(true, Ordering::SeqCst);
    let released = a.release_drained(&[support::split_id("c0")]);
    flag.store(false, Ordering::SeqCst);
    barrier(&mut a, "c1");

    assert!(released.is_ok(), "{released:?}");
    assert_eq!(store.updates(Keyspace::Durable, "split.c0"), before);
    assert_eq!(store.epoch(&rt, "split.c0"), epoch);
    a.depart(&[]).unwrap();
    assert!(store.owned(&rt, &["split.c0", "split.c1"]).is_empty());
}

/// A revocation decline made while the stop is set sends nothing, so the
/// revocation stays pending and this worker keeps the split.
#[test]
fn a_decline_at_the_stop_leaves_the_revocation_pending() {
    use spate_core::coordination::CoordinationEvent;
    let rt = runtime();
    let store = FaultStore::new(LEASE);
    let ids = ["v0", "v1", "v2", "v3"];
    let mut config = config_for(LEASE, Some("worker-a"));
    config.reconcile_interval = NO_RECONCILE;
    config.drain_deadline = LEASE * 20;
    let mut a = holding(&rt, &store, config, &ids);
    let flag = Arc::new(AtomicBool::new(false));
    a.set_stop(StopSignal::new(Arc::clone(&flag)));
    let mut b = StoreCoordinator::new(
        store.clone(),
        config_for(LEASE, Some("worker-b")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    b.start(Box::new(PhasedPlanner::one_final("departure:v1", &ids)))
        .unwrap();
    let mut held_b = Held::default();
    let deadline = Instant::now() + support::DEADLINE;
    let revoked = loop {
        assert!(Instant::now() < deadline, "no revocation was requested");
        let asked = a.poll().unwrap().into_iter().find_map(|event| match event {
            CoordinationEvent::RevokeRequested { split } => Some(split),
            _ => None,
        });
        held_b.fold(b.poll().unwrap());
        if let Some(split) = asked {
            break split;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    };
    let key = format!("split.{}", revoked.as_str());
    let other = ids
        .into_iter()
        .find(|id| *id != revoked.as_str())
        .expect("another split");
    let epoch = store.epoch(&rt, &key);

    flag.store(true, Ordering::SeqCst);
    let declined = a.decline_revoke(&revoked);
    flag.store(false, Ordering::SeqCst);
    barrier(&mut a, other);

    assert!(declined.is_ok(), "{declined:?}");
    let record = store.record(&rt, &key);
    assert_eq!(record["owner"], "worker-a", "{record}");
    assert_eq!(store.epoch(&rt, &key), epoch);
    let lost = a
        .poll()
        .unwrap()
        .into_iter()
        .any(|e| matches!(&e, CoordinationEvent::Lost { split } if *split == revoked));
    assert!(!lost, "the revocation was forced");
}
