//! Fault-injection regressions: scripted store failures at the exact
//! writes whose loss used to wedge the protocol. Both scenarios run over
//! the real coordinator through the public API; the fault store is a
//! [`CoordinationStore`] like any custom backend.

mod support;

use futures_util::StreamExt;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use spate_coordination::{CoordinationEvent, SplitCoordinator, SplitProgress, StoreCoordinator};
use spate_core::clock::tokio::Clock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::{DEADLINE, Held, PhasedPlanner, TestClock, config, runtime, split_id};

/// A [`MemoryStore`] with scripted faults on specific writes.
#[derive(Clone)]
struct FaultStore {
    inner: MemoryStore,
    /// Per-call script for durable updates of the plan record: `true`
    /// fails the call (Retryable, nothing written). Exhausted = pass.
    plan_update_script: Arc<Mutex<VecDeque<bool>>>,
    /// Once: the next ephemeral split-lease update WRITES but returns an
    /// error, the maybe-landed renewal.
    lease_maybe_land: Arc<AtomicBool>,
    /// Split-lease updates that won since the maybe-landed renewal.
    renewals_after_fault: Arc<AtomicU64>,
    /// While armed: the next durable split-record write that clears the
    /// owner (a graceful release, or a revocation's final hand-back) is
    /// dropped (Retryable, nothing written), disarming afterward.
    /// Owner-setting writes (claims, commits) are untouched, so it targets
    /// exactly the release CAS.
    drop_owner_clear: Arc<AtomicBool>,
    /// Once: the next durable `assign.` write is dropped, so the leader
    /// believes it has told a worker something it never heard. Nothing is
    /// wedged by that alone; the point under test is that the leader
    /// republishes rather than treating the fleet as informed.
    drop_assignment_publish: Arc<AtomicBool>,
}

impl FaultStore {
    fn new(inner: MemoryStore) -> FaultStore {
        FaultStore {
            inner,
            plan_update_script: Arc::new(Mutex::new(VecDeque::new())),
            lease_maybe_land: Arc::new(AtomicBool::new(false)),
            renewals_after_fault: Arc::new(AtomicU64::new(0)),
            drop_owner_clear: Arc::new(AtomicBool::new(false)),
            drop_assignment_publish: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl CoordinationStore for FaultStore {
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
        if ks == Keyspace::Durable
            && key == "plan"
            && self
                .plan_update_script
                .lock()
                .expect("script")
                .pop_front()
                .unwrap_or(false)
        {
            return Err(StoreError::Retryable("injected: plan write dropped".into()));
        }
        if ks == Keyspace::Durable
            && key.starts_with("split.")
            && self.drop_owner_clear.load(Ordering::Acquire)
            && serde_json::from_slice::<serde_json::Value>(&value)
                .ok()
                .and_then(|v| v.get("owner").map(serde_json::Value::is_null))
                .unwrap_or(false)
        {
            self.drop_owner_clear.store(false, Ordering::Release);
            return Err(StoreError::Retryable(
                "injected: revocation release write dropped".into(),
            ));
        }
        if ks == Keyspace::Durable
            && key.starts_with("assign.")
            && self.drop_assignment_publish.swap(false, Ordering::AcqRel)
        {
            // The leader's assignment write never lands. Nothing is
            // wedged by this on its own; the leader must notice and
            // republish rather than believing the fleet was told.
            return Err(StoreError::Retryable(
                "injected: assignment publish dropped".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key.starts_with("split.")
            && self.lease_maybe_land.swap(false, Ordering::AcqRel)
        {
            // The write LANDS but the caller sees a failure, the
            // maybe-landed renewal a flaky round-trip produces.
            let _ = self.inner.update(ks, key, value, expected).await?;
            self.renewals_after_fault.store(0, Ordering::Release);
            return Err(StoreError::Retryable(
                "injected: renewal reply lost after the write landed".into(),
            ));
        }
        let outcome = self.inner.update(ks, key, value, expected).await?;
        if ks == Keyspace::Ephemeral
            && key.starts_with("split.")
            && matches!(outcome, CasOutcome::Won(_))
        {
            self.renewals_after_fault.fetch_add(1, Ordering::AcqRel);
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

/// A failed plan publish must not desynchronize terminal detection: the
/// splits were already seeded, so `planned` must be recounted from the
/// store on the next run. Accounting by `planned += creates won this run`
/// made the re-run count zero, the totals never matched again, and a
/// bounded job idled forever instead of draining.
#[test]
fn failed_plan_publish_heals_and_the_job_still_completes() {
    let rt = runtime();
    let store = FaultStore::new(MemoryStore::new(support::LEASE));
    // Plan-record updates: [generation bump: ok, first publish: FAILS].
    store
        .plan_update_script
        .lock()
        .expect("script")
        .extend([false, true]);

    let planner = Box::new(PhasedPlanner::one_final("publish-fault:v1", &["p0", "p1"]));
    let mut worker = StoreCoordinator::new(store, config(Some("solo")), rt.handle().clone(), None)
        .expect("coordinator");
    worker.start(planner).unwrap();

    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming both splits", |h| {
        h.splits.len() == 2
    });
    for id in ["p0", "p1"] {
        worker
            .commit(&split_id(id), &SplitProgress::completed(100, vec![]))
            .unwrap();
    }
    // The replan tick recounts the seeded records and publishes Final;
    // without the recount this drive times out; nothing ever fires.
    support::drive(&mut worker, &mut held, "healing the failed publish", |h| {
        h.all_complete
    });
}

/// A renewal whose write lands but whose reply is lost must be ADOPTED on
/// the next heartbeat (the lease still carries our owner+nonce), not
/// treated as a fence. Dropping the split as Lost and re-acquiring it
/// through an attempt-consuming reclaim let four flakes quarantine a
/// healthy split.
#[test]
fn maybe_landed_renewal_is_adopted_not_fenced() {
    let rt = runtime();
    // Freeze time. Every protocol deadline reads this clock (lease expiry,
    // the self-fence, AND the renewal cadence), so nothing fires until the
    // test advances it. That makes the negative assertion below meaningful:
    // a Lost/Quarantined can only come from mishandling the maybe-landed
    // renewal, never from a CI scheduler stall. We drive the renewals
    // ourselves by stepping the clock, one fraction of a renew-interval at a
    // time so a live worker always gets to renew before the self-fence would
    // fire (the "advance to settle" pattern; see `support::TestClock`).
    let clock = support::TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let lease_maybe_land = store.lease_maybe_land.clone();
    let renewals_after_fault = store.renewals_after_fault.clone();

    let planner = Box::new(PhasedPlanner::one_final("renewal-fault:v1", &["r0"]));
    let mut worker = StoreCoordinator::with_clock(
        store,
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker.start(planner).unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);

    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    // Poll the worker once, folding events and asserting the split is never
    // dropped. `RefCell` so it can be called from inside the `advance_stepped`
    // step closure, which already borrows the clock.
    let held = std::cell::RefCell::new(held);
    let worker = std::cell::RefCell::new(worker);
    let pump = || {
        fleet.settle(&clock);
        for event in worker.borrow_mut().poll().expect("poll") {
            assert!(
                !matches!(
                    event,
                    CoordinationEvent::Lost { .. } | CoordinationEvent::Quarantined { .. }
                ),
                "a maybe-landed renewal must not cost the split: {event:?}"
            );
            held.borrow_mut().fold(vec![event]);
        }
    };

    // Arm the fault: the next lease renewal writes but reports an error, and
    // the one after loses its CAS against that landed write and must ADOPT it
    // (not fence). Step a renew-interval per iteration until the fault has
    // fired (the flag clears), then until a renewal wins against the adopted
    // revision, all under the no-loss assertion. A step of a quarter
    // renew-interval keeps `last_ok_write` within a lease throughout, so a
    // genuine self-fence never masquerades as the loss we are refuting.
    lease_maybe_land.store(true, Ordering::Release);
    let renew = support::LEASE / 3;
    let deadline = Instant::now() + DEADLINE;
    while lease_maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        clock.advance_stepped(renew, renew / 4, pump);
    }
    while renewals_after_fault.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline, "no renewal won after the fault");
        clock.advance_stepped(renew, renew / 4, pump);
    }

    let mut held = held.into_inner();
    let mut worker = worker.into_inner();
    assert_eq!(held.splits.len(), 1, "still held");
    // The tenancy is intact: the fenced commit path would reject this.
    worker
        .commit(&split_id("r0"), &SplitProgress::completed(7, vec![]))
        .unwrap();
    // The completion sweep runs on a reconcile tick, which is clock-driven;
    // keep stepping the frozen clock so it fires.
    support::drive_clocked(
        &mut worker,
        &clock,
        &mut held,
        "completing after the flake",
        |h| h.all_complete,
    );
}

/// A dropped assignment publish must be republished, not treated as
/// delivered. The leader caches the revision it believes each
/// `assign.{instance}` record holds so it can CAS the next one; if a
/// failed write left that cache believing a write it never made, every
/// later publish for that instance would CAS against a revision the store
/// never had and lose forever, and the worker, never having seen a
/// record, would hold nothing and claim nothing.
#[test]
fn a_dropped_assignment_publish_is_republished() {
    let rt = runtime();
    let store = FaultStore::new(MemoryStore::new(support::LEASE));
    let drop_publish = store.drop_assignment_publish.clone();
    let ids = ["a0", "a1"];
    let planner = || Box::new(PhasedPlanner::one_final("assign-fault:v1", &ids));

    // Arm before the worker starts, so the very first publish is the one
    // that vanishes.
    drop_publish.store(true, Ordering::Release);
    let mut worker = StoreCoordinator::new(store, config(Some("solo")), rt.handle().clone(), None)
        .expect("coordinator");
    worker.start(planner()).unwrap();

    let mut held = Held::default();
    support::drive(
        &mut worker,
        &mut held,
        "republishing the lost assignment",
        |h| h.splits.len() == ids.len(),
    );
    assert!(
        !drop_publish.load(Ordering::Acquire),
        "the fault never fired, so this proved nothing"
    );
}

/// A revocation release whose owner-clear write is dropped still gives the
/// split up: the lease key goes with it, so a peer takes over on expiry
/// rather than the split staying pinned to a worker the leader has already
/// stopped assigning it to. The replay that costs is the price of the
/// dropped write, not of the protocol.
#[test]
fn a_dropped_release_write_still_surrenders_the_split() {
    let rt = runtime();
    let store = FaultStore::new(MemoryStore::new(support::LEASE));
    let drop_clear = store.drop_owner_clear.clone();
    let ids = ["d0", "d1"];
    let planner = || Box::new(PhasedPlanner::one_final("release-fault:v1", &ids));

    let mut a = StoreCoordinator::new(
        store.clone(),
        config(Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    support::drive(&mut a, &mut held_a, "worker-a takes the plan", |h| {
        h.splits.len() == ids.len()
    });
    support::commit_held(&mut a, &held_a);

    // B joins: the leader revokes one split, and A's release write is the
    // one that gets dropped.
    drop_clear.store(true, Ordering::Release);
    let mut b = StoreCoordinator::new(store, config(Some("worker-b")), rt.handle().clone(), None)
        .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    let deadline = Instant::now() + DEADLINE;
    while held_b.splits.is_empty() {
        assert!(
            Instant::now() < deadline,
            "a dropped release write pinned the split to worker-a forever"
        );
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
        support::commit_held(&mut a, &held_a);
        support::consent_to_revocations(&mut a, &mut held_a);
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        !drop_clear.load(Ordering::Acquire),
        "the fault never fired, so this proved nothing"
    );
    assert!(
        held_a.splits.keys().all(|k| !held_b.splits.contains_key(k)),
        "the split is held twice: a={:?} b={:?}",
        held_a.splits.keys().collect::<Vec<_>>(),
        held_b.splits.keys().collect::<Vec<_>>()
    );
}

/// How [`ReleaseStore`] treats the lease delete of its armed split key.
#[derive(Clone, Copy)]
enum LeaseDeleteFault {
    /// Delete, then hold the call until another worker owns the split
    /// record.
    HoldUntilClaimed,
    /// Return an error without deleting.
    Fail,
}

/// A [`MemoryStore`] that applies one [`LeaseDeleteFault`] to the lease of
/// the armed split key.
#[derive(Clone)]
struct ReleaseStore {
    inner: MemoryStore,
    fault: LeaseDeleteFault,
    armed: Arc<Mutex<Option<String>>>,
    fired: Arc<AtomicBool>,
    /// The foreign owner the hold observed before returning.
    claimed_by: Arc<Mutex<Option<String>>>,
}

impl ReleaseStore {
    fn new(inner: MemoryStore, fault: LeaseDeleteFault) -> ReleaseStore {
        ReleaseStore {
            inner,
            fault,
            armed: Arc::new(Mutex::new(None)),
            fired: Arc::new(AtomicBool::new(false)),
            claimed_by: Arc::new(Mutex::new(None)),
        }
    }

    /// The first owner other than worker-a that the split record carries.
    async fn foreign_owner(&self, key: &str) -> Option<String> {
        let mut watch = self.inner.watch(Keyspace::Durable, key).await.ok()?;
        while let Some(Ok(event)) = watch.next().await {
            if let WatchEvent::Put(entry) = event
                && entry.key == key
                && let Some(owner) = record_json(&entry.value)["owner"].as_str()
                && owner != "worker-a"
            {
                return Some(owner.to_string());
            }
        }
        None
    }
}

impl CoordinationStore for ReleaseStore {
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
        let armed =
            ks == Keyspace::Ephemeral && self.armed.lock().expect("armed").as_deref() == Some(key);
        if !armed {
            return self.inner.delete(ks, key, expected).await;
        }
        self.fired.store(true, Ordering::Release);
        match self.fault {
            LeaseDeleteFault::Fail => Err(StoreError::Retryable(
                "injected: lease delete dropped".into(),
            )),
            LeaseDeleteFault::HoldUntilClaimed => {
                let outcome = self.inner.delete(ks, key, expected).await?;
                *self.claimed_by.lock().expect("claimed by") = self.foreign_owner(key).await;
                Ok(outcome)
            }
        }
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

fn record_json(value: &[u8]) -> serde_json::Value {
    serde_json::from_slice(value).expect("record json")
}

/// Starts worker-a on `store` and worker-b on the store beneath it, both on
/// `clock`, arms the fault on the split the leader revokes from worker-a,
/// and drops worker-a so the direct release hands it back. Returns the
/// split and its record once worker-b holds it.
fn drop_during_revocation(
    store: &ReleaseStore,
    clock: &Arc<TestClock>,
) -> (String, serde_json::Value) {
    let rt = runtime();
    let ids = ["r0", "r1"];
    let planner = || Box::new(PhasedPlanner::one_final("direct-release:v1", &ids));
    // Both workers are alive while the clock moves, so each step stays
    // under a renew interval.
    let step = support::LEASE / 12;

    // A drain deadline the test never reaches, so the revocation is not
    // forced through the task before the drop.
    let mut cfg_a = config(Some("worker-a"));
    cfg_a.drain_deadline = DEADLINE;
    let mut a = StoreCoordinator::with_clock(
        store.clone(),
        cfg_a,
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut fleet = support::Fleet::new(&store.inner, rt.handle());
    fleet.join(&a);
    let mut held_a = Held::default();
    let deadline = Instant::now() + DEADLINE;
    while held_a.splits.len() < ids.len() {
        assert!(Instant::now() < deadline, "worker-a never took the plan");
        fleet.step(clock, step);
        held_a.fold(a.poll().expect("poll a"));
    }
    support::commit_held(&mut a, &held_a);

    let mut b =
        support::worker_with_clock(&store.inner, rt.handle(), Some("worker-b"), clock.clone());
    b.start(planner()).unwrap();
    fleet.join(&b);
    let mut held_b = Held::default();
    while held_a.revoke_requests.is_empty() {
        assert!(Instant::now() < deadline, "no revocation was requested");
        fleet.step(clock, step);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
    }
    let moved = held_a.revoke_requests[0].clone();
    let key = format!("split.{moved}");
    *store.armed.lock().expect("armed") = Some(key.clone());

    // `Drop` runs the direct release and blocks, so it gets its own thread.
    // The clock does not move until it returns.
    fleet.forget("worker-a");
    let (dropped_tx, dropped) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        drop(a);
        let _ = dropped_tx.send(());
    });
    dropped
        .recv_timeout(DEADLINE)
        .expect("the direct release never returned");
    assert!(
        store.fired.load(Ordering::Acquire),
        "the direct release never deleted the lease of {moved}, so this proved nothing"
    );

    while !held_b.splits.contains_key(&moved) {
        assert!(Instant::now() < deadline, "worker-b never took {moved}");
        fleet.step(clock, step);
        held_b.fold(b.poll().expect("poll b"));
    }
    let entry = rt
        .block_on(store.inner.get(Keyspace::Durable, &key))
        .expect("get")
        .expect("split record");
    (moved, record_json(&entry.value))
}

/// A peer that claims a split before the direct release handing it back
/// returns consumes no delivery attempt. Regression for #759.
#[test]
fn a_claim_during_a_direct_release_consumes_no_attempt() {
    let clock = TestClock::frozen();
    let store = ReleaseStore::new(
        support::store_with_clock(clock.clone()),
        LeaseDeleteFault::HoldUntilClaimed,
    );
    let (moved, record) = drop_during_revocation(&store, &clock);
    assert_eq!(
        store.claimed_by.lock().expect("claimed by").as_deref(),
        Some("worker-b"),
        "worker-b did not claim {moved} before the direct release returned, so this proved nothing"
    );
    assert_eq!(
        record["attempts"], 0,
        "a graceful direct release charged {moved} a delivery attempt: {record}"
    );
}

/// A direct release whose lease delete fails still hands the split back
/// without an attempt: the peer claims it once the lease expires.
#[test]
fn a_direct_release_whose_lease_delete_fails_consumes_no_attempt() {
    let clock = TestClock::frozen();
    let store = ReleaseStore::new(
        support::store_with_clock(clock.clone()),
        LeaseDeleteFault::Fail,
    );
    let (moved, record) = drop_during_revocation(&store, &clock);
    assert_eq!(
        record["attempts"], 0,
        "a direct release with a failed lease delete charged {moved} a delivery attempt: {record}"
    );
}
