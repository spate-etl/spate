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
use spate_coordination::{
    CoordinationConfig, CoordinationEvent, SplitCoordinator, SplitProgress, StoreCoordinator,
};
use spate_core::clock::tokio::Clock;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::{DEADLINE, Held, PhasedPlanner, TestClock, config, runtime, split_id};

/// The clock a stalled write advances, and by how much.
type Stall = (Arc<TestClock>, Duration);

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
    /// The split lease the maybe-landed renewal wrote.
    lease_fault_key: Arc<Mutex<Option<String>>>,
    /// Split-lease update calls per key.
    lease_updates: Arc<Mutex<BTreeMap<String, u64>>>,
    /// The split lease before the maybe-landed renewal, for
    /// `lease_stale_read`.
    lease_before: Arc<Mutex<Option<Entry>>>,
    /// Once: the next read of the faulted split lease answers with the
    /// lease as it stood before the maybe-landed renewal.
    lease_stale_read: Arc<AtomicBool>,
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
    /// Once: the next leader-key update writes but returns an error.
    leader_maybe_land: Arc<AtomicBool>,
    /// When set, the maybe-landed leader write advances this clock by this
    /// much before it returns, as a reply lost to `op_timeout` does.
    leader_stall: Arc<Mutex<Option<Stall>>>,
    /// Once: the next leader-key read after the maybe-landed write answers
    /// with the key as it stood before that write.
    leader_stale_read: Arc<AtomicBool>,
    /// The leader key before the maybe-landed write, for `leader_stale_read`.
    leader_before: Arc<Mutex<Option<Entry>>>,
    /// Once: the next leader-key read fails.
    leader_read_fails: Arc<AtomicBool>,
    /// Once: after a leader-key read, the next leader-key update fails
    /// without writing.
    leader_update_after_read_fails: Arc<AtomicBool>,
    /// Armed by the read `leader_update_after_read_fails` waits for.
    leader_update_fail_armed: Arc<AtomicBool>,
}

impl FaultStore {
    fn new(inner: MemoryStore) -> FaultStore {
        FaultStore {
            inner,
            plan_update_script: Arc::new(Mutex::new(VecDeque::new())),
            lease_maybe_land: Arc::new(AtomicBool::new(false)),
            renewals_after_fault: Arc::new(AtomicU64::new(0)),
            lease_fault_key: Arc::default(),
            lease_updates: Arc::default(),
            lease_before: Arc::default(),
            lease_stale_read: Arc::default(),
            drop_owner_clear: Arc::new(AtomicBool::new(false)),
            drop_assignment_publish: Arc::new(AtomicBool::new(false)),
            leader_maybe_land: Arc::default(),
            leader_stall: Arc::default(),
            leader_stale_read: Arc::default(),
            leader_before: Arc::default(),
            leader_read_fails: Arc::default(),
            leader_update_after_read_fails: Arc::default(),
            leader_update_fail_armed: Arc::default(),
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
        if ks == Keyspace::Ephemeral && key.starts_with("split.") {
            *self
                .lease_updates
                .lock()
                .expect("updates")
                .entry(key.to_string())
                .or_default() += 1;
        }
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
            *self.lease_before.lock().expect("before") = self.inner.get(ks, key).await?;
            let _ = self.inner.update(ks, key, value, expected).await?;
            self.renewals_after_fault.store(0, Ordering::Release);
            *self.lease_fault_key.lock().expect("fault key") = Some(key.to_string());
            return Err(StoreError::Retryable(
                "injected: renewal reply lost after the write landed".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key == "leader"
            && self.leader_update_fail_armed.swap(false, Ordering::AcqRel)
        {
            return Err(StoreError::Retryable(
                "injected: leader update failed".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key == "leader"
            && self.leader_maybe_land.swap(false, Ordering::AcqRel)
        {
            *self.leader_before.lock().expect("before") = self.inner.get(ks, key).await?;
            let _ = self.inner.update(ks, key, value, expected).await?;
            if let Some((clock, by)) = self.leader_stall.lock().expect("stall").clone() {
                clock.advance(by);
            }
            return Err(StoreError::Retryable(
                "injected: leader renewal reply lost after the write landed".into(),
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
        if ks == Keyspace::Ephemeral
            && self.lease_fault_key.lock().expect("fault key").as_deref() == Some(key)
            && self.lease_stale_read.swap(false, Ordering::AcqRel)
        {
            return Ok(self.lease_before.lock().expect("before").clone());
        }
        if ks == Keyspace::Ephemeral && key == "leader" {
            if self.leader_read_fails.swap(false, Ordering::AcqRel) {
                return Err(StoreError::Retryable("injected: leader read failed".into()));
            }
            if self
                .leader_update_after_read_fails
                .swap(false, Ordering::AcqRel)
            {
                self.leader_update_fail_armed.store(true, Ordering::Release);
            }
            let before = self.leader_before.lock().expect("before").take();
            if let Some(before) = before
                && self.leader_stale_read.swap(false, Ordering::AcqRel)
            {
                return Ok(Some(before));
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

/// A solo worker on a frozen clock holding `r0` and `r1`, stepped until a
/// lease renewal applies with its reply lost, then `act` on that split's id.
/// With `stale_read`, the first read of the lease after the fault answers
/// from before the renewal. Returns the lease and the split record after
/// `act`.
fn after_unseen_lease_renewal(
    stale_read: bool,
    act: impl FnOnce(&mut StoreCoordinator<FaultStore>, &str),
) -> (Option<String>, serde_json::Value) {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let maybe_land = store.lease_maybe_land.clone();
    let fault_key = store.lease_fault_key.clone();
    let updates = store.lease_updates.clone();
    let stale = store.lease_stale_read.clone();
    let mut worker = StoreCoordinator::with_clock(
        store,
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "lease-fault:v1",
            &["r0", "r1"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming both splits", |h| {
        h.splits.len() == 2
    });

    // Step a twelfth of a lease at a time, so no second heartbeat runs
    // between the fault and `act`.
    maybe_land.store(true, Ordering::Release);
    let deadline = Instant::now() + DEADLINE;
    while maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        held.fold(worker.poll().expect("poll"));
    }
    let key = fault_key
        .lock()
        .unwrap()
        .clone()
        .expect("the faulted lease");
    let renewals = updates.lock().unwrap()[&key];
    stale.store(stale_read, Ordering::Release);
    act(
        &mut worker,
        key.strip_prefix("split.").expect("a split key"),
    );

    assert_eq!(
        updates.lock().unwrap()[&key],
        renewals,
        "a renewal ran between the fault and the act"
    );
    assert!(
        !stale.load(Ordering::Acquire),
        "the stale read was not served"
    );
    let lease = rt
        .block_on(inner.get(Keyspace::Ephemeral, &key))
        .unwrap()
        .map(|e| String::from_utf8_lossy(&e.value).into_owned());
    let record = rt
        .block_on(inner.get(Keyspace::Durable, &key))
        .unwrap()
        .expect("split record");
    (lease, record_json(&record.value))
}

/// A release right after a lease renewal that applied with its reply lost
/// deletes that lease.
/// Regression for #865.
#[test]
fn a_release_after_an_unseen_lease_renewal_deletes_the_lease() {
    let (lease, record) = after_unseen_lease_renewal(false, |worker, _| {
        worker
            .release(&[split_id("r0"), split_id("r1")])
            .expect("release");
    });
    assert!(lease.is_none(), "the lease was left: {lease:?}");
    assert!(record["owner"].is_null());
}

/// A completing commit right after a lease renewal that applied with its
/// reply lost deletes that lease.
/// Regression for #865.
#[test]
fn a_completion_after_an_unseen_lease_renewal_deletes_the_lease() {
    let (lease, _) = after_unseen_lease_renewal(false, |worker, id| {
        worker
            .commit(&split_id(id), &SplitProgress::completed(1, vec![]))
            .expect("complete");
    });
    assert!(lease.is_none(), "the lease was left: {lease:?}");
}

/// A release whose lease read answers from before an unseen renewal leaves
/// the lease to expire.
#[test]
fn a_release_whose_lease_read_lags_the_renewal_leaves_the_lease() {
    let (lease, record) = after_unseen_lease_renewal(true, |worker, _| {
        worker
            .release(&[split_id("r0"), split_id("r1")])
            .expect("release");
    });
    assert!(lease.is_some(), "the lease was deleted");
    assert!(record["owner"].is_null());
}

/// A solo leader whose next leader-key renewal applies with its reply lost,
/// faulted further by `arm`, then stepped for two leases: asserts that the
/// plan generation did not move and the leader key still names the worker.
fn leader_keeps_leading_after(
    tune: impl FnOnce(&mut CoordinationConfig),
    arm: impl FnOnce(&FaultStore, &Arc<TestClock>),
) {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let maybe_land = store.leader_maybe_land.clone();
    arm(&store, &clock);

    let mut cfg = config(Some("solo"));
    tune(&mut cfg);
    let planner = Box::new(PhasedPlanner::one_final("leader-fault:v1", &["r0"]));
    let mut worker = StoreCoordinator::with_clock(
        store,
        cfg,
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
    let generation = plan_generation(&rt, &inner);

    let mut pump = || {
        fleet.settle(&clock);
        held.fold(worker.poll().expect("poll"));
    };
    maybe_land.store(true, Ordering::Release);
    let renew = support::LEASE / 3;
    let deadline = Instant::now() + DEADLINE;
    while maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        clock.advance_stepped(renew, renew / 4, &mut pump);
    }
    clock.advance_stepped(support::LEASE * 2, renew / 4, &mut pump);

    assert_eq!(
        plan_generation(&rt, &inner),
        generation,
        "the leader gave up leadership and was re-elected"
    );
    let leader = rt
        .block_on(inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap()
        .expect("leader key");
    assert_eq!(record_json(&leader.value)["owner"], "solo");
}

fn plan_generation(rt: &tokio::runtime::Runtime, store: &MemoryStore) -> u64 {
    let plan = rt
        .block_on(store.get(Keyspace::Durable, "plan"))
        .unwrap()
        .expect("plan record");
    record_json(&plan.value)["generation"].as_u64().unwrap()
}

/// A leader whose renewal applied with its reply lost keeps leading.
/// Regression for #865.
#[test]
fn a_leader_whose_renewal_applied_unseen_keeps_leading() {
    leader_keeps_leading_after(|_| {}, |_, _| {});
}

/// A leader whose renewal reply was lost to `op_timeout`, half a lease after
/// the write applied, renews before the key expires.
/// Regression for #865.
#[test]
fn a_leader_whose_renewal_reply_timed_out_keeps_leading() {
    leader_keeps_leading_after(
        |cfg| cfg.op_timeout = support::LEASE / 2,
        |store, clock| {
            *store.leader_stall.lock().unwrap() = Some((clock.clone(), support::LEASE / 2));
        },
    );
}

/// A leader whose read-back after a lost renewal comes from a replica behind
/// the applied write keeps leading.
/// Regression for #865.
#[test]
fn a_leader_whose_read_back_lags_keeps_leading() {
    leader_keeps_leading_after(
        |_| {},
        |store, _| store.leader_stale_read.store(true, Ordering::Release),
    );
}

/// A leader whose read-back after a lost renewal fails keeps leading.
#[test]
fn a_leader_whose_read_back_fails_keeps_leading() {
    leader_keeps_leading_after(
        |_| {},
        |store, _| store.leader_read_fails.store(true, Ordering::Release),
    );
}

/// A leader whose renewal after the read-back of a lost renewal fails keeps
/// leading.
#[test]
fn a_leader_whose_renewal_after_the_read_back_fails_keeps_leading() {
    leader_keeps_leading_after(
        |_| {},
        |store, _| {
            store
                .leader_update_after_read_fails
                .store(true, Ordering::Release)
        },
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

/// A store that opens a TCP connection on every operation once `io` is set,
/// as a client that dials per request does.
#[derive(Clone)]
struct DialingStore {
    inner: MemoryStore,
    io: Arc<AtomicBool>,
    peer: std::net::SocketAddr,
}

impl DialingStore {
    async fn dial(&self) -> Result<(), StoreError> {
        if self.io.load(Ordering::Acquire) {
            tokio::net::TcpStream::connect(self.peer)
                .await
                .map_err(|e| StoreError::Retryable(format!("dial: {e}")))?;
        }
        Ok(())
    }
}

impl CoordinationStore for DialingStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.dial().await?;
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.dial().await?;
        self.inner.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.dial().await?;
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.dial().await?;
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.dial().await?;
        self.inner.list(ks, prefix).await
    }
}

/// Dropping a coordinator hands its splits back through a store whose
/// client opens connections, so a peer claims them without waiting out the
/// lease.
#[test]
fn a_drop_time_release_can_open_connections() {
    let rt = runtime();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let store = DialingStore {
        inner: MemoryStore::new(support::LEASE),
        io: Arc::new(AtomicBool::new(false)),
        peer: listener.local_addr().unwrap(),
    };
    let mut w = StoreCoordinator::new(
        store.clone(),
        config(Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("dial:v1", &["d0"])))
        .unwrap();
    support::drive(&mut w, &mut Held::default(), "claiming d0", |h| {
        h.splits.len() == 1
    });

    store.io.store(true, Ordering::Release);
    drop(w);
    let record = rt
        .block_on(store.inner.get(Keyspace::Durable, "split.d0"))
        .unwrap()
        .expect("record");
    let record = record_json(&record.value);
    assert!(
        record["owner"].is_null(),
        "the split was not released: {record}"
    );
}
