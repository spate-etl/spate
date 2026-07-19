//! Fault-injection regressions: scripted store failures at the exact
//! writes whose loss used to wedge the protocol. Both scenarios run over
//! the real coordinator through the public API — the fault store is a
//! [`CoordinationStore`] like any custom backend.

mod support;

use etl_coordination::store::memory::MemoryStore;
use etl_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream,
};
use etl_coordination::{
    Clock, CoordinationEvent, SplitCoordinator, SplitProgress, StoreCoordinator,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::{DEADLINE, Held, PhasedPlanner, config, runtime, split_id};

/// A [`MemoryStore`] with scripted faults on specific writes.
#[derive(Clone)]
struct FaultStore {
    inner: MemoryStore,
    /// Per-call script for durable updates of the plan record: `true`
    /// fails the call (Retryable, nothing written). Exhausted = pass.
    plan_update_script: Arc<Mutex<VecDeque<bool>>>,
    /// Once: the next ephemeral split-lease update WRITES but returns an
    /// error — the maybe-landed renewal.
    lease_maybe_land: Arc<AtomicBool>,
    /// While armed: the next durable split-record write that clears the
    /// owner (a graceful release / handoff grant) is dropped (Retryable,
    /// nothing written), disarming afterward. Owner-setting writes (claims,
    /// commits) are untouched, so it targets exactly the release CAS.
    drop_owner_clear: Arc<AtomicBool>,
    /// While armed: every victim grant-ANNOTATION write (an ephemeral
    /// `handoff.` update carrying a non-null `granted`) is dropped, so a
    /// requester's key stays UNANNOTATED. Under grant attribution the
    /// re-arm-without-resetting-the-clock path applies only to an unannotated
    /// key (an annotated one that vanishes is read as served, not a blip), so
    /// dropping the annotation is what keeps that path under test.
    drop_grant_annotation: Arc<AtomicBool>,
}

impl FaultStore {
    fn new(inner: MemoryStore) -> FaultStore {
        FaultStore {
            inner,
            plan_update_script: Arc::new(Mutex::new(VecDeque::new())),
            lease_maybe_land: Arc::new(AtomicBool::new(false)),
            drop_owner_clear: Arc::new(AtomicBool::new(false)),
            drop_grant_annotation: Arc::new(AtomicBool::new(false)),
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
                "injected: handoff release write dropped".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key.starts_with("handoff.")
            && self.drop_grant_annotation.load(Ordering::Acquire)
            && serde_json::from_slice::<serde_json::Value>(&value)
                .ok()
                .and_then(|v| v.get("granted").map(|g| !g.is_null()))
                .unwrap_or(false)
        {
            // The victim's grant annotation never lands: the requester's key
            // stays unannotated, so its disappearance is a genuine TTL blip.
            return Err(StoreError::Retryable(
                "injected: grant annotation dropped".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key.starts_with("split.")
            && self.lease_maybe_land.swap(false, Ordering::AcqRel)
        {
            // The write LANDS but the caller sees a failure — the
            // maybe-landed renewal a flaky round-trip produces.
            let _ = self.inner.update(ks, key, value, expected).await?;
            return Err(StoreError::Retryable(
                "injected: renewal reply lost after the write landed".into(),
            ));
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

/// A failed plan publish must not desynchronize terminal detection: the
/// splits were already seeded, so `planned` must be recounted from the
/// store on the next run — under the pre-fix accounting (`planned +=
/// creates won this run`) the re-run counted zero, the totals never
/// matched again, and a bounded job idled forever instead of draining.
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
    // without the recount this drive times out — nothing ever fires.
    support::drive(&mut worker, &mut held, "healing the failed publish", |h| {
        h.all_complete
    });
}

/// A renewal whose write lands but whose reply is lost must be ADOPTED on
/// the next heartbeat (the lease still carries our owner+nonce), not
/// treated as a fence: the pre-fix path dropped the split as Lost and
/// re-acquired it through an attempt-consuming reclaim — four flakes
/// quarantined a healthy split.
#[test]
fn maybe_landed_renewal_is_adopted_not_fenced() {
    let rt = runtime();
    // Freeze time. The self-fence (task) and lease expiry (store) both read
    // this clock, so a real wall-clock stall — a renewal task starved past
    // the lease TTL under parallel CI load — can no longer expire the lease:
    // `clock.now() - last_ok_write` stays 0 while frozen. Only a genuine peer
    // fence could drop the split, and there is no peer here. Renewal *cadence*
    // still runs on real `.elapsed()`, so the maybe-landed fault below is
    // really exercised. This removes the wall-clock flake (#45) without
    // weakening what the test asserts.
    let clock: Arc<dyn Clock> = support::TestClock::frozen();
    let store = FaultStore::new(MemoryStore::with_clock(support::LEASE, clock.clone()));
    let lease_maybe_land = store.lease_maybe_land.clone();

    let planner = Box::new(PhasedPlanner::one_final("renewal-fault:v1", &["r0"]));
    let mut worker = StoreCoordinator::with_clock(
        store,
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock,
    )
    .expect("coordinator");
    worker.start(planner).unwrap();

    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });
    // Arm the fault: the next lease renewal writes but reports an error, and
    // the one after loses its CAS against the landed write and must ADOPT it
    // (not fence). Drive until the fault has fired (the flag clears), then a
    // settle window so the adopting renewal runs — asserting no
    // Lost/Quarantined throughout. Frozen time makes this robust: the
    // negative assertion can only be tripped by a real mishandling of the
    // maybe-landed renewal, never by a scheduler stall.
    lease_maybe_land.store(true, Ordering::Release);
    let deadline = Instant::now() + DEADLINE;
    let mut settle_until: Option<Instant> = None;
    loop {
        assert!(Instant::now() < deadline, "adoption never settled");
        for event in worker.poll().expect("poll") {
            assert!(
                !matches!(
                    event,
                    CoordinationEvent::Lost { .. } | CoordinationEvent::Quarantined { .. }
                ),
                "a maybe-landed renewal must not cost the split: {event:?}"
            );
            held.fold(vec![event]);
        }
        match settle_until {
            // Fault has fired: watch a few more renewal intervals so the
            // adopting renewal executes under the no-loss assertion.
            None if !lease_maybe_land.load(Ordering::Acquire) => {
                settle_until = Some(Instant::now() + support::LEASE);
            }
            Some(until) if Instant::now() >= until => break,
            _ => {}
        }
    }
    assert_eq!(held.splits.len(), 1, "still held");
    // The tenancy is intact: the fenced commit path would reject this.
    worker
        .commit(&split_id("r0"), &SplitProgress::completed(7, vec![]))
        .unwrap();
    support::drive(&mut worker, &mut held, "completing after the flake", |h| {
        h.all_complete
    });
}

/// A cooperative handoff whose durable release write is dropped must not
/// wedge: the victim has already removed the split from its working set and
/// dropped the lease, so the requester takes it over through the fallback
/// path (an attempt-costing Expired takeover here, since the lease is gone)
/// rather than hanging on a grant that never lands. Zero loss regardless.
#[test]
fn a_dropped_handoff_release_degrades_to_the_fallback_takeover() {
    let rt = runtime();
    let store = FaultStore::new(MemoryStore::new(support::LEASE));
    // Drop the first owner-clearing durable write (the handoff grant).
    store.drop_owner_clear.store(true, Ordering::Release);

    let planner = || Box::new(PhasedPlanner::one_final("drop-release:v1", &["d0", "d1"]));
    let mut a = StoreCoordinator::new(
        store.clone(),
        config(Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    support::drive(&mut a, &mut held_a, "A claiming both", |h| {
        h.splits.len() == 2
    });
    // Checkpoint both so a fallback takeover has a resume point.
    for id in ["d0", "d1"] {
        a.commit(&split_id(id), &SplitProgress::new(1, vec![]))
            .unwrap();
    }

    // B requests a handoff; A grants one split but the release's owner-clear
    // write is dropped once. A keeps its other split (so it stays at target
    // and does not race B for the released one); B must still end up owning
    // the granted split.
    let mut b = StoreCoordinator::new(
        store.clone(),
        config(Some("worker-b")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    let mut granted: Option<String> = None;
    let deadline = Instant::now() + DEADLINE;
    loop {
        assert!(
            Instant::now() < deadline,
            "the requester never took the dropped-release split over"
        );
        for event in a.poll().unwrap() {
            if let CoordinationEvent::HandoffRequested { split } = &event
                && granted.is_none()
            {
                granted = Some(split.as_str().to_string());
                // The owner-clear is faulted; ignore the deferred error.
                let _ = a.release_handoff(&[split_id(split.as_str())]);
            }
            held_a.fold(vec![event]);
        }
        held_b.fold(b.poll().unwrap());
        if let Some(g) = &granted
            && held_b.splits.contains_key(g)
        {
            break;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let granted = granted.unwrap();
    assert_eq!(
        held_b.splits[&granted].1.as_ref().map(|p| p.watermark),
        Some(1),
        "the takeover carries the committed watermark — no data lost to the dropped release"
    );
}

/// An UNANNOTATED request key that keeps disappearing (a per-bucket TTL blip,
/// a lost watch delete) must be re-armed WITHOUT resetting the fallback clock:
/// the round budget is anchored to the FIRST request, so the fenced steal
/// still fires within a bounded number of rounds. If a recreate reset the
/// clock, a flapping key would defer the fallback forever and this would time
/// out.
///
/// Under grant attribution this re-arm path applies only to an *unannotated*
/// key — once the victim annotates a grant, a vanished key is read as served
/// (the requester stands down, trusting the promised hand-over), not a blip.
/// So the victim's grant annotation is dropped here (`drop_grant_annotation`),
/// keeping the key unannotated and the blip path under test.
#[test]
fn a_lost_request_key_is_re_armed_without_resetting_the_fallback_clock() {
    let rt = runtime();
    let store = MemoryStore::new(support::LEASE);
    let ids = ["k0", "k1", "k2", "k3"];
    let planner = || Box::new(PhasedPlanner::one_final("rearm:v1", &ids));

    // A wraps the shared store so its grant annotations are dropped: B's
    // request key never gets annotated, so its deletions stay genuine blips.
    let a_store = FaultStore::new(store.clone());
    a_store.drop_grant_annotation.store(true, Ordering::Release);
    let mut a = StoreCoordinator::new(a_store, config(Some("worker-a")), rt.handle().clone(), None)
        .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    support::drive(&mut a, &mut held_a, "A claiming all four", |h| {
        h.splits.len() == 4
    });
    support::commit_held(&mut a, &held_a);

    // B requests a handoff of a split A will never grant (A ignores the
    // request, and its annotation write is faulted away). Every time B's
    // request key appears, delete it: a flapping key must not perpetually
    // defer the fallback.
    let mut b = StoreCoordinator::new(
        store.clone(),
        config(Some("worker-b")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    let deadline = Instant::now() + DEADLINE;
    let mut deletes = 0u32;
    let mut rearmed = false;
    loop {
        assert!(
            Instant::now() < deadline,
            "the fallback steal never fired: a re-armed request key reset the round clock"
        );
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
        let present = rt
            .block_on(store.get(Keyspace::Ephemeral, "handoff.worker-a"))
            .unwrap()
            .is_some();
        if present {
            if deletes > 0 {
                rearmed = true; // it came back after an earlier delete
            }
            let _ = rt.block_on(store.delete(Keyspace::Ephemeral, "handoff.worker-a", None));
            deletes += 1;
        }
        // Success: A lost a split to B's fenced fallback steal.
        if held_a.splits.len() < 4 {
            break;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        rearmed,
        "the request key must be recreated after being dropped"
    );
    assert!(
        !held_b.splits.is_empty(),
        "B took a split over via the fenced fallback steal"
    );
    for (_, progress) in held_b.splits.values() {
        assert_eq!(
            progress.as_ref().map(|p| p.watermark),
            Some(1),
            "the fallback steal resumes from A's committed watermark"
        );
    }
}
