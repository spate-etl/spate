//! Regressions for decisions a worker makes from a view that lags the
//! store: a claim on a record that finished or ran out of attempts, a
//! takeover that adopts a late commit, a plan older than the one it read, a
//! lost compare-and-swap, and a stale lease-keyspace put.
//!
//! Each worker watches through a [`TapStore`] that hides the events a lagging
//! watch has not delivered yet, and the reconcile interval is long, so only
//! the path under test can bring the view up to date.

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use spate_coordination::{
    CoordinationConfig, CoordinationEvent, SplitCoordinator, StoreCoordinator,
};
use spate_coordination::{PlanFinality, SplitProgress};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use support::tap::TapStore;
use support::{
    DEADLINE, Fleet, Held, LEASE, PhasedPlanner, QUIET_ROUNDS, TestClock, config, config_for,
    crash, drive, drive_clocked, runtime, settle_pair_clocked, split_id, store, store_with_clock,
};

/// Longer than any test here runs, so reconcile never repairs a view.
const NO_RECONCILE: Duration = Duration::from_secs(600);

fn tuned(instance: &str) -> CoordinationConfig {
    let mut config = config_for(LEASE, Some(instance));
    config.reconcile_interval = NO_RECONCILE;
    config
}

fn worker(
    store: &TapStore<MemoryStore>,
    io: &tokio::runtime::Handle,
    instance: &str,
) -> StoreCoordinator<TapStore<MemoryStore>> {
    StoreCoordinator::new(store.clone(), tuned(instance), io.clone(), None).expect("coordinator")
}

/// Rewrite `key`'s durable record as `edit` changes its JSON, bypassing any
/// worker.
fn rewrite(
    rt: &tokio::runtime::Runtime,
    store: &MemoryStore,
    key: &str,
    edit: impl FnOnce(&mut serde_json::Value),
) {
    rt.block_on(async {
        let entry = store
            .get(Keyspace::Durable, key)
            .await
            .unwrap()
            .expect("record");
        let mut json: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        edit(&mut json);
        let outcome = store
            .update(
                Keyspace::Durable,
                key,
                serde_json::to_vec(&json).unwrap(),
                entry.revision,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CasOutcome::Won(_)), "rewrite of {key}");
    });
}

fn delete_lease(rt: &tokio::runtime::Runtime, store: &MemoryStore, key: &str) {
    let outcome = rt
        .block_on(store.delete(Keyspace::Ephemeral, key, None))
        .unwrap();
    assert!(matches!(outcome, CasOutcome::Won(_)));
}

fn durable(rt: &tokio::runtime::Runtime, store: &MemoryStore, key: &str) -> Entry {
    rt.block_on(store.get(Keyspace::Durable, key))
        .unwrap()
        .expect("record")
}

/// Drive `worker` until `done`, returning the splits it gained on the way.
fn gains(
    worker: &mut impl SplitCoordinator,
    held: &mut Held,
    what: &str,
    mut done: impl FnMut(&Held) -> bool,
) -> Vec<String> {
    let mut gained = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    while !done(held) {
        assert!(Instant::now() < deadline, "timed out: {what}");
        let batch = worker.poll().unwrap_or_else(|e| panic!("{what}: {e}"));
        if batch.is_empty() {
            std::thread::sleep(support::POLL_INTERVAL);
        }
        for event in &batch {
            if let CoordinationEvent::Gained { split, .. } = event {
                gained.push(split.id.as_str().to_string());
            }
        }
        held.fold(batch);
    }
    gained
}

/// A worker that sees its lease vanish before the record that finished it
/// takes the split as expired. The re-read shows it complete, and the worker
/// writes nothing and gains nothing.
#[test]
fn an_expired_claim_on_a_finished_record_writes_nothing() {
    let rt = runtime();
    let inner = store();
    let tap = TapStore::new(inner.clone());
    tap.hide(|ks, key| ks == Keyspace::Durable && key.starts_with("split."));
    let mut w = worker(&tap, rt.handle(), "worker-a");
    w.start(Box::new(PhasedPlanner::one_final("finished:v1", &["x"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming x", |h| h.splits.len() == 1);

    // Another worker finished x; this one hears about the lease first.
    rewrite(&rt, &inner, "split.x", |r| {
        r["status"] = "completed".into();
        r["completed"] = true.into();
        r["owner"] = "worker-z".into();
        r["epoch"] = (r["epoch"].as_u64().unwrap() + 1).into();
        r["watermark"] = 9.into();
    });
    let finished = durable(&rt, &inner, "split.x");
    delete_lease(&rt, &inner, "split.x");

    let gained = gains(&mut w, &mut held, "the verdict", |h| h.all_complete);
    assert!(gained.is_empty(), "claimed a finished split: {gained:?}");
    assert_eq!(
        durable(&rt, &inner, "split.x").revision,
        finished.revision,
        "the finished record was rewritten"
    );
}

/// A worker whose view lags a record at the attempts cap parks the split
/// instead of claiming it past the cap.
#[test]
fn an_expired_claim_at_the_attempts_cap_quarantines_instead() {
    let rt = runtime();
    let inner = store();
    let tap = TapStore::new(inner.clone());
    tap.hide(|ks, key| ks == Keyspace::Durable && key.starts_with("split."));
    let mut w = worker(&tap, rt.handle(), "worker-a");
    w.start(Box::new(PhasedPlanner::one_final("capped:v1", &["x"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming x", |h| h.splits.len() == 1);

    // Tenancies this worker never saw used up all but the last attempt.
    let max_attempts = tuned("worker-a").max_attempts;
    rewrite(&rt, &inner, "split.x", |r| {
        r["attempts"] = (max_attempts - 1).into();
        r["owner"] = "worker-z".into();
        r["epoch"] = (r["epoch"].as_u64().unwrap() + 1).into();
    });
    delete_lease(&rt, &inner, "split.x");

    let gained = gains(&mut w, &mut held, "the quarantine", |h| {
        !h.quarantined.is_empty()
    });
    assert!(gained.is_empty(), "claimed past the cap: {gained:?}");
    let record: serde_json::Value =
        serde_json::from_slice(&durable(&rt, &inner, "split.x").value).unwrap();
    assert_eq!(record["status"], "quarantined");
    assert_eq!(record["attempts"], max_attempts);
}

/// A store that, once, rewrites `key`'s record with `edit` just before the
/// first update of it that `when` matches, so that update loses its
/// compare-and-swap.
#[derive(Clone)]
struct WriteFirst {
    inner: MemoryStore,
    key: &'static str,
    when: fn(&serde_json::Value) -> bool,
    edit: fn(&mut serde_json::Value),
    armed: Arc<AtomicBool>,
}

impl WriteFirst {
    fn new(
        inner: MemoryStore,
        key: &'static str,
        when: fn(&serde_json::Value) -> bool,
        edit: fn(&mut serde_json::Value),
    ) -> WriteFirst {
        WriteFirst {
            inner,
            key,
            when,
            edit,
            armed: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl CoordinationStore for WriteFirst {
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
        let matches = ks == Keyspace::Durable
            && key == self.key
            && serde_json::from_slice::<serde_json::Value>(&value).is_ok_and(|v| (self.when)(&v));
        if matches && self.armed.swap(false, Ordering::AcqRel) {
            let entry = self.inner.get(ks, key).await?.expect("record");
            let mut record: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            (self.edit)(&mut record);
            let landed = self
                .inner
                .update(
                    ks,
                    key,
                    serde_json::to_vec(&record).unwrap(),
                    entry.revision,
                )
                .await?;
            assert!(matches!(landed, CasOutcome::Won(_)), "the interposed write");
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

/// A dead owner's split taken over by worker-b, whose first ownership write
/// loses to `interposed`: the record as the takeover left it.
fn takeover_after(interposed: fn(&mut serde_json::Value)) -> serde_json::Value {
    let inner = store();
    let planner = || Box::new(PhasedPlanner::one_final("takeover-race:v1", &["x"]));

    let rt_a = runtime();
    let mut a = StoreCoordinator::new(
        inner.clone(),
        tuned("worker-a"),
        rt_a.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming x", |h| h.splits.len() == 1);
    a.commit(&split_id("x"), &SplitProgress::new(5, vec![]))
        .unwrap();
    crash(rt_a, a);

    let rt = runtime();
    let late = WriteFirst::new(
        inner.clone(),
        "split.x",
        |record| record["owner"] == "worker-b",
        interposed,
    );
    let armed = Arc::clone(&late.armed);
    let mut b = StoreCoordinator::new(late, tuned("worker-b"), rt.handle().clone(), None)
        .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive(&mut b, &mut held_b, "B taking x over", |h| {
        h.splits.len() == 1
    });
    assert!(
        !armed.load(Ordering::Acquire),
        "the interposed write never landed"
    );
    serde_json::from_slice(&durable(&rt, &inner, "split.x").value).unwrap()
}

/// A takeover whose first ownership write loses to the dead owner's late
/// commit adopts that commit, and its winning retry still counts the
/// delivery attempt the takeover consumes.
#[test]
fn a_takeover_that_adopts_a_late_commit_still_counts_its_attempt() {
    let record = takeover_after(|record| {
        record["watermark"] = (record["watermark"].as_i64().unwrap() + 1).into();
    });
    assert_eq!(record["owner"], "worker-b");
    assert_eq!(record["watermark"], 6, "the late commit was adopted");
    assert_eq!(
        record["attempts"], 1,
        "the takeover's attempt was not counted"
    );
}

/// A takeover that loses to a write taking the split to its last attempt
/// parks it instead of claiming it past the cap.
#[test]
fn a_takeover_that_loses_to_the_last_attempt_quarantines_instead() {
    let max = tuned("worker-b").max_attempts;
    assert_eq!(max, 4, "the edit below assumes the default cap");
    let inner = store();
    let planner = || Box::new(PhasedPlanner::one_final("takeover-cap:v1", &["x"]));
    let rt_a = runtime();
    let mut a = StoreCoordinator::new(
        inner.clone(),
        tuned("worker-a"),
        rt_a.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    drive(&mut a, &mut Held::default(), "A claiming x", |h| {
        h.splits.len() == 1
    });
    crash(rt_a, a);

    let rt = runtime();
    let late = WriteFirst::new(
        inner.clone(),
        "split.x",
        |record| record["owner"] == "worker-b",
        |record| record["attempts"] = 3.into(),
    );
    let mut b = StoreCoordinator::new(late, tuned("worker-b"), rt.handle().clone(), None)
        .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    let gained = gains(&mut b, &mut held_b, "the quarantine", |h| {
        !h.quarantined.is_empty()
    });
    assert!(gained.is_empty(), "claimed past the cap: {gained:?}");
}

/// A takeover that loses to the previous owner's release costs no attempt:
/// the split was handed back, not abandoned.
#[test]
fn a_takeover_that_loses_to_a_release_costs_no_attempt() {
    let record = takeover_after(|record| record["owner"] = serde_json::Value::Null);
    assert_eq!(record["owner"], "worker-b");
    assert_eq!(record["attempts"], 0, "a release was charged an attempt");
}

/// A takeover that loses to the previous owner's failure report counts that
/// failure once.
#[test]
fn a_takeover_that_loses_to_a_failure_report_counts_it_once() {
    let record = takeover_after(|record| {
        record["owner"] = serde_json::Value::Null;
        record["attempts"] = (record["attempts"].as_u64().unwrap() + 1).into();
    });
    assert_eq!(record["owner"], "worker-b");
    assert_eq!(record["attempts"], 1, "the failure was counted twice");
}

/// A leader whose first assignment write fails publishes it again on a later
/// step, without waiting for a reconcile.
#[test]
fn a_failed_assignment_publish_is_retried() {
    let rt = runtime();
    let tap = TapStore::new(store());
    let failed = Arc::new(AtomicBool::new(false));
    let fired = Arc::clone(&failed);
    // The write that names x, after which no other input changes.
    tap.on_write(move |write| {
        let names_x = write.key.starts_with("assign.")
            && write
                .value
                .is_some_and(|v| String::from_utf8_lossy(v).contains("\"x\""));
        (names_x && !fired.swap(true, Ordering::AcqRel))
            .then(|| StoreError::Retryable("injected: assignment write dropped".into()))
    });
    let mut w = worker(&tap, rt.handle(), "worker-a");
    w.start(Box::new(PhasedPlanner::one_final(
        "publish-retry:v1",
        &["x"],
    )))
    .unwrap();
    let mut held = Held::default();
    drive(
        &mut w,
        &mut held,
        "claiming x after the failed publish",
        |h| h.splits.len() == 1,
    );
    assert!(failed.load(Ordering::Acquire), "the fault never fired");
}

/// A leader whose cached assignment revision went stale reads the record
/// after its write loses, and the next write lands, so the fleet rebalances
/// without waiting for a reconcile.
#[test]
fn a_lost_assignment_write_refreshes_the_record() {
    let rt = runtime();
    let inner = store();
    let tap = TapStore::new(inner.clone());
    tap.hide(|ks, key| ks == Keyspace::Durable && key.starts_with("assign."));
    let planner = || Box::new(PhasedPlanner::one_final("assign-lost:v1", &["x", "y"]));
    let mut a = worker(&tap, rt.handle(), "worker-a");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A taking both splits", |h| {
        h.splits.len() == 2
    });

    // Something else rewrites A's record; A's cached revision is now a ghost.
    rewrite(&rt, &inner, "assign.worker-a", |_| {});

    let mut b = StoreCoordinator::new(inner, tuned("worker-b"), rt.handle().clone(), None)
        .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    let deadline = Instant::now() + DEADLINE;
    while held_b.splits.is_empty() {
        assert!(Instant::now() < deadline, "the fleet never rebalanced");
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
        support::consent_to_revocations(&mut a, &mut held_a);
        std::thread::sleep(support::POLL_INTERVAL);
    }
}

fn plan_generation(rt: &tokio::runtime::Runtime, store: &MemoryStore) -> u64 {
    let plan: serde_json::Value =
        serde_json::from_slice(&durable(rt, store, "plan").value).unwrap();
    plan["generation"].as_u64().unwrap()
}

fn ephemeral(rt: &tokio::runtime::Runtime, store: &MemoryStore, key: &str) -> Entry {
    rt.block_on(store.get(Keyspace::Ephemeral, key))
        .unwrap()
        .expect("live key")
}

/// A leader-key put older than the one the leader holds leaves its
/// leadership alone, so the plan generation does not move.
#[test]
fn a_stale_leader_put_does_not_depose_the_leader() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = store_with_clock(clock.clone());
    let tap = TapStore::new(inner.clone());
    let mut w = StoreCoordinator::with_clock(
        tap.clone(),
        config(Some("worker-a")),
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final(
        "stale-leader:v1",
        &["x"],
    )))
    .unwrap();
    let mut held = Held::default();
    drive_clocked(&mut w, &clock, &mut held, "claiming x", |h| {
        h.splits.len() == 1
    });
    let mut fleet = Fleet::new(&inner, rt.handle());
    fleet.join(&w);
    fleet.settle(&clock);
    let generation = plan_generation(&rt, &inner);

    let leader = ephemeral(&rt, &inner, "leader");
    let stale = serde_json::json!({
        "schema": 3, "owner": "worker-z", "nonce": "z", "generation": 0
    });
    tap.inject(
        Keyspace::Ephemeral,
        WatchEvent::Put(Entry {
            key: "leader".into(),
            value: serde_json::to_vec(&stale).unwrap(),
            revision: Revision(leader.revision.0 - 1),
        }),
    );
    clock.advance_stepped(LEASE * 3, LEASE / 12, || {
        fleet.settle(&clock);
        held.fold(w.poll().expect("poll"));
    });
    assert_eq!(
        plan_generation(&rt, &inner),
        generation,
        "the leader was deposed and re-elected"
    );
}

/// A stale put and delete of the leader's own presence key, both at or below
/// the revision it holds, leave it in the membership, so its assignment
/// record is not withdrawn.
#[test]
fn a_stale_presence_echo_does_not_drop_a_member() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = store_with_clock(clock.clone());
    let planner = || Box::new(PhasedPlanner::one_final("stale-presence:v1", &["s0", "s1"]));
    let tap = TapStore::new(inner.clone());
    let mut a = StoreCoordinator::with_clock(
        tap.clone(),
        config(Some("worker-a")),
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive_clocked(&mut a, &clock, &mut held_a, "A taking the plan", |h| {
        h.splits.len() == 2
    });
    let mut b = StoreCoordinator::with_clock(
        TapStore::new(inner.clone()),
        config(Some("worker-b")),
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    settle_pair_clocked(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        &clock,
        "one split each",
        |x, y| x.splits.len() == 1 && y.splits.len() == 1,
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ephemeral(&rt, &inner, "leader").value)
            .unwrap()["owner"],
        "worker-a",
        "the test needs worker-a to lead"
    );

    let assignment = durable(&rt, &inner, "assign.worker-a").revision;
    let presence = ephemeral(&rt, &inner, "worker.worker-a");
    tap.inject(
        Keyspace::Ephemeral,
        WatchEvent::Put(Entry {
            key: "worker.worker-a".into(),
            value: presence.value.clone(),
            revision: Revision(presence.revision.0 - 1),
        }),
    );
    tap.inject(
        Keyspace::Ephemeral,
        WatchEvent::Delete {
            key: "worker.worker-a".into(),
            revision: presence.revision,
        },
    );
    let mut fleet = Fleet::new(&inner, rt.handle());
    fleet.join(&a);
    fleet.join(&b);
    for _ in 0..QUIET_ROUNDS {
        fleet.step(&clock, LEASE / 12);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
    }
    let now = rt
        .block_on(inner.get(Keyspace::Durable, "assign.worker-a"))
        .unwrap()
        .map(|entry| entry.revision);
    assert_eq!(
        now,
        Some(assignment),
        "the leader dropped itself from the fleet and withdrew its own assignment"
    );
}

/// A worker whose quarantine write loses to a concurrent write reads the
/// record, and its next quarantine write lands.
#[test]
fn a_lost_quarantine_write_refreshes_the_record() {
    let rt = runtime();
    let inner = store();
    let interposed = WriteFirst::new(
        inner.clone(),
        "split.x",
        |record| record["status"] == "quarantined",
        |record| {
            record["written_at_ms"] = (record["written_at_ms"].as_i64().unwrap() + 1).into();
        },
    );
    let armed = Arc::clone(&interposed.armed);
    let tap = TapStore::new(interposed);
    tap.hide(|ks, key| ks == Keyspace::Durable && key.starts_with("split."));
    let mut w = StoreCoordinator::new(tap, tuned("worker-a"), rt.handle().clone(), None)
        .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final(
        "quarantine-lost:v1",
        &["x"],
    )))
    .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming x", |h| h.splits.len() == 1);

    let max_attempts = tuned("worker-a").max_attempts;
    rewrite(&rt, &inner, "split.x", |r| {
        r["attempts"] = (max_attempts - 1).into();
        r["owner"] = "worker-z".into();
        r["epoch"] = (r["epoch"].as_u64().unwrap() + 1).into();
    });
    delete_lease(&rt, &inner, "split.x");

    drive(&mut w, &mut held, "the quarantine landing", |h| {
        !h.quarantined.is_empty()
    });
    assert!(!armed.load(Ordering::Acquire), "no quarantine write lost");
}

/// Replaces the plan record in every durable watch snapshot with `stale`.
#[derive(Clone)]
struct StalePlan {
    inner: MemoryStore,
    stale: Entry,
}

impl CoordinationStore for StalePlan {
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
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        let stream = self.inner.watch(ks, prefix).await?;
        if ks != Keyspace::Durable {
            return Ok(stream);
        }
        let stale = self.stale.clone();
        let mut in_snapshot = true;
        Ok(stream
            .map(move |event| match event {
                Ok(WatchEvent::Put(entry)) if in_snapshot && entry.key == "plan" => {
                    Ok(WatchEvent::Put(stale.clone()))
                }
                Ok(WatchEvent::SnapshotDone) => {
                    in_snapshot = false;
                    Ok(WatchEvent::SnapshotDone)
                }
                other => other,
            })
            .boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// A worker joining a finished job keeps the plan it read at startup when
/// its watch snapshot carries an older one, and reaches the verdict without
/// waiting for a reconcile.
#[test]
fn an_older_plan_in_the_watch_snapshot_does_not_replace_the_one_read_at_startup() {
    let rt = runtime();
    let inner = store();
    let planner = || {
        Box::new(PhasedPlanner {
            fingerprint: "stale-plan:v1".to_string(),
            phases: vec![
                (support::splits(&["x"]), PlanFinality::Open),
                (Vec::new(), PlanFinality::Final),
            ],
        })
    };
    let mut a = StoreCoordinator::new(inner.clone(), tuned("worker-a"), rt.handle().clone(), None)
        .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming x", |h| h.splits.len() == 1);
    let stale = durable(&rt, &inner, "plan");
    a.commit(&split_id("x"), &SplitProgress::completed(1, vec![]))
        .unwrap();
    drive(&mut a, &mut held_a, "A finishing the job", |h| {
        h.all_complete
    });
    assert!(durable(&rt, &inner, "plan").revision > stale.revision);

    let late = StalePlan {
        inner: inner.clone(),
        stale,
    };
    let mut s = StoreCoordinator::new(late, tuned("worker-s"), rt.handle().clone(), None)
        .expect("coordinator");
    s.start(planner()).unwrap();
    drive(
        &mut s,
        &mut Held::default(),
        "the late worker's verdict",
        |h| h.all_complete,
    );
}
