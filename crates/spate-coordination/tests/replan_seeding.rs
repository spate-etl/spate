//! Leader seeding: which splits a plan run writes to the store, and how
//! many of those writes it keeps in flight.

mod support;

use spate_coordination::store::{CoordinationStore as _, Keyspace};
use spate_coordination::{PlanFinality, SplitCoordinator, SplitProgress};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;
use support::{
    CountingStore, Held, PhasedPlanner, crash, drive, runtime, split_id, splits, store, worker,
};

/// A replan that returns splits the leader has already seeded issues no
/// creates for them.
#[test]
fn replanning_seeded_splits_issues_no_creates() {
    let rt = runtime();
    let store = CountingStore::new(store());
    let ids = ["r0", "r1", "r2", "r3"];
    let planner = Box::new(PhasedPlanner {
        fingerprint: "reseed:v1".to_string(),
        phases: vec![(splits(&ids), PlanFinality::Open); 3],
    });
    let mut worker = store.worker(rt.handle(), "solo");
    worker.start(planner).unwrap();

    // The generation bump, then one publish per plan run: the third update
    // lands after a replan of the same splits has seeded.
    drive(
        &mut worker,
        &mut Held::default(),
        "a replan publishing",
        |_| store.stats.plan_updates.load(SeqCst) >= 3,
    );
    assert_eq!(store.stats.creates.load(SeqCst), 2 * ids.len() as u64);
}

/// Seeding keeps up to 64 creates in flight, and creates each split's spec
/// before its progress record.
#[test]
fn seeding_bounds_creates_in_flight_and_orders_spec_first() {
    let rt = runtime();
    let store = CountingStore::new(store()).with_create_delay(Duration::from_millis(20));
    let ids: Vec<String> = (0..100).map(|i| format!("c{i:03}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let planner = Box::new(PhasedPlanner::one_final("concurrent:v1", &ids));
    let mut worker = store.worker(rt.handle(), "solo");
    worker.start(planner).unwrap();

    drive(
        &mut worker,
        &mut Held::default(),
        "the plan publishing",
        |_| store.stats.plan_updates.load(SeqCst) >= 2,
    );
    assert_eq!(store.stats.creates.load(SeqCst), 200);
    assert_eq!(store.stats.max_in_flight.load(SeqCst), 64);
    assert!(!store.stats.progress_before_spec.load(SeqCst));
}

/// A split whose spec landed and whose progress create failed is seeded
/// again on the next plan run, and the job completes.
#[test]
fn a_failed_progress_create_is_seeded_on_the_next_run() {
    let rt = runtime();
    let store = CountingStore::new(store());
    store.fail_create_once("split.h1");
    let planner = Box::new(PhasedPlanner::one_final(
        "heal-seed:v1",
        &["h0", "h1", "h2"],
    ));
    let mut worker = store.worker(rt.handle(), "solo");
    worker.start(planner).unwrap();

    let mut held = Held::default();
    drive(&mut worker, &mut held, "claiming all three splits", |h| {
        h.splits.len() == 3
    });
    for id in ["h0", "h1", "h2"] {
        worker
            .commit(&split_id(id), &SplitProgress::completed(1, vec![]))
            .unwrap();
    }
    drive(&mut worker, &mut held, "the job completing", |h| {
        h.all_complete
    });
    let records = rt
        .block_on(store.inner.list(Keyspace::Durable, "split."))
        .unwrap();
    assert_eq!(records.len(), 3);
}

/// A leader whose view holds a progress record without its spec creates
/// the spec on replan, and the split becomes assignable.
#[test]
fn a_missing_spec_is_created_on_replan() {
    let rt = runtime();
    let store = store();
    let planner = || {
        Box::new(PhasedPlanner {
            fingerprint: "missing-spec:v1".to_string(),
            phases: vec![
                (splits(&["m0"]), PlanFinality::Open),
                (splits(&["m0"]), PlanFinality::Final),
            ],
        })
    };

    let rt_a = runtime();
    let mut a = worker(&store, rt_a.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    drive(
        &mut a,
        &mut Held::default(),
        "A seeding and claiming",
        |h| h.splits.len() == 1,
    );
    crash(rt_a, a);
    let deleted = rt
        .block_on(store.delete(Keyspace::Durable, "spec.m0", None))
        .unwrap();
    assert!(deleted.won().is_some());

    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held = Held::default();
    drive(
        &mut b,
        &mut held,
        "B claiming m0 once its spec is back",
        |h| h.splits.len() == 1,
    );
    b.commit(&split_id("m0"), &SplitProgress::completed(1, vec![]))
        .unwrap();
    drive(&mut b, &mut held, "the job completing", |h| h.all_complete);
    assert!(
        rt.block_on(store.get(Keyspace::Durable, "spec.m0"))
            .unwrap()
            .is_some()
    );
}
