//! Regressions for the reads a worker on a polled store makes for what its
//! watch cannot deliver: the records of a split it was assigned, a record
//! the leader never saw change, the records a new leader never saw, and the
//! whole job once a peer has reported it terminal.
//!
//! Each worker that must not learn a record from its watch watches through
//! a [`TapStore`] that hides it, and the reconcile interval is long, so only
//! the read under test can bring the view up to date.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{CoordinationStore, Keyspace, StoreError};
use spate_coordination::{
    CoordinationConfig, CoordinationErrorKind, CoordinationEvent, PlanFinality, SplitCoordinator,
    SplitProgress, StoreCoordinator,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use support::polled::PolledStore;
use support::tap::{Op, TapStore};
use support::{
    DEADLINE, Held, LEASE, PhasedPlanner, config_for, crash, drive, drive_pair, runtime, split_id,
    store,
};

/// Longer than any test here runs, so reconcile never repairs a view.
const NO_RECONCILE: Duration = Duration::from_secs(600);

/// Each worker's watch lists the store this often.
const POLL: Duration = Duration::from_millis(150);

type Tapped = TapStore<PolledStore<MemoryStore>>;

fn tuned(instance: &str, tune: impl FnOnce(&mut CoordinationConfig)) -> CoordinationConfig {
    let mut config = config_for(LEASE, Some(instance));
    config.reconcile_interval = NO_RECONCILE;
    config.max_in_flight = 1;
    tune(&mut config);
    config
}

/// A handle on `inner` whose watches hide every event `hidden` matches.
fn tapped(
    inner: &MemoryStore,
    hidden: impl Fn(Keyspace, &str) -> bool + Send + Sync + 'static,
) -> Tapped {
    let tap = TapStore::new(PolledStore::new(inner.clone(), POLL));
    tap.hide(hidden);
    tap
}

fn worker(
    store: Tapped,
    io: &tokio::runtime::Handle,
    config: CoordinationConfig,
    ids: &[&str],
) -> StoreCoordinator<Tapped> {
    planned(
        store,
        io,
        config,
        PhasedPlanner::one_final("polled-reads:v1", ids),
    )
}

fn planned(
    store: Tapped,
    io: &tokio::runtime::Handle,
    config: CoordinationConfig,
    planner: PhasedPlanner,
) -> StoreCoordinator<Tapped> {
    let mut w = StoreCoordinator::new(store, config, io.clone(), None).expect("coordinator");
    w.start(Box::new(planner)).unwrap();
    w
}

/// An open job whose every plan run names the same four splits.
fn open_four() -> PhasedPlanner {
    PhasedPlanner {
        fingerprint: "polled-reads:v1".to_string(),
        phases: vec![(support::splits(&["a", "b", "c", "d"]), PlanFinality::Open); 8],
    }
}

/// Durable split and spec records.
fn records(ks: Keyspace, key: &str) -> bool {
    ks == Keyspace::Durable && (key.starts_with("split.") || key.starts_with("spec."))
}

/// The splits `instance`'s assignment record names, empty before it exists.
fn assigned(rt: &tokio::runtime::Runtime, store: &MemoryStore, instance: &str) -> Vec<String> {
    rt.block_on(store.get(Keyspace::Durable, &format!("assign.{instance}")))
        .unwrap()
        .map(|entry| {
            let val: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            val["splits"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| id.as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Poll both workers until `done`, as [`drive_pair`] does, for workers on
/// different store types.
fn drive_two(
    a: (&mut impl SplitCoordinator, &mut Held),
    b: (&mut impl SplitCoordinator, &mut Held),
    what: &str,
    done: impl Fn(&Held, &Held) -> bool,
) {
    let deadline = Instant::now() + DEADLINE;
    while !done(a.1, b.1) {
        assert!(Instant::now() < deadline, "timed out: {what}");
        a.1.fold(a.0.poll().unwrap_or_else(|e| panic!("{what}: {e}")));
        b.1.fold(b.0.poll().unwrap_or_else(|e| panic!("{what}: {e}")));
        std::thread::sleep(support::POLL_INTERVAL);
    }
}

/// A polled store whose interval is zero, or not below the lease, is
/// rejected at construction.
#[test]
fn a_poll_interval_outside_the_lease_is_rejected() {
    let rt = runtime();
    for interval in [Duration::ZERO, LEASE] {
        let err = StoreCoordinator::new(
            PolledStore::new(store(), interval),
            config_for(LEASE, Some("worker-a")),
            rt.handle().clone(),
            None,
        )
        .err()
        .unwrap_or_else(|| panic!("{interval:?} was accepted"));
        assert_eq!(err.kind, CoordinationErrorKind::Fatal, "{err}");
    }
}

/// A worker whose watch never delivers the records of a split it was
/// assigned reads them, and claims the split.
#[test]
fn an_assigned_split_the_worker_has_never_seen_is_claimed() {
    let rt = runtime();
    let inner = store();
    let ids = ["w", "x"];
    let mut a = worker(
        tapped(&inner, |_, _| false),
        rt.handle(),
        tuned("worker-a", |_| {}),
        &ids,
    );
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming one split", |h| {
        h.splits.len() == 1
    });
    let mut b = worker(
        tapped(&inner, records),
        rt.handle(),
        tuned("worker-b", |_| {}),
        &ids,
    );
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "B claiming the split it was assigned",
        |_, b| b.splits.len() == 1,
    );
}

/// A spec read that fails is taken again on the next refresh.
#[test]
fn an_assigned_split_whose_spec_read_failed_is_read_again() {
    let rt = runtime();
    let inner = store();
    let ids = ["w", "x"];
    let mut a = worker(
        tapped(&inner, |_, _| false),
        rt.handle(),
        tuned("worker-a", |_| {}),
        &ids,
    );
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming one split", |h| {
        h.splits.len() == 1
    });
    let failing = tapped(&inner, records);
    let failed = Arc::new(AtomicBool::new(false));
    let once = Arc::clone(&failed);
    failing.on_get(move |ks, key| {
        (ks == Keyspace::Durable && key.starts_with("spec.") && !once.swap(true, Ordering::AcqRel))
            .then(|| StoreError::Retryable("injected: read timed out".into()))
    });
    let mut b = worker(failing, rt.handle(), tuned("worker-b", |_| {}), &ids);
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "B claiming the split whose spec read failed",
        |_, b| b.splits.len() == 1,
    );
    assert!(failed.load(Ordering::Acquire), "no spec read failed");
}

/// A read that fails waits for the next poll interval before it is taken
/// again.
#[test]
fn a_failed_read_waits_for_the_next_interval() {
    let rt = runtime();
    let inner = store();
    let ids = ["w", "x"];
    let mut a = worker(
        tapped(&inner, |_, _| false),
        rt.handle(),
        tuned("worker-a", |_| {}),
        &ids,
    );
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming one split", |h| {
        h.splits.len() == 1
    });
    let failing = tapped(&inner, records);
    let attempts = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&attempts);
    failing.on_get(move |ks, key| {
        (ks == Keyspace::Durable && key.starts_with("spec.")).then(|| {
            counted.fetch_add(1, Ordering::AcqRel);
            StoreError::Retryable("injected: read timed out".into())
        })
    });
    let mut b = worker(failing, rt.handle(), tuned("worker-b", |_| {}), &ids);
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "B's first spec read",
        |_, _| attempts.load(Ordering::Acquire) > 0,
    );
    let window = POLL * 6;
    let until = Instant::now() + window;
    while Instant::now() < until {
        held_a.fold(a.poll().expect("poll A"));
        held_b.fold(b.poll().expect("poll B"));
        std::thread::sleep(support::POLL_INTERVAL);
    }
    // The read the drive waited for, one per interval, and one more for the
    // interval already started.
    let taken = attempts.load(Ordering::Acquire);
    assert!(taken <= 8, "{taken} spec reads in six poll intervals");
}

/// A leader that never sees a peer's lease or record learns of the peer's
/// completion by reading the split it assigned, and stops assigning it.
#[test]
fn a_completion_the_leader_never_saw_leaves_the_assignment() {
    let rt = runtime();
    let inner = store();
    let ids = ["w", "x"];
    let splits = |_: Keyspace, key: &str| key.starts_with("split.");
    let mut a = worker(
        tapped(&inner, splits),
        rt.handle(),
        tuned("worker-a", |_| {}),
        &ids,
    );
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming one split", |h| {
        h.splits.len() == 1
    });
    let mut b = worker(
        tapped(&inner, |_, _| false),
        rt.handle(),
        tuned("worker-b", |_| {}),
        &ids,
    );
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "B claiming the other split",
        |_, b| b.splits.len() == 1,
    );
    let theirs = held_b.splits.keys().next().expect("held").clone();
    b.commit(&split_id(&theirs), &SplitProgress::completed(1, vec![]))
        .unwrap();
    // A keeps its own split, so no claim of its own can read B's record.
    let deadline = Instant::now() + DEADLINE;
    while assigned(&rt, &inner, "worker-b").contains(&theirs) {
        assert!(
            Instant::now() < deadline,
            "the completed split stayed assigned"
        );
        held_a.fold(a.poll().expect("poll A"));
        held_b.fold(b.poll().expect("poll B"));
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert_eq!(held_a.splits.len(), 1);
}

/// A leader that saw a dead peer's lease vanish, but never its claim,
/// does not gain the split inside the rebalance delay.
#[test]
fn a_departed_owners_split_is_not_gained_inside_the_rebalance_delay() {
    let rt = runtime();
    let inner = store();
    let ids = ["w", "x"];
    let durable_splits =
        |ks: Keyspace, key: &str| ks == Keyspace::Durable && key.starts_with("split.");
    let delay = |c: &mut CoordinationConfig| c.rebalance_delay = LEASE * 50;
    let mut a = worker(
        tapped(&inner, durable_splits),
        rt.handle(),
        tuned("worker-a", delay),
        &ids,
    );
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming one split", |h| {
        h.splits.len() == 1
    });
    let rt_b = runtime();
    let mut b = worker(
        tapped(&inner, |_, _| false),
        rt_b.handle(),
        tuned("worker-b", delay),
        &ids,
    );
    let mut held_b = Held::default();
    drive_two(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "B claiming the other split",
        |_, b| b.splits.len() == 1,
    );
    let theirs = held_b.splits.keys().next().expect("held").clone();
    let ours = held_a.splits.keys().next().expect("held").clone();
    a.commit(&split_id(&ours), &SplitProgress::completed(1, vec![]))
        .unwrap();
    held_a.splits.remove(&ours);
    crash(rt_b, b);

    let deadline = Instant::now() + DEADLINE;
    while rt
        .block_on(inner.get(Keyspace::Ephemeral, "worker.worker-b"))
        .unwrap()
        .is_some()
    {
        assert!(Instant::now() < deadline, "B's presence never expired");
        held_a.fold(a.poll().expect("poll A"));
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let until = Instant::now() + LEASE * 2;
    while Instant::now() < until {
        for event in a.poll().expect("poll A") {
            assert!(
                !matches!(&event, CoordinationEvent::Gained { split, .. } if split.id.as_str() == theirs),
                "A took the dead worker's split inside the rebalance delay"
            );
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
}

/// Seed an open job's four splits on A, which holds two; B, which never
/// sees a split or spec record on its watch, holds the rest. Then A
/// crashes. Returns B and its events so far.
fn leader_dies(
    rt: &tokio::runtime::Runtime,
    inner: &MemoryStore,
    b_store: Tapped,
) -> (StoreCoordinator<Tapped>, Held) {
    let rt_a = runtime();
    let two = |c: &mut CoordinationConfig| c.max_in_flight = 2;
    let mut a = planned(
        tapped(inner, |_, _| false),
        rt_a.handle(),
        tuned("worker-a", two),
        open_four(),
    );
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming two splits", |h| {
        h.splits.len() == 2
    });
    let four = |c: &mut CoordinationConfig| c.max_in_flight = 4;
    let mut b = planned(b_store, rt.handle(), tuned("worker-b", four), open_four());
    let mut held_b = Held::default();
    drive_two(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "B claiming the other two",
        |_, b| b.splits.len() == 2,
    );
    crash(rt_a, a);
    (b, held_b)
}

/// A new leader whose watch never delivered the records of its
/// predecessor's splits lists them before it assigns, and takes them over.
#[test]
fn a_new_leader_assigns_splits_it_never_saw() {
    let rt = runtime();
    let inner = store();
    let (mut b, mut held_b) = leader_dies(&rt, &inner, tapped(&inner, records));
    drive(&mut b, &mut held_b, "B taking over all four splits", |h| {
        h.splits.len() == 4
    });
}

/// A new leader whose first catch-up listing fails lists again, and its
/// plan run waits for the listing, so the replan re-creates no record that
/// exists.
#[test]
fn a_new_leader_seeds_nothing_it_already_has() {
    let rt = runtime();
    let inner = store();
    let b_store = tapped(&inner, records);
    let armed = Arc::new(AtomicBool::new(false));
    let creates = Arc::new(AtomicU64::new(0));
    let (armed_w, counted) = (Arc::clone(&armed), Arc::clone(&creates));
    b_store.on_write(move |w| {
        if armed_w.load(Ordering::Acquire) && w.op == Op::Create && records(w.ks, w.key) {
            counted.fetch_add(1, Ordering::AcqRel);
        }
        None
    });
    let (armed_l, failed) = (Arc::clone(&armed), Arc::new(AtomicBool::new(false)));
    let failed_once = Arc::clone(&failed);
    b_store.on_list(move |ks, prefix| {
        (armed_l.load(Ordering::Acquire)
            && ks == Keyspace::Durable
            && prefix == "split."
            && !failed_once.swap(true, Ordering::AcqRel))
        .then(|| StoreError::Retryable("injected: listing timed out".into()))
    });
    let (mut b, mut held_b) = {
        let hook = Arc::clone(&armed);
        let out = leader_dies(&rt, &inner, b_store);
        hook.store(true, Ordering::Release);
        out
    };
    drive(&mut b, &mut held_b, "B taking over all four splits", |h| {
        h.splits.len() == 4
    });
    assert!(failed.load(Ordering::Acquire), "no catch-up listing failed");
    assert_eq!(
        creates.load(Ordering::Acquire),
        0,
        "the new leader re-seeded"
    );
}

/// A new leader publishes nothing before its catch-up listing lands, so it
/// never writes a peer an assignment that omits splits the leader has not
/// listed yet.
#[test]
fn a_new_leader_revokes_nothing_it_has_not_listed() {
    let rt = runtime();
    let inner = store();
    let ids = ["a", "b", "c", "d", "e", "f"];
    let two = |c: &mut CoordinationConfig| c.max_in_flight = 2;
    let rt_a = runtime();
    let mut a = worker(
        tapped(&inner, |_, _| false),
        rt_a.handle(),
        tuned("worker-a", two),
        &ids,
    );
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming two splits", |h| {
        h.splits.len() == 2
    });
    // C never wins an election, so the leader after A is B.
    let c_store = tapped(&inner, |_, _| false);
    c_store.on_write(|w| {
        (w.op == Op::Create && w.key == "leader")
            .then(|| StoreError::Retryable("injected: not a candidate".into()))
    });
    let mut c = worker(c_store, rt.handle(), tuned("worker-c", two), &ids);
    let mut held_c = Held::default();
    drive_two(
        (&mut a, &mut held_a),
        (&mut c, &mut held_c),
        "C claiming two splits",
        |_, c| c.splits.len() == 2,
    );
    let b_store = tapped(&inner, records);
    // Every assignment record B writes for C, as the number of splits it names.
    let written_for_c = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = Arc::clone(&written_for_c);
    b_store.on_write(move |w| {
        if w.key == "assign.worker-c"
            && let Some(value) = w.value
        {
            let val: serde_json::Value = serde_json::from_slice(value).unwrap();
            log.lock()
                .unwrap()
                .push(val["splits"].as_array().unwrap().len());
        }
        None
    });
    let mut b = worker(b_store, rt.handle(), tuned("worker-b", two), &ids);
    let mut held_b = Held::default();
    let deadline = Instant::now() + DEADLINE;
    while held_b.splits.len() < 2 {
        assert!(Instant::now() < deadline, "B never claimed two splits");
        held_a.fold(a.poll().expect("poll A"));
        held_b.fold(b.poll().expect("poll B"));
        held_c.fold(c.poll().expect("poll C"));
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        held_c.revoke_requests.is_empty(),
        "C asked to give up splits early"
    );
    crash(rt_a, a);

    let deadline = Instant::now() + DEADLINE;
    while rt
        .block_on(inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap()
        .is_none_or(|leader| !String::from_utf8_lossy(&leader.value).contains("worker-b"))
    {
        assert!(Instant::now() < deadline, "B never led");
        held_b.fold(b.poll().expect("poll B"));
        held_c.fold(c.poll().expect("poll C"));
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let until = Instant::now() + LEASE;
    while Instant::now() < until {
        held_b.fold(b.poll().expect("poll B"));
        held_c.fold(c.poll().expect("poll C"));
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        held_c.revoke_requests.is_empty(),
        "the new leader revoked C's splits: {:?}",
        held_c.revoke_requests
    );
    assert_eq!(held_c.splits.len(), 2);
    let written = written_for_c.lock().unwrap().clone();
    assert!(
        written.iter().all(|&n| n == 2),
        "B wrote C an assignment naming fewer splits than C holds: {written:?}"
    );
}

/// Drive `a`, alone, through every split of its job to the verdict, and
/// wait for its verdict marker.
fn finish_alone(
    rt: &tokio::runtime::Runtime,
    inner: &MemoryStore,
    a: &mut StoreCoordinator<Tapped>,
) {
    let mut held = Held::default();
    drive(a, &mut held, "A claiming both splits", |h| {
        h.splits.len() == 2
    });
    for id in held.splits.keys() {
        a.commit(&split_id(id), &SplitProgress::completed(1, vec![]))
            .unwrap();
    }
    drive(a, &mut held, "A's verdict", |h| h.all_complete);
    let deadline = Instant::now() + DEADLINE;
    while rt
        .block_on(inner.get(Keyspace::Durable, "verdict"))
        .unwrap()
        .is_none()
    {
        assert!(
            Instant::now() < deadline,
            "A never wrote the verdict marker"
        );
        a.poll().expect("poll A");
        std::thread::sleep(support::POLL_INTERVAL);
    }
}

/// A standby whose watch never delivered a split record reports the verdict
/// once a peer has written the marker.
#[test]
fn a_standby_that_never_saw_a_record_reports_the_verdict() {
    let rt = runtime();
    let inner = store();
    let two = |c: &mut CoordinationConfig| c.max_in_flight = 2;
    let ids = ["a", "b"];
    let mut a = worker(
        tapped(&inner, |_, _| false),
        rt.handle(),
        tuned("worker-a", two),
        &ids,
    );
    finish_alone(&rt, &inner, &mut a);
    let mut c = worker(
        tapped(&inner, records),
        rt.handle(),
        tuned("worker-c", |_| {}),
        &ids,
    );
    drive(&mut c, &mut Held::default(), "the standby's verdict", |h| {
        h.all_complete
    });
}

/// A standby whose verdict listing fails lists again on the next poll
/// interval.
#[test]
fn a_verdict_listing_that_fails_is_taken_again() {
    let rt = runtime();
    let inner = store();
    let two = |c: &mut CoordinationConfig| c.max_in_flight = 2;
    let ids = ["a", "b"];
    let mut a = worker(
        tapped(&inner, |_, _| false),
        rt.handle(),
        tuned("worker-a", two),
        &ids,
    );
    finish_alone(&rt, &inner, &mut a);
    let failing = tapped(&inner, records);
    let failed = Arc::new(AtomicBool::new(false));
    let once = Arc::clone(&failed);
    failing.on_list(move |ks, prefix| {
        (ks == Keyspace::Durable && prefix == "split." && !once.swap(true, Ordering::AcqRel))
            .then(|| StoreError::Retryable("injected: listing timed out".into()))
    });
    let mut c = worker(failing, rt.handle(), tuned("worker-c", |_| {}), &ids);
    drive(&mut c, &mut Held::default(), "the standby's verdict", |h| {
        h.all_complete
    });
    assert!(failed.load(Ordering::Acquire), "no verdict listing failed");
}
