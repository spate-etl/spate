//! Regressions for the reads a worker on a polled store makes for what its
//! watch cannot deliver: the records of a split it was assigned, and a
//! record the leader never saw change.
//!
//! Each worker that must not learn a record from its watch watches through
//! a [`TapStore`] that hides it, and the reconcile interval is long, so only
//! the read under test can bring the view up to date.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{CoordinationStore, Keyspace, StoreError};
use spate_coordination::{
    CoordinationConfig, CoordinationErrorKind, CoordinationEvent, SplitCoordinator, SplitProgress,
    StoreCoordinator,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use support::polled::PolledStore;
use support::tap::TapStore;
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
    let mut w = StoreCoordinator::new(store, config, io.clone(), None).expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("polled-reads:v1", ids)))
        .unwrap();
    w
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
    // One read per interval, and one more for the interval already started.
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
