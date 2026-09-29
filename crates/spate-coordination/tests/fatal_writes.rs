//! A store that rejects a write or a listing as fatal after startup stops
//! the coordinator: every steady-state call the store refuses outright is
//! reported, never retried forever.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{Keyspace, StoreError};
use spate_coordination::{
    CoordinationErrorKind, SplitCoordinator, SplitProgress, StoreCoordinator,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use support::tap::{Op, TapStore, Write};
use support::{DEADLINE, Held, PhasedPlanner, config, drive, runtime, split_id, store};

fn denied() -> StoreError {
    StoreError::Fatal("injected: access denied".into())
}

/// A worker whose store rejects every write `rejects` matches once `armed`.
fn rejecting(
    io: &tokio::runtime::Handle,
    armed: &Arc<AtomicBool>,
    rejects: impl Fn(&Write<'_>) -> bool + Send + Sync + 'static,
) -> StoreCoordinator<TapStore<MemoryStore>> {
    let tap = TapStore::new(store());
    let armed = Arc::clone(armed);
    tap.on_write(move |write| (armed.load(Ordering::Acquire) && rejects(write)).then(denied));
    started(tap, io)
}

/// A worker whose store rejects every listing `rejects` matches once `armed`.
fn rejecting_lists(
    io: &tokio::runtime::Handle,
    armed: &Arc<AtomicBool>,
    rejects: impl Fn(Keyspace, &str) -> bool + Send + Sync + 'static,
) -> StoreCoordinator<TapStore<MemoryStore>> {
    let tap = TapStore::new(store());
    let armed = Arc::clone(armed);
    tap.on_list(move |ks, prefix| {
        (armed.load(Ordering::Acquire) && rejects(ks, prefix)).then(denied)
    });
    started(tap, io)
}

fn started(
    tap: TapStore<MemoryStore>,
    io: &tokio::runtime::Handle,
) -> StoreCoordinator<TapStore<MemoryStore>> {
    let mut w = StoreCoordinator::new(tap, config(Some("worker-a")), io.clone(), None)
        .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("fatal:v1", &["x"])))
        .unwrap();
    w
}

/// Drive `w` until it holds x, then arm the store's rejection.
fn claim_then_arm(w: &mut impl SplitCoordinator, armed: &AtomicBool) {
    drive(w, &mut Held::default(), "claiming x", |h| {
        h.splits.len() == 1
    });
    armed.store(true, Ordering::Release);
}

/// Poll until the coordinator fails, and assert it failed with the store's
/// fatal error.
fn expect_fatal(w: &mut impl SplitCoordinator, what: &str) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        assert!(Instant::now() < deadline, "{what}: never failed");
        match w.poll() {
            Ok(_) => std::thread::sleep(support::POLL_INTERVAL),
            Err(e) => {
                assert_eq!(e.kind, CoordinationErrorKind::Fatal, "{what}: {e}");
                assert!(e.to_string().contains("access denied"), "{what}: {e}");
                return;
            }
        }
    }
}

#[test]
fn a_rejected_lease_renewal_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(false));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Update && write.ks == Keyspace::Ephemeral && write.key.starts_with("split.")
    });
    drive(&mut w, &mut Held::default(), "claiming x", |h| {
        h.splits.len() == 1
    });
    armed.store(true, Ordering::Release);
    expect_fatal(&mut w, "renewal");
}

#[test]
fn a_rejected_claim_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(true));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Create && write.ks == Keyspace::Ephemeral && write.key.starts_with("split.")
    });
    expect_fatal(&mut w, "claim");
}

#[test]
fn a_rejected_assignment_publish_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(true));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.key.starts_with("assign.")
    });
    expect_fatal(&mut w, "publish");
}

#[test]
fn a_rejected_seed_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(true));
    let mut w = rejecting(rt.handle(), &armed, |write| write.key.starts_with("spec."));
    expect_fatal(&mut w, "seeding");
}

#[test]
fn a_rejected_presence_renewal_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(false));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Update && write.key.starts_with("worker.")
    });
    claim_then_arm(&mut w, &armed);
    expect_fatal(&mut w, "presence renewal");
}

#[test]
fn a_rejected_leadership_renewal_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(false));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Update && write.key == "leader"
    });
    claim_then_arm(&mut w, &armed);
    expect_fatal(&mut w, "leadership renewal");
}

#[test]
fn a_rejected_election_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(true));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Create && write.key == "leader"
    });
    expect_fatal(&mut w, "election");
}

#[test]
fn a_rejected_ownership_write_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(true));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Update
            && write.ks == Keyspace::Durable
            && write.key.starts_with("split.")
            && write
                .value
                .is_some_and(|v| String::from_utf8_lossy(v).contains("worker-a"))
    });
    expect_fatal(&mut w, "the ownership CAS");
}

#[test]
fn a_rejected_plan_publish_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(true));
    let plan_writes = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&plan_writes);
    // The first plan write is the election's generation bump; the second is
    // the run's publish.
    let mut w = rejecting(rt.handle(), &armed, move |write| {
        write.key == "plan" && write.op == Op::Update && counted.fetch_add(1, Ordering::AcqRel) == 1
    });
    expect_fatal(&mut w, "plan publish");
}

#[test]
fn a_rejected_lease_delete_on_completion_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(false));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Delete && write.ks == Keyspace::Ephemeral && write.key.starts_with("split.")
    });
    claim_then_arm(&mut w, &armed);
    let committed = w.commit(&split_id("x"), &SplitProgress::completed(1, vec![]));
    assert!(
        committed.is_err_and(|e| e.kind == CoordinationErrorKind::Fatal),
        "the commit reported no fatal error"
    );
    expect_fatal(&mut w, "lease delete");
}

#[test]
fn a_rejected_failure_report_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(false));
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Update && write.ks == Keyspace::Durable && write.key.starts_with("split.")
    });
    claim_then_arm(&mut w, &armed);
    let failed = w.fail(&split_id("x"), "unreadable");
    assert!(
        failed.is_err_and(|e| e.kind == CoordinationErrorKind::Fatal),
        "the failure report returned no fatal error"
    );
    expect_fatal(&mut w, "failure report");
}

#[test]
fn a_rejected_release_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(false));
    // Only the owner-clearing write, so nothing but the release can fail.
    let mut w = rejecting(rt.handle(), &armed, |write| {
        write.op == Op::Update
            && write.key.starts_with("split.")
            && write
                .value
                .is_some_and(|v| String::from_utf8_lossy(v).contains("\"owner\":null"))
    });
    claim_then_arm(&mut w, &armed);
    let _ = w.release_drained(&[split_id("x")]);
    expect_fatal(&mut w, "release");
}

#[test]
fn a_rejected_reconcile_listing_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(false));
    let mut w = rejecting_lists(rt.handle(), &armed, |ks, prefix| {
        ks == Keyspace::Ephemeral && prefix.is_empty()
    });
    claim_then_arm(&mut w, &armed);
    expect_fatal(&mut w, "reconcile listing");
}

#[test]
fn a_rejected_recount_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(true));
    let mut w = rejecting_lists(rt.handle(), &armed, |ks, prefix| {
        ks == Keyspace::Durable && prefix == "split."
    });
    expect_fatal(&mut w, "planned recount");
}

#[test]
fn a_rejected_verdict_listing_stops_the_coordinator() {
    let rt = runtime();
    let armed = Arc::new(AtomicBool::new(false));
    let mut w = rejecting_lists(rt.handle(), &armed, |ks, prefix| {
        ks == Keyspace::Durable && prefix == "split."
    });
    claim_then_arm(&mut w, &armed);
    let _ = w.commit(&split_id("x"), &SplitProgress::completed(1, vec![]));
    expect_fatal(&mut w, "verdict listing");
}
