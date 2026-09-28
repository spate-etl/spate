//! A store that rejects a write as fatal after startup stops the coordinator:
//! a lease renewal, a claim, an assignment publish or a seed that the store
//! refuses outright is reported, never retried forever.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{Keyspace, StoreError};
use spate_coordination::{CoordinationErrorKind, SplitCoordinator, StoreCoordinator};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use support::tap::{Op, TapStore, Write};
use support::{DEADLINE, Held, PhasedPlanner, config, drive, runtime, store};

/// A worker whose store rejects every write `rejects` matches once `armed`.
fn rejecting(
    io: &tokio::runtime::Handle,
    armed: &Arc<AtomicBool>,
    rejects: impl Fn(&Write<'_>) -> bool + Send + Sync + 'static,
) -> StoreCoordinator<TapStore<MemoryStore>> {
    let tap = TapStore::new(store());
    let armed = Arc::clone(armed);
    tap.on_write(move |write| {
        (armed.load(Ordering::Acquire) && rejects(write))
            .then(|| StoreError::Fatal("injected: access denied".into()))
    });
    let mut w = StoreCoordinator::new(tap, config(Some("worker-a")), io.clone(), None)
        .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("fatal:v1", &["x"])))
        .unwrap();
    w
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
