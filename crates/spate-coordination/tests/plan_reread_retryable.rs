//! An election whose fence CAS loses and whose plan-record re-read then
//! fails Retryable.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{CasOutcome, CoordinationStore as _, Keyspace, StoreError};
use spate_coordination::{SplitCoordinator, SplitProgress, StoreCoordinator};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;
use support::tap::{Op, TapStore};
use support::{DEADLINE, Held, PhasedPlanner, config, drive, runtime, split_id, store};

/// The record at `key` as JSON, or `None` when the key is absent.
fn record(
    rt: &tokio::runtime::Runtime,
    tap: &TapStore<MemoryStore>,
    ks: Keyspace,
    key: &str,
) -> Option<serde_json::Value> {
    rt.block_on(tap.inner().get(ks, key))
        .expect("read the store")
        .map(|entry| serde_json::from_slice(&entry.value).expect("a JSON record"))
}

/// A Retryable plan re-read after a lost generation bump leaves the task
/// running: a later election fences a new generation and a commit lands.
/// Regression for #864.
#[test]
fn a_retryable_plan_reread_during_election_keeps_the_task() {
    let rt = runtime();
    let tap = TapStore::new(store());
    let mut cfg = config(Some("worker-a"));
    // Longer than the test runs, so no reconcile lists the hidden rewrite.
    cfg.reconcile_interval = Duration::from_secs(600);
    let mut a =
        StoreCoordinator::new(tap.clone(), cfg, rt.handle().clone(), None).expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final("reread:v1", &["x0"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "claiming x0", |h| h.splits.len() == 1);
    drive(&mut a, &mut held, "sealing the plan", |_| {
        let plan = record(&rt, &tap, Keyspace::Durable, "plan");
        plan.is_some_and(|plan| plan["finality"] == "final")
    });

    // A competing write hidden from the worker's watch leaves its cached
    // plan revision stale, so its next generation bump loses the CAS.
    tap.hide(|ks, key| ks == Keyspace::Durable && key == "plan");
    let plan = rt
        .block_on(tap.inner().get(Keyspace::Durable, "plan"))
        .unwrap()
        .expect("plan record");
    let g0 = serde_json::from_slice::<serde_json::Value>(&plan.value).unwrap()["generation"]
        .as_u64()
        .unwrap();
    let rewrite = rt
        .block_on(
            tap.inner()
                .update(Keyspace::Durable, "plan", plan.value, plan.revision),
        )
        .unwrap();
    assert!(matches!(rewrite, CasOutcome::Won(_)), "{rewrite:?}");

    // The test deletes the leader key through the inner store, so this
    // counts only the worker's own demotes.
    let leader_deletes = Arc::new(AtomicU64::new(0));
    let deletes = Arc::clone(&leader_deletes);
    tap.on_write(move |w| {
        if w.op == Op::Delete && w.ks == Keyspace::Ephemeral && w.key == "leader" {
            deletes.fetch_add(1, Ordering::SeqCst);
        }
        None
    });
    let refusing = Arc::new(AtomicBool::new(true));
    let (tx, rx) = mpsc::channel();
    tap.on_get(move |ks, key| {
        (ks == Keyspace::Durable && key == "plan" && refusing.swap(false, Ordering::AcqRel)).then(
            || {
                let _ = tx.send(());
                StoreError::Retryable("injected: store unreachable".into())
            },
        )
    });
    let _: CasOutcome = rt
        .block_on(tap.inner().delete(Keyspace::Ephemeral, "leader", None))
        .unwrap();
    rx.recv_timeout(DEADLINE)
        .expect("the worker never re-read the plan record");

    drive(&mut a, &mut held, "fencing a generation", |_| {
        let Some(leader) = record(&rt, &tap, Keyspace::Ephemeral, "leader") else {
            return false;
        };
        let plan = record(&rt, &tap, Keyspace::Durable, "plan").expect("plan record");
        let generation = plan["generation"].as_u64().unwrap();
        leader["generation"].as_u64() == Some(generation) && generation > g0
    });
    assert!(
        leader_deletes.load(Ordering::SeqCst) >= 1,
        "the worker kept leadership through the refused re-read"
    );
    let committed = a.commit(&split_id("x0"), &SplitProgress::new(7, vec![]));
    assert!(committed.is_ok(), "commit returned {committed:?}");
}
