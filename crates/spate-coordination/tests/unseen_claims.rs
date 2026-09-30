//! A fleet whose store reports each change some time after it lands keeps
//! every split with the worker it was assigned to until that worker's claim
//! reaches the leader.

mod support;

use spate_coordination::{
    CoordinationErrorKind, CoordinationEvent, SplitCoordinator, SplitProgress, StoreCoordinator,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use support::lagged::LaggedStore;
use support::{
    DEADLINE, Held, POLL_INTERVAL, PhasedPlanner, config, consent_to_revocations, runtime,
    split_id, store,
};

const WORKERS: usize = 4;
const LANES: usize = 2;
const SPLITS: usize = 48;
/// Completions measured. The queue then still holds two fleets' worth of
/// lanes, so every lane stays busy and no improving move is due.
const MEASURED: usize = SPLITS - 2 * WORKERS * LANES;
/// How late each worker's watch reports a change.
const LAG: Duration = Duration::from_millis(100);
/// How long a worker holds a split before completing it.
const HOLD: Duration = Duration::from_millis(200);
/// Room for a worker stalled past its presence lease, whose splits move
/// to its peers and are revoked when it returns.
const REVOCATIONS: usize = 2;

/// A fleet working through a deep queue of short splits revokes at most
/// [`REVOCATIONS`] while each claim reaches the leader [`LAG`] late.
/// Regression for #820.
#[test]
fn short_splits_stay_with_their_assignees_until_their_claims_are_seen() {
    let rt = runtime();
    let shared = store();
    let ids: Vec<String> = (0..SPLITS).map(|i| format!("c{i:02}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let mut fleet: Vec<_> = (0..WORKERS)
        .map(|w| {
            let mut cfg = config(Some(&format!("worker-{w}")));
            cfg.max_in_flight = LANES as u32;
            let store = LaggedStore::new(shared.clone(), LAG);
            let mut worker =
                StoreCoordinator::new(store, cfg, rt.handle().clone(), None).expect("coordinator");
            worker
                .start(Box::new(PhasedPlanner::one_final("unseen-claims:v1", &ids)))
                .unwrap();
            (worker, Held::default(), BTreeMap::<String, Instant>::new())
        })
        .collect();

    let mut completed = 0;
    let mut revoked: Vec<String> = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    while completed < MEASURED {
        assert!(
            Instant::now() < deadline,
            "timed out after {completed} completions and {} revocations",
            revoked.len()
        );
        for (worker, held, since) in &mut fleet {
            let events = worker.poll().unwrap();
            for event in &events {
                if let CoordinationEvent::RevokeRequested { split } = event {
                    revoked.push(split.as_str().to_string());
                }
            }
            held.fold(events);
            consent_to_revocations(worker, held);
            since.retain(|id, _| held.splits.contains_key(id));
            for id in held.splits.keys() {
                since.entry(id.clone()).or_insert_with(Instant::now);
            }
            let done: Vec<String> = since
                .iter()
                .filter(|(_, at)| at.elapsed() >= HOLD)
                .map(|(id, _)| id.clone())
                .collect();
            for id in done {
                match worker.commit(&split_id(&id), &SplitProgress::completed(100, vec![])) {
                    Ok(()) => completed += 1,
                    Err(e) if e.kind == CoordinationErrorKind::Fenced => {}
                    Err(e) => panic!("commit failed: {e}"),
                }
                held.splits.remove(&id);
                since.remove(&id);
            }
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    assert!(
        revoked.len() <= REVOCATIONS,
        "{} revocations over {completed} completions: {revoked:?}",
        revoked.len()
    );
}
