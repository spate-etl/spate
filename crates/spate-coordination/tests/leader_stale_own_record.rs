//! A leader's own assignment record overwritten at an older generation.

mod support;

use spate_coordination::SplitCoordinator as _;
use spate_coordination::SplitProgress;
use spate_coordination::store::{CasOutcome, CoordinationStore as _, Keyspace};
use std::time::Instant;
use support::{Held, LEASE, PhasedPlanner, runtime};

/// A leader keeps reassigning itself after a late older-generation write
/// lands on its own assignment record.
///
/// Regression for #822.
#[test]
fn a_leader_reassigns_itself_after_an_older_generation_write_to_its_record() {
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let ids = ["x", "y", "z"];
    let mut a = support::worker_tuned_clock(&store, rt.handle(), "worker-a", clock.clone(), |c| {
        c.max_in_flight = 2;
    });
    a.start(Box::new(PhasedPlanner::one_final("self-record:v1", &ids)))
        .unwrap();
    let mut held = Held::default();
    let mut fleet = support::Fleet::new(&store, rt.handle());
    fleet.join(&a);
    let deadline = Instant::now() + support::DEADLINE;
    while held.splits.len() < 2 {
        assert!(Instant::now() < deadline, "worker-a never held two splits");
        fleet.step(&clock, LEASE / 12);
        held.fold(a.poll().expect("poll"));
        support::commit_held(&mut a, &held);
    }
    for _ in 0..support::QUIET_ROUNDS {
        fleet.step(&clock, LEASE / 12);
        held.fold(a.poll().expect("poll"));
        support::commit_held(&mut a, &held);
    }
    let queued = ids
        .iter()
        .find(|id| !held.splits.contains_key(**id))
        .expect("one split queued")
        .to_string();

    rt.block_on(async {
        let entry = store
            .get(Keyspace::Durable, "assign.worker-a")
            .await
            .unwrap()
            .expect("record");
        let mut record: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        assert!(
            record["generation"].as_u64().unwrap() >= 1,
            "needs an older generation"
        );
        record["generation"] = 0.into();
        let outcome = store
            .update(
                Keyspace::Durable,
                "assign.worker-a",
                serde_json::to_vec(&record).unwrap(),
                entry.revision,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CasOutcome::Won(_)));
    });

    let done = held.splits.keys().next().expect("held").clone();
    a.commit(
        &support::split_id(&done),
        &SplitProgress::completed(1, vec![]),
    )
    .unwrap();
    held.splits.remove(&done);

    clock.advance_stepped(LEASE * 4, LEASE / 12, || {
        fleet.settle(&clock);
        held.fold(a.poll().expect("poll"));
        support::commit_held(&mut a, &held);
    });
    assert!(
        held.splits.contains_key(&queued),
        "worker-a never claimed {queued}; holds {:?}",
        held.splits.keys().collect::<Vec<_>>()
    );
}
