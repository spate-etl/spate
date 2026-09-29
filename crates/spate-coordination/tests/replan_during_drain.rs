//! A leader that republishes while a split it moved is still draining.

mod support;

use spate_coordination::{PlanFinality, PlannedSplit, SplitCoordinator as _, SplitId, SplitSpec};
use std::time::Instant;
use support::{Held, LEASE, PhasedPlanner, runtime};

fn weighted(entries: &[(&str, u64)]) -> Vec<PlannedSplit> {
    entries
        .iter()
        .map(|(id, w)| {
            PlannedSplit::new(
                SplitSpec::new(
                    SplitId::new(*id).unwrap(),
                    format!("descriptor:{id}").into_bytes(),
                )
                .with_weight(*w),
            )
        })
        .collect()
}

/// worker-0 fills its three lanes with `b` (weight 3), `c` and `d`, and `a`
/// is queued behind them. worker-1 then joins, and nobody consents to a
/// revocation, so every move stays draining. Returns the revocations each
/// worker was asked for.
fn join_while_full(fingerprint: &str) -> (Vec<String>, Vec<String>) {
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let planner = || {
        Box::new(PhasedPlanner {
            fingerprint: fingerprint.to_string(),
            phases: vec![
                (
                    weighted(&[("b", 3), ("c", 1), ("d", 1)]),
                    PlanFinality::Open,
                ),
                (weighted(&[("a", 1)]), PlanFinality::Final),
            ],
        })
    };
    let tune = |c: &mut spate_coordination::CoordinationConfig| {
        c.max_in_flight = 3;
        c.drain_deadline = LEASE * 20;
    };
    let mut w0 = support::worker_tuned_clock(&store, rt.handle(), "worker-0", clock.clone(), tune);
    w0.start(planner()).unwrap();
    let mut fleet = support::Fleet::new(&store, rt.handle());
    fleet.join(&w0);
    let mut h0 = Held::default();
    let step = LEASE / 12;
    let deadline = Instant::now() + support::DEADLINE;
    while h0.splits.len() < 3 {
        assert!(
            Instant::now() < deadline,
            "worker-0 never held three splits"
        );
        fleet.step(&clock, step);
        h0.fold(w0.poll().unwrap());
        support::commit_held(&mut w0, &h0);
    }
    // Three leases of clock: the second plan phase lands `a` while worker-0 is full.
    for _ in 0..36 {
        fleet.step(&clock, step);
        h0.fold(w0.poll().unwrap());
        support::commit_held(&mut w0, &h0);
    }
    assert_eq!(
        h0.splits.keys().cloned().collect::<Vec<_>>(),
        ["b", "c", "d"]
    );
    assert!(h0.revoke_requests.is_empty(), "{:?}", h0.revoke_requests);

    let mut w1 = support::worker_tuned_clock(&store, rt.handle(), "worker-1", clock.clone(), tune);
    w1.start(planner()).unwrap();
    fleet.join(&w1);
    let mut h1 = Held::default();
    for _ in 0..48 {
        fleet.step(&clock, step);
        h0.fold(w0.poll().unwrap());
        h1.fold(w1.poll().unwrap());
        support::commit_held(&mut w0, &h0);
        support::commit_held(&mut w1, &h1);
    }
    (h0.revoke_requests, h1.revoke_requests)
}

/// A publish on a view that has not changed since the last one asks for no
/// new revocation while the first move is still draining.
#[test]
fn a_replan_during_a_drain_revokes_nothing_new() {
    // The fingerprint keys the tie-break, which picks the move. Moving `b`
    // leaves worker-0 draining it with a lane that `a`'s last assignee would
    // otherwise claim, so at least one fingerprint has to pick `b`.
    let mut moved_b = false;
    for fingerprint in [
        "replan-during-drain:v1",
        "replan-during-drain:v2",
        "replan-during-drain:v3",
        "replan-during-drain:v4",
    ] {
        let (w0, w1) = join_while_full(fingerprint);
        // Either set balances the fleet at 3 against 3. Asking for both
        // means a publish reversed the first move.
        assert!(
            [vec!["b"], vec!["c", "d"]].contains(&w0.iter().map(String::as_str).collect()),
            "{fingerprint}: worker-0 was asked to give up {w0:?}"
        );
        assert!(
            w1.is_empty(),
            "{fingerprint}: worker-1 was asked to give up {w1:?}"
        );
        moved_b |= w0 == ["b"];
    }
    assert!(
        moved_b,
        "no fingerprint moved b, so nothing contested a lane"
    );
}
