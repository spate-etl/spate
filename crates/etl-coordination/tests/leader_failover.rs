//! Leadership and planning under failure: election, generation fencing,
//! idempotent replanning, and open plans that grow across replan ticks.

mod support;

use etl_coordination::store::{CoordinationStore as _, Keyspace};
use etl_coordination::{PlanFinality, SplitCoordinator, SplitProgress};
use std::time::Instant;
use support::{Held, LEASE, PhasedPlanner, crash, drive, runtime, split_id, splits, store, worker};

#[test]
fn leader_death_hands_planning_over_and_replans_idempotently() {
    let rt = runtime();
    let store = store();
    // Both workers present the same two-phase planner: phase 0 plans the
    // batch as Open, phase 1 seals it Final. Phases are cursor-keyed, so
    // whoever leads next continues rather than restarting.
    let planner = || {
        Box::new(PhasedPlanner {
            fingerprint: "failover:v1".to_string(),
            phases: vec![
                (splits(&["f0", "f1"]), PlanFinality::Open),
                (splits(&["f0", "f1"]), PlanFinality::Final),
            ],
        })
    };

    // A starts alone: it elects itself and plans phase 0.
    let rt_a = runtime();
    let mut a = worker(&store, rt_a.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(
        &mut a,
        &mut held_a,
        "A planning and claiming phase 0",
        |h| h.splits.len() == 2,
    );

    // The leader dies. B must take leadership after the lease, re-run
    // the planner (create-if-absent: the same ids are a no-op), seal the
    // plan on the next phase, take the work over, and finish the job.
    crash(rt_a, a);
    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive(
        &mut b,
        &mut held_b,
        "B taking over the dead leader's work",
        |h| h.splits.len() == 2,
    );
    for id in ["f0", "f1"] {
        b.commit(&split_id(id), &SplitProgress::completed(1, vec![]))
            .unwrap();
    }
    drive(&mut b, &mut held_b, "B finishing the sealed plan", |h| {
        h.all_complete
    });

    // The store agrees: exactly the two splits exist (idempotent replan,
    // no duplicates), and the plan's generation moved past A's.
    let records = rt
        .block_on(store.list(Keyspace::Durable, "split."))
        .unwrap();
    assert_eq!(records.len(), 2, "replanning must not duplicate splits");
    let plan = rt
        .block_on(store.get(Keyspace::Durable, "plan"))
        .unwrap()
        .expect("plan record");
    let plan: serde_json::Value = serde_json::from_slice(&plan.value).unwrap();
    assert!(
        plan["generation"].as_u64().unwrap() >= 2,
        "B's leadership must bump the generation: {plan}"
    );
    assert_eq!(plan["planned"].as_u64().unwrap(), 2);
    assert_eq!(plan["finality"], "final");
}

#[test]
fn open_plans_grow_across_replan_ticks_until_sealed() {
    let rt = runtime();
    let store = store();
    // Three phases: two Open batches, then a Final seal. The planner
    // cursor persisted in the plan record drives the progression.
    let planner = || {
        Box::new(PhasedPlanner {
            fingerprint: "growth:v1".to_string(),
            phases: vec![
                (splits(&["g0"]), PlanFinality::Open),
                (splits(&["g1", "g2"]), PlanFinality::Open),
                (Vec::new(), PlanFinality::Final),
            ],
        })
    };

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held = Held::default();

    // Phase 0 lands immediately; later phases arrive on replan ticks
    // (one lease apart in the test config).
    drive(&mut a, &mut held, "phase 0 work arriving", |h| {
        h.splits.contains_key("g0")
    });
    let phase0_at = Instant::now();
    drive(&mut a, &mut held, "phase 1 work arriving via replan", |h| {
        h.splits.contains_key("g1") && h.splits.contains_key("g2")
    });
    assert!(
        phase0_at.elapsed() >= LEASE / 2,
        "growth must come from a later replan tick, not the first plan"
    );

    // Nothing completes the job while the plan is open; sealing it does.
    for id in ["g0", "g1", "g2"] {
        a.commit(&split_id(id), &SplitProgress::completed(1, vec![]))
            .unwrap();
    }
    drive(&mut a, &mut held, "the sealed plan completing", |h| {
        h.all_complete
    });

    // The plan record reflects the whole arc: three splits, final.
    let plan = rt
        .block_on(store.get(Keyspace::Durable, "plan"))
        .unwrap()
        .expect("plan record");
    let plan: serde_json::Value = serde_json::from_slice(&plan.value).unwrap();
    assert_eq!(plan["planned"].as_u64().unwrap(), 3);
    assert_eq!(plan["finality"], "final");
}
