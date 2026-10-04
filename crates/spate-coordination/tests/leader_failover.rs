//! Leadership and planning under failure: election, generation fencing,
//! idempotent replanning, open plans that grow across replan ticks, and
//! elections over a final plan, which run no planner.

mod support;

use spate_coordination::store::{CoordinationStore as _, Keyspace};
use spate_coordination::{
    CoordinationError, PlanContext, PlanFinality, SplitCoordinator, SplitPlan, SplitPlanner,
    SplitProgress,
};
use spate_core::clock::tokio::Clock;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Instant;
use support::{
    CountingStore, Fleet, Held, LEASE, PhasedPlanner, TestClock, crash, drive, runtime, split_id,
    splits, store, store_with_clock, worker, worker_with_clock,
};

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
    // the planner (the same ids are already in its view, so it writes
    // none of them), seal the plan on the next phase, take the work over,
    // and finish the job.
    crash(rt_a, a);
    let counted = CountingStore::new(store.clone());
    let mut b = counted.worker(rt.handle(), "worker-b");
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
    assert_eq!(
        counted.stats.creates.load(SeqCst),
        0,
        "B must not re-seed splits its watch already delivered"
    );
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

/// Plans the single split `unfinished` at a fixed finality and counts its runs.
struct CountedPlanner {
    calls: Arc<AtomicUsize>,
    finality: PlanFinality,
}

impl SplitPlanner for CountedPlanner {
    fn fingerprint(&self) -> String {
        "election:v1".into()
    }

    fn plan(&mut self, _: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
        self.calls.fetch_add(1, SeqCst);
        Ok(SplitPlan::new(splits(&["unfinished"]), self.finality))
    }
}

/// Runs a leader that plans at `finality` and departs, elects a second
/// worker, and returns the planner runs counted fleet-wide.
///
/// The clock stays frozen, so no replan tick fires.
fn elect_after_departure(finality: PlanFinality) -> usize {
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = store_with_clock(clock.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let planner = || {
        Box::new(CountedPlanner {
            calls: calls.clone(),
            finality,
        })
    };

    let mut first = worker_with_clock(
        &store,
        rt.handle(),
        Some("first"),
        clock.clone() as Arc<dyn Clock>,
    );
    first.start(planner()).unwrap();
    let mut held = Held::default();
    drive(&mut first, &mut held, "the first worker planning", |h| {
        h.splits.contains_key("unfinished")
    });
    assert_eq!(calls.load(SeqCst), 1);
    first.depart(&[split_id("unfinished")]).unwrap();

    let mut second = worker_with_clock(
        &store,
        rt.handle(),
        Some("second"),
        clock.clone() as Arc<dyn Clock>,
    );
    second.start(planner()).unwrap();
    let mut fleet = Fleet::new(&store, rt.handle());
    fleet.join(&second);
    let mut held = Held::default();
    drive(
        &mut second,
        &mut held,
        "the second worker acquiring the departed split",
        |h| h.splits.contains_key("unfinished"),
    );
    fleet.settle(&clock);

    let leader = rt
        .block_on(store.get(Keyspace::Ephemeral, "leader"))
        .unwrap()
        .expect("leader key");
    let leader: serde_json::Value = serde_json::from_slice(&leader.value).unwrap();
    assert_eq!(leader["owner"], "second", "the second worker must lead");
    calls.load(SeqCst)
}

/// A leader elected over a final plan assigns its splits without running the
/// planner. Regression for #883.
#[test]
fn a_leader_elected_over_a_final_plan_runs_no_planner() {
    assert_eq!(
        elect_after_departure(PlanFinality::Final),
        1,
        "a leader elected over a final plan ran the planner again"
    );
}

/// A leader elected over an open plan runs the planner once more.
#[test]
fn a_leader_elected_over_an_open_plan_runs_the_planner() {
    assert_eq!(
        elect_after_departure(PlanFinality::Open),
        2,
        "a leader elected over an open plan did not run the planner"
    );
}
