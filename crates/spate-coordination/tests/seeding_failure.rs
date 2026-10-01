//! A plan run whose seeding fails partway. Its own test binary because it
//! installs the process-wide metrics recorder.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{CoordinationStore as _, Keyspace};
use spate_coordination::{CoordinationConfig, SplitCoordinator, StoreCoordinator};
use spate_core::clock::tokio::Clock;
use spate_core::metrics::{
    ComponentLabels, CoordinationMetrics, Exporter, MetricsHandle, MetricsSettings, install,
};
use spate_test::metric_value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::{CountingStore, PhasedPlanner, TestClock, runtime, store_with_clock};

const SPLITS: usize = 100;

fn recorder() -> MetricsHandle {
    install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter")
}

/// A solo leader over `store` planning `SPLITS` splits in one final phase.
fn leader(
    rt: &tokio::runtime::Runtime,
    store: &CountingStore,
    clock: &Arc<TestClock>,
    config: CoordinationConfig,
    component: &'static str,
) -> StoreCoordinator<CountingStore> {
    let ids: Vec<String> = (0..SPLITS).map(|i| format!("c{i:03}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let labels = ComponentLabels::new(component, component, "s3");
    let mut worker = StoreCoordinator::with_clock(
        store.clone(),
        config,
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&labels)),
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            &format!("{component}:v1"),
            &ids,
        )))
        .unwrap();
    worker
}

/// The plan record as JSON, `null` before it exists.
fn plan(rt: &tokio::runtime::Runtime, store: &MemoryStore) -> serde_json::Value {
    rt.block_on(store.get(Keyspace::Durable, "plan"))
        .unwrap()
        .map_or(serde_json::Value::Null, |entry| {
            serde_json::from_slice(&entry.value).unwrap()
        })
}

/// Step `clock` by `step` until `done`, polling `worker` between steps.
fn advance_until(
    worker: &mut StoreCoordinator<CountingStore>,
    clock: &TestClock,
    step: Duration,
    what: &str,
    mut done: impl FnMut() -> bool,
) {
    let deadline = Instant::now() + support::DEADLINE;
    while !done() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        clock.advance(step);
        std::thread::sleep(support::POLL_INTERVAL);
        worker.poll().expect("poll");
    }
}

fn seeded(rt: &tokio::runtime::Runtime, store: &MemoryStore) -> usize {
    rt.block_on(store.list(Keyspace::Durable, "split."))
        .unwrap()
        .len()
}

/// A run whose first 70 creates are throttled seeds and publishes the whole
/// plan before a replan tick is due. Regression for #870.
#[test]
fn a_throttled_seed_run_seeds_the_whole_plan_within_the_run() {
    let handle = recorder();
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()));
    store.fail_first_creates(70);
    let mut config = support::config(Some("solo"));
    config.replan_interval = Duration::from_secs(60);
    let replan_interval = config.replan_interval;
    let step = config.op_timeout / 4;
    let mut worker = leader(&rt, &store, &clock, config, "seed-throttled");

    let deadline = Instant::now() + support::DEADLINE;
    let mut advanced = Duration::ZERO;
    while plan(&rt, &store.inner)["planned"].as_u64() != Some(SPLITS as u64) {
        assert!(
            advanced < replan_interval,
            "seeding waited for a replan tick: {} of {SPLITS} splits seeded",
            seeded(&rt, &store.inner)
        );
        assert!(Instant::now() < deadline, "timed out seeding the plan");
        clock.advance(step);
        advanced += step;
        std::thread::sleep(support::POLL_INTERVAL);
        worker.poll().expect("poll");
    }
    assert_eq!(seeded(&rt, &store.inner), SPLITS);
    assert_eq!(plan(&rt, &store.inner)["finality"], "final");
    assert_eq!(
        metric_value(
            &handle.render(),
            "spate_coordination_splits_planned_total",
            &[("component", "seed-throttled")],
        ),
        Some(SPLITS as f64)
    );
}

/// A split whose create keeps failing ends the run once `replan_interval`
/// passes without a split seeded: the other splits are seeded and counted,
/// the run records an error, and the plan is not published.
#[test]
fn a_seed_that_keeps_failing_ends_the_run_unpublished() {
    let handle = recorder();
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()));
    store.fail_create_always("spec.c000");
    let config = support::config(Some("solo"));
    let step = support::LEASE / 12;
    let mut worker = leader(&rt, &store, &clock, config, "seed-failing");
    let metric = |name: &str, labels: &[(&str, &str)]| {
        let mut labels = labels.to_vec();
        labels.push(("component", "seed-failing"));
        metric_value(&handle.render(), name, &labels)
    };

    let deadline = Instant::now() + support::DEADLINE;
    while metric("spate_coordination_replans_total", &[("outcome", "error")]) < Some(1.0) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the run to end"
        );
        clock.advance(step);
        std::thread::sleep(support::POLL_INTERVAL);
        worker.poll().expect("poll");
    }
    assert_eq!(seeded(&rt, &store.inner), SPLITS - 1);
    assert_eq!(
        metric("spate_coordination_splits_planned_total", &[]),
        Some((SPLITS - 1) as f64)
    );
    let plan = plan(&rt, &store.inner);
    assert_eq!(plan["planned"].as_u64(), Some(0), "{plan}");
    assert_eq!(plan["finality"], "open", "{plan}");
}

/// A leader re-elected while its run retries a write ends that run without
/// publishing, and the next run publishes the plan under the new generation
/// with the leader still in place.
#[test]
fn a_run_from_an_earlier_term_publishes_nothing() {
    let _handle = recorder();
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()));
    store.fail_create_always("spec.c000");
    let mut config = support::config(Some("solo"));
    config.replan_interval = Duration::from_secs(60);
    let step = config.op_timeout / 4;
    let mut worker = leader(&rt, &store, &clock, config, "seed-reelected");

    advance_until(&mut worker, &clock, step, "seeding all but c000", || {
        seeded(&rt, &store.inner) == SPLITS - 1
    });
    let deleted = rt.block_on(store.inner.delete(Keyspace::Ephemeral, "leader", None));
    assert!(deleted.unwrap().won().is_some());
    advance_until(&mut worker, &clock, step, "the re-election", || {
        plan(&rt, &store.inner)["generation"].as_u64() == Some(2)
    });
    store.heal();
    advance_until(&mut worker, &clock, step, "the plan publishing", || {
        plan(&rt, &store.inner)["planned"].as_u64() == Some(SPLITS as u64)
    });
    let plan = plan(&rt, &store.inner);
    assert_eq!(plan["generation"].as_u64(), Some(2), "{plan}");
}

/// A run that keeps seeding while every other write fails is not cut off at
/// `replan_interval`: it seeds and publishes the whole plan with no failed
/// run.
#[test]
fn a_run_that_keeps_seeding_outlasts_replan_interval() {
    let handle = recorder();
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()));
    store.fail_every(2);
    let config = support::config(Some("solo"));
    let replan_interval = config.replan_interval;
    let step = support::LEASE / 12;
    let mut worker = leader(&rt, &store, &clock, config, "seed-slow");

    let mut advanced = Duration::ZERO;
    advance_until(&mut worker, &clock, step, "the plan publishing", || {
        advanced += step;
        plan(&rt, &store.inner)["planned"].as_u64() == Some(SPLITS as u64)
    });
    assert!(
        advanced > replan_interval,
        "the run took {advanced:?}, inside one replan_interval"
    );
    assert_eq!(
        metric_value(
            &handle.render(),
            "spate_coordination_replans_total",
            &[("component", "seed-slow"), ("outcome", "error")],
        ),
        Some(0.0)
    );
}
