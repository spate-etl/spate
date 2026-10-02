//! A plan run whose seeding fails partway. Its own test binary because it
//! installs the process-wide metrics recorder.

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_coordination::{CoordinationConfig, SplitCoordinator, StoreCoordinator};
use spate_core::clock::tokio::Clock;
use spate_core::metrics::{
    ComponentLabels, CoordinationMetrics, Exporter, MetricsHandle, MetricsSettings, install,
};
use spate_test::metric_value;
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
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

/// A run that keeps seeding while most writes fail is not cut off at
/// `replan_interval`: it seeds and publishes the whole plan with no failed
/// run.
#[test]
fn a_run_that_keeps_seeding_outlasts_replan_interval() {
    let handle = recorder();
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()));
    store.pass_every(3);
    let config = support::config(Some("solo"));
    let replan_interval = config.replan_interval;
    let step = config.op_timeout / 4;
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

/// A [`CountingStore`] whose `split.` listings wait while the gate is shut.
#[derive(Clone)]
struct GatedStore {
    inner: CountingStore,
    shut: Arc<tokio::sync::watch::Sender<bool>>,
}

impl CoordinationStore for GatedStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        if ks == Keyspace::Durable && prefix == "split." {
            let mut rx = self.shut.subscribe();
            let _ = rx.wait_for(|shut| !*shut).await;
        }
        self.inner.list(ks, prefix).await
    }
}

/// A run that seeds every split while its worker is re-elected during the
/// recount publishes nothing, and the next run publishes under the new
/// generation.
#[test]
fn a_run_deposed_during_its_recount_publishes_nothing() {
    let _handle = recorder();
    let rt = runtime();
    let clock = TestClock::frozen();
    let counting = CountingStore::new(store_with_clock(clock.clone()));
    counting.fail_create_always("spec.c000");
    let store = GatedStore {
        inner: counting.clone(),
        shut: Arc::new(tokio::sync::watch::Sender::new(false)),
    };
    let mut config = support::config(Some("solo"));
    config.replan_interval = Duration::from_secs(60);
    let step = config.op_timeout / 4;
    let ids: Vec<String> = (0..SPLITS).map(|i| format!("c{i:03}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let mut worker = StoreCoordinator::with_clock(
        store.clone(),
        config,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("seed-recount:v1", &ids)))
        .unwrap();
    let step_until =
        |worker: &mut StoreCoordinator<GatedStore>, what: &str, done: &dyn Fn() -> bool| {
            let deadline = Instant::now() + support::DEADLINE;
            while !done() {
                assert!(Instant::now() < deadline, "timed out: {what}");
                clock.advance(step);
                std::thread::sleep(support::POLL_INTERVAL);
                worker.poll().expect("poll");
            }
        };

    step_until(&mut worker, "seeding all but c000", &|| {
        seeded(&rt, &counting.inner) == SPLITS - 1
    });
    store.shut.send_replace(true);
    counting.heal();
    step_until(&mut worker, "c000 seeded", &|| {
        seeded(&rt, &counting.inner) == SPLITS
    });
    let deleted = rt.block_on(counting.inner.delete(Keyspace::Ephemeral, "leader", None));
    assert!(deleted.unwrap().won().is_some());
    step_until(&mut worker, "the re-election", &|| {
        plan(&rt, &counting.inner)["generation"].as_u64() == Some(2)
    });
    store.shut.send_replace(false);
    step_until(&mut worker, "the plan publishing", &|| {
        plan(&rt, &counting.inner)["planned"].as_u64() == Some(SPLITS as u64)
    });
    let plan = plan(&rt, &counting.inner);
    assert_eq!(plan["generation"].as_u64(), Some(2), "{plan}");
}

/// A run whose worker is re-elected while a write keeps failing ends well
/// inside `replan_interval`, without waiting out its patience.
#[test]
fn a_deposed_run_stops_before_its_patience_runs_out() {
    let handle = recorder();
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()));
    store.fail_create_always("spec.c000");
    let mut config = support::config(Some("solo"));
    config.replan_interval = Duration::from_secs(60);
    let replan_interval = config.replan_interval;
    let step = config.op_timeout / 4;
    let mut worker = leader(&rt, &store, &clock, config, "seed-deposed");
    let errors = || {
        metric_value(
            &handle.render(),
            "spate_coordination_replans_total",
            &[("component", "seed-deposed"), ("outcome", "error")],
        )
    };

    advance_until(&mut worker, &clock, step, "seeding all but c000", || {
        seeded(&rt, &store.inner) == SPLITS - 1
    });
    let deleted = rt.block_on(store.inner.delete(Keyspace::Ephemeral, "leader", None));
    assert!(deleted.unwrap().won().is_some());
    let mut advanced = Duration::ZERO;
    advance_until(&mut worker, &clock, step, "the deposed run ending", || {
        assert!(
            advanced < replan_interval / 2,
            "the deposed run kept retrying for {advanced:?}"
        );
        advanced += step;
        errors() >= Some(1.0)
    });
}

/// After a retryable failure a run keeps at most half its writes in flight.
#[test]
fn a_pause_halves_the_writes_in_flight() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()))
        .with_create_delay(Duration::from_millis(20));
    store.fail_first_creates(64);
    let ids: Vec<String> = (0..64).map(|i| format!("h{i:02}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let config = support::config(Some("solo"));
    let step = config.op_timeout / 4;
    let mut worker = StoreCoordinator::with_clock(
        store.clone(),
        config,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("seed-halved:v1", &ids)))
        .unwrap();
    advance_until(&mut worker, &clock, step, "seeding every split", || {
        seeded(&rt, &store.inner) == 64
    });
    let max = store.stats.max_in_flight.load(SeqCst);
    assert!(max <= 32, "{max} writes in flight after a pause");
}

/// A final plan whose seeding outlasts a replan tick runs the planner once.
#[test]
fn a_final_plan_seeded_across_a_replan_tick_plans_once() {
    let handle = recorder();
    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()));
    store.pass_every(3);
    let config = support::config(Some("solo"));
    let step = config.op_timeout / 4;
    let mut worker = leader(&rt, &store, &clock, config, "seed-final-once");
    advance_until(&mut worker, &clock, step, "the plan publishing", || {
        plan(&rt, &store.inner)["planned"].as_u64() == Some(SPLITS as u64)
    });
    for _ in 0..24 {
        clock.advance(step);
        std::thread::sleep(support::POLL_INTERVAL);
        worker.poll().expect("poll");
    }
    assert_eq!(
        metric_value(
            &handle.render(),
            "spate_coordination_replans_total",
            &[("component", "seed-final-once"), ("outcome", "noop")],
        ),
        Some(0.0)
    );
}

/// A store that declares a polled watch whose listing never reports a change
/// after its snapshot, so only the coordinator's own work moves it.
#[derive(Clone)]
struct QuietPolled(CountingStore);

impl CoordinationStore for QuietPolled {
    fn lease_ttl(&self) -> Duration {
        self.0.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        WatchMode::Polled {
            interval: support::LEASE / 10,
        }
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.0.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.0.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.0.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.0.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        let snapshot = self.0.list(ks, prefix).await?;
        let head = snapshot
            .into_iter()
            .map(WatchEvent::Put)
            .chain(std::iter::once(WatchEvent::SnapshotDone))
            .map(Ok);
        Ok(futures_util::stream::iter(head)
            .chain(futures_util::stream::pending())
            .boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.0.list(ks, prefix).await
    }
}

/// On a store whose watch is polled, splits a run has seeded are assigned
/// while another split's write keeps failing, before the run ends.
#[test]
fn seeded_splits_are_assigned_before_the_run_ends_on_a_polled_store() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let counting = CountingStore::new(store_with_clock(clock.clone()));
    counting.fail_create_always("spec.c000");
    let ids: Vec<String> = (0..SPLITS).map(|i| format!("c{i:03}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let mut worker = StoreCoordinator::with_clock(
        QuietPolled(counting.clone()),
        support::config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("seed-polled:v1", &ids)))
        .unwrap();
    let mut held = support::Held::default();
    support::drive(&mut worker, &mut held, "claiming a seeded split", |h| {
        !h.splits.is_empty()
    });
    assert_eq!(plan(&rt, &counting.inner)["planned"].as_u64(), Some(0));
}

/// [`QuietPolled`] at a poll interval no test reaches, so no refresh tick steps the loop.
#[derive(Clone)]
struct SlowPolled(QuietPolled);

impl CoordinationStore for SlowPolled {
    fn lease_ttl(&self) -> Duration {
        self.0.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        WatchMode::Polled {
            interval: Duration::from_secs(10),
        }
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.0.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.0.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.0.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.0.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.0.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.0.list(ks, prefix).await
    }
}

/// Splits folded inside the step interval after the first fold are assigned once
/// it passes, with no heartbeat, refresh, reconcile or run end due.
#[test]
fn splits_folded_inside_the_step_interval_are_assigned_when_it_passes() {
    let handle = recorder();
    let lease = Duration::from_secs(30);
    let rt = runtime();
    let clock = TestClock::frozen();
    let counting = CountingStore::new(MemoryStore::with_clock(lease, clock.clone()));
    // The first wave's first ten creates fail, so the run pauses and seeds the
    // rest only once the clock moves past the first fold.
    counting.fail_first_creates(10);
    // The run never ends, so no Done step assigns what was folded.
    counting.fail_create_always("spec.c099");
    let mut config = support::config_for(lease, Some("solo"));
    config.max_in_flight = SPLITS as u32;
    // The first reconcile lands at 0 or at least 3.5 s, never inside the
    // second this test steps through.
    config.reconcile_interval = Duration::from_secs(3600);
    let ids: Vec<String> = (0..SPLITS).map(|i| format!("c{i:03}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let labels = ComponentLabels::new("seed-deferred", "seed-deferred", "s3");
    let mut worker = StoreCoordinator::with_clock(
        SlowPolled(QuietPolled(counting.clone())),
        config,
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&labels)),
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("seed-deferred:v1", &ids)))
        .unwrap();
    let planned = || {
        metric_value(
            &handle.render(),
            "spate_coordination_splits_planned_total",
            &[("component", "seed-deferred")],
        )
        .unwrap_or(0.0)
    };

    let mut held = support::Held::default();
    support::drive(&mut worker, &mut held, "claiming the first fold", |h| {
        !h.splits.is_empty()
    });
    // Ends the first pause: every split but c099 is seeded and folded inside
    // the interval the first fold opened.
    clock.advance(Duration::from_millis(100));
    let deadline = Instant::now() + support::DEADLINE;
    while planned() < (SPLITS - 1) as f64 {
        assert!(
            Instant::now() < deadline,
            "timed out folding the seeded splits"
        );
        std::thread::sleep(support::POLL_INTERVAL);
        held.fold(worker.poll().expect("poll"));
    }
    assert!(
        held.splits.len() < SPLITS - 1,
        "a step ran before the interval passed"
    );
    // The first fold's interval ends here, and no other timer is due.
    clock.advance(Duration::from_millis(900));
    support::drive(&mut worker, &mut held, "claiming every folded split", |h| {
        h.splits.len() == SPLITS - 1
    });
}
