//! The fleet size an idle worker reports after its own presence key lapsed.
//!
//! Its own binary: the exporter installs a process-global recorder.
mod support;

use spate_coordination::SplitCoordinator as _;
use spate_coordination::StoreCoordinator;
use spate_coordination::store::{CoordinationStore as _, Keyspace, StoreError};
use spate_core::clock::tokio::Clock as _;
use spate_core::metrics::{
    ComponentLabels, CoordinationMetrics, Exporter, MetricsSettings, install,
};
use spate_test::metric_value;
use support::tap::{Op, TapStore};
use support::{Held, LEASE, PhasedPlanner, runtime};

const KEY: &str = "worker.idle-b";

/// A worker that holds no split and is not parting counts itself after its
/// presence key expired.
#[test]
fn an_idle_worker_whose_presence_lapsed_counts_itself() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let tap = TapStore::new(store.clone());
    let mut fleet = support::Fleet::new(&store, rt.handle());
    let planner = || Box::new(PhasedPlanner::one_final("idle-lapsed:v1", &["p0"]));
    let labels = |n: &'static str| {
        Some(CoordinationMetrics::new(&ComponentLabels::new(
            "idle-lapsed",
            n,
            "s3",
        )))
    };
    let mut a = StoreCoordinator::with_clock(
        store.clone(),
        support::config(Some("idle-a")),
        rt.handle().clone(),
        labels("idle-a"),
        clock.clone(),
    )
    .expect("coordinator a");
    a.start(planner()).unwrap();
    fleet.join(&a);
    let mut held_a = Held::default();
    let budget = clock.now() + 4 * LEASE;
    while held_a.splits.is_empty() {
        assert!(clock.now() < budget, "idle-a never claimed its split");
        fleet.step(&clock, LEASE / 12);
        held_a.fold(a.poll().expect("poll a"));
    }
    let mut b = StoreCoordinator::with_clock(
        tap.clone(),
        support::config(Some("idle-b")),
        rt.handle().clone(),
        labels("idle-b"),
        clock.clone(),
    )
    .expect("coordinator b");
    b.start(planner()).unwrap();
    fleet.join(&b);
    let mut held_b = Held::default();
    let live = || {
        metric_value(
            &handle.render(),
            "spate_coordination_live_workers",
            &[("component", "idle-b")],
        )
    };
    let budget = clock.now() + 4 * LEASE;
    while live() != Some(2.0) {
        assert!(clock.now() < budget, "idle-b never saw a fleet of two");
        fleet.step(&clock, LEASE / 12);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
    }
    tap.on_write(|w| {
        (matches!(w.op, Op::Create | Op::Update) && w.ks == Keyspace::Ephemeral && w.key == KEY)
            .then(|| StoreError::Retryable("injected: throttled".into()))
    });
    let budget = clock.now() + 3 * LEASE;
    while rt
        .block_on(store.get(Keyspace::Ephemeral, KEY))
        .unwrap()
        .is_some()
    {
        assert!(clock.now() < budget, "the presence key never lapsed");
        fleet.step(&clock, LEASE / 12);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
    }
    for _ in 0..4 {
        fleet.step(&clock, LEASE / 12);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
    }
    assert!(
        rt.block_on(store.get(Keyspace::Ephemeral, KEY))
            .unwrap()
            .is_none(),
        "the presence key came back"
    );
    assert!(held_b.splits.is_empty(), "idle-b claimed a split");
    assert_eq!(
        live(),
        Some(2.0),
        "an idle worker without its presence key counts itself"
    );
}
