//! The fleet size a running worker reports after its own presence key lapsed.
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

const KEY: &str = "worker.lapse-a";

/// A worker that is not parting counts itself after its presence key expired.
#[test]
fn a_running_worker_whose_presence_lapsed_counts_itself() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let tap = TapStore::new(support::store_with_clock(clock.clone()));
    let mut fleet = support::Fleet::new(tap.inner(), rt.handle());
    let mut a = StoreCoordinator::with_clock(
        tap.clone(),
        support::config(Some("lapse-a")),
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&ComponentLabels::new(
            "lapsed-gauge",
            "lapse-a",
            "s3",
        ))),
        clock.clone(),
    )
    .expect("coordinator");
    a.start(Box::new(PhasedPlanner::one_final(
        "lapsed-gauge:v1",
        &["p0"],
    )))
    .unwrap();
    fleet.join(&a);
    let mut held = Held::default();
    let budget = clock.now() + 4 * LEASE;
    while held.splits.is_empty() {
        assert!(clock.now() < budget, "lapse-a never claimed its split");
        fleet.step(&clock, LEASE / 12);
        held.fold(a.poll().expect("poll"));
    }
    let live = || {
        metric_value(
            &handle.render(),
            "spate_coordination_live_workers",
            &[("component", "lapse-a")],
        )
    };
    assert!(
        rt.block_on(tap.inner().get(Keyspace::Ephemeral, KEY))
            .unwrap()
            .is_some(),
        "no presence key before arming"
    );
    assert_eq!(live(), Some(1.0), "gauge before arming");
    tap.on_write(|w| {
        (matches!(w.op, Op::Create | Op::Update) && w.ks == Keyspace::Ephemeral && w.key == KEY)
            .then(|| StoreError::Retryable("injected: throttled".into()))
    });
    let budget = clock.now() + 3 * LEASE;
    while rt
        .block_on(tap.inner().get(Keyspace::Ephemeral, KEY))
        .unwrap()
        .is_some()
    {
        assert!(clock.now() < budget, "the presence key never lapsed");
        fleet.step(&clock, LEASE / 12);
        held.fold(a.poll().expect("poll"));
    }
    for _ in 0..4 {
        fleet.step(&clock, LEASE / 12);
        held.fold(a.poll().expect("poll"));
    }
    assert!(
        rt.block_on(tap.inner().get(Keyspace::Ephemeral, KEY))
            .unwrap()
            .is_none(),
        "the presence key came back"
    );
    assert!(!held.splits.is_empty(), "lapse-a stopped holding its split");
    assert_eq!(
        live(),
        Some(1.0),
        "a running worker without its presence key counts itself"
    );
}
