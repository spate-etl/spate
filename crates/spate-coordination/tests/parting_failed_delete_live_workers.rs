//! The fleet size a parting worker reports while its presence delete failed.
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

/// A worker whose presence delete was refused on release counts its own key
/// once its view holds it again, as the leader does.
#[test]
fn a_parting_worker_counts_its_own_undeleted_key() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let tap = TapStore::new(store.clone());
    let planner = || {
        Box::new(PhasedPlanner::one_final(
            "parting-undeleted:v1",
            &["p0", "p1"],
        ))
    };
    let labels = |n: &'static str| {
        Some(CoordinationMetrics::new(&ComponentLabels::new(
            "parting-undeleted",
            n,
            "s3",
        )))
    };
    let mut a = StoreCoordinator::with_clock(
        tap.clone(),
        support::config(Some("pu-a")),
        rt.handle().clone(),
        labels("pu-a"),
        clock.clone(),
    )
    .unwrap();
    let mut fleet = support::Fleet::new(&store, rt.handle());
    a.start(planner()).unwrap();
    fleet.join(&a);
    let mut held_a = Held::default();
    let budget = clock.now() + 4 * LEASE;
    while held_a.splits.len() < 2 {
        assert!(clock.now() < budget, "pu-a never held both splits");
        fleet.step(&clock, LEASE / 12);
        held_a.fold(a.poll().unwrap());
    }
    tap.on_write(|w| {
        (w.op == Op::Delete && w.ks == Keyspace::Ephemeral && w.key == "worker.pu-a")
            .then(|| StoreError::Retryable("injected: throttled".into()))
    });
    a.release(&[support::split_id("p0"), support::split_id("p1")])
        .unwrap();

    let mut b = StoreCoordinator::with_clock(
        store.clone(),
        support::config(Some("pu-b")),
        rt.handle().clone(),
        labels("pu-b"),
        clock.clone(),
    )
    .unwrap();
    b.start(planner()).unwrap();
    fleet.join(&b);
    let live = |c: &str| {
        metric_value(
            &handle.render(),
            "spate_coordination_live_workers",
            &[("component", c)],
        )
    };
    loop {
        assert!(
            rt.block_on(store.get(Keyspace::Ephemeral, "worker.pu-a"))
                .unwrap()
                .is_some(),
            "pu-a's key expired before both gauges read 2 (pu-a={:?}, pu-b={:?})",
            live("pu-a"),
            live("pu-b"),
        );
        if live("pu-b") == Some(2.0) && live("pu-a") == Some(2.0) {
            break;
        }
        fleet.step(&clock, LEASE / 12);
        let _ = a.poll().unwrap();
        let _ = b.poll().unwrap();
    }
}
