//! The fleet size a worker reports after it has left the fleet.
//!
//! Its own binary: the exporter installs a process-global recorder and the
//! log capture a process-global subscriber.

mod support;

use spate_coordination::SplitCoordinator as _;
use spate_coordination::StoreCoordinator;
use spate_coordination::store::{CoordinationStore as _, Keyspace};
use spate_core::clock::tokio::Clock as _;
use spate_core::metrics::{
    ComponentLabels, CoordinationMetrics, Exporter, MetricsSettings, install,
};
use spate_test::{LogCapture, metric_value};
use support::{Held, LEASE, PhasedPlanner, runtime};

/// A worker that released its last split reports the same fleet size as the
/// leader that took its work, on its gauge and on its `peer joined` line.
/// Regression for #860.
#[test]
fn a_parting_worker_does_not_count_itself_live() {
    let capture = LogCapture::new();
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::INFO)
        .without_time()
        .init();
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");

    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let ids = ["p0", "p1"];
    let planner = || Box::new(PhasedPlanner::one_final("parting-gauge:v1", &ids));
    let worker = |name: &'static str| {
        StoreCoordinator::with_clock(
            store.clone(),
            support::config(Some(name)),
            rt.handle().clone(),
            Some(CoordinationMetrics::new(&ComponentLabels::new(
                "parting-gauge",
                name,
                "s3",
            ))),
            clock.clone(),
        )
        .expect("coordinator")
    };
    let mut fleet = support::Fleet::new(&store, rt.handle());

    let mut a = worker("part-a");
    a.start(planner()).unwrap();
    fleet.join(&a);
    let mut held_a = Held::default();
    let budget = clock.now() + 4 * LEASE;
    while held_a.splits.len() < 2 {
        assert!(clock.now() < budget, "part-a never held both splits");
        fleet.step(&clock, LEASE / 12);
        held_a.fold(a.poll().expect("poll a"));
    }
    a.release(&[support::split_id("p0"), support::split_id("p1")])
        .unwrap();
    let presence = rt
        .block_on(store.get(Keyspace::Ephemeral, "worker.part-a"))
        .expect("get");
    assert!(presence.is_none(), "part-a kept its presence key");

    let mut b = worker("part-b");
    b.start(planner()).unwrap();
    fleet.join(&b);
    let mut held_b = Held::default();
    let budget = clock.now() + 4 * LEASE;
    while held_b.splits.len() < 2 {
        assert!(clock.now() < budget, "part-b never held both splits");
        fleet.step(&clock, LEASE / 12);
        let _ = a.poll().expect("poll a");
        held_b.fold(b.poll().expect("poll b"));
    }

    let text = handle.render();
    let live = |component| {
        metric_value(
            &text,
            "spate_coordination_live_workers",
            &[("component", component)],
        )
    };
    assert_eq!(live("part-b"), Some(1.0), "the leader counts only itself");
    assert_eq!(
        live("part-a"),
        live("part-b"),
        "the parting worker's live_workers disagrees with the leader's"
    );
    let lines = capture.lines();
    assert!(
        lines.iter().any(|l| l.contains("peer joined")
            && l.contains("instance=part-b")
            && l.contains("live=1")),
        "part-a did not report part-b joining a fleet of one\n--- captured ---\n{}",
        lines.join("\n")
    );
}
