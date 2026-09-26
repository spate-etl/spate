//! A plan run whose seeding fails partway. Its own test binary because it
//! installs the process-wide metrics recorder.

mod support;

use spate_coordination::StoreCoordinator;
use spate_coordination::store::{CoordinationStore as _, Keyspace};
use spate_core::clock::tokio::Clock;
use spate_core::coordination::SplitCoordinator;
use spate_core::metrics::{
    ComponentLabels, CoordinationMetrics, Exporter, MetricsSettings, install,
};
use spate_test::wait_until;
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;
use support::{CountingStore, PhasedPlanner, TestClock, runtime, store_with_clock};

/// After the first failed create no new split starts, the splits already
/// in flight finish, and their wins reach `splits_planned_total`. The clock
/// is frozen, so no replan follows the failed run.
#[test]
fn a_failed_seed_stops_new_splits_and_counts_the_in_flight_wins() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        ..MetricsSettings::default()
    })
    .expect("install the exporter");
    let planned_total = || {
        handle
            .render()
            .lines()
            .find(|l| {
                l.starts_with("spate_coordination_splits_planned_total")
                    && l.contains(r#"component="seed-failure""#)
            })
            .and_then(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
    };

    let rt = runtime();
    let clock = TestClock::frozen();
    let store = CountingStore::new(store_with_clock(clock.clone()))
        .with_create_delay(Duration::from_millis(20));
    // The first split's spec create fails before any other create lands,
    // so the run has started exactly the first 64 splits.
    store.fail_create_once("spec.c000");
    let ids: Vec<String> = (0..100).map(|i| format!("c{i:03}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let labels = ComponentLabels::new("seed-failure", "seed-failure", "s3");
    let mut worker = StoreCoordinator::with_clock(
        store.clone(),
        support::config(Some("solo")),
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&labels)),
        clock as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("seed-failure:v1", &ids)))
        .unwrap();

    wait_until(
        support::DEADLINE,
        "the failed run counting its wins",
        || planned_total().is_some_and(|v| v > 0.0),
    );
    // 64 spec creates, the first of them failed, then 63 progress creates.
    assert_eq!(store.stats.creates.load(SeqCst), 127);
    let seeded = rt
        .block_on(store.inner.list(Keyspace::Durable, "split."))
        .unwrap();
    assert_eq!(seeded.len(), 63);
    assert_eq!(planned_total(), Some(63.0));
}
