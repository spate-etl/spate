//! A store handle kept after its coordinator drops leaves the coordination
//! series free for the next coordinator.

mod support;

use spate_coordination::StoreCoordinator;
use spate_coordination::store::CoordinationStore;
use spate_core::coordination::SplitCoordinator as _;
use spate_core::metrics::{ComponentLabels, CoordinationMetrics};
use spate_test::{unique_name, wait_until};
use support::{DEADLINE, PhasedPlanner, runtime};

fn frees_the_series_while_the_store_is_kept<S: CoordinationStore + Clone>(store: S, name: &str) {
    let rt = runtime();
    let labels = ComponentLabels::new("store-kept", unique_name(name), "s3");
    let mut first = StoreCoordinator::new(
        store.clone(),
        support::config(Some("solo")),
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&labels)),
    )
    .expect("coordinator");
    first
        .start(Box::new(PhasedPlanner::one_final("kept:v1", &["a"])))
        .unwrap();
    drop(first);
    wait_until(DEADLINE, "the dropped coordinator's series freed", || {
        CoordinationMetrics::try_new(&labels).is_ok()
    });
    drop(store);
}

#[test]
fn a_kept_memory_store_frees_the_series() {
    frees_the_series_while_the_store_is_kept(support::store(), "memory");
}

#[cfg(feature = "dynamodb")]
#[test]
fn a_kept_dynamodb_store_frees_the_series() {
    use spate_coordination::store::dynamodb::FakeTable;
    use spate_core::clock::tokio::SystemClock;
    use std::sync::Arc;

    let table = FakeTable::new();
    frees_the_series_while_the_store_is_kept(
        support::dynamodb::store(&table, Arc::new(SystemClock)),
        "dynamodb",
    );
}
