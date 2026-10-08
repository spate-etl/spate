//! The fault scenarios: worker processes over real containers, judged by the
//! oracle. Each needs Docker and is ignored by default; run them with
//! `cargo xtask fault-test`.

#![cfg(unix)]

use std::path::Path;

use spate_faults::run::{self, Spec};

const WORKER: &str = env!("CARGO_BIN_EXE_spate-faults-worker");

/// Three NATS workers with no faults deliver every record once.
#[test]
#[ignore = "requires Docker"]
fn nats_no_faults_writes_no_duplicates() {
    run::run(&Spec {
        name: "nats_no_faults_writes_no_duplicates",
        instances: 3,
        worker: Path::new(WORKER),
        sink_delay_ms: 20,
        fault_free: true,
    });
}

/// One NATS worker, killed and replaced on the seeded schedule, delivers every
/// record under the five properties.
#[test]
#[ignore = "requires Docker"]
fn nats_one_instance() {
    run::run(&Spec {
        name: "nats_one_instance",
        instances: 1,
        worker: Path::new(WORKER),
        sink_delay_ms: 600,
        fault_free: false,
    });
}

/// Three NATS workers, killed and replaced on the seeded schedule, deliver
/// every record under the five properties, while two of them land writes as
/// split owners at overlapping times.
#[test]
#[ignore = "requires Docker"]
fn nats_three_instances() {
    run::run(&Spec {
        name: "nats_three_instances",
        instances: 3,
        worker: Path::new(WORKER),
        sink_delay_ms: 600,
        fault_free: false,
    });
}
