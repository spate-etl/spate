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
