//! The fault scenarios: worker processes over real containers, judged by the
//! oracle. Each needs Docker and is ignored by default; run them with
//! `cargo xtask fault-test`.

#![cfg(unix)]

use std::path::Path;

use spate_faults::oracle::StoreKind;
use spate_faults::run::{self, Faults, Spec};

const WORKER: &str = env!("CARGO_BIN_EXE_spate-faults-worker");

fn faulted(name: &str, store: StoreKind, instances: u32) {
    run::run(&Spec {
        name,
        store,
        instances,
        worker: Path::new(WORKER),
        sink_delay_ms: 600,
        faults: Faults::Schedule,
    });
}

fn fault_free(name: &str, store: StoreKind) {
    run::run(&Spec {
        name,
        store,
        instances: 3,
        worker: Path::new(WORKER),
        sink_delay_ms: 20,
        faults: Faults::None,
    });
}

fn stopped_writer(name: &str, store: StoreKind, broken_fence: bool) {
    run::run(&Spec {
        name,
        store,
        instances: 2,
        worker: Path::new(WORKER),
        sink_delay_ms: 600,
        faults: Faults::StoppedWriter { broken_fence },
    });
}

/// Three NATS workers with no faults deliver every record once.
#[test]
#[ignore = "requires Docker"]
fn nats_no_faults_writes_no_duplicates() {
    fault_free("nats_no_faults_writes_no_duplicates", StoreKind::Nats);
}

/// One NATS worker, killed and replaced on the seeded schedule and handed
/// one lost reply, delivers every record under the five properties and
/// recovers the landed write. It is also stopped on some seeds: a stop is
/// skipped while the first process, which carries the lost reply, is live.
#[test]
#[ignore = "requires Docker"]
fn nats_one_instance() {
    faulted("nats_one_instance", StoreKind::Nats, 1);
}

/// Three NATS workers under seeded kills, stops, one lost reply and aborts
/// before or after a write deliver every record under the five properties,
/// while two of them land writes as split owners at overlapping times.
#[test]
#[ignore = "requires Docker"]
fn nats_three_instances() {
    faulted("nats_three_instances", StoreKind::Nats, 3);
}

/// Three DynamoDB workers with no faults deliver every record once.
#[test]
#[ignore = "requires Docker"]
fn dynamodb_no_faults_writes_no_duplicates() {
    fault_free(
        "dynamodb_no_faults_writes_no_duplicates",
        StoreKind::DynamoDb,
    );
}

/// One DynamoDB worker, killed and replaced on the seeded schedule and handed
/// one lost reply, delivers every record under the five properties and
/// recovers the landed write. It is also stopped on some seeds: a stop is
/// skipped while the first process, which carries the lost reply, is live.
#[test]
#[ignore = "requires Docker"]
fn dynamodb_one_instance() {
    faulted("dynamodb_one_instance", StoreKind::DynamoDb, 1);
}

/// Three DynamoDB workers under seeded kills, stops, one lost reply and
/// aborts before or after a write deliver every record under the five
/// properties, while two of them land writes as split owners at overlapping
/// times.
#[test]
#[ignore = "requires Docker"]
fn dynamodb_three_instances() {
    faulted("dynamodb_three_instances", StoreKind::DynamoDb, 3);
}

/// A NATS worker stopped inside a commit for longer than a lease, while its
/// peer claims the split, writes nothing stale on resume.
#[test]
#[ignore = "requires Docker"]
fn nats_stopped_writer_with_fence_passes() {
    stopped_writer(
        "nats_stopped_writer_with_fence_passes",
        StoreKind::Nats,
        false,
    );
}

/// A NATS worker that re-sends its stopped commit after losing the CAS lands
/// a stale epoch, and the oracle reports it against that worker.
#[test]
#[ignore = "requires Docker"]
fn nats_broken_fence_fails_the_run() {
    stopped_writer("nats_broken_fence_fails_the_run", StoreKind::Nats, true);
}

/// A DynamoDB worker stopped inside a commit for longer than a lease, while
/// its peer claims the split, writes nothing stale on resume.
#[test]
#[ignore = "requires Docker"]
fn dynamodb_stopped_writer_with_fence_passes() {
    stopped_writer(
        "dynamodb_stopped_writer_with_fence_passes",
        StoreKind::DynamoDb,
        false,
    );
}

/// A DynamoDB worker that re-sends its stopped commit after losing the CAS
/// lands a stale epoch, and the oracle reports it against that worker.
#[test]
#[ignore = "requires Docker"]
fn dynamodb_broken_fence_fails_the_run() {
    stopped_writer(
        "dynamodb_broken_fence_fails_the_run",
        StoreKind::DynamoDb,
        true,
    );
}
