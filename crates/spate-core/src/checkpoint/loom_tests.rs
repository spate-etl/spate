//! Loom model checks for the acknowledgement primitives.
//!
//! Run with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p spate-core --release checkpoint::loom_tests
//! ```

use super::ack::AckTx;
use super::{AckMsg, AckRef, AckStatus, BatchId};
use crate::record::PartitionId;
use loom::sync::{Arc, Mutex};

fn recorded_ack() -> (AckRef, Arc<Mutex<Vec<AckMsg>>>) {
    let recorder = Arc::new(Mutex::new(Vec::new()));
    let ack = AckRef::new(
        BatchId {
            partition: PartitionId(3),
            epoch: 1,
            seq: 9,
        },
        99,
        AckTx::Recorder(Arc::clone(&recorder)),
    );
    (ack, recorder)
}

/// Across every interleaving of clone/fail/drop on three threads, the batch
/// resolves exactly once, and as `Failed` iff some thread failed it.
#[test]
fn resolves_exactly_once_with_worst_status() {
    loom::model(|| {
        let (ack, recorder) = recorded_ack();
        let failing = ack.clone();
        let passing = ack.clone();

        let t1 = loom::thread::spawn(move || {
            failing.fail();
            drop(failing);
        });
        let t2 = loom::thread::spawn(move || {
            drop(passing);
        });
        drop(ack);
        t1.join().unwrap();
        t2.join().unwrap();

        let msgs = recorder.lock().unwrap();
        assert_eq!(msgs.len(), 1, "batch must resolve exactly once");
        assert_eq!(msgs[0].status, AckStatus::Failed, "worst status must win");
        assert_eq!(msgs[0].id.seq, 9);
        assert_eq!(msgs[0].last_offset, 99);
    });
}

/// With no failure on any thread, every interleaving resolves `Delivered`.
#[test]
fn resolves_delivered_without_failure() {
    loom::model(|| {
        let (ack, recorder) = recorded_ack();
        let a = ack.clone();
        let b = ack.clone();

        let t1 = loom::thread::spawn(move || drop(a));
        let t2 = loom::thread::spawn(move || drop(b));
        drop(ack);
        t1.join().unwrap();
        t2.join().unwrap();

        let msgs = recorder.lock().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].status, AckStatus::Delivered);
    });
}

/// Cloning from a handle concurrently with dropping other handles never
/// double-sends or loses the resolution.
#[test]
fn clone_drop_races_resolve_once() {
    loom::model(|| {
        let (ack, recorder) = recorded_ack();
        let seed = ack.clone();

        let t1 = loom::thread::spawn(move || {
            let extra = seed.clone();
            drop(seed);
            drop(extra);
        });
        drop(ack);
        t1.join().unwrap();

        let msgs = recorder.lock().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].status, AckStatus::Delivered);
    });
}
