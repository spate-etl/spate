//! Batch acknowledgement handles (Vector-finalizer style).

use crate::checkpoint::sync::{Arc, AtomicU8, Ordering};
use crate::record::PartitionId;
use std::fmt;

/// Identity of one source poll batch within a partition and assignment
/// epoch. Epochs are bumped on every rebalance so acknowledgements from
/// revoked assignments are recognized as stale and discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BatchId {
    /// Partition the batch was polled from.
    pub partition: PartitionId,
    /// Assignment epoch the batch belongs to.
    pub epoch: u32,
    /// Per-partition, per-epoch monotonically increasing sequence number.
    pub seq: u64,
}

/// Resolution status of a batch. Forms a "worst-of" lattice: once any
/// record of the batch fails, the batch is failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum AckStatus {
    /// Every record was durably delivered, filtered, or intentionally
    /// skipped by policy.
    Delivered = 0,
    /// At least one record could not be delivered; the partition watermark
    /// must not advance past this batch.
    Failed = 1,
}

/// Message sent to the checkpointer when the last [`AckRef`] clone of a
/// batch drops.
#[derive(Clone, Copy, Debug)]
pub struct AckMsg {
    /// Which batch resolved.
    pub id: BatchId,
    /// Highest source offset contained in the batch (inclusive). The
    /// committable position after this batch is `last_offset + 1`.
    pub last_offset: i64,
    /// Worst status observed across the batch's records.
    pub status: AckStatus,
}

/// Where a batch's resolution message is delivered: normally the
/// checkpointer's unbounded channel; under `--cfg loom` an additional
/// recorder variant lets the drop/fail races be model-checked without an
/// unmodelled channel implementation.
#[derive(Clone)]
pub(crate) enum AckTx {
    /// The checkpointer's unbounded channel.
    Channel(crossbeam_channel::Sender<AckMsg>),
    /// Loom-test recorder.
    #[cfg(loom)]
    Recorder(Arc<loom::sync::Mutex<Vec<AckMsg>>>),
}

impl AckTx {
    fn send(&self, msg: AckMsg) {
        match self {
            // A send error means the checkpointer is gone (shutdown
            // teardown); there is nothing left to acknowledge to.
            AckTx::Channel(tx) => {
                let _ = tx.send(msg);
            }
            #[cfg(loom)]
            AckTx::Recorder(rec) => rec.lock().unwrap().push(msg),
        }
    }
}

impl fmt::Debug for AckTx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AckTx")
    }
}

/// Shared acknowledgement state of one source poll batch.
///
/// Dropping the final handle sends an [`AckMsg`] on the checkpointer's
/// unbounded channel. `Arc`'s release/acquire on the refcount orders all
/// `fail()` calls before the drop-time status read, so no additional
/// synchronization is needed.
#[derive(Debug)]
struct AckState {
    id: BatchId,
    last_offset: i64,
    status: AtomicU8,
    tx: AckTx,
}

impl Drop for AckState {
    fn drop(&mut self) {
        let status = if self.status.load(Ordering::Relaxed) == AckStatus::Delivered as u8 {
            AckStatus::Delivered
        } else {
            AckStatus::Failed
        };
        self.tx.send(AckMsg {
            id: self.id,
            last_offset: self.last_offset,
            status,
        });
    }
}

/// Refcounted handle to a batch's acknowledgement state. Clone = one atomic
/// increment, no allocation; records hold one each.
#[derive(Clone, Debug)]
pub struct AckRef(Arc<AckState>);

impl AckRef {
    /// Create the handle for a new batch. Called by the source driver once
    /// per poll batch; everything downstream only clones.
    pub(crate) fn new(id: BatchId, last_offset: i64, tx: AckTx) -> Self {
        AckRef(Arc::new(AckState {
            id,
            last_offset,
            status: AtomicU8::new(AckStatus::Delivered as u8),
            tx,
        }))
    }

    /// Mark the batch failed. Idempotent; any single failure poisons the
    /// batch (worst-of lattice), stalling the partition watermark.
    pub fn fail(&self) {
        self.0
            .status
            .store(AckStatus::Failed as u8, Ordering::Relaxed);
    }

    /// Identity of the batch this handle belongs to.
    #[must_use]
    pub fn batch_id(&self) -> BatchId {
        self.0.id
    }

    /// Build a handle plus the receiver its resolution message arrives on.
    /// Test-support constructor used across the workspace's test suites.
    #[doc(hidden)]
    #[must_use]
    pub fn test_pair() -> (Self, crossbeam_channel::Receiver<AckMsg>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        (
            Self::new(
                BatchId {
                    partition: PartitionId(0),
                    epoch: 0,
                    seq: 0,
                },
                0,
                AckTx::Channel(tx),
            ),
            rx,
        )
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn make(tx: crossbeam_channel::Sender<AckMsg>, seq: u64, last_offset: i64) -> AckRef {
        AckRef::new(
            BatchId {
                partition: PartitionId(1),
                epoch: 7,
                seq,
            },
            last_offset,
            AckTx::Channel(tx),
        )
    }

    #[test]
    fn resolves_once_on_last_drop() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let ack = make(tx, 3, 99);
        let clones: Vec<_> = (0..8).map(|_| ack.clone()).collect();
        drop(ack);
        assert!(rx.try_recv().is_err(), "must not resolve while clones live");
        drop(clones);
        let msg = rx.try_recv().expect("resolved on last drop");
        assert_eq!(msg.id.seq, 3);
        assert_eq!(msg.last_offset, 99);
        assert_eq!(msg.status, AckStatus::Delivered);
        assert!(rx.try_recv().is_err(), "resolves exactly once");
    }

    #[test]
    fn any_failure_poisons_the_batch() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let ack = make(tx, 0, 10);
        let sibling = ack.clone();
        sibling.fail();
        drop(sibling);
        drop(ack);
        assert_eq!(rx.try_recv().unwrap().status, AckStatus::Failed);
    }

    #[test]
    fn dropped_checkpointer_does_not_panic() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let ack = make(tx, 0, 0);
        drop(rx);
        drop(ack); // send fails silently — shutdown teardown ordering
    }
}
