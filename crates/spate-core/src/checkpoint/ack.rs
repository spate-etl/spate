//! Batch acknowledgment handles (Vector-finalizer style).

use crate::checkpoint::sync::{Arc, AtomicU8, Ordering};
use crate::record::PartitionId;
use std::fmt;

/// Identity of one source poll batch within a partition and assignment
/// epoch. Epochs are bumped on every rebalance so acknowledgments from
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

/// Shared acknowledgment state of one source poll batch.
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

/// Refcounted handle to a batch's acknowledgment state. Clone = one atomic
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
        drop(ack); // send fails silently during shutdown teardown
    }
}

/// A fail-safe collection of acknowledgment handles for data that has not
/// been durably written yet (encoded chunks, sink batches).
///
/// A bare [`AckRef`] resolves *Delivered* on drop, the default for records,
/// where drop means "consumed" (filtered out, skipped by policy, or written).
/// On the sink path, drop without an explicit outcome means the data was torn
/// down (task aborted, queue dropped, worker cancelled), and resolving
/// Delivered would commit offsets for rows that never reached the sink.
/// `AckSet` therefore **fails** every handle it still holds when dropped; the
/// happy path calls [`AckSet::deliver`] after the durable write. Over-failing
/// is safe under at-least-once, costing replay rather than loss.
#[derive(Debug, Default)]
pub struct AckSet(Vec<AckRef>);

impl AckSet {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        AckSet(Vec::new())
    }

    /// Add a handle to the set.
    pub fn push(&mut self, ack: AckRef) {
        self.0.push(ack);
    }

    /// Move every handle out of `other` into `self`.
    pub fn absorb(&mut self, mut other: AckSet) {
        self.0.append(&mut other.0);
    }

    /// Whether the set holds no handles.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of handles held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Resolve the set as delivered, meaning the data it covers is durably
    /// written. Consumes the set; the handles drop with their default
    /// (Delivered) resolution.
    pub fn deliver(mut self) {
        self.0.clear();
    }
}

impl Drop for AckSet {
    fn drop(&mut self) {
        for ack in &self.0 {
            ack.fail();
        }
    }
}

impl From<Vec<AckRef>> for AckSet {
    fn from(acks: Vec<AckRef>) -> Self {
        AckSet(acks)
    }
}

#[cfg(all(test, not(loom)))]
mod ack_set_tests {
    use super::*;

    #[test]
    fn dropping_a_set_fails_its_batches() {
        let (ack, rx) = AckRef::test_pair();
        let mut set = AckSet::new();
        set.push(ack.clone());
        drop(ack);
        drop(set);
        assert_eq!(rx.try_recv().unwrap().status, AckStatus::Failed);
    }

    #[test]
    fn delivering_a_set_resolves_delivered() {
        let (ack, rx) = AckRef::test_pair();
        let mut set = AckSet::new();
        set.push(ack.clone());
        drop(ack);
        set.deliver();
        assert_eq!(rx.try_recv().unwrap().status, AckStatus::Delivered);
    }

    #[test]
    fn absorb_moves_handles_without_resolving() {
        let (ack, rx) = AckRef::test_pair();
        let mut a = AckSet::new();
        a.push(ack.clone());
        drop(ack);
        let mut b = AckSet::new();
        b.absorb(a);
        assert!(rx.try_recv().is_err(), "absorb must not resolve");
        assert_eq!(b.len(), 1);
        b.deliver();
        assert_eq!(rx.try_recv().unwrap().status, AckStatus::Delivered);
    }
}
