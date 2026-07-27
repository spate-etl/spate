//! The checkpointer: turns asynchronous batch resolutions into per-partition
//! committable watermarks.
//!
//! Ownership model: the pipeline runtime owns the [`Checkpointer`]
//! (`&mut self`, single-threaded); each pipeline thread owns an
//! [`AckIssuer`] and creates one [`AckRef`] per source poll batch. Both
//! directions are wait-free for producers: issuing sends a registration on
//! an unbounded channel, and batch resolution happens in `AckRef`'s drop
//! path. Acks can therefore never block behind data — the invariant that
//! makes the backpressure design deadlock-free.

use super::ack::AckTx;
use super::tracker::{PartitionTracker, ResolveOutcome};
use super::{AckMsg, AckRef, BatchId};
use crate::error::FatalError;
use crate::record::PartitionId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

/// Registration of a newly issued batch, sent issuer → checkpointer.
#[derive(Clone, Copy, Debug)]
struct Registration {
    id: BatchId,
    last_offset: i64,
}

/// Counters from one [`Checkpointer::drain`] call, for metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DrainStats {
    /// Resolutions applied to a tracker.
    pub applied: usize,
    /// Registrations or resolutions discarded because their epoch is not
    /// current or their partition is not assigned (normal after rebalance).
    pub stale_epoch: usize,
    /// Duplicate resolutions (already resolved or already advanced).
    pub duplicates: usize,
    /// Resolutions that never found a registration — driver bug.
    pub unknown: usize,
}

/// Creates acknowledgement handles on pipeline threads.
///
/// One issuer per pipeline thread. Within an epoch, a partition must be
/// issued from exactly one issuer (the runtime guarantees this: a partition
/// is owned by exactly one thread) — sequence numbering is issuer-local.
/// Cloning yields an issuer with fresh sequence state for use by another
/// thread and another set of partitions.
#[derive(Debug)]
pub struct AckIssuer {
    ack_tx: crossbeam_channel::Sender<AckMsg>,
    reg_tx: crossbeam_channel::Sender<Registration>,
    shared_epoch: Arc<AtomicU32>,
    local_epoch: u32,
    seqs: HashMap<PartitionId, u64>,
}

impl Clone for AckIssuer {
    fn clone(&self) -> Self {
        AckIssuer {
            ack_tx: self.ack_tx.clone(),
            reg_tx: self.reg_tx.clone(),
            shared_epoch: Arc::clone(&self.shared_epoch),
            local_epoch: self.local_epoch,
            seqs: HashMap::new(),
        }
    }
}

impl AckIssuer {
    /// Issue the acknowledgement handle for a new source poll batch whose
    /// highest contained offset is `last_offset`.
    ///
    /// Wait-free: one atomic load, one unbounded send, one allocation for
    /// the batch's shared state.
    pub fn issue(&mut self, partition: PartitionId, last_offset: i64) -> AckRef {
        let epoch = self.shared_epoch.load(Ordering::Acquire);
        if epoch != self.local_epoch {
            // New assignment epoch: sequences restart at zero.
            self.local_epoch = epoch;
            self.seqs.clear();
        }
        let seq_slot = self.seqs.entry(partition).or_insert(0);
        let seq = *seq_slot;
        *seq_slot += 1;

        let id = BatchId {
            partition,
            epoch,
            seq,
        };
        // Registration is sent before any AckRef exists, so a resolution
        // observed by the checkpointer always has its registration already
        // in the registration channel (drain exploits this causality).
        let _ = self.reg_tx.send(Registration { id, last_offset });
        AckRef::new(id, last_offset, AckTx::Channel(self.ack_tx.clone()))
    }
}

/// Aggregates batch resolutions into per-partition committable watermarks.
///
/// ```
/// use spate_core::checkpoint::{AckStatus, Checkpointer};
/// use spate_core::record::PartitionId;
///
/// let mut cp = Checkpointer::new();
/// let p = PartitionId(0);
/// cp.begin_epoch(&[p], 1);
/// let mut issuer = cp.handle();
///
/// let ack = issuer.issue(p, 99); // batch covering offsets ..=99
/// drop(ack); // all records delivered
///
/// cp.drain();
/// assert_eq!(cp.take_watermarks(), vec![(p, 100)]);
/// assert_eq!(cp.take_watermarks(), vec![]); // idempotent until new acks
/// ```
#[derive(Debug)]
pub struct Checkpointer {
    ack_tx: crossbeam_channel::Sender<AckMsg>,
    ack_rx: crossbeam_channel::Receiver<AckMsg>,
    reg_tx: crossbeam_channel::Sender<Registration>,
    reg_rx: crossbeam_channel::Receiver<Registration>,
    shared_epoch: Arc<AtomicU32>,
    epoch: u32,
    trackers: HashMap<PartitionId, PartitionTracker>,
    /// Every partition admitted to the current epoch, including ones since
    /// revoked. `trackers` alone cannot enforce the additive contract: a
    /// revocation removes the tracker, so a re-add would look fresh.
    admitted: HashSet<PartitionId>,
}

impl Default for Checkpointer {
    fn default() -> Self {
        Self::new()
    }
}

impl Checkpointer {
    /// A checkpointer with no assignment. Call [`begin_epoch`] when the
    /// source reports its first assignment.
    ///
    /// [`begin_epoch`]: Checkpointer::begin_epoch
    #[must_use]
    pub fn new() -> Self {
        let (ack_tx, ack_rx) = crossbeam_channel::unbounded();
        let (reg_tx, reg_rx) = crossbeam_channel::unbounded();
        Checkpointer {
            ack_tx,
            ack_rx,
            reg_tx,
            reg_rx,
            shared_epoch: Arc::new(AtomicU32::new(0)),
            epoch: 0,
            trackers: HashMap::new(),
            admitted: HashSet::new(),
        }
    }

    /// An issuer handle for a pipeline thread.
    #[must_use]
    pub fn handle(&self) -> AckIssuer {
        AckIssuer {
            ack_tx: self.ack_tx.clone(),
            reg_tx: self.reg_tx.clone(),
            shared_epoch: Arc::clone(&self.shared_epoch),
            local_epoch: self.shared_epoch.load(Ordering::Acquire),
            seqs: HashMap::new(),
        }
    }

    /// Start a new assignment epoch covering exactly `partitions`. Every
    /// rebalance bumps the epoch; in-flight batches from earlier epochs
    /// resolve as stale and their offsets are re-delivered by the source
    /// (at-least-once). Epochs must be strictly increasing.
    ///
    /// Ordering contract: the runtime calls this *before* distributing the
    /// new assignment's lanes to pipeline threads, so issuers observe the
    /// new epoch before issuing for it.
    pub fn begin_epoch(&mut self, partitions: &[PartitionId], epoch: u32) {
        assert!(
            epoch > self.epoch || (self.epoch == 0 && self.trackers.is_empty()),
            "assignment epochs must be strictly increasing: {} -> {epoch}",
            self.epoch
        );
        self.epoch = epoch;
        self.trackers = partitions
            .iter()
            .map(|&p| (p, PartitionTracker::new()))
            .collect();
        // A new epoch clears the admission ledger: every issuer restarts its
        // sequences on the epoch change, so a partition may legitimately
        // reappear here.
        self.admitted = partitions.iter().copied().collect();
        // Publish after trackers exist: an issuer that observes the new
        // epoch will have its registrations accepted.
        self.shared_epoch.store(epoch, Ordering::Release);
    }

    /// Add partitions to the *current* epoch without disturbing existing
    /// trackers (additive lane gains — [`SourceEvent::LanesAdded`]). The
    /// epoch does not change, so in-flight batches for existing partitions
    /// keep resolving; only genuinely new partitions may be added — a
    /// partition revoked mid-epoch can only return in a new epoch, and
    /// re-adding a live partition would discard its ack state.
    ///
    /// Ordering contract: as with [`Checkpointer::begin_epoch`], call this
    /// *before* distributing the new lanes to pipeline threads.
    ///
    /// # Errors
    ///
    /// Returns a [`FatalError`] if a partition was already admitted to this
    /// epoch, whether it is still live or has since been revoked. Both are
    /// source bugs, and the revoked case is the dangerous one: its tracker
    /// is gone, so it *looks* fresh, while issuers keep their sequence
    /// counters until the epoch changes. Admitting it would pair a
    /// mid-sequence registration with a tracker expecting zero, and
    /// [`PartitionTracker::register`] would panic on the controller thread —
    /// taking down the pipeline with a message naming neither this method
    /// nor the contract that was broken.
    ///
    /// [`SourceEvent::LanesAdded`]: crate::source::SourceEvent::LanesAdded
    /// [`PartitionTracker::register`]: crate::checkpoint::PartitionTracker::register
    pub fn extend_epoch(&mut self, partitions: &[PartitionId]) -> Result<(), FatalError> {
        // Check before mutating: a rejected extension must leave the epoch
        // exactly as it was.
        for &p in partitions {
            if self.admitted.contains(&p) {
                let live = if self.trackers.contains_key(&p) {
                    "is already tracked"
                } else {
                    "was revoked earlier in this epoch"
                };
                return Err(FatalError {
                    component: "checkpoint".into(),
                    reason: format!(
                        "additive assignment reused partition {} which {live}; every \
                         added lane must carry a partition never seen in this epoch \
                         (a returning partition needs a new epoch)",
                        p.0
                    ),
                });
            }
        }
        for &p in partitions {
            self.trackers.insert(p, PartitionTracker::new());
            self.admitted.insert(p);
        }
        Ok(())
    }

    /// Drop tracking for revoked partitions mid-epoch (partial revocation
    /// or shutdown). Later resolutions for them are discarded as stale.
    /// A partition revoked this way can only return in a *new* epoch.
    pub fn revoke(&mut self, partitions: &[PartitionId]) {
        for p in partitions {
            self.trackers.remove(p);
        }
    }

    /// Apply all pending registrations and resolutions.
    ///
    /// Two passes exploit the causal order guaranteed by [`AckIssuer`]
    /// (registration is sent before the batch's `AckRef` exists): a
    /// resolution whose registration has not been drained yet is retried
    /// once after re-draining registrations; if it is still unknown, the
    /// driver is buggy and the resolution is counted and dropped.
    pub fn drain(&mut self) -> DrainStats {
        let mut stats = DrainStats::default();
        self.drain_registrations(&mut stats);

        let mut deferred = Vec::new();
        while let Ok(msg) = self.ack_rx.try_recv() {
            self.apply(msg, &mut stats, Some(&mut deferred));
        }

        if !deferred.is_empty() {
            self.drain_registrations(&mut stats);
            for msg in deferred {
                self.apply(msg, &mut stats, None);
            }
        }
        stats
    }

    fn drain_registrations(&mut self, stats: &mut DrainStats) {
        while let Ok(reg) = self.reg_rx.try_recv() {
            if reg.id.epoch != self.epoch {
                stats.stale_epoch += 1;
                continue;
            }
            match self.trackers.get_mut(&reg.id.partition) {
                Some(tracker) => tracker.register(reg.id.seq, reg.last_offset),
                // Revoked mid-epoch while the issuer still held the lane.
                None => stats.stale_epoch += 1,
            }
        }
    }

    fn apply(&mut self, msg: AckMsg, stats: &mut DrainStats, defer: Option<&mut Vec<AckMsg>>) {
        if msg.id.epoch != self.epoch {
            stats.stale_epoch += 1;
            return;
        }
        let Some(tracker) = self.trackers.get_mut(&msg.id.partition) else {
            stats.stale_epoch += 1;
            return;
        };
        match tracker.resolve(msg.id.seq, msg.status) {
            ResolveOutcome::Applied => stats.applied += 1,
            ResolveOutcome::Duplicate | ResolveOutcome::AlreadyAdvanced => stats.duplicates += 1,
            ResolveOutcome::Unregistered => match defer {
                Some(deferred) => deferred.push(msg),
                None => {
                    debug_assert!(false, "resolution without registration: {:?}", msg.id);
                    stats.unknown += 1;
                }
            },
        }
    }

    /// Watermarks that advanced since the last call: `(partition,
    /// committable offset)` pairs ready for `Source::commit`. Empty when
    /// nothing moved — callers skip the commit entirely.
    #[must_use]
    pub fn take_watermarks(&mut self) -> Vec<(PartitionId, i64)> {
        let mut out: Vec<_> = self
            .trackers
            .iter_mut()
            .filter_map(|(&p, t)| t.advance().map(|w| (p, w)))
            .collect();
        out.sort_unstable_by_key(|&(p, _)| p);
        out
    }

    /// Unadvanced batches for one partition (backpressure trigger).
    #[must_use]
    pub fn pending(&self, partition: PartitionId) -> usize {
        self.trackers
            .get(&partition)
            .map_or(0, PartitionTracker::pending)
    }

    /// The largest per-partition pending count.
    #[must_use]
    pub fn max_pending(&self) -> usize {
        self.trackers
            .values()
            .map(PartitionTracker::pending)
            .max()
            .unwrap_or(0)
    }

    /// Partitions whose watermark is permanently stalled behind a failed
    /// batch, with the stall start (health-probe input).
    #[must_use]
    pub fn stalled_partitions(&self) -> Vec<(PartitionId, Instant)> {
        let mut out: Vec<_> = self
            .trackers
            .iter()
            .filter_map(|(&p, t)| t.stalled_since().map(|since| (p, since)))
            .collect();
        out.sort_unstable_by_key(|&(p, _)| p);
        out
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    const P0: PartitionId = PartitionId(0);
    const P1: PartitionId = PartitionId(1);

    fn checkpointer(partitions: &[PartitionId]) -> (Checkpointer, AckIssuer) {
        let mut cp = Checkpointer::new();
        cp.begin_epoch(partitions, 1);
        let issuer = cp.handle();
        (cp, issuer)
    }

    #[test]
    fn issue_drain_take_happy_path() {
        let (mut cp, mut issuer) = checkpointer(&[P0]);
        drop(issuer.issue(P0, 99));
        drop(issuer.issue(P0, 199));
        let stats = cp.drain();
        assert_eq!(stats.applied, 2);
        assert_eq!(
            stats,
            DrainStats {
                applied: 2,
                ..Default::default()
            }
        );
        assert_eq!(cp.take_watermarks(), vec![(P0, 200)]);
    }

    #[test]
    fn extend_epoch_adds_partitions_without_disturbing_inflight_acks() {
        let (mut cp, mut issuer) = checkpointer(&[P0]);
        // In flight on P0 before the extension...
        let ack = issuer.issue(P0, 99);
        cp.extend_epoch(&[P1]).unwrap();
        // ...still resolves after it: the epoch did not change.
        drop(ack);
        drop(issuer.issue(P1, 9));
        let stats = cp.drain();
        assert_eq!(stats.applied, 2);
        assert_eq!(stats.stale_epoch, 0);
        assert_eq!(cp.take_watermarks(), vec![(P0, 100), (P1, 10)]);
    }

    #[test]
    fn extend_epoch_rejects_a_live_partition() {
        let (mut cp, _issuer) = checkpointer(&[P0]);
        let err = cp.extend_epoch(&[P0]).unwrap_err();
        assert!(err.reason.contains("already tracked"), "{err}");
    }

    #[test]
    fn extend_epoch_rejects_a_partition_revoked_earlier_in_the_epoch() {
        // The dangerous half of the contract. `revoke` drops the tracker, so
        // a re-add looks fresh — but issuers keep their sequence counters
        // until the epoch changes, so the next batch registers mid-sequence
        // against a tracker expecting zero. That used to panic inside
        // `PartitionTracker::register`, on the controller thread, naming
        // neither this method nor the contract it broke.
        let (mut cp, mut issuer) = checkpointer(&[P0]);
        drop(issuer.issue(P0, 9));
        cp.drain();
        cp.revoke(&[P0]);

        let err = cp.extend_epoch(&[P0]).unwrap_err();
        assert_eq!(err.component, "checkpoint");
        assert!(
            err.reason.contains("revoked earlier in this epoch"),
            "{err}"
        );

        // Rejected means unchanged: the partition is still revoked, so the
        // issuer's next batch is discarded as stale rather than registered —
        // its registration and its resolution both.
        drop(issuer.issue(P0, 19));
        let stats = cp.drain();
        assert_eq!(stats.applied, 0);
        assert_eq!(stats.stale_epoch, 2);

        // A new epoch is how it legitimately returns.
        cp.begin_epoch(&[P0], 2);
        cp.extend_epoch(&[P1]).unwrap();
    }

    #[test]
    fn take_watermarks_is_empty_until_new_progress() {
        let (mut cp, mut issuer) = checkpointer(&[P0]);
        drop(issuer.issue(P0, 9));
        cp.drain();
        assert_eq!(cp.take_watermarks(), vec![(P0, 10)]);
        assert_eq!(cp.take_watermarks(), vec![]);
        drop(issuer.issue(P0, 19));
        cp.drain();
        assert_eq!(cp.take_watermarks(), vec![(P0, 20)]);
    }

    #[test]
    fn out_of_order_acks_across_partitions() {
        let (mut cp, mut issuer) = checkpointer(&[P0, P1]);
        let a0 = issuer.issue(P0, 9);
        let a1 = issuer.issue(P0, 19);
        let b0 = issuer.issue(P1, 99);
        // P0's second batch and P1's batch resolve before P0's first.
        drop(a1);
        drop(b0);
        cp.drain();
        assert_eq!(cp.take_watermarks(), vec![(P1, 100)]);
        assert_eq!(cp.pending(P0), 2);
        drop(a0);
        cp.drain();
        assert_eq!(cp.take_watermarks(), vec![(P0, 20)]);
    }

    #[test]
    fn failed_batch_stalls_partition_and_reports() {
        let (mut cp, mut issuer) = checkpointer(&[P0]);
        let bad = issuer.issue(P0, 9);
        bad.fail();
        drop(bad);
        drop(issuer.issue(P0, 19));
        cp.drain();
        assert_eq!(cp.take_watermarks(), vec![]);
        let stalled = cp.stalled_partitions();
        assert_eq!(stalled.len(), 1);
        assert_eq!(stalled[0].0, P0);
    }

    #[test]
    fn stale_epoch_acks_are_discarded() {
        let (mut cp, mut issuer) = checkpointer(&[P0]);
        let old = issuer.issue(P0, 9);
        cp.begin_epoch(&[P0], 2);
        drop(old); // resolves with epoch 1
        let stats = cp.drain();
        assert_eq!(stats.applied, 0);
        // Both the registration and the resolution are stale.
        assert_eq!(stats.stale_epoch, 2);
        assert_eq!(cp.take_watermarks(), vec![]);

        // The issuer picks up the new epoch and sequences restart.
        drop(issuer.issue(P0, 49));
        let stats = cp.drain();
        assert_eq!(stats.applied, 1);
        assert_eq!(cp.take_watermarks(), vec![(P0, 50)]);
    }

    #[test]
    fn revoke_mid_flight_discards_later_acks() {
        let (mut cp, mut issuer) = checkpointer(&[P0, P1]);
        let in_flight = issuer.issue(P1, 9);
        cp.drain(); // registration lands first
        cp.revoke(&[P1]);
        drop(in_flight);
        let stats = cp.drain();
        assert_eq!(stats.stale_epoch, 1);
        assert_eq!(cp.take_watermarks(), vec![]);
        assert_eq!(cp.pending(P1), 0);
    }

    #[test]
    fn registration_and_ack_in_same_drain() {
        // Issue and resolve between two drains: the resolution's
        // registration is found via the causality retry.
        let (mut cp, mut issuer) = checkpointer(&[P0]);
        drop(issuer.issue(P0, 9));
        let stats = cp.drain();
        assert_eq!(stats.applied, 1);
        assert_eq!(stats.unknown, 0);
        assert_eq!(cp.take_watermarks(), vec![(P0, 10)]);
    }

    #[test]
    fn cross_thread_issue_and_resolve() {
        let (mut cp, issuer) = checkpointer(&[P0, P1]);
        let handles: Vec<_> = [P0, P1]
            .into_iter()
            .map(|p| {
                let mut issuer = issuer.clone();
                std::thread::spawn(move || {
                    for i in 0..100i64 {
                        drop(issuer.issue(p, (i + 1) * 10 - 1));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let stats = cp.drain();
        assert_eq!(stats.applied, 200);
        assert_eq!(stats.unknown, 0);
        assert_eq!(cp.take_watermarks(), vec![(P0, 1000), (P1, 1000)]);
    }

    #[test]
    fn pending_counts_feed_backpressure() {
        let (mut cp, mut issuer) = checkpointer(&[P0, P1]);
        let held: Vec<_> = (0..5).map(|i| issuer.issue(P0, i)).collect();
        drop(issuer.issue(P1, 9));
        cp.drain();
        assert_eq!(cp.pending(P0), 5);
        assert_eq!(cp.max_pending(), 5);
        drop(held);
        cp.drain();
        let _ = cp.take_watermarks();
        assert_eq!(cp.max_pending(), 0);
    }

    #[test]
    #[should_panic(expected = "strictly increasing")]
    fn epoch_regression_panics() {
        let mut cp = Checkpointer::new();
        cp.begin_epoch(&[P0], 5);
        cp.begin_epoch(&[P0], 5);
    }
}

#[cfg(all(test, not(loom)))]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    enum Op {
        Issue { partition: u8, fail: bool },
        ResolveOldest,
        Rebalance { partitions: Vec<u8> },
        DrainAndTake,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                (0..3u8, any::<bool>()).prop_map(|(partition, fail)| Op::Issue { partition, fail }),
                Just(Op::ResolveOldest),
                prop::collection::vec(0..3u8, 1..3)
                    .prop_map(|partitions| Op::Rebalance { partitions }),
                Just(Op::DrainAndTake),
            ],
            0..120,
        )
    }

    proptest! {
        /// Watermarks are per-partition monotonic, never move for
        /// unassigned partitions, and acknowledgements issued under an old
        /// epoch never affect a newer epoch's watermarks.
        #[test]
        fn epoch_churn_never_leaks_stale_acks(ops in ops()) {
            let mut cp = Checkpointer::new();
            let mut epoch = 1u32;
            let mut assigned: Vec<PartitionId> = vec![PartitionId(0), PartitionId(1), PartitionId(2)];
            cp.begin_epoch(&assigned, epoch);
            let mut issuer = cp.handle();
            let mut offsets: std::collections::HashMap<PartitionId, i64> =
                std::collections::HashMap::new();
            // Held (unresolved) acks with the epoch they were issued under.
            let mut held: std::collections::VecDeque<(AckRef, u32, bool)> =
                std::collections::VecDeque::new();
            let mut last_watermark: std::collections::HashMap<PartitionId, i64> =
                std::collections::HashMap::new();

            for op in ops {
                match op {
                    Op::Issue { partition, fail } => {
                        let p = PartitionId(u32::from(partition));
                        if !assigned.contains(&p) {
                            continue;
                        }
                        let next = offsets.entry(p).or_insert(0);
                        *next += 10;
                        let ack = issuer.issue(p, *next - 1);
                        if fail {
                            ack.fail();
                        }
                        held.push_back((ack, epoch, fail));
                    }
                    Op::ResolveOldest => {
                        held.pop_front(); // drop resolves it
                    }
                    Op::Rebalance { partitions } => {
                        epoch += 1;
                        assigned = partitions
                            .into_iter()
                            .map(|p| PartitionId(u32::from(p)))
                            .collect::<std::collections::BTreeSet<_>>()
                            .into_iter()
                            .collect();
                        cp.begin_epoch(&assigned, epoch);
                        // Sequences and offsets restart with the epoch;
                        // watermark monotonicity is per-epoch.
                        offsets.clear();
                        last_watermark.clear();
                    }
                    Op::DrainAndTake => {
                        cp.drain();
                        for (p, w) in cp.take_watermarks() {
                            prop_assert!(
                                assigned.contains(&p),
                                "watermark for unassigned partition {p:?}"
                            );
                            if let Some(&prev) = last_watermark.get(&p) {
                                prop_assert!(w > prev, "watermark not monotonic for {p:?}");
                            }
                            last_watermark.insert(p, w);
                        }
                    }
                }
            }

            // Resolve everything still held (stale epochs included), then
            // verify stale resolutions changed nothing they shouldn't.
            let stale_epochs: Vec<u32> =
                held.iter().map(|&(_, e, _)| e).filter(|&e| e != epoch).collect();
            held.clear();
            let stats = cp.drain();
            prop_assert!(stats.unknown == 0, "driver-bug resolutions: {stats:?}");
            for (p, w) in cp.take_watermarks() {
                prop_assert!(assigned.contains(&p));
                if let Some(&prev) = last_watermark.get(&p) {
                    prop_assert!(w > prev);
                }
            }
            // Sanity: if there were stale-epoch acks, they were counted.
            if !stale_epochs.is_empty() {
                prop_assert!(stats.stale_epoch > 0);
            }
        }
    }
}
