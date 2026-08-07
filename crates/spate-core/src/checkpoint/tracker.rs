//! Per-partition contiguity tracking of batch acknowledgments.
//!
//! A [`PartitionTracker`] receives batch registrations in sequence order
//! and resolutions in *any* order, and computes the committable watermark:
//! the offset just past the contiguous prefix of delivered batches. A
//! failed batch at the head of that prefix stalls the watermark permanently
//! — the at-least-once invariant is that a source offset is never committed
//! past unacknowledged or failed data.
//!
//! Purely synchronous and tokio-free; all methods are `&mut self`. The
//! concurrency of the ack path lives entirely in
//! [`AckRef`](super::AckRef)'s atomics and the checkpointer's channel.

use super::AckStatus;
use std::collections::VecDeque;
use std::time::Instant;

/// Result of applying one resolution to the tracker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum ResolveOutcome {
    /// The batch moved from pending to delivered or failed.
    Applied,
    /// The batch was already resolved — duplicate resolution message.
    Duplicate,
    /// The sequence number precedes the tracked window: the batch already
    /// advanced through the watermark. Duplicate of a consumed resolution.
    AlreadyAdvanced,
    /// The sequence number is ahead of every registration seen so far. With
    /// a correctly ordered driver this means the registration is still in
    /// flight; the caller should retry after draining registrations.
    Unregistered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotState {
    Pending,
    Delivered,
    Failed,
}

#[derive(Clone, Copy, Debug)]
struct Slot {
    last_offset: i64,
    state: SlotState,
}

/// Contiguity tracker for one partition within one assignment epoch.
///
/// Sequence numbers start at 0 and must be registered contiguously; a gap
/// is a driver bug and panics. Resolutions arrive out of order (sharded
/// sinks ack whenever their flushes complete).
///
/// ```
/// use spate_core::checkpoint::{AckStatus, PartitionTracker};
///
/// let mut t = PartitionTracker::new();
/// t.register(0, 99); // batch 0 covers offsets ..=99
/// t.register(1, 199);
/// let _ = t.resolve(1, AckStatus::Delivered); // out of order
/// assert_eq!(t.advance(), None); // batch 0 still pending
/// let _ = t.resolve(0, AckStatus::Delivered);
/// assert_eq!(t.advance(), Some(200)); // committable position
/// ```
#[derive(Debug, Default)]
pub struct PartitionTracker {
    /// Sequence number of `ring[0]`.
    head_seq: u64,
    /// Next expected registration sequence (`head_seq + ring.len()`).
    next_seq: u64,
    ring: VecDeque<Slot>,
    /// Set when a failed batch reaches the head of the ring; permanent.
    stalled_since: Option<Instant>,
    /// Sequence of the failed batch that caused the stall.
    stalled_seq: Option<u64>,
}

impl PartitionTracker {
    /// A fresh tracker expecting its first registration at sequence 0.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a newly issued batch. Sequences must be contiguous.
    ///
    /// Registering after a stall is legal (batches already in flight when
    /// the failure surfaced still arrive); they will never advance the
    /// watermark.
    ///
    /// # Panics
    ///
    /// Panics on a sequence gap — the driver issued batches out of order,
    /// which would silently corrupt watermark accounting.
    pub fn register(&mut self, seq: u64, last_offset: i64) {
        assert_eq!(
            seq, self.next_seq,
            "batch registration gap: expected seq {}, got {seq}",
            self.next_seq
        );
        self.ring.push_back(Slot {
            last_offset,
            state: SlotState::Pending,
        });
        self.next_seq += 1;
    }

    /// Apply one resolution. Out-of-order and cross-batch interleavings are
    /// expected; duplicates and unknown sequences are reported (and
    /// `debug_assert`ed) rather than corrupting state.
    pub fn resolve(&mut self, seq: u64, status: AckStatus) -> ResolveOutcome {
        if seq < self.head_seq {
            debug_assert!(false, "resolution for already-advanced batch {seq}");
            return ResolveOutcome::AlreadyAdvanced;
        }
        let Ok(idx) = usize::try_from(seq - self.head_seq) else {
            return ResolveOutcome::Unregistered;
        };
        let Some(slot) = self.ring.get_mut(idx) else {
            return ResolveOutcome::Unregistered;
        };
        if slot.state != SlotState::Pending {
            debug_assert!(false, "duplicate resolution for batch {seq}");
            return ResolveOutcome::Duplicate;
        }
        slot.state = match status {
            AckStatus::Delivered => SlotState::Delivered,
            AckStatus::Failed => SlotState::Failed,
        };
        ResolveOutcome::Applied
    }

    /// Pop the contiguous delivered prefix and return the new committable
    /// offset (one past the last delivered batch), or `None` if the
    /// watermark did not move.
    ///
    /// If the batch at the head has failed, the tracker stalls permanently:
    /// everything delivered *before* the failure is still reported (commit
    /// up to the failure is correct), but nothing past it ever will be.
    pub fn advance(&mut self) -> Option<i64> {
        let mut committable = None;
        while let Some(slot) = self.ring.front() {
            match slot.state {
                SlotState::Delivered => {
                    committable = Some(slot.last_offset + 1);
                    self.ring.pop_front();
                    self.head_seq += 1;
                }
                SlotState::Failed => {
                    if self.stalled_since.is_none() {
                        self.stalled_since = Some(Instant::now());
                        self.stalled_seq = Some(self.head_seq);
                    }
                    break;
                }
                SlotState::Pending => break,
            }
        }
        committable
    }

    /// Whether a failed batch has reached the head and blocked the
    /// watermark. Permanent once set.
    #[must_use]
    pub fn stalled(&self) -> bool {
        self.stalled_since.is_some()
    }

    /// When the stall was first observed, for watermark-age alerting.
    #[must_use]
    pub fn stalled_since(&self) -> Option<Instant> {
        self.stalled_since
    }

    /// Sequence number of the failed batch that caused the stall.
    #[must_use]
    pub fn stalled_seq(&self) -> Option<u64> {
        self.stalled_seq
    }

    /// Batches issued but not yet advanced through the watermark. This is
    /// the backpressure trigger: it bounds tracker memory and replay size.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.ring.len()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn delivered(t: &mut PartitionTracker, seq: u64) {
        assert_eq!(
            t.resolve(seq, AckStatus::Delivered),
            ResolveOutcome::Applied
        );
    }

    fn failed(t: &mut PartitionTracker, seq: u64) {
        assert_eq!(t.resolve(seq, AckStatus::Failed), ResolveOutcome::Applied);
    }

    #[test]
    fn happy_path_in_order() {
        let mut t = PartitionTracker::new();
        t.register(0, 9);
        t.register(1, 19);
        t.register(2, 29);
        delivered(&mut t, 0);
        assert_eq!(t.advance(), Some(10));
        delivered(&mut t, 1);
        delivered(&mut t, 2);
        assert_eq!(t.advance(), Some(30));
        assert_eq!(t.advance(), None);
        assert_eq!(t.pending(), 0);
    }

    #[test]
    fn single_batch() {
        let mut t = PartitionTracker::new();
        t.register(0, 41);
        assert_eq!(t.advance(), None);
        delivered(&mut t, 0);
        assert_eq!(t.advance(), Some(42));
    }

    #[test]
    fn out_of_order_resolution_waits_for_head() {
        let mut t = PartitionTracker::new();
        for seq in 0..4 {
            t.register(seq, (seq as i64 + 1) * 10 - 1);
        }
        delivered(&mut t, 2);
        delivered(&mut t, 3);
        assert_eq!(t.advance(), None);
        delivered(&mut t, 0);
        assert_eq!(t.advance(), Some(10));
        delivered(&mut t, 1);
        assert_eq!(t.advance(), Some(40));
        assert_eq!(t.pending(), 0);
    }

    #[test]
    fn failure_at_head_stalls_permanently() {
        let mut t = PartitionTracker::new();
        t.register(0, 9);
        t.register(1, 19);
        failed(&mut t, 0);
        delivered(&mut t, 1);
        assert_eq!(t.advance(), None);
        assert!(t.stalled());
        assert_eq!(t.stalled_seq(), Some(0));
        assert!(t.stalled_since().is_some());
        // Nothing past the failure ever commits, even new deliveries.
        t.register(2, 29);
        delivered(&mut t, 2);
        assert_eq!(t.advance(), None);
        assert_eq!(t.pending(), 3);
    }

    #[test]
    fn advance_commits_up_to_failure() {
        let mut t = PartitionTracker::new();
        t.register(0, 9);
        t.register(1, 19);
        t.register(2, 29);
        delivered(&mut t, 0);
        failed(&mut t, 1);
        delivered(&mut t, 2);
        // Batch 0 is committable; the failure blocks 1 and 2.
        assert_eq!(t.advance(), Some(10));
        assert!(t.stalled());
        assert_eq!(t.stalled_seq(), Some(1));
    }

    #[test]
    fn failure_behind_pending_stalls_only_when_reached() {
        let mut t = PartitionTracker::new();
        t.register(0, 9);
        t.register(1, 19);
        failed(&mut t, 1);
        assert_eq!(t.advance(), None);
        assert!(!t.stalled(), "failure not yet at head");
        delivered(&mut t, 0);
        assert_eq!(t.advance(), Some(10));
        assert!(t.stalled());
    }

    #[test]
    fn duplicate_and_stale_resolutions_are_reported() {
        let mut t = PartitionTracker::new();
        t.register(0, 9);
        t.register(1, 19);
        delivered(&mut t, 0);
        assert_eq!(t.advance(), Some(10));

        // Unknown: not yet registered.
        assert_eq!(
            t.resolve(5, AckStatus::Delivered),
            ResolveOutcome::Unregistered
        );

        // These two trip debug_assert in debug builds by design; validate
        // the outcomes in release builds only.
        if cfg!(not(debug_assertions)) {
            assert_eq!(
                t.resolve(0, AckStatus::Delivered),
                ResolveOutcome::AlreadyAdvanced
            );
            delivered(&mut t, 1);
            assert_eq!(
                t.resolve(1, AckStatus::Delivered),
                ResolveOutcome::Duplicate
            );
        }
    }

    #[test]
    #[should_panic(expected = "batch registration gap")]
    fn registration_gap_panics() {
        let mut t = PartitionTracker::new();
        t.register(0, 9);
        t.register(2, 29);
    }

    #[test]
    fn watermark_is_monotonic() {
        let mut t = PartitionTracker::new();
        let mut last = 0;
        for seq in 0..100u64 {
            t.register(seq, (seq as i64 + 1) * 7);
        }
        // Resolve in a scrambled but deterministic order.
        for step in 0..100u64 {
            let seq = (step * 37) % 100;
            delivered(&mut t, seq);
            if let Some(w) = t.advance() {
                assert!(w > last, "watermark went backwards: {last} -> {w}");
                last = w;
            }
        }
        assert_eq!(last, 100 * 7 + 1);
        assert_eq!(t.pending(), 0);
    }
}

#[cfg(all(test, not(loom)))]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    #[derive(Clone, Debug)]
    enum Op {
        Register {
            last_offset_step: i64,
        },
        /// Resolve the `pick`-th unresolved batch (mod count), failing if
        /// `fail` is set.
        Resolve {
            pick: usize,
            fail: bool,
        },
        Advance,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                (1..100i64).prop_map(|last_offset_step| Op::Register { last_offset_step }),
                (any::<usize>(), any::<bool>()).prop_map(|(pick, fail)| Op::Resolve { pick, fail }),
                Just(Op::Advance),
            ],
            0..200,
        )
    }

    /// Reference model: every registered batch with its resolution, plus
    /// the committed prefix length.
    #[derive(Default)]
    struct Reference {
        batches: BTreeMap<u64, (i64, Option<AckStatus>)>,
        committed: u64,
        next_seq: u64,
        next_offset: i64,
        stalled: bool,
    }

    impl Reference {
        /// Longest contiguous delivered prefix from `committed`, unless a
        /// failure blocks it first.
        fn expected_advance(&mut self) -> Option<i64> {
            let mut committable = None;
            while let Some(&(last_offset, status)) = self.batches.get(&self.committed) {
                match status {
                    Some(AckStatus::Delivered) => {
                        committable = Some(last_offset + 1);
                        self.batches.remove(&self.committed);
                        self.committed += 1;
                    }
                    Some(AckStatus::Failed) => {
                        self.stalled = true;
                        break;
                    }
                    None => break,
                }
            }
            committable
        }
    }

    proptest! {
        #[test]
        fn tracker_matches_reference_model(ops in ops()) {
            let mut tracker = PartitionTracker::new();
            let mut reference = Reference::default();
            let mut unresolved: Vec<u64> = Vec::new();
            let mut watermark = None;

            for op in ops {
                match op {
                    Op::Register { last_offset_step } => {
                        reference.next_offset += last_offset_step;
                        let seq = reference.next_seq;
                        reference.next_seq += 1;
                        reference.batches.insert(seq, (reference.next_offset, None));
                        tracker.register(seq, reference.next_offset);
                        unresolved.push(seq);
                    }
                    Op::Resolve { pick, fail } => {
                        if unresolved.is_empty() {
                            continue;
                        }
                        let seq = unresolved.swap_remove(pick % unresolved.len());
                        let status = if fail { AckStatus::Failed } else { AckStatus::Delivered };
                        reference.batches.get_mut(&seq).unwrap().1 = Some(status);
                        prop_assert_eq!(tracker.resolve(seq, status), ResolveOutcome::Applied);
                    }
                    Op::Advance => {
                        let got = tracker.advance();
                        let expected = reference.expected_advance();
                        prop_assert_eq!(got, expected);
                        if let Some(w) = got {
                            if let Some(prev) = watermark {
                                prop_assert!(w > prev, "watermark not monotonic");
                            }
                            watermark = Some(got.unwrap());
                        }
                        prop_assert_eq!(tracker.stalled(), reference.stalled);
                    }
                }
                prop_assert_eq!(
                    tracker.pending(),
                    reference.batches.len(),
                    "pending mismatch"
                );
            }

            // Final settle: one more advance must also agree.
            let got = tracker.advance();
            let expected = reference.expected_advance();
            prop_assert_eq!(got, expected);
            prop_assert_eq!(tracker.stalled(), reference.stalled);
        }
    }
}
