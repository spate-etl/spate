//! The in-flight tracker: the delivered offsets each owned partition has not
//! finished, and how far each partition may commit.
//!
//! A partition's commit point is one past the offset delivered last, or its
//! first unfinished offset where that is lower. An offset is finished when
//! the reader acknowledges it, or when the write that moves it to another
//! topic is confirmed. An offset delivered more than once under one
//! assignment finishes when every copy has. The tracker is pure and
//! synchronous; the caller holds it under a lock and never keeps that lock
//! across a broker call.
//!
//! Each assignment of a partition carries a generation. A receipt names the
//! generation its delivery was made under, and once the partition is revoked
//! or reassigned the receipt is gone: acknowledging it or confirming its
//! write completes nothing. A held receipt is dropped when the reader next
//! uses it.

use spate_core::feed::Receipt;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A partition of one of the topics the feed reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Tp {
    pub(crate) topic: Arc<str>,
    pub(crate) partition: i32,
}

impl Tp {
    pub(crate) fn new(topic: &Arc<str>, partition: i32) -> Tp {
        Tp {
            topic: Arc::clone(topic),
            partition,
        }
    }
}

/// One delivered message: its partition, its offset, and the generation of
/// the assignment it was delivered under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) tp: Tp,
    pub(crate) offset: i64,
    pub(crate) generation: u64,
}

/// What the tracker keeps for one owned partition.
#[derive(Debug)]
struct Partition {
    generation: u64,
    /// Offsets delivered under this assignment and not yet finished, with
    /// the number of copies of each still unfinished.
    unfinished: BTreeMap<i64, u32>,
    /// One past the offset delivered last under this assignment.
    next: Option<i64>,
    /// The commit point [`Tracker::take_commits`] last returned.
    taken: Option<i64>,
}

impl Partition {
    fn commit_point(&self) -> Option<i64> {
        match self.unfinished.first_key_value() {
            // An unfinished offset above `next` was delivered before the
            // fetch position moved back, and the offsets from `next` up to
            // it are not read again yet.
            Some((first, _)) => Some(self.next.map_or(*first, |n| n.min(*first))),
            None => self.next,
        }
    }
}

/// The delivered and unfinished offsets of every partition the feed owns.
#[derive(Debug, Default)]
pub(crate) struct Tracker {
    /// Raised on every assignment change.
    generation: u64,
    next_receipt: u64,
    partitions: HashMap<Tp, Partition>,
    /// Deliveries the reader holds, by receipt.
    held: HashMap<u64, Position>,
}

impl Tracker {
    #[cfg(test)]
    pub(crate) fn new() -> Tracker {
        Tracker::default()
    }

    /// Take ownership of `tps` under a new generation, with nothing delivered.
    pub(crate) fn assign(&mut self, tps: impl IntoIterator<Item = Tp>) {
        self.generation += 1;
        for tp in tps {
            self.partitions.insert(
                tp,
                Partition {
                    generation: self.generation,
                    unfinished: BTreeMap::new(),
                    next: None,
                    taken: None,
                },
            );
        }
    }

    /// Give up `tps`, and return the commit point of each that has one, for
    /// the commit that must precede the release.
    pub(crate) fn revoke(&mut self, tps: &[Tp]) -> Vec<(Tp, i64)> {
        self.generation += 1;
        tps.iter()
            .filter_map(|tp| {
                let point = self.partitions.remove(tp)?.commit_point()?;
                Some((tp.clone(), point))
            })
            .collect()
    }

    /// Give up every partition with nothing to commit, as after a rebalance
    /// error, which has already released them.
    pub(crate) fn unassign_all(&mut self) {
        self.generation += 1;
        self.partitions.clear();
    }

    /// Whether `tp` is owned.
    #[cfg(test)]
    pub(crate) fn owns(&self, tp: &Tp) -> bool {
        self.partitions.contains_key(tp)
    }

    /// Record `offset` of `tp` as delivered and unfinished, and mint the
    /// receipt the reader holds it by. `None` when `tp` is not owned.
    pub(crate) fn deliver(&mut self, tp: &Tp, offset: i64) -> Option<Receipt> {
        let partition = self.partitions.get_mut(tp)?;
        // A delivery below `next` means the consumer reads forward from that
        // offset again, so `next` follows the last delivery down, and each
        // copy of an offset must finish before the offset does.
        *partition.unfinished.entry(offset).or_insert(0) += 1;
        partition.next = Some(offset + 1);
        self.next_receipt += 1;
        let receipt = self.next_receipt;
        self.held.insert(
            receipt,
            Position {
                tp: tp.clone(),
                offset,
                generation: partition.generation,
            },
        );
        Some(Receipt(receipt))
    }

    /// Whether `position` was delivered under its partition's current
    /// assignment.
    pub(crate) fn current(&self, position: &Position) -> bool {
        self.partitions
            .get(&position.tp)
            .is_some_and(|p| p.generation == position.generation)
    }

    /// The position a held receipt names. `None`, dropping the receipt, when
    /// its partition was revoked or reassigned since.
    pub(crate) fn held(&mut self, receipt: Receipt) -> Option<Position> {
        let position = self.held.get(&receipt.0)?;
        if self.current(position) {
            return Some(position.clone());
        }
        self.held.remove(&receipt.0);
        None
    }

    /// Finish the offset a held receipt names. `false`, completing nothing,
    /// when the receipt is not held under its partition's current
    /// assignment.
    pub(crate) fn acknowledge(&mut self, receipt: Receipt) -> bool {
        match self.release(receipt) {
            Some(position) => self.complete(&position),
            None => false,
        }
    }

    /// Take a held delivery from the reader for a write that finishes its
    /// offset once confirmed. The offset stays unfinished until
    /// [`complete`](Tracker::complete). `None` when the receipt is not held
    /// under its partition's current assignment.
    pub(crate) fn release(&mut self, receipt: Receipt) -> Option<Position> {
        let position = self.held.remove(&receipt.0)?;
        self.current(&position).then_some(position)
    }

    /// Finish `position` once the write that moved it is confirmed. `false`,
    /// completing nothing, when its partition was revoked or reassigned since
    /// it was delivered.
    pub(crate) fn complete(&mut self, position: &Position) -> bool {
        match self.partitions.get_mut(&position.tp) {
            Some(p) if p.generation == position.generation => {
                if let Some(copies) = p.unfinished.get_mut(&position.offset) {
                    *copies -= 1;
                    if *copies == 0 {
                        p.unfinished.remove(&position.offset);
                    }
                }
                true
            }
            _ => false,
        }
    }

    /// The offset `tp` may commit: one past the offset delivered last, or its
    /// first unfinished offset where that is lower. `None` before its first
    /// delivery.
    #[cfg(test)]
    pub(crate) fn commit_point(&self, tp: &Tp) -> Option<i64> {
        self.partitions.get(tp)?.commit_point()
    }

    /// The commit point of every owned partition whose point moved since the
    /// last call, for the periodic commit.
    pub(crate) fn take_commits(&mut self) -> Vec<(Tp, i64)> {
        let mut moved = Vec::new();
        for (tp, p) in &mut self.partitions {
            if let Some(point) = p.commit_point()
                && p.taken != Some(point)
            {
                p.taken = Some(point);
                moved.push((tp.clone(), point));
            }
        }
        moved
    }

    /// Mark `points` as not taken, so the next
    /// [`take_commits`](Tracker::take_commits) returns them again. For a
    /// commit that failed. A partition whose point moved since is left as
    /// it is.
    pub(crate) fn untake(&mut self, points: &[(Tp, i64)]) {
        for (tp, point) in points {
            if let Some(p) = self.partitions.get_mut(tp)
                && p.taken == Some(*point)
            {
                p.taken = None;
            }
        }
    }

    /// The partitions owned now.
    pub(crate) fn owned(&self) -> impl Iterator<Item = &Tp> {
        self.partitions.keys()
    }

    /// Replace every held copy of `offset` of `tp` with the delivery about
    /// to be made: each held receipt naming it under the current assignment
    /// is removed, and its copy finished. Returns those receipts, so the
    /// caller can drop what it keeps for them.
    ///
    /// A receipt is valid until a redelivery replaces it, and the copy it
    /// held must still finish, or the offset would stay unfinished for the
    /// rest of the assignment. A copy released for a write is not held, and
    /// finishes when its write is confirmed.
    pub(crate) fn replace_copies(&mut self, tp: &Tp, offset: i64) -> Vec<Receipt> {
        let Some(partition) = self.partitions.get_mut(tp) else {
            return Vec::new();
        };
        // Only an offset delivered again has a copy to replace, which is
        // rare, so the scan of `held` stays off the common path.
        if !partition.unfinished.contains_key(&offset) {
            return Vec::new();
        }
        let generation = partition.generation;
        let replaced: Vec<u64> = self
            .held
            .iter()
            .filter(|(_, p)| p.tp == *tp && p.offset == offset && p.generation == generation)
            .map(|(&receipt, _)| receipt)
            .collect();
        for receipt in &replaced {
            if let Some(position) = self.held.remove(receipt) {
                self.complete(&position);
            }
        }
        replaced.into_iter().map(Receipt).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(partition: i32) -> Tp {
        Tp::new(&Arc::from("work"), partition)
    }

    /// Pins the commit point of deliveries in offset order: the first
    /// unfinished offset while one is unfinished, one past the last delivered
    /// once none is, and nothing before the first delivery.
    #[test]
    fn a_partition_commits_up_to_its_first_unfinished_offset() {
        let mut t = Tracker::new();
        t.assign([tp(0)]);
        assert_eq!(t.commit_point(&tp(0)), None);
        let r5 = t.deliver(&tp(0), 5).unwrap();
        let r6 = t.deliver(&tp(0), 6).unwrap();
        let r9 = t.deliver(&tp(0), 9).unwrap();
        assert_eq!(t.commit_point(&tp(0)), Some(5));
        assert!(t.acknowledge(r6));
        assert_eq!(t.commit_point(&tp(0)), Some(5), "6 done, 5 is not");
        assert!(t.acknowledge(r5));
        assert_eq!(t.commit_point(&tp(0)), Some(9));
        let moving = t.release(r9).unwrap();
        assert_eq!(t.commit_point(&tp(0)), Some(9), "9 waits for its write");
        assert!(t.complete(&moving));
        assert_eq!(t.commit_point(&tp(0)), Some(10));
        assert_eq!(t.take_commits(), vec![(tp(0), 10)]);
        assert_eq!(t.take_commits(), vec![], "unchanged since the last take");
    }

    /// Pins that a receipt from before a reassignment is gone, and that
    /// acknowledging it or confirming its write leaves the redelivered
    /// offset unfinished.
    #[test]
    fn a_receipt_from_an_earlier_assignment_completes_nothing() {
        let mut t = Tracker::new();
        t.assign([tp(0)]);
        let old = t.deliver(&tp(0), 5).unwrap();
        let old_write = t.deliver(&tp(0), 6).unwrap();
        let moving = t.release(old_write).unwrap();
        assert_eq!(t.revoke(&[tp(0)]), vec![(tp(0), 5)]);
        assert!(!t.owns(&tp(0)));

        t.assign([tp(0)]);
        let again = t.deliver(&tp(0), 5).unwrap();
        let _ = t.deliver(&tp(0), 6).unwrap();
        assert_ne!(again, old);
        assert!(!t.acknowledge(old));
        assert!(!t.complete(&moving));
        assert_eq!(t.commit_point(&tp(0)), Some(5));
        assert!(t.held(old).is_none());
        assert!(t.held(again).is_some());
    }

    /// Pins that a rebalance error gives up every partition with nothing to
    /// commit, and that a partition not owned takes no delivery.
    #[test]
    fn a_rebalance_error_gives_up_every_partition() {
        let mut t = Tracker::new();
        t.assign([tp(0), tp(1)]);
        let r = t.deliver(&tp(1), 3).unwrap();
        t.unassign_all();
        assert!(!t.owns(&tp(0)) && !t.owns(&tp(1)));
        assert!(t.deliver(&tp(0), 0).is_none());
        assert!(!t.acknowledge(r));
        assert_eq!(t.take_commits(), vec![]);
    }

    /// Pins that a delivery below offsets already finished moves the commit
    /// point back to one past it. The offsets read again may carry messages
    /// not yet read, so the point must not stay at the old high mark.
    #[test]
    fn a_delivery_below_finished_offsets_moves_the_commit_point_back() {
        let mut t = Tracker::new();
        t.assign([tp(0)]);
        for offset in 0..10 {
            let r = t.deliver(&tp(0), offset).unwrap();
            assert!(t.acknowledge(r));
        }
        assert_eq!(t.take_commits(), vec![(tp(0), 10)]);
        let again = t.deliver(&tp(0), 5).unwrap();
        assert_eq!(t.commit_point(&tp(0)), Some(5));
        assert!(t.acknowledge(again));
        assert_eq!(t.commit_point(&tp(0)), Some(6));
        assert_eq!(t.take_commits(), vec![(tp(0), 6)]);
    }

    /// Pins that an offset delivered twice under one assignment finishes
    /// only once both copies have finished, whether the earlier copy is
    /// still held or already released for a write.
    #[test]
    fn an_offset_delivered_twice_finishes_with_its_last_copy() {
        let mut t = Tracker::new();
        t.assign([tp(0)]);
        let first = t.deliver(&tp(0), 5).unwrap();
        let second = t.deliver(&tp(0), 5).unwrap();
        assert_ne!(first, second);
        assert!(t.acknowledge(first));
        assert_eq!(t.commit_point(&tp(0)), Some(5), "the second copy is held");
        assert!(t.acknowledge(second));
        assert_eq!(t.commit_point(&tp(0)), Some(6));

        let earlier = t.deliver(&tp(0), 6).unwrap();
        let moving = t.release(earlier).unwrap();
        let later = t.deliver(&tp(0), 6).unwrap();
        assert!(t.complete(&moving));
        assert_eq!(t.commit_point(&tp(0)), Some(6), "the later copy is held");
        assert!(t.acknowledge(later));
        assert_eq!(t.commit_point(&tp(0)), Some(7));
    }

    /// Pins that revoking one partition leaves another partition's receipts
    /// valid.
    #[test]
    fn revoking_one_partition_keeps_the_others_receipts() {
        let mut t = Tracker::new();
        t.assign([tp(0), tp(1)]);
        let _r0 = t.deliver(&tp(0), 3).unwrap();
        let r1 = t.deliver(&tp(1), 7).unwrap();
        assert_eq!(t.revoke(&[tp(0)]), vec![(tp(0), 3)]);
        assert!(t.held(r1).is_some());
        assert!(t.acknowledge(r1));
        assert_eq!(t.commit_point(&tp(1)), Some(8));
    }

    /// Pins that an offset held above a delivery that moved back does not
    /// lift the commit point past the offsets read again.
    #[test]
    fn an_offset_held_above_a_reset_keeps_the_point_at_the_reset() {
        let mut t = Tracker::new();
        t.assign([tp(0)]);
        let _held = t.deliver(&tp(0), 10).unwrap();
        let again = t.deliver(&tp(0), 5).unwrap();
        assert!(t.acknowledge(again));
        assert_eq!(t.commit_point(&tp(0)), Some(6));
    }

    /// Pins that a redelivery replaces a held copy of the same offset: the
    /// old receipt is gone, its copy finished, and the offset finishes with
    /// the new copy. A copy released for a write is left to its write.
    #[test]
    fn a_redelivery_replaces_the_held_copy_and_finishes_it() {
        let mut t = Tracker::new();
        t.assign([tp(0)]);
        let old = t.deliver(&tp(0), 5).unwrap();
        assert_eq!(t.replace_copies(&tp(0), 5), vec![old]);
        let new = t.deliver(&tp(0), 5).unwrap();
        assert!(t.held(old).is_none(), "the replaced receipt is gone");
        assert_eq!(t.commit_point(&tp(0)), Some(5));
        assert!(t.acknowledge(new));
        assert_eq!(t.commit_point(&tp(0)), Some(6), "no copy of 5 is left");

        let writing = t.deliver(&tp(0), 6).unwrap();
        let moving = t.release(writing).unwrap();
        assert_eq!(t.replace_copies(&tp(0), 6), vec![]);
        let again = t.deliver(&tp(0), 6).unwrap();
        assert!(t.acknowledge(again));
        assert_eq!(t.commit_point(&tp(0)), Some(6), "the write's copy is open");
        assert!(t.complete(&moving));
        assert_eq!(t.commit_point(&tp(0)), Some(7));

        assert_eq!(t.replace_copies(&tp(1), 0), vec![], "not owned");
    }

    /// Pins that a point marked untaken is returned by the next take, and
    /// that a point which moved since is returned at its new value.
    #[test]
    fn an_untaken_point_is_taken_again() {
        let mut t = Tracker::new();
        t.assign([tp(0), tp(1)]);
        let r0 = t.deliver(&tp(0), 3).unwrap();
        let r1 = t.deliver(&tp(1), 8).unwrap();
        assert!(t.acknowledge(r0));
        assert!(t.acknowledge(r1));
        let mut taken = t.take_commits();
        taken.sort();
        assert_eq!(taken, vec![(tp(0), 4), (tp(1), 9)]);
        t.untake(&taken);
        let r1 = t.deliver(&tp(1), 9).unwrap();
        assert!(t.acknowledge(r1));
        let mut again = t.take_commits();
        again.sort();
        assert_eq!(again, vec![(tp(0), 4), (tp(1), 10)]);
        t.untake(&[(tp(1), 9)]);
        assert_eq!(t.take_commits(), vec![], "10 was taken after 9 failed");
    }

    /// Pins that a held receipt from an earlier assignment leaves the
    /// tracker when the reader next uses it.
    #[test]
    fn a_stale_receipt_is_dropped_when_next_used() {
        let mut t = Tracker::new();
        t.assign([tp(0)]);
        let old = t.deliver(&tp(0), 5).unwrap();
        t.revoke(&[tp(0)]);
        t.assign([tp(0)]);
        assert!(t.held.contains_key(&old.0));
        assert!(t.held(old).is_none());
        assert!(!t.held.contains_key(&old.0));
    }
}

#[cfg(test)]
mod proptests {
    //! The tracker against an oracle that keeps, per partition, the copies
    //! of each delivered offset not yet finished and the offset the reader
    //! reads next, and never calls the tracker to decide anything.

    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    /// Partitions over two topics, by index.
    const PARTITIONS: usize = 3;

    fn tp_of(index: usize) -> Tp {
        let (topic, partition) = [("work", 0), ("work", 1), ("retry", 0)][index];
        Tp::new(&Arc::from(topic), partition)
    }

    #[derive(Clone, Debug)]
    enum Op {
        /// Deliver offset `last + gap` of partition `p`, floored at 0. A gap
        /// below 1 delivers an offset again, as after a truncation.
        Deliver { p: usize, gap: i64 },
        /// Deliver offset `last + 1 - back` of partition `p`, floored at 0:
        /// the fetch position moving back past offsets that may still be
        /// held.
        Rewind { p: usize, back: i64 },
        /// Acknowledge the `pick`-th receipt ever minted.
        Ack { pick: usize },
        /// Acknowledge the receipt minted last, which after a rewind
        /// finishes an offset below others still held.
        AckLast,
        /// Renew the `pick`-th receipt ever minted.
        Renew { pick: usize },
        /// Release the `pick`-th receipt ever minted for a write.
        Fail { pick: usize },
        /// Confirm the `pick`-th write started.
        Confirm { pick: usize },
        /// Revoke every owned partition.
        Revoke,
        /// A rebalance error: give up every partition, committing nothing.
        Unassign,
        /// Assign the partitions in `mask` that are not owned.
        Assign { mask: u8 },
        /// Take the periodic commits.
        Take,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                4 => (0..PARTITIONS, -1..3i64).prop_map(|(p, gap)| Op::Deliver { p, gap }),
                2 => (0..PARTITIONS, 2..8i64).prop_map(|(p, back)| Op::Rewind { p, back }),
                3 => any::<usize>().prop_map(|pick| Op::Ack { pick }),
                2 => Just(Op::AckLast),
                1 => any::<usize>().prop_map(|pick| Op::Renew { pick }),
                2 => any::<usize>().prop_map(|pick| Op::Fail { pick }),
                2 => any::<usize>().prop_map(|pick| Op::Confirm { pick }),
                1 => Just(Op::Revoke),
                1 => Just(Op::Unassign),
                2 => (1..8u8).prop_map(|mask| Op::Assign { mask }),
                2 => Just(Op::Take),
            ],
            0..300,
        )
    }

    /// The oracle's view of one owned partition.
    #[derive(Default)]
    struct Owned {
        generation: u64,
        /// Unfinished copies, by offset.
        unfinished: BTreeMap<i64, u32>,
        /// The offset delivered last, if any.
        last: Option<i64>,
        taken: Option<i64>,
    }

    impl Owned {
        /// The highest offset at or below `cursor`, where the reader reads
        /// next, with no unfinished copy below it. `None` before the first
        /// delivery under this assignment.
        fn point(&self, cursor: i64) -> Option<i64> {
            self.last?;
            (0..=cursor)
                .rev()
                .find(|c| self.unfinished.range(..*c).next().is_none())
        }

        fn finish(&mut self, offset: i64) {
            let copies = self
                .unfinished
                .get_mut(&offset)
                .expect("an unfinished copy");
            *copies -= 1;
            if *copies == 0 {
                self.unfinished.remove(&offset);
            }
        }
    }

    #[derive(Default)]
    struct Oracle {
        generation: u64,
        owned: BTreeMap<usize, Owned>,
        /// The group's committed offset per partition, where the next
        /// assignment starts reading.
        committed: BTreeMap<usize, i64>,
        /// Where the next delivery of each owned partition may start.
        cursor: BTreeMap<usize, i64>,
        /// Every receipt minted: its partition, offset and generation, and
        /// whether the reader still holds it.
        receipts: Vec<(Receipt, usize, i64, u64, bool)>,
        /// Writes started and not yet confirmed.
        writes: Vec<(Position, usize)>,
    }

    impl Oracle {
        /// The commit point of partition `p`, if it is owned.
        fn point(&self, p: usize) -> Option<i64> {
            self.owned.get(&p)?.point(self.cursor[&p])
        }

        /// Whether receipt `i` names a delivery the reader holds under its
        /// partition's current assignment.
        fn valid(&self, i: usize) -> bool {
            let (_, p, _, generation, held) = self.receipts[i];
            held && self
                .owned
                .get(&p)
                .is_some_and(|o| o.generation == generation)
        }
    }

    proptest! {
        // A commit point that ignores a delivery below a held offset fails
        // here within 40 cases on average, so 512 cases pass it with odds of
        // about 2 in a million.
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn the_commit_point_never_passes_an_unfinished_offset(ops in ops()) {
            let mut t = Tracker::new();
            let mut o = Oracle::default();

            for op in ops {
                match op {
                    Op::Deliver { .. } | Op::Rewind { .. } => {
                        let (p, step) = match op {
                            Op::Deliver { p, gap } => (p, gap - 1),
                            Op::Rewind { p, back } => (p, -back),
                            _ => unreachable!(),
                        };
                        let offset = (o.cursor.get(&p).copied().unwrap_or(0) + step).max(0);
                        let got = t.deliver(&tp_of(p), offset);
                        match o.owned.get_mut(&p) {
                            Some(owned) => {
                                o.cursor.insert(p, offset + 1);
                                *owned.unfinished.entry(offset).or_insert(0) += 1;
                                owned.last = Some(offset);
                                let receipt = got.expect("an owned partition takes a delivery");
                                prop_assert!(
                                    o.receipts.iter().all(|r| r.0 != receipt),
                                    "receipt {receipt} minted twice"
                                );
                                o.receipts.push((receipt, p, offset, owned.generation, true));
                            }
                            None => prop_assert!(got.is_none(), "delivered to an unowned partition"),
                        }
                    }
                    Op::Ack { .. } | Op::AckLast | Op::Renew { .. } | Op::Fail { .. } if o.receipts.is_empty() => {}
                    Op::Ack { .. } | Op::AckLast => {
                        let i = match op {
                            Op::Ack { pick } => pick % o.receipts.len(),
                            _ => o.receipts.len() - 1,
                        };
                        let expected = o.valid(i);
                        prop_assert_eq!(t.acknowledge(o.receipts[i].0), expected);
                        if expected {
                            let (_, p, offset, _, _) = o.receipts[i];
                            o.owned.get_mut(&p).unwrap().finish(offset);
                        }
                        o.receipts[i].4 = false;
                    }
                    Op::Renew { pick } => {
                        let i = pick % o.receipts.len();
                        let expected = o.valid(i);
                        prop_assert_eq!(t.held(o.receipts[i].0).is_some(), expected);
                        if !expected {
                            o.receipts[i].4 = false;
                        }
                    }
                    Op::Fail { pick } => {
                        let i = pick % o.receipts.len();
                        let expected = o.valid(i);
                        let got = t.release(o.receipts[i].0);
                        prop_assert_eq!(got.is_some(), expected);
                        if let Some(position) = got {
                            let (_, p, offset, generation, _) = o.receipts[i];
                            prop_assert_eq!(&position.tp, &tp_of(p));
                            prop_assert_eq!(position.offset, offset);
                            prop_assert_eq!(position.generation, generation);
                            o.writes.push((position, p));
                        }
                        o.receipts[i].4 = false;
                    }
                    Op::Confirm { pick } => {
                        if o.writes.is_empty() {
                            continue;
                        }
                        let (position, p) = o.writes.swap_remove(pick % o.writes.len());
                        let expected = o
                            .owned
                            .get(&p)
                            .is_some_and(|owned| owned.generation == position.generation);
                        prop_assert_eq!(t.complete(&position), expected);
                        if expected {
                            o.owned.get_mut(&p).unwrap().finish(position.offset);
                        }
                    }
                    Op::Revoke => {
                        let tps: Vec<Tp> = o.owned.keys().map(|p| tp_of(*p)).collect();
                        let mut got = t.revoke(&tps);
                        got.sort();
                        let mut expected = Vec::new();
                        for p in o.owned.keys().copied().collect::<Vec<_>>() {
                            if let Some(point) = o.point(p) {
                                expected.push((tp_of(p), point));
                                o.committed.insert(p, point);
                            }
                        }
                        o.owned.clear();
                        expected.sort();
                        prop_assert_eq!(got, expected);
                        o.generation += 1;
                    }
                    Op::Unassign => {
                        t.unassign_all();
                        o.owned.clear();
                        o.generation += 1;
                    }
                    Op::Assign { mask } => {
                        let new: Vec<usize> = (0..PARTITIONS)
                            .filter(|p| mask & (1 << p) != 0 && !o.owned.contains_key(p))
                            .collect();
                        t.assign(new.iter().map(|p| tp_of(*p)));
                        o.generation += 1;
                        for p in new {
                            o.owned.insert(p, Owned { generation: o.generation, ..Owned::default() });
                            o.cursor.insert(p, o.committed.get(&p).copied().unwrap_or(0));
                        }
                    }
                    Op::Take => {
                        let mut got = t.take_commits();
                        got.sort();
                        let mut expected = Vec::new();
                        for p in o.owned.keys().copied().collect::<Vec<_>>() {
                            let point = o.point(p);
                            let owned = o.owned.get_mut(&p).unwrap();
                            if let Some(point) = point
                                && owned.taken != Some(point)
                            {
                                owned.taken = Some(point);
                                expected.push((tp_of(p), point));
                            }
                        }
                        expected.sort();
                        prop_assert_eq!(got, expected);
                    }
                }
                for p in 0..PARTITIONS {
                    let expected = o.point(p);
                    prop_assert_eq!(t.commit_point(&tp_of(p)), expected, "partition {}", p);
                    prop_assert_eq!(t.owns(&tp_of(p)), o.owned.contains_key(&p));
                }
            }
        }
    }
}
