//! The leader epoch of each offset a partition delivered, for the epoch a
//! commit carries.
//!
//! A committed offset carries the epoch of the record before it. A member
//! that starts at the commit hands the epoch to librdkafka, which asks the
//! leader where that epoch ended and moves the start down when the log was
//! truncated below it.

use crate::client::{epoch_of, listed_offset, set_epoch};
use rdkafka::error::{KafkaResult, RDKafkaErrorCode};
use rdkafka::{Offset, TopicPartitionList};
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

/// The most epoch changes one partition keeps. An entry needs a leader change,
/// so the cap is reached only by a partition whose leader changes 64 times
/// between two commits.
const CAP: usize = 64;

/// The leader epoch in force from each offset on, for one partition.
#[derive(Debug, Default)]
pub(crate) struct EpochCache {
    /// `(first offset, epoch)`, ascending by offset.
    entries: VecDeque<(i64, i32)>,
}

impl EpochCache {
    /// Records that the record at `offset` carries `epoch`. An epoch below 0
    /// means the broker sent none and is ignored. A record below the newest
    /// entry drops the entries above it, because the log it came from
    /// diverged from theirs.
    pub(crate) fn observe(&mut self, offset: i64, epoch: i32) {
        if epoch < 0 {
            return;
        }
        while self
            .entries
            .back()
            .is_some_and(|&(start, _)| start > offset)
        {
            self.entries.pop_back();
        }
        match self.entries.back_mut() {
            Some((_, last)) if *last == epoch => {}
            Some((start, last)) if *start == offset => *last = epoch,
            _ => {
                self.entries.push_back((offset, epoch));
                if self.entries.len() > CAP {
                    self.entries.pop_front();
                }
            }
        }
    }

    /// The epoch of the record at `offset`, or -1 when no entry covers it.
    pub(crate) fn at(&self, offset: i64) -> i32 {
        self.entries
            .iter()
            .rev()
            .find(|&&(start, _)| start <= offset)
            .map_or(-1, |&(_, epoch)| epoch)
    }

    /// Drops the entries no offset at or above `offset` reads.
    pub(crate) fn prune_below(&mut self, offset: i64) {
        while self
            .entries
            .get(1)
            .is_some_and(|&(start, _)| start <= offset)
        {
            self.entries.pop_front();
        }
    }

    /// Forgets every epoch, for a position whose record before it is unknown.
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}

/// How long a retried seek waits for its answer. librdkafka answers a seek
/// from its own threads before any broker request, so the answer comes well
/// within this.
pub(crate) const RETRY_SEEK_WAIT: Duration = Duration::from_millis(100);

/// A seek a test answers in place of the consumer.
#[cfg(test)]
pub(crate) type SeekWith =
    Box<dyn FnMut(TopicPartitionList, Duration) -> KafkaResult<TopicPartitionList> + Send + Sync>;

/// How a member starts a partition it was assigned, from the committed
/// offset and epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Start {
    /// Seek to the commit with its epoch, so librdkafka checks it against
    /// the leader before fetching.
    Validate,
    /// Read from the log start: the commit is above the partition's end.
    FromLogStart,
    /// Start where the committed list says, unchecked: no epoch, no concrete
    /// offset, or an offset with nothing below it.
    AsIs,
}

/// How to start a partition whose commit is `offset` with leader epoch
/// `epoch`, when its end offset is `end`, or `None` when the end is unknown.
/// `above_end_from_log_start` is false where a commit above the end must
/// reach librdkafka's reset policy, which then decides.
///
/// A seek with the commit's epoch, above the end of a log nothing is written
/// to, moves the member to the epoch's end and reads only later writes, so a
/// commit above the end starts from the log start instead.
pub(crate) fn plan_start(
    offset: Offset,
    epoch: i32,
    end: Option<i64>,
    above_end_from_log_start: bool,
) -> Start {
    match offset {
        Offset::Offset(offset) if offset > 0 && epoch >= 0 => match end {
            Some(end) if offset > end && above_end_from_log_start => Start::FromLogStart,
            _ => Start::Validate,
        },
        _ => Start::AsIs,
    }
}

/// The partitions of `committed` whose start depends on their end offset:
/// each with a concrete offset and an epoch, listed at [`Offset::End`] for
/// [`list_offsets`](crate::client::list_offsets).
pub(crate) fn ends_to_read(committed: &TopicPartitionList) -> TopicPartitionList {
    let mut ends = TopicPartitionList::new();
    for elem in committed.elements() {
        let epoch = epoch_of(committed, elem.topic(), elem.partition());
        if plan_start(elem.offset(), epoch, None, false) == Start::Validate {
            // A topic name the list already holds cannot fail to convert.
            let _ = ends.add_partition_offset(elem.topic(), elem.partition(), Offset::End);
        }
    }
    ends
}

/// Applies [`plan_start`] to each element of `committed`, given the end
/// offsets in `ends` (`None` when they could not be read): a start above the
/// end becomes [`Offset::Beginning`]. Returns the partitions to seek, each at
/// its commit and epoch.
pub(crate) fn plan_starts(
    committed: &mut TopicPartitionList,
    ends: Option<&TopicPartitionList>,
    above_end_from_log_start: bool,
) -> TopicPartitionList {
    let mut seek = TopicPartitionList::new();
    let epochs: Vec<i32> = committed
        .elements()
        .iter()
        .map(|e| epoch_of(committed, e.topic(), e.partition()))
        .collect();
    for (mut elem, epoch) in committed.elements().into_iter().zip(epochs) {
        let end = ends
            .and_then(|ends| ends.find_partition(elem.topic(), elem.partition()))
            .and_then(|e| listed_offset(&e).ok());
        match plan_start(elem.offset(), epoch, end, above_end_from_log_start) {
            Start::AsIs => {}
            Start::FromLogStart => {
                let _ = elem.set_offset(Offset::Beginning);
            }
            Start::Validate => {
                let (topic, partition) = (elem.topic(), elem.partition());
                if seek
                    .add_partition_offset(topic, partition, elem.offset())
                    .is_ok()
                {
                    set_epoch(&mut seek, topic, partition, epoch);
                }
            }
        }
    }
    seek
}

/// Whether a seek's answer for one partition means it was not started yet,
/// and the seek is to be sent again.
fn refused(code: Option<RDKafkaErrorCode>) -> bool {
    matches!(
        code,
        Some(
            RDKafkaErrorCode::State
                | RDKafkaErrorCode::InProgress
                | RDKafkaErrorCode::PreviousInProgress
        )
    )
}

/// Seeks each partition of `list` to its offset and epoch through `seek`,
/// given `budget`, and returns the partitions whose seek was refused, to be
/// sent again. librdkafka refuses a seek for a partition it has not started
/// yet, and one whose answer did not come within the budget. A budget of
/// zero sends nothing: librdkafka would return at once and leave every seek
/// unanswered. Any other failure is logged, and the partition starts at its
/// commit unchecked.
pub(crate) fn seek_starts(
    list: TopicPartitionList,
    budget: Duration,
    seek: impl FnOnce(TopicPartitionList, Duration) -> KafkaResult<TopicPartitionList>,
) -> Vec<(String, i32)> {
    let all: Vec<(String, i32)> = list
        .elements()
        .iter()
        .map(|e| (e.topic().to_owned(), e.partition()))
        .collect();
    if all.is_empty() || budget.is_zero() {
        return all;
    }
    let answered = match seek(list, budget) {
        Ok(answered) => answered,
        Err(e) => {
            tracing::debug!(error = %e, "a seek to the committed offsets was refused");
            return all;
        }
    };
    let mut again = Vec::new();
    for elem in answered.elements() {
        let Err(e) = elem.error() else { continue };
        if refused(e.rdkafka_error_code()) {
            again.push((elem.topic().to_owned(), elem.partition()));
        } else {
            tracing::warn!(
                topic = elem.topic(),
                partition = elem.partition(),
                error = %e,
                "seeking to the committed offset failed; the partition reads from it unchecked"
            );
        }
    }
    again
}

/// The partitions whose seek librdkafka refused, each with the commit and
/// epoch to seek to and the refusals in a row. Each stays paused until a
/// seek is accepted.
#[derive(Debug)]
pub(crate) struct Validating<K> {
    pending: BTreeMap<K, Pending>,
    /// The refusals in a row after which a partition is reported.
    report_after: u32,
}

#[derive(Debug)]
struct Pending {
    offset: i64,
    epoch: i32,
    refusals: u32,
}

impl<K: Ord + Clone> Validating<K> {
    pub(crate) fn new(report_after: u32) -> Validating<K> {
        Validating {
            pending: BTreeMap::new(),
            report_after,
        }
    }

    pub(crate) fn contains(&self, key: &K) -> bool {
        self.pending.contains_key(key)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Each partition with the commit and epoch its seek goes to.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&K, i64, i32)> {
        self.pending.iter().map(|(k, p)| (k, p.offset, p.epoch))
    }

    /// Counts a refused seek of `key` to `offset` at `epoch`. True once,
    /// when the count reaches the report threshold.
    pub(crate) fn refused(&mut self, key: K, offset: i64, epoch: i32) -> bool {
        let pending = self.pending.entry(key).or_insert(Pending {
            offset,
            epoch,
            refusals: 0,
        });
        pending.refusals = pending.refusals.saturating_add(1);
        pending.refusals == self.report_after
    }

    /// Drops `key`, for an accepted seek or a partition given up. True when
    /// it was there.
    pub(crate) fn remove(&mut self, key: &K) -> bool {
        self.pending.remove(key).is_some()
    }

    /// Drops every key `keep` refuses.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        self.pending.retain(|k, _| keep(k));
    }

    /// Drops every key, and returns them.
    pub(crate) fn take_all(&mut self) -> Vec<K> {
        std::mem::take(&mut self.pending).into_keys().collect()
    }
}

/// The refusals in a row that span `bound` when one is counted every `step`.
/// A refusal while a commit is in flight can last one group-call bound, so a
/// lower count would report a normal rebalance.
pub(crate) fn refusals_spanning(bound: Duration, step: Duration) -> u32 {
    let steps = bound.as_millis() / step.as_millis().max(1);
    u32::try_from(steps).unwrap_or(u32::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_of(observed: &[(i64, i32)]) -> EpochCache {
        let mut cache = EpochCache::default();
        for &(offset, epoch) in observed {
            cache.observe(offset, epoch);
        }
        cache
    }

    #[test]
    fn each_offset_reads_the_epoch_in_force_at_it() {
        let cache = cache_of(&[(10, 1), (11, 1), (20, 3), (25, 3)]);
        assert_eq!(cache.entries, [(10, 1), (20, 3)]);
        assert_eq!(cache.at(9), -1);
        assert_eq!(cache.at(10), 1);
        assert_eq!(cache.at(19), 1);
        assert_eq!(cache.at(20), 3);
        assert_eq!(cache.at(1_000), 3);
    }

    #[test]
    fn a_missing_epoch_is_ignored() {
        let cache = cache_of(&[(10, 1), (20, -1)]);
        assert_eq!(cache.entries, [(10, 1)]);
    }

    /// A record below the newest entry comes from a log that diverged at or
    /// before it, so the entries above it no longer describe the log.
    #[test]
    fn a_record_below_the_newest_entry_drops_the_entries_above_it() {
        let cache = cache_of(&[(10, 1), (20, 3), (30, 5), (15, 7)]);
        assert_eq!(cache.entries, [(10, 1), (15, 7)]);
        assert_eq!(cache.at(30), 7);

        let cache = cache_of(&[(10, 1), (20, 3), (20, 4)]);
        assert_eq!(cache.entries, [(10, 1), (20, 4)]);

        let cache = cache_of(&[(10, 1), (20, 3), (15, 1)]);
        assert_eq!(cache.entries, [(10, 1)]);
    }

    #[test]
    fn the_cap_drops_the_oldest_entry() {
        let cap = i32::try_from(CAP).expect("cap fits");
        let observed: Vec<(i64, i32)> = (0..=cap).map(|e| (i64::from(e) * 10, e)).collect();
        let cache = cache_of(&observed);
        assert_eq!(cache.entries.len(), CAP);
        assert_eq!(cache.at(0), -1, "the oldest entry is gone");
        assert_eq!(cache.at(10), 1);
    }

    #[test]
    fn a_prune_keeps_the_entry_covering_the_offset() {
        let mut cache = cache_of(&[(10, 1), (20, 3), (30, 5)]);
        cache.prune_below(25);
        assert_eq!(cache.entries, [(20, 3), (30, 5)]);
        cache.prune_below(30);
        assert_eq!(cache.entries, [(30, 5)]);
        cache.prune_below(5);
        assert_eq!(cache.entries, [(30, 5)]);
    }

    #[test]
    fn a_cleared_cache_reads_no_epoch() {
        let mut cache = cache_of(&[(10, 1), (20, 3)]);
        cache.clear();
        assert_eq!(cache.at(25), -1);
    }
}
