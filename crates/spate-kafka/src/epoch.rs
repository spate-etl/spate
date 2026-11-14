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
use std::time::{Duration, Instant};

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
    /// means the broker sent none and is ignored. A record drops the entries
    /// that start at or above it, because the log it came from diverged from
    /// theirs.
    pub(crate) fn observe(&mut self, offset: i64, epoch: i32) {
        if epoch < 0 {
            return;
        }
        while self
            .entries
            .back()
            .is_some_and(|&(start, _)| start >= offset)
        {
            self.entries.pop_back();
        }
        if self.entries.back().is_none_or(|&(_, last)| last != epoch) {
            self.entries.push_back((offset, epoch));
            if self.entries.len() > CAP {
                self.entries.pop_front();
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
    Beginning,
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
            Some(end) if offset > end && above_end_from_log_start => Start::Beginning,
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
            Start::Beginning => {
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
/// epoch to seek to and when its refusals began. Each stays paused until a
/// seek is accepted.
#[derive(Debug)]
pub(crate) struct Validating<K> {
    pending: BTreeMap<K, Pending>,
    /// How long a partition is refused before it is reported. A refusal
    /// while a commit is in flight can last one group-call bound, so a
    /// shorter time would report a normal rebalance.
    report_after: Duration,
}

#[derive(Debug)]
struct Pending {
    offset: i64,
    epoch: i32,
    since: Instant,
    reported: bool,
}

impl<K: Ord + Clone> Validating<K> {
    pub(crate) fn new(report_after: Duration) -> Validating<K> {
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

    /// Records a refused seek of `key` to `offset` at `epoch`, at `now`.
    /// True once, when its refusals have lasted the report time.
    pub(crate) fn refused(&mut self, key: K, offset: i64, epoch: i32, now: Instant) -> bool {
        let pending = self.pending.entry(key).or_insert(Pending {
            offset,
            epoch,
            since: now,
            reported: false,
        });
        if pending.reported || now.saturating_duration_since(pending.since) < self.report_after {
            return false;
        }
        pending.reported = true;
        true
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::testing::mark_errors;
    use rdkafka::types::RDKafkaRespErr;

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

    /// The start each commit takes, for each end and policy.
    #[test]
    fn each_commit_takes_its_start() {
        use Start::{AsIs, Beginning, Validate};
        let at = Offset::Offset;
        for (offset, epoch, end, from_log_start, want) in [
            (at(15), 3, Some(20), true, Validate),
            (at(20), 3, Some(20), true, Validate),
            (at(21), 3, Some(20), true, Beginning),
            (at(21), 3, Some(20), false, Validate),
            (at(21), 3, None, true, Validate),
            (at(15), 0, Some(20), true, Validate),
            (at(15), -1, Some(20), true, AsIs),
            (at(0), 3, Some(20), true, AsIs),
            (Offset::Beginning, 3, Some(20), true, AsIs),
            (Offset::Stored, 3, Some(20), true, AsIs),
        ] {
            assert_eq!(
                plan_start(offset, epoch, end, from_log_start),
                want,
                "{offset:?} at epoch {epoch}, end {end:?}, from_log_start {from_log_start}"
            );
        }
    }

    fn committed(entries: &[(i32, Offset, i32)]) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for &(partition, offset, epoch) in entries {
            tpl.add_partition_offset("t", partition, offset)
                .expect("offset");
            set_epoch(&mut tpl, "t", partition, epoch);
        }
        tpl
    }

    /// Only a commit whose start depends on its end has its end read; a
    /// commit above its end starts from the log start, one within its log is
    /// sought with its epoch, and the rest stay as they are.
    #[test]
    fn the_planned_starts_follow_the_ends() {
        let mut list = committed(&[
            (0, Offset::Offset(15), 3),
            (1, Offset::Offset(40), 3),
            (2, Offset::Offset(15), -1),
            (3, Offset::Beginning, -1),
        ]);
        let to_read = ends_to_read(&list);
        let read: Vec<i32> = to_read.elements().iter().map(|e| e.partition()).collect();
        assert_eq!(read, [0, 1]);
        assert!(to_read.elements().iter().all(|e| e.offset() == Offset::End));

        let mut ends = TopicPartitionList::new();
        ends.add_partition_offset("t", 0, Offset::Offset(20))
            .expect("end");
        ends.add_partition_offset("t", 1, Offset::Offset(20))
            .expect("end");
        let seek = plan_starts(&mut list, Some(&ends), true);

        let starts: Vec<Offset> = list.elements().iter().map(|e| e.offset()).collect();
        assert_eq!(
            starts,
            [
                Offset::Offset(15),
                Offset::Beginning,
                Offset::Offset(15),
                Offset::Beginning
            ]
        );
        let sought: Vec<(i32, Offset)> = seek
            .elements()
            .iter()
            .map(|e| (e.partition(), e.offset()))
            .collect();
        assert_eq!(sought, [(0, Offset::Offset(15))]);
        assert_eq!(epoch_of(&seek, "t", 0), 3);
    }

    /// With the ends unknown, every commit with an epoch is sought.
    #[test]
    fn unknown_ends_seek_every_commit_with_an_epoch() {
        let mut list = committed(&[(0, Offset::Offset(15), 3), (1, Offset::Offset(40), 3)]);
        let seek = plan_starts(&mut list, None, true);
        assert_eq!(seek.count(), 2);
        assert!(
            list.elements()
                .iter()
                .all(|e| matches!(e.offset(), Offset::Offset(_)))
        );
    }

    /// `list` with `code` set on partition `partition`'s element.
    fn answered(
        mut list: TopicPartitionList,
        partition: i32,
        code: RDKafkaRespErr,
    ) -> TopicPartitionList {
        mark_errors(&mut list, |_, p| (p == partition).then_some(code));
        list
    }

    fn seeking(partitions: &[i32]) -> TopicPartitionList {
        let entries: Vec<(i32, Offset, i32)> = partitions
            .iter()
            .map(|&p| (p, Offset::Offset(15), 3))
            .collect();
        committed(&entries)
    }

    /// A seek librdkafka could not start, or did not answer in time, is sent
    /// again; any other failure is logged and not retried.
    #[test]
    fn each_seek_answer_is_classed() {
        for code in [
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__STATE,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__IN_PROGRESS,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS,
        ] {
            let again = seek_starts(seeking(&[0, 1]), Duration::from_secs(1), |list, _| {
                Ok(answered(list, 1, code))
            });
            assert_eq!(again, [("t".to_owned(), 1)], "{code:?}");
        }
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            let again = seek_starts(seeking(&[0, 1]), Duration::from_secs(1), |list, _| {
                Ok(answered(
                    list,
                    1,
                    RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
                ))
            });
            assert_eq!(again, []);
        });
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert!(lines[0].contains("partition=1"), "{}", lines[0]);
        assert!(lines[0].contains("reads from it unchecked"), "{}", lines[0]);
    }

    /// A seek the call as a whole failed is sent again for every partition.
    #[test]
    fn a_failed_seek_call_sends_every_seek_again() {
        let again = seek_starts(seeking(&[0, 1]), Duration::from_secs(1), |_, _| {
            Err(rdkafka::error::KafkaError::Seek("timed out".to_owned()))
        });
        assert_eq!(again, [("t".to_owned(), 0), ("t".to_owned(), 1)]);
    }

    /// With no time left no seek is sent, and every partition waits for the
    /// next one; an empty list sends nothing either.
    #[test]
    fn a_spent_budget_sends_no_seek() {
        let again = seek_starts(seeking(&[0]), Duration::ZERO, |_, _| {
            panic!("a seek with no time left")
        });
        assert_eq!(again, [("t".to_owned(), 0)]);
        let again = seek_starts(TopicPartitionList::new(), Duration::from_secs(1), |_, _| {
            panic!("a seek of nothing")
        });
        assert_eq!(again, []);
    }

    /// The seek gets the budget it was given.
    #[test]
    fn the_seek_gets_its_budget() {
        let mut got = None;
        let _ = seek_starts(seeking(&[0]), Duration::from_millis(250), |list, budget| {
            got = Some(budget);
            Ok(list)
        });
        assert_eq!(got, Some(Duration::from_millis(250)));
    }

    /// A partition is reported once, when its refusals have lasted the
    /// report time, and again only after it left and came back.
    #[test]
    fn a_refused_partition_is_reported_once_at_the_threshold() {
        let start = Instant::now();
        let at = |ms| start + Duration::from_millis(ms);
        let mut v = Validating::new(Duration::from_secs(3));
        assert!(!v.refused(0, 15, 3, at(0)));
        assert!(!v.refused(0, 15, 3, at(2_999)));
        assert!(v.refused(0, 15, 3, at(3_000)));
        assert!(!v.refused(0, 15, 3, at(9_000)));
        assert_eq!(v.entries().collect::<Vec<_>>(), [(&0, 15, 3)]);
        assert!(v.remove(&0));
        assert!(!v.remove(&0));
        assert!(v.is_empty());
        assert!(!v.refused(0, 15, 3, at(9_000)));
        assert!(!v.refused(0, 15, 3, at(11_999)));
        v.refused(1, 9, 2, at(0));
        v.retain(|p| *p == 1);
        assert!(!v.contains(&0));
        assert_eq!(v.take_all(), [1]);
        assert!(v.is_empty());
    }
}
