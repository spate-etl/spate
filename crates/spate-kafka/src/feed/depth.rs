//! The feed's depth reads: the lag of the feed's group on the work and retry
//! topics, for a bounded job's end check, and the sum of the dead-letter
//! topic's end offsets, for the end check and the runtime's dead-letter reads.
//!
//! The reads run on a thread of the feed's own, through a consumer that
//! carries the feed's `group.id` and never subscribes, so no read runs on
//! the group member's poll thread and none counts against its poll
//! interval (INV-2). [`Offsets`] is the seam the unit tests script.
//!
//! That consumer reads at `read_uncommitted`, so an end offset is the high
//! watermark and counts a confirmed write behind another writer's open
//! transaction. At `read_committed` it would read the last stable offset, and
//! an open transaction would hide every offset from its first record on. An
//! end offset counts offsets: a transaction marker, an aborted record and a
//! record behind an open transaction each read as one.

use super::{Core, LatchOnPanic, Topics};
use crate::client::list_offsets;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::{Offset, TopicPartitionList};
use spate_core::feed::{Depth, DepthOf, DepthReporter, FeedError, FeedErrorKind};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

/// The bound on each metadata and offset request.
const REQUEST_BOUND: Duration = Duration::from_secs(5);

/// How many depth reads with the group behind and its commits unchanged pass
/// before [`Hold`] says why. About five minutes at one read per 15 s. The
/// figure has no measured basis.
const HOLD_READS: u32 = 20;

/// How long the depth thread waits for a request before it serves its
/// client's queued events.
const SERVE_EVERY: Duration = Duration::from_secs(1);

/// One partition of one of the feed's topics.
pub(crate) type Partition = (Arc<str>, i32);

/// The offset reads a depth read takes.
pub(crate) trait Offsets {
    /// The partitions of `topic`.
    fn partitions(&self, topic: &Arc<str>) -> Result<Vec<i32>, FeedError>;

    /// The group's committed offset on each of `partitions`, in their order,
    /// read in one request.
    fn committed(&self, partitions: &[Partition]) -> Result<Vec<Offset>, FeedError>;

    /// The log start offset of each of `partitions` for
    /// [`Offset::Beginning`], or the high watermark for [`Offset::End`], in
    /// their order, read in one request.
    fn listed(&self, partitions: &[Partition], at: Offset) -> Result<Vec<i64>, FeedError>;

    /// Serve the client's queued statistics, log lines and errors.
    fn serve(&self) {}
}

/// How long the group's commits have stood still with offsets above them, so
/// a job waiting on an open transaction says so. It counts commits and leaves
/// the lag out, because a live writer behind an open transaction raises the end
/// on every read.
#[derive(Debug, Default)]
pub(crate) struct Hold {
    committed: Vec<Offset>,
    reads: u32,
}

/// The first partition a depth read found behind.
#[derive(Debug)]
struct Behind<'a> {
    topic: &'a str,
    partition: i32,
    end: i64,
    committed: i64,
}

impl Hold {
    /// Count one depth read of `committed`, with `behind` the partitions it
    /// found behind, the first of them named. Logs once per stretch of
    /// [`HOLD_READS`] reads with the same commits.
    fn read(&mut self, committed: &[Offset], first: Option<Behind<'_>>, behind: usize) {
        let Some(first) = first else {
            self.reads = 0;
            return;
        };
        if self.committed == committed {
            self.reads += 1;
        } else {
            self.committed = committed.to_vec();
            self.reads = 1;
        }
        if self.reads == HOLD_READS {
            tracing::info!(
                topic = first.topic,
                partition = first.partition,
                end = first.end,
                committed = first.committed,
                others = behind - 1,
                "the feed's group is behind and its commits have not moved: an open \
                 transaction, a unit a peer holds or a unit at this worker's gate keeps the \
                 job open"
            );
        }
    }
}

/// The depth of the work and retry topics: each partition's high watermark less
/// the group's committed offset, as visible units. It counts offsets above the
/// commit, whether or not a record is there to read.
///
/// The committed offsets are read before any end offset. A unit failed
/// between the two reads has its retry write confirmed before its work
/// commit moves, so it shows on the retry topic's end offset either way.
/// A partition with no commit, or a commit below its log start, reads from
/// its log start: units retention deleted before they were read leave no
/// trace (INV-1's statement places them outside the property).
pub(crate) fn feed_depth(
    offsets: &dyn Offsets,
    topics: &Topics,
    hold: &mut Hold,
) -> Result<Depth, FeedError> {
    let mut partitions = Vec::new();
    for topic in [&topics.work, &topics.retry] {
        for p in offsets.partitions(topic)? {
            partitions.push((Arc::clone(topic), p));
        }
    }
    let committed = offsets.committed(&partitions)?;
    let starts = offsets.listed(&partitions, Offset::Beginning)?;
    let ends = offsets.listed(&partitions, Offset::End)?;
    let mut lag: u64 = 0;
    let mut first = None;
    let mut behind_count = 0;
    for (i, (topic, p)) in partitions.iter().enumerate() {
        let start = starts[i];
        let read_from = match committed[i] {
            Offset::Offset(c) => c.max(start),
            Offset::Invalid => start,
            other => {
                return Err(FeedError::new(
                    FeedErrorKind::Retryable,
                    format!("`{topic}` partition {p} reports its committed offset as {other:?}"),
                ));
            }
        };
        let behind = u64::try_from(ends[i].saturating_sub(read_from)).unwrap_or(0);
        lag = lag.saturating_add(behind);
        if behind > 0 {
            behind_count += 1;
            first.get_or_insert(Behind {
                topic,
                partition: *p,
                end: ends[i],
                committed: read_from,
            });
        }
    }
    hold.read(&committed, first, behind_count);
    let mut depth = Depth::new(
        format!(
            "the group's lag on `{}` and `{}`",
            topics.work, topics.retry
        ),
        false,
    );
    depth.visible = lag;
    Ok(depth)
}

/// The depth of the dead-letter topic: the sum of its partitions' end
/// offsets. Each job writes a dead-letter topic of its own, so any record
/// ever written to it counts, whether or not retention has deleted it since,
/// and so does every other offset: a marker, an aborted record or a record
/// behind an open transaction.
pub(crate) fn dead_letter_depth(
    offsets: &dyn Offsets,
    topics: &Topics,
) -> Result<Depth, FeedError> {
    let partitions: Vec<Partition> = offsets
        .partitions(&topics.dead_letter)?
        .into_iter()
        .map(|p| (Arc::clone(&topics.dead_letter), p))
        .collect();
    let ends = offsets.listed(&partitions, Offset::End)?;
    let written = ends
        .iter()
        .map(|e| u64::try_from(*e).unwrap_or(0))
        .fold(0u64, u64::saturating_add);
    let mut depth = Depth::new(
        format!(
            "the dead-letter topic `{}`, counted by end offset",
            topics.dead_letter
        ),
        false,
    );
    depth.visible = written;
    Ok(depth)
}

/// [`Offsets`] that refuses a request once the feed has stopped, so a read
/// in flight ends after the request it is on and the feed's drop waits for
/// one request at most.
struct UntilStopped<'a> {
    offsets: &'a dyn Offsets,
    core: &'a Core,
}

impl UntilStopped<'_> {
    fn check(&self) -> Result<(), FeedError> {
        if self.core.stopped() {
            return Err(FeedError::new(
                FeedErrorKind::Retryable,
                "the feed stopped during the read",
            ));
        }
        Ok(())
    }
}

impl Offsets for UntilStopped<'_> {
    fn partitions(&self, topic: &Arc<str>) -> Result<Vec<i32>, FeedError> {
        self.check()?;
        self.offsets.partitions(topic)
    }

    fn committed(&self, partitions: &[Partition]) -> Result<Vec<Offset>, FeedError> {
        self.check()?;
        self.offsets.committed(partitions)
    }

    fn listed(&self, partitions: &[Partition], at: Offset) -> Result<Vec<i64>, FeedError> {
        self.check()?;
        self.offsets.listed(partitions, at)
    }

    fn serve(&self) {
        self.offsets.serve();
    }
}

/// Answer each depth request from `requests` until the feed drops its
/// sender or stops.
pub(crate) fn run(
    core: &Arc<Core>,
    offsets: &dyn Offsets,
    requests: &Receiver<(DepthOf, DepthReporter)>,
) {
    let _guard = LatchOnPanic {
        core: Arc::clone(core),
        thread: "depth",
    };
    let mut hold = Hold::default();
    let offsets: &dyn Offsets = &UntilStopped { offsets, core };
    loop {
        let (of, reporter) = match requests.recv_timeout(SERVE_EVERY) {
            Ok(request) => request,
            Err(RecvTimeoutError::Timeout) => {
                offsets.serve();
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => return,
        };
        if core.stopped() {
            return;
        }
        let read = match of {
            DepthOf::DeadLetters => dead_letter_depth(offsets, core.topics()),
            _ => feed_depth(offsets, core.topics(), &mut hold),
        };
        // A credential or TLS rejection latches the feed, and the depth read
        // then reports it, whatever the read itself returned.
        let read = match core.lock().signal.latched().cloned() {
            Some(fatal) => Err(fatal),
            None => read,
        };
        reporter.report(read);
        // The source checks a reading only when it polls, so the answer
        // ends its park.
        core.lock().signal.wake();
    }
}

/// The class of a failed offset read: an authorization failure cannot clear
/// on its own, and anything else may.
fn class(e: &KafkaError) -> FeedErrorKind {
    match e.rdkafka_error_code() {
        Some(
            RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed,
        ) => FeedErrorKind::Fatal,
        _ => FeedErrorKind::Retryable,
    }
}

fn error(doing: &str, e: &KafkaError) -> FeedError {
    FeedError::new(class(e), format!("{doing}: {e}"))
}

/// [`Offsets`] through a consumer with the feed's `group.id` that never
/// subscribes.
pub(crate) struct Brokers {
    pub(crate) consumer: BaseConsumer<crate::context::SourceContext>,
}

impl Brokers {
    fn list(&self, partitions: &[Partition], at: Offset) -> Result<TopicPartitionList, FeedError> {
        let mut tpl = TopicPartitionList::new();
        for (topic, p) in partitions {
            tpl.add_partition_offset(topic, *p, at)
                .map_err(|e| error("building an offset request", &e))?;
        }
        Ok(tpl)
    }
}

/// The distinct topics of `partitions`, quoted, for an error's text.
fn named(partitions: &[Partition]) -> String {
    let mut topics: Vec<&str> = Vec::new();
    for (topic, _) in partitions {
        if !topics.contains(&&**topic) {
            topics.push(topic);
        }
    }
    topics
        .iter()
        .map(|t| format!("`{t}`"))
        .collect::<Vec<_>>()
        .join(" and ")
}

/// Each element of `read` for `partitions`, in their order, or the first
/// element's error.
fn elements(
    read: &TopicPartitionList,
    partitions: &[Partition],
    doing: &str,
) -> Result<Vec<Offset>, FeedError> {
    partitions
        .iter()
        .map(|(topic, p)| {
            let element = read.find_partition(topic, *p).ok_or_else(|| {
                FeedError::new(
                    FeedErrorKind::Retryable,
                    format!("{doing}: no answer for `{topic}` partition {p}"),
                )
            })?;
            element
                .error()
                .map_err(|e| error(&format!("{doing} on `{topic}` partition {p}"), &e))?;
            Ok(element.offset())
        })
        .collect()
}

impl Offsets for Brokers {
    fn serve(&self) {
        // The consumer carries a `group.id`, so librdkafka queues its
        // statistics, log lines and errors where only a poll drains them.
        let _ = self.consumer.poll(Duration::from_millis(50));
    }

    fn partitions(&self, topic: &Arc<str>) -> Result<Vec<i32>, FeedError> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(topic), REQUEST_BOUND)
            .map_err(|e| error(&format!("reading the partitions of `{topic}`"), &e))?;
        let found: Vec<_> = metadata
            .topics()
            .iter()
            .filter(|t| t.name() == &**topic)
            .collect();
        // A refusal arrives as the topic's own error in an answered request.
        if let Some(err) = found.iter().find_map(|t| t.error()) {
            return Err(error(
                &format!("reading the partitions of `{topic}`"),
                &KafkaError::MetadataFetch(RDKafkaErrorCode::from(err)),
            ));
        }
        let partitions: Vec<i32> = found
            .iter()
            .flat_map(|t| t.partitions().iter().map(|p| p.id()))
            .collect();
        if partitions.is_empty() {
            return Err(FeedError::new(
                FeedErrorKind::Retryable,
                format!("`{topic}` reports no partitions"),
            ));
        }
        Ok(partitions)
    }

    fn committed(&self, partitions: &[Partition]) -> Result<Vec<Offset>, FeedError> {
        let tpl = self.list(partitions, Offset::Invalid)?;
        let read = self
            .consumer
            .committed_offsets(tpl, REQUEST_BOUND)
            .map_err(|e| {
                error(
                    &format!(
                        "reading the group's committed offsets on {}",
                        named(partitions)
                    ),
                    &e,
                )
            })?;
        elements(&read, partitions, "reading the group's committed offset")
    }

    fn listed(&self, partitions: &[Partition], at: Offset) -> Result<Vec<i64>, FeedError> {
        let mut read = self.list(partitions, at)?;
        let doing = match at {
            Offset::Beginning => "reading log start offsets",
            _ => "reading end offsets",
        };
        list_offsets(self.consumer.client(), &mut read, REQUEST_BOUND)
            .map_err(|e| error(&format!("{doing} of {}", named(partitions)), &e))?;
        elements(&read, partitions, doing)?
            .into_iter()
            .zip(partitions)
            .map(|(offset, (topic, p))| match offset {
                Offset::Offset(o) => Ok(o),
                other => Err(FeedError::new(
                    FeedErrorKind::Retryable,
                    format!("{doing}: `{topic}` partition {p} answered {other:?}"),
                )),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SpateContext;
    use crate::client::testing::raw;
    use crate::context::SourceHooks;
    use crate::feed::KafkaFeed;
    use crate::feed::tests::core;
    use spate_core::hash::FastMap;
    use spate_test::HANG;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    /// Offsets per partition, scripted, and every call in order.
    #[derive(Default)]
    struct Scripted {
        partitions: FastMap<String, Vec<i32>>,
        committed: FastMap<(String, i32), Offset>,
        starts: FastMap<(String, i32), i64>,
        ends: FastMap<(String, i32), i64>,
        fail_committed: Option<FeedError>,
        fail_listed: Option<FeedError>,
        calls: Mutex<Vec<String>>,
    }

    impl Scripted {
        fn topic(mut self, topic: &str, partitions: &[(i32, Offset, i64, i64)]) -> Scripted {
            self.partitions.insert(
                topic.to_owned(),
                partitions.iter().map(|(p, ..)| *p).collect(),
            );
            for (p, committed, start, end) in partitions {
                let key = (topic.to_owned(), *p);
                self.committed.insert(key.clone(), *committed);
                self.starts.insert(key.clone(), *start);
                self.ends.insert(key, *end);
            }
            self
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl Offsets for Scripted {
        fn partitions(&self, topic: &Arc<str>) -> Result<Vec<i32>, FeedError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("partitions {topic}"));
            Ok(self.partitions.get(&**topic).cloned().unwrap_or_default())
        }

        fn committed(&self, partitions: &[Partition]) -> Result<Vec<Offset>, FeedError> {
            self.calls.lock().unwrap().push("committed".to_owned());
            if let Some(e) = &self.fail_committed {
                return Err(e.clone());
            }
            Ok(partitions
                .iter()
                .map(|(t, p)| self.committed[&(t.to_string(), *p)])
                .collect())
        }

        fn listed(&self, partitions: &[Partition], at: Offset) -> Result<Vec<i64>, FeedError> {
            self.calls.lock().unwrap().push(format!("listed {at:?}"));
            if let Some(e) = &self.fail_listed {
                return Err(e.clone());
            }
            let table = match at {
                Offset::Beginning => &self.starts,
                _ => &self.ends,
            };
            Ok(partitions
                .iter()
                .map(|(t, p)| table[&(t.to_string(), *p)])
                .collect())
        }
    }

    fn topics() -> Topics {
        Topics {
            work: Arc::from("work"),
            retry: Arc::from("retry"),
            dead_letter: Arc::from("dead"),
            attempt: Arc::from("attempts"),
        }
    }

    /// Counts the times `run` serves the client.
    #[derive(Default)]
    struct Serving(AtomicUsize);

    impl Offsets for Serving {
        fn partitions(&self, _: &Arc<str>) -> Result<Vec<i32>, FeedError> {
            Ok(Vec::new())
        }
        fn committed(&self, _: &[Partition]) -> Result<Vec<Offset>, FeedError> {
            Ok(Vec::new())
        }
        fn listed(&self, _: &[Partition], _: Offset) -> Result<Vec<i64>, FeedError> {
            Ok(Vec::new())
        }
        fn serve(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Pins that `run` serves its client while it waits for a request, so
    /// the client's queued statistics, log lines and errors do not pile up
    /// over a job.
    #[test]
    fn the_depth_thread_serves_its_client_between_requests() {
        let core = Arc::new(core(3));
        let serving = Serving::default();
        let (requests, received) = mpsc::channel::<(DepthOf, DepthReporter)>();
        std::thread::scope(|s| {
            let (core, serving) = (&core, &serving);
            s.spawn(move || run(core, serving, &received));
            spate_test::wait_until(HANG, "the client is served", || {
                serving.0.load(Ordering::SeqCst) > 0
            });
            drop(requests);
        });
    }

    /// Pins that `run` wakes the reader once it has answered a depth read,
    /// so the reader's park ends on the answer.
    #[test]
    fn an_answered_depth_read_wakes_the_reader() {
        let core = Arc::new(core(3));
        let (waker, park) = spate_core::feed::wake_channel();
        core.lock().signal.set_waker(waker, false);
        let offsets = Scripted::default().topic("units-dead", &[(0, Offset::Invalid, 0, 0)]);
        let (requests, received) = mpsc::channel();
        let (reporter, mut reading) = spate_core::feed::depth_reading();
        requests.send((DepthOf::DeadLetters, reporter)).unwrap();
        drop(requests);
        run(&core, &offsets, &received);
        assert!(reading.try_outcome().unwrap().unwrap().is_empty());
        assert!(
            park.park(std::time::Duration::ZERO),
            "the answer did not wake the reader"
        );
    }

    /// Pins when a held job says why: on the twentieth read with the group
    /// behind and its commits unchanged, once, and again only after a commit
    /// moves. A rising end does not restart the count, and a read with the
    /// group caught up does, whether or not its commits moved.
    #[test]
    fn a_job_held_still_logs_once_per_twenty_reads() {
        let read = |committed: i64, start: i64, end: i64, hold: &mut Hold| {
            let offsets = Scripted::default()
                .topic("work", &[(0, Offset::Offset(committed), start, end)])
                .topic("retry", &[(0, Offset::Invalid, 0, 0)]);
            feed_depth(&offsets, &topics(), hold).unwrap();
        };
        let held = |reads: &mut dyn FnMut()| {
            spate_test::capture_logs(tracing::Level::INFO, reads)
                .into_iter()
                .filter(|line| line.contains("keeps the job open"))
                .collect::<Vec<_>>()
        };
        let mut hold = Hold::default();
        let mut before = || {
            for reads in 1..=19 {
                read(3, 0, 3 + reads, &mut hold);
            }
        };
        assert_eq!(held(&mut before).len(), 0, "before the twentieth");
        let twentieth = held(&mut || read(3, 0, 30, &mut hold));
        assert_eq!(twentieth.len(), 1, "on the twentieth");
        for field in [
            "topic=\"work\"",
            "partition=0",
            "end=30",
            "committed=3",
            "others=0",
        ] {
            assert!(twentieth[0].contains(field), "{field} in {}", twentieth[0]);
        }
        assert_eq!(
            held(&mut || read(3, 0, 31, &mut hold)).len(),
            0,
            "only once"
        );
        let mut moved = || {
            for _ in 0..20 {
                read(4, 0, 31, &mut hold);
            }
        };
        assert_eq!(held(&mut moved).len(), 1, "after a moved commit");
        let mut caught_up = || {
            for _ in 0..40 {
                read(31, 0, 31, &mut hold);
            }
        };
        assert_eq!(held(&mut caught_up).len(), 0, "with the group caught up");

        // The log start reaches the end with the commit unmoved, so only the
        // caught-up read restarts the count.
        let mut hold = Hold::default();
        let mut restarted = || {
            for _ in 0..10 {
                read(3, 0, 9, &mut hold);
            }
            read(3, 9, 9, &mut hold);
            for _ in 0..19 {
                read(3, 0, 9, &mut hold);
            }
        };
        assert_eq!(
            held(&mut restarted).len(),
            0,
            "restarted by a caught-up read"
        );
    }

    /// Pins that the depth thread keeps its count across requests, so a held
    /// job's line reaches the log.
    #[test]
    fn the_depth_thread_counts_a_hold_across_requests() {
        let core = Arc::new(core(3));
        let offsets = Scripted::default()
            .topic("units", &[(0, Offset::Offset(3), 0, 9)])
            .topic("units-retry", &[(0, Offset::Invalid, 0, 0)]);
        let (requests, received) = mpsc::channel();
        let mut readings = Vec::new();
        for _ in 0..HOLD_READS {
            let (reporter, reading) = spate_core::feed::depth_reading();
            requests.send((DepthOf::Feed, reporter)).unwrap();
            readings.push(reading);
        }
        drop(requests);
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            run(&core, &offsets, &received);
        });
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("keeps the job open"), "{}", lines[0]);
    }

    /// Pins that a held job's line counts the other partitions behind.
    #[test]
    fn a_held_job_names_how_many_others_are_behind() {
        let offsets = Scripted::default()
            .topic(
                "work",
                &[(0, Offset::Offset(3), 0, 9), (1, Offset::Offset(1), 0, 2)],
            )
            .topic("retry", &[(0, Offset::Invalid, 0, 0)]);
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            let mut hold = Hold::default();
            for _ in 0..HOLD_READS {
                feed_depth(&offsets, &topics(), &mut hold).unwrap();
            }
        });
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("others=1"), "{}", lines[0]);
    }

    /// Pins which partition a held job's line names: the first one behind,
    /// past a caught-up partition, with its own end and commit.
    #[test]
    fn a_held_job_names_the_first_partition_behind() {
        let offsets = Scripted::default()
            .topic(
                "work",
                &[
                    (0, Offset::Offset(9), 0, 9),
                    (1, Offset::Offset(3), 0, 7),
                    (2, Offset::Offset(1), 0, 2),
                ],
            )
            .topic("retry", &[(0, Offset::Invalid, 0, 0)]);
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            let mut hold = Hold::default();
            for _ in 0..HOLD_READS {
                feed_depth(&offsets, &topics(), &mut hold).unwrap();
            }
        });
        assert_eq!(lines.len(), 1, "{lines:?}");
        for field in ["partition=1", "end=7", "committed=3", "others=1"] {
            assert!(lines[0].contains(field), "{field} in {}", lines[0]);
        }
    }

    /// Pins the read order: the partitions of the work and retry topics,
    /// then every committed offset in one request, before any log start or
    /// end offset. Reading ends first would let a unit move from the work
    /// topic to the retry topic between the two reads unseen.
    #[test]
    fn the_end_check_reads_committed_offsets_before_end_offsets() {
        let offsets = Scripted::default()
            .topic(
                "work",
                &[(0, Offset::Offset(3), 0, 3), (1, Offset::Offset(2), 0, 2)],
            )
            .topic("retry", &[(0, Offset::Invalid, 0, 0)]);
        let depth = feed_depth(&offsets, &topics(), &mut Hold::default()).unwrap();
        assert!(depth.is_empty(), "{depth}");
        assert_eq!(
            offsets.calls(),
            vec![
                "partitions work",
                "partitions retry",
                "committed",
                "listed Beginning",
                "listed End"
            ]
        );
    }

    /// Pins the lag per partition, summed over the work and retry topics:
    /// lag on one partition only, on the retry topic only, and of exactly one.
    #[test]
    fn the_lag_is_summed_over_every_work_and_retry_partition() {
        let lag = |work1: i64, retry: i64| {
            let offsets = Scripted::default()
                .topic(
                    "work",
                    &[
                        (0, Offset::Offset(4), 0, 4),
                        (1, Offset::Offset(2), 0, 2 + work1),
                    ],
                )
                .topic("retry", &[(0, Offset::Offset(1), 0, 1 + retry)]);
            feed_depth(&offsets, &topics(), &mut Hold::default())
                .unwrap()
                .visible
        };
        assert_eq!(lag(0, 0), 0);
        assert_eq!(lag(1, 0), 1);
        assert_eq!(lag(0, 1), 1);
        assert_eq!(lag(3, 2), 5);
    }

    /// Pins each committed-offset form: no commit reads from the log start,
    /// a commit at the end is drained, one short of it lags by one, a commit
    /// below the log start reads from the log start, and a stored or end
    /// marker fails the read.
    #[test]
    fn a_partition_drained_covers_every_offset_variant() {
        let read = |committed: Offset, start: i64, end: i64| {
            let offsets = Scripted::default()
                .topic("work", &[(0, committed, start, end)])
                .topic("retry", &[(0, Offset::Invalid, 0, 0)]);
            feed_depth(&offsets, &topics(), &mut Hold::default())
        };
        assert_eq!(read(Offset::Invalid, 0, 0).unwrap().visible, 0);
        assert_eq!(read(Offset::Invalid, 3, 5).unwrap().visible, 2);
        assert_eq!(read(Offset::Offset(5), 0, 5).unwrap().visible, 0);
        assert_eq!(read(Offset::Offset(4), 0, 5).unwrap().visible, 1);
        assert_eq!(read(Offset::Offset(2), 4, 5).unwrap().visible, 1);
        for other in [Offset::Stored, Offset::End, Offset::Beginning] {
            let e = read(other, 0, 5).unwrap_err();
            assert_eq!(e.kind, FeedErrorKind::Retryable, "{other:?}");
        }
    }

    /// Pins that a partition with no commit whose records retention deleted
    /// reads as drained: its log start has reached its end.
    #[test]
    fn a_partition_with_no_commit_reads_as_its_log_start() {
        let offsets = Scripted::default()
            .topic("work", &[(0, Offset::Invalid, 7, 7)])
            .topic("retry", &[(0, Offset::Invalid, 0, 0)]);
        assert!(
            feed_depth(&offsets, &topics(), &mut Hold::default())
                .unwrap()
                .is_empty()
        );
    }

    /// Pins that a failed committed or end read fails the depth read with
    /// its own class.
    #[test]
    fn an_element_error_fails_the_read() {
        let base = || {
            Scripted::default()
                .topic("work", &[(0, Offset::Offset(1), 0, 1)])
                .topic("retry", &[(0, Offset::Invalid, 0, 0)])
        };
        let mut offsets = base();
        offsets.fail_committed = Some(FeedError::new(FeedErrorKind::Fatal, "denied"));
        assert_eq!(
            feed_depth(&offsets, &topics(), &mut Hold::default())
                .unwrap_err()
                .kind,
            FeedErrorKind::Fatal
        );
        let mut offsets = base();
        offsets.fail_listed = Some(FeedError::new(FeedErrorKind::Retryable, "timed out"));
        assert_eq!(
            feed_depth(&offsets, &topics(), &mut Hold::default())
                .unwrap_err()
                .kind,
            FeedErrorKind::Retryable
        );
        assert_eq!(
            dead_letter_depth(&offsets, &topics()).unwrap_err().kind,
            FeedErrorKind::Retryable
        );
    }

    /// Pins the dead-letter depth: the sum of the topic's end offsets, so any
    /// offset ever written there counts whatever its log start.
    #[test]
    fn a_dead_letter_end_offset_above_zero_reads_as_not_empty() {
        let read = |ends: &[(i32, i64, i64)]| {
            let partitions: Vec<_> = ends
                .iter()
                .map(|(p, start, end)| (*p, Offset::Invalid, *start, *end))
                .collect();
            dead_letter_depth(&Scripted::default().topic("dead", &partitions), &topics()).unwrap()
        };
        assert!(read(&[(0, 0, 0), (1, 0, 0)]).is_empty());
        assert_eq!(read(&[(0, 0, 0), (1, 0, 1)]).visible, 1);
        assert!(
            read(&[(0, 0, 1)]).what.contains("counted by end offset"),
            "the stalled error names what it counted"
        );
        assert_eq!(
            read(&[(0, 1, 1), (1, 2, 3)]).visible,
            4,
            "retention deleted them"
        );
    }

    /// Pins that an error the broker answers for one partition of a
    /// committed-offset read fails the read, naming the partition.
    #[test]
    fn a_partition_error_in_a_committed_read_fails_it() {
        use crate::testutil::mock::MockKafka;
        let cluster = MockKafka::new();
        cluster.create_topic("work", 1, 1).expect("create topic");
        let consumer = raw(
            &[
                ("bootstrap.servers", &cluster.bootstrap_servers()),
                ("group.id", "depth-partition-error"),
            ],
            SourceHooks::default(),
        );
        let reads = Brokers { consumer };
        let e = reads.committed(&[(Arc::from("work"), 9)]).unwrap_err();
        assert!(e.reason.contains("`work` partition 9"), "{e}");
    }

    /// Pins that serving the depth client hands its queued events to the
    /// context: a statistics event, queued every 10 ms here, reaches it only
    /// through a serve.
    #[test]
    fn serving_the_depth_client_drains_its_events() {
        use crate::testutil::mock::MockKafka;
        use rdkafka::consumer::Consumer as _;
        let cluster = MockKafka::new();
        let consumer = raw(
            &[
                ("bootstrap.servers", &cluster.bootstrap_servers()),
                ("group.id", "depth-serve"),
                ("statistics.interval.ms", "10"),
            ],
            SourceHooks::default(),
        );
        let reads = Brokers { consumer };
        spate_test::wait_until(HANG, "a statistics event reaches the context", || {
            reads.serve();
            reads.consumer.context().stats.lock().unwrap().is_some()
        });
    }

    /// Pins the topics a whole-request failure names: each once, in the
    /// order the request lists them.
    #[test]
    fn a_request_names_each_of_its_topics_once() {
        let (work, retry): (Arc<str>, Arc<str>) = (Arc::from("units"), Arc::from("units-retry"));
        let partitions = [
            (Arc::clone(&work), 0),
            (Arc::clone(&retry), 0),
            (Arc::clone(&work), 1),
        ];
        assert_eq!(named(&partitions), "`units` and `units-retry`");
        assert_eq!(named(&partitions[..1]), "`units`");
    }

    /// Pins that a topic whose metadata lists no partitions, as an unknown
    /// topic's does, fails the read as Retryable, so it never reads as
    /// drained.
    #[test]
    fn a_topic_reporting_no_partitions_fails_the_read() {
        use crate::testutil::mock::MockKafka;
        use rdkafka::types::RDKafkaRespErr;
        let cluster = MockKafka::new();
        cluster.create_topic("dead", 1, 1).expect("create topic");
        cluster
            .topic_error(
                "dead",
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
            )
            .unwrap();
        let consumer = raw(
            &[
                ("bootstrap.servers", &cluster.bootstrap_servers()),
                ("group.id", "depth-unknown"),
            ],
            SourceHooks::default(),
        );
        let reads = Brokers { consumer };
        let e = reads.partitions(&Arc::from("dead")).unwrap_err();
        assert_eq!(e.kind, FeedErrorKind::Retryable, "{e}");
        assert!(e.reason.contains("`dead`"), "{e}");
    }

    /// A panic on the depth thread latches the feed, so the run fails where
    /// it would otherwise read a depth nothing answers. Regression for #1760.
    #[test]
    #[expect(clippy::disallowed_methods, reason = "the test joins the thread")]
    fn a_panic_on_the_depth_thread_latches_the_feed() {
        struct Panics;
        impl Offsets for Panics {
            fn partitions(&self, _: &Arc<str>) -> Result<Vec<i32>, FeedError> {
                panic!("scripted")
            }
            fn committed(&self, _: &[Partition]) -> Result<Vec<Offset>, FeedError> {
                unreachable!()
            }
            fn listed(&self, _: &[Partition], _: Offset) -> Result<Vec<i64>, FeedError> {
                unreachable!()
            }
        }
        let core = Arc::new(core(3));
        let (requests, received) = mpsc::channel();
        let (reporter, _reading) = spate_core::feed::depth_reading();
        requests.send((DepthOf::DeadLetters, reporter)).unwrap();
        let thread = {
            let core = Arc::clone(&core);
            std::thread::spawn(move || run(&core, &Panics, &received))
        };
        assert!(thread.join().is_err(), "the read panics");
        let latched = core.lock().signal.latched().cloned().expect("latched");
        assert_eq!(latched.kind, FeedErrorKind::Fatal);
        assert_eq!(latched.reason, "the Kafka feed's depth thread panicked");
    }

    /// Pins that the depth thread reads nothing once the feed has stopped,
    /// and the reading it drops settles Retryable.
    #[test]
    fn a_stopped_feed_reads_no_depth() {
        let core = Arc::new(core(3));
        core.stop.store(true, Ordering::Release);
        let offsets = Scripted::default().topic("units-dead", &[(0, Offset::Invalid, 0, 0)]);
        let (requests, received) = mpsc::channel();
        let (reporter, mut reading) = spate_core::feed::depth_reading();
        requests.send((DepthOf::DeadLetters, reporter)).unwrap();
        drop(requests);
        run(&core, &offsets, &received);
        assert!(offsets.calls().is_empty(), "{:?}", offsets.calls());
        let e = reading.try_outcome().unwrap().unwrap_err();
        assert_eq!(e.kind, FeedErrorKind::Retryable);
    }

    /// Pins that a feed stopped during any request of a read, of the feed or
    /// of the dead letters, sends none of the requests after it. The read
    /// settles Retryable, and the feed's drop waits for one request at most.
    /// Regression for #1761.
    #[test]
    fn a_feed_stopped_at_any_request_of_a_read_sends_no_further_request() {
        struct StopsAt {
            scripted: Scripted,
            core: Arc<Core>,
            at: usize,
            seen: AtomicUsize,
        }
        impl StopsAt {
            fn tick(&self) {
                if self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.at {
                    self.core.stop.store(true, Ordering::Release);
                }
            }
        }
        impl Offsets for StopsAt {
            fn partitions(&self, topic: &Arc<str>) -> Result<Vec<i32>, FeedError> {
                self.tick();
                self.scripted.partitions(topic)
            }
            fn committed(&self, partitions: &[Partition]) -> Result<Vec<Offset>, FeedError> {
                self.tick();
                self.scripted.committed(partitions)
            }
            fn listed(&self, partitions: &[Partition], at: Offset) -> Result<Vec<i64>, FeedError> {
                self.tick();
                self.scripted.listed(partitions, at)
            }
        }
        for (of, requests_in_a_read) in [(DepthOf::Feed, 5), (DepthOf::DeadLetters, 2)] {
            for at in 1..requests_in_a_read {
                let core = Arc::new(core(3));
                let t = core.topics();
                let offsets = StopsAt {
                    scripted: Scripted::default()
                        .topic(&t.work, &[(0, Offset::Invalid, 0, 0)])
                        .topic(&t.retry, &[(0, Offset::Invalid, 0, 0)])
                        .topic(&t.dead_letter, &[(0, Offset::Invalid, 0, 0)]),
                    core: Arc::clone(&core),
                    at,
                    seen: AtomicUsize::new(0),
                };
                let (requests, received) = mpsc::channel();
                let (reporter, mut reading) = spate_core::feed::depth_reading();
                requests.send((of, reporter)).unwrap();
                drop(requests);
                run(&core, &offsets, &received);
                assert_eq!(
                    offsets.scripted.calls().len(),
                    at,
                    "{of:?} stopped at request {at}: {:?}",
                    offsets.scripted.calls()
                );
                let e = reading.try_outcome().unwrap().unwrap_err();
                assert_eq!(e.kind, FeedErrorKind::Retryable);
                assert!(e.reason.contains("stopped during the read"), "{e}");
            }
        }
    }

    /// A feed connected on `cluster`, and the runtime its clients use.
    fn connected(
        cluster: &crate::testutil::mock::MockKafka,
    ) -> (KafkaFeed, tokio::runtime::Runtime) {
        for (topic, partitions) in [
            ("units", 1),
            ("units-retry", 1),
            ("units-dead", 1),
            ("units-attempts", 2),
        ] {
            cluster.create_topic(topic, partitions, 1).unwrap();
        }
        let section: spate_core::config::FeedSection = serde_yaml::from_str(&format!(
            "attempt_cap: 3\nkafka:\n  brokers: \"{}\"\n  group_id: workers\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n  attempt_topic: \
             units-attempts\n  statistics_interval: 0s\n",
            cluster.bootstrap_servers()
        ))
        .unwrap();
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let feed = KafkaFeed::connect_without_config_checks(&section, io.handle()).unwrap();
        (feed, io)
    }

    /// Pins that the depth thread has ended when a connected feed's drop
    /// returns: its share of the feed's core is gone. Regression for #1761.
    #[test]
    fn the_depth_thread_ends_before_the_feed_s_drop_returns() {
        let cluster = crate::testutil::mock::MockKafka::new();
        let (feed, _io) = connected(&cluster);
        let core = Arc::clone(&feed.core);
        drop(feed);
        assert_eq!(
            Arc::strong_count(&core),
            1,
            "the depth thread outlives the feed's drop"
        );
    }

    /// Pins that the feed's drop joins the depth thread while a request of
    /// its read is in flight: the coordinator holds the thread's OffsetFetch
    /// reply past the request's bound, and the thread's share of the core is
    /// gone when the drop returns. Regression for #1761.
    #[test]
    fn the_drop_waits_for_a_depth_request_in_flight() {
        use crate::testutil::assert_returns;
        use rdkafka::types::RDKafkaApiKey;
        use spate_core::feed::Feed as _;
        use spate_test::{HANG, wait_until};
        let cluster = crate::testutil::mock::MockKafka::new();
        cluster.track_requests();
        let (feed, _io) = connected(&cluster);
        // The group member fetches its committed offsets at assignment, and
        // the hold below is for the request after that one.
        wait_until(HANG, "the member's OffsetFetch", || {
            cluster.received(RDKafkaApiKey::OffsetFetch) >= 1
        });
        cluster.hold_next_reply(1, RDKafkaApiKey::OffsetFetch, 2 * REQUEST_BOUND);
        let fetches = cluster.received(RDKafkaApiKey::OffsetFetch);
        let _reading = feed.read_depth(DepthOf::Feed).expect("a reading");
        wait_until(HANG, "the depth thread's OffsetFetch", || {
            cluster.received(RDKafkaApiKey::OffsetFetch) > fetches
        });
        let listed = cluster.received(RDKafkaApiKey::ListOffsets);
        let core = Arc::clone(&feed.core);
        let handle = cluster.native() as usize;
        assert_returns(handle, HANG, "the feed's drop", move || drop(feed));
        assert_eq!(
            Arc::strong_count(&core),
            1,
            "the depth thread outlives the feed's drop"
        );
        assert_eq!(
            cluster.received(RDKafkaApiKey::ListOffsets),
            listed,
            "the read sent a request after the stop"
        );
    }

    /// Pins that a feed which has latched a Fatal error answers a depth read
    /// with that error, whatever the offsets read.
    #[test]
    fn a_latched_feed_answers_a_depth_read_with_its_error() {
        let core = Arc::new(core(3));
        core.latch("the broker rejected the credential");
        let offsets = Scripted::default().topic("units-dead", &[(0, Offset::Invalid, 0, 0)]);
        let (requests, received) = mpsc::channel();
        let (reporter, mut reading) = spate_core::feed::depth_reading();
        requests.send((DepthOf::DeadLetters, reporter)).unwrap();
        drop(requests);
        run(&core, &offsets, &received);
        let e = reading.try_outcome().unwrap().unwrap_err();
        assert_eq!(e.kind, FeedErrorKind::Fatal);
        assert!(e.reason.contains("rejected the credential"), "{e}");
    }

    /// Pins the reads through a consumer outside the group against the
    /// MockCluster: every partition of a two-partition topic, end offsets per
    /// partition, no commit reading as invalid, and a read of a partition the
    /// topic lacks failing, naming the kind of read.
    #[test]
    fn the_brokers_read_each_partition_of_a_topic() {
        use crate::testutil::mock::MockKafka;
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _};
        let cluster = MockKafka::new();
        cluster.create_topic("work", 2, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let producer: BaseProducer<SpateContext<()>> = raw(&[("bootstrap.servers", &brokers)], ());
        for _ in 0..3 {
            producer
                .send(
                    BaseRecord::<[u8], [u8]>::to("work")
                        .partition(1)
                        .payload(b"x"),
                )
                .map_err(|(e, _)| e)
                .unwrap();
        }
        producer.flush(Duration::from_secs(30)).unwrap();
        let consumer = raw(
            &[("bootstrap.servers", &brokers), ("group.id", "depth-test")],
            SourceHooks::default(),
        );
        let reads = Brokers { consumer };
        let work: Arc<str> = Arc::from("work");
        assert_eq!(reads.partitions(&work).unwrap(), vec![0, 1]);
        let both = [(Arc::clone(&work), 0), (Arc::clone(&work), 1)];
        assert_eq!(reads.listed(&both, Offset::End).unwrap(), vec![0, 3]);
        assert_eq!(reads.listed(&both, Offset::Beginning).unwrap(), vec![0, 0]);
        assert_eq!(
            reads.committed(&both).unwrap(),
            vec![Offset::Invalid, Offset::Invalid]
        );
        let missing = [(Arc::clone(&work), 9)];
        let e = reads.listed(&missing, Offset::Beginning).unwrap_err();
        assert!(e.reason.contains("reading log start offsets"), "{e}");
        let e = reads.listed(&missing, Offset::End).unwrap_err();
        assert!(e.reason.contains("reading end offsets"), "{e}");
    }

    /// Pins the error class of a failed offset request: an authorization
    /// failure is fatal, a timeout retryable.
    #[test]
    fn an_authorization_failure_is_fatal() {
        for code in [
            RDKafkaErrorCode::GroupAuthorizationFailed,
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::ClusterAuthorizationFailed,
        ] {
            assert_eq!(
                class(&KafkaError::MetadataFetch(code)),
                FeedErrorKind::Fatal,
                "{code:?}"
            );
        }
        assert_eq!(
            class(&KafkaError::MetadataFetch(
                RDKafkaErrorCode::RequestTimedOut
            )),
            FeedErrorKind::Retryable
        );
    }

    /// Pins the class of an authorization failure a broker answers to the
    /// committed and end offset reads: both are Fatal.
    #[test]
    fn a_broker_refusing_an_offset_read_on_authorization_fails_it_as_fatal() {
        use crate::testutil::mock::MockKafka;
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let cluster = MockKafka::new();
        cluster.create_topic("work", 1, 1).expect("create topic");
        let consumer = raw(
            &[
                ("bootstrap.servers", &cluster.bootstrap_servers()),
                ("group.id", "depth-denied"),
            ],
            SourceHooks::default(),
        );
        let reads = Brokers { consumer };
        let one = [(Arc::from("work"), 0)];
        cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED],
        );
        let e = reads.committed(&one).unwrap_err();
        assert_eq!(e.kind, FeedErrorKind::Fatal, "{e}");
        cluster.request_errors(
            RDKafkaApiKey::ListOffsets,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
        );
        let e = reads.listed(&one, Offset::End).unwrap_err();
        assert_eq!(e.kind, FeedErrorKind::Fatal, "{e}");
    }
}
