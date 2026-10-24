//! The feed's depth reads, which a bounded job's end check takes: the lag
//! of the feed's group on the work and retry topics, and whether the
//! dead-letter topic has ever been written.
//!
//! The reads run on a thread of the feed's own, through a consumer that
//! carries the feed's `group.id` and never subscribes, so no read runs on
//! the group member's poll thread and none counts against its poll
//! interval (INV-2). [`Offsets`] is the seam the unit tests script.

use super::{Core, Topics};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::{Offset, TopicPartitionList};
use spate_core::feed::{Depth, DepthOf, DepthReporter, FeedError, FeedErrorKind};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

/// The bound on each metadata and offset request.
const REQUEST_BOUND: Duration = Duration::from_secs(5);

/// How long the depth thread waits for a request before it serves its
/// client's queued events.
const SERVE_EVERY: Duration = Duration::from_secs(1);

/// One partition of one of the feed's topics.
pub(crate) type Partition = (Arc<str>, i32);

/// The offset reads a depth read takes.
pub(crate) trait Offsets: Send {
    /// The partitions of `topic`.
    fn partitions(&self, topic: &Arc<str>) -> Result<Vec<i32>, FeedError>;

    /// The group's committed offset on each of `partitions`, in their order,
    /// read in one request.
    fn committed(&self, partitions: &[Partition]) -> Result<Vec<Offset>, FeedError>;

    /// The log start offset of each of `partitions` for
    /// [`Offset::Beginning`], or the end offset for [`Offset::End`], in their
    /// order, read in one request.
    fn listed(&self, partitions: &[Partition], at: Offset) -> Result<Vec<i64>, FeedError>;

    /// Serve the client's queued statistics, log lines and errors.
    fn serve(&self) {}
}

/// The depth of the work and retry topics: each partition's end offset less
/// the group's committed offset, as visible units.
///
/// The committed offsets are read before any end offset. A unit failed
/// between the two reads has its retry write confirmed before its work
/// commit moves, so it shows on the retry topic's end offset either way.
/// A partition with no commit, or a commit below its log start, reads from
/// its log start: units retention deleted before they were read leave no
/// trace (INV-1's statement places them outside the property).
pub(crate) fn feed_depth(offsets: &dyn Offsets, topics: &Topics) -> Result<Depth, FeedError> {
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
    }
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
/// ever written to it counts, whether or not retention has deleted it since.
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
        format!("the dead-letter topic `{}`", topics.dead_letter),
        false,
    );
    depth.visible = written;
    Ok(depth)
}

/// Answer each depth request from `requests` until the feed drops its
/// sender or stops.
pub(crate) fn run(
    core: &Core,
    offsets: &dyn Offsets,
    requests: &Receiver<(DepthOf, DepthReporter)>,
) {
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
            _ => feed_depth(offsets, core.topics()),
        };
        // A credential or TLS rejection latches the feed, and the depth read
        // then reports it, whatever the read itself returned.
        let read = match core.lock().fatal.clone() {
            Some(fatal) => Err(fatal),
            None => read,
        };
        reporter.report(read);
        // The source checks a reading only when it polls, so the answer
        // ends its park.
        let waker = core.lock().waker.clone();
        if let Some(waker) = waker {
            waker.wake();
        }
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
        let tpl = self.list(partitions, at)?;
        let doing = match at {
            Offset::Beginning => "reading log start offsets",
            _ => "reading end offsets",
        };
        let read = self
            .consumer
            .offsets_for_times(tpl, REQUEST_BOUND)
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
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    /// Offsets per partition, scripted, and every call in order.
    #[derive(Default)]
    struct Scripted {
        partitions: HashMap<String, Vec<i32>>,
        committed: HashMap<(String, i32), Offset>,
        starts: HashMap<(String, i32), i64>,
        ends: HashMap<(String, i32), i64>,
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
        let section: spate_core::config::FeedSection = serde_yaml::from_str(
            "attempt_cap: 3\nkafka:\n  brokers: localhost:9092\n  group_id: workers\n  \
             topic: units\n  retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: units-attempts\n",
        )
        .unwrap();
        let config = crate::feed::KafkaFeedConfig::from_section(&section).unwrap();
        let core = Core::new(&config, std::num::NonZeroU32::MIN);
        let serving = Serving::default();
        let (requests, received) = mpsc::channel::<(DepthOf, DepthReporter)>();
        std::thread::scope(|s| {
            let (core, serving) = (&core, &serving);
            s.spawn(move || run(core, serving, &received));
            spate_test::wait_until(
                std::time::Duration::from_secs(10),
                "the client is served",
                || serving.0.load(Ordering::SeqCst) > 0,
            );
            drop(requests);
        });
    }

    /// Pins that `run` wakes the reader once it has answered a depth read,
    /// so the reader's park ends on the answer.
    #[test]
    fn an_answered_depth_read_wakes_the_reader() {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(
            "attempt_cap: 3\nkafka:\n  brokers: localhost:9092\n  group_id: workers\n  \
             topic: units\n  retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: units-attempts\n",
        )
        .unwrap();
        let config = crate::feed::KafkaFeedConfig::from_section(&section).unwrap();
        let core = Core::new(&config, std::num::NonZeroU32::MIN);
        let (waker, park) = spate_core::feed::wake_channel();
        core.lock().waker = Some(waker);
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
        let depth = feed_depth(&offsets, &topics()).unwrap();
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
            feed_depth(&offsets, &topics()).unwrap().visible
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
            feed_depth(&offsets, &topics())
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
        assert!(feed_depth(&offsets, &topics()).unwrap().is_empty());
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
            feed_depth(&offsets, &topics()).unwrap_err().kind,
            FeedErrorKind::Fatal
        );
        let mut offsets = base();
        offsets.fail_listed = Some(FeedError::new(FeedErrorKind::Retryable, "timed out"));
        assert_eq!(
            feed_depth(&offsets, &topics()).unwrap_err().kind,
            FeedErrorKind::Retryable
        );
        assert_eq!(
            dead_letter_depth(&offsets, &topics()).unwrap_err().kind,
            FeedErrorKind::Retryable
        );
    }

    /// Pins the dead-letter depth: the sum of the topic's end offsets, so a
    /// record ever written there counts whatever its log start.
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
        use rdkafka::ClientConfig;
        use rdkafka::mocking::MockCluster;
        let cluster = MockCluster::new(1).expect("mock cluster");
        cluster.create_topic("work", 1, 1).expect("create topic");
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .set("group.id", "depth-partition-error")
            .create_with_context(crate::context::SourceContext::default())
            .unwrap();
        let reads = Brokers { consumer };
        let e = reads.committed(&[(Arc::from("work"), 9)]).unwrap_err();
        assert!(e.reason.contains("`work` partition 9"), "{e}");
    }

    /// Pins that serving the depth client hands its queued events to the
    /// context: a statistics event, queued every 10 ms here, reaches it only
    /// through a serve.
    #[test]
    fn serving_the_depth_client_drains_its_events() {
        use rdkafka::ClientConfig;
        use rdkafka::consumer::Consumer as _;
        use rdkafka::mocking::MockCluster;
        let cluster = MockCluster::new(1).expect("mock cluster");
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .set("group.id", "depth-serve")
            .set("statistics.interval.ms", "10")
            .create_with_context(crate::context::SourceContext::default())
            .unwrap();
        let reads = Brokers { consumer };
        spate_test::wait_until(
            std::time::Duration::from_secs(10),
            "a statistics event reaches the context",
            || {
                reads.serve();
                reads.consumer.context().stats.lock().unwrap().is_some()
            },
        );
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
        use rdkafka::ClientConfig;
        use rdkafka::mocking::MockCluster;
        use rdkafka::types::RDKafkaRespErr;
        let cluster = MockCluster::new(1).expect("mock cluster");
        cluster.create_topic("dead", 1, 1).expect("create topic");
        cluster
            .topic_error(
                "dead",
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
            )
            .unwrap();
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .set("group.id", "depth-unknown")
            .create_with_context(crate::context::SourceContext::default())
            .unwrap();
        let reads = Brokers { consumer };
        let e = reads.partitions(&Arc::from("dead")).unwrap_err();
        assert_eq!(e.kind, FeedErrorKind::Retryable, "{e}");
        assert!(e.reason.contains("`dead`"), "{e}");
    }

    /// Pins that the depth thread reads nothing once the feed has stopped,
    /// and the reading it drops settles Retryable.
    #[test]
    fn a_stopped_feed_reads_no_depth() {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(
            "attempt_cap: 3\nkafka:\n  brokers: localhost:9092\n  group_id: workers\n  \
             topic: units\n  retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: units-attempts\n",
        )
        .unwrap();
        let config = crate::feed::KafkaFeedConfig::from_section(&section).unwrap();
        let core = Core::new(&config, std::num::NonZeroU32::MIN);
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

    /// Pins that a feed which has latched a Fatal error answers a depth read
    /// with that error, whatever the offsets read.
    #[test]
    fn a_latched_feed_answers_a_depth_read_with_its_error() {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(
            "attempt_cap: 3\nkafka:\n  brokers: localhost:9092\n  group_id: workers\n  \
             topic: units\n  retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: units-attempts\n",
        )
        .unwrap();
        let config = crate::feed::KafkaFeedConfig::from_section(&section).unwrap();
        let core = Core::new(&config, std::num::NonZeroU32::MIN);
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
        use rdkafka::ClientConfig;
        use rdkafka::mocking::MockCluster;
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _};
        let cluster = MockCluster::new(1).expect("mock cluster");
        cluster.create_topic("work", 2, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
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
        let consumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "depth-test")
            .create_with_context(crate::context::SourceContext::default())
            .unwrap();
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
        use rdkafka::ClientConfig;
        use rdkafka::mocking::MockCluster;
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let cluster = MockCluster::new(1).expect("mock cluster");
        cluster.create_topic("work", 1, 1).expect("create topic");
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .set("group.id", "depth-denied")
            .create_with_context(crate::context::SourceContext::default())
            .unwrap();
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
