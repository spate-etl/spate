//! The control plane: one or more consumers whose partitions fan out to lanes.
//!
//! Each consumer is a group member with its own client, rebalance intents and
//! ownership, and every lane carries its own consumer's partition queue. The
//! choreography below runs per consumer, with two rules joining them into the
//! runtime's one lane set. Assignments are held until every consumer of the
//! rebalance has one, or for `ASSIGN_HOLD`, then accepted together as one
//! [`SourceEvent::LanesAssigned`]; one that arrives while lanes are live drains
//! them and accepts every consumer's partitions again as a fresh epoch. Once
//! two offers overlap, each offer below the newest generation is answered with
//! an empty `assign`, and its consumer rejoins. Eager assignment revokes every member
//! that has answered its last assignment, so the first revocation
//! on any consumer drains every consumer's lanes, and each consumer is released
//! with `unassign` once its own revocation has arrived. A consumer's retryable
//! poll error does not hold back the source's next rebalance intent. The call
//! still acts on it, and returns the error when neither the intent nor the
//! held assignments yield an event. A fatal poll error returns before any
//! intent is acted on.
//!
//! # Rebalance choreography (deferred completion)
//!
//! librdkafka runs rebalance callbacks inside `poll()` on the thread that
//! calls it, which here is the runtime controller calling
//! [`Source::poll_events`]. For assignment and revocation events the
//! callback ([`SourceHooks::rebalance`]) only records an intent and
//! returns without acknowledging, which leaves the rebalance legally in
//! progress until we call `assign`/`unassign`. Completion then happens on
//! the controller thread, interleaved with the runtime's own drain
//! choreography:
//!
//! **Assignment** (all inside one `poll_events` call):
//! 1. read the committed offsets and give each partition a concrete start
//!    offset, applying the reset policy (`auto.offset.reset`) where none is
//!    committed. Under `latest` one consumer reads every offer's partitions, and
//!    a partition with no commit has its end listed and committed before any
//!    consumer is assigned, so every later start resumes from it; a failure holds
//!    the assignment and retries it, or discards it when the group has moved past
//!    this consumer. Under `earliest`
//!    and `error`, the start offset is left for librdkafka to resolve for a
//!    partition whose committed offset could not be read (the lookup failed
//!    or timed out, or returned an error for that partition) and, under
//!    `error`, for one with no committed offset. Per partition this runs
//!    lookup, list, commit, `assign`, resume, lane. Consumers of a process have
//!    no order among them, and the broker's generation check orders them across
//!    generations;
//! 2. `assign(tpl)` — accept the partitions. With concrete offsets the
//!    fetchers start before `assign` returns. The fetcher of a partition
//!    left for librdkafka does not start before `assign` returns, and a
//!    fetcher librdkafka starts later can lose its start to a concurrent
//!    `pause`/`resume`, which leaves the partition unfetched until the next
//!    rebalance
//!    ([librdkafka#5591](https://github.com/confluentinc/librdkafka/issues/5591));
//! 3. `pause(tpl)` — a fetch that completed before it is discarded as
//!    outdated and refetched, so no message leaks onto the main queue. It also
//!    waits for the stop of an earlier assignment on this client, which resets
//!    the position a lane reads;
//! 4. `split_partition_queue` per partition (must be redone after *every*
//!    assign, since assign deactivates existing queues) and build lanes;
//! 5. `resume(tpl)` — messages start flowing into the split queues, which
//!    buffer until pipeline threads take the lanes over;
//! 6. return [`SourceEvent::LanesAssigned`].
//!
//! **Revocation** (spans two `poll_events` calls, or one with nothing live,
//! where the consumer is released inline):
//! 1. surface [`SourceEvent::LanesRevoked`] with a [`DrainBarrier`] sized
//!    by lane count (the runtime's drivers arrive once per stopped lane);
//! 2. the runtime stops the lanes, waits for the barrier, drains the
//!    checkpointer, calls [`Source::commit`] + [`Source::flush_commits`].
//!    The sync commit runs before the rebalance is acknowledged, so the
//!    generation is valid for a member the group has kept. For a member it
//!    removed, the client refuses the commit and the drained work replays;
//! 3. the controller loops back into `poll_events`, which sees the pending
//!    completion and calls `unassign()`, letting the rebalance finish.
//!
//! **Rebalance error** (the arbitrary-error event; spans two calls, or one
//! with no live lanes): the
//! callback completes it inline with `unassign`, because the event has no
//! deferred form and an unacknowledged one wedges the member for the
//! process lifetime, so ownership is gone before `poll_events` consumes
//! the intent. `poll_events` then surfaces [`SourceEvent::LanesRevoked`]
//! for every live lane; the runtime drains them, but unlike a revocation
//! the final commit is refused (`commit` consults ownership, which is
//! empty) and the drained work replays. The next call reports the error,
//! classified like every other consumer error; librdkafka rejoins on its
//! own and a fresh assignment follows.
//!
//! **Lost revocation**: a member the client was told is out of its generation
//! gets the same revocation event, and the steps above run. With partitions
//! assigned, the flush fails with `AssignmentLost`, and with more than one
//! consumer that failure is logged per consumer. A discarded consumer holds
//! nothing, so its flush has nothing to refuse. `unassign` follows either way.
//!
//! **Stale held assignment**: no event reaches the source when the group
//! removes a member holding an offer, so the offer is held like any other. If
//! no offers overlap it is accepted, and the consumer's next heartbeat finds it
//! out of the group. If any overlap, [`KafkaSource::accept_held`] discards each
//! offer below the newest generation, and it discards an offer whose start
//! commit the group refuses. That consumer's lost revocation drains its peers'
//! lanes, and it rejoins after `unassign`, so a discard costs one more drain. A
//! partition still in two offers after that is fatal.
//!
//! Revoked lanes' queues go silent immediately (fetching stops), and a
//! partition split with `split_partition_queue` stays split. The split
//! sets `RD_KAFKA_Q_F_FWD_APP` on the partition's fetch queue, librdkafka
//! refuses to re-forward a queue carrying that flag, and nothing clears
//! it, so neither dropping the `PartitionQueue` nor a later unassign
//! restores main-queue forwarding. The rewind in
//! [`Source::poll_events`] stands against a librdkafka that stops holding
//! the contract.

use crate::client::{PendingCommit, list_offsets, listed_offset, send_commit};
use crate::config::KafkaSourceConfig;
use crate::context::{Intent, SourceContext, SourceHooks};
use crate::error::{StartFailure, classify_start_error};
use crate::lane::KafkaLane;
use crate::metrics::KafkaStatsMetrics;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::TopicPartitionListElem;
use rdkafka::{Offset, TopicPartitionList};
use spate_core::checkpoint::AckIssuer;
use spate_core::clock::Stopwatch;
use spate_core::clock::SyncClock;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::hash::{FastMap, FastSet};
use spate_core::metrics::SourceMetrics;
use spate_core::record::PartitionId;
use spate_core::source::{DrainBarrier, LaneId, Source, SourceCtx, SourceEvent};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

/// Bound on the committed-offset reads made before one assignment, shared by
/// every consumer it accepts.
const COMMITTED_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the source holds one consumer's assignment for the others of the
/// same rebalance. librdkafka's default `heartbeat.interval.ms`, the longest a
/// steady member goes before it learns of a rebalance.
const ASSIGN_HOLD: Duration = Duration::from_secs(3);

/// Bound on waiting for the start commits of one accept, which are in flight
/// together. An answer that takes longer leaves the offers held for the next
/// attempt, which sends the same offsets again.
const START_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long an offer whose start could not be settled waits for its next
/// attempt. Without a pace every poll repeats a blocking broker round trip.
const START_RETRY_PACE: Duration = Duration::from_secs(1);

/// Since when the source has been without an assignment, and what took the
/// last one. `cause` is `None` before the first assignment, the window
/// `startup_timeout` governs; after it, `assignment_timeout` does.
struct AssignmentWait {
    since: Instant,
    cause: Option<String>,
}

/// Where one consumer stands in the group's rebalance protocol.
enum Phase {
    /// Rejoining the group: an assignment is expected, or librdkafka has
    /// queued one the source has not taken.
    Joining,
    /// Offered this assignment, not yet accepted with `assign`. librdkafka
    /// keeps heartbeating and answers no later rebalance for this member until
    /// `assign` is called, so the group can elect a generation without it and
    /// remove it.
    Held(TopicPartitionList),
    /// Its assignment is accepted.
    Assigned,
}

/// One consumer of the source and the partitions it owns.
struct Member {
    consumer: Arc<BaseConsumer<SourceContext>>,
    /// The consumer's connector-owned `spate_kafka_source_*` translator.
    /// `None` when statistics are off or the runtime provides no Meter.
    stats: Option<KafkaStatsMetrics>,
    /// Live lanes of this consumer's partitions, by id.
    assignment: FastMap<LaneId, i32>,
    /// Lanes surfaced as revoked whose partitions this consumer still owns:
    /// `unassign` has not released them, so the post-drain commit may still
    /// store their offsets. Cleared when the consumer releases or re-assigns
    /// them.
    revoking: FastMap<LaneId, i32>,
    /// This consumer's own revocation event has been consumed, so `unassign`
    /// completes it once the runtime has drained.
    revoke_arrived: bool,
    saw_first_assignment: bool,
    phase: Phase,
    /// Committed offset per held partition, read before its assignment.
    /// Stands in for librdkafka's committed position in the lag statistic,
    /// which the lookup does not update.
    committed_at_assign: FastMap<i32, i64>,
    /// The latest watermark stored for each partition, shared with its lane.
    watermarks: FastMap<i32, Arc<AtomicI64>>,
    /// The generation [`Member::group_generation`] reads in place of the
    /// client's, since a unit test's consumer never joins a group.
    #[cfg(test)]
    generation: Option<i32>,
}

impl Member {
    /// Partitions this consumer holds live lanes for.
    fn live_partition_ids(&self) -> Vec<PartitionId> {
        partition_ids(self.assignment.values())
    }

    /// Give up what this consumer owns and expect it to rejoin: a late commit
    /// for its partitions is refused again.
    fn rejoin(&mut self) {
        self.revoking.clear();
        self.revoke_arrived = false;
        self.phase = Phase::Joining;
    }

    /// The generation this consumer last joined with, or `-1` for none.
    fn group_generation(&self) -> i32 {
        #[cfg(test)]
        if let Some(generation) = self.generation {
            return generation;
        }
        crate::client::group_generation(&self.consumer)
    }
}

/// One consumer's start positions, read and not yet assigned.
struct Prepared {
    /// The list to assign.
    list: TopicPartitionList,
    /// What to commit before assigning: each partition with no commit, at its
    /// listed end.
    to_commit: TopicPartitionList,
}

impl Prepared {
    /// Assign `list`, with nothing to commit.
    fn assigning(list: TopicPartitionList) -> Prepared {
        Prepared {
            list,
            to_commit: TopicPartitionList::new(),
        }
    }
}

/// What the next `poll_events` call completes, once the runtime has drained
/// and committed the lanes the previous call surfaced as revoked.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AfterDrain {
    /// Release with `unassign` every consumer whose revocation arrived.
    Release,
    /// Accept every consumer's partitions again as one assignment, adding
    /// the one that arrived while lanes were live.
    Reassign,
}

/// Kafka source: one or more consumer-group members per process, their
/// partitions split into per-lane queues polled by pipeline threads.
/// Constructed from config ([`KafkaSource::new`]) or a pipeline component
/// section ([`KafkaSource::from_component_config`]).
pub struct KafkaSource {
    config: KafkaSourceConfig,
    /// One per [`KafkaSourceConfig::consumers`], created at `open`.
    members: Vec<Member>,
    issuer: Option<AckIssuer>,
    /// The framework's source-stage handles, shared by the runtime at `open`.
    /// Only consumer lag is published through them; the runtime records
    /// everything else. `None` when the source is driven outside a pipeline.
    metrics: Option<Arc<SourceMetrics>>,
    next_lane: u32,
    /// Any consumer has accepted an assignment, empty or not.
    saw_first_assignment: bool,
    after_drain: Option<AfterDrain>,
    /// A rebalance error on a consumer whose lanes were surfaced as revoked;
    /// the classified error is reported on the next `poll_events` call, after
    /// the runtime finished draining them. The callback already released that
    /// consumer's ownership with `unassign`.
    pending_error: Option<(usize, rdkafka::error::RDKafkaErrorCode)>,
    /// When the first held assignment of the current rebalance arrived.
    hold_since: Option<Instant>,
    /// A start commit went unanswered inside its bound. Consumer close waits for
    /// every outstanding commit, and one to a silent coordinator stays outstanding
    /// for as long as the coordinator is silent.
    commit_unanswered: bool,
    /// When an accept that could not settle a start may run again. `None`
    /// outside such a wait, and cleared when an accept gets past the starts.
    start_retry_at: Option<Instant>,
    /// The consumer polled first on the next call, rotated so each one takes
    /// its turn at the call's blocking wait.
    poll_start: usize,
    /// Set while the source holds no partitions: from `open` until the first
    /// assignment, and from every later loss of the last one. The deadline is
    /// measured from `since`; an accepted assignment, empty or not, clears it.
    assignment_wait: Option<AssignmentWait>,
    /// The latest consumer poll error other than `AllBrokersDown`, timed
    /// from its arrival, for the deadline error to name. Cleared with
    /// `assignment_wait` by an accepted assignment.
    last_consumer_error: Option<(Stopwatch, String)>,
    /// Messages that leaked onto a main queue and were rewound.
    main_queue_rewinds: u64,
    /// The clock the assignment windows, the hold and the error's age read.
    clock: SyncClock,
    /// An error the next poll of this consumer returns in place of
    /// `BaseConsumer::poll`, for a test that needs a poll error in one call.
    #[cfg(test)]
    injected_poll_error: Option<(usize, rdkafka::error::KafkaError)>,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("topic", &self.config.topic)
            .field("group_id", &self.config.group_id)
            .field("consumers", &self.members.len())
            .field(
                "lanes",
                &self
                    .members
                    .iter()
                    .map(|m| m.assignment.len())
                    .sum::<usize>(),
            )
            .finish_non_exhaustive()
    }
}

/// `partitions` as `PartitionId`s, skipping any that is negative.
fn partition_ids<'a>(partitions: impl IntoIterator<Item = &'a i32>) -> Vec<PartitionId> {
    partitions
        .into_iter()
        .filter_map(|p| u32::try_from(*p).ok().map(PartitionId))
        .collect()
}

/// Record `error` as the one a poll reports when it takes precedence: the
/// first fatal error, else the first error. A polled error is gone from its
/// queue, so a fatal one replaces a retryable one an earlier consumer
/// returned.
fn keep_reported(reported: &mut Option<SourceError>, error: SourceError) {
    if reported.as_ref().is_none_or(|first| {
        error.class() == ErrorClass::Fatal && first.class() != ErrorClass::Fatal
    }) {
        *reported = Some(error);
    }
}

/// The first partition two of `offers` carry, with the consumers carrying it.
fn first_repeat(offers: &[(usize, TopicPartitionList)]) -> Option<(i32, usize, usize)> {
    let mut seen: FastMap<i32, usize> = FastMap::default();
    for (k, tpl) in offers {
        for elem in tpl.elements() {
            let partition = elem.partition();
            if let Some(first) = seen.insert(partition, *k) {
                return Some((partition, first, *k));
            }
        }
    }
    None
}

/// The newest generation among `(consumer, generation)` offers.
fn newest_generation(offers: &[(usize, i32)]) -> i32 {
    offers.iter().map(|(_, g)| *g).fold(i32::MIN, i32::max)
}

/// The consumers whose offer is stale, from each offer's `(consumer,
/// generation)`: a generation below the newest among them, or none at all. So
/// offers that all read no generation are all stale, offers of one valid
/// generation are not, and the outcome does not depend on a consumer's index.
fn stale_offers(offers: &[(usize, i32)]) -> Vec<usize> {
    let newest = newest_generation(offers);
    offers
        .iter()
        .filter(|(_, g)| *g < newest || *g < 0)
        .map(|(k, _)| *k)
        .collect()
}

/// The order one `poll_events` call polls `n` consumers in, starting at
/// `start`, with the wait each poll may spend: zero for every consumer but
/// the last, which takes `timeout`. A call therefore waits at most `timeout`
/// whatever the count, and serves every consumer's callbacks.
fn poll_order(
    start: usize,
    n: usize,
    timeout: Duration,
) -> impl Iterator<Item = (usize, Duration)> {
    (0..n).map(move |i| {
        let wait = if i + 1 == n { timeout } else { Duration::ZERO };
        ((start + i) % n, wait)
    })
}

impl KafkaSource {
    /// Create a source from validated configuration.
    #[must_use]
    pub fn new(config: KafkaSourceConfig) -> Self {
        KafkaSource {
            config,
            members: Vec::new(),
            issuer: None,
            metrics: None,
            next_lane: 0,
            saw_first_assignment: false,
            after_drain: None,
            pending_error: None,
            hold_since: None,
            commit_unanswered: false,
            start_retry_at: None,
            poll_start: 0,
            assignment_wait: None,
            last_consumer_error: None,
            main_queue_rewinds: 0,
            clock: SyncClock::system(),
            #[cfg(test)]
            injected_poll_error: None,
        }
    }

    /// Read the assignment windows from `clock` in place of the system
    /// clock.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_clock(mut self, clock: spate_core::clock::SharedClock) -> KafkaSource {
        self.clock = SyncClock::new(clock);
        self
    }

    /// Create a source from the pipeline's opaque `source: { kafka: ... }`
    /// section.
    ///
    /// # Errors
    ///
    /// As [`KafkaSourceConfig::from_component_config`].
    pub fn from_component_config(
        section: &spate_core::config::ComponentConfig,
    ) -> Result<Self, spate_core::config::ConfigError> {
        Ok(Self::new(KafkaSourceConfig::from_component_config(
            section,
        )?))
    }

    fn opened(&self) -> Result<(), SourceError> {
        if self.members.is_empty() {
            return Err(SourceError::fatal("source used before open()"));
        }
        Ok(())
    }

    fn tpl_for(&self, partitions: impl IntoIterator<Item = i32>) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for p in partitions {
            tpl.add_partition(&self.config.topic, p);
        }
        tpl
    }

    /// Partitions still owned (the live assignment of every consumer), as
    /// `PartitionId`s. Partitions librdkafka reports outside this set belong
    /// to another member now, so their lag is neither published nor left
    /// standing.
    fn retained_partition_ids(&self) -> Vec<PartitionId> {
        partition_ids(self.members.iter().flat_map(|m| m.assignment.values()))
    }

    /// The consumer that owns `partition` and its Kafka partition number:
    /// the one holding it live, else the one still revoking it. `None` when
    /// no consumer of this source owns it.
    fn owner(&self, partition: PartitionId) -> Option<(usize, i32)> {
        let p = i32::try_from(partition.0).ok()?;
        self.members
            .iter()
            .position(|m| m.assignment.values().any(|q| *q == p))
            .or_else(|| {
                self.members
                    .iter()
                    .position(|m| m.revoking.values().any(|q| *q == p))
            })
            .map(|k| (k, p))
    }

    fn holds_no_lanes(&self) -> bool {
        self.members.iter().all(|m| m.assignment.is_empty())
    }

    /// Zero and drop the per-partition series for partitions the source
    /// lost in the rebalance that just completed: the lag family and every
    /// per-partition gauge the connectors' own statistics translations
    /// register, which all carry the same ownership meaning. The lag family
    /// keeps what any consumer owns; each translator keeps what its own
    /// consumer owns.
    ///
    /// Called once a new assignment is accepted, and on `Intent::Error`,
    /// where ownership is already released and no assignment is coming until
    /// the member rejoins. Never on `Intent::Revoke`: under eager rebalancing
    /// a revoke covers *every* partition, including the ones about to be
    /// handed straight back, so pruning there would zero the whole family on
    /// every rebalance and read as a phantom drain. It would also blank the
    /// partitions the runtime is still draining and committing. The error
    /// path has no such partitions, which is why pruning before its drain is
    /// sound.
    ///
    /// This is the only place either family is released, so both hold their
    /// values through a rebalance rather than following the empty
    /// assignment `publish_stats` reads while one is in flight.
    fn prune_partition_series(&mut self) {
        for member in &mut self.members {
            let owned: Vec<i32> = member.assignment.values().copied().collect();
            member.committed_at_assign.retain(|p, _| owned.contains(p));
            member.watermarks.retain(|p, _| owned.contains(p));
            if let Some(stats) = member.stats.as_mut() {
                stats.retain_partitions(&partition_ids(&owned));
            }
        }
        if let Some(m) = &self.metrics {
            m.retain_partitions(&self.retained_partition_ids());
        }
    }

    /// The error a rebalance-error event on consumer `k` surfaces as,
    /// classified through the same table as every other consumer error.
    fn rebalance_error(&self, k: usize, code: rdkafka::error::RDKafkaErrorCode) -> SourceError {
        let started = self.members.get(k).is_some_and(|m| m.saw_first_assignment);
        SourceError::new(
            crate::error::classify_consumer_error(code, started),
            format!("rebalance error: {code}"),
        )
    }

    /// Accept one consumer's assignment from its settled `start` offsets:
    /// assign → pause → split → resume → lanes. An empty `tpl` acknowledges an
    /// empty assignment, which the rebalance protocol still requires.
    fn accept_assignment(
        &mut self,
        k: usize,
        tpl: &TopicPartitionList,
        start: &TopicPartitionList,
        started_at_end: usize,
    ) -> Result<Vec<KafkaLane>, SourceError> {
        let consumer = Arc::clone(&self.members[k].consumer);
        self.members[k].revoking.clear();
        self.members[k].phase = Phase::Assigned;
        self.members[k].saw_first_assignment = true;
        if tpl.count() == 0 {
            consumer.assign(tpl).map_err(fatal("assign empty"))?;
            return Ok(Vec::new());
        }
        let issuer = self
            .issuer
            .as_ref()
            .ok_or_else(|| SourceError::fatal("assignment before open()"))?;

        consumer.assign(start).map_err(fatal("assign"))?;
        // A fetch that completes before this pause is discarded as outdated
        // and refetched, so no pre-split message reaches the main queue.
        consumer.pause(tpl).map_err(fatal("pause new assignment"))?;

        let mut lanes = Vec::new();
        for elem in tpl.elements() {
            let partition = elem.partition();
            let queue = consumer
                .split_partition_queue(&self.config.topic, partition)
                .ok_or_else(|| {
                    SourceError::fatal(format!("no queue for assigned partition {partition}"))
                })?;
            let lane_id = LaneId(self.next_lane);
            self.next_lane += 1;
            self.members[k].assignment.insert(lane_id, partition);
            let mut position = TopicPartitionList::with_capacity(1);
            position.add_partition(&self.config.topic, partition);
            let watermark = Arc::new(AtomicI64::new(i64::MIN));
            self.members[k]
                .watermarks
                .insert(partition, Arc::clone(&watermark));
            lanes.push(KafkaLane::new(
                lane_id,
                PartitionId(u32::try_from(partition).unwrap_or(0)),
                queue,
                Arc::clone(&consumer),
                issuer.clone(),
                position,
                watermark,
            ));
        }
        consumer
            .resume(tpl)
            .map_err(fatal("resume new assignment"))?;
        tracing::info!(
            consumer = k,
            partitions = lanes.len(),
            started_at_end,
            topic = %self.config.topic,
            "accepted assignment"
        );
        Ok(lanes)
    }

    /// Accept every held assignment, and with `reassign` every assigned
    /// consumer's partitions again, as one epoch: one `LanesAssigned` with
    /// every consumer's lanes, or `Idle` when they hold none. When any two
    /// offers overlap, each offer below the newest generation is discarded
    /// first, overlapping or not, and a partition still in two offers is fatal.
    fn accept_held(&mut self, reassign: bool) -> Result<SourceEvent<KafkaLane>, SourceError> {
        let mut offers: Vec<(usize, TopicPartitionList)> = Vec::new();
        let mut held = Vec::new();
        for (k, member) in self.members.iter_mut().enumerate() {
            match std::mem::replace(&mut member.phase, Phase::Assigned) {
                Phase::Held(tpl) => {
                    held.push(k);
                    offers.push((k, tpl));
                }
                Phase::Assigned if reassign && !member.revoking.is_empty() => {
                    let mut tpl = TopicPartitionList::new();
                    for p in member.revoking.values() {
                        tpl.add_partition(&self.config.topic, *p);
                    }
                    offers.push((k, tpl));
                }
                phase => member.phase = phase,
            }
        }
        if first_repeat(&offers).is_some() {
            offers = self.discard_stale_offers(offers)?;
            if let Some((partition, a, b)) = first_repeat(&offers) {
                return Err(SourceError::fatal(format!(
                    "partition {partition} was assigned to consumers {a} and {b} of this \
                         source in generation {}",
                    self.members[a].group_generation()
                )));
            }
        }
        // One budget for every lookup and listing of the accept: the controller
        // thread polls no consumer meanwhile.
        let lookup_until = Instant::now() + COMMITTED_LOOKUP_TIMEOUT;
        // Every start settles ahead of the first `assign`, so a start that
        // fails leaves no consumer accepted.
        let starts = if self.config.uncommitted_start() == Some(Offset::End) {
            match self.read_latest_starts(&offers, lookup_until)? {
                Ok(starts) => starts,
                Err(reason) => return Ok(self.retry_starts(offers, &held, reassign, &reason)),
            }
        } else {
            let mut starts = Vec::with_capacity(offers.len());
            for (k, tpl) in &offers {
                starts.push(Prepared::assigning(if tpl.count() == 0 {
                    TopicPartitionList::new()
                } else {
                    self.start_positions(*k, tpl, lookup_until)
                }));
            }
            starts
        };
        let offered = offers.len();
        let (offers, starts, retry) = self.commit_starts(offers, starts)?;
        if let Some(reason) = retry {
            return Ok(self.retry_starts(offers, &held, reassign, &reason));
        }
        if offers.is_empty() {
            // Nothing was accepted and no lane is left, so the assignment
            // window is open and its deadline ends a run that never accepts
            // one.
            self.hold_since = None;
            self.start_retry_at = None;
            self.note_assignment_loss("every held assignment was discarded".to_owned());
            return Ok(SourceEvent::Idle);
        }
        self.hold_since = None;
        self.start_retry_at = None;
        let mut lanes = Vec::new();
        for ((k, tpl), start) in offers.iter().zip(&starts) {
            let at_end = start.to_commit.count();
            lanes.extend(self.accept_assignment(*k, tpl, &start.list, at_end)?);
        }
        // An empty peer accepted beside a discarded offer holds nothing the group
        // meant to give, so the deadline keeps running for the discarded partitions.
        if offers.len() < offered && lanes.is_empty() {
            self.note_assignment_loss("every offered partition was discarded".to_owned());
            return Ok(SourceEvent::Idle);
        }
        let joining: Vec<usize> = self
            .members
            .iter()
            .enumerate()
            .filter(|(_, m)| matches!(m.phase, Phase::Joining))
            .map(|(k, _)| k)
            .collect();
        if !joining.is_empty() {
            tracing::warn!(
                joining = ?joining,
                "accepting the assignments held; these consumers have none yet"
            );
        }
        self.saw_first_assignment = true;
        self.assignment_wait = None;
        self.last_consumer_error = None;
        self.prune_partition_series();
        if lanes.is_empty() {
            return Ok(SourceEvent::Idle);
        }
        Ok(SourceEvent::LanesAssigned(lanes))
    }

    /// Commit what each consumer has to commit, all in flight together under one
    /// bound, so an accept waits for the slowest answer and not their sum. Returns
    /// the offers and starts left, each stale offer discarded, with the reason to
    /// retry when any answer asks for it. A commit left unanswered may still land,
    /// and the next attempt's lookup then finds it.
    ///
    /// # Errors
    ///
    /// A failure [`classify_start_error`] classes as fatal.
    #[allow(clippy::type_complexity)]
    fn commit_starts(
        &mut self,
        offers: Vec<(usize, TopicPartitionList)>,
        starts: Vec<Prepared>,
    ) -> Result<
        (
            Vec<(usize, TopicPartitionList)>,
            Vec<Prepared>,
            Option<String>,
        ),
        SourceError,
    > {
        let mut retry = None;
        let mut sent: Vec<Option<PendingCommit<SourceContext>>> = Vec::with_capacity(offers.len());
        let mut kept = Vec::with_capacity(offers.len());
        for ((k, tpl), start) in offers.into_iter().zip(starts) {
            let mut commit = None;
            if start.to_commit.count() > 0 {
                // Read here, after the listing: a heartbeat reply can move the
                // generation while it waits, and the broker takes a generation of
                // -1 as a simple consumer's, which could replace a peer's offset.
                if self.members[k].group_generation() < 0 {
                    self.discard_refused(k, &tpl)?;
                    continue;
                }
                match send_commit(&self.members[k].consumer, &start.to_commit) {
                    Ok(pending) => commit = Some(pending),
                    // Nothing reached the group, so nothing here says it moved on.
                    Err(e) => {
                        start_failure("committing a start offset", &e)?;
                        retry = Some(format!("committing a start offset: {e}"));
                    }
                }
            }
            sent.push(commit);
            kept.push((k, tpl, start));
        }
        let until = Instant::now() + START_COMMIT_TIMEOUT;
        let mut offers = Vec::with_capacity(kept.len());
        let mut starts = Vec::with_capacity(kept.len());
        for ((k, tpl, start), commit) in kept.into_iter().zip(sent) {
            if let Some(commit) = commit
                && let Err(e) = commit.wait(until.saturating_duration_since(Instant::now()))
            {
                if e.rdkafka_error_code() == Some(RDKafkaErrorCode::OperationTimedOut) {
                    self.commit_unanswered = true;
                }
                match start_failure("committing a start offset", &e)? {
                    StartFailure::Discard => {
                        self.discard_refused(k, &tpl)?;
                        continue;
                    }
                    _ => retry = Some(format!("committing a start offset: {e}")),
                }
            }
            offers.push((k, tpl));
            starts.push(start);
        }
        Ok((offers, starts, retry))
    }

    /// Discard consumer `k`'s offer for a start the group has refused.
    fn discard_refused(&mut self, k: usize, tpl: &TopicPartitionList) -> Result<(), SourceError> {
        let generation = self.members[k].group_generation();
        self.discard_offer(k, tpl, generation, None)
    }

    /// Give back every offer of an accept whose start could not be settled, and
    /// pace the next attempt. Nothing was assigned, so the next attempt starts
    /// over from the lookup, which finds any start this one committed.
    fn retry_starts(
        &mut self,
        offers: Vec<(usize, TopicPartitionList)>,
        held: &[usize],
        reassign: bool,
        reason: &str,
    ) -> SourceEvent<KafkaLane> {
        for (k, tpl) in offers {
            if held.contains(&k) {
                self.members[k].phase = Phase::Held(tpl);
            }
        }
        if reassign {
            self.after_drain = Some(AfterDrain::Reassign);
        }
        self.start_retry_at = Some(self.clock.now() + START_RETRY_PACE);
        tracing::warn!(
            error = reason,
            topic = %self.config.topic,
            "could not settle a start position; holding the assignment to retry"
        );
        self.note_assignment_loss(format!("a start position: {reason}"));
        // `note_assignment_loss` keeps no cause before the first assignment, so
        // the startup deadline names this as the last error.
        if !self.saw_first_assignment {
            self.last_consumer_error = Some((Stopwatch::start(&*self.clock), reason.to_owned()));
        }
        SourceEvent::Idle
    }

    /// Whether a paced retry of the starts has yet to come due.
    fn start_retry_pending(&self) -> bool {
        self.start_retry_at.is_some_and(|at| self.clock.now() < at)
    }

    /// The offers that remain once each stale one is answered and dropped, by
    /// [`stale_offers`] over every offer's generation.
    fn discard_stale_offers(
        &mut self,
        offers: Vec<(usize, TopicPartitionList)>,
    ) -> Result<Vec<(usize, TopicPartitionList)>, SourceError> {
        let generations: Vec<(usize, i32)> = offers
            .iter()
            .map(|(k, _)| (*k, self.members[*k].group_generation()))
            .collect();
        let newest = newest_generation(&generations);
        let stale = stale_offers(&generations);
        let mut kept = Vec::with_capacity(offers.len());
        for ((k, tpl), (_, generation)) in offers.into_iter().zip(generations) {
            if stale.contains(&k) {
                self.discard_offer(k, &tpl, generation, Some(newest))?;
            } else {
                kept.push((k, tpl));
            }
        }
        Ok(kept)
    }

    /// Answer consumer `k`'s stale offer with an empty `assign`, the call that
    /// moves it out of librdkafka's wait for an answer: `unassign` leaves it
    /// waiting, and then no later rebalance reaches it. Its next heartbeat
    /// finds it out of the group or in a rebalance, which queues a revocation
    /// and a rejoin. After
    /// a stall past `max.poll.interval.ms` it sends no heartbeat, and the
    /// client's session timeout brings the revocation.
    fn discard_offer(
        &mut self,
        k: usize,
        tpl: &TopicPartitionList,
        generation: i32,
        newest: Option<i32>,
    ) -> Result<(), SourceError> {
        let member = &mut self.members[k];
        member
            .consumer
            .assign(&TopicPartitionList::new())
            .map_err(fatal("assign empty"))?;
        member.rejoin();
        tracing::warn!(
            consumer = k,
            partitions = tpl.count(),
            generation,
            newest,
            "discarded an assignment the group has moved past; this consumer rejoins"
        );
        Ok(())
    }

    /// Under `latest`, every offer's start positions, in offer order. A partition
    /// with a commit starts from it. One with none takes its listed end, which
    /// [`Self::commit_starts`] commits ahead of any read, so every later start
    /// resumes from it and the reset policy never applies to it again.
    ///
    /// Committed offsets belong to the group, so one consumer reads every offer's
    /// partitions with one lookup and one listing, and the cost of an accept does
    /// not grow with the consumer count. `Err` gives the reason to hold the offers
    /// and retry, since librdkafka reads `latest` as `earliest` here and would
    /// start the partition at its beginning.
    ///
    /// # Errors
    ///
    /// A failure [`classify_start_error`] classes as fatal, or an offer naming a
    /// partition of another topic.
    fn read_latest_starts(
        &mut self,
        offers: &[(usize, TopicPartitionList)],
        lookup_until: Instant,
    ) -> Result<Result<Vec<Prepared>, String>, SourceError> {
        // The lists below name the configured topic whatever the offer named, so
        // a partition of another topic would commit a position on this one.
        if !offers
            .iter()
            .all(|(_, tpl)| only_on_topic(tpl, &self.config.topic))
        {
            return Err(SourceError::fatal(format!(
                "the group offered a partition of a topic other than `{}`",
                self.config.topic
            )));
        }
        for (k, _) in offers {
            self.members[*k].committed_at_assign.clear();
        }
        let mut wanted = TopicPartitionList::new();
        for (_, tpl) in offers {
            for elem in tpl.elements() {
                wanted.add_partition(&self.config.topic, elem.partition());
            }
        }
        let Some((reader, _)) = offers.iter().find(|(_, tpl)| tpl.count() > 0) else {
            return Ok(Ok(offers
                .iter()
                .map(|_| Prepared::assigning(TopicPartitionList::new()))
                .collect()));
        };
        let consumer = Arc::clone(&self.members[*reader].consumer);
        let budget = lookup_until.saturating_duration_since(Instant::now());
        if budget.is_zero() {
            return Ok(Err(
                "no time left for the committed offset lookup".to_owned()
            ));
        }
        let committed = match consumer.committed_offsets(wanted, budget) {
            Ok(committed) => committed,
            Err(e) => return read_failure("reading committed offsets", &e),
        };
        let mut starts: FastMap<i32, i64> = FastMap::default();
        let mut ends = TopicPartitionList::new();
        for elem in committed.elements() {
            if let Err(e) = elem.error() {
                return read_failure("reading a committed offset", &e);
            }
            match elem.offset() {
                Offset::Invalid => ends
                    .add_partition_offset(&self.config.topic, elem.partition(), Offset::End)
                    .map_err(fatal("build offset list"))?,
                // rdkafka reads -3 to -999 as `Offset::Offset` too.
                Offset::Offset(offset) if offset >= 0 => {
                    starts.insert(elem.partition(), offset);
                }
                // A commit that names no position: taking it as no commit would
                // commit the end over records nothing read.
                offset => {
                    return Ok(Err(format!(
                        "reading a committed offset: partition {} holds {offset:?}, which names \
                         no position",
                        elem.partition()
                    )));
                }
            }
        }
        let mut listed: FastMap<i32, i64> = FastMap::default();
        if ends.count() > 0 {
            let budget = lookup_until.saturating_duration_since(Instant::now());
            if budget.is_zero() {
                return Ok(Err("no time left to list end offsets".to_owned()));
            }
            if let Err(e) = list_offsets(consumer.client(), &mut ends, budget) {
                return read_failure("listing end offsets", &e);
            }
            for elem in ends.elements() {
                match listed_start(&elem) {
                    Ok(offset) => {
                        listed.insert(elem.partition(), offset);
                    }
                    Err(e) => return read_failure("listing an end offset", &e),
                }
            }
        }
        let mut prepared = Vec::with_capacity(offers.len());
        for (k, tpl) in offers {
            let mut start = Prepared::assigning(TopicPartitionList::new());
            for elem in tpl.elements() {
                let partition = elem.partition();
                let offset = if let Some(offset) = listed.get(&partition) {
                    start
                        .to_commit
                        .add_partition_offset(
                            &self.config.topic,
                            partition,
                            Offset::Offset(*offset),
                        )
                        .map_err(fatal("build offset list"))?;
                    *offset
                } else if let Some(offset) = starts.get(&partition) {
                    *offset
                } else {
                    return Ok(Err(format!("no start read for partition {partition}")));
                };
                start
                    .list
                    .add_partition_offset(&self.config.topic, partition, Offset::Offset(offset))
                    .map_err(fatal("build offset list"))?;
                self.members[*k]
                    .committed_at_assign
                    .insert(partition, offset);
            }
            prepared.push(start);
        }
        Ok(Ok(prepared))
    }

    /// `tpl` with a concrete start offset for each partition, under `earliest` or
    /// `error`, so `assign` starts the fetchers before it returns. A partition
    /// whose committed offset could not be read, or whose reset policy is `error`,
    /// keeps `Offset::Stored` and is started by librdkafka after the call.
    fn start_positions(
        &mut self,
        k: usize,
        tpl: &TopicPartitionList,
        lookup_until: Instant,
    ) -> TopicPartitionList {
        let member = &mut self.members[k];
        member.committed_at_assign.clear();
        let budget = lookup_until.saturating_duration_since(Instant::now());
        if budget.is_zero() {
            tracing::warn!(
                consumer = k,
                topic = %self.config.topic,
                "no time left for the committed offset lookup; librdkafka resolves the start offsets"
            );
            return tpl.clone();
        }
        let committed = match member.consumer.committed_offsets(tpl.clone(), budget) {
            Ok(committed) => committed,
            Err(e) => {
                tracing::warn!(
                    consumer = k,
                    error = %e,
                    topic = %self.config.topic,
                    "committed offset lookup failed; librdkafka resolves the start offsets"
                );
                return tpl.clone();
            }
        };
        let uncommitted = self.config.uncommitted_start().unwrap_or(Offset::Stored);
        for mut elem in committed.elements() {
            if let Err(e) = elem.error() {
                tracing::warn!(
                    consumer = k,
                    partition = elem.partition(),
                    error = %e,
                    "committed offset lookup failed; librdkafka resolves the start offset"
                );
            }
            match start_offset(elem.error(), elem.offset(), uncommitted) {
                Some(start) => {
                    let _ = elem.set_offset(start);
                }
                None => {
                    if let Offset::Offset(offset) = elem.offset() {
                        member.committed_at_assign.insert(elem.partition(), offset);
                    }
                }
            }
        }
        committed
    }

    /// Record the loss of the source's last partitions, opening the window
    /// `assignment_timeout` governs, or naming the latest event when that
    /// window is already open. The instant is the first loss, so a run of
    /// rebalance events with no assignment between them does not extend the
    /// deadline.
    ///
    /// Before the first assignment the wait already runs from `open` under
    /// `startup_timeout`, and a loss there leaves it alone: a source that
    /// has never been assigned anything is starting up, whatever the group
    /// reports in the meantime.
    fn note_assignment_loss(&mut self, cause: String) {
        if !self.saw_first_assignment {
            return;
        }
        match &mut self.assignment_wait {
            Some(wait) => wait.cause = Some(cause),
            None => {
                self.assignment_wait = Some(AssignmentWait {
                    since: self.clock.now(),
                    cause: Some(cause),
                })
            }
        }
    }

    /// Release consumer `k` with `unassign` and expect it to rejoin.
    fn release(&mut self, k: usize) {
        let member = &mut self.members[k];
        if let Err(e) = member.consumer.unassign() {
            tracing::warn!(consumer = k, error = %e, "unassign after drained revocation");
        }
        member.rejoin();
    }

    /// Move every consumer's live lanes to its `revoking` set and return
    /// their ids. Each consumer still owns these partitions until it calls
    /// `unassign` or assigns them again, so the runtime's post-drain final
    /// commit may store their offsets.
    fn revoke_all_live(&mut self) -> Vec<LaneId> {
        let mut lanes = Vec::new();
        for member in &mut self.members {
            for (lane, p) in member.assignment.drain() {
                member.revoking.insert(lane, p);
                lanes.push(lane);
            }
        }
        lanes
    }

    /// Feed the latest librdkafka statistics into the framework lag metrics
    /// and each consumer's `spate_kafka_source_*` families.
    fn publish_stats(&mut self) {
        let reads_committed = self.config.reads_committed();
        for member in &mut self.members {
            let Some(mut stats) = member
                .consumer
                .context()
                .stats
                .lock()
                .expect("stats lock")
                .take()
            else {
                continue;
            };
            fill_unknown_lag(
                &mut stats,
                &self.config.topic,
                &member.committed_at_assign,
                reads_committed,
            );
            // Each consumer's snapshot carries every partition of the topic;
            // only its own are its to publish.
            let owned = member.live_partition_ids();
            if let Some(metrics) = self.metrics.as_ref() {
                publish_lag(&stats, &self.config.topic, &owned, metrics);
            }
            if let Some(stats_metrics) = member.stats.as_mut() {
                stats_metrics.update(&stats, &self.config.topic, &owned);
            }
        }
    }

    /// Poll every consumer once, rewinding any message that reached a main
    /// queue. The first fatal poll error, else the first poll error, is
    /// returned after every consumer has been served, so a failing one does
    /// not starve the others of their rebalance callbacks and
    /// `max.poll.interval.ms`.
    fn poll_consumers(&mut self, timeout: Duration) -> Result<(), SourceError> {
        let n = self.members.len();
        let start = self.poll_start;
        self.poll_start = (start + 1) % n;
        let mut first_error = None;
        for (k, wait) in poll_order(start, n, timeout) {
            let consumer = Arc::clone(&self.members[k].consumer);
            // With all partitions split and choreographed correctly no
            // message should ever surface here. If one does, rewind so it is
            // refetched through its split queue; dropping it would let the
            // watermark commit past an unprocessed record.
            #[cfg(test)]
            let polled = match self.take_injected_poll_error(k) {
                Some(e) => Some(Err(e)),
                None => consumer.poll(wait),
            };
            #[cfg(not(test))]
            let polled = consumer.poll(wait);
            match polled {
                None => {}
                Some(Ok(msg)) => {
                    self.main_queue_rewinds += 1;
                    tracing::warn!(
                        consumer = k,
                        partition = msg.partition(),
                        offset = msg.offset(),
                        total = self.main_queue_rewinds,
                        "message on the main queue; rewinding partition; this client passes no more transaction markers until it restarts"
                    );
                    let tpl = self.tpl_for([msg.partition()]);
                    // Ahead of the seek, and however it ends: the popped record
                    // moved the position, and no lane issued it.
                    consumer.context().mark_rewound();
                    if let Err(e) = consumer.pause(&tpl) {
                        tracing::error!(error = %e, "pause for main-queue rewind failed");
                    }
                    if let Err(e) = consumer.seek(
                        &self.config.topic,
                        msg.partition(),
                        Offset::Offset(msg.offset()),
                        Duration::from_secs(5),
                    ) {
                        tracing::error!(error = %e, "seek for main-queue rewind failed");
                    }
                    if let Err(e) = consumer.resume(&tpl) {
                        tracing::error!(error = %e, "resume for main-queue rewind failed");
                    }
                }
                Some(Err(e)) => {
                    let error = self.poll_error(k, &consumer, &e);
                    keep_reported(&mut first_error, error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Classify consumer `k`'s poll error and record it for the deadline
    /// error. Permanent broker-side failures (authorization revoked, deleted
    /// topic, unsupported protocol, a rejected TLS handshake, a client
    /// librdkafka has failed) must fail fast rather than retry forever behind
    /// a green health probe.
    fn poll_error(
        &mut self,
        k: usize,
        consumer: &BaseConsumer<SourceContext>,
        e: &rdkafka::error::KafkaError,
    ) -> SourceError {
        let code = e.rdkafka_error_code();
        let text = code.and_then(|code| consumer.context().take_error(code));
        let mut error = crate::error::poll_error(
            "consumer poll",
            e,
            self.members[k].saw_first_assignment,
            text.as_deref(),
        );
        // librdkafka's fencing text does not name the instance.
        if let rdkafka::error::KafkaError::MessageConsumptionFatal(
            rdkafka::error::RDKafkaErrorCode::FencedInstanceId,
        ) = e
            && let Some(id) = self.config.rdkafka.get("group.instance.id")
        {
            error = error.map_reason(|mut reason| {
                reason.push_str(&format!(" (group.instance.id {id:?})"));
                reason
            });
        }
        // Every broker's failure is followed by `AllBrokersDown`, which would
        // otherwise hide it from the deadline error.
        if code != Some(rdkafka::error::RDKafkaErrorCode::AllBrokersDown) {
            self.last_consumer_error =
                Some((Stopwatch::start(&*self.clock), error.reason().to_owned()));
        }
        error
    }

    /// The next rebalance intent to act on and its consumer: a revocation or
    /// an error on any consumer before any assignment. Each consumer's own
    /// intents are taken in order, and order across consumers follows the
    /// index.
    fn next_intent(&self) -> Option<(usize, Intent)> {
        let mut assign = None;
        for (k, member) in self.members.iter().enumerate() {
            let ctx = member.consumer.context();
            let mut intents = ctx.intents.lock().expect("intent lock");
            // A pileup means rebalances arrived faster than they completed,
            // which is the precondition under which a stale intent *could* be
            // acted on after the group moved past it, though some shapes are
            // benign (an error with the fresh rejoin assignment already
            // queued behind it). It is surfaced loudly either way.
            if intents.len() > 1 {
                let queued: Vec<&'static str> = intents.iter().map(Intent::kind).collect();
                tracing::warn!(
                    consumer = k,
                    queued = ?queued,
                    "rebalance intents piled up; completing one per poll"
                );
            }
            match intents.front() {
                Some(Intent::Assign(_)) => {
                    if assign.is_none() {
                        assign = Some(k);
                    }
                }
                Some(_) => return intents.pop_front().map(|i| (k, i)),
                None => {}
            }
        }
        let k = assign?;
        let intent = self.members[k]
            .consumer
            .context()
            .intents
            .lock()
            .expect("intent lock")
            .pop_front()?;
        Some((k, intent))
    }

    /// Act on consumer `k`'s rebalance intent.
    fn on_intent(
        &mut self,
        k: usize,
        intent: Intent,
    ) -> Result<SourceEvent<KafkaLane>, SourceError> {
        match intent {
            Intent::Assign(tpl) => {
                // Eager assignment replaces whatever the consumer held.
                self.members[k].revoking.clear();
                self.members[k].revoke_arrived = false;
                self.members[k].phase = Phase::Held(tpl);
                if self.hold_since.is_none() {
                    self.hold_since = Some(self.clock.now());
                }
                Ok(SourceEvent::Idle)
            }
            Intent::Revoke(_) => {
                // Eager rebalancing revokes every member that has answered its
                // last assignment, so every such consumer of this source is
                // revoked in this rebalance. Draining them all on the first
                // revocation keeps a rebalance to one drain: draining each on
                // its own event
                // would leave the later consumers unrevoked, and the group
                // waiting for them, through every earlier drain.
                let lanes = self.revoke_all_live();
                for member in &mut self.members {
                    if !matches!(member.phase, Phase::Held(_)) {
                        member.phase = Phase::Joining;
                    }
                }
                if lanes.is_empty() {
                    // Nothing live: this consumer's partitions were drained
                    // with an earlier consumer's revocation, or it held none.
                    self.release(k);
                    self.note_assignment_loss("a revocation".to_owned());
                    return Ok(SourceEvent::Idle);
                }
                // The lag series are deliberately NOT pruned here. These
                // partitions are still being drained and committed, and an
                // eager rebalance revokes everything before handing most of
                // it back, so zeroing now would blank the whole family for a
                // rebalance that changed nothing. The prune happens once the
                // new assignment is accepted.
                self.members[k].revoke_arrived = true;
                self.after_drain = Some(AfterDrain::Release);
                let barrier = DrainBarrier::new(lanes.len());
                Ok(SourceEvent::LanesRevoked { lanes, barrier })
            }
            Intent::Error(code) => {
                // The callback already released consumer `k`'s ownership with
                // `unassign` (the contract for the arbitrary-error event), so
                // it holds nothing: each of its live lanes is dead and must be
                // drained by the runtime before the error is reported. Unlike
                // an ordinary revocation, ownership is already gone, so
                // `commit` refuses the drained watermarks and that work
                // replays; delivery stays at-least-once. The other consumers
                // keep their lanes.
                let member = &mut self.members[k];
                let lanes: Vec<LaneId> = member.assignment.keys().copied().collect();
                member.assignment.clear();
                member.rejoin();
                if self.holds_no_lanes() {
                    self.note_assignment_loss(format!("rebalance error: {code}"));
                }
                self.prune_partition_series();
                if lanes.is_empty() {
                    return Err(self.rebalance_error(k, code));
                }
                self.pending_error = Some((k, code));
                let barrier = DrainBarrier::new(lanes.len());
                Ok(SourceEvent::LanesRevoked { lanes, barrier })
            }
        }
    }

    /// Accept the held assignments once every consumer of the rebalance has
    /// one, or once `ASSIGN_HOLD` has passed. One that arrives while lanes are
    /// live starts a new epoch instead: every live lane is revoked, and once
    /// drained every consumer's partitions are accepted again together, since
    /// the runtime admits a lane set only as a whole. [`Self::accept_held`]
    /// states what happens to offers that overlap.
    fn settle_held(&mut self) -> Result<SourceEvent<KafkaLane>, SourceError> {
        // Waiting on the pace. A reassign still armed here came due during this
        // call's poll, and runs at the top of the next: accepting these offers
        // alone would drop the other consumers'.
        if self.start_retry_pending() || self.after_drain.is_some() {
            return Ok(SourceEvent::Idle);
        }
        if !self
            .members
            .iter()
            .any(|m| matches!(m.phase, Phase::Held(_)))
        {
            return Ok(SourceEvent::Idle);
        }
        if !self.holds_no_lanes() {
            let lanes = self.revoke_all_live();
            tracing::warn!(
                lanes = lanes.len(),
                "an assignment arrived while other consumers held lanes; \
                 draining them to admit every consumer's partitions together"
            );
            self.after_drain = Some(AfterDrain::Reassign);
            let barrier = DrainBarrier::new(lanes.len());
            return Ok(SourceEvent::LanesRevoked { lanes, barrier });
        }
        let joining = self
            .members
            .iter()
            .any(|m| matches!(m.phase, Phase::Joining));
        let held_for = self.hold_since.map_or(Duration::ZERO, |since| {
            self.clock.now().saturating_duration_since(since)
        });
        if joining && held_for < ASSIGN_HOLD {
            return Ok(SourceEvent::Idle);
        }
        self.accept_held(false)
    }

    /// The injected error for consumer `k`'s poll, removed so it is returned
    /// once.
    #[cfg(test)]
    fn take_injected_poll_error(&mut self, k: usize) -> Option<rdkafka::error::KafkaError> {
        self.injected_poll_error
            .take_if(|(consumer, _)| *consumer == k)
            .map(|(_, e)| e)
    }

    /// Partitions consumer `k` may still store offsets for: its live
    /// assignment plus partitions being revoked but not yet released.
    #[cfg(test)]
    fn committable_partitions(&self, k: usize) -> Vec<i32> {
        let member = &self.members[k];
        member
            .assignment
            .values()
            .chain(member.revoking.values())
            .copied()
            .collect()
    }
}

/// The start offset to assign for one partition of a committed-offset
/// lookup, or `None` to keep the committed offset it returned. A partition
/// whose lookup failed gets `Offset::Stored`, never the reset policy.
pub(crate) fn start_offset(
    lookup: KafkaResult<()>,
    committed: Offset,
    uncommitted: Offset,
) -> Option<Offset> {
    match (lookup, committed) {
        (Ok(()), Offset::Offset(_)) => None,
        (Ok(()), Offset::Invalid) => Some(uncommitted),
        _ => Some(Offset::Stored),
    }
}

/// Whether every element of `tpl` is a distinct partition of `topic`. It looks
/// each one up through the list, since `elem.topic()` panics on a name that is
/// not UTF-8, and a group's reply can carry one.
fn only_on_topic(tpl: &TopicPartitionList, topic: &str) -> bool {
    let mut seen = FastSet::default();
    tpl.elements().iter().all(|elem| {
        seen.insert(elem.partition()) && tpl.find_partition(topic, elem.partition()).is_some()
    })
}

/// The start offset a listing names for one partition: only a concrete offset,
/// and never a negative one, which `assign` and a commit would read as logical.
///
/// # Errors
///
/// The element's failure, or `OffsetNotAvailable` for anything else.
fn listed_start(elem: &TopicPartitionListElem<'_>) -> KafkaResult<i64> {
    start_from(listed_offset(elem)?)
}

/// `offset` when it can start a partition. rdkafka reads a negative offset from
/// a reply as concrete, and refuses it in a list.
fn start_from(offset: i64) -> KafkaResult<i64> {
    if offset >= 0 {
        Ok(offset)
    } else {
        Err(KafkaError::MetadataFetch(
            RDKafkaErrorCode::OffsetNotAvailable,
        ))
    }
}

/// What a failure of `doing` means for the offer being settled.
///
/// # Errors
///
/// A code [`classify_start_error`] classes as fatal, naming `doing`.
fn start_failure(doing: &str, e: &KafkaError) -> Result<StartFailure, SourceError> {
    match e.rdkafka_error_code().map(classify_start_error) {
        Some(StartFailure::Fatal) => Err(SourceError::fatal(format!("{doing}: {e}"))),
        Some(failure) => Ok(failure),
        None => Ok(StartFailure::Retry),
    }
}

/// A failed read of start positions: fatal, or the reason to retry. A read names
/// no member, so a code that would discard one offer retries them all.
fn read_failure<T>(doing: &str, e: &KafkaError) -> Result<Result<T, String>, SourceError> {
    start_failure(doing, e)?;
    Ok(Err(format!("{doing}: {e}")))
}

/// Fill `consumer_lag` for a partition librdkafka reports as unknown, from
/// the committed offset read before its assignment, when that offset lies in
/// `0..=end`. librdkafka's own figure takes over once it has one, after this
/// member's first commit.
fn fill_unknown_lag(
    stats: &mut Statistics,
    topic: &str,
    committed: &FastMap<i32, i64>,
    reads_committed: bool,
) {
    let Some(topic) = stats.topics.get_mut(topic) else {
        return;
    };
    for (pid, p) in &mut topic.partitions {
        let end = if reads_committed {
            p.ls_offset
        } else {
            p.hi_offset
        };
        if p.consumer_lag < 0
            && let Some(&committed) = committed.get(pid)
            && (0..=end).contains(&committed)
        {
            p.consumer_lag = end - committed;
        }
    }
}

/// Translate one statistics snapshot into the framework's per-partition
/// consumer-lag series.
///
/// Free function rather than a method so it is reachable from a unit test:
/// `publish_stats` needs a live consumer, and this translation rendered a
/// permanent zero for as long as it went untested.
///
/// librdkafka reports `consumer_lag = -1` while the lag is unknown: before
/// the first commit, and for any partition whose leader has not answered yet
/// (`consumer_lag` is `(hi_offset or ls_offset) - committed_offset`; see the
/// librdkafka `STATISTICS.md`). Those partitions are skipped rather than
/// published as `0`, which would be indistinguishable from "caught up": a
/// maximally backlogged consumer would report no lag and every alert keyed on
/// it would stay green. A partition that has never reported a number is
/// therefore absent from the exposition, and one that reported before keeps
/// its last value.
///
/// `owned` restricts publication to the live assignment. The snapshot carries
/// every partition the client holds metadata for, so without this filter a
/// partition that moved to another member would keep being refreshed here and
/// a `sum` across the family would exceed *this member's* backlog. The filter
/// is one half of that; the other is
/// [`SourceMetrics::retain_partitions`](spate_core::metrics::SourceMetrics::retain_partitions),
/// which zeroes what the member lost. The exporter cannot delete a series,
/// so a partition left alone renders its last value forever.
// ANCHOR: lag
fn publish_lag(stats: &Statistics, topic: &str, owned: &[PartitionId], metrics: &SourceMetrics) {
    let Some(topic) = stats.topics.get(topic) else {
        return;
    };
    for (pid, p) in &topic.partitions {
        if p.consumer_lag >= 0
            && let Ok(part) = u32::try_from(*pid)
            && owned.contains(&PartitionId(part))
        {
            metrics.set_partition_lag(
                PartitionId(part),
                u64::try_from(p.consumer_lag).unwrap_or(0),
            );
        }
    }
}
// ANCHOR_END: lag

/// The fatal error for a member that has held no partitions for `waited`.
/// `None` while it is still inside the deadline, and whenever that deadline
/// is zero, which disables it.
///
/// `cause` selects the window. `None` is the member's first assignment,
/// which `startup_timeout` bounds and whose error names the topic and the
/// brokers, the two things a pipeline that never joins usually has wrong.
/// `Some` is a member that had partitions and lost them, which
/// `assignment_timeout` bounds and whose error names the group and the
/// event that took them.
///
/// `last_error` is the latest consumer error and its age when the error
/// is reported, which either message ends with.
///
/// Free function taking the elapsed time rather than a method reading the
/// clock, so a unit test pins the boundaries and the messages directly. The
/// same reason `publish_lag` is one.
fn assignment_deadline_error(
    config: &KafkaSourceConfig,
    waited: Duration,
    cause: Option<&str>,
    last_error: Option<(Duration, &str)>,
) -> Option<SourceError> {
    let deadline = match cause {
        None => config.startup_timeout,
        Some(_) => config.assignment_timeout,
    };
    if deadline.is_zero() || waited <= deadline {
        return None;
    }
    let mut reason = match cause {
        None => format!(
            "no partition assignment within {waited:?} \
             (topic {:?}, brokers {:?})",
            config.topic, config.brokers
        ),
        Some(cause) => format!(
            "no partition assignment for {waited:?} after {cause} \
             (group {:?}, topic {:?})",
            config.group_id, config.topic
        ),
    };
    if let Some((age, error)) = last_error {
        reason.push_str(&format!("; last consumer error {age:?} ago: {error}"));
    }
    Some(SourceError::fatal(reason))
}

fn fatal(what: &'static str) -> impl Fn(rdkafka::error::KafkaError) -> SourceError {
    move |e| SourceError::fatal(format!("{what}: {e}"))
}

impl Source for KafkaSource {
    type Lane = KafkaLane;

    fn component_type(&self) -> &str {
        "kafka"
    }

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        if !self.members.is_empty() {
            return Err(SourceError::fatal("open() called twice"));
        }
        // Enforce the passthrough guard (and the whole denylist) before any
        // client is created. The sink's choke point is `build()`; `open()` is
        // the source's, catching programmatic construction via
        // `KafkaSource::new` that bypasses `from_component_config`'s validation.
        self.config
            .validate()
            .map_err(|e| SourceError::fatal(e.to_string()))?;
        self.metrics = ctx.stage_metrics.clone();
        if self.config.statistics_interval.is_zero() {
            // Consumer lag is derived from the statistics snapshot and has no
            // other source, so disabling statistics removes a golden signal
            // outright. The series is then absent rather than frozen at a
            // `0` that reads as "caught up", but the absence must not be
            // silent.
            tracing::warn!(
                topic = %self.config.topic,
                "statistics disabled (statistics_interval: 0s): consumer lag \
                 and the spate_kafka_source_* families will not be published"
            );
        }
        let mut members = Vec::new();
        members
            .try_reserve_exact(self.config.consumers)
            .map_err(|_| {
                SourceError::fatal(format!(
                    "source.kafka.consumers: {} consumers cannot be allocated",
                    self.config.consumers
                ))
            })?;
        for k in 0..self.config.consumers {
            let role = crate::config::Consumer::member(&self.config, k);
            // Resolve the connector-owned metric handles once, before the
            // poll loop, and only when statistics are enabled. With
            // `statistics_interval: 0s` librdkafka never emits a snapshot, so
            // registering the families would leave them frozen at their unset
            // default forever (e.g. `group_healthy 0`, a documented alert
            // signal). `absolute()`-mapped counters are scoped to this
            // consumer's lifetime, which is sound because open() creates each
            // consumer exactly once (see the `metrics` module docs).
            let stats = if self.config.statistics_interval.is_zero() {
                None
            } else {
                ctx.meter.as_ref().map(|m| {
                    KafkaStatsMetrics::for_source_consumer(
                        m.clone(),
                        ctx.per_partition_detail,
                        role.1,
                    )
                })
            };
            let consumer: BaseConsumer<SourceContext> = self
                .config
                .base(crate::security::openssl_env_overrides())
                .create(&role, SourceHooks::default())
                .map_err(|reason| SourceError::fatal(format!("create consumer: {reason}")))?;
            consumer
                .subscribe(&[&self.config.topic])
                .map_err(fatal("subscribe"))?;
            members.push(Member {
                consumer: Arc::new(consumer),
                stats,
                assignment: FastMap::default(),
                revoking: FastMap::default(),
                revoke_arrived: false,
                saw_first_assignment: false,
                phase: Phase::Joining,
                committed_at_assign: FastMap::default(),
                watermarks: FastMap::default(),
                #[cfg(test)]
                generation: None,
            });
        }
        self.members = members;
        self.issuer = Some(ctx.issuer);
        // The startup window opens here: no assignment yet, so
        // `startup_timeout` governs until the first one arrives.
        self.assignment_wait = Some(AssignmentWait {
            since: self.clock.now(),
            cause: None,
        });
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<KafkaLane>, SourceError> {
        self.opened()?;
        // The assignment deadline first, so its error is reported in place of
        // anything the polls below return. With unreachable brokers each of
        // them reports a Retryable transport error, and a misconfigured
        // pipeline would retry forever instead of failing fast.
        if let Some(wait) = &self.assignment_wait
            && let Some(e) = assignment_deadline_error(
                &self.config,
                self.clock.now().saturating_duration_since(wait.since),
                wait.cause.as_deref(),
                self.last_consumer_error
                    .as_ref()
                    .map(|(at, error)| (at.elapsed(&*self.clock), error.as_str())),
            )
        {
            return Err(e);
        }

        // Complete what the last surfaced revocation deferred: the runtime
        // has finished draining and committing by the time it calls
        // poll_events again.
        match self.after_drain.take() {
            Some(AfterDrain::Release) => {
                for k in 0..self.members.len() {
                    if self.members[k].revoke_arrived {
                        self.release(k);
                    }
                }
                if self.holds_no_lanes() {
                    self.note_assignment_loss("a revocation".to_owned());
                }
            }
            Some(AfterDrain::Reassign) if !self.start_retry_pending() => {
                return self.accept_held(true);
            }
            // That arm returns without polling, and the group needs the polls
            // while the retry waits.
            Some(AfterDrain::Reassign) => self.after_drain = Some(AfterDrain::Reassign),
            None => {}
        }

        // A rebalance error whose lanes were surfaced as revoked on the
        // previous call: the runtime has drained them, and ownership was
        // already released in the callback. Report the classified error;
        // librdkafka rejoins on its own and a fresh assignment follows.
        if let Some((k, code)) = self.pending_error.take() {
            return Err(self.rebalance_error(k, code));
        }

        // A fatal poll error returns before any intent is acted on. A
        // retryable one is already recorded and logged, so an intent's event
        // or error takes its place, and one consumer's transport errors do
        // not hold back another consumer's rebalance.
        let polled = match self.poll_consumers(timeout) {
            Err(e) if e.class() == ErrorClass::Fatal => return Err(e),
            polled => polled,
        };
        self.publish_stats();

        // Rebalance intents recorded by the callbacks during the polls above
        // (or earlier ones). One per call: a revocation needs its runtime
        // choreography to complete before the next intent may be acted on.
        if let Some((k, intent)) = self.next_intent() {
            let event = self.on_intent(k, intent)?;
            if !matches!(event, SourceEvent::Idle) {
                return Ok(event);
            }
        }
        match self.settle_held()? {
            SourceEvent::Idle => polled.map(|()| SourceEvent::Idle),
            event => Ok(event),
        }
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        if watermarks.is_empty() {
            return Ok(());
        }
        self.opened()?;
        // Each watermark goes to the consumer that owns its partition: its
        // live assignment, or a partition being revoked but not yet released
        // by `unassign`. The revocation choreography drains and commits those
        // partitions while ownership is still valid. Dropping them here
        // would silently lose the offsets the drain produced, replaying that
        // work after the move. librdkafka refuses a store for a partition the
        // consumer does not hold without failing the call, so a watermark
        // sent to the wrong consumer would be lost the same way.
        let mut lists: Vec<TopicPartitionList> = (0..self.members.len())
            .map(|_| TopicPartitionList::new())
            .collect();
        let mut refused = 0;
        for (p, offset) in watermarks {
            match self.owner(*p) {
                Some((k, partition)) => {
                    if let Some(watermark) = self.members[k].watermarks.get(&partition) {
                        watermark.store(*offset, Ordering::Release);
                    }
                    lists[k]
                        .add_partition_offset(
                            &self.config.topic,
                            partition,
                            Offset::Offset(*offset),
                        )
                        .map_err(fatal("build offset list"))?;
                }
                None => {
                    refused += 1;
                    tracing::debug!(
                        partition = p.0,
                        offset,
                        "skipping store for partition no longer owned"
                    );
                }
            }
        }
        if refused > 0 {
            // Normal in one situation: a drain after a consumer's ownership
            // was already released (a rebalance-error revocation), where the
            // drained work replays. The other consumers' watermarks ride in
            // the same call and are stored, so the call succeeds, and without
            // this line the refusal is invisible outside per-partition DEBUG.
            tracing::warn!(
                refused,
                "refusing to store watermarks for partitions no longer owned; \
                 their work will replay"
            );
        }
        if refused == watermarks.len() {
            return Ok(());
        }
        for (member, tpl) in self.members.iter().zip(&lists) {
            if tpl.count() == 0 {
                continue;
            }
            member
                .consumer
                .store_offsets(tpl)
                .map_err(|e| SourceError::retryable(format!("store offsets: {e}")))?;
            if let Some(elem) = tpl.elements().into_iter().find(|e| e.error().is_err()) {
                return Err(SourceError::retryable(format!(
                    "store offset for partition {}: {}",
                    elem.partition(),
                    elem.error()
                        .err()
                        .map_or_else(String::new, |e| e.to_string())
                )));
            }
        }
        Ok(())
    }

    fn flush_commits(&mut self) -> Result<(), SourceError> {
        self.opened()?;
        // Every consumer commits, even after one fails, so one consumer's
        // failure does not hold back the others' stored offsets.
        let mut first_error = None;
        for (k, member) in self.members.iter().enumerate() {
            match member
                .consumer
                .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
            {
                // Nothing stored since the last commit: not an error.
                Ok(())
                | Err(rdkafka::error::KafkaError::ConsumerCommit(
                    rdkafka::error::RDKafkaErrorCode::NoOffset,
                )) => {}
                Err(e) => {
                    // With one consumer the controller's line on the returned
                    // error says the same.
                    if self.members.len() > 1 {
                        tracing::warn!(
                            consumer = k,
                            error = %e,
                            "sync commit failed; this consumer's stored offsets may replay"
                        );
                    }
                    first_error.get_or_insert(SourceError::retryable(format!("sync commit: {e}")));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        self.for_owning_consumers(lanes, "pause", |consumer, tpl| consumer.pause(tpl))
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        self.for_owning_consumers(lanes, "resume", |consumer, tpl| consumer.resume(tpl))
    }
}

impl KafkaSource {
    /// Apply `op` to each consumer with the partitions of `lanes` it holds
    /// live. librdkafka accepts a pause or resume for a partition the
    /// consumer does not hold and does nothing, so each partition must reach
    /// its own consumer.
    fn for_owning_consumers(
        &self,
        lanes: &[LaneId],
        what: &'static str,
        op: impl Fn(&BaseConsumer<SourceContext>, &TopicPartitionList) -> KafkaResult<()>,
    ) -> Result<(), SourceError> {
        for member in &self.members {
            let tpl = self.tpl_for(
                lanes
                    .iter()
                    .filter_map(|l| member.assignment.get(l).copied()),
            );
            if tpl.count() == 0 {
                continue;
            }
            op(&member.consumer, &tpl)
                .map_err(|e| SourceError::retryable(format!("{what}: {e}")))?;
        }
        Ok(())
    }
}

/// The events waiting on `consumer`'s queue, of every kind.
fn queued_events(consumer: &BaseConsumer<SourceContext>) -> usize {
    let rk = consumer.client().native_ptr();
    // SAFETY: `consumer` keeps `rk` live for the call. `BaseConsumer::new`
    // refuses a client with no consumer queue when `group.id` is set, and
    // `KafkaSourceConfig` always sets it, so the handle is not null. The
    // handle is released before return.
    unsafe {
        let q = rdkafka::bindings::rd_kafka_queue_get_consumer(rk);
        let n = rdkafka::bindings::rd_kafka_queue_length(q);
        rdkafka::bindings::rd_kafka_queue_destroy(q);
        n
    }
}

/// Serves as many events as `consumer`'s queue holds at the time of the call.
///
/// One poll serves one event of any kind and cannot report an empty queue, so
/// the count comes from one read of the queue's length, and the serve cannot
/// follow a growing queue. librdkafka sorts log and statistics events ahead of
/// a rebalance, so one arriving during the serve takes a poll of the count and
/// can leave a rebalance for close.
fn serve_queued(consumer: &BaseConsumer<SourceContext>) {
    for _ in 0..queued_events(consumer) {
        // A served message stores no offset, since the source owns
        // `enable.auto.offset.store`, so it is dropped with no rewind. The error
        // hook has logged an error.
        let _ = consumer.poll(Duration::ZERO);
    }
}

/// Sets every consumer's context to complete revocations in the callback and
/// leave assignments to close, then serves the events queued on each consumer
/// and releases it with `unassign`.
///
/// Consumer close, inside `BaseConsumer::drop`, polls until the group
/// protocol completes and serves any rebalance still queued through the
/// callback. A deferred intent there would never be completed, so
/// `closing` is set first. Close also completes an assignment or a
/// revocation the source took and has not acknowledged, unless librdkafka
/// has marked that assignment lost. A lost revocation waits for the
/// application's `unassign`, and close would poll forever without it.
/// Under eager assignment `unassign` is valid in any group state, so
/// releasing a consumer with nothing pending costs nothing.
///
/// A queued assignment is served with no `assign`, and close completes it as
/// it does a held one. `unassign` answers a queued revocation without serving
/// its event, which would leave close to serve it as the group terminates,
/// where the callback can wait forever on a reply. Each consumer's queue is
/// served first, and a rebalance queued later stays close's to serve.
impl Drop for KafkaSource {
    fn drop(&mut self) {
        for member in &self.members {
            member
                .consumer
                .context()
                .closing
                .store(true, Ordering::Release);
        }
        for (k, member) in self.members.iter().enumerate() {
            // After `unassign`, a queued revocation is a stale event for close.
            serve_queued(&member.consumer);
            if let Err(e) = member.consumer.unassign() {
                tracing::warn!(consumer = k, error = %e, "unassign during source teardown failed");
            }
        }
        if !self.commit_unanswered {
            return;
        }
        // Close waits for an unanswered commit with no bound, so it runs off this
        // thread and the pipeline still ends when its deadline says so.
        let members = std::mem::take(&mut self.members);
        let (closed, wait) = mpsc::channel();
        #[expect(
            clippy::disallowed_methods,
            reason = "joining would wait out the silence; the thread ends when close does"
        )]
        let spawned = std::thread::Builder::new()
            .name("spate-kafka-close".to_owned())
            .spawn(move || {
                drop(members);
                let _ = closed.send(());
            });
        match spawned {
            Ok(_) => {
                if wait.recv_timeout(START_COMMIT_TIMEOUT).is_err() {
                    tracing::warn!(
                        topic = %self.config.topic,
                        "a start commit has no answer; its consumer closes in the background"
                    );
                }
            }
            Err(e) => tracing::warn!(error = %e, "could not start the consumers' close"),
        }
    }
}

#[cfg(test)]
#[expect(
    clippy::disallowed_types,
    reason = "rdkafka's `Statistics` fields are std maps"
)]
mod tests {
    use super::*;
    use crate::testutil::mock::MockKafka;
    use rdkafka::error::RDKafkaErrorCode;
    use spate_core::checkpoint::Checkpointer;
    use spate_core::clock::TestClock;

    /// Put `source` on a frozen clock, and return the clock.
    fn freeze(source: &mut KafkaSource) -> Arc<TestClock> {
        let clock = TestClock::frozen();
        source.clock = SyncClock::new(Arc::clone(&clock) as _);
        clock
    }

    fn test_config() -> KafkaSourceConfig {
        KafkaSourceConfig {
            brokers: "localhost:9092".into(),
            topic: "orders".into(),
            group_id: "test".into(),
            consumers: 1,
            commit_interval: Duration::from_secs(5),
            startup_timeout: Duration::from_secs(30),
            assignment_timeout: Duration::from_mins(5),
            statistics_interval: Duration::ZERO,
            // These tests accept offers against a refused broker, which under
            // the default `latest` would retry its committed offset lookup.
            rdkafka: std::collections::BTreeMap::from([(
                "auto.offset.reset".to_string(),
                "earliest".to_string(),
            )]),
        }
    }

    /// `open()` runs the TLS guard on a source made with `new()`, which skips
    /// `from_component_config`: without the `tls` feature it rejects a security
    /// passthrough before creating a client, and with it the consumer is
    /// created.
    #[test]
    fn open_enforces_tls_guard_on_programmatic_source() {
        use spate_core::checkpoint::Checkpointer;
        let mut config = test_config();
        config
            .rdkafka
            .insert("security.protocol".into(), "ssl".into());
        let mut source = KafkaSource::new(config);
        let cp = Checkpointer::new();
        let result = source.open(SourceCtx::new(cp.handle()));
        if cfg!(feature = "tls") {
            result.expect("tls build: open succeeds");
        } else {
            let msg = result
                .expect_err("non-tls build: open rejects the security config")
                .to_string();
            assert!(msg.contains("kafka-tls"), "actionable: {msg}");
            assert!(msg.contains("source.kafka.rdkafka"), "scoped: {msg}");
        }
    }

    /// A rejected passthrough value is absent from the consumer-creation
    /// error.
    #[test]
    fn open_omits_a_rejected_passthrough_value() {
        use spate_core::checkpoint::Checkpointer;
        let mut config = test_config();
        config
            .rdkafka
            .insert("partition.assignment.strategy".into(), "hunter2".into());
        let mut source = KafkaSource::new(config);
        let cp = Checkpointer::new();
        let err = source
            .open(SourceCtx::new(cp.handle()))
            .expect_err("librdkafka rejects the strategy");
        assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
        let msg = err.to_string();
        assert!(msg.contains("create consumer"), "{msg}");
        assert!(!msg.contains("hunter2"), "{msg}");
    }

    /// `open()` refuses a sub-millisecond `statistics_interval` on a source
    /// made with `new()`, as a fatal error, before it creates a consumer.
    /// Regression for #1528.
    #[test]
    fn open_refuses_a_sub_millisecond_statistics_interval() {
        let mut config = test_config();
        config.statistics_interval = Duration::from_micros(500);
        let mut source = KafkaSource::new(config);
        let cp = Checkpointer::new();
        let err = source
            .open(SourceCtx::new(cp.handle()))
            .expect_err("open accepted a 500us statistics_interval");
        assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
        let reason = err.reason();
        assert!(
            reason.contains("source.kafka.statistics_interval"),
            "{reason}"
        );
    }

    /// `open()` refuses a sub-millisecond `commit_interval` on a source made
    /// with `new()`, as a fatal error that names the key and no passthrough
    /// value, before it creates a consumer. Regression for #1661.
    #[test]
    fn open_refuses_a_sub_millisecond_commit_interval() {
        let mut config = test_config();
        config.commit_interval = Duration::from_micros(500);
        config.rdkafka.insert("client.id".into(), "hunter2".into());
        let mut source = KafkaSource::new(config);
        let cp = Checkpointer::new();
        let err = source
            .open(SourceCtx::new(cp.handle()))
            .expect_err("open accepted a 500us commit_interval");
        assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
        let reason = err.reason();
        assert!(reason.contains("source.kafka.commit_interval"), "{reason}");
        assert!(!reason.contains("hunter2"), "{reason}");
    }

    /// Reproduces the assignment bookkeeping of a partial revocation on
    /// consumer `k`: lanes for the revoked partitions move from `assignment`
    /// into `revoking`.
    fn revoke_lanes(source: &mut KafkaSource, k: usize, revoked: &[i32]) {
        let member = &mut source.members[k];
        let lanes: Vec<LaneId> = member
            .assignment
            .iter()
            .filter(|(_, p)| revoked.contains(p))
            .map(|(l, _)| *l)
            .collect();
        for lane in &lanes {
            if let Some(p) = member.assignment.remove(lane) {
                member.revoking.insert(*lane, p);
            }
        }
    }

    /// After a revocation the offsets of the partitions being revoked must
    /// still be committable, since they are drained and committed while the
    /// member still owns them, while truly unowned partitions stay filtered
    /// out.
    #[test]
    fn committable_partitions_include_revoking_until_released() {
        let mut source = opened_source(&[(0, 0), (1, 1), (2, 2), (3, 3)]);

        revoke_lanes(&mut source, 0, &[2, 3]);

        let mut owned = source.committable_partitions(0);
        owned.sort_unstable();
        assert_eq!(
            owned,
            vec![0, 1, 2, 3],
            "revoked partitions stay committable until unassign releases them"
        );

        // Releasing the revocation (what `unassign` completion does) removes
        // them: a late commit for a released partition is refused.
        source.members[0].revoking.clear();
        let mut owned = source.committable_partitions(0);
        owned.sort_unstable();
        assert_eq!(owned, vec![0, 1]);
    }

    /// An opened source against a broker that refuses connections, with `lanes`
    /// pre-seeded into the assignment map.
    fn opened_source(lanes: &[(u32, i32)]) -> KafkaSource {
        opened_source_with(test_config(), lanes)
    }

    /// [`opened_source`] with a configuration of the test's own. The lanes
    /// go to consumer 0.
    fn opened_source_with(cfg: KafkaSourceConfig, lanes: &[(u32, i32)]) -> KafkaSource {
        opened_consumers(cfg, &[lanes])
    }

    /// A source of `consumers` from `open`, against a broker that refuses
    /// connections, on a frozen clock, with no assignment yet.
    fn opened_unassigned(
        mut cfg: KafkaSourceConfig,
        consumers: usize,
    ) -> (KafkaSource, Arc<TestClock>) {
        cfg.brokers = crate::testutil::refused_broker();
        cfg.consumers = consumers;
        let mut source = KafkaSource::new(cfg);
        let clock = freeze(&mut source);
        let cp = Checkpointer::new();
        source.open(SourceCtx::new(cp.handle())).expect("open");
        (source, clock)
    }

    /// An opened source with one consumer per entry of `lanes`, each with
    /// its entry's lanes pre-seeded and its assignment accepted.
    fn opened_consumers(cfg: KafkaSourceConfig, lanes: &[&[(u32, i32)]]) -> KafkaSource {
        let (mut source, _) = opened_unassigned(cfg, lanes.len());
        // A source that has had an assignment: past the startup window, so
        // `assignment_timeout` governs what follows.
        source.saw_first_assignment = true;
        source.assignment_wait = None;
        for (member, lanes) in source.members.iter_mut().zip(lanes) {
            member.saw_first_assignment = true;
            member.phase = Phase::Assigned;
            for &(lane, part) in *lanes {
                member.assignment.insert(LaneId(lane), part);
                source.next_lane = source.next_lane.max(lane + 1);
            }
        }
        source
    }

    /// Call `poll_events` once and assert that it yielded nothing: `Idle`,
    /// or the retryable `consumer poll` error of the refused broker.
    fn poll_quiet(source: &mut KafkaSource) {
        match source.poll_events(Duration::ZERO) {
            Ok(SourceEvent::Idle) => {}
            Err(e)
                if e.class() == ErrorClass::Retryable
                    && e.reason().starts_with("consumer poll") => {}
            other => panic!("expected nothing to happen, got {other:?}"),
        }
    }

    fn lane_ids(lanes: &[LaneId]) -> Vec<u32> {
        let mut ids: Vec<u32> = lanes.iter().map(|l| l.0).collect();
        ids.sort_unstable();
        ids
    }

    fn sorted(mut partitions: Vec<i32>) -> Vec<i32> {
        partitions.sort_unstable();
        partitions
    }

    fn tpl_of(partitions: &[i32]) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for p in partitions {
            tpl.add_partition("orders", *p);
        }
        tpl
    }

    /// [`test_config`] under `latest`.
    fn latest_config() -> KafkaSourceConfig {
        let mut cfg = test_config();
        cfg.rdkafka
            .insert("auto.offset.reset".to_string(), "latest".to_string());
        cfg
    }

    /// With no time left, `latest` holds the offer, since leaving the start to
    /// librdkafka would start the partition at its end. `earliest` leaves it.
    #[test]
    fn no_budget_leaves_the_offer_held() {
        let (mut source, _clock) = opened_unassigned(latest_config(), 1);
        let offers = [(0, tpl_of(&[0]))];
        assert!(matches!(
            source.read_latest_starts(&offers, Instant::now()),
            Ok(Err(_))
        ));
        let (mut source, _clock) = opened_unassigned(test_config(), 1);
        let list = source.start_positions(0, &offers[0].1, Instant::now());
        assert_eq!(
            list.elements()[0].offset(),
            offers[0].1.elements()[0].offset()
        );
    }

    /// An offer of a partition of another topic stops the run under `latest`: the
    /// lists name the configured topic, so it would commit a position there.
    #[test]
    fn an_offer_on_another_topic_is_fatal() {
        let (mut source, _clock) = opened_unassigned(latest_config(), 1);
        let mut other = TopicPartitionList::new();
        other.add_partition("other", 0);
        assert!(matches!(
            source.read_latest_starts(&[(0, other)], Instant::now()),
            Err(e) if e.class() == ErrorClass::Fatal
        ));
    }

    /// An empty offer is answered with no lookup, so a consumer with no
    /// partitions never holds an accept under `latest`, whatever the broker
    /// answers. The broker here refuses connections.
    #[test]
    fn an_empty_offer_needs_no_lookup_under_latest() {
        let (mut source, _clock) = opened_unassigned(latest_config(), 1);
        source.members[0].phase = Phase::Held(TopicPartitionList::new());
        assert!(matches!(source.accept_held(false), Ok(SourceEvent::Idle)));
        assert!(source.start_retry_at.is_none());
        assert!(matches!(source.members[0].phase, Phase::Assigned));
    }

    /// A negative offset in a listing's reply is no start, for the partition
    /// to be retried with no error from building the next list.
    #[test]
    fn a_negative_listing_is_no_start() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("orders", 0);
        // SAFETY: the list owns one element. A reply writes the offset straight
        // into it, past the check `set_offset` makes.
        unsafe { (*(*tpl.ptr()).elems).offset = -3 };
        assert!(listed_start(&tpl.elements()[0]).is_err());
    }

    /// Only a concrete offset that is not negative is a start.
    #[test]
    fn the_listing_is_read_as_a_start() {
        let elem = |offset: Offset| {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset("orders", 0, offset)
                .expect("offset");
            listed_start(&tpl.elements()[0]).ok()
        };
        assert_eq!(elem(Offset::Offset(0)), Some(0));
        assert_eq!(elem(Offset::Offset(5)), Some(5));
        assert_eq!(elem(Offset::End), None);
        assert_eq!(elem(Offset::Invalid), None);
        assert_eq!(start_from(0).ok(), Some(0));
        assert_eq!(start_from(-3).ok(), None);
    }

    mod consumers {
        use super::*;
        use spate_core::source::SourceLane;

        /// `open` refuses a consumer count of zero on a source built without
        /// `from_component_config`.
        #[test]
        fn open_rejects_zero_consumers_on_programmatic_source() {
            let mut config = test_config();
            config.consumers = 0;
            let mut source = KafkaSource::new(config);
            let cp = Checkpointer::new();
            let err = source.open(SourceCtx::new(cp.handle())).unwrap_err();
            assert_eq!(err.class(), ErrorClass::Fatal);
            assert!(err.reason().contains("source.kafka.consumers"), "{err}");
        }

        /// A poll reports its first fatal error, else its first error.
        #[test]
        fn a_poll_reports_its_first_fatal_error_else_its_first() {
            let err = SourceError::new;
            let reported = |errors: Vec<SourceError>| {
                let mut kept = None;
                for e in errors {
                    keep_reported(&mut kept, e);
                }
                kept.expect("a poll keeps an error").reason().to_owned()
            };
            let (r, f) = (ErrorClass::Retryable, ErrorClass::Fatal);
            assert_eq!(reported(vec![err(r, "r0"), err(r, "r1")]), "r0");
            assert_eq!(reported(vec![err(r, "r0"), err(f, "f1")]), "f1");
            assert_eq!(reported(vec![err(f, "f0"), err(r, "r1")]), "f0");
            assert_eq!(reported(vec![err(f, "f0"), err(f, "f1")]), "f0");
        }

        /// One `poll_events` call polls every consumer once and spends the
        /// caller's timeout once, on the last of them, starting where the
        /// caller says.
        #[test]
        fn poll_order_spends_one_timeout() {
            let t = Duration::from_millis(50);
            assert_eq!(poll_order(0, 1, t).collect::<Vec<_>>(), [(0, t)]);
            assert_eq!(
                poll_order(2, 4, t).collect::<Vec<_>>(),
                [
                    (2, Duration::ZERO),
                    (3, Duration::ZERO),
                    (0, Duration::ZERO),
                    (1, t)
                ]
            );
        }

        /// The poll order rotates its start from one call to the next.
        #[test]
        fn the_first_consumer_polled_rotates() {
            let mut source = opened_consumers(test_config(), &[&[], &[], &[]]);
            let mut starts = Vec::new();
            for _ in 0..4 {
                starts.push(source.poll_start);
                let _ = source.poll_events(Duration::ZERO);
            }
            assert_eq!(starts, [0, 1, 2, 0]);
        }

        /// Each consumer may store offsets for its own partitions only,
        /// including those it is revoking but has not released.
        #[test]
        fn committable_partitions_are_per_consumer() {
            let mut source =
                opened_consumers(test_config(), &[&[(0, 0), (1, 1)], &[(2, 2), (3, 3)]]);
            assert_eq!(sorted(source.committable_partitions(0)), [0, 1]);
            assert_eq!(sorted(source.committable_partitions(1)), [2, 3]);

            revoke_lanes(&mut source, 1, &[3]);
            assert_eq!(sorted(source.committable_partitions(1)), [2, 3]);
            source.members[1].revoking.clear();
            assert_eq!(sorted(source.committable_partitions(1)), [2]);
            assert_eq!(sorted(source.committable_partitions(0)), [0, 1]);
        }

        /// A rebalance error on one consumer revokes that consumer's lanes
        /// only, then reports the error. The other consumer keeps its lanes
        /// and may still commit.
        #[test]
        fn a_rebalance_error_on_one_consumer_revokes_its_scope() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[(1, 1)]]);
            push_intent_to(
                &source,
                1,
                Intent::Error(RDKafkaErrorCode::RebalanceInProgress),
            );

            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => {
                    assert_eq!(lane_ids(&lanes), [1]);
                }
                other => panic!("expected the erroring consumer's lanes revoked, got {other:?}"),
            }
            assert_eq!(source.committable_partitions(0), [0]);
            assert!(source.committable_partitions(1).is_empty());
            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert!(e.reason().contains("rebalance error"), "{e}");
                }
                other => panic!("expected the error, got {other:?}"),
            }
            assert_eq!(source.committable_partitions(0), [0]);
        }

        /// Eager rebalancing revokes every consumer, so the first revocation
        /// drains every consumer's lanes at once, and each consumer may still
        /// commit until its own revocation arrives and releases it. A
        /// revocation arriving after the drain releases its consumer with no
        /// second drain.
        #[test]
        fn a_revoke_on_one_consumer_revokes_every_consumers_lanes() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[(1, 1)]]);
            push_intent_to(&source, 0, Intent::Revoke(tpl_of(&[0])));

            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, barrier }) => {
                    assert_eq!(lane_ids(&lanes), [0, 1]);
                    assert_eq!(barrier.remaining(), 2);
                }
                other => panic!("expected every lane revoked, got {other:?}"),
            }
            assert_eq!(source.committable_partitions(0), [0]);
            assert_eq!(source.committable_partitions(1), [1]);

            // The runtime has drained: consumer 0 is released, consumer 1
            // still owns its partition until its own revocation arrives.
            poll_quiet(&mut source);
            assert!(source.members[0].revoking.is_empty());
            assert!(source.committable_partitions(0).is_empty());
            assert_eq!(source.committable_partitions(1), [1]);

            push_intent_to(&source, 1, Intent::Revoke(tpl_of(&[1])));
            poll_quiet(&mut source);
            assert!(source.members[1].revoking.is_empty(), "no second drain");
            assert!(source.committable_partitions(1).is_empty());
        }

        /// Each consumer's assignment is held until every consumer of the
        /// rebalance has one, then accepted together. Past `ASSIGN_HOLD` the
        /// held ones are accepted without the consumers still joining.
        #[test]
        fn assignments_are_held_until_every_consumer_has_one() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            for member in &mut source.members {
                member.phase = Phase::Joining;
            }
            push_intent_to(&source, 0, Intent::Assign(TopicPartitionList::new()));
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Held(_)));
            assert!(matches!(source.members[1].phase, Phase::Joining));

            push_intent_to(&source, 1, Intent::Assign(TopicPartitionList::new()));
            poll_quiet(&mut source);
            assert!(
                source
                    .members
                    .iter()
                    .all(|m| matches!(m.phase, Phase::Assigned))
            );

            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            let clock = freeze(&mut source);
            for member in &mut source.members {
                member.phase = Phase::Joining;
            }
            push_intent_to(&source, 0, Intent::Assign(TopicPartitionList::new()));
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Held(_)));
            clock.advance(ASSIGN_HOLD - Duration::from_millis(1));
            poll_quiet(&mut source);
            assert!(
                matches!(source.members[0].phase, Phase::Held(_)),
                "inside the hold"
            );
            clock.advance(Duration::from_millis(1));
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Assigned));
            assert!(matches!(source.members[1].phase, Phase::Joining));
        }

        /// An assignment that arrives while another consumer has live lanes
        /// drains them, then accepts every consumer's partitions together as
        /// one new lane set with fresh lane ids.
        #[test]
        fn an_assignment_while_lanes_are_live_starts_a_new_epoch() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[]]);
            push_intent_to(&source, 1, Intent::Assign(TopicPartitionList::new()));

            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => assert_eq!(lane_ids(&lanes), [0]),
                other => panic!("expected the live lanes drained, got {other:?}"),
            }
            assert_eq!(
                source.committable_partitions(0),
                [0],
                "still owned while draining"
            );
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesAssigned(lanes)) => {
                    let assigned: Vec<(u32, u32)> =
                        lanes.iter().map(|l| (l.id().0, l.partition().0)).collect();
                    assert_eq!(assigned, [(1, 0)]);
                }
                other => panic!("expected one new lane set, got {other:?}"),
            }
            assert!(
                source
                    .members
                    .iter()
                    .all(|m| matches!(m.phase, Phase::Assigned))
            );
        }

        /// Two consumers offered one partition in one generation is a broken
        /// assignment.
        #[test]
        fn a_partition_in_two_held_assignments_is_fatal() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            source.members[0].phase = Phase::Held(tpl_of(&[3]));
            source.members[1].phase = Phase::Held(tpl_of(&[1, 3]));
            source.members[0].generation = Some(7);
            source.members[1].generation = Some(7);
            match source.settle_held() {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal);
                    assert!(
                        e.reason().contains("partition 3")
                            && e.reason().contains("consumers 0 and 1")
                            && e.reason().contains("generation 7"),
                        "{e}"
                    );
                }
                other => panic!("expected a fatal error, got {other:?}"),
            }
        }

        /// Offers of `partitions` per consumer, each held at `generation`.
        fn hold_offers(
            source: &mut KafkaSource,
            offers: &[(usize, &[i32], i32)],
        ) -> Vec<(usize, TopicPartitionList)> {
            offers
                .iter()
                .map(|&(k, partitions, generation)| {
                    source.members[k].generation = Some(generation);
                    (k, tpl_of(partitions))
                })
                .collect()
        }

        /// An offer is stale below the newest generation among the offers, or
        /// with no generation at all, whatever its consumer's index.
        #[test]
        fn stale_offers_compare_generations() {
            assert_eq!(stale_offers(&[(0, 5), (1, 6)]), [0]);
            assert_eq!(stale_offers(&[(0, 6), (1, 5)]), [1]);
            assert_eq!(stale_offers(&[(0, 7), (1, 7)]), [] as [usize; 0]);
            assert_eq!(stale_offers(&[(0, 0), (1, 0)]), [] as [usize; 0]);
            assert_eq!(stale_offers(&[(0, -1), (1, -1)]), [0, 1]);
            assert_eq!(stale_offers(&[(0, -1), (1, 3)]), [0]);
        }

        /// Of two offers carrying a partition, the one below the newer
        /// generation is the one discarded, on either consumer, with one warn
        /// naming the consumer and both generations. Regression for #1685.
        #[test]
        fn a_stale_offer_is_recognised_on_either_consumer() {
            for (stale, fresh) in [(0, 1), (1, 0)] {
                let mut source = opened_consumers(test_config(), &[&[], &[]]);
                let offers = hold_offers(&mut source, &[(0, &[0], 0), (1, &[0], 0)]);
                source.members[stale].generation = Some(5);
                source.members[fresh].generation = Some(6);
                assert_eq!(first_repeat(&offers), Some((0, 0, 1)));

                let mut kept = None;
                let warned = spate_test::capture_logs(tracing::Level::WARN, || {
                    kept = Some(source.discard_stale_offers(offers).expect("discard"));
                });
                let kept: Vec<usize> = kept.expect("ran").into_iter().map(|(k, _)| k).collect();
                assert_eq!(kept, [fresh]);
                assert!(matches!(source.members[stale].phase, Phase::Joining));
                assert!(source.committable_partitions(stale).is_empty());
                assert_eq!(
                    source.members[stale]
                        .consumer
                        .assignment()
                        .expect("assignment")
                        .count(),
                    0,
                    "the answer assigns nothing"
                );
                let discards: Vec<&String> = warned
                    .iter()
                    .filter(|l| l.contains("discarded an assignment"))
                    .collect();
                assert_eq!(discards.len(), 1, "{warned:#?}");
                for field in [
                    format!("consumer={stale}"),
                    "partitions=1".to_owned(),
                    "generation=5".to_owned(),
                    "newest=6".to_owned(),
                ] {
                    assert!(discards[0].contains(&field), "{field}: {warned:#?}");
                }
            }
        }

        /// A stale offer goes whole: the partitions no newer offer carries
        /// belong to no consumer of this source until the group offers them.
        #[test]
        fn a_stale_offer_covering_more_partitions_loses_none() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            let offers = hold_offers(&mut source, &[(0, &[0, 1, 2, 3], 5), (1, &[0, 1], 6)]);
            let kept = source.discard_stale_offers(offers).expect("discard");
            assert_eq!(kept.len(), 1);
            assert_eq!(kept[0].0, 1);
            assert!(matches!(source.members[0].phase, Phase::Joining));
            for p in [2, 3] {
                assert_eq!(source.owner(PartitionId(p)), None, "partition {p}");
            }
        }

        /// A stale offer whose empty `assign` fails ends the run, since that
        /// consumer stays in librdkafka's wait for an answer.
        #[test]
        fn a_failed_answer_to_a_stale_offer_is_fatal() {
            use crate::client::testing::raw;
            // `Drop` reads a consumer queue, which the client below does not have.
            let mut source =
                std::mem::ManuallyDrop::new(opened_consumers(test_config(), &[&[], &[]]));
            let offers = hold_offers(&mut source, &[(0, &[0], 5), (1, &[0], 6)]);
            // A client with no group refuses `assign`.
            source.members[0].consumer = Arc::new(raw(
                &[("bootstrap.servers", "127.0.0.1:1")],
                SourceHooks::default(),
            ));
            match source.discard_stale_offers(offers) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(e.reason().contains("assign empty"), "{e}");
                }
                other => panic!("expected a fatal error, got {other:?}"),
            }
        }

        /// A new offer carrying a partition another consumer is revoking is
        /// not fatal: the older consumer's partitions are not offered again,
        /// and the newer offer takes them. Regression for #1685.
        #[test]
        fn an_offer_overlapping_a_revoking_set_is_not_fatal() {
            let mut source = opened_consumers(test_config(), &[&[], &[(0, 1)]]);
            source.members[0].generation = Some(6);
            source.members[1].generation = Some(5);
            push_intent_to(&source, 0, Intent::Assign(tpl_of(&[0, 1])));
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => assert_eq!(lane_ids(&lanes), [0]),
                other => panic!("expected the live lane drained, got {other:?}"),
            }
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesAssigned(lanes)) => {
                    let mut partitions: Vec<u32> = lanes.iter().map(|l| l.partition().0).collect();
                    partitions.sort_unstable();
                    assert_eq!(partitions, [0, 1]);
                }
                other => panic!("expected the newer offer's lanes, got {other:?}"),
            }
            assert_eq!(sorted(source.committable_partitions(0)), [0, 1]);
            assert!(source.committable_partitions(1).is_empty());
            assert!(matches!(source.members[1].phase, Phase::Joining));
        }

        /// An offer overlapping two newer ones is the one discarded, and those
        /// two stay.
        #[test]
        fn an_offer_overlapping_two_others_is_the_one_discarded() {
            let mut source = opened_consumers(test_config(), &[&[], &[], &[]]);
            let offers = hold_offers(&mut source, &[(0, &[0, 1], 5), (1, &[0], 6), (2, &[1], 6)]);
            let kept: Vec<usize> = source
                .discard_stale_offers(offers)
                .expect("discard")
                .into_iter()
                .map(|(k, _)| k)
                .collect();
            assert_eq!(kept, [1, 2]);

            let mut source = opened_consumers(test_config(), &[&[], &[], &[]]);
            let clock = freeze(&mut source);
            source.members[0].phase = Phase::Held(tpl_of(&[0, 1]));
            source.members[1].phase = Phase::Held(tpl_of(&[0]));
            source.members[2].phase = Phase::Joining;
            source.members[0].generation = Some(5);
            source.members[1].generation = Some(6);
            source.hold_since = Some(source.clock.now());
            assert!(matches!(source.settle_held(), Ok(SourceEvent::Idle)));
            assert!(matches!(source.members[0].phase, Phase::Held(_)));
            clock.advance(ASSIGN_HOLD);
            match source.settle_held() {
                Ok(SourceEvent::LanesAssigned(lanes)) => {
                    assert_eq!(lanes.len(), 1);
                    assert_eq!(lanes[0].partition(), PartitionId(0));
                }
                other => panic!("expected the newer offer's lane, got {other:?}"),
            }
            assert!(matches!(source.members[0].phase, Phase::Joining));
        }

        /// A third consumer's offer of an accepted partition replaces the
        /// accepted consumers of the older generation.
        #[test]
        fn a_third_consumers_offer_replaces_a_stale_accepted_one() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[(1, 1)], &[]]);
            source.members[0].generation = Some(5);
            source.members[1].generation = Some(5);
            source.members[2].generation = Some(6);
            push_intent_to(&source, 2, Intent::Assign(tpl_of(&[0])));
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => assert_eq!(lane_ids(&lanes), [0, 1]),
                other => panic!("expected the live lanes drained, got {other:?}"),
            }
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesAssigned(lanes)) => {
                    assert_eq!(lanes.len(), 1);
                    assert_eq!(lanes[0].partition(), PartitionId(0));
                }
                other => panic!("expected one lane, got {other:?}"),
            }
            assert_eq!(source.committable_partitions(2), [0]);
            assert!(source.committable_partitions(0).is_empty());
        }

        /// An offer held across a peer's revocation does not collide with the
        /// peer's next offer.
        #[test]
        fn a_held_offer_outlasting_its_peers_revocation_does_not_collide() {
            let mut source = opened_consumers(test_config(), &[&[], &[(0, 1)]]);
            source.members[0].phase = Phase::Held(tpl_of(&[0, 1]));
            source.members[0].generation = Some(5);
            source.members[1].generation = Some(6);
            freeze(&mut source);
            push_intent_to(&source, 1, Intent::Revoke(tpl_of(&[1])));
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => assert_eq!(lane_ids(&lanes), [0]),
                other => panic!("expected the live lane drained, got {other:?}"),
            }
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Held(_)));

            push_intent_to(&source, 1, Intent::Assign(tpl_of(&[1])));
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesAssigned(lanes)) => {
                    assert_eq!(lanes.len(), 1);
                    assert_eq!(lanes[0].partition(), PartitionId(1));
                }
                other => panic!("expected the peer's lane, got {other:?}"),
            }
            assert!(matches!(source.members[0].phase, Phase::Joining));
            assert_eq!(source.committable_partitions(1), [1]);
        }

        /// Discarding every offer accepts nothing, so the startup window stays
        /// open and its deadline ends a run that repeats such offers.
        #[test]
        fn a_discarded_offer_keeps_the_startup_window() {
            let (mut source, clock) = opened_unassigned(test_config(), 2);
            for _ in 0..2 {
                source.members[0].phase = Phase::Held(tpl_of(&[0]));
                source.members[1].phase = Phase::Held(tpl_of(&[0]));
                assert!(matches!(source.settle_held(), Ok(SourceEvent::Idle)));
            }
            assert!(!source.saw_first_assignment);
            assert!(source.members.iter().all(|m| !m.saw_first_assignment));
            assert!(source.assignment_wait.is_some());

            clock.advance(source.config.startup_timeout + Duration::from_millis(1));
            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(e.reason().contains("no partition assignment"), "{e}");
                }
                other => panic!("expected the deadline error, got {other:?}"),
            }
        }

        /// Discarding every offer ends the hold, so the next offer's hold runs
        /// from its own arrival.
        #[test]
        fn a_discarded_offer_restarts_the_hold() {
            let (mut source, clock) = opened_unassigned(test_config(), 2);
            source.members[0].phase = Phase::Held(tpl_of(&[0]));
            source.members[1].phase = Phase::Held(tpl_of(&[0]));
            source.hold_since = Some(source.clock.now());
            clock.advance(ASSIGN_HOLD);
            assert!(matches!(source.settle_held(), Ok(SourceEvent::Idle)));
            assert_eq!(source.hold_since, None);

            push_intent_to(&source, 0, Intent::Assign(TopicPartitionList::new()));
            poll_quiet(&mut source);
            clock.advance(ASSIGN_HOLD - Duration::from_millis(1));
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Held(_)));
            clock.advance(Duration::from_millis(1));
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Assigned));
        }

        /// A stale offer overlapping nothing is accepted beside the newer one.
        #[test]
        fn a_stale_offer_overlapping_nothing_is_accepted() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            source.members[0].phase = Phase::Held(tpl_of(&[0]));
            source.members[1].phase = Phase::Held(tpl_of(&[1]));
            source.members[0].generation = Some(5);
            source.members[1].generation = Some(6);
            match source.settle_held() {
                Ok(SourceEvent::LanesAssigned(lanes)) => assert_eq!(lanes.len(), 2),
                other => panic!("expected both offers' lanes, got {other:?}"),
            }
            assert!(matches!(source.members[0].phase, Phase::Assigned));
        }

        /// Accepting offers ends the hold, also when an offer was discarded
        /// beside them, so the next rebalance's hold runs from its own offer.
        #[test]
        fn an_accepted_offer_ends_the_hold() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            source.members[0].phase = Phase::Held(tpl_of(&[0]));
            source.members[1].phase = Phase::Held(tpl_of(&[0]));
            source.members[0].generation = Some(5);
            source.members[1].generation = Some(6);
            source.hold_since = Some(source.clock.now());
            assert!(matches!(
                source.settle_held(),
                Ok(SourceEvent::LanesAssigned(_))
            ));
            assert_eq!(source.hold_since, None);
        }

        /// A drained revocation is complete once released, so a peer's later
        /// revocation does not release this consumer again.
        #[test]
        fn a_release_clears_the_arrived_revocation() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[(1, 1)]]);
            push_intent_to(&source, 0, Intent::Revoke(tpl_of(&[0])));
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { .. }) => {}
                other => panic!("expected the live lanes drained, got {other:?}"),
            }
            poll_quiet(&mut source);
            assert!(!source.members[0].revoke_arrived);
        }

        /// A reassign that discards every offer leaves a source with no lanes
        /// inside the window `assignment_timeout` governs.
        #[test]
        fn a_reassign_discarding_every_offer_opens_the_window() {
            let mut source = opened_consumers(test_config(), &[&[], &[(0, 1)]]);
            let clock = freeze(&mut source);
            source.members[0].generation = Some(-1);
            source.members[1].generation = Some(-1);
            push_intent_to(&source, 0, Intent::Assign(tpl_of(&[0, 1])));
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => assert_eq!(lane_ids(&lanes), [0]),
                other => panic!("expected the live lane drained, got {other:?}"),
            }
            assert!(matches!(
                source.poll_events(Duration::ZERO),
                Ok(SourceEvent::Idle)
            ));
            assert!(source.holds_no_lanes());

            clock.advance(source.config.assignment_timeout + Duration::from_millis(1));
            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(e.reason().contains("no partition assignment"), "{e}");
                }
                other => panic!("expected the deadline error, got {other:?}"),
            }
        }

        /// A consumer that has not joined a group reads no generation.
        #[test]
        fn a_member_not_in_a_group_reads_no_generation() {
            let source = opened_consumers(test_config(), &[&[]]);
            assert_eq!(source.members[0].group_generation(), -1);
        }

        /// A consumer's second offer replaces its first, so it holds one list.
        #[test]
        fn a_second_offer_to_one_consumer_replaces_its_first() {
            let mut source = opened_consumers(test_config(), &[&[]]);
            source
                .on_intent(0, Intent::Assign(tpl_of(&[0])))
                .expect("first offer");
            source
                .on_intent(0, Intent::Assign(tpl_of(&[1])))
                .expect("second offer");
            match &source.members[0].phase {
                Phase::Held(tpl) => {
                    let partitions: Vec<i32> =
                        tpl.elements().iter().map(|e| e.partition()).collect();
                    assert_eq!(partitions, [1]);
                }
                _ => panic!("expected the second offer held"),
            }
        }

        /// A rebalance error on a consumer drops the offer it held.
        #[test]
        fn a_rebalance_error_discards_a_held_offer() {
            let mut source = opened_consumers(test_config(), &[&[]]);
            source.members[0].phase = Phase::Held(tpl_of(&[0]));
            let _ = source.on_intent(0, Intent::Error(RDKafkaErrorCode::RebalanceInProgress));
            assert!(matches!(source.members[0].phase, Phase::Joining));
        }

        /// A rebalance error is classified by its own consumer's start: an
        /// unknown topic is metadata catching up for a consumer never
        /// assigned, and a deleted topic for one that was.
        #[test]
        fn startup_errors_classify_per_consumer() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            source.members[1].saw_first_assignment = false;
            let class = |source: &KafkaSource, k| {
                source
                    .rebalance_error(k, RDKafkaErrorCode::UnknownTopicOrPartition)
                    .class()
            };
            assert_eq!(class(&source, 0), ErrorClass::Fatal);
            assert_eq!(class(&source, 1), ErrorClass::Retryable);
        }

        /// A watermark goes to the consumer holding its partition live, else
        /// to the one still revoking it, and to none when no consumer owns it.
        #[test]
        fn a_watermark_goes_to_the_owning_consumer() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[(1, 1), (2, 2)]]);
            revoke_lanes(&mut source, 1, &[2]);
            // Consumer 0 is still revoking partition 1, which consumer 1 holds
            // live: the live holder takes it.
            source.members[0].revoking.insert(LaneId(9), 1);
            assert_eq!(source.owner(PartitionId(0)), Some((0, 0)));
            assert_eq!(source.owner(PartitionId(1)), Some((1, 1)));
            assert_eq!(source.owner(PartitionId(2)), Some((1, 2)));
            assert_eq!(source.owner(PartitionId(3)), None);
            assert_eq!(source.owner(PartitionId(u32::MAX)), None);
        }

        /// A commit whose every watermark is for a partition no consumer owns
        /// stores nothing, succeeds, and says how many it refused.
        #[test]
        fn a_commit_of_only_unowned_partitions_is_refused_with_a_warning() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)]]);
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                source
                    .commit(&[(PartitionId(5), 10), (PartitionId(6), 3)])
                    .expect("a refusal is not an error");
            });
            assert!(
                lines
                    .iter()
                    .any(|l| l.contains("refusing to store") && l.contains("refused=2")),
                "{lines:#?}"
            );
        }

        /// A second `open` is a fatal error.
        #[test]
        fn open_twice_is_fatal() {
            let mut source = opened_source(&[]);
            let cp = Checkpointer::new();
            let err = source
                .open(SourceCtx::new(cp.handle()))
                .expect_err("a second open fails");
            assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
            assert_eq!(err.reason(), "open() called twice");
        }

        /// A source used before `open` reports a fatal error from each call
        /// that needs its consumers.
        #[test]
        fn a_source_used_before_open_is_fatal() {
            let mut source = KafkaSource::new(test_config());
            let polled = source.poll_events(Duration::ZERO).map(|_| ());
            let committed = source.commit(&[(PartitionId(0), 1)]);
            let flushed = source.flush_commits();
            for result in [polled, committed, flushed] {
                let err = result.expect_err("a source used before open fails");
                assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
                assert!(err.reason().contains("before open()"), "{err}");
            }
        }

        /// Accepting the held assignments leaves alone a consumer whose
        /// revocation has not arrived: it keeps its partitions until it is
        /// released.
        #[test]
        fn accepting_held_assignments_leaves_an_unreleased_consumer_alone() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[]]);
            revoke_lanes(&mut source, 0, &[0]);
            source.members[1].phase = Phase::Held(TopicPartitionList::new());
            match source.accept_held(false) {
                Ok(SourceEvent::Idle) => {}
                other => panic!("expected no lanes, got {other:?}"),
            }
            assert_eq!(source.committable_partitions(0), [0]);
            assert!(matches!(source.members[1].phase, Phase::Assigned));
        }

        /// Accepting held assignments while a consumer is still joining names
        /// it, and accepting with every consumer held names none.
        #[test]
        fn accepting_without_every_consumer_names_the_ones_joining() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            source.members[0].phase = Phase::Held(TopicPartitionList::new());
            source.members[1].phase = Phase::Joining;
            let partial = spate_test::capture_logs(tracing::Level::WARN, || {
                source.accept_held(false).expect("accept");
            });
            assert!(
                partial
                    .iter()
                    .any(|l| l.contains("these consumers have none yet")),
                "{partial:#?}"
            );

            for member in &mut source.members {
                member.phase = Phase::Held(TopicPartitionList::new());
            }
            let whole = spate_test::capture_logs(tracing::Level::WARN, || {
                source.accept_held(false).expect("accept");
            });
            assert!(
                !whole
                    .iter()
                    .any(|l| l.contains("these consumers have none yet")),
                "{whole:#?}"
            );
        }

        /// More than one intent queued on a consumer is logged as a pileup;
        /// a single one is not.
        #[test]
        fn piled_up_intents_are_logged() {
            let source = opened_consumers(test_config(), &[&[], &[]]);
            push_intent_to(&source, 1, Intent::Assign(TopicPartitionList::new()));
            let single = spate_test::capture_logs(tracing::Level::WARN, || {
                assert!(source.next_intent().is_some());
            });
            assert!(
                !single.iter().any(|l| l.contains("piled up")),
                "{single:#?}"
            );

            push_intent_to(&source, 1, Intent::Revoke(TopicPartitionList::new()));
            push_intent_to(&source, 1, Intent::Assign(TopicPartitionList::new()));
            let piled = spate_test::capture_logs(tracing::Level::WARN, || {
                assert!(source.next_intent().is_some());
            });
            assert!(piled.iter().any(|l| l.contains("piled up")), "{piled:#?}");
        }

        /// A revocation sets every consumer back to joining except one whose
        /// assignment is held. librdkafka queues a held member no revocation,
        /// and its offer still waits for `assign`.
        #[test]
        fn a_revocation_leaves_a_held_assignment_held() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[]]);
            source.members[1].phase = Phase::Held(TopicPartitionList::new());
            match source.on_intent(0, Intent::Revoke(tpl_of(&[0]))) {
                Ok(SourceEvent::LanesRevoked { .. }) => {}
                other => panic!("expected a revocation, got {other:?}"),
            }
            assert!(matches!(source.members[0].phase, Phase::Joining));
            assert!(matches!(source.members[1].phase, Phase::Held(_)));
        }

        /// `open` refuses a consumer count it cannot allocate with an error
        /// naming the key.
        #[test]
        fn open_refuses_a_count_it_cannot_allocate() {
            let mut config = test_config();
            config.consumers = usize::MAX;
            let mut source = KafkaSource::new(config);
            let cp = Checkpointer::new();
            match source.open(SourceCtx::new(cp.handle())) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal);
                    assert!(e.reason().contains("source.kafka.consumers"), "{e}");
                }
                other => panic!("expected a fatal error naming the key, got {other:?}"),
            }
        }

        /// `topic.client.id` is no librdkafka property, so `open` refuses it
        /// at every consumer count.
        #[test]
        fn topic_client_id_is_refused_at_every_count() {
            for consumers in [1, 2] {
                let mut config = test_config();
                config.consumers = consumers;
                config
                    .rdkafka
                    .insert("topic.client.id".into(), "app".into());
                let mut source = KafkaSource::new(config);
                let cp = Checkpointer::new();
                match source.open(SourceCtx::new(cp.handle())) {
                    Err(e) => {
                        assert!(e.reason().contains("topic.client.id"), "{e}");
                    }
                    other => panic!("consumers: {consumers} opened: {other:?}"),
                }
            }
        }

        /// After a rebalance error on one consumer, the drain's commit carries
        /// that consumer's drained watermarks beside another consumer's live
        /// ones. The refused watermarks replay, and the refusal is warned.
        #[test]
        fn a_refusal_beside_owned_watermarks_is_warned() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[(1, 1)]]);
            push_intent_to(
                &source,
                1,
                Intent::Error(RDKafkaErrorCode::RebalanceInProgress),
            );
            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => {
                    assert_eq!(lane_ids(&lanes), [1]);
                }
                other => panic!("expected the erroring consumer's lanes revoked, got {other:?}"),
            }
            let quiet = spate_test::capture_logs(tracing::Level::WARN, || {
                let _ = source.commit(&[(PartitionId(0), 9)]);
            });
            assert!(
                !quiet.iter().any(|l| l.contains("refusing to store")),
                "nothing was refused: {quiet:#?}"
            );
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                let _ = source.commit(&[(PartitionId(0), 10), (PartitionId(1), 7)]);
            });
            assert!(
                lines
                    .iter()
                    .any(|l| l.contains("refusing to store") && l.contains("refused=1")),
                "partition 1's refusal was not warned: {lines:#?}"
            );
        }

        /// An opened source on a mock cluster holding `records` on partition 0,
        /// with its first assignment accepted, and the cluster that serves it.
        fn assigned_on_mock(records: usize) -> (MockKafka, KafkaSource, Vec<KafkaLane>) {
            let cluster = mock_holding(records);
            let (source, lanes) = assigned_on(&cluster, "earliest");
            (cluster, source, lanes)
        }

        /// A mock cluster whose topic `t` holds `records` on its one partition.
        fn mock_holding(records: usize) -> MockKafka {
            let cluster = MockKafka::new();
            cluster.create_topic("t", 1, 1).expect("create topic");
            use crate::client::testing::{Raw as _, raw_config};
            let producer: rdkafka::producer::BaseProducer = raw_config()
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .build()
                .expect("producer");
            for i in 0..records {
                let payload = i.to_string();
                producer
                    .send(
                        rdkafka::producer::BaseRecord::<(), _>::to("t")
                            .partition(0)
                            .payload(payload.as_bytes()),
                    )
                    .expect("enqueue");
            }
            rdkafka::producer::Producer::flush(&producer, Duration::from_secs(30)).expect("flush");
            cluster
        }

        /// A source of group `positions` on `cluster`'s topic `t` under the
        /// `reset` policy, opened, with its first assignment accepted.
        fn assigned_on(cluster: &MockKafka, reset: &str) -> (KafkaSource, Vec<KafkaLane>) {
            assigned_with(cluster, reset, &[])
        }

        /// [`assigned_on`] with `extra` librdkafka properties.
        fn assigned_with(
            cluster: &MockKafka,
            reset: &str,
            extra: &[(&str, &str)],
        ) -> (KafkaSource, Vec<KafkaLane>) {
            let mut cfg = KafkaSourceConfig::new(cluster.bootstrap_servers(), "t", "positions");
            cfg.startup_timeout = Duration::from_secs(30);
            cfg.rdkafka = std::collections::BTreeMap::from([
                ("auto.offset.reset".to_string(), reset.to_string()),
                ("session.timeout.ms".to_string(), "6000".to_string()),
            ]);
            for (key, value) in extra {
                cfg.rdkafka.insert((*key).to_string(), (*value).to_string());
            }
            let cp = spate_core::checkpoint::Checkpointer::new();
            let mut source = KafkaSource::new(cfg);
            source
                .open(SourceCtx::new(cp.handle()))
                .expect("open the source");
            let lanes = super::queued_rebalance::live_lanes(&mut source);
            (source, lanes)
        }

        /// Under `auto.offset.reset: error`, a fetch past the log end fails the
        /// lane with a fatal error naming the partition: librdkafka stops
        /// fetching the partition after it reports the reset.
        /// Regression for #1268.
        #[test]
        fn a_reset_under_the_error_policy_is_fatal() {
            let cluster = mock_holding(3);
            commit_positions(&cluster, 100);

            let (mut source, mut lanes) = assigned_on(&cluster, "error");
            let mut failed = None;
            spate_test::wait_until(Duration::from_secs(30), "the lane's error", || {
                source
                    .poll_events(Duration::from_millis(10))
                    .expect("poll_events");
                match lanes[0].poll(1, Duration::from_millis(100)) {
                    Err(e) => failed = Some(e),
                    Ok(batch) => assert!(batch.is_none(), "no record lies past the end"),
                }
                failed.is_some()
            });
            let e = failed.expect("wait_until returned");
            assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
            assert!(e.reason().contains("partition 0"), "{e}");
        }

        /// Commit `offset` on partition 0 of `cluster`'s topic `t` for group
        /// `positions`.
        fn commit_positions(cluster: &MockKafka, offset: i64) {
            use crate::client::testing::{Raw as _, raw_config};
            use rdkafka::consumer::Consumer as _;
            let committer: rdkafka::consumer::BaseConsumer = raw_config()
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .set("group.id", "positions")
                .build()
                .expect("committer");
            let mut list = TopicPartitionList::new();
            list.add_partition_offset("t", 0, Offset::Offset(offset))
                .expect("offset");
            committer
                .commit(&list, rdkafka::consumer::CommitMode::Sync)
                .expect("commit");
        }

        /// Write 6 MiB to partition 0 of `cluster`'s topic `t`, past the 5 MiB the
        /// mock keeps of a partition before it drops its oldest message sets, and
        /// return the log's start and end.
        fn write_past_retention(cluster: &MockKafka) -> (i64, i64) {
            use crate::client::testing::{Raw as _, raw_config};
            use rdkafka::consumer::Consumer as _;
            let producer: rdkafka::producer::BaseProducer = raw_config()
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .set("linger.ms", "0")
                .build()
                .expect("producer");
            let pad = vec![b'x'; 256 * 1024];
            for _ in 0..24 {
                producer
                    .send(
                        rdkafka::producer::BaseRecord::<(), _>::to("t")
                            .partition(0)
                            .payload(&pad),
                    )
                    .expect("enqueue");
                rdkafka::producer::Producer::flush(&producer, Duration::from_secs(30))
                    .expect("flush");
            }
            let probe: rdkafka::consumer::BaseConsumer = raw_config()
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .build()
                .expect("probe");
            probe
                .fetch_watermarks("t", 0, Duration::from_secs(10))
                .expect("watermarks")
        }

        /// Serve `source` and poll `lane`, writing one record to partition 0 on
        /// each turn, until the lane hands out an offset past `after`. Returns that
        /// offset and every offset the lane handed out. The writes let a lane that
        /// jumped to the end read something, so it fails on its offset and not on
        /// the hang bound.
        fn first_read_past(
            cluster: &MockKafka,
            source: &mut KafkaSource,
            lane: &mut KafkaLane,
            after: i64,
        ) -> (i64, Vec<i64>) {
            use crate::client::testing::{Raw as _, raw_config};
            use spate_core::source::PayloadBatch as _;
            let producer: rdkafka::producer::BaseProducer = raw_config()
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .build()
                .expect("producer");
            let (mut first, mut seen) = (None, Vec::new());
            spate_test::wait_until(spate_test::HANG, "a record past the start", || {
                producer
                    .send(
                        rdkafka::producer::BaseRecord::<(), _>::to("t")
                            .partition(0)
                            .payload("later"),
                    )
                    .expect("enqueue");
                rdkafka::producer::Producer::flush(&producer, Duration::from_secs(30))
                    .expect("flush");
                source
                    .poll_events(Duration::from_millis(10))
                    .expect("poll_events");
                if let Some(mut batch) = lane
                    .poll(64, Duration::from_millis(100))
                    .expect("lane poll")
                {
                    while let Some(raw) = batch.next_payload() {
                        seen.push(raw.offset);
                        if raw.offset > after && first.is_none() {
                            first = Some(raw.offset);
                        }
                    }
                }
                first.is_some()
            });
            (first.expect("wait_until returned"), seen)
        }

        /// Under `latest`, a commit below the log start resumes at the log start,
        /// so the records the log still holds are read. Regression for #1713.
        #[test]
        fn a_commit_below_the_log_start_resumes_at_the_log_start() {
            let cluster = mock_holding(3);
            commit_positions(&cluster, 1);
            let (low, high) = write_past_retention(&cluster);
            assert!(low > 1, "the log start {low} has not passed the commit");

            let (mut source, mut lanes) = assigned_on(&cluster, "latest");
            let (first, _) = first_read_past(&cluster, &mut source, &mut lanes[0], -1);
            assert_eq!(
                first, low,
                "log {low}..{high}: the lane did not resume at the log start"
            );
        }

        /// Under `latest`, a running lane whose fetch position falls below the log
        /// start while it lags resumes at the log start. Regression for #1713.
        #[test]
        fn a_lagging_lane_resumes_at_the_log_start() {
            use spate_core::source::PayloadBatch as _;
            let cluster = mock_holding(3);
            commit_positions(&cluster, 0);
            // The fetcher stops while one message waits in the partition's queue.
            let (mut source, mut lanes) =
                assigned_with(&cluster, "latest", &[("queued.min.messages", "1")]);
            // Offset 0 alone, so 1 and 2 stay queued and the fetch position stays
            // at 3 while the log start moves.
            let mut seen = Vec::new();
            spate_test::wait_until(spate_test::HANG, "offset 0", || {
                source
                    .poll_events(Duration::from_millis(10))
                    .expect("poll_events");
                if let Some(mut batch) = lanes[0]
                    .poll(1, Duration::from_millis(100))
                    .expect("lane poll")
                {
                    while let Some(raw) = batch.next_payload() {
                        seen.push(raw.offset);
                    }
                }
                !seen.is_empty()
            });
            assert_eq!(seen, [0]);
            let (low, high) = write_past_retention(&cluster);
            assert!(
                low > 3,
                "the log start {low} has not passed the fetch position"
            );

            let (first, seen) = first_read_past(&cluster, &mut source, &mut lanes[0], 2);
            assert_eq!(
                first, low,
                "log {low}..{high}, read {seen:?}: the lagging lane did not resume at the log start"
            );
        }

        /// Under `latest`, a commit past the log end resumes at the log start.
        #[test]
        fn a_commit_past_the_log_end_resumes_at_the_log_start() {
            let cluster = mock_holding(3);
            commit_positions(&cluster, 100);
            let (mut source, mut lanes) = assigned_on(&cluster, "latest");
            let (first, _) = first_read_past(&cluster, &mut source, &mut lanes[0], -1);
            assert_eq!(first, 0, "the lane did not resume at the log start");
        }

        /// Serve `source` and poll `lane` until the lane issues a batch.
        fn read_the_record(source: &mut KafkaSource, lane: &mut KafkaLane) {
            spate_test::wait_until(Duration::from_secs(30), "the record", || {
                source
                    .poll_events(Duration::from_millis(10))
                    .expect("poll_events");
                lane.poll(1, Duration::from_millis(100))
                    .expect("lane poll")
                    .is_some()
            });
        }

        /// The offset a lane would read for partition 0 of member 0.
        fn position_of(source: &KafkaSource) -> Offset {
            let mut list = TopicPartitionList::with_capacity(1);
            list.add_partition("t", 0);
            crate::client::read_position(&source.members[0].consumer, &mut list)
        }

        /// A fresh assignment's position reads `Invalid` until an op is served,
        /// so an empty return ahead of any read retires nothing.
        #[test]
        fn the_position_reads_invalid_before_any_op() {
            let (_cluster, source, _lanes) = assigned_on_mock(1);
            assert_eq!(position_of(&source), Offset::Invalid);
        }

        /// Accepting an assignment leaves no position from the one before it on
        /// the client, so a new lane cannot retire offsets through a stale
        /// position. The lane dropped while holding messages closes cleanly.
        #[test]
        fn a_reassign_leaves_no_earlier_position() {
            let (_cluster, mut source, mut lanes) = assigned_on_mock(1);
            read_the_record(&mut source, &mut lanes[0]);
            assert_eq!(position_of(&source), Offset::Offset(1));
            drop(lanes);

            let tpl = {
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition("t", 0);
                tpl
            };
            let lanes = source.accept_assignment(0, &tpl, &tpl, 0).expect("accept");
            assert_eq!(position_of(&source), Offset::Invalid);
            drop(lanes);
        }

        /// A partition the member no longer holds gives up its watermark, and one
        /// it holds keeps it.
        #[test]
        fn a_partition_no_longer_held_gives_up_its_watermark() {
            let (_cluster, mut source, _lanes) = assigned_on_mock(0);
            source.members[0]
                .watermarks
                .insert(7, Arc::new(AtomicI64::new(i64::MIN)));

            source.prune_partition_series();

            let held: Vec<i32> = source.members[0].watermarks.keys().copied().collect();
            assert_eq!(held, [0]);
        }

        /// A message polled off the main queue latches the client against
        /// retiring offsets, since it moved the position with no lane issuing it.
        /// A partition that was never split delivers there.
        #[test]
        #[expect(
            clippy::disallowed_methods,
            reason = "a test producer is configured outside the factory's roles"
        )]
        fn a_main_queue_message_latches_the_rewind() {
            let (cluster, mut source, _lanes) = assigned_on_mock(0);
            cluster.create_topic("unsplit", 1, 1).expect("create topic");
            let producer: rdkafka::producer::BaseProducer = rdkafka::ClientConfig::new()
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .create()
                .expect("producer");
            producer
                .send(
                    rdkafka::producer::BaseRecord::<(), _>::to("unsplit")
                        .partition(0)
                        .payload(&b"leak"[..]),
                )
                .expect("enqueue");
            rdkafka::producer::Producer::flush(&producer, Duration::from_secs(30)).expect("flush");
            assert!(!source.members[0].consumer.context().rewound());

            let mut unsplit = TopicPartitionList::new();
            unsplit
                .add_partition_offset("unsplit", 0, Offset::Beginning)
                .expect("list entry");
            source.members[0].consumer.assign(&unsplit).expect("assign");
            spate_test::wait_until(Duration::from_secs(30), "the rewind", || {
                let _ = source.poll_events(Duration::from_millis(100));
                source.main_queue_rewinds > 0
            });
            assert!(source.members[0].consumer.context().rewound());
        }

        /// A rewound client's lanes retire nothing, where the same lane on an
        /// unrewound client does: the position may hold a record no lane issued.
        #[test]
        fn a_rewound_client_retires_no_offsets() {
            let (_cluster, mut source, mut lanes) = assigned_on_mock(1);
            read_the_record(&mut source, &mut lanes[0]);

            lanes[0].forget_issued();
            assert!(
                lanes[0]
                    .poll(1, Duration::from_millis(10))
                    .expect("lane poll")
                    .is_some(),
                "an unrewound client retires"
            );

            lanes[0].forget_issued();
            source.members[0].consumer.context().mark_rewound();
            assert!(
                lanes[0]
                    .poll(1, Duration::from_millis(10))
                    .expect("lane poll")
                    .is_none(),
                "a rewound client retired"
            );
        }

        /// A store that librdkafka refuses for one partition of the list,
        /// while it accepts the others, fails the commit as retryable and
        /// names the partition.
        #[test]
        fn a_refused_element_fails_the_commit() {
            let mut cfg = test_config();
            // Bounds the close, which tries to commit the stored offset.
            cfg.rdkafka
                .insert("session.timeout.ms".into(), "6000".into());
            let mut source = opened_consumers(cfg, &[&[(0, 0), (1, 1)]]);
            // librdkafka holds partition 0 only; the source believes it owns both.
            source.members[0]
                .consumer
                .assign(&tpl_of(&[0]))
                .expect("local assign");
            match source.commit(&[(PartitionId(0), 5), (PartitionId(1), 5)]) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Retryable);
                    assert!(e.reason().contains("partition 1"), "{e}");
                }
                other => panic!("expected the refused element to fail the commit, got {other:?}"),
            }
        }

        /// A consumer poll's `UnknownTopicOrPartition` is retryable before the
        /// consumer's first assignment and fatal after it.
        #[test]
        fn unknown_topic_is_fatal_only_after_the_first_assignment() {
            for (started, class) in [(false, ErrorClass::Retryable), (true, ErrorClass::Fatal)] {
                let mut source = opened_consumers(test_config(), &[&[]]);
                source.members[0].saw_first_assignment = started;
                source.injected_poll_error = Some((
                    0,
                    rdkafka::error::KafkaError::MessageConsumption(
                        RDKafkaErrorCode::UnknownTopicOrPartition,
                    ),
                ));
                let err = source
                    .poll_events(Duration::ZERO)
                    .expect_err("the poll error");
                assert_eq!(err.class(), class, "started {started}: {err}");
                assert!(err.reason().starts_with("consumer poll"), "{err}");
            }
        }

        /// A store that librdkafka refuses for every partition of the list
        /// fails the commit as retryable.
        #[test]
        fn a_store_refused_for_every_partition_fails_the_commit() {
            // librdkafka holds no partition; the source believes it owns 0.
            let mut source = opened_consumers(test_config(), &[&[(0, 0)]]);
            let err = source
                .commit(&[(PartitionId(0), 5)])
                .expect_err("a store of an unassigned partition fails");
            assert_eq!(err.class(), ErrorClass::Retryable, "{err}");
            assert!(err.reason().starts_with("store offsets: "), "{err}");
        }

        /// A rebalance error on one consumer while another keeps its lanes
        /// opens no assignment deadline, since the source still holds
        /// partitions.
        #[test]
        fn an_error_beside_live_lanes_opens_no_deadline() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[(1, 1)]]);
            push_intent_to(
                &source,
                1,
                Intent::Error(RDKafkaErrorCode::RebalanceInProgress),
            );
            assert!(matches!(
                source.poll_events(Duration::ZERO),
                Ok(SourceEvent::LanesRevoked { .. })
            ));
            assert!(
                source.assignment_wait.is_none(),
                "consumer 0 still holds partition 0"
            );
        }

        /// A transport failure as a consumer poll returns it.
        fn transport_failure() -> rdkafka::error::KafkaError {
            rdkafka::error::KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure)
        }

        /// A call whose consumer poll returns a retryable error still acts on
        /// another consumer's queued assignment, and reports the poll error
        /// when holding that assignment yields no event. Regression for #1673.
        #[test]
        fn a_poll_error_does_not_leave_the_intent_queued() {
            let mut source = opened_consumers(test_config(), &[&[], &[], &[]]);
            for member in &mut source.members {
                member.phase = Phase::Joining;
            }
            push_intent_to(&source, 0, Intent::Assign(TopicPartitionList::new()));
            source.injected_poll_error = Some((1, transport_failure()));

            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Retryable, "{e}");
                    assert!(e.reason().starts_with("consumer poll"), "{e}");
                }
                other => panic!("expected the poll error, got {other:?}"),
            }
            assert!(matches!(source.members[0].phase, Phase::Held(_)));
        }

        /// A revocation acted on in a call whose consumer poll returns a
        /// retryable error is returned in place of that error, so the runtime
        /// drains the lanes before they are released. Regression for #1673.
        #[test]
        fn a_revocation_beside_a_poll_error_is_surfaced() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[]]);
            push_intent_to(&source, 0, Intent::Revoke(tpl_of(&[0])));
            source.injected_poll_error = Some((1, transport_failure()));

            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => {
                    assert_eq!(lane_ids(&lanes), [0]);
                }
                other => panic!("expected lane 0 revoked, got {other:?}"),
            }
            assert!(
                source.injected_poll_error.is_none() && source.last_consumer_error.is_some(),
                "the call polled the injected error"
            );
        }

        /// The drain that settling held assignments starts is returned in
        /// place of a retryable poll error in the same call.
        #[test]
        fn an_epoch_drain_beside_a_poll_error_is_surfaced() {
            let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[]]);
            push_intent_to(&source, 1, Intent::Assign(TopicPartitionList::new()));
            source.injected_poll_error = Some((0, transport_failure()));

            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, .. }) => {
                    assert_eq!(lane_ids(&lanes), [0]);
                }
                other => panic!("expected the live lanes drained, got {other:?}"),
            }
            assert!(
                source.injected_poll_error.is_none(),
                "the call polled the error"
            );
        }

        /// An error from settling the held assignments is reported in place
        /// of a retryable poll error in the same call.
        #[test]
        fn a_settle_error_beside_a_poll_error_is_reported() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            source.members[0].phase = Phase::Held(tpl_of(&[3]));
            source.members[1].phase = Phase::Held(tpl_of(&[1, 3]));
            source.members[0].generation = Some(7);
            source.members[1].generation = Some(7);
            source.injected_poll_error = Some((1, transport_failure()));

            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(e.reason().contains("partition 3"), "{e}");
                }
                other => panic!("expected the settle error, got {other:?}"),
            }
        }

        /// A fatal consumer poll error is reported by the call that polled
        /// it, before that call acts on a queued assignment.
        #[test]
        fn a_fatal_poll_error_beside_a_queued_intent_is_reported() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            source.members[0].phase = Phase::Joining;
            push_intent_to(&source, 0, Intent::Assign(TopicPartitionList::new()));
            source.injected_poll_error = Some((
                1,
                rdkafka::error::KafkaError::MessageConsumptionFatal(
                    RDKafkaErrorCode::FencedInstanceId,
                ),
            ));

            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                }
                other => panic!("expected the fatal poll error, got {other:?}"),
            }
            assert!(
                matches!(source.members[0].phase, Phase::Joining),
                "the assignment was acted on"
            );
        }

        /// A rebalance error acted on in a call whose consumer poll returns a
        /// retryable error is reported in place of that error.
        #[test]
        fn a_rebalance_error_beside_a_poll_error_is_reported() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            push_intent_to(
                &source,
                0,
                Intent::Error(RDKafkaErrorCode::GroupAuthorizationFailed),
            );
            source.injected_poll_error = Some((1, transport_failure()));

            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(e.reason().contains("rebalance error"), "{e}");
                }
                other => panic!("expected the rebalance error, got {other:?}"),
            }
        }

        // ANCHOR: hold_on_frozen_clock
        /// The hold runs from the first held assignment of a rebalance, and a
        /// later consumer's assignment does not extend it.
        #[test]
        fn a_later_assignment_does_not_extend_the_hold() {
            let mut source = opened_consumers(test_config(), &[&[], &[], &[]]);
            let clock = freeze(&mut source);
            for member in &mut source.members {
                member.phase = Phase::Joining;
            }
            push_intent_to(&source, 0, Intent::Assign(TopicPartitionList::new()));
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Held(_)));

            clock.advance(ASSIGN_HOLD - Duration::from_millis(1));
            push_intent_to(&source, 1, Intent::Assign(TopicPartitionList::new()));
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Held(_)));
            assert!(matches!(source.members[1].phase, Phase::Held(_)));

            // The clock is at the end of the first hold. A hold measured from
            // consumer 1's assignment would still be running.
            clock.advance(Duration::from_millis(1));
            poll_quiet(&mut source);
            assert!(matches!(source.members[0].phase, Phase::Assigned));
            assert!(matches!(source.members[1].phase, Phase::Assigned));
            assert!(matches!(source.members[2].phase, Phase::Joining));
        }
        // ANCHOR_END: hold_on_frozen_clock

        /// The source's `Debug` names its consumer count and live lanes.
        #[test]
        fn debug_names_the_consumers_and_lanes() {
            let source = opened_consumers(test_config(), &[&[(0, 0), (1, 1)], &[(2, 2)]]);
            let shown = format!("{source:?}");
            assert!(shown.contains("consumers: 2"), "{shown}");
            assert!(shown.contains("lanes: 3"), "{shown}");
        }

        /// Each consumer publishes lag only for its own partitions: the other
        /// consumer's snapshot carries the partition too, with the stale
        /// figure it last saw, and must not overwrite the owner's.
        #[test]
        fn lag_comes_from_the_owning_consumer_only() {
            for owner in [0, 1] {
                let component = spate_test::unique_name("source");
                let rendered = spate_test::render_metrics(|| {
                    let lanes: [&[(u32, i32)]; 2] = if owner == 0 {
                        [&[(0, 0)], &[]]
                    } else {
                        [&[], &[(0, 0)]]
                    };
                    let mut source = opened_consumers(test_config(), &lanes);
                    source.metrics = Some(Arc::new(SourceMetrics::new(
                        &spate_core::metrics::ComponentLabels::new(
                            "orders",
                            component.clone(),
                            "kafka",
                        ),
                    )));
                    for (k, member) in source.members.iter().enumerate() {
                        let lag = if k == owner { 5 } else { 100 };
                        let mut snapshot = parked(&[0]);
                        for p in snapshot
                            .topics
                            .get_mut("orders")
                            .unwrap()
                            .partitions
                            .values_mut()
                        {
                            p.consumer_lag = lag;
                        }
                        *member.consumer.context().stats.lock().unwrap() = Some(Box::new(snapshot));
                    }
                    source.publish_stats();
                });
                let series = format!(
                    r#"spate_source_lag_records{{pipeline="orders",component="{component}",component_type="kafka",partition="0"}} 5"#
                );
                assert!(rendered.contains(&series), "owner {owner}:\n{rendered}");
            }
        }

        /// A partition that moved from one consumer to another is released
        /// from the first consumer's series and kept in the second's.
        #[test]
        fn the_prune_releases_a_partition_that_moved_between_consumers() {
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            metrics::with_local_recorder(&recorder, || {
                let mut source = opened_consumers(test_config(), &[&[(0, 0)], &[]]);
                for k in 0..2 {
                    let meter = spate_core::metrics::Meter::with_namespace(
                        "kafka", "moved", "source", "kafka",
                    );
                    source.members[k].stats =
                        Some(crate::metrics::KafkaStatsMetrics::for_source_consumer(
                            meter,
                            true,
                            Some(k),
                        ));
                }
                let owned = source.members[0].live_partition_ids();
                source.members[0]
                    .stats
                    .as_mut()
                    .unwrap()
                    .update(&parked(&[0]), "orders", &owned);

                // Partition 0 moves to consumer 1.
                source.members[0].assignment.clear();
                source.members[1].assignment.insert(LaneId(5), 0);
                let owned = source.members[1].live_partition_ids();
                source.members[1]
                    .stats
                    .as_mut()
                    .unwrap()
                    .update(&parked(&[0]), "orders", &owned);
                source.prune_partition_series();
            });
            let rendered = handle.render();
            let std = r#"pipeline="moved",component="source",component_type="kafka""#;
            for (client, value) in [("consumer-0", 0), ("consumer-1", 1)] {
                let series = format!(
                    r#"spate_kafka_partition_not_fetching{{{std},client="{client}",partition="0"}} {value}"#
                );
                assert!(rendered.contains(&series), "{series}:\n{rendered}");
            }
        }
    }

    /// Dropping the source while one consumer's assignment is held, before
    /// the others have theirs, returns. Consumer close completes a held
    /// assignment on its own.
    #[test]
    fn dropping_with_a_held_assignment_returns() {
        use crate::testutil::mock::MockKafka;
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic("orders", 4, 1).expect("create topic");
        let mut cfg = test_config();
        cfg.brokers = cluster.bootstrap_servers();
        cfg.group_id = "drop-held".into();
        cfg.consumers = 3;
        cfg.rdkafka = std::collections::BTreeMap::from([(
            "session.timeout.ms".to_string(),
            "6000".to_string(),
        )]);
        let mut source = KafkaSource::new(cfg);
        let cp = Checkpointer::new();
        source.open(SourceCtx::new(cp.handle())).expect("open");
        spate_test::wait_until(Duration::from_secs(60), "an assignment held", || {
            let _ = source.poll_events(Duration::from_millis(50));
            source
                .members
                .iter()
                .any(|m| matches!(m.phase, Phase::Held(_)))
        });
        crate::testutil::assert_returns(
            cluster,
            Duration::from_secs(30),
            "dropping the source with an assignment held",
            move || drop(source),
        );
    }

    /// Dropping the source with a rebalance on a consumer's queue, as after
    /// the controller's last `poll_events`. The tests stop on librdkafka's
    /// group lines, and each asserts what the drop did, since a drop that
    /// hangs depends on a race these lines do not reproduce.
    mod queued_rebalance {
        use super::*;
        use crate::client::testing::{Raw as _, raw_config};
        use crate::testutil::mock::MockKafka;
        use crate::testutil::{ASSIGN_STOP, returns_within, stop_at};
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        use spate_test::{capture_logs, wait_until};
        use std::sync::mpsc;
        use tracing::Level;

        const DEADLINE: Duration = Duration::from_secs(60);
        const CLOSE: Duration = Duration::from_secs(30);
        const REVOKE_STOP: &str = "delegating revoke";
        const LOST_STOP: &str = r#"fac="LOST""#;
        /// The member starts rebalancing. Its `delegating revoke` line and the
        /// rebalance event follow.
        const REBALANCE_STOP: &str = r#"fac="REBALANCE""#;
        /// `unassign` ran. A serve that follows it would reach this line.
        const UNASSIGNED: &str = r#"fac="CLEARASSIGN""#;
        /// The first line after the revocation's callback calls `unassign`. The
        /// serve's count was read before it, so the serve stops short of it.
        const UNASSIGN_OP: &str = "received op ASSIGN";

        type Member = Arc<BaseConsumer<SourceContext>>;

        fn mock() -> MockKafka {
            let cluster = MockKafka::new();
            cluster.create_topic("orders", 2, 1).expect("create topic");
            cluster
        }

        /// A source of `consumers` logging librdkafka's group protocol.
        fn config(cluster: &MockKafka, group: &str, consumers: usize) -> KafkaSourceConfig {
            let mut cfg = test_config();
            cfg.brokers = cluster.bootstrap_servers();
            cfg.group_id = group.into();
            cfg.consumers = consumers;
            cfg.rdkafka = std::collections::BTreeMap::from([
                ("debug".to_string(), "cgrp".to_string()),
                ("session.timeout.ms".to_string(), "6000".to_string()),
            ]);
            cfg
        }

        /// An opened source, and its consumers, which outlive a drop of the
        /// source so a test can read them before they close.
        fn open(cfg: KafkaSourceConfig) -> (KafkaSource, Vec<Member>) {
            let mut source = KafkaSource::new(cfg);
            let cp = Checkpointer::new();
            source.open(SourceCtx::new(cp.handle())).expect("open");
            let members = source
                .members
                .iter()
                .map(|m| Arc::clone(&m.consumer))
                .collect();
            (source, members)
        }

        /// `poll_events` until an assignment is live, and return its lanes.
        pub(super) fn live_lanes(source: &mut KafkaSource) -> Vec<KafkaLane> {
            let mut lanes = Vec::new();
            wait_until(DEADLINE, "a live assignment", || {
                match source.poll_events(Duration::from_millis(50)) {
                    Ok(SourceEvent::LanesAssigned(new)) => {
                        lanes = new;
                        true
                    }
                    _ => false,
                }
            });
            lanes
        }

        /// A member with no generation commits nothing and loses its offer, since
        /// the broker takes a generation of -1 as a simple consumer's and it could
        /// replace a peer's offset. A member with one sends the commit, whose
        /// scripted failure holds the offer.
        #[test]
        fn a_member_with_no_generation_is_discarded_with_no_commit() {
            let cluster = mock();
            cluster.request_errors(
                RDKafkaApiKey::OffsetCommit,
                &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
            );
            let mut cfg = config(&cluster, "no-generation", 1);
            cfg.rdkafka
                .insert("auto.offset.reset".to_string(), "latest".to_string());
            let (mut source, _members) = open(cfg);
            let mut to_commit = TopicPartitionList::new();
            to_commit
                .add_partition_offset("orders", 0, Offset::Offset(0))
                .expect("offset");
            let starts = |to_commit: &TopicPartitionList| {
                vec![Prepared {
                    list: tpl_of(&[0]),
                    to_commit: to_commit.clone(),
                }]
            };
            source.members[0].generation = Some(-1);
            let (kept, _, retry) = source
                .commit_starts(vec![(0, tpl_of(&[0]))], starts(&to_commit))
                .expect("no fatal");
            assert!(kept.is_empty() && retry.is_none());
            source.members[0].generation = Some(0);
            let (kept, _, retry) = source
                .commit_starts(vec![(0, tpl_of(&[0]))], starts(&to_commit))
                .expect("no fatal");
            assert_eq!(kept.len(), 1);
            assert!(retry.is_some());
        }

        /// A reassign whose lookup fails keeps the offer of the consumer that
        /// held one and the partitions of the consumer that was only drained,
        /// and once the pace comes due every consumer's lanes arrive as one set.
        #[test]
        fn a_reassign_whose_lookup_fails_is_retried_as_one_lane_set() {
            let cluster = mock();
            cluster.hold_first_rebalance();
            let mut cfg = config(&cluster, "retried-reassign", 2);
            cfg.rdkafka
                .insert("auto.offset.reset".to_string(), "latest".to_string());
            let mut source = KafkaSource::new(cfg);
            let clock = freeze(&mut source);
            let cp = Checkpointer::new();
            source.open(SourceCtx::new(cp.handle())).expect("open");
            // The first accept can go to one consumer alone, so wait for the group
            // to give each consumer a lane.
            let mut lanes = Vec::new();
            wait_until(DEADLINE, "a lane on each consumer", || {
                if let Ok(SourceEvent::LanesAssigned(new)) =
                    source.poll_events(Duration::from_millis(50))
                {
                    lanes = new;
                }
                source.members.iter().all(|m| !m.assignment.is_empty())
            });
            let held = 0;
            let partition = source.members[held]
                .assignment
                .values()
                .copied()
                .next()
                .expect("a partition");
            // An offer for one consumer while lanes are live: the source drains
            // every lane, and the other consumer's come back from its revoking set.
            push_intent_to(&source, held, Intent::Assign(tpl_of(&[partition])));
            wait_until(DEADLINE, "the drain", || {
                matches!(
                    source.poll_events(Duration::from_millis(10)),
                    Ok(SourceEvent::LanesRevoked { .. })
                )
            });
            drop(lanes);
            cluster.request_errors(
                RDKafkaApiKey::OffsetFetch,
                &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
            );
            assert!(matches!(
                source.poll_events(Duration::ZERO),
                Ok(SourceEvent::Idle)
            ));
            assert!(source.start_retry_at.is_some(), "the lookup failed");
            assert!(matches!(source.members[held].phase, Phase::Held(_)));
            assert!(matches!(source.members[1 - held].phase, Phase::Assigned));
            clock.advance(START_RETRY_PACE);
            let mut assigned = None;
            wait_until(DEADLINE, "the next lane set", || {
                match source.poll_events(Duration::from_millis(10)) {
                    Ok(SourceEvent::LanesAssigned(new)) => {
                        assigned = Some(new.len());
                        drop(new);
                        true
                    }
                    _ => false,
                }
            });
            assert_eq!(assigned, Some(2), "every consumer's lanes arrive together");
        }

        /// A retry outside a reassign leaves no reassign behind, so the next
        /// attempt still waits for a consumer that has not joined.
        #[test]
        fn a_retried_accept_arms_no_reassign() {
            let (mut source, _clock) = opened_unassigned(latest_config(), 1);
            let offers = vec![(0, tpl_of(&[0]))];
            let _ = source.retry_starts(offers, &[0], false, "a reason");
            assert_eq!(source.after_drain, None);
        }

        /// A committed-offset lookup that fails for one partition holds the offer
        /// under `latest`, since leaving it to librdkafka would start the
        /// partition at its end. `earliest` leaves it to librdkafka.
        #[test]
        fn a_partition_whose_lookup_fails_holds_the_offer_under_latest() {
            let cluster = mock();
            let open_on = |policy: &str| {
                let mut cfg = config(&cluster, &format!("element-{policy}"), 1);
                cfg.rdkafka
                    .insert("auto.offset.reset".to_string(), policy.to_string());
                open(cfg).0
            };
            let until = || Instant::now() + COMMITTED_LOOKUP_TIMEOUT;
            let mut latest = open_on("latest");
            assert!(matches!(
                latest.read_latest_starts(&[(0, tpl_of(&[99]))], until()),
                Ok(Err(_))
            ));
            let mut earliest = open_on("earliest");
            let list = earliest.start_positions(0, &tpl_of(&[99]), until());
            assert_eq!(list.elements()[0].offset(), Offset::Stored);
        }

        /// A reassign waiting on its pace stays pending while the source keeps
        /// polling, and runs once the pace comes due.
        #[test]
        fn a_pending_reassign_waits_for_its_pace() {
            let (mut source, clock) = opened_unassigned(latest_config(), 1);
            source.after_drain = Some(AfterDrain::Reassign);
            source.start_retry_at = Some(source.clock.now() + START_RETRY_PACE);
            assert!(matches!(
                source.poll_events(Duration::ZERO),
                Ok(SourceEvent::Idle)
            ));
            assert_eq!(source.after_drain, Some(AfterDrain::Reassign));
            clock.advance(START_RETRY_PACE);
            let _ = source.poll_events(Duration::ZERO);
            assert_eq!(source.after_drain, None);
        }

        /// A rebalance error on a partition with only its start commit restarts
        /// from that commit. Regression for #1704.
        #[test]
        fn a_rebalance_error_on_a_never_committed_partition_loses_no_record() {
            let cluster = mock();
            let mut cfg = config(&cluster, "error-start", 1);
            cfg.rdkafka
                .insert("auto.offset.reset".to_string(), "latest".to_string());
            let (mut source, members) = open(cfg);
            let lanes = live_lanes(&mut source);
            drop(lanes);
            push_intent_to(
                &source,
                0,
                Intent::Error(RDKafkaErrorCode::RebalanceInProgress),
            );
            wait_until(DEADLINE, "the error's revocation", || {
                matches!(
                    source.poll_events(Duration::from_millis(50)),
                    Ok(SourceEvent::LanesRevoked { .. })
                )
            });
            let committed = members[0]
                .committed_offsets(tpl_of(&[0, 1]), Duration::from_secs(10))
                .expect("committed");
            assert!(
                committed
                    .elements()
                    .iter()
                    .all(|e| matches!(e.offset(), Offset::Offset(0))),
                "the next start reads from the start commit: {committed:?}"
            );
        }

        /// A member of `group` that takes a partition from the source. Its
        /// join needs no poll.
        fn joiner(cluster: &MockKafka, group: &str) -> BaseConsumer {
            let c: BaseConsumer = raw_config()
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .set("group.id", group)
                .build()
                .expect("joiner");
            c.subscribe(&["orders"]).expect("subscribe");
            c
        }

        /// Serve `consumer` until its queue reads empty, so a failed assert
        /// unwinds into a close with nothing queued.
        fn empty_queue(consumer: &BaseConsumer<SourceContext>) {
            wait_until(DEADLINE, "an empty queue", || {
                let _ = consumer.poll(Duration::ZERO);
                queued_events(consumer) == 0
            });
        }

        fn assert_no_intents(members: &[Member]) {
            for consumer in members {
                let intents = consumer.context().intents.lock().expect("intent lock");
                assert!(intents.is_empty(), "{intents:?}");
            }
        }

        /// Run `close`, and fail if it does not return.
        fn assert_closes(cluster: MockKafka, close: impl FnOnce() + Send + 'static) {
            crate::testutil::assert_returns(cluster, CLOSE, "a consumer's close", close);
        }

        /// Run `drop_source` under a capture on a thread of its own, so a drop
        /// that never returns fails under `CLOSE`, and return `cluster` and the
        /// lines. The capture is scoped to the thread that runs the drop.
        fn lines_of_drop<C>(
            cluster: C,
            level: Level,
            drop_source: impl FnOnce() + Send + 'static,
        ) -> (C, Vec<String>) {
            let (lines, captured) = mpsc::channel();
            let returned = returns_within(CLOSE, move || {
                let _ = lines.send(capture_logs(level, drop_source));
            });
            if !returned {
                std::mem::forget(cluster);
                panic!("dropping the source did not return");
            }
            (cluster, captured.recv().expect("the captured lines"))
        }

        /// The drop serves each consumer's queued assignment with no `assign`
        /// and ahead of its `unassign`, whether or not a leave is pending. A
        /// static member has none. Regression for #1683.
        #[test]
        fn dropping_with_a_queued_assignment_returns() {
            for (group, consumers, static_id) in [
                ("drop-queued-assign", 2, None),
                ("drop-queued-assign-static", 1, Some("worker-a")),
            ] {
                let cluster = mock();
                cluster.hold_first_rebalance();
                let mut cfg = config(&cluster, group, consumers);
                if let Some(id) = static_id {
                    cfg.rdkafka.insert("group.instance.id".into(), id.into());
                }
                let (source, members) = open(cfg);
                for consumer in &members {
                    stop_at(consumer, ASSIGN_STOP);
                }
                let (cluster, lines) = lines_of_drop(cluster, Level::DEBUG, move || drop(source));
                members.iter().for_each(|c| empty_queue(c));

                for k in 0..consumers {
                    let named = format!("consumer={k}");
                    assert!(
                        lines.iter().any(|l| l.contains("delegating assign")
                            && (consumers == 1 || l.contains(&named))),
                        "{group}: consumer {k}'s assignment was not served: {lines:#?}"
                    );
                }
                assert!(
                    !lines.iter().any(|l| l.contains(UNASSIGNED)),
                    "{group}: a serve ran after unassign: {lines:#?}"
                );
                for consumer in &members {
                    assert_eq!(consumer.assignment().expect("assignment").count(), 0);
                }
                assert_no_intents(&members);
                assert_closes(cluster, move || drop(members));
            }
        }

        /// The serve reaches a rebalance behind the log lines ahead of it,
        /// which one poll does not.
        #[test]
        fn serving_a_consumer_reaches_a_rebalance_behind_its_log_lines() {
            let cluster = mock();
            let (source, members) = open(config(&cluster, "drop-queued-serve", 1));
            stop_at(&members[0], ASSIGN_STOP);
            serve_queued(&members[0]);
            {
                let intents = members[0].context().intents.lock().expect("intent lock");
                assert!(
                    matches!(intents.front(), Some(Intent::Assign(_))),
                    "{intents:?}"
                );
            }
            drop(source);
            empty_queue(&members[0]);
            assert_closes(cluster, move || drop(members));
        }

        /// The serve keeps going past an error event, which the error hook
        /// logs.
        #[test]
        fn dropping_with_an_error_event_queued_returns() {
            let mut cfg = test_config();
            cfg.rdkafka
                .insert("reconnect.backoff.ms".into(), "60000".into());
            cfg.rdkafka
                .insert("reconnect.backoff.max.ms".into(), "60000".into());
            let source = opened_consumers(cfg, &[&[]]);
            let consumer = Arc::clone(&source.members[0].consumer);
            // A refused connection queues a log line, then two error events.
            wait_until(DEADLINE, "error events queued", || {
                queued_events(&consumer) >= 3
            });
            let ((), lines) = lines_of_drop((), Level::WARN, move || drop(source));
            let errors = lines
                .iter()
                .filter(|l| l.contains("librdkafka") && l.contains("error="))
                .count();
            assert_eq!(errors, 2, "the serve stopped at an error event: {lines:#?}");
            assert!(
                returns_within(CLOSE, move || drop(consumer)),
                "a consumer's close did not return"
            );
        }

        /// The drop's serve completes a queued revocation, and close then
        /// returns.
        #[test]
        fn dropping_with_a_queued_revocation_returns() {
            let cluster = mock();
            let group = "drop-queued-revoke";
            let (mut source, members) = open(config(&cluster, group, 1));
            let lanes = live_lanes(&mut source);
            let joiner = joiner(&cluster, group);
            stop_at(&members[0], REBALANCE_STOP);
            let (cluster, lines) = lines_of_drop(cluster, Level::DEBUG, move || {
                drop(lanes);
                drop(source);
            });
            empty_queue(&members[0]);
            assert!(
                lines.iter().any(|l| l.contains(REVOKE_STOP)),
                "the revocation was not served: {lines:#?}"
            );
            assert_no_intents(&members);
            assert_closes(cluster, move || {
                drop(members);
                drop(joiner);
            });
        }

        /// The drop serves a queued revocation of a lost assignment ahead of
        /// the `unassign` that would otherwise answer it and leave its event
        /// stale. An assignment can queue behind a revocation only once the
        /// revocation is answered, so this also holds that order. Regression
        /// for #1683.
        #[test]
        fn dropping_with_a_queued_lost_revocation_returns() {
            let cluster = mock();
            let (mut source, members) = open(config(&cluster, "drop-queued-lost", 1));
            let lanes = live_lanes(&mut source);
            cluster.request_errors(
                RDKafkaApiKey::Heartbeat,
                &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID],
            );
            stop_at(&members[0], LOST_STOP);
            let (cluster, lines) = lines_of_drop(cluster, Level::DEBUG, move || {
                drop(lanes);
                drop(source);
            });
            empty_queue(&members[0]);
            assert!(
                lines.iter().any(|l| l.contains(REVOKE_STOP)),
                "the revocation was not served: {lines:#?}"
            );
            assert!(
                !lines.iter().any(|l| l.contains(UNASSIGNED)),
                "a serve ran after unassign: {lines:#?}"
            );
            assert!(
                !lines.iter().any(|l| l.contains(UNASSIGN_OP)),
                "the serve went past its count: {lines:#?}"
            );
            assert_closes(cluster, move || drop(members));
        }

        /// Pins that a lane outliving the source keeps its consumer open, and
        /// that the last lane's drop closes it with the revocation completed.
        #[test]
        fn dropping_the_source_before_its_lanes_with_a_queued_revocation_completes() {
            let cluster = mock();
            let group = "drop-queued-lanes";
            let (mut source, mut members) = open(config(&cluster, group, 1));
            let lanes = live_lanes(&mut source);
            let joiner = joiner(&cluster, group);
            let consumer = members.pop().expect("one consumer");
            stop_at(&consumer, REVOKE_STOP);
            drop(consumer);
            assert_closes(cluster, move || {
                drop(source);
                drop(lanes);
                drop(joiner);
            });
        }
    }

    /// A consumer of `group` on `brokers`, outside the source, that answers
    /// its own rebalances.
    fn raw_group_member(brokers: &str, group: &str) -> rdkafka::consumer::BaseConsumer {
        use crate::client::testing::{Raw as _, raw_config};
        raw_config()
            .set("bootstrap.servers", brokers)
            .set("group.id", group)
            .set("session.timeout.ms", "6000")
            .build()
            .expect("raw consumer")
    }

    /// A source of `consumers` consumers on `brokers` sharing four partitions,
    /// with an offset stored for every partition.
    fn settled_with_stored_offsets(brokers: &str, group: &str, consumers: usize) -> KafkaSource {
        let mut cfg = test_config();
        cfg.brokers = brokers.to_owned();
        cfg.group_id = group.into();
        cfg.consumers = consumers;
        cfg.commit_interval = Duration::from_hours(1);
        cfg.rdkafka = std::collections::BTreeMap::from([
            ("session.timeout.ms".to_string(), "6000".to_string()),
            ("heartbeat.interval.ms".to_string(), "500".to_string()),
            ("auto.offset.reset".to_string(), "earliest".to_string()),
        ]);
        let mut source = KafkaSource::new(cfg);
        let cp = Checkpointer::new();
        source.open(SourceCtx::new(cp.handle())).expect("open");
        spate_test::wait_until(Duration::from_secs(60), "every partition held", || {
            if let Ok(SourceEvent::LanesRevoked { lanes, barrier }) =
                source.poll_events(Duration::from_millis(50))
            {
                for _ in &lanes {
                    barrier.arrive();
                }
            }
            let held: usize = source.members.iter().map(|m| m.assignment.len()).sum();
            held == 4 && source.members.iter().all(|m| !m.assignment.is_empty())
        });
        let watermarks: Vec<(PartitionId, i64)> = (0..4).map(|p| (PartitionId(p), 1)).collect();
        source.commit(&watermarks).expect("store");
        source
    }

    /// A consumer whose flush fails does not hold back its peer's commit, in
    /// either order, and the call still reports the failure.
    #[test]
    fn a_lost_member_does_not_hold_back_its_peers_commit() {
        use crate::testutil::mock::MockKafka;
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic("orders", 4, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let mut source = settled_with_stored_offsets(&brokers, "lost-first", 2);

        cluster.request_errors(
            RDKafkaApiKey::Heartbeat,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION],
        );
        spate_test::wait_until(Duration::from_secs(30), "a member marked lost", || {
            source.members.iter().any(|m| m.consumer.assignment_lost())
        });
        let lost = source
            .members
            .iter()
            .position(|m| m.consumer.assignment_lost())
            .expect("lost");
        // The lost member flushes first, so an early return skips its peer.
        source.members.swap(0, lost);
        let owned: Vec<i32> = source.members[1].assignment.values().copied().collect();

        match source.flush_commits() {
            Err(e) => {
                assert_eq!(e.class(), ErrorClass::Retryable, "{e}");
                assert!(e.reason().contains("AssignmentLost"), "{e}");
            }
            other => panic!("expected the lost member's failure, got {other:?}"),
        }
        let read_back = crate::testutil::committed_offsets(&brokers, "lost-first", "orders", 0..4);
        for p in owned {
            assert_eq!(
                read_back[usize::try_from(p).expect("partition")],
                Offset::Offset(1),
                "peer's partition {p}: {read_back:?}"
            );
        }
    }

    /// Two consumers failing for different reasons: the call reports the
    /// first consumer's.
    #[test]
    fn a_flush_reports_the_first_failure() {
        use crate::testutil::mock::MockKafka;
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic("orders", 4, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let mut source = settled_with_stored_offsets(&brokers, "first-failure", 2);

        cluster.request_errors(
            RDKafkaApiKey::Heartbeat,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION],
        );
        spate_test::wait_until(Duration::from_secs(30), "a member marked lost", || {
            source.members.iter().any(|m| m.consumer.assignment_lost())
        });
        let lost = source
            .members
            .iter()
            .position(|m| m.consumer.assignment_lost())
            .expect("lost");
        source.members.swap(0, lost);
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION],
        );
        match source.flush_commits() {
            Err(e) => {
                assert!(e.reason().contains("AssignmentLost"), "{e}");
            }
            other => panic!("expected the lost member's failure, got {other:?}"),
        }
    }

    /// With one consumer the failed commit logs no line of its own, since the
    /// controller's line on the returned error says the same.
    #[test]
    fn a_failed_commit_on_one_consumer_logs_no_line_of_its_own() {
        use crate::testutil::mock::MockKafka;
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let cluster = MockKafka::new();
        cluster.create_topic("orders", 4, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let mut source = settled_with_stored_offsets(&brokers, "refused-one", 1);

        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION],
        );
        let mut flushed = None;
        let warned = spate_test::capture_logs(tracing::Level::WARN, || {
            flushed = Some(source.flush_commits());
        });
        let err = flushed
            .expect("flush ran")
            .expect_err("the broker refuses the commit");
        assert_eq!(err.class(), ErrorClass::Retryable, "{err}");
        assert!(err.reason().starts_with("sync commit: "), "{err}");
        assert!(
            !warned.iter().any(|l| l.contains("sync commit failed")),
            "{warned:#?}"
        );
    }

    /// Each consumer whose commit the broker refuses logs its own line.
    #[test]
    fn a_failed_commit_names_each_consumer() {
        use crate::testutil::mock::MockKafka;
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic("orders", 4, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let mut source = settled_with_stored_offsets(&brokers, "refused-commits", 2);

        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION; 2],
        );
        let mut flushed = None;
        let warned = spate_test::capture_logs(tracing::Level::WARN, || {
            flushed = Some(source.flush_commits());
        });
        let err = flushed
            .expect("flush ran")
            .expect_err("the broker refuses the commit");
        assert_eq!(err.class(), ErrorClass::Retryable, "{err}");
        assert!(err.reason().starts_with("sync commit: "), "{err}");
        for k in 0..2 {
            let field = format!("consumer={k}");
            assert!(
                warned.iter().any(|l| l.contains("sync commit failed")
                    && l.contains(&field)
                    && l.contains("error=")),
                "{field}: {warned:#?}"
            );
        }
    }

    /// A source polled until a consumer holds an offer, that consumer's index,
    /// and a raw consumer of the same group polled until the group has handed it
    /// every partition, so the group has left the held member.
    fn held_while_the_group_moves_on(
        brokers: &str,
        group: &str,
    ) -> (KafkaSource, usize, rdkafka::consumer::BaseConsumer) {
        let mut cfg = test_config();
        cfg.brokers = brokers.to_owned();
        cfg.group_id = group.into();
        cfg.consumers = 2;
        cfg.rdkafka = std::collections::BTreeMap::from([(
            "session.timeout.ms".to_string(),
            "6000".to_string(),
        )]);
        let mut source = KafkaSource::new(cfg);
        let cp = Checkpointer::new();
        source.open(SourceCtx::new(cp.handle())).expect("open");
        let mut held = None;
        spate_test::wait_until(Duration::from_secs(60), "an assignment held", || {
            let _ = source.poll_events(Duration::from_millis(50));
            held = source
                .members
                .iter()
                .position(|m| matches!(m.phase, Phase::Held(_)));
            held.is_some()
        });
        let raw = raw_group_member(brokers, group);
        raw.subscribe(&["orders"]).expect("subscribe");
        spate_test::wait_until(
            Duration::from_secs(60),
            "the raw consumer holds all",
            || {
                let _ = raw.poll(Duration::from_millis(100));
                raw.assignment().is_ok_and(|a| a.count() == 4)
            },
        );
        (source, held.expect("held"), raw)
    }

    /// Dropping a source whose held consumer the group has removed returns.
    #[test]
    fn dropping_with_a_stale_held_offer_returns() {
        use crate::testutil::mock::MockKafka;
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic("orders", 4, 1).expect("create topic");
        let (source, _, raw) =
            held_while_the_group_moves_on(&cluster.bootstrap_servers(), "drop-stale");
        crate::testutil::assert_returns(
            (cluster, raw),
            Duration::from_secs(30),
            "dropping the source with a stale offer held",
            move || drop(source),
        );
    }

    /// A held consumer the group has removed reads a generation older than
    /// the group's, and its client does not report the assignment lost.
    #[test]
    fn a_member_the_group_left_reads_an_older_generation() {
        use crate::testutil::mock::MockKafka;
        let cluster = MockKafka::new();
        cluster.hold_first_rebalance();
        cluster.create_topic("orders", 4, 1).expect("create topic");
        let (source, held, raw) =
            held_while_the_group_moves_on(&cluster.bootstrap_servers(), "stale-generation");

        let member = &source.members[held];
        let generation = member.group_generation();
        assert!(generation >= 0, "a member that joined reads {generation}");
        assert!(generation < crate::client::group_generation(&raw));
        assert!(!member.consumer.assignment_lost());
    }

    /// Queue a rebalance intent for `poll_events` on consumer 0, as the
    /// callback does.
    fn push_intent(source: &KafkaSource, intent: Intent) {
        push_intent_to(source, 0, intent);
    }

    /// Queue a rebalance intent on consumer `k`.
    fn push_intent_to(source: &KafkaSource, k: usize, intent: Intent) {
        source.members[k]
            .consumer
            .context()
            .intents
            .lock()
            .expect("intent lock")
            .push_back(intent);
    }

    fn push_error(source: &KafkaSource, code: RDKafkaErrorCode) {
        push_intent(source, Intent::Error(code));
    }

    mod rebalance_error {
        use super::*;

        /// A rebalance error with live lanes surfaces their revocation
        /// first, so the runtime drains them, and reports the classified
        /// error on the next call. Ownership bookkeeping is cleared: the
        /// callback already released the partitions, so nothing may remain
        /// committable.
        #[test]
        fn live_lanes_are_revoked_then_the_error_reports() {
            let mut source = opened_source(&[(0, 0), (1, 1)]);
            push_error(&source, RDKafkaErrorCode::RebalanceInProgress);

            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { mut lanes, barrier }) => {
                    lanes.sort();
                    assert_eq!(lanes, vec![LaneId(0), LaneId(1)]);
                    assert_eq!(barrier.remaining(), 2);
                    barrier.arrive();
                    barrier.arrive();
                }
                other => panic!("expected LanesRevoked, got {other:?}"),
            }
            assert!(source.members[0].assignment.is_empty(), "ownership cleared");
            assert!(source.committable_partitions(0).is_empty());

            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Retryable, "transient code: {e}");
                    assert!(e.reason().contains("rebalance error"), "{e}");
                }
                other => panic!("expected the classified error, got {other:?}"),
            }
        }

        /// With nothing assigned the classified error reports immediately,
        /// and a permanent code (an authorization failure) is fatal rather
        /// than retried forever behind a green probe.
        #[test]
        fn an_authorization_error_is_fatal() {
            let mut source = opened_source(&[]);
            push_error(&source, RDKafkaErrorCode::GroupAuthorizationFailed);

            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(e.reason().contains("rebalance error"), "{e}");
                }
                other => panic!("expected a fatal error, got {other:?}"),
            }
        }
    }

    mod fatal_consumer_error {
        use super::*;
        use rdkafka::bindings::rd_kafka_resp_err_t as RespErr;
        use std::ffi::CStr;

        /// A static member librdkafka fences fails the source, and the error
        /// carries librdkafka's text and the member's `group.instance.id`.
        /// Regression for #727.
        #[test]
        fn a_fenced_static_member_is_fatal() {
            let mut cfg = test_config();
            cfg.rdkafka
                .insert("group.instance.id".into(), "worker-a".into());
            let mut source = opened_source_with(cfg, &[]);
            raise_fatal(
                &source,
                0,
                RespErr::RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                c"fenced by test",
            );

            let mut seen = None;
            spate_test::wait_until(Duration::from_secs(20), "the fatal error", || match source
                .poll_events(Duration::from_millis(50))
            {
                Err(e) if e.reason().contains("Static consumer fenced") => {
                    seen = Some((e.class(), e.reason().to_owned()));
                    true
                }
                _ => false,
            });
            let (class, reason) = seen.expect("wait_until returned");
            assert_eq!(class, ErrorClass::Fatal, "{reason}");
            assert!(
                reason.contains("test_fatal_error: fenced by test"),
                "{reason}"
            );
            assert!(
                reason.contains(r#"group.instance.id "worker-a""#),
                "{reason}"
            );
        }

        /// Raise `code` as consumer `k`'s fatal error, with `reason` as its
        /// text.
        fn raise_fatal(source: &KafkaSource, k: usize, code: RespErr, reason: &CStr) {
            crate::testutil::raise_fatal(source.members[k].consumer.client(), code, reason);
        }

        /// Call `poll_events` until `done` holds for the lines logged at
        /// WARN and above so far, and return them.
        ///
        /// Which events one call meets depends on librdkafka's queue order,
        /// so a test asserts on what every call together logged.
        fn lines_until(
            source: &mut KafkaSource,
            what: &str,
            done: impl Fn(&[String]) -> bool,
        ) -> Vec<String> {
            let mut lines = Vec::new();
            spate_test::wait_until(Duration::from_secs(20), what, || {
                lines.extend(spate_test::capture_logs(tracing::Level::WARN, || {
                    let _ = source.poll_events(Duration::from_millis(50));
                }));
                done(&lines)
            });
            lines
        }

        /// Whether a line names consumer `k` and contains `text`.
        fn names(line: &str, k: usize, text: &str) -> bool {
            line.contains(&format!("consumer={k}")) && line.contains(text)
        }

        /// A fatal error one consumer returns is reported even when an
        /// earlier consumer in the same poll returned a retryable one.
        #[test]
        fn a_fatal_error_behind_a_retryable_one_is_reported() {
            let mut cfg = test_config();
            cfg.rdkafka
                .insert("reconnect.backoff.ms".into(), "60000".into());
            cfg.rdkafka
                .insert("reconnect.backoff.max.ms".into(), "60000".into());
            // Leaked: closing a consumer after a fatal error waits on a
            // broker this test never brings up.
            let mut source = std::mem::ManuallyDrop::new(opened_consumers(cfg, &[&[], &[]]));

            // Serve consumer 1's refused connection and AllBrokersDown first,
            // so the fatal error is the next event on its queue.
            let c1 = Arc::clone(&source.members[1].consumer);
            let mut c1_errors = Vec::new();
            spate_test::wait_until(Duration::from_secs(20), "consumer 1's errors", || {
                if let Some(Err(e)) = c1.poll(Duration::from_millis(50)) {
                    c1_errors.push(e.to_string());
                }
                c1_errors.iter().any(|e| e.contains("AllBrokersDown"))
            });
            // Leave exactly consumer 0's two retryable errors queued, so it
            // returns one in the call that also polls consumer 1.
            spate_test::wait_until(Duration::from_secs(20), "consumer 0's errors", || {
                queued_events(&source.members[0].consumer) >= 2
            });
            let c0 = Arc::clone(&source.members[0].consumer);
            while queued_events(&source.members[0].consumer) > 2 {
                assert!(
                    c0.poll(Duration::ZERO).is_none(),
                    "only non-error events precede"
                );
            }
            raise_fatal(
                &source,
                1,
                RespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
                c"raised by test",
            );

            source.poll_start = 0;
            let mut returned = Vec::new();
            let mut fatal = None;
            for _ in 0..20 {
                match source.poll_events(Duration::from_millis(50)) {
                    Err(e) if e.class() == ErrorClass::Fatal => {
                        fatal = Some(e.reason().to_owned());
                        break;
                    }
                    Err(e) => returned.push(e.to_string()),
                    Ok(_) => {}
                }
            }
            assert!(
                fatal.is_some(),
                "consumer 1's fatal error was never reported; poll_events returned: {returned:#?}"
            );
        }

        /// Above one consumer, each poll error's line names the consumer that
        /// returned it, `AllBrokersDown` included, whichever error a call
        /// reports. Regression for #1663.
        #[test]
        fn each_consumer_poll_error_names_its_consumer() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            let lines = lines_until(&mut source, "both consumers' errors", |lines| {
                (0..2).all(|k| {
                    ["BrokerTransportFailure", "AllBrokersDown"]
                        .iter()
                        .all(|text| lines.iter().any(|l| names(l, k, text)))
                })
            });
            let errors: Vec<&String> = lines.iter().filter(|l| l.contains("error=")).collect();
            assert!(
                errors
                    .iter()
                    .all(|l| l.contains("consumer=0") || l.contains("consumer=1")),
                "{errors:#?}"
            );
        }

        /// A fatal error on each consumer is logged under its own index.
        #[test]
        fn each_consumer_fatal_error_names_its_consumer() {
            // Leaked, as in `a_fatal_error_behind_a_retryable_one_is_reported`.
            let mut source =
                std::mem::ManuallyDrop::new(opened_consumers(test_config(), &[&[], &[]]));
            raise_fatal(
                &source,
                0,
                RespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
                c"raised on 0",
            );
            raise_fatal(
                &source,
                1,
                RespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
                c"raised on 1",
            );

            let texts = ["raised on 0", "raised on 1"];
            let lines = lines_until(&mut source, "both fatal errors", |lines| {
                texts
                    .iter()
                    .enumerate()
                    .all(|(k, text)| lines.iter().any(|l| names(l, k, text)))
            });
            for (k, text) in texts.iter().enumerate() {
                for line in lines.iter().filter(|l| l.contains(text)) {
                    assert!(names(line, k, text), "{line}");
                }
            }
        }

        /// One consumer's librdkafka lines carry no `consumer` field.
        #[test]
        fn one_consumer_error_lines_are_unchanged() {
            let mut source = opened_consumers(test_config(), &[&[]]);
            let lines = lines_until(&mut source, "a poll error", |lines| {
                lines.iter().any(|l| l.contains("BrokerTransportFailure"))
            });
            let librdkafka: Vec<&String> =
                lines.iter().filter(|l| l.contains("librdkafka")).collect();
            assert!(
                librdkafka.iter().all(|l| !l.contains("consumer=")),
                "{librdkafka:#?}"
            );
        }

        /// Above one consumer, every librdkafka line a consumer forwards
        /// through `poll_events` names that consumer. Regression for #1658.
        #[test]
        fn forwarded_lines_name_their_consumer() {
            let mut source = opened_consumers(test_config(), &[&[], &[]]);
            let lines = lines_until(&mut source, "a forwarded line per consumer", |lines| {
                (0..2).all(|k| lines.iter().any(|l| names(l, k, "fac=")))
            });
            let forwarded: Vec<&String> = lines.iter().filter(|l| l.contains("fac=")).collect();
            assert!(
                forwarded
                    .iter()
                    .all(|l| l.contains("consumer=0") || l.contains("consumer=1")),
                "{forwarded:#?}"
            );
        }
    }
    #[cfg(feature = "tls")]
    mod tls_rejection {
        use super::*;

        /// The first consumer error, other than `AllBrokersDown`, from a
        /// source whose only broker answers the TLS handshake with `alert`.
        fn first_handshake_error(alert: u8) -> (ErrorClass, String) {
            let mut cfg = test_config();
            cfg.brokers = spate_test::tls_alert_server(b"", alert).to_string();
            cfg.rdkafka.insert("security.protocol".into(), "ssl".into());
            let mut source = KafkaSource::new(cfg);
            let cp = Checkpointer::new();
            source.open(SourceCtx::new(cp.handle())).expect("open");
            let mut seen = None;
            spate_test::wait_until(
                Duration::from_secs(20),
                "a handshake error",
                || match source.poll_events(Duration::from_millis(50)) {
                    Err(e) if !e.reason().contains("AllBrokersDown") => {
                        seen = Some((e.class(), e.reason().to_owned()));
                        true
                    }
                    _ => false,
                },
            );
            seen.expect("wait_until returned")
        }

        /// A listed alert from the broker fails the source, and the error
        /// carries librdkafka's text; `decode_error` (50) and
        /// `internal_error` (80) stay `Retryable`.
        #[test]
        fn a_rejecting_tls_alert_is_fatal() {
            for (alert, expected) in [
                (40, ErrorClass::Fatal),
                (48, ErrorClass::Fatal),
                (70, ErrorClass::Fatal),
                (116, ErrorClass::Fatal),
                (50, ErrorClass::Retryable),
                (80, ErrorClass::Retryable),
            ] {
                let (class, reason) = first_handshake_error(alert);
                assert!(
                    reason.contains(&format!("SSL alert number {alert} ")),
                    "alert {alert}: {reason}"
                );
                assert_eq!(class, expected, "alert {alert}: {reason}");
            }
        }
    }

    mod assignment_deadline {
        use super::*;

        /// Past the deadline the member reports a fatal error, naming the
        /// group, the last rebalance event, how long it has held nothing and
        /// the last consumer error.
        #[test]
        fn an_expired_deadline_is_fatal_and_names_the_group() {
            let mut cfg = test_config();
            cfg.assignment_timeout = Duration::from_secs(300);

            let error = assignment_deadline_error(
                &cfg,
                Duration::from_secs(301),
                Some("rebalance error: Broker: Not coordinator"),
                Some((Duration::from_secs(12), "consumer poll: SSL")),
            )
            .expect("the deadline has passed");

            assert_eq!(error.class(), ErrorClass::Fatal, "{error}");
            assert!(
                error.reason().contains("no partition assignment for 301s"),
                "{error}"
            );
            assert!(
                error
                    .reason()
                    .contains("rebalance error: Broker: Not coordinator"),
                "names the last rebalance event: {error}"
            );
            assert!(
                error.reason().contains(r#"group "test""#),
                "names the group: {error}"
            );
            assert!(
                error
                    .reason()
                    .ends_with("; last consumer error 12s ago: consumer poll: SSL"),
                "names the last consumer error: {error}"
            );
        }

        /// A member inside the deadline keeps running, and the deadline
        /// itself is exclusive: waiting exactly `assignment_timeout` is
        /// still inside it.
        #[test]
        fn a_member_inside_the_deadline_keeps_running() {
            let mut cfg = test_config();
            cfg.assignment_timeout = Duration::from_secs(300);

            assert!(
                assignment_deadline_error(
                    &cfg,
                    Duration::from_secs(299),
                    Some("a revocation"),
                    None
                )
                .is_none()
            );
            assert!(
                assignment_deadline_error(
                    &cfg,
                    Duration::from_secs(300),
                    Some("a revocation"),
                    None
                )
                .is_none(),
                "the deadline is exclusive"
            );
        }

        /// A zero timeout disables the deadline in either window, however
        /// long the member has held nothing.
        #[test]
        fn a_zero_timeout_disables_the_deadline() {
            let mut cfg = test_config();
            cfg.assignment_timeout = Duration::ZERO;
            cfg.startup_timeout = Duration::ZERO;
            let a_day = Duration::from_secs(86_400);

            assert!(assignment_deadline_error(&cfg, a_day, Some("a revocation"), None).is_none());
            assert!(
                assignment_deadline_error(&cfg, a_day, None, None).is_none(),
                "a member still waiting for its first assignment"
            );
        }

        /// Before the first assignment `startup_timeout` is the deadline,
        /// and the much longer `assignment_timeout` does not govern that
        /// window. Its error names the topic, the brokers and the last
        /// consumer error with its age.
        #[test]
        fn the_startup_window_has_its_own_deadline_and_message() {
            let mut cfg = test_config();
            cfg.startup_timeout = Duration::from_secs(30);
            cfg.assignment_timeout = Duration::from_secs(300);

            assert!(
                assignment_deadline_error(&cfg, Duration::from_secs(30), None, None).is_none(),
                "the deadline is exclusive"
            );
            let error = assignment_deadline_error(
                &cfg,
                Duration::from_secs(31),
                None,
                Some((Duration::from_secs(2), "consumer poll: SSL")),
            )
            .expect("the startup deadline has passed");

            assert_eq!(error.class(), ErrorClass::Fatal, "{error}");
            assert!(
                error
                    .reason()
                    .contains("no partition assignment within 31s"),
                "{error}"
            );
            assert!(
                error.reason().contains(r#"topic "orders""#),
                "names the topic: {error}"
            );
            assert!(
                error.reason().contains(r#"brokers "localhost:9092""#),
                "names the brokers: {error}"
            );
            assert!(
                error
                    .reason()
                    .ends_with("; last consumer error 2s ago: consumer poll: SSL"),
                "names the last consumer error: {error}"
            );
        }

        /// `open` opens the startup window, and a loss before the first
        /// assignment leaves it alone: the member is still starting up, so
        /// `startup_timeout` keeps governing and the wait keeps running from
        /// `open` rather than from the event.
        #[test]
        fn a_loss_before_the_first_assignment_stays_in_the_startup_window() {
            let mut source = KafkaSource::new(test_config());
            let cp = Checkpointer::new();
            source.open(SourceCtx::new(cp.handle())).expect("open");

            let opened_at = source
                .assignment_wait
                .as_ref()
                .expect("open arms the startup wait")
                .since;

            source.note_assignment_loss("a revocation".to_owned());

            let wait = source.assignment_wait.as_ref().expect("still waiting");
            assert!(wait.cause.is_none(), "the startup window still governs");
            assert_eq!(wait.since, opened_at, "the wait still runs from open");
        }

        /// `poll_events` reports the deadline error ahead of the consumer
        /// poll, so the transport errors of brokers it cannot reach do not
        /// hide it.
        #[test]
        fn poll_events_reports_an_expired_deadline() {
            let mut cfg = test_config();
            cfg.assignment_timeout = Duration::from_millis(1);
            let mut source = opened_source_with(cfg, &[]);
            let clock = freeze(&mut source);
            source.note_assignment_loss("a revocation".to_owned());
            clock.advance(Duration::from_millis(2));

            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(e.reason().contains("no partition assignment for"), "{e}");
                }
                other => panic!("expected the deadline error, got {other:?}"),
            }
        }

        /// Drive `poll_events` until the consumer has reported a broker
        /// transport failure and its latest error is `AllBrokersDown`.
        fn poll_until_brokers_down(source: &mut KafkaSource) {
            let (mut transport, mut down) = (false, false);
            spate_test::wait_until(
                Duration::from_secs(20),
                "a transport failure, then AllBrokersDown",
                || {
                    if let Err(e) = source.poll_events(Duration::from_millis(50)) {
                        let e = e.to_string();
                        transport |= e.contains("BrokerTransportFailure");
                        down = e.contains("AllBrokersDown");
                    }
                    transport && down
                },
            );
        }

        /// The deadline error names the last consumer error with librdkafka's
        /// text, and `AllBrokersDown` is never that error.
        fn assert_names_the_transport_failure(source: &mut KafkaSource, window: &str) {
            match source.poll_events(Duration::from_millis(50)) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(e.reason().contains(window), "{e}");
                    assert!(e.reason().contains("last consumer error"), "{e}");
                    assert!(e.reason().contains("BrokerTransportFailure"), "{e}");
                    assert!(e.reason().contains("Connection refused"), "{e}");
                    assert!(!e.reason().contains("AllBrokersDown"), "{e}");
                }
                other => panic!("expected the deadline error, got {other:?}"),
            }
        }

        /// The deadline error gives the last consumer error's age as time
        /// before the error is reported, which holds for an error recorded
        /// after the deadline passed.
        #[test]
        fn an_error_after_the_deadline_is_reported_by_its_age() {
            let (mut source, clock) = opened_unassigned(test_config(), 1);
            clock.advance(Duration::from_secs(10));
            spate_test::wait_until(Duration::from_secs(20), "a consumer error", || {
                let _ = source.poll_events(Duration::from_millis(50));
                source.last_consumer_error.is_some()
            });

            // The deadline passed 1ns after `open`, before the error arrived.
            source.config.startup_timeout = Duration::from_nanos(1);
            clock.advance(Duration::from_secs(2));
            match source.poll_events(Duration::from_millis(50)) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(!e.reason().contains("before the deadline"), "{e}");
                    assert!(
                        e.reason()
                            .contains("; last consumer error 2s ago: consumer poll: "),
                        "{e}"
                    );
                }
                other => panic!("expected the deadline error, got {other:?}"),
            }
        }

        /// A member that never joins because its brokers refuse the
        /// connection fails at `startup_timeout` with the refusal in the
        /// error. Regression for #711.
        #[test]
        fn the_startup_deadline_names_the_last_consumer_error() {
            let (mut source, clock) = opened_unassigned(test_config(), 1);

            poll_until_brokers_down(&mut source);
            source.config.startup_timeout = Duration::from_nanos(1);
            clock.advance(Duration::from_secs(1));
            assert_names_the_transport_failure(&mut source, "no partition assignment within");
        }

        /// A member that lost its partitions and cannot reach a broker fails
        /// at `assignment_timeout` with the refusal in the error.
        /// Regression for #711.
        #[test]
        fn the_assignment_deadline_names_the_last_consumer_error() {
            let mut source = opened_source(&[]);
            let clock = freeze(&mut source);
            source.note_assignment_loss("a revocation".to_owned());

            poll_until_brokers_down(&mut source);
            source.config.assignment_timeout = Duration::from_nanos(1);
            clock.advance(Duration::from_secs(1));
            assert_names_the_transport_failure(&mut source, "no partition assignment for");
        }

        /// Whether `outcome` is the assignment deadline's error.
        fn is_deadline_error(outcome: &Result<SourceEvent<KafkaLane>, SourceError>) -> bool {
            matches!(outcome, Err(e) if e.reason().starts_with("no partition assignment"))
        }

        /// The startup window runs on the source's clock from `open` and
        /// ends once that clock is past `startup_timeout`.
        #[test]
        fn the_startup_window_ends_at_startup_timeout() {
            let mut cfg = test_config();
            cfg.brokers = crate::testutil::refused_broker();
            cfg.startup_timeout = Duration::from_secs(30);
            let mut source = KafkaSource::new(cfg);
            let clock = freeze(&mut source);
            let cp = Checkpointer::new();
            source.open(SourceCtx::new(cp.handle())).expect("open");

            clock.advance(Duration::from_secs(30));
            let outcome = source.poll_events(Duration::from_millis(50));
            assert!(!is_deadline_error(&outcome), "{outcome:?}");
            clock.advance(Duration::from_nanos(1));
            match source.poll_events(Duration::from_millis(50)) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(
                        e.reason()
                            .starts_with("no partition assignment within 30.000000001s"),
                        "{e}"
                    );
                }
                other => panic!("expected the deadline error, got {other:?}"),
            }
        }

        /// The assignment window runs on the source's clock from the loss of
        /// the last partition and ends once that clock is past
        /// `assignment_timeout`.
        #[test]
        fn the_assignment_window_runs_from_the_loss() {
            let mut cfg = test_config();
            cfg.assignment_timeout = Duration::from_secs(300);
            let mut source = opened_source_with(cfg, &[]);
            let clock = freeze(&mut source);
            clock.advance(Duration::from_secs(3600));
            source.note_assignment_loss("a revocation".to_owned());

            clock.advance(Duration::from_secs(300));
            let outcome = source.poll_events(Duration::from_millis(50));
            assert!(!is_deadline_error(&outcome), "{outcome:?}");
            clock.advance(Duration::from_nanos(1));
            match source.poll_events(Duration::from_millis(50)) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(
                        e.reason().starts_with(
                            "no partition assignment for 300.000000001s after a revocation"
                        ),
                        "{e}"
                    );
                }
                other => panic!("expected the deadline error, got {other:?}"),
            }
        }

        /// The deadline error gives the last consumer error's age on the
        /// source's clock.
        #[test]
        fn the_deadline_error_gives_the_last_errors_age() {
            let (mut source, clock) = opened_unassigned(test_config(), 1);
            spate_test::wait_until(Duration::from_secs(20), "a consumer error", || {
                let _ = source.poll_events(Duration::from_millis(50));
                source.last_consumer_error.is_some()
            });

            source.config.startup_timeout = Duration::from_secs(10);
            clock.advance(Duration::from_secs(12));
            match source.poll_events(Duration::from_millis(50)) {
                Err(e) => {
                    assert_eq!(e.class(), ErrorClass::Fatal, "{e}");
                    assert!(
                        e.reason()
                            .contains("; last consumer error 12s ago: consumer poll: "),
                        "{e}"
                    );
                }
                other => panic!("expected the deadline error, got {other:?}"),
            }
        }

        /// The revocation that releases a member's last partitions arms the
        /// deadline, once `unassign` completes it on the following call.
        /// This is the path every eager rebalance takes.
        #[test]
        fn a_revocation_arms_the_deadline() {
            let mut source = opened_source(&[(0, 0)]);
            push_intent(&source, Intent::Revoke(source.tpl_for([0])));

            match source.poll_events(Duration::ZERO) {
                Ok(SourceEvent::LanesRevoked { lanes, barrier }) => {
                    assert_eq!(lanes, vec![LaneId(0)]);
                    barrier.arrive(); // the driver drained
                }
                other => panic!("expected LanesRevoked, got {other:?}"),
            }

            poll_quiet(&mut source);
            let wait = source
                .assignment_wait
                .as_ref()
                .expect("the release arms it");
            assert_eq!(wait.cause.as_deref(), Some("a revocation"));
        }

        /// An assignment clears the deadline and the recorded consumer error
        /// even when it is empty. A group with more members than partitions
        /// hands some member nothing, and that member keeps running: it is in
        /// the group, and the next rebalance can give it partitions.
        #[test]
        fn an_accepted_empty_assignment_clears_the_deadline() {
            let mut source = opened_source(&[]);
            push_error(&source, RDKafkaErrorCode::RebalanceInProgress);

            match source.poll_events(Duration::ZERO) {
                Err(e) => {
                    assert!(e.reason().contains("rebalance error"), "{e}");
                }
                other => panic!("expected the classified error, got {other:?}"),
            }
            let cause = source
                .assignment_wait
                .as_ref()
                .and_then(|wait| wait.cause.clone())
                .expect("the error arms the deadline");
            assert!(
                cause.starts_with("rebalance error:"),
                "the error arms the deadline and names itself: {cause}"
            );

            source.last_consumer_error = Some((
                Stopwatch::start(&*source.clock),
                "consumer poll: SSL".to_owned(),
            ));
            push_intent(&source, Intent::Assign(TopicPartitionList::new()));
            poll_quiet(&mut source);
            assert!(source.assignment_wait.is_none());
            assert!(source.last_consumer_error.is_none());
        }
    }

    /// The retained set that prunes per-partition metric series must exclude
    /// revoked partitions, so the prune can zero the lag they left behind.
    #[test]
    fn retained_partition_ids_drop_revoked_partitions() {
        let mut source = opened_source(&[(0, 0), (1, 1), (2, 2)]);

        revoke_lanes(&mut source, 0, &[2]);

        let mut kept: Vec<u32> = source
            .retained_partition_ids()
            .iter()
            .map(|p| p.0)
            .collect();
        kept.sort_unstable();
        assert_eq!(kept, vec![0, 1], "revoked partition 2 is not retained");
    }

    /// The prune reaches the connector's per-partition series as well as the
    /// lag family. `publish_stats` follows the live assignment, which an
    /// eager revoke empties for every window until the new assignment lands,
    /// so this is the only place `partition_not_fetching` is released.
    #[test]
    fn the_prune_releases_the_connector_partition_series() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let mut source = opened_source(&[(0, 0), (1, 1)]);
            let meter =
                spate_core::metrics::Meter::with_namespace("kafka", "prune", "source", "kafka");
            source.members[0].stats = Some(crate::metrics::KafkaStatsMetrics::new(meter, true));
            let stats = parked(&[0, 1]);
            let owned = source.retained_partition_ids();
            source.members[0]
                .stats
                .as_mut()
                .expect("stats metrics")
                .update(&stats, "orders", &owned);

            revoke_lanes(&mut source, 0, &[1]);
            source.prune_partition_series();
        });
        let rendered = handle.render();
        let std = r#"pipeline="prune",component="source",component_type="kafka""#;
        assert!(
            rendered.contains(&format!(
                r#"spate_kafka_partition_not_fetching{{{std},partition="1"}} 0"#
            )),
            "the lost partition was not released:\n{rendered}"
        );
        assert!(
            rendered.contains(&format!(
                r#"spate_kafka_partition_not_fetching{{{std},partition="0"}} 1"#
            )),
            "the retained partition lost its value:\n{rendered}"
        );
    }

    /// A snapshot for `orders` in which each partition is stopped.
    fn parked(partitions: &[i32]) -> Statistics {
        Statistics {
            topics: std::collections::HashMap::from([(
                "orders".to_owned(),
                rdkafka::statistics::Topic {
                    topic: "orders".to_owned(),
                    partitions: partitions
                        .iter()
                        .map(|&pid| {
                            (
                                pid,
                                rdkafka::statistics::Partition {
                                    partition: pid,
                                    fetch_state: "stopped".to_owned(),
                                    ..Default::default()
                                },
                            )
                        })
                        .collect(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }
    }

    mod lag {
        use super::*;
        use rdkafka::statistics::{Partition, Topic};
        use spate_core::metrics::ComponentLabels;
        use std::collections::HashMap;

        /// Run `f` over a `SourceMetrics` whose component name is unique to the
        /// call, and return the exposition with the standard label string its
        /// series carry.
        fn render(f: impl FnOnce(&SourceMetrics)) -> (String, String) {
            let component = spate_test::unique_name("source");
            let std =
                format!(r#"pipeline="orders",component="{component}",component_type="kafka""#);
            let rendered = spate_test::render_metrics(|| {
                let m = SourceMetrics::new(&ComponentLabels::new("orders", component, "kafka"));
                f(&m);
            });
            (rendered, std)
        }

        /// `(partition, consumer_lag)` pairs into a snapshot for `orders`.
        fn stats(parts: &[(i32, i64)]) -> Statistics {
            Statistics {
                topics: HashMap::from([(
                    "orders".to_owned(),
                    Topic {
                        topic: "orders".to_owned(),
                        partitions: parts
                            .iter()
                            .map(|&(pid, consumer_lag)| {
                                (
                                    pid,
                                    Partition {
                                        partition: pid,
                                        consumer_lag,
                                        ..Default::default()
                                    },
                                )
                            })
                            .collect(),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }
        }

        /// The regression this pins: a maximally backlogged consumer must publish
        /// its backlog, per partition, at full magnitude.
        #[test]
        fn a_large_backlog_publishes_per_partition_lag() {
            let (rendered, std) = render(|m| {
                publish_lag(
                    &stats(&[(0, 150_000_000), (1, 90_000_000)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
            });
            assert!(
                rendered.contains(&format!(
                    r#"spate_source_lag_records{{{std},partition="0"}} 150000000"#
                )),
                "backlogged partition must report its lag:\n{rendered}"
            );
            assert!(
                rendered.contains(&format!(
                    r#"spate_source_lag_records{{{std},partition="1"}} 90000000"#
                )),
                "every owned partition gets its own series:\n{rendered}"
            );
        }

        /// There is no aggregate series: readers aggregate in the query layer.
        /// An unlabeled series sharing this family name would make
        /// `sum(spate_source_lag_records)` double-count.
        #[test]
        fn no_unlabelled_aggregate_series_is_published() {
            let (rendered, _std) = render(|m| {
                publish_lag(
                    &stats(&[(0, 17), (1, 4)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
            });
            let unlabelled = rendered
                .lines()
                .filter(|l| l.starts_with("spate_source_lag_records{"))
                .any(|l| !l.contains("partition="));
            assert!(
                !unlabelled,
                "every lag series must carry a partition label:\n{rendered}"
            );
        }

        /// `consumer_lag = -1` means "not measured yet", covering the period
        /// before the first commit and before the partition leader has
        /// answered. Publishing it
        /// as `0` would read as "caught up" on exactly the consumer that is
        /// most behind.
        #[test]
        fn unknown_lag_registers_no_series() {
            let (rendered, _std) = render(|m| {
                publish_lag(
                    &stats(&[(0, -1), (1, -1)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
            });
            assert!(
                !rendered.contains("spate_source_lag_records"),
                "an all-unknown snapshot must publish nothing:\n{rendered}"
            );
        }

        /// A mixed snapshot publishes the partitions that have a number and
        /// stays silent about the rest, rather than dragging the unknown ones
        /// to zero.
        #[test]
        fn mixed_snapshot_publishes_only_known_partitions() {
            let (rendered, std) = render(|m| {
                publish_lag(
                    &stats(&[(0, 4_200), (1, -1)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
            });
            assert!(rendered.contains(&format!(
                r#"spate_source_lag_records{{{std},partition="0"}} 4200"#
            )));
            assert!(
                !rendered.contains(r#"partition="1""#),
                "unknown partition must be absent:\n{rendered}"
            );
        }

        /// Once measured, a partition holds its last value through snapshots
        /// where librdkafka temporarily reports the lag as unknown (a leader
        /// change, say). Dropping to `0` would look like a drain that never
        /// happened.
        #[test]
        fn a_known_partition_holds_its_value_when_lag_goes_unknown() {
            let (rendered, std) = render(|m| {
                publish_lag(&stats(&[(0, 5_000)]), "orders", &[PartitionId(0)], m);
                publish_lag(&stats(&[(0, -1)]), "orders", &[PartitionId(0)], m);
            });
            assert!(
                rendered.contains(&format!(
                    r#"spate_source_lag_records{{{std},partition="0"}} 5000"#
                )),
                "last known value is held:\n{rendered}"
            );
        }

        /// A snapshot for a different topic must not publish anything: the
        /// source owns exactly one topic.
        #[test]
        fn a_snapshot_without_our_topic_publishes_nothing() {
            let (rendered, _std) = render(|m| {
                publish_lag(&stats(&[(0, 900)]), "other-topic", &[PartitionId(0)], m);
            });
            assert!(
                !rendered.contains("spate_source_lag_records"),
                "wrong topic must publish nothing:\n{rendered}"
            );
        }

        /// A partition that moved to another member must contribute nothing
        /// to this member's total, or every reader that sums across
        /// partitions double-counts it.
        ///
        /// The exporter has no deletion and no idle timeout is configured, so
        /// "contribute nothing" cannot mean "disappear"; the series renders
        /// for the life of the process whatever we do with the handle. It
        /// means `0`, which is the truth for a partition this member no
        /// longer owns. Both halves are asserted: the value is zeroed at the
        /// prune, and the ownership filter keeps later snapshots from
        /// reviving it.
        #[test]
        fn revoked_partitions_zero_out_and_stop_updating() {
            let (rendered, std) = render(|m| {
                // Both owned.
                publish_lag(
                    &stats(&[(0, 11), (1, 22)]),
                    "orders",
                    &[PartitionId(0), PartitionId(1)],
                    m,
                );
                // Partition 1 is revoked: it leaves the owned set and the
                // prune zeroes it. librdkafka keeps reporting it in the
                // snapshot for a while, so the filter has to hold too.
                m.retain_partitions(&[PartitionId(0)]);
                publish_lag(&stats(&[(0, 33), (1, 44)]), "orders", &[PartitionId(0)], m);
            });
            assert!(rendered.contains(&format!(
                r#"spate_source_lag_records{{{std},partition="0"}} 33"#
            )));
            assert!(
                rendered.contains(&format!(
                    r#"spate_source_lag_records{{{std},partition="1"}} 0"#
                )),
                "revoked partition must be zeroed, not left at its last lag:\n{rendered}"
            );
            assert!(
                !rendered.contains(r#"partition="1"} 44"#),
                "revoked partition must not resume updating:\n{rendered}"
            );
        }
    }

    mod start_offsets {
        use super::*;
        use rdkafka::error::KafkaError;
        use rdkafka::statistics::{Partition, Topic};
        use std::collections::HashMap;

        fn failed() -> KafkaResult<()> {
            Err(KafkaError::OffsetFetch(
                RDKafkaErrorCode::GroupAuthorizationFailed,
            ))
        }

        /// A partition whose lookup failed is left to librdkafka, whatever
        /// the reset policy, so it cannot start past its committed offset.
        #[test]
        fn a_failed_lookup_keeps_the_stored_offset() {
            for committed in [Offset::Invalid, Offset::Offset(3)] {
                assert_eq!(
                    start_offset(failed(), committed, Offset::End),
                    Some(Offset::Stored)
                );
            }
        }

        #[test]
        fn an_uncommitted_partition_takes_the_reset_policy() {
            assert_eq!(
                start_offset(Ok(()), Offset::Invalid, Offset::Beginning),
                Some(Offset::Beginning)
            );
        }

        #[test]
        fn an_uncommitted_partition_under_latest_starts_at_its_end() {
            assert_eq!(
                start_offset(Ok(()), Offset::Invalid, Offset::End),
                Some(Offset::End)
            );
        }

        #[test]
        fn a_committed_offset_is_kept() {
            assert_eq!(start_offset(Ok(()), Offset::Offset(3), Offset::End), None);
        }

        /// One partition of `orders` with the given statistics offsets.
        fn snapshot(consumer_lag: i64, hi_offset: i64, ls_offset: i64) -> Statistics {
            let partition = Partition {
                partition: 0,
                consumer_lag,
                hi_offset,
                ls_offset,
                ..Default::default()
            };
            Statistics {
                topics: HashMap::from([(
                    "orders".to_owned(),
                    Topic {
                        topic: "orders".to_owned(),
                        partitions: HashMap::from([(0, partition)]),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }
        }

        fn filled(mut stats: Statistics, committed: &[(i32, i64)], reads_committed: bool) -> i64 {
            fill_unknown_lag(
                &mut stats,
                "orders",
                &committed.iter().copied().collect(),
                reads_committed,
            );
            stats.topics["orders"].partitions[&0].consumer_lag
        }

        #[test]
        fn unknown_lag_is_measured_from_the_committed_offset() {
            assert_eq!(filled(snapshot(-1, 10, 8), &[(0, 3)], true), 5);
            assert_eq!(filled(snapshot(-1, 10, 8), &[(0, 3)], false), 7);
            assert_eq!(filled(snapshot(-1, 10, 10), &[(0, 0)], true), 10);
            assert_eq!(filled(snapshot(-1, 10, 10), &[(0, 10)], true), 0);
        }

        #[test]
        fn a_known_lag_is_left_alone() {
            assert_eq!(filled(snapshot(2, 10, 10), &[(0, 3)], true), 2);
        }

        /// Pins that a negative committed offset leaves the lag unknown, and
        /// that the end minus it does not overflow. Regression for #1690.
        #[test]
        fn a_negative_committed_offset_leaves_the_lag_unknown() {
            assert_eq!(
                filled(snapshot(-1, i64::MAX, i64::MAX), &[(0, -500)], false),
                -1
            );
        }

        /// No committed offset, or no end offset yet, leaves the lag unknown.
        #[test]
        fn nothing_to_measure_from_stays_unknown() {
            assert_eq!(filled(snapshot(-1, 10, 10), &[], true), -1);
            assert_eq!(filled(snapshot(-1, -1, -1), &[(0, 3)], true), -1);
        }
    }
}
