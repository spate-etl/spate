//! The feed's consumer thread, one of its four: it polls the group
//! member, hands each unit to the gate, answers rebalances, pauses when the
//! feed holds as many units as it buffers, commits, and sends again the
//! writes that failed.
//!
//! # Rebalances
//!
//! The consumer context defers each assignment and revocation to this
//! thread, as the source's does.
//!
//! - **Assignment.** The thread reads the committed offsets, assigns at a
//!   concrete start offset for every partition (earliest where none is
//!   committed), and takes the partitions into the tracker under a new
//!   generation. With concrete offsets the fetchers start before `assign`
//!   returns, out of the window where a later pause or resume can lose a
//!   fetcher's start
//!   ([librdkafka#5591](https://github.com/confluentinc/librdkafka/issues/5591)).
//!   When the committed-offset read fails or times out, the start offset
//!   is left for librdkafka to resolve, and that window stays open.
//! - **Revocation.** The thread takes the partitions out of the tracker,
//!   drops their buffered units, commits each partition up to its first
//!   unfinished offset, resumes any it paused, and unassigns.
//! - **Rebalance error.** The context has already unassigned. The thread
//!   gives up every partition with nothing committed, and the units read
//!   since the last commit are delivered again.
//!
//! # Commits
//!
//! Every commit is synchronous and made from this thread, so no two are
//! ever in flight: a lower commit point, after the fetch position moved
//! back, cannot be overtaken by an earlier, higher one that librdkafka
//! retried. librdkafka bounds a commit by the session timeout while it
//! waits for a coordinator, and by `socket.timeout.ms` for each of the
//! request's attempts once sent. The tracker lock is never held across a
//! commit or any other broker call.
//!
//! Once the feed stops receiving, the thread keeps every owned partition
//! paused, and pauses each partition an assignment gives it, while it goes
//! on polling and committing. When the feed closes, or drops without a
//! close, the thread waits for the writes in flight, commits each partition
//! up to its first unfinished offset, and then the consumer leaves the
//! group.
//!
//! Once the feed latches a Fatal error, the thread commits nothing more and
//! ends, and the consumer leaves the group. The next owner of each
//! partition reads it again from the last commit.

use super::attempt::PublishId;
use super::events::Cause;
use super::inflight::{Position, Tp};
use super::write::{self, Destination, Finish, Move, Outgoing, Producer, Tombstone, Write};
use super::{Closing, Core, Gating, LatchOnPanic, READY_CAPACITY, State, Topics};
use crate::context::{Intent, SourceContext};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::producer::Producer as _;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use spate_core::error::ErrorClass;
use spate_core::feed::{FeedError, FeedErrorKind, Unit};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// How often finished offsets are committed.
pub(crate) const COMMIT_INTERVAL: Duration = Duration::from_secs(1);

/// The longest one poll waits for a message.
pub(crate) const POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// Messages taken per pass before the thread serves rebalances, pauses and
/// commits.
pub(crate) const POLL_BATCH: usize = 64;

/// How long a feed dropped without a close waits for its writes in flight
/// before its last commit.
pub(crate) const STOP_SETTLE: Duration = Duration::from_secs(1);

/// The longest the committed-offset read before an assignment may take.
const COMMITTED_LOOKUP_MAX: Duration = Duration::from_secs(10);

/// librdkafka's default `max.poll.interval.ms`.
const DEFAULT_MAX_POLL_INTERVAL: Duration = Duration::from_secs(300);

/// The consumer thread's own state.
pub(crate) struct Driver {
    pub(crate) core: Arc<Core>,
    pub(crate) consumer: BaseConsumer<SourceContext>,
    producer: Producer,
    /// The partitions this thread paused, and only those: a resume never
    /// touches a partition it did not pause.
    pub(crate) paused: BTreeSet<Tp>,
    /// The bound on each committed-offset read.
    lookup_timeout: Duration,
    assigned_once: bool,
    last_commit: Instant,
}

impl Driver {
    pub(crate) fn new(
        core: Arc<Core>,
        consumer: BaseConsumer<SourceContext>,
        producer: Producer,
        lookup_timeout: Duration,
    ) -> Driver {
        Driver {
            core,
            consumer,
            producer,
            paused: BTreeSet::new(),
            lookup_timeout,
            assigned_once: false,
            last_commit: Instant::now(),
        }
    }
}

/// Run the consumer thread until the feed is closed, dropped or latches,
/// then make its last commit, leave the group, and settle the close.
pub(crate) fn run(mut d: Driver) {
    let core = Arc::clone(&d.core);
    let _guard = LatchOnPanic {
        core: Arc::clone(&core),
        thread: "consumer",
    };
    while !d.core.stopped() && !d.core.latched() {
        d.resend_due();
        d.poll();
        d.serve_intents();
        d.backpressure();
        if d.last_commit.elapsed() >= COMMIT_INTERVAL {
            d.commit_finished("periodic");
        }
    }
    let close = core.lock().close.take();
    let flushed = if core.latched() {
        true
    } else {
        let wait = close
            .as_ref()
            .map_or(STOP_SETTLE, |c| close_flush(c.within));
        // A tombstone queued once its dead-letter write was confirmed goes
        // out with the flush, so the last commit can pass its unit.
        d.resend_due();
        // Confirmed tombstones finish their offsets before the last commit.
        // The flush polls the producer from this thread, beside the
        // producer's own poll thread. Each report is still taken once, and
        // an error event's two callbacks run on the thread that popped it,
        // but two pollers' callbacks can interleave on the producer
        // context's one unpaired-error slot, so within this window a
        // producer error can be logged twice.
        d.producer.flush(wait).is_ok()
    };
    let committed = d.commit_finished(if close.is_some() {
        "on close"
    } else {
        "on stop"
    });
    // The consumer leaves the group as it drops, before the close settles,
    // so a settled close means this worker no longer holds its partitions.
    drop(d);
    if let Some(close) = close {
        settle_close(&core, close, flushed, committed);
    }
}

/// How long a close given `within` waits for the feed's writes in flight:
/// half of it, so the commit and the leave keep the other half.
pub(crate) fn close_flush(within: Duration) -> Duration {
    within / 2
}

/// Settle `close` once the consumer has left: rejected with the latched
/// error if the feed latched, `Retryable` if writes were still in flight
/// at the flush's end or the last commit failed, and confirmed otherwise.
fn settle_close(core: &Core, close: Closing, flushed: bool, committed: bool) {
    if let Some(fatal) = core.lock().fatal.clone() {
        close.confirmer.reject(fatal);
    } else if !flushed {
        close.confirmer.reject(FeedError::new(
            FeedErrorKind::Retryable,
            "the Kafka feed closed with writes still in flight; the units they would have \
             finished are delivered again",
        ));
    } else if !committed {
        close.confirmer.reject(FeedError::new(
            FeedErrorKind::Retryable,
            "the Kafka feed's last commit failed; the units it would have committed are \
             delivered again",
        ));
    } else {
        close.confirmer.confirm();
    }
}

impl Driver {
    /// Send the writes whose backoff has passed.
    fn resend_due(&mut self) {
        let due = take_due(&mut self.core.lock(), Instant::now());
        for w in due {
            write::send(&self.core, &self.producer, Box::new(w));
        }
    }

    /// Take up to a batch of messages, stopping early when a rebalance
    /// needs serving.
    pub(crate) fn poll(&mut self) {
        for i in 0..POLL_BATCH {
            let timeout = if i == 0 { POLL_TIMEOUT } else { Duration::ZERO };
            match self.consumer.poll(timeout) {
                None => break,
                Some(Ok(message)) => self.on_message(&message),
                Some(Err(error)) => {
                    self.on_error(&error);
                    break;
                }
            }
            if !self
                .consumer
                .context()
                .intents
                .lock()
                .expect("intent lock")
                .is_empty()
            {
                break;
            }
        }
    }

    fn on_message(&self, message: &BorrowedMessage<'_>) {
        let tp = Tp::new(&self.core.topics().of(message.topic()), message.partition());
        let offset = message.offset();
        let read = read_message(message, self.core.topics());
        let dead_letter = {
            let mut st = self.core.lock();
            let replaced = st.tracker.replace_copies(&tp, offset);
            st.forget(&replaced);
            let Some(receipt) = st.tracker.deliver(&tp, offset) else {
                // Read after its partition was given up; the next owner
                // reads it again.
                return;
            };
            match read {
                Read::Unit { unit, identity } => {
                    st.gating.push_back(Gating {
                        receipt,
                        unit,
                        identity,
                    });
                    // Under the state lock the gate checks `gating` under.
                    self.core.gate_wake.notify_one();
                    None
                }
                Read::NoUnit(outgoing) => {
                    let position = st
                        .tracker
                        .release(receipt)
                        .expect("a receipt minted under the lock is current");
                    Some(no_unit_move(&self.core, outgoing, position))
                }
            }
        };
        if let Some(moving) = dead_letter {
            tracing::warn!(
                topic = message.topic(),
                partition = message.partition(),
                offset,
                "a message names no unit; writing it to the dead-letter topic"
            );
            write::send(&self.core, &self.producer, Box::new(Write::Move(moving)));
        }
    }

    fn on_error(&self, error: &KafkaError) {
        let code = error.rdkafka_error_code();
        let text = code.and_then(|code| self.consumer.context().take_error(code));
        let reason = crate::error::poll_reason("consumer poll", error, text.as_deref());
        match crate::error::classify_poll_error(error, self.assigned_once, text.as_deref()) {
            ErrorClass::Fatal => {
                self.core.latch(&reason);
            }
            _ => {
                let mut st = self.core.lock();
                st.retryable = Some(FeedError::new(FeedErrorKind::Retryable, reason));
                st.wake();
            }
        }
    }

    /// Serve every rebalance the context recorded.
    pub(crate) fn serve_intents(&mut self) {
        loop {
            let intent = self
                .consumer
                .context()
                .intents
                .lock()
                .expect("intent lock")
                .pop_front();
            match intent {
                None => return,
                Some(Intent::Assign(tpl)) => self.assign(&tpl),
                Some(Intent::Revoke(tpl)) => self.revoke(&tpl),
                Some(Intent::Error(code)) => self.lose_all(code),
            }
        }
    }

    fn tps(&self, tpl: &TopicPartitionList) -> Vec<Tp> {
        tpl.elements()
            .iter()
            .map(|e| Tp::new(&self.core.topics().of(e.topic()), e.partition()))
            .collect()
    }

    fn assign(&mut self, tpl: &TopicPartitionList) {
        let start = if tpl.count() == 0 {
            tpl.clone()
        } else {
            self.start_positions(tpl)
        };
        // Accepted even when empty: under deferred completion the
        // rebalance stays in progress until `assign` is called.
        if let Err(e) = self.consumer.assign(&start) {
            self.core.latch(&format!("accepting an assignment: {e}"));
            return;
        }
        self.assigned_once = true;
        let tps = self.tps(tpl);
        let pause = {
            let mut st = self.core.lock();
            st.tracker.assign(tps.iter().cloned());
            st.stopping || st.held_for_reader() >= READY_CAPACITY
        };
        if pause {
            self.pause(tps.clone());
        }
        tracing::info!(
            partitions = tps.len(),
            "the Kafka feed accepted an assignment"
        );
    }

    /// `tpl` with a concrete start offset for each partition: its committed
    /// offset, or the earliest where none is committed. A partition whose
    /// lookup failed keeps `Offset::Stored`, which librdkafka resolves.
    fn start_positions(&self, tpl: &TopicPartitionList) -> TopicPartitionList {
        let committed = match self
            .consumer
            .committed_offsets(tpl.clone(), self.lookup_timeout)
        {
            Ok(committed) => committed,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "committed offset lookup failed; librdkafka resolves the start offsets"
                );
                return tpl.clone();
            }
        };
        for mut elem in committed.elements() {
            if let Some(start) =
                crate::source::start_offset(elem.error(), elem.offset(), Offset::Beginning)
            {
                let _ = elem.set_offset(start);
            }
        }
        committed
    }

    fn revoke(&mut self, tpl: &TopicPartitionList) {
        let tps = self.tps(tpl);
        commit_revoked(&self.core, &tps, |points| {
            // A latched feed commits nothing, as in `commit_finished`.
            if !self.core.latched() {
                commit(&self.consumer, points, "on revocation");
            }
        });
        let paused: Vec<Tp> = tps
            .iter()
            .filter(|tp| self.paused.remove(tp))
            .cloned()
            .collect();
        // An application pause outlives the assignment, so a partition
        // paused here would stay paused when it is assigned again.
        self.resume(&paused);
        if let Err(e) = self.consumer.unassign() {
            tracing::warn!(error = %e, "unassign after a revocation failed");
        }
    }

    /// A rebalance error: the context already unassigned, so nothing can be
    /// committed.
    fn lose_all(&mut self, code: RDKafkaErrorCode) {
        {
            let mut st = self.core.lock();
            st.tracker.unassign_all();
            st.drop_unowned();
        }
        let paused: Vec<Tp> = std::mem::take(&mut self.paused).into_iter().collect();
        self.resume(&paused);
        match crate::error::classify_consumer_error(code, self.assigned_once) {
            ErrorClass::Fatal => {
                self.core.latch(&format!("rebalance error: {code}"));
            }
            _ => tracing::warn!(
                %code,
                "a rebalance error gave up every partition; units read since the last \
                 commit are delivered again"
            ),
        }
    }

    /// Pause every owned partition while the feed holds as many units as
    /// it buffers, those at the gate and those with their attempt write in
    /// flight included, or once it stops receiving, and resume what this
    /// thread paused once that count is down to half, unless it is
    /// stopping.
    pub(crate) fn backpressure(&mut self) {
        let (len, stopping, owned): (usize, bool, Vec<Tp>) = {
            let st = self.core.lock();
            (
                st.held_for_reader(),
                st.stopping,
                st.tracker.owned().cloned().collect(),
            )
        };
        if stopping || len >= READY_CAPACITY {
            let more: Vec<Tp> = owned
                .into_iter()
                .filter(|tp| !self.paused.contains(tp))
                .collect();
            if !more.is_empty() {
                self.pause(more);
            }
        } else if len <= READY_CAPACITY / 2 && !self.paused.is_empty() {
            let paused: Vec<Tp> = std::mem::take(&mut self.paused).into_iter().collect();
            self.resume(&paused);
        }
    }

    fn pause(&mut self, tps: Vec<Tp>) {
        match self.consumer.pause(&tpl_of(&tps)) {
            Ok(()) => self.paused.extend(tps),
            Err(e) => tracing::warn!(error = %e, "pausing the feed's partitions failed"),
        }
    }

    fn resume(&self, tps: &[Tp]) {
        if tps.is_empty() {
            return;
        }
        if let Err(e) = self.consumer.resume(&tpl_of(tps)) {
            tracing::warn!(error = %e, "resuming the feed's partitions failed");
        }
    }

    /// Commit the commit points that moved. `false` when the feed has
    /// latched or the commit failed, `true` when it succeeded or nothing
    /// moved. `what` names the commit in a failure's log line.
    pub(crate) fn commit_finished(&mut self, what: &str) -> bool {
        // After a `BadMessage` latch, librdkafka may have skipped an offset,
        // and a commit point could pass the unit there.
        if self.core.latched() {
            return false;
        }
        self.last_commit = Instant::now();
        commit_moved(&self.core, |points| commit(&self.consumer, points, what))
    }
}

/// Leaves the group as the consumer drops, on a normal stop and on a panic
/// alike: without `closing` set, the consumer's close waits on a final
/// revocation that no poll of this thread will serve.
impl Drop for Driver {
    fn drop(&mut self) {
        let context = self.consumer.context();
        // The context completes the final revocation inline from here on.
        context.closing.store(true, Ordering::Release);
        // A poisoned lock reads as no deferred revocation: this runs during
        // an unwind, where a second panic aborts.
        let deferred_revoke = context
            .intents
            .lock()
            .map(|q| q.iter().any(|i| matches!(i, Intent::Revoke(_))))
            .unwrap_or(false);
        if deferred_revoke && let Err(e) = self.consumer.unassign() {
            tracing::warn!(error = %e, "unassign while the feed stops failed");
        }
    }
}

/// The bound on the committed-offset read made before each assignment, for
/// a consumer configured by `cc`: a quarter of its `max.poll.interval.ms`,
/// at most 10 s. librdkafka's poll-interval clock keeps running during the
/// read, and a member that outlasts the interval is evicted from the group.
pub(crate) fn committed_lookup_timeout(cc: &ClientConfig) -> Duration {
    let max_poll = cc
        .get("max.poll.interval.ms")
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map_or(DEFAULT_MAX_POLL_INTERVAL, Duration::from_millis);
    (max_poll / 4).min(COMMITTED_LOOKUP_MAX)
}

/// Take the writes due at `now` out of `st`'s resend queue. A move or a
/// tombstone whose partition was given up since is dropped: its offset was
/// not committed past, so the next owner reads the unit again and a resend
/// would only add a copy. An attempt write whose receipt is no longer held
/// is dropped for the same reason, and gives back its share of
/// `admitting`.
pub(crate) fn take_due(st: &mut State, now: Instant) -> Vec<Write> {
    if st.resends.is_empty() {
        return Vec::new();
    }
    let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut st.resends)
        .into_iter()
        .partition(|(at, _)| *at <= now);
    st.resends = later;
    let mut taken = Vec::with_capacity(due.len());
    for (_, w) in due {
        let keep = match &w {
            Write::Move(Move { position, .. }) | Write::Tombstone(Tombstone { position, .. }) => {
                st.tracker.current(position)
            }
            Write::Attempt(attempt) => {
                let held = st.tracker.held(attempt.receipt).is_some();
                if !held {
                    st.admitting = st.admitting.saturating_sub(1);
                }
                held
            }
            Write::Publish { .. } => true,
        };
        if keep {
            taken.push(w);
        }
    }
    taken
}

/// Take the revoked partitions out of the tracker and drop their buffered
/// units, then hand their commit points to `commit` with the lock released.
pub(crate) fn commit_revoked(core: &Core, tps: &[Tp], commit: impl FnOnce(&[(Tp, i64)])) {
    let points = {
        let mut st = core.lock();
        let points = st.tracker.revoke(tps);
        st.drop_unowned();
        points
    };
    if !points.is_empty() {
        commit(&points);
    }
}

/// Hand the commit points that moved to `commit` with the lock released. A
/// commit that fails leaves them to be taken again. `false` when the commit
/// failed.
pub(crate) fn commit_moved(core: &Core, commit: impl FnOnce(&[(Tp, i64)]) -> bool) -> bool {
    let points = core.lock().tracker.take_commits();
    if points.is_empty() {
        return true;
    }
    let committed = commit(&points);
    if !committed {
        core.lock().tracker.untake(&points);
    }
    committed
}

/// Commit `points` synchronously. `false` when the commit failed, which is
/// logged and never fatal: the units are delivered again.
fn commit(consumer: &BaseConsumer<SourceContext>, points: &[(Tp, i64)], what: &str) -> bool {
    let mut tpl = TopicPartitionList::new();
    for (tp, point) in points {
        if let Err(e) = tpl.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*point)) {
            tracing::warn!(error = %e, "building a commit failed");
            return false;
        }
    }
    match consumer.commit(&tpl, CommitMode::Sync) {
        Ok(()) | Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => true,
        Err(e) => {
            tracing::warn!(error = %e, commit = what, "committing the feed's offsets failed");
            false
        }
    }
}

fn tpl_of(tps: &[Tp]) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    for tp in tps {
        tpl.add_partition(&tp.topic, tp.partition);
    }
    tpl
}

/// What one message read from the work or retry topic holds.
#[derive(Debug)]
pub(crate) enum Read {
    Unit {
        unit: Unit,
        /// The publish identity the unit's attempts count under.
        identity: PublishId,
    },
    /// The message as read, for the dead-letter topic.
    NoUnit(Outgoing),
}

fn read_message(message: &BorrowedMessage<'_>, topics: &Topics) -> Read {
    let headers: Vec<(String, Option<Vec<u8>>)> = message
        .headers()
        .map(|hs| {
            hs.iter()
                .map(|h| (h.key.to_owned(), h.value.map(<[u8]>::to_vec)))
                .collect()
        })
        .unwrap_or_default();
    read(
        (message.topic(), message.partition(), message.offset()),
        message.key(),
        message.payload(),
        headers,
        topics,
    )
}

/// The unit a message names, given the message's topic, partition and
/// offset in `at`: its key as the id, or its position
/// `<topic>/<partition>/<offset>` when it has no key, its value as the body,
/// and the publish identity its attempts count under. Its count comes from
/// its attempt record, which the gate reads. A message whose key is not
/// UTF-8 names no unit.
pub(crate) fn read(
    at: (&str, i32, i64),
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
    headers: Vec<(String, Option<Vec<u8>>)>,
    topics: &Topics,
) -> Read {
    let (topic, partition, offset) = at;
    let id = match key.map(std::str::from_utf8) {
        Some(Ok(id)) => id.to_owned(),
        // The message's own position, on whichever topic it was read from. A
        // retry copy of the unit is keyed by this id, so the unit keeps it on
        // every later delivery.
        None => format!("{topic}/{partition}/{offset}"),
        Some(Err(_)) => {
            return Read::NoUnit(Outgoing {
                topic: Arc::from(topic),
                partition: None,
                key: key.map(<[u8]>::to_vec),
                payload: payload.map(<[u8]>::to_vec),
                headers,
            });
        }
    };
    Read::Unit {
        unit: Unit::new(id, payload.unwrap_or_default()),
        identity: PublishId::of_message(topic, partition, offset, &headers, topics),
    }
}

/// The dead-letter write for a message that names no unit.
fn no_unit_move(core: &Core, mut message: Outgoing, position: Position) -> Move {
    message.topic = Arc::clone(&core.topics().dead_letter);
    Move::new(
        message,
        position,
        Destination::DeadLetter(Cause::NoUnit),
        None,
        1,
        Finish::Complete,
    )
}
