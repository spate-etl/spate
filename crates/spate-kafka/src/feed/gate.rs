//! The feed's gate thread: it records each delivered unit's attempt on the
//! attempt topic before the unit reaches the reader, and moves a unit whose
//! recorded attempts have reached the attempt cap to the dead-letter topic
//! without reading it.
//!
//! A pass takes every delivery waiting at the gate, reads the high
//! watermark of each attempt partition they hash to, and waits until the
//! follower has read those partitions up to it. The ledger then holds every
//! attempt recorded before the pass began, and each delivery is written
//! with its recorded count plus one, or moved at the cap. A delivery whose
//! attempt partition's watermark read fails, or that the pass reaches its
//! bound before reading, goes back to the gate, and the rest of the pass
//! goes on without it; the gate then waits one step before its next pass.
//! A delivery put back counts against the reader's buffer, so an attempt
//! partition that stays unreadable pauses every work partition once its
//! deliveries fill that buffer. The gate never runs on the consumer's poll
//! thread (INV-2), and it sends nothing under the state lock: a refused
//! enqueue settles inline, and settling takes that lock.

use super::attempt::{self, partition_of};
use super::events::Cause;
use super::write::{self, Attempt, Destination, Finish, Move, Outgoing, Producer, Write};
use super::{Core, Gating, LatchOnPanic};
use rdkafka::error::{IsError as _, KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::producer::Producer as _;
use rdkafka::{Offset, TopicPartitionList};
use spate_core::error::ErrorClass;
use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

/// The bound on each of the gate's waits and watermark requests.
pub(crate) const GATE_STEP: Duration = Duration::from_secs(1);

/// The gate thread's state.
pub(crate) struct Gate {
    core: Arc<Core>,
    producer: Producer,
    step: Duration,
    /// The bound on a pass, from the moment it takes its deliveries. A pass
    /// can run up to two steps past it.
    delivery_timeout: Duration,
}

/// Why a pass ended without admitting what it took.
enum Halt {
    /// The feed stopped or latched.
    Ended,
    /// A watermark read failed, or the follower did not catch up within
    /// the pass's bound; the text says which.
    Behind(String),
}

/// What reading one attempt partition's watermarks gave a pass.
enum Read {
    /// The partition's low and high watermarks.
    Marks(i64, i64),
    /// The read failed in a way retrying can fix; the text says how.
    Failed(String),
    /// The pass reached its bound before the read finished.
    Late,
}

/// The attempt partitions a pass did not read, and why.
#[derive(Default)]
struct Unread {
    partitions: Vec<i32>,
    reasons: Vec<String>,
}

impl Gate {
    pub(crate) fn new(core: Arc<Core>, producer: Producer, delivery_timeout: Duration) -> Gate {
        Gate {
            core,
            producer,
            step: GATE_STEP,
            delivery_timeout,
        }
    }

    /// The gate with `step` as the bound on each wait and watermark request.
    #[cfg(test)]
    pub(crate) fn with_step(mut self, step: Duration) -> Gate {
        self.step = step;
        self
    }

    /// Start the gate on a thread of its own, which runs until the feed
    /// stops or latches.
    ///
    /// # Errors
    ///
    /// When the thread cannot be started.
    pub(crate) fn spawn(self) -> std::io::Result<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("spate-kafka-feed-gate".to_owned())
            .spawn(move || self.run())
    }

    fn run(self) {
        let _guard = LatchOnPanic {
            core: Arc::clone(&self.core),
            thread: "gate",
        };
        while !self.core.stopped() && !self.core.latched() {
            self.pass();
        }
    }

    /// Run one pass: take the deliveries waiting, wait for the follower to
    /// catch up on their attempt partitions, and admit them. The deliveries
    /// of a partition whose watermark read failed, or that the pass reached
    /// its bound before reading, go back for the next pass, which starts
    /// after one step, and count against the reader's buffer while they
    /// wait. A pass whose follower does not catch up within its bound puts
    /// all its deliveries back.
    pub(crate) fn pass(&self) {
        let entries = self.take();
        if entries.is_empty() {
            return;
        }
        let started = Instant::now();
        match self.catch_up(&entries, started) {
            Ok(unread) if unread.partitions.is_empty() => self.admit(entries),
            Ok(unread) => {
                let (back, read): (Vec<Gating>, Vec<Gating>) = entries
                    .into_iter()
                    .partition(|e| unread.partitions.contains(&self.partition(e)));
                self.put_back(back, Halt::Behind(unread.reasons.join("; ")));
                self.admit(read);
                self.rest();
            }
            Err(halt) => self.put_back(entries, halt),
        }
    }

    /// The attempt partition of `entry`.
    fn partition(&self, entry: &Gating) -> i32 {
        partition_of(entry.identity.as_bytes(), self.core.attempt_partitions)
    }

    /// Wait one step before the next pass, so a watermark read that fails
    /// at once is not repeated in a busy loop. A stop, or a delivery that
    /// arrives while it waits, ends the wait early.
    fn rest(&self) {
        let st = self.core.lock();
        // Checked under the state lock, as in `take`.
        if st.fatal.is_none() && !self.core.stopped() {
            drop(
                self.core
                    .gate_wake
                    .wait_timeout(st, self.step)
                    .unwrap_or_else(PoisonError::into_inner),
            );
        }
    }

    /// Every delivery waiting at the gate, counted into `admitting`, after
    /// waiting up to one step for one to arrive. None once the feed stops
    /// receiving, stops or latches: a delivery left at the gate gets no
    /// attempt record, and the next owner of its partition reads it.
    fn take(&self) -> Vec<Gating> {
        let mut st = self.core.lock();
        // Checked under the state lock that `on_message` pushes under and
        // `stop` stores the stop flag under, so neither wake is missed. A
        // stopping feed waits too, so the loop does not spin over
        // deliveries it will not take.
        if (st.gating.is_empty() || st.stopping) && st.fatal.is_none() && !self.core.stopped() {
            st = self
                .core
                .gate_wake
                .wait_timeout(st, self.step)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        if st.stopping || self.core.stopped() || st.fatal.is_some() {
            return Vec::new();
        }
        let entries: Vec<Gating> = st.gating.drain(..).collect();
        st.admitting += entries.len();
        entries
    }

    /// Wait until the ledger holds every record written to the attempt
    /// partitions of `entries` before the pass's watermark reads. Returns
    /// each partition whose watermark read failed in a way retrying can
    /// fix, or that the pass reached its bound before reading, with the
    /// reasons; the others are caught up.
    fn catch_up(&self, entries: &[Gating], started: Instant) -> Result<Unread, Halt> {
        let partitions: BTreeSet<i32> = entries.iter().map(|e| self.partition(e)).collect();
        let mut marks = Vec::with_capacity(partitions.len());
        let mut unread = Unread::default();
        let mut late = Vec::new();
        let mut slowest: Option<(i32, Duration)> = None;
        for partition in partitions {
            let begun = Instant::now();
            match self.read(started, partition)? {
                Read::Marks(lwm, hwm) => marks.push((partition, lwm, hwm)),
                Read::Failed(reason) => {
                    unread.partitions.push(partition);
                    unread.reasons.push(reason);
                }
                Read::Late => late.push(partition),
            }
            let took = begun.elapsed();
            if slowest.is_none_or(|(_, longest)| took > longest) {
                slowest = Some((partition, took));
            }
        }
        if !late.is_empty() {
            let mut reason = format!(
                "the pass reached its bound of {:?} before reading `{}` partitions {late:?}",
                self.delivery_timeout, self.core.topics.attempt
            );
            if let Some((partition, took)) = slowest {
                reason.push_str(&format!(
                    "; its slowest read, of partition {partition}, took {took:?}"
                ));
            }
            unread.partitions.extend(late);
            unread.reasons.push(reason);
        }
        for (partition, lwm, hwm) in marks {
            loop {
                {
                    let ledger = attempt::lock(&self.core.ledger);
                    // Checked under the ledger's lock, which `Drop` takes
                    // after the stop flag is stored and before it notifies.
                    if self.core.stopped() {
                        return Err(Halt::Ended);
                    }
                    if ledger.caught_up(partition, lwm, hwm) {
                        break;
                    }
                    #[cfg(test)]
                    let ledger = {
                        let mut ledger = ledger;
                        ledger.gate_waits += 1;
                        ledger
                    };
                    drop(
                        self.core
                            .ledger
                            .1
                            .wait_timeout(ledger, self.step)
                            .unwrap_or_else(PoisonError::into_inner),
                    );
                }
                // With the ledger's lock released: `latched` takes the
                // state lock, which is never taken under it.
                self.check(started, partition)?;
            }
        }
        Ok(unread)
    }

    /// Read the low and high watermarks of attempt `partition` for the
    /// pass begun at `started`, each request bounded by a step and begun
    /// only before the pass's bound.
    ///
    /// librdkafka reads the two with one ListOffsets request each, and when
    /// a refused reply is served together with the other's success it
    /// returns that success's offset as both watermarks. An empty partition
    /// also reads as two equal watermarks, so equal watermarks are read
    /// again, each offset on a request of its own.
    fn read(&self, started: Instant, partition: i32) -> Result<Read, Halt> {
        if self.late(started)? {
            return Ok(Read::Late);
        }
        #[cfg(test)]
        self.core.gate_reads.fetch_add(1, Ordering::SeqCst);
        let (lwm, hwm) = match self.producer.client().fetch_watermarks(
            &self.core.topics.attempt,
            partition,
            self.step,
        ) {
            Ok(marks) => marks,
            Err(e) => return self.failed(partition, &e),
        };
        if lwm != hwm {
            return Ok(Read::Marks(lwm, hwm));
        }
        let mut marks = [0; 2];
        for (mark, at) in marks.iter_mut().zip([Offset::Beginning, Offset::End]) {
            if self.late(started)? {
                return Ok(Read::Late);
            }
            #[cfg(test)]
            self.core.gate_reads.fetch_add(1, Ordering::SeqCst);
            *mark = match self.list_offset(partition, at) {
                Ok(offset) => offset,
                Err(e) => return self.failed(partition, &e),
            };
        }
        Ok(Read::Marks(marks[0], marks[1]))
    }

    /// Whether the pass begun at `started` has reached its bound. `Err`
    /// once the feed stopped or latched.
    fn late(&self, started: Instant) -> Result<bool, Halt> {
        self.ended()?;
        Ok(started.elapsed() >= self.delivery_timeout)
    }

    /// `Err` once the feed stopped or latched.
    fn ended(&self) -> Result<(), Halt> {
        if self.core.stopped() || self.core.latched() {
            return Err(Halt::Ended);
        }
        Ok(())
    }

    /// The read of `partition` that failed with `error`. `Err`, latching
    /// the feed, on a Fatal error.
    fn failed(&self, partition: i32, error: &KafkaError) -> Result<Read, Halt> {
        match self.watermark_error(partition, error) {
            Halt::Behind(reason) => Ok(Read::Failed(reason)),
            Halt::Ended => Err(Halt::Ended),
        }
    }

    /// The offset `at` names on attempt `partition`, `Beginning` for the
    /// low watermark and `End` for the high, read with one ListOffsets
    /// request, so a refusal is reported as an error.
    fn list_offset(&self, partition: i32, at: Offset) -> KafkaResult<i64> {
        let topic = &*self.core.topics.attempt;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, at)?;
        let timeout = i32::try_from(self.step.as_millis()).unwrap_or(i32::MAX);
        // SAFETY: the client pointer is valid while `self.producer` lives,
        // and `tpl` owns its list until the end of this function.
        // `rd_kafka_offsets_for_times` writes each result into the list and
        // keeps neither pointer past the call.
        let code = unsafe {
            rdkafka::bindings::rd_kafka_offsets_for_times(
                self.producer.client().native_ptr(),
                tpl.ptr(),
                timeout,
            )
        };
        if code.is_error() {
            return Err(KafkaError::MetadataFetch(code.into()));
        }
        let read = tpl
            .find_partition(topic, partition)
            .ok_or(KafkaError::MetadataFetch(RDKafkaErrorCode::BadMessage))?;
        read.error()?;
        match read.offset() {
            Offset::Offset(offset) => Ok(offset),
            _ => Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::OffsetNotAvailable,
            )),
        }
    }

    /// `Err` when the feed stopped or latched, or the pass begun at
    /// `started` has run past the delivery timeout waiting for the follower
    /// on `partition`.
    fn check(&self, started: Instant, partition: i32) -> Result<(), Halt> {
        self.ended()?;
        if started.elapsed() >= self.delivery_timeout {
            let read_to = attempt::lock(&self.core.ledger).read_to(partition);
            return Err(Halt::Behind(format!(
                "the attempt follower did not catch up on `{}` partition {partition} within \
                 {:?}; it has read up to offset {}",
                self.core.topics.attempt,
                self.delivery_timeout,
                read_to.map_or_else(|| "none".to_owned(), |o| o.to_string())
            )));
        }
        Ok(())
    }

    /// The halt for a failed watermark read of `partition`, latching the
    /// feed on a Fatal error.
    fn watermark_error(&self, partition: i32, error: &KafkaError) -> Halt {
        let reason = format!(
            "reading the watermarks of `{}` partition {partition}: {error}",
            self.core.topics.attempt
        );
        let class = error
            .rdkafka_error_code()
            .map(|code| crate::error::classify_consumer_error(code, true));
        if class == Some(ErrorClass::Fatal) {
            self.core.latch(&reason);
            return Halt::Ended;
        }
        Halt::Behind(reason)
    }

    /// Return `entries` to the front of the gate's queue, in their order,
    /// and give back their share of `admitting`. An entry whose partition
    /// was given up during the pass is dropped.
    fn put_back(&self, entries: Vec<Gating>, halt: Halt) {
        if let Halt::Behind(reason) = &halt {
            tracing::warn!(
                units = entries.len(),
                "{reason}; the units wait at the gate and are tried again"
            );
        }
        let mut st = self.core.lock();
        st.admitting = st.admitting.saturating_sub(entries.len());
        for entry in entries.into_iter().rev() {
            if st.tracker.held(entry.receipt).is_some() {
                st.gating.push_front(entry);
            }
        }
    }

    /// Send each delivery's admission, with the state lock released.
    pub(crate) fn admit(&self, entries: Vec<Gating>) {
        for entry in entries {
            if let Some(write) = admission(&self.core, entry) {
                write::send(&self.core, &self.producer, Box::new(write));
            }
        }
    }
}

/// The write that admits `entry`, taken by the gate: its attempt record
/// below the attempt cap, with its receipt still held, or its move to the
/// dead-letter topic at the cap, with its receipt released. `None`, giving
/// back its share of `admitting`, when its partition was given up or the
/// copy was replaced since, and when the feed has stopped receiving, which
/// puts `entry` back at the gate with no attempt recorded.
pub(crate) fn admission(core: &Core, entry: Gating) -> Option<Write> {
    let mut st = core.lock();
    // A pass that took its deliveries before the stop admits none of them.
    if st.stopping {
        st.admitting = st.admitting.saturating_sub(1);
        if st.tracker.held(entry.receipt).is_some() {
            st.gating.push_back(entry);
        }
        return None;
    }
    // The state lock first, then the ledger's.
    let count = attempt::lock(&core.ledger).count(entry.identity.as_bytes());
    let partition = partition_of(entry.identity.as_bytes(), core.attempt_partitions);
    if count < core.attempt_cap.get() {
        if st.tracker.held(entry.receipt).is_none() {
            st.admitting = st.admitting.saturating_sub(1);
            return None;
        }
        return Some(Write::Attempt(Attempt::new(
            &core.topics.attempt,
            entry.identity,
            partition,
            count + 1,
            entry.receipt,
            entry.unit,
        )));
    }
    st.admitting = st.admitting.saturating_sub(1);
    let position = st.tracker.release(entry.receipt)?;
    drop(st);
    tracing::warn!(
        unit = entry.unit.id,
        attempts = count,
        "a unit's recorded attempts reached the attempt cap; writing it to the dead-letter \
         topic unread"
    );
    Some(Write::Move(Move::new(
        Outgoing::unit(
            Arc::clone(&core.topics.dead_letter),
            &entry.unit,
            count,
            Some(&entry.identity),
        ),
        position,
        Destination::DeadLetter(Cause::AttemptCap),
        Some(entry.unit.id.clone()),
        count,
        Finish::Tombstone(entry.identity),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::config::KafkaFeedConfig;
    use crate::feed::producer::FeedProducerContext;
    use rdkafka::config::FromClientConfigAndContext;
    use rdkafka::error::RDKafkaErrorCode as C;
    use std::num::NonZeroU32;
    use std::sync::mpsc;

    /// Pins that the wait after a pass that put units back is skipped on a
    /// feed that stopped or latched before the wait began, whose notify has
    /// already fired or never fires: with a step of an hour, `rest` returns.
    #[test]
    fn the_wait_after_a_partial_pass_is_skipped_once_the_feed_ends() {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(
            "kafka:\n  brokers: 127.0.0.1:1\n  group_id: workers\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: units-attempts\n  statistics_interval: 0s\n",
        )
        .unwrap();
        let config = KafkaFeedConfig::from_section(&section).unwrap();
        for how in ["stopped", "latched"] {
            let core = Arc::new(Core::new(&config, NonZeroU32::MIN));
            let producer = Producer::from_config_and_context(
                &config.producer_config(false),
                FeedProducerContext::new(Arc::clone(&core)),
            )
            .unwrap();
            let gate = Gate::new(Arc::clone(&core), producer, config.delivery_timeout)
                .with_step(Duration::from_secs(3600));
            if how == "stopped" {
                crate::feed::stop(&core);
            } else {
                drop(core.latch("probe"));
            }
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                gate.rest();
                let _ = tx.send(());
            });
            assert!(
                rx.recv_timeout(Duration::from_secs(10)).is_ok(),
                "the gate of a {how} feed waited a step"
            );
        }
    }

    /// Pins how a failed watermark read ends a pass: a refusal retrying
    /// cannot fix stops the feed, naming the read, and a timeout leaves the
    /// feed running for the next pass.
    #[test]
    fn only_a_refused_watermark_read_stops_the_feed() {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(
            "kafka:\n  brokers: 127.0.0.1:1\n  group_id: workers\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
             attempt_topic: units-attempts\n  statistics_interval: 0s\n",
        )
        .unwrap();
        let config = KafkaFeedConfig::from_section(&section).unwrap();
        let core = Arc::new(Core::new(&config, NonZeroU32::MIN));
        let producer = Producer::from_config_and_context(
            &config.producer_config(false),
            FeedProducerContext::new(Arc::clone(&core)),
        )
        .unwrap();
        let gate = Gate::new(Arc::clone(&core), producer, config.delivery_timeout);

        let halt = gate.watermark_error(0, &KafkaError::MetadataFetch(C::OperationTimedOut));
        assert!(matches!(halt, Halt::Behind(_)));
        assert!(!core.latched());

        let halt = gate.watermark_error(1, &KafkaError::MetadataFetch(C::TopicAuthorizationFailed));
        assert!(matches!(halt, Halt::Ended));
        let reason = core.lock().fatal.clone().expect("the feed stopped").reason;
        assert!(
            reason.contains("reading the watermarks of `units-attempts` partition 1"),
            "{reason}"
        );
    }
}
