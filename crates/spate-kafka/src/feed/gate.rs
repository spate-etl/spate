//! The feed's gate thread: it records each delivered unit's attempt on the
//! attempt topic before the unit reaches the reader, and moves a unit whose
//! recorded attempts have reached the attempt cap to the dead-letter topic
//! without reading it.
//!
//! A pass takes every delivery waiting at the gate, reads the high
//! watermark of each attempt partition they hash to, and waits until the
//! follower has read those partitions up to it. The ledger then holds every
//! attempt recorded before the pass began, and each delivery is written
//! with its recorded count plus one, or moved at the cap. The gate never
//! runs on the consumer's poll thread (INV-2), and it sends nothing under
//! the state lock: a refused enqueue settles inline, and settling takes
//! that lock.

use super::attempt::{self, partition_of};
use super::events::Cause;
use super::write::{self, Attempt, Destination, Finish, Move, Outgoing, Producer, Write};
use super::{Core, Gating, LatchOnPanic};
use rdkafka::error::KafkaError;
use rdkafka::producer::Producer as _;
use spate_core::error::ErrorClass;
use std::collections::BTreeSet;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

/// The bound on each of the gate's waits and watermark reads.
pub(crate) const GATE_STEP: Duration = Duration::from_secs(1);

/// The gate thread's state.
pub(crate) struct Gate {
    core: Arc<Core>,
    producer: Producer,
    step: Duration,
    /// The bound on a pass, from the moment it takes its deliveries.
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

impl Gate {
    pub(crate) fn new(core: Arc<Core>, producer: Producer, delivery_timeout: Duration) -> Gate {
        Gate {
            core,
            producer,
            step: GATE_STEP,
            delivery_timeout,
        }
    }

    /// The gate with `step` as the bound on each wait and watermark read.
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
    /// catch up on their attempt partitions, and admit them. A pass that
    /// cannot catch up puts its deliveries back for the next.
    pub(crate) fn pass(&self) {
        let entries = self.take();
        if entries.is_empty() {
            return;
        }
        let started = Instant::now();
        match self.catch_up(&entries, started) {
            Ok(()) => self.admit(entries),
            Err(halt) => self.put_back(entries, halt),
        }
    }

    /// Every delivery waiting at the gate, counted into `admitting`, after
    /// waiting up to one step for one to arrive. None once the feed stops
    /// or latches.
    fn take(&self) -> Vec<Gating> {
        let mut st = self.core.lock();
        // Checked under the state lock that `on_message` pushes under and
        // `Drop` stores the stop flag under, so neither wake is missed.
        if st.gating.is_empty() && st.fatal.is_none() && !self.core.stopped() {
            st = self
                .core
                .gate_wake
                .wait_timeout(st, self.step)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        if self.core.stopped() || st.fatal.is_some() {
            return Vec::new();
        }
        let entries: Vec<Gating> = st.gating.drain(..).collect();
        st.admitting += entries.len();
        entries
    }

    /// Wait until the ledger holds every record written to the attempt
    /// partitions of `entries` before the pass's watermark reads.
    fn catch_up(&self, entries: &[Gating], started: Instant) -> Result<(), Halt> {
        let partitions: BTreeSet<i32> = entries
            .iter()
            .map(|e| partition_of(e.identity.as_bytes(), self.core.attempt_partitions))
            .collect();
        let mut marks = Vec::with_capacity(partitions.len());
        for partition in partitions {
            self.check(started, partition)?;
            let (lwm, hwm) = self
                .producer
                .client()
                .fetch_watermarks(&self.core.topics.attempt, partition, self.step)
                .map_err(|e| self.watermark_error(partition, &e))?;
            marks.push((partition, lwm, hwm));
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
        Ok(())
    }

    /// `Err` when the feed stopped or latched, or the pass begun at
    /// `started` has run past the delivery timeout on `partition`.
    fn check(&self, started: Instant, partition: i32) -> Result<(), Halt> {
        if self.core.stopped() || self.core.latched() {
            return Err(Halt::Ended);
        }
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
    /// and give back their share of `admitting`.
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
            st.gating.push_front(entry);
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
/// copy was replaced since.
pub(crate) fn admission(core: &Core, entry: Gating) -> Option<Write> {
    let mut st = core.lock();
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
