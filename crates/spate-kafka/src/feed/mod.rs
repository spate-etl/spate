//! [`KafkaFeed`]: the work feed over a work topic, a retry topic, a
//! dead-letter topic and a compacted attempt topic.
//!
//! The feed runs three threads. The consumer thread reads the work and
//! retry topics as a member of the feed's group. The follower thread reads
//! the attempt topic into memory. The gate thread records each delivered
//! unit's attempt before the unit reaches the reader. One producer writes
//! publishes, attempt records and their tombstones, retries and dead
//! letters. The [`Feed`] methods lock the shared state, change it, and hand
//! any write to the producer, so none of them waits on I/O (INV-2).
//!
//! # Offsets
//!
//! A unit is a message: its key is the unit's id and its value the unit's
//! body. The feed's offset tracker holds every delivered offset until it
//! finishes, and each partition commits up to its first unfinished offset.
//! An acknowledged unit's offset finishes once the tombstone of its attempt
//! record is confirmed by its delivery report. A failed unit's offset
//! finishes once its write to the retry topic is confirmed, or, for a write
//! to the dead-letter topic, once the tombstone sent after that write is
//! confirmed. A write that fails transiently is sent again after a backoff,
//! and its offset stays unfinished meanwhile, so a stuck write holds back
//! only its own partition.
//!
//! # Counting
//!
//! A unit's attempts are recorded on the attempt topic under the publish
//! identity of its message, as the `attempt` module describes. Before a
//! unit is read, the gate waits until the follower has read the unit's
//! attempt partition up to its high watermark, then writes the recorded
//! count plus one. The unit reaches the reader with that count once the
//! write is confirmed, so every read counts, including one whose worker
//! dies. Two copies of one publish at the gate at the same time can read the
//! same count, so they count once between them. A unit whose recorded
//! attempts have reached the attempt cap goes to the dead-letter topic
//! without being read. A unit failed on a delivery at or past the cap goes
//! to the dead-letter topic, and below it to the retry topic.

mod attempt;
mod config;
mod consumer;
mod error;
mod events;
mod gate;
mod inflight;
mod producer;
mod startup;
#[cfg(test)]
mod tests;
mod write;

pub use error::KafkaFeedError;

use crate::context::SourceContext;
use crate::error::redacted_client_error;
use attempt::{Follower, Ledger, PublishId, SharedLedger};
use config::{BACKEND, KafkaFeedConfig};
use consumer::Driver;
use inflight::Tracker;
use producer::FeedProducerContext;
use rdkafka::admin::AdminClient;
use rdkafka::config::{FromClientConfig, FromClientConfigAndContext};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::ThreadedProducer;
use spate_core::config::FeedSection;
use spate_core::feed::{
    Confirmation, Delivery, Feed, FeedError, FeedErrorKind, FeedWaker, Receipt, Unit, confirmation,
};
use startup::TopicAdmin as _;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use write::{Destination, Failure, Finish, Move, Outgoing, Producer, Tombstone, Write, WriteClass};

/// Units the feed holds for the reader, those waiting at the gate and those
/// with their attempt write in flight included, before the consumer pauses
/// its partitions. It resumes them once the count is down to half.
const READY_CAPACITY: usize = 128;

/// The topics the feed reads and writes.
#[derive(Debug)]
pub(crate) struct Topics {
    pub(crate) work: Arc<str>,
    pub(crate) retry: Arc<str>,
    pub(crate) dead_letter: Arc<str>,
    pub(crate) attempt: Arc<str>,
}

impl Topics {
    fn new(config: &KafkaFeedConfig) -> Topics {
        Topics {
            work: Arc::from(config.topic.as_str()),
            retry: Arc::from(config.retry_topic.as_str()),
            dead_letter: Arc::from(config.dead_letter_topic.as_str()),
            attempt: Arc::from(config.attempt_topic.as_str()),
        }
    }

    /// `name` as one of the feed's topics, sharing its allocation.
    pub(crate) fn of(&self, name: &str) -> Arc<str> {
        [&self.work, &self.retry, &self.dead_letter]
            .into_iter()
            .find(|t| &***t == name)
            .map_or_else(|| Arc::from(name), Arc::clone)
    }
}

/// A delivery waiting at the gate for its attempt to be recorded.
#[derive(Debug)]
pub(crate) struct Gating {
    pub(crate) receipt: Receipt,
    pub(crate) unit: Unit,
    pub(crate) identity: PublishId,
}

/// A delivery buffered for the reader, or handed to it and not yet
/// settled.
#[derive(Debug)]
pub(crate) struct Ready {
    pub(crate) receipt: Receipt,
    pub(crate) unit: Unit,
    /// The attempt recorded for this read.
    pub(crate) deliveries: u32,
    pub(crate) identity: PublishId,
    /// The attempt-topic partition of `identity`.
    pub(crate) attempt_partition: i32,
}

#[derive(Debug, Default)]
pub(crate) struct State {
    pub(crate) tracker: Tracker,
    /// Deliveries the gate has not taken yet, oldest first.
    pub(crate) gating: VecDeque<Gating>,
    /// Deliveries the gate took whose attempt write has not settled.
    pub(crate) admitting: usize,
    /// Deliveries not yet handed to the reader, oldest first.
    pub(crate) ready: VecDeque<Ready>,
    /// Deliveries handed to the reader, by receipt.
    pub(crate) handed: HashMap<Receipt, Ready>,
    pub(crate) fatal: Option<FeedError>,
    /// A transient consumer failure, returned by the next `receive` only.
    pub(crate) retryable: Option<FeedError>,
    /// Writes to send again, each with the instant it is due.
    pub(crate) resends: Vec<(Instant, Write)>,
    pub(crate) waker: Option<FeedWaker>,
}

impl State {
    /// Forget the deliveries `receipts` name, at the gate, buffered or
    /// handed out.
    pub(crate) fn forget(&mut self, receipts: &[Receipt]) {
        if receipts.is_empty() {
            return;
        }
        self.gating.retain(|g| !receipts.contains(&g.receipt));
        self.ready.retain(|r| !receipts.contains(&r.receipt));
        for receipt in receipts {
            self.handed.remove(receipt);
        }
    }

    /// Drop the deliveries at the gate and the buffered ones of partitions
    /// the tracker no longer owns, and their receipts with them.
    pub(crate) fn drop_unowned(&mut self) {
        let State {
            tracker,
            gating,
            ready,
            ..
        } = self;
        // `held` also drops the stale receipt from the tracker, which no
        // reader will ever touch.
        gating.retain(|g| tracker.held(g.receipt).is_some());
        ready.retain(|r| tracker.held(r.receipt).is_some());
    }

    /// The deliveries the feed holds for the reader, as counted against
    /// [`READY_CAPACITY`]: at the gate, with their attempt write in flight,
    /// and buffered.
    pub(crate) fn held_for_reader(&self) -> usize {
        self.ready.len() + self.gating.len() + self.admitting
    }

    pub(crate) fn wake(&self) {
        if let Some(waker) = &self.waker {
            waker.wake();
        }
    }
}

/// What the feed's consumer, gate and follower threads, its producer's
/// delivery reports and its [`Feed`] methods share.
///
/// Lock order: the state lock, then the ledger's. The state lock is never
/// taken while the ledger's is held.
#[derive(Debug)]
pub(crate) struct Core {
    state: Mutex<State>,
    /// Paired with `state`: the gate waits on it for deliveries.
    gate_wake: Condvar,
    /// Set when the feed is dropped; the consumer, gate and follower
    /// threads exit on it.
    stop: AtomicBool,
    topics: Topics,
    attempt_cap: NonZeroU32,
    /// The attempt topic's partition count, read at connect.
    attempt_partitions: NonZeroU32,
    /// The attempt counts the follower reads and the gate answers from.
    ledger: SharedLedger,
    /// How many watermark calls the gate has begun, counted just before
    /// each `fetch_watermarks` and each `list_offset`.
    #[cfg(test)]
    gate_reads: std::sync::atomic::AtomicUsize,
}

impl Core {
    fn new(config: &KafkaFeedConfig, attempt_partitions: NonZeroU32) -> Core {
        Core {
            state: Mutex::new(State::default()),
            gate_wake: Condvar::new(),
            stop: AtomicBool::new(false),
            topics: Topics::new(config),
            attempt_cap: config.attempt_cap,
            attempt_partitions,
            ledger: Arc::new((Mutex::new(Ledger::new(attempt_partitions)), Condvar::new())),
            #[cfg(test)]
            gate_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn topics(&self) -> &Topics {
        &self.topics
    }

    pub(crate) fn stopped(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    /// Whether the feed has latched a Fatal error.
    pub(crate) fn latched(&self) -> bool {
        self.lock().fatal.is_some()
    }

    /// Make the feed answer every later call with a Fatal error built from
    /// `reason`, drop the writes waiting to be sent again, and wake the
    /// reader. The consumer thread then commits nothing more and ends. The
    /// first latch wins. Returns the latched error.
    pub(crate) fn latch(&self, reason: &str) -> FeedError {
        let (error, first, waker) = {
            let mut st = self.lock();
            let first = st.fatal.is_none();
            let error = st
                .fatal
                .get_or_insert_with(|| FeedError::new(FeedErrorKind::Fatal, reason))
                .clone();
            st.resends.clear();
            (error, first, st.waker.clone())
        };
        if first {
            tracing::error!(error = %error.reason, "the Kafka feed stopped");
        }
        if let Some(waker) = waker {
            waker.wake();
        }
        error
    }

    /// Settle `write` with the outcome its delivery report, or the refused
    /// enqueue, gave it.
    pub(crate) fn settle(&self, write: Write, outcome: Result<(), Failure>) {
        events::delivery_report(outcome.is_ok());
        match outcome {
            Ok(()) => self.confirmed(write),
            Err(failure) => self.failed(write, failure),
        }
    }

    fn confirmed(&self, write: Write) {
        match write {
            Write::Publish { confirmer, .. } => confirmer.confirm(),
            Write::Move(moving) => {
                match &moving.finish {
                    // A report for a partition revoked since completes
                    // nothing: the unit is read again by its new owner.
                    Finish::Complete => {
                        self.lock().tracker.complete(&moving.position);
                    }
                    Finish::Tombstone(identity) => {
                        let tombstone = Tombstone::new(
                            &self.topics.attempt,
                            identity,
                            attempt::partition_of(identity.as_bytes(), self.attempt_partitions),
                            moving.position.clone(),
                        );
                        // Queued for the consumer thread to send: `Core`
                        // holds no producer, since the producer's context
                        // holds `Core`.
                        let mut st = self.lock();
                        if st.fatal.is_none() && !self.stopped() {
                            st.resends
                                .push((Instant::now(), Write::Tombstone(tombstone)));
                        }
                    }
                }
                events::moved(&moving);
            }
            Write::Attempt(attempt) => {
                events::attempt_written(attempt.started.elapsed());
                let mut st = self.lock();
                st.admitting = st.admitting.saturating_sub(1);
                // `None` when the partition was given up or the copy was
                // replaced since; the read stays recorded.
                if st.tracker.held(attempt.receipt).is_some() {
                    st.ready.push_back(Ready {
                        receipt: attempt.receipt,
                        unit: attempt.unit,
                        deliveries: attempt.attempts,
                        identity: attempt.identity,
                        attempt_partition: attempt.attempt_partition,
                    });
                    st.wake();
                }
            }
            Write::Tombstone(tombstone) => {
                self.lock().tracker.complete(&tombstone.position);
                events::tombstoned();
            }
        }
    }

    fn failed(&self, mut write: Write, failure: Failure) {
        if let Write::Publish { confirmer, .. } = write {
            let error = match write::classify(failure.code) {
                WriteClass::Refused => FeedError::new(FeedErrorKind::Refused, failure.reason),
                WriteClass::Fatal => self.latch(&failure.reason),
                WriteClass::Retryable => FeedError::new(FeedErrorKind::Retryable, failure.reason),
            };
            confirmer.reject(error);
            return;
        }
        let topic = Arc::clone(&write.message().topic);
        match write::classify(failure.code) {
            WriteClass::Refused | WriteClass::Fatal => {
                self.latch(&format!(
                    "writing {} to `{topic}` failed: {}",
                    write.what(),
                    failure.reason
                ));
            }
            WriteClass::Retryable => {
                let Some(backoff) = write.backoff_mut() else {
                    return;
                };
                let retry_in = *backoff;
                *backoff = write::next_backoff(retry_in);
                tracing::warn!(
                    %topic,
                    error = %failure.reason,
                    ?retry_in,
                    "writing {} failed; sending it again",
                    write.what()
                );
                let mut st = self.lock();
                // A resent attempt write keeps its share of `admitting`
                // until it settles or `take_due` drops it.
                if st.fatal.is_none() && !self.stopped() {
                    st.resends.push((Instant::now() + retry_in, write));
                }
            }
        }
    }
}

/// Latches the feed when one of its threads unwinds, so the reader stops
/// instead of waiting on a feed that delivers nothing.
pub(crate) struct LatchOnPanic {
    pub(crate) core: Arc<Core>,
    /// The thread's name in the latch reason, as in "the consumer thread".
    pub(crate) thread: &'static str,
}

impl Drop for LatchOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.core
                .latch(&format!("the Kafka feed's {} thread panicked", self.thread));
        }
    }
}

/// The work feed over Kafka topics, built from a `feed: { kafka: … }`
/// section.
///
/// Workers that share a `group_id` share the work: each partition of the
/// work and retry topics is read by one of them. A publish writes the unit
/// to the work topic. Before a unit is read, its attempt is recorded on the
/// attempt topic, and a unit whose recorded attempts have reached the
/// attempt cap goes to the dead-letter topic unread. An acknowledged unit's
/// attempt record is tombstoned before its offset is committed. A failed
/// unit is written to the retry topic, or to the dead-letter topic at the
/// attempt cap, before its offset is. A message with no key, or a key that
/// is not UTF-8, names no unit and goes to the dead-letter topic at once.
///
/// Dropping the feed waits up to 1 s for its writes in flight, commits what
/// has finished, leaves the group and stops its threads. Units not yet
/// finished are delivered again to the next owner of their partition. A
/// feed that stopped on a Fatal error commits nothing more, so every unit
/// since its last commit is delivered again.
pub struct KafkaFeed {
    core: Arc<Core>,
    producer: Producer,
    thread: Option<std::thread::JoinHandle<()>>,
    gate: Option<std::thread::JoinHandle<()>>,
    follower: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for KafkaFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaFeed")
            .field("topics", &self.core.topics)
            .field("attempt_cap", &self.core.attempt_cap)
            .finish_non_exhaustive()
    }
}

impl KafkaFeed {
    /// Build the feed a `feed: { kafka: … }` section names, check its topics,
    /// and start its threads.
    ///
    /// Connecting reads the metadata and configuration of the work, retry,
    /// dead-letter and attempt topics, on the runtime `io`, before any of the
    /// feed's clients joins the group. A missing topic, a compacted work,
    /// retry or dead-letter topic, or an attempt topic that is not compacted
    /// only, fails here. The checks take at most 30 seconds. The feed itself
    /// runs on threads of its own.
    ///
    /// # Errors
    ///
    /// [`KafkaFeedError::Config`] for a section the feed cannot read.
    /// [`KafkaFeedError::Refused`] when librdkafka refuses the client
    /// configuration, as for a property value it does not accept; when a
    /// broker rejects the client's credential or either side rejects the
    /// TLS handshake, naming the rejection, as soon as the client reports
    /// it; when the cluster refuses a read of a topic as unauthorized or for
    /// an invalid topic name, or describes no configuration entry for one;
    /// and for topics the feed cannot run on: a missing work, retry,
    /// dead-letter or attempt topic, a work, retry or dead-letter topic whose
    /// `cleanup.policy` includes `compact`, an attempt topic whose
    /// `cleanup.policy` is anything but `compact`, and an attempt topic that
    /// reports no partitions.
    /// [`KafkaFeedError::Unavailable`] when a read of the topics fails
    /// otherwise or the checks take longer than 30 seconds, as they do when
    /// no broker accepts a connection, and when one of the feed's consumer,
    /// gate and follower threads cannot be started. A failed gate or consumer
    /// start stops and joins the threads already started before it returns.
    ///
    /// # Panics
    ///
    /// When called from inside an async context, as
    /// [`Handle::block_on`](tokio::runtime::Handle::block_on) does.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spate_core::config::FeedSection;
    /// use spate_kafka::feed::KafkaFeed;
    ///
    /// # fn run(section: &FeedSection, io: &tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>> {
    /// let feed = KafkaFeed::connect(section, io)?;
    /// # Ok(()) }
    /// ```
    pub fn connect(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
    ) -> Result<KafkaFeed, KafkaFeedError> {
        Self::connect_with(section, io, startup::Scope::All, startup::CONNECT_BOUND)
    }

    /// [`connect`](Self::connect) with only the checks that the topics
    /// exist, for rdkafka's MockCluster, which answers no request for a
    /// topic's configuration.
    ///
    /// # Errors
    ///
    /// As [`connect`](Self::connect), less the configuration checks.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn connect_without_config_checks(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
    ) -> Result<KafkaFeed, KafkaFeedError> {
        Self::connect_with(
            section,
            io,
            startup::Scope::Existence,
            startup::CONNECT_BOUND,
        )
    }

    fn connect_with(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
        scope: startup::Scope,
        bound: Duration,
    ) -> Result<KafkaFeed, KafkaFeedError> {
        let config = KafkaFeedConfig::from_section(section)?;
        let openssl_env = crate::security::openssl_env_overrides();
        let admin_config = config.admin_config(openssl_env);
        let admin = AdminClient::from_config(&admin_config).map_err(|e| {
            KafkaFeedError::Refused(format!(
                "creating the admin client: {}",
                redacted_client_error(&e)
            ))
        })?;
        let reader = ThreadedProducer::from_config_and_context(
            &admin_config,
            crate::sink::SinkContext::detached(),
        )
        .map_err(|e| {
            KafkaFeedError::Refused(format!(
                "creating the metadata client: {}",
                redacted_client_error(&e)
            ))
        })?;
        let topics = Topics::new(&config);
        let step = Mutex::new("starting".to_owned());
        let checked = {
            let admin = Arc::new(startup::Brokers::new(admin, reader));
            let watched = Arc::clone(&admin);
            io.block_on(startup::bounded(
                &step,
                bound,
                move || watched.rejection(),
                startup::check(&*admin, &topics, scope, &step),
            ))?
        };
        let (core, producer, driver) = parts(&config, checked.attempt_partitions)?;
        let follower = Follower::new(&config, Arc::clone(&core))?
            .spawn()
            .map_err(|e| {
                KafkaFeedError::Unavailable(format!("starting the attempt follower's thread: {e}"))
            })?;
        // Built before the gate and consumer start, so a failed start
        // returns through `Drop`, which stops and joins what has started.
        let mut feed = KafkaFeed {
            core,
            producer,
            thread: None,
            gate: None,
            follower: Some(follower),
        };
        let gate = gate::Gate::new(
            Arc::clone(&feed.core),
            feed.producer.clone(),
            config.delivery_timeout,
        );
        feed.gate = Some(gate.spawn().map_err(|e| {
            KafkaFeedError::Unavailable(format!("starting the feed's gate thread: {e}"))
        })?);
        feed.thread = Some(
            std::thread::Builder::new()
                .name("spate-kafka-feed".to_owned())
                .spawn(move || consumer::run(driver))
                .map_err(|e| {
                    KafkaFeedError::Unavailable(format!("starting the feed's consumer thread: {e}"))
                })?,
        );
        log_started(&config, &checked);
        Ok(feed)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.core.lock()
    }
}

/// Log the INFO line a feed that connected writes: its section and what the
/// startup checks read, `unreported` where the broker gave no value.
fn log_started(config: &KafkaFeedConfig, checked: &startup::Checked) {
    let reported = |v: &Option<String>| v.clone().unwrap_or_else(|| "unreported".to_owned());
    tracing::info!(
        group_id = %config.group_id,
        topic = %config.topic,
        retry_topic = %config.retry_topic,
        dead_letter_topic = %config.dead_letter_topic,
        attempt_topic = %config.attempt_topic,
        attempt_cap = config.attempt_cap,
        cleanup_policy = %reported(&checked.cleanup_policy),
        retry_cleanup_policy = %reported(&checked.retry_cleanup_policy),
        dead_letter_cleanup_policy = %reported(&checked.dead_letter_cleanup_policy),
        attempt_cleanup_policy = %reported(&checked.attempt_cleanup_policy),
        attempt_partitions = checked.attempt_partitions,
        "the Kafka feed started"
    );
}

/// The feed's shared state, its producer, and its consumer thread's state
/// with the consumer subscribed, before the thread starts.
/// `attempt_partitions` is the attempt topic's partition count.
fn parts(
    config: &KafkaFeedConfig,
    attempt_partitions: NonZeroU32,
) -> Result<(Arc<Core>, Producer, Driver), KafkaFeedError> {
    let openssl_env = crate::security::openssl_env_overrides();
    let core = Arc::new(Core::new(config, attempt_partitions));
    let producer = Producer::from_config_and_context(
        &config.producer_config(openssl_env),
        FeedProducerContext::new(Arc::clone(&core)),
    )
    .map_err(|e| {
        KafkaFeedError::Refused(format!(
            "creating the producer: {}",
            redacted_client_error(&e)
        ))
    })?;
    let consumer_config = config.consumer_config(openssl_env);
    let lookup_timeout = consumer::committed_lookup_timeout(&consumer_config);
    let consumer =
        BaseConsumer::from_config_and_context(&consumer_config, SourceContext::default()).map_err(
            |e| {
                KafkaFeedError::Refused(format!(
                    "creating the consumer: {}",
                    redacted_client_error(&e)
                ))
            },
        )?;
    consumer
        .subscribe(&[&config.topic, &config.retry_topic])
        .map_err(|e| KafkaFeedError::Refused(format!("subscribing: {e}")))?;
    let driver = Driver::new(
        Arc::clone(&core),
        consumer,
        producer.clone(),
        lookup_timeout,
    );
    Ok((core, producer, driver))
}

impl Drop for KafkaFeed {
    fn drop(&mut self) {
        stop(&self.core);
        for (name, thread) in [
            ("gate", self.gate.take()),
            ("follower", self.follower.take()),
            ("consumer", self.thread.take()),
        ] {
            if let Some(thread) = thread
                && thread.join().is_err()
            {
                tracing::error!("the Kafka feed's {name} thread panicked");
            }
        }
    }
}

/// Set the stop flag and wake every thread waiting on `core`.
///
/// The gate checks the flag under the state lock before it waits on
/// `gate_wake`, and under the ledger's lock before it waits on the ledger's
/// condvar. Storing under the state lock, and taking the ledger's lock
/// before the notifies, keeps either check from falling between the store
/// and its notify.
pub(crate) fn stop(core: &Core) {
    {
        let _st = core.lock();
        core.stop.store(true, Ordering::Release);
    }
    drop(attempt::lock(&core.ledger));
    core.gate_wake.notify_all();
    core.ledger.1.notify_all();
}

fn gone(receipt: Receipt) -> FeedError {
    FeedError::new(
        FeedErrorKind::Gone,
        format!("{receipt} no longer names a delivery the feed holds"),
    )
}

impl Feed for KafkaFeed {
    fn backend(&self) -> &str {
        BACKEND
    }

    fn set_waker(&self, waker: FeedWaker) {
        let mut st = self.lock();
        if !st.ready.is_empty() {
            waker.wake();
        }
        st.waker = Some(waker);
    }

    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        let mut st = self.lock();
        if let Some(fatal) = &st.fatal {
            return Err(fatal.clone());
        }
        if let Some(e) = st.retryable.take() {
            return Err(e);
        }
        let mut out = Vec::new();
        while out.len() < max {
            let Some(ready) = st.ready.pop_front() else {
                break;
            };
            out.push(Delivery::new(
                ready.unit.clone(),
                ready.receipt,
                ready.deliveries,
            ));
            st.handed.insert(ready.receipt, ready);
        }
        if !out.is_empty() && !st.ready.is_empty() {
            st.wake();
        }
        Ok(out)
    }

    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        let mut st = self.lock();
        if let Some(fatal) = &st.fatal {
            return Err(fatal.clone());
        }
        if st.handed.contains_key(&receipt) && st.tracker.held(receipt).is_some() {
            return Ok(());
        }
        st.handed.remove(&receipt);
        Err(gone(receipt))
    }

    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        let tombstone = {
            let mut st = self.lock();
            if let Some(fatal) = &st.fatal {
                return Err(fatal.clone());
            }
            let Some(handed) = st.handed.remove(&receipt) else {
                return Err(gone(receipt));
            };
            let Some(position) = st.tracker.release(receipt) else {
                return Err(gone(receipt));
            };
            Tombstone::new(
                &self.core.topics.attempt,
                &handed.identity,
                handed.attempt_partition,
                position,
            )
        };
        // Sent with the lock released: a refused enqueue settles inline,
        // and settling takes the lock.
        write::send(
            &self.core,
            &self.producer,
            Box::new(Write::Tombstone(tombstone)),
        );
        Ok(())
    }

    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        let moving = {
            let mut st = self.lock();
            if let Some(fatal) = &st.fatal {
                return Err(fatal.clone());
            }
            let Some(handed) = st.handed.remove(&receipt) else {
                return Err(gone(receipt));
            };
            let Some(position) = st.tracker.release(receipt) else {
                return Err(gone(receipt));
            };
            let to = write::route(handed.deliveries, self.core.attempt_cap);
            let (topic, finish) = match to {
                Destination::Retry => (Arc::clone(&self.core.topics.retry), Finish::Complete),
                Destination::DeadLetter(_) => (
                    Arc::clone(&self.core.topics.dead_letter),
                    Finish::Tombstone(handed.identity.clone()),
                ),
            };
            Move::new(
                Outgoing::unit(
                    topic,
                    &handed.unit,
                    handed.deliveries,
                    Some(&handed.identity),
                ),
                position,
                to,
                Some(handed.unit.id),
                handed.deliveries,
                finish,
            )
        };
        match moving.to {
            Destination::Retry => tracing::debug!(
                unit = moving.unit.as_deref().unwrap_or_default(),
                deliveries = moving.deliveries,
                reason,
                "writing a failed unit to the retry topic"
            ),
            Destination::DeadLetter(_) => tracing::warn!(
                unit = moving.unit.as_deref().unwrap_or_default(),
                deliveries = moving.deliveries,
                reason,
                "writing a unit to the dead-letter topic at its attempt cap"
            ),
        }
        write::send(&self.core, &self.producer, Box::new(Write::Move(moving)));
        Ok(())
    }

    fn publish(&self, unit: Unit) -> Confirmation {
        let (confirmer, confirmation) = confirmation();
        if let Some(fatal) = &self.lock().fatal {
            confirmer.reject(fatal.clone());
            return confirmation;
        }
        let message = Outgoing::unit(Arc::clone(&self.core.topics.work), &unit, 0, None);
        write::send(
            &self.core,
            &self.producer,
            Box::new(Write::Publish { message, confirmer }),
        );
        confirmation
    }
}
