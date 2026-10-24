//! [`SqsFeed`]: the work feed over one SQS standard queue.
//!
//! Four tasks on the I/O runtime do the queue's I/O. The receive task long
//! polls while the reader wants more units than are buffered. The renewal
//! task sends each hand-back as it is queued, and extends, every third of
//! the visibility period, each unit handed out or renewed since the last
//! extension, and each unit still buffered when the reader has called the
//! feed since then; from a stop until the close, it extends every unit
//! handed out. The settle task sends deletes and fails, and the publish task
//! sends publishes, each in batches of up to ten. A close adds a task that
//! settles its confirmation. The [`Feed`] methods only lock the shared
//! state, change it and signal a task or spawn one, so none of them waits
//! (INV-2).
//!
//! # The 12-hour hold
//!
//! SQS keeps a message hidden for at most 12 hours from the receive that
//! issued its receipt handle. The feed counts that hold from the moment each
//! receive's reply arrives, less 30 seconds, and clamps every deadline and
//! extension to it. A unit whose deadline is less than one renewal period
//! away when the reader asks for it is dropped unseen and returns to the
//! queue at that deadline. A buffered unit whose extension SQS refused,
//! because its receipt no longer holds the message, is dropped unseen too.

use crate::client;
use crate::config::{BACKEND, SqsFeedConfig};
use crate::error::{CallError, Class, SqsError, code_is};
use crate::queue::{
    BatchOutcome, DeleteEntry, Queue, ReceiveRequest, Received, SendEntry, UNIT_ATTRIBUTE,
    VisibilityEntry,
};
use crate::startup::{self, COUNT_ATTRIBUTES, QueueAttributes, counts};
use spate_core::config::{FeedSection, redact};
use spate_core::feed::{
    Confirmation, Confirmer, Delivery, Depth, DepthOf, DepthReading, Feed, FeedError,
    FeedErrorKind, FeedWaker, Receipt, Unit, confirmation, depth_reading,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio::time::{Instant, MissedTickBehavior};

/// How long SQS keeps a message hidden, counted from the receive that issued
/// its receipt handle.
pub(crate) const HOLD: Duration = Duration::from_secs(12 * 60 * 60);

/// Taken off the hold for the time between SQS issuing a receipt handle and
/// the reply reaching this host.
pub(crate) const MARGIN: Duration = Duration::from_secs(30);

/// Entries per batch call.
const BATCH_ENTRIES: usize = 10;

/// The most a message, or a whole `SendMessageBatch`, may carry.
pub(crate) const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// The longest long-poll wait SQS accepts.
const MAX_WAIT_SECONDS: u64 = 20;

/// The most an extension sent during a stop may take, retries included. A
/// hand-back of a unit queues behind the extension call in flight, so this
/// bounds how long it waits.
const STOP_EXTENSION_BOUND: Duration = Duration::from_secs(1);

/// How long after its last hand-back a close waits for a receive in flight.
/// SQS answers a long poll as soon as a message is visible, so a receive
/// that takes a unit just handed back has answered by then, and the feed
/// hands that unit back again.
const RECEIVE_GRACE: Duration = Duration::from_secs(1);

const BACKOFF_START: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// How long [`SqsFeed::connect`] may take, every step included.
const CONNECT_BOUND: Duration = Duration::from_secs(30);

/// `connect` under [`CONNECT_BOUND`]. Past the bound, an
/// [`SqsError::Unavailable`] naming the step `step` holds.
async fn bounded<T>(
    step: &Mutex<&'static str>,
    connect: impl Future<Output = Result<T, SqsError>>,
) -> Result<T, SqsError> {
    match tokio::time::timeout(CONNECT_BOUND, connect).await {
        Ok(result) => result,
        Err(_) => {
            let step = *step.lock().unwrap_or_else(PoisonError::into_inner);
            Err(SqsError::Unavailable(format!(
                "connecting took longer than {CONNECT_BOUND:?}, while {step}"
            )))
        }
    }
}

/// The long-poll wait for `visibility`: at most a third of it, so a message
/// that arrives at the end of a poll still has two renewal periods left.
pub(crate) fn wait_seconds(visibility: Duration) -> i32 {
    // At most 20, so the cast cannot truncate.
    (visibility.as_secs() / 3).min(MAX_WAIT_SECONDS) as i32
}

/// When the hold of a message ends, for a receive whose reply arrived at
/// `received_at`.
pub(crate) fn hold_end(received_at: Instant) -> Instant {
    received_at + HOLD - MARGIN
}

/// The extension to send at `now` for a unit whose hold ends at `hold_end`:
/// the visibility period, or the time left if that is shorter, in whole
/// seconds. `None` when under a second is left.
pub(crate) fn extension(visibility: Duration, hold_end: Instant, now: Instant) -> Option<Duration> {
    let left = hold_end.saturating_duration_since(now);
    let ext = Duration::from_secs(visibility.min(left).as_secs());
    (ext >= Duration::from_secs(1)).then_some(ext)
}

/// A retention in seconds, or `unreported`, for the connect log line.
fn unreported(seconds: Option<u64>) -> String {
    seconds.map_or_else(|| "unreported".to_owned(), |s| s.to_string())
}

/// Whether SQS accepts `c` in a message body or attribute value.
fn sqs_legal(c: char) -> bool {
    matches!(
        c,
        '\u{9}' | '\u{A}' | '\u{D}' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..
    )
}

/// The message a unit is sent as, and its size as SQS counts it, or why SQS
/// would refuse it.
fn message_of(unit: &Unit) -> Result<(String, usize), String> {
    let body = std::str::from_utf8(&unit.body)
        .map_err(|_| "the unit's body is not UTF-8, and an SQS message body is text".to_owned())?;
    if body.is_empty() {
        return Err("the unit's body is empty, and SQS needs at least one character".to_owned());
    }
    if let Some(c) = body.chars().find(|c| !sqs_legal(*c)) {
        return Err(format!(
            "the unit's body holds {c:?}, which SQS does not accept"
        ));
    }
    if let Some(c) = unit.id.chars().find(|c| !sqs_legal(*c)) {
        return Err(format!(
            "the unit's id holds {c:?}, which SQS does not accept"
        ));
    }
    let attribute = if unit.id.is_empty() {
        0
    } else {
        UNIT_ATTRIBUTE.len() + "String".len() + unit.id.len()
    };
    let size = body.len() + attribute;
    if size > MAX_MESSAGE_BYTES {
        return Err(format!(
            "the unit is {size} bytes as a message, over SQS's {MAX_MESSAGE_BYTES}"
        ));
    }
    Ok((body.to_owned(), size))
}

/// One delivery the feed holds.
#[derive(Debug)]
struct Held {
    handle: String,
    /// The unit, until [`Feed::receive`] hands it out.
    unit: Option<Unit>,
    deliveries: u32,
    /// When SQS may hand the message to another reader. Never past
    /// `hold_end`.
    deadline: Instant,
    hold_end: Instant,
    /// Handed out or renewed since the last extension.
    marked: bool,
    /// SQS refused an extension: the receipt no longer holds the message.
    gone: bool,
}

/// A delete, a fail or a hand-back waiting to be sent.
#[derive(Debug)]
struct Settle {
    handle: String,
    /// The receipt's deadline. A retry past it is pointless.
    deadline: Instant,
}

/// A close under way.
#[derive(Debug)]
struct Closing {
    within: Duration,
    /// When the close gives up. Set from the runtime's clock by the first
    /// task that reads it, since the caller of `close` may run off the
    /// runtime, where tokio's clock is not the one the tasks read.
    bound: Option<Instant>,
    confirmer: Option<Confirmer>,
    /// Entries given up unapplied since the close began.
    dropped: usize,
}

/// A publish waiting to be sent.
#[derive(Debug)]
struct Publish {
    unit_id: String,
    body: String,
    size: usize,
    confirmer: Confirmer,
}

#[derive(Debug, Default)]
struct State {
    fatal: Option<FeedError>,
    /// A transient receive failure, returned by the next `receive` only.
    retryable: Option<FeedError>,
    abandoned: bool,
    /// Units the reader asked for and did not get at its last `receive`.
    demand: usize,
    /// The reader called `receive` or `renew` since the last tick.
    reader_active: bool,
    /// Received units not yet handed out, oldest first.
    buffered: VecDeque<u64>,
    held: HashMap<u64, Held>,
    next: u64,
    deletes: Vec<Settle>,
    fails: Vec<Settle>,
    /// Deletes and fails the settle task has taken and not finished sending.
    settling: usize,
    /// The worker is stopping: nothing more is received.
    stopping: bool,
    /// Units to set visible at once, for the renewal task to send.
    hand_backs: Vec<Settle>,
    /// Hand-backs the renewal task has taken and not finished sending.
    handing_back: usize,
    /// When the last hand-back call returned.
    last_hand_back: Option<Instant>,
    /// A receive is in flight.
    receiving: bool,
    close: Option<Closing>,
    publishes: Vec<Publish>,
    /// The batch the publish task is sending, in entry order.
    sending: Vec<Publish>,
    waker: Option<FeedWaker>,
}

impl State {
    /// Stop receiving, and queue every buffered unit to be handed back.
    /// Returns how many were queued.
    fn begin_stop(&mut self) -> usize {
        self.stopping = true;
        self.demand = 0;
        let buffered = std::mem::take(&mut self.buffered);
        let before = self.hand_backs.len();
        for receipt in buffered {
            if let Some(h) = self.held.remove(&receipt)
                && !h.gone
            {
                self.hand_backs.push(Settle {
                    handle: h.handle,
                    deadline: h.deadline,
                });
            }
        }
        self.hand_backs.len() - before
    }

    /// When the close gives up, once one has begun. Call it only on the
    /// runtime.
    fn close_bound(&mut self) -> Option<Instant> {
        let close = self.close.as_mut()?;
        let within = close.within;
        Some(*close.bound.get_or_insert_with(|| Instant::now() + within))
    }

    /// Count `n` entries given up unapplied, toward a close under way.
    fn dropped(&mut self, n: usize) {
        if let Some(close) = &mut self.close {
            close.dropped += n;
        }
    }

    /// Deletes, fails and hand-backs queued or being sent.
    fn unsent(&self) -> usize {
        self.deletes.len()
            + self.fails.len()
            + self.settling
            + self.hand_backs.len()
            + self.handing_back
    }
}

/// What the feed was built with.
#[derive(Clone, Copy, Debug)]
struct Settings {
    visibility: Duration,
    period: Duration,
    wait: i32,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    /// Signals the receive task that demand rose.
    receive: Notify,
    /// Signals the settle task that a delete or fail was queued.
    settle: Notify,
    /// Signals the publish task that a publish was queued.
    publish: Notify,
    /// Signals the renewal task that a hand-back was queued.
    hand_back: Notify,
    /// Signals a close under way that a batch was settled.
    progress: Notify,
    queue: Arc<dyn Queue>,
    /// Reads the counts a depth read reports.
    attributes: Arc<dyn QueueAttributes>,
    queue_url: String,
    dead_letter_url: String,
    settings: Settings,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Make `e` the answer to every later call, reject with it every publish
    /// still queued and a close under way, and wake the reader. The batch
    /// being sent is rejected as Retryable: SQS may have stored it.
    fn set_fatal(&self, e: &CallError) {
        let error = FeedError::new(FeedErrorKind::Fatal, e.message.clone());
        tracing::error!(error = %e.message, "the SQS feed stopped");
        let (sending, queued, close, waker) = {
            let mut st = self.lock();
            st.fatal.get_or_insert(error.clone());
            (
                std::mem::take(&mut st.sending),
                std::mem::take(&mut st.publishes),
                st.close.as_mut().and_then(|c| c.confirmer.take()),
                st.waker.clone(),
            )
        };
        if let Some(close) = close {
            close.reject(error.clone());
        }
        let unknown = FeedError::new(
            FeedErrorKind::Retryable,
            format!(
                "the feed stopped while the unit was being sent, so SQS may or may not \
                 have stored it: {}",
                e.message
            ),
        );
        for p in sending {
            p.confirmer.reject(unknown.clone());
        }
        for p in queued {
            p.confirmer.reject(error.clone());
        }
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn stopped(&self) -> bool {
        let st = self.lock();
        st.fatal.is_some() || st.abandoned
    }
}

/// The work feed over one SQS standard queue, built from a
/// `feed: { sqs: … }` section.
///
/// A unit is a message: its body is the message body, sent as text, and its
/// id travels in the String message attribute `spate.unit`. A delivery's
/// count is the message's `ApproximateReceiveCount`. The queue's redrive
/// policy decides when a failed unit moves to the dead-letter queue; the
/// feed never sends to it.
///
/// A stopping worker hands back the units it has not finished, each set
/// visible at once, and [`close`](Feed::close) sends the deletes and fails
/// still queued and gives up every unit still held within its bound.
/// Dropping the feed without a close stops its tasks and drops what is
/// still queued unsent, so those units return to the queue after their
/// visibility period.
pub struct SqsFeed {
    shared: Arc<Shared>,
    tasks: Vec<AbortHandle>,
    /// Runs each depth read.
    io: tokio::runtime::Handle,
}

impl std::fmt::Debug for SqsFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsFeed")
            .field("settings", &self.shared.settings)
            .finish_non_exhaustive()
    }
}

impl SqsFeed {
    /// Build the feed a `feed: { sqs: … }` section names, connect it, and
    /// start its tasks on `io`, the pipeline's I/O runtime.
    ///
    /// Credentials come from the AWS provider chain. Connecting reads the
    /// queue's attributes and its dead-letter queue's once, so a missing
    /// queue or a refused credential fails here. The whole call takes at
    /// most 30 seconds.
    ///
    /// # Errors
    ///
    /// [`SqsError::Config`] for a section the feed cannot read, a
    /// `queue_url` ending in `.fifo` and a `settling_window` below one
    /// minute among them.
    /// [`SqsError::Refused`] when SQS refuses the credentials or the queue,
    /// and for a queue the feed cannot dead-letter from: one whose ARN names
    /// a FIFO queue, one with no redrive policy or one the feed cannot read,
    /// or one whose dead-letter queue cannot be read or keeps messages no
    /// longer than the queue does. Also when the queue or its dead-letter
    /// queue does not report its message counts.
    /// [`SqsError::Unavailable`] when SQS does not answer in time or keeps
    /// failing transiently.
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
    /// use spate_sqs::SqsFeed;
    ///
    /// # fn run(section: &FeedSection, io: &tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>> {
    /// let feed = SqsFeed::connect(section, io)?;
    /// # Ok(()) }
    /// ```
    pub fn connect(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
    ) -> Result<SqsFeed, SqsError> {
        Self::connect_with(section, io, None)
    }

    /// [`connect`](Self::connect) with a static access key in place of the
    /// AWS provider chain, for tests against a local emulator.
    ///
    /// # Errors
    ///
    /// As [`connect`](Self::connect).
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn connect_with_static_credentials(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Result<SqsFeed, SqsError> {
        let credentials = aws_sdk_sqs::config::Credentials::new(
            access_key_id,
            secret_access_key,
            None,
            None,
            "spate-sqs-static",
        );
        Self::connect_with(
            section,
            io,
            Some(aws_sdk_sqs::config::SharedCredentialsProvider::new(
                credentials,
            )),
        )
    }

    fn connect_with(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
        credentials: Option<aws_sdk_sqs::config::SharedCredentialsProvider>,
    ) -> Result<SqsFeed, SqsError> {
        let config = SqsFeedConfig::from_section(section)?;
        let step = Mutex::new("starting");
        let connect = async {
            let queue = client::sdk_queue(&config, credentials, &step).await?;
            let checked = startup::check(&queue, &config.queue_url, &step).await?;
            Ok::<_, SqsError>((queue, checked))
        };
        // `bounded` builds its timeout inside `block_on`: the timer needs the
        // runtime's context.
        let (queue, checked) = io.block_on(bounded(&step, connect))?;
        tracing::info!(
            queue = %checked.arn,
            visibility = ?config.visibility,
            queue_visibility_timeout = checked.visibility_timeout.as_deref().unwrap_or("unreported"),
            retention_seconds = %unreported(checked.retention),
            max_receive_count = checked.max_receive_count,
            dead_letter_queue = %checked.dead_letter_arn,
            dead_letter_retention_seconds = %unreported(checked.dead_letter_retention),
            visible = checked.counts.visible,
            delayed = checked.counts.delayed,
            in_flight = checked.counts.not_visible,
            dead_letter_visible = checked.dead_letter_counts.visible,
            dead_letter_delayed = checked.dead_letter_counts.delayed,
            dead_letter_in_flight = checked.dead_letter_counts.not_visible,
            "the SQS feed is connected"
        );
        let queue = Arc::new(queue);
        Ok(Self::start(
            Arc::clone(&queue) as Arc<dyn Queue>,
            queue,
            checked.dead_letter_url,
            &config,
            io,
        ))
    }

    /// Start the feed's tasks over `queue` on `io`, reading counts through
    /// `attributes`.
    fn start(
        queue: Arc<dyn Queue>,
        attributes: Arc<dyn QueueAttributes>,
        dead_letter_url: String,
        config: &SqsFeedConfig,
        io: &tokio::runtime::Handle,
    ) -> SqsFeed {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            receive: Notify::new(),
            settle: Notify::new(),
            publish: Notify::new(),
            hand_back: Notify::new(),
            progress: Notify::new(),
            queue,
            attributes,
            queue_url: config.queue_url.clone(),
            dead_letter_url,
            settings: Settings {
                visibility: config.visibility,
                period: config.renewal_period(),
                wait: wait_seconds(config.visibility),
            },
        });
        let tasks = vec![
            io.spawn(receive_task(Arc::clone(&shared))).abort_handle(),
            io.spawn(renewal_task(Arc::clone(&shared))).abort_handle(),
            io.spawn(settle_task(Arc::clone(&shared))).abort_handle(),
            io.spawn(publish_task(Arc::clone(&shared))).abort_handle(),
        ];
        SqsFeed {
            shared,
            tasks,
            io: io.clone(),
        }
    }

    /// Stop the feed as a crashed worker would: its tasks end, and buffered
    /// units and queued deletes, fails, hand-backs and publishes are dropped
    /// unsent. Held units return to the queue at their deadlines. Afterwards
    /// [`renew`](Feed::renew) and [`hand_back`](Feed::hand_back) answer `Ok`
    /// until each deadline, then [`Gone`](FeedErrorKind::Gone);
    /// [`stop_receiving`](Feed::stop_receiving) and `hand_back` send nothing;
    /// and [`close`](Feed::close) answers `None`.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn abandon(&self) {
        for task in &self.tasks {
            task.abort();
        }
        let mut st = self.shared.lock();
        st.abandoned = true;
        let buffered = std::mem::take(&mut st.buffered);
        for receipt in buffered {
            st.held.remove(&receipt);
        }
        st.deletes.clear();
        st.fails.clear();
        st.hand_backs.clear();
        st.publishes.clear();
        st.sending.clear();
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.shared.lock()
    }

    #[cfg(test)]
    fn deadline_of(&self, receipt: Receipt) -> Option<Instant> {
        self.lock().held.get(&receipt.0).map(|h| h.deadline)
    }

    #[cfg(test)]
    fn buffered(&self) -> usize {
        self.lock().buffered.len()
    }
}

impl Drop for SqsFeed {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn gone(receipt: Receipt) -> FeedError {
    FeedError::new(
        FeedErrorKind::Gone,
        format!("{receipt} no longer holds its message"),
    )
}

impl Feed for SqsFeed {
    fn backend(&self) -> &str {
        BACKEND
    }

    fn set_waker(&self, waker: FeedWaker) {
        let mut st = self.lock();
        if !st.buffered.is_empty() {
            waker.wake();
        }
        st.waker = Some(waker);
    }

    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        let period = self.shared.settings.period;
        let (out, more, waker) = {
            let mut st = self.lock();
            st.reader_active = true;
            if let Some(fatal) = &st.fatal {
                return Err(fatal.clone());
            }
            if let Some(e) = st.retryable.take() {
                return Err(e);
            }
            if st.abandoned {
                return Ok(Vec::new());
            }
            let now = Instant::now();
            // A unit this close to its deadline would lapse before the next
            // renewal could keep it, and a gone one no longer holds its
            // message. A unit kept is marked when handed out, so
            // a tick before the reader's first renew still extends it.
            let State { buffered, held, .. } = &mut *st;
            buffered.retain(|receipt| {
                let keep = held.get(receipt).is_some_and(|h| {
                    !h.gone && h.deadline.saturating_duration_since(now) >= period
                });
                if !keep {
                    held.remove(receipt);
                    tracing::debug!(
                        receipt,
                        "dropping a received unit that is gone or too close to its deadline"
                    );
                }
                keep
            });
            let mut out = Vec::new();
            while out.len() < max {
                let Some(receipt) = st.buffered.pop_front() else {
                    break;
                };
                if let Some(h) = st.held.get_mut(&receipt)
                    && let Some(unit) = h.unit.take()
                {
                    h.marked = true;
                    out.push(Delivery::new(unit, Receipt(receipt), h.deliveries));
                }
            }
            st.demand = max - out.len();
            let more = st.demand > st.buffered.len();
            let wake = (!out.is_empty() && !st.buffered.is_empty())
                .then(|| st.waker.clone())
                .flatten();
            (out, more, wake)
        };
        if more {
            self.shared.receive.notify_one();
        }
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(out)
    }

    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        let mut st = self.lock();
        st.reader_active = true;
        if let Some(fatal) = &st.fatal {
            return Err(fatal.clone());
        }
        let now = Instant::now();
        match st.held.get_mut(&receipt.0) {
            Some(h) if h.unit.is_none() && !h.gone && now < h.deadline => {
                h.marked = true;
                Ok(())
            }
            Some(h) if h.unit.is_none() => {
                st.held.remove(&receipt.0);
                Err(gone(receipt))
            }
            _ => Err(gone(receipt)),
        }
    }

    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        {
            let mut st = self.lock();
            if let Some(fatal) = &st.fatal {
                return Err(fatal.clone());
            }
            // A lapsed receipt is acknowledged too: the reader acknowledges
            // only after the unit's last record is committed, and a delete
            // SQS refuses changes nothing.
            match st.held.get(&receipt.0) {
                Some(h) if h.unit.is_none() => {}
                _ => return Err(gone(receipt)),
            }
            let h = st.held.remove(&receipt.0).expect("checked above");
            if st.abandoned {
                return Ok(());
            }
            st.deletes.push(Settle {
                handle: h.handle,
                deadline: h.deadline,
            });
        }
        self.shared.settle.notify_one();
        Ok(())
    }

    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        {
            let mut st = self.lock();
            if let Some(fatal) = &st.fatal {
                return Err(fatal.clone());
            }
            let now = Instant::now();
            match st.held.get(&receipt.0) {
                Some(h) if h.unit.is_none() => {}
                _ => return Err(gone(receipt)),
            }
            let h = st.held.remove(&receipt.0).expect("checked above");
            // The unit returns at its deadline anyway; a fail past it would
            // reach a message another reader may hold.
            if h.gone || now >= h.deadline {
                return Err(gone(receipt));
            }
            tracing::debug!(%receipt, reason, "failing a unit back to the queue");
            if st.abandoned {
                return Ok(());
            }
            st.fails.push(Settle {
                handle: h.handle,
                deadline: h.deadline,
            });
        }
        self.shared.settle.notify_one();
        Ok(())
    }

    fn publish(&self, unit: Unit) -> Confirmation {
        let (confirmer, confirmation) = confirmation();
        let (body, size) = match message_of(&unit) {
            Ok(message) => message,
            Err(reason) => {
                confirmer.reject(FeedError::new(FeedErrorKind::Refused, reason));
                return confirmation;
            }
        };
        {
            let mut st = self.lock();
            if let Some(fatal) = &st.fatal {
                confirmer.reject(fatal.clone());
                return confirmation;
            }
            if st.abandoned {
                // The dropped confirmer reads as Retryable.
                return confirmation;
            }
            st.publishes.push(Publish {
                unit_id: unit.id,
                body,
                size,
                confirmer,
            });
        }
        self.shared.publish.notify_one();
        confirmation
    }

    fn read_depth(&self, of: DepthOf) -> Option<DepthReading> {
        let (reporter, reading) = depth_reading();
        let (url, what, unsent) = match of {
            DepthOf::DeadLetters => {
                let url = self.shared.dead_letter_url.clone();
                let what = format!("the dead-letter queue {}", redact::url(&url));
                (url, what, 0)
            }
            _ => {
                // `unsent` counts acknowledges and fails, so hand-backs stay
                // out.
                let unsent = {
                    let st = self.lock();
                    st.deletes.len() + st.fails.len() + st.settling
                };
                let url = self.shared.queue_url.clone();
                let what = format!("the queue {}", redact::url(&url));
                (url, what, unsent as u64)
            }
        };
        let attributes = Arc::clone(&self.shared.attributes);
        let shared = Arc::clone(&self.shared);
        // Detached: the reporter settles the reading, or reads as Retryable
        // when the runtime drops the task.
        drop(self.io.spawn(async move {
            let outcome = match attributes.attributes(&url, COUNT_ATTRIBUTES).await {
                Ok(read) => counts(&read)
                    .map(|c| {
                        let mut depth = Depth::new(what.as_str(), true);
                        depth.visible = c.visible;
                        depth.delayed = c.delayed;
                        depth.in_flight = c.not_visible;
                        depth.unsent = unsent;
                        depth
                    })
                    .map_err(|e| FeedError::new(FeedErrorKind::Fatal, format!("{e}: {what}"))),
                Err(e) => Err(FeedError::new(
                    match e.class {
                        Class::Fatal => FeedErrorKind::Fatal,
                        Class::Retryable => FeedErrorKind::Retryable,
                    },
                    format!("reading the counts of {what}: {}", e.message),
                )),
            };
            reporter.report(outcome);
            // The source checks a reading only when it polls, so the answer
            // ends its park.
            let waker = shared.lock().waker.clone();
            if let Some(waker) = waker {
                waker.wake();
            }
        }));
        Some(reading)
    }

    fn hands_back(&self) -> bool {
        true
    }

    /// Receive nothing more, and set each unit received and not yet handed
    /// out visible at once. A receive already waiting runs to its reply,
    /// whose units are set visible the same way.
    fn stop_receiving(&self) {
        {
            let mut st = self.lock();
            if st.fatal.is_some() || st.stopping {
                return;
            }
            let buffered = st.begin_stop();
            tracing::info!(
                buffered,
                "the SQS feed stopped receiving; handing back the units it had not handed out"
            );
        }
        self.shared.hand_back.notify_one();
    }

    /// Set a held unit visible at once. The unit has used one receive
    /// toward the queue's `maxReceiveCount`.
    ///
    /// # Errors
    ///
    /// The feed's [`Fatal`](FeedErrorKind::Fatal) error once it has stopped,
    /// and [`Gone`](FeedErrorKind::Gone) for a receipt not held, gone or
    /// past its deadline. Never [`Retryable`](FeedErrorKind::Retryable).
    fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
        {
            let mut st = self.lock();
            if let Some(fatal) = &st.fatal {
                return Err(fatal.clone());
            }
            let now = Instant::now();
            match st.held.get(&receipt.0) {
                Some(h) if h.unit.is_none() => {}
                _ => return Err(gone(receipt)),
            }
            let h = st.held.remove(&receipt.0).expect("checked above");
            // Past its deadline the message is visible already, and a
            // visibility change could reach another reader's receive.
            if h.gone || now >= h.deadline {
                return Err(gone(receipt));
            }
            tracing::debug!(%receipt, "handing back a unit");
            if st.abandoned {
                return Ok(());
            }
            st.hand_backs.push(Settle {
                handle: h.handle,
                deadline: h.deadline,
            });
        }
        self.shared.hand_back.notify_one();
        Ok(())
    }

    /// Stop receiving if no stop came first, send the deletes and fails
    /// still queued, and set every unit still held or buffered visible at
    /// once, all within `within`. With a receive in flight that may take a
    /// unit just handed back, it waits up to a second after the last
    /// hand-back for that receive, and hands back what it took. The
    /// confirmation is rejected when an entry was given up unapplied, and
    /// with the feed's Fatal error once it has stopped. `None` after the
    /// test-only `abandon`.
    fn close(&self, within: Duration) -> Option<Confirmation> {
        let (confirmer, confirmation) = confirmation();
        {
            let mut st = self.lock();
            if st.abandoned {
                return None;
            }
            if let Some(fatal) = &st.fatal {
                confirmer.reject(fatal.clone());
                return Some(confirmation);
            }
            if !st.stopping {
                st.begin_stop();
            }
            let held = std::mem::take(&mut st.held);
            for h in held.into_values().filter(|h| !h.gone) {
                st.hand_backs.push(Settle {
                    handle: h.handle,
                    deadline: h.deadline,
                });
            }
            if st.unsent() == 0 && !(st.receiving && st.last_hand_back.is_some()) {
                confirmer.confirm();
                return Some(confirmation);
            }
            st.close = Some(Closing {
                within,
                bound: None,
                confirmer: Some(confirmer),
                dropped: 0,
            });
        }
        // Detached: it ends at the bound at the latest.
        drop(self.io.spawn(close_task(Arc::clone(&self.shared))));
        self.shared.settle.notify_one();
        self.shared.hand_back.notify_one();
        Some(confirmation)
    }
}

/// Settles the close's confirmation once nothing is left to send and no
/// receive in flight may hold a unit just handed back, or at its bound.
async fn close_task(shared: Arc<Shared>) {
    let Some(bound) = shared.lock().close_bound() else {
        return;
    };
    loop {
        // Made before the state is read, so a batch settled in between
        // still ends the wait.
        let progress = shared.progress.notified();
        let wake_at = {
            // A Fatal settles the close itself, and leaves this task to end
            // at the bound with no confirmer to settle.
            let mut st = shared.lock();
            let now = Instant::now();
            let unsent = st.unsent();
            let grace = st
                .last_hand_back
                .map(|at| at + RECEIVE_GRACE)
                .filter(|end| st.receiving && now < *end);
            let wake_at = if now >= bound {
                None
            } else if unsent > 0 {
                Some(bound)
            } else {
                grace.map(|end| end.min(bound))
            };
            let Some(at) = wake_at else {
                settle_close(&mut st, unsent, grace.is_some());
                return;
            };
            at
        };
        // A timeout ends the wait as progress does: the loop reads the state
        // again.
        let _ = tokio::time::timeout_at(wake_at, progress).await;
    }
}

/// Settle the close's confirmation: confirmed when every entry was applied,
/// rejected with the count when some were given up or `unsent` remain, and
/// rejected when `receiving`: a receive still in flight may hold a unit just
/// handed back.
fn settle_close(st: &mut State, unsent: usize, receiving: bool) {
    let Some(close) = st.close.as_mut() else {
        return;
    };
    let Some(confirmer) = close.confirmer.take() else {
        return;
    };
    let dropped = close.dropped;
    let reason = if unsent > 0 && dropped > 0 {
        format!(
            "the close reached its bound with {unsent} deletes, fails or hand-backs unsent \
             and gave up {dropped} unapplied; those units return after their visibility period"
        )
    } else if unsent > 0 {
        format!(
            "the close reached its bound with {unsent} deletes, fails or hand-backs unsent; \
             those units return after their visibility period"
        )
    } else if dropped > 0 {
        format!(
            "the close gave up {dropped} of its deletes, fails and hand-backs unapplied; \
             those units return after their visibility period"
        )
    } else if receiving {
        "the close reached its bound with a receive in flight; a unit it takes returns \
         after its visibility period"
            .to_owned()
    } else {
        confirmer.confirm();
        return;
    };
    confirmer.reject(FeedError::new(FeedErrorKind::Retryable, reason));
}

/// Long polls while the reader wants more units than are buffered.
async fn receive_task(shared: Arc<Shared>) {
    let settings = shared.settings;
    // At most 12 h, so the cast cannot truncate.
    let visibility = settings.visibility.as_secs() as i32;
    let mut backoff = BACKOFF_START;
    loop {
        let want = {
            let mut st = shared.lock();
            if st.fatal.is_some() || st.abandoned {
                return;
            }
            let want = if st.stopping {
                0
            } else {
                st.demand.saturating_sub(st.buffered.len())
            };
            st.receiving = want > 0;
            want
        };
        if want == 0 {
            shared.receive.notified().await;
            continue;
        }
        let request = ReceiveRequest {
            // At most 10, so the cast cannot truncate.
            max: want.min(BATCH_ENTRIES) as i32,
            visibility,
            wait: settings.wait,
        };
        let sent_at = Instant::now();
        let received = shared.queue.receive(request).await;
        // A close waits for this receive, so it ends under the same lock
        // that queues its units.
        let ended = |st: &mut State| st.receiving = false;
        match received {
            Ok(messages) if messages.is_empty() => {
                backoff = BACKOFF_START;
                ended(&mut shared.lock());
                shared.progress.notify_one();
            }
            Ok(messages) => {
                backoff = BACKOFF_START;
                let hold_end = hold_end(Instant::now());
                let (waker, handed_back) = {
                    let mut st = shared.lock();
                    ended(&mut st);
                    // A receive cannot be withdrawn, and a reply dropped
                    // unread still hides its messages for the visibility
                    // period, so a reply that lands once the worker stops
                    // is handed back.
                    let stopping = st.stopping;
                    for m in messages {
                        let held = held_from(m, sent_at, hold_end, settings.visibility);
                        if stopping {
                            st.hand_backs.push(Settle {
                                handle: held.handle,
                                deadline: held.deadline,
                            });
                            continue;
                        }
                        let receipt = st.next;
                        st.next += 1;
                        st.held.insert(receipt, held);
                        st.buffered.push_back(receipt);
                    }
                    (st.waker.clone().filter(|_| !stopping), stopping)
                };
                if handed_back {
                    shared.hand_back.notify_one();
                }
                shared.progress.notify_one();
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
            Err(e) if e.class == Class::Fatal => {
                shared.set_fatal(&e);
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e.message, "receiving from SQS failed; retrying");
                let waker = {
                    let mut st = shared.lock();
                    ended(&mut st);
                    st.retryable = Some(FeedError::new(FeedErrorKind::Retryable, e.message));
                    st.waker.clone()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
                shared.progress.notify_one();
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

/// A received message as the feed holds it.
fn held_from(m: Received, sent_at: Instant, hold_end: Instant, visibility: Duration) -> Held {
    let deliveries = m
        .receive_count
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let id = m.unit_id.unwrap_or(m.message_id);
    Held {
        handle: m.handle,
        unit: Some(Unit::new(id, m.body.into_bytes())),
        deliveries,
        deadline: (sent_at + visibility).min(hold_end),
        hold_end,
        marked: false,
        gone: false,
    }
}

/// Sends each hand-back as it is queued. Every renewal period, extends each
/// held unit marked since the last tick, and each buffered unit when the
/// reader was active since the last tick; once the worker stops, every held
/// unit.
///
/// Hand-backs and extensions go out from this one task, so a unit is never
/// handed back while an extension of it is in flight, and is not extended
/// after its hand-back.
async fn renewal_task(shared: Arc<Shared>) {
    let settings = shared.settings;
    // No tick at start: a unit received before the first one still has its
    // whole visibility left, and extending it would only spend a call.
    let mut tick = tokio::time::interval_at(Instant::now() + settings.period, settings.period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let ticked = {
            let mut handed_back = std::pin::pin!(shared.hand_back.notified());
            std::future::poll_fn(|cx| {
                if tick.poll_tick(cx).is_ready() {
                    return Poll::Ready(true);
                }
                handed_back.as_mut().poll(cx).map(|()| false)
            })
            .await
        };
        if send_hand_backs(&shared).await.is_err() {
            return;
        }
        if !ticked {
            continue;
        }
        let now = Instant::now();
        let mut due: Vec<(u64, String, Duration)> = {
            let mut st = shared.lock();
            if st.fatal.is_some() || st.abandoned {
                return;
            }
            // A buffered unit is extended only for a reader still polling, or
            // to retry a failed extension, so a stalled worker loses it one
            // visibility period after its last extension. A stopping worker
            // keeps every unit it holds until it acknowledges or hands it
            // back.
            let active = std::mem::take(&mut st.reader_active);
            let stopping = st.stopping;
            st.held
                .iter_mut()
                .filter(|(_, h)| !h.gone && (stopping || h.marked || (active && h.unit.is_some())))
                .filter_map(|(&receipt, h)| {
                    h.marked = false;
                    if now >= h.deadline {
                        return None;
                    }
                    extension(settings.visibility, h.hold_end, now)
                        .map(|ext| (receipt, h.handle.clone(), ext))
                })
                .collect()
        };
        due.sort_by_key(|(receipt, _, _)| *receipt);
        for chunk in due.chunks(BATCH_ENTRIES) {
            // Re-read: a unit handed back or settled while an earlier chunk
            // was in flight is extended no more, and a stop that came in
            // the meantime bounds the calls left.
            let (chunk, within): (Vec<&(u64, String, Duration)>, _) = {
                let st = shared.lock();
                (
                    chunk
                        .iter()
                        .filter(|(receipt, _, _)| st.held.contains_key(receipt))
                        .collect(),
                    st.stopping.then_some(STOP_EXTENSION_BOUND),
                )
            };
            if chunk.is_empty() {
                continue;
            }
            let entries = chunk
                .iter()
                .enumerate()
                .map(|(i, (_, handle, ext))| VisibilityEntry {
                    id: i.to_string(),
                    handle: handle.clone(),
                    // At most 12 h, so the cast cannot truncate.
                    seconds: ext.as_secs() as i32,
                })
                .collect();
            let sent_at = Instant::now();
            match shared.queue.change_visibility_batch(entries, within).await {
                Ok(outcome) => {
                    let mut st = shared.lock();
                    for (i, (receipt, _, ext)) in chunk.iter().enumerate() {
                        let Some(h) = st.held.get_mut(receipt) else {
                            continue;
                        };
                        match entry_result(&outcome, i) {
                            // `ext` was cut to the hold at the tick, before
                            // the calls for earlier chunks ran.
                            Some(Ok(())) => h.deadline = (sent_at + *ext).min(h.hold_end),
                            Some(Err(failure))
                                if code_is(&failure.code, "ReceiptHandleIsInvalid")
                                    || code_is(&failure.code, "MessageNotInflight") =>
                            {
                                h.gone = true;
                            }
                            // It lapses at its own deadline.
                            Some(Err(failure)) => tracing::debug!(
                                code = %failure.code,
                                "SQS refused to extend a held unit"
                            ),
                            None => {}
                        }
                    }
                }
                Err(e) if e.class == Class::Fatal => {
                    shared.set_fatal(&e);
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e.message, "extending held units failed");
                    // Marked again, so the next tick tries them with a fresh
                    // extension.
                    let mut st = shared.lock();
                    for (receipt, _, _) in &chunk {
                        if let Some(h) = st.held.get_mut(receipt) {
                            h.marked = true;
                        }
                    }
                }
            }
            if send_hand_backs(&shared).await.is_err() {
                return;
            }
        }
    }
}

/// Send the queued hand-backs, each setting its unit visible at once, in
/// batches of up to ten. A hand-back past its unit's deadline is dropped
/// unsent, since the unit is visible already, and so is one past a close's
/// bound.
///
/// `Err` when the feed has stopped.
async fn send_hand_backs(shared: &Shared) -> Result<(), ()> {
    loop {
        let chunk: Vec<Settle> = {
            let mut st = shared.lock();
            if st.fatal.is_some() || st.abandoned {
                return Err(());
            }
            let now = Instant::now();
            st.hand_backs.retain(|s| now < s.deadline);
            if let Some(bound) = st.close_bound()
                && now >= bound
            {
                let n = std::mem::take(&mut st.hand_backs).len();
                st.dropped(n);
            }
            let n = st.hand_backs.len().min(BATCH_ENTRIES);
            st.handing_back += n;
            st.hand_backs.drain(..n).collect()
        };
        let Some(oldest) = chunk.iter().map(|s| s.deadline).min() else {
            // A close waits on what is unsent, which the drops above may
            // have emptied.
            shared.progress.notify_one();
            return Ok(());
        };
        let entries: Vec<VisibilityEntry> = chunk
            .iter()
            .enumerate()
            .map(|(i, s)| VisibilityEntry {
                id: i.to_string(),
                handle: s.handle.clone(),
                seconds: 0,
            })
            .collect();
        let sent = settle_batch(shared, "handing back", &chunk, |bound| {
            let left = until(oldest, bound).saturating_duration_since(Instant::now());
            shared
                .queue
                .change_visibility_batch(entries.clone(), Some(left))
        })
        .await;
        {
            let mut st = shared.lock();
            st.handing_back -= chunk.len();
            st.last_hand_back = Some(Instant::now());
            if let Ok(unapplied) = sent {
                st.dropped(unapplied);
            }
        }
        shared.progress.notify_one();
        sent?;
    }
}

/// The earlier of `deadline` and a close's `bound`.
fn until(deadline: Instant, bound: Option<Instant>) -> Instant {
    bound.map_or(deadline, |bound| bound.min(deadline))
}

/// The answer a batch gave entry `i`: applied, failed, or neither.
fn entry_result(
    outcome: &BatchOutcome,
    i: usize,
) -> Option<Result<(), &crate::queue::EntryFailure>> {
    let id = i.to_string();
    if outcome.successful.contains(&id) {
        return Some(Ok(()));
    }
    outcome.failed.iter().find(|f| f.id == id).map(Err)
}

/// Sends queued deletes and fails.
async fn settle_task(shared: Arc<Shared>) {
    loop {
        let (deletes, mut fails) = {
            let mut st = shared.lock();
            if st.fatal.is_some() || st.abandoned {
                return;
            }
            st.settling = st.deletes.len() + st.fails.len();
            (
                std::mem::take(&mut st.deletes),
                std::mem::take(&mut st.fails),
            )
        };
        if deletes.is_empty() && fails.is_empty() {
            shared.settle.notified().await;
            continue;
        }
        for chunk in deletes.chunks(BATCH_ENTRIES) {
            let bound = shared.lock().close_bound();
            if bound.is_some_and(|bound| Instant::now() >= bound) {
                shared.lock().dropped(chunk.len());
                continue;
            }
            let entries: Vec<DeleteEntry> = chunk
                .iter()
                .enumerate()
                .map(|(i, s)| DeleteEntry {
                    id: i.to_string(),
                    handle: s.handle.clone(),
                })
                .collect();
            // Unbounded outside a close: a delete still applies past its
            // unit's deadline.
            let sent = settle_batch(&shared, "deleting", chunk, |bound| {
                let left = bound.map(|bound| bound.saturating_duration_since(Instant::now()));
                shared.queue.delete_batch(entries.clone(), left)
            });
            match sent.await {
                Ok(unapplied) => shared.lock().dropped(unapplied),
                Err(()) => return,
            }
            shared.progress.notify_one();
        }
        while !fails.is_empty() {
            // The deletes before a fail can take until their own deadlines.
            // A fail sent past its unit's deadline could reach a message SQS
            // has handed to another reader.
            let now = Instant::now();
            fails.retain(|s| now < s.deadline);
            let bound = shared.lock().close_bound();
            if bound.is_some_and(|bound| now >= bound) {
                let n = std::mem::take(&mut fails).len();
                shared.lock().dropped(n);
                break;
            }
            let chunk: Vec<Settle> = fails.drain(..fails.len().min(BATCH_ENTRIES)).collect();
            let Some(oldest) = chunk.iter().map(|s| s.deadline).min() else {
                break;
            };
            let entries: Vec<VisibilityEntry> = chunk
                .iter()
                .enumerate()
                .map(|(i, s)| VisibilityEntry {
                    id: i.to_string(),
                    handle: s.handle.clone(),
                    seconds: 0,
                })
                .collect();
            // The SDK retries inside one call, so the call itself is bounded
            // by the earliest deadline. A request already sent when the bound
            // ends can still reach SQS after it.
            let sent = settle_batch(&shared, "failing", &chunk, |bound| {
                let left = until(oldest, bound).saturating_duration_since(Instant::now());
                shared
                    .queue
                    .change_visibility_batch(entries.clone(), Some(left))
            });
            match sent.await {
                Ok(unapplied) => shared.lock().dropped(unapplied),
                Err(()) => return,
            }
        }
        shared.lock().settling = 0;
        shared.progress.notify_one();
    }
}

/// Send one batch of deletes, fails or hand-backs, retrying a transient
/// failure until the oldest entry's deadline, or a close's bound if that
/// comes first. `call` takes the close's bound. An entry SQS fails
/// is dropped: the unit returns at its deadline, which the ack contract
/// allows.
///
/// Returns how many entries SQS did not apply, or `Err` when the feed has
/// stopped.
async fn settle_batch<F, Fut>(
    shared: &Shared,
    doing: &str,
    chunk: &[Settle],
    call: F,
) -> Result<usize, ()>
where
    F: Fn(Option<Instant>) -> Fut,
    Fut: std::future::Future<Output = Result<BatchOutcome, CallError>>,
{
    let oldest = chunk.iter().map(|s| s.deadline).min().expect("a chunk");
    let mut backoff = BACKOFF_START;
    loop {
        let bound = shared.lock().close_bound();
        match call(bound).await {
            Ok(outcome) => {
                for failure in &outcome.failed {
                    tracing::debug!(
                        code = %failure.code,
                        message = %failure.message,
                        "SQS refused {doing} a unit; it returns at its deadline"
                    );
                }
                return Ok(outcome.failed.len());
            }
            Err(e) if e.class == Class::Fatal => {
                shared.set_fatal(&e);
                return Err(());
            }
            Err(e) => {
                let bound = shared.lock().close_bound();
                if Instant::now() + backoff >= until(oldest, bound) {
                    tracing::debug!(error = %e.message, "dropping a batch {doing} units past its deadline");
                    return Ok(chunk.len());
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

/// Sends queued publishes one batch at a time, apart from the settle task,
/// so that a publish SQS keeps failing holds back no delete or fail.
async fn publish_task(shared: Arc<Shared>) {
    loop {
        let entries = {
            let mut st = shared.lock();
            if st.fatal.is_some() || st.abandoned {
                return;
            }
            let batch = next_batch(&mut st.publishes);
            let entries: Vec<SendEntry> = batch
                .iter()
                .enumerate()
                .map(|(i, p)| SendEntry {
                    id: i.to_string(),
                    body: p.body.clone(),
                    unit_id: p.unit_id.clone(),
                })
                .collect();
            st.sending = batch;
            entries
        };
        if entries.is_empty() {
            shared.publish.notified().await;
            continue;
        }
        if publish_batch(&shared, entries).await.is_err() {
            return;
        }
    }
}

/// Takes the oldest queued publishes that fit one batch: at most ten entries
/// and 1 MiB.
fn next_batch(queued: &mut Vec<Publish>) -> Vec<Publish> {
    let mut bytes = 0;
    let n = queued
        .iter()
        .take(BATCH_ENTRIES)
        .take_while(|p| {
            bytes += p.size;
            bytes <= MAX_MESSAGE_BYTES
        })
        .count();
    queued.drain(..n).collect()
}

/// Send the batch in [`State::sending`] and settle each confirmation by its
/// own entry. A transient failure of the whole call is retried until the
/// feed stops, and a Fatal anywhere in the feed rejects the batch. Each send
/// is a single attempt, so every retry runs through this loop. A Fatal
/// answering a send after a transient failure rejects the batch as
/// Retryable, since SQS may have stored an earlier attempt. A batch-shape
/// Fatal rejects it as Refused, after a transient failure too, since every
/// attempt sends the same entries.
///
/// `Err` when the feed has stopped.
async fn publish_batch(shared: &Shared, entries: Vec<SendEntry>) -> Result<(), ()> {
    let mut backoff = BACKOFF_START;
    let mut retried = false;
    loop {
        if shared.stopped() {
            return Err(());
        }
        match shared.queue.send_batch(entries.clone()).await {
            Ok(outcome) => {
                let batch = std::mem::take(&mut shared.lock().sending);
                for (i, p) in batch.into_iter().enumerate() {
                    match entry_result(&outcome, i) {
                        Some(Ok(())) => p.confirmer.confirm(),
                        Some(Err(f)) => {
                            let kind = if f.sender_fault {
                                FeedErrorKind::Refused
                            } else {
                                FeedErrorKind::Retryable
                            };
                            p.confirmer
                                .reject(FeedError::new(kind, format!("{}: {}", f.code, f.message)));
                        }
                        None => p.confirmer.reject(FeedError::new(
                            FeedErrorKind::Retryable,
                            "SQS reported neither success nor failure for the unit",
                        )),
                    }
                }
                return Ok(());
            }
            Err(e) if e.class == Class::Fatal => {
                let error = if e.batch_shape {
                    FeedError::new(FeedErrorKind::Refused, e.message.clone())
                } else if retried {
                    FeedError::new(
                        FeedErrorKind::Retryable,
                        format!(
                            "an earlier send of the unit failed, so SQS may or may not \
                             have stored it: {}",
                            e.message
                        ),
                    )
                } else {
                    FeedError::new(FeedErrorKind::Fatal, e.message.clone())
                };
                let batch = std::mem::take(&mut shared.lock().sending);
                for p in batch {
                    p.confirmer.reject(error.clone());
                }
                shared.set_fatal(&e);
                return Err(());
            }
            Err(e) => {
                tracing::warn!(error = %e.message, "publishing to SQS failed; retrying");
                retried = true;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests;
