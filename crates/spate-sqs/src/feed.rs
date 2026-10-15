//! [`SqsFeed`]: the work feed over one SQS standard queue.
//!
//! Four tasks on the I/O runtime do the queue's I/O. The receive task long
//! polls while the reader wants more units than are buffered. The renewal
//! task extends, every third of the visibility period, each held unit handed
//! out or renewed since the last extension. The settle task sends deletes and
//! fails, and the publish task sends publishes, each in batches of up to
//! ten. The [`Feed`] methods only lock the shared state, change it and
//! signal a task, so none of them waits (INV-2).
//!
//! # The 12-hour hold
//!
//! SQS keeps a message hidden for at most 12 hours from the receive that
//! issued its receipt handle. The feed counts that hold from the moment each
//! receive's reply arrives, less 30 seconds, and clamps every deadline and
//! extension to it. A unit whose deadline is less than one renewal period
//! away when the reader asks for it is dropped unseen and returns to the
//! queue at that deadline.

use crate::client;
use crate::config::{BACKEND, SqsFeedConfig};
use crate::error::{CallError, Class, SqsError, code_is};
use crate::queue::{
    BatchOutcome, DeleteEntry, Queue, ReceiveRequest, Received, SendEntry, UNIT_ATTRIBUTE,
    VisibilityEntry,
};
use crate::startup;
use spate_core::config::FeedSection;
use spate_core::feed::{
    Confirmation, Confirmer, Delivery, Feed, FeedError, FeedErrorKind, FeedWaker, Receipt, Unit,
    confirmation,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
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

/// A delete or a fail waiting to be sent.
#[derive(Debug)]
struct Settle {
    handle: String,
    /// The receipt's deadline. A retry past it is pointless.
    deadline: Instant,
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
    /// Received units not yet handed out, oldest first.
    buffered: VecDeque<u64>,
    held: HashMap<u64, Held>,
    next: u64,
    deletes: Vec<Settle>,
    fails: Vec<Settle>,
    publishes: Vec<Publish>,
    /// The batch the publish task is sending, in entry order.
    sending: Vec<Publish>,
    waker: Option<FeedWaker>,
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
    queue: Arc<dyn Queue>,
    settings: Settings,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Make `e` the answer to every later call, reject with it every publish
    /// still queued, and wake the reader. The batch being sent is rejected
    /// as Retryable: SQS may have stored it.
    fn set_fatal(&self, e: &CallError) {
        let error = FeedError::new(FeedErrorKind::Fatal, e.message.clone());
        tracing::error!(error = %e.message, "the SQS feed stopped");
        let (sending, queued, waker) = {
            let mut st = self.lock();
            st.fatal.get_or_insert(error.clone());
            (
                std::mem::take(&mut st.sending),
                std::mem::take(&mut st.publishes),
                st.waker.clone(),
            )
        };
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
/// Dropping the feed stops its tasks. Deletes and fails still queued are
/// dropped unsent, so those units return to the queue after their
/// visibility period.
pub struct SqsFeed {
    shared: Arc<Shared>,
    tasks: Vec<AbortHandle>,
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
    /// `queue_url` ending in `.fifo` among them.
    /// [`SqsError::Refused`] when SQS refuses the credentials or the queue,
    /// and for a queue the feed cannot dead-letter from: one whose ARN names
    /// a FIFO queue, one with no redrive policy or one the feed cannot read,
    /// or one whose dead-letter queue cannot be read or keeps messages no
    /// longer than the queue does.
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
            "the SQS feed is connected"
        );
        Ok(Self::start(Arc::new(queue), &config, io))
    }

    /// Start the feed's tasks over `queue` on `io`.
    fn start(
        queue: Arc<dyn Queue>,
        config: &SqsFeedConfig,
        io: &tokio::runtime::Handle,
    ) -> SqsFeed {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            receive: Notify::new(),
            settle: Notify::new(),
            publish: Notify::new(),
            queue,
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
        SqsFeed { shared, tasks }
    }

    /// Stop the feed as a crashed worker would: its tasks end, and buffered
    /// units and queued deletes, fails and publishes are dropped unsent.
    /// Held units return to the queue at their deadlines. Afterwards
    /// [`renew`](Feed::renew) answers `Ok` until each deadline, then
    /// [`Gone`](FeedErrorKind::Gone).
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
            // renewal could keep it. A unit kept is marked when handed out, so
            // a tick before the reader's first renew still extends it.
            let State { buffered, held, .. } = &mut *st;
            buffered.retain(|receipt| {
                let keep = held
                    .get(receipt)
                    .is_some_and(|h| h.deadline.saturating_duration_since(now) >= period);
                if !keep {
                    held.remove(receipt);
                    tracing::debug!(
                        receipt,
                        "dropping a received unit too close to its deadline"
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
}

/// Long polls while the reader wants more units than are buffered.
async fn receive_task(shared: Arc<Shared>) {
    let settings = shared.settings;
    // At most 12 h, so the cast cannot truncate.
    let visibility = settings.visibility.as_secs() as i32;
    let mut backoff = BACKOFF_START;
    loop {
        let want = {
            let st = shared.lock();
            if st.fatal.is_some() || st.abandoned {
                return;
            }
            st.demand.saturating_sub(st.buffered.len())
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
        match shared.queue.receive(request).await {
            Ok(messages) => {
                backoff = BACKOFF_START;
                if messages.is_empty() {
                    continue;
                }
                let hold_end = hold_end(Instant::now());
                let waker = {
                    let mut st = shared.lock();
                    for m in messages {
                        let receipt = st.next;
                        st.next += 1;
                        let held = held_from(m, sent_at, hold_end, settings.visibility);
                        st.held.insert(receipt, held);
                        st.buffered.push_back(receipt);
                    }
                    st.waker.clone()
                };
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
                    st.retryable = Some(FeedError::new(FeedErrorKind::Retryable, e.message));
                    st.waker.clone()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
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

/// Every renewal period, extends each held unit marked since the last tick.
async fn renewal_task(shared: Arc<Shared>) {
    let settings = shared.settings;
    let mut tick = tokio::time::interval(settings.period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let now = Instant::now();
        let mut due: Vec<(u64, String, Duration)> = {
            let mut st = shared.lock();
            if st.fatal.is_some() || st.abandoned {
                return;
            }
            st.held
                .iter_mut()
                .filter(|(_, h)| h.marked && !h.gone && h.unit.is_none())
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
            match shared.queue.change_visibility_batch(entries, None).await {
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
                    for (receipt, _, _) in chunk {
                        if let Some(h) = st.held.get_mut(receipt) {
                            h.marked = true;
                        }
                    }
                }
            }
        }
    }
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
            let entries: Vec<DeleteEntry> = chunk
                .iter()
                .enumerate()
                .map(|(i, s)| DeleteEntry {
                    id: i.to_string(),
                    handle: s.handle.clone(),
                })
                .collect();
            let sent = settle_batch(&shared, "deleting", chunk, || {
                shared.queue.delete_batch(entries.clone())
            });
            if sent.await.is_err() {
                return;
            }
        }
        while !fails.is_empty() {
            // The deletes before a fail can take until their own deadlines.
            // A fail sent past its unit's deadline could reach a message SQS
            // has handed to another reader.
            let now = Instant::now();
            fails.retain(|s| now < s.deadline);
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
            let sent = settle_batch(&shared, "failing", &chunk, || {
                let left = oldest.saturating_duration_since(Instant::now());
                shared
                    .queue
                    .change_visibility_batch(entries.clone(), Some(left))
            });
            if sent.await.is_err() {
                return;
            }
        }
    }
}

/// Send one batch of deletes or fails, retrying a transient failure until
/// the oldest entry's deadline. An entry SQS fails is dropped: the unit
/// returns at its deadline, which the ack contract allows.
///
/// `Err` when the feed has stopped.
async fn settle_batch<F, Fut>(
    shared: &Shared,
    doing: &str,
    chunk: &[Settle],
    call: F,
) -> Result<(), ()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<BatchOutcome, CallError>>,
{
    let oldest = chunk.iter().map(|s| s.deadline).min();
    let mut backoff = BACKOFF_START;
    loop {
        match call().await {
            Ok(outcome) => {
                for failure in &outcome.failed {
                    tracing::debug!(
                        code = %failure.code,
                        message = %failure.message,
                        "SQS refused {doing} a unit; it returns at its deadline"
                    );
                }
                return Ok(());
            }
            Err(e) if e.class == Class::Fatal => {
                shared.set_fatal(&e);
                return Err(());
            }
            Err(e) => {
                if oldest.is_none_or(|oldest| Instant::now() + backoff >= oldest) {
                    tracing::debug!(error = %e.message, "dropping a batch {doing} units past its deadline");
                    return Ok(());
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
