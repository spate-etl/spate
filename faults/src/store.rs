//! The coordination-store wrappers a worker runs under: one journals every
//! durable `split.*` write with its reply and every durable `split.*` entry
//! it reads, one injects an in-process fault at a chosen write, one stops the
//! process at a chosen commit, and one re-sends a commit that lost its CAS.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt as _;
use serde::{Deserialize, Serialize};
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_core::metrics::CoordinationMetrics;

use crate::classify::{Classifier, WriteKind, classify_ephemeral};
use crate::journal::{AbortPoint, Event, Journal, Progress, Reply, Source, WriteOp};

const SPLIT_PREFIX: &str = "split.";

/// Forwards every call to `S` and journals the durable `split.*` traffic.
///
/// A write's `send` line is appended before the call and its `done` line
/// after it; a write dropped before it returns, as at an `op_timeout`, still
/// appends `done: cancelled`, and a SIGKILL leaves the `send` without a `done`.
/// A `get` of a split that fails or is dropped before it returns appends
/// `read_failed`.
/// Every value journalled as `seen` or landed by a `won` write is also taught
/// to the classifier.
#[derive(Clone, Debug)]
pub struct JournalStore<S> {
    inner: S,
    journal: Arc<Journal>,
    classifier: Arc<Classifier>,
    calls: Arc<AtomicU64>,
}

impl<S> JournalStore<S> {
    /// Wraps `inner`, appending to `journal` and teaching `classifier`.
    pub fn new(inner: S, journal: Arc<Journal>, classifier: Arc<Classifier>) -> JournalStore<S> {
        JournalStore {
            inner,
            journal,
            classifier,
            calls: Arc::new(AtomicU64::new(0)),
        }
    }

    fn seen(&self, entry: &Entry, from: Source) {
        if !entry.key.starts_with(SPLIT_PREFIX) {
            return;
        }
        match Progress::parse(&entry.value) {
            Ok(value) => {
                self.classifier
                    .learn(&entry.key, entry.revision.0, value.clone());
                record(
                    &self.journal,
                    Event::Seen {
                        key: entry.key.clone(),
                        rev: entry.revision.0,
                        value,
                        from,
                    },
                );
            }
            Err(e) => eprintln!("not journalled: {} at {}: {e}", entry.key, entry.revision.0),
        }
    }

    /// Appends the `send` line for a durable `split.*` write, and returns the
    /// guard that appends its `done`.
    fn send(
        &self,
        ks: Keyspace,
        op: WriteOp,
        key: &str,
        value: &[u8],
        expected: Option<u64>,
    ) -> Option<Pending<'_>> {
        if ks != Keyspace::Durable || !key.starts_with(SPLIT_PREFIX) {
            return None;
        }
        let value = match Progress::parse(value) {
            Ok(value) => value,
            Err(e) => {
                eprintln!("not journalled: a write to {key}: {e}");
                return None;
            }
        };
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        record(
            &self.journal,
            Event::Send {
                call,
                op,
                key: key.to_owned(),
                expected,
                value: value.clone(),
            },
        );
        Some(Pending {
            journal: &self.journal,
            classifier: &self.classifier,
            call,
            key: key.to_owned(),
            value,
            done: false,
        })
    }
}

/// Appends `done: cancelled` for its call unless [`Pending::finish`] ran.
struct Pending<'a> {
    journal: &'a Journal,
    classifier: &'a Classifier,
    call: u64,
    key: String,
    value: Progress,
    done: bool,
}

impl Pending<'_> {
    fn finish(mut self, result: &Result<CasOutcome, StoreError>) {
        let reply = match result {
            Ok(CasOutcome::Won(rev)) => {
                self.classifier.learn(&self.key, rev.0, self.value.clone());
                Reply::Won(rev.0)
            }
            Ok(CasOutcome::Lost) => Reply::Lost,
            Err(StoreError::Retryable(_)) => Reply::Err("retryable".to_owned()),
            Err(_) => Reply::Err("fatal".to_owned()),
        };
        self.append(reply);
        self.done = true;
    }

    fn append(&self, reply: Reply) {
        record(
            self.journal,
            Event::Done {
                call: self.call,
                key: self.key.clone(),
                reply,
            },
        );
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.append(Reply::Cancelled);
        }
    }
}

/// Appends `read_failed` for its key unless the read returned `Ok`.
struct ReadGuard<'a> {
    journal: &'a Journal,
    key: Option<&'a str>,
    ok: bool,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        if !self.ok
            && let Some(key) = self.key
        {
            record(
                self.journal,
                Event::ReadFailed {
                    key: key.to_owned(),
                },
            );
        }
    }
}

/// Appends `event`, or exits the process with status 3 when the journal
/// cannot be written.
pub fn record(journal: &Journal, event: Event) {
    if let Err(e) = journal.append(event) {
        eprintln!("journal write failed: {e}");
        std::process::exit(3);
    }
}

impl<S: CoordinationStore + Clone> CoordinationStore for JournalStore<S> {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }

    fn op_timeout(&self) -> Option<Duration> {
        self.inner.op_timeout()
    }

    fn attach_metrics(&self, metrics: &CoordinationMetrics) {
        self.inner.attach_metrics(metrics);
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        let pending = self.send(ks, WriteOp::Create, key, &value, None);
        let result = self.inner.create(ks, key, value).await;
        if let Some(pending) = pending {
            pending.finish(&result);
        }
        result
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        let pending = self.send(ks, WriteOp::Update, key, &value, Some(expected.0));
        let result = self.inner.update(ks, key, value, expected).await;
        if let Some(pending) = pending {
            pending.finish(&result);
        }
        result
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        let mut guard = ReadGuard {
            journal: &self.journal,
            key: (ks == Keyspace::Durable && key.starts_with(SPLIT_PREFIX)).then_some(key),
            ok: false,
        };
        let entry = self.inner.get(ks, key).await?;
        guard.ok = true;
        drop(guard);
        if ks == Keyspace::Durable
            && let Some(entry) = &entry
        {
            self.seen(entry, Source::Get);
        }
        Ok(entry)
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        let inner = self.inner.watch(ks, prefix).await?;
        if ks != Keyspace::Durable {
            return Ok(inner);
        }
        let this = self.clone();
        Ok(inner
            .inspect(move |event| {
                if let Ok(WatchEvent::Put(entry)) = event {
                    this.seen(entry, Source::Watch);
                }
            })
            .boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        let entries = self.inner.list(ks, prefix).await?;
        if ks == Keyspace::Durable {
            for entry in &entries {
                self.seen(entry, Source::List);
            }
        }
        Ok(entries)
    }
}

/// What an [`AbortAt`] does at its write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbortMode {
    /// Aborts the process before sending the `n`th write.
    Before,
    /// Aborts the process once a write from the `n`th on lands.
    After,
    /// Once a write from the `n`th on lands, hands its caller
    /// [`StoreError::Retryable`] in place of the `Won` reply.
    ErrAfterLand,
}

/// An in-process fault at the `n`th write of one kind, counting from 1. An
/// [`AbortMode::ErrAfterLand`] plan on [`WriteKind::Commit`] also counts a
/// [`WriteKind::Complete`], so a process whose splits each finish in one
/// write still reaches it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortPlan {
    /// The kind of write counted.
    pub kind: WriteKind,
    /// The write's ordinal among writes of `kind`.
    pub n: u32,
    /// What happens there.
    pub mode: AbortMode,
}

impl AbortPlan {
    fn counts(&self, kind: Option<WriteKind>) -> bool {
        let Some(kind) = kind else {
            return false;
        };
        kind == self.kind
            || (self.mode == AbortMode::ErrAfterLand
                && self.kind == WriteKind::Commit
                && kind == WriteKind::Complete)
    }
}

impl std::fmt::Display for AbortPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = match self.mode {
            AbortMode::Before => "abort before",
            AbortMode::After => "abort after",
            AbortMode::ErrAfterLand => "err_after_land on",
        };
        write!(f, "{mode} {:?} {}", self.kind, self.n)
    }
}

/// Counts one process's writes of the plan's kind and decides when it fires.
/// It fires at most once.
#[derive(Debug)]
struct Trigger {
    plan: AbortPlan,
    count: AtomicU32,
    fired: AtomicBool,
}

impl Trigger {
    fn new(plan: AbortPlan) -> Trigger {
        Trigger {
            plan,
            count: AtomicU32::new(0),
            fired: AtomicBool::new(false),
        }
    }

    /// Counts a write of `kind`, and returns its ordinal when the plan may
    /// fire on it: at the `n`th for [`AbortMode::Before`], and from the `n`th
    /// on, until it fires, for the modes that wait for a landed write.
    fn arm(&self, kind: Option<WriteKind>) -> Option<u32> {
        if !self.plan.counts(kind) || self.fired.load(Ordering::SeqCst) {
            return None;
        }
        let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        let armed = match self.plan.mode {
            AbortMode::Before => n == self.plan.n,
            AbortMode::After | AbortMode::ErrAfterLand => n >= self.plan.n,
        };
        armed.then_some(n)
    }

    /// Marks the plan fired, and returns whether this call did it.
    fn fire(&self) -> bool {
        !self.fired.swap(true, Ordering::SeqCst)
    }
}

/// Forwards every call to `S` and applies one [`AbortPlan`] to the updates
/// it classifies through the shared [`Classifier`].
///
/// A durable `split.*` update is classified against the value at its expected
/// revision, and an ephemeral `split.*` update is a [`WriteKind::Renew`]. An
/// abort appends its `abort` line first and an `ErrAfterLand` its
/// `err_after_land` line, so it sits outside the [`JournalStore`] whose
/// classifier it reads.
#[derive(Clone, Debug)]
pub struct AbortAt<S> {
    inner: S,
    journal: Arc<Journal>,
    classifier: Arc<Classifier>,
    trigger: Arc<Trigger>,
}

impl<S> AbortAt<S> {
    /// Wraps `inner`, applying `plan`.
    pub fn new(
        inner: S,
        plan: AbortPlan,
        journal: Arc<Journal>,
        classifier: Arc<Classifier>,
    ) -> AbortAt<S> {
        AbortAt {
            inner,
            journal,
            classifier,
            trigger: Arc::new(Trigger::new(plan)),
        }
    }

    fn kind(&self, ks: Keyspace, key: &str, value: &[u8], expected: Revision) -> Option<WriteKind> {
        match ks {
            Keyspace::Durable if key.starts_with(SPLIT_PREFIX) => {
                let next = Progress::parse(value).ok()?;
                self.classifier.classify(key, expected.0, &next)
            }
            Keyspace::Ephemeral => classify_ephemeral(key),
            Keyspace::Durable => None,
        }
    }

    fn abort(&self, key: &str, n: u32, at: AbortPoint) -> ! {
        record(
            &self.journal,
            Event::Abort {
                key: key.to_owned(),
                kind: self.trigger.plan.kind,
                n,
                at,
            },
        );
        std::process::abort()
    }
}

impl<S: CoordinationStore + Clone> CoordinationStore for AbortAt<S> {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }

    fn op_timeout(&self) -> Option<Duration> {
        self.inner.op_timeout()
    }

    fn attach_metrics(&self, metrics: &CoordinationMetrics) {
        self.inner.attach_metrics(metrics);
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        let Some(n) = self.trigger.arm(self.kind(ks, key, &value, expected)) else {
            return self.inner.update(ks, key, value, expected).await;
        };
        if self.trigger.plan.mode == AbortMode::Before && self.trigger.fire() {
            self.abort(key, n, AbortPoint::Before);
        }
        let result = self.inner.update(ks, key, value, expected).await;
        let Ok(CasOutcome::Won(rev)) = result else {
            return result;
        };
        if !self.trigger.fire() {
            return result;
        }
        match self.trigger.plan.mode {
            AbortMode::Before => result,
            AbortMode::After => self.abort(key, n, AbortPoint::After),
            AbortMode::ErrAfterLand => {
                record(
                    &self.journal,
                    Event::ErrAfterLand {
                        key: key.to_owned(),
                        rev: rev.0,
                    },
                );
                Err(StoreError::Retryable(
                    "injected: the write landed and its reply was lost".to_owned(),
                ))
            }
        }
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// The write at which a worker stops itself: the `n`th of `kind`, counting
/// from 1. A plan on [`WriteKind::Commit`] also counts a
/// [`WriteKind::Complete`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopPlan {
    /// The kind of write counted.
    pub kind: WriteKind,
    /// The write's ordinal among writes of `kind`.
    pub n: u32,
}

impl StopPlan {
    fn counts(self, kind: Option<WriteKind>) -> bool {
        kind == Some(self.kind)
            || (self.kind == WriteKind::Commit && kind == Some(WriteKind::Complete))
    }
}

impl std::fmt::Display for StopPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stop at {:?} {}", self.kind, self.n)
    }
}

/// The durable key whose next lost update a [`BrokenFence`] re-sends.
#[derive(Debug, Default)]
pub struct Fence {
    armed: std::sync::Mutex<Option<String>>,
}

impl Fence {
    fn arm(&self, key: &str) {
        *self.lock() = Some(key.to_owned());
    }

    /// Disarms the fence and returns whether it was armed for `key`.
    fn take(&self, key: &str) -> bool {
        let mut armed = self.lock();
        if armed.as_deref() == Some(key) {
            *armed = None;
            return true;
        }
        false
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// How many times a [`BrokenFence`] re-sends a lost update.
const RESENDS: u32 = 8;

/// Forwards every call to `S`, except that a durable update on the key its
/// [`Fence`] is armed for, once it loses its CAS, is read back and re-sent
/// unchanged at the current revision until it lands, up to eight times.
/// The fence fires once.
#[derive(Clone, Debug)]
pub struct BrokenFence<S> {
    inner: S,
    fence: Arc<Fence>,
}

impl<S> BrokenFence<S> {
    /// Wraps `inner`, re-sending on the key `fence` is armed for.
    pub fn new(inner: S, fence: Arc<Fence>) -> BrokenFence<S> {
        BrokenFence { inner, fence }
    }
}

impl<S: CoordinationStore + Clone> CoordinationStore for BrokenFence<S> {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }

    fn op_timeout(&self) -> Option<Duration> {
        self.inner.op_timeout()
    }

    fn attach_metrics(&self, metrics: &CoordinationMetrics) {
        self.inner.attach_metrics(metrics);
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        // Boxed, so this layer adds a pointer to the caller's future and not
        // the inner futures, which overflow the I/O thread's stack in debug
        // builds.
        if ks != Keyspace::Durable || !self.fence.take(key) {
            return Box::pin(self.inner.update(ks, key, value, expected)).await;
        }
        let mut result = Box::pin(self.inner.update(ks, key, value.clone(), expected)).await;
        for _ in 0..RESENDS {
            if !matches!(result, Ok(CasOutcome::Lost)) {
                break;
            }
            let Some(current) = Box::pin(self.inner.get(ks, key)).await? else {
                break;
            };
            result = Box::pin(self.inner.update(ks, key, value.clone(), current.revision)).await;
        }
        result
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// Stops the whole process with SIGSTOP, from the calling thread.
fn raise_stop() {
    #[cfg(unix)]
    // Thread-directed, so the calling thread stops before it runs anything
    // more. Do not change this to `kill(getpid(), SIGSTOP)`: a
    // process-directed stop can let this thread run on briefly first.
    // SAFETY: `raise` takes no pointers.
    unsafe {
        libc::raise(libc::SIGSTOP);
    }
    #[cfg(not(unix))]
    unimplemented!("stopping a worker needs SIGSTOP");
}

/// Forwards every call to `S`, and stops the process at the write its
/// [`StopPlan`] names, before that write is sent.
///
/// The stop runs when the `update` future is built, not when it is polled.
/// Under the coordinator's per-call timeout, which starts on the first poll,
/// the resumed write therefore gets a whole `op_timeout`. Before stopping it
/// appends a `stop` line and arms its [`Fence`], when it has one.
#[derive(Clone, Debug)]
pub struct StopAt<S> {
    inner: S,
    plan: Option<StopPlan>,
    fence: Option<Arc<Fence>>,
    journal: Arc<Journal>,
    classifier: Arc<Classifier>,
    count: Arc<AtomicU32>,
    stop: fn(),
}

impl<S> StopAt<S> {
    /// Wraps `inner`, stopping at `plan` when there is one and arming
    /// `fence` there.
    pub fn new(
        inner: S,
        plan: Option<StopPlan>,
        fence: Option<Arc<Fence>>,
        journal: Arc<Journal>,
        classifier: Arc<Classifier>,
    ) -> StopAt<S> {
        StopAt {
            inner,
            plan,
            fence,
            journal,
            classifier,
            count: Arc::new(AtomicU32::new(0)),
            stop: raise_stop,
        }
    }

    /// Counts a write the plan counts, and returns its epoch when it is the
    /// one to stop at.
    fn due(&self, ks: Keyspace, key: &str, value: &[u8], expected: Revision) -> Option<u64> {
        let plan = self.plan?;
        if ks != Keyspace::Durable || !key.starts_with(SPLIT_PREFIX) {
            return None;
        }
        let next = Progress::parse(value).ok()?;
        if !plan.counts(self.classifier.classify(key, expected.0, &next)) {
            return None;
        }
        let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        (n == plan.n).then_some(next.epoch)
    }
}

impl<S: CoordinationStore + Clone> CoordinationStore for StopAt<S> {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }

    fn op_timeout(&self) -> Option<Duration> {
        self.inner.op_timeout()
    }

    fn attach_metrics(&self, metrics: &CoordinationMetrics) {
        self.inner.attach_metrics(metrics);
    }

    fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> impl Future<Output = Result<CasOutcome, StoreError>> + Send {
        self.inner.create(ks, key, value)
    }

    // Not an `async fn`: the stop must happen before the caller's timeout
    // exists, which a body that runs on the first poll would not do.
    fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> impl Future<Output = Result<CasOutcome, StoreError>> + Send {
        if let Some(epoch) = self.due(ks, key, &value, expected) {
            record(
                &self.journal,
                Event::Stop {
                    key: key.to_owned(),
                    expected: expected.0,
                    epoch,
                },
            );
            if let Some(fence) = &self.fence {
                fence.arm(key);
            }
            (self.stop)();
        }
        self.inner.update(ks, key, value, expected)
    }

    fn get(
        &self,
        ks: Keyspace,
        key: &str,
    ) -> impl Future<Output = Result<Option<Entry>, StoreError>> + Send {
        self.inner.get(ks, key)
    }

    fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> impl Future<Output = Result<CasOutcome, StoreError>> + Send {
        self.inner.delete(ks, key, expected)
    }

    fn watch(
        &self,
        ks: Keyspace,
        prefix: &str,
    ) -> impl Future<Output = Result<WatchStream, StoreError>> + Send {
        self.inner.watch(ks, prefix)
    }

    fn list(
        &self,
        ks: Keyspace,
        prefix: &str,
    ) -> impl Future<Output = Result<Vec<Entry>, StoreError>> + Send {
        self.inner.list(ks, prefix)
    }
}

#[cfg(test)]
mod tests;
