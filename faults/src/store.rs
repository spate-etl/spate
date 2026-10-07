//! The coordination-store wrapper a worker runs under: it journals every
//! durable `split.*` write with its reply and every durable `split.*` entry
//! it reads.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt as _;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_core::metrics::CoordinationMetrics;

use crate::journal::{Event, Journal, Progress, Reply, Source, WriteOp};

const SPLIT_PREFIX: &str = "split.";

/// Forwards every call to `S` and journals the durable `split.*` traffic.
///
/// A write's `send` line is appended before the call and its `done` line
/// after it. A call dropped before it returns, as at an `op_timeout`, still
/// appends `done: cancelled`; a SIGKILL leaves the `send` without a `done`.
#[derive(Clone, Debug)]
pub struct JournalStore<S> {
    inner: S,
    journal: Arc<Journal>,
    calls: Arc<AtomicU64>,
}

impl<S> JournalStore<S> {
    /// Wraps `inner`, appending to `journal`.
    pub fn new(inner: S, journal: Arc<Journal>) -> JournalStore<S> {
        JournalStore {
            inner,
            journal,
            calls: Arc::new(AtomicU64::new(0)),
        }
    }

    fn seen(&self, entry: &Entry, from: Source) {
        if !entry.key.starts_with(SPLIT_PREFIX) {
            return;
        }
        match Progress::parse(&entry.value) {
            Ok(value) => record(
                &self.journal,
                Event::Seen {
                    key: entry.key.clone(),
                    rev: entry.revision.0,
                    value,
                    from,
                },
            ),
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
                value,
            },
        );
        Some(Pending {
            journal: &self.journal,
            call,
            key: key.to_owned(),
            done: false,
        })
    }
}

/// Appends `done: cancelled` for its call unless [`Pending::finish`] ran.
struct Pending<'a> {
    journal: &'a Journal,
    call: u64,
    key: String,
    done: bool,
}

impl Pending<'_> {
    fn finish(mut self, result: &Result<CasOutcome, StoreError>) {
        let reply = match result {
            Ok(CasOutcome::Won(rev)) => Reply::Won(rev.0),
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
        let entry = self.inner.get(ks, key).await?;
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

#[cfg(test)]
mod tests;
