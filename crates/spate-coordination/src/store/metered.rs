//! Store decorator: per-operation deadlines + the
//! `spate_coordination_store_op_duration_seconds` histograms, applied in
//! one place instead of at thirty call sites.

use super::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchMode, WatchStream,
};
use spate_core::metrics::{CoordinationMetrics, StoreOp};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Pause between attempts while a [`RetryWindow`] is open.
const RETRY_PAUSE: Duration = Duration::from_millis(50);

/// A deadline, shared between the coordinator handle and its store, until
/// which every deadline-bounded primitive retries a Retryable failure.
/// Closed until [`open_until`](RetryWindow::open_until) is called.
#[derive(Clone, Default)]
pub(crate) struct RetryWindow(Arc<WindowState>);

#[derive(Default)]
struct WindowState {
    until: Mutex<Option<Instant>>,
    exhausted: AtomicBool,
}

impl RetryWindow {
    pub(crate) fn open_until(&self, until: Instant) {
        *self.0.until.lock().expect("retry window") = Some(until);
    }

    fn until(&self) -> Option<Instant> {
        *self.0.until.lock().expect("retry window")
    }

    /// Whether a primitive returned a Retryable failure because the window
    /// closed before it succeeded.
    pub(crate) fn exhausted(&self) -> bool {
        self.0.exhausted.load(Ordering::SeqCst)
    }
}

/// Wraps any [`CoordinationStore`] with the configured `op_timeout` on
/// every primitive but `list` (a hung store surfaces as a Retryable
/// failure the protocol already tolerates, instead of wedging the task
/// loop) and records each primitive's latency. The timeout runs on real
/// time because it bounds store I/O.
///
/// `list` is metered but **not** deadline-bounded: its duration grows
/// with the number of live keys (the NATS backend point-reads each one),
/// so a fixed per-op deadline would starve reconciliation on large jobs.
/// A dead store still fails it fast through the client's own transport
/// errors; a slow-but-alive one is paced by the reconcile interval.
#[derive(Clone)]
pub(crate) struct Metered<S> {
    inner: S,
    op_timeout: Duration,
    metrics: Option<CoordinationMetrics>,
    retry: RetryWindow,
}

impl<S> Metered<S> {
    pub(crate) fn new(
        inner: S,
        op_timeout: Duration,
        metrics: Option<CoordinationMetrics>,
    ) -> Metered<S> {
        Metered {
            inner,
            op_timeout,
            metrics,
            retry: RetryWindow::default(),
        }
    }

    /// Retry Retryable failures while `retry` is open.
    pub(crate) fn with_retry_window(mut self, retry: RetryWindow) -> Metered<S> {
        self.retry = retry;
        self
    }

    /// One attempt of `attempt` under `op_timeout`, or under what is left
    /// of an open retry window, repeated on a Retryable failure until that
    /// window closes.
    async fn timed<T, F>(
        &self,
        op: StoreOp,
        what: &str,
        attempt: impl Fn() -> F,
    ) -> Result<T, StoreError>
    where
        F: Future<Output = Result<T, StoreError>>,
    {
        loop {
            let started = Instant::now();
            let until = self.retry.until();
            let limit = until.map_or(self.op_timeout, |until| {
                self.op_timeout
                    .min(until.saturating_duration_since(started))
            });
            let out = match tokio::time::timeout(limit, attempt()).await {
                Ok(out) => out,
                Err(_) => Err(StoreError::Retryable(format!(
                    "store {what} timed out after {limit:?}"
                ))),
            };
            if let Some(m) = &self.metrics {
                m.store_op(op, started.elapsed());
            }
            if let (Err(StoreError::Retryable(_)), Some(until)) = (&out, until) {
                if Instant::now() + RETRY_PAUSE < until {
                    tokio::time::sleep(RETRY_PAUSE).await;
                    continue;
                }
                self.retry.0.exhausted.store(true, Ordering::SeqCst);
            }
            return out;
        }
    }

    async fn metered_only<T>(
        &self,
        op: StoreOp,
        fut: impl Future<Output = Result<T, StoreError>>,
    ) -> Result<T, StoreError> {
        let started = Instant::now();
        let out = fut.await;
        if let Some(m) = &self.metrics {
            m.store_op(op, started.elapsed());
        }
        out
    }
}

impl<S: CoordinationStore> CoordinationStore for Metered<S> {
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
        self.timed(StoreOp::Put, "create", || {
            self.inner.create(ks, key, value.clone())
        })
        .await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.timed(StoreOp::Put, "update", || {
            self.inner.update(ks, key, value.clone(), expected)
        })
        .await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.timed(StoreOp::Get, "get", || self.inner.get(ks, key))
            .await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.timed(StoreOp::Delete, "delete", || {
            self.inner.delete(ks, key, expected)
        })
        .await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        // The deadline covers establishment only; the stream itself lives
        // as long as the watch.
        self.timed(StoreOp::Watch, "watch", || self.inner.watch(ks, prefix))
            .await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.metered_only(StoreOp::List, self.inner.list(ks, prefix))
            .await
    }
}
