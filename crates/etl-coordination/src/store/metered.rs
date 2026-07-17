//! Store decorator: per-operation deadlines + the
//! `etl_coordination_store_op_duration_seconds` histograms, applied in
//! one place instead of at thirty call sites.

use super::{CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream};
use etl_core::metrics::{CoordinationMetrics, StoreOp};
use std::future::Future;
use std::time::{Duration, Instant};

/// Wraps any [`CoordinationStore`] with the configured `op_timeout` on
/// every single-round-trip primitive (a hung store surfaces as a
/// Retryable failure the protocol already tolerates, instead of wedging
/// the task loop) and records each primitive's round-trip latency.
///
/// `list` is metered but **not** deadline-bounded: its duration grows
/// with the number of live keys (the NATS backend point-reads each one),
/// so a fixed per-op deadline would starve reconciliation on large jobs.
/// A dead store still fails it fast through the client's own transport
/// errors; a slow-but-alive one is paced by the reconcile interval.
pub(crate) struct Metered<S> {
    inner: S,
    op_timeout: Duration,
    metrics: Option<CoordinationMetrics>,
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
        }
    }

    async fn timed<T>(
        &self,
        op: StoreOp,
        what: &str,
        fut: impl Future<Output = Result<T, StoreError>>,
    ) -> Result<T, StoreError> {
        let started = Instant::now();
        let out = match tokio::time::timeout(self.op_timeout, fut).await {
            Ok(out) => out,
            Err(_) => Err(StoreError::Retryable(format!(
                "store {what} timed out after {:?}",
                self.op_timeout
            ))),
        };
        if let Some(m) = &self.metrics {
            m.store_op(op, started.elapsed());
        }
        out
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

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.timed(StoreOp::Put, "create", self.inner.create(ks, key, value))
            .await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.timed(
            StoreOp::Put,
            "update",
            self.inner.update(ks, key, value, expected),
        )
        .await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.timed(StoreOp::Get, "get", self.inner.get(ks, key))
            .await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.timed(
            StoreOp::Delete,
            "delete",
            self.inner.delete(ks, key, expected),
        )
        .await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        // The deadline covers establishment only; the stream itself lives
        // as long as the watch.
        self.timed(StoreOp::Watch, "watch", self.inner.watch(ks, prefix))
            .await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.metered_only(StoreOp::List, self.inner.list(ks, prefix))
            .await
    }
}
