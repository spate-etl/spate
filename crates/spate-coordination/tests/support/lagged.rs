//! [`LaggedStore`]: a store whose watch delivers every change a fixed time
//! after it happens, as a store with a slow notification path does.

use futures_util::StreamExt as _;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchMode, WatchStream,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Wraps `S` and delivers each watch event `lag` after `S` does, in order.
/// Every other operation goes straight to `S`.
#[derive(Clone)]
pub struct LaggedStore<S> {
    inner: S,
    lag: Duration,
}

impl<S> LaggedStore<S> {
    pub fn new(inner: S, lag: Duration) -> LaggedStore<S> {
        LaggedStore { inner, lag }
    }
}

impl<S: CoordinationStore + Clone> CoordinationStore for LaggedStore<S> {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
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
        self.inner.update(ks, key, value, expected).await
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

    /// The inner watch is drained by a task of its own, which stamps each
    /// event on arrival, so the lag does not grow with the event rate.
    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        let mut inner = self.inner.watch(ks, prefix).await?;
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = inner.next().await {
                if tx.send((Instant::now(), event)).is_err() {
                    return;
                }
            }
        });
        let lag = self.lag;
        let lagged = futures_util::stream::unfold(rx, move |mut rx| async move {
            let (at, event) = rx.recv().await?;
            tokio::time::sleep_until(at + lag).await;
            Some((event, rx))
        });
        Ok(lagged.boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}
