//! [`PolledStore`]: a store whose watch discovers changes by listing at an
//! interval, as a store with no push notifications does.

use super::{Backend, LEASE, store};
use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use std::collections::BTreeMap;
use std::time::Duration;

/// Wraps `S` and replaces its watch with a listing diff every `interval`.
///
/// A key written and removed between two listings is never reported, and
/// several writes to one key between listings arrive as one `Put`. A key that
/// vanishes is reported as a `Delete` one revision above the last `Put` this
/// watch delivered for it, which is above every revision the key held when
/// `S` numbers revisions from one sequence per keyspace, as [`MemoryStore`]
/// does. Every other operation goes straight to `S`.
#[derive(Clone)]
pub struct PolledStore<S> {
    inner: S,
    interval: Duration,
}

impl<S> PolledStore<S> {
    pub fn new(inner: S, interval: Duration) -> PolledStore<S> {
        PolledStore { inner, interval }
    }
}

/// The events that move a watch from `seen` to `listed`; `seen` becomes `listed`.
pub fn diff(seen: &mut BTreeMap<String, Revision>, listed: Vec<Entry>) -> Vec<WatchEvent> {
    let mut events = Vec::new();
    let mut live = BTreeMap::new();
    for entry in listed {
        live.insert(entry.key.clone(), entry.revision);
        if seen.get(&entry.key) != Some(&entry.revision) {
            events.push(WatchEvent::Put(entry));
        }
    }
    for (key, revision) in seen.iter() {
        if !live.contains_key(key) {
            events.push(WatchEvent::Delete {
                key: key.clone(),
                revision: Revision(revision.0 + 1),
            });
        }
    }
    *seen = live;
    events
}

impl<S: CoordinationStore + Clone> CoordinationStore for PolledStore<S> {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
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

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        let snapshot = self.inner.list(ks, prefix).await?;
        let seen: BTreeMap<String, Revision> = snapshot
            .iter()
            .map(|entry| (entry.key.clone(), entry.revision))
            .collect();
        let head = futures_util::stream::iter(
            snapshot
                .into_iter()
                .map(WatchEvent::Put)
                .chain(std::iter::once(WatchEvent::SnapshotDone))
                .map(Ok),
        );
        let state = (self.inner.clone(), prefix.to_string(), seen);
        let interval = self.interval;
        let tail =
            futures_util::stream::unfold(state, move |(inner, prefix, mut seen)| async move {
                loop {
                    tokio::time::sleep(interval).await;
                    match inner.list(ks, &prefix).await {
                        Ok(listed) => {
                            let events = diff(&mut seen, listed);
                            if !events.is_empty() {
                                let events: Vec<_> = events.into_iter().map(Ok).collect();
                                return Some((events, (inner, prefix, seen)));
                            }
                        }
                        Err(e) => return Some((vec![Err(e)], (inner, prefix, seen))),
                    }
                }
            })
            .flat_map(futures_util::stream::iter);
        Ok(head.chain(tail).boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// One [`MemoryStore`] namespace; each worker watches it through its own
/// [`PolledStore`].
pub struct PolledBackend {
    inner: MemoryStore,
    interval: Duration,
}

impl PolledBackend {
    /// Each worker's watch lists the store every tenth of [`LEASE`].
    pub fn new() -> PolledBackend {
        PolledBackend {
            inner: store(),
            interval: LEASE / 10,
        }
    }
}

impl Backend for PolledBackend {
    type Store = PolledStore<MemoryStore>;

    fn store(&self) -> PolledStore<MemoryStore> {
        PolledStore::new(self.inner.clone(), self.interval)
    }

    fn lease(&self) -> Duration {
        LEASE
    }
}
