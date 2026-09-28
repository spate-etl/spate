//! [`PolledStore`]: a store whose watch discovers changes by listing at an
//! interval, as a store with no push notifications does.

use super::{Backend, LEASE, store};
use futures_util::StreamExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The highest revision a handle has seen for each key, by any operation.
type HighWater = Arc<Mutex<HashMap<(Keyspace, String), Revision>>>;

/// Wraps `S` and replaces its watch with a listing diff every `interval`.
///
/// A key written and removed between two listings is never reported, and
/// several writes to one key between listings arrive as one `Put`. A key that
/// vanishes is reported as a `Delete` one above the highest revision this
/// handle saw for it, through the watch, a read, a listing or its own write.
/// Every other operation goes straight to `S`. Clones share the high-water.
#[derive(Clone)]
pub struct PolledStore<S> {
    inner: S,
    interval: Duration,
    high: HighWater,
}

impl<S> PolledStore<S> {
    pub fn new(inner: S, interval: Duration) -> PolledStore<S> {
        PolledStore {
            inner,
            interval,
            high: HighWater::default(),
        }
    }

    fn saw(&self, ks: Keyspace, key: &str, revision: Revision) {
        let mut high = self.high.lock().expect("high-water");
        let held = high.entry((ks, key.to_string())).or_insert(revision);
        *held = (*held).max(revision);
    }
}

/// The events that move a watch from `seen` to `listed`; `seen` becomes
/// `listed`. A vanished key's delete sits one above the larger of its last
/// delivered revision and `high(key)`.
pub fn diff(
    seen: &mut BTreeMap<String, Revision>,
    listed: Vec<Entry>,
    high: impl Fn(&str) -> Option<Revision>,
) -> Vec<WatchEvent> {
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
            let top = high(key).map_or(*revision, |held| held.max(*revision));
            events.push(WatchEvent::Delete {
                key: key.clone(),
                revision: Revision(top.0 + 1),
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
        let outcome = self.inner.create(ks, key, value).await?;
        if let CasOutcome::Won(revision) = outcome {
            self.saw(ks, key, revision);
        }
        Ok(outcome)
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        let outcome = self.inner.update(ks, key, value, expected).await?;
        if let CasOutcome::Won(revision) = outcome {
            self.saw(ks, key, revision);
        }
        Ok(outcome)
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        let entry = self.inner.get(ks, key).await?;
        if let Some(entry) = &entry {
            self.saw(ks, key, entry.revision);
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
        let snapshot = self.list(ks, prefix).await?;
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
        let state = (self.clone(), prefix.to_string(), seen);
        let interval = self.interval;
        let tail =
            futures_util::stream::unfold(state, move |(store, prefix, mut seen)| async move {
                loop {
                    tokio::time::sleep(interval).await;
                    match store.inner.list(ks, &prefix).await {
                        Ok(listed) => {
                            let events = {
                                let high = store.high.lock().expect("high-water");
                                diff(&mut seen, listed, |key| {
                                    high.get(&(ks, key.to_string())).copied()
                                })
                            };
                            for event in &events {
                                if let WatchEvent::Put(entry) = event {
                                    store.saw(ks, &entry.key, entry.revision);
                                }
                            }
                            if !events.is_empty() {
                                let events: Vec<_> = events.into_iter().map(Ok).collect();
                                return Some((events, (store, prefix, seen)));
                            }
                        }
                        Err(e) => return Some((vec![Err(e)], (store, prefix, seen))),
                    }
                }
            })
            .flat_map(futures_util::stream::iter);
        Ok(head.chain(tail).boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        let entries = self.inner.list(ks, prefix).await?;
        for entry in &entries {
            self.saw(ks, &entry.key, entry.revision);
        }
        Ok(entries)
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
