//! In-memory [`CoordinationStore`]: full protocol semantics, no
//! infrastructure.
//!
//! Backs the default-CI integration suites and single-process embedding
//! (several pipeline instances in one process sharing an
//! `Arc<MemoryStore>`), and doubles as the reference implementation for
//! custom backends. Ephemeral expiry runs via a lazily spawned sweeper
//! task against an injected [`Clock`](crate::clock::Clock). The default is
//! real wall time ([`new`](MemoryStore::new)), so realistic sub-second lease
//! tests need no clock plumbing; a frozen clock
//! ([`with_clock`](MemoryStore::with_clock)) makes expiry deterministic
//! under scheduler jitter.

use super::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use crate::clock::{Clock, SystemClock};
use futures_util::StreamExt as _;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::Instant;

/// How often the sweeper checks for expired ephemeral keys. Far below any
/// realistic lease floor; tests with 300ms+ leases stay deterministic.
const SWEEP_INTERVAL: Duration = Duration::from_millis(20);

/// Broadcast capacity for watch fan-out. Lagged watchers get a Retryable
/// stream error and re-watch, per the store contract.
const WATCH_CAPACITY: usize = 4096;

#[derive(Debug)]
struct Versioned {
    value: Vec<u8>,
    revision: Revision,
    /// Ephemeral keys: when the last write stops being live.
    deadline: Option<Instant>,
}

#[derive(Debug)]
struct Space {
    entries: Mutex<BTreeMap<String, Versioned>>,
    sequence: AtomicU64,
    watchers: broadcast::Sender<WatchEvent>,
}

impl Space {
    fn new() -> Space {
        Space {
            entries: Mutex::new(BTreeMap::new()),
            sequence: AtomicU64::new(0),
            watchers: broadcast::channel(WATCH_CAPACITY).0,
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Versioned>> {
        self.entries.lock().expect("memory store poisoned")
    }

    fn next_revision(&self) -> Revision {
        Revision(self.sequence.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

#[derive(Debug)]
struct Inner {
    durable: Space,
    ephemeral: Space,
    lease_ttl: Duration,
    sweeper_started: AtomicBool,
    /// Time source for ephemeral deadlines and expiry. `SystemClock` in
    /// production; a frozen clock in tests makes expiry deterministic.
    clock: Arc<dyn Clock>,
}

impl Inner {
    fn space(&self, ks: Keyspace) -> &Space {
        match ks {
            Keyspace::Durable => &self.durable,
            Keyspace::Ephemeral => &self.ephemeral,
        }
    }
}

/// See the [module docs](self).
#[derive(Clone, Debug)]
pub struct MemoryStore {
    inner: Arc<Inner>,
}

impl MemoryStore {
    /// A store whose ephemeral keyspace expires keys `lease_ttl` after
    /// their last write, on real wall time.
    #[must_use]
    pub fn new(lease_ttl: Duration) -> MemoryStore {
        MemoryStore::with_clock(lease_ttl, Arc::new(SystemClock))
    }

    /// Like [`new`](MemoryStore::new) but drives ephemeral expiry from an
    /// injected [`Clock`]. A frozen clock makes lease expiry deterministic
    /// under CI scheduler jitter; see [`crate::clock`].
    #[doc(hidden)]
    #[must_use]
    pub fn with_clock(lease_ttl: Duration, clock: Arc<dyn Clock>) -> MemoryStore {
        MemoryStore {
            inner: Arc::new(Inner {
                durable: Space::new(),
                ephemeral: Space::new(),
                lease_ttl,
                sweeper_started: AtomicBool::new(false),
                clock,
            }),
        }
    }

    /// Spawn the expiry sweeper on first ephemeral use. Called from async
    /// context only, so a runtime is guaranteed.
    fn ensure_sweeper(&self) {
        if self
            .inner
            .sweeper_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(SWEEP_INTERVAL).await;
                let Some(inner) = weak.upgrade() else {
                    return; // store dropped; sweeper dies with it
                };
                let now = inner.clock.now();
                // Draw and broadcast under the lock, like every other
                // write path; see `delete`.
                let mut entries = inner.ephemeral.lock();
                entries.retain(|key, v| {
                    let live = v.deadline.is_none_or(|d| d > now);
                    if !live {
                        let _ = inner.ephemeral.watchers.send(WatchEvent::Delete {
                            key: key.clone(),
                            revision: inner.ephemeral.next_revision(),
                        });
                    }
                    live
                });
                drop(entries);
            }
        });
    }

    /// Drop an ephemeral entry that expired between sweeps: reads must
    /// never observe a logically dead key. Broadcasts the deletion (the
    /// sweeper cannot, since the entry is gone before its next pass). `now` is
    /// the caller's clock snapshot, so every op in a call shares one instant.
    fn expire_in_place(
        space: &Space,
        entries: &mut BTreeMap<String, Versioned>,
        key: &str,
        now: Instant,
    ) {
        let dead = entries
            .get(key)
            .is_some_and(|v| v.deadline.is_some_and(|d| d <= now));
        if dead {
            entries.remove(key);
            let _ = space.watchers.send(WatchEvent::Delete {
                key: key.to_string(),
                revision: space.next_revision(),
            });
        }
    }

    fn deadline_for(&self, ks: Keyspace) -> Option<Instant> {
        match ks {
            Keyspace::Durable => None,
            Keyspace::Ephemeral => Some(self.inner.clock.now() + self.inner.lease_ttl),
        }
    }
}

impl CoordinationStore for MemoryStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        if ks == Keyspace::Ephemeral {
            self.ensure_sweeper();
        }
        let space = self.inner.space(ks);
        // Revision draw AND broadcast happen under the entry lock: a
        // watcher must never see this put reordered against a racing
        // write's event (the watch contract is ordered per key, and the
        // task layer trusts revisions to order puts against deletes).
        let mut entries = space.lock();
        Self::expire_in_place(space, &mut entries, key, self.inner.clock.now());
        if entries.contains_key(key) {
            return Ok(CasOutcome::Lost);
        }
        let revision = space.next_revision();
        entries.insert(
            key.to_string(),
            Versioned {
                value: value.clone(),
                revision,
                deadline: self.deadline_for(ks),
            },
        );
        let _ = space.watchers.send(WatchEvent::Put(Entry {
            key: key.to_string(),
            value,
            revision,
        }));
        Ok(CasOutcome::Won(revision))
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        if ks == Keyspace::Ephemeral {
            self.ensure_sweeper();
        }
        let space = self.inner.space(ks);
        // Broadcast under the lock; see `create`.
        let mut entries = space.lock();
        Self::expire_in_place(space, &mut entries, key, self.inner.clock.now());
        let Some(current) = entries.get_mut(key) else {
            return Ok(CasOutcome::Lost);
        };
        if current.revision != expected {
            return Ok(CasOutcome::Lost);
        }
        let revision = space.next_revision();
        current.value.clone_from(&value);
        current.revision = revision;
        current.deadline = self.deadline_for(ks);
        let _ = space.watchers.send(WatchEvent::Put(Entry {
            key: key.to_string(),
            value,
            revision,
        }));
        Ok(CasOutcome::Won(revision))
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        let space = self.inner.space(ks);
        let mut entries = space.lock();
        Self::expire_in_place(space, &mut entries, key, self.inner.clock.now());
        Ok(entries.get(key).map(|v| Entry {
            key: key.to_string(),
            value: v.value.clone(),
            revision: v.revision,
        }))
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        let space = self.inner.space(ks);
        // The deletion's own revision is drawn AND broadcast under the
        // lock: drawn outside it, a racing re-create could slot a lower
        // revision between removal and draw, and watchers would order the
        // fresh key's put BELOW this delete, reading a live lease as
        // expired.
        let mut entries = space.lock();
        Self::expire_in_place(space, &mut entries, key, self.inner.clock.now());
        match (entries.get(key), expected) {
            (None, _) => Ok(CasOutcome::Won(Revision(0))), // vacuous
            (Some(v), Some(rev)) if v.revision != rev => Ok(CasOutcome::Lost),
            (Some(v), _) => {
                let revision = v.revision;
                entries.remove(key);
                let _ = space.watchers.send(WatchEvent::Delete {
                    key: key.to_string(),
                    revision: space.next_revision(),
                });
                Ok(CasOutcome::Won(revision))
            }
        }
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        if ks == Keyspace::Ephemeral {
            self.ensure_sweeper();
        }
        let space = self.inner.space(ks);
        let prefix = prefix.to_string();
        // Subscribe under the snapshot lock so nothing lands between the
        // snapshot and the live tail (duplicates are fine, since consumers
        // dedupe by revision; gaps are not).
        let (snapshot, live) = {
            let entries = space.lock();
            let snapshot: Vec<WatchEvent> = entries
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(k, v)| {
                    WatchEvent::Put(Entry {
                        key: k.clone(),
                        value: v.value.clone(),
                        revision: v.revision,
                    })
                })
                .collect();
            (snapshot, space.watchers.subscribe())
        };
        let head = futures_util::stream::iter(
            snapshot
                .into_iter()
                .chain(std::iter::once(WatchEvent::SnapshotDone))
                .map(Ok),
        );
        let tail = futures_util::stream::unfold(live, |mut rx| async move {
            match rx.recv().await {
                Ok(event) => Some((Ok(event), rx)),
                Err(broadcast::error::RecvError::Lagged(n)) => Some((
                    Err(StoreError::Retryable(format!(
                        "watch lagged by {n} events; re-watch"
                    ))),
                    rx,
                )),
                Err(broadcast::error::RecvError::Closed) => None,
            }
        })
        .filter(move |event| {
            let keep = match event {
                Ok(WatchEvent::Put(entry)) => entry.key.starts_with(&prefix),
                Ok(WatchEvent::Delete { key, .. }) => key.starts_with(&prefix),
                Ok(WatchEvent::SnapshotDone) => false, // only ours counts
                Err(_) => true,
            };
            std::future::ready(keep)
        });
        Ok(head.chain(tail).boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        let space = self.inner.space(ks);
        let mut entries = space.lock();
        let now = self.inner.clock.now();
        let mut expired = Vec::new();
        entries.retain(|key, v| {
            let live = v.deadline.is_none_or(|d| d > now);
            if !live {
                expired.push((key.clone(), space.next_revision()));
            }
            live
        });
        for (key, revision) in expired {
            let _ = space.watchers.send(WatchEvent::Delete { key, revision });
        }
        Ok(entries
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| Entry {
                key: k.clone(),
                value: v.value.clone(),
                revision: v.revision,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_millis(120);

    fn store() -> MemoryStore {
        MemoryStore::new(TTL)
    }

    #[tokio::test]
    async fn create_is_create_if_absent_and_update_is_cas() {
        let s = store();
        let r1 = s
            .create(Keyspace::Durable, "k", b"v1".to_vec())
            .await
            .unwrap()
            .won()
            .expect("first create wins");
        assert_eq!(
            s.create(Keyspace::Durable, "k", b"dup".to_vec())
                .await
                .unwrap(),
            CasOutcome::Lost
        );
        let r2 = s
            .update(Keyspace::Durable, "k", b"v2".to_vec(), r1)
            .await
            .unwrap()
            .won()
            .expect("matched update wins");
        assert!(r2 > r1, "revisions strictly increase");
        assert_eq!(
            s.update(Keyspace::Durable, "k", b"v3".to_vec(), r1)
                .await
                .unwrap(),
            CasOutcome::Lost,
            "stale revision loses"
        );
        let entry = s.get(Keyspace::Durable, "k").await.unwrap().unwrap();
        assert_eq!(entry.value, b"v2");
        assert_eq!(entry.revision, r2);
    }

    #[tokio::test]
    async fn guarded_delete_and_vacuous_delete() {
        let s = store();
        let r = s
            .create(Keyspace::Durable, "k", b"v".to_vec())
            .await
            .unwrap()
            .won()
            .unwrap();
        assert_eq!(
            s.delete(Keyspace::Durable, "k", Some(Revision(r.0 + 9)))
                .await
                .unwrap(),
            CasOutcome::Lost
        );
        assert!(matches!(
            s.delete(Keyspace::Durable, "k", Some(r)).await.unwrap(),
            CasOutcome::Won(_)
        ));
        assert!(s.get(Keyspace::Durable, "k").await.unwrap().is_none());
        assert!(matches!(
            s.delete(Keyspace::Durable, "k", None).await.unwrap(),
            CasOutcome::Won(_)
        ));
        // A deleted key accepts a fresh create.
        assert!(
            s.create(Keyspace::Durable, "k", b"v2".to_vec())
                .await
                .unwrap()
                .won()
                .is_some()
        );
    }

    #[tokio::test]
    async fn ephemeral_writes_rearm_the_ttl_and_silence_expires() {
        let s = store();
        let mut rev = s
            .create(Keyspace::Ephemeral, "lease", b"0".to_vec())
            .await
            .unwrap()
            .won()
            .unwrap();
        // Heartbeat for 3x TTL: the key must survive.
        for _ in 0..9 {
            tokio::time::sleep(TTL / 3).await;
            rev = s
                .update(Keyspace::Ephemeral, "lease", b"hb".to_vec(), rev)
                .await
                .unwrap()
                .won()
                .expect("heartbeated lease stays alive");
        }
        // Silence: the key expires, reads as absent, and re-creates.
        tokio::time::sleep(TTL * 2).await;
        assert!(s.get(Keyspace::Ephemeral, "lease").await.unwrap().is_none());
        assert!(
            s.create(Keyspace::Ephemeral, "lease", b"1".to_vec())
                .await
                .unwrap()
                .won()
                .is_some()
        );
    }

    #[tokio::test]
    async fn watch_replays_snapshot_then_streams_live_including_expiry() {
        let s = store();
        s.create(Keyspace::Ephemeral, "split.a", b"a".to_vec())
            .await
            .unwrap()
            .won()
            .unwrap();
        let mut watch = s.watch(Keyspace::Ephemeral, "split.").await.unwrap();

        // Snapshot: the live key, then the marker.
        let first = watch.next().await.unwrap().unwrap();
        assert!(matches!(first, WatchEvent::Put(ref e) if e.key == "split.a"));
        assert_eq!(
            watch.next().await.unwrap().unwrap(),
            WatchEvent::SnapshotDone
        );

        // Live put (prefix-filtered: an off-prefix key is invisible).
        s.create(Keyspace::Ephemeral, "leader", b"x".to_vec())
            .await
            .unwrap()
            .won()
            .unwrap();
        s.create(Keyspace::Ephemeral, "split.b", b"b".to_vec())
            .await
            .unwrap()
            .won()
            .unwrap();
        let live = watch.next().await.unwrap().unwrap();
        assert!(matches!(live, WatchEvent::Put(ref e) if e.key == "split.b"));

        // Expiry of the untouched keys surfaces as deletes.
        let deadline = tokio::time::Instant::now() + TTL * 10;
        let mut deleted = std::collections::BTreeSet::new();
        while deleted.len() < 2 {
            let event = tokio::time::timeout_at(deadline, watch.next())
                .await
                .expect("expiry must surface to the watcher")
                .unwrap()
                .unwrap();
            if let WatchEvent::Delete { key, .. } = event {
                deleted.insert(key);
            }
        }
        assert!(deleted.contains("split.a") && deleted.contains("split.b"));
    }

    #[tokio::test]
    async fn list_is_a_live_snapshot() {
        let s = store();
        s.create(Keyspace::Durable, "split.a", b"a".to_vec())
            .await
            .unwrap()
            .won()
            .unwrap();
        s.create(Keyspace::Durable, "plan", b"p".to_vec())
            .await
            .unwrap()
            .won()
            .unwrap();
        let listed = s.list(Keyspace::Durable, "split.").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].key, "split.a");
    }
}
