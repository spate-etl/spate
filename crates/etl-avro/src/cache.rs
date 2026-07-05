//! The compiled-schema cache shared between pipeline threads (readers) and
//! the registry fetcher task (writer).

use apache_avro::Schema;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// A registry schema parsed once, shared by every pipeline thread.
#[derive(Debug)]
pub(crate) struct CompiledSchema {
    /// Registry id this schema was fetched under (0 for fixed schemas).
    pub(crate) id: u32,
    /// The parsed writer schema.
    pub(crate) schema: Schema,
}

/// Cache lookup result.
#[derive(Clone, Debug)]
pub(crate) enum Lookup {
    /// Schema available.
    Ready(Arc<CompiledSchema>),
    /// The registry gave a *permanent* verdict about this id (unknown id,
    /// unsupported schema, references, unparseable schema) — a poison
    /// payload, not a transient state. Transient outages never land here.
    Failed(String),
    /// Not cached: a fetch needs to happen (or is in flight).
    Missing,
}

#[derive(Clone, Debug)]
enum Entry {
    Ready(Arc<CompiledSchema>),
    /// Negative entry with its insertion time; expires after the TTL so a
    /// registry hiccup doesn't poison an id forever.
    Failed {
        reason: String,
        at: Instant,
    },
}

/// An immutable point-in-time view of the cache map.
///
/// A deserializer keeps one of these as a lock-free local memo and refreshes
/// it only when a lookup does not already have a usable answer, so the
/// steady-state repeated-id path (the overwhelmingly common case) never
/// touches the shared lock at all. See [`SchemaCache::lookup`].
#[derive(Clone, Debug)]
pub(crate) struct CacheSnapshot(Arc<HashMap<u32, Entry>>);

/// Shared schema cache.
///
/// Reads are the hot path. The map is held behind an `RwLock<Arc<..>>` in the
/// arc-swap style: writers copy-on-write (clone the map, mutate, swap the
/// `Arc` — writes are rare, only on a fetch completing), and readers clone the
/// `Arc` under one brief read lock. Hot-path callers go one step further and
/// hold a [`CacheSnapshot`] memo (see [`SchemaCache::lookup`]) so a repeated,
/// already-`Ready` id costs *zero* shared-lock acquisitions — the previous
/// design took a read lock per payload, which ping-ponged the lock's cache
/// line across pinned pipeline threads. Kept std-only on purpose (no
/// `arc-swap` dependency): a plain `RwLock<Arc<HashMap>>` is enough because
/// writes are rare and the memo removes reads from the shared path.
#[derive(Debug)]
pub(crate) struct SchemaCache {
    entries: RwLock<Arc<HashMap<u32, Entry>>>,
    negative_ttl: Duration,
}

impl SchemaCache {
    pub(crate) fn new(negative_ttl: Duration) -> Self {
        SchemaCache {
            entries: RwLock::new(Arc::new(HashMap::new())),
            negative_ttl,
        }
    }

    /// A fresh, empty memo for a newly built deserializer.
    pub(crate) fn empty_snapshot() -> CacheSnapshot {
        CacheSnapshot(Arc::new(HashMap::new()))
    }

    fn snapshot(&self) -> CacheSnapshot {
        CacheSnapshot(Arc::clone(&self.entries.read().expect("schema cache lock")))
    }

    /// Shared-lock lookup. Used off the hot path (the fetcher's dedup check).
    pub(crate) fn get(&self, id: u32) -> Lookup {
        let entries = self.entries.read().expect("schema cache lock");
        Self::eval(&entries, id, self.negative_ttl).unwrap_or(Lookup::Missing)
    }

    /// Hot-path lookup that consults the caller's local `memo` first and only
    /// takes the shared read lock when the memo has no *usable* answer.
    ///
    /// A `Ready` schema is immutable and is never downgraded (see
    /// [`Self::insert_failed`]), so a memo hit on `Ready` is always valid and
    /// needs no refresh — that is the case this optimization targets. On a
    /// miss, or a `Failed`/expired memo answer, the memo is refreshed once (a
    /// single `Arc` clone under the read lock) and re-evaluated, so a
    /// since-published schema or an expired negative entry is always observed.
    pub(crate) fn lookup(&self, memo: &mut CacheSnapshot, id: u32) -> Lookup {
        if let Some(ready @ Lookup::Ready(_)) = Self::eval(&memo.0, id, self.negative_ttl) {
            return ready;
        }
        *memo = self.snapshot();
        Self::eval(&memo.0, id, self.negative_ttl).unwrap_or(Lookup::Missing)
    }

    fn eval(map: &HashMap<u32, Entry>, id: u32, ttl: Duration) -> Option<Lookup> {
        match map.get(&id) {
            Some(Entry::Ready(schema)) => Some(Lookup::Ready(Arc::clone(schema))),
            Some(Entry::Failed { reason, at }) => {
                if at.elapsed() >= ttl {
                    // Expired: treat as absent so the caller re-fetches.
                    None
                } else {
                    Some(Lookup::Failed(reason.clone()))
                }
            }
            None => None,
        }
    }

    pub(crate) fn insert_ready(&self, schema: CompiledSchema) {
        let mut guard = self.entries.write().expect("schema cache lock");
        let mut map = (**guard).clone();
        map.insert(schema.id, Entry::Ready(Arc::new(schema)));
        *guard = Arc::new(map);
    }

    pub(crate) fn insert_failed(&self, id: u32, reason: String) {
        let mut guard = self.entries.write().expect("schema cache lock");
        // Never downgrade a compiled schema to a poison entry. The prewarm
        // task and the by-id fetcher run concurrently; a late by-id failure
        // must not clobber a `Ready` schema another path already published,
        // which would surface a valid, cached schema as unavailable for the
        // whole negative TTL.
        if matches!(guard.get(&id), Some(Entry::Ready(_))) {
            return;
        }
        let mut map = (**guard).clone();
        map.insert(
            id,
            Entry::Failed {
                reason,
                at: Instant::now(),
            },
        );
        *guard = Arc::new(map);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"T","fields":[{"name":"a","type":"long"}]}"#)
            .unwrap()
    }

    #[test]
    fn ready_and_missing() {
        let cache = SchemaCache::new(Duration::from_secs(30));
        assert!(matches!(cache.get(1), Lookup::Missing));
        cache.insert_ready(CompiledSchema {
            id: 1,
            schema: schema(),
        });
        assert!(matches!(cache.get(1), Lookup::Ready(s) if s.id == 1));
    }

    #[test]
    fn negative_entries_expire() {
        let cache = SchemaCache::new(Duration::ZERO);
        cache.insert_failed(9, "unknown id".into());
        // TTL zero: expired immediately — treated as missing to allow a
        // refetch.
        assert!(matches!(cache.get(9), Lookup::Missing));

        let cache = SchemaCache::new(Duration::from_secs(600));
        cache.insert_failed(9, "unknown id".into());
        assert!(matches!(cache.get(9), Lookup::Failed(r) if r.contains("unknown id")));
    }

    #[test]
    fn ready_is_never_downgraded_to_failed() {
        // Prewarm/fetcher race: a compiled schema must survive a concurrent
        // by-id fetch failing for the same id.
        let cache = SchemaCache::new(Duration::from_secs(600));
        cache.insert_ready(CompiledSchema {
            id: 5,
            schema: schema(),
        });
        cache.insert_failed(5, "registry returned 500".into());
        assert!(
            matches!(cache.get(5), Lookup::Ready(s) if s.id == 5),
            "a late failure must not clobber a cached Ready schema"
        );
    }

    #[test]
    fn memo_serves_ready_and_refreshes_on_non_ready() {
        let cache = SchemaCache::new(Duration::from_secs(600));
        let mut memo = SchemaCache::empty_snapshot();

        // Miss: refreshes the memo, reports Missing.
        assert!(matches!(cache.lookup(&mut memo, 1), Lookup::Missing));

        // Published after the first lookup: the stale memo must refresh.
        cache.insert_ready(CompiledSchema {
            id: 1,
            schema: schema(),
        });
        assert!(matches!(cache.lookup(&mut memo, 1), Lookup::Ready(s) if s.id == 1));
        // Repeated Ready lookups keep working (served from the memo).
        assert!(matches!(cache.lookup(&mut memo, 1), Lookup::Ready(_)));

        // A Failed memo answer must not stick once the id is published: the
        // lookup refreshes on any non-Ready memo state.
        cache.insert_failed(2, "unknown".into());
        assert!(matches!(cache.lookup(&mut memo, 2), Lookup::Failed(_)));
        cache.insert_ready(CompiledSchema {
            id: 2,
            schema: schema(),
        });
        assert!(matches!(cache.lookup(&mut memo, 2), Lookup::Ready(_)));
    }
}
