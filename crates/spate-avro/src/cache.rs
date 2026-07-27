//! The compiled-schema cache shared between pipeline threads (readers) and
//! the registry fetcher task (writer).

use apache_avro::Schema;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// A registry schema parsed once, shared by every pipeline thread.
///
/// The compiled form is a `Result` rather than a hard failure so that a
/// schema the parser rejects can be *negatively cached* instead of retried
/// per record: the stored reason is surfaced per record as
/// `SchemaUnavailable`. That reason string is fully rendered here, once, so
/// the per-record path only clones it (never re-formats).
#[derive(Debug)]
pub(crate) struct CompiledSchema {
    /// Registry id this schema was fetched under (0 for fixed schemas).
    pub(crate) id: u32,
    /// The parsed writer schema, or why the parser rejected it.
    pub(crate) schema: Result<Schema, String>,
}

impl CompiledSchema {
    /// Compile a schema. `json` must be the schema's *original* JSON source —
    /// never a canonical form, which strips logical types and defaults.
    ///
    /// apache-avro 0.21 *panics* rather than erroring on some malformed names
    /// (e.g. a record named `"my-record"`). The panic is caught and stored as
    /// an ordinary failure: a poison schema arriving from the registry must
    /// negatively cache, not unwind the pipeline thread that happened to
    /// touch it first. (The caught panic prints a backtrace to stderr; that
    /// is harmless.)
    pub(crate) fn compile(id: u32, json: &str) -> Self {
        let schema = match std::panic::catch_unwind(|| Schema::parse_str(json)) {
            Ok(Ok(schema)) => Ok(schema),
            Ok(Err(e)) => Err(format!("schema {id} is not usable: {e}")),
            Err(_panic) => Err(format!("schema {id} is not usable: the parser panicked")),
        };
        CompiledSchema { id, schema }
    }

    /// `None` when the schema compiled (the entry is `Ready` material);
    /// otherwise the reason — the negative-cache payload for an unusable
    /// schema.
    pub(crate) fn unusable_reason(&self) -> Option<String> {
        match &self.schema {
            Ok(_) => None,
            Err(reason) => Some(reason.clone()),
        }
    }
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

    const SCHEMA_JSON: &str =
        r#"{"type":"record","name":"T","fields":[{"name":"a","type":"long"}]}"#;

    /// apache-avro 0.21 *panics* rather than erroring on the dashed record
    /// name — the case `compile` has to catch.
    const PARSER_PANICS_JSON: &str =
        r#"{"type":"record","name":"my-record","fields":[{"name":"a","type":"long"}]}"#;

    fn compiled(id: u32) -> CompiledSchema {
        CompiledSchema::compile(id, SCHEMA_JSON)
    }

    #[test]
    fn well_formed_schema_compiles() {
        let entry = compiled(4);
        assert!(entry.schema.is_ok());
        assert!(entry.unusable_reason().is_none());
    }

    #[test]
    fn a_parser_panic_becomes_an_ordinary_failure() {
        // The point of the `catch_unwind` in `compile`: a poison schema from
        // the registry must negatively cache, not unwind whichever pipeline
        // thread happened to touch it first.
        let entry = CompiledSchema::compile(6, PARSER_PANICS_JSON);
        assert!(
            matches!(&entry.schema, Err(reason) if reason.contains("panicked")),
            "{:?}",
            entry.schema.as_ref().err()
        );
        assert!(entry.unusable_reason().is_some());
    }

    #[test]
    fn an_unparseable_schema_is_unusable() {
        let entry = CompiledSchema::compile(8, "not a schema");
        let reason = entry.unusable_reason().expect("junk JSON is not a schema");
        assert!(reason.contains("schema 8 is not usable"), "{reason}");
    }

    #[test]
    fn ready_and_missing() {
        let cache = SchemaCache::new(Duration::from_secs(30));
        assert!(matches!(cache.get(1), Lookup::Missing));
        cache.insert_ready(compiled(1));
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
        cache.insert_ready(compiled(5));
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
        cache.insert_ready(compiled(1));
        assert!(matches!(cache.lookup(&mut memo, 1), Lookup::Ready(s) if s.id == 1));
        // Repeated Ready lookups keep working (served from the memo).
        assert!(matches!(cache.lookup(&mut memo, 1), Lookup::Ready(_)));

        // A Failed memo answer must not stick once the id is published: the
        // lookup refreshes on any non-Ready memo state.
        cache.insert_failed(2, "unknown".into());
        assert!(matches!(cache.lookup(&mut memo, 2), Lookup::Failed(_)));
        cache.insert_ready(compiled(2));
        assert!(matches!(cache.lookup(&mut memo, 2), Lookup::Ready(_)));
    }
}
