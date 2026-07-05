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
    /// The registry said this id is unusable (unknown id, unsupported
    /// schema, references) — a poison payload, not a transient state.
    Failed(String),
    /// Not cached: a fetch needs to happen (or is in flight).
    Missing,
}

#[derive(Debug)]
enum Entry {
    Ready(Arc<CompiledSchema>),
    /// Negative entry with its insertion time; expires after the TTL so a
    /// registry hiccup doesn't poison an id forever.
    Failed {
        reason: String,
        at: Instant,
    },
}

/// Shared schema cache. Reads are the hot path: one brief `RwLock` read
/// guard per cache miss *check* — the deserializer holds the returned
/// `Arc<CompiledSchema>` for the whole payload, so the guard scope is a
/// map lookup only.
#[derive(Debug)]
pub(crate) struct SchemaCache {
    entries: RwLock<HashMap<u32, Entry>>,
    negative_ttl: Duration,
}

impl SchemaCache {
    pub(crate) fn new(negative_ttl: Duration) -> Self {
        SchemaCache {
            entries: RwLock::new(HashMap::new()),
            negative_ttl,
        }
    }

    pub(crate) fn get(&self, id: u32) -> Lookup {
        let entries = self.entries.read().expect("schema cache lock");
        match entries.get(&id) {
            Some(Entry::Ready(schema)) => Lookup::Ready(Arc::clone(schema)),
            Some(Entry::Failed { reason, at }) => {
                if at.elapsed() >= self.negative_ttl {
                    // Expired: report Missing so the caller re-fetches.
                    Lookup::Missing
                } else {
                    Lookup::Failed(reason.clone())
                }
            }
            None => Lookup::Missing,
        }
    }

    pub(crate) fn insert_ready(&self, schema: CompiledSchema) {
        let mut entries = self.entries.write().expect("schema cache lock");
        entries.insert(schema.id, Entry::Ready(Arc::new(schema)));
    }

    pub(crate) fn insert_failed(&self, id: u32, reason: String) {
        let mut entries = self.entries.write().expect("schema cache lock");
        entries.insert(
            id,
            Entry::Failed {
                reason,
                at: Instant::now(),
            },
        );
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
}
