//! The compiled-schema cache shared between pipeline threads (readers) and
//! the registry fetcher task (writer).

use apache_avro::Schema;
use spate_core::clock::Clock;
use spate_core::clock::SyncClock;
use spate_core::hash::QualityMap;
use spate_core::telemetry::RateLimit;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    /// The single-pass datum path's decode spec: the named-type index its
    /// walk resolves `Schema::Ref` through, or the pre-rendered reason the
    /// schema cannot datum-decode. Gated per *path*, not per entry: a
    /// schema the datum path rejects (e.g. a `duration` logical type)
    /// stays fully usable on the Value/serde paths; only the datum
    /// deserializer surfaces the stored reason, per record. A datum-only
    /// failure therefore never negative-caches the id
    /// ([`Self::unusable_reason`] stays schema-based).
    pub(crate) datum: Result<crate::de::Names, String>,
    /// An estimate of the most bytes this schema holds, while it is parsed or
    /// after, from [`weigh`]. Zero for a schema the parser rejected.
    pub(crate) weight: usize,
    /// Whether a lookup has served this schema since the cache last
    /// considered evicting it.
    used: AtomicBool,
}

/// Bytes a compiled schema holds for each byte of its JSON, for each copy of
/// it. The largest ratio measured over flat, nested, enum, attribute, alias
/// and default-heavy schemas was 16.
const BYTES_PER_SOURCE_BYTE: usize = 20;

/// An estimate of the bytes a schema of `source_len` JSON bytes holds once
/// compiled, when `cloned` of those bytes are held again: `cloned` comes from
/// [`cloned_bytes`].
fn weigh(source_len: usize, cloned: usize) -> usize {
    source_len
        .saturating_add(cloned)
        .saturating_mul(BYTES_PER_SOURCE_BYTE)
}

/// The JSON bytes of a schema that the parser holds more than once. It keeps
/// a clone of each named type, with everything inside it, under its name and
/// under each alias, until the parse ends, and the datum index then holds
/// one per name. So a named type counts its own length once per name, and a
/// type nested inside others counts again inside each of them. Read from the
/// JSON so that a schema can be weighed before the parser clones it.
fn cloned_bytes(schema: &serde_json::Value) -> usize {
    cloned_within(schema, json_len(schema))
}

/// [`cloned_bytes`] for a part of a schema whose whole JSON is `whole` long.
fn cloned_within(schema: &serde_json::Value, whole: usize) -> usize {
    use serde_json::Value;
    let each = |parts: Option<&Value>| match parts {
        Some(Value::Array(parts)) => parts.iter().map(|part| cloned_within(part, whole)).sum(),
        Some(part) => cloned_within(part, whole),
        None => 0,
    };
    // A record, enum or fixed written without its body: where a type of that
    // name was defined earlier, the parser puts a clone of it here, and its
    // length is unknown at this point.
    let named = |object: &serde_json::Map<String, Value>, body: &str| {
        if !object.contains_key(body) {
            return whole;
        }
        let aliases = object.get("aliases").and_then(Value::as_array);
        json_len(schema).saturating_mul(1 + aliases.map_or(0, Vec::len))
    };
    match schema {
        Value::Array(_) => each(Some(schema)),
        Value::Object(object) => match object.get("type") {
            Some(Value::String(kind)) => match kind.as_str() {
                "record" | "error" => {
                    let fields = object.get("fields").and_then(Value::as_array);
                    let inside: usize = fields
                        .into_iter()
                        .flatten()
                        .map(|field| each(field.get("type")))
                        .sum();
                    named(object, "fields").saturating_add(inside)
                }
                "enum" => named(object, "symbols"),
                "fixed" => named(object, "size"),
                "array" => each(object.get("items")),
                "map" => each(object.get("values")),
                _ => 0,
            },
            nested => each(nested),
        },
        _ => 0,
    }
}

/// The length of `value` written as compact JSON, without string escapes.
fn json_len(value: &serde_json::Value) -> usize {
    use serde_json::Value;
    // Two brackets and a comma between entries.
    let framed = |entries: usize, inside: usize| 2 + entries.saturating_sub(1) + inside;
    match value {
        Value::Null => 4,
        Value::Bool(flag) => {
            if *flag {
                4
            } else {
                5
            }
        }
        Value::Number(number) => number.to_string().len(),
        Value::String(text) => text.len() + 2,
        Value::Array(items) => framed(items.len(), items.iter().map(json_len).sum()),
        Value::Object(object) => framed(
            object.len(),
            object
                .iter()
                .map(|(key, value)| key.len() + 3 + json_len(value))
                .sum(),
        ),
    }
}

/// [`weigh`] for the schema `json`, or `None` when `json` is not JSON.
fn weight_of(json: &str) -> Option<usize> {
    let schema: serde_json::Value = serde_json::from_str(json).ok()?;
    Some(weigh(json.len(), cloned_bytes(&schema)))
}

/// The weight the cache gives the schema `json`, or `None` when `json` is not
/// JSON.
#[cfg(feature = "testing")]
#[doc(hidden)]
#[must_use]
pub fn schema_weight(json: &str) -> Option<usize> {
    weight_of(json)
}

/// The JSON of a record nesting `depth` records, each with `width` long
/// fields beside the next record. Decodes from `depth * width + 1` zero
/// bytes.
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
#[must_use]
pub fn nested_schema(depth: usize, width: usize) -> String {
    let mut inner = String::from(r#""long""#);
    for level in (0..depth).rev() {
        let pad: Vec<String> = (0..width)
            .map(|f| format!(r#"{{"name":"p{f}","type":"long"}},"#))
            .collect();
        inner = format!(
            r#"{{"type":"record","name":"L{level}","fields":[{}{{"name":"c","type":{inner}}}]}}"#,
            pad.concat()
        );
    }
    inner
}

impl CompiledSchema {
    /// Compile a schema. `json` must be the schema's *original* JSON source,
    /// never a canonical form, which strips logical types and defaults.
    ///
    /// A schema the parser rejects becomes an ordinary failure whether it
    /// errors or panics: a poison schema arriving from the registry must
    /// negatively cache, not unwind the pipeline thread that happened to
    /// touch it first. The `catch_unwind` covers the panicking half; do not
    /// remove it. (A caught panic prints a backtrace to stderr; that is
    /// harmless.)
    pub(crate) fn compile(id: u32, json: &str) -> Self {
        Self::compile_within(id, json, usize::MAX)
    }

    /// [`Self::compile`] for a schema the cache will hold. A schema whose
    /// weight is above [`MAX_READY_BYTES`] is unusable, and the reason says
    /// so. The check runs before the parser, which clones a nested named type
    /// once per level, so a refused schema costs its JSON and no more.
    pub(crate) fn compile_for_cache(id: u32, json: &str) -> Self {
        Self::compile_within(id, json, MAX_READY_BYTES)
    }

    fn compile_within(id: u32, json: &str, limit: usize) -> Self {
        let weight = weight_of(json).unwrap_or(0);
        if weight > limit {
            let reason = format!(
                "schema {id} is not usable: it would hold about {} MiB compiled, above the {} \
                 MiB the schema cache holds",
                weight >> 20,
                limit >> 20
            );
            return CompiledSchema {
                id,
                schema: Err(reason.clone()),
                datum: Err(reason),
                weight,
                used: AtomicBool::new(false),
            };
        }
        let compiled = std::panic::catch_unwind(|| match Schema::parse_str(json) {
            Ok(parsed) => {
                let datum = crate::de::compile_spec(&parsed).map_err(|reason| {
                    format!("schema {id} is not usable on the datum path: {reason}")
                });
                (Ok(parsed), datum)
            }
            Err(e) => {
                let reason = format!("schema {id} is not usable: {e}");
                (Err(reason.clone()), Err(reason))
            }
        });
        let (schema, datum) = compiled.unwrap_or_else(|_panic| {
            let reason = format!("schema {id} is not usable: the parser panicked");
            (Err(reason.clone()), Err(reason))
        });
        let weight = if schema.is_ok() { weight } else { 0 };
        CompiledSchema {
            id,
            schema,
            datum,
            weight,
            used: AtomicBool::new(false),
        }
    }

    /// `None` when the schema compiled (the entry is `Ready` material);
    /// otherwise the reason, which is the negative-cache payload for an
    /// unusable schema.
    pub(crate) fn unusable_reason(&self) -> Option<String> {
        match &self.schema {
            Ok(_) => None,
            Err(reason) => Some(reason.clone()),
        }
    }

    /// Marks the schema as in use. The store is skipped while the mark is
    /// set, so pipeline threads decoding one schema share its cache line
    /// read-only between sweeps.
    #[inline]
    fn touch(&self) {
        if !self.used.load(Ordering::Relaxed) {
            self.used.store(true, Ordering::Relaxed);
        }
    }
}

/// Cache lookup result.
#[derive(Clone, Debug)]
pub(crate) enum Lookup {
    /// Schema available.
    Ready(Arc<CompiledSchema>),
    /// The registry gave a *permanent* verdict about this id (unknown id,
    /// unsupported schema, references, unparseable schema), which is a poison
    /// payload rather than a transient state. Transient outages never land here.
    Failed(String),
    /// Not cached: a fetch needs to happen (or is in flight).
    Missing,
}

#[derive(Clone, Debug)]
enum Entry {
    Ready(ReadyEntry),
    /// Negative entry with its insertion time; expires after the TTL so a
    /// registry hiccup doesn't poison an id forever.
    Failed {
        reason: String,
        at: Instant,
    },
}

/// A compiled schema with its place in the eviction order.
#[derive(Clone, Debug)]
struct ReadyEntry {
    schema: Arc<CompiledSchema>,
    /// Position in the eviction queue: the lowest is considered first. An
    /// entry found in use moves to the back.
    queued: u64,
    /// The cache's insert count when this entry was inserted.
    born: u64,
}

/// Negative entries the cache holds at most. Schema ids are wire data, so
/// without a bound a producer naming distinct unknown ids grows the map that
/// every write clones under the lock. A full cache drops its oldest negative
/// entry, which costs at most one more registry fetch for that id.
const MAX_NEGATIVE_ENTRIES: usize = 1024;

/// Compiled schemas the cache holds at most. Schema ids are wire data, so
/// without a bound a producer naming distinct ids the registry resolves grows
/// the cache for the life of the process. An evicted schema is fetched again
/// on its next record.
pub(crate) const MAX_READY_ENTRIES: usize = 1024;

/// The summed weight of the compiled schemas the cache holds at most, in
/// bytes. A count alone would let a few large schemas hold what a thousand
/// ordinary ones do.
pub(crate) const MAX_READY_BYTES: usize = 64 << 20;

/// The most recently inserted compiled schemas the cache evicts last. A
/// record blocked on a fetch replays after the schema lands, and until then
/// nothing has marked the schema as in use.
const PROTECTED_NEWEST: u64 = 256;

/// An immutable point-in-time view of the cache map.
///
/// A deserializer keeps one of these as a lock-free local memo and refreshes
/// it when a lookup does not already have a usable answer, or when the cache
/// has evicted a compiled schema since the memo was taken. The steady-state
/// repeated-id path (the overwhelmingly common case) never touches the shared
/// lock at all. See [`SchemaCache::lookup`].
#[derive(Clone, Debug)]
pub(crate) struct CacheSnapshot {
    map: Arc<QualityMap<u32, Entry>>,
    /// The cache's eviction count when `map` was read.
    evictions: u64,
}

impl CacheSnapshot {
    /// The cache's eviction count when this snapshot was taken.
    pub(crate) fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Whether this snapshot's entry for `schema`'s id is `schema` itself.
    pub(crate) fn holds(&self, schema: &Arc<CompiledSchema>) -> bool {
        matches!(
            self.map.get(&schema.id),
            Some(Entry::Ready(entry)) if Arc::ptr_eq(&entry.schema, schema)
        )
    }
}

/// The map and the counters a writer advances with it.
#[derive(Debug)]
struct Shared {
    map: Arc<QualityMap<u32, Entry>>,
    /// Compiled schemas inserted so far.
    inserts: u64,
    /// The last eviction-queue position handed out.
    queue: u64,
}

/// Shared schema cache.
///
/// Reads are the hot path. The map is held behind an `RwLock<Arc<..>>` in the
/// arc-swap style: writers copy-on-write (clone the map, mutate, swap the
/// `Arc`; writes are rare, only on a fetch completing), and readers clone the
/// `Arc` under one brief read lock. Hot-path callers go one step further and
/// hold a [`CacheSnapshot`] memo (see [`SchemaCache::lookup`]) so a repeated,
/// already-`Ready` id costs *zero* shared-lock acquisitions. A read lock per
/// payload would ping-pong the lock's cache line across pinned pipeline
/// threads. Kept std-only on purpose (no
/// `arc-swap` dependency): a plain `RwLock<Arc<..>>` is enough because
/// writes are rare and the memo removes reads from the shared path.
///
/// Compiled schemas are bounded by [`MAX_READY_ENTRIES`] and
/// [`MAX_READY_BYTES`]. At a bound the cache evicts the oldest schema that no
/// lookup has served since it was last considered, and never one of the
/// [`PROTECTED_NEWEST`] unless nothing else is left.
#[derive(Debug)]
pub(crate) struct SchemaCache {
    shared: RwLock<Shared>,
    /// Advanced, under the write lock, by each insert that evicted a compiled
    /// schema. A memo taken at another count holds a map the cache has left.
    evictions: AtomicU64,
    negative_ttl: Duration,
    /// Stamps and ages negative entries. The fetcher and the pipeline
    /// threads read one clock, so they agree on whether an entry expired.
    clock: SyncClock,
    /// Bounds the eviction warning: schema ids are wire data, so evictions
    /// can run at the rate records name fresh ids.
    evicted_warn: RateLimit,
}

impl SchemaCache {
    pub(crate) fn new(negative_ttl: Duration, clock: SyncClock) -> Self {
        SchemaCache {
            shared: RwLock::new(Shared {
                map: Arc::new(QualityMap::default()),
                inserts: 0,
                queue: 0,
            }),
            evictions: AtomicU64::new(0),
            negative_ttl,
            clock,
            evicted_warn: RateLimit::hot_path(),
        }
    }

    /// A fresh, empty memo for a newly built deserializer.
    pub(crate) fn empty_snapshot() -> CacheSnapshot {
        CacheSnapshot {
            map: Arc::new(QualityMap::default()),
            evictions: 0,
        }
    }

    #[inline]
    fn snapshot(&self) -> CacheSnapshot {
        let shared = self.shared.read().expect("schema cache lock");
        CacheSnapshot {
            map: Arc::clone(&shared.map),
            // Read under the lock its writer holds, so the count and the map
            // belong together.
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Shared-lock lookup. Used off the hot path (the fetcher's check before a
    /// fetch). It does not mark the schema as in use.
    pub(crate) fn get(&self, id: u32) -> Lookup {
        let shared = self.shared.read().expect("schema cache lock");
        Self::eval(&shared.map, id, self.negative_ttl, &*self.clock).unwrap_or(Lookup::Missing)
    }

    /// Hot-path lookup that consults the caller's local `memo` first and only
    /// takes the shared read lock when the memo has no *usable* answer.
    ///
    /// A `Ready` schema is immutable and is never downgraded (see
    /// [`Self::insert_failed`]), so a memo hit on `Ready` decodes correctly
    /// for as long as the memo is kept. The memo is still replaced once the
    /// cache has evicted a compiled schema, so that a deserializer stops
    /// holding what the cache dropped. The eviction count is loaded without
    /// the lock: a load that races an eviction serves one more record from
    /// the old map. On a miss, or a `Failed`/expired memo answer, the memo is
    /// refreshed once (a single `Arc` clone under the read lock) and
    /// re-evaluated, so a since-published schema or an expired negative entry
    /// is always observed.
    // This and `snapshot` are on the per-record path. The hints keep them
    // inlinable into the decoder whichever codegen unit holds this module.
    #[inline]
    pub(crate) fn lookup(&self, memo: &mut CacheSnapshot, id: u32) -> Lookup {
        if memo.evictions == self.evictions.load(Ordering::Relaxed)
            && let Some(Lookup::Ready(schema)) =
                Self::eval(&memo.map, id, self.negative_ttl, &*self.clock)
        {
            schema.touch();
            return Lookup::Ready(schema);
        }
        *memo = self.snapshot();
        let found =
            Self::eval(&memo.map, id, self.negative_ttl, &*self.clock).unwrap_or(Lookup::Missing);
        if let Lookup::Ready(schema) = &found {
            schema.touch();
        }
        found
    }

    fn eval(
        map: &QualityMap<u32, Entry>,
        id: u32,
        ttl: Duration,
        clock: &dyn Clock,
    ) -> Option<Lookup> {
        match map.get(&id) {
            Some(Entry::Ready(entry)) => Some(Lookup::Ready(Arc::clone(&entry.schema))),
            Some(Entry::Failed { reason, at }) => {
                if clock.now().saturating_duration_since(*at) >= ttl {
                    // Expired: treat as absent so the caller re-fetches.
                    None
                } else {
                    Some(Lookup::Failed(reason.clone()))
                }
            }
            None => None,
        }
    }

    /// A copy of `map` for a writer to mutate, without the negative entries
    /// that have expired. Expiry is the rule [`Self::eval`] applies.
    fn live_copy(&self, map: &QualityMap<u32, Entry>) -> QualityMap<u32, Entry> {
        let now = self.clock.now();
        let mut copy = map.clone();
        copy.retain(|_, entry| match entry {
            Entry::Ready(_) => true,
            Entry::Failed { at, .. } => now.saturating_duration_since(*at) < self.negative_ttl,
        });
        copy
    }

    /// [`Self::insert_ready`], with a rate-limited warning when the insert
    /// evicted.
    pub(crate) fn publish(&self, schema: CompiledSchema) {
        let evicted = self.insert_ready(schema);
        if evicted > 0 {
            spate_core::rate_limited_warn!(
                self.evicted_warn,
                at: self.clock.now(),
                evicted,
                max_schemas = MAX_READY_ENTRIES,
                max_bytes = MAX_READY_BYTES,
                "schema cache is full; an evicted schema is fetched again on its next record"
            );
        }
    }

    /// Holds `schema`, evicting compiled schemas until it fits both bounds,
    /// and returns how many it evicted. A schema already held under the same
    /// id is replaced and counts once.
    pub(crate) fn insert_ready(&self, schema: CompiledSchema) -> usize {
        let mut shared = self.shared.write().expect("schema cache lock");
        let mut map = self.live_copy(&shared.map);
        map.remove(&schema.id);
        shared.inserts += 1;
        let inserts = shared.inserts;
        let evicted = make_room(&mut map, inserts, &mut shared.queue, schema.weight);
        shared.queue += 1;
        map.insert(
            schema.id,
            Entry::Ready(ReadyEntry {
                schema: Arc::new(schema),
                queued: shared.queue,
                born: inserts,
            }),
        );
        shared.map = Arc::new(map);
        if evicted > 0 {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        evicted
    }

    pub(crate) fn insert_failed(&self, id: u32, reason: String) {
        let mut shared = self.shared.write().expect("schema cache lock");
        // Never downgrade a compiled schema to a poison entry. The prewarm
        // task and the by-id fetcher run concurrently; a late by-id failure
        // must not clobber a `Ready` schema another path already published,
        // which would surface a valid, cached schema as unavailable for the
        // whole negative TTL.
        if matches!(shared.map.get(&id), Some(Entry::Ready(_))) {
            return;
        }
        let mut map = self.live_copy(&shared.map);
        let negative = oldest_first(&map, |entry| match entry {
            Entry::Failed { at, .. } => Some(*at),
            Entry::Ready(_) => None,
        });
        if !map.contains_key(&id)
            && negative.len() >= MAX_NEGATIVE_ENTRIES
            && let Some(oldest) = negative.front()
        {
            map.remove(oldest);
        }
        map.insert(
            id,
            Entry::Failed {
                reason,
                at: self.clock.now(),
            },
        );
        shared.map = Arc::new(map);
    }
}

/// The ids of the entries `age` gives an age for, oldest first.
fn oldest_first<A: Ord>(
    map: &QualityMap<u32, Entry>,
    age: impl Fn(&Entry) -> Option<A>,
) -> VecDeque<u32> {
    let mut aged: Vec<(A, u32)> = map
        .iter()
        .filter_map(|(id, entry)| age(entry).map(|age| (age, *id)))
        .collect();
    aged.sort_unstable();
    aged.into_iter().map(|(_, id)| id).collect()
}

/// Evicts compiled schemas from `map` until one more of `weight` fits both
/// bounds, and returns how many it evicted. `inserts` counts the insert
/// being made room for.
fn make_room(
    map: &mut QualityMap<u32, Entry>,
    inserts: u64,
    queue: &mut u64,
    weight: usize,
) -> usize {
    let (mut count, mut bytes) = (0usize, 0usize);
    for entry in map.values() {
        if let Entry::Ready(ready) = entry {
            count += 1;
            bytes = bytes.saturating_add(ready.schema.weight);
        }
    }
    let over = |count: usize, bytes: usize| {
        count >= MAX_READY_ENTRIES || bytes.saturating_add(weight) > MAX_READY_BYTES
    };
    if !over(count, bytes) {
        return 0;
    }
    let mut order = oldest_first(map, |entry| match entry {
        Entry::Ready(ready) => Some(ready.queued),
        Entry::Failed { .. } => None,
    });
    let mut evicted = 0;
    // A schema heavier than the byte bound is held alone.
    while count > 0 && over(count, bytes) {
        let victim = pick_victim(map, &mut order, inserts, queue);
        if let Some(Entry::Ready(gone)) = map.remove(&victim) {
            count -= 1;
            bytes = bytes.saturating_sub(gone.schema.weight);
            evicted += 1;
        }
    }
    evicted
}

/// Removes from `order` the id to evict: the oldest compiled schema that is
/// neither among the newest [`PROTECTED_NEWEST`] nor in use. A schema found in
/// use is unmarked and moves to the back of the queue. When every schema is
/// one or the other, the oldest that was in use goes, and failing that the
/// oldest of all.
fn pick_victim(
    map: &mut QualityMap<u32, Entry>,
    order: &mut VecDeque<u32>,
    inserts: u64,
    queue: &mut u64,
) -> u32 {
    let mut oldest_used = None;
    for _ in 0..order.len() {
        let id = order.pop_front().expect("one id per pass");
        let Some(Entry::Ready(entry)) = map.get_mut(&id) else {
            continue;
        };
        if inserts - entry.born <= PROTECTED_NEWEST {
            order.push_back(id);
        } else if entry.schema.used.swap(false, Ordering::Relaxed) {
            *queue += 1;
            entry.queued = *queue;
            order.push_back(id);
            oldest_used.get_or_insert(id);
        } else {
            return id;
        }
    }
    let victim = oldest_used
        .or_else(|| order.front().copied())
        .expect("a compiled schema to evict");
    order.retain(|id| *id != victim);
    victim
}

/// A schema cache holding a set number of compiled schemas, with one more
/// compiled and waiting, so a bench counts one insert and nothing else.
#[cfg(feature = "testing")]
#[doc(hidden)]
#[derive(Debug)]
pub struct InsertRig {
    cache: SchemaCache,
    pending: Option<CompiledSchema>,
}

#[cfg(feature = "testing")]
impl InsertRig {
    /// A cache holding ids `0..held` compiled from `schema_json`, with id
    /// `held` waiting.
    #[must_use]
    pub fn holding(held: u32, schema_json: &str) -> Self {
        let cache = SchemaCache::new(Duration::from_secs(30), SyncClock::system());
        for id in 0..held {
            cache.insert_ready(CompiledSchema::compile(id, schema_json));
        }
        InsertRig {
            cache,
            pending: Some(CompiledSchema::compile(held, schema_json)),
        }
    }

    /// Inserts the waiting schema.
    ///
    /// # Panics
    ///
    /// Panics on a second call.
    pub fn insert(&mut self) {
        let schema = self.pending.take().expect("one insert per rig");
        self.cache.insert_ready(schema);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::clock::TestClock;

    const SCHEMA_JSON: &str =
        r#"{"type":"record","name":"T","fields":[{"name":"a","type":"long"}]}"#;

    /// A record name the Avro name rules reject.
    const BAD_NAME_JSON: &str =
        r#"{"type":"record","name":"my-record","fields":[{"name":"a","type":"long"}]}"#;

    fn compiled(id: u32) -> CompiledSchema {
        CompiledSchema::compile(id, SCHEMA_JSON)
    }

    /// [`compiled`] with its weight set to `weight`.
    fn weighing(id: u32, weight: usize) -> CompiledSchema {
        let mut schema = compiled(id);
        schema.weight = weight;
        schema
    }

    /// Entries the cache holds, of either kind.
    fn entries(cache: &SchemaCache) -> usize {
        cache.shared.read().expect("schema cache lock").map.len()
    }

    /// Compiled schemas the cache holds.
    fn ready(cache: &SchemaCache) -> usize {
        let shared = cache.shared.read().expect("schema cache lock");
        shared
            .map
            .values()
            .filter(|entry| matches!(entry, Entry::Ready(_)))
            .count()
    }

    fn is_ready(cache: &SchemaCache, id: u32) -> bool {
        matches!(cache.get(id), Lookup::Ready(_))
    }

    fn cache() -> SchemaCache {
        SchemaCache::new(
            Duration::from_secs(3600),
            SyncClock::new(TestClock::frozen()),
        )
    }

    /// The count bound as an id.
    fn cap() -> u32 {
        u32::try_from(MAX_READY_ENTRIES).unwrap()
    }

    /// The protected window as an id count.
    fn window() -> u32 {
        u32::try_from(PROTECTED_NEWEST).unwrap()
    }

    #[test]
    fn well_formed_schema_compiles() {
        let entry = compiled(4);
        assert!(entry.schema.is_ok());
        assert!(entry.unusable_reason().is_none());
    }

    #[test]
    fn a_rejected_schema_name_becomes_an_ordinary_failure() {
        // A poison schema from the registry negatively caches rather than
        // unwinding whichever pipeline thread happened to touch it first,
        // whichever way the parser refuses it.
        let entry = CompiledSchema::compile(6, BAD_NAME_JSON);
        assert!(
            matches!(&entry.schema, Err(reason) if reason.contains("my-record")),
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
        let cache = SchemaCache::new(Duration::from_secs(30), SyncClock::system());
        assert!(matches!(cache.get(1), Lookup::Missing));
        cache.insert_ready(compiled(1));
        assert!(matches!(cache.get(1), Lookup::Ready(s) if s.id == 1));
    }

    /// A zero TTL expires a negative entry on a clock that has not moved,
    /// and a long one keeps it.
    #[test]
    fn negative_entries_expire() {
        let clock = SyncClock::new(TestClock::frozen());
        let cache = SchemaCache::new(Duration::ZERO, clock.clone());
        cache.insert_failed(9, "unknown id".into());
        // TTL zero: expired immediately, so it is treated as missing and a
        // refetch is allowed.
        assert!(matches!(cache.get(9), Lookup::Missing));

        let cache = SchemaCache::new(Duration::from_secs(600), clock);
        cache.insert_failed(9, "unknown id".into());
        assert!(matches!(cache.get(9), Lookup::Failed(r) if r.contains("unknown id")));
    }

    /// A negative entry answers `Failed` until its clock has moved exactly
    /// the TTL past its stamp, then `Missing`, on both the shared-lock and
    /// the memo lookup.
    #[test]
    fn a_negative_entry_expires_exactly_at_its_ttl() {
        let ttl = Duration::from_millis(300);
        let clock = TestClock::frozen();
        let cache = SchemaCache::new(ttl, SyncClock::new(clock.clone()));
        let mut memo = SchemaCache::empty_snapshot();
        cache.insert_failed(9, "unknown id".into());

        clock.advance(ttl - Duration::from_nanos(1));
        assert!(
            matches!(cache.get(9), Lookup::Failed(_)),
            "one nanosecond short of the TTL"
        );
        assert!(
            matches!(cache.lookup(&mut memo, 9), Lookup::Failed(_)),
            "one nanosecond short of the TTL"
        );

        clock.advance(Duration::from_nanos(1));
        assert!(matches!(cache.get(9), Lookup::Missing), "at the TTL");
        assert!(
            matches!(cache.lookup(&mut memo, 9), Lookup::Missing),
            "at the TTL"
        );
    }

    /// A negative entry past its TTL leaves the map at the next write, so
    /// unknown schema ids named by records cannot grow the cache without
    /// bound.
    #[test]
    fn expired_negative_entries_leave_the_map() {
        let ttl = Duration::from_secs(30);
        let clock = TestClock::frozen();
        let cache = SchemaCache::new(ttl, SyncClock::new(clock.clone()));
        cache.insert_ready(compiled(u32::MAX));
        for id in 0..10_000u32 {
            cache.insert_failed(id, format!("schema {id} unknown"));
            clock.advance(ttl);
        }
        let held = entries(&cache);
        assert_eq!(held, 2, "the Ready schema and the newest negative entry");
        cache.insert_ready(compiled(1));
        let held = entries(&cache);
        assert_eq!(held, 2, "a Ready insert drops the expired negative entry");
    }

    /// Past [`MAX_NEGATIVE_ENTRIES`] live negative entries, a new one replaces
    /// the oldest, and a Ready schema is never the one dropped.
    #[test]
    fn negative_entries_are_capped_oldest_first() {
        let clock = TestClock::frozen();
        let cache = SchemaCache::new(Duration::from_secs(3600), SyncClock::new(clock.clone()));
        cache.insert_ready(compiled(u32::MAX));
        let cap = u32::try_from(MAX_NEGATIVE_ENTRIES).unwrap();
        for id in 0..=cap {
            cache.insert_failed(id, format!("schema {id} unknown"));
            clock.advance(Duration::from_millis(1));
        }
        let held = entries(&cache);
        assert_eq!(held, MAX_NEGATIVE_ENTRIES + 1);
        assert!(
            matches!(cache.get(0), Lookup::Missing),
            "the oldest is dropped"
        );
        assert!(matches!(cache.get(1), Lookup::Failed(_)));
        assert!(matches!(cache.get(cap), Lookup::Failed(_)));
        assert!(matches!(cache.get(u32::MAX), Lookup::Ready(_)));
        // Refreshing an id already held keeps the count and drops nothing,
        // so the oldest entry, id 1, survives a refresh of id 2.
        cache.insert_failed(2, "schema 2 unknown".into());
        let held = entries(&cache);
        assert_eq!(held, MAX_NEGATIVE_ENTRIES + 1);
        assert!(matches!(cache.get(1), Lookup::Failed(_)));
    }

    #[test]
    fn ready_is_never_downgraded_to_failed() {
        // Prewarm/fetcher race: a compiled schema must survive a concurrent
        // by-id fetch failing for the same id.
        let cache = SchemaCache::new(Duration::from_secs(600), SyncClock::system());
        cache.insert_ready(compiled(5));
        cache.insert_failed(5, "registry returned 500".into());
        assert!(
            matches!(cache.get(5), Lookup::Ready(s) if s.id == 5),
            "a late failure must not clobber a cached Ready schema"
        );
    }

    #[test]
    fn memo_serves_ready_and_refreshes_on_non_ready() {
        let cache = SchemaCache::new(Duration::from_secs(600), SyncClock::system());
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

    /// A schema's weight is its JSON length plus the length of each named
    /// type once per name, at the measured bytes per byte.
    #[test]
    fn weight_counts_each_named_type_once_per_name() {
        assert_eq!(weigh(100, 0), 100 * BYTES_PER_SOURCE_BYTE);
        assert_eq!(weigh(100, 300), 400 * BYTES_PER_SOURCE_BYTE);
        assert_eq!(weigh(usize::MAX, 3), usize::MAX, "saturates");

        let cloned = |json: &str| cloned_bytes(&serde_json::from_str(json).unwrap());
        assert_eq!(cloned(r#""long""#), 0);
        assert_eq!(cloned(r#"{"type":"long"}"#), 0);
        assert_eq!(cloned(r#"{"type":"array","items":"long"}"#), 0);

        let fixed = r#"{"type":"fixed","name":"F","size":4}"#;
        let aliased = r#"{"type":"fixed","name":"F","size":4,"aliases":["G","H"]}"#;
        let inner = r#"{"type":"enum","name":"E","symbols":["X"]}"#;
        let record = |inside: &str| {
            format!(
                r#"{{"type":"record","name":"A","fields":[{{"name":"x","type":"long","aliases":["y"]}},{{"name":"e","type":{inside}}}]}}"#
            )
        };
        assert_eq!(cloned(fixed), fixed.len());
        assert_eq!(cloned(aliased), 3 * aliased.len(), "once per name");
        assert_eq!(cloned(&record(r#""long""#)), record(r#""long""#).len());
        for inside in [
            inner.to_owned(),
            format!(r#"{{"type":{inner}}}"#),
            format!(r#"["null",{inner}]"#),
            format!(r#"{{"type":"array","items":{inner}}}"#),
            format!(r#"{{"type":"map","values":{inner}}}"#),
        ] {
            let outer = record(&inside);
            assert_eq!(cloned(&outer), outer.len() + inner.len(), "{inside}");
        }
        let union = format!(r#"["null",{fixed},{inner}]"#);
        assert_eq!(cloned(&union), fixed.len() + inner.len());
        let error = record(inner).replace(r#""type":"record""#, r#""type":"error""#);
        assert_eq!(cloned(&error), error.len() + inner.len());

        let json = record(inner);
        assert_eq!(
            CompiledSchema::compile(1, &json).weight,
            (2 * json.len() + inner.len()) * BYTES_PER_SOURCE_BYTE
        );
        assert_eq!(CompiledSchema::compile(1, "not a schema").weight, 0);
        assert_eq!(CompiledSchema::compile(1, r#"{"type":"record"}"#).weight, 0);
    }

    /// The length of a value written as compact JSON.
    #[test]
    fn json_len_is_the_compact_length() {
        for json in [
            "null",
            "true",
            "false",
            "12345",
            "-1.5",
            r#""text""#,
            "[]",
            "{}",
            r#"[1,"a",null]"#,
            r#"{"a":1,"b":[true,{"c":"d"}]}"#,
        ] {
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            assert_eq!(json_len(&value), json.len(), "{json}");
        }
    }

    /// A record, enum or fixed written without its body weighs the whole
    /// schema: the parser puts a clone of an earlier type of that name there.
    /// A record named `record` that holds itself 400 times this way is
    /// refused.
    #[test]
    fn a_type_written_without_its_body_weighs_the_whole_schema() {
        let cloned = |json: &str| cloned_bytes(&serde_json::from_str(json).unwrap());
        for bare in [
            r#"{"type":"record"}"#,
            r#"{"type":"error"}"#,
            r#"{"type":"enum"}"#,
            r#"{"type":"fixed"}"#,
        ] {
            let json = format!(
                r#"{{"type":"record","name":"A","fields":[{{"name":"a","type":{bare}}},{{"name":"b","type":{bare}}}]}}"#
            );
            assert_eq!(cloned(&json), 3 * json.len(), "{bare}");
        }

        let uses: String = (0..400)
            .map(|u| format!(r#"{{"name":"u{u}","type":{{"type":"record"}}}},"#))
            .collect();
        let json = nested_schema(1, 200)
            .replacen(r#""L0""#, r#""record""#, 1)
            .replacen(r#"{"name":"c","#, &format!(r#"{uses}{{"name":"c","#), 1);
        assert_eq!(cloned(&json), 401 * json.len());
        assert!(
            CompiledSchema::compile_for_cache(1, &json)
                .unusable_reason()
                .is_some_and(|reason| reason.contains("MiB compiled"))
        );
    }

    /// A wide record with one deep branch weighs its fields once and the
    /// branch once per level, so it stays far below a schema as long that
    /// nests every field.
    #[test]
    fn one_deep_branch_does_not_multiply_a_wide_record() {
        let branch = nested_schema(6, 1);
        let wide = nested_schema(1, 1500).replacen(r#""long"}]}"#, &format!("{branch}}}]}}"), 1);
        assert!(Schema::parse_str(&wide).is_ok());
        let weight = weight_of(&wide).unwrap();
        assert!(weight < 3 * wide.len() * BYTES_PER_SOURCE_BYTE, "{weight}");
        assert!(CompiledSchema::compile_for_cache(1, &wide).schema.is_ok());
    }

    /// The parser clones a record once per alias, so a record under the byte
    /// bound by its length alone is refused for its aliases.
    #[test]
    fn a_record_with_many_aliases_is_refused_for_the_cache() {
        let aliases: Vec<String> = (0..1000).map(|a| format!(r#""A{a}""#)).collect();
        let aliased = nested_schema(1, 500).replacen(
            r#""name":"L0","#,
            &format!(r#""name":"L0","aliases":[{}],"#, aliases.join(",")),
            1,
        );
        assert!(weigh(aliased.len(), aliased.len()) < MAX_READY_BYTES);
        assert_eq!(
            weight_of(&aliased),
            Some(weigh(aliased.len(), 1001 * aliased.len()))
        );
        assert!(CompiledSchema::compile(1, &aliased).schema.is_ok());
        let reason = CompiledSchema::compile_for_cache(1, &aliased)
            .unusable_reason()
            .expect("refused");
        assert!(reason.contains("MiB compiled"), "{reason}");
    }

    /// A schema is weighed before it is parsed: one above the bound that the
    /// parser would also reject is refused for its size.
    #[test]
    fn the_byte_bound_is_checked_before_the_parser_runs() {
        let rejected = nested_schema(40, 200).replace("L39", "not-a-name");
        assert!(Schema::parse_str(&rejected).is_err());
        let reason = CompiledSchema::compile_for_cache(7, &rejected)
            .unusable_reason()
            .expect("refused");
        assert!(reason.contains("MiB compiled"), "{reason}");
    }

    /// A schema weighing more than the cache's byte bound is unusable when
    /// compiled for the cache, with neither a schema nor a datum index, and
    /// compiles when it is not for the cache. One at the bound compiles.
    #[test]
    fn a_schema_above_the_byte_bound_is_refused_for_the_cache() {
        let json = nested_schema(40, 200);
        let weight = CompiledSchema::compile(7, &json).weight;
        assert!(weight > MAX_READY_BYTES, "{weight}");

        let refused = CompiledSchema::compile_for_cache(7, &json);
        let reason = refused.unusable_reason().expect("above the bound");
        assert_eq!(
            reason,
            format!(
                "schema 7 is not usable: it would hold about {} MiB compiled, above the 64 MiB \
                 the schema cache holds",
                weight >> 20
            )
        );
        assert_eq!(refused.datum.as_ref().err(), Some(&reason));
        assert_eq!(refused.weight, weight);

        assert!(CompiledSchema::compile(7, &json).schema.is_ok());
        let at = CompiledSchema::compile_within(7, &json, weight);
        assert!(at.schema.is_ok() && at.datum.is_ok(), "at the bound");
        let under = CompiledSchema::compile_within(7, &json, weight - 1);
        assert!(under.schema.is_err(), "one byte under the weight");
    }

    /// Ten times the count bound of distinct ids leaves the bound's worth of
    /// compiled schemas, the newest among them, and a negative entry stays.
    #[test]
    fn ready_entries_are_capped() {
        let cache = cache();
        cache.insert_failed(u32::MAX, "unknown".into());
        for id in 0..cap() {
            assert_eq!(cache.insert_ready(compiled(id)), 0, "id {id} fits");
        }
        assert_eq!(ready(&cache), MAX_READY_ENTRIES);
        for id in cap()..10 * cap() {
            assert_eq!(cache.insert_ready(compiled(id)), 1, "id {id} evicts one");
        }
        assert_eq!(ready(&cache), MAX_READY_ENTRIES);
        assert!(is_ready(&cache, 10 * cap() - 1), "the newest is held");
        assert!(!is_ready(&cache, 0), "the oldest is gone");
        assert!(matches!(cache.get(u32::MAX), Lookup::Failed(_)));
    }

    /// The first schema inserted is the first evicted, whatever the ids:
    /// here the ids fall as the inserts go on.
    #[test]
    fn eviction_follows_insert_order_whatever_the_ids() {
        let cache = cache();
        for id in (1..=cap()).rev() {
            cache.insert_ready(compiled(id));
        }
        assert_eq!(cache.insert_ready(compiled(cap() + 1)), 1);
        assert!(!is_ready(&cache, cap()), "the first inserted is evicted");
        assert!(is_ready(&cache, 1), "the last inserted is held");
    }

    /// A schema found in use moves behind every schema inserted before the
    /// sweep that found it, and leaves once they have.
    #[test]
    fn a_schema_found_in_use_goes_to_the_back_of_the_queue() {
        let cache = cache();
        let mut memo = SchemaCache::empty_snapshot();
        for id in 0..cap() {
            cache.insert_ready(compiled(id));
        }
        assert!(matches!(cache.lookup(&mut memo, 0), Lookup::Ready(_)));
        // The first of these inserts finds id 0 in use and evicts id 1.
        for id in cap()..=2 * cap() - 2 {
            cache.insert_ready(compiled(id));
        }
        assert!(!is_ready(&cache, cap() - 1), "the last inserted before it");
        assert!(is_ready(&cache, 0), "outlives every schema it moved behind");
        cache.insert_ready(compiled(2 * cap() - 1));
        assert!(!is_ready(&cache, 0), "and is the next to leave");
    }

    /// The bench rig holds the waiting schema after its insert.
    #[cfg(feature = "testing")]
    #[test]
    fn the_insert_rig_inserts_its_waiting_schema() {
        let mut rig = InsertRig::holding(2, SCHEMA_JSON);
        assert!(!is_ready(&rig.cache, 2));
        rig.insert();
        assert!(is_ready(&rig.cache, 2));
    }

    /// Weights summing to the byte bound fit. One byte more evicts the
    /// oldest, and an insert that needs the room of two evicts two.
    #[test]
    fn ready_bytes_are_capped() {
        let cache = cache();
        let half = MAX_READY_BYTES / 2;
        assert_eq!(cache.insert_ready(weighing(1, half)), 0);
        assert_eq!(
            cache.insert_ready(weighing(2, half)),
            0,
            "exactly at the bound"
        );
        assert_eq!(cache.insert_ready(weighing(3, 1)), 1, "one byte over");
        assert!(!is_ready(&cache, 1), "the oldest is evicted");
        assert!(is_ready(&cache, 2) && is_ready(&cache, 3));

        assert_eq!(cache.insert_ready(weighing(4, MAX_READY_BYTES)), 2);
        assert_eq!(ready(&cache), 1);
        assert!(is_ready(&cache, 4));
    }

    /// A schema heavier than the byte bound evicts every other and is held.
    #[test]
    fn a_schema_over_the_byte_bound_is_held_alone() {
        let cache = cache();
        for id in 0..3 {
            cache.insert_ready(compiled(id));
        }
        assert_eq!(cache.insert_ready(weighing(9, MAX_READY_BYTES + 1)), 3);
        assert_eq!(ready(&cache), 1);
        assert!(is_ready(&cache, 9));
        assert_eq!(
            cache.insert_ready(compiled(10)),
            1,
            "and leaves for the next"
        );
        assert!(is_ready(&cache, 10));
    }

    /// With every schema outside the protected window in use, an insert
    /// evicts the oldest of them and holds the new one.
    #[test]
    fn an_insert_with_every_resident_in_use_evicts_the_oldest() {
        let cache = cache();
        let mut memo = SchemaCache::empty_snapshot();
        for id in 0..cap() {
            cache.insert_ready(compiled(id));
        }
        for id in 0..cap() {
            assert!(matches!(cache.lookup(&mut memo, id), Lookup::Ready(_)));
        }
        assert_eq!(cache.insert_ready(compiled(cap())), 1);
        assert_eq!(ready(&cache), MAX_READY_ENTRIES);
        assert!(!is_ready(&cache, 0), "the oldest in use is evicted");
        assert!(is_ready(&cache, 1) && is_ready(&cache, cap()));
    }

    /// A second insert of a held id replaces it: nothing is evicted and the
    /// count does not move, at the bound included.
    #[test]
    fn reinserting_a_held_id_counts_it_once() {
        let cache = cache();
        for id in 0..cap() {
            cache.insert_ready(compiled(id));
        }
        assert_eq!(cache.insert_ready(compiled(5)), 0);
        assert_eq!(ready(&cache), MAX_READY_ENTRIES);
        assert!(is_ready(&cache, 0), "nothing made room for it");
    }

    /// At both bounds, a compiled insert evicts no negative entry and a
    /// negative insert evicts no compiled schema.
    #[test]
    fn each_kind_evicts_only_its_own() {
        let clock = TestClock::frozen();
        let cache = SchemaCache::new(Duration::from_secs(3600), SyncClock::new(clock.clone()));
        for id in 0..cap() {
            cache.insert_ready(compiled(id));
        }
        let negatives = u32::try_from(MAX_NEGATIVE_ENTRIES).unwrap();
        for id in 0..negatives {
            cache.insert_failed(cap() + id, "unknown".into());
            clock.advance(Duration::from_millis(1));
        }
        assert_eq!(entries(&cache), MAX_READY_ENTRIES + MAX_NEGATIVE_ENTRIES);

        cache.insert_ready(compiled(u32::MAX));
        assert_eq!(ready(&cache), MAX_READY_ENTRIES);
        assert_eq!(entries(&cache), MAX_READY_ENTRIES + MAX_NEGATIVE_ENTRIES);

        cache.insert_failed(u32::MAX - 1, "unknown".into());
        assert_eq!(ready(&cache), MAX_READY_ENTRIES);
        assert_eq!(entries(&cache), MAX_READY_ENTRIES + MAX_NEGATIVE_ENTRIES);
    }

    /// A schema looked up between inserts outlives twice the count bound of
    /// fresh ids, and one never looked up does not outlive the bound.
    #[test]
    fn a_used_schema_outlives_unused_ones() {
        let cache = cache();
        let mut memo = SchemaCache::empty_snapshot();
        cache.insert_ready(compiled(0));
        for id in 1..=2 * cap() + window() {
            assert!(
                matches!(cache.lookup(&mut memo, 0), Lookup::Ready(_)),
                "id 0 is held before insert {id}"
            );
            cache.insert_ready(compiled(id));
        }
        assert!(is_ready(&cache, 0), "the schema in use");
        assert!(!is_ready(&cache, 1), "a schema never looked up");
        assert_eq!(ready(&cache), MAX_READY_ENTRIES);
    }

    /// A schema nobody has looked up yet outlives as many newer inserts as
    /// the protected window, while every other schema is in use, and can be
    /// evicted by the next.
    #[test]
    fn a_new_entry_outlives_its_protected_inserts() {
        let cache = cache();
        let mut memo = SchemaCache::empty_snapshot();
        let waiting = cap();
        let use_all_but_waiting = |memo: &mut CacheSnapshot, upto: u32| {
            for id in (0..=upto).filter(|id| *id != waiting) {
                let _ = cache.lookup(memo, id);
            }
        };
        for id in 0..cap() {
            cache.insert_ready(compiled(id));
        }
        cache.insert_ready(compiled(waiting));
        for newer in 1..=window() {
            use_all_but_waiting(&mut memo, waiting + newer);
            cache.insert_ready(compiled(waiting + newer));
            assert!(is_ready(&cache, waiting), "after {newer} newer inserts");
        }
        use_all_but_waiting(&mut memo, waiting + window());
        cache.insert_ready(compiled(waiting + window() + 1));
        assert!(!is_ready(&cache, waiting), "past the protected window");
    }

    /// After an eviction a memo that held the evicted schema answers
    /// `Missing` for it and no longer keeps it alive, and still serves a
    /// schema the cache holds.
    #[test]
    fn a_memo_drops_an_evicted_schema() {
        let cache = cache();
        let mut memo = SchemaCache::empty_snapshot();
        cache.insert_ready(compiled(0));
        let Lookup::Ready(evicted) = cache.get(0) else {
            panic!("id 0 is held");
        };
        // A miss refreshes the memo, which then holds id 0, unused.
        assert!(matches!(cache.lookup(&mut memo, u32::MAX), Lookup::Missing));
        assert!(memo.holds(&evicted));

        for id in 1..=cap() {
            cache.insert_ready(compiled(id));
        }
        assert!(!is_ready(&cache, 0), "id 0 is evicted");
        assert!(memo.holds(&evicted), "the memo is the map from before");

        assert!(matches!(cache.lookup(&mut memo, 0), Lookup::Missing));
        assert!(!memo.holds(&evicted));
        assert_eq!(Arc::strong_count(&evicted), 1, "only the test holds it");
        assert!(matches!(cache.lookup(&mut memo, cap()), Lookup::Ready(_)));
    }

    /// The eviction count moves on an insert that evicted and on no other
    /// write, and a snapshot carries the count it was taken at.
    #[test]
    fn only_an_eviction_moves_the_eviction_count() {
        let cache = cache();
        for id in 0..cap() {
            cache.insert_ready(compiled(id));
        }
        cache.insert_ready(compiled(3));
        cache.insert_failed(u32::MAX, "unknown".into());
        assert_eq!(cache.evictions.load(Ordering::Relaxed), 0);
        assert_eq!(cache.snapshot().evictions(), 0);

        cache.insert_ready(compiled(cap()));
        assert_eq!(cache.evictions.load(Ordering::Relaxed), 1);
        assert_eq!(cache.snapshot().evictions(), 1);
    }

    /// `publish` warns when it evicted, at most five times in the limiter's
    /// window, and not at all while everything fits.
    #[test]
    fn eviction_warnings_are_rate_limited() {
        const FULL: &str =
            "schema cache is full; an evicted schema is fetched again on its next record";
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            let cache = cache();
            for id in 0..cap() {
                cache.publish(compiled(id));
            }
            for id in cap()..cap() + 50 {
                cache.publish(compiled(id));
            }
        });
        let warned: Vec<&str> = lines
            .iter()
            .filter_map(|line| line.split_once(FULL))
            .map(|(_, fields)| fields.trim())
            .collect();
        assert_eq!(
            warned, ["evicted=1 max_schemas=1024 max_bytes=67108864"; 5],
            "{lines:#?}"
        );
    }
}
