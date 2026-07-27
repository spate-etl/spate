//! The registry fetcher: the only place that talks HTTP.
//!
//! Pipeline threads never touch the network. On a cache miss the
//! deserializer sends the schema id here (an unbounded, non-blocking send)
//! and returns [`DeserError::NotReady`](spate_core::error::DeserError); this
//! task fetches, parses, and publishes the schema into the shared cache,
//! and the driver's blocked-batch retry picks it up.
//!
//! `schema_registry_converter` is used strictly as the registry HTTP
//! client — its decoders never appear on the hot path.
//!
//! # Transient vs permanent failures
//!
//! Only a *permanent* verdict about an id — the registry answering `404`
//! (unknown id/subject/version), a schema that uses unsupported references,
//! or a schema **no enabled backend** can parse — is negatively cached. A
//! schema only one backend accepts is not a negative verdict: it is
//! published `Ready` with the rejecting backend's reason stored per-backend
//! inside the entry (see `CompiledSchema`). A *transient* outage
//! (any other 5xx, `429`, a timeout, a refused/black-holed connection)
//! leaves the id **absent** so the deserializer's next replay refetches it:
//! poisoning a transient blip would drop (and ack) perfectly decodable
//! records for the whole negative-cache TTL. Per-id backoff, held here in
//! the fetcher, keeps those replays from hot-looping the registry.
//!
//! # Concurrency
//!
//! Fetches run concurrently (up to [`MAX_CONCURRENT_FETCHES`]) so one slow
//! or black-holed id cannot head-of-line-block every other id. Per-id
//! dedup (a fetch already in flight is never started twice) and per-id
//! backoff are preserved across the concurrency.

use crate::cache::{CompiledSchema, Lookup, SchemaCache};
use schema_registry_converter::async_impl::schema_registry::{self, SrSettings, SrSettingsBuilder};
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common::{SchemaType, SubjectNameStrategy};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Per-id backoff bounds applied after a transient registry failure, so
/// repeated NotReady replays cannot hot-loop the registry.
const FETCH_BACKOFF_INITIAL: Duration = Duration::from_millis(200);
const FETCH_BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Maximum number of schema ids fetched concurrently. Bounds head-of-line
/// blocking (a slow id no longer stalls the rest) while keeping registry
/// load and open-socket count modest.
const MAX_CONCURRENT_FETCHES: usize = 4;

/// Cloneable handle held by deserializers: request a fetch, read the cache.
#[derive(Clone, Debug)]
pub(crate) struct RegistryHandle {
    tx: mpsc::UnboundedSender<u32>,
    pub(crate) cache: Arc<SchemaCache>,
}

impl RegistryHandle {
    /// Request an asynchronous fetch of `id`. Never blocks; duplicate
    /// requests are deduplicated by the fetcher. A dropped fetcher (I/O
    /// runtime shut down) makes this a no-op — the pipeline is draining.
    pub(crate) fn request(&self, id: u32) {
        let _ = self.tx.send(id);
    }
}

/// Registry connection settings.
#[derive(Clone, Debug)]
pub(crate) struct RegistryConfig {
    pub url: String,
    pub basic_auth: Option<(String, Option<String>)>,
    pub negative_cache_ttl: Duration,
}

fn sr_settings(cfg: &RegistryConfig) -> SrSettings {
    let mut builder: SrSettingsBuilder = SrSettings::new_builder(cfg.url.clone());
    if let Some((user, pass)) = &cfg.basic_auth {
        builder.set_basic_authorization(user, pass.as_deref());
    }
    builder.build().expect("registry settings")
}

/// What a single fetch resolved to, used to drive per-id backoff.
enum FetchOutcome {
    /// A definitive verdict was written to the cache: a compiled schema, or
    /// a negative entry for a permanently unusable id. Backoff cleared.
    Resolved,
    /// A transient registry failure. The id was left absent so a later
    /// replay refetches it; the fetcher grows this id's backoff.
    Transient,
}

/// Per-id backoff state kept in the fetcher.
struct Backoff {
    delay: Duration,
    next_allowed: Instant,
}

/// Spawn the fetcher task on `handle` and return the requester side.
pub(crate) fn spawn_fetcher(
    cfg: RegistryConfig,
    runtime: &tokio::runtime::Handle,
) -> RegistryHandle {
    let cache = Arc::new(SchemaCache::new(cfg.negative_cache_ttl));
    let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
    let task_cache = Arc::clone(&cache);
    let settings = Arc::new(sr_settings(&cfg));
    runtime.spawn(async move {
        // Ids with a fetch currently running: dedup across the concurrency.
        let mut in_flight: HashSet<u32> = HashSet::new();
        // Per-id backoff after transient failures.
        let mut backoff: HashMap<u32, Backoff> = HashMap::new();
        let mut tasks: JoinSet<(u32, FetchOutcome)> = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                // Drain finished fetches first so slots free promptly.
                Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                    let Ok((id, outcome)) = joined else {
                        // A fetch task panicked. The known panic source —
                        // apache-avro's `Schema::parse_str` — is caught inside
                        // `fetch_one` and negative-cached, so reaching here
                        // means an unexpected panic elsewhere in the task. The
                        // `JoinError` does not carry our schema id, so that id
                        // stays in `in_flight` and won't refetch; this
                        // last-resort path is no worse than the pre-existing
                        // fetcher-death behavior.
                        tracing::error!("registry fetch task panicked");
                        continue;
                    };
                    in_flight.remove(&id);
                    match outcome {
                        FetchOutcome::Resolved => {
                            backoff.remove(&id);
                        }
                        FetchOutcome::Transient => match backoff.get_mut(&id) {
                            Some(b) => {
                                b.delay = (b.delay * 2).min(FETCH_BACKOFF_MAX);
                                b.next_allowed = Instant::now() + b.delay;
                            }
                            None => {
                                backoff.insert(
                                    id,
                                    Backoff {
                                        delay: FETCH_BACKOFF_INITIAL,
                                        next_allowed: Instant::now() + FETCH_BACKOFF_INITIAL,
                                    },
                                );
                            }
                        },
                    }
                }
                maybe_id = rx.recv(), if tasks.len() < MAX_CONCURRENT_FETCHES => {
                    let Some(id) = maybe_id else {
                        // All deserializers dropped: the pipeline is draining.
                        break;
                    };
                    // Dedup: the id may be requested by several pipeline
                    // threads before the first fetch lands, or already
                    // resolved / negatively cached.
                    if in_flight.contains(&id) {
                        continue;
                    }
                    if !matches!(task_cache.get(id), Lookup::Missing) {
                        continue;
                    }
                    // Honor per-id backoff so replays after a transient
                    // failure don't hammer the registry.
                    if backoff.get(&id).is_some_and(|b| Instant::now() < b.next_allowed) {
                        continue;
                    }
                    in_flight.insert(id);
                    let cache = Arc::clone(&task_cache);
                    let settings = Arc::clone(&settings);
                    tasks.spawn(async move {
                        let outcome = fetch_one(id, &settings, &cache).await;
                        (id, outcome)
                    });
                }
            }
        }
    });
    RegistryHandle { tx, cache }
}

/// Fetch, parse, and publish schema `id`, or classify the failure. A single
/// HTTP attempt: transient failures are retried by the deserializer replaying
/// the payload (bounded by this id's backoff), not by blocking here — which
/// also stops one slow id from monopolizing a fetch slot for minutes.
async fn fetch_one(id: u32, settings: &SrSettings, cache: &SchemaCache) -> FetchOutcome {
    match schema_registry::get_schema_by_id_and_type(id, settings, SchemaType::Avro).await {
        Ok(registered) => {
            if !registered.references.is_empty() {
                cache.insert_failed(
                    id,
                    format!(
                        "schema {id} uses {} registry reference(s), which spate-avro \
                         does not support yet",
                        registered.references.len()
                    ),
                );
                return FetchOutcome::Resolved;
            }
            // Each backend compiles independently (`CompiledSchema::compile`,
            // which also catches apache-avro 0.21's parse *panics* on
            // malformed names like `"my-record"`): a schema only one backend
            // accepts is published `Ready` and stays fully usable there,
            // while the rejecting backend surfaces its stored reason per
            // record. Only a schema **no** enabled backend accepts is
            // negative-cached — a poison payload for every pipeline. Either
            // way the id resolves; a bad schema can never stall it at
            // NotReady forever.
            let compiled = CompiledSchema::compile(id, &registered.schema);
            match compiled.unusable_reason() {
                None => {
                    tracing::info!(schema_id = id, "schema fetched and compiled");
                    cache.insert_ready(compiled);
                }
                Some(reason) => {
                    cache.insert_failed(id, reason);
                }
            }
            FetchOutcome::Resolved
        }
        Err(e) if is_permanent(&e) => {
            // A genuinely unknown id (registry 404). Negative-cache it so we
            // don't hammer the registry for an id that will never resolve;
            // the deserializer applies its ErrorPolicy to the poison payload.
            tracing::warn!(schema_id = id, error = %e, "registry reports schema id unknown");
            cache.insert_failed(id, format!("registry fetch for schema {id} failed: {e}"));
            FetchOutcome::Resolved
        }
        Err(e) => {
            // Transient outage (5xx other than 404, 429, timeout, refused or
            // black-holed connection, retriable error). Leave the id absent:
            // poisoning it here would drop (and ack) decodable records for the
            // whole negative TTL. The next replay refetches, subject to
            // per-id backoff.
            tracing::warn!(schema_id = id, error = %e, "registry fetch failed transiently; will retry");
            FetchOutcome::Transient
        }
    }
}

/// Whether a registry error is a *permanent* verdict about the id (a `404`
/// not-found) rather than a transient outage.
///
/// `schema_registry_converter` (a 0.x dependency) does not expose the HTTP
/// status as a field — it only formats it into the error message
/// (`"...failed with status 404 Not Found"`) — so we match on that. This is
/// deliberately narrow: anything we cannot positively identify as a `404` is
/// treated as transient, because the safe failure mode is to refetch (a
/// bounded stall), never to negatively cache and silently drop valid records.
fn is_permanent(e: &SRCError) -> bool {
    e.error.contains("status 404")
}

/// Fetch the latest version of every configured subject into the cache
/// (startup pre-warm). Failures are logged, not fatal: the id will be
/// fetched on demand when it first appears in a payload.
pub(crate) async fn prewarm(cfg: &RegistryConfig, subjects: &[String], cache: &SchemaCache) {
    let settings = sr_settings(cfg);
    for subject in subjects {
        let strategy = SubjectNameStrategy::RecordNameStrategy(subject.clone());
        match schema_registry::get_schema_by_subject(&settings, &strategy).await {
            Ok(registered) if registered.references.is_empty() => {
                // Per-backend compile with the parse-panic guard inside
                // (see `fetch_one`), so one poison schema cannot kill this
                // detached task mid-list and silently skip every remaining
                // subject's pre-warm.
                let compiled = CompiledSchema::compile(registered.id, &registered.schema);
                match compiled.unusable_reason() {
                    None => {
                        tracing::info!(subject, schema_id = registered.id, "pre-warmed schema");
                        cache.insert_ready(compiled);
                    }
                    Some(reason) => {
                        tracing::warn!(subject, %reason, "pre-warm parse failed; skipping subject");
                    }
                }
            }
            Ok(_) => {
                tracing::warn!(subject, "pre-warm skipped: schema references unsupported");
            }
            Err(e) => tracing::warn!(subject, error = %e, "pre-warm fetch failed"),
        }
    }
}
