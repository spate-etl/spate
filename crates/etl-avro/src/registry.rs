//! The registry fetcher: the only place that talks HTTP.
//!
//! Pipeline threads never touch the network. On a cache miss the
//! deserializer sends the schema id here (an unbounded, non-blocking send)
//! and returns [`DeserError::NotReady`](etl_core::error::DeserError); this
//! task fetches, parses, and publishes the schema into the shared cache,
//! and the driver's blocked-batch retry picks it up.
//!
//! `schema_registry_converter` is used strictly as the registry HTTP
//! client — its decoders never appear on the hot path.

use crate::cache::{CompiledSchema, SchemaCache};
use apache_avro::Schema;
use schema_registry_converter::async_impl::schema_registry::{self, SrSettings, SrSettingsBuilder};
use schema_registry_converter::schema_registry_common::{SchemaType, SubjectNameStrategy};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// How many times a retriable registry error is retried before an id gets
/// a (TTL-bounded) negative cache entry.
const FETCH_ATTEMPTS: u32 = 5;
const FETCH_BACKOFF_INITIAL: Duration = Duration::from_millis(200);
const FETCH_BACKOFF_MAX: Duration = Duration::from_secs(5);

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

/// Spawn the fetcher task on `handle` and return the requester side.
pub(crate) fn spawn_fetcher(
    cfg: RegistryConfig,
    runtime: &tokio::runtime::Handle,
) -> RegistryHandle {
    let cache = Arc::new(SchemaCache::new(cfg.negative_cache_ttl));
    let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
    let task_cache = Arc::clone(&cache);
    let settings = sr_settings(&cfg);
    runtime.spawn(async move {
        while let Some(id) = rx.recv().await {
            // Dedup: the id may have been requested by several pipeline
            // threads before the first fetch landed.
            if !matches!(task_cache.get(id), crate::cache::Lookup::Missing) {
                continue;
            }
            fetch_one(id, &settings, &task_cache).await;
        }
    });
    RegistryHandle { tx, cache }
}

async fn fetch_one(id: u32, settings: &SrSettings, cache: &SchemaCache) {
    let mut backoff = FETCH_BACKOFF_INITIAL;
    for attempt in 1..=FETCH_ATTEMPTS {
        match schema_registry::get_schema_by_id_and_type(id, settings, SchemaType::Avro).await {
            Ok(registered) => {
                if !registered.references.is_empty() {
                    cache.insert_failed(
                        id,
                        format!(
                            "schema {id} uses {} registry reference(s), which etl-avro \
                             does not support yet",
                            registered.references.len()
                        ),
                    );
                    return;
                }
                match Schema::parse_str(&registered.schema) {
                    Ok(schema) => {
                        tracing::info!(schema_id = id, "schema fetched and compiled");
                        cache.insert_ready(CompiledSchema { id, schema });
                    }
                    Err(e) => {
                        cache.insert_failed(id, format!("schema {id} failed to parse: {e}"));
                    }
                }
                return;
            }
            Err(e) if e.retriable && attempt < FETCH_ATTEMPTS => {
                tracing::warn!(schema_id = id, attempt, error = %e, "registry fetch retry");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(FETCH_BACKOFF_MAX);
            }
            Err(e) => {
                tracing::warn!(schema_id = id, error = %e, "registry fetch failed");
                cache.insert_failed(id, format!("registry fetch for schema {id} failed: {e}"));
                return;
            }
        }
    }
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
                match Schema::parse_str(&registered.schema) {
                    Ok(schema) => {
                        tracing::info!(subject, schema_id = registered.id, "pre-warmed schema");
                        cache.insert_ready(CompiledSchema {
                            id: registered.id,
                            schema,
                        });
                    }
                    Err(e) => tracing::warn!(subject, error = %e, "pre-warm parse failed"),
                }
            }
            Ok(_) => {
                tracing::warn!(subject, "pre-warm skipped: schema references unsupported");
            }
            Err(e) => tracing::warn!(subject, error = %e, "pre-warm fetch failed"),
        }
    }
}
