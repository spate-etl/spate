//! The registry fetcher: the only place that talks HTTP.
//!
//! Pipeline threads never touch the network. On a cache miss the deserializer
//! asks for the schema id here (a non-blocking send on a bounded queue) and
//! returns a [`NotReady`](spate_core::error::DeserErrorKind::NotReady) error;
//! this task fetches, parses, and publishes the schema into the shared cache,
//! and the driver's blocked-batch retry picks it up.
//!
//! # Transient, permanent and rejected
//!
//! Only a *permanent* verdict about an id is negatively cached: the registry
//! answering `404` (unknown id/subject/version), a schema that is not Avro or
//! uses unsupported references, or a schema the parser rejects
//! (`CompiledSchema` pre-renders the reason). A *transient* outage
//! (any other 5xx, `429`, a timeout, a refused/black-holed connection)
//! leaves the id **absent** so the deserializer's next replay refetches it:
//! poisoning a transient blip would drop (and ack) perfectly decodable
//! records for the whole negative-cache TTL. Per-id backoff, held here in
//! the fetcher, keeps those replays from hot-looping the registry.
//!
//! A registry that answers `401`/`403`, or that rejects the TLS handshake
//! or the client after it ([`tls_rejection!`](spate_core::tls_rejection)),
//! is *rejected*: the reason is recorded once in the handle's [`Rejection`],
//! and every later cache miss is fatal.
//!
//! # Concurrency
//!
//! Fetches run concurrently (up to [`MAX_CONCURRENT_FETCHES`]) so one slow
//! or black-holed id cannot head-of-line-block every other id. The handle
//! queues an id once from its request until the fetcher settles or declines
//! it, so a fetch in flight is never started twice, and per-id backoff is
//! preserved across the concurrency.

use crate::cache::{CompiledSchema, Lookup, SchemaCache};
use crate::config::section_path;
use reqwest::{StatusCode, Url};
use rustls::AlertDescription;
use serde::Deserialize;
use spate_core::backoff::{Backoff, DEFAULT_JITTER};
use spate_core::clock::SyncClock;
use spate_core::config::{ConfigError, UrlRule};
use spate_core::error::chain;
use spate_core::hash::{QualityMap, QualitySet};
use spate_core::telemetry::{Decision, RateLimit};
use spate_core::tls::{CertificateDer, ClientRoots, SystemRoots, TrustError, client_roots};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Delay between fetches of an id after a transient registry failure, so
/// repeated NotReady replays cannot hot-loop the registry. Each id's copy
/// seeds its own jitter, so ids and replicas that fail together fetch apart.
const FETCH_BACKOFF: Backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(5))
    .with_jitter(DEFAULT_JITTER)
    .with_fresh_seed();
/// Maximum number of schema ids fetched concurrently. Bounds head-of-line
/// blocking (a slow id no longer stalls the rest) while keeping registry
/// load and open-socket count modest.
const MAX_CONCURRENT_FETCHES: usize = 4;
/// Bound on the part of an error response's body a [`Failure::Status`] keeps.
const ERROR_BODY_LIMIT: usize = 256;
/// Bound on one registry request, from connect to the end of the body. A
/// fetch holds a slot in [`MAX_CONCURRENT_FETCHES`] until it ends.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Most schema ids waiting on the fetcher at once, queued or being fetched.
/// Schema ids are wire data, and a registry that holds its requests open
/// keeps every waiting id here.
const MAX_PENDING: usize = 1024;

/// Why the registry rejected this client, set at most once.
pub(crate) type Rejection = Arc<OnceLock<String>>;

/// Cloneable handle held by deserializers: request a fetch, read the cache.
#[derive(Clone, Debug)]
pub(crate) struct RegistryHandle {
    tx: mpsc::Sender<u32>,
    pending: Pending,
    pub(crate) cache: Arc<SchemaCache>,
    pub(crate) rejection: Rejection,
}

impl RegistryHandle {
    /// A handle over `cache` with no fetcher behind it: a request goes
    /// nowhere.
    #[cfg(test)]
    pub(crate) fn detached(cache: Arc<SchemaCache>) -> Self {
        let (tx, _rx) = mpsc::channel(MAX_PENDING);
        RegistryHandle {
            tx,
            pending: Pending::default(),
            cache,
            rejection: Rejection::default(),
        }
    }

    /// Request an asynchronous fetch of `id`. Never blocks. A request for an
    /// id already pending, or made while [`MAX_PENDING`] ids are pending,
    /// does nothing: the payload's next replay asks again. Once the fetcher
    /// has ended, because the I/O runtime shut down or the fetcher panicked,
    /// this is a no-op.
    pub(crate) fn request(&self, id: u32) {
        let mut pending = self.pending.0.lock().expect("pending lock");
        if pending.len() >= MAX_PENDING || !pending.insert(id) {
            return;
        }
        if self.tx.try_send(id).is_err() {
            pending.remove(&id);
        }
    }

    /// How many ids are pending.
    #[cfg(test)]
    fn pending(&self) -> usize {
        self.pending.0.lock().expect("pending lock").len()
    }

    /// Whether `id` is pending.
    #[cfg(test)]
    fn is_pending(&self, id: u32) -> bool {
        self.pending.0.lock().expect("pending lock").contains(&id)
    }

    /// How many requests the fetcher has not received yet.
    #[cfg(test)]
    fn queued(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}

/// The schema ids queued for the fetcher or being fetched. An id enters on
/// its request and leaves when the fetcher settles or declines it, so the
/// queue holds each id at most once.
#[derive(Clone, Debug, Default)]
struct Pending(Arc<Mutex<QualitySet<u32>>>);

impl Pending {
    /// Lets `id` be requested again.
    fn forget(&self, id: u32) {
        self.0.lock().expect("pending lock").remove(&id);
    }
}

/// The rule `registry.url` meets.
const REGISTRY_URL: UrlRule<'static> =
    UrlRule::new("set them in `registry.username` and `registry.password`")
        .schemes(&["http", "https"]);

/// `registry.url` parsed and checked.
///
/// # Errors
///
/// [`ConfigError::Component`] at `deserializer.avro.registry.url` when it is
/// not an `http` or `https` URL, or carries userinfo.
pub(crate) fn registry_url(url: &str) -> Result<Url, ConfigError> {
    REGISTRY_URL
        .check(url)
        .map_err(|e| section_path().invalid("registry.url", e.to_string()))
}

/// Where one schema registry is and how to authenticate to it. Requests go
/// through the client [`http_client`] builds, which this does not hold.
pub(crate) struct Endpoint {
    /// The configured base URL used for registry requests.
    base: Url,
    basic_auth: Option<(String, Option<String>)>,
    /// The URL without credentials, for messages.
    registry: Arc<str>,
}

/// The fields of a registry schema response the fetcher reads.
#[derive(Deserialize)]
struct RegistrySchema {
    /// Absent from a by-id response.
    id: Option<u32>,
    schema: String,
    /// `AVRO` when absent.
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    references: Option<Vec<serde::de::IgnoredAny>>,
}

impl RegistrySchema {
    /// Why spate-avro cannot decode with schema `id`, if it cannot.
    fn unsupported(&self, id: u32) -> Option<String> {
        if let Some(kind) = self.schema_type.as_deref().filter(|kind| *kind != "AVRO") {
            return Some(format!(
                "schema {id} is a {kind} schema, which spate-avro does not decode"
            ));
        }
        let references = self.references.as_ref().map_or(0, Vec::len);
        (references > 0).then(|| {
            format!(
                "schema {id} uses {references} registry reference(s), which spate-avro does not \
                 support yet"
            )
        })
    }
}

/// Why a registry request failed.
#[derive(Debug)]
enum Failure {
    /// The registry answered with a status other than success. `body` is the
    /// start of the response body, at most [`ERROR_BODY_LIMIT`] bytes.
    Status { status: StatusCode, body: String },
    /// A [`tls_rejection!`](spate_core::tls_rejection); the reason names the
    /// registry.
    Rejected(String),
    /// Anything else, rendered with its source chain.
    Transient(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Status { status, body } if body.is_empty() => {
                write!(f, "the registry answered {status}")
            }
            Failure::Status { status, body } => write!(f, "the registry answered {status}: {body}"),
            Failure::Rejected(reason) | Failure::Transient(reason) => f.write_str(reason),
        }
    }
}

impl Endpoint {
    /// The endpoint at `base`, a URL [`registry_url`] returned.
    pub(crate) fn new(base: Url, basic_auth: Option<(String, Option<String>)>) -> Endpoint {
        // `registry_url` refused userinfo, so only the query and fragment
        // stay out of messages.
        let mut registry = base.clone();
        registry.set_query(None);
        registry.set_fragment(None);
        Endpoint {
            base,
            basic_auth,
            registry: registry.as_str().into(),
        }
    }

    /// The registry's URL, without credentials.
    pub(crate) fn registry(&self) -> &str {
        &self.registry
    }

    /// `GET /schemas/ids/{id}?deleted=true`: a soft-deleted schema still
    /// decodes the records written with it.
    fn schema_url(&self, id: u32) -> Url {
        let mut url = self.url(&["schemas", "ids", &id.to_string()]);
        url.set_query(Some("deleted=true"));
        url
    }

    /// `GET /subjects/{subject}/versions/latest`, with `subject` encoded as
    /// one path segment.
    fn latest_url(&self, subject: &str) -> Url {
        self.url(&["subjects", subject, "versions", "latest"])
    }

    /// `segments` appended to the configured URL's path.
    fn url(&self, segments: &[&str]) -> Url {
        let mut url = self.base.clone();
        url.set_query(None);
        url.set_fragment(None);
        url.path_segments_mut()
            .expect("registry_url admits only http(s) URLs")
            .pop_if_empty()
            .extend(segments);
        url
    }

    async fn get(&self, http: &reqwest::Client, url: Url) -> Result<RegistrySchema, Failure> {
        let mut request = http.get(url);
        if let Some((user, password)) = &self.basic_auth {
            request = request.basic_auth(user, password.as_deref());
        }
        let mut response = request.send().await.map_err(|e| self.failure(e))?;
        let status = response.status();
        if !status.is_success() {
            let mut body = Vec::new();
            while body.len() < ERROR_BODY_LIMIT {
                match response.chunk().await {
                    Ok(Some(chunk)) => body.extend_from_slice(&chunk),
                    _ => break,
                }
            }
            return Err(Failure::Status {
                status,
                body: excerpt(&body),
            });
        }
        let body = response.bytes().await.map_err(|e| self.failure(e))?;
        serde_json::from_slice(&body)
            .map_err(|e| Failure::Transient(format!("unreadable registry response: {e}")))
    }

    fn failure(&self, e: reqwest::Error) -> Failure {
        match spate_core::tls_rejection!(rustls, &e) {
            Some(tls) => Failure::Rejected(tls_reason(&self.registry, self.base.scheme(), tls)),
            None => Failure::Transient(chain(&e.without_url())),
        }
    }
}

/// The reason recorded for a TLS rejection by `registry`, whose URL has
/// `scheme`.
fn tls_reason(registry: &str, scheme: &str, tls: &rustls::Error) -> String {
    match tls {
        rustls::Error::InvalidCertificate(_) => {
            // The key is refused unless the registry URL is `https://`; an
            // `http://` registry reaches TLS through a redirect or a proxy.
            let key = (scheme == "https").then_some("registry.tls.root_ca");
            format!(
                "schema registry {registry} presented a certificate the client rejects: {}",
                spate_core::tls_reason!(rustls, tls, key)
            )
        }
        rustls::Error::AlertReceived(AlertDescription::CertificateRequired) => format!(
            "schema registry {registry} requires a client certificate, which the client does \
             not present: {tls}"
        ),
        _ => format!("the TLS handshake with schema registry {registry} failed: {tls}"),
    }
}

/// `body` as trimmed text, cut to at most [`ERROR_BODY_LIMIT`] bytes.
fn excerpt(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    let mut end = text.len().min(ERROR_BODY_LIMIT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].trim_end().to_owned()
}

/// The HTTP client for the registry at a URL with `scheme`, verifying TLS
/// against the system trust store and `root_ca`.
pub(crate) fn http_client(
    root_ca: Option<&Path>,
    scheme: &str,
) -> Result<reqwest::Client, ConfigError> {
    http_client_with(root_ca, scheme, SystemRoots::load)
}

fn http_client_with(
    root_ca: Option<&Path>,
    scheme: &str,
    system: impl FnOnce() -> SystemRoots,
) -> Result<reqwest::Client, ConfigError> {
    client_builder(system, root_ca, scheme)?
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|e| {
            section_path().invalid(
                "registry",
                format!("the registry client could not be built: {}", chain(&e)),
            )
        })
}

/// The HTTP client builder, trusting the certificates in `root_ca` in
/// addition to the system roots, as [`client_roots`] decides. Where
/// `PLATFORM_VERIFIER` is unset, an `https://` registry with no root fails.
fn client_builder(
    system: impl FnOnce() -> SystemRoots,
    root_ca: Option<&Path>,
    scheme: &str,
) -> Result<reqwest::ClientBuilder, ConfigError> {
    // An `http://` registry needs these roots too: a redirect to `https://`
    // or an `https://` proxy connects over TLS.
    let roots = client_roots(system, root_ca, scheme == "https").map_err(root_ca_error)?;
    let builder = reqwest::Client::builder();
    Ok(match roots {
        ClientRoots::Add(certs) => builder.tls_certs_merge(certificates(certs)?),
        ClientRoots::Only(certs) => builder.tls_certs_only(certificates(certs)?),
    })
}

fn certificates(
    certs: Vec<CertificateDer<'static>>,
) -> Result<Vec<reqwest::Certificate>, ConfigError> {
    certs
        .iter()
        .map(|cert| reqwest::Certificate::from_der(cert))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| section_path().invalid("registry.tls.root_ca", e.to_string()))
}

fn root_ca_error(e: TrustError) -> ConfigError {
    section_path().invalid("registry.tls.root_ca", e.to_string())
}

/// The limits on the fetcher's per-id log lines, on the fetcher's clock.
/// Schema ids are wire data, and an id evicted from the cache is fetched
/// again, so either line can recur per record.
struct FetchLogs {
    /// The warning for an id the registry does not know.
    unknown_id: RateLimit,
    /// The line for a schema fetched and compiled.
    fetched: RateLimit,
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

/// An id's fetch schedule and the instant its next fetch may start.
struct IdBackoff {
    schedule: Backoff,
    next_allowed: Instant,
}

impl IdBackoff {
    /// The backoff after an id's first transient failure at `now`.
    fn first(now: Instant) -> IdBackoff {
        let mut backoff = IdBackoff {
            schedule: FETCH_BACKOFF,
            next_allowed: now,
        };
        backoff.grow(now);
        backoff
    }

    /// Step the schedule after another transient failure at `now`.
    fn grow(&mut self, now: Instant) {
        // A deadline past the `Instant` range admits at once: the overflow
        // panic would end the fetcher and leave every uncached id NotReady.
        self.next_allowed = now.checked_add(self.schedule.next_delay()).unwrap_or(now);
    }

    /// Whether a fetch requested at `now` may start.
    fn admits(&self, now: Instant) -> bool {
        now >= self.next_allowed
    }
}

/// Clear `id`'s backoff on a resolved fetch, or start or step it on a
/// transient failure stamped with one read of `clock`.
fn settle(
    backoff: &mut QualityMap<u32, IdBackoff>,
    id: u32,
    outcome: FetchOutcome,
    clock: &SyncClock,
) {
    match outcome {
        FetchOutcome::Resolved => {
            backoff.remove(&id);
        }
        FetchOutcome::Transient => match backoff.get_mut(&id) {
            Some(b) => b.grow(clock.now()),
            None => {
                backoff.insert(id, IdBackoff::first(clock.now()));
            }
        },
    }
}

/// Spawn the fetcher task on `handle` and return the requester side.
pub(crate) fn spawn_fetcher(
    http: reqwest::Client,
    endpoint: Arc<Endpoint>,
    rejection: Rejection,
    negative_cache_ttl: Duration,
    clock: SyncClock,
    runtime: &tokio::runtime::Handle,
) -> RegistryHandle {
    let cache = Arc::new(SchemaCache::new(negative_cache_ttl, clock.clone()));
    let (tx, mut rx) = mpsc::channel::<u32>(MAX_PENDING);
    let pending = Pending::default();
    let task_pending = pending.clone();
    let task_cache = Arc::clone(&cache);
    let task_rejection = Arc::clone(&rejection);
    let logs = Arc::new(FetchLogs {
        unknown_id: RateLimit::hot_path(),
        fetched: RateLimit::new(100, Duration::from_secs(10)),
    });
    let fetcher = async move {
        // Per-id backoff after transient failures.
        let mut backoff: QualityMap<u32, IdBackoff> = QualityMap::default();
        let mut tasks: JoinSet<(u32, FetchOutcome)> = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                // Drain finished fetches first so slots free promptly.
                Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                    let (id, outcome) = match joined {
                        Ok(done) => done,
                        Err(err) => {
                            // The known panic source, apache-avro's
                            // `Schema::parse_str`, is caught in `fetch_one`. A
                            // `JoinError` carries no schema id, so that id stays
                            // pending and is never refetched.
                            if let Ok(payload) = err.try_into_panic() {
                                tracing::error!(
                                    panic = spate_core::task::panic_text(&*payload),
                                    "registry fetch task panicked"
                                );
                            }
                            continue;
                        }
                    };
                    // The backoff is recorded before the id can be asked
                    // for again.
                    settle(&mut backoff, id, outcome, &clock);
                    task_pending.forget(id);
                }
                maybe_id = rx.recv(), if tasks.len() < MAX_CONCURRENT_FETCHES => {
                    let Some(id) = maybe_id else {
                        // All deserializers dropped: the pipeline is draining.
                        break;
                    };
                    // Declined: the id was cached while it waited, or its
                    // backoff after a transient failure still holds.
                    if !matches!(task_cache.get(id), Lookup::Missing)
                        || backoff.get(&id).is_some_and(|b| !b.admits(clock.now()))
                    {
                        task_pending.forget(id);
                        continue;
                    }
                    let cache = Arc::clone(&task_cache);
                    let http = http.clone();
                    let endpoint = Arc::clone(&endpoint);
                    let rejection = Arc::clone(&task_rejection);
                    let logs = Arc::clone(&logs);
                    let clock = clock.clone();
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "the set belongs to the fetcher loop, so the loop's end drops it and aborts each per-id fetch"
                    )]
                    tasks.spawn(async move {
                        let outcome = fetch_one(
                            id,
                            &http,
                            &endpoint,
                            &cache,
                            &rejection,
                            &logs,
                            &clock,
                        )
                        .await;
                        (id, outcome)
                    });
                }
            }
        }
    };
    // Detached: every deserializer holds a clone of the `RegistryHandle`, so
    // no one clone can own the task. It ends when the last sender drops.
    spate_core::task::spawn_on("avro-registry-fetcher", fetcher, runtime).detach();
    RegistryHandle {
        tx,
        pending,
        cache,
        rejection,
    }
}

/// Fetch, parse, and publish schema `id`, or classify the failure. A single
/// HTTP attempt: transient failures are retried by the deserializer replaying
/// the payload (bounded by this id's backoff), not by blocking here, which
/// also stops one slow id from monopolizing a fetch slot for minutes.
async fn fetch_one(
    id: u32,
    http: &reqwest::Client,
    endpoint: &Endpoint,
    cache: &SchemaCache,
    rejection: &Rejection,
    logs: &FetchLogs,
    clock: &SyncClock,
) -> FetchOutcome {
    let registry = endpoint.registry();
    match endpoint.get(http, endpoint.schema_url(id)).await {
        Ok(registered) => {
            if let Some(reason) = registered.unsupported(id) {
                cache.insert_failed(id, reason);
                return FetchOutcome::Resolved;
            }
            let compiled = CompiledSchema::compile_for_cache(id, &registered.schema);
            match compiled.unusable_reason() {
                None => {
                    match logs.fetched.check_at(clock.now()) {
                        Decision::Allow {
                            suppressed_before: 0,
                        } => tracing::info!(schema_id = id, "schema fetched and compiled"),
                        Decision::Allow { suppressed_before } => tracing::info!(
                            schema_id = id,
                            suppressed = suppressed_before,
                            "schema fetched and compiled"
                        ),
                        Decision::Suppress => {}
                    }
                    cache.publish(compiled);
                }
                Some(reason) => {
                    cache.insert_failed(id, reason);
                }
            }
            FetchOutcome::Resolved
        }
        Err(
            failure @ Failure::Status {
                status: StatusCode::NOT_FOUND,
                ..
            },
        ) => {
            // Negative-cache the unknown id; the deserializer applies its
            // ErrorPolicy to the poison payload.
            spate_core::rate_limited_warn!(
                logs.unknown_id,
                at: clock.now(),
                schema_id = id,
                error = %failure,
                "registry reports schema id unknown"
            );
            cache.insert_failed(
                id,
                format!("registry fetch for schema {id} failed: {failure}"),
            );
            FetchOutcome::Resolved
        }
        Err(
            failure @ Failure::Status {
                status: StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN,
                ..
            },
        ) => {
            record(
                rejection,
                format!("schema registry {registry} refused the fetch of schema {id}: {failure}"),
            );
            // Leave the id absent: a negative entry would reach the
            // ErrorPolicy, and Skip would drop the payload.
            FetchOutcome::Transient
        }
        Err(Failure::Rejected(reason)) => {
            record(rejection, reason);
            FetchOutcome::Transient
        }
        Err(failure) => {
            // Leave the id absent: poisoning it here would drop (and ack)
            // decodable records for the whole negative TTL. The next replay
            // refetches, subject to per-id backoff.
            tracing::warn!(schema_id = id, error = %failure, "registry fetch failed transiently; will retry");
            FetchOutcome::Transient
        }
    }
}

/// Records `reason` as the client's rejection unless one is already set.
fn record(rejection: &Rejection, reason: String) {
    tracing::error!(%reason, "registry rejected the client");
    let _ = rejection.set(reason);
}

/// Fetch the latest version of every configured subject into the cache
/// (startup pre-warm). A `401` or a TLS rejection is recorded in
/// `rejection`, and any recorded rejection ends the pre-warm; any other
/// failure is logged, and the id is fetched on demand when it first appears
/// in a payload.
pub(crate) async fn prewarm(
    http: &reqwest::Client,
    endpoint: &Endpoint,
    subjects: &[String],
    cache: &SchemaCache,
    rejection: &Rejection,
) {
    let registry = endpoint.registry();
    for subject in subjects {
        if rejection.get().is_some() {
            return;
        }
        match endpoint.get(http, endpoint.latest_url(subject)).await {
            Ok(registered) => {
                let Some(id) = registered.id else {
                    tracing::warn!(
                        subject,
                        "pre-warm skipped: the response carries no schema id"
                    );
                    continue;
                };
                if let Some(reason) = registered.unsupported(id) {
                    tracing::warn!(subject, %reason, "pre-warm skipped");
                    continue;
                }
                // Per-backend compile with the parse-panic guard inside (see
                // `fetch_one`), so one poison schema cannot kill this detached
                // task mid-list and skip the remaining pre-warm.
                let compiled = CompiledSchema::compile_for_cache(id, &registered.schema);
                match compiled.unusable_reason() {
                    None => {
                        tracing::info!(subject, schema_id = id, "pre-warmed schema");
                        cache.publish(compiled);
                    }
                    Some(reason) => {
                        tracing::warn!(subject, %reason, "pre-warm parse failed; skipping subject");
                    }
                }
            }
            Err(
                failure @ Failure::Status {
                    status: StatusCode::UNAUTHORIZED,
                    ..
                },
            ) => record(
                rejection,
                format!(
                    "schema registry {registry} refused the pre-warm of subject {subject}: \
                     {failure}"
                ),
            ),
            Err(Failure::Rejected(reason)) => record(rejection, reason),
            Err(failure) => tracing::warn!(subject, error = %failure, "pre-warm fetch failed"),
        }
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "a test task ends when the test's runtime drops"
)]
#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use spate_core::clock::{Clock, TestClock};
    use spate_test_support::{TestCa, pem};

    const SCHEMA: &str = r#"{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}"#;

    /// The endpoint at `url`, which must pass `registry_url`.
    fn endpoint(url: &str) -> Endpoint {
        Endpoint::new(registry_url(url).unwrap(), None)
    }

    /// The fetch delays of one id over seven transient failures a second
    /// apart, each measured from its failure.
    fn seven_delays() -> Vec<Duration> {
        let clock = TestClock::frozen();
        let start = clock.now();
        let mut backoff = IdBackoff::first(start);
        let mut delays = vec![backoff.next_allowed - start];
        for _ in 1..=6 {
            clock.advance(Duration::from_secs(1));
            let now = clock.now();
            backoff.grow(now);
            delays.push(backoff.next_allowed - now);
        }
        delays
    }

    /// Whether `delay` lies in the jitter window of a `step_ms` step. The
    /// floor gives a nanosecond to the `f64` arithmetic the draw is made in.
    fn within_step(delay: Duration, step_ms: u64) -> bool {
        let step = Duration::from_millis(step_ms);
        let floor = step.mul_f64(1.0 - DEFAULT_JITTER) - Duration::from_nanos(1);
        floor <= delay && delay <= step
    }

    /// An id's fetch delay starts at 200 ms and doubles on each transient
    /// failure until it holds at the 5 s cap, each shortened by at most its
    /// jitter.
    #[test]
    fn id_backoff_doubles_from_200ms_to_the_5s_cap() {
        let delays = seven_delays();
        let steps = [200, 400, 800, 1600, 3200, 5000, 5000];
        assert!(
            delays.iter().zip(steps).all(|(&d, s)| within_step(d, s)),
            "{delays:?} against steps {steps:?}"
        );
    }

    /// Two ids failing at the same instants draw different delays, so they
    /// do not refetch together. With no jitter, or one seed for every copy,
    /// the two sequences match.
    #[test]
    fn two_ids_back_off_apart() {
        let (first, second) = (seven_delays(), seven_delays());
        assert_ne!(first, second, "two ids drew alike");
    }

    /// An id's backoff refuses a fetch until its clock reaches
    /// `next_allowed`, and admits one from that instant on.
    #[test]
    fn id_backoff_admits_from_next_allowed() {
        let backoff = IdBackoff::first(TestClock::frozen().now());
        let at = backoff.next_allowed;
        assert!(!backoff.admits(at - Duration::from_nanos(1)));
        assert!(backoff.admits(at));
        assert!(backoff.admits(at + Duration::from_secs(1)));
    }

    /// A resolved fetch clears the id's backoff, so a later transient
    /// failure waits the 200 ms step again.
    #[test]
    fn a_resolved_id_backs_off_from_200ms_again() {
        let clock = TestClock::frozen();
        let sync = SyncClock::new(clock.clone());
        let mut backoff = QualityMap::default();
        for _ in 0..7 {
            settle(&mut backoff, 15, FetchOutcome::Transient, &sync);
            clock.advance(Duration::from_secs(1));
        }
        settle(&mut backoff, 15, FetchOutcome::Resolved, &sync);
        clock.advance(Duration::from_secs(1));
        settle(&mut backoff, 15, FetchOutcome::Transient, &sync);
        let delay = backoff[&15].next_allowed - clock.now();
        assert!(within_step(delay, 200), "{delay:?}");
    }

    /// A second transient failure steps the id's backoff, so the next fetch
    /// waits the 400 ms step from that failure.
    #[test]
    fn a_second_transient_failure_waits_400ms() {
        let clock = TestClock::frozen();
        let sync = SyncClock::new(clock.clone());
        let mut backoff = QualityMap::default();
        settle(&mut backoff, 15, FetchOutcome::Transient, &sync);
        clock.advance(Duration::from_millis(200));
        settle(&mut backoff, 15, FetchOutcome::Transient, &sync);
        let delay = backoff[&15].next_allowed - clock.now();
        assert!(within_step(delay, 400), "{delay:?}");
    }

    /// A transient failure stamped where the delay would pass the end of the
    /// `Instant` range admits the next fetch at once.
    #[test]
    fn a_deadline_past_the_instant_range_admits_at_once() {
        let mut far = TestClock::frozen().now();
        let mut step = Duration::MAX;
        while !step.is_zero() {
            if let Some(later) = far.checked_add(step) {
                far = later;
            } else {
                step /= 2;
            }
        }
        assert!(IdBackoff::first(far).admits(far));
    }

    /// Serves `SCHEMA` as every registry response on `127.0.0.1`, over a
    /// certificate `ca` signed, and returns the `https://` URL. With `clients`
    /// set, the server requires a client certificate that CA signed. It is
    /// its own server because `spate_test_support::RegistryStub` serves plain
    /// HTTP only.
    async fn serve(ca: &TestCa, clients: Option<&TestCa>) -> String {
        let body = serde_json::json!({ "schema": SCHEMA }).to_string();
        let addr = spate_test_support::serve_tls(ca.server_config(clients), move |tls| {
            let body = body.clone();
            async move {
                let respond = hyper::service::service_fn(move |_| {
                    let body = Full::new(Bytes::from(body.clone()));
                    async move { Ok::<_, std::convert::Infallible>(hyper::Response::new(body)) }
                });
                let _ = http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(tls), respond)
                    .await;
            }
        })
        .await;
        format!("https://{addr}")
    }

    /// Fetches schema 1 from `url`, trusting `root_ca`, with `system`
    /// standing in for the system trust store where the platform reads one.
    async fn fetch(
        url: &str,
        root_ca: Option<&Path>,
        system: Vec<CertificateDer<'static>>,
    ) -> Result<String, Failure> {
        let scheme = url.split(':').next().unwrap();
        let http = http_client_with(root_ca, scheme, || SystemRoots::from_certs(system)).unwrap();
        let endpoint = endpoint(url);
        endpoint
            .get(&http, endpoint.schema_url(1))
            .await
            .map(|registered| registered.schema)
    }

    /// The reason of a [`Failure::Rejected`].
    ///
    /// # Panics
    ///
    /// Panics on a success or any other failure.
    fn rejected(fetched: Result<String, Failure>) -> String {
        match fetched {
            Err(Failure::Rejected(reason)) => reason,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    /// A registry that accepts the connection and never answers fails the
    /// fetch as transient once the request timeout elapses.
    #[tokio::test(start_paused = true)]
    async fn a_registry_that_never_answers_times_out() {
        let addr = spate_test_support::silent_server();
        let start = tokio::time::Instant::now();
        let fetched = tokio::time::timeout(
            REQUEST_TIMEOUT * 2,
            fetch(&format!("http://{addr}"), None, vec![]),
        )
        .await
        .expect("the request timeout ends the fetch");
        assert!(
            matches!(&fetched, Err(Failure::Transient(reason)) if reason.contains("timed out")),
            "{fetched:?}"
        );
        assert_eq!(start.elapsed(), REQUEST_TIMEOUT);
    }

    /// Answers every request with `head` and then `chunks` written in one
    /// write, and holds the connection open; returns the `http://` URL. It is
    /// its own server because `spate_test_support::RegistryStub` frames each
    /// response itself and closes the connection.
    async fn serve_raw(head: &'static str, chunks: Vec<u8>, drip: Option<Duration>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let chunks = chunks.clone();
                tokio::spawn(async move {
                    let mut request = [0u8; 4096];
                    let _ = tcp.read(&mut request).await;
                    let mut out = head.as_bytes().to_vec();
                    out.extend_from_slice(&chunks);
                    let _ = tcp.write_all(&out).await;
                    match drip {
                        Some(every) => loop {
                            tokio::time::sleep(every).await;
                            if tcp.write_all(b"1\r\n \r\n").await.is_err() {
                                break;
                            }
                        },
                        None => std::future::pending::<()>().await,
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    fn chunk(data: &[u8]) -> Vec<u8> {
        let mut out = format!("{:x}\r\n", data.len()).into_bytes();
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
        out
    }

    /// A registry that sends the headers and then drips the body fails the
    /// fetch once the request timeout elapses.
    #[tokio::test(start_paused = true)]
    async fn a_dripping_body_times_out() {
        let url = serve_raw(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
            chunk(b"{"),
            Some(REQUEST_TIMEOUT / 3),
        )
        .await;
        // A 1 ms ticker bounds each auto-advance of the paused clock, so I/O
        // completes before a request timer can fire.
        tokio::spawn(async {
            let mut tick = tokio::time::interval(Duration::from_millis(1));
            loop {
                tick.tick().await;
            }
        });
        let start = tokio::time::Instant::now();
        let fetched = tokio::time::timeout(REQUEST_TIMEOUT * 2, fetch(&url, None, vec![]))
            .await
            .expect("the request timeout ends the fetch");
        assert!(
            matches!(&fetched, Err(Failure::Transient(reason)) if reason.contains("timed out")),
            "{fetched:?}"
        );
        let elapsed = start.elapsed();
        assert!(
            elapsed >= REQUEST_TIMEOUT && elapsed < REQUEST_TIMEOUT + Duration::from_secs(1),
            "{elapsed:?}"
        );
    }

    /// An error body that arrives in several chunks and never ends is read
    /// until [`ERROR_BODY_LIMIT`] bytes have arrived, and no further chunk is
    /// awaited.
    #[tokio::test]
    async fn a_chunked_error_body_is_read_to_the_limit() {
        let mut chunks = chunk(&[b'a'; 10]);
        chunks.extend(chunk(&[b'b'; 300]));
        let url = serve_raw(
            "HTTP/1.1 500 Internal Server Error\r\nTransfer-Encoding: chunked\r\n\r\n",
            chunks,
            None,
        )
        .await;
        let fetched = tokio::time::timeout(spate_test::HANG, fetch(&url, None, vec![]))
            .await
            .expect("the fetch ends once the limit is read");
        match fetched {
            Err(Failure::Status { status, body }) => {
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                assert_eq!(body.len(), ERROR_BODY_LIMIT, "{body}");
                assert!(body.starts_with(&"a".repeat(10)), "{body}");
            }
            other => panic!("{other:?}"),
        }
    }

    /// A clock whose every read panics.
    struct PanickingClock;

    impl spate_core::clock::Clock for PanickingClock {
        fn now(&self) -> Instant {
            panic!("clock read")
        }
    }

    /// Pins that a panic in the fetcher task is logged at `ERROR` with the
    /// task's name. The fetcher reads the clock first when it records the
    /// backoff after a transient failure, here a `500`.
    #[test]
    fn a_fetcher_panic_is_logged_with_its_name() {
        let lines = spate_test::capture_logs(tracing::Level::ERROR, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap()
                .block_on(async {
                    let url = serve_raw(
                        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
                        Vec::new(),
                        None,
                    )
                    .await;
                    let handle = spawn_fetcher(
                        http_client(None, "http").unwrap(),
                        Arc::new(endpoint(&url)),
                        Rejection::default(),
                        Duration::from_secs(60),
                        SyncClock::new(Arc::new(PanickingClock)),
                        &tokio::runtime::Handle::current(),
                    );
                    handle.request(1);
                    // The receiver drops once the panicked task's future is
                    // dropped, after the panic is logged.
                    tokio::time::timeout(REQUEST_TIMEOUT * 2, handle.tx.closed())
                        .await
                        .expect("the fetcher ends");
                });
        });
        assert_eq!(
            lines,
            vec![
                r#"ERROR spate_core::task: background task panicked task="avro-registry-fetcher" panic="clock read""#
                    .to_owned()
            ]
        );
    }

    use std::sync::atomic::{AtomicBool, Ordering};

    /// A handle with no fetcher, and the receiver its requests queue on.
    fn unserved() -> (RegistryHandle, mpsc::Receiver<u32>) {
        let (tx, rx) = mpsc::channel(MAX_PENDING);
        let cache = SchemaCache::new(Duration::from_secs(60), SyncClock::system());
        let handle = RegistryHandle {
            tx,
            pending: Pending::default(),
            cache: Arc::new(cache),
            rejection: Rejection::default(),
        };
        (handle, rx)
    }

    /// A fetcher on the current runtime against a registry at `addr`, with
    /// its backoff and negative cache on `clock`.
    fn fetcher_at(addr: std::net::SocketAddr, clock: &Arc<TestClock>) -> RegistryHandle {
        spawn_fetcher(
            http_client(None, "http").unwrap(),
            Arc::new(endpoint(&format!("http://{addr}"))),
            Rejection::default(),
            Duration::from_secs(600),
            SyncClock::new(Arc::clone(clock) as _),
            &tokio::runtime::Handle::current(),
        )
    }

    /// Runs the fetcher until it has received every queued request it can.
    async fn until_queued(handle: &RegistryHandle, queued: usize) {
        for _ in 0..1_000 {
            if handle.queued() == queued {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "{} requests stay queued, expected {queued}",
            handle.queued()
        );
    }

    /// Replays of one payload queue its id once.
    #[test]
    fn a_repeated_request_queues_the_id_once() {
        let (handle, _rx) = unserved();
        for _ in 0..1_000 {
            handle.request(7);
        }
        assert_eq!((handle.pending(), handle.queued()), (1, 1));
    }

    /// The set takes ids up to [`MAX_PENDING`] and drops a request past it.
    #[test]
    fn requests_past_the_bound_are_dropped() {
        let (handle, _rx) = unserved();
        let bound = u32::try_from(MAX_PENDING).unwrap();
        for id in 1..bound {
            handle.request(id);
        }
        assert_eq!(handle.pending(), MAX_PENDING - 1);
        handle.request(bound);
        assert_eq!(
            (handle.pending(), handle.queued()),
            (MAX_PENDING, MAX_PENDING)
        );
        handle.request(bound + 1);
        assert_eq!(
            (handle.pending(), handle.queued()),
            (MAX_PENDING, MAX_PENDING)
        );
        assert!(!handle.is_pending(bound + 1));
    }

    /// A request made after the fetcher ended holds nothing.
    #[test]
    fn a_request_after_the_fetcher_ended_leaves_nothing_pending() {
        let (handle, rx) = unserved();
        drop(rx);
        handle.request(7);
        assert_eq!(handle.pending(), 0);
    }

    /// An id that gains a cache entry while it waits behind four running
    /// fetches is declined when the fetcher reaches it, and can then be
    /// requested again.
    #[tokio::test(start_paused = true)]
    async fn an_id_cached_while_queued_leaves_the_set() {
        let clock = TestClock::frozen();
        let handle = fetcher_at(spate_test_support::silent_server(), &clock);
        for id in 1..=4 {
            handle.request(id);
        }
        until_queued(&handle, 0).await;
        handle.request(5);
        until_queued(&handle, 1).await;
        handle.cache.insert_failed(5, "unknown".into());

        // The four fetches end at the request timeout, and the fetcher then
        // receives id 5.
        tokio::time::sleep(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
        until_queued(&handle, 0).await;
        assert_eq!(handle.pending(), 0);
    }

    /// An id requested inside its backoff is declined, and can then be
    /// requested again.
    #[test]
    fn an_id_declined_in_backoff_leaves_the_set() {
        let stub = spate_test_support::RegistryStub::start();
        stub.script("/schemas/ids/7", 500, "{}", 0);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let clock = TestClock::frozen();
        let handle = {
            let _on = runtime.enter();
            fetcher_at(stub.addr(), &clock)
        };
        handle.request(7);
        spate_test::wait_until(spate_test::HANG, "the failed fetch settles", || {
            stub.path_hits("/schemas/ids/7") == 1 && handle.pending() == 0
        });

        handle.request(7);
        spate_test::wait_until(spate_test::HANG, "the fetcher declines the id", || {
            handle.pending() == 0
        });
        assert_eq!(stub.path_hits("/schemas/ids/7"), 1);
    }

    /// A request dropped at the bound is taken once a fetch has settled.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_request_is_taken_once_the_set_has_room() {
        let clock = TestClock::frozen();
        let handle = fetcher_at(spate_test_support::silent_server(), &clock);
        let bound = u32::try_from(MAX_PENDING).unwrap();
        for id in 1..=bound {
            handle.request(id);
        }
        handle.request(bound + 1);
        assert!(!handle.is_pending(bound + 1));
        // Four ids are being fetched, so the queue has room and the set
        // has none.
        until_queued(&handle, MAX_PENDING - MAX_CONCURRENT_FETCHES).await;
        handle.request(bound + 1);
        assert!(!handle.is_pending(bound + 1));

        tokio::time::sleep(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
        assert_eq!(handle.pending(), MAX_PENDING - MAX_CONCURRENT_FETCHES);
        handle.request(bound + 1);
        assert!(handle.is_pending(bound + 1));
    }

    /// Eight ids replayed every 2 ms against a registry that never answers
    /// stay eight pending ids across five request timeouts, while their
    /// fetches keep being retried.
    #[tokio::test(start_paused = true)]
    async fn ids_keep_cycling_through_timeouts_within_the_bound() {
        use std::sync::atomic::AtomicUsize;
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&accepted);
        let registry = spate_test_support::serve_each(move |mut stream| {
            count.fetch_add(1, Ordering::SeqCst);
            let _ = std::io::copy(&mut stream, &mut std::io::sink());
        });
        let clock = TestClock::frozen();
        let handle = fetcher_at(registry, &clock);
        let replay = Duration::from_millis(2);
        let replays = (REQUEST_TIMEOUT * 5).as_millis() / replay.as_millis();
        for _ in 0..replays {
            for id in 1..=8 {
                handle.request(id);
            }
            assert!(
                handle.pending() <= 8 && handle.queued() <= 8,
                "{} pending, {} queued",
                handle.pending(),
                handle.queued()
            );
            clock.advance(replay);
            tokio::time::sleep(replay).await;
        }
        assert!(
            accepted.load(Ordering::SeqCst) > 8,
            "the fetches were retried: {accepted:?}"
        );
    }

    /// A clock that records its first read and then panics.
    struct FlaggingPanicClock(Arc<AtomicBool>);

    impl spate_core::clock::Clock for FlaggingPanicClock {
        fn now(&self) -> Instant {
            self.0.store(true, Ordering::SeqCst);
            panic!("clock read")
        }
    }

    /// Pins that a panic in a per-id fetch task reaches `tracing` with its
    /// panic text. A `404` makes the fetch task read the clock for its
    /// warning, so the panic is raised inside the fetch task.
    #[test]
    fn a_fetch_task_panic_is_logged_with_its_text() {
        let lines = spate_test::capture_logs(tracing::Level::ERROR, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let read = Arc::new(AtomicBool::new(false));
            let handle = rt.block_on(async {
                let url = serve_raw(
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
                    Vec::new(),
                    None,
                )
                .await;
                spawn_fetcher(
                    http_client(None, "http").unwrap(),
                    Arc::new(endpoint(&url)),
                    Rejection::default(),
                    Duration::from_secs(60),
                    SyncClock::new(Arc::new(FlaggingPanicClock(Arc::clone(&read)))),
                    &tokio::runtime::Handle::current(),
                )
            });
            handle.request(1);
            // The fetcher runs on `rt`, so each check first drives it.
            let drive =
                || rt.block_on(async { tokio::time::sleep(Duration::from_millis(1)).await });
            spate_test::wait_until(spate_test::HANG, "the fetch task reads the clock", || {
                drive();
                read.load(Ordering::SeqCst)
            });
            // The fetcher drains the finished fetch before it sees the closed
            // channel, so it logs before it drops its cache clone.
            let cache = Arc::clone(&handle.cache);
            drop(handle);
            spate_test::wait_until(spate_test::HANG, "the fetcher ends", || {
                drive();
                Arc::strong_count(&cache) == 1
            });
        });
        assert_eq!(
            lines,
            vec![
                r#"ERROR spate_avro::registry: registry fetch task panicked panic="clock read""#
                    .to_owned()
            ]
        );
    }

    /// An error body is kept trimmed and cut to [`ERROR_BODY_LIMIT`] bytes on
    /// a character boundary.
    #[test]
    fn an_error_body_excerpt_is_bounded() {
        assert_eq!(
            excerpt(b"  {\"error_code\":40403}\n"),
            r#"{"error_code":40403}"#
        );
        let long = format!("a{}", "é".repeat(ERROR_BODY_LIMIT));
        let cut = excerpt(long.as_bytes());
        assert_eq!(cut.len(), ERROR_BODY_LIMIT - 1);
        assert!(cut.chars().skip(1).all(|c| c == 'é'));
    }

    /// Request paths extend the configured path, a trailing slash included,
    /// and a subject is one encoded segment.
    #[test]
    fn request_urls_extend_the_configured_path() {
        for base in ["https://sr:8081/registry", "https://sr:8081/registry/"] {
            let endpoint = endpoint(base);
            assert_eq!(
                endpoint.schema_url(7).as_str(),
                "https://sr:8081/registry/schemas/ids/7?deleted=true"
            );
            assert_eq!(
                endpoint.latest_url("a/b c?").as_str(),
                "https://sr:8081/registry/subjects/a%2Fb%20c%3F/versions/latest"
            );
        }
    }

    /// Pins each refusal of `registry.url` by its text, none echoing the URL.
    #[test]
    fn an_unusable_url_fails() {
        for (url, expected) in [
            ("not a url", "is not a URL: relative URL without a base"),
            (
                "https://user:secret@sr:99999",
                "is not a URL: invalid port number",
            ),
            ("localhost:8081", "has no `//` after its scheme"),
            ("user:secret@sr:8081", "has no `//` after its scheme"),
            (
                "ftp://user:secret@sr",
                "must be a URL with scheme http or https; found ftp",
            ),
            (
                "https://user:secret@sr",
                "carries userinfo; set them in `registry.username` and \
                 `registry.password`",
            ),
        ] {
            let err = registry_url(url)
                .expect_err("the URL is rejected")
                .to_string();
            assert_eq!(
                err,
                format!("deserializer.avro.registry.url: {expected}"),
                "{url}"
            );
        }
    }

    /// Messages name the registry without the URL's query or fragment.
    #[test]
    fn the_registry_name_drops_the_query_and_fragment() {
        let endpoint = endpoint("https://sr:8081/registry?token=t#f");
        assert_eq!(endpoint.registry(), "https://sr:8081/registry");
    }

    /// An `http://` registry builds a client with no usable system root,
    /// so a plaintext pipeline runs on an image with no CA bundle.
    #[test]
    fn an_http_registry_builds_with_an_empty_system_store() {
        let unparsable = CertificateDer::from(vec![0u8; 8]);
        for system in [Vec::new(), vec![unparsable]] {
            http_client_with(None, "http", || SystemRoots::from_certs(system))
                .expect("an http registry needs no root");
        }
    }

    /// A `root_ca` that is missing, holds no certificate, or holds a malformed
    /// one fails with its path in the message.
    #[test]
    fn an_unusable_root_ca_fails() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        let malformed = dir.path().join("malformed.pem");
        std::fs::write(&malformed, pem("CERTIFICATE", b"not DER")).unwrap();
        for path in [dir.path().join("missing.pem"), empty, malformed] {
            let err =
                http_client_with(Some(&path), "https", || SystemRoots::from_certs(Vec::new()))
                    .expect_err("the root CA is rejected")
                    .to_string();
            assert!(err.contains("registry.tls.root_ca"), "{err}");
            assert!(err.contains(&path.display().to_string()), "{err}");
        }
    }

    /// A registry whose certificate chains to `root_ca` is trusted with an
    /// empty system store.
    #[tokio::test]
    async fn a_registry_signed_by_root_ca_is_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("private");
        let url = serve(&ca, None).await;
        let fetched = fetch(&url, Some(&ca.write(dir.path())), vec![]).await;
        assert_eq!(fetched.expect("the private CA is trusted"), SCHEMA);
    }

    /// A registry whose CA is in neither the system store nor `root_ca` is
    /// rejected, and the reason names `registry.tls.root_ca`.
    #[tokio::test]
    async fn an_unknown_ca_is_rejected_with_the_root_ca_hint() {
        let dir = tempfile::tempdir().unwrap();
        let (registry, other) = (TestCa::new("registry"), TestCa::new("other"));
        let url = serve(&registry, None).await;
        let reason = rejected(fetch(&url, Some(&other.write(dir.path())), vec![]).await);
        assert!(reason.contains("registry.tls.root_ca"), "{reason}");
    }

    /// A handshake alert that rejects the client is a rejection that names
    /// it; `decode_error`, which reports a malformed message, is transient.
    #[tokio::test]
    async fn a_rejecting_tls_alert_is_a_rejection() {
        use rustls::AlertDescription as A;
        for (alert, is_rejection) in [
            (A::HandshakeFailure, true),
            (A::ProtocolVersion, true),
            (A::DecodeError, false),
        ] {
            let addr = spate_test::tls_alert_server(b"", u8::from(alert));
            let unrelated = vec![TestCa::new("unrelated").der()];
            match fetch(&format!("https://{addr}"), None, unrelated).await {
                Err(Failure::Rejected(reason)) => {
                    assert!(is_rejection, "{alert:?} rejected: {reason}");
                    assert!(reason.contains(&format!("{alert:?}")), "{reason}");
                }
                Err(Failure::Transient(_)) => assert!(!is_rejection, "{alert:?} transient"),
                other => panic!("{alert:?}: {other:?}"),
            }
        }
    }

    /// A TLS 1.3 registry that refuses the client for presenting no
    /// certificate is a rejection, and the reason names `CertificateRequired`.
    #[tokio::test]
    async fn a_refused_client_certificate_is_a_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let registry = TestCa::new("registry");
        let url = serve(&registry, Some(&TestCa::new("clients"))).await;
        let reason = rejected(fetch(&url, Some(&registry.write(dir.path())), vec![]).await);
        assert!(reason.contains("CertificateRequired"), "{reason}");
        assert!(reason.contains("requires a client certificate"), "{reason}");
    }

    /// The client takes `system` only where `PLATFORM_VERIFIER` is unset;
    /// elsewhere reqwest's platform verifier reads the OS store, so the tests
    /// that inject system roots return at once.
    mod system_store {
        use super::*;
        use spate_core::tls::PLATFORM_VERIFIER;

        /// An `http://` server on `127.0.0.1` that answers every request with
        /// a `307` to the same path under `target`, and returns its URL. It is
        /// its own server because a `spate_test_support::RegistryStub` script
        /// sets no `Location` header.
        async fn redirect_to(target: String) -> String {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move {
                while let Ok((mut tcp, _)) = listener.accept().await {
                    let mut request = [0u8; 4096];
                    let n = tcp.read(&mut request).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&request[..n]);
                    let path = request.split_whitespace().nth(1).unwrap_or("/");
                    let response = format!(
                        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {target}{path}\r\n\
                         Content-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    let _ = tcp.write_all(response.as_bytes()).await;
                }
            });
            format!("http://127.0.0.1:{port}")
        }

        /// `root_ca` adds to the system roots: a registry signed by a CA in
        /// the system store stays trusted.
        #[tokio::test]
        async fn root_ca_is_merged_with_the_system_roots() {
            if PLATFORM_VERIFIER {
                return;
            }
            let dir = tempfile::tempdir().unwrap();
            let (system, extra) = (TestCa::new("system"), TestCa::new("extra"));
            let url = serve(&system, None).await;
            let fetched = fetch(&url, Some(&extra.write(dir.path())), vec![system.der()]).await;
            assert_eq!(fetched.expect("the system CA is trusted"), SCHEMA);
        }

        /// An `http://` registry that redirects to `https://` is verified
        /// against the system roots.
        #[tokio::test]
        async fn an_http_registry_redirected_to_https_is_trusted() {
            if PLATFORM_VERIFIER {
                return;
            }
            let registry = TestCa::new("registry");
            let target = serve(&registry, None).await;
            let url = redirect_to(target).await;
            let fetched = fetch(&url, None, vec![registry.der()]).await;
            assert_eq!(fetched.expect("the redirect target is trusted"), SCHEMA);
        }

        /// An `https://` registry is trusted when its CA is among the system
        /// roots, and rejected when it is not.
        #[tokio::test]
        async fn an_https_registry_is_verified_against_the_system_roots() {
            if PLATFORM_VERIFIER {
                return;
            }
            let (registry, other) = (TestCa::new("registry"), TestCa::new("other"));
            let url = serve(&registry, None).await;
            let fetched = fetch(&url, None, vec![registry.der()]).await;
            assert_eq!(fetched.expect("the registry's CA is trusted"), SCHEMA);
            rejected(fetch(&url, None, vec![other.der()]).await);
        }

        /// An `https://` registry with no usable system root and no `root_ca`
        /// fails at client build, at `registry.tls.root_ca`, with the fixes
        /// named. With the platform verifier it builds, because that verifier
        /// reads the OS store.
        #[test]
        fn an_https_registry_with_no_trust_root_fails() {
            let unparsable = CertificateDer::from(vec![0u8; 8]);
            for (system, problem) in [
                (Vec::new(), "the system trust store has no certificates"),
                (
                    vec![unparsable],
                    "none of the 1 certificates in the system trust store parsed",
                ),
            ] {
                let built = http_client_with(None, "https", || SystemRoots::from_certs(system));
                if PLATFORM_VERIFIER {
                    built.expect("the platform verifier reads the OS store");
                    continue;
                }
                let err = built.expect_err("no root to verify against").to_string();
                assert_eq!(
                    err,
                    format!(
                        "deserializer.avro.registry.tls.root_ca: {problem}; install a \
                         CA bundle, point SSL_CERT_FILE at one, or set this key to the issuing CA"
                    )
                );
            }
        }

        /// An `http://` registry redirected to an `https://` one whose CA is
        /// not trusted, whether the system store holds another CA or none, is
        /// rejected with a hint that leaves out `registry.tls.root_ca`, which
        /// an `http://` URL refuses.
        #[tokio::test]
        async fn a_redirect_to_an_unknown_ca_omits_the_root_ca_hint() {
            if PLATFORM_VERIFIER {
                return;
            }
            let (registry, other) = (TestCa::new("registry"), TestCa::new("other"));
            let target = serve(&registry, None).await;
            let url = redirect_to(target).await;
            for system in [vec![other.der()], Vec::new()] {
                let reason = rejected(fetch(&url, None, system).await);
                assert!(
                    reason.ends_with("add the issuing CA to the system trust store"),
                    "{reason}"
                );
            }
        }
    }
}
