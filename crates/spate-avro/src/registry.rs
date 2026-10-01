//! The registry fetcher: the only place that talks HTTP.
//!
//! Pipeline threads never touch the network. On a cache miss the
//! deserializer sends the schema id here (an unbounded, non-blocking send)
//! and returns [`DeserError::NotReady`](spate_core::error::DeserError); this
//! task fetches, parses, and publishes the schema into the shared cache,
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
//! or black-holed id cannot head-of-line-block every other id. Per-id
//! dedup (a fetch already in flight is never started twice) and per-id
//! backoff are preserved across the concurrency.

use crate::cache::{CompiledSchema, Lookup, SchemaCache};
use crate::config::AvroConfigError;
use reqwest::{StatusCode, Url};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls::{AlertDescription, CertificateError, ClientConfig, RootCertStore};
use rustls_native_certs::CertificateResult;
use serde::Deserialize;
use spate_core::config::redact;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
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
/// Bound on one registry request, from connect to the end of the body. A
/// fetch holds a slot in [`MAX_CONCURRENT_FETCHES`] until it ends.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Why the registry rejected this client, set at most once.
pub(crate) type Rejection = Arc<OnceLock<String>>;

/// Cloneable handle held by deserializers: request a fetch, read the cache.
#[derive(Clone, Debug)]
pub(crate) struct RegistryHandle {
    tx: mpsc::UnboundedSender<u32>,
    pub(crate) cache: Arc<SchemaCache>,
    pub(crate) rejection: Rejection,
}

impl RegistryHandle {
    /// Request an asynchronous fetch of `id`. Never blocks; duplicate
    /// requests are deduplicated by the fetcher. A dropped fetcher (I/O
    /// runtime shut down) makes this a no-op; the pipeline is draining.
    pub(crate) fn request(&self, id: u32) {
        let _ = self.tx.send(id);
    }
}

/// Registry connection settings.
#[derive(Clone)]
pub(crate) struct RegistryConfig {
    pub url: String,
    pub basic_auth: Option<(String, Option<String>)>,
    pub root_ca: Option<PathBuf>,
}

// Hand-written: the password and the URL userinfo are credentials. The
// destructure lists every field so a new one cannot reach `Debug` unredacted.
impl std::fmt::Debug for RegistryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let RegistryConfig {
            url,
            basic_auth,
            root_ca,
        } = self;
        f.debug_struct("RegistryConfig")
            .field("url", &redact::url(url))
            .field(
                "basic_auth",
                &basic_auth
                    .as_ref()
                    .map(|(user, password)| (user, redact::option(password))),
            )
            .field("root_ca", root_ca)
            .finish()
    }
}

impl RegistryConfig {
    /// The URL without userinfo, query or fragment, for error messages.
    pub(crate) fn display_url(&self) -> String {
        match reqwest::Url::parse(&self.url) {
            Ok(mut url) => {
                let _ = url.set_username("");
                let _ = url.set_password(None);
                url.set_query(None);
                url.set_fragment(None);
                url.to_string()
            }
            Err(_) => "(unparseable URL)".to_owned(),
        }
    }
}

/// Where one schema registry is and how to authenticate to it. Requests go
/// through the client [`http_client`] builds, which this does not hold.
pub(crate) struct Endpoint {
    /// The configured URL, userinfo included; reqwest sends the userinfo as
    /// basic auth.
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
    /// The registry answered with a status other than success.
    Status(StatusCode),
    /// A [`tls_rejection!`](spate_core::tls_rejection); the reason names the
    /// registry.
    Rejected(String),
    /// Anything else, rendered with its source chain.
    Transient(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Status(status) => write!(f, "the registry answered {status}"),
            Failure::Rejected(reason) | Failure::Transient(reason) => f.write_str(reason),
        }
    }
}

impl Endpoint {
    /// The endpoint at `cfg.url`. Fails when the URL is not an `http://` or
    /// `https://` URL.
    pub(crate) fn new(cfg: &RegistryConfig) -> Result<Endpoint, AvroConfigError> {
        // The detail never echoes the URL, which may carry credentials.
        let base = Url::parse(&cfg.url).map_err(|e| AvroConfigError::Invalid {
            detail: format!("registry.url is not a URL: {e}"),
        })?;
        if !matches!(base.scheme(), "http" | "https") {
            return Err(AvroConfigError::Invalid {
                detail: "registry.url must be an http:// or https:// URL".into(),
            });
        }
        Ok(Endpoint {
            base,
            basic_auth: cfg.basic_auth.clone(),
            registry: cfg.display_url().into(),
        })
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
            .expect("Endpoint::new admits only http(s) URLs")
            .pop_if_empty()
            .extend(segments);
        url
    }

    async fn get(&self, http: &reqwest::Client, url: Url) -> Result<RegistrySchema, Failure> {
        let mut request = http.get(url);
        if let Some((user, password)) = &self.basic_auth {
            request = request.basic_auth(user, password.as_deref());
        }
        let response = request.send().await.map_err(|e| self.failure(e))?;
        let status = response.status();
        if !status.is_success() {
            return Err(Failure::Status(status));
        }
        let body = response.bytes().await.map_err(|e| self.failure(e))?;
        serde_json::from_slice(&body)
            .map_err(|e| Failure::Transient(format!("unreadable registry response: {e}")))
    }

    fn failure(&self, e: reqwest::Error) -> Failure {
        match spate_core::tls_rejection!(rustls, &e) {
            Some(tls) => Failure::Rejected(tls_reason(&self.registry, tls)),
            None => Failure::Transient(chain(&e.without_url())),
        }
    }
}

/// The reason recorded for a TLS rejection by `registry`.
fn tls_reason(registry: &str, tls: &rustls::Error) -> String {
    match tls {
        rustls::Error::InvalidCertificate(cert) => {
            let hint = match cert {
                CertificateError::UnknownIssuer => {
                    "; add the issuing CA to the system trust store or `registry.tls.root_ca`"
                }
                _ => "",
            };
            format!(
                "schema registry {registry} presented a certificate the client rejects: \
                 {cert}{hint}"
            )
        }
        rustls::Error::AlertReceived(AlertDescription::CertificateRequired) => format!(
            "schema registry {registry} requires a client certificate, which the client does \
             not present: {tls}"
        ),
        _ => format!("the TLS handshake with schema registry {registry} failed: {tls}"),
    }
}

/// `e` and each error in its source chain, joined by `": "`.
fn chain(e: &(dyn Error + 'static)) -> String {
    let mut rendered = e.to_string();
    let mut next = e.source();
    while let Some(source) = next {
        rendered.push_str(": ");
        rendered.push_str(&source.to_string());
        next = source.source();
    }
    rendered
}

/// The HTTP client for the registry, verifying an `https://` registry
/// against the system trust store and `root_ca`.
pub(crate) fn http_client(root_ca: Option<&Path>) -> Result<reqwest::Client, AvroConfigError> {
    http_client_with(root_ca, rustls_native_certs::load_native_certs)
}

fn http_client_with(
    root_ca: Option<&Path>,
    system: impl FnOnce() -> CertificateResult,
) -> Result<reqwest::Client, AvroConfigError> {
    client_builder(system, root_ca)?
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|e| AvroConfigError::Registry { detail: chain(&e) })
}

/// The HTTP client builder, trusting the certificates in `root_ca` in
/// addition to the system roots. On macOS, Windows and Android it keeps
/// reqwest's platform verifier; elsewhere TLS is verified against
/// [`root_store`].
fn client_builder(
    system: impl FnOnce() -> CertificateResult,
    root_ca: Option<&Path>,
) -> Result<reqwest::ClientBuilder, AvroConfigError> {
    let builder = reqwest::Client::builder();
    if cfg!(target_os = "android") && root_ca.is_some() {
        return Err(AvroConfigError::Invalid {
            detail: "registry.tls.root_ca is not supported on Android".into(),
        });
    }
    let extra = match root_ca {
        Some(path) => read_root_ca(path)?,
        None => Vec::new(),
    };
    if cfg!(any(target_vendor = "apple", windows, target_os = "android")) {
        let certs = extra
            .iter()
            .map(|cert| reqwest::Certificate::from_der(cert))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AvroConfigError::Registry {
                detail: format!("registry.tls.root_ca: {e}"),
            })?;
        return Ok(builder.tls_certs_merge(certs));
    }
    let mut roots = root_store(system);
    roots.add_parsable_certificates(extra);
    // An `http://` registry needs these roots too: a redirect to `https://`
    // or an `https://` proxy connects over TLS.
    Ok(builder.tls_backend_preconfigured(client_config(roots)))
}

/// Every certificate in the PEM file at `path`. Fails when the file cannot be
/// read, holds no certificate, or holds one that is not a valid trust anchor.
fn read_root_ca(path: &Path) -> Result<Vec<CertificateDer<'static>>, AvroConfigError> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| root_ca_error(path, &e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| root_ca_error(path, &e.to_string()))?;
    if certs.is_empty() {
        return Err(root_ca_error(path, "no PEM certificates found"));
    }
    // The platform verifiers report a malformed certificate without its path.
    let mut check = RootCertStore::empty();
    for cert in &certs {
        check
            .add(cert.clone())
            .map_err(|e| root_ca_error(path, &e.to_string()))?;
    }
    Ok(certs)
}

fn root_ca_error(path: &Path, why: &str) -> AvroConfigError {
    AvroConfigError::Registry {
        detail: format!("registry.tls.root_ca `{}`: {why}", path.display()),
    }
}

/// The certificates `system` yields, or the Mozilla bundle when it yields none.
fn root_store(system: impl FnOnce() -> CertificateResult) -> RootCertStore {
    let loaded = system();
    if !loaded.errors.is_empty() {
        tracing::warn!(errors = ?loaded.errors, "deserializer.avro: errors reading the system trust store");
    }
    let mut roots = RootCertStore::empty();
    let (added, _unparsable) = roots.add_parsable_certificates(loaded.certs);
    if added == 0 {
        tracing::warn!(
            "deserializer.avro: the system trust store has no certificates; \
             verifying the registry against the Mozilla root bundle"
        );
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    roots
}

fn client_config(roots: RootCertStore) -> ClientConfig {
    // rustls has no process-wide default provider when a build enables both
    // `ring` and `aws-lc-rs`.
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports rustls's default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
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
    http: reqwest::Client,
    endpoint: Arc<Endpoint>,
    rejection: Rejection,
    negative_cache_ttl: Duration,
    runtime: &tokio::runtime::Handle,
) -> RegistryHandle {
    let cache = Arc::new(SchemaCache::new(negative_cache_ttl));
    let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
    let task_cache = Arc::clone(&cache);
    let task_rejection = Arc::clone(&rejection);
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
                        // The known panic source, apache-avro's
                        // `Schema::parse_str`, is caught in `fetch_one`. A
                        // `JoinError` carries no schema id, so that id stays in
                        // `in_flight` and is never refetched.
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
                    // Dedup: several pipeline threads may request the id before
                    // the first fetch lands, or it may already be cached.
                    if in_flight.contains(&id) {
                        continue;
                    }
                    if !matches!(task_cache.get(id), Lookup::Missing) {
                        continue;
                    }
                    // Honor per-id backoff after a transient failure.
                    if backoff.get(&id).is_some_and(|b| Instant::now() < b.next_allowed) {
                        continue;
                    }
                    in_flight.insert(id);
                    let cache = Arc::clone(&task_cache);
                    let http = http.clone();
                    let endpoint = Arc::clone(&endpoint);
                    let rejection = Arc::clone(&task_rejection);
                    tasks.spawn(async move {
                        let outcome = fetch_one(id, &http, &endpoint, &cache, &rejection).await;
                        (id, outcome)
                    });
                }
            }
        }
    });
    RegistryHandle {
        tx,
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
) -> FetchOutcome {
    let registry = endpoint.registry();
    match endpoint.get(http, endpoint.schema_url(id)).await {
        Ok(registered) => {
            if let Some(reason) = registered.unsupported(id) {
                cache.insert_failed(id, reason);
                return FetchOutcome::Resolved;
            }
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
        Err(Failure::Status(StatusCode::NOT_FOUND)) => {
            // Negative-cache the unknown id; the deserializer applies its
            // ErrorPolicy to the poison payload.
            tracing::warn!(schema_id = id, "registry reports schema id unknown");
            cache.insert_failed(
                id,
                format!(
                    "registry fetch for schema {id} failed: the registry answered 404 Not Found"
                ),
            );
            FetchOutcome::Resolved
        }
        Err(Failure::Status(status @ (StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN))) => {
            record(
                rejection,
                format!("schema registry {registry} answered {status} to the fetch of schema {id}"),
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
                let compiled = CompiledSchema::compile(id, &registered.schema);
                match compiled.unusable_reason() {
                    None => {
                        tracing::info!(subject, schema_id = id, "pre-warmed schema");
                        cache.insert_ready(compiled);
                    }
                    Some(reason) => {
                        tracing::warn!(subject, %reason, "pre-warm parse failed; skipping subject");
                    }
                }
            }
            Err(Failure::Status(status @ StatusCode::UNAUTHORIZED)) => record(
                rejection,
                format!(
                    "schema registry {registry} answered {status} to the pre-warm of subject \
                     {subject}"
                ),
            ),
            Err(Failure::Rejected(reason)) => record(rejection, reason),
            Err(failure) => tracing::warn!(subject, error = %failure, "pre-warm fetch failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use hyper::body::Bytes;
    use spate_test_support::{TestCa, native_certs, pem};

    const SCHEMA: &str = r#"{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}"#;

    fn config(url: &str, root_ca: Option<&Path>) -> RegistryConfig {
        RegistryConfig {
            url: url.to_owned(),
            basic_auth: None,
            root_ca: root_ca.map(Path::to_path_buf),
        }
    }

    /// Serves `SCHEMA` as every registry response on `127.0.0.1`, over a
    /// certificate `ca` signed, and returns the `https://` URL. With `clients`
    /// set, the server requires a client certificate that CA signed.
    async fn serve(ca: &TestCa, clients: Option<&TestCa>) -> String {
        let body = serde_json::json!({ "schema": SCHEMA }).to_string();
        let addr = spate_test_support::serve_tls(ca.server_config(clients), move |tls| {
            let body = body.clone();
            async move {
                let respond = hyper::service::service_fn(move |_| {
                    let body = Full::new(Bytes::from(body.clone()));
                    async move { Ok::<_, std::convert::Infallible>(hyper::Response::new(body)) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(tls), respond)
                    .await;
            }
        })
        .await;
        format!("https://{addr}")
    }

    /// Fetches schema 1 from `cfg`, with `system` standing in for the system
    /// trust store where the platform reads one.
    async fn fetch(
        cfg: &RegistryConfig,
        system: Vec<CertificateDer<'static>>,
    ) -> Result<String, Failure> {
        let http = http_client_with(cfg.root_ca.as_deref(), || native_certs(system)).unwrap();
        let endpoint = Endpoint::new(cfg).unwrap();
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

    /// Request paths extend the configured path, a trailing slash included,
    /// and a subject is one encoded segment.
    #[test]
    fn request_urls_extend_the_configured_path() {
        for base in ["https://sr:8081/registry", "https://sr:8081/registry/"] {
            let endpoint = Endpoint::new(&config(base, None)).unwrap();
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

    /// A URL that does not parse, or whose scheme is not `http` or `https`,
    /// fails at startup without echoing the URL.
    #[test]
    fn an_unusable_url_fails() {
        for url in ["not a url", "localhost:8081", "ftp://user:secret@sr"] {
            let err = Endpoint::new(&config(url, None))
                .err()
                .expect("the URL is rejected")
                .to_string();
            assert!(err.contains("registry.url"), "{err}");
            assert!(!err.contains("secret"), "{err}");
        }
    }

    /// An empty system store falls back to the Mozilla bundle, and a
    /// non-empty one keeps it out.
    #[test]
    fn an_empty_system_store_falls_back_to_the_mozilla_roots() {
        let roots = root_store(CertificateResult::default);
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
        assert_eq!(
            root_store(|| native_certs(vec![TestCa::new("sr").der()])).len(),
            1
        );
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
            let cfg = config("https://sr", Some(&path));
            let err = http_client_with(cfg.root_ca.as_deref(), CertificateResult::default)
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
        let fetched = fetch(&config(&url, Some(&ca.write(dir.path()))), vec![]).await;
        assert_eq!(fetched.expect("the private CA is trusted"), SCHEMA);
    }

    /// A registry whose CA is in neither the system store nor `root_ca` is
    /// rejected, and the reason names `registry.tls.root_ca`.
    #[tokio::test]
    async fn an_unknown_ca_is_rejected_with_the_root_ca_hint() {
        let dir = tempfile::tempdir().unwrap();
        let (registry, other) = (TestCa::new("registry"), TestCa::new("other"));
        let url = serve(&registry, None).await;
        let reason = rejected(fetch(&config(&url, Some(&other.write(dir.path()))), vec![]).await);
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
            match fetch(&config(&format!("https://{addr}"), None), vec![]).await {
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
        let reason =
            rejected(fetch(&config(&url, Some(&registry.write(dir.path()))), vec![]).await);
        assert!(reason.contains("CertificateRequired"), "{reason}");
    }

    #[cfg(not(any(target_vendor = "apple", windows, target_os = "android")))]
    mod system_store {
        use super::*;

        /// `root_ca` adds to the system roots: a registry signed by a CA in
        /// the system store stays trusted.
        #[tokio::test]
        async fn root_ca_is_merged_with_the_system_roots() {
            let dir = tempfile::tempdir().unwrap();
            let (system, extra) = (TestCa::new("system"), TestCa::new("extra"));
            let url = serve(&system, None).await;
            let cfg = config(&url, Some(&extra.write(dir.path())));
            let fetched = fetch(&cfg, vec![system.der()]).await;
            assert_eq!(fetched.expect("the system CA is trusted"), SCHEMA);
        }

        /// An `http://` registry that redirects to `https://` is verified
        /// against the system roots.
        #[tokio::test]
        async fn an_http_registry_redirected_to_https_is_trusted() {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let registry = TestCa::new("registry");
            let target = serve(&registry, None).await;
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
            let url = format!("http://127.0.0.1:{port}");
            let fetched = fetch(&config(&url, None), vec![registry.der()]).await;
            assert_eq!(fetched.expect("the redirect target is trusted"), SCHEMA);
        }

        /// An `https://` registry is trusted when its CA is among the system
        /// roots, and rejected when it is not.
        #[tokio::test]
        async fn an_https_registry_is_verified_against_the_system_roots() {
            let (registry, other) = (TestCa::new("registry"), TestCa::new("other"));
            let url = serve(&registry, None).await;
            let fetched = fetch(&config(&url, None), vec![registry.der()]).await;
            assert_eq!(fetched.expect("the registry's CA is trusted"), SCHEMA);
            rejected(fetch(&config(&url, None), vec![other.der()]).await);
        }
    }
}
