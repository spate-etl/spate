//! The registry fetcher: the only place that talks HTTP.
//!
//! Pipeline threads never touch the network. On a cache miss the
//! deserializer sends the schema id here (an unbounded, non-blocking send)
//! and returns [`DeserError::NotReady`](spate_core::error::DeserError); this
//! task fetches, parses, and publishes the schema into the shared cache,
//! and the driver's blocked-batch retry picks it up.
//!
//! `schema_registry_converter` is used strictly as the registry HTTP
//! client; its decoders never appear on the hot path.
//!
//! # Transient, permanent and rejected
//!
//! Only a *permanent* verdict about an id is negatively cached: the registry
//! answering `404` (unknown id/subject/version), a schema that uses
//! unsupported references, or a schema the parser rejects (`CompiledSchema`
//! pre-renders the reason). A *transient* outage
//! (any other 5xx, `429`, a timeout, a refused/black-holed connection)
//! leaves the id **absent** so the deserializer's next replay refetches it:
//! poisoning a transient blip would drop (and ack) perfectly decodable
//! records for the whole negative-cache TTL. Per-id backoff, held here in
//! the fetcher, keeps those replays from hot-looping the registry.
//!
//! A registry that answers `401`/`403`, or that rejects the TLS handshake
//! ([`tls_rejection!`](spate_core::tls_rejection)), is *rejected*: the reason
//! is recorded once in the handle's [`Rejection`], and every later cache miss
//! is fatal.
//!
//! # Concurrency
//!
//! Fetches run concurrently (up to [`MAX_CONCURRENT_FETCHES`]) so one slow
//! or black-holed id cannot head-of-line-block every other id. Per-id
//! dedup (a fetch already in flight is never started twice) and per-id
//! backoff are preserved across the concurrency.

use crate::cache::{CompiledSchema, Lookup, SchemaCache};
use crate::config::AvroConfigError;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls::{CertificateError, ClientConfig, RootCertStore};
use rustls_native_certs::CertificateResult;
use schema_registry_converter::async_impl::schema_registry::{self, SrSettings, SrSettingsBuilder};
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common::{SchemaType, SubjectNameStrategy};
use spate_core::config::redact;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
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

/// The registry client, verifying an `https://` registry against the system
/// trust store and `root_ca`. A TLS rejection ([`tls_rejection!`](spate_core::tls_rejection))
/// is recorded in `rejection`.
pub(crate) fn sr_settings(
    cfg: &RegistryConfig,
    rejection: &Rejection,
) -> Result<SrSettings, AvroConfigError> {
    sr_settings_with(cfg, rejection, rustls_native_certs::load_native_certs)
}

fn sr_settings_with(
    cfg: &RegistryConfig,
    rejection: &Rejection,
    system: impl FnOnce() -> CertificateResult,
) -> Result<SrSettings, AvroConfigError> {
    let mut builder: SrSettingsBuilder = SrSettings::new_builder(cfg.url.clone());
    if let Some((user, pass)) = &cfg.basic_auth {
        builder.set_basic_authorization(user, pass.as_deref());
    }
    let layer = RecordRejection {
        registry: cfg.display_url().into(),
        rejection: Arc::clone(rejection),
    };
    builder
        .build_with(client_builder(system, cfg.root_ca.as_deref())?.connector_layer(layer))
        .map_err(|e| AvroConfigError::Registry {
            detail: match e.cause {
                Some(cause) => format!("{}: {cause}", e.error),
                None => e.error,
            },
        })
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

/// A connector layer that records a TLS rejection in its
/// [`Rejection`] and passes every result through unchanged.
///
/// `schema_registry_converter` renders a request error with `Display`, which
/// drops the source chain, so the rejection is read here instead.
#[derive(Clone)]
struct RecordRejection {
    registry: Arc<str>,
    rejection: Rejection,
}

impl<S> tower_layer::Layer<S> for RecordRejection {
    type Service = RecordRejectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RecordRejectionService {
            inner,
            layer: self.clone(),
        }
    }
}

#[derive(Clone)]
struct RecordRejectionService<S> {
    inner: S,
    layer: RecordRejection,
}

type BoxError = Box<dyn Error + Send + Sync>;

impl<S, Req> tower_service::Service<Req> for RecordRejectionService<S>
where
    S: tower_service::Service<Req, Error = BoxError>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let connecting = self.inner.call(req);
        let layer = self.layer.clone();
        Box::pin(async move {
            connecting.await.inspect_err(|e| {
                let reason = match spate_core::tls_rejection!(rustls, e.as_ref()) {
                    Some(rustls::Error::InvalidCertificate(cert)) => {
                        let hint = match cert {
                            CertificateError::UnknownIssuer => {
                                "; add the issuing CA to the system trust store or \
                                 `registry.tls.root_ca`"
                            }
                            _ => "",
                        };
                        format!(
                            "schema registry {} presented a certificate the client rejects: \
                             {cert}{hint}",
                            layer.registry
                        )
                    }
                    Some(tls) => format!(
                        "the TLS handshake with schema registry {} failed: {tls}",
                        layer.registry
                    ),
                    None => return,
                };
                let _ = layer.rejection.set(reason);
            })
        })
    }
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
    settings: Arc<SrSettings>,
    registry: Arc<str>,
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
                    let settings = Arc::clone(&settings);
                    let registry = Arc::clone(&registry);
                    let rejection = Arc::clone(&task_rejection);
                    tasks.spawn(async move {
                        let outcome = fetch_one(id, &settings, &cache, &registry, &rejection).await;
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
    settings: &SrSettings,
    cache: &SchemaCache,
    registry: &str,
    rejection: &Rejection,
) -> FetchOutcome {
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
            // An unknown id (registry 404). Negative-cache it; the deserializer
            // applies its ErrorPolicy to the poison payload.
            tracing::warn!(schema_id = id, error = %e, "registry reports schema id unknown");
            cache.insert_failed(id, format!("registry fetch for schema {id} failed: {e}"));
            FetchOutcome::Resolved
        }
        Err(e) if is_auth_rejection(&e) => {
            let reason = format!(
                "schema registry {registry} rejected the fetch of schema {id}: {}",
                e.error
            );
            tracing::error!(schema_id = id, %reason, "registry rejected the client");
            let _ = rejection.set(reason);
            // Leave the id absent: a negative entry would reach the
            // ErrorPolicy, and Skip would drop the payload.
            FetchOutcome::Transient
        }
        Err(e) => {
            // Transient outage. Leave the id absent: poisoning it here would
            // drop (and ack) decodable records for the whole negative TTL. The
            // next replay refetches, subject to per-id backoff.
            tracing::warn!(schema_id = id, error = %e, "registry fetch failed transiently; will retry");
            FetchOutcome::Transient
        }
    }
}

/// Whether a registry error is a *permanent* verdict about the id (a `404`
/// not-found) rather than a transient outage.
///
/// `schema_registry_converter` (a 0.x dependency) does not expose the HTTP
/// status as a field, only formatting it into the error message
/// (`"...failed with status 404 Not Found"`), so we match on that. The match
/// is narrow: anything we cannot positively identify as a `404` is treated as
/// transient, because the safe failure mode is to refetch (a bounded stall),
/// never to negatively cache and silently drop valid records.
fn is_permanent(e: &SRCError) -> bool {
    e.error.contains("status 404")
}

/// Whether the registry refused the client's credentials or access (`401`,
/// `403`). Matched on the message for the reason [`is_permanent`] gives.
fn is_auth_rejection(e: &SRCError) -> bool {
    e.error.contains("status 401") || e.error.contains("status 403")
}

/// Fetch the latest version of every configured subject into the cache
/// (startup pre-warm). A `401` is recorded in `rejection`, and any recorded
/// rejection, a TLS rejection included, ends the pre-warm; any other failure
/// is logged, and the id is fetched on demand when it first appears in a
/// payload.
pub(crate) async fn prewarm(
    settings: &SrSettings,
    subjects: &[String],
    cache: &SchemaCache,
    registry: &str,
    rejection: &Rejection,
) {
    for subject in subjects {
        if rejection.get().is_some() {
            return;
        }
        let strategy = SubjectNameStrategy::RecordNameStrategy(subject.clone());
        match schema_registry::get_schema_by_subject(settings, &strategy).await {
            Ok(registered) if registered.references.is_empty() => {
                // Per-backend compile with the parse-panic guard inside (see
                // `fetch_one`), so one poison schema cannot kill this detached
                // task mid-list and skip the remaining pre-warm.
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
            Err(e) if e.error.contains("status 401") => {
                let reason = format!(
                    "schema registry {registry} rejected the pre-warm of subject {subject}: {}",
                    e.error
                );
                tracing::error!(subject, %reason, "registry rejected the client");
                let _ = rejection.set(reason);
            }
            Err(e) => tracing::warn!(subject, error = %e, "pre-warm fetch failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use http_body_util::Full;
    use hyper::body::Bytes;
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    const SCHEMA: &str = r#"{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}"#;

    fn loaded(certs: Vec<CertificateDer<'static>>) -> CertificateResult {
        let mut result = CertificateResult::default();
        result.certs = certs;
        result
    }

    fn config(url: &str, root_ca: Option<&Path>) -> RegistryConfig {
        RegistryConfig {
            url: url.to_owned(),
            basic_auth: None,
            root_ca: root_ca.map(Path::to_path_buf),
        }
    }

    struct TestCa {
        name: String,
        der: CertificateDer<'static>,
        issuer: Issuer<'static, KeyPair>,
    }

    impl TestCa {
        fn new(name: &str) -> TestCa {
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            params.distinguished_name.push(DnType::CommonName, name);
            let key = KeyPair::generate().unwrap();
            let der = params.self_signed(&key).unwrap().der().clone();
            TestCa {
                name: name.to_owned(),
                der,
                issuer: Issuer::new(params, key),
            }
        }

        /// Writes the CA certificate as PEM into `dir` and returns its path.
        fn write(&self, dir: &Path) -> PathBuf {
            let path = dir.join(format!("{}.pem", self.name));
            std::fs::write(&path, pem(&self.der)).unwrap();
            path
        }

        /// Serves `SCHEMA` as every registry response on `127.0.0.1`, over a
        /// certificate this CA signed, and returns the `https://` URL.
        async fn serve(&self) -> String {
            let key = KeyPair::generate().unwrap();
            // rcgen's default validity starts before 2019-07-01, which exempts
            // the leaf from Apple's 825-day limit.
            let mut params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            let leaf = params.signed_by(&key, &self.issuer).unwrap();
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
            )
            .unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let body = serde_json::json!({ "schema": SCHEMA }).to_string();
            tokio::spawn(async move {
                while let Ok((tcp, _)) = listener.accept().await {
                    let (acceptor, body) = (acceptor.clone(), body.clone());
                    tokio::spawn(async move {
                        let Ok(tls) = acceptor.accept(tcp).await else {
                            return;
                        };
                        let respond = hyper::service::service_fn(move |_| {
                            let body = Full::new(Bytes::from(body.clone()));
                            async move { Ok::<_, std::convert::Infallible>(hyper::Response::new(body)) }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(hyper_util::rt::TokioIo::new(tls), respond)
                            .await;
                    });
                }
            });
            format!("https://127.0.0.1:{port}")
        }
    }

    fn pem(der: &[u8]) -> String {
        let body = base64::engine::general_purpose::STANDARD.encode(der);
        let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
        for line in body.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(line).unwrap());
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
        pem
    }

    /// Fetches schema 1 from `cfg`, with `system` standing in for the system
    /// trust store where the platform reads one.
    async fn fetch(
        cfg: &RegistryConfig,
        system: Vec<CertificateDer<'static>>,
    ) -> (Result<String, SRCError>, Rejection) {
        let rejection = Rejection::default();
        let settings = sr_settings_with(cfg, &rejection, || loaded(system)).unwrap();
        let fetched = schema_registry::get_schema_by_id_and_type(1, &settings, SchemaType::Avro)
            .await
            .map(|registered| registered.schema);
        (fetched, rejection)
    }

    /// An empty system store falls back to the Mozilla bundle, and a
    /// non-empty one keeps it out.
    #[test]
    fn an_empty_system_store_falls_back_to_the_mozilla_roots() {
        let roots = root_store(CertificateResult::default);
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
        let cert = rcgen::generate_simple_self_signed(vec!["sr".to_owned()]).unwrap();
        assert_eq!(
            root_store(|| loaded(vec![cert.cert.der().clone()])).len(),
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
        std::fs::write(&malformed, pem(b"not DER")).unwrap();
        for path in [dir.path().join("missing.pem"), empty, malformed] {
            let cfg = config("https://sr", Some(&path));
            let err = sr_settings_with(&cfg, &Rejection::default(), CertificateResult::default)
                .unwrap_err()
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
        let url = ca.serve().await;
        let (fetched, _) = fetch(&config(&url, Some(&ca.write(dir.path()))), vec![]).await;
        assert_eq!(fetched.expect("the private CA is trusted"), SCHEMA);
    }

    /// A registry whose CA is in neither the system store nor `root_ca` is
    /// rejected, and the reason names `registry.tls.root_ca`.
    #[tokio::test]
    async fn an_unknown_ca_is_rejected_with_the_root_ca_hint() {
        let dir = tempfile::tempdir().unwrap();
        let (registry, other) = (TestCa::new("registry"), TestCa::new("other"));
        let url = registry.serve().await;
        let (fetched, rejection) =
            fetch(&config(&url, Some(&other.write(dir.path()))), vec![]).await;
        fetched.expect_err("an unknown CA is rejected");
        let reason = rejection.get().expect("the rejection is recorded");
        assert!(reason.contains("registry.tls.root_ca"), "{reason}");
    }

    /// A handshake alert that rejects the client is recorded with its name;
    /// `decode_error`, which reports a malformed message, is not.
    #[tokio::test]
    async fn a_rejecting_tls_alert_is_recorded() {
        use rustls::AlertDescription as A;
        for (alert, recorded) in [
            (A::HandshakeFailure, true),
            (A::ProtocolVersion, true),
            (A::DecodeError, false),
        ] {
            let addr = spate_test::tls_alert_server(b"", u8::from(alert));
            let (fetched, rejection) =
                fetch(&config(&format!("https://{addr}"), None), vec![]).await;
            fetched.expect_err("the server answers every handshake with an alert");
            match rejection.get() {
                Some(reason) => {
                    assert!(recorded, "{alert:?} recorded: {reason}");
                    assert!(reason.contains(&format!("{alert:?}")), "{reason}");
                }
                None => assert!(!recorded, "{alert:?} not recorded"),
            }
        }
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
            let url = system.serve().await;
            let cfg = config(&url, Some(&extra.write(dir.path())));
            let (fetched, _) = fetch(&cfg, vec![system.der.clone()]).await;
            assert_eq!(fetched.expect("the system CA is trusted"), SCHEMA);
        }

        /// An `http://` registry that redirects to `https://` is verified
        /// against the system roots.
        #[tokio::test]
        async fn an_http_registry_redirected_to_https_is_trusted() {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let registry = TestCa::new("registry");
            let target = registry.serve().await;
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
            let (fetched, _) = fetch(&config(&url, None), vec![registry.der.clone()]).await;
            assert_eq!(fetched.expect("the redirect target is trusted"), SCHEMA);
        }

        /// An `https://` registry is trusted when its CA is among the system
        /// roots, and rejected when it is not.
        #[tokio::test]
        async fn an_https_registry_is_verified_against_the_system_roots() {
            let (registry, other) = (TestCa::new("registry"), TestCa::new("other"));
            let url = registry.serve().await;
            let (fetched, _) = fetch(&config(&url, None), vec![registry.der.clone()]).await;
            assert_eq!(fetched.expect("the registry's CA is trusted"), SCHEMA);
            let (fetched, _) = fetch(&config(&url, None), vec![other.der.clone()]).await;
            fetched.expect_err("an unknown CA is rejected");
        }
    }
}
