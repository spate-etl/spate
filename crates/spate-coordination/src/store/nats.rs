//! NATS JetStream KV [`CoordinationStore`]: the production backend.
//!
//! Two KV buckets per job carry the two keyspaces:
//!
//! - `spate_coordination_{job}_state` — durable: no age limit; split
//!   records and the plan record survive owner death. A delete writes a
//!   marker with a per-message TTL of `lease_ttl` once the bucket allows
//!   message TTLs, which connecting enables on a bucket that lacks them.
//! - `spate_coordination_{job}_lease` — ephemeral: bucket-level
//!   `max_age = lease_ttl` with limit markers (server >= 2.11). NATS KV
//!   cannot re-arm a per-key TTL on update, but `max_age` applies per
//!   *message*, so every CAS rewrite restarts the key's clock (that IS
//!   the heartbeat), an untouched key expires, and the expiry surfaces to
//!   watchers as a marker (`Operation::Purge`; graceful deletes surface
//!   as `Operation::Delete`). The `nats_spike` integration test pins all
//!   of these observations against a real server.
//!
//! Construction is synchronous and lazy: the connection and bucket
//! provisioning happen on the first store operation, the coordinator's
//! startup probe, so an unreachable server rides the startup retry budget.
//! Misconfiguration, and a credential or certificate the connection
//! rejects, are Fatal with an actionable message. No `async-nats` type
//! appears in any public signature (0.x policy: single pinned minor,
//! internal only).
//!
//! TLS connections use rustls with the `ring` provider, whatever other rustls
//! features the build enables.

use super::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use async_nats::jetstream::stream::LastRawMessageErrorKind;
use async_nats::jetstream::{kv, stream};
use futures_util::StreamExt as _;
use serde::Deserialize;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(test)]
mod test_tls;
mod tls;

/// The NATS server floor: per-message TTLs and limit markers shipped in
/// 2.11, and marker precision is one second, so leases below 2s are
/// dominated by server-side granularity.
const MIN_SERVER: (u64, u64) = (2, 11);
const MIN_LEASE: Duration = Duration::from_secs(2);

/// Hard cap on stored values: descriptor + base64 + record envelope must
/// stay far below NATS's 1 MiB message ceiling.
const MAX_VALUE_BYTES: i32 = 512 * 1024;

/// A secret that never prints: `Debug`/`Display` render `<redacted>`.
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// Wrap a secret value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Secret {
        Secret(value.into())
    }

    fn reveal(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// How the client authenticates to the NATS cluster.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NatsCredentials {
    /// Anonymous (dev clusters).
    #[default]
    None,
    /// Username and password.
    UserPassword {
        /// The username.
        username: String,
        /// The password (redacted from all Debug output).
        password: Secret,
    },
    /// A bearer token.
    Token(Secret),
    /// A `.creds` file (NKey + JWT), the NATS-native mechanism.
    CredsFile(PathBuf),
}

/// TLS material for the NATS connection. Presence of this section
/// requires TLS on every server.
///
/// Construct with [`NatsTls::default`] and set fields. The struct is
/// `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct NatsTls {
    /// PEM bundle of root CAs trusted in addition to the system trust store.
    pub root_ca: Option<PathBuf>,
    /// Client certificate (PEM), for mutual TLS.
    pub client_cert: Option<PathBuf>,
    /// Client key (PEM), paired with `client_cert`.
    pub client_key: Option<PathBuf>,
}

/// Connection and job configuration for the NATS backend.
///
/// Construct with [`NatsConfig::new`] and set the optional fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without
/// breaking callers.
///
/// `Debug` is safe to log: every secret field redacts itself.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct NatsConfig {
    /// Server URLs (`nats://host:4222`, `tls://...`). At least one.
    pub servers: Vec<String>,
    /// Job identity: the bucket-name suffix, `[A-Za-z0-9_-]{1,64}`.
    /// Every worker of one coordinated job uses the same value; two
    /// different jobs must never share one.
    pub job: String,
    /// Authentication. Default anonymous.
    #[serde(default)]
    pub credentials: NatsCredentials,
    /// TLS material. Default none (plain or server-driven TLS).
    #[serde(default)]
    pub tls: Option<NatsTls>,
    /// Replication factor for both buckets (1, 3, or 5; 3+ needs a
    /// JetStream cluster). Default 1.
    #[serde(default = "default_replicas")]
    pub replicas: usize,
}

fn default_replicas() -> usize {
    1
}

impl NatsConfig {
    /// Anonymous plaintext connection defaults: no credentials, no TLS,
    /// replication factor 1.
    #[must_use]
    pub fn new(servers: Vec<String>, job: impl Into<String>) -> NatsConfig {
        NatsConfig {
            servers,
            job: job.into(),
            credentials: NatsCredentials::None,
            tls: None,
            replicas: 1,
        }
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.servers.is_empty() {
            return Err(StoreError::Fatal("nats.servers must not be empty".into()));
        }
        if self.job.is_empty()
            || self.job.len() > 64
            || !self
                .job
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(StoreError::Fatal(format!(
                "nats.job must be 1..=64 chars of [A-Za-z0-9_-], got {:?}",
                self.job
            )));
        }
        if !matches!(self.replicas, 1 | 3 | 5) {
            return Err(StoreError::Fatal(format!(
                "nats.replicas must be 1, 3, or 5, got {}",
                self.replicas
            )));
        }
        if let Some(tls) = &self.tls
            && tls.client_cert.is_some() != tls.client_key.is_some()
        {
            // Half a client identity would otherwise be silently dropped
            // and the connection would proceed without mutual TLS.
            return Err(StoreError::Fatal(
                "nats.tls: client_cert and client_key must be set together (mutual TLS \
                 needs both; remove both for server-only TLS)"
                    .into(),
            ));
        }
        Ok(())
    }
}

struct Buckets {
    state: kv::Store,
    lease: kv::Store,
    /// Set once the state bucket accepts per-message TTLs.
    state_marker_ttl: Arc<AtomicBool>,
}

struct Lazy {
    config: NatsConfig,
    lease_ttl: Duration,
    buckets: tokio::sync::OnceCell<Buckets>,
}

impl fmt::Debug for Lazy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsStore")
            .field("job", &self.config.job)
            .field("lease_ttl", &self.lease_ttl)
            .field("connected", &self.buckets.initialized())
            .finish_non_exhaustive()
    }
}

/// See the [module docs](self).
#[derive(Clone, Debug)]
pub struct NatsStore {
    inner: Arc<Lazy>,
}

impl NatsStore {
    /// Configure the store (no I/O; the connection is made lazily on the
    /// first operation, under the coordinator's startup budget).
    ///
    /// # Errors
    ///
    /// Fatal on invalid configuration or a lease below the NATS floor.
    pub fn new(config: NatsConfig, lease_ttl: Duration) -> Result<NatsStore, StoreError> {
        config.validate()?;
        if lease_ttl < MIN_LEASE {
            return Err(StoreError::Fatal(format!(
                "lease_duration must be >= {MIN_LEASE:?} on NATS (marker granularity is \
                 one second), got {lease_ttl:?}"
            )));
        }
        Ok(NatsStore {
            inner: Arc::new(Lazy {
                config,
                lease_ttl,
                buckets: tokio::sync::OnceCell::new(),
            }),
        })
    }

    async fn buckets(&self) -> Result<&Buckets, StoreError> {
        self.inner
            .buckets
            .get_or_try_init(|| connect(&self.inner.config, self.inner.lease_ttl))
            .await
    }

    /// How long a listing or watch snapshot waits for its next message.
    fn stall_bound(&self) -> Duration {
        self.inner.lease_ttl / 4
    }

    fn bucket<'a>(&self, buckets: &'a Buckets, ks: Keyspace) -> &'a kv::Store {
        match ks {
            Keyspace::Durable => &buckets.state,
            Keyspace::Ephemeral => &buckets.lease,
        }
    }
}

async fn connect(config: &NatsConfig, lease_ttl: Duration) -> Result<Buckets, StoreError> {
    let mut options = async_nats::ConnectOptions::new();
    match &config.credentials {
        NatsCredentials::None => {}
        NatsCredentials::UserPassword { username, password } => {
            options = options.user_and_password(username.clone(), password.reveal().to_string());
        }
        NatsCredentials::Token(token) => {
            options = options.token(token.reveal().to_string());
        }
        NatsCredentials::CredsFile(path) => {
            options = options
                .credentials_file(path)
                .await
                .map_err(|e| StoreError::Fatal(format!("reading NATS credentials file: {e}")))?;
        }
    }
    if config.tls.is_some() {
        options = options.require_tls(true);
    }
    let tls_certain = config.tls.is_some()
        || config
            .servers
            .iter()
            .any(|s| s.starts_with("tls://") || s.starts_with("wss://"));
    let section = config.tls.clone();
    let (tls_config, fallback) = tokio::task::spawn_blocking(move || {
        tls::client_config(
            section.as_ref(),
            tls_certain,
            rustls_native_certs::load_native_certs,
        )
    })
    .await
    .map_err(|e| StoreError::Fatal(format!("building the NATS TLS config: {e}")))??;
    if fallback && tls_certain {
        warn_mozilla_fallback();
    }
    // Passed without `tls` too: a server that requires TLS upgrades a
    // `nats://` connection through it.
    options = options.tls_client_config(tls_config);
    let client = options
        .connect(config.servers.join(","))
        .await
        .map_err(connect_error)?;
    if fallback && !tls_certain && client.server_info().tls_required {
        warn_mozilla_fallback();
    }

    let info = client.server_info();
    if !server_at_least(&info.version, MIN_SERVER) {
        return Err(StoreError::Fatal(format!(
            "NATS server {} is too old: coordination needs >= {}.{} (per-message TTLs \
             and KV limit markers); upgrade the server",
            info.version, MIN_SERVER.0, MIN_SERVER.1
        )));
    }

    let jetstream = async_nats::jetstream::new(client);
    let (state, adopted) = ensure_bucket(
        &jetstream,
        kv::Config {
            bucket: format!("spate_coordination_{}_state", config.job),
            description: "Spate coordination: durable split and plan records".into(),
            history: 1,
            max_value_size: MAX_VALUE_BYTES,
            num_replicas: config.replicas,
            limit_markers: Some(lease_ttl),
            ..Default::default()
        },
    )
    .await?;
    let state_marker_ttl = Arc::new(AtomicBool::new(true));
    if let Some(patched) = adopted.and_then(|c| with_message_ttls(c, lease_ttl)) {
        state_marker_ttl.store(false, Ordering::Release);
        // Off the startup path: a denied update only times out.
        tokio::spawn(enable_message_ttls(
            jetstream.clone(),
            patched,
            Arc::clone(&state_marker_ttl),
        ));
    }
    let (lease, _) = ensure_bucket(
        &jetstream,
        kv::Config {
            bucket: format!("spate_coordination_{}_lease", config.job),
            description: "Spate coordination: ephemeral lease keys".into(),
            history: 1,
            max_value_size: MAX_VALUE_BYTES,
            num_replicas: config.replicas,
            max_age: lease_ttl,
            limit_markers: Some(lease_ttl),
            ..Default::default()
        },
    )
    .await?;
    Ok(Buckets {
        state,
        lease,
        state_marker_ttl,
    })
}

/// Fatal when the connection rejects a credential or a certificate on
/// either side, Retryable otherwise.
fn connect_error(e: async_nats::ConnectError) -> StoreError {
    use async_nats::ConnectErrorKind;
    use async_nats::rustls::{AlertDescription, Error as TlsError};
    let rejected = match e.kind() {
        ConnectErrorKind::AuthorizationViolation
        | ConnectErrorKind::Authentication
        | ConnectErrorKind::Tls => true,
        _ => matches!(
            find_source::<TlsError>(&e),
            Some(
                TlsError::InvalidCertificate(_)
                    | TlsError::AlertReceived(
                        AlertDescription::BadCertificate
                            | AlertDescription::UnsupportedCertificate
                            | AlertDescription::CertificateRevoked
                            | AlertDescription::CertificateExpired
                            | AlertDescription::CertificateUnknown
                            | AlertDescription::UnknownCA
                            | AlertDescription::AccessDenied
                            | AlertDescription::CertificateRequired
                            | AlertDescription::DecryptError
                    )
            )
        ),
    };
    let message = format!("connecting to NATS: {e}");
    if rejected {
        StoreError::Fatal(message)
    } else {
        StoreError::Retryable(message)
    }
}

/// The first `T` in `err`'s source chain, `err` included.
fn find_source<'a, T: std::error::Error + 'static>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a T> {
    let mut pending = vec![err];
    while let Some(e) = pending.pop() {
        if let Some(found) = e.downcast_ref() {
            return Some(found);
        }
        // `io::Error::source` skips the error it wraps, so reach it through
        // `get_ref`.
        if let Some(inner) = e
            .downcast_ref::<std::io::Error>()
            .and_then(|io| io.get_ref())
        {
            pending.push(inner);
        }
        pending.extend(e.source());
    }
    None
}

/// `config` with per-message TTLs enabled, or `None` when it allows them
/// already.
fn with_message_ttls(mut config: stream::Config, marker_ttl: Duration) -> Option<stream::Config> {
    if config.allow_message_ttl {
        return None;
    }
    config.allow_message_ttl = true;
    config.subject_delete_marker_ttl = Some(marker_ttl);
    Some(config)
}

/// Apply `config` to an existing bucket's stream and set `enabled` once
/// the server reports per-message TTLs allowed. A failure is logged and
/// leaves `enabled` unset.
async fn enable_message_ttls(
    jetstream: async_nats::jetstream::Context,
    config: stream::Config,
    enabled: Arc<AtomicBool>,
) {
    match jetstream.update_stream(&config).await {
        Ok(info) if info.config.allow_message_ttl => enabled.store(true, Ordering::Release),
        Ok(_) => tracing::warn!(
            stream = %config.name,
            "the NATS server did not enable per-message TTLs on the coordination state \
             bucket; deleted keys keep a marker"
        ),
        Err(error) => tracing::warn!(
            stream = %config.name,
            %error,
            "could not enable per-message TTLs on the coordination state bucket, so \
             deleted keys keep a marker; the workers' credentials need \
             $JS.API.STREAM.UPDATE on its stream"
        ),
    }
}

fn warn_mozilla_fallback() {
    tracing::warn!(
        "the system trust store has no certificates; verifying NATS servers against \
         the Mozilla root bundle"
    );
}

/// Parse `major.minor[.patch][-pre]` leniently and compare.
fn server_at_least(version: &str, (want_major, want_minor): (u64, u64)) -> bool {
    let core = version.split(['-', '+']).next().unwrap_or(version);
    let mut parts = core.split('.').map(|p| p.parse::<u64>().unwrap_or(0));
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    (major, minor) >= (want_major, want_minor)
}

/// Create the bucket or adopt an existing one, verifying that the config
/// that matters (max_age, which IS the lease TTL) matches. Returns an
/// adopted bucket's stream config.
async fn ensure_bucket(
    jetstream: &async_nats::jetstream::Context,
    config: kv::Config,
) -> Result<(kv::Store, Option<stream::Config>), StoreError> {
    let name = config.bucket.clone();
    let expected_age = config.max_age;
    match jetstream.get_key_value(&name).await {
        Ok(store) => {
            let status = store
                .status()
                .await
                .map_err(|e| StoreError::Retryable(format!("reading bucket {name}: {e}")))?;
            if status.max_age() != expected_age {
                return Err(StoreError::Fatal(format!(
                    "bucket {name} exists with max_age {:?} but this worker is configured \
                     for {expected_age:?}: lease_duration cannot change mid-job — finish or \
                     delete the job's buckets first",
                    status.max_age()
                )));
            }
            Ok((store, Some(status.info.config)))
        }
        Err(_) => jetstream
            .create_key_value(config)
            .await
            .map(|store| (store, None))
            .map_err(|e| StoreError::Retryable(format!("creating bucket {name}: {e}"))),
    }
}

/// Whether `key` holds a value, read through the stream leader.
///
/// `kv::Store::entry` uses direct get, which any replica may answer, so a
/// lagging follower can miss a write the leader has acknowledged.
async fn live_on_leader(bucket: &kv::Store, key: &str) -> Result<bool, StoreError> {
    let subject = format!("{}{}", bucket.prefix, key);
    match bucket
        .stream
        .get_last_raw_message_by_subject(&subject)
        .await
    {
        // Every marker carries one of these headers.
        Ok(message) => Ok(message
            .headers
            .get(async_nats::header::NATS_MARKER_REASON)
            .is_none()
            && message
                .headers
                .get("KV-Operation")
                .is_none_or(|op| op.as_str() == "PUT")),
        Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(false),
        Err(e) => Err(StoreError::Retryable(format!("read {key}: {e}"))),
    }
}

/// The next item of a listing or watch snapshot, or Retryable when none
/// arrives within `bound`. Such a stream ends only on a delivered message
/// that reports nothing pending, so messages that expire before delivery
/// leave it waiting forever.
async fn next_within<S: futures_util::Stream + Unpin>(
    stream: &mut S,
    bound: Duration,
    what: &str,
) -> Result<Option<S::Item>, StoreError> {
    tokio::time::timeout(bound, stream.next())
        .await
        .map_err(|_| StoreError::Retryable(format!("{what} stalled for {bound:?}")))
}

/// Map a KV entry to the store contract: non-Put operations are markers,
/// i.e. deletions.
fn to_event(entry: kv::Entry) -> WatchEvent {
    match entry.operation {
        kv::Operation::Put => WatchEvent::Put(Entry {
            key: entry.key,
            value: entry.value.to_vec(),
            revision: Revision(entry.revision),
        }),
        kv::Operation::Delete | kv::Operation::Purge => WatchEvent::Delete {
            key: entry.key,
            revision: Revision(entry.revision),
        },
    }
}

impl CoordinationStore for NatsStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        let buckets = self.buckets().await?;
        match self.bucket(buckets, ks).create(key, value.into()).await {
            Ok(revision) => Ok(CasOutcome::Won(Revision(revision))),
            Err(e) if e.kind() == kv::CreateErrorKind::AlreadyExists => Ok(CasOutcome::Lost),
            Err(e) => Err(StoreError::Retryable(format!("create {key}: {e}"))),
        }
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        let buckets = self.buckets().await?;
        match self
            .bucket(buckets, ks)
            .update(key, value.into(), expected.0)
            .await
        {
            Ok(revision) => Ok(CasOutcome::Won(Revision(revision))),
            Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => Ok(CasOutcome::Lost),
            Err(e) => Err(StoreError::Retryable(format!("update {key}: {e}"))),
        }
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        let buckets = self.buckets().await?;
        let entry = self
            .bucket(buckets, ks)
            .entry(key)
            .await
            .map_err(|e| StoreError::Retryable(format!("get {key}: {e}")))?;
        // entry() surfaces delete/purge MARKERS; only a Put is a value.
        Ok(entry.and_then(|entry| match to_event(entry) {
            WatchEvent::Put(entry) => Some(entry),
            _ => None,
        }))
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        let buckets = self.buckets().await?;
        let bucket = self.bucket(buckets, ks);
        let ttl = self.inner.lease_ttl;
        // A purge carrying a TTL leaves a marker the server removes after
        // it; a delete marker in the state bucket stays forever.
        let marker_ttl =
            ks == Keyspace::Durable && buckets.state_marker_ttl.load(Ordering::Acquire);
        let result = match (marker_ttl, expected) {
            (false, expected) => {
                bucket
                    .delete_expect_revision(key, expected.map(|r| r.0))
                    .await
            }
            (true, Some(revision)) => {
                bucket
                    .purge_expect_revision_with_ttl(key, revision.0, ttl)
                    .await
            }
            (true, None) => bucket.purge_with_ttl(key, ttl).await,
        };
        match result {
            // NATS does not report the marker's revision.
            Ok(()) => Ok(CasOutcome::Won(Revision(0))),
            // JetStream checks `expected` against the subject's last
            // message, which for an absent key is nothing or a marker.
            Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => {
                if live_on_leader(bucket, key).await? {
                    Ok(CasOutcome::Lost)
                } else {
                    Ok(CasOutcome::Won(Revision(0)))
                }
            }
            Err(e) => Err(StoreError::Retryable(format!("delete {key}: {e}"))),
        }
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        let buckets = self.buckets().await?;
        let store = self.bucket(buckets, ks).clone();
        let filter = match prefix {
            "" => ">".to_string(),
            p if p.ends_with('.') => format!("{p}>"),
            p => p.to_string(),
        };
        // An empty bucket has no entry to carry `seen_current`, so the
        // snapshot boundary must be synthesized immediately. Emptiness
        // comes from `keys()`, the one API that answers "are there live
        // keys" (bucket status counts messages, markers included, and is
        // not that answer).
        let empty = {
            let mut keys = store
                .keys()
                .await
                .map_err(|e| StoreError::Retryable(format!("listing keys: {e}")))?;
            next_within(&mut keys, self.stall_bound(), "listing keys")
                .await?
                .is_none()
        };
        let watcher = store
            .watch_with_history(&filter)
            .await
            .map_err(|e| StoreError::Retryable(format!("watch {filter}: {e}")))?;

        let head = futures_util::stream::iter(if empty {
            vec![Ok(WatchEvent::SnapshotDone)]
        } else {
            Vec::new()
        });
        let caught_up = empty;
        let stall = self.stall_bound();
        let tail = futures_util::stream::unfold(
            (watcher, caught_up),
            move |(mut watcher, mut caught_up)| async move {
                let next = if caught_up {
                    watcher.next().await
                } else {
                    match next_within(&mut watcher, stall, "watch snapshot").await {
                        Ok(next) => next,
                        Err(e) => return Some((vec![Err(e)], (watcher, caught_up))),
                    }
                };
                match next {
                    Some(Ok(entry)) => {
                        let mark_done = !caught_up && entry.seen_current;
                        caught_up |= entry.seen_current;
                        let event = to_event(entry);
                        let out: Vec<Result<WatchEvent, StoreError>> = if mark_done {
                            vec![Ok(event), Ok(WatchEvent::SnapshotDone)]
                        } else {
                            vec![Ok(event)]
                        };
                        Some((out, (watcher, caught_up)))
                    }
                    Some(Err(e)) => Some((
                        vec![Err(StoreError::Retryable(format!("watch: {e}")))],
                        (watcher, caught_up),
                    )),
                    None => None,
                }
            },
        )
        .flat_map(futures_util::stream::iter);
        Ok(head.chain(tail).boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        // `keys()` is the authoritative live-key view (markers excluded);
        // point-read each one. The listing backs the reconcile pass. A
        // key it omits is treated as DEAD by the protocol, so this must
        // never underreport. N+1 round trips are fine at reconcile
        // cadence over a working set of keys.
        let buckets = self.buckets().await?;
        let store = self.bucket(buckets, ks);
        let mut keys = store
            .keys()
            .await
            .map_err(|e| StoreError::Retryable(format!("listing keys: {e}")))?;
        let stall = self.stall_bound();
        let mut out = Vec::new();
        while let Some(key) = next_within(&mut keys, stall, "listing keys").await? {
            let key = key.map_err(|e| StoreError::Retryable(format!("listing keys: {e}")))?;
            if !key.starts_with(prefix) {
                continue;
            }
            if let Some(entry) = self.get(ks, &key).await? {
                out.push(entry);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_never_debug_print() {
        let config = NatsConfig {
            servers: vec!["nats://localhost:4222".into()],
            job: "orders".into(),
            credentials: NatsCredentials::UserPassword {
                username: "svc".into(),
                password: Secret::new("hunter2"),
            },
            tls: None,
            replicas: 1,
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
        assert!(rendered.contains("svc"), "usernames are not secret");
    }

    #[test]
    fn config_floors_reject_actionably() {
        let base = NatsConfig {
            servers: vec!["nats://localhost:4222".into()],
            job: "ok_job-1".into(),
            credentials: NatsCredentials::None,
            tls: None,
            replicas: 1,
        };
        NatsStore::new(base.clone(), Duration::from_secs(30)).unwrap();

        let short = NatsStore::new(base.clone(), Duration::from_millis(500)).unwrap_err();
        assert!(short.to_string().contains("lease_duration"), "{short}");

        let bad_job = NatsConfig {
            job: "has.dots".into(),
            ..base.clone()
        };
        let err = NatsStore::new(bad_job, Duration::from_secs(30)).unwrap_err();
        assert!(err.to_string().contains("nats.job"), "{err}");

        let bad_replicas = NatsConfig {
            replicas: 2,
            ..base.clone()
        };
        let err = NatsStore::new(bad_replicas, Duration::from_secs(30)).unwrap_err();
        assert!(err.to_string().contains("replicas"), "{err}");

        let no_servers = NatsConfig {
            servers: vec![],
            ..base
        };
        let err = NatsStore::new(no_servers, Duration::from_secs(30)).unwrap_err();
        assert!(err.to_string().contains("servers"), "{err}");
    }

    #[test]
    fn message_ttls_patch_only_the_ttl_fields() {
        let existing = stream::Config {
            name: "KV_spate_coordination_orders_state".into(),
            description: Some("operator note".into()),
            num_replicas: 3,
            max_bytes: 1 << 30,
            ..Default::default()
        };
        let patched =
            with_message_ttls(existing.clone(), Duration::from_secs(30)).expect("TTLs are off");
        assert_eq!(
            patched,
            stream::Config {
                allow_message_ttl: true,
                subject_delete_marker_ttl: Some(Duration::from_secs(30)),
                ..existing
            }
        );
        assert_eq!(with_message_ttls(patched, Duration::from_secs(30)), None);
    }

    /// A server that answers the CONNECT with an authorization violation fails
    /// the connect with a fatal error. Regression for #634.
    #[tokio::test]
    async fn a_rejected_credential_is_fatal() {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let mut tcp = BufReader::new(tcp);
                let info = format!(
                    "INFO {{\"server_id\":\"test\",\"version\":\"2.11.0\",\"proto\":1,\
                     \"host\":\"127.0.0.1\",\"port\":{port},\"max_payload\":1048576,\
                     \"auth_required\":true}}\r\n"
                );
                tcp.get_mut().write_all(info.as_bytes()).await.unwrap();
                let mut line = String::new();
                tcp.read_line(&mut line).await.unwrap();
                tcp.get_mut()
                    .write_all(b"-ERR 'Authorization Violation'\r\n")
                    .await
                    .unwrap();
            }
        });
        let mut config = NatsConfig::new(vec![format!("nats://127.0.0.1:{port}")], "auth_test");
        config.credentials = NatsCredentials::UserPassword {
            username: "spate".into(),
            password: Secret::new("wrong"),
        };
        let store = NatsStore::new(config, Duration::from_secs(30)).unwrap();
        match store.get(Keyspace::Durable, "k").await {
            Err(StoreError::Fatal(message)) => {
                assert!(message.contains("authorization violation"), "{message}");
            }
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    #[test]
    fn version_floor_parses_real_world_strings() {
        assert!(server_at_least("2.11.4", MIN_SERVER));
        assert!(server_at_least("2.12.0-beta.1", MIN_SERVER));
        assert!(server_at_least("3.0.0", MIN_SERVER));
        assert!(!server_at_least("2.10.22", MIN_SERVER));
        assert!(!server_at_least("2.9", MIN_SERVER));
        assert!(!server_at_least("garbage", MIN_SERVER));
    }
}
