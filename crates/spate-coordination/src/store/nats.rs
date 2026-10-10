//! NATS JetStream KV [`CoordinationStore`]: the production backend.
//!
//! A production server needs the settings under [the store page's
//! requirements].
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
//!   watchers as a limit marker (`Operation::Purge`; a graceful delete is
//!   `Operation::Delete`). [`NatsStore`]'s watch reports both as
//!   [`WatchEvent::Delete`]. The `nats_spike` integration test pins the
//!   raw KV observations against a real server.
//!
//! Construction is synchronous and lazy: the connection and bucket
//! provisioning happen on the first store operation, the coordinator's
//! startup probe, so an unreachable server rides the startup retry budget.
//! Misconfiguration, and a credential, certificate or TLS handshake
//! rejected on the first connection, are Fatal with an actionable message.
//! The first connection tries each server once; only a rejected credential
//! ends it early, so another rejection is Fatal only on the last server tried.
//! After startup the client reconnects on its own, and store operations are
//! Fatal once every server has failed since the last successful connect with
//! a rejection as its latest failure.
//! No `async-nats` type appears in any public signature (0.x policy: single
//! pinned minor, internal only).
//!
//! TLS connections use rustls with the `ring` provider, whatever other rustls
//! features the build enables.
//!
//! [the store page's requirements]: https://spate.kainth.dev/docs/user-guide/connectors/coordination/nats#requirements

use super::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use async_nats::jetstream::consumer::{DeliverPolicy, ReplayPolicy, push};
use async_nats::jetstream::response::Response;
use async_nats::jetstream::stream::LastRawMessageErrorKind;
use async_nats::jetstream::{kv, stream};
use futures_util::StreamExt as _;
use serde::Deserialize;
use spate_core::config::redact;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

mod reconnect;
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

/// Subjects each page of a subject listing repeats from the page before it.
const SUBJECT_PAGE_OVERLAP: usize = 1024;

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
///
/// Deserializes from `none`, or from a single-key map naming the mechanism:
/// `{ user_password: { username, password } }`, `{ token: … }` or
/// `{ creds_file: … }`. The YAML-tagged forms (`!token …`) parse too.
#[derive(Clone, Debug, Default)]
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

const CREDENTIAL_KEYS: &[&str] = &["user_password", "token", "creds_file"];
const CREDENTIAL_VARIANTS: &[&str] = &["none", "user_password", "token", "creds_file"];

impl<'de> Deserialize<'de> for NatsCredentials {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct UserPassword {
            username: String,
            password: Secret,
        }

        struct Mechanism;

        impl<'de> serde::de::Visitor<'de> for Mechanism {
            type Value = NatsCredentials;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`none`, or a map with one of `user_password`, `token`, `creds_file`")
            }

            // The value is never echoed: a mistyped secret would land here.
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<NatsCredentials, E> {
                if v == "none" {
                    Ok(NatsCredentials::None)
                } else {
                    Err(E::invalid_value(
                        serde::de::Unexpected::Other("a string"),
                        &self,
                    ))
                }
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<NatsCredentials, A::Error> {
                use serde::de::Error as _;
                let Some(key) = map.next_key::<String>()? else {
                    return Err(A::Error::invalid_length(0, &self));
                };
                let credentials = match key.as_str() {
                    "user_password" => {
                        let UserPassword { username, password } = map.next_value()?;
                        NatsCredentials::UserPassword { username, password }
                    }
                    "token" => NatsCredentials::Token(map.next_value()?),
                    "creds_file" => NatsCredentials::CredsFile(map.next_value()?),
                    other => return Err(A::Error::unknown_field(other, CREDENTIAL_KEYS)),
                };
                if map.next_key::<String>()?.is_some() {
                    return Err(A::Error::custom("credentials name exactly one mechanism"));
                }
                Ok(credentials)
            }

            fn visit_enum<A: serde::de::EnumAccess<'de>>(
                self,
                data: A,
            ) -> Result<NatsCredentials, A::Error> {
                use serde::de::{Error as _, VariantAccess as _};
                let (tag, variant) = data.variant::<String>()?;
                match tag.as_str() {
                    "none" => {
                        variant.unit_variant()?;
                        Ok(NatsCredentials::None)
                    }
                    "user_password" => {
                        let UserPassword { username, password } = variant.newtype_variant()?;
                        Ok(NatsCredentials::UserPassword { username, password })
                    }
                    "token" => Ok(NatsCredentials::Token(variant.newtype_variant()?)),
                    "creds_file" => Ok(NatsCredentials::CredsFile(variant.newtype_variant()?)),
                    other => Err(A::Error::unknown_variant(other, CREDENTIAL_VARIANTS)),
                }
            }
        }

        deserializer.deserialize_any(Mechanism)
    }
}

/// TLS material for the NATS connection. Presence of this section
/// requires TLS on every server, and rejects a `ws://` server.
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
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct NatsConfig {
    /// Server URLs (`nats://host:4222`, `tls://...`, `ws://...`, `wss://...`).
    /// At least one.
    pub servers: Vec<String>,
    /// Job identity: the bucket-name suffix, `[A-Za-z0-9_-]{1,64}`.
    /// Every worker of one coordinated job uses the same value; two
    /// different jobs must never share one.
    pub job: String,
    /// Authentication. Default anonymous.
    #[serde(default)]
    pub credentials: NatsCredentials,
    /// TLS material. Default none (plain or server-driven TLS; a `ws://`
    /// server never upgrades).
    #[serde(default)]
    pub tls: Option<NatsTls>,
    /// Replication factor for both buckets (1, 3, or 5; 3+ needs a
    /// JetStream cluster). Default 1. An existing bucket must already have
    /// this replica count, or connecting fails.
    #[serde(default = "default_replicas")]
    pub replicas: usize,
}

// Hand-written: server URLs can carry credentials. The destructure lists every
// field so a new one cannot reach `Debug` unredacted.
impl fmt::Debug for NatsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let NatsConfig {
            servers,
            job,
            credentials,
            tls,
            replicas,
        } = self;
        f.debug_struct("NatsConfig")
            .field(
                "servers",
                &servers.iter().map(|s| redact::url(s)).collect::<Vec<_>>(),
            )
            .field("job", job)
            .field("credentials", credentials)
            .field("tls", tls)
            .field("replicas", replicas)
            .finish()
    }
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

    fn validate(&self) -> Result<Vec<async_nats::ServerAddr>, StoreError> {
        if self.servers.is_empty() {
            return Err(StoreError::Fatal("nats.servers must not be empty".into()));
        }
        let mut addrs = Vec::with_capacity(self.servers.len());
        // Named by index: a server URL can carry credentials.
        for (i, server) in self.servers.iter().enumerate() {
            if server.contains(',') {
                return Err(StoreError::Fatal(format!(
                    "nats.servers[{i}] holds a comma; list each server as its own entry"
                )));
            }
            // The client's own parser, so each entry's scheme reads as the
            // connection reads it.
            let addr = server.parse::<async_nats::ServerAddr>().map_err(|e| {
                StoreError::Fatal(format!("nats.servers[{i}] is not a NATS server URL: {e}"))
            })?;
            if self.tls.is_some() && addr.scheme() == "ws" {
                return Err(StoreError::Fatal(format!(
                    "nats.servers[{i}] is a ws:// server, which never uses TLS; use wss://, \
                     or remove nats.tls"
                )));
            }
            addrs.push(addr);
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
        Ok(addrs)
    }
}

struct Buckets {
    client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
    rejection: Arc<reconnect::Rejection>,
    state: kv::Store,
    lease: kv::Store,
    /// Set once the state bucket accepts per-message TTLs.
    state_marker_ttl: Arc<AtomicBool>,
}

struct Lazy {
    config: NatsConfig,
    /// `config.servers`, parsed by `validate`. Holds credentials; keep out of
    /// `Debug`.
    servers: Vec<async_nats::ServerAddr>,
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
        let servers = config.validate()?;
        if lease_ttl < MIN_LEASE {
            return Err(StoreError::Fatal(format!(
                "lease_duration must be >= {MIN_LEASE:?} on NATS (marker granularity is \
                 one second), got {lease_ttl:?}"
            )));
        }
        Ok(NatsStore {
            inner: Arc::new(Lazy {
                config,
                servers,
                lease_ttl,
                buckets: tokio::sync::OnceCell::new(),
            }),
        })
    }

    async fn buckets(&self) -> Result<&Buckets, StoreError> {
        let buckets = self
            .inner
            .buckets
            .get_or_try_init(|| {
                connect(
                    &self.inner.config,
                    &self.inner.servers,
                    self.inner.lease_ttl,
                )
            })
            .await?;
        buckets.rejection.check(&buckets.client)?;
        Ok(buckets)
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

/// A client connected to `servers`, whose reconnects report to `rejection`.
async fn client(
    config: &NatsConfig,
    servers: &[async_nats::ServerAddr],
    rejection: &Arc<reconnect::Rejection>,
) -> Result<async_nats::Client, StoreError> {
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
    let tls_certain =
        config.tls.is_some() || servers.iter().any(|s| matches!(s.scheme(), "tls" | "wss"));
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
    let observer = Arc::clone(rejection);
    options = options.reconnect_to_server_callback(move |pool, _| {
        observer.observe(&pool);
        std::future::ready(None)
    });
    let client = options.connect(servers).await.map_err(connect_error)?;
    if fallback && !tls_certain && client.server_info().tls_required {
        warn_mozilla_fallback();
    }
    rejection.attach(&client);
    Ok(client)
}

async fn connect(
    config: &NatsConfig,
    servers: &[async_nats::ServerAddr],
    lease_ttl: Duration,
) -> Result<Buckets, StoreError> {
    let rejection = Arc::new(reconnect::Rejection::default());
    let client = client(config, servers, &rejection).await?;
    let info = client.server_info();
    if !server_at_least(&info.version, MIN_SERVER) {
        return Err(StoreError::Fatal(format!(
            "NATS server {} is too old: coordination needs >= {}.{} (per-message TTLs \
             and KV limit markers); upgrade the server",
            info.version, MIN_SERVER.0, MIN_SERVER.1
        )));
    }

    let jetstream = async_nats::jetstream::new(client.clone());
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
    if let Some(patched) = adopted.and_then(|c| {
        warn_async_state_persistence(&c);
        with_message_ttls(c, lease_ttl)
    }) {
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
        client,
        jetstream,
        rejection,
        state,
        lease,
        state_marker_ttl,
    })
}

/// Fatal when the connection rejects a credential, a certificate or the TLS
/// handshake, Retryable otherwise.
fn connect_error(e: async_nats::ConnectError) -> StoreError {
    use async_nats::ConnectErrorKind;
    let rejected = match e.kind() {
        ConnectErrorKind::AuthorizationViolation
        | ConnectErrorKind::Authentication
        | ConnectErrorKind::Tls => true,
        _ => spate_core::tls_rejection!(async_nats::rustls, &e).is_some(),
    };
    let message = format!("connecting to NATS: {e}");
    if rejected {
        StoreError::Fatal(message)
    } else {
        StoreError::Retryable(message)
    }
}

fn warn_async_state_persistence(existing: &stream::Config) {
    if existing.persist_mode == Some(stream::PersistenceMode::Async) {
        tracing::warn!(
            stream = %existing.name,
            persistence = "async",
            "the adopted coordination state bucket uses async persistence; acknowledged \
             writes can be lost despite sync_interval: always; provision production state \
             buckets with default stream persistence"
        );
    }
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

/// Create the bucket or adopt an existing one whose settings pass
/// [`check_adopted`]. Returns an adopted bucket's stream config.
async fn ensure_bucket(
    jetstream: &async_nats::jetstream::Context,
    config: kv::Config,
) -> Result<(kv::Store, Option<stream::Config>), StoreError> {
    let name = config.bucket.clone();
    match jetstream.get_key_value(&name).await {
        Ok(store) => {
            let status = store
                .status()
                .await
                .map_err(|e| StoreError::Retryable(format!("reading bucket {name}: {e}")))?;
            check_adopted(&config, &status.info.config)?;
            Ok((store, Some(status.info.config)))
        }
        Err(_) => jetstream
            .create_key_value(config)
            .await
            .map(|store| (store, None))
            .map_err(|e| StoreError::Retryable(format!("creating bucket {name}: {e}"))),
    }
}

/// Fatal when an existing bucket's age limit or replica count differs from
/// `wanted`.
fn check_adopted(wanted: &kv::Config, existing: &stream::Config) -> Result<(), StoreError> {
    let name = &wanted.bucket;
    if existing.max_age != wanted.max_age {
        return Err(StoreError::Fatal(format!(
            "bucket {name} exists with max_age {:?} but this worker is configured \
             for {:?}: lease_duration cannot change mid-job — finish or \
             delete the job's buckets first",
            existing.max_age, wanted.max_age
        )));
    }
    if existing.num_replicas != wanted.num_replicas {
        return Err(StoreError::Fatal(format!(
            "bucket {name} exists with replica count {} but this worker is configured \
             for {}: set nats.replicas to {}, or change the bucket's replica count \
             (nats kv edit {name} --replicas {})",
            existing.num_replicas, wanted.num_replicas, existing.num_replicas, wanted.num_replicas
        )));
    }
    Ok(())
}

/// The last message for `key`, value or marker, read through the stream
/// leader.
///
/// `kv::Store::entry` uses direct get, which any replica may answer, so a
/// lagging follower can miss a write the leader has acknowledged.
async fn last_on_leader(
    bucket: &kv::Store,
    key: &str,
) -> Result<Option<async_nats::jetstream::message::StreamMessage>, StoreError> {
    let subject = format!("{}{}", bucket.prefix, key);
    match bucket
        .stream
        .get_last_raw_message_by_subject(&subject)
        .await
    {
        Ok(message) => Ok(Some(message)),
        Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
        Err(e) => Err(StoreError::Retryable(format!("read {key}: {e}"))),
    }
}

/// Whether `key` holds a value, read through the stream leader.
async fn live_on_leader(bucket: &kv::Store, key: &str) -> Result<bool, StoreError> {
    Ok(last_on_leader(bucket, key)
        .await?
        .is_some_and(|message| holds_value(&message.headers)))
}

#[derive(serde::Serialize)]
struct SubjectsRequest<'a> {
    offset: usize,
    subjects_filter: &'a str,
}

#[derive(Deserialize)]
struct SubjectsPage {
    state: SubjectsState,
    cluster: Option<SubjectsCluster>,
    #[serde(default)]
    total: usize,
    #[serde(default)]
    limit: usize,
}

#[derive(Deserialize)]
struct SubjectsState {
    subjects: Option<BTreeMap<String, u64>>,
}

#[derive(Deserialize)]
struct SubjectsCluster {
    leader: Option<String>,
}

/// Every subject of `stream` matching `filter` that holds a message, as the
/// stream leader reports it.
///
/// A subject present for the whole call is in the result. Retryable when the
/// stream has no leader, or when the set shifts between pages by more than
/// [`SUBJECT_PAGE_OVERLAP`].
async fn subjects_under(
    js: &async_nats::jetstream::Context,
    stream: &str,
    filter: &str,
) -> Result<BTreeSet<String>, StoreError> {
    let what = || format!("subjects under {filter}");
    let mut subjects = BTreeSet::new();
    let mut offset = 0;
    let mut prev_max: Option<String> = None;
    loop {
        let reply: Response<SubjectsPage> = js
            .request(
                format!("STREAM.INFO.{stream}"),
                &SubjectsRequest {
                    offset,
                    subjects_filter: filter,
                },
            )
            .await
            .map_err(|e| StoreError::Retryable(format!("{}: {e}", what())))?;
        let page = match reply {
            Response::Ok(page) => page,
            Response::Err { error } => {
                return Err(StoreError::Retryable(format!("{}: {error}", what())));
            }
        };
        // A group with no leader lets a replica answer from its own state.
        if page
            .cluster
            .and_then(|c| c.leader)
            .is_none_or(|l| l.is_empty())
        {
            return Err(StoreError::Retryable(format!(
                "{}: the stream has no leader",
                what()
            )));
        }
        let page_subjects = page.state.subjects.unwrap_or_default();
        if let Some(prev_max) = &prev_max
            && !pages_join(prev_max, page_subjects.keys().next().map(String::as_str))
        {
            return Err(StoreError::Retryable(format!(
                "{}: the subject set moved between pages",
                what()
            )));
        }
        let len = page_subjects.len();
        prev_max = page_subjects.keys().next_back().cloned();
        subjects.extend(page_subjects.into_keys());
        match next_subject_page(offset, len, page.total, page.limit) {
            Some(next) => offset = next,
            None => return Ok(subjects),
        }
    }
}

/// The offset of the page after one of `len` subjects at `offset`, or `None`
/// when that page reached `total`. Consecutive pages share up to
/// [`SUBJECT_PAGE_OVERLAP`] subjects.
fn next_subject_page(offset: usize, len: usize, total: usize, limit: usize) -> Option<usize> {
    if len == 0 || offset + len >= total {
        return None;
    }
    let overlap = SUBJECT_PAGE_OVERLAP.min(limit / 2).min(len / 2);
    Some(offset + len - overlap)
}

/// Whether a sorted page whose smallest subject is `first` leaves no gap after
/// a page whose largest subject was `prev_max`.
fn pages_join(prev_max: &str, first: Option<&str>) -> bool {
    first.is_some_and(|first| first <= prev_max)
}

/// The keys under `key_prefix` among `subjects` of the bucket whose subjects
/// start with `bucket_prefix`, less the keys in `seen`.
fn unseen_keys(
    subjects: &BTreeSet<String>,
    bucket_prefix: &str,
    key_prefix: &str,
    seen: &BTreeSet<String>,
) -> Vec<String> {
    subjects
        .iter()
        .filter_map(|subject| subject.strip_prefix(bucket_prefix))
        .filter(|key| key.starts_with(key_prefix) && !seen.contains(*key))
        .map(str::to_string)
        .collect()
}

/// Whether a message with `headers` holds a value. Every marker carries
/// `Nats-Marker-Reason` or a `KV-Operation` other than `PUT`.
fn holds_value(headers: &async_nats::HeaderMap) -> bool {
    headers
        .get(async_nats::header::NATS_MARKER_REASON)
        .is_none()
        && headers
            .get("KV-Operation")
            .is_none_or(|op| op.as_str() == "PUT")
}

/// Applies one delivered message for `key` to a listing. A value replaces the
/// key's entry, so a key delivered twice keeps its later message; a marker
/// drops the key.
fn fold_listed(
    live: &mut BTreeMap<String, Entry>,
    key: &str,
    headers: Option<&async_nats::HeaderMap>,
    value: &[u8],
    revision: u64,
) {
    if headers.is_none_or(holds_value) {
        live.insert(
            key.to_string(),
            Entry {
                key: key.to_string(),
                value: value.to_vec(),
                revision: Revision(revision),
            },
        );
    } else {
        live.remove(key);
    }
}

/// The next item of a listing or watch snapshot, or Retryable when none
/// arrives within `bound`. Messages that expire before delivery can leave
/// such a stream waiting for one that reports nothing pending.
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
        // A subject filter matches whole tokens, so a prefix that ends
        // mid-token watches every key and is filtered here.
        let (filter, watched): (String, fn(&str, &str) -> bool) = match prefix {
            p if p.is_empty() || p.ends_with('.') => (format!("{p}>"), |_, _| true),
            _ => (">".to_string(), |key, prefix| key.starts_with(prefix)),
        };
        // A filter with no live key has no entry to carry `seen_current`, so
        // the snapshot boundary must be synthesized immediately. Emptiness
        // comes from `keys()`, the one API that answers "are there live
        // keys" (bucket status counts messages, markers included, and is
        // not that answer).
        let empty = {
            let mut keys = store
                .keys()
                .await
                .map_err(|e| StoreError::Retryable(format!("listing keys: {e}")))?;
            let mut empty = true;
            while let Some(key) = next_within(&mut keys, self.stall_bound(), "listing keys").await?
            {
                let key = key.map_err(|e| StoreError::Retryable(format!("listing keys: {e}")))?;
                if filter == ">" || key.starts_with(prefix) {
                    empty = false;
                    break;
                }
            }
            empty
        };
        let prefix = prefix.to_string();
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
            (watcher, caught_up, prefix),
            move |(mut watcher, mut caught_up, prefix)| async move {
                let next = if caught_up {
                    watcher.next().await
                } else {
                    match next_within(&mut watcher, stall, "watch snapshot").await {
                        Ok(next) => next,
                        Err(e) => return Some((vec![Err(e)], (watcher, caught_up, prefix))),
                    }
                };
                match next {
                    Some(Ok(entry)) => {
                        let mark_done = !caught_up && entry.seen_current;
                        caught_up |= entry.seen_current;
                        let mut out: Vec<Result<WatchEvent, StoreError>> = Vec::new();
                        if watched(&entry.key, &prefix) {
                            out.push(Ok(to_event(entry)));
                        }
                        if mark_done {
                            out.push(Ok(WatchEvent::SnapshotDone));
                        }
                        Some((out, (watcher, caught_up, prefix)))
                    }
                    Some(Err(e)) => Some((
                        vec![Err(StoreError::Retryable(format!("watch: {e}")))],
                        (watcher, caught_up, prefix),
                    )),
                    None => None,
                }
            },
        )
        .flat_map(futures_util::stream::iter);
        Ok(head.chain(tail).boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        // One pass over the last message of each subject under the filter.
        let buckets = self.buckets().await?;
        let store = self.bucket(buckets, ks);
        let filter = match prefix {
            p if p.is_empty() || p.ends_with('.') => format!("{p}>"),
            _ => ">".to_string(),
        };
        let consumer = store
            .stream
            .create_consumer(push::OrderedConfig {
                deliver_subject: buckets.client.new_inbox(),
                description: Some("spate listing".to_string()),
                filter_subject: format!("{}{filter}", store.prefix),
                deliver_policy: DeliverPolicy::LastPerSubject,
                replay_policy: ReplayPolicy::Instant,
                ..Default::default()
            })
            .await
            .map_err(|e| StoreError::Retryable(format!("listing {filter}: {e}")))?;
        let mut live = BTreeMap::new();
        let mut seen = BTreeSet::new();
        // An empty filter delivers nothing, so no message reports the end.
        if consumer.cached_info().num_pending != 0 {
            let mut messages = consumer
                .messages()
                .await
                .map_err(|e| StoreError::Retryable(format!("listing {filter}: {e}")))?;
            let stall = self.stall_bound();
            loop {
                let Some(message) = next_within(&mut messages, stall, "listing").await? else {
                    return Err(StoreError::Retryable(format!(
                        "listing {filter} ended early"
                    )));
                };
                let message =
                    message.map_err(|e| StoreError::Retryable(format!("listing {filter}: {e}")))?;
                let info = message
                    .info()
                    .map_err(|e| StoreError::Retryable(format!("listing {filter}: {e}")))?;
                let (pending, revision) = (info.pending, info.stream_sequence);
                if let Some(key) = message.subject.strip_prefix(store.prefix.as_str())
                    && key.starts_with(prefix)
                {
                    fold_listed(
                        &mut live,
                        key,
                        message.headers.as_ref(),
                        &message.payload,
                        revision,
                    );
                    seen.insert(key.to_string());
                }
                if pending == 0 {
                    break;
                }
            }
        }
        // Pending under-counts while a rewrite replaces a subject's last
        // message, so the consumer can stop before a live key; the leader's
        // subject set names every key the consumer skipped.
        let subjects = subjects_under(
            &buckets.jetstream,
            &store.stream_name,
            &format!("{}{filter}", store.prefix),
        )
        .await?;
        let mut reads =
            futures_util::stream::iter(unseen_keys(&subjects, &store.prefix, prefix, &seen))
                .map(|key| async move {
                    let last = last_on_leader(store, &key).await?;
                    Ok::<_, StoreError>((key, last))
                })
                .buffer_unordered(16);
        while let Some(read) = reads.next().await {
            if let (key, Some(message)) = read? {
                fold_listed(
                    &mut live,
                    &key,
                    Some(&message.headers),
                    &message.payload,
                    message.sequence,
                );
            }
        }
        Ok(live.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> async_nats::HeaderMap {
        let mut headers = async_nats::HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(*name, *value);
        }
        headers
    }

    /// A listing keeps each key's latest value and drops a key whose later
    /// message is any marker the server writes into a bucket, including a
    /// MaxAge limit marker, which carries no `KV-Operation`.
    #[test]
    fn a_listing_folds_values_and_markers() {
        let markers = [
            headers(&[("KV-Operation", "DEL")]),
            headers(&[("KV-Operation", "PURGE"), ("Nats-TTL", "2s")]),
            headers(&[
                ("Nats-Marker-Reason", "MaxAge"),
                ("Nats-TTL", "2s"),
                ("Nats-Rollup", "sub"),
            ]),
            headers(&[("Nats-Marker-Reason", "Remove")]),
        ];
        for marker in &markers {
            let mut live = BTreeMap::new();
            fold_listed(&mut live, "k", None, b"v", 1);
            fold_listed(&mut live, "k", Some(marker), b"", 2);
            assert!(live.is_empty(), "{marker:?}");
        }
        let mut live = BTreeMap::new();
        fold_listed(&mut live, "k", None, b"old", 1);
        fold_listed(
            &mut live,
            "k",
            Some(&headers(&[("KV-Operation", "PUT")])),
            b"new",
            3,
        );
        assert_eq!(
            live.into_values().collect::<Vec<_>>(),
            [Entry {
                key: "k".to_string(),
                value: b"new".to_vec(),
                revision: Revision(3),
            }]
        );
    }

    /// A subject yields a key when it is in the bucket, under the prefix, mid-token
    /// prefixes included, and not already delivered.
    #[test]
    fn unseen_keys_are_undelivered_and_under_the_prefix() {
        let subjects: BTreeSet<String> = [
            "$KV.b.hb.1",
            "$KV.b.hb.2",
            "$KV.b.hbx",
            "$KV.b.other.1",
            "$KV.c.hb.3",
        ]
        .map(String::from)
        .into();
        let seen: BTreeSet<String> = ["hb.1".to_string()].into();
        assert_eq!(
            unseen_keys(&subjects, "$KV.b.", "hb", &seen),
            ["hb.2", "hbx"]
        );
        assert_eq!(unseen_keys(&subjects, "$KV.b.", "hb.", &seen), ["hb.2"]);
        assert_eq!(
            unseen_keys(&subjects, "$KV.b.", "", &BTreeSet::new()),
            ["hb.1", "hb.2", "hbx", "other.1"]
        );
    }

    /// Pages after the first start inside the page before them, and a page
    /// whose first subject sorts past that page's last leaves a gap.
    #[test]
    fn subject_pages_overlap_and_join() {
        assert_eq!(next_subject_page(0, 40_000, 40_000, 100_000), None);
        assert_eq!(next_subject_page(0, 0, 0, 0), None);
        assert_eq!(
            next_subject_page(0, 100_000, 250_000, 100_000),
            Some(100_000 - SUBJECT_PAGE_OVERLAP)
        );
        let second = 100_000 - SUBJECT_PAGE_OVERLAP;
        assert_eq!(
            next_subject_page(second, 100_000, 250_000, 100_000),
            Some(second + 100_000 - SUBJECT_PAGE_OVERLAP)
        );
        assert_eq!(
            next_subject_page(second, 100_000, second + 100_000, 100_000),
            None
        );
        assert!(pages_join("k5", Some("k3")));
        assert!(pages_join("k5", Some("k5")));
        assert!(!pages_join("k5", Some("k6")));
        assert!(!pages_join("k5", None));
    }

    /// Both spellings parse from YAML text: the single-key map and the tagged
    /// form.
    #[test]
    fn credentials_parse_from_maps_and_tags() {
        for (yaml, expect) in [
            ("none", "None"),
            ("!none", "None"),
            ("{ token: t }", "Token"),
            ("!token t", "Token"),
            ("{ creds_file: /c }", "CredsFile"),
            ("!creds_file /c", "CredsFile"),
            (
                "{ user_password: { username: u, password: p } }",
                "UserPassword",
            ),
            (
                "!user_password { username: u, password: p }",
                "UserPassword",
            ),
        ] {
            let parsed: NatsCredentials =
                serde_yaml::from_str(yaml).unwrap_or_else(|e| panic!("{yaml}: {e}"));
            assert!(
                format!("{parsed:?}").starts_with(expect),
                "{yaml}: {parsed:?}"
            );
        }
    }

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

    /// With `tls` set, a `ws://` server in any spelling async-nats accepts
    /// fails validation without echoing the URL. Regression for #633.
    #[test]
    fn tls_rejects_a_plain_websocket_server() {
        let with_tls = |servers: &[&str]| {
            let mut config =
                NatsConfig::new(servers.iter().map(|s| (*s).into()).collect(), "orders");
            config.tls = Some(NatsTls::default());
            NatsStore::new(config, Duration::from_secs(30))
        };
        for (servers, index) in [
            (&["ws://nats-1.internal:8080"][..], 0),
            (&["WS://nats-1.internal:8080"], 0),
            (&[" ws://nats-1.internal:8080\n"], 0),
            (&["ws:nats-1.internal:8080/?next=nats://x"], 0),
            (
                &["wss://nats-0.internal:443", "ws://nats-1.internal:8080"],
                1,
            ),
        ] {
            let err = with_tls(servers).unwrap_err().to_string();
            assert!(err.contains(&format!("nats.servers[{index}]")), "{err}");
            assert!(err.contains("wss://"), "{err}");
            assert!(!err.contains("nats-1.internal"), "{err}");
        }
        with_tls(&["wss://nats-1.internal:443"]).unwrap();
        with_tls(&["nats://nats-1.internal:4222"]).unwrap();
        with_tls(&["nats-1.internal:4222"]).unwrap();
        let plain = NatsConfig::new(vec!["ws://nats-1.internal:8080".into()], "orders");
        NatsStore::new(plain, Duration::from_secs(30)).unwrap();
    }

    /// Server URL userinfo never reaches `Debug`. Regression for #754.
    #[test]
    fn debug_never_prints_server_userinfo() {
        let config = NatsConfig::new(
            vec![
                "nats://svc:hunter2@n1:4222".into(),
                "svc:hunter2@n2:4222".into(),
            ],
            "orders",
        );
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(
            rendered.contains("\"nats://<redacted>@n1:4222\""),
            "{rendered}"
        );
        assert!(rendered.contains("\"<redacted>@n2:4222\""), "{rendered}");
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
            ..base.clone()
        };
        let err = NatsStore::new(no_servers, Duration::from_secs(30)).unwrap_err();
        assert!(err.to_string().contains("servers"), "{err}");

        let with_second = |server: &str| {
            let config = NatsConfig {
                servers: vec!["nats://localhost:4222".into(), server.into()],
                ..base.clone()
            };
            NatsStore::new(config, Duration::from_secs(30))
        };
        for (server, reason) in [
            ("nats://secret-host:notaport", "not a NATS server URL"),
            ("nats://secret-host:4222,nats://b:4222", "holds a comma"),
            ("secret-host,b", "holds a comma"),
        ] {
            let err = with_second(server).unwrap_err().to_string();
            assert!(err.contains("nats.servers[1]"), "{err}");
            assert!(err.contains(reason), "{err}");
            assert!(!err.contains("secret-host"), "{err}");
        }
        with_second("localhost:4223").unwrap();
    }

    /// Adopting a bucket whose replica count differs from the configured one
    /// is fatal, and the message names both counts. Regression for #807.
    #[test]
    fn adoption_rejects_another_replica_count() {
        let wanted = kv::Config {
            bucket: "spate_coordination_orders_lease".into(),
            max_age: Duration::from_secs(30),
            num_replicas: 3,
            ..Default::default()
        };
        let existing = stream::Config {
            name: "KV_spate_coordination_orders_lease".into(),
            max_age: Duration::from_secs(30),
            num_replicas: 3,
            ..Default::default()
        };
        check_adopted(&wanted, &existing).expect("matching bucket");
        for replicas in [1, 5] {
            let other = stream::Config {
                num_replicas: replicas,
                ..existing.clone()
            };
            let err = check_adopted(&wanted, &other).unwrap_err();
            assert!(matches!(err, StoreError::Fatal(_)), "{err}");
            let err = err.to_string();
            assert!(
                err.contains(&format!("exists with replica count {replicas} ")),
                "{err}"
            );
            assert!(err.contains("configured for 3"), "{err}");
            assert!(err.contains("spate_coordination_orders_lease"), "{err}");
        }
        let aged = stream::Config {
            max_age: Duration::from_secs(10),
            ..existing
        };
        let err = check_adopted(&wanted, &aged).unwrap_err().to_string();
        assert!(err.contains("max_age"), "{err}");
    }

    #[test]
    fn message_ttls_patch_only_the_ttl_fields() {
        let existing = stream::Config {
            name: "KV_spate_coordination_orders_state".into(),
            description: Some("operator note".into()),
            persist_mode: Some(stream::PersistenceMode::Async),
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

    /// Serves a NATS server on `127.0.0.1` that answers every CONNECT with an
    /// authorization violation, and returns its port.
    pub(super) async fn serve_authorization_violation() -> u16 {
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
        port
    }

    /// Connects to `servers` as a user the server rejects and returns the
    /// first store operation's result.
    async fn get_as_rejected_user(servers: Vec<String>) -> Result<Option<Entry>, StoreError> {
        let mut config = NatsConfig::new(servers, "auth_test");
        config.credentials = NatsCredentials::UserPassword {
            username: "spate".into(),
            password: Secret::new("wrong"),
        };
        let store = NatsStore::new(config, Duration::from_secs(30)).unwrap();
        store.get(Keyspace::Durable, "k").await
    }

    /// A server that answers the CONNECT with an authorization violation fails
    /// the connect with a fatal error. Regression for #634.
    #[tokio::test]
    async fn a_rejected_credential_is_fatal() {
        let port = serve_authorization_violation().await;
        match get_as_rejected_user(vec![format!("nats://127.0.0.1:{port}")]).await {
            Err(StoreError::Fatal(message)) => {
                assert!(message.contains("authorization violation"), "{message}");
            }
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    /// With two servers, the connect reaches the one that answers whichever
    /// order it tries them in. Regression for #753.
    #[tokio::test]
    async fn every_listed_server_joins_the_pool() {
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_port = closed.local_addr().unwrap().port();
        drop(closed);
        let port = serve_authorization_violation().await;
        let servers = vec![
            format!("nats://127.0.0.1:{closed_port}"),
            format!("nats://127.0.0.1:{port}"),
        ];
        match get_as_rejected_user(servers).await {
            Err(StoreError::Fatal(message)) => {
                assert!(message.contains("authorization violation"), "{message}");
            }
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    /// A connect that fails with no handshake parameter in common is fatal;
    /// one that fails on an alert outside `TLS_REJECTION_ALERTS` is not.
    /// Regression for #718.
    #[test]
    fn connect_error_covers_an_incompatible_peer_and_only_listed_alerts() {
        use async_nats::rustls;
        let failed = |tls: rustls::Error| {
            connect_error(async_nats::ConnectError::with_source(
                async_nats::ConnectErrorKind::Io,
                std::io::Error::new(std::io::ErrorKind::InvalidData, tls),
            ))
        };
        let incompatible = failed(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::NoCipherSuitesInCommon,
        ));
        assert!(
            matches!(&incompatible, StoreError::Fatal(m) if m.contains("NoCipherSuitesInCommon")),
            "{incompatible:?}"
        );
        let internal = failed(rustls::Error::AlertReceived(
            rustls::AlertDescription::InternalError,
        ));
        assert!(matches!(internal, StoreError::Retryable(_)), "{internal:?}");
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
