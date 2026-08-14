//! NATS JetStream KV [`CoordinationStore`]: the production backend.
//!
//! Two KV buckets per job carry the two keyspaces:
//!
//! - `spate_coordination_{job}_state` — durable: no age limit; split
//!   records and the plan record survive owner death.
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
//! startup probe, so connect failures ride the startup
//! retry budget and startup-time misconfiguration is Fatal with an
//! actionable message. No `async-nats` type appears in any public
//! signature (0.x policy: single pinned minor, internal only).

use super::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use async_nats::jetstream::kv;
use futures_util::StreamExt as _;
use serde::Deserialize;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
    /// Extra root CA bundle (PEM), for private CAs.
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
    if let Some(tls) = &config.tls {
        options = options.require_tls(true);
        if let Some(root_ca) = &tls.root_ca {
            options = options.add_root_certificates(root_ca.clone());
        }
        if let (Some(cert), Some(key)) = (&tls.client_cert, &tls.client_key) {
            options = options.add_client_certificate(cert.clone(), key.clone());
        }
    }
    let client = options
        .connect(config.servers.join(","))
        .await
        .map_err(|e| StoreError::Retryable(format!("connecting to NATS: {e}")))?;

    let info = client.server_info();
    if !server_at_least(&info.version, MIN_SERVER) {
        return Err(StoreError::Fatal(format!(
            "NATS server {} is too old: coordination needs >= {}.{} (per-message TTLs \
             and KV limit markers); upgrade the server",
            info.version, MIN_SERVER.0, MIN_SERVER.1
        )));
    }

    let jetstream = async_nats::jetstream::new(client);
    let state = ensure_bucket(
        &jetstream,
        kv::Config {
            bucket: format!("spate_coordination_{}_state", config.job),
            description: "Spate coordination: durable split and plan records".into(),
            history: 1,
            max_value_size: MAX_VALUE_BYTES,
            num_replicas: config.replicas,
            ..Default::default()
        },
    )
    .await?;
    let lease = ensure_bucket(
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
    Ok(Buckets { state, lease })
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
/// that matters (max_age, which IS the lease TTL) matches.
async fn ensure_bucket(
    jetstream: &async_nats::jetstream::Context,
    config: kv::Config,
) -> Result<kv::Store, StoreError> {
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
            Ok(store)
        }
        Err(_) => jetstream
            .create_key_value(config)
            .await
            .map_err(|e| StoreError::Retryable(format!("creating bucket {name}: {e}"))),
    }
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
        let result = self
            .bucket(buckets, ks)
            .delete_expect_revision(key, expected.map(|r| r.0))
            .await;
        match result {
            // NATS does not report the marker's revision; callers only
            // branch on the outcome.
            Ok(()) => Ok(CasOutcome::Won(Revision(0))),
            Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => Ok(CasOutcome::Lost),
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
            keys.next().await.is_none()
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
        let tail = futures_util::stream::unfold(
            (watcher, caught_up),
            |(mut watcher, mut caught_up)| async move {
                match watcher.next().await {
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
        let mut out = Vec::new();
        while let Some(key) = keys.next().await {
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
    fn version_floor_parses_real_world_strings() {
        assert!(server_at_least("2.11.4", MIN_SERVER));
        assert!(server_at_least("2.12.0-beta.1", MIN_SERVER));
        assert!(server_at_least("3.0.0", MIN_SERVER));
        assert!(!server_at_least("2.10.22", MIN_SERVER));
        assert!(!server_at_least("2.9", MIN_SERVER));
        assert!(!server_at_least("garbage", MIN_SERVER));
    }
}
