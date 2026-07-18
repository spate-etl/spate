//! The control plane: [`S3Source`] implements the framework's
//! [`Source`] trait by delegating the entire assignment choreography to
//! the coordination layer's `CoordinationDriver`.
//!
//! Lifecycle: `open` builds the data store, takes the coordinator the
//! deployer assembled ([`S3Source::with_coordinator`]; a solo in-process
//! one is built when none was injected), and stashes the planner; the
//! first `poll_events` joins the job via `driver.start` (leader-only
//! listing + packing happen in the planner), and every later call drains
//! poison reports into `driver.fail` and then delegates to
//! `driver.poll_events` — split gains materialize lanes through the
//! sibling [`SplitCtx`], losses retire them, `AllComplete` drains the
//! pipeline. `commit` routes acked watermarks into per-split fenced
//! progress commits; the coordination store is the source's **only**
//! progress store.
//!
//! No coordination backend appears in this crate's public API or in its
//! cargo features: which store a deployment uses (NATS today, others
//! tomorrow) is assembly wiring, kept out of every connector on purpose.
//! The one backend it does link is `etl-coordination`'s in-process
//! `MemoryStore`, unconditionally, as the solo fallback below.

use crate::config::S3SourceConfig;
use crate::lane::S3Lane;
use crate::metrics::S3Metrics;
use crate::planner::{S3Planner, job_fingerprint};
use crate::split_ctx::SplitCtx;
use etl_coordination::store::memory::MemoryStore;
use etl_coordination::{CoordinationConfig, StoreCoordinator};
use etl_core::config::{ComponentConfig, ConfigError};
use etl_core::coordination::driver::CoordinationDriver;
use etl_core::coordination::{PlanFinality, SplitCoordinator};
use etl_core::error::{ErrorClass, SourceError};
use etl_core::framing::{FramingContract, RecordFramer};
use etl_core::metrics::CoordinationMetrics;
use etl_core::record::PartitionId;
use etl_core::source::{LaneId, Source, SourceCtx, SourceEvent};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ClientConfigKey, ObjectStore, ObjectStoreScheme};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// First backoff step for fetcher-internal GET retries.
const RETRY_BASE: Duration = Duration::from_millis(200);

/// Where the source is in its lifecycle.
enum State {
    /// Constructed, not yet opened.
    Created,
    /// Opened: driver and lane-assembly context live side by side (they
    /// borrow disjointly); the planner is handed to the driver on the
    /// first `poll_events`. Boxed: the open state dwarfs `Created` and
    /// lives once per source.
    Open(Box<OpenState>),
}

/// The contents of [`State::Open`].
struct OpenState {
    driver: CoordinationDriver,
    ctx: SplitCtx,
    planner: Option<S3Planner>,
}

/// Coordinated object-storage backfill source. See the crate docs for the
/// delivery model and the split identity/drift contract.
pub struct S3Source {
    config: S3SourceConfig,
    handle: tokio::runtime::Handle,
    /// The per-object record framer, supplied by the chosen format via
    /// [`with_framer`](S3Source::with_framer). Required before the pipeline
    /// opens the source — `etl-s3` is a transport and owns no framing itself.
    framer: Option<crate::framer::FramerFactory>,
    /// A coordinator injected via [`with_coordinator`](S3Source::with_coordinator),
    /// overriding the config-driven store construction.
    coordinator: Option<Box<dyn SplitCoordinator>>,
    /// A data store injected via the `testing`-gated `with_store` seam.
    store: Option<Arc<dyn ObjectStore>>,
    state: State,
}

impl std::fmt::Debug for S3Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Source")
            .field("url", &self.config.url)
            .field("opened", &matches!(self.state, State::Open { .. }))
            .finish()
    }
}

impl S3Source {
    /// A source over `config`, doing its network I/O on `io` — pass the
    /// pipeline's I/O runtime handle
    /// ([`Pipeline::io_handle`](etl_core::pipeline::Pipeline::io_handle)).
    ///
    /// The handle **must belong to a multi-thread runtime** that outlives
    /// the pipeline (the pipeline's I/O runtime satisfies both): the
    /// source, its lanes, and the coordinator briefly block pipeline
    /// threads on it, which a current-thread runtime cannot drive.
    #[must_use]
    pub fn new(config: S3SourceConfig, io: tokio::runtime::Handle) -> S3Source {
        S3Source {
            config,
            handle: io,
            framer: None,
            coordinator: None,
            store: None,
            state: State::Created,
        }
    }

    /// Build from the pipeline's opaque `source: { s3: ... }` section.
    pub fn from_component_config(
        section: &ComponentConfig,
        io: tokio::runtime::Handle,
    ) -> Result<S3Source, ConfigError> {
        Ok(S3Source::new(
            S3SourceConfig::from_component_config(section)?,
            io,
        ))
    }

    /// Set the record framer that cuts each object's byte stream into records.
    /// `etl-s3` is a transport and owns no framing, so this is **required**:
    /// supply the framer for the objects' format — e.g. `etl-json`'s
    /// `NdjsonFramer` for NDJSON — before the pipeline opens the source.
    ///
    /// `factory` builds a fresh
    /// [`RecordFramer`](etl_core::framing::RecordFramer) per object (framers are
    /// per-object stateful and each lane frames its own split). A framed source
    /// always emits one record per payload, so its
    /// [`FramingContract`] is [`PerRecord`](FramingContract::PerRecord) and the
    /// paired deserializer decodes a single unit.
    #[must_use]
    pub fn with_framer<F>(mut self, factory: F) -> S3Source
    where
        F: Fn() -> Box<dyn RecordFramer> + Send + Sync + 'static,
    {
        self.framer = Some(Arc::new(factory));
        self
    }

    /// Hand the source its coordinator — **the** multi-instance seam.
    /// Build any [`SplitCoordinator`] at assembly time (e.g.
    /// `StoreCoordinator` over the NATS store from `etl-coordination`,
    /// with your own tuning) and inject it here; run more replicas of the
    /// same pipeline against the same backend and they share the
    /// backfill. Must be called before the pipeline opens the source.
    ///
    /// Without it the source runs **solo** over an in-process store:
    /// correct and self-terminating, but progress is ephemeral — a
    /// restart replays the whole prefix (a startup WARN says so).
    #[must_use]
    pub fn with_coordinator(mut self, coordinator: Box<dyn SplitCoordinator>) -> S3Source {
        self.coordinator = Some(coordinator);
        self
    }

    /// Test seam: replace the data store built from `config.url` (the URL
    /// still supplies the listing prefix). Lets tests wrap the store with
    /// failure- or call-counting decorators.
    ///
    /// Behind the off-by-default `testing` feature because it exposes an
    /// `object_store` type, which this crate's public API otherwise avoids
    /// entirely; no stability promise attaches to it.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_store(mut self, store: Arc<dyn ObjectStore>) -> S3Source {
        self.store = Some(store);
        self
    }
}

impl Source for S3Source {
    type Lane = S3Lane;

    fn component_type(&self) -> &str {
        "s3"
    }

    fn framing_contract(&self) -> FramingContract {
        // A framed source always emits one record per payload. (The framer is
        // required; a missing one is caught at `open`.)
        FramingContract::PerRecord
    }

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        if !matches!(self.state, State::Created) {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "source opened twice".into(),
            });
        }
        let Some(make_framer) = self.framer.clone() else {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "S3Source has no record framer; supply one with `with_framer(...)` \
                         (e.g. etl-json's NdjsonFramer) before running the pipeline"
                    .into(),
            });
        };
        // Defense in depth for hand-constructed configs; cheap.
        self.config.validate().map_err(|e| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: e.to_string(),
        })?;

        let url = parse(&self.config.url)?;
        let (store, prefix) = match self.store.take() {
            // Injected store: the URL still names the listing prefix.
            Some(store) => {
                let (_, path) =
                    ObjectStoreScheme::parse(&url).map_err(|e| SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!("parsing {}: {e}", self.config.url),
                    })?;
                (store, path)
            }
            None => {
                let (store, path) =
                    build_store(&url, &self.config.store).map_err(|e| SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!("building the object store for {}: {e}", self.config.url),
                    })?;
                (Arc::from(store), path)
            }
        };

        let metrics = ctx.meter.as_ref().map(S3Metrics::new);
        let coordinator = match self.coordinator.take() {
            Some(injected) => injected,
            None => {
                // Coordination metrics are opt-in and constructor-injected
                // (there is no later hook); the solo coordinator shares
                // the source's labels.
                let coord_metrics = ctx
                    .meter
                    .as_ref()
                    .map(|m| CoordinationMetrics::new(m.labels()));
                solo_coordinator(self.handle.clone(), coord_metrics)?
            }
        };

        let finality = if self.config.refresh_listing {
            PlanFinality::Open
        } else {
            PlanFinality::Final
        };
        let planner = S3Planner::new(
            Arc::clone(&store),
            Some(prefix),
            self.handle.clone(),
            self.config.split_target_bytes.as_u64(),
            finality,
            job_fingerprint(
                &self.config.url,
                self.config.compression,
                self.config.split_target_bytes.as_u64(),
                self.config.refresh_listing,
            ),
            metrics.clone(),
        );
        let split_ctx = SplitCtx::new(
            store,
            self.handle.clone(),
            ctx.issuer,
            make_framer,
            self.config.compression,
            self.config.chunk_bytes.as_u64() as usize,
            self.config.prefetch_bytes.as_u64() as usize,
            RETRY_BASE,
            metrics,
        );
        self.state = State::Open(Box::new(OpenState {
            driver: CoordinationDriver::new(coordinator),
            ctx: split_ctx,
            planner: Some(planner),
        }));
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<S3Lane>, SourceError> {
        let State::Open(open) = &mut self.state else {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "poll_events before open".into(),
            });
        };
        let OpenState {
            driver,
            ctx,
            planner,
        } = open.as_mut();
        if let Some(planner) = planner.take() {
            // Join the job; the empty ready signal — splits arrive as
            // later assignments while claims race.
            return driver.start(Box::new(planner));
        }
        // Object-level failures first: hand each poisoned split back (a
        // delivery attempt is consumed; quarantine happens at the cap).
        for report in ctx.drain_poison() {
            tracing::warn!(
                split = %report.split,
                reason = report.kind.reason_label(),
                detail = %report.reason,
                "object-level failure; handing the split back to the coordinator"
            );
            driver.fail(ctx, &report.split, &report.reason)?;
        }
        driver.poll_events(ctx, timeout)
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        let State::Open(open) = &mut self.state else {
            debug_assert!(watermarks.is_empty(), "watermarks before open");
            return Ok(());
        };
        let OpenState { driver, ctx, .. } = open.as_mut();
        driver.commit(ctx, watermarks)
    }

    // `flush_commits` keeps the trait's no-op default: an Ok fenced commit
    // is already store-durable, and a Retryable one is driver-cached and
    // recommitted on the next tick.

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        if let State::Open(open) = &self.state {
            open.ctx.set_paused(lanes, true);
        }
        Ok(())
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        if let State::Open(open) = &self.state {
            open.ctx.set_paused(lanes, false);
        }
        Ok(())
    }
}

impl Drop for S3Source {
    fn drop(&mut self) {
        if let State::Open(open) = &mut self.state {
            // Graceful hand-back: peers claim the splits without waiting
            // out the lease. Fetchers are detached and exit as their lanes
            // drop.
            open.driver.release();
        }
    }
}

/// The fallback when no coordinator was injected: a solo, in-process
/// coordinator with default tuning. Correct and self-terminating, but its
/// store dies with the process — hence the WARN.
fn solo_coordinator(
    handle: tokio::runtime::Handle,
    metrics: Option<CoordinationMetrics>,
) -> Result<Box<dyn SplitCoordinator>, SourceError> {
    tracing::warn!(
        "no coordinator injected: running solo over an in-process store; progress is \
         EPHEMERAL and a restart replays the entire prefix (at-least-once, safe but \
         wasteful) — inject one via with_coordinator (e.g. a StoreCoordinator over a \
         durable backend) for durable resume and multi-instance sharing"
    );
    let tuning = CoordinationConfig::default();
    let store = MemoryStore::new(tuning.lease_duration);
    let coordinator =
        StoreCoordinator::new(store, tuning, handle, metrics).map_err(|e| SourceError::Client {
            class: e.class(),
            reason: format!("building the solo coordinator: {e}"),
        })?;
    Ok(Box::new(coordinator))
}

fn parse(url: &str) -> Result<Url, SourceError> {
    Url::parse(url).map_err(|e| SourceError::Client {
        class: ErrorClass::Fatal,
        reason: format!("invalid URL {url}: {e}"),
    })
}

fn opts(map: &std::collections::BTreeMap<String, String>) -> Vec<(String, String)> {
    map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Build the object store for `url`, applying the raw `store` passthrough
/// `options`.
///
/// For S3, credentials and region are seeded from the standard AWS
/// environment via [`AmazonS3Builder::from_env`] — the same mechanism the AWS
/// SDK uses — so EKS Pod Identity, IRSA (web identity), ECS task roles, IMDS,
/// and `AWS_REGION`/`AWS_DEFAULT_REGION` all resolve without configuration.
/// The explicit `options` are overlaid on top and win over the environment;
/// the bucket comes from the URL. `object_store::parse_url_opts` (which every
/// other scheme still uses) deliberately does *not* read the environment, so
/// the S3 scheme is special-cased here.
///
/// We must not simply pass `std::env::vars()` through `parse_url_opts`:
/// `AmazonS3ConfigKey` parses *unprefixed* keys (`region`, `token`,
/// `endpoint`, ...), so an unrelated environment variable could hijack the
/// client. `from_env` guards on the `AWS_` prefix; we rely on that.
fn build_store(
    url: &Url,
    options: &std::collections::BTreeMap<String, String>,
) -> Result<(Box<dyn ObjectStore>, object_store::path::Path), object_store::Error> {
    // `ObjectStoreScheme::parse` returns the scheme and the in-store `Path` —
    // the very call `parse_url_opts` makes, so `path` is the exact listing
    // prefix it would return and every planned descriptor that depends on it
    // stays valid.
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    // Every non-S3 scheme (notably `file://` for infrastructure-free runs and
    // tests) keeps the stock, scheme-generic behaviour.
    if !matches!(scheme, ObjectStoreScheme::AmazonS3) {
        return object_store::parse_url_opts(url, opts(options));
    }
    // Harden the HTTP client (idle-connection recycling, explicit timeouts)
    // before overlaying the operator's `store` options, so the passthrough
    // still wins — see `harden_client_defaults`.
    let builder = harden_client_defaults(AmazonS3Builder::from_env().with_url(url.as_str()));
    let store = apply_options(builder, options).build()?;
    Ok((Box::new(store), path))
}

/// Conservative HTTP-client defaults for the S3 backend, applied *before* the
/// operator's `store` passthrough so the passthrough still overrides them (and
/// so unrelated client options seeded from the environment — `allow_http`, a
/// proxy — are left untouched, unlike a wholesale `with_client_options`).
///
/// `pool_idle_timeout` is the load-bearing one: object_store leaves it unset, so
/// a lane can pull a connection the server (or an intermediary) has already
/// half-closed and fail its next request with an incomplete-message body error
/// — the churn a back-pressured backfill provokes. Recycling idle connections
/// after 30s avoids that. `read_timeout` bounds an otherwise-unbounded stalled
/// body read; `connect_timeout` pins object_store's own 5s default explicitly.
/// The overall request `timeout` keeps object_store's 30s default: bounded
/// ranged GETs (see [`fetch`](crate::fetch)) each complete at network speed,
/// well inside it.
fn harden_client_defaults(builder: AmazonS3Builder) -> AmazonS3Builder {
    builder
        .with_config(
            AmazonS3ConfigKey::Client(ClientConfigKey::ConnectTimeout),
            "5s",
        )
        .with_config(
            AmazonS3ConfigKey::Client(ClientConfigKey::ReadTimeout),
            "30s",
        )
        .with_config(
            AmazonS3ConfigKey::Client(ClientConfigKey::PoolIdleTimeout),
            "30s",
        )
}

/// Overlay the raw `store` passthrough options onto an S3 builder. Keys that
/// are not recognised object_store configuration are ignored (matching
/// `parse_url_opts`); a later key wins, so these override any environment
/// value seeded by [`AmazonS3Builder::from_env`].
fn apply_options(
    mut builder: AmazonS3Builder,
    options: &std::collections::BTreeMap<String, String>,
) -> AmazonS3Builder {
    for (k, v) in options {
        if let Ok(key) = k.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() {
            builder = builder.with_config(key, v.clone());
        }
    }
    builder
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn apply_options_overrides_env_seeded_values() {
        // A builder already seeded from the environment (as `from_env` would).
        let seeded = AmazonS3Builder::new()
            .with_config(AmazonS3ConfigKey::Region, "env-region")
            .with_config(AmazonS3ConfigKey::Endpoint, "http://env-endpoint");
        let b = apply_options(seeded, &map(&[("aws_region", "opt-region")]));
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::Region).as_deref(),
            Some("opt-region"),
            "an explicit passthrough option overrides the environment"
        );
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::Endpoint).as_deref(),
            Some("http://env-endpoint"),
            "a key we did not set leaves the seeded value intact"
        );
    }

    #[test]
    fn apply_options_sets_pod_identity_keys_and_ignores_unknown() {
        const URI: &str = "http://169.254.170.23/v1/credentials";
        const TOKEN: &str =
            "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token";
        let b = apply_options(
            AmazonS3Builder::new(),
            &map(&[
                ("aws_container_credentials_full_uri", URI),
                ("aws_container_authorization_token_file", TOKEN),
                // Not an object_store key — must be ignored, not panic.
                ("not_a_real_key", "whatever"),
            ]),
        );
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::ContainerCredentialsFullUri)
                .as_deref(),
            Some(URI)
        );
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::ContainerAuthorizationTokenFile)
                .as_deref(),
            Some(TOKEN)
        );
    }

    /// The S3 listing prefix must be byte-identical to what `parse_url_opts`
    /// produces — planned descriptors and their offsets depend on it.
    #[test]
    fn build_store_s3_prefix_matches_parse_url_opts() {
        let url = Url::parse("s3://bucket/exports/2026/").unwrap();
        let (_, got) = build_store(&url, &BTreeMap::new()).unwrap();
        let (_, want) =
            object_store::parse_url_opts(&url, std::iter::empty::<(String, String)>()).unwrap();
        assert_eq!(got, want);
    }

    #[test]
    fn hardened_client_defaults_are_set_and_overridable() {
        let idle = AmazonS3ConfigKey::Client(ClientConfigKey::PoolIdleTimeout);
        let b = harden_client_defaults(AmazonS3Builder::new());
        assert_eq!(
            b.get_config_value(&idle).as_deref(),
            Some("30s"),
            "pool-idle recycling is on by default, so half-closed connections are \
             not reused"
        );
        // The operator's `store` passthrough still wins over the hardened value.
        let overridden = apply_options(
            harden_client_defaults(AmazonS3Builder::new()),
            &map(&[("pool_idle_timeout", "5s")]),
        );
        assert_eq!(
            overridden.get_config_value(&idle).as_deref(),
            Some("5s"),
            "a store: entry overrides the hardened default"
        );
    }

    #[test]
    fn build_store_delegates_non_s3_schemes() {
        let dir = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(dir.path()).unwrap();
        let (_, got) = build_store(&url, &BTreeMap::new()).unwrap();
        let (_, want) =
            object_store::parse_url_opts(&url, std::iter::empty::<(String, String)>()).unwrap();
        assert_eq!(got, want, "file:// delegates to parse_url_opts unchanged");
    }
}
