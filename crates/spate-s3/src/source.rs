//! The control plane: [`S3Source`] implements the framework's
//! [`Source`] trait by delegating the entire assignment choreography to
//! the coordination layer's `CoordinationDriver`.
//!
//! Lifecycle: `open` builds the data store, takes its coordinator (one
//! injected with [`S3Source::with_coordinator`], else one built from the
//! pipeline's `coordination:` section, else a solo in-process one), and
//! stashes the planner; the
//! first `poll_events` joins the job via `driver.start` (leader-only
//! listing + packing happen in the planner), and every later call drains
//! poison reports into `driver.fail` and then delegates to
//! `driver.poll_events`. Split gains materialize lanes through the
//! sibling [`SplitCtx`], losses retire them, and `AllComplete` drains the
//! pipeline. `commit` routes acked watermarks into per-split fenced
//! progress commits; the coordination store is the source's **only**
//! progress store.
//!
//! A source given a feed, by the pipeline's `feed:` section or through the
//! `testing`-gated `with_feed`, reads work units through
//! `unit_reader::UnitReader` in place of all of the above. When the section
//! names a `location`, `open` first waits for the lister's job fingerprint
//! there and refuses a mismatch.
//!
//! No coordination backend appears in this crate's public API or in its
//! cargo features. The store comes from the `coordination:` section through
//! `spate-coordination`, whose features decide which stores a build has.
//! The one backend this crate links itself is the in-process `MemoryStore`,
//! unconditionally, as the solo fallback below.

#![expect(deprecated, reason = "v0.4.0 removes this with spate-coordination")]

use crate::config::{S3SourceConfig, s3_store_key};
use crate::lane::S3Lane;
use crate::listing::{UnaddressableConnector, UnaddressableKeys};
use crate::location::{Location, SETTLING_WINDOW, await_fingerprint, feed_fingerprint};
use crate::metrics::S3Metrics;
use crate::planner::{S3Planner, job_fingerprint};
use crate::split::Packing;
use crate::split_ctx::SplitCtx;
use crate::unit::Scope;
use crate::unit_reader::{LaneParts, UnitReader};
use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ClientConfigKey, ObjectStore, ObjectStoreScheme};
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::{CoordinationConfig, CoordinatorSpec, StoreCoordinator};
use spate_core::clock::tokio::{Clock, SystemClock};
use spate_core::config::{ComponentConfig, ConfigError, CoordinationSection, FeedSection};
use spate_core::coordination::driver::CoordinationDriver;
use spate_core::coordination::{PlanFinality, SplitCoordinator};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::feed::Feed;
use spate_core::framing::{FramingContract, RecordFramer};
use spate_core::metrics::CoordinationMetrics;
use spate_core::record::PartitionId;
use spate_core::source::{LaneId, Source, SourceCtx, SourceEvent};
use std::num::NonZeroUsize;
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
    /// Opened over a feed: units are read one lane per part in progress.
    Feed(Box<UnitReader>),
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
    /// opens the source; `spate-s3` is a transport and owns no framing itself.
    framer: Option<crate::framer::FramerFactory>,
    /// A coordinator injected via [`with_coordinator`](S3Source::with_coordinator).
    coordinator: Option<Box<dyn SplitCoordinator>>,
    /// The pipeline's `coordination:` section, validated and built in `open`.
    spec: Option<CoordinatorSpec>,
    /// A data store injected via the `testing`-gated `with_store` seam.
    store: Option<Arc<dyn ObjectStore>>,
    /// The feed to read units from: the one the `feed:` section names, or
    /// one injected via the `testing`-gated `with_feed` seam.
    feed: Option<Arc<dyn Feed>>,
    /// The `feed:` section's `units_held`.
    units_held: Option<NonZeroUsize>,
    /// The `feed:` section's `location`, for a bounded job.
    location: Option<Location>,
    /// A location store injected via the `testing`-gated
    /// `with_location_store` seam.
    location_store: Option<Arc<dyn ObjectStore>>,
    /// Times the wait for the job fingerprint.
    clock: Arc<dyn Clock>,
    state: State,
}

impl std::fmt::Debug for S3Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Source")
            .field("url", &self.config.url)
            .field("opened", &!matches!(self.state, State::Created))
            .finish()
    }
}

impl S3Source {
    /// A source over `config`, doing its network I/O on `io`. Pass the
    /// pipeline's I/O runtime handle
    /// ([`Pipeline::io_handle`](spate_core::pipeline::Pipeline::io_handle)).
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
            spec: None,
            store: None,
            feed: None,
            units_held: None,
            location: None,
            location_store: None,
            clock: Arc::new(SystemClock),
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
    /// `spate-s3` is a transport and owns no framing, so this is **required**:
    /// supply the framer for the objects' format (e.g. `spate-json`'s
    /// `NdjsonFramer` for NDJSON) before the pipeline opens the source.
    ///
    /// `factory` builds a fresh
    /// [`RecordFramer`](spate_core::framing::RecordFramer) per object (framers are
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

    /// Hand the source a coordinator built in code, such as a
    /// `StoreCoordinator` over a custom store. Must be called before the
    /// pipeline opens the source, and conflicts with a `coordination:`
    /// section in the pipeline config.
    ///
    /// Without either the source runs **solo** over an in-process store with
    /// default tuning; a section naming `store: { memory: {} }` runs it with
    /// the section's tuning. Either way progress is ephemeral, and a restart
    /// replays the whole prefix (a startup WARN says so).
    #[must_use]
    #[deprecated(
        since = "0.3.0",
        note = "removed in v0.4.0, where a work feed replaces split coordination; see https://github.com/spate-etl/spate/issues/1088"
    )]
    pub fn with_coordinator(mut self, coordinator: Box<dyn SplitCoordinator>) -> S3Source {
        self.coordinator = Some(coordinator);
        self
    }

    /// Test seam: replace the data store built from `config.url` (the URL
    /// still supplies the listing prefix). Lets tests wrap the store with
    /// failure- or call-counting decorators. On the feed path a source given
    /// its store reads storage notifications only under an `s3://` or
    /// `s3a://` URL, whose host is the bucket their records must name. A
    /// lister over a store given here resumes from its checkpoint as over the
    /// S3 client, except under a `file://` URL, where it keeps none. Under any
    /// other URL the store must list the keys after an offset in its own
    /// listing order, as `InMemory` and the S3 client do and
    /// `LocalFileSystem` does not.
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

    /// Test seam: read work units from `feed` in place of coordinated
    /// splits. Each unit's body is a [`SplitDescriptor`] naming one object,
    /// whole or one byte range of it, or a storage notification read as one
    /// part per non-empty created object; any other body is failed back to
    /// the feed. A whole object that qualifies for byte ranges under
    /// `split_target_bytes` is read in ranges. Conflicts with
    /// [`with_coordinator`](S3Source::with_coordinator), with a
    /// `coordination:` section and with a `feed:` section, reads no job
    /// location, and never reports the input drained, so the run ends at a
    /// shutdown.
    ///
    /// Behind the off-by-default `testing` feature, outside the semver
    /// surface.
    ///
    /// [`SplitDescriptor`]: crate::SplitDescriptor
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_feed(mut self, feed: Arc<dyn Feed>) -> S3Source {
        self.feed = Some(feed);
        self
    }

    /// Test seam: replace the store built from the `feed:` section's
    /// `location` (the location still names the prefix within it).
    ///
    /// Behind the off-by-default `testing` feature, outside the semver
    /// surface.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_location_store(mut self, store: Arc<dyn ObjectStore>) -> S3Source {
        self.location_store = Some(store);
        self
    }

    /// Test seam: time the wait for the job fingerprint on `clock`.
    ///
    /// Behind the off-by-default `testing` feature, outside the semver
    /// surface.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> S3Source {
        self.clock = clock;
        self
    }

    /// Test seam: list the prefix once and publish one unit per object or
    /// byte range to `feed`, blocking the calling thread on the I/O handle.
    ///
    /// With a `location` in `section` it first compares the job fingerprint
    /// there, or, when there is none, deletes a checkpoint it finds and
    /// creates the fingerprint, then deletes an earlier end marker. Beside a
    /// fingerprint it compared, it reads the lister's checkpoint and lists
    /// only the keys after it. It rewrites the
    /// checkpoint at most once per [`CHECKPOINT_EVERY`] listed keys and once
    /// at the end, each time after the feed confirmed every unit published
    /// before it. Over a `file://` store or an S3 directory bucket it keeps
    /// no checkpoint. It writes no end marker. The lister run's entry point
    /// replaces it.
    ///
    /// Behind the off-by-default `testing` feature, outside the semver
    /// surface.
    ///
    /// # Errors
    ///
    /// Fatal for a durable feed with no `location`, the in-process feed with
    /// one, a source with no framer, another fingerprint at the location, a
    /// checkpoint that is not a key under the listing prefix, and a store
    /// that cannot create an object conditionally. A publish the feed refuses
    /// keeps the class the feed gave it, and a failed checkpoint read or write
    /// the class of its store error.
    ///
    /// [`CHECKPOINT_EVERY`]: crate::lister::CHECKPOINT_EVERY
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn list_once(
        mut self,
        section: &FeedSection,
        feed: Arc<dyn Feed>,
    ) -> Result<(), SourceError> {
        let fatal = |reason: String| SourceError::Client {
            class: ErrorClass::Fatal,
            reason,
        };
        let location = section
            .location
            .as_deref()
            .map(|location| self.parse_location(location))
            .transpose()
            .map_err(|e| fatal(e.to_string()))?;
        if location.is_none() && section.backend().type_tag() != "memory" {
            return Err(fatal(format!(
                "a lister on the `{}` feed runs a bounded job, which keeps its fingerprint at \
                 a job location; set `feed.location`",
                section.backend().type_tag()
            )));
        }
        if location.is_some() && section.backend().type_tag() == "memory" {
            return Err(fatal(
                "the in-process feed keeps no job location; remove `feed.location`".into(),
            ));
        }
        let make_framer = self.framer.clone().ok_or_else(|| {
            fatal("S3Source has no record framer; supply one with `with_framer(...)`".into())
        })?;
        self.config.validate().map_err(|e| fatal(e.to_string()))?;
        let (store, prefix, unaddressable, _, resume) = self.data_store()?;
        let delimiter = make_framer().resync_delimiter();
        let location = match location {
            Some(location) => Some((self.location_store(&location)?, location)),
            None => None,
        };
        let lister = crate::lister::Lister {
            store,
            prefix,
            packing: Packing {
                target_bytes: self.config.split_target_bytes.as_u64(),
                compression: self.config.compression,
                delimiter,
            },
            feed,
            location,
            resume,
            checkpoint_every: crate::lister::CHECKPOINT_EVERY,
            unaddressable,
            fingerprint: feed_fingerprint(
                &self.config.url,
                self.config.compression,
                self.config.split_target_bytes.as_u64(),
                delimiter,
            ),
        };
        self.handle.block_on(lister.run())
    }
}

impl S3Source {
    /// The store the source reads: the injected one, or one built from
    /// `config.url`. Returns it with the listing prefix, the unaddressable
    /// keys its client takes out of listings, the bucket a notification's
    /// records must name, and whether a lister can resume over it.
    fn data_store(&mut self) -> Result<DataStore, SourceError> {
        let url = parse(&self.config.url)?;
        Ok(match self.store.take() {
            // Injected store: the URL still names the listing prefix, and the
            // bucket for notifications is its host under `s3` or `s3a`.
            Some(store) => {
                let (_, path) =
                    ObjectStoreScheme::parse(&url).map_err(|e| SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!("parsing {}: {e}", self.config.url),
                    })?;
                let bucket = matches!(url.scheme(), "s3" | "s3a")
                    .then(|| url.host_str().map(str::to_owned))
                    .flatten();
                // A `file://` URL stands for `LocalFileSystem`, whose offset
                // listing walks unsorted.
                let resumable = url.scheme() != "file";
                (store, path, None, bucket, resumable)
            }
            None => {
                let built =
                    build_store(&url, &self.config.store).map_err(|e| SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!("building the object store for {}: {e}", self.config.url),
                    })?;
                (
                    Arc::from(built.store),
                    built.prefix,
                    built.unaddressable,
                    built.bucket,
                    built.resumable,
                )
            }
        })
    }

    /// Parse the `feed:` section's `location` against this source's `url`
    /// and `store` options, which also build the location's store.
    ///
    /// Beyond [`Location::parse`], an S3 location is refused when a `store`
    /// key is not an S3 option, since load checks the keys against the
    /// source URL's scheme only, and when the options or the environment ask
    /// for virtual-hosted requests on a set endpoint, which then addresses
    /// the source's bucket whatever the location names.
    fn parse_location(&self, location: &str) -> Result<Location, ConfigError> {
        let parsed = Location::parse(location, &self.config.url)?;
        if !matches!(
            ObjectStoreScheme::parse(parsed.url()),
            Ok((ObjectStoreScheme::AmazonS3, _))
        ) {
            return Ok(parsed);
        }
        let refuse = |why: String| ConfigError::Component {
            context: "feed".into(),
            message: format!("`location` {}: {why}", parsed.shown()),
        };
        if let Some(key) = self.config.store.keys().find(|k| s3_store_key(k).is_none()) {
            return Err(refuse(format!(
                "source.s3.store.\"{key}\" is not an object_store S3 option, and the \
                 location's store is built from it"
            )));
        }
        let options = apply_options(AmazonS3Builder::from_env(), &self.config.store);
        let virtual_hosted = flag_set(&options, AmazonS3ConfigKey::VirtualHostedStyleRequest);
        let endpoint = [AmazonS3ConfigKey::Endpoint, AmazonS3ConfigKey::S3Endpoint]
            .iter()
            .any(|key| options.get_config_value(key).is_some());
        // An `https://` source URL names its store by host, and an `s3://`
        // location's store reaches that host only through a set endpoint;
        // with none it goes to AWS, signed with the source's credentials.
        let source_by_host = Url::parse(&self.config.url).is_ok_and(|u| {
            matches!(u.scheme(), "https" | "http")
                && !u
                    .host_str()
                    .is_some_and(|h| h == "amazonaws.com" || h.ends_with(".amazonaws.com"))
        });
        if source_by_host && !endpoint {
            return Err(refuse(
                "the source's `url` names a store other than AWS by its host, which an `s3://` \
                 location does not carry; set that store's endpoint under the source's `store`"
                    .into(),
            ));
        }
        if virtual_hosted && endpoint {
            return Err(refuse(
                "the source's `store` or the environment sends virtual-hosted requests to a \
                 set endpoint, which names one bucket whatever the location names; remove \
                 `location`, or `virtual_hosted_style_request` from `store` and \
                 `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` from the environment"
                    .into(),
            ));
        }
        Ok(parsed)
    }

    /// The store holding `location`: the injected one, or one built from its
    /// URL with the source's `store` options and the environment.
    fn location_store(&self, location: &Location) -> Result<Arc<dyn ObjectStore>, SourceError> {
        if let Some(store) = &self.location_store {
            return Ok(Arc::clone(store));
        }
        build_store(location.url(), &self.config.store)
            .map(|built| Arc::from(built.store))
            .map_err(|e| SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!(
                    "building the object store for the job location {}: {e}",
                    location.shown()
                ),
            })
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

    fn configure_coordination(&mut self, section: &CoordinationSection) -> Result<(), ConfigError> {
        if self.feed.is_some() {
            return Err(ConfigError::Component {
                context: "coordination".into(),
                message: "the S3 source reads from a feed given with `with_feed`; \
                          remove the `coordination:` section"
                    .into(),
            });
        }
        if self.coordinator.is_some() {
            return Err(ConfigError::Component {
                context: "coordination".into(),
                message: "the S3 source was also given a coordinator with `with_coordinator`; \
                          remove the `coordination:` section or drop `with_coordinator`"
                    .into(),
            });
        }
        self.spec = Some(CoordinatorSpec::from_section(section)?);
        Ok(())
    }

    fn configure_feed(
        &mut self,
        section: &FeedSection,
        feed: Arc<dyn Feed>,
    ) -> Result<(), ConfigError> {
        if self.feed.is_some() {
            return Err(ConfigError::Component {
                context: "feed".into(),
                message: "the S3 source was also given a feed with `with_feed`; \
                          remove the `feed:` section or drop `with_feed`"
                    .into(),
            });
        }
        if self.coordinator.is_some() {
            return Err(ConfigError::Component {
                context: "feed".into(),
                message: "the S3 source was also given a coordinator with `with_coordinator`; \
                          remove the `feed:` section or drop `with_coordinator`"
                    .into(),
            });
        }
        self.location = section
            .location
            .as_deref()
            .map(|location| self.parse_location(location))
            .transpose()?;
        self.feed = Some(feed);
        self.units_held = section.units_held;
        Ok(())
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
                         (e.g. spate-json's NdjsonFramer) before running the pipeline"
                    .into(),
            });
        };
        // Defense in depth for hand-constructed configs; cheap.
        self.config.validate().map_err(|e| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: e.to_string(),
        })?;

        let (store, prefix, unaddressable, bucket, _) = self.data_store()?;

        let metrics = ctx.meter.as_ref().map(S3Metrics::new);
        if let Some(feed) = self.feed.take() {
            if self.coordinator.is_some() {
                return Err(SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: "the S3 source was given both a feed and a coordinator; \
                             drop `with_coordinator`"
                        .into(),
                });
            }
            let delimiter = make_framer().resync_delimiter();
            if delimiter.is_none() {
                tracing::info!(
                    "the record framer declares no resync delimiter, so objects above \
                     split_target_bytes are read whole and units naming a byte range are \
                     failed back to the feed"
                );
            }
            if let Some(location) = &self.location {
                let location_store = self.location_store(location)?;
                let mine = feed_fingerprint(
                    &self.config.url,
                    self.config.compression,
                    self.config.split_target_bytes.as_u64(),
                    delimiter,
                );
                tracing::info!(
                    location = %location.shown(),
                    window = ?SETTLING_WINDOW,
                    "waiting for the job fingerprint at the job location"
                );
                self.handle.block_on(await_fingerprint(
                    location_store.as_ref(),
                    location,
                    &mine,
                    self.clock.as_ref(),
                    &ctx.stop,
                    SETTLING_WINDOW,
                ))?;
            }
            let parts = LaneParts {
                store,
                handle: self.handle.clone(),
                issuer: ctx.issuer,
                make_framer,
                packing: Packing {
                    target_bytes: self.config.split_target_bytes.as_u64(),
                    compression: self.config.compression,
                    delimiter,
                },
                chunk_bytes: self.config.chunk_bytes.as_u64() as usize,
                range_bytes: self.config.prefetch_bytes.as_u64() as usize,
                retry_base: RETRY_BASE,
                metrics,
            };
            tracing::warn!(
                feed = feed.backend(),
                "the S3 source publishes no units of its own and reads only the units and \
                 storage notifications published to the feed"
            );
            let scope = Scope {
                bucket,
                prefix: prefix.clone(),
            };
            self.state = State::Feed(Box::new(UnitReader::new(
                feed,
                parts,
                scope,
                self.units_held,
            )));
            return Ok(());
        }
        // Coordination metrics are constructor-injected (there is no later
        // hook); a coordinator built here shares the source's labels.
        let coord_metrics = || {
            ctx.meter
                .as_ref()
                .map(|m| CoordinationMetrics::new(m.labels()))
        };
        let coordinator = match (self.coordinator.take(), self.spec.take()) {
            (Some(injected), _) => injected,
            (None, Some(spec)) => {
                spec.build(self.handle.clone(), coord_metrics())
                    .map_err(|e| SourceError::Client {
                        class: e.class(),
                        reason: format!("building the coordinator from `coordination:`: {e}"),
                    })?
            }
            (None, None) => solo_coordinator(self.handle.clone(), coord_metrics())?,
        };

        let finality = if self.config.refresh_listing {
            PlanFinality::Open
        } else {
            PlanFinality::Final
        };
        let delimiter = make_framer().resync_delimiter();
        if delimiter.is_none() {
            tracing::info!(
                "the record framer declares no resync delimiter, so objects above \
                 split_target_bytes are read whole, one split each"
            );
        }
        let planner = S3Planner::new(
            Arc::clone(&store),
            Some(prefix),
            self.handle.clone(),
            Packing {
                target_bytes: self.config.split_target_bytes.as_u64(),
                compression: self.config.compression,
                delimiter,
            },
            finality,
            job_fingerprint(
                &self.config.url,
                self.config.compression,
                self.config.split_target_bytes.as_u64(),
                self.config.refresh_listing,
                delimiter,
            ),
            metrics.clone(),
        )
        .with_unaddressable(unaddressable);
        let split_ctx = SplitCtx::new(
            store,
            self.handle.clone(),
            ctx.issuer,
            make_framer,
            delimiter,
            self.config.compression,
            self.config.chunk_bytes.as_u64() as usize,
            self.config.prefetch_bytes.as_u64() as usize,
            RETRY_BASE,
            metrics,
        );
        let mut driver = CoordinationDriver::new(coordinator);
        driver.set_stop(ctx.stop.clone());
        self.state = State::Open(Box::new(OpenState {
            driver,
            ctx: split_ctx,
            planner: Some(planner),
        }));
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<S3Lane>, SourceError> {
        let open = match &mut self.state {
            State::Open(open) => open,
            State::Feed(reader) => return reader.poll_events(timeout),
            State::Created => {
                return Err(SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: "poll_events before open".into(),
                });
            }
        };
        let OpenState {
            driver,
            ctx,
            planner,
        } = open.as_mut();
        if let Some(planner) = planner.take() {
            // Join the job; the ready signal is empty, and splits arrive
            // as later assignments while claims race.
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
            driver.fail(ctx, &report.split, report.epoch, &report.reason)?;
        }
        driver.poll_events(ctx, timeout)
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        let open = match &mut self.state {
            State::Open(open) => open,
            State::Feed(reader) => return reader.commit(watermarks),
            State::Created => {
                debug_assert!(watermarks.is_empty(), "watermarks before open");
                return Ok(());
            }
        };
        let OpenState { driver, ctx, .. } = open.as_mut();
        driver.commit(ctx, watermarks)
    }

    fn commit_final(
        &mut self,
        watermarks: &[(PartitionId, i64)],
    ) -> Result<Vec<PartitionId>, SourceError> {
        let open = match &mut self.state {
            State::Open(open) => open,
            State::Feed(reader) => return reader.commit(watermarks).map(|()| Vec::new()),
            State::Created => {
                debug_assert!(watermarks.is_empty(), "watermarks before open");
                return Ok(Vec::new());
            }
        };
        let OpenState { driver, ctx, .. } = open.as_mut();
        driver.commit_final(ctx, watermarks)
    }

    // `flush_commits` keeps the trait's no-op default. An Ok commit is
    // durable in the store. A Retryable tick commit is cached and carried on
    // this split's next commit. A final commit returns the partitions it did
    // not store, and those splits may replay under their next owner.

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        match &self.state {
            State::Open(open) => open.ctx.set_paused(lanes, true),
            State::Feed(reader) => reader.set_paused(lanes, true),
            State::Created => {}
        }
        Ok(())
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        match &self.state {
            State::Open(open) => open.ctx.set_paused(lanes, false),
            State::Feed(reader) => reader.set_paused(lanes, false),
            State::Created => {}
        }
        Ok(())
    }
}

impl Drop for S3Source {
    fn drop(&mut self) {
        if let State::Open(open) = &mut self.state {
            // Graceful departure: peers take over this worker's splits and
            // roles without waiting out a lease. Fetchers are detached and
            // exit as their lanes drop.
            open.driver.release();
        }
    }
}

/// The fallback when no coordinator was injected: a solo, in-process
/// coordinator with default tuning. Correct and self-terminating, but its
/// store dies with the process, hence the WARN.
fn solo_coordinator(
    handle: tokio::runtime::Handle,
    metrics: Option<CoordinationMetrics>,
) -> Result<Box<dyn SplitCoordinator>, SourceError> {
    tracing::warn!(
        "no coordinator injected: running solo over an in-process store; progress is \
         EPHEMERAL and a restart replays the entire prefix (at-least-once, safe but \
         wasteful) — configure a durable store under `coordination:` for durable resume \
         and multi-instance sharing"
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
/// environment via [`AmazonS3Builder::from_env`], the same mechanism the AWS
/// SDK uses, so EKS Pod Identity, IRSA (web identity), ECS task roles, IMDS,
/// and `AWS_REGION`/`AWS_DEFAULT_REGION` all resolve without configuration.
/// The explicit `options` are overlaid on top and win over the environment;
/// the bucket comes from the URL. `object_store::parse_url_opts` (which every
/// other scheme still uses) does *not* read the environment, so
/// the S3 scheme is special-cased here.
///
/// This must not pass `std::env::vars()` through `parse_url_opts`:
/// `AmazonS3ConfigKey` parses *unprefixed* keys (`region`, `token`,
/// `endpoint`, ...), so an unrelated environment variable could hijack the
/// client. `from_env` guards on the `AWS_` prefix; we rely on that.
fn build_store(
    url: &Url,
    options: &std::collections::BTreeMap<String, String>,
) -> Result<BuiltStore, object_store::Error> {
    // `ObjectStoreScheme::parse` returns the scheme and the in-store `Path` —
    // the very call `parse_url_opts` makes, so `path` is the exact listing
    // prefix it would return and every planned descriptor that depends on it
    // stays valid.
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    // Every non-S3 scheme (notably `file://` for infrastructure-free runs and
    // tests) keeps the stock, scheme-generic behavior.
    if !matches!(scheme, ObjectStoreScheme::AmazonS3) {
        let (store, prefix) = object_store::parse_url_opts(url, opts(options))?;
        return Ok(BuiltStore {
            store,
            prefix,
            unaddressable: None,
            bucket: None,
            // `LocalFileSystem` walks a directory unsorted and skips the keys
            // at or before an offset, so a resume there loses keys.
            resumable: false,
        });
    }
    // Harden the HTTP client (idle-connection recycling, explicit timeouts)
    // before overlaying the operator's `store` options, so the passthrough
    // still wins; see `harden_client_defaults`.
    let builder = harden_client_defaults(AmazonS3Builder::from_env().with_url(url.as_str()));
    let unaddressable = UnaddressableKeys::default();
    let builder = apply_options(builder, options);
    // `object_store` sends no `start-after` to an S3 directory bucket. It
    // lists the whole prefix and skips the keys at or before the offset,
    // and a directory bucket does not list in key order.
    let resumable = !flag_set(&builder, AmazonS3ConfigKey::S3Express);
    let store = builder
        .with_http_connector(UnaddressableConnector::new(unaddressable.clone()))
        .build()?;
    Ok(BuiltStore {
        bucket: scope_bucket(&store),
        store: Box::new(store),
        prefix: path,
        unaddressable: Some(unaddressable),
        resumable,
    })
}

/// Whether `builder` holds `key` set to true, in any of the spellings
/// `object_store` reads as true.
fn flag_set(builder: &AmazonS3Builder, key: AmazonS3ConfigKey) -> bool {
    builder.get_config_value(&key).is_some_and(|v| {
        matches!(
            v.to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes" | "y"
        )
    })
}

/// The bucket `store` reads, from its `Display` of `AmazonS3(<bucket>)`, so
/// that `object_store` stays the only parser of the source URL. The builder
/// refuses a missing bucket and builds an empty one from an `https://` URL
/// with no bucket in it; an empty bucket is `None`, so every created record
/// fails its message.
pub(crate) fn scope_bucket(store: &AmazonS3) -> Option<String> {
    store
        .to_string()
        .strip_prefix("AmazonS3(")
        .and_then(|rest| rest.strip_suffix(')'))
        .filter(|bucket| !bucket.is_empty())
        .map(str::to_owned)
}

/// The store the source reads, its listing prefix, the unaddressable keys
/// its client takes out of listings, the bucket a notification's records
/// must name, and whether a lister can resume over the store.
type DataStore = (
    Arc<dyn ObjectStore>,
    object_store::path::Path,
    Option<UnaddressableKeys>,
    Option<String>,
    bool,
);

/// What [`build_store`] returns.
struct BuiltStore {
    store: Box<dyn ObjectStore>,
    /// The listing prefix within the store.
    prefix: object_store::path::Path,
    /// The objects the S3 client takes out of each listing; `None` for any
    /// other scheme.
    unaddressable: Option<UnaddressableKeys>,
    /// The bucket a notification's records must name; `None` for any other
    /// scheme.
    bucket: Option<String>,
    /// Whether `object_store` lists after an offset with `start-after`,
    /// which a lister needs to resume from its checkpoint.
    resumable: bool,
}

/// Conservative HTTP-client defaults for the S3 backend, applied *before* the
/// operator's `store` passthrough so the passthrough still overrides them (and
/// so unrelated client options seeded from the environment, such as
/// `allow_http` or a proxy, are left untouched, unlike a wholesale
/// `with_client_options`).
///
/// `pool_idle_timeout` matters most: object_store leaves it unset, so a lane
/// can pull a connection the server (or an intermediary) has already
/// half-closed and fail its next request with an incomplete-message body
/// error, which is the churn a back-pressured backfill provokes. Recycling
/// idle connections
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

/// Overlay the raw `store` passthrough options onto an S3 builder. A later
/// key wins, so these override any environment value seeded by
/// [`AmazonS3Builder::from_env`]. [`S3SourceConfig::validate`] rejects a key
/// that is not an S3 option, so none is skipped here.
fn apply_options(
    mut builder: AmazonS3Builder,
    options: &std::collections::BTreeMap<String, String>,
) -> AmazonS3Builder {
    for (k, v) in options {
        if let Some(key) = s3_store_key(k) {
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
    fn apply_options_sets_pod_identity_keys() {
        const URI: &str = "http://169.254.170.23/v1/credentials";
        const TOKEN: &str =
            "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token";
        let b = apply_options(
            AmazonS3Builder::new(),
            &map(&[
                ("aws_container_credentials_full_uri", URI),
                ("aws_container_authorization_token_file", TOKEN),
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

    /// Every key `validate` accepts reaches the builder, in any case.
    #[test]
    fn apply_options_applies_upper_case_keys() {
        let b = apply_options(
            AmazonS3Builder::new(),
            &map(&[("AWS_ENDPOINT_URL", "http://127.0.0.1:9000")]),
        );
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::Endpoint).as_deref(),
            Some("http://127.0.0.1:9000")
        );
    }

    /// The S3 listing prefix must be byte-identical to what `parse_url_opts`
    /// produces; planned descriptors and their offsets depend on it.
    #[test]
    fn build_store_s3_prefix_matches_parse_url_opts() {
        let url = Url::parse("s3://bucket/exports/2026/").unwrap();
        let got = build_store(&url, &BTreeMap::new()).unwrap().prefix;
        let (_, want) =
            object_store::parse_url_opts(&url, std::iter::empty::<(String, String)>()).unwrap();
        assert_eq!(got, want);
    }

    /// Pins that the bucket a notification's records must name is the one
    /// the built S3 store reads, for each URL form `object_store` accepts,
    /// and that a URL with an empty bucket scopes none. A change to
    /// `AmazonS3`'s `Display` fails here first.
    #[test]
    fn the_scope_bucket_is_the_built_stores_for_every_s3_url_form() {
        for url in [
            "s3://my-bucket/data/",
            "s3a://my-bucket/data/",
            "https://s3.eu-west-2.amazonaws.com/my-bucket/data/",
            "https://my-bucket.s3.eu-west-2.amazonaws.com/data/",
        ] {
            let built = build_store(&Url::parse(url).unwrap(), &BTreeMap::new()).unwrap();
            assert_eq!(built.bucket.as_deref(), Some("my-bucket"), "{url}");
            assert_eq!(built.prefix.as_ref(), "data", "{url}");
        }
        for url in [
            "https://s3.eu-west-2.amazonaws.com/",
            "https://s3.eu-west-2.amazonaws.com//data/",
            "https://.s3.eu-west-2.amazonaws.com/data/",
        ] {
            let built = build_store(&Url::parse(url).unwrap(), &BTreeMap::new()).unwrap();
            assert_eq!(built.bucket, None, "{url} names no bucket");
        }
        let dir = tempfile::tempdir().unwrap();
        let file = Url::from_directory_path(dir.path()).unwrap();
        assert_eq!(build_store(&file, &BTreeMap::new()).unwrap().bucket, None);
    }

    /// Pins which built stores a lister resumes over: the S3 client of a
    /// general-purpose bucket, and neither one with `aws_s3_express` set nor
    /// a `file://` store. Building sends nothing.
    #[test]
    fn only_an_s3_client_without_s3_express_resumes() {
        let built = build_store(&Url::parse("s3://b/data/").unwrap(), &BTreeMap::new()).unwrap();
        assert!(built.resumable);
        let express = BTreeMap::from([("aws_s3_express".to_owned(), "true".to_owned())]);
        let built = build_store(
            &Url::parse("s3://b--usw2-az1--x-s3/data/").unwrap(),
            &express,
        )
        .unwrap();
        assert!(!built.resumable);
        let dir = tempfile::tempdir().unwrap();
        let file = Url::from_directory_path(dir.path()).unwrap();
        assert!(!build_store(&file, &BTreeMap::new()).unwrap().resumable);
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
        let got = build_store(&url, &BTreeMap::new()).unwrap().prefix;
        let (_, want) =
            object_store::parse_url_opts(&url, std::iter::empty::<(String, String)>()).unwrap();
        assert_eq!(got, want, "file:// delegates to parse_url_opts unchanged");
    }

    fn source_with_store(store: &str) -> (S3Source, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let body = format!("url: \"file:///tmp/spate-location/data/\"\nstore:\n{store}");
        let source = S3Source::from_component_config(
            &spate_test::component_config("s3", &body),
            rt.handle().clone(),
        )
        .unwrap();
        (source, rt)
    }

    /// Pins that the location's store is built with the source's `store`
    /// options: an `s3://` location reaches the endpoint they name, which
    /// answers 403.
    #[test]
    fn the_location_store_uses_the_source_store_options() {
        let endpoint = crate::test_servers::status_server("403 Forbidden");
        let (source, rt) = source_with_store(&format!(
            "  endpoint: \"{endpoint}\"\n  allow_http: \"true\"\n  access_key_id: \"AKIDTEST\"\n  \
             secret_access_key: \"test\"\n  region: \"us-east-1\"\n"
        ));
        let location = source.parse_location("s3://ops/jobs/a/").unwrap();
        let store = source.location_store(&location).unwrap();
        let err = rt
            .block_on(crate::location::read_object(
                store.as_ref(),
                &location,
                &location.fingerprint(),
            ))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("403") && err.contains("s3:ListBucket"),
            "{err}"
        );
    }

    /// Pins that an S3 location is refused when the source's `store` sends
    /// virtual-hosted requests to a set endpoint, which names one bucket,
    /// under every spelling and endpoint key `object_store` reads, and is kept
    /// with an endpoint alone or virtual-hosted requests alone.
    #[test]
    fn a_location_beside_a_virtual_hosted_endpoint_is_refused() {
        let (source, _rt) = source_with_store(
            "  endpoint: \"https://b.store.example\"\n  virtual_hosted_style_request: \"true\"\n",
        );
        let err = source
            .parse_location("s3://elsewhere/jobs/a/")
            .unwrap_err()
            .to_string();
        assert!(err.contains("virtual_hosted_style_request"), "{err}");
        for store in [
            "  endpoint: \"https://b.store.example\"\n  virtual_hosted_style_request: \"True\"\n",
            "  endpoint: \"https://b.store.example\"\n  virtual_hosted_style_request: \"1\"\n",
            "  aws_endpoint_url_s3: \"https://b.store.example\"\n  virtual_hosted_style_request: \"yes\"\n",
            "  endpoint: \"https://b.store.example\"\n  virtual_hosted_style_request: \"on\"\n",
            "  endpoint: \"https://b.store.example\"\n  virtual_hosted_style_request: \"Y\"\n",
        ] {
            let (source, _rt) = source_with_store(store);
            let err = source
                .parse_location("s3://elsewhere/jobs/a/")
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("virtual_hosted_style_request"),
                "{store}: {err}"
            );
            assert!(
                err.contains("AWS_VIRTUAL_HOSTED_STYLE_REQUEST"),
                "{store}: {err}"
            );
        }
        for store in [
            "  endpoint: \"https://store.example\"\n",
            "  virtual_hosted_style_request: \"true\"\n",
            "  endpoint: \"https://store.example\"\n  virtual_hosted_style_request: \"false\"\n",
        ] {
            let (source, _rt) = source_with_store(store);
            source.parse_location("s3://elsewhere/jobs/a/").unwrap();
        }
    }

    /// Pins that an S3 location beside a source named by a non-AWS host is
    /// refused unless `store` sets that store's endpoint, so its requests
    /// never go to AWS signed with the source's credentials.
    #[test]
    fn a_location_beside_a_source_named_by_another_host_needs_an_endpoint() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let source = |store: &str| {
            let body =
                format!("url: \"https://0123abcd.r2.cloudflarestorage.com/data/events/\"\n{store}");
            S3Source::from_component_config(
                &spate_test::component_config("s3", &body),
                rt.handle().clone(),
            )
            .unwrap()
        };
        let err = source("")
            .parse_location("s3://ops/jobs/a/")
            .unwrap_err()
            .to_string();
        assert!(err.contains("endpoint"), "{err}");
        source("store:\n  endpoint: \"https://0123abcd.r2.cloudflarestorage.com\"\n")
            .parse_location("s3://ops/jobs/a/")
            .unwrap();
        let aws = S3Source::from_component_config(
            &spate_test::component_config(
                "s3",
                "url: \"https://data.s3.eu-west-2.amazonaws.com/events/\"\n",
            ),
            rt.handle().clone(),
        )
        .unwrap();
        aws.parse_location("s3://ops/jobs/a/").unwrap();
    }

    /// Pins that an S3 location is refused when a `store` key is not an S3
    /// option, which load checks only against an S3 source URL.
    #[test]
    fn an_s3_location_refuses_a_store_key_the_s3_client_does_not_know() {
        let (source, _rt) = source_with_store("  endpont: \"http://127.0.0.1:9000\"\n");
        let err = source
            .parse_location("s3://ops/jobs/a/")
            .unwrap_err()
            .to_string();
        assert!(err.contains("\"endpont\""), "{err}");
        let (source, _rt) = source_with_store("  endpont: \"http://127.0.0.1:9000\"\n");
        let dir = tempfile::tempdir().unwrap();
        source
            .parse_location(&format!("file://{}/jobs/a/", dir.path().display()))
            .unwrap();
    }
}
