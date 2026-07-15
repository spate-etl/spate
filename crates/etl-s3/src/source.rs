//! The control plane: [`S3Source`] implements the framework's
//! [`Source`] trait.
//!
//! Lifecycle: `open` builds the stores and spawns the listing task;
//! `poll_events` hands out the K lanes once the (sorted) listing and the
//! manifest agree, then idles until every lane reports end-of-slice and
//! returns [`SourceEvent::Drained`] — the bounded-job completion signal.
//! `commit` folds advanced watermarks into the manifest and durably saves
//! it through the [`OffsetStore`].
//!
//! Assignment is static: one epoch, no rebalances, each lane its own
//! framework partition. Backpressure `pause`/`resume` toggles per-lane
//! flags the fetchers honor between sends.

use crate::config::S3SourceConfig;
use crate::fetch::{
    BACKOFF_CAP, FetcherParams, MAX_ATTEMPTS, ObjectEntry, assign_lanes, list_all, run_fetcher,
    validate_resume,
};
use crate::framer::FramerFactory;
use crate::lane::S3Lane;
use crate::metrics::S3Metrics;
use crate::offset::Position;
use crate::store::{
    LaneState, Manifest, ObjectManifestStore, OffsetStore, SourceIdentity, chain_hash,
};
use etl_core::checkpoint::AckIssuer;
use etl_core::config::{ComponentConfig, ConfigError};
use etl_core::error::{ErrorClass, SourceError};
use etl_core::framing::{FramingContract, RecordFramer};
use etl_core::record::PartitionId;
use etl_core::source::{LaneId, Source, SourceCtx, SourceEvent};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use url::Url;

/// First backoff step for fetcher-internal GET retries and for the
/// startup (listing / manifest-load) retries.
const RETRY_BASE: Duration = Duration::from_millis(200);

/// Capped exponential backoff before retryable-startup attempt
/// `attempt` (1-based).
fn startup_backoff(attempt: u32) -> Duration {
    RETRY_BASE
        .saturating_mul(1 << (attempt - 1).min(16))
        .min(BACKOFF_CAP)
}

/// Where the source is in its bounded lifecycle.
enum Phase {
    /// Constructed, not yet opened.
    Created,
    /// Listing task in flight.
    Listing {
        join: tokio::task::JoinHandle<Result<Vec<ObjectEntry>, object_store::Error>>,
        /// Retryable listing failures so far; the attempt budget
        /// ([`MAX_ATTEMPTS`]) turns a persistent outage into a fatal error
        /// instead of an invisible forever-retry.
        restarts: u32,
    },
    /// Listing done and dealt into slices; manifest load (retryable)
    /// still pending.
    Prepared {
        slices: Vec<Vec<ObjectEntry>>,
        /// Retryable manifest-load failures so far, same budget as above.
        attempts: u32,
        /// Earliest next attempt — retries are paced with capped backoff,
        /// never hot-looped through the controller.
        next_attempt: Instant,
    },
    /// Lanes handed out; streaming.
    Running(Running),
}

struct Running {
    manifest: Manifest,
    /// Per-lane slices, shared with the fetchers (commit looks up keys
    /// and ETags by ordinal).
    slices: Vec<Arc<Vec<ObjectEntry>>>,
    /// Per-lane rolling key-prefix hashes (`prefix_hashes[lane][i]` =
    /// chain hash of keys `0..=i`), precomputed so commits are O(1).
    prefix_hashes: Vec<Vec<u64>>,
    /// Per-lane end-of-slice flags, set by the lanes.
    eof: Vec<Arc<AtomicBool>>,
    /// Per-lane fetcher pause flags.
    pause: Vec<Arc<AtomicBool>>,
    /// Fetcher tasks (aborted on drop).
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// Bounded object-storage backfill source. See the crate docs for the
/// delivery model and the frozen-key-set contract.
pub struct S3Source {
    config: S3SourceConfig,
    handle: tokio::runtime::Handle,
    /// Durable watermark storage; the object-store manifest by default,
    /// swappable via [`with_offset_store`](S3Source::with_offset_store).
    offset_store: Option<Box<dyn OffsetStore>>,
    /// The per-object record framer, supplied by the chosen format via
    /// [`with_framer`](S3Source::with_framer). Required before the pipeline
    /// opens the source — `etl-s3` is a transport and owns no framing itself.
    framer: Option<FramerFactory>,
    issuer: Option<AckIssuer>,
    metrics: Option<S3Metrics>,
    /// The data store, built from `config.url` at `open`.
    store: Option<Arc<dyn ObjectStore>>,
    /// The listing prefix within `store`, from `config.url`.
    prefix: Option<object_store::path::Path>,
    phase: Phase,
}

impl std::fmt::Debug for S3Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Source")
            .field("url", &self.config.url)
            .field("lanes", &self.config.lanes)
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
    /// source and its lanes briefly block pipeline threads on it, which a
    /// current-thread runtime cannot drive.
    #[must_use]
    pub fn new(config: S3SourceConfig, io: tokio::runtime::Handle) -> S3Source {
        S3Source {
            config,
            handle: io,
            offset_store: None,
            framer: None,
            issuer: None,
            metrics: None,
            store: None,
            prefix: None,
            phase: Phase::Created,
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

    /// Replace the durable watermark backend (default: a JSON manifest
    /// object at `checkpoint.url`). Must be called before the pipeline
    /// opens the source.
    #[must_use]
    pub fn with_offset_store(mut self, store: Box<dyn OffsetStore>) -> S3Source {
        self.offset_store = Some(store);
        self
    }

    /// Set the record framer that cuts each object's byte stream into records.
    /// `etl-s3` is a transport and owns no framing, so this is **required**:
    /// supply the framer for the objects' format — e.g. `etl-json`'s
    /// `NdjsonFramer` for NDJSON — before the pipeline opens the source.
    ///
    /// `factory` builds a fresh
    /// [`RecordFramer`](etl_core::framing::RecordFramer) per object (framers are
    /// per-object stateful and each lane frames its own slice). A framed source
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

    fn identity(&self) -> SourceIdentity {
        SourceIdentity {
            url: self.config.url.clone(),
            compression: self.config.compression,
        }
    }

    /// Finish startup once the listing is in: deal slices, load and
    /// validate the manifest, hand out lanes, spawn fetchers. `attempts`
    /// counts prior retryable manifest-load failures.
    fn finish_prepare(
        &mut self,
        slices: Vec<Vec<ObjectEntry>>,
        attempts: u32,
    ) -> Result<SourceEvent<S3Lane>, SourceError> {
        let offset_store = self
            .offset_store
            .as_mut()
            .expect("offset store built at open");
        let manifest = match offset_store.load() {
            Ok(m) => m,
            Err(e) => {
                let reason = format!("loading the checkpoint manifest: {}", e.reason);
                if e.class != ErrorClass::Retryable {
                    return Err(SourceError::Client {
                        class: e.class,
                        reason,
                    });
                }
                let attempts = attempts + 1;
                // A persistent outage fails fast (the crate's retry
                // philosophy — see the fetcher's attempt budget) instead
                // of wedging the pipeline in an invisible retry loop.
                if attempts >= MAX_ATTEMPTS {
                    return Err(SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!("{reason} (still failing after {attempts} attempts)"),
                    });
                }
                // Retryable: keep the listing, back off, try again on a
                // later poll.
                self.phase = Phase::Prepared {
                    slices,
                    attempts,
                    next_attempt: Instant::now() + startup_backoff(attempts),
                };
                return Err(SourceError::Client {
                    class: ErrorClass::Retryable,
                    reason,
                });
            }
        };
        let manifest = match manifest {
            Some(m) => {
                m.check_compatible(self.config.lanes, &self.identity())
                    .map_err(|reason| SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason,
                    })?;
                validate_resume(&slices, &m)?;
                tracing::info!(
                    lanes = self.config.lanes,
                    committed_lanes = m.lane_states.len(),
                    "resuming backfill from the checkpoint manifest"
                );
                m
            }
            None => {
                tracing::info!(lanes = self.config.lanes, "starting a fresh backfill");
                Manifest::new(self.config.lanes, self.identity())
            }
        };

        let issuer = self.issuer.clone().expect("issuer stashed at open");
        let store = Arc::clone(self.store.as_ref().expect("store built at open"));
        let total_objects: usize = slices.iter().map(Vec::len).sum();
        let chunk_bytes = self.config.chunk_bytes.as_u64() as usize;
        let capacity =
            usize::try_from(self.config.prefetch_bytes.as_u64() / chunk_bytes as u64).unwrap_or(1);
        let capacity = capacity.max(1);

        let mut running = Running {
            manifest,
            slices: Vec::new(),
            prefix_hashes: Vec::new(),
            eof: Vec::new(),
            pause: Vec::new(),
            tasks: Vec::new(),
        };
        let mut lanes = Vec::new();
        let mut already_complete: u64 = 0;
        // Supplied once via `with_framer` (checked at `open`), cloned per lane
        // (each lane frames its own slice).
        let make_framer = self.framer.clone().expect("framer set at open");

        for (i, slice) in slices.into_iter().enumerate() {
            let lane_ix = i as u32;
            let state = running.manifest.lane_states.get(&lane_ix);
            let resume = state.map(|s| Position::decode(s.watermark));
            let start_ordinal = resume.map_or(0, |p| p.ordinal);
            already_complete += u64::from(start_ordinal);

            let mut hashes = Vec::with_capacity(slice.len());
            let mut h = 0u64;
            for e in &slice {
                h = chain_hash(h, &e.key);
                hashes.push(h);
            }

            let slice = Arc::new(slice);
            let eof = Arc::new(AtomicBool::new(false));
            let pause = Arc::new(AtomicBool::new(false));
            let (tx, rx) = mpsc::channel(capacity);
            let task = self.handle.spawn(run_fetcher(FetcherParams {
                lane: lane_ix,
                store: Arc::clone(&store),
                slice: Arc::clone(&slice),
                start_ordinal,
                resume_etag: state.and_then(|s| s.etag.clone()),
                chunk_bytes,
                tx,
                pause: Arc::clone(&pause),
                retry_base: RETRY_BASE,
                retries: self.metrics.as_ref().map(|m| m.get_retries.clone()),
            }));
            lanes.push(S3Lane::new(
                LaneId(lane_ix),
                PartitionId(lane_ix),
                rx,
                self.handle.clone(),
                issuer.clone(),
                self.config.compression,
                Arc::clone(&make_framer),
                resume,
                Arc::clone(&eof),
                self.metrics.clone(),
            ));
            running.slices.push(slice);
            running.prefix_hashes.push(hashes);
            running.eof.push(eof);
            running.pause.push(pause);
            running.tasks.push(task);
        }

        if let Some(m) = &self.metrics {
            m.objects_listed.increment(total_objects as u64);
            m.objects_remaining
                .set((total_objects as u64).saturating_sub(already_complete) as f64);
        }
        tracing::info!(
            objects = total_objects,
            lanes = lanes.len(),
            "object listing dealt into lanes; streaming"
        );
        self.phase = Phase::Running(running);
        Ok(SourceEvent::LanesAssigned(lanes))
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
        if !matches!(self.phase, Phase::Created) {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "source opened twice".into(),
            });
        }
        if self.framer.is_none() {
            return Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "S3Source has no record framer; supply one with `with_framer(...)` \
                         (e.g. etl-json's NdjsonFramer) before running the pipeline"
                    .into(),
            });
        }
        // Defense in depth for hand-constructed configs; cheap.
        self.config.validate().map_err(|e| SourceError::Client {
            class: ErrorClass::Fatal,
            reason: e.to_string(),
        })?;

        let url = parse(&self.config.url)?;
        let (store, prefix) = object_store::parse_url_opts(&url, opts(&self.config.store))
            .map_err(|e| SourceError::Client {
                class: ErrorClass::Fatal,
                reason: format!("building the object store for {}: {e}", self.config.url),
            })?;
        let store: Arc<dyn ObjectStore> = Arc::from(store);

        if self.offset_store.is_none() {
            let ck_url = parse(&self.config.checkpoint.url)?;
            // An empty checkpoint `store` section on the same store means
            // "same options as the source" (documented on the field).
            let same_store =
                ck_url.scheme() == url.scheme() && ck_url.authority() == url.authority();
            let ck_opts = if self.config.checkpoint.store.is_empty() && same_store {
                &self.config.store
            } else {
                &self.config.checkpoint.store
            };
            let (ck_store, ck_path) = object_store::parse_url_opts(&ck_url, opts(ck_opts))
                .map_err(|e| SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!(
                        "building the checkpoint store for {}: {e}",
                        self.config.checkpoint.url
                    ),
                })?;
            self.offset_store = Some(Box::new(ObjectManifestStore::new(
                Arc::from(ck_store),
                ck_path,
                self.config.checkpoint.timeout,
                self.handle.clone(),
            )));
        }

        self.issuer = Some(ctx.issuer);
        self.metrics = ctx.meter.as_ref().map(S3Metrics::new);
        let listing_store = Arc::clone(&store);
        let listing_prefix = prefix.clone();
        self.store = Some(store);
        self.prefix = Some(prefix);
        self.phase = Phase::Listing {
            join: self
                .handle
                .spawn(async move { list_all(&listing_store, Some(&listing_prefix)).await }),
            restarts: 0,
        };
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<S3Lane>, SourceError> {
        match &mut self.phase {
            Phase::Created => Err(SourceError::Client {
                class: ErrorClass::Fatal,
                reason: "poll_events before open".into(),
            }),
            Phase::Listing { join, restarts } => {
                let restarts = *restarts;
                let handle = self.handle.clone();
                let outcome = handle.block_on(async { tokio::time::timeout(timeout, join).await });
                match outcome {
                    Err(_) => Ok(SourceEvent::Idle), // still listing
                    Ok(Err(join_err)) => Err(SourceError::Client {
                        class: ErrorClass::Fatal,
                        reason: format!("listing task failed: {join_err}"),
                    }),
                    Ok(Ok(Err(e))) => {
                        let err = crate::error::source_error("listing objects", &e);
                        // Retryable listing failures restart the listing
                        // (with backoff, under the shared attempt budget);
                        // the controller logs and keeps polling.
                        if matches!(&err, SourceError::Client { class, .. } if *class == ErrorClass::Retryable)
                        {
                            let restarts = restarts + 1;
                            if restarts >= MAX_ATTEMPTS {
                                return Err(SourceError::Client {
                                    class: ErrorClass::Fatal,
                                    reason: format!(
                                        "listing objects still failing after {restarts} \
                                         attempts: {e}"
                                    ),
                                });
                            }
                            let store =
                                Arc::clone(self.store.as_ref().expect("store built at open"));
                            let prefix = self.prefix.clone().expect("prefix stored at open");
                            let backoff = startup_backoff(restarts);
                            self.phase = Phase::Listing {
                                join: self.handle.spawn(async move {
                                    tokio::time::sleep(backoff).await;
                                    list_all(&store, Some(&prefix)).await
                                }),
                                restarts,
                            };
                        }
                        Err(err)
                    }
                    Ok(Ok(Ok(entries))) => {
                        let slices = assign_lanes(entries, self.config.lanes)?;
                        self.finish_prepare(slices, 0)
                    }
                }
            }
            Phase::Prepared {
                slices,
                attempts,
                next_attempt,
            } => {
                // Pace the retry: never re-attempt (or hot-loop the
                // controller) before the backoff elapses.
                let now = Instant::now();
                if now < *next_attempt {
                    std::thread::sleep((*next_attempt - now).min(timeout));
                    if Instant::now() < *next_attempt {
                        return Ok(SourceEvent::Idle);
                    }
                }
                let (slices, attempts) = (std::mem::take(slices), *attempts);
                self.finish_prepare(slices, attempts)
            }
            Phase::Running(r) => {
                let drained = |r: &Running| r.eof.iter().all(|f| f.load(Ordering::Acquire));
                if drained(r) {
                    return Ok(SourceEvent::Drained);
                }
                // Idle wait; lanes stream independently of this thread.
                std::thread::sleep(timeout);
                if drained(&*r) {
                    Ok(SourceEvent::Drained)
                } else {
                    Ok(SourceEvent::Idle)
                }
            }
        }
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        let Phase::Running(r) = &mut self.phase else {
            debug_assert!(watermarks.is_empty(), "watermarks before assignment");
            return Ok(());
        };
        for &(partition, watermark) in watermarks {
            let lane = partition.0;
            let pos = Position::decode(watermark);
            let (Some(slice), Some(hashes)) = (
                r.slices.get(lane as usize),
                r.prefix_hashes.get(lane as usize),
            ) else {
                return Err(SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!("commit for unknown lane {lane}"),
                });
            };
            let ordinal = pos.ordinal as usize;
            let (Some(entry), Some(&keys_hash)) = (slice.get(ordinal), hashes.get(ordinal)) else {
                return Err(SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!(
                        "lane {lane} watermark {watermark} decodes to ordinal {ordinal}, \
                         outside its {}-object slice — offset accounting bug",
                        slice.len()
                    ),
                });
            };
            r.manifest.lane_states.insert(
                lane,
                LaneState {
                    watermark,
                    ordinal: pos.ordinal,
                    key: entry.key.clone(),
                    etag: entry.etag.clone(),
                    keys_hash,
                },
            );
        }
        self.offset_store
            .as_mut()
            .expect("offset store built at open")
            .save(&r.manifest)
            .map_err(|e| SourceError::Client {
                class: e.class,
                reason: format!("saving the checkpoint manifest: {}", e.reason),
            })
    }

    fn flush_commits(&mut self) -> Result<(), SourceError> {
        let Phase::Running(r) = &self.phase else {
            return Ok(());
        };
        self.offset_store
            .as_mut()
            .expect("offset store built at open")
            .save(&r.manifest)
            .map_err(|e| SourceError::Client {
                class: e.class,
                reason: format!("flushing the checkpoint manifest: {}", e.reason),
            })
    }

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        if let Phase::Running(r) = &self.phase {
            for id in lanes {
                if let Some(flag) = r.pause.get(id.0 as usize) {
                    flag.store(true, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        if let Phase::Running(r) = &self.phase {
            for id in lanes {
                if let Some(flag) = r.pause.get(id.0 as usize) {
                    flag.store(false, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }
}

impl Drop for S3Source {
    fn drop(&mut self) {
        match &self.phase {
            Phase::Listing { join, .. } => join.abort(),
            Phase::Running(r) => {
                for t in &r.tasks {
                    t.abort();
                }
            }
            _ => {}
        }
    }
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
