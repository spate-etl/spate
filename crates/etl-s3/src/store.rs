//! Durable watermark storage: the manifest and the [`OffsetStore`] seam.
//!
//! Object storage has no broker-side commit, so `Source::commit` persists
//! watermarks itself. The default backend writes a small JSON **manifest
//! object** on every commit tick (single writer per pipeline component;
//! last-writer-wins is safe because a stale manifest only costs replay,
//! never loss). The seam is the [`OffsetStore`] trait — swap in another
//! backend without touching the source.
//!
//! Beyond watermarks, the manifest pins what the watermarks *mean*:
//! the lane count, the source identity (url/format/compression — these
//! change record indexing), and per lane the object key, its ETag, and a
//! rolling hash of the lane's committed key prefix. A resume validates all
//! of it against the fresh listing and fails fast on drift instead of
//! replaying or skipping the wrong data.

use etl_core::error::ErrorClass;
use object_store::ObjectStoreExt as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{Compression, Format};

/// Version of the manifest layout **and** of everything that defines
/// record indexing (the framing rules in [`framer`](crate::framer), the
/// listing order, the round-robin lane assignment). Changing any of those
/// bumps this, and an old manifest is rejected rather than misread.
pub const MANIFEST_SCHEMA: u32 = 1;

/// What the source was pointed at when the manifest was written. A resume
/// with a different identity would reinterpret the committed offsets, so
/// any mismatch is fatal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SourceIdentity {
    /// The configured source URL (bucket + prefix), verbatim.
    pub url: String,
    /// Record framing.
    pub format: Format,
    /// Compression policy.
    pub compression: Compression,
}

/// One lane's committed progress.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LaneState {
    /// The committed watermark: one past the last acknowledged record's
    /// composite offset.
    pub watermark: i64,
    /// Object ordinal the watermark points into (decoded from it; kept
    /// explicit for debuggability and validation).
    pub ordinal: u32,
    /// Object key at that ordinal in the lane's slice.
    pub key: String,
    /// ETag of that object when it was opened, if the store reported one.
    /// Resume re-opens the object conditioned on it, so a same-key
    /// overwrite cannot silently change what a record index means.
    pub etag: Option<String>,
    /// Rolling hash over the lane's key sequence `[0..=ordinal]`
    /// (see [`chain_hash`]). Catches listing drift that key-at-ordinal
    /// alone cannot (compensating insert + delete below the watermark).
    pub keys_hash: u64,
}

/// The persisted checkpoint document.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Manifest {
    /// [`MANIFEST_SCHEMA`] at write time.
    pub schema: u32,
    /// Lane count the offsets were minted under. Changing it reshuffles
    /// every lane slice, so a mismatch is fatal.
    pub lanes: u32,
    /// Source identity at write time.
    pub source: SourceIdentity,
    /// Per-lane progress, keyed by lane index. A lane with no committed
    /// progress has no entry and resumes from the start of its slice.
    pub lane_states: BTreeMap<u32, LaneState>,
}

impl Manifest {
    /// A fresh manifest with no committed progress.
    pub fn new(lanes: u32, source: SourceIdentity) -> Manifest {
        Manifest {
            schema: MANIFEST_SCHEMA,
            lanes,
            source,
            lane_states: BTreeMap::new(),
        }
    }

    /// Validate that this manifest's offsets are meaningful for the given
    /// configuration. Any mismatch is a fatal misconfiguration: the
    /// operator either changed the job under a live checkpoint or pointed
    /// two jobs at one manifest.
    pub fn check_compatible(&self, lanes: u32, source: &SourceIdentity) -> Result<(), String> {
        if self.schema != MANIFEST_SCHEMA {
            return Err(format!(
                "manifest schema {} is not the supported version {MANIFEST_SCHEMA}; \
                 the checkpoint was written by an incompatible release",
                self.schema
            ));
        }
        if self.lanes != lanes {
            return Err(format!(
                "manifest was written with lanes = {}, configured lanes = {lanes}; \
                 changing the lane count reshuffles every slice and invalidates all \
                 committed offsets (start a fresh checkpoint to re-run)",
                self.lanes
            ));
        }
        if self.source != *source {
            return Err(format!(
                "manifest was written for {:?}, configured source is {:?}; \
                 offsets are only meaningful for the exact source they were \
                 committed against",
                self.source, source
            ));
        }
        Ok(())
    }
}

/// Rolling key-sequence hash: `h_i = xxh64(key_i, seed = h_{i-1})`,
/// starting from seed 0. Incremental per object, order-sensitive, and
/// cheap to recompute over a fresh listing during resume validation.
pub(crate) fn chain_hash(prev: u64, key: &str) -> u64 {
    twox_hash::XxHash64::oneshot(prev, key.as_bytes())
}

/// A failure loading or saving the manifest.
#[derive(Debug)]
#[non_exhaustive]
pub struct OffsetStoreError {
    /// Retryable (the controller retries on the next commit tick) or
    /// fatal.
    pub class: ErrorClass,
    /// Human-readable cause.
    pub reason: String,
}

impl OffsetStoreError {
    /// An error with the given retry class and human-readable cause.
    /// External [`OffsetStore`] implementations construct errors through
    /// this (the struct is `#[non_exhaustive]`).
    pub fn new(class: ErrorClass, reason: impl Into<String>) -> OffsetStoreError {
        OffsetStoreError {
            class,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for OffsetStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "offset store error ({:?}): {}", self.class, self.reason)
    }
}

impl std::error::Error for OffsetStoreError {}

/// Durable storage for the source's committed watermarks. Implementations
/// are called from the pipeline's controller thread only; `save` must be
/// durable when it returns `Ok`.
pub trait OffsetStore: Send {
    /// Load the last saved manifest, or `None` when none exists yet.
    fn load(&mut self) -> Result<Option<Manifest>, OffsetStoreError>;

    /// Durably persist the manifest, replacing any previous one.
    fn save(&mut self, manifest: &Manifest) -> Result<(), OffsetStoreError>;
}

/// The default [`OffsetStore`]: one JSON object in object storage,
/// replaced on every save.
pub(crate) struct ObjectManifestStore {
    store: Arc<dyn object_store::ObjectStore>,
    path: object_store::path::Path,
    timeout: Duration,
    handle: tokio::runtime::Handle,
}

impl fmt::Debug for ObjectManifestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectManifestStore")
            .field("path", &self.path)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl ObjectManifestStore {
    /// A manifest store at `path` within `store`. The handle must belong
    /// to a multi-thread runtime (the pipeline's I/O runtime): loads and
    /// saves block the calling controller thread on it, bounded by
    /// `timeout`.
    pub(crate) fn new(
        store: Arc<dyn object_store::ObjectStore>,
        path: object_store::path::Path,
        timeout: Duration,
        handle: tokio::runtime::Handle,
    ) -> ObjectManifestStore {
        ObjectManifestStore {
            store,
            path,
            timeout,
            handle,
        }
    }

    /// Drive `fut` on the runtime, bounded by the configured timeout,
    /// keeping the raw store error so callers can match on its variant.
    fn run_raw<T, F>(&self, fut: F) -> Result<Result<T, object_store::Error>, OffsetStoreError>
    where
        F: std::future::Future<Output = Result<T, object_store::Error>>,
    {
        let deadline = self.timeout;
        self.handle
            // The timeout future must be constructed inside the runtime
            // context (its timer registers with the ambient reactor).
            .block_on(async move { tokio::time::timeout(deadline, fut).await })
            .map_err(|_| OffsetStoreError {
                class: ErrorClass::Retryable,
                reason: format!(
                    "manifest access to {} timed out after {:?}",
                    self.path, self.timeout
                ),
            })
    }

    fn run<T, F>(&self, what: &str, fut: F) -> Result<T, OffsetStoreError>
    where
        F: std::future::Future<Output = Result<T, object_store::Error>>,
    {
        self.run_raw(fut)?.map_err(|e| OffsetStoreError {
            class: crate::error::classify(&e),
            reason: format!("{what} {}: {e}", self.path),
        })
    }
}

impl OffsetStore for ObjectManifestStore {
    fn load(&mut self) -> Result<Option<Manifest>, OffsetStoreError> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let got = self.run_raw(async move { store.get(&path).await?.bytes().await })?;
        let bytes = match got {
            Ok(bytes) => bytes,
            // No manifest yet: a fresh backfill.
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(OffsetStoreError {
                    class: crate::error::classify(&e),
                    reason: format!("loading manifest {}: {e}", self.path),
                });
            }
        };
        parse_manifest(&bytes).map(Some)
    }

    fn save(&mut self, manifest: &Manifest) -> Result<(), OffsetStoreError> {
        let body = serde_json::to_vec_pretty(manifest).map_err(|e| OffsetStoreError {
            class: ErrorClass::Fatal,
            reason: format!("serializing manifest: {e}"),
        })?;
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        self.run("saving manifest", async move {
            store.put(&path, body.into()).await
        })
        .map(|_| ())
    }
}

/// Parse manifest bytes, distinguishing "newer/other schema" from
/// "corrupt" for actionable errors. Both are fatal: guessing at a
/// checkpoint is never safe.
fn parse_manifest(bytes: &[u8]) -> Result<Manifest, OffsetStoreError> {
    let probe: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| OffsetStoreError {
        class: ErrorClass::Fatal,
        reason: format!("manifest is not valid JSON (corrupt checkpoint?): {e}"),
    })?;
    match probe.get("schema").and_then(serde_json::Value::as_u64) {
        Some(v) if v == MANIFEST_SCHEMA as u64 => {}
        Some(v) => {
            return Err(OffsetStoreError {
                class: ErrorClass::Fatal,
                reason: format!(
                    "manifest schema {v} is not the supported version {MANIFEST_SCHEMA}"
                ),
            });
        }
        None => {
            return Err(OffsetStoreError {
                class: ErrorClass::Fatal,
                reason: "manifest has no schema field (corrupt checkpoint?)".into(),
            });
        }
    }
    serde_json::from_value(probe).map_err(|e| OffsetStoreError {
        class: ErrorClass::Fatal,
        reason: format!("manifest does not match schema {MANIFEST_SCHEMA}: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    fn identity() -> SourceIdentity {
        SourceIdentity {
            url: "s3://bucket/exports/".into(),
            format: Format::Ndjson,
            compression: Compression::Auto,
        }
    }

    fn sample() -> Manifest {
        let mut m = Manifest::new(2, identity());
        m.lane_states.insert(
            0,
            LaneState {
                watermark: (7_i64 << 40) | 123,
                ordinal: 7,
                key: "exports/part-014.ndjson.gz".into(),
                etag: Some("\"abc123\"".into()),
                keys_hash: chain_hash(chain_hash(0, "a"), "b"),
            },
        );
        m
    }

    #[test]
    fn round_trips_through_an_object_store() {
        let rt = runtime();
        let store = Arc::new(InMemory::new());
        let mut s = ObjectManifestStore::new(
            store,
            Path::from("_etl/backfill.json"),
            Duration::from_secs(5),
            rt.handle().clone(),
        );
        assert!(s.load().unwrap().is_none(), "absent manifest loads as None");
        let manifest = sample();
        s.save(&manifest).unwrap();
        assert_eq!(s.load().unwrap().unwrap(), manifest);
        // Saving again replaces (last write wins).
        let mut second = manifest.clone();
        second.lane_states.get_mut(&0).unwrap().watermark += 1;
        s.save(&second).unwrap();
        assert_eq!(s.load().unwrap().unwrap(), second);
    }

    #[test]
    fn corrupt_and_alien_manifests_are_fatal() {
        let rt = runtime();
        let store = Arc::new(InMemory::new());
        let path = Path::from("_etl/backfill.json");
        for (body, expect) in [
            (&b"not json"[..], "not valid JSON"),
            (&br#"{"lanes": 2}"#[..], "no schema field"),
            (&br#"{"schema": 99}"#[..], "not the supported version"),
            (&br#"{"schema": 1, "lanes": "two"}"#[..], "does not match"),
        ] {
            rt.block_on(store.put(&path, body.to_vec().into())).unwrap();
            let mut s = ObjectManifestStore::new(
                Arc::clone(&store) as Arc<dyn object_store::ObjectStore>,
                path.clone(),
                Duration::from_secs(5),
                rt.handle().clone(),
            );
            let err = s.load().unwrap_err();
            assert_eq!(err.class, ErrorClass::Fatal, "{}", err.reason);
            assert!(err.reason.contains(expect), "{}", err.reason);
        }
    }

    #[test]
    fn compatibility_check_names_the_mismatch() {
        let m = sample();
        assert!(m.check_compatible(2, &identity()).is_ok());
        let err = m.check_compatible(4, &identity()).unwrap_err();
        assert!(err.contains("lanes"), "{err}");
        let mut other = identity();
        other.compression = Compression::Zstd;
        let err = m.check_compatible(2, &other).unwrap_err();
        assert!(
            err.contains("identity") || err.contains("written for"),
            "{err}"
        );
        let mut alien = m.clone();
        alien.schema = 3;
        let err = alien.check_compatible(2, &identity()).unwrap_err();
        assert!(err.contains("schema"), "{err}");
    }

    #[test]
    fn chain_hash_is_order_sensitive_and_stable() {
        let ab = chain_hash(chain_hash(0, "a"), "b");
        let ba = chain_hash(chain_hash(0, "b"), "a");
        assert_ne!(ab, ba, "order matters");
        assert_eq!(
            ab,
            chain_hash(chain_hash(0, "a"), "b"),
            "deterministic across runs"
        );
    }
}
