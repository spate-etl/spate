//! Connector-owned metric families, registered through the source's
//! [`Meter`] (namespace `s3`, role `source` → `spate_s3_source_*`).
//!
//! All handles are resolved once at `open` and cloned into the lanes and
//! fetchers; nothing resolves names on the record path. Counters are
//! incremented at batch/object/chunk boundaries per the taxonomy's
//! hot-path discipline.

use crate::split_ctx::PoisonKind;
use spate_core::metrics::{Counter, Gauge, Meter};

/// Pre-registered `spate_s3_source_*` handles.
#[derive(Clone)]
pub(crate) struct S3Metrics {
    /// Objects enumerated by the planner's listing (leader-only: only the
    /// instance that runs the plan increments it).
    pub(crate) objects_listed: Counter,
    /// Objects fully framed and handed to the pipeline.
    pub(crate) objects_completed: Counter,
    /// Objects not yet completed across this worker's currently-held
    /// splits (rises on split gain, falls per completed object, settles on
    /// split close).
    pub(crate) objects_remaining: Gauge,
    /// Bytes read from the store (as stored, pre-decompression).
    pub(crate) bytes_read: Counter,
    /// Bytes after decompression (equals `bytes_read` for plain objects).
    pub(crate) bytes_decoded: Counter,
    /// Object GET attempts beyond the first (transient failures).
    pub(crate) get_retries: Counter,
    /// Objects that poisoned their split, by bounded reason, with one
    /// pre-registered handle per [`PoisonKind`].
    objects_failed_not_found: Counter,
    objects_failed_etag_drift: Counter,
    objects_failed_undecodable: Counter,
    objects_failed_retries_exhausted: Counter,
}

impl std::fmt::Debug for S3Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("S3Metrics")
    }
}

impl S3Metrics {
    /// Resolve every family under the runtime-minted scope. Build-time
    /// only.
    pub(crate) fn new(meter: &Meter) -> S3Metrics {
        let failed = |reason: &'static str| {
            meter.counter("objects_failed_total", &[("reason", reason.into())])
        };
        S3Metrics {
            objects_listed: meter.counter("objects_listed_total", &[]),
            objects_completed: meter.counter("objects_completed_total", &[]),
            objects_remaining: meter.gauge("objects_remaining", &[]),
            bytes_read: meter.counter("bytes_read_total", &[]),
            bytes_decoded: meter.counter("bytes_decoded_total", &[]),
            get_retries: meter.counter("get_retries_total", &[]),
            objects_failed_not_found: failed(PoisonKind::NotFound.reason_label()),
            objects_failed_etag_drift: failed(PoisonKind::EtagDrift.reason_label()),
            objects_failed_undecodable: failed(PoisonKind::Undecodable.reason_label()),
            objects_failed_retries_exhausted: failed(PoisonKind::RetriesExhausted.reason_label()),
        }
    }

    /// The pre-registered failure counter for `kind` (never resolves a
    /// name at increment time).
    pub(crate) fn objects_failed(&self, kind: PoisonKind) -> &Counter {
        match kind {
            PoisonKind::NotFound => &self.objects_failed_not_found,
            PoisonKind::EtagDrift => &self.objects_failed_etag_drift,
            PoisonKind::Undecodable => &self.objects_failed_undecodable,
            PoisonKind::RetriesExhausted => &self.objects_failed_retries_exhausted,
        }
    }
}
