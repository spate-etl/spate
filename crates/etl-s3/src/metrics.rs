//! Connector-owned metric families, registered through the source's
//! [`Meter`] (namespace `s3`, role `source` → `etl_s3_source_*`).
//!
//! All handles are resolved once at `open` and cloned into the lanes and
//! fetchers; nothing resolves names on the record path. Counters are
//! incremented at batch/object/chunk boundaries per the taxonomy's
//! hot-path discipline.

use etl_core::metrics::{Counter, Gauge, Meter};

/// Pre-registered `etl_s3_source_*` handles.
#[derive(Clone)]
pub(crate) struct S3Metrics {
    /// Objects discovered by the startup listing.
    pub(crate) objects_listed: Counter,
    /// Objects fully framed and handed to the pipeline.
    pub(crate) objects_completed: Counter,
    /// Objects not yet completed (listing minus completed; resumes start
    /// below the listing total).
    pub(crate) objects_remaining: Gauge,
    /// Bytes read from the store (as stored, pre-decompression).
    pub(crate) bytes_read: Counter,
    /// Bytes after decompression (equals `bytes_read` for plain objects).
    pub(crate) bytes_decoded: Counter,
    /// Object GET attempts beyond the first (transient failures).
    pub(crate) get_retries: Counter,
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
        S3Metrics {
            objects_listed: meter.counter("objects_listed_total", &[]),
            objects_completed: meter.counter("objects_completed_total", &[]),
            objects_remaining: meter.gauge("objects_remaining", &[]),
            bytes_read: meter.counter("bytes_read_total", &[]),
            bytes_decoded: meter.counter("bytes_decoded_total", &[]),
            get_retries: meter.counter("get_retries_total", &[]),
        }
    }
}
