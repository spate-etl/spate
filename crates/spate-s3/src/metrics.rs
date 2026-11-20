//! Connector-owned metric families, registered through the source's
//! [`Meter`] (namespace `s3`, role `source` → `spate_s3_source_*`).
//!
//! All handles are resolved once at `open` and cloned into the lanes and
//! fetchers; nothing resolves names on the record path. Counters are
//! incremented at batch/object/chunk boundaries per the taxonomy's
//! hot-path discipline.

use crate::lane_ctx::PoisonKind;
use spate_core::metrics::names::L_REASON;
use spate_core::metrics::{Counter, Gauge, Meter};

const OBJECTS_COMPLETED_TOTAL: &str = "objects_completed_total";
const REMAINING_OBJECTS: &str = "remaining_objects";
const BYTES_READ_TOTAL: &str = "bytes_read_total";
const BYTES_DECODED_TOTAL: &str = "bytes_decoded_total";
const GET_RETRIES_TOTAL: &str = "get_retries_total";
const OBJECTS_OVERWRITTEN_TOTAL: &str = "objects_overwritten_total";
const OVERWRITES_DROPPED_TOTAL: &str = "overwrites_dropped_total";
const OBJECTS_FAILED_TOTAL: &str = "objects_failed_total";

/// Pre-registered `spate_s3_source_*` handles.
#[derive(Clone, Debug)]
pub(crate) struct S3SourceMetrics {
    /// Objects, or byte ranges of one, fully framed and handed to the
    /// pipeline.
    objects_completed: Counter,
    /// Objects not yet completed across this worker's currently-held
    /// units, a cut object counting once per range (rises by the objects a
    /// lane reads when it opens, falls per completed object, settles when the
    /// lane closes).
    remaining_objects: Gauge,
    /// Bytes after decompression, as the framer receives them. For a plain
    /// object read whole this equals `bytes_read`; the bytes a byte-range
    /// read trims count in `bytes_read` alone.
    bytes_decoded: Counter,
    /// Keys whose read an overwrite cut short in a notification unit, once
    /// per key and delivery, whether or not the key was re-enqueued.
    objects_overwritten: Counter,
    /// Of those, the keys acknowledged at the re-enqueue cap without a
    /// re-enqueue.
    overwrites_dropped: Counter,
    /// Objects, or byte ranges of one, that failed their unit, by bounded
    /// reason, with one pre-registered handle per [`PoisonKind`].
    objects_failed_not_found: Counter,
    objects_failed_etag_drift: Counter,
    objects_failed_undecodable: Counter,
    objects_failed_retries_exhausted: Counter,
    /// The families the fetcher task counts as well as the lane.
    fetch: FetchCounters,
}

impl S3SourceMetrics {
    /// Resolve every family under the runtime-minted scope. Build-time
    /// only.
    pub(crate) fn new(meter: &Meter) -> S3SourceMetrics {
        let failed = |reason: &'static str| {
            meter.counter(OBJECTS_FAILED_TOTAL, &[(L_REASON, reason.into())])
        };
        S3SourceMetrics {
            objects_completed: meter.counter(OBJECTS_COMPLETED_TOTAL, &[]),
            remaining_objects: meter.gauge(REMAINING_OBJECTS, &[]),
            bytes_decoded: meter.counter(BYTES_DECODED_TOTAL, &[]),
            objects_overwritten: meter.counter(OBJECTS_OVERWRITTEN_TOTAL, &[]),
            overwrites_dropped: meter.counter(OVERWRITES_DROPPED_TOTAL, &[]),
            objects_failed_not_found: failed(PoisonKind::NotFound.reason_label()),
            objects_failed_etag_drift: failed(PoisonKind::EtagDrift.reason_label()),
            objects_failed_undecodable: failed(PoisonKind::Undecodable.reason_label()),
            objects_failed_retries_exhausted: failed(PoisonKind::RetriesExhausted.reason_label()),
            fetch: FetchCounters {
                get_retries: meter.counter(GET_RETRIES_TOTAL, &[]),
                bytes_read: meter.counter(BYTES_READ_TOTAL, &[]),
            },
        }
    }

    /// The handles a fetcher task counts into.
    pub(crate) fn fetch_counters(&self) -> FetchCounters {
        self.fetch.clone()
    }

    /// Count one object, or byte range of one, framed and handed on.
    pub(crate) fn object_completed(&self) {
        self.objects_completed.increment(1);
    }

    /// Raise `remaining_objects` by the `objects` a lane opens with.
    pub(crate) fn remaining_objects_added(&self, objects: u64) {
        self.remaining_objects.increment(objects as f64);
    }

    /// Lower `remaining_objects` by `objects` completed or taken back.
    pub(crate) fn remaining_objects_settled(&self, objects: u64) {
        self.remaining_objects.decrement(objects as f64);
    }

    /// Count `bytes` read from the store.
    pub(crate) fn bytes_read(&self, bytes: u64) {
        self.fetch.read(bytes);
    }

    /// Count `bytes` the framer received after decompression.
    pub(crate) fn bytes_decoded(&self, bytes: u64) {
        self.bytes_decoded.increment(bytes);
    }

    /// Count one key whose read an overwrite cut short.
    pub(crate) fn object_overwritten(&self) {
        self.objects_overwritten.increment(1);
    }

    /// Count one overwritten key acknowledged at the re-enqueue cap.
    pub(crate) fn overwrite_dropped(&self) {
        self.overwrites_dropped.increment(1);
    }

    /// Count one object, or byte range of one, that failed its unit for
    /// `kind`, on its pre-registered handle.
    pub(crate) fn object_failed(&self, kind: PoisonKind) {
        let counter = match kind {
            PoisonKind::NotFound => &self.objects_failed_not_found,
            PoisonKind::EtagDrift => &self.objects_failed_etag_drift,
            PoisonKind::Undecodable => &self.objects_failed_undecodable,
            PoisonKind::RetriesExhausted => &self.objects_failed_retries_exhausted,
        };
        counter.increment(1);
    }
}

/// The handles a fetcher task counts into, cloned into each one.
#[derive(Clone, Debug)]
pub(crate) struct FetchCounters {
    /// Object GET and HEAD attempts beyond the first (transient failures).
    get_retries: Counter,
    /// Bytes read from the store (as stored, pre-decompression).
    bytes_read: Counter,
}

impl FetchCounters {
    /// Wrap handles a test observes.
    #[cfg(test)]
    pub(crate) fn from_handles(get_retries: Counter, bytes_read: Counter) -> FetchCounters {
        FetchCounters {
            get_retries,
            bytes_read,
        }
    }

    /// Count one GET or HEAD attempt beyond the first.
    pub(crate) fn get_retried(&self) {
        self.get_retries.increment(1);
    }

    /// Count `bytes` read from the store.
    pub(crate) fn read(&self, bytes: u64) {
        self.bytes_read.increment(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_test::render_metrics;

    /// Each family renders under its exported name with what was counted
    /// through its method. The test `Meter` renders `spate_s3_<local>`; the
    /// runtime's role-scoped name is `spate_s3_source_<local>`.
    #[test]
    fn every_family_renders_under_its_exported_name() {
        let rendered = render_metrics(|| {
            let metrics =
                S3SourceMetrics::new(&Meter::with_namespace("s3", "metrics", "families", "s3"));
            metrics.object_completed();
            metrics.remaining_objects_added(7);
            metrics.remaining_objects_settled(2);
            metrics.bytes_read(11);
            metrics.fetch_counters().read(2);
            metrics.bytes_decoded(17);
            metrics.object_overwritten();
            metrics.overwrite_dropped();
            metrics.overwrite_dropped();
            let fetch = metrics.fetch_counters();
            for _ in 0..3 {
                fetch.get_retried();
            }
            for (kind, times) in [
                (PoisonKind::NotFound, 1),
                (PoisonKind::EtagDrift, 2),
                (PoisonKind::Undecodable, 3),
                (PoisonKind::RetriesExhausted, 4),
            ] {
                for _ in 0..times {
                    metrics.object_failed(kind);
                }
            }
        });
        let labels = r#"pipeline="metrics",component="families",component_type="s3""#;
        let mut want: Vec<String> = [
            ("objects_completed_total", 1),
            ("remaining_objects", 5),
            ("bytes_read_total", 13),
            ("bytes_decoded_total", 17),
            ("objects_overwritten_total", 1),
            ("overwrites_dropped_total", 2),
            ("get_retries_total", 3),
        ]
        .iter()
        .map(|(name, value)| format!("spate_s3_{name}{{{labels}}} {value}"))
        .collect();
        for (kind, value) in [
            (PoisonKind::NotFound, 1),
            (PoisonKind::EtagDrift, 2),
            (PoisonKind::Undecodable, 3),
            (PoisonKind::RetriesExhausted, 4),
        ] {
            want.push(format!(
                r#"spate_s3_objects_failed_total{{{labels},reason="{}"}} {value}"#,
                kind.reason_label()
            ));
        }
        for line in want {
            assert!(
                rendered.lines().any(|rendered_line| rendered_line == line),
                "missing {line} in:\n{rendered}"
            );
        }
    }
}
