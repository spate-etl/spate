//! Deserializer-stage handles (`etl_deser_*`).

use super::labels::ComponentLabels;
use super::names;
use metrics::{Counter, Histogram};
use std::time::Duration;

/// Deserializer-stage handles (`etl_deser_*`).
#[derive(Debug)]
pub struct DeserMetrics {
    ok: Counter,
    errors: Counter,
    dropped_skip: Counter,
    not_ready: Counter,
    batch_duration: Histogram,
}

impl DeserMetrics {
    /// Resolve all deserializer handles.
    pub fn new(labels: &ComponentLabels) -> Self {
        DeserMetrics {
            ok: labels.counter1(names::DESER_RECORDS_TOTAL, names::L_OUTCOME, "ok"),
            errors: labels.counter1(names::DESER_RECORDS_TOTAL, names::L_OUTCOME, "error"),
            dropped_skip: labels.counter1(
                names::DESER_RECORDS_DROPPED_TOTAL,
                names::L_REASON,
                "skip_policy",
            ),
            not_ready: labels.counter(names::DESER_NOT_READY_TOTAL),
            batch_duration: labels.histogram(names::DESER_BATCH_DURATION_SECONDS),
        }
    }

    /// Record one deserialized batch: emitted records, failed payloads,
    /// and time spent.
    #[inline]
    pub fn batch(&self, ok: u64, errors: u64, d: Duration) {
        self.ok.increment(ok);
        if errors > 0 {
            self.errors.increment(errors);
        }
        self.batch_duration.record(d.as_secs_f64());
    }

    /// Count payloads dropped by the Skip policy.
    #[inline]
    pub fn dropped(&self, n: u64) {
        self.dropped_skip.increment(n);
    }

    /// Count not-ready replays (payloads waiting on an upstream
    /// dependency such as a schema fetch).
    #[inline]
    pub fn not_ready(&self, n: u64) {
        self.not_ready.increment(n);
    }
}
