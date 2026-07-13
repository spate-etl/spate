//! Producer context: the delivery-report → batch-countdown bridge.
//!
//! The framework acks a batch when [`write_batch`] returns `Ok`, so the
//! writer must not return until every message of the batch has a confirmed
//! delivery report. Reports arrive asynchronously on the producer's poll
//! thread; each produced message carries an `Arc<BatchInflight>` as its
//! librdkafka opaque, and the callback decrements the batch's countdown —
//! recording the first failure — then wakes the awaiting writer on the
//! last report. The callback never blocks: it is atomics plus one
//! `Notify::notify_one`.
//!
//! Abort safety: librdkafka holds one `Arc` reference per outstanding
//! message and delivers exactly one report for each — success, error, or
//! `Purge*` at teardown — so a `write_batch` future aborted at the drain
//! deadline leaks nothing; late reports decrement a countdown nobody
//! awaits and the last `Arc` drop frees the state.
//!
//! [`write_batch`]: etl_core::sink::ShardWriter::write_batch

use crate::sink::metrics::KafkaSinkStatsMetrics;
use rdkafka::client::ClientContext;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::ProducerContext;
use rdkafka::statistics::Statistics;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tokio::sync::Notify;

/// Per-batch delivery countdown. Created by `write_batch` after parsing
/// the batch (so the total is known up front — incrementing per send would
/// race reports arriving mid-loop), cloned into every produced message's
/// delivery opaque.
#[derive(Debug)]
pub(crate) struct BatchInflight {
    remaining: AtomicUsize,
    failed: AtomicUsize,
    first_error: OnceLock<(Option<RDKafkaErrorCode>, String)>,
    done: Notify,
}

impl BatchInflight {
    pub(crate) fn new(messages: usize) -> Self {
        BatchInflight {
            remaining: AtomicUsize::new(messages),
            failed: AtomicUsize::new(0),
            first_error: OnceLock::new(),
            done: Notify::new(),
        }
    }

    /// Record a failed report. The first error is kept verbatim (it
    /// classifies the batch); later ones only count.
    pub(crate) fn record_failure(&self, code: Option<RDKafkaErrorCode>, reason: String) {
        let _ = self.first_error.set((code, reason));
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    /// One message resolved (delivered or failed). Wakes the waiter on the
    /// last one. Failure recording must happen *before* this call so the
    /// waiter's acquire on the final decrement observes it.
    pub(crate) fn complete_one(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.done.notify_one();
        }
    }

    /// Await all reports up to `deadline`. Returns `false` on timeout.
    /// Lost-wakeup-safe: the `Notify` future is created before each
    /// re-check of the countdown, so a `notify_one` racing the check is
    /// stored as a permit and observed by the next await.
    pub(crate) async fn wait(&self, deadline: Instant) -> bool {
        loop {
            let notified = self.done.notified();
            if self.remaining.load(Ordering::Acquire) == 0 {
                return true;
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return self.remaining.load(Ordering::Acquire) == 0;
            };
            if tokio::time::timeout(left, notified).await.is_err() {
                return self.remaining.load(Ordering::Acquire) == 0;
            }
        }
    }

    /// The first failed report, when any report failed.
    pub(crate) fn first_error(&self) -> Option<&(Option<RDKafkaErrorCode>, String)> {
        self.first_error.get()
    }

    /// How many reports failed.
    pub(crate) fn failed(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
    }
}

/// Client context for the sink's producers: routes librdkafka logs to
/// `tracing` (mirroring the source's context), translates delivery
/// reports into batch countdowns, and publishes statistics snapshots
/// through the slot shared with the writer.
#[derive(Debug)]
pub(crate) struct SinkContext {
    /// Filled by the writer's `attach_metrics` (main producer only, and
    /// only when statistics are enabled); the `stats` callback publishes
    /// through it from the producer's poll thread.
    stats: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>,
}

impl SinkContext {
    /// The main producer's context, sharing the writer's statistics slot.
    pub(crate) fn new(stats: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>) -> Self {
        SinkContext { stats }
    }

    /// A context for probe producers: its slot is never filled (and the
    /// probe client config sets no statistics interval), so it publishes
    /// nothing — the single main producer stays the sole statistics
    /// source, keeping the absolute-counter mapping sound.
    pub(crate) fn detached() -> Self {
        SinkContext::new(Arc::new(Mutex::new(None)))
    }
}

impl ClientContext for SinkContext {
    /// Runs on the producer's poll thread, once per statistics interval —
    /// never on the record path.
    fn stats(&self, statistics: Statistics) {
        if let Some(metrics) = self.stats.lock().expect("stats slot lock").as_mut() {
            metrics.update(&statistics);
        }
    }

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        use rdkafka::config::RDKafkaLogLevel as L;
        match level {
            L::Emerg | L::Alert | L::Critical | L::Error => {
                tracing::error!(target: "librdkafka", fac, "{log_message}");
            }
            L::Warning => tracing::warn!(target: "librdkafka", fac, "{log_message}"),
            L::Notice | L::Info => tracing::info!(target: "librdkafka", fac, "{log_message}"),
            L::Debug => tracing::debug!(target: "librdkafka", fac, "{log_message}"),
        }
    }

    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        tracing::warn!(target: "librdkafka", %error, "{reason}");
    }
}

impl ProducerContext for SinkContext {
    type DeliveryOpaque = Arc<BatchInflight>;

    /// Runs on the producer's poll thread — never block here.
    fn delivery(&self, delivery_result: &DeliveryResult<'_>, inflight: Arc<BatchInflight>) {
        if let Err((error, _message)) = delivery_result {
            inflight.record_failure(error.rdkafka_error_code(), error.to_string());
        }
        inflight.complete_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn countdown_completes_on_last_delivery() {
        let inflight = Arc::new(BatchInflight::new(3));
        let waiter = {
            let inflight = Arc::clone(&inflight);
            tokio::spawn(
                async move { inflight.wait(Instant::now() + Duration::from_secs(5)).await },
            )
        };
        // Reports fire from a foreign (non-tokio) thread, as librdkafka's
        // poll thread would.
        let reporter = {
            let inflight = Arc::clone(&inflight);
            std::thread::spawn(move || {
                for _ in 0..3 {
                    inflight.complete_one();
                }
            })
        };
        assert!(waiter.await.unwrap(), "all reports in → wait resolves true");
        reporter.join().unwrap();
    }

    #[tokio::test]
    async fn wait_times_out_when_reports_are_missing() {
        let inflight = BatchInflight::new(2);
        inflight.complete_one();
        let deadline = Instant::now() + Duration::from_millis(50);
        assert!(!inflight.wait(deadline).await, "one report missing → false");
    }

    #[tokio::test]
    async fn first_error_wins_and_is_visible_after_countdown() {
        let inflight = Arc::new(BatchInflight::new(2));
        let reporter = {
            let inflight = Arc::clone(&inflight);
            std::thread::spawn(move || {
                inflight
                    .record_failure(Some(RDKafkaErrorCode::MessageTimedOut), "first".to_string());
                inflight.complete_one();
                inflight.record_failure(
                    Some(RDKafkaErrorCode::MessageSizeTooLarge),
                    "second".to_string(),
                );
                inflight.complete_one();
            })
        };
        assert!(inflight.wait(Instant::now() + Duration::from_secs(5)).await);
        reporter.join().unwrap();
        let (code, reason) = inflight.first_error().expect("an error was recorded");
        assert_eq!(*code, Some(RDKafkaErrorCode::MessageTimedOut));
        assert_eq!(reason, "first");
        assert_eq!(inflight.failed(), 2, "later failures still count");
    }

    #[tokio::test]
    async fn abandoned_countdown_reclaims_arcs() {
        // A write_batch future aborted at the drain deadline drops its
        // waiter; late reports must still decrement harmlessly and release
        // their references.
        let inflight = Arc::new(BatchInflight::new(2));
        let opaques = [Arc::clone(&inflight), Arc::clone(&inflight)];
        {
            // Waiter is created and dropped without resolving (the abort).
            let _abandoned = inflight.wait(Instant::now());
        }
        for opaque in opaques {
            opaque.record_failure(None, "purged at teardown".to_string());
            opaque.complete_one();
            drop(opaque);
        }
        assert_eq!(
            Arc::strong_count(&inflight),
            1,
            "all opaque references reclaimed"
        );
        // A late waiter (none exists in practice) would still see completion.
        assert!(inflight.wait(Instant::now()).await);
    }
}
