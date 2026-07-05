//! Consumer context: rebalance interception and statistics capture.
//!
//! The framework needs rebalances to complete only after the pipeline has
//! drained and committed, but librdkafka runs the rebalance callback inside
//! `poll()` on the controller thread — the same thread that must orchestrate
//! the drain. The escape hatch is librdkafka's deferred-acknowledgement
//! protocol: the callback may return **without** calling `assign`/`unassign`;
//! the rebalance then stays in progress until the application calls them
//! later. [`SourceContext::rebalance`] therefore only records an intent, and
//! [`KafkaSource::poll_events`](crate::KafkaSource) performs the actual
//! assign/pause/split (assignments) or barrier-drain/commit/unassign
//! (revocations) choreography from the controller thread.

use rdkafka::TopicPartitionList;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::statistics::Statistics;
use rdkafka::types::RDKafkaRespErr;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// A rebalance event recorded by the callback, consumed by `poll_events`.
#[derive(Debug)]
pub(crate) enum Intent {
    /// Partitions offered to this member. Not yet accepted: `poll_events`
    /// calls `assign` (then pause → split → resume).
    Assign(TopicPartitionList),
    /// Partitions being taken away. Not yet released: `poll_events`
    /// surfaces `LanesRevoked`, the runtime drains and commits, and the
    /// next `poll_events` call completes with `unassign`.
    Revoke(TopicPartitionList),
    /// Rebalance error reported by librdkafka.
    Error(String),
}

/// Shared state between the librdkafka callbacks and [`KafkaSource`].
#[derive(Debug, Default)]
pub(crate) struct SourceContext {
    pub(crate) intents: Mutex<VecDeque<Intent>>,
    pub(crate) stats: Mutex<Option<Box<Statistics>>>,
    /// Set when the source is shutting down. From then on the rebalance
    /// callback completes the protocol inline (`assign`/`unassign`) instead
    /// of deferring: consumer close triggers a final revoke *inside*
    /// `BaseConsumer::drop`'s close-poll loop, where no `poll_events` call
    /// will ever run to complete a deferred intent — deferring there
    /// deadlocks the drop forever.
    pub(crate) closing: AtomicBool,
}

impl ClientContext for SourceContext {
    fn stats(&self, statistics: Statistics) {
        // Latest wins; parsing/aggregation happens on the controller thread.
        *self.stats.lock().expect("stats lock") = Some(Box::new(statistics));
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

impl ConsumerContext for SourceContext {
    /// Full override of the rebalance handler: record the event and return
    /// WITHOUT acknowledging it (no `assign`/`unassign`), deferring
    /// completion to the controller-side choreography described in the
    /// module docs.
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        if self.closing.load(Ordering::Acquire) {
            // Teardown: complete the protocol inline so consumer close can
            // finish. Nothing is consuming any more; the drain/commit
            // choreography has already run (or been abandoned by policy).
            let result = match err {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => base_consumer.assign(tpl),
                _ => base_consumer.unassign(),
            };
            if let Err(e) = result {
                tracing::warn!(error = %e, "rebalance completion during close failed");
            }
            return;
        }
        let intent = match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => Intent::Assign(tpl.clone()),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => Intent::Revoke(tpl.clone()),
            other => {
                let code: rdkafka::error::RDKafkaErrorCode = other.into();
                Intent::Error(code.to_string())
            }
        };
        self.intents.lock().expect("intent lock").push_back(intent);
    }
}
