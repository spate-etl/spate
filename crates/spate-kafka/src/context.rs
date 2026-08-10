//! Consumer context: rebalance interception and statistics capture.
//!
//! The framework needs rebalances to complete only after the pipeline has
//! drained and committed, but librdkafka runs the rebalance callback inside
//! `poll()` on the controller thread — the same thread that must orchestrate
//! the drain. The escape hatch is librdkafka's deferred-acknowledgment
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
    /// Rebalance error reported by librdkafka. The callback has already
    /// synchronized state with `unassign` (the contract for an event that
    /// is neither assign nor revoke), so the member owns nothing when this
    /// intent is consumed; `poll_events` surfaces the lane revocation and
    /// then reports the classified error.
    Error(rdkafka::error::RDKafkaErrorCode),
}

impl Intent {
    /// The variant name, for the intent-pileup log line.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Intent::Assign(_) => "assign",
            Intent::Revoke(_) => "revoke",
            Intent::Error(_) => "error",
        }
    }
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
                // An arbitrary rebalance failure. librdkafka's contract
                // (`rd_kafka_conf_set_rebalance_cb`) requires the
                // application to call `rd_kafka_assign(rk, NULL)` here to
                // synchronize state; deferring it — unlike assign/revoke,
                // which stay legally in progress until completed — leaves
                // the member wedged mid-rebalance forever: no rejoin, no
                // fresh assignment, until the process restarts.
                if let Err(e) = base_consumer.unassign() {
                    tracing::warn!(error = %e, "unassign after a rebalance error failed");
                }
                let code: rdkafka::error::RDKafkaErrorCode = other.into();
                Intent::Error(code)
            }
        };
        self.intents.lock().expect("intent lock").push_back(intent);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::ClientConfig;
    use rdkafka::config::FromClientConfigAndContext;

    /// A lazily-connecting consumer with this crate's context; no broker is
    /// contacted (assign/unassign/assignment are local operations).
    fn consumer() -> BaseConsumer<SourceContext> {
        let mut cc = ClientConfig::new();
        cc.set("bootstrap.servers", "127.0.0.1:1");
        cc.set("group.id", "ctx-test");
        BaseConsumer::from_config_and_context(&cc, SourceContext::default()).expect("consumer")
    }

    /// The contract this pins (librdkafka `rd_kafka_conf_set_rebalance_cb`):
    /// a rebalance event that is neither assign nor revoke must be answered
    /// with `assign(NULL)` to synchronize state. A handler that only records
    /// the error leaves the member mid-rebalance forever — no rejoin until
    /// the process restarts.
    #[test]
    fn an_arbitrary_rebalance_error_unassigns_and_queues_the_code() {
        let consumer = consumer();
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("orders", 0);
        consumer.assign(&tpl).expect("local assign");
        assert_eq!(consumer.assignment().expect("assignment").count(), 1);

        let mut cb_tpl = TopicPartitionList::new();
        consumer.context().rebalance(
            &consumer,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            &mut cb_tpl,
        );

        assert_eq!(
            consumer.assignment().expect("assignment").count(),
            0,
            "the error arm must synchronize state with unassign"
        );
        let intents = consumer.context().intents.lock().expect("intent lock");
        assert!(
            matches!(
                intents.front(),
                Some(Intent::Error(
                    rdkafka::error::RDKafkaErrorCode::RebalanceInProgress
                ))
            ),
            "the typed code is queued for poll_events: {intents:?}"
        );
    }

    /// Assign and revoke events keep deferring: the callback records the
    /// intent and leaves the rebalance legally in progress (no assign or
    /// unassign call) for the controller-side choreography to complete.
    #[test]
    fn assign_and_revoke_events_stay_deferred() {
        let consumer = consumer();
        let mut cb_tpl = TopicPartitionList::new();
        cb_tpl.add_partition("orders", 0);
        consumer.context().rebalance(
            &consumer,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
            &mut cb_tpl,
        );
        assert_eq!(
            consumer.assignment().expect("assignment").count(),
            0,
            "an assign event must not be accepted from the callback"
        );
        let kinds: Vec<&'static str> = consumer
            .context()
            .intents
            .lock()
            .expect("intent lock")
            .iter()
            .map(Intent::kind)
            .collect();
        assert_eq!(kinds, vec!["assign"]);
    }
}
