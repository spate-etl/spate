//! The feed producer's hooks: delivery reports, statistics translated into
//! the feed's families, and librdkafka's errors.

use super::Core;
use super::write::{Failure, Write};
use crate::client::{Hooks, ProducerHooks, SpateContext, log_error};
use rdkafka::message::DeliveryResult;
use rdkafka::statistics::Statistics;
use std::sync::Arc;

/// The context the feed's producer runs under.
pub(crate) type FeedProducerContext = SpateContext<FeedProducerHooks>;

/// The hooks of the feed's producer. Its callbacks run on the producer's
/// poll thread and take the feed's state lock only briefly.
#[derive(Debug)]
pub(crate) struct FeedProducerHooks {
    core: Arc<Core>,
}

impl FeedProducerHooks {
    pub(crate) fn new(core: Arc<Core>) -> FeedProducerHooks {
        FeedProducerHooks { core }
    }
}

impl Hooks for FeedProducerHooks {
    fn stats(&self, statistics: Statistics) {
        self.core.publish_producer_stats(&statistics);
    }

    /// Log the error, and stop the feed on a fatal producer error or a
    /// [`rejection`](crate::error::rejection). librdkafka raises a fatal
    /// error before it purges the queue, so the latch is in place when the
    /// purged writes settle and every later publish fails with the cause.
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str, instance: Option<usize>) {
        log_error(&error, reason, instance);
        match error.rdkafka_error_code() {
            Some(code) if crate::error::ends_idempotent_producer(code) => {
                self.core
                    .latch(&format!("the feed's producer stopped: {reason}"));
            }
            Some(code) if crate::error::rejection(code, reason) => {
                self.core.latch(&format!(
                    "the client's credential or TLS handshake was rejected: {reason}"
                ));
            }
            _ => {}
        }
    }
}

impl ProducerHooks for FeedProducerHooks {
    type DeliveryOpaque = Box<Write>;

    fn delivery(&self, result: &DeliveryResult<'_>, write: Box<Write>) {
        let outcome = match result {
            Ok(_) => Ok(()),
            Err((error, _)) => Err(Failure::of(error)),
        };
        self.core.settle(*write, outcome);
    }
}
