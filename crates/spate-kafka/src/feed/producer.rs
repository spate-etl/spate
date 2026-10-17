//! The feed producer's context: delivery reports, the latest statistics
//! snapshot and librdkafka's errors and logs.

use super::Core;
use super::write::{Failure, Write};
use rdkafka::client::ClientContext;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::ProducerContext;
use rdkafka::statistics::Statistics;
use std::sync::{Arc, Mutex};

/// The context of the feed's producer. Its callbacks run on the producer's
/// poll thread and take the feed's state lock only briefly.
#[derive(Debug)]
pub(crate) struct FeedProducerContext {
    core: Arc<Core>,
    /// The latest statistics snapshot, latest wins.
    stats: Mutex<Option<Box<Statistics>>>,
    /// The last error callback, until the identical call that repeats it.
    unpaired_error: Mutex<Option<(Option<RDKafkaErrorCode>, String)>>,
}

impl FeedProducerContext {
    pub(crate) fn new(core: Arc<Core>) -> FeedProducerContext {
        FeedProducerContext {
            core,
            stats: Mutex::new(None),
            unpaired_error: Mutex::new(None),
        }
    }

    /// Whether this error callback repeats the previous one, which it then
    /// consumes. rdkafka's producer poll hands each error event to the
    /// context twice in a row on the poll thread;
    /// `rdkafka_delivers_each_producer_error_twice` pins that. The flush a
    /// stopping feed runs on its consumer thread is a second poller, whose
    /// callbacks can interleave with the poll thread's on this one slot, so
    /// within that flush an error can be logged twice.
    fn repeats_previous(&self, code: Option<RDKafkaErrorCode>, reason: &str) -> bool {
        let mut unpaired = self.unpaired_error.lock().expect("unpaired error lock");
        if unpaired
            .as_ref()
            .is_some_and(|(c, r)| *c == code && r == reason)
        {
            *unpaired = None;
            return true;
        }
        *unpaired = Some((code, reason.to_owned()));
        false
    }
}

impl ClientContext for FeedProducerContext {
    fn stats(&self, statistics: Statistics) {
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
        if self.repeats_previous(error.rdkafka_error_code(), reason) {
            return;
        }
        tracing::warn!(target: "librdkafka", %error, "{reason}");
    }
}

impl ProducerContext for FeedProducerContext {
    type DeliveryOpaque = Box<Write>;

    fn delivery(&self, result: &DeliveryResult<'_>, write: Box<Write>) {
        let outcome = match result {
            Ok(_) => Ok(()),
            Err((error, _)) => Err(Failure::of(error)),
        };
        self.core.settle(*write, outcome);
    }
}
