//! The feed producer's hooks: delivery reports, the latest statistics
//! snapshot and librdkafka's errors.

use super::Core;
use super::write::{Failure, Write};
use crate::client::{Hooks, ProducerHooks, SpateContext, log_error};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::DeliveryResult;
use rdkafka::statistics::Statistics;
use std::sync::{Arc, Mutex};

/// The context the feed's producer runs under.
pub(crate) type FeedProducerContext = SpateContext<FeedProducerHooks>;

/// The hooks of the feed's producer. Its callbacks run on the producer's
/// poll thread and take the feed's state lock only briefly.
#[derive(Debug)]
pub(crate) struct FeedProducerHooks {
    core: Arc<Core>,
    /// The latest statistics snapshot, latest wins.
    stats: Mutex<Option<Box<Statistics>>>,
    /// The last error callback, until the identical call that repeats it.
    unpaired_error: Mutex<Option<(Option<RDKafkaErrorCode>, String)>>,
}

impl FeedProducerHooks {
    pub(crate) fn new(core: Arc<Core>) -> FeedProducerHooks {
        FeedProducerHooks {
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

impl Hooks for FeedProducerHooks {
    fn stats(&self, statistics: Statistics) {
        *self.stats.lock().expect("stats lock") = Some(Box::new(statistics));
    }

    /// Log the error, and stop the feed on a fatal producer error or a
    /// [`rejection`](crate::error::rejection). librdkafka raises a fatal
    /// error before it purges the queue, so the latch is in place when the
    /// purged writes settle and every later publish fails with the cause.
    /// rdkafka reports a fatal error under its cause's code, so the codes
    /// librdkafka makes fatal for an idempotent producer are named here,
    /// `Fenced` among them, the code librdkafka gives a producer id refused
    /// for an old epoch.
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        let code = error.rdkafka_error_code();
        if self.repeats_previous(code, reason) {
            return;
        }
        log_error(&error, reason);
        match code {
            Some(
                RDKafkaErrorCode::Fatal
                | RDKafkaErrorCode::ClusterAuthorizationFailed
                | RDKafkaErrorCode::OutOfOrderSequenceNumber
                | RDKafkaErrorCode::InvalidProducerEpoch
                | RDKafkaErrorCode::InvalidProducerIdMapping
                | RDKafkaErrorCode::UnknownProducerId
                | RDKafkaErrorCode::Fenced
                | RDKafkaErrorCode::Inconsistent
                | RDKafkaErrorCode::GaplessGuarantee,
            ) => {
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
