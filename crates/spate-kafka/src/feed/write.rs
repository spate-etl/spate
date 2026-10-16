//! The writes the feed makes, how a failed one is classified, and where a
//! failed unit goes.

use super::Core;
use super::events::Cause;
use super::inflight::Position;
use super::producer::FeedProducerContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, ThreadedProducer};
use spate_core::error::ErrorClass;
use spate_core::feed::{Confirmer, Unit};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The header that carries how many failures were reported for a unit.
pub(crate) const DELIVERIES_HEADER: &str = "spate.deliveries";

const BACKOFF_START: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// The feed's producer.
pub(crate) type Producer = ThreadedProducer<FeedProducerContext>;

/// A message to produce, kept whole so a failed write can be sent again.
#[derive(Debug)]
pub(crate) struct Outgoing {
    pub(crate) topic: Arc<str>,
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) headers: Vec<(String, Option<Vec<u8>>)>,
}

impl Outgoing {
    /// The message for `unit` on `topic`. A `deliveries` of 0 writes no
    /// count header.
    pub(crate) fn unit(topic: Arc<str>, unit: &Unit, deliveries: u32) -> Outgoing {
        let headers = if deliveries == 0 {
            Vec::new()
        } else {
            vec![(
                DELIVERIES_HEADER.to_owned(),
                Some(deliveries.to_string().into_bytes()),
            )]
        };
        Outgoing {
            topic,
            key: Some(unit.id.clone().into_bytes()),
            payload: Some(unit.body.clone()),
            headers,
        }
    }
}

/// Where a failed unit goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Destination {
    Retry,
    DeadLetter(Cause),
}

/// A write that moves a delivered message to the retry or dead-letter
/// topic. Its offset finishes when the write is confirmed.
#[derive(Debug)]
pub(crate) struct Move {
    pub(crate) message: Outgoing,
    pub(crate) position: Position,
    pub(crate) to: Destination,
    /// The unit's id; `None` for a message that names no unit.
    pub(crate) unit: Option<String>,
    /// The delivery being moved, counted as the header plus one.
    pub(crate) deliveries: u32,
    /// When the first attempt was enqueued.
    pub(crate) started: Instant,
    /// The wait before the next attempt, should this one fail.
    pub(crate) backoff: Duration,
}

impl Move {
    pub(crate) fn new(
        message: Outgoing,
        position: Position,
        to: Destination,
        unit: Option<String>,
        deliveries: u32,
    ) -> Move {
        Move {
            message,
            position,
            to,
            unit,
            deliveries,
            started: Instant::now(),
            backoff: BACKOFF_START,
        }
    }
}

/// One write, carried through librdkafka as the message's delivery opaque.
/// Each enqueued write gets exactly one report, so a `Move` is completed
/// or sent again once per attempt and never twice.
#[derive(Debug)]
pub(crate) enum Write {
    Publish {
        message: Outgoing,
        confirmer: Confirmer,
    },
    Move(Move),
}

impl Write {
    fn message(&self) -> &Outgoing {
        match self {
            Write::Publish { message, .. } | Write::Move(Move { message, .. }) => message,
        }
    }
}

/// A failed write: librdkafka's code, when it gave one, and the text.
#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) code: Option<RDKafkaErrorCode>,
    pub(crate) reason: String,
}

impl Failure {
    pub(crate) fn of(error: &KafkaError) -> Failure {
        Failure {
            code: error.rdkafka_error_code(),
            reason: error.to_string(),
        }
    }
}

/// How the feed reacts to a failed write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteClass {
    /// The message is over a size limit. A publish is refused; a move
    /// cannot be sent at all, and the feed stops.
    Refused,
    /// The feed stops.
    Fatal,
    /// The write is sent again after a backoff.
    Retryable,
}

/// Classify a failed write by the sink's table, except that an over-size
/// message is refused.
pub(crate) fn classify(code: Option<RDKafkaErrorCode>) -> WriteClass {
    match code {
        Some(RDKafkaErrorCode::MessageSizeTooLarge) => WriteClass::Refused,
        // librdkafka's local code for a topic it has marked missing. One
        // stale metadata response can mark an existing topic missing for a
        // propagation window, so the write is sent again.
        Some(RDKafkaErrorCode::UnknownTopic) => WriteClass::Retryable,
        code => match crate::sink::report_class(code) {
            ErrorClass::Fatal => WriteClass::Fatal,
            _ => WriteClass::Retryable,
        },
    }
}

/// The wait after `backoff` for a write that failed again.
pub(crate) fn next_backoff(backoff: Duration) -> Duration {
    (backoff * 2).min(BACKOFF_MAX)
}

/// The count of a delivery whose message carries `header` as its
/// `spate.deliveries` value: the failures reported so far, plus one. An
/// absent or unreadable header counts no failure.
pub(crate) fn deliveries_of(header: Option<&[u8]>) -> u32 {
    header
        .and_then(|h| std::str::from_utf8(h).ok())
        .and_then(|h| h.trim().parse::<u32>().ok())
        .unwrap_or(0)
        .saturating_add(1)
}

/// Where a unit failed on delivery `deliveries` goes under `cap`.
pub(crate) fn route(deliveries: u32, cap: NonZeroU32) -> Destination {
    if deliveries >= cap.get() {
        Destination::DeadLetter(Cause::AttemptCap)
    } else {
        Destination::Retry
    }
}

/// Enqueue `write` on `producer`. A write librdkafka refuses at once is
/// settled as a failed report would settle it.
pub(crate) fn send(core: &Core, producer: &Producer, write: Box<Write>) {
    // librdkafka copies the key, value and headers at enqueue, and the
    // write itself travels as the opaque, so the record borrows copies.
    let (topic, key, payload, headers) = {
        let m = write.message();
        let mut headers = OwnedHeaders::new_with_capacity(m.headers.len());
        for (key, value) in &m.headers {
            headers = headers.insert(Header {
                key,
                value: value.as_deref(),
            });
        }
        (
            Arc::clone(&m.topic),
            m.key.clone(),
            m.payload.clone(),
            headers,
        )
    };
    let mut record =
        BaseRecord::<[u8], [u8], Box<Write>>::with_opaque_to(&topic, write).headers(headers);
    if let Some(key) = &key {
        record = record.key(key.as_slice());
    }
    if let Some(payload) = &payload {
        record = record.payload(payload.as_slice());
    }
    if let Err((error, record)) = producer.send(record) {
        core.settle(*record.delivery_opaque, Err(Failure::of(&error)));
    }
}
