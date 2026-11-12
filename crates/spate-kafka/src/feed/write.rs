//! The writes the feed makes, how a failed one is classified, and where a
//! failed unit goes.

use super::Core;
use super::attempt::{FENCE_KEY, ORIGIN_HEADER, PublishId, encode};
use super::events::Cause;
use super::inflight::Position;
use super::producer::FeedProducerContext;
use crate::sink::owned_headers;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseRecord, ThreadedProducer};
use spate_core::backoff::Backoff;
use spate_core::clock::{Clock, Stopwatch};
use spate_core::error::ErrorClass;
use spate_core::feed::{Confirmer, Receipt, Unit};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

/// The header that carries, on a copy written to the retry or dead-letter
/// topic, the count of the delivery the copy was written from. The feed
/// writes it for an operator and never reads it.
pub(crate) const DELIVERIES_HEADER: &str = "spate.deliveries";

/// The resend schedule of a write that fails transiently: 200 ms doubling
/// to 5 s.
const RESEND_BACKOFF: Backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(5));

/// The feed's producer.
pub(crate) type Producer = ThreadedProducer<FeedProducerContext>;

/// A message to produce, kept whole so a failed write can be sent again.
#[derive(Debug)]
pub(crate) struct Outgoing {
    pub(crate) topic: Arc<str>,
    /// The partition to write to; `None` leaves it to librdkafka's
    /// partitioner.
    pub(crate) partition: Option<i32>,
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) headers: Vec<(String, Option<Vec<u8>>)>,
}

impl Outgoing {
    /// The message for `unit` on `topic`. A `deliveries` of 0 writes no
    /// count header, and an `origin` is written as the `spate.origin`
    /// header.
    pub(crate) fn unit(
        topic: Arc<str>,
        unit: &Unit,
        deliveries: u32,
        origin: Option<&PublishId>,
    ) -> Outgoing {
        let mut headers = Vec::new();
        if deliveries != 0 {
            headers.push((
                DELIVERIES_HEADER.to_owned(),
                Some(deliveries.to_string().into_bytes()),
            ));
        }
        if let Some(origin) = origin {
            headers.push((ORIGIN_HEADER.to_owned(), Some(origin.as_bytes().to_vec())));
        }
        Outgoing {
            topic,
            partition: None,
            key: Some(unit.id.clone().into_bytes()),
            payload: Some(unit.body.clone()),
            headers,
        }
    }

    /// The record keyed `key` on `partition` of the attempt topic `topic`:
    /// `attempts`, or a tombstone for `None`.
    fn attempt_record(
        topic: &Arc<str>,
        key: &[u8],
        partition: i32,
        attempts: Option<u32>,
    ) -> Outgoing {
        Outgoing {
            topic: Arc::clone(topic),
            partition: Some(partition),
            key: Some(key.to_vec()),
            payload: attempts.map(encode),
            headers: Vec::new(),
        }
    }
}

/// Where a failed unit goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Destination {
    Retry,
    DeadLetter(Cause),
}

/// What finishes a moved message's offset once the move is confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Finish {
    /// The confirmed move finishes it.
    Complete,
    /// A tombstone of the unit's attempt record, sent once the move is
    /// confirmed, finishes it when the tombstone is confirmed.
    Tombstone(PublishId),
}

/// A write that moves a delivered message to the retry or dead-letter
/// topic. A retry write finishes its offset when it is confirmed. A
/// dead-letter write of a unit is followed by a tombstone of the unit's
/// attempt record, and the tombstone's confirmation finishes the offset.
#[derive(Debug)]
pub(crate) struct Move {
    pub(crate) message: Outgoing,
    pub(crate) position: Position,
    pub(crate) to: Destination,
    /// The unit's id; `None` for a message that names no unit.
    pub(crate) unit: Option<String>,
    /// The count of the delivery being moved.
    pub(crate) deliveries: u32,
    pub(crate) finish: Finish,
    /// When the first attempt was enqueued.
    pub(crate) started: Stopwatch,
    /// The write's resend schedule, whose next step is the wait should this
    /// attempt fail.
    pub(crate) backoff: Backoff,
}

impl Move {
    pub(crate) fn new(
        message: Outgoing,
        position: Position,
        to: Destination,
        unit: Option<String>,
        deliveries: u32,
        finish: Finish,
        clock: &dyn Clock,
    ) -> Move {
        Move {
            message,
            position,
            to,
            unit,
            deliveries,
            finish,
            started: Stopwatch::start(clock),
            backoff: RESEND_BACKOFF,
        }
    }
}

/// The write of a unit's attempt record before the unit is read. The unit
/// reaches the reader once the write is confirmed, its receipt held
/// meanwhile.
#[derive(Debug)]
pub(crate) struct Attempt {
    pub(crate) message: Outgoing,
    /// The count the record holds, which the delivery is read with.
    pub(crate) attempts: u32,
    pub(crate) receipt: Receipt,
    pub(crate) unit: Unit,
    pub(crate) identity: PublishId,
    pub(crate) attempt_partition: i32,
    pub(crate) started: Stopwatch,
    pub(crate) backoff: Backoff,
}

impl Attempt {
    /// The write of `attempts` for `identity` on `partition` of the attempt
    /// topic `topic`, for the delivery `receipt` of `unit`.
    pub(crate) fn new(
        topic: &Arc<str>,
        identity: PublishId,
        partition: i32,
        attempts: u32,
        receipt: Receipt,
        unit: Unit,
        clock: &dyn Clock,
    ) -> Attempt {
        Attempt {
            message: Outgoing::attempt_record(
                topic,
                identity.as_bytes(),
                partition,
                Some(attempts),
            ),
            attempts,
            receipt,
            unit,
            identity,
            attempt_partition: partition,
            started: Stopwatch::start(clock),
            backoff: RESEND_BACKOFF,
        }
    }
}

/// The tombstone of a unit's attempt record, which finishes the unit's
/// offset once it is confirmed.
#[derive(Debug)]
pub(crate) struct Tombstone {
    pub(crate) message: Outgoing,
    pub(crate) position: Position,
    pub(crate) backoff: Backoff,
}

impl Tombstone {
    /// The tombstone for `identity` on `partition` of the attempt topic
    /// `topic`, finishing `position`.
    pub(crate) fn new(
        topic: &Arc<str>,
        identity: &PublishId,
        partition: i32,
        position: Position,
    ) -> Tombstone {
        Tombstone {
            message: Outgoing::attempt_record(topic, identity.as_bytes(), partition, None),
            position,
            backoff: RESEND_BACKOFF,
        }
    }
}

/// One write, carried through librdkafka as the message's delivery opaque.
/// Each enqueued write gets exactly one report, so a write is settled once
/// per attempt and never twice.
#[derive(Debug)]
pub(crate) enum Write {
    Publish {
        message: Outgoing,
        confirmer: Confirmer,
    },
    Move(Move),
    Attempt(Attempt),
    Tombstone(Tombstone),
    /// A fence the gate writes past the end of an attempt partition (see
    /// the `attempt` module). It finishes nothing.
    Fence {
        message: Outgoing,
    },
}

impl Write {
    /// The fence for `partition` of the attempt topic `topic`.
    pub(crate) fn fence(topic: &Arc<str>, partition: i32) -> Write {
        Write::Fence {
            message: Outgoing::attempt_record(topic, FENCE_KEY.as_bytes(), partition, None),
        }
    }

    pub(crate) fn message(&self) -> &Outgoing {
        match self {
            Write::Publish { message, .. }
            | Write::Move(Move { message, .. })
            | Write::Attempt(Attempt { message, .. })
            | Write::Tombstone(Tombstone { message, .. })
            | Write::Fence { message } => message,
        }
    }

    /// The resend schedule of a write the feed sends again; `None` for a
    /// publish, whose failure goes to its publisher, and for a fence, which
    /// the gate's next stalled pass writes again.
    pub(crate) fn backoff_mut(&mut self) -> Option<&mut Backoff> {
        match self {
            Write::Publish { .. } | Write::Fence { .. } => None,
            Write::Move(Move { backoff, .. })
            | Write::Attempt(Attempt { backoff, .. })
            | Write::Tombstone(Tombstone { backoff, .. }) => Some(backoff),
        }
    }

    /// What the write carries, for a log line or a latch reason.
    pub(crate) fn what(&self) -> &'static str {
        match self {
            Write::Publish { .. } | Write::Move(_) => "a unit",
            Write::Attempt(_) => "an attempt record",
            Write::Tombstone(_) => "an attempt record's tombstone",
            Write::Fence { .. } => "a fence",
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
    let (topic, partition, key, payload, headers) = {
        let m = write.message();
        let headers = owned_headers(
            m.headers
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_deref())),
        );
        (
            Arc::clone(&m.topic),
            m.partition,
            m.key.clone(),
            m.payload.clone(),
            headers,
        )
    };
    let mut record =
        BaseRecord::<[u8], [u8], Box<Write>>::with_opaque_to(&topic, write).headers(headers);
    if let Some(partition) = partition {
        record = record.partition(partition);
    }
    if let Some(key) = &key {
        record = record.key(key.as_slice());
    }
    // A record with no payload has a null value: a tombstone.
    if let Some(payload) = &payload {
        record = record.payload(payload.as_slice());
    }
    if let Err((error, record)) = producer.send(record) {
        core.settle(*record.delivery_opaque, Err(Failure::of(&error)));
    }
}
