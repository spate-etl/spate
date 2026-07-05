//! The data plane: one lane per assigned partition, polling its split
//! partition queue on a pipeline thread.
//!
//! # Zero-copy lifetime strategy
//!
//! `RawPayload`s must borrow librdkafka's message memory without copying,
//! and stay valid for exactly as long as the seam contract promises: until
//! the batch is dropped, which happens before the next `poll` on the same
//! lane. A `BorrowedMessage<'a>`'s payload is freed when the message is
//! dropped (`rd_kafka_message_destroy`), so the messages themselves must
//! outlive every payload reference handed out.
//!
//! The lane therefore **owns** the polled messages across the batch's
//! lifetime: `held` stores them with an erased lifetime, and is cleared
//! only at the start of the next `poll(&mut self)` — at which point the
//! borrow checker has already proven no batch (and no payload borrowed
//! from it) is still alive, because the batch borrows `&mut self`. The
//! erased lifetime is never observable: everything handed out is re-tied
//! to the lane borrow.

use crate::context::SourceContext;
use etl_core::checkpoint::{AckIssuer, AckRef};
use etl_core::error::SourceError;
use etl_core::record::{PartitionId, RawPayload};
use etl_core::source::{LaneId, PayloadBatch, SourceLane};
use rdkafka::Message;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::message::BorrowedMessage;
// NOTE: `PartitionQueue` is not re-exported at `rdkafka::consumer`; the
// `base_consumer` module path is the public location in 0.39.
use std::time::Duration;

/// One assigned partition's pollable queue.
pub struct KafkaLane {
    id: LaneId,
    partition: PartitionId,
    // Declared before `queue`: messages are destroyed before the queue on
    // lane drop.
    //
    // INVARIANT: `held` is cleared and refilled only inside
    // `poll(&mut self)`; every reference derived from it is tied to a
    // borrow of the lane, so no payload can outlive the messages backing it.
    held: Vec<BorrowedMessage<'static>>,
    queue: PartitionQueue<SourceContext>,
    issuer: AckIssuer,
}

impl std::fmt::Debug for KafkaLane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaLane")
            .field("id", &self.id)
            .field("partition", &self.partition)
            .field("held", &self.held.len())
            .finish_non_exhaustive()
    }
}

impl KafkaLane {
    pub(crate) fn new(
        id: LaneId,
        partition: PartitionId,
        queue: PartitionQueue<SourceContext>,
        issuer: AckIssuer,
    ) -> Self {
        KafkaLane {
            id,
            partition,
            held: Vec::new(),
            queue,
            issuer,
        }
    }
}

/// Erase a borrowed message's phantom lifetime so the lane can own it.
///
/// SAFETY: `BorrowedMessage<'a>` is `{ ptr: NativePtr<RDKafkaMessage>,
/// _event: Arc<NativeEvent>, _owner: PhantomData<&'a u8> }` (rdkafka
/// 0.39.0) — the `'a` parameter is phantom only: it affects no layout and
/// no drop behaviour, so transmuting the lifetime is sound. Validity of
/// the message memory is self-contained: the message holds an `Arc` of its
/// owning native event, and destruction happens in the `BorrowedMessage`
/// drop. The lane additionally keeps the consumer alive through its
/// `queue` (which holds a consumer `Arc`), with `held` declared to drop
/// first.
unsafe fn erase_lifetime(msg: BorrowedMessage<'_>) -> BorrowedMessage<'static> {
    // SAFETY: lifetime-only transmute; see function docs.
    unsafe { std::mem::transmute::<BorrowedMessage<'_>, BorrowedMessage<'static>>(msg) }
}

impl SourceLane for KafkaLane {
    type Batch<'a> = KafkaBatch<'a>;

    fn id(&self) -> LaneId {
        self.id
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        max_records: usize,
        timeout: Duration,
    ) -> Result<Option<Self::Batch<'_>>, SourceError> {
        // Exclusive access proves the previous batch (and every payload
        // borrowed from it) is gone; the messages can be destroyed.
        self.held.clear();

        // First message: block up to `timeout` (idle lanes must not spin).
        match self.queue.poll(timeout) {
            None => return Ok(None),
            Some(Err(e)) => {
                // A lane only exists after its partition was assigned, so
                // any permanent condition here is genuinely post-startup.
                return Err(SourceError::Client {
                    class: crate::error::classify_poll_error(&e, true),
                    reason: format!("partition {} poll: {e}", self.partition.0),
                });
            }
            Some(Ok(msg)) => {
                // SAFETY: see `erase_lifetime`; the queue (and through it
                // the consumer) outlives `held` within this lane.
                self.held.push(unsafe { erase_lifetime(msg) });
            }
        }
        // Batch what is already buffered, without waiting.
        while self.held.len() < max_records {
            match self.queue.poll(Duration::ZERO) {
                Some(Ok(msg)) => {
                    // SAFETY: as above.
                    self.held.push(unsafe { erase_lifetime(msg) });
                }
                Some(Err(e)) => {
                    // Deliver what we have; the error will resurface on the
                    // next poll if it persists.
                    tracing::debug!(partition = self.partition.0, error = %e,
                        "queue error while batching; delivering partial batch");
                    break;
                }
                None => break,
            }
        }

        let last_offset = self
            .held
            .last()
            .expect("batch has at least the first message")
            .offset();
        let ack = self.issuer.issue(self.partition, last_offset);
        Ok(Some(KafkaBatch {
            msgs: &self.held,
            idx: 0,
            ack,
            partition: self.partition,
        }))
    }
}

/// One poll's worth of messages, borrowed from the lane.
#[derive(Debug)]
pub struct KafkaBatch<'a> {
    msgs: &'a [BorrowedMessage<'static>],
    idx: usize,
    ack: AckRef,
    partition: PartitionId,
}

impl<'a> PayloadBatch<'a> for KafkaBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let msg = self.msgs.get(self.idx)?;
        self.idx += 1;
        Some(RawPayload {
            // A Kafka tombstone (null payload) surfaces as an empty slice;
            // deserializers treat empty input as a tombstone/skip.
            bytes: msg.payload().unwrap_or(&[]),
            key: msg.key(),
            partition: self.partition,
            offset: msg.offset(),
            timestamp_ms: msg.timestamp().to_millis().unwrap_or(0),
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}
