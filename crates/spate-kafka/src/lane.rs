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
//! only at the start of the next `poll(&mut self)`, at which point the
//! borrow checker has already proven no batch (and no payload borrowed
//! from it) is still alive, because the batch borrows `&mut self`. The
//! erased lifetime is never observable: everything handed out is re-tied
//! to the lane borrow.

use crate::context::SourceContext;
use rdkafka::Message;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Timestamp};
use spate_core::checkpoint::{AckIssuer, AckRef};
use spate_core::error::SourceError;
use spate_core::record::{PartitionId, RawPayload, RecordMeta};
use spate_core::source::{LaneId, PayloadBatch, SourceLane};
use std::sync::Arc;
use std::time::Duration;

/// One assigned partition's pollable queue.
pub struct KafkaLane {
    id: LaneId,
    partition: PartitionId,
    // Declared before `queue`: the messages are destroyed while the consumer
    // behind the queue is still alive.
    held: Vec<BorrowedMessage<'static>>,
    queue: PartitionQueue<SourceContext>,
    context: Arc<SourceContext>,
    issuer: AckIssuer,
    /// An error the queue returned after the batch's first message. librdkafka
    /// may queue it once and fetch past it, so the next poll returns it.
    pending: Option<SourceError>,
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
        context: Arc<SourceContext>,
        issuer: AckIssuer,
    ) -> Self {
        KafkaLane {
            id,
            partition,
            held: Vec::new(),
            queue,
            context,
            issuer,
            pending: None,
        }
    }

    /// The source error for a queue error, read on the thread that polled
    /// the queue, where librdkafka's error callback left its text.
    fn poll_error(&self, e: &KafkaError) -> SourceError {
        let text = e
            .rdkafka_error_code()
            .and_then(|code| self.context.take_error(code));
        SourceError::Client {
            // Post-startup: a lane exists only once its partition is assigned.
            class: crate::error::classify_poll_error(e, true, text.as_deref()),
            reason: crate::error::poll_reason(
                &format!("partition {} poll", self.partition.0),
                e,
                text.as_deref(),
            ),
        }
    }
}

/// Erase a borrowed message's phantom lifetime so the lane can own it.
///
/// # Safety
///
/// `BorrowedMessage<'a>` is
/// `{ ptr: NativePtr<RDKafkaMessage>, _event: Arc<NativeEvent>,
/// _owner: PhantomData<&'a u8> }`. The `'a` parameter affects no layout and
/// no drop behavior, so transmuting the lifetime is sound. Validity of
/// the message memory is self-contained: its `Arc` keeps the native event
/// alive through the message's drop. The lane keeps the consumer alive
/// through `queue` (which holds a consumer `Arc`), with `held` declared
/// to drop first.
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
        self.held.clear();
        if let Some(e) = self.pending.take() {
            return Err(e);
        }

        // First message: block up to `timeout` (idle lanes must not spin).
        match self.queue.poll(timeout) {
            None => return Ok(None),
            Some(Err(e)) => return Err(self.poll_error(&e)),
            Some(Ok(msg)) => {
                // SAFETY: see `erase_lifetime`; the queue (and through it
                // the consumer) outlives `held` within this lane.
                self.held.push(unsafe { erase_lifetime(msg) });
            }
        }
        while self.held.len() < max_records {
            match self.queue.poll(Duration::ZERO) {
                Some(Ok(msg)) => {
                    // SAFETY: as above.
                    self.held.push(unsafe { erase_lifetime(msg) });
                }
                Some(Err(e)) => {
                    self.pending = Some(self.poll_error(&e));
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
            bytes: msg.payload().unwrap_or(&[]),
            key: msg.key(),
            partition: self.partition,
            offset: msg.offset(),
            timestamp_ms: source_event_time_ms(msg.timestamp()),
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

fn source_event_time_ms(timestamp: Timestamp) -> i64 {
    timestamp.to_millis().unwrap_or(RecordMeta::NO_EVENT_TIME)
}

#[cfg(test)]
mod tests {
    use super::source_event_time_ms;
    use rdkafka::message::Timestamp;
    use spate_core::record::RecordMeta;

    /// A missing Kafka timestamp is distinct from Unix epoch. Regression for #1285.
    #[test]
    fn unavailable_timestamp_uses_no_event_time_sentinel() {
        assert_eq!(
            source_event_time_ms(Timestamp::NotAvailable),
            RecordMeta::NO_EVENT_TIME
        );
        assert_eq!(source_event_time_ms(Timestamp::CreateTime(0)), 0);
    }
}
