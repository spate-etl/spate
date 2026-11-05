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
//!
//! # Offsets with no record
//!
//! A transaction marker takes an offset and is never delivered, and under
//! `read_committed` an aborted transaction's records are skipped the same way.
//! librdkafka moves the position past them while this lane's queue is polled.
//! When a poll comes back empty the lane reads the position, and when it is
//! past the last offset the lane issued, it returns a batch with no payloads
//! whose ack covers those offsets.
//!
//! The read sits on this thread because a record moves the position when it is
//! popped, before its batch is issued. At an empty return this thread has issued
//! everything it popped, so the offsets below the position hold nothing
//! unissued. The watermark passes the ack's offsets only once every earlier batch
//! on the partition is delivered, so the committed offset passes a marker only
//! once every record below it is acknowledged.
//!
//! The lane returns such a batch only once every batch it issued has reached a
//! watermark. Until then it keeps polling and serving markers, and one batch then
//! covers them all, so markers behind a batch the sink has not acknowledged take
//! no room under the pending ceiling and leave the lane polling.

use crate::client::read_position;
use crate::context::SourceContext;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::{Message, Offset, TopicPartitionList};
use spate_core::checkpoint::{AckIssuer, AckRef};
use spate_core::error::SourceError;
use spate_core::record::{PartitionId, RawPayload};
use spate_core::source::{LaneId, PayloadBatch, SourceLane};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// How many zero-timeout empty returns pass between position reads. A thread
/// with a busy sibling lane never polls this one with a timeout, so without this
/// an idle partition would hold its marker. It bounds how many sibling batches
/// that wait lasts, and has no measured basis.
const ZERO_TIMEOUT_READ_EVERY: u32 = 256;

/// One assigned partition's pollable queue.
pub struct KafkaLane {
    id: LaneId,
    partition: PartitionId,
    // Declared before `queue`: the messages are destroyed while the consumer
    // behind the queue is still alive.
    held: Vec<BorrowedMessage<'static>>,
    queue: PartitionQueue<SourceContext>,
    consumer: Arc<BaseConsumer<SourceContext>>,
    issuer: AckIssuer,
    /// An error the queue returned after the batch's first message. librdkafka
    /// may queue it once and fetch past it, so the next poll returns it.
    pending: Option<SourceError>,
    /// This lane's partition, the one element [`read_position`] fills.
    position: TopicPartitionList,
    /// One past the last offset this lane issued.
    covered_to: Option<i64>,
    /// The latest watermark the source was asked to store for this partition.
    watermark: Arc<AtomicI64>,
    /// A poll failed since the last record. An error does not move the
    /// position, so a later marker moves it past the offsets that failed.
    saw_error: bool,
    /// Zero-timeout empty returns since the last read.
    empty_returns: u32,
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
        consumer: Arc<BaseConsumer<SourceContext>>,
        issuer: AckIssuer,
        position: TopicPartitionList,
        watermark: Arc<AtomicI64>,
    ) -> Self {
        KafkaLane {
            id,
            partition,
            held: Vec::new(),
            queue,
            consumer,
            issuer,
            pending: None,
            position,
            covered_to: None,
            watermark,
            saw_error: false,
            empty_returns: 0,
        }
    }

    /// The batch issuing an ack through `last_offset`, over whatever `held`
    /// carries.
    fn issue_batch(&mut self, last_offset: i64) -> KafkaBatch<'_> {
        let ack = self.issuer.issue(self.partition, last_offset);
        KafkaBatch {
            msgs: &self.held,
            idx: 0,
            ack,
            partition: self.partition,
        }
    }

    /// The batch with no payloads retiring offsets past what this lane issued,
    /// when an empty return finds any. `held` is empty here.
    fn retire_offsets(&mut self, timeout: Duration) -> Option<KafkaBatch<'_>> {
        if self.saw_error
            || self.consumer.context().rewound()
            || !issued_batches_settled(self.covered_to, self.watermark.load(Ordering::Acquire))
            || !read_is_due(timeout, &mut self.empty_returns)
        {
            return None;
        }
        let position = read_position(&self.consumer, &mut self.position);
        let last_offset = offset_to_retire(position, self.covered_to)?;
        self.covered_to = Some(last_offset + 1);
        tracing::debug!(
            partition = self.partition.0,
            last_offset,
            "retiring offsets that hold no record"
        );
        Some(self.issue_batch(last_offset))
    }

    /// The source error for a queue error, read on the thread that polled
    /// the queue, where librdkafka's error callback left its text.
    fn poll_error(&self, e: &KafkaError) -> SourceError {
        let text = e
            .rdkafka_error_code()
            .and_then(|code| self.consumer.context().take_error(code));
        crate::error::poll_error(
            &format!("partition {} poll", self.partition.0),
            e,
            // Post-startup: a lane exists only once its partition is assigned.
            true,
            text.as_deref(),
        )
    }
}

/// The last offset to retire when the position reads `position` and the lane has
/// issued up to `covered_to`, or `None` when nothing lies between them. A logical
/// position means no served op has set it, and with no issue yet an offset of 0
/// has nothing below it.
fn offset_to_retire(position: Offset, covered_to: Option<i64>) -> Option<i64> {
    let Offset::Offset(position) = position else {
        return None;
    };
    (position > covered_to.unwrap_or(0)).then(|| position - 1)
}

/// Whether every batch a lane issued, through `covered_to`, has reached
/// `watermark`. A lane that has issued nothing has nothing to wait for, and a
/// watermark past `covered_to` predates a rewind and says nothing of what the lane
/// issued since.
fn issued_batches_settled(covered_to: Option<i64>, watermark: i64) -> bool {
    covered_to.is_none_or(|covered| watermark == covered)
}

/// Whether an empty return from a poll of `timeout` reads the position. Every
/// return that may have blocked reads, and a zero-timeout one reads once per
/// [`ZERO_TIMEOUT_READ_EVERY`], counted in `empty_returns`.
fn read_is_due(timeout: Duration, empty_returns: &mut u32) -> bool {
    if !timeout.is_zero() {
        return true;
    }
    *empty_returns += 1;
    if *empty_returns < ZERO_TIMEOUT_READ_EVERY {
        return false;
    }
    *empty_returns = 0;
    true
}

#[cfg(test)]
impl KafkaLane {
    /// Treat the offsets this lane issued as not issued, so the next empty return
    /// finds them past what it covers, as it finds a marker.
    pub(crate) fn forget_issued(&mut self) {
        self.covered_to = None;
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
            self.saw_error = true;
            return Err(e);
        }

        // First message: block up to `timeout` (idle lanes must not spin).
        match self.queue.poll(timeout) {
            None => return Ok(self.retire_offsets(timeout)),
            Some(Err(e)) => {
                self.saw_error = true;
                return Err(self.poll_error(&e));
            }
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
        self.covered_to = Some(last_offset.saturating_add(1));
        self.saw_error = false;
        Ok(Some(self.issue_batch(last_offset)))
    }
}

/// One poll's worth of messages, borrowed from the lane. A batch retiring
/// offsets that hold no record has no messages.
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
            timestamp_ms: msg.timestamp().to_millis().unwrap_or(0),
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use spate_core::checkpoint::Checkpointer;

    /// The position rule: only a concrete position past what the lane issued
    /// retires offsets, and it retires through the offset below the position.
    /// Regression for #1709.
    #[test]
    fn the_position_rule_advances_only_past_what_is_covered() {
        for logical in [
            Offset::Invalid,
            Offset::Stored,
            Offset::End,
            Offset::Beginning,
            Offset::OffsetTail(1),
        ] {
            assert_eq!(offset_to_retire(logical, None), None, "{logical:?}");
            assert_eq!(offset_to_retire(logical, Some(5)), None, "{logical:?}");
        }
        assert_eq!(offset_to_retire(Offset::Offset(4), Some(5)), None);
        assert_eq!(offset_to_retire(Offset::Offset(5), Some(5)), None);
        assert_eq!(offset_to_retire(Offset::Offset(6), Some(5)), Some(5));
        assert_eq!(offset_to_retire(Offset::Offset(0), None), None);
        assert_eq!(offset_to_retire(Offset::Offset(1), None), Some(0));
    }

    /// A lane waits for every batch it issued to reach a watermark, and one that
    /// has issued nothing waits for nothing. Regression for #1709.
    #[test]
    fn a_lane_waits_for_what_it_issued_to_reach_a_watermark() {
        assert!(issued_batches_settled(None, i64::MIN));
        assert!(!issued_batches_settled(Some(5), 4));
        assert!(issued_batches_settled(Some(5), 5));
        assert!(!issued_batches_settled(Some(5), 6));
    }

    /// A return that may have blocked always reads. A zero-timeout one reads
    /// every [`ZERO_TIMEOUT_READ_EVERY`], so a lane beside a busy sibling still
    /// reads. Regression for #1709.
    #[test]
    fn the_read_is_due_on_a_blocking_return_and_every_nth_zero_timeout() {
        let mut empty = 0;
        assert!(read_is_due(Duration::from_millis(1), &mut empty));
        assert_eq!(empty, 0);
        for _ in 1..ZERO_TIMEOUT_READ_EVERY {
            assert!(!read_is_due(Duration::ZERO, &mut empty));
        }
        assert!(read_is_due(Duration::ZERO, &mut empty));
        assert!(
            !read_is_due(Duration::ZERO, &mut empty),
            "the count restarts"
        );
    }

    proptest! {
        /// However records and gaps interleave and however many records are
        /// acknowledged, the watermark stops at or below the first record not
        /// acknowledged. Regression for #1709.
        #[test]
        fn a_commit_never_passes_an_unacknowledged_record(
            gaps in proptest::collection::vec(0i64..4, 1..24),
            trailing in 0i64..4,
            acknowledged in any::<proptest::sample::Index>(),
        ) {
            let partition = PartitionId(0);
            let mut cp = Checkpointer::new();
            let mut issuer = cp.handle();
            cp.begin_epoch(&[partition], 1);

            let mut covered = None;
            let mut records = Vec::new();
            let mut retiring = Vec::new();
            let mut next = 0;
            for gap in gaps {
                let offset = next + gap;
                if let Some(last) = offset_to_retire(Offset::Offset(offset), covered) {
                    retiring.push(issuer.issue(partition, last));
                }
                records.push((offset, issuer.issue(partition, offset)));
                covered = Some(offset + 1);
                next = offset + 1;
            }
            if let Some(last) = offset_to_retire(Offset::Offset(next + trailing), covered) {
                retiring.push(issuer.issue(partition, last));
            }
            drop(retiring);

            let split = acknowledged.index(records.len() + 1);
            let held = records.split_off(split);
            drop(records);
            cp.drain();
            let watermark = cp.take_watermarks().last().map_or(0, |(_, w)| *w);
            match held.first() {
                Some((first, _)) => prop_assert!(watermark <= *first, "{watermark} past {first}"),
                None => prop_assert_eq!(watermark, next + trailing),
            }
        }
    }
}
