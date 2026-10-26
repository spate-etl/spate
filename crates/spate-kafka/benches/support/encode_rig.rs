//! The encode rig both the counted tier and the allocation snapshot drive:
//! one encoder, the records it encodes, and the buffer it frames them into.
//!
//! Included with `#[path]`: a bench target is its own crate, so two targets
//! measure one region only by compiling the same source. Each includer also
//! declares `mod messages` from `support/messages.rs`.

use bytes::BytesMut;
use spate_core::checkpoint::AckRef;
use spate_core::deser::Owned;
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::RowEncoder;
use spate_kafka::sink::{KafkaBytesEncoder, KafkaEncoder, KafkaJsonEncoder, MessageEncoder};
use std::hint::black_box;

use crate::messages::{self, Event, HeaderStamp, RECORDS};

/// One encoder with the records it encodes and a buffer sized to hold the
/// result, so the measurement is the encode path's work rather than the
/// allocator growing a `BytesMut` under it.
pub(crate) struct Rig<T: Send + 'static, M> {
    enc: KafkaEncoder<Owned<T>, M>,
    records: Vec<Record<T>>,
    pub(crate) buf: BytesMut,
}

/// The measured work: a run of records through one encoder, returning how many
/// were accepted.
///
/// Removing `#[inline(never)]` fails nothing and silently empties the
/// measurement. Callgrind toggles
/// collection on the benchmark function, and `KafkaEncoder::encode` is generic
/// over both the record family and the message encoder, so it monomorphises
/// into *this* crate and is an ordinary inlining candidate. Without a frame
/// the optimizer may not erase, the loop is reshaped across the toggle
/// boundary and the region no longer contains the work: dropping the attribute
/// takes `bytes_keyless` from 3,500,338 instructions to **22**, and three of
/// the other four cases with it, while every one of them still runs and still
/// reports a number.
///
/// Counting the accepted records is what keeps the loop alive: every case's
/// output is otherwise unobserved, and without a use the optimizer is free to
/// delete the call being counted. The caller asserts the count, so a
/// case that silently stopped encoding, or started failing, cannot pass as a
/// fast one.
#[inline(never)]
pub(crate) fn encode_run<T, M>(rig: &mut Rig<T, M>) -> usize
where
    T: Send + 'static,
    M: MessageEncoder<Owned<T>>,
{
    rig.buf.clear();
    let mut accepted = 0usize;
    for rec in &rig.records {
        if rig.enc.encode(black_box(rec), &mut rig.buf).is_ok() {
            accepted += 1;
        }
    }
    accepted
}

/// Records sharing one ack.
///
/// One `AckRef` cloned across the corpus rather than one per record: at this
/// record count a pair each would be ten thousand live channels built in
/// setup. Nothing on this path touches an acknowledgment, since the encoder
/// never resolves one, so the only traffic is the single message the last handle
/// sends as it drops, and leaking the receiver keeps the channel connected so
/// that send is delivered rather than discarded. Either way it costs the
/// measurement nothing: the rig outlives the measured closure, so the drop
/// happens at teardown, outside the counted region.
fn records<T>(payloads: Vec<T>) -> Vec<Record<T>> {
    let (ack, rx) = AckRef::test_pair();
    std::mem::forget(rx);
    payloads
        .into_iter()
        .enumerate()
        .map(|(i, payload)| Record {
            payload,
            meta: RecordMeta {
                partition: PartitionId(0),
                offset: i as i64,
                event_time_ms: 0,
                key_hash: None,
            },
            ack: ack.clone(),
        })
        .collect()
}

/// A rig whose buffer already holds the run, so no case measures a `BytesMut`
/// doubling its way up to the frame size.
fn rig<T: Send + 'static, M>(enc: KafkaEncoder<Owned<T>, M>, payloads: Vec<T>) -> Rig<T, M> {
    Rig {
        enc,
        records: records(payloads),
        buf: BytesMut::with_capacity(16 << 20),
    }
}

pub(crate) fn bytes_keyless() -> Rig<Vec<u8>, KafkaBytesEncoder> {
    rig(
        KafkaEncoder::new(KafkaBytesEncoder::new()),
        messages::payloads(RECORDS),
    )
}

pub(crate) fn bytes_keyed() -> Rig<Vec<u8>, KafkaBytesEncoder> {
    rig(
        KafkaEncoder::new(KafkaBytesEncoder::with_key_fn(messages::key_prefix)),
        messages::payloads(RECORDS),
    )
}

pub(crate) fn stamped_headers() -> Rig<Vec<u8>, HeaderStamp> {
    rig(KafkaEncoder::new(HeaderStamp), messages::payloads(RECORDS))
}

/// The same stamped messages under a limit the headers push them past.
pub(crate) fn stamped_over_limit() -> Rig<Vec<u8>, HeaderStamp> {
    rig(
        KafkaEncoder::with_max_message_bytes(HeaderStamp, messages::GUARD_LIMIT),
        messages::payloads(RECORDS),
    )
}

pub(crate) fn json_typed() -> Rig<Event, KafkaJsonEncoder<Owned<Event>>> {
    rig(
        KafkaEncoder::new(KafkaJsonEncoder::<Owned<Event>>::new()),
        messages::events(RECORDS),
    )
}
