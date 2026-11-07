//! The batch both the counted tier and the allocation snapshot prepare: one
//! frame of encoded messages, as a shard worker receives it.
//!
//! Included with `#[path]`. Each includer also declares `mod encode_rig` from
//! `support/encode_rig.rs` and `mod messages` from `support/messages.rs`, since
//! the frame is written by the same encoder the encode benches measure.

use bytes::Bytes;
use spate_core::deser::Owned;
use spate_kafka::bench_seams::prepare_batch;
use spate_kafka::sink::MessageEncoder;

use crate::encode_rig::{self, Rig, encode_run};
use crate::messages::RECORDS;

/// A sealed batch's frames and its row count, with the message and header
/// counts its preparation must report.
pub(crate) struct Batch {
    pub(crate) frames: Vec<Bytes>,
    pub(crate) rows: u64,
    pub(crate) expected: (usize, usize),
}

/// The measured work: one batch through the writer's preparation, returning
/// how many messages and headers it produced.
///
/// `#[inline(never)]` keeps the work inside the collected region, for the
/// reason `encode_rig::encode_run` states. The caller asserts both counts, so
/// a case that stopped parsing or stopped building headers fails instead of
/// reporting a smaller number.
#[inline(never)]
pub(crate) fn prepare_run(batch: &Batch) -> (usize, usize) {
    prepare_batch(&batch.frames, batch.rows).expect("the batch prepares")
}

fn encoded<T, M>(mut rig: Rig<T, M>, headers_per_message: usize) -> Batch
where
    T: Send + 'static,
    M: MessageEncoder<Owned<T>>,
{
    assert_eq!(encode_run(&mut rig), RECORDS, "a record failed to encode");
    Batch {
        frames: vec![rig.buf.split().freeze()],
        rows: RECORDS as u64,
        expected: (RECORDS, RECORDS * headers_per_message),
    }
}

/// No key, no headers.
pub(crate) fn keyless() -> Batch {
    encoded(encode_rig::bytes_keyless(), 0)
}

/// A key, no headers.
pub(crate) fn keyed() -> Batch {
    encoded(encode_rig::bytes_keyed(), 0)
}

/// The four headers `HeaderStamp` adds to every message.
pub(crate) fn stamped() -> Batch {
    encoded(encode_rig::stamped_headers(), 4)
}
