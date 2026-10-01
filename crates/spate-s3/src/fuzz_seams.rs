//! Entry points into code that reads bytes this process did not write: the
//! composite offset codec, the object framer, and the trim of a ranged read.
//!
//! Follows the rules [`bench_seams`](crate::bench_seams) states.

use crate::config::Compression;
use crate::framer::{Codec, ObjectFramer};
use crate::offset::{self, Position};
use crate::split::SplitRange;
use crate::trim::RangeTrim;
use bytes::Bytes;
use spate_core::framing::RecordFramer;
use std::io;
use std::sync::Arc;

/// Highest object ordinal a lane's composite offset carries.
pub const MAX_ORDINAL: u32 = offset::MAX_ORDINAL;

/// Highest record index a composite offset carries for an emitted record. A
/// watermark reaches one above it.
pub const MAX_RECORD_INDEX: u64 = offset::MAX_RECORD_INDEX;

/// Pack an object ordinal and a record index into the framework's `i64`
/// offset space. `None` when either field is out of range.
#[must_use]
pub fn encode_position(ordinal: u32, record: u64) -> Option<i64> {
    Position { ordinal, record }.encode().ok()
}

/// Unpack an offset into its object ordinal and record index.
///
/// `offset` is a position this crate encoded or a watermark one past one, so
/// it is non-negative. A debug build asserts that.
#[must_use]
pub fn decode_position(offset: i64) -> (u32, u64) {
    let position = Position::decode(offset);
    (position.ordinal, position.record)
}

/// Frame one object's chunks into records, resolving the codec from `key`
/// under `compression` and decompressing as a lane does. Returns how many
/// records the object produced and what the run reported, stopping at the
/// first error the way a lane does.
///
/// The count covers a failing run too, up to the records the framer had
/// completed when the error surfaced. A run reports whatever the decompressor
/// or the framer gave it: a truncated stream, a corrupt frame, or a record
/// over the framer's cap.
///
/// `chunks` is one object's bytes in delivery order, already compressed when
/// the resolved codec says so. Each record is dropped as it is popped, so a
/// stream that decompresses to far more than the framer holds is framed in
/// constant memory.
pub fn frame_object<M>(
    compression: Compression,
    key: &str,
    chunks: &[&[u8]],
    make_framer: M,
) -> (usize, io::Result<()>)
where
    M: Fn() -> Box<dyn RecordFramer> + Send + Sync + 'static,
{
    let mut framer = ObjectFramer::new(Arc::new(make_framer));
    let mut records = 0;
    let mut outcome = framer.begin_object(Codec::resolve(compression, key));
    for chunk in chunks {
        if outcome.is_err() {
            break;
        }
        outcome = framer.push_chunk(chunk);
        while framer.pop_record().is_some() {
            records += 1;
        }
    }
    if outcome.is_ok() {
        outcome = framer.finish_object();
        while framer.pop_record().is_some() {
            records += 1;
        }
    }
    (records, outcome)
}

/// The bytes a split reading `[start, end)` of `object` forwards, fetched in
/// the windows the fetcher schedules for `range_bytes` and `chunk_bytes`.
/// `None` when the range is empty or extends past the object.
///
/// # Panics
///
/// If the schedule asks for an empty window.
#[must_use]
pub fn read_range(
    object: &[u8],
    start: u64,
    end: u64,
    delimiter: u8,
    range_bytes: u64,
    chunk_bytes: u64,
) -> Option<Vec<u8>> {
    let size = object.len() as u64;
    if start >= end || end > size {
        return None;
    }
    let mut trim = RangeTrim::new(SplitRange::new(start, end, delimiter), size);
    let mut pos = trim.first_byte();
    let mut owned = Vec::new();
    while let Some(window_end) = trim.next_window(pos, range_bytes, chunk_bytes) {
        assert!(window_end > pos, "an empty window at byte {pos}");
        let window = Bytes::copy_from_slice(&object[pos as usize..window_end as usize]);
        owned.extend_from_slice(&trim.feed(pos, window));
        pos = window_end;
    }
    Some(owned)
}
