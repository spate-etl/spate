//! Entry points into two decoders that read bytes this process did not write,
//! the composite offset codec and the object framer.
//!
//! Follows the rules [`bench_seams`](crate::bench_seams) states.

use crate::config::Compression;
use crate::framer::{Codec, ObjectFramer};
use crate::offset::{self, Position};
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
/// records the object produced.
///
/// `chunks` is one object's bytes in delivery order, already compressed when
/// the resolved codec says so. Each record is dropped as it is popped, so a
/// stream that decompresses to far more than the framer holds is framed in
/// constant memory.
///
/// # Errors
///
/// Whatever the decompressor or the framer reports: a truncated stream, a
/// corrupt frame, or a record over the framer's cap.
pub fn frame_object<M>(
    compression: Compression,
    key: &str,
    chunks: &[&[u8]],
    make_framer: M,
) -> io::Result<usize>
where
    M: Fn() -> Box<dyn RecordFramer> + Send + Sync + 'static,
{
    let mut framer = ObjectFramer::new(Arc::new(make_framer));
    let mut records = 0;
    framer.begin_object(Codec::resolve(compression, key))?;
    for chunk in chunks {
        framer.push_chunk(chunk)?;
        while framer.pop_record().is_some() {
            records += 1;
        }
    }
    framer.finish_object()?;
    while framer.pop_record().is_some() {
        records += 1;
    }
    Ok(records)
}
