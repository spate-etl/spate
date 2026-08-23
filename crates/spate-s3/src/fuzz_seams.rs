//! Entry points into the decoders that read bytes this process did not write.
//!
//! The composite offset codec decodes the `i64` watermark a coordination
//! store hands back on resume, and the object framer decodes an object's
//! bytes as the bucket serves them, gzip and zstd streams included. Both are
//! private to this crate, and `fuzz/` is a workspace of its own that reaches
//! this crate's public API and nothing else.
//!
//! The seam holds to the rules [`bench_seams`](crate::bench_seams) states:
//! behind the off-by-default `testing` feature and `#[doc(hidden)]`,
//! exporting functions and the aliases they need rather than this crate's own
//! types, and one whole unit of a stage's work per function.

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
/// stream that decompresses to far more than the framer holds costs the
/// caller nothing per record.
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
    // The tail: `finish_object` completes an unterminated final record.
    while framer.pop_record().is_some() {
        records += 1;
    }
    Ok(records)
}
