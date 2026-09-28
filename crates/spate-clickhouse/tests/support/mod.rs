//! Helpers shared by this crate's integration test binaries.

use bytes::BytesMut;
use serde::Serialize;
use spate_clickhouse::serialize_row;
use spate_core::sink::SealedBatch;

/// `rows` encoded as a sealed batch of `frames` frames, the earlier frames
/// taking the remainder of an uneven split.
///
/// # Panics
///
/// Panics if `rows` is empty or a row does not encode.
pub(crate) fn sealed<T: Serialize>(rows: &[T], token: &str, frames: usize) -> SealedBatch {
    let per = rows.len().div_ceil(frames);
    let mut out = Vec::new();
    let mut bytes = 0u64;
    for chunk in rows.chunks(per) {
        let mut buf = BytesMut::new();
        for row in chunk {
            serialize_row(row, &mut buf).expect("encode");
        }
        bytes += buf.len() as u64;
        out.push(buf.freeze());
    }
    SealedBatch {
        frames: out,
        rows: rows.len() as u64,
        bytes,
        dedup_token: token.to_string(),
    }
}
