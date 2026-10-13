//! Helpers shared by this crate's integration test binaries.

#![allow(dead_code, reason = "each test binary uses a different subset")]

use bytes::{Bytes, BytesMut};
use serde::Serialize;
use spate_clickhouse::{Compression, serialize_row};
use spate_core::sink::SealedBatch;

/// `rows` encoded as a sealed batch of `frames` frames, the earlier frames
/// taking the remainder of an uneven split, sent as the encoder wrote them.
///
/// # Panics
///
/// Panics if `rows` is empty or a row does not encode.
pub(crate) fn sealed<T: Serialize>(rows: &[T], token: &str, frames: usize) -> SealedBatch {
    sealed_for(Compression::None, rows, token, frames)
}

/// [`sealed`], with each frame coded by the sink's frame codec for
/// `compression`, as a pipeline thread codes each chunk at seal. One codec
/// instance codes every frame, as one chain's does.
///
/// # Panics
///
/// Panics if `rows` is empty or a row does not encode.
pub(crate) fn sealed_for<T: Serialize>(
    compression: Compression,
    rows: &[T],
    token: &str,
    frames: usize,
) -> SealedBatch {
    let per = rows.len().div_ceil(frames);
    let raw = rows
        .chunks(per)
        .map(|chunk| {
            let mut buf = BytesMut::new();
            for row in chunk {
                serialize_row(row, &mut buf).expect("encode");
            }
            buf.freeze()
        })
        .collect();
    coded_batch(compression, raw, rows.len() as u64, token)
}

/// `raw` frames holding `rows` rows as one sealed batch under `token`, each
/// frame coded by the codec for `compression`. `bytes` is the raw total, as
/// the shard worker counts it.
///
/// # Panics
///
/// Panics if a frame does not code.
pub(crate) fn coded_batch(
    compression: Compression,
    raw: Vec<Bytes>,
    rows: u64,
    token: &str,
) -> SealedBatch {
    let mut codec = spate_clickhouse::testing::frame_codec(compression).map(|f| f.make());
    let bytes = raw.iter().map(|f| f.len() as u64).sum();
    let frames = raw
        .into_iter()
        .map(|frame| match codec.as_mut() {
            Some(codec) => {
                Bytes::copy_from_slice(codec.encode_frame(&frame).expect("the frame codes"))
            }
            None => frame,
        })
        .collect();
    SealedBatch {
        frames,
        rows,
        bytes,
        dedup_token: token.to_string(),
    }
}
