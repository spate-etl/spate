//! Native insert frames for the compression and write-path benches.
//!
//! Each frame is one Native block of `events` rows from `rows.rs`, sealed by
//! the rule the sink's terminal stage applies: once the bytes written plus
//! the encoder's buffered estimate reach `ChunkConfig`'s default
//! `target_bytes`, the encoder finishes the block. The frames are therefore
//! the size and shape a production chunk has at the default chunk target, and
//! every bench that compresses or sends them prices the same bytes.
//!
//! Rows are streamed from `rows::event`, so a long batch never holds its
//! source rows at once. Frame `k` of a longer fixture is frame `k` of every
//! shorter one: the frames a counted case compresses are the head of the
//! batch a wall-clock case writes.
//!
//! `tests/bench_fixtures.rs` pins the frame count, rows per frame, the size
//! range, the lz4 ratio and a digest of the batch's bytes, so a change to the
//! corpus or to the encoder's size estimate fails there before it re-baselines
//! a bench.

#![allow(dead_code, reason = "each target uses a different subset")]

use bytes::{Bytes, BytesMut};
use spate_clickhouse::{ClickHouseRow, NativeEncoder, NativeSchema};
use spate_core::deser::Owned;
use spate_core::sink::{RowEncoder, SealedBatch};

use super::rows::{self, EventRow};

/// Frames a per-frame compression case covers.
pub(crate) const FRAMES: usize = 8;

/// Frames in the batch a write-path case sends.
pub(crate) const BATCH_FRAMES: usize = 1_600;

/// The insert column list a sink built for these frames names.
/// `tests/bench_fixtures.rs` checks it against `rows::event_columns`.
impl ClickHouseRow for EventRow {
    const COLUMNS: &'static [&'static str] = &[
        "event_id",
        "event_time",
        "user_id",
        "event_type",
        "country",
        "city",
        "url",
        "referrer",
        "status",
        "duration_ms",
        "revenue",
        "tags",
        "is_conversion",
    ];
}

/// One sealed Native block and the rows it holds.
#[derive(Clone, Debug)]
pub(crate) struct Frame {
    pub(crate) bytes: Bytes,
    pub(crate) rows: u64,
}

/// The first `count` frames of the `events` corpus.
///
/// # Panics
///
/// Panics when the events schema or a row fails to encode, which
/// `tests/bench_fixtures.rs` would report first.
pub(crate) fn native_frames(count: usize) -> Vec<Frame> {
    let schema = NativeSchema::from_columns(&rows::event_columns()).expect("event schema builds");
    let mut enc = NativeEncoder::<Owned<EventRow>>::new(schema);
    let target_bytes = spate_core::ops::ChunkConfig::default().target_bytes;
    let mut buf = BytesMut::new();
    let mut frames = Vec::with_capacity(count);
    let mut rows_in_frame = 0u64;
    let mut i = 0usize;
    while frames.len() < count {
        enc.encode(&spate_test::record(rows::event(i)), &mut buf)
            .expect("row encodes");
        i += 1;
        rows_in_frame += 1;
        if buf.len() + enc.buffered_bytes() >= target_bytes {
            enc.finish_chunk(&mut buf).expect("block finishes");
            frames.push(Frame {
                bytes: buf.split().freeze(),
                rows: rows_in_frame,
            });
            rows_in_frame = 0;
        }
    }
    frames
}

/// `frames` as one sealed batch under `token`, as a shard worker hands it to
/// the writer.
pub(crate) fn sealed(frames: &[Frame], token: &str) -> SealedBatch {
    SealedBatch {
        frames: frames.iter().map(|f| f.bytes.clone()).collect(),
        rows: frames.iter().map(|f| f.rows).sum(),
        bytes: frames.iter().map(|f| f.bytes.len() as u64).sum(),
        dedup_token: token.to_owned(),
    }
}

/// [`sealed`], with each frame coded by one instance of the sink's frame
/// codec for `compression` (`lz4`, `zstd:3`, `off`), as a pipeline thread
/// codes each chunk at seal.
///
/// # Panics
///
/// Panics on an unknown compression value or a frame that does not code.
pub(crate) fn coded(frames: &[Frame], compression: &str, token: &str) -> SealedBatch {
    let compression = compression.parse().expect("a compression value");
    let mut batch = sealed(frames, token);
    if let Some(factory) = spate_clickhouse::testing::frame_codec(compression) {
        let mut codec = factory.make();
        for frame in &mut batch.frames {
            *frame = Bytes::copy_from_slice(codec.encode_frame(frame).expect("the frame codes"));
        }
    }
    batch
}
