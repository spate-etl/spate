//! The sink's frame codec: each sealed chunk becomes one compressed block in
//! ClickHouse's native framing, on the pipeline thread that sealed it.
//!
//! A block is laid out as:
//!
//! - 16 bytes: CityHash128 (v1.0.2) of the rest of the block, its two halves
//!   swapped, little-endian;
//! - 1 byte: the method, `0x82` for LZ4 or `0x90` for zstd;
//! - u32 LE: the size of the header and payload, which is the block's length
//!   less the checksum;
//! - u32 LE: the chunk's uncompressed length;
//! - the payload: one LZ4 block or one zstd frame, which decodes on its own.
//!
//! A server reads a run of these blocks as an insert body sent with
//! `decompress=1`. The bytes equal what the `clickhouse` client writes for
//! the same chunk, which the tests pin against the client's own codec.

use crate::Compression;
use spate_core::error::{ErrorClass, SinkError};
use spate_core::sink::{FrameCodec, FrameCodecFactory};
use zstd::zstd_safe::{self, CCtx};

#[cfg(test)]
mod tests;

const CHECKSUM_LEN: usize = 16;
const HEADER_LEN: usize = 9;
/// Bytes ahead of the payload: the checksum, then the header.
const META_LEN: usize = CHECKSUM_LEN + HEADER_LEN;
const LZ4_MAGIC: u8 = 0x82;
const ZSTD_MAGIC: u8 = 0x90;
/// A scratch at or under this capacity is never replaced, so chunks near the
/// default chunk target reuse one allocation.
const SCRATCH_FLOOR: usize = 256 * 1024;

/// One block compressor, made per chain by the factory from
/// [`BlockCodec::factory`].
pub(crate) struct BlockCodec {
    method: Method,
    /// The block being built. Bytes past the last block are stale, and its
    /// length only grows until the scratch is replaced, so a call that keeps
    /// the scratch zero-fills only what a larger chunk adds.
    scratch: Vec<u8>,
}

enum Method {
    Lz4,
    /// One context for every chunk. `CCtx::compress` resets it per call, so
    /// no chunk's output depends on an earlier one.
    Zstd {
        ctx: CCtx<'static>,
        level: i32,
    },
}

impl BlockCodec {
    /// The factory for `compression`, or `None` under `off`, where frames
    /// ship as the encoder wrote them.
    pub(crate) fn factory(compression: Compression) -> Option<FrameCodecFactory> {
        let zstd_level = match compression {
            Compression::None => return None,
            Compression::Lz4 => None,
            Compression::Zstd(level) => Some(level),
        };
        Some(FrameCodecFactory::new(move || {
            Box::new(BlockCodec::new(zstd_level))
        }))
    }

    /// An lz4 codec, or a zstd codec at `zstd_level`.
    fn new(zstd_level: Option<i32>) -> Self {
        let method = match zstd_level {
            None => Method::Lz4,
            Some(level) => Method::Zstd {
                ctx: CCtx::create(),
                level,
            },
        };
        BlockCodec {
            method,
            scratch: Vec::new(),
        }
    }
}

impl FrameCodec for BlockCodec {
    fn encode_frame(&mut self, chunk: &[u8]) -> Result<&[u8], SinkError> {
        let uncompressed = u32::try_from(chunk.len()).map_err(|_| too_long(chunk.len()))?;
        let bound = match self.method {
            Method::Lz4 => lz4_flex::block::get_maximum_output_size(chunk.len()),
            Method::Zstd { .. } => zstd_safe::compress_bound(chunk.len()),
        };
        let needed = META_LEN + bound;
        if self.scratch.capacity() > (2 * needed).max(SCRATCH_FLOOR) {
            // One oversized chunk must not pin its scratch for the life of
            // the chain.
            self.scratch = vec![0; needed];
        } else if self.scratch.len() < needed {
            self.scratch.resize(needed, 0);
        }

        let out = &mut self.scratch[META_LEN..];
        let (magic, written) = match &mut self.method {
            Method::Lz4 => {
                let n = lz4_flex::block::compress_into(chunk, out)
                    .map_err(|e| fatal(format!("lz4 could not compress a chunk: {e}")))?;
                (LZ4_MAGIC, n)
            }
            Method::Zstd { ctx, level } => {
                let n = ctx.compress(out, chunk, *level).map_err(|code| {
                    fatal(format!(
                        "zstd could not compress a chunk: {}",
                        zstd_safe::get_error_name(code)
                    ))
                })?;
                (ZSTD_MAGIC, n)
            }
        };
        let sized = u32::try_from(HEADER_LEN + written).map_err(|_| too_long(chunk.len()))?;

        let block = &mut self.scratch[..META_LEN + written];
        block[CHECKSUM_LEN] = magic;
        block[CHECKSUM_LEN + 1..CHECKSUM_LEN + 5].copy_from_slice(&sized.to_le_bytes());
        block[CHECKSUM_LEN + 5..META_LEN].copy_from_slice(&uncompressed.to_le_bytes());
        let checksum = cityhash_rs::cityhash_102_128(&block[CHECKSUM_LEN..]).rotate_right(64);
        block[..CHECKSUM_LEN].copy_from_slice(&checksum.to_le_bytes());
        Ok(block)
    }
}

fn fatal(reason: String) -> SinkError {
    SinkError::Client {
        class: ErrorClass::Fatal,
        reason,
    }
}

/// The framing records both sizes as u32, so a block cannot describe a
/// longer chunk.
fn too_long(len: usize) -> SinkError {
    fatal(format!(
        "a {len}-byte chunk is too long for a compressed block, whose sizes are 32-bit"
    ))
}
