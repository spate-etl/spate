use super::*;
use proptest::prelude::*;

#[allow(dead_code, reason = "the codec tests use the events corpus only")]
#[path = "../../benches/support/rows.rs"]
mod rows;

#[path = "../../benches/support/frames.rs"]
mod frames;

/// `len` bytes from a xorshift stream over `alphabet` symbols: one symbol
/// compresses well, 256 not at all.
fn bytes(len: usize, seed: u64, alphabet: u16) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % u64::from(alphabet)) as u8
        })
        .collect()
}

/// The production-sized corpus frames, then a one-byte chunk and an
/// incompressible 64 KiB chunk.
fn oracle_inputs() -> Vec<Vec<u8>> {
    let mut inputs: Vec<Vec<u8>> = frames::native_frames(frames::FRAMES)
        .into_iter()
        .map(|f| f.bytes.to_vec())
        .collect();
    inputs.push(vec![0x2a]);
    inputs.push(bytes(64 * 1024, 0x9e37_79b9_7f4a_7c15, 256));
    inputs
}

fn codec(compression: Compression) -> Box<dyn FrameCodec> {
    BlockCodec::factory(compression)
        .expect("a compressing codec")
        .make()
}

/// The codec's lz4 blocks are the `clickhouse` client's, byte for byte, for
/// every input in one sequence through one instance.
#[test]
fn lz4_block_matches_the_client_crates_block() {
    let mut codec = codec(Compression::Lz4);
    for (i, raw) in oracle_inputs().iter().enumerate() {
        let ours = codec.encode_frame(raw).expect("lz4 codes").to_vec();
        let theirs = clickhouse::_priv::lz4_compress(raw).expect("the client compresses");
        assert!(ours[..] == theirs[..], "input {i} ({} bytes)", raw.len());
    }
}

/// The codec's zstd level-3 blocks are the `clickhouse` client's, byte for
/// byte, for every input in one sequence through one instance.
#[test]
fn zstd3_block_matches_the_client_crates_block() {
    // `_priv::zstd_compress` uses zstd's default level.
    assert_eq!(zstd::DEFAULT_COMPRESSION_LEVEL, 3, "zstd's default level");
    let mut codec = codec(Compression::Zstd(3));
    for (i, raw) in oracle_inputs().iter().enumerate() {
        let ours = codec.encode_frame(raw).expect("zstd codes").to_vec();
        let theirs = clickhouse::_priv::zstd_compress(raw).expect("the client compresses");
        assert!(ours[..] == theirs[..], "input {i} ({} bytes)", raw.len());
    }
}

/// Checks `block`'s framing against `raw` and decompresses its payload on
/// its own, as a server reads one block of an insert body.
fn decode(block: &[u8], raw: &[u8], compression: Compression) -> Result<(), TestCaseError> {
    prop_assert!(block.len() >= META_LEN, "a block holds its header");
    let checksum = u128::from_le_bytes(block[..CHECKSUM_LEN].try_into().expect("16 bytes"));
    prop_assert_eq!(
        checksum,
        cityhash_rs::cityhash_102_128(&block[CHECKSUM_LEN..]).rotate_right(64),
        "checksum"
    );
    let magic = if compression == Compression::Lz4 {
        LZ4_MAGIC
    } else {
        ZSTD_MAGIC
    };
    prop_assert_eq!(block[CHECKSUM_LEN], magic, "method");
    let field = |at: usize| u32::from_le_bytes(block[at..at + 4].try_into().expect("4 bytes"));
    prop_assert_eq!(
        field(CHECKSUM_LEN + 1) as usize,
        block.len() - CHECKSUM_LEN,
        "compressed size"
    );
    prop_assert_eq!(
        field(CHECKSUM_LEN + 5) as usize,
        raw.len(),
        "uncompressed size"
    );
    let payload = &block[META_LEN..];
    let decoded = if compression == Compression::Lz4 {
        lz4_flex::block::decompress(payload, raw.len()).expect("the lz4 block decodes alone")
    } else {
        zstd::bulk::decompress(payload, raw.len()).expect("the zstd frame decodes alone")
    };
    prop_assert!(decoded == raw, "the payload decodes to the chunk");
    Ok(())
}

/// A chunk length up to 200 KiB, a seed and an alphabet size: from
/// a single repeated symbol to incompressible bytes.
fn chunk() -> impl Strategy<Value = Vec<u8>> {
    (0usize..200 * 1024, any::<u64>(), 1u16..=256)
        .prop_map(|(len, seed, alphabet)| bytes(len, seed, alphabet))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// Every block in a sequence through one instance carries a valid
    /// checksum, method and sizes, and decodes back to its chunk without the
    /// blocks before it, under lz4 and zstd at levels 1, 3, 9 and 22.
    #[test]
    fn blocks_decode_back_byte_identical(chunks in prop::collection::vec(chunk(), 1..4)) {
        for compression in [
            Compression::Lz4,
            Compression::Zstd(1),
            Compression::Zstd(3),
            Compression::Zstd(9),
            Compression::Zstd(22),
        ] {
            let mut codec = codec(compression);
            for raw in &chunks {
                let block = codec.encode_frame(raw).expect("codes").to_vec();
                decode(&block, raw, compression)?;
            }
        }
    }
}

/// A zstd payload from the codec's one reused context equals a fresh
/// context's for the same chunk, for every chunk in a sequence and at levels
/// 1, 3, 9, 19 and 22.
#[test]
fn a_reused_zstd_context_matches_a_fresh_one() {
    let inputs: Vec<Vec<u8>> = [
        (42 * 1024, 4),
        (0, 1),
        (64 * 1024, 256),
        (1, 1),
        (42 * 1024, 16),
    ]
    .iter()
    .zip(1u64..)
    .map(|(&(len, alphabet), seed)| bytes(len, seed, alphabet))
    .collect();
    for level in [1, 3, 9, 19, 22] {
        let mut codec = codec(Compression::Zstd(level));
        for (i, raw) in inputs.iter().enumerate() {
            let block = codec.encode_frame(raw).expect("zstd codes").to_vec();
            let mut fresh = Vec::with_capacity(zstd_safe::compress_bound(raw.len()));
            zstd_safe::compress(&mut fresh, raw, level).expect("a fresh context compresses");
            assert!(
                block.get(META_LEN..) == Some(&fresh[..]),
                "level {level}, input {i} ({} bytes)",
                raw.len()
            );
        }
    }
}

/// One 4 MiB chunk does not pin a 4 MiB scratch: the next chunk, 40 KB,
/// leaves the scratch within twice its own need or the floor, whichever is
/// larger.
#[test]
fn the_scratch_shrinks_after_an_oversized_frame() {
    let large = bytes(4 * 1024 * 1024, 7, 256);
    let small = bytes(40_000, 11, 16);
    for (zstd_level, bound) in [
        (None, lz4_flex::block::get_maximum_output_size(small.len())),
        (Some(3), zstd_safe::compress_bound(small.len())),
    ] {
        let mut codec = BlockCodec::new(zstd_level);
        codec.encode_frame(&large).expect("the large chunk codes");
        assert!(
            codec.scratch.capacity() >= large.len(),
            "zstd level {zstd_level:?}: the large chunk grew the scratch"
        );
        codec.encode_frame(&small).expect("the small chunk codes");
        let limit = (2 * (META_LEN + bound)).max(SCRATCH_FLOOR);
        assert!(
            codec.scratch.capacity() <= limit,
            "zstd level {zstd_level:?}: scratch capacity {} over {limit}",
            codec.scratch.capacity()
        );
    }
}

/// A short chunk after a chunk at the default chunk target keeps the
/// scratch's allocation, because that scratch is under the floor.
#[test]
fn a_short_chunk_after_a_full_one_keeps_the_scratch() {
    for zstd_level in [None, Some(3)] {
        let mut codec = BlockCodec::new(zstd_level);
        codec
            .encode_frame(&bytes(64 * 1024, 3, 16))
            .expect("the full chunk codes");
        let kept = codec.scratch.as_ptr();
        codec.encode_frame(&[0x2a]).expect("the short chunk codes");
        assert_eq!(
            codec.scratch.as_ptr(),
            kept,
            "zstd level {zstd_level:?}: the scratch was replaced"
        );
    }
}

/// Above the floor, a chunk that needs more than half the scratch keeps it,
/// so chunks near a large chunk target reuse one allocation.
#[test]
fn a_slightly_shorter_chunk_above_the_floor_keeps_the_scratch() {
    for zstd_level in [None, Some(3)] {
        let mut codec = BlockCodec::new(zstd_level);
        codec
            .encode_frame(&bytes(1024 * 1024, 3, 16))
            .expect("the first chunk codes");
        let kept = codec.scratch.as_ptr();
        codec
            .encode_frame(&bytes(900 * 1024, 5, 16))
            .expect("the second chunk codes");
        assert_eq!(
            codec.scratch.as_ptr(),
            kept,
            "zstd level {zstd_level:?}: the scratch was replaced"
        );
    }
}

/// Above the floor, the scratch is replaced once it holds more than twice
/// what the next chunk needs, and kept while it holds at most twice.
#[test]
fn the_scratch_is_replaced_past_twice_the_next_need() {
    for zstd_level in [None, Some(3)] {
        let need = |len: usize| {
            META_LEN
                + match zstd_level {
                    None => lz4_flex::block::get_maximum_output_size(len),
                    Some(_) => zstd_safe::compress_bound(len),
                }
        };
        let mut probe = BlockCodec::new(zstd_level);
        probe
            .encode_frame(&bytes(1024 * 1024, 3, 16))
            .expect("the first chunk codes");
        let capacity = probe.scratch.capacity();
        // The shortest chunk whose need is at least half the scratch.
        let kept_len = (0..1024 * 1024)
            .find(|&len| 2 * need(len) >= capacity)
            .expect("a chunk under 1 MiB needs half the scratch");
        for (len, replaced) in [(kept_len, false), (kept_len - 1, true)] {
            let mut codec = BlockCodec::new(zstd_level);
            codec
                .encode_frame(&bytes(1024 * 1024, 3, 16))
                .expect("the first chunk codes");
            let kept = codec.scratch.as_ptr();
            codec
                .encode_frame(&bytes(len, 5, 16))
                .expect("the second chunk codes");
            assert_eq!(
                codec.scratch.as_ptr() != kept,
                replaced,
                "zstd level {zstd_level:?}, second chunk {len} bytes"
            );
        }
    }
}
