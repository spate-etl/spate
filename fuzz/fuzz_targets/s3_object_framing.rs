//! Object framing over adversarial compressed bytes.
//!
//! A lane resolves an object's codec from its key and feeds the object's
//! chunks through the matching decompressor into a record framer. The bytes
//! are the bucket's, so the gzip and zstd decoders read input this process
//! did not produce: a truncated stream, a corrupt frame, or a key whose
//! extension names a codec the bytes are not in.
//!
//! The framer is a pure function of the object's byte stream, so the target
//! frames one stream twice, once as a single chunk and once split at
//! fuzzer-chosen offsets, and asserts the two runs agree on the outcome and
//! on the decoded bytes they delivered.
//!
//! The object is either the fuzzer's bytes as they are, which is what reaches
//! a decompressor with a malformed stream, or those bytes compressed here,
//! which is what reaches it with a well-formed one. The compression is chosen
//! independently of the policy and the key, so a well-formed stream in one
//! codec also reaches the decoder for the other.
//!
//! The framer digests the decoded bytes and emits no record, so an object
//! that decompresses to hundreds of megabytes is framed in constant memory.

#![no_main]

use arbitrary::Arbitrary;
use flate2::Compression as GzLevel;
use flate2::write::GzEncoder;
use libfuzzer_sys::fuzz_target;
use spate_core::framing::RecordFramer;
use spate_s3::Compression;
use spate_s3::fuzz_seams::frame_object;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Arbitrary, Debug)]
struct Input {
    /// The object's key. Under `Compression::Auto` its extension picks the
    /// codec.
    key: String,
    /// Index into the four compression policies.
    policy: u8,
    /// Index into the three encodings the object's bytes are supplied in.
    encoding: u8,
    object: Vec<u8>,
    /// Offsets into the encoded object, taken modulo its length plus one.
    cuts: Vec<u16>,
}

/// A record framer that digests the decoded bytes and emits no record.
/// Nothing accumulates between the decompressor and the target, whatever the
/// object decompresses to.
struct Digest {
    /// FNV-1a over the decoded stream, shared with the target because the
    /// framer is built inside the framing run.
    hash: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl RecordFramer for Digest {
    fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
        // FNV-1a: order-sensitive, so two runs agree only if the decoder
        // delivered the same bytes in the same order.
        let mut hash = self.hash.load(Ordering::Relaxed);
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        self.hash.store(hash, Ordering::Relaxed);
        self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        None
    }

    fn decoded_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Frame `chunks` as one object and report whether it framed, the digest of
/// the bytes the decompressor delivered, and how many there were.
fn frame(policy: Compression, key: &str, chunks: &[&[u8]]) -> (bool, u64, u64) {
    let hash = Arc::new(AtomicU64::new(0xcbf2_9ce4_8422_2325));
    let bytes = Arc::new(AtomicU64::new(0));
    let (hash_out, bytes_out) = (Arc::clone(&hash), Arc::clone(&bytes));
    let framed = frame_object(policy, key, chunks, move || {
        Box::new(Digest {
            hash: Arc::clone(&hash),
            bytes: Arc::clone(&bytes),
        })
    })
    .is_ok();
    (
        framed,
        hash_out.load(Ordering::Relaxed),
        bytes_out.load(Ordering::Relaxed),
    )
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), GzLevel::default());
    encoder.write_all(bytes).expect("a gzip encoder over a Vec");
    encoder.finish().expect("a gzip encoder over a Vec")
}

fuzz_target!(|input: Input| {
    let policy = match input.policy % 4 {
        0 => Compression::None,
        1 => Compression::Gzip,
        2 => Compression::Zstd,
        _ => Compression::Auto,
    };
    let encoded = match input.encoding % 3 {
        1 => gzip(&input.object),
        2 => zstd::encode_all(input.object.as_slice(), 1).expect("a zstd encoder over a slice"),
        _ => input.object,
    };

    let mut cuts: Vec<usize> = input
        .cuts
        .iter()
        .map(|cut| usize::from(*cut) % (encoded.len() + 1))
        .collect();
    cuts.sort_unstable();

    let mut chunks: Vec<&[u8]> = Vec::with_capacity(cuts.len() + 1);
    let mut start = 0;
    for cut in cuts {
        chunks.push(&encoded[start..cut]);
        start = cut;
    }
    chunks.push(&encoded[start..]);

    let whole = frame(policy, &input.key, &[&encoded]);
    let split = frame(policy, &input.key, &chunks);
    assert_eq!(
        whole.0,
        split.0,
        "framing {} bytes under {policy:?} at key {:?} succeeded under one chunking and \
         failed under the other",
        encoded.len(),
        input.key
    );
    if whole.0 {
        assert_eq!(
            (whole.1, whole.2),
            (split.1, split.2),
            "framing {} bytes under {policy:?} at key {:?} decoded differently under two \
             chunkings",
            encoded.len(),
            input.key
        );
    }
});
