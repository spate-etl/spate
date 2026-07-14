//! Chunk-fed record framing: streaming decompression plus NDJSON line
//! splitting, on the pipeline thread.
//!
//! The framer is a **pure function of the object's byte stream**: however
//! the fetcher slices an object into chunks, the emitted record sequence is
//! identical. That determinism is load-bearing — resume positions are
//! record indexes, and a resume replays the object and discards a count
//! (see [`offset`](crate::offset)).
//!
//! Framing rules (pinned; changing any of them changes record indexes and
//! is a manifest schema break):
//!
//! - Records are split on `\n`. Exactly one trailing `\r` is stripped
//!   (CRLF input), nothing else is trimmed.
//! - Lines that are empty or all-ASCII-whitespace are skipped and do not
//!   consume a record index.
//! - An unterminated final line is a record.
//! - Decompressor and splitter state reset at every object boundary; a
//!   record never spans objects.
//! - gzip objects may contain multiple members; zstd objects multiple
//!   frames. Both are read to the end. A truncated or corrupt stream is an
//!   error at [`ObjectFramer::finish_object`] (or at the failing chunk).

use crate::config::Compression;
use flate2::write::MultiGzDecoder;
use std::collections::VecDeque;
use std::io::{self, Write};
use zstd::stream::{raw, zio};

/// Effective codec of one object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Codec {
    /// No compression.
    Plain,
    /// gzip / multi-member gzip.
    Gzip,
    /// zstd / multi-frame zstd.
    Zstd,
}

impl Codec {
    /// Resolve the codec for an object key under the configured policy.
    pub(crate) fn resolve(config: Compression, key: &str) -> Codec {
        match config {
            Compression::None => Codec::Plain,
            Compression::Gzip => Codec::Gzip,
            Compression::Zstd => Codec::Zstd,
            Compression::Auto => {
                if key.ends_with(".gz") || key.ends_with(".gzip") {
                    Codec::Gzip
                } else if key.ends_with(".zst") || key.ends_with(".zstd") {
                    Codec::Zstd
                } else {
                    Codec::Plain
                }
            }
        }
    }
}

/// `Write` target of the decompressors: splits the decoded stream into
/// record lines. The only policy knob is the record-size cap; the framing
/// rules themselves are pinned — see the module rules.
#[derive(Debug)]
struct LineSplitter {
    /// Bytes of the current, not-yet-terminated line.
    partial: Vec<u8>,
    /// Completed records, in order.
    lines: VecDeque<Vec<u8>>,
    /// Decoded bytes seen (metrics).
    decoded_bytes: u64,
    /// Upper bound on one record line (decoded bytes). Without it, an
    /// object holding no newline at all would buffer unboundedly and abort
    /// the process instead of failing the pipeline with a policy error.
    max_record_bytes: usize,
}

impl LineSplitter {
    fn new(max_record_bytes: usize) -> LineSplitter {
        LineSplitter {
            partial: Vec::new(),
            lines: VecDeque::new(),
            decoded_bytes: 0,
            max_record_bytes,
        }
    }

    /// Extend the current line, enforcing the record-size cap before the
    /// bytes are buffered.
    fn push_partial(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.partial.len() + bytes.len() > self.max_record_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record line exceeds the configured max_record_bytes ({}); \
                     is the object really newline-delimited?",
                    self.max_record_bytes
                ),
            ));
        }
        self.partial.extend_from_slice(bytes);
        Ok(())
    }

    /// Complete the current line: strip one `\r`, skip whitespace-only.
    fn complete_line(&mut self) {
        if self.partial.last() == Some(&b'\r') {
            self.partial.pop();
        }
        if self.partial.iter().all(u8::is_ascii_whitespace) {
            self.partial.clear();
            return;
        }
        self.lines.push_back(std::mem::take(&mut self.partial));
    }

    /// End of the object: an unterminated final line is a record.
    fn finish(&mut self) {
        if !self.partial.is_empty() {
            self.complete_line();
        }
    }
}

impl Write for LineSplitter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.decoded_bytes += buf.len() as u64;
        let mut rest = buf;
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            self.push_partial(&rest[..nl])?;
            self.complete_line();
            rest = &rest[nl + 1..];
        }
        self.push_partial(rest)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// One object's decode state: the codec-specific decompressor wrapping the
/// line splitter.
enum Sink {
    Plain(LineSplitter),
    Gzip(MultiGzDecoder<LineSplitter>),
    Zstd(zio::Writer<LineSplitter, raw::Decoder<'static>>),
}

impl Sink {
    fn new(codec: Codec, max_record_bytes: usize) -> io::Result<Sink> {
        Ok(match codec {
            Codec::Plain => Sink::Plain(LineSplitter::new(max_record_bytes)),
            Codec::Gzip => Sink::Gzip(MultiGzDecoder::new(LineSplitter::new(max_record_bytes))),
            Codec::Zstd => Sink::Zstd(zio::Writer::new(
                LineSplitter::new(max_record_bytes),
                raw::Decoder::new()?,
            )),
        })
    }

    fn splitter_mut(&mut self) -> &mut LineSplitter {
        match self {
            Sink::Plain(s) => s,
            Sink::Gzip(d) => d.get_mut(),
            Sink::Zstd(w) => w.writer_mut(),
        }
    }

    /// Validate end-of-stream (trailers, complete frames) and return the
    /// splitter for the final drain.
    fn finish(self) -> io::Result<LineSplitter> {
        match self {
            Sink::Plain(s) => Ok(s),
            Sink::Gzip(d) => d.finish(),
            Sink::Zstd(mut w) => {
                w.finish()?;
                Ok(w.into_inner().0)
            }
        }
    }
}

/// Chunk-fed framer for a lane: feed one object's chunks in order, pop
/// completed records, finish the object, repeat. Records queue across
/// object boundaries, so a poll batch may span objects.
pub(crate) struct ObjectFramer {
    /// Decode state of the in-progress object, if any.
    sink: Option<Sink>,
    /// Completed records not yet handed to the lane.
    ready: VecDeque<Vec<u8>>,
    /// Decoded bytes of finished objects (the in-progress object's count
    /// lives in its splitter).
    finished_decoded_bytes: u64,
    /// Record-size cap applied to every object's splitter.
    max_record_bytes: usize,
}

impl std::fmt::Debug for ObjectFramer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectFramer")
            .field("in_object", &self.sink.is_some())
            .field("ready", &self.ready.len())
            .finish()
    }
}

impl ObjectFramer {
    pub(crate) fn new(max_record_bytes: usize) -> ObjectFramer {
        ObjectFramer {
            sink: None,
            ready: VecDeque::new(),
            finished_decoded_bytes: 0,
            max_record_bytes,
        }
    }

    /// Start decoding an object. The previous object must have been
    /// finished (or none started yet).
    pub(crate) fn begin_object(&mut self, codec: Codec) -> io::Result<()> {
        debug_assert!(self.sink.is_none(), "previous object not finished");
        self.sink = Some(Sink::new(codec, self.max_record_bytes)?);
        Ok(())
    }

    /// Feed the next chunk of the in-progress object.
    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        let sink = self.sink.as_mut().expect("push_chunk outside an object");
        match sink {
            Sink::Plain(s) => s.write_all(chunk)?,
            Sink::Gzip(d) => d.write_all(chunk)?,
            Sink::Zstd(w) => w.write_all(chunk)?,
        }
        let splitter = sink.splitter_mut();
        self.ready.append(&mut splitter.lines);
        Ok(())
    }

    /// End of the object: validates the compressed stream ran to
    /// completion and emits an unterminated final line.
    pub(crate) fn finish_object(&mut self) -> io::Result<()> {
        let sink = self.sink.take().expect("finish_object outside an object");
        let mut splitter = sink.finish()?;
        splitter.finish();
        self.ready.append(&mut splitter.lines);
        self.finished_decoded_bytes += splitter.decoded_bytes;
        Ok(())
    }

    /// The next completed record, in stream order.
    pub(crate) fn pop_record(&mut self) -> Option<Vec<u8>> {
        self.ready.pop_front()
    }

    /// Completed records currently queued.
    pub(crate) fn queued(&self) -> usize {
        self.ready.len()
    }

    /// Total decoded (decompressed) bytes seen so far, including the
    /// in-progress object.
    pub(crate) fn decoded_bytes(&self) -> u64 {
        self.finished_decoded_bytes
            + self.sink.as_ref().map_or(0, |s| match s {
                Sink::Plain(sp) => sp.decoded_bytes,
                Sink::Gzip(d) => d.get_ref().decoded_bytes,
                Sink::Zstd(w) => w.writer().decoded_bytes,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression as GzLevel;
    use flate2::write::GzEncoder;
    use proptest::prelude::*;

    /// Reference implementation of the framing rules on a whole decoded
    /// object, used as the proptest oracle.
    fn reference_frames(decoded: &[u8]) -> Vec<Vec<u8>> {
        decoded
            .split(|&b| b == b'\n')
            .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
            .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
            .map(<[u8]>::to_vec)
            .collect()
    }

    /// A cap far above anything the tests feed, so only the dedicated cap
    /// tests exercise it.
    const TEST_CAP: usize = 1 << 20;

    fn frame_all(codec: Codec, chunks: &[&[u8]]) -> io::Result<Vec<Vec<u8>>> {
        let mut framer = ObjectFramer::new(TEST_CAP);
        framer.begin_object(codec)?;
        for chunk in chunks {
            framer.push_chunk(chunk)?;
        }
        framer.finish_object()?;
        let mut out = Vec::new();
        while let Some(r) = framer.pop_record() {
            out.push(r);
        }
        Ok(out)
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), GzLevel::default());
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn splits_strips_cr_and_skips_blank_lines() {
        let object = b"{\"a\":1}\r\n\n   \n{\"b\":2}\n\t\r\n{\"c\":3}";
        let records = frame_all(Codec::Plain, &[object]).unwrap();
        assert_eq!(
            records,
            vec![
                b"{\"a\":1}".to_vec(),
                b"{\"b\":2}".to_vec(),
                b"{\"c\":3}".to_vec()
            ],
            "one CR stripped, whitespace-only lines skipped, unterminated final line kept"
        );
    }

    #[test]
    fn empty_object_frames_no_records() {
        assert!(frame_all(Codec::Plain, &[b""]).unwrap().is_empty());
    }

    #[test]
    fn multi_member_gzip_is_fully_read_even_split_mid_member() {
        // Two members; a record spans the member boundary (members
        // concatenate at the byte-stream level).
        let mut object = gzip(b"first\nsecond-start");
        object.extend(gzip(b"-second-end\nthird\n"));
        // Split at every byte to prove chunking independence at the seam.
        for split in 1..object.len() {
            let records = frame_all(Codec::Gzip, &[&object[..split], &object[split..]]).unwrap();
            assert_eq!(
                records,
                vec![
                    b"first".to_vec(),
                    b"second-start-second-end".to_vec(),
                    b"third".to_vec()
                ],
                "split at {split}"
            );
        }
    }

    #[test]
    fn multi_frame_zstd_is_fully_read() {
        let mut object = zstd::encode_all(&b"alpha\nbe"[..], 3).unwrap();
        object.extend(zstd::encode_all(&b"ta\ngamma\n"[..], 3).unwrap());
        let records = frame_all(Codec::Zstd, &[&object]).unwrap();
        assert_eq!(
            records,
            vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
        );
    }

    #[test]
    fn truncated_gzip_is_an_error() {
        let object = gzip(b"only\nline\n");
        // Cut inside the 8-byte trailer (checksum mismatch) and inside the
        // deflate stream (unexpected EOF): the exact kind differs, an error
        // is required either way.
        for cut in [object.len() - 6, object.len() / 2] {
            assert!(
                frame_all(Codec::Gzip, &[&object[..cut]]).is_err(),
                "truncation at {cut} must error"
            );
        }
    }

    #[test]
    fn truncated_zstd_is_an_error() {
        let object = zstd::encode_all(&b"only\nline\n"[..], 3).unwrap();
        let cut = &object[..object.len() - 3];
        let err = frame_all(Codec::Zstd, &[cut]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{err}");
    }

    #[test]
    fn corrupt_gzip_surfaces_as_an_error() {
        let mut object = gzip(b"data\n");
        let mid = object.len() / 2;
        object[mid] ^= 0xFF;
        assert!(frame_all(Codec::Gzip, &[&object]).is_err());
    }

    #[test]
    fn codec_resolution_by_extension_and_override() {
        assert_eq!(
            Codec::resolve(Compression::Auto, "a/b.ndjson.gz"),
            Codec::Gzip
        );
        assert_eq!(Codec::resolve(Compression::Auto, "a/b.zst"), Codec::Zstd);
        assert_eq!(
            Codec::resolve(Compression::Auto, "a/b.ndjson"),
            Codec::Plain
        );
        assert_eq!(Codec::resolve(Compression::Gzip, "a/b.zst"), Codec::Gzip);
        assert_eq!(Codec::resolve(Compression::None, "a/b.gz"), Codec::Plain);
    }

    #[test]
    fn a_line_over_the_record_cap_is_an_error_not_an_allocation() {
        // Plain: the third chunk pushes the (never-terminated) line past
        // the cap; the error surfaces at the offending push, before the
        // bytes are buffered.
        let mut framer = ObjectFramer::new(8);
        framer.begin_object(Codec::Plain).unwrap();
        framer.push_chunk(b"1234").unwrap();
        framer.push_chunk(b"5678").unwrap();
        let err = framer.push_chunk(b"9").unwrap_err();
        assert!(err.to_string().contains("max_record_bytes"), "{err}");

        // Compressed: the cap applies to *decoded* bytes.
        let body = gzip(b"0123456789ABCDEF no newline anywhere");
        let mut framer = ObjectFramer::new(8);
        framer.begin_object(Codec::Gzip).unwrap();
        let result = framer
            .push_chunk(&body)
            .and_then(|()| framer.finish_object());
        assert!(
            result.unwrap_err().to_string().contains("max_record_bytes"),
            "gzip-decoded oversized line must fail"
        );

        // A line exactly at the cap is fine.
        let mut framer = ObjectFramer::new(8);
        framer.begin_object(Codec::Plain).unwrap();
        framer.push_chunk(b"12345678\n").unwrap();
        framer.finish_object().unwrap();
        assert_eq!(framer.pop_record().unwrap(), b"12345678");
    }

    #[test]
    fn records_queue_across_objects_and_bytes_are_counted() {
        let mut framer = ObjectFramer::new(TEST_CAP);
        framer.begin_object(Codec::Plain).unwrap();
        framer.push_chunk(b"one\n").unwrap();
        framer.finish_object().unwrap();
        framer.begin_object(Codec::Plain).unwrap();
        framer.push_chunk(b"two\n").unwrap();
        framer.finish_object().unwrap();
        assert_eq!(framer.queued(), 2);
        assert_eq!(framer.decoded_bytes(), 8);
        assert_eq!(framer.pop_record().unwrap(), b"one");
        assert_eq!(framer.pop_record().unwrap(), b"two");
    }

    /// Arbitrary line content: no `\n` (the separator), but everything
    /// else including `\r` and whitespace runs.
    fn arb_line() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(
            prop_oneof![
                any::<u8>().prop_filter("no newline", |b| *b != b'\n'),
                Just(b' '),
                Just(b'\t'),
                Just(b'\r'),
            ],
            0..80,
        )
    }

    fn arb_object() -> impl Strategy<Value = Vec<u8>> {
        (
            proptest::collection::vec(arb_line(), 0..40),
            any::<bool>(), // terminated by a final newline or not
        )
            .prop_map(|(lines, terminated)| {
                let mut object = lines.join(&b'\n');
                if terminated && !object.is_empty() {
                    object.push(b'\n');
                }
                object
            })
    }

    /// Random split points over the object's encoded bytes.
    fn arb_splits(len: usize) -> impl Strategy<Value = Vec<usize>> {
        proptest::collection::vec(0..=len, 0..8).prop_map(move |mut cuts| {
            cuts.sort_unstable();
            cuts.dedup();
            cuts
        })
    }

    fn chunked(bytes: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
        let mut chunks = Vec::new();
        let mut prev = 0;
        for &cut in cuts {
            chunks.push(bytes[prev..cut].to_vec());
            prev = cut;
        }
        chunks.push(bytes[prev..].to_vec());
        chunks
    }

    proptest! {
        /// The framer is a pure function of the object byte stream: any
        /// chunking of any codec's encoding yields the reference framing.
        #[test]
        fn framing_is_chunking_independent(
            object in arb_object(),
            codec_pick in 0..3usize,
            seed_cuts in proptest::collection::vec(0..10_000usize, 0..8),
        ) {
            let expected = reference_frames(&object);
            let (codec, encoded) = match codec_pick {
                0 => (Codec::Plain, object.clone()),
                1 => (Codec::Gzip, gzip(&object)),
                _ => (Codec::Zstd, zstd::encode_all(&object[..], 1).unwrap()),
            };
            let cuts: Vec<usize> = {
                let mut c: Vec<usize> =
                    seed_cuts.iter().map(|s| s % (encoded.len() + 1)).collect();
                c.sort_unstable();
                c.dedup();
                c
            };
            let chunks = chunked(&encoded, &cuts);
            let chunk_refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
            let framed = frame_all(codec, &chunk_refs).unwrap();
            prop_assert_eq!(framed, expected);
        }

        /// Explicit oracle equivalence for the splitter alone (all split
        /// points, uncompressed) — cheap enough to run at a byte level.
        #[test]
        fn plain_framing_matches_reference_at_every_split(object in arb_object()) {
            let expected = reference_frames(&object);
            for split in 0..=object.len() {
                let framed =
                    frame_all(Codec::Plain, &[&object[..split], &object[split..]]).unwrap();
                prop_assert_eq!(&framed, &expected, "split at {}", split);
            }
        }
    }

    // Keep `arb_splits` exercised (documents the intended helper for
    // future chunking tests without a modulo dance).
    proptest! {
        #[test]
        fn chunking_helper_reassembles(object in arb_object(), cuts in arb_splits(64)) {
            let cuts: Vec<usize> =
                cuts.into_iter().filter(|c| *c <= object.len()).collect();
            let reassembled: Vec<u8> =
                chunked(&object, &cuts).into_iter().flatten().collect();
            prop_assert_eq!(reassembled, object);
        }
    }
}
