//! Chunk-fed record framing: streaming decompression plus a pluggable record
//! framer, on the pipeline thread.
//!
//! The framer is a **pure function of the object's byte stream**: however the
//! fetcher slices an object into chunks, the emitted record sequence is
//! identical. Resume positions are record indexes, and a resume replays the
//! object and discards a count (see [`offset`](crate::offset)), so that
//! determinism is what makes a resume land on the same record.
//!
//! The record-boundary logic itself lives behind
//! [`spate_core::framing::RecordFramer`], whose concrete impl is supplied by the
//! chosen format (e.g. `spate-json`'s `NdjsonFramer`) via
//! [`S3Source::with_framer`](crate::S3Source::with_framer); `spate-s3` owns no
//! framing of its own. This module owns only the S3-specific parts:
//! compression codec resolution by object-key extension, streaming
//! decompression, and driving the supplied framer across object boundaries.
//!
//! - Decompressor and framer state reset at every object boundary; a record
//!   never spans objects.
//! - gzip objects may contain multiple members; zstd objects multiple frames.
//!   Both are read to the end. A truncated or corrupt stream is an error at
//!   [`ObjectFramer::finish_object`] (or at the failing chunk).

use crate::config::Compression;
use flate2::write::MultiGzDecoder;
use spate_core::framing::{FramerWriter, RecordFramer};
use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::Arc;
use zstd::stream::{raw, zio};

/// Builds a fresh [`RecordFramer`] for one object. Shared across a source's
/// lanes and called once per object: framers are per-object stateful and each
/// lane frames its own slice, so a single instance cannot be reused.
pub(crate) type FramerFactory = Arc<dyn Fn() -> Box<dyn RecordFramer> + Send + Sync>;

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

/// One object's decode state: the codec-specific decompressor wrapping the
/// framer's [`Write`] shim.
enum Sink {
    Plain(FramerWriter),
    Gzip(MultiGzDecoder<FramerWriter>),
    Zstd(zio::Writer<FramerWriter, raw::Decoder<'static>>),
}

impl Sink {
    fn new(codec: Codec, framer: Box<dyn RecordFramer>) -> io::Result<Sink> {
        let writer = FramerWriter::new(framer);
        Ok(match codec {
            Codec::Plain => Sink::Plain(writer),
            Codec::Gzip => Sink::Gzip(MultiGzDecoder::new(writer)),
            Codec::Zstd => Sink::Zstd(zio::Writer::new(writer, raw::Decoder::new()?)),
        })
    }

    fn framer(&self) -> &dyn RecordFramer {
        match self {
            Sink::Plain(w) => w.framer(),
            Sink::Gzip(d) => d.get_ref().framer(),
            Sink::Zstd(w) => w.writer().framer(),
        }
    }

    fn framer_mut(&mut self) -> &mut dyn RecordFramer {
        match self {
            Sink::Plain(w) => w.framer_mut(),
            Sink::Gzip(d) => d.get_mut().framer_mut(),
            Sink::Zstd(w) => w.writer_mut().framer_mut(),
        }
    }

    /// Flush the codec's tail into the framer and validate end-of-stream
    /// (trailers, complete frames).
    ///
    /// The sink is left intact, so [`framer_mut`](Self::framer_mut) reaches
    /// the framer afterwards. The flush can complete a record and then fail
    /// the validation, and that record belongs to the object's sequence.
    fn try_finish(&mut self) -> io::Result<()> {
        match self {
            Sink::Plain(_) => Ok(()),
            // `try_finish` runs the same flush and CRC check as `finish`.
            Sink::Gzip(d) => d.try_finish(),
            Sink::Zstd(w) => w.finish(),
        }
    }
}

/// Chunk-fed framer for a lane: feed one object's chunks in order, pop
/// completed records, finish the object, repeat. Records queue across object
/// boundaries, so a poll batch may span objects.
pub(crate) struct ObjectFramer {
    /// Decode state of the in-progress object, if any.
    sink: Option<Sink>,
    /// Completed records not yet handed to the lane.
    ready: VecDeque<Vec<u8>>,
    /// Decoded bytes of finished objects (the in-progress object's count lives
    /// in its framer).
    finished_decoded_bytes: u64,
    /// Builds the per-object framer, supplied by the format via
    /// [`S3Source::with_framer`](crate::S3Source::with_framer).
    make_framer: FramerFactory,
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
    pub(crate) fn new(make_framer: FramerFactory) -> ObjectFramer {
        ObjectFramer {
            sink: None,
            ready: VecDeque::new(),
            finished_decoded_bytes: 0,
            make_framer,
        }
    }

    /// Start decoding an object. The previous object must have been finished
    /// (or none started yet).
    pub(crate) fn begin_object(&mut self, codec: Codec) -> io::Result<()> {
        debug_assert!(self.sink.is_none(), "previous object not finished");
        let framer = (self.make_framer)();
        self.sink = Some(Sink::new(codec, framer)?);
        Ok(())
    }

    /// Feed the next chunk of the in-progress object.
    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        let sink = self.sink.as_mut().expect("push_chunk outside an object");
        let written = match sink {
            Sink::Plain(w) => w.write_all(chunk),
            Sink::Gzip(d) => d.write_all(chunk),
            Sink::Zstd(w) => w.write_all(chunk),
        };
        // A framer queues each record as it completes it, so a chunk that
        // fails part-way through can leave completed records behind. The
        // drain runs before the write result is propagated. Moving it under a
        // `?` makes the emitted sequence depend on where the fetcher cut the
        // object.
        while let Some(record) = sink.framer_mut().pop() {
            self.ready.push_back(record);
        }
        written
    }

    /// End of the object: validates the compressed stream ran to completion
    /// and emits an unterminated final record. Records the flush completes
    /// are emitted whether or not the validation fails.
    pub(crate) fn finish_object(&mut self) -> io::Result<()> {
        let mut sink = self.sink.take().expect("finish_object outside an object");
        let finished = sink.try_finish().and_then(|()| sink.framer_mut().finish());
        // The codec's tail flush reaches the framer in the two calls above,
        // so an object's last records are completed there. The drain runs
        // before the outcome is propagated. Moving it under a `?` drops those
        // records whenever the flush or the framer reports an error.
        while let Some(record) = sink.framer_mut().pop() {
            self.ready.push_back(record);
        }
        self.finished_decoded_bytes += sink.framer().decoded_bytes();
        finished
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
        self.finished_decoded_bytes + self.sink.as_ref().map_or(0, |s| s.framer().decoded_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TestLineFramer;
    use flate2::Compression as GzLevel;
    use flate2::write::GzEncoder;
    use proptest::prelude::*;

    /// Reference implementation of the framing rules on a whole decoded
    /// object, used as the proptest oracle. (The pure-framer determinism is
    /// owned by `spate_core::framing`; here it anchors the codec integration.)
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

    /// A newline framer factory (the tests' stand-in for a format's framer),
    /// bounding each line at `cap`.
    fn line_factory(cap: usize) -> FramerFactory {
        Arc::new(move || Box::new(TestLineFramer::new(cap)))
    }

    fn frame_all(codec: Codec, chunks: &[&[u8]]) -> io::Result<Vec<Vec<u8>>> {
        let mut framer = ObjectFramer::new(line_factory(TEST_CAP));
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

    /// Drive the framer over `chunks` under a `cap`-byte record cap, stopping
    /// at the first error the way a lane does. Returns the records emitted and
    /// whether framing failed.
    fn frame_to_first_error(cap: usize, codec: Codec, chunks: &[&[u8]]) -> (Vec<Vec<u8>>, bool) {
        let mut framer = ObjectFramer::new(line_factory(cap));
        let mut records = Vec::new();
        let mut failed = framer.begin_object(codec).is_err();
        for chunk in chunks {
            if failed {
                break;
            }
            failed = framer.push_chunk(chunk).is_err();
            while let Some(record) = framer.pop_record() {
                records.push(record);
            }
        }
        if !failed {
            failed = framer.finish_object().is_err();
            while let Some(record) = framer.pop_record() {
                records.push(record);
            }
        }
        (records, failed)
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), GzLevel::default());
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap()
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
        // Plain: the third chunk pushes the (never-terminated) line past the
        // cap; the error surfaces at the offending push, before the bytes are
        // buffered.
        let mut framer = ObjectFramer::new(line_factory(8));
        framer.begin_object(Codec::Plain).unwrap();
        framer.push_chunk(b"1234").unwrap();
        framer.push_chunk(b"5678").unwrap();
        let err = framer.push_chunk(b"9").unwrap_err();
        assert!(err.to_string().contains("max_record_bytes"), "{err}");

        // Compressed: the cap applies to *decoded* bytes.
        let body = gzip(b"0123456789ABCDEF no newline anywhere");
        let mut framer = ObjectFramer::new(line_factory(8));
        framer.begin_object(Codec::Gzip).unwrap();
        let result = framer
            .push_chunk(&body)
            .and_then(|()| framer.finish_object());
        assert!(
            result.unwrap_err().to_string().contains("max_record_bytes"),
            "gzip-decoded oversized line must fail"
        );

        // A line exactly at the cap is fine.
        let mut framer = ObjectFramer::new(line_factory(8));
        framer.begin_object(Codec::Plain).unwrap();
        framer.push_chunk(b"12345678\n").unwrap();
        framer.finish_object().unwrap();
        assert_eq!(framer.pop_record().unwrap(), b"12345678");
    }

    #[test]
    fn framing_is_chunking_independent_on_the_error_path() {
        // The second line is over the cap. Framed whole, the failure lands
        // in the chunk that completed the first line.
        let object = b"a\nbb";
        let whole = frame_to_first_error(1, Codec::Plain, &[object]);
        let split = frame_to_first_error(1, Codec::Plain, &[&object[..2], &object[2..]]);
        assert_eq!(whole, (vec![b"a".to_vec()], true));
        assert_eq!(split, whole);
    }

    #[test]
    fn a_failing_gzip_object_frames_the_same_under_both_chunkings() {
        // A compressed codec hands the framer an object's last records at the
        // tail flush, so the cap error surfaces from `finish_object` rather
        // than from a chunk.
        let object = gzip(b"r1\nr2\nBBBB\n");
        let mid = object.len() / 2;
        let whole = frame_to_first_error(3, Codec::Gzip, &[&object]);
        let split = frame_to_first_error(3, Codec::Gzip, &[&object[..mid], &object[mid..]]);
        assert_eq!(whole, (vec![b"r1".to_vec(), b"r2".to_vec()], true));
        assert_eq!(split, whole);
    }

    #[test]
    fn records_queue_across_objects_and_bytes_are_counted() {
        let mut framer = ObjectFramer::new(line_factory(TEST_CAP));
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

    /// Arbitrary line content: no `\n` (the separator), but everything else
    /// including `\r` and whitespace runs.
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

    /// The object encoded under one of the three codecs, picked by index.
    fn encode(codec_pick: usize, object: &[u8]) -> (Codec, Vec<u8>) {
        match codec_pick {
            0 => (Codec::Plain, object.to_vec()),
            1 => (Codec::Gzip, gzip(object)),
            _ => (Codec::Zstd, zstd::encode_all(object, 1).unwrap()),
        }
    }

    /// Seed values folded into sorted, deduplicated offsets into `bytes`.
    fn cuts(seeds: &[usize], bytes: &[u8]) -> Vec<usize> {
        let mut cuts: Vec<usize> = seeds.iter().map(|s| s % (bytes.len() + 1)).collect();
        cuts.sort_unstable();
        cuts.dedup();
        cuts
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
        /// The codec + framer integration is a pure function of the object
        /// byte stream: any chunking of any codec's encoding yields the
        /// reference framing (the shared framer's own determinism is covered
        /// in `spate_core::framing`).
        #[test]
        fn framing_is_chunking_independent(
            object in arb_object(),
            codec_pick in 0..3usize,
            seed_cuts in proptest::collection::vec(0..10_000usize, 0..8),
        ) {
            let expected = reference_frames(&object);
            let (codec, encoded) = encode(codec_pick, &object);
            let chunks = chunked(&encoded, &cuts(&seed_cuts, &encoded));
            let chunk_refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
            let framed = frame_all(codec, &chunk_refs).unwrap();
            prop_assert_eq!(framed, expected);
        }

        /// The property holds under the framer's record cap, where a run can
        /// fail part-way through. The records emitted before the error match
        /// the ones the whole object emits. The cap is small enough that most
        /// objects reach it, and the compressed codecs re-chunk what the
        /// framer sees.
        #[test]
        fn framing_under_a_record_cap_is_chunking_independent(
            object in arb_object(),
            codec_pick in 0..3usize,
            cap in 1..16usize,
            seed_cuts in proptest::collection::vec(0..10_000usize, 0..8),
        ) {
            let (codec, encoded) = encode(codec_pick, &object);
            let chunks = chunked(&encoded, &cuts(&seed_cuts, &encoded));
            let chunk_refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
            prop_assert_eq!(
                frame_to_first_error(cap, codec, &chunk_refs),
                frame_to_first_error(cap, codec, &[&encoded]),
            );
        }
    }
}
