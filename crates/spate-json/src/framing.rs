//! Streaming NDJSON record framing.
//!
//! [`NdjsonFramer`] is the JSON connector's
//! [`RecordFramer`](spate_core::framing::RecordFramer): it cuts a byte *stream*
//! (an object streamed by `spate-s3`, an HTTP body, a file tail) into
//! newline-delimited records, so a streaming source can hand the JSON
//! deserializer one document per payload. Wire it into a streaming source at
//! assembly time:
//!
//! ```no_run
//! # use spate_json::NdjsonFramer;
//! # struct S3Source;
//! # impl S3Source {
//! #     fn with_framer<F: Fn() -> Box<dyn spate_core::framing::RecordFramer> + Send + Sync>(self, _: F) -> Self { self }
//! # }
//! # let source = S3Source;
//! let max_record_bytes = 64 << 20;
//! let source = source.with_framer(move || Box::new(NdjsonFramer::new(max_record_bytes)));
//! ```
//!
//! This is the source-side counterpart to the deserializer's in-memory
//! [`ndjson`](crate::JsonFraming::Ndjson) framing, which splits a whole payload
//! already resident in RAM (a Kafka message). `NdjsonFramer` is bounded and
//! chunk-fed instead, for byte streams that never fit in memory. A source that
//! frames one record per payload pairs with the deserializer's `single` mode
//! (the [`PerRecord`](spate_core::framing::FramingContract::PerRecord) contract).

use spate_core::framing::RecordFramer;
use std::collections::VecDeque;
use std::io;

/// Newline-delimited record framing (NDJSON / JSON Lines) as a streaming
/// [`RecordFramer`].
///
/// The framer knows nothing about JSON beyond the newline convention NDJSON
/// guarantees: a JSON document never contains an unescaped newline, so every
/// `\n` is an unambiguous record boundary.
///
/// Framing rules (pinned; changing any of them changes record indexes, which
/// is a resume-compatibility break for sources that checkpoint by index):
///
/// - Records are split on `\n`. Exactly one trailing `\r` is stripped (CRLF
///   input); nothing else is trimmed.
/// - Lines that are empty or all-ASCII-whitespace are skipped and do not
///   consume a record index.
/// - An unterminated final line (no closing `\n`) is a record.
#[derive(Debug)]
pub struct NdjsonFramer {
    /// Bytes of the current, not-yet-terminated line.
    partial: Vec<u8>,
    /// Completed records, in order.
    ready: VecDeque<Vec<u8>>,
    /// Decoded bytes seen (metrics).
    decoded_bytes: u64,
    /// Upper bound on one record line (decoded bytes). Without it, a stream
    /// holding no newline at all would buffer unboundedly and abort the
    /// process instead of failing the pipeline with a policy error.
    max_record_bytes: usize,
}

impl NdjsonFramer {
    /// A line framer bounding each record at `max_record_bytes` decoded bytes.
    #[must_use]
    pub fn new(max_record_bytes: usize) -> NdjsonFramer {
        NdjsonFramer {
            partial: Vec::new(),
            ready: VecDeque::new(),
            decoded_bytes: 0,
            max_record_bytes,
        }
    }

    /// Extend the current line, enforcing the record-size cap before the bytes
    /// are buffered.
    fn push_partial(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.partial.len() + bytes.len() > self.max_record_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record line exceeds the configured max_record_bytes ({}); \
                     is the stream really newline-delimited?",
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
        self.ready.push_back(std::mem::take(&mut self.partial));
    }
}

impl RecordFramer for NdjsonFramer {
    fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.decoded_bytes += bytes.len() as u64;
        let mut rest = bytes;
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            self.push_partial(&rest[..nl])?;
            self.complete_line();
            rest = &rest[nl + 1..];
        }
        self.push_partial(rest)
    }

    fn finish(&mut self) -> io::Result<()> {
        if !self.partial.is_empty() {
            self.complete_line();
        }
        Ok(())
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        self.ready.pop_front()
    }

    fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A cap far above anything the tests feed, so only the dedicated cap test
    /// exercises it.
    const TEST_CAP: usize = 1 << 20;

    /// Reference implementation of the line-framing rules on a whole byte
    /// stream, used as the proptest oracle.
    fn reference_frames(decoded: &[u8]) -> Vec<Vec<u8>> {
        decoded
            .split(|&b| b == b'\n')
            .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
            .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
            .map(<[u8]>::to_vec)
            .collect()
    }

    /// Feed `chunks` through a fresh `NdjsonFramer` and collect every record.
    fn frame_all(max: usize, chunks: &[&[u8]]) -> io::Result<Vec<Vec<u8>>> {
        let mut f = NdjsonFramer::new(max);
        for chunk in chunks {
            f.push(chunk)?;
        }
        f.finish()?;
        let mut out = Vec::new();
        while let Some(r) = f.pop() {
            out.push(r);
        }
        Ok(out)
    }

    #[test]
    fn splits_strips_cr_and_skips_blank_lines() {
        let stream = b"{\"a\":1}\r\n\n   \n{\"b\":2}\n\t\r\n{\"c\":3}";
        let records = frame_all(TEST_CAP, &[stream]).unwrap();
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
    fn empty_stream_frames_no_records() {
        assert!(frame_all(TEST_CAP, &[b""]).unwrap().is_empty());
    }

    #[test]
    fn decoded_bytes_counts_every_fed_byte() {
        let mut f = NdjsonFramer::new(TEST_CAP);
        f.push(b"one\n").unwrap();
        f.push(b"two").unwrap();
        f.finish().unwrap();
        assert_eq!(f.decoded_bytes(), 7);
    }

    #[test]
    fn a_line_over_the_record_cap_is_an_error_not_an_allocation() {
        // The third push carries the (never-terminated) line past the cap; the
        // error surfaces at the offending push, before the bytes are buffered.
        let mut f = NdjsonFramer::new(8);
        f.push(b"1234").unwrap();
        f.push(b"5678").unwrap();
        let err = f.push(b"9").unwrap_err();
        assert!(err.to_string().contains("max_record_bytes"), "{err}");

        // A line exactly at the cap is fine.
        let records = frame_all(8, &[b"12345678\n"]).unwrap();
        assert_eq!(records, vec![b"12345678".to_vec()]);
    }

    #[test]
    fn record_framer_is_object_safe() {
        // Compiles only if the trait is dyn-compatible (the seam's contract).
        let mut framer: Box<dyn RecordFramer> = Box::new(NdjsonFramer::new(TEST_CAP));
        framer.push(b"x\n").unwrap();
        framer.finish().unwrap();
        assert_eq!(framer.pop(), Some(b"x".to_vec()));
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

    fn arb_stream() -> impl Strategy<Value = Vec<u8>> {
        (
            proptest::collection::vec(arb_line(), 0..40),
            any::<bool>(), // terminated by a final newline or not
        )
            .prop_map(|(lines, terminated)| {
                let mut stream = lines.join(&b'\n');
                if terminated && !stream.is_empty() {
                    stream.push(b'\n');
                }
                stream
            })
    }

    proptest! {
        /// The framer is a pure function of the byte stream: any two chunkings
        /// of the same bytes yield the reference framing. This determinism is
        /// what makes resume-by-record-index safe for every streaming source
        /// that reuses `NdjsonFramer`.
        #[test]
        fn line_framing_is_chunking_independent(stream in arb_stream()) {
            let expected = reference_frames(&stream);
            for split in 0..=stream.len() {
                let framed = frame_all(TEST_CAP, &[&stream[..split], &stream[split..]]).unwrap();
                prop_assert_eq!(&framed, &expected, "split at {}", split);
            }
        }
    }
}
