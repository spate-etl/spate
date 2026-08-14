//! Streaming record framing: the seam that cuts a decoded byte stream into
//! records.
//!
//! Framing is a **transport- and format-agnostic seam**. Any streaming or pull
//! source (object-storage backfills in `spate-s3`, HTTP chunked bodies,
//! WebSocket message streams, file tails) must turn a byte stream into records
//! before a [`Deserializer`](crate::deser::Deserializer) decodes each one.
//! That "one payload → one record" split lets a single decoder serve every
//! source; the framer decides what one payload *is*.
//!
//! `spate-core` owns the seam here (the [`RecordFramer`] trait, the
//! [`FramerWriter`] decompressor shim, and the [`FramingContract`] handshake).
//! The concrete framers are owned by the **format** crates, because how a byte
//! stream splits into records is a property of the format rather than the
//! transport. `spate-json` provides newline-delimited framing for NDJSON, a
//! future `spate-avro` an object-container block framer, and so on. A source
//! stays format-agnostic and receives its framer at pipeline-assembly time
//! (e.g. `S3Source::with_framer`), so the same framer serves every streaming
//! source and no source hard-codes a format.
//!
//! A [`RecordFramer`] is fed decoded bytes in arbitrary chunks and yields
//! completed record byte-slices. It **must be a pure function of the byte
//! stream**, independent of how the bytes are chunked, so a source that
//! resumes by record index replays deterministically; at-least-once resume
//! depends on it (see the `spate-s3` offset model). Compression is *not* part
//! of framing. A source that decompresses wraps its decompressor around a
//! [`FramerWriter`], so the framer only ever sees already-decoded bytes.
//!
//! Dispatch through `Box<dyn RecordFramer>` happens once per fed chunk, never
//! per record, so each impl's per-record scan stays monomorphized.

use std::io::{self, Write};

/// What one payload emitted by a source represents, so the framework can pair
/// a source with a deserializer without the two being coordinated by hand.
///
/// - [`PerRecord`](FramingContract::PerRecord): the source already framed one
///   record per payload, having run a [`RecordFramer`] over its byte stream
///   (e.g. an `spate-s3` backfill framing each object with the format's
///   framer). The deserializer must decode a *single* unit; a deserializer
///   configured to *also* frame the payload is a double-framing error.
/// - [`WholePayload`](FramingContract::WholePayload): the source emits whole
///   payloads and the deserializer owns framing (e.g. a Kafka message, which
///   may carry one record or many). This is the default for sources that do
///   not frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FramingContract {
    /// One record per payload; the deserializer decodes a single unit.
    PerRecord,
    /// Whole payloads; the deserializer frames them.
    WholePayload,
}

/// A streaming record framer: fed decoded bytes, yields record byte-payloads.
///
/// Concrete implementations live in the format crates (`spate-json`'s NDJSON
/// framer, a future `spate-avro` container framer, ...). Implementations
/// **must be a pure function of the byte stream**. The record sequence must
/// not depend on how bytes are split across [`push`](Self::push) calls, so
/// resume-by-record-index stays deterministic. Enforce any record-size bound
/// inside `push` (return an error rather than buffer unboundedly).
pub trait RecordFramer: Send {
    /// Feed the next run of decoded bytes.
    fn push(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// End of the stream/object: complete an unterminated final record if the
    /// format allows one, and validate any end-of-stream structure.
    fn finish(&mut self) -> io::Result<()>;

    /// Pop the next completed record, in stream order.
    fn pop(&mut self) -> Option<Vec<u8>>;

    /// Total decoded bytes fed so far (for metrics).
    fn decoded_bytes(&self) -> u64;
}

/// A [`Write`] adapter over a [`RecordFramer`], so a source whose decompressor
/// (or any other `Write`-sink codec) emits decoded bytes through `Write` can
/// feed a framer without the framer implementing `Write` itself. Every
/// `write` forwards the buffer to [`RecordFramer::push`].
pub struct FramerWriter {
    framer: Box<dyn RecordFramer>,
}

impl FramerWriter {
    /// Wrap `framer` as a `Write` target.
    #[must_use]
    pub fn new(framer: Box<dyn RecordFramer>) -> FramerWriter {
        FramerWriter { framer }
    }

    /// The wrapped framer (shared), for reading `decoded_bytes`.
    #[must_use]
    pub fn framer(&self) -> &dyn RecordFramer {
        &*self.framer
    }

    /// The wrapped framer, for popping records, reading `decoded_bytes`, and
    /// `finish`.
    pub fn framer_mut(&mut self) -> &mut dyn RecordFramer {
        &mut *self.framer
    }

    /// Reclaim the wrapped framer (e.g. after a decompressor's end-of-stream
    /// validation hands the writer back).
    #[must_use]
    pub fn into_inner(self) -> Box<dyn RecordFramer> {
        self.framer
    }
}

impl std::fmt::Debug for FramerWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramerWriter").finish_non_exhaustive()
    }
}

impl Write for FramerWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.framer.push(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// A minimal in-crate [`RecordFramer`] for exercising the seam without a
    /// concrete framer; those live in the format crates. It concatenates
    /// everything pushed and emits it as one record at `finish`.
    #[derive(Default)]
    struct WholeFramer {
        buf: Vec<u8>,
        ready: VecDeque<Vec<u8>>,
        decoded: u64,
    }

    impl RecordFramer for WholeFramer {
        fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.decoded += bytes.len() as u64;
            self.buf.extend_from_slice(bytes);
            Ok(())
        }

        fn finish(&mut self) -> io::Result<()> {
            if !self.buf.is_empty() {
                self.ready.push_back(std::mem::take(&mut self.buf));
            }
            Ok(())
        }

        fn pop(&mut self) -> Option<Vec<u8>> {
            self.ready.pop_front()
        }

        fn decoded_bytes(&self) -> u64 {
            self.decoded
        }
    }

    #[test]
    fn record_framer_is_object_safe() {
        // Compiles only if the trait is dyn-compatible (the seam's contract).
        let mut framer: Box<dyn RecordFramer> = Box::new(WholeFramer::default());
        framer.push(b"x").unwrap();
        framer.finish().unwrap();
        assert_eq!(framer.pop(), Some(b"x".to_vec()));
    }

    #[test]
    fn framer_writer_forwards_writes_to_push_and_counts_bytes() {
        let mut w = FramerWriter::new(Box::new(WholeFramer::default()));
        w.write_all(b"a\nb\n").unwrap();
        assert_eq!(
            w.framer().decoded_bytes(),
            4,
            "every written byte reaches push"
        );
        w.framer_mut().finish().unwrap();
        let mut out = Vec::new();
        while let Some(r) = w.framer_mut().pop() {
            out.push(r);
        }
        assert_eq!(out, vec![b"a\nb\n".to_vec()]);
    }

    #[test]
    fn framer_writer_into_inner_reclaims_the_framer() {
        let w = FramerWriter::new(Box::new(WholeFramer::default()));
        let framer = w.into_inner();
        assert_eq!(framer.decoded_bytes(), 0);
    }
}
