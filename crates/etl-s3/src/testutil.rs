//! Test-only framer shared across the crate's unit tests.
//!
//! `etl-s3` ships no record framer of its own — production wires the format's
//! framer via [`S3Source::with_framer`](crate::S3Source::with_framer). The
//! tests still need to turn newline-delimited object bytes into records, so
//! they carry this local line framer, mirroring `etl-json`'s `NdjsonFramer`
//! semantics without taking a dependency on a format crate.

use etl_core::framing::RecordFramer;
use std::collections::VecDeque;
use std::io;

/// Newline-delimited [`RecordFramer`]: split on `\n`, strip one trailing `\r`,
/// skip whitespace-only lines, keep an unterminated final line, bounded per
/// record. Behaviourally identical to `etl-json`'s `NdjsonFramer`, so the
/// resume-by-record-index tests exercise the same record boundaries.
pub(crate) struct TestLineFramer {
    partial: Vec<u8>,
    ready: VecDeque<Vec<u8>>,
    decoded_bytes: u64,
    max_record_bytes: usize,
}

impl TestLineFramer {
    pub(crate) fn new(max_record_bytes: usize) -> TestLineFramer {
        TestLineFramer {
            partial: Vec::new(),
            ready: VecDeque::new(),
            decoded_bytes: 0,
            max_record_bytes,
        }
    }

    fn push_partial(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.partial.len() + bytes.len() > self.max_record_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record line exceeds the configured max_record_bytes ({})",
                    self.max_record_bytes
                ),
            ));
        }
        self.partial.extend_from_slice(bytes);
        Ok(())
    }

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

impl RecordFramer for TestLineFramer {
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
