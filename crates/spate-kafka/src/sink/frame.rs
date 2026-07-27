//! Connector-internal message framing between the sink's encoder and
//! writer.
//!
//! The framework's sink seam ships opaque byte frames
//! ([`SealedBatch::frames`](spate_core::sink::SealedBatch)) from pipeline
//! threads to shard workers; the unit of a Kafka produce, however, is a
//! *message* — key, headers, and payload. This module bridges the two: the
//! encoder appends each record as one length-delimited message entry, and
//! the writer parses the entries back out. The format never leaves this
//! crate, so it can change freely.
//!
//! Per message (integers little-endian `u32` unless noted):
//!
//! ```text
//! flags: u8                     bit0 = has_key, bit1 = tombstone
//! [key_len, key]                present when bit0 is set
//! header_count                  then per header:
//!   [name_len, name (UTF-8), value_len, value]
//! [payload_len, payload]        absent when bit1 (tombstone) is set
//! ```
//!
//! Messages are appended back-to-back, so frames concatenate — the
//! contract [`EncodedChunk`](spate_core::sink::EncodedChunk) requires for
//! workers to merge chunks without re-encoding. The unused flag bits are
//! reserved for additive extensions (a version bump would set a new bit).

use bytes::{BufMut, BytesMut};

const FLAG_HAS_KEY: u8 = 0b0000_0001;
const FLAG_TOMBSTONE: u8 = 0b0000_0010;
const KNOWN_FLAGS: u8 = FLAG_HAS_KEY | FLAG_TOMBSTONE;

/// A framing failure. On the write side only oversized sections trip it;
/// on the parse side any structural mismatch does. Both indicate a bug in
/// this crate (the encoder writes what the parser reads), never bad user
/// data — the writer surfaces parse failures as fatal.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FrameError {
    /// A section exceeds the format's `u32` length limit.
    TooLong {
        /// Which section (`"key"`, `"header name"`, ...).
        what: &'static str,
        /// Its actual length.
        len: usize,
    },
    /// The frame bytes do not parse.
    Corrupt {
        /// Byte offset of the failed read.
        offset: usize,
        /// What went wrong there.
        reason: &'static str,
    },
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::TooLong { what, len } => {
                write!(
                    f,
                    "{what} of {len} bytes exceeds the frame format's u32 limit"
                )
            }
            FrameError::Corrupt { offset, reason } => {
                write!(f, "corrupt frame at byte {offset}: {reason}")
            }
        }
    }
}

fn put_section(buf: &mut BytesMut, what: &'static str, bytes: &[u8]) -> Result<(), FrameError> {
    let len = u32::try_from(bytes.len()).map_err(|_| FrameError::TooLong {
        what,
        len: bytes.len(),
    })?;
    buf.put_u32_le(len);
    buf.put_slice(bytes);
    Ok(())
}

/// Append one message entry to `buf`. `payload: None` encodes a Kafka
/// tombstone (null payload), distinct from `Some(&[])`.
pub(crate) fn write_message<'a, H>(
    buf: &mut BytesMut,
    key: Option<&[u8]>,
    headers: H,
    payload: Option<&[u8]>,
) -> Result<(), FrameError>
where
    H: ExactSizeIterator<Item = (&'a str, &'a [u8])>,
{
    let mut flags = 0u8;
    if key.is_some() {
        flags |= FLAG_HAS_KEY;
    }
    if payload.is_none() {
        flags |= FLAG_TOMBSTONE;
    }
    buf.put_u8(flags);
    if let Some(key) = key {
        put_section(buf, "key", key)?;
    }
    let count = u32::try_from(headers.len()).map_err(|_| FrameError::TooLong {
        what: "header count",
        len: headers.len(),
    })?;
    buf.put_u32_le(count);
    for (name, value) in headers {
        put_section(buf, "header name", name.as_bytes())?;
        put_section(buf, "header value", value)?;
    }
    if let Some(payload) = payload {
        put_section(buf, "payload", payload)?;
    }
    Ok(())
}

/// One parsed message, borrowing the frame.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MessageRef<'a> {
    /// Message key, when the record set one.
    pub(crate) key: Option<&'a [u8]>,
    /// Message headers in insertion order.
    pub(crate) headers: Vec<(&'a str, &'a [u8])>,
    /// Message payload; `None` is a tombstone.
    pub(crate) payload: Option<&'a [u8]>,
}

/// Streaming parser over one frame (or any concatenation of frames).
/// Yields each message in order; a structural failure yields one `Err` and
/// ends iteration.
#[derive(Debug)]
pub(crate) struct FrameParser<'a> {
    buf: &'a [u8],
    offset: usize,
    poisoned: bool,
}

impl<'a> FrameParser<'a> {
    pub(crate) fn new(frame: &'a [u8]) -> Self {
        FrameParser {
            buf: frame,
            offset: 0,
            poisoned: false,
        }
    }

    fn take(&mut self, n: usize, reason: &'static str) -> Result<&'a [u8], FrameError> {
        let start = self.offset;
        let end = start.checked_add(n).filter(|&e| e <= self.buf.len());
        match end {
            Some(end) => {
                self.offset = end;
                Ok(&self.buf[start..end])
            }
            None => Err(FrameError::Corrupt {
                offset: start,
                reason,
            }),
        }
    }

    fn take_u32(&mut self, reason: &'static str) -> Result<usize, FrameError> {
        let bytes = self.take(4, reason)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("4 bytes")) as usize)
    }

    fn take_section(&mut self, reason: &'static str) -> Result<&'a [u8], FrameError> {
        let len = self.take_u32(reason)?;
        self.take(len, reason)
    }

    fn parse_one(&mut self) -> Result<MessageRef<'a>, FrameError> {
        let flag_offset = self.offset;
        let flags = self.take(1, "truncated flags byte")?[0];
        if flags & !KNOWN_FLAGS != 0 {
            return Err(FrameError::Corrupt {
                offset: flag_offset,
                reason: "unknown flag bits set",
            });
        }
        let key = if flags & FLAG_HAS_KEY != 0 {
            Some(self.take_section("truncated key")?)
        } else {
            None
        };
        let count = self.take_u32("truncated header count")?;
        // Each header occupies at least 8 bytes (two `u32` length prefixes,
        // even when both name and value are empty), so a count claiming more
        // headers than the remaining bytes could possibly hold is corrupt. Cap
        // the pre-allocation at that bound: a bogus count near `u32::MAX` then
        // errors on the read below instead of triggering a multi-gigabyte
        // speculative allocation (which would abort rather than yield a
        // `FrameError`).
        let remaining = self.buf.len() - self.offset;
        let mut headers = Vec::with_capacity(count.min(remaining / 8));
        for _ in 0..count {
            let name_offset = self.offset;
            let name = self.take_section("truncated header name")?;
            let name = std::str::from_utf8(name).map_err(|_| FrameError::Corrupt {
                offset: name_offset,
                reason: "header name is not UTF-8",
            })?;
            let value = self.take_section("truncated header value")?;
            headers.push((name, value));
        }
        let payload = if flags & FLAG_TOMBSTONE != 0 {
            None
        } else {
            Some(self.take_section("truncated payload")?)
        };
        Ok(MessageRef {
            key,
            headers,
            payload,
        })
    }
}

impl<'a> Iterator for FrameParser<'a> {
    type Item = Result<MessageRef<'a>, FrameError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned || self.offset == self.buf.len() {
            return None;
        }
        let result = self.parse_one();
        if result.is_err() {
            self.poisoned = true;
        }
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(frame: &[u8]) -> Vec<Result<MessageRef<'_>, FrameError>> {
        FrameParser::new(frame).collect()
    }

    #[test]
    fn round_trips_key_headers_payload() {
        let mut buf = BytesMut::new();
        let headers = [("trace", &b"abc123"[..]), ("origin", &b"spate"[..])];
        write_message(
            &mut buf,
            Some(b"key-1"),
            headers.iter().copied(),
            Some(b"the payload"),
        )
        .unwrap();

        let messages = parse_all(&buf);
        assert_eq!(messages.len(), 1);
        let msg = messages[0].as_ref().unwrap();
        assert_eq!(msg.key, Some(&b"key-1"[..]));
        assert_eq!(
            msg.headers,
            vec![("trace", &b"abc123"[..]), ("origin", &b"spate"[..])]
        );
        assert_eq!(msg.payload, Some(&b"the payload"[..]));
    }

    #[test]
    fn tombstone_and_keyless_flags_round_trip() {
        let mut buf = BytesMut::new();
        // Keyless with an empty (but present) payload.
        write_message(&mut buf, None, std::iter::empty(), Some(b"")).unwrap();
        // Keyed tombstone (the compaction-delete shape).
        write_message(&mut buf, Some(b"gone"), std::iter::empty(), None).unwrap();
        // Empty key is distinct from no key.
        write_message(&mut buf, Some(b""), std::iter::empty(), Some(b"x")).unwrap();

        let messages = parse_all(&buf);
        assert_eq!(messages.len(), 3);
        let first = messages[0].as_ref().unwrap();
        assert_eq!(first.key, None);
        assert_eq!(
            first.payload,
            Some(&b""[..]),
            "empty payload is not a tombstone"
        );
        let second = messages[1].as_ref().unwrap();
        assert_eq!(second.key, Some(&b"gone"[..]));
        assert_eq!(second.payload, None, "tombstone parses as a null payload");
        let third = messages[2].as_ref().unwrap();
        assert_eq!(third.key, Some(&b""[..]), "empty key survives");
    }

    #[test]
    fn frames_concatenate() {
        // Two frames written independently (as two pipeline-thread chunks
        // would be) parse identically to one frame holding both messages —
        // the concatenability contract the shard worker relies on.
        let mut a = BytesMut::new();
        write_message(&mut a, Some(b"k1"), std::iter::empty(), Some(b"p1")).unwrap();
        let mut b = BytesMut::new();
        write_message(&mut b, None, std::iter::empty(), Some(b"p2")).unwrap();

        let mut joined = BytesMut::new();
        joined.extend_from_slice(&a);
        joined.extend_from_slice(&b);

        let messages: Vec<_> = parse_all(&joined).into_iter().map(|m| m.unwrap()).collect();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].payload, Some(&b"p1"[..]));
        assert_eq!(messages[1].payload, Some(&b"p2"[..]));
    }

    #[test]
    fn truncated_and_overrun_frames_are_errors() {
        let mut buf = BytesMut::new();
        write_message(
            &mut buf,
            Some(b"key"),
            [("h", &b"v"[..])].iter().copied(),
            Some(b"payload"),
        )
        .unwrap();

        // Every proper prefix must fail (never panic, never mis-parse) —
        // except the empty prefix, which is a legal empty frame.
        for cut in 1..buf.len() {
            let results = parse_all(&buf[..cut]);
            assert!(
                results.iter().any(Result::is_err),
                "prefix of {cut} bytes must surface a frame error"
            );
            // Poisoning: nothing after the first error.
            let first_err = results.iter().position(Result::is_err).unwrap();
            assert_eq!(first_err, results.len() - 1, "iteration stops at the error");
        }

        // A length pointing past the end of the buffer must not overrun.
        let mut overrun = BytesMut::new();
        overrun.put_u8(FLAG_HAS_KEY);
        overrun.put_u32_le(u32::MAX);
        let results = parse_all(&overrun);
        assert!(results[0].is_err());

        // Unknown flag bits are rejected (future format versions).
        let mut unknown = BytesMut::new();
        unknown.put_u8(0b1000_0000);
        assert!(parse_all(&unknown)[0].is_err());

        // Non-UTF-8 header names are rejected.
        let mut bad_name = BytesMut::new();
        bad_name.put_u8(0);
        bad_name.put_u32_le(1); // one header
        bad_name.put_u32_le(2);
        bad_name.put_slice(&[0xFF, 0xFE]); // invalid UTF-8 name
        bad_name.put_u32_le(0); // empty value
        bad_name.put_u32_le(0); // empty payload
        assert!(parse_all(&bad_name)[0].is_err());
    }

    #[test]
    fn absurd_header_count_errors_without_a_huge_allocation() {
        // A corrupt header count near `u32::MAX` with no header bytes behind it
        // must surface a clean frame error via the bounded read — not attempt a
        // multi-gigabyte pre-allocation (which would abort the process).
        let mut buf = BytesMut::new();
        buf.put_u8(0); // no key, no tombstone
        buf.put_u32_le(u32::MAX); // claims ~4.29e9 headers
        // (no header bytes follow)
        let results = parse_all(&buf);
        assert!(results[0].is_err(), "absurd header count must error");
    }
}
