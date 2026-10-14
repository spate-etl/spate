//! Byte-level helpers shared by the Native column writers.
//!
//! The LEB128 (`VarUInt`) and `String` encodings mirror [`crate::rowbinary`]
//! exactly: a Native column reuses the same *leaf* byte encodings as
//! RowBinary, only laid out per-column (all of a column's values contiguous)
//! instead of per-row.

use bytes::{BufMut, BytesMut};

/// ClickHouse `VarUInt` (unsigned LEB128, 1–10 bytes). Mirrors
/// `rowbinary::put_leb128`, kept in step so the two encoders agree on the
/// wire (a byte-unit test pins the encoding).
pub(crate) fn put_leb128(buf: &mut BytesMut, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf.put_u8(byte);
            break;
        }
        buf.put_u8(byte | 0x80);
    }
}

/// The number of bytes [`put_leb128`] writes for `value`.
pub(crate) fn leb128_len(value: u64) -> usize {
    (u64::BITS - (value | 1).leading_zeros()).div_ceil(7) as usize
}

/// A ClickHouse `String` value: LEB128 length prefix then the raw bytes.
pub(crate) fn put_string(buf: &mut BytesMut, bytes: &[u8]) {
    put_leb128(buf, bytes.len() as u64);
    buf.put_slice(bytes);
}

/// The number of bytes [`put_string`] writes for a value of `s`'s length.
pub(crate) fn string_len(s: &str) -> usize {
    leb128_len(s.len() as u64) + s.len()
}
