//! Byte-level encodings shared by both encoders.
//!
//! RowBinary and Native write a LEB128 (`VarUInt`) and a `String` the same
//! way. Native lays a column's values out contiguously, and RowBinary lays
//! out one row at a time.

use std::ptr;

use bytes::{BufMut, BytesMut};

/// ClickHouse `VarUInt` (unsigned LEB128, 1–10 bytes).
#[inline]
pub(crate) fn put_leb128(buf: &mut BytesMut, value: u64) {
    // Most lengths and counts are below 128, so they take one byte.
    // `extend_from_slice` stays inline without LTO, where `put_u8` calls
    // `BytesMut`'s out-of-line `put_slice`.
    if value < 0x80 {
        buf.extend_from_slice(&[value as u8]);
    } else {
        put_leb128_multi(buf, value);
    }
}

fn put_leb128_multi(buf: &mut BytesMut, mut value: u64) {
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
#[inline]
pub(crate) fn put_string(buf: &mut BytesMut, bytes: &[u8]) {
    let n = bytes.len();
    if n < 0x80 {
        put_short_string(buf, n as u8, bytes);
    } else {
        put_leb128_multi(buf, n as u64);
        buf.extend_from_slice(bytes);
    }
}

/// Appends a string shorter than 128 bytes and its one-byte length `len`
/// under one reservation. Lengths from 4 to 32 copy with two overlapping
/// fixed-width moves and lengths 1 to 3 copy single bytes, so a short string
/// makes no call into libc's `memcpy`.
#[inline]
fn put_short_string(buf: &mut BytesMut, len: u8, src: &[u8]) {
    let n = src.len();
    buf.reserve(1 + n);
    let base = buf.spare_capacity_mut().as_mut_ptr().cast::<u8>();
    let src = src.as_ptr();
    // SAFETY: `reserve(1 + n)` leaves at least `1 + n` bytes of spare
    // capacity at `base`. The length goes to `base[0]`. Each branch reads only
    // inside `src[..n]` and writes only inside `dst[..n]`, which is
    // `base[1..1 + n]`, and its writes cover every byte of it, so `set_len`
    // exposes only initialized bytes.
    unsafe {
        *base = len;
        let dst = base.add(1);
        if n > 32 {
            ptr::copy_nonoverlapping(src, dst, n);
        } else if n >= 16 {
            let head = ptr::read_unaligned(src.cast::<[u8; 16]>());
            let tail = ptr::read_unaligned(src.add(n - 16).cast::<[u8; 16]>());
            ptr::write_unaligned(dst.cast::<[u8; 16]>(), head);
            ptr::write_unaligned(dst.add(n - 16).cast::<[u8; 16]>(), tail);
        } else if n >= 8 {
            let head = ptr::read_unaligned(src.cast::<u64>());
            let tail = ptr::read_unaligned(src.add(n - 8).cast::<u64>());
            ptr::write_unaligned(dst.cast::<u64>(), head);
            ptr::write_unaligned(dst.add(n - 8).cast::<u64>(), tail);
        } else if n >= 4 {
            let head = ptr::read_unaligned(src.cast::<u32>());
            let tail = ptr::read_unaligned(src.add(n - 4).cast::<u32>());
            ptr::write_unaligned(dst.cast::<u32>(), head);
            ptr::write_unaligned(dst.add(n - 4).cast::<u32>(), tail);
        } else if n != 0 {
            *dst = *src;
            *dst.add(n / 2) = *src.add(n / 2);
            *dst.add(n - 1) = *src.add(n - 1);
        }
        buf.set_len(buf.len() + 1 + n);
    }
}

/// The number of bytes [`put_string`] writes for a value of `s`'s length.
pub(crate) fn string_len(s: &str) -> usize {
    leb128_len(s.len() as u64) + s.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lengths on both sides of each copy width, and the first two-byte
    /// LEB128 lengths.
    fn lengths() -> impl Iterator<Item = usize> {
        (0..=40).chain([127, 128, 300])
    }

    /// `n` bytes, each different from its neighbours and from zero, so a
    /// byte copied to the wrong place or left unwritten shows.
    fn distinct(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8 + 1).collect()
    }

    /// The prefix comes from `put_leb128`, which the boundary test pins.
    fn expected_string(bytes: &[u8]) -> Vec<u8> {
        let mut prefix = BytesMut::new();
        put_leb128(&mut prefix, bytes.len() as u64);
        [&prefix[..], bytes].concat()
    }

    #[test]
    fn every_string_length_is_written_whole_between_its_neighbours() {
        for n in lengths() {
            let value = distinct(n);
            let mut buf = BytesMut::with_capacity(1024);
            buf.put_u8(0xEE);
            put_string(&mut buf, &value);
            put_string(&mut buf, b"next");

            let mut expected = vec![0xEE];
            expected.extend(expected_string(&value));
            expected.extend(expected_string(b"next"));
            assert_eq!(&buf[..], &expected[..], "length {n}");
        }
    }

    #[test]
    fn a_string_grows_a_buffer_with_no_spare_capacity() {
        for n in lengths() {
            let value = distinct(n);

            let mut empty = BytesMut::new();
            put_string(&mut empty, &value);
            assert_eq!(&empty[..], &expected_string(&value)[..], "length {n}");

            let mut full = BytesMut::with_capacity(1);
            full.put_u8(0xEE);
            assert_eq!(full.len(), full.capacity());
            put_string(&mut full, &value);
            assert_eq!(full[0], 0xEE, "length {n}");
            assert_eq!(&full[1..], &expected_string(&value)[..], "length {n}");
        }
    }

    #[test]
    fn leb128_writes_seven_bits_per_byte_at_each_boundary() {
        let cases: [(u64, &[u8]); 8] = [
            (0, &[0x00]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (300, &[0xac, 0x02]),
            (16_383, &[0xff, 0x7f]),
            (16_384, &[0x80, 0x80, 0x01]),
            (
                u64::MAX,
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
            ),
        ];
        for (value, bytes) in cases {
            let mut buf = BytesMut::new();
            put_leb128(&mut buf, value);
            assert_eq!(&buf[..], bytes, "value {value}");
            assert_eq!(leb128_len(value), bytes.len(), "value {value}");
        }
    }
}
