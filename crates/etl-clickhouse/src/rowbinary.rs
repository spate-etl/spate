//! A serde `Serializer` producing ClickHouse [RowBinary].
//!
//! The `clickhouse` crate's own row serializer is crate-private, so
//! `etl-clickhouse` ships its own. Wire semantics deliberately mirror the
//! crate's (its deserializer is used to read rows back in tests, so
//! compatibility is proven by round-trips):
//!
//! | Rust / serde type | RowBinary encoding |
//! |---|---|
//! | `bool` | 1 byte, `0`/`1` |
//! | `i8..i128`, `u8..u128` | little-endian fixed width |
//! | `f32`, `f64` | little-endian IEEE 754 |
//! | `String`, `&str` | LEB128 byte length + UTF-8 bytes (`String`) |
//! | `serde_bytes`-style `&[u8]` | LEB128 length + bytes (`String`) |
//! | `Option<T>` | 1 prefix byte: `1` = NULL, `0` = value follows (`Nullable(T)`) |
//! | `Vec<T>` / slices (seq) | LEB128 element count + elements (`Array(T)`) |
//! | tuples, `[T; N]`, tuple structs | elements back-to-back, no length (`Tuple`, `FixedString(N)` for `[u8; N]`) |
//! | structs | fields in **declaration order**, no header |
//! | maps | LEB128 entry count + alternating key, value (`Map(K, V)`) |
//! | newtype structs | transparent |
//! | newtype enum variants | 1 discriminant byte + value (`Variant`) |
//! | `char`, `()`, unit structs/variants, struct/tuple variants | **unsupported** — record-level error |
//!
//! ## The wire contract is field order
//!
//! RowBinary carries no column names: the `INSERT INTO t (a, b, c)` column
//! list must match the struct's field declaration order exactly. Reordering
//! struct fields is a breaking change to the pipeline's wire format.
//! Column-type niceties (e.g. `DateTime64(3)` is an `Int64` of milliseconds
//! on the wire) are handled with plain integer fields or newtype wrappers
//! such as [`DateTime64Millis`].
//!
//! [RowBinary]: https://clickhouse.com/docs/en/interfaces/formats#rowbinary

use bytes::{BufMut, BytesMut};
use serde::ser::{self, Serialize};
use std::fmt;

/// A row failed to serialize. Record-level: subject to the sink stage's
/// error policy, never fatal to the pipeline by itself.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RowBinaryError {
    /// The Rust type has no RowBinary representation.
    #[error("type has no RowBinary representation: {0}")]
    Unsupported(&'static str),
    /// Sequences must know their length up front (LEB128 prefix).
    #[error("sequences must have a known length ahead of time")]
    SequenceMustHaveLength,
    /// `Variant` discriminants are single bytes.
    #[error("variant discriminant {0} exceeds 255")]
    VariantOutOfRange(u32),
    /// An error raised by a `Serialize` implementation.
    #[error("serialize error: {0}")]
    Custom(String),
}

impl ser::Error for RowBinaryError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        RowBinaryError::Custom(msg.to_string())
    }
}

/// Serialize one row, appending its RowBinary encoding to `buf`.
///
/// On error the buffer may contain a partial row; the caller rolls back to
/// its pre-record length (the chain's terminal stage does this).
pub fn serialize_row<T: Serialize + ?Sized>(
    row: &T,
    buf: &mut BytesMut,
) -> Result<(), RowBinaryError> {
    row.serialize(&mut RowBinarySer { buf })
}

/// Milliseconds since the Unix epoch, matching a `DateTime64(3)` column's
/// wire representation (`Int64`). A documentation-carrying newtype: the
/// encoding is transparently the inner `i64`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DateTime64Millis(pub i64);

impl Serialize for DateTime64Millis {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.0)
    }
}

/// Seconds since the Unix epoch, matching a `DateTime` column's wire
/// representation (`UInt32`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DateTimeSeconds(pub u32);

impl Serialize for DateTimeSeconds {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.0)
    }
}

fn put_leb128(buf: &mut BytesMut, mut value: u64) {
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

struct RowBinarySer<'a> {
    buf: &'a mut BytesMut,
}

impl<'a, 'b> ser::Serializer for &'a mut RowBinarySer<'b> {
    type Ok = ();
    type Error = RowBinaryError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = ser::Impossible<(), RowBinaryError>;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = ser::Impossible<(), RowBinaryError>;

    #[inline]
    fn serialize_bool(self, v: bool) -> Result<(), RowBinaryError> {
        self.buf.put_u8(u8::from(v));
        Ok(())
    }

    #[inline]
    fn serialize_i8(self, v: i8) -> Result<(), RowBinaryError> {
        self.buf.put_i8(v);
        Ok(())
    }

    #[inline]
    fn serialize_i16(self, v: i16) -> Result<(), RowBinaryError> {
        self.buf.put_i16_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_i32(self, v: i32) -> Result<(), RowBinaryError> {
        self.buf.put_i32_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_i64(self, v: i64) -> Result<(), RowBinaryError> {
        self.buf.put_i64_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_i128(self, v: i128) -> Result<(), RowBinaryError> {
        self.buf.put_i128_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_u8(self, v: u8) -> Result<(), RowBinaryError> {
        self.buf.put_u8(v);
        Ok(())
    }

    #[inline]
    fn serialize_u16(self, v: u16) -> Result<(), RowBinaryError> {
        self.buf.put_u16_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_u32(self, v: u32) -> Result<(), RowBinaryError> {
        self.buf.put_u32_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_u64(self, v: u64) -> Result<(), RowBinaryError> {
        self.buf.put_u64_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_u128(self, v: u128) -> Result<(), RowBinaryError> {
        self.buf.put_u128_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_f32(self, v: f32) -> Result<(), RowBinaryError> {
        self.buf.put_f32_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_f64(self, v: f64) -> Result<(), RowBinaryError> {
        self.buf.put_f64_le(v);
        Ok(())
    }

    fn serialize_char(self, _v: char) -> Result<(), RowBinaryError> {
        Err(RowBinaryError::Unsupported(
            "char (use a String column instead)",
        ))
    }

    #[inline]
    fn serialize_str(self, v: &str) -> Result<(), RowBinaryError> {
        put_leb128(self.buf, v.len() as u64);
        self.buf.put_slice(v.as_bytes());
        Ok(())
    }

    #[inline]
    fn serialize_bytes(self, v: &[u8]) -> Result<(), RowBinaryError> {
        put_leb128(self.buf, v.len() as u64);
        self.buf.put_slice(v);
        Ok(())
    }

    #[inline]
    fn serialize_none(self) -> Result<(), RowBinaryError> {
        // Nullable(T): 1 = NULL.
        self.buf.put_u8(1);
        Ok(())
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), RowBinaryError> {
        // Nullable(T): 0 then the value.
        self.buf.put_u8(0);
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), RowBinaryError> {
        Err(RowBinaryError::Unsupported("() has no column type"))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<(), RowBinaryError> {
        let _ = name;
        Err(RowBinaryError::Unsupported("unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<(), RowBinaryError> {
        Err(RowBinaryError::Unsupported(
            "unit enum variant (map enums to an integer or String column explicitly)",
        ))
    }

    #[inline]
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), RowBinaryError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<(), RowBinaryError> {
        // ClickHouse `Variant`: one discriminant byte then the value,
        // mirroring the clickhouse crate's encoding.
        let idx = u8::try_from(variant_index)
            .map_err(|_| RowBinaryError::VariantOutOfRange(variant_index))?;
        self.buf.put_u8(idx);
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, RowBinaryError> {
        let len = len.ok_or(RowBinaryError::SequenceMustHaveLength)?;
        put_leb128(self.buf, len as u64);
        Ok(self)
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, RowBinaryError> {
        Ok(self)
    }

    #[inline]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, RowBinaryError> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, RowBinaryError> {
        Err(RowBinaryError::Unsupported("tuple enum variant"))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, RowBinaryError> {
        let len = len.ok_or(RowBinaryError::SequenceMustHaveLength)?;
        put_leb128(self.buf, len as u64);
        Ok(self)
    }

    #[inline]
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, RowBinaryError> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, RowBinaryError> {
        Err(RowBinaryError::Unsupported("struct enum variant"))
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a, 'b> ser::SerializeSeq for &'a mut RowBinarySer<'b> {
    type Ok = ();
    type Error = RowBinaryError;

    #[inline]
    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), RowBinaryError> {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<(), RowBinaryError> {
        Ok(())
    }
}

impl<'a, 'b> ser::SerializeTuple for &'a mut RowBinarySer<'b> {
    type Ok = ();
    type Error = RowBinaryError;

    #[inline]
    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), RowBinaryError> {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<(), RowBinaryError> {
        Ok(())
    }
}

impl<'a, 'b> ser::SerializeTupleStruct for &'a mut RowBinarySer<'b> {
    type Ok = ();
    type Error = RowBinaryError;

    #[inline]
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), RowBinaryError> {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<(), RowBinaryError> {
        Ok(())
    }
}

impl<'a, 'b> ser::SerializeMap for &'a mut RowBinarySer<'b> {
    type Ok = ();
    type Error = RowBinaryError;

    #[inline]
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), RowBinaryError> {
        key.serialize(&mut **self)
    }

    #[inline]
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), RowBinaryError> {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<(), RowBinaryError> {
        Ok(())
    }
}

impl<'a, 'b> ser::SerializeStruct for &'a mut RowBinarySer<'b> {
    type Ok = ();
    type Error = RowBinaryError;

    #[inline]
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), RowBinaryError> {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<(), RowBinaryError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    fn enc<T: Serialize>(v: &T) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_row(v, &mut buf).expect("serialize");
        buf.to_vec()
    }

    #[test]
    fn integers_are_little_endian_fixed_width() {
        assert_eq!(enc(&0x0102_0304u32), [0x04, 0x03, 0x02, 0x01]);
        assert_eq!(enc(&-2i16), [0xfe, 0xff]);
        assert_eq!(enc(&1u8), [0x01]);
        assert_eq!(
            enc(&0x0102_0304_0506_0708u64),
            [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        );
        assert_eq!(enc(&1u128).len(), 16);
    }

    #[test]
    fn floats_and_bools() {
        assert_eq!(enc(&1.0f32), 1.0f32.to_le_bytes());
        assert_eq!(enc(&-2.5f64), (-2.5f64).to_le_bytes());
        assert_eq!(enc(&true), [1]);
        assert_eq!(enc(&false), [0]);
    }

    #[test]
    fn strings_are_leb128_prefixed() {
        assert_eq!(enc(&"abc"), [3, b'a', b'b', b'c']);
        assert_eq!(enc(&""), [0]);
        // 300 bytes: LEB128 = [0xAC, 0x02].
        let long = "x".repeat(300);
        let bytes = enc(&long);
        assert_eq!(&bytes[..2], &[0xac, 0x02]);
        assert_eq!(bytes.len(), 302);
    }

    #[test]
    fn options_use_null_prefix_bytes() {
        // Matches the clickhouse crate: 1 = NULL, 0 = value follows.
        assert_eq!(enc(&Option::<u8>::None), [1]);
        assert_eq!(enc(&Some(7u8)), [0, 7]);
        assert_eq!(enc(&Some("hi")), [0, 2, b'h', b'i']);
    }

    #[test]
    fn sequences_carry_a_count_tuples_do_not() {
        assert_eq!(enc(&vec![1u8, 2, 3]), [3, 1, 2, 3]);
        assert_eq!(enc(&Vec::<u8>::new()), [0]);
        assert_eq!(enc(&(1u8, 2u8, 3u8)), [1, 2, 3]);
        assert_eq!(
            enc(&[1u8, 2, 3]),
            [1, 2, 3],
            "fixed arrays = FixedString/Tuple"
        );
    }

    #[test]
    fn maps_are_counted_key_value_pairs() {
        let mut m = std::collections::BTreeMap::new();
        m.insert("a".to_string(), 1u8);
        m.insert("b".to_string(), 2u8);
        assert_eq!(enc(&m), [2, 1, b'a', 1, 1, b'b', 2]);
    }

    #[test]
    fn structs_encode_fields_in_declaration_order() {
        #[derive(Serialize)]
        struct Row {
            id: u64,
            name: String,
            score: Option<f64>,
        }
        let bytes = enc(&Row {
            id: 5,
            name: "n".into(),
            score: None,
        });
        assert_eq!(bytes, [5, 0, 0, 0, 0, 0, 0, 0, 1, b'n', 1]);
    }

    #[test]
    fn spike_fixture_row_matches_hand_encoding() {
        // The exact encoding the validation spike wrote and read back
        // through the clickhouse crate against a live server.
        #[derive(Serialize)]
        struct SpikeRow {
            id: u64,
            name: String,
        }
        let bytes = enc(&SpikeRow {
            id: 1500,
            name: "raw-1500".into(),
        });
        let mut expected = 1500u64.to_le_bytes().to_vec();
        expected.push(8);
        expected.extend_from_slice(b"raw-1500");
        assert_eq!(bytes, expected);
    }

    #[test]
    fn newtypes_are_transparent() {
        assert_eq!(enc(&DateTime64Millis(1_000)), enc(&1_000i64));
        assert_eq!(enc(&DateTimeSeconds(42)), enc(&42u32));
    }

    #[test]
    fn unsupported_types_error_instead_of_panicking() {
        #[derive(Serialize)]
        enum Unit {
            A,
        }
        assert!(matches!(
            serialize_row(&Unit::A, &mut BytesMut::new()),
            Err(RowBinaryError::Unsupported(_))
        ));
        assert!(matches!(
            serialize_row(&'x', &mut BytesMut::new()),
            Err(RowBinaryError::Unsupported(_))
        ));
        assert!(matches!(
            serialize_row(&(), &mut BytesMut::new()),
            Err(RowBinaryError::Unsupported(_))
        ));
    }

    #[test]
    fn variant_newtype_gets_a_discriminant_byte() {
        #[derive(Serialize)]
        enum V {
            #[allow(dead_code)]
            A(u8),
            B(u16),
        }
        assert_eq!(enc(&V::B(7)), [1, 7, 0]);
    }
}
