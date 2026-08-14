//! A serde `Serializer` producing ClickHouse [RowBinary].
//!
//! The `clickhouse` crate's own row serializer is crate-private, so
//! `spate-clickhouse` ships its own. Wire semantics mirror the
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
//! | `Option<T>` (T not itself `Option`) | 1 prefix byte: `1` = NULL, `0` = value follows (`Nullable(T)`) |
//! | `Vec<T>` / slices (seq) | LEB128 element count + elements (`Array(T)`) |
//! | tuples, `[T; N]`, tuple structs | elements back-to-back, no length (`Tuple`, `FixedString(N)` for `[u8; N]`) |
//! | structs | fields in **declaration order**, no header |
//! | maps | LEB128 entry count + alternating key, value (`Map(K, V)`) |
//! | newtype structs | transparent |
//! | newtype enum variants | 1 discriminant byte (serde `variant_index`) + value (`Variant`) |
//! | `char`, `()`, unit structs/variants, struct/tuple variants, `Option<Option<T>>` | **unsupported** — record-level error |
//!
//! ## Column-type coverage beyond the primitives
//!
//! | ClickHouse type | Rust field |
//! |---|---|
//! | `Date` / `Date32` | [`DateDays`](crate::DateDays) / [`Date32Days`](crate::Date32Days), or `chrono::NaiveDate` / `time::Date` via [`crate::serde`] (feature-gated) |
//! | `DateTime` | [`DateTimeSeconds`], or `chrono::DateTime<Utc>` / `time::OffsetDateTime` via [`crate::serde`] |
//! | `DateTime64(0/3/6/9)` | [`DateTime64Secs`](crate::DateTime64Secs) / [`DateTime64Millis`] / [`DateTime64Micros`](crate::DateTime64Micros) / [`DateTime64Nanos`](crate::DateTime64Nanos), or the `datetime64::*` serde modules |
//! | `Time` / `Time64(p)` | [`TimeSeconds`](crate::TimeSeconds) / [`Time64Secs`](crate::Time64Secs)-family, or the `time`/`time64::*` serde modules (server ≥ 25.6, `enable_time_time64_type=1`) |
//! | `UUID` | `uuid::Uuid` with `#[serde(with = "spate_clickhouse::serde::uuid")]` (feature `uuid`). ⚠ `Uuid`'s default impl writes a LEB128-prefixed 16-byte string — silently wrong |
//! | `IPv4` | `std::net::Ipv4Addr` with `#[serde(with = "spate_clickhouse::serde::ipv4")]`. ⚠ the default impl writes big-endian octets — silently wrong |
//! | `IPv6` | `std::net::Ipv6Addr` bare (its default impl — 16 network-order bytes — is correct) |
//! | `Enum8` / `Enum16` | `serde_repr` enums: `#[derive(Serialize_repr)] #[repr(i8)]` (or `i16`) |
//! | `Decimal(P, S)` | [`Decimal32<S>`](crate::Decimal32) / [`Decimal64<S>`](crate::Decimal64) / [`Decimal128<S>`](crate::Decimal128) pre-scaled wrappers (or raw ints); `rust_decimal` conversions behind the `rust_decimal` feature |
//! | `Int256` / `UInt256` | [`Int256`](crate::Int256) / [`UInt256`](crate::UInt256) (32 raw LE bytes) |
//! | `JSON` | a `String` of JSON text **plus** the per-insert setting `input_format_binary_read_json_as_string: "1"` (see the crate docs' wiring example) |
//! | `LowCardinality(T)` | the plain `T` — transparent on insert; the server builds the dictionary |
//! | Geo (`Point`, `Ring`, `LineString`, `Polygon`, ...) | the [`Point`](crate::Point)/[`Ring`](crate::Ring)/... aliases (tuples and `Vec`s of `Float64`) |
//! | `Dynamic`, `AggregateFunction(...)`, `Decimal256` | **unsupported** — no serde shape maps to them (`Decimal256`: scale manually into an [`Int256`](crate::Int256)) |
//!
//! ## The wire contract is field order
//!
//! RowBinary carries no column names: the `INSERT INTO t (a, b, c)` column
//! list must match the struct's field declaration order exactly. Reordering
//! struct fields is a breaking change to the pipeline's wire format.
//! Column-type niceties (e.g. `DateTime64(3)` is an `Int64` of milliseconds
//! on the wire) are handled with plain integer fields, the newtype wrappers
//! above, or the [`crate::serde`] field attributes.
//!
//! ## Unsupported serde attributes (they misalign columns)
//!
//! Because rows are positional and fixed-width, several serde attributes
//! that make a struct emit a variable or unexpected number of fields are
//! **not supported** on row types:
//!
//! - **`#[serde(skip)]` / `#[serde(skip_serializing_if = ...)]`** — a skipped
//!   field emits *fewer* columns for some rows, shifting every following
//!   byte into the wrong column. This serializer **cannot reliably detect
//!   it**: serde reports the already-reduced field count to
//!   `serialize_struct`, so a short row looks self-consistent. Never put a
//!   skip attribute on a row struct; model absent values as `Option<T>`
//!   (`Nullable`) instead. (A `Serialize` impl that *lies* about its field
//!   count, declaring N but writing a different number, is caught with
//!   [`RowBinaryError::FieldCountMismatch`].)
//! - **`#[serde(flatten)]`** — serde lowers a flattened struct to a
//!   length-less map; rejected with [`RowBinaryError::FlattenUnsupported`].
//!   Inline the fields into the row struct in column order instead.
//! - **internally/adjacently-tagged enums (`#[serde(tag = ...)]`)** — emit
//!   the tag as a phantom string column. Map enums to an explicit integer or
//!   `String` column, or use a newtype-variant `Variant` (below).
//!
//! ## `Variant` discriminant ordering
//!
//! A newtype enum variant writes serde's `variant_index`, the enum's
//! **declaration order**, as the `Variant` discriminant byte. ClickHouse
//! does **not** use declaration order: it sorts a `Variant(T1, T2, ...)`'s
//! member types **alphabetically by type name** and numbers the
//! discriminators in that sorted order. There is no way to infer that
//! mapping here, so **you must declare your Rust enum variants in the same
//! order ClickHouse will sort the column's types.**
//!
//! For a `Variant(Int64, String)` column (ClickHouse order: `Int64` = 0,
//! `String` = 1, sorted alphabetically), declare:
//!
//! ```
//! # use serde::Serialize;
//! #[derive(Serialize)]
//! enum Payload {
//!     Num(i64),     // variant_index 0 -> Int64
//!     Text(String), // variant_index 1 -> String
//! }
//! # let _ = (Payload::Num(0), Payload::Text(String::new()));
//! ```
//!
//! Also note: `Option<Enum>` against such a column emits the `Nullable`
//! `0`/`1` prefix, but ClickHouse encodes a `Variant` NULL as discriminant
//! `255` and forbids `Nullable(Variant)`, so a nullable variant column is
//! not representable; use the `Variant`'s own NULL support instead.
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
    /// `#[serde(flatten)]` (serde serializes the outer struct as a
    /// length-less map, which RowBinary cannot position into columns).
    #[error(
        "#[serde(flatten)] is not supported by RowBinary: it produces \
         length-less, unordered fields; inline the flattened fields into the \
         row struct in column order instead"
    )]
    FlattenUnsupported,
    /// A nested `Option` (`Option<Option<T>>`): ClickHouse has no
    /// `Nullable(Nullable(T))`, so the double null-prefix is unparseable.
    #[error(
        "nested Option (Option<Option<T>>) has no ClickHouse type: \
         ClickHouse forbids Nullable(Nullable(T))"
    )]
    NestedOption,
    /// A `Serialize` impl declared one struct field count to
    /// `serialize_struct` but serialized a different number of fields.
    /// RowBinary is positional, so a variable-width row misaligns every
    /// following column. (Note: `#[serde(skip)]`/`#[serde(skip_serializing_if)]`
    /// evade this check, since serde reports the post-skip count, which is
    /// why those attributes are documented as unsupported.)
    #[error(
        "struct declared {expected} field(s) but serialized {got}: RowBinary rows are positional and must be fixed-width"
    )]
    FieldCountMismatch {
        /// Fields the `Serialize` impl declared.
        expected: usize,
        /// Fields it serialized.
        got: usize,
    },
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
    row.serialize(&mut RowBinarySer {
        buf,
        option_inner: false,
        struct_depth: 0,
        top_expected: 0,
        top_written: 0,
    })
}

pub use crate::types::{DateTime64Millis, DateTimeSeconds};

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
    /// `true` while the next `serialize` call is the *direct* inner of an
    /// `Option`. Lets `serialize_some`/`serialize_none` reject nested
    /// `Option<Option<T>>` (unparseable double null-prefix). Composite
    /// serializers clear it: an `Option` inside a struct/seq/tuple field is
    /// a separate column, not a nested nullable.
    option_inner: bool,
    /// Depth of nested `serialize_struct` calls; the outermost struct
    /// (depth 1) is the one whose fields map to the insert columns.
    struct_depth: u32,
    /// The field count the outermost struct declared to `serialize_struct`.
    top_expected: usize,
    /// The field count it serialized (compared at `end`).
    top_written: usize,
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
        if self.option_inner {
            // The null of a nested Option (`Some(None)`): unparseable.
            return Err(RowBinaryError::NestedOption);
        }
        // Nullable(T): 1 = NULL.
        self.buf.put_u8(1);
        Ok(())
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), RowBinaryError> {
        if self.option_inner {
            // We are the direct inner of an Option: `Option<Option<T>>`.
            return Err(RowBinaryError::NestedOption);
        }
        // Nullable(T): 0 then the value. Mark the inner so a directly-nested
        // Option is rejected; composite/leaf serializers clear it.
        self.buf.put_u8(0);
        self.option_inner = true;
        let result = value.serialize(&mut *self);
        self.option_inner = false;
        result
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
        self.option_inner = false;
        // ClickHouse `Variant`: one discriminant byte then the value.
        //
        // IMPORTANT: the discriminant written here is serde's
        // `variant_index`, the enum's *declaration order*. ClickHouse
        // assigns `Variant(...)` discriminators in a different order (its
        // types sorted alphabetically by name), so declaration order must be
        // made to match by the user. See the crate/type docs; there is no
        // way to infer the mapping here.
        let idx = u8::try_from(variant_index)
            .map_err(|_| RowBinaryError::VariantOutOfRange(variant_index))?;
        self.buf.put_u8(idx);
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, RowBinaryError> {
        self.option_inner = false;
        let len = len.ok_or(RowBinaryError::SequenceMustHaveLength)?;
        put_leb128(self.buf, len as u64);
        Ok(self)
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, RowBinaryError> {
        self.option_inner = false;
        Ok(self)
    }

    #[inline]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, RowBinaryError> {
        self.option_inner = false;
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
        self.option_inner = false;
        // A real `Map` always reports its length; a length-less map is
        // serde's `#[serde(flatten)]` lowering. Reject it with a message that
        // names the actual cause instead of the misleading "sequences must
        // have a known length" error.
        let len = len.ok_or(RowBinaryError::FlattenUnsupported)?;
        put_leb128(self.buf, len as u64);
        Ok(self)
    }

    #[inline]
    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, RowBinaryError> {
        self.option_inner = false;
        // Track the outermost struct's declared field count so `end` can
        // reject a `Serialize` impl that emits a different number of fields
        // (a positional-row misalignment).
        if self.struct_depth == 0 {
            self.top_expected = len;
            self.top_written = 0;
        }
        self.struct_depth += 1;
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
        // Count only the outermost struct's fields (depth 1); nested structs
        // are Tuple columns with their own widths.
        if self.struct_depth == 1 {
            self.top_written += 1;
        }
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<(), RowBinaryError> {
        self.struct_depth -= 1;
        if self.struct_depth == 0 && self.top_written != self.top_expected {
            return Err(RowBinaryError::FieldCountMismatch {
                expected: self.top_expected,
                got: self.top_written,
            });
        }
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
        // The discriminant is serde's variant_index (declaration order): B
        // is index 1. The user must declare variants in the order ClickHouse
        // sorts the column's Variant types (see the module docs).
        assert_eq!(enc(&V::B(7)), [1, 7, 0]);
    }

    #[test]
    fn nested_option_is_rejected() {
        // Option<Option<T>> has no ClickHouse type (no Nullable(Nullable(T)));
        // it previously emitted an unparseable double null-prefix.
        assert!(matches!(
            serialize_row(&Some(Option::<u8>::None), &mut BytesMut::new()),
            Err(RowBinaryError::NestedOption)
        ));
        assert!(matches!(
            serialize_row(&Some(Some(7u8)), &mut BytesMut::new()),
            Err(RowBinaryError::NestedOption)
        ));

        // A plain Option, and Options nested inside a struct/seq (separate
        // columns/elements, not nested nullables), still work.
        assert_eq!(enc(&Some(7u8)), [0, 7]);
        assert_eq!(enc(&None::<u8>), [1]);
        #[derive(Serialize)]
        struct Row {
            a: Option<u8>,
            b: Option<u8>,
        }
        assert_eq!(
            enc(&Row {
                a: Some(1),
                b: None
            }),
            [0, 1, 1]
        );
        assert_eq!(enc(&vec![Some(1u8), None]), [2, 0, 1, 1]);
    }

    #[test]
    fn flatten_is_rejected_with_a_clear_error() {
        #[derive(Serialize)]
        struct Inner {
            x: u8,
        }
        #[derive(Serialize)]
        struct Flat {
            id: u8,
            #[serde(flatten)]
            inner: Inner,
        }
        let err = serialize_row(
            &Flat {
                id: 1,
                inner: Inner { x: 2 },
            },
            &mut BytesMut::new(),
        )
        .unwrap_err();
        assert!(matches!(err, RowBinaryError::FlattenUnsupported), "{err}");
        // The message names flatten, not the old misleading "sequences".
        assert!(err.to_string().contains("flatten"), "{err}");
    }

    #[test]
    fn struct_field_count_mismatch_is_caught() {
        // A Serialize impl that declares 3 fields but serializes 1 would
        // misalign every following column; caught at end().
        struct Liar;
        impl Serialize for Liar {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeStruct;
                let mut st = s.serialize_struct("Liar", 3)?;
                st.serialize_field("a", &1u8)?;
                st.end()
            }
        }
        assert!(
            matches!(
                serialize_row(&Liar, &mut BytesMut::new()),
                Err(RowBinaryError::FieldCountMismatch {
                    expected: 3,
                    got: 1
                })
            ),
            "declared/serialized field-count mismatch must error"
        );
    }

    #[test]
    fn serde_repr_enums_encode_as_enum8_and_enum16() {
        // The documented mechanism for Enum8/Enum16 columns: serde_repr
        // serializes the discriminant as its #[repr] integer, which the
        // serializer writes fixed-width like any other i8/i16.
        #[derive(serde_repr::Serialize_repr)]
        #[repr(i8)]
        enum Level8 {
            #[allow(dead_code)]
            Low = -1,
            High = 2,
        }
        #[derive(serde_repr::Serialize_repr)]
        #[repr(i16)]
        enum Level16 {
            Big = 300,
        }
        assert_eq!(enc(&Level8::High), [2]);
        assert_eq!(enc(&Level8::Low), [0xff]);
        assert_eq!(enc(&Level16::Big), 300i16.to_le_bytes());
    }

    #[test]
    fn ipv6_default_impl_matches_the_16_byte_wire_format() {
        // Unlike Ipv4Addr (see crate::serde::ipv4), Ipv6Addr's default
        // serde impl (16 network-order octets) is exactly the IPv6
        // column's FixedString(16)-style layout.
        use std::net::Ipv6Addr;
        let localhost = Ipv6Addr::LOCALHOST;
        assert_eq!(enc(&localhost), localhost.octets());
        let addr: Ipv6Addr = "2001:db8::8a2e:370:7334".parse().unwrap();
        assert_eq!(enc(&addr), addr.octets());
    }

    #[test]
    fn skip_serializing_if_yields_a_short_row_the_serializer_cannot_detect() {
        // Documents the known limitation behind the "skip attributes are
        // unsupported" docs: serde reports the *post-skip* field count to
        // serialize_struct, so a skipped column looks self-consistent here.
        #[derive(Serialize)]
        struct Row {
            id: u8,
            #[serde(skip_serializing_if = "Option::is_none")]
            score: Option<u8>,
            name: String,
        }
        // score present -> 3 fields.
        assert_eq!(
            enc(&Row {
                id: 1,
                score: Some(9),
                name: "ab".into()
            }),
            [1, 0, 9, 2, b'a', b'b']
        );
        // score absent -> only 2 fields emitted (no error), which would
        // misalign against a 3-column table. Hence: never use skip on rows.
        assert_eq!(
            enc(&Row {
                id: 1,
                score: None,
                name: "ab".into()
            }),
            [1, 2, b'a', b'b']
        );
    }
}
