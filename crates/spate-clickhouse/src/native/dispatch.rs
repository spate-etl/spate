//! Serde serializers that route a `T: Serialize` row into the columnar
//! [`ColumnWriter`]s.
//!
//! - [`RowDispatchSer`]: the top-level serializer, where field *i* of the
//!   row struct goes to column *i*.
//! - [`ColumnSerializer`]: one value into one column, dispatching on the
//!   **writer variant** (not the value) and writing the same leaf bytes
//!   RowBinary would.
//! - [`Compound`]: the composite sub-serializer (Array elements, Tuple
//!   fields, Map entries).
//! - [`ByteSink`]: accumulates a value's raw little-endian bytes for the
//!   fixed-width blob columns (`UUID`, `IPv6`, `Int256`, `UInt256`,
//!   `FixedString`).

use super::NativeError;
use super::column::{ColumnWriter, ScalarKind};
use super::leaf::put_string;
use bytes::BufMut;
use serde::Serialize;
use serde::ser::{self, Impossible, Serializer};

// ---- ColumnSerializer -----------------------------------------------------

/// Serializes one value into one column.
pub(crate) struct ColumnSerializer<'a> {
    pub(crate) writer: &'a mut ColumnWriter,
}

impl<'a> ColumnSerializer<'a> {
    /// Borrow the leaf buffer iff the column is a `Scalar` of `want`.
    #[inline]
    fn scalar(self, want: ScalarKind) -> Result<&'a mut bytes::BytesMut, NativeError> {
        match self.writer {
            ColumnWriter::Scalar { kind, buf } if *kind == want => Ok(buf),
            _ => Err(NativeError::TypeMismatch {
                expected: want.name(),
            }),
        }
    }
}

macro_rules! scalar_method {
    ($method:ident, $ty:ty, $kind:expr, $put:ident) => {
        #[inline]
        fn $method(self, v: $ty) -> Result<(), NativeError> {
            self.scalar($kind)?.$put(v);
            Ok(())
        }
    };
}

impl<'a> Serializer for ColumnSerializer<'a> {
    type Ok = ();
    type Error = NativeError;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Impossible<(), NativeError>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Impossible<(), NativeError>;

    #[inline]
    fn serialize_bool(self, v: bool) -> Result<(), NativeError> {
        self.scalar(ScalarKind::Bool)?.put_u8(u8::from(v));
        Ok(())
    }

    scalar_method!(serialize_i8, i8, ScalarKind::I8, put_i8);
    scalar_method!(serialize_i16, i16, ScalarKind::I16, put_i16_le);
    scalar_method!(serialize_i32, i32, ScalarKind::I32, put_i32_le);
    scalar_method!(serialize_i64, i64, ScalarKind::I64, put_i64_le);
    scalar_method!(serialize_i128, i128, ScalarKind::I128, put_i128_le);
    scalar_method!(serialize_u8, u8, ScalarKind::U8, put_u8);
    scalar_method!(serialize_u16, u16, ScalarKind::U16, put_u16_le);
    scalar_method!(serialize_u32, u32, ScalarKind::U32, put_u32_le);
    scalar_method!(serialize_u64, u64, ScalarKind::U64, put_u64_le);
    scalar_method!(serialize_u128, u128, ScalarKind::U128, put_u128_le);
    scalar_method!(serialize_f32, f32, ScalarKind::F32, put_f32_le);
    scalar_method!(serialize_f64, f64, ScalarKind::F64, put_f64_le);

    #[inline]
    fn serialize_str(self, v: &str) -> Result<(), NativeError> {
        match self.writer {
            ColumnWriter::Str(buf) => {
                put_string(buf, v.as_bytes());
                Ok(())
            }
            ColumnWriter::LowCard(lc) => {
                lc.intern(v.as_bytes());
                Ok(())
            }
            _ => Err(NativeError::TypeMismatch {
                expected: "String / LowCardinality(String) column",
            }),
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), NativeError> {
        match self.writer {
            ColumnWriter::Str(buf) => {
                put_string(buf, v);
                Ok(())
            }
            ColumnWriter::LowCard(lc) => {
                lc.intern(v);
                Ok(())
            }
            _ => Err(NativeError::TypeMismatch {
                expected: "String / LowCardinality(String) column",
            }),
        }
    }

    #[inline]
    fn serialize_none(self) -> Result<(), NativeError> {
        match self.writer {
            ColumnWriter::Nullable { nullmap, inner } => {
                nullmap.put_u8(1);
                inner.push_default();
                Ok(())
            }
            ColumnWriter::LowCard(lc) if lc.is_nullable() => {
                lc.push_null();
                Ok(())
            }
            _ => Err(NativeError::TypeMismatch {
                expected: "Nullable column",
            }),
        }
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), NativeError> {
        match self.writer {
            ColumnWriter::Nullable { nullmap, inner } => {
                nullmap.put_u8(0);
                inner.append(value)
            }
            w @ ColumnWriter::LowCard(_) => value.serialize(ColumnSerializer { writer: w }),
            _ => Err(NativeError::TypeMismatch {
                expected: "Nullable column",
            }),
        }
    }

    #[inline]
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), NativeError> {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Compound<'a>, NativeError> {
        match self.writer {
            ColumnWriter::Array {
                offsets,
                cum,
                inner,
            } => Ok(Compound::Seq {
                offsets,
                cum,
                inner,
                added: 0,
            }),
            _ => Err(NativeError::TypeMismatch {
                expected: "Array column",
            }),
        }
    }

    fn serialize_tuple(self, _len: usize) -> Result<Compound<'a>, NativeError> {
        self.tuple_like()
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Compound<'a>, NativeError> {
        self.tuple_like()
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Compound<'a>, NativeError> {
        self.tuple_like()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Compound<'a>, NativeError> {
        match self.writer {
            ColumnWriter::Map {
                offsets,
                cum,
                key,
                val,
            } => Ok(Compound::Map {
                offsets,
                cum,
                key,
                val,
                added: 0,
            }),
            _ => Err(NativeError::TypeMismatch {
                expected: "Map column",
            }),
        }
    }

    // ---- unsupported value shapes ----
    fn serialize_char(self, _v: char) -> Result<(), NativeError> {
        Err(NativeError::TypeMismatch {
            expected: "non-char column (use String)",
        })
    }
    fn serialize_unit(self) -> Result<(), NativeError> {
        Err(NativeError::TypeMismatch {
            expected: "a column value, not unit",
        })
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), NativeError> {
        Err(NativeError::TypeMismatch {
            expected: "a column value, not a unit struct",
        })
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<(), NativeError> {
        Err(NativeError::TypeMismatch {
            expected: "a column value, not a unit enum variant",
        })
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), NativeError> {
        Err(NativeError::TypeMismatch {
            expected: "a column value (Variant is not supported)",
        })
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, NativeError> {
        Err(NativeError::TypeMismatch {
            expected: "a column value (tuple variant unsupported)",
        })
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, NativeError> {
        Err(NativeError::TypeMismatch {
            expected: "a column value (struct variant unsupported)",
        })
    }

    fn is_human_readable(&self) -> bool {
        // Match rowbinary: net types serialize as bytes, not strings.
        false
    }
}

impl<'a> ColumnSerializer<'a> {
    fn tuple_like(self) -> Result<Compound<'a>, NativeError> {
        match self.writer {
            ColumnWriter::Tuple(elems) => Ok(Compound::Tuple(elems.iter_mut())),
            _ => Err(NativeError::TypeMismatch {
                expected: "Tuple column",
            }),
        }
    }
}

// ---- Compound (Array / Tuple / Map sub-serializer) ------------------------

pub(crate) enum Compound<'a> {
    Seq {
        offsets: &'a mut bytes::BytesMut,
        cum: &'a mut u64,
        inner: &'a mut ColumnWriter,
        added: u64,
    },
    Tuple(std::slice::IterMut<'a, ColumnWriter>),
    Map {
        offsets: &'a mut bytes::BytesMut,
        cum: &'a mut u64,
        key: &'a mut ColumnWriter,
        val: &'a mut ColumnWriter,
        added: u64,
    },
}

const NOT_SEQ: NativeError = NativeError::Internal("compound used as the wrong shape");

impl<'a> ser::SerializeSeq for Compound<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        match self {
            Compound::Seq { inner, added, .. } => {
                inner.append(value)?;
                *added += 1;
                Ok(())
            }
            _ => Err(NOT_SEQ),
        }
    }
    fn end(self) -> Result<(), NativeError> {
        match self {
            Compound::Seq {
                offsets,
                cum,
                added,
                ..
            } => {
                *cum += added;
                offsets.put_u64_le(*cum);
                Ok(())
            }
            _ => Err(NOT_SEQ),
        }
    }
}

impl<'a> ser::SerializeTuple for Compound<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        tuple_field(self, value)
    }
    fn end(self) -> Result<(), NativeError> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for Compound<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        tuple_field(self, value)
    }
    fn end(self) -> Result<(), NativeError> {
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for Compound<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), NativeError> {
        tuple_field(self, value)
    }
    fn end(self) -> Result<(), NativeError> {
        Ok(())
    }
}

fn tuple_field<T: Serialize + ?Sized>(c: &mut Compound<'_>, value: &T) -> Result<(), NativeError> {
    match c {
        Compound::Tuple(iter) => iter.next().ok_or(NativeError::TupleArity)?.append(value),
        _ => Err(NOT_SEQ),
    }
}

impl<'a> ser::SerializeMap for Compound<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), NativeError> {
        match self {
            Compound::Map { key: k, .. } => k.append(key),
            _ => Err(NOT_SEQ),
        }
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        match self {
            Compound::Map { val, added, .. } => {
                val.append(value)?;
                *added += 1;
                Ok(())
            }
            _ => Err(NOT_SEQ),
        }
    }
    fn end(self) -> Result<(), NativeError> {
        match self {
            Compound::Map {
                offsets,
                cum,
                added,
                ..
            } => {
                *cum += added;
                offsets.put_u64_le(*cum);
                Ok(())
            }
            _ => Err(NOT_SEQ),
        }
    }
}

// ---- ByteSink (fixed-width raw blobs) -------------------------------------

/// Accumulates a value's raw little-endian bytes: integers append their LE
/// bytes, `bytes`/`str` append verbatim, tuples/arrays/newtypes recurse.
/// Used by [`ColumnWriter::append`](super::column::ColumnWriter::append) for
/// `RawExact` (UUID/IPv6/Int256/UInt256) and `Fixed` (FixedString) columns.
pub(crate) struct ByteSink<'a> {
    pub(crate) buf: &'a mut bytes::BytesMut,
}

macro_rules! sink_method {
    ($method:ident, $ty:ty, $put:ident) => {
        fn $method(self, v: $ty) -> Result<(), NativeError> {
            self.buf.$put(v);
            Ok(())
        }
    };
}

impl<'a> Serializer for ByteSink<'a> {
    type Ok = ();
    type Error = NativeError;
    type SerializeSeq = ByteSink<'a>;
    type SerializeTuple = ByteSink<'a>;
    type SerializeTupleStruct = ByteSink<'a>;
    type SerializeTupleVariant = Impossible<(), NativeError>;
    type SerializeMap = Impossible<(), NativeError>;
    type SerializeStruct = Impossible<(), NativeError>;
    type SerializeStructVariant = Impossible<(), NativeError>;

    fn serialize_bool(self, v: bool) -> Result<(), NativeError> {
        self.buf.put_u8(u8::from(v));
        Ok(())
    }
    sink_method!(serialize_i8, i8, put_i8);
    sink_method!(serialize_i16, i16, put_i16_le);
    sink_method!(serialize_i32, i32, put_i32_le);
    sink_method!(serialize_i64, i64, put_i64_le);
    sink_method!(serialize_i128, i128, put_i128_le);
    sink_method!(serialize_u8, u8, put_u8);
    sink_method!(serialize_u16, u16, put_u16_le);
    sink_method!(serialize_u32, u32, put_u32_le);
    sink_method!(serialize_u64, u64, put_u64_le);
    sink_method!(serialize_u128, u128, put_u128_le);
    sink_method!(serialize_f32, f32, put_f32_le);
    sink_method!(serialize_f64, f64, put_f64_le);

    fn serialize_str(self, v: &str) -> Result<(), NativeError> {
        self.buf.put_slice(v.as_bytes());
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), NativeError> {
        self.buf.put_slice(v);
        Ok(())
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), NativeError> {
        value.serialize(self)
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<ByteSink<'a>, NativeError> {
        Ok(self)
    }
    fn serialize_tuple(self, _len: usize) -> Result<ByteSink<'a>, NativeError> {
        Ok(self)
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<ByteSink<'a>, NativeError> {
        Ok(self)
    }

    fn serialize_char(self, v: char) -> Result<(), NativeError> {
        let mut b = [0u8; 4];
        self.buf.put_slice(v.encode_utf8(&mut b).as_bytes());
        Ok(())
    }
    fn serialize_none(self) -> Result<(), NativeError> {
        Err(NativeError::Internal("Option in a fixed-width blob column"))
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _v: &T) -> Result<(), NativeError> {
        Err(NativeError::Internal("Option in a fixed-width blob column"))
    }
    fn serialize_unit(self) -> Result<(), NativeError> {
        Ok(())
    }
    fn serialize_unit_struct(self, _n: &'static str) -> Result<(), NativeError> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
    ) -> Result<(), NativeError> {
        Err(NativeError::Internal(
            "variant in a fixed-width blob column",
        ))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
        _val: &T,
    ) -> Result<(), NativeError> {
        Err(NativeError::Internal(
            "variant in a fixed-width blob column",
        ))
    }
    fn serialize_tuple_variant(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, NativeError> {
        Err(NativeError::Internal(
            "variant in a fixed-width blob column",
        ))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, NativeError> {
        Err(NativeError::Internal("Map in a fixed-width blob column"))
    }
    fn serialize_struct(
        self,
        _n: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, NativeError> {
        Err(NativeError::Internal("struct in a fixed-width blob column"))
    }
    fn serialize_struct_variant(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, NativeError> {
        Err(NativeError::Internal(
            "variant in a fixed-width blob column",
        ))
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a> ser::SerializeSeq for ByteSink<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        value.serialize(ByteSink { buf: self.buf })
    }
    fn end(self) -> Result<(), NativeError> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for ByteSink<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        value.serialize(ByteSink { buf: self.buf })
    }
    fn end(self) -> Result<(), NativeError> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for ByteSink<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        value.serialize(ByteSink { buf: self.buf })
    }
    fn end(self) -> Result<(), NativeError> {
        Ok(())
    }
}

// ---- RowDispatchSer (row struct -> columns) -------------------------------

/// The top-level serializer: routes the row's fields to columns by position.
pub(crate) struct RowDispatchSer<'a> {
    pub(crate) columns: &'a mut [ColumnWriter],
}

impl<'a> Serializer for RowDispatchSer<'a> {
    type Ok = ();
    type Error = NativeError;
    type SerializeSeq = RowFields<'a>;
    type SerializeTuple = RowFields<'a>;
    type SerializeTupleStruct = RowFields<'a>;
    type SerializeTupleVariant = Impossible<(), NativeError>;
    type SerializeMap = Impossible<(), NativeError>;
    type SerializeStruct = RowFields<'a>;
    type SerializeStructVariant = Impossible<(), NativeError>;

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<RowFields<'a>, NativeError> {
        Ok(RowFields::new(self.columns))
    }
    fn serialize_tuple(self, _len: usize) -> Result<RowFields<'a>, NativeError> {
        Ok(RowFields::new(self.columns))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<RowFields<'a>, NativeError> {
        Ok(RowFields::new(self.columns))
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<RowFields<'a>, NativeError> {
        Ok(RowFields::new(self.columns))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), NativeError> {
        value.serialize(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, NativeError> {
        // serde `flatten` lowers a struct to a length-less map; positional
        // columns cannot absorb that.
        Err(NativeError::NotAStruct)
    }

    // Everything else: a row must serialize as a struct/tuple of columns.
    fn serialize_bool(self, _v: bool) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_i8(self, _v: i8) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_i16(self, _v: i16) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_i32(self, _v: i32) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_i64(self, _v: i64) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_i128(self, _v: i128) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_u8(self, _v: u8) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_u16(self, _v: u16) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_u32(self, _v: u32) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_u64(self, _v: u64) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_u128(self, _v: u128) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_f32(self, _v: f32) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_f64(self, _v: f64) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_char(self, _v: char) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_str(self, _v: &str) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_bytes(self, _v: &[u8]) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_none(self) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _v: &T) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_unit(self) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_unit_struct(self, _n: &'static str) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_unit_variant(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
    ) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
        _val: &T,
    ) -> Result<(), NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_tuple_variant(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn serialize_struct_variant(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, NativeError> {
        Err(NativeError::NotAStruct)
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}

/// Routes a row's fields to columns by position.
pub(crate) struct RowFields<'a> {
    iter: std::slice::IterMut<'a, ColumnWriter>,
    total: usize,
    done: usize,
}

impl<'a> RowFields<'a> {
    fn new(columns: &'a mut [ColumnWriter]) -> Self {
        let total = columns.len();
        RowFields {
            iter: columns.iter_mut(),
            total,
            done: 0,
        }
    }

    fn next_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        match self.iter.next() {
            Some(col) => {
                self.done += 1;
                col.append(value)
            }
            None => Err(NativeError::RowArity {
                expected: self.total,
                got: self.done + 1,
            }),
        }
    }

    fn finish(self) -> Result<(), NativeError> {
        if self.done == self.total {
            Ok(())
        } else {
            Err(NativeError::RowArity {
                expected: self.total,
                got: self.done,
            })
        }
    }
}

impl<'a> ser::SerializeStruct for RowFields<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), NativeError> {
        self.next_field(value)
    }
    fn end(self) -> Result<(), NativeError> {
        self.finish()
    }
}

impl<'a> ser::SerializeSeq for RowFields<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        self.next_field(value)
    }
    fn end(self) -> Result<(), NativeError> {
        self.finish()
    }
}

impl<'a> ser::SerializeTuple for RowFields<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        self.next_field(value)
    }
    fn end(self) -> Result<(), NativeError> {
        self.finish()
    }
}

impl<'a> ser::SerializeTupleStruct for RowFields<'a> {
    type Ok = ();
    type Error = NativeError;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), NativeError> {
        self.next_field(value)
    }
    fn end(self) -> Result<(), NativeError> {
        self.finish()
    }
}
