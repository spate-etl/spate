//! The columnar layout engine: one [`ColumnWriter`] per column, built from
//! its parsed [`ChType`], accumulating per-record values into per-column
//! buffers and serializing them as a Native block's column streams.
//!
//! Each writer exposes four operations:
//! - [`ColumnWriter::build`] — construct recursively from a `ChType`.
//! - [`ColumnWriter::append`] — fold one record's value in (via the
//!   [`ColumnSerializer`](super::dispatch::ColumnSerializer)).
//! - [`ColumnWriter::push_default`] — append one default entry (the
//!   placeholder a `Nullable` dense stream needs at a NULL).
//! - [`ColumnWriter::write_prefix`] / [`ColumnWriter::write_data`] — the two
//!   finalize phases (state prefix first, then the column data), which make
//!   the composite prefix-ordering rule fall out for free.
//!
//! There is no per-record rollback: a mid-row failure poisons the block and
//! the encoder refuses to finalize it (see [`NativeEncoder`](super::NativeEncoder)),
//! so a failed record never leaves a half-written block on the wire.

use super::NativeError;
use super::lowcard::LowCard;
use crate::schema::typeparse::ChType;
use bytes::{BufMut, BytesMut};

/// A fixed-width little-endian numeric leaf. Temporal, decimal, enum, and
/// `IPv4` columns all reduce to one of these (their meaning lives in the
/// column's type-name string, not the wire bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScalarKind {
    Bool,
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
}

impl ScalarKind {
    pub(crate) fn width(self) -> usize {
        match self {
            ScalarKind::Bool | ScalarKind::I8 | ScalarKind::U8 => 1,
            ScalarKind::I16 | ScalarKind::U16 => 2,
            ScalarKind::I32 | ScalarKind::U32 | ScalarKind::F32 => 4,
            ScalarKind::I64 | ScalarKind::U64 | ScalarKind::F64 => 8,
            ScalarKind::I128 | ScalarKind::U128 => 16,
        }
    }

    /// A short label for type-mismatch errors.
    pub(crate) fn name(self) -> &'static str {
        match self {
            ScalarKind::Bool => "Bool",
            ScalarKind::I8 => "Int8",
            ScalarKind::I16 => "Int16",
            ScalarKind::I32 => "Int32-width",
            ScalarKind::I64 => "Int64-width",
            ScalarKind::I128 => "Int128-width",
            ScalarKind::U8 => "UInt8",
            ScalarKind::U16 => "UInt16-width",
            ScalarKind::U32 => "UInt32-width",
            ScalarKind::U64 => "UInt64",
            ScalarKind::U128 => "UInt128",
            ScalarKind::F32 => "Float32",
            ScalarKind::F64 => "Float64",
        }
    }
}

/// A column-level writer. Built once per column, reused block after block
/// (finalize resets it).
pub(crate) enum ColumnWriter {
    /// Fixed-width LE numeric leaf.
    Scalar { kind: ScalarKind, buf: BytesMut },
    /// `String`: `[VarUInt len][bytes]` per row.
    Str(BytesMut),
    /// `FixedString(N)`: exactly `width` bytes per row (short values are
    /// right-padded with NUL; over-long is a record error).
    Fixed { width: usize, buf: BytesMut },
    /// A fixed-width blob assembled from the value's raw bytes: `UUID`
    /// (two LE u64 = 16), `IPv6` (16 network-order octets), `Int256` /
    /// `UInt256` (32 LE). Exactly `width` bytes per row.
    RawExact { width: usize, buf: BytesMut },
    /// `Nullable(T)`: a leading null-map stream (one byte per row) then the
    /// inner's dense value stream, which carries a placeholder at each NULL.
    Nullable {
        nullmap: BytesMut,
        inner: Box<ColumnWriter>,
    },
    /// `Array(T)`: cumulative `UInt64` end-offsets then the flattened inner
    /// values. Offsets are accumulated as their final LE wire bytes (`cum`
    /// tracks the running total the next offset commits).
    Array {
        offsets: BytesMut,
        cum: u64,
        inner: Box<ColumnWriter>,
    },
    /// `Map(K, V)`, byte-identical to `Array(Tuple(K, V))`: cumulative
    /// offsets (as LE wire bytes) then the (separate) key and value streams.
    Map {
        offsets: BytesMut,
        cum: u64,
        key: Box<ColumnWriter>,
        val: Box<ColumnWriter>,
    },
    /// `Tuple(T1..Tn)` (and nested structs / geo `Point`): one stream per
    /// element, in order.
    Tuple(Vec<ColumnWriter>),
    /// `LowCardinality(T)`: a per-block dictionary + width-selected keys.
    LowCard(LowCard),
}

impl ColumnWriter {
    /// Build a writer for `ty`, or return the offending type's description
    /// for an unsupported type (the caller attaches the column name).
    pub(crate) fn build(ty: &ChType) -> Result<ColumnWriter, String> {
        Ok(match ty {
            ChType::Named(name) => Self::build_named(name)?,
            ChType::Nullable(inner) => ColumnWriter::Nullable {
                nullmap: BytesMut::new(),
                inner: Box::new(Self::build(inner)?),
            },
            ChType::LowCardinality(inner) => ColumnWriter::LowCard(LowCard::build(inner)?),
            ChType::Array(inner) => ColumnWriter::Array {
                offsets: BytesMut::new(),
                cum: 0,
                inner: Box::new(Self::build(inner)?),
            },
            ChType::Map(k, v) => ColumnWriter::Map {
                offsets: BytesMut::new(),
                cum: 0,
                key: Box::new(Self::build(k)?),
                val: Box::new(Self::build(v)?),
            },
            ChType::Tuple(elems) => ColumnWriter::Tuple(
                elems
                    .iter()
                    .map(Self::build)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            ChType::FixedString(n) => ColumnWriter::Fixed {
                width: *n as usize,
                buf: BytesMut::new(),
            },
            ChType::DateTime { .. } => Self::scalar(ScalarKind::U32),
            ChType::DateTime64 { .. } => Self::scalar(ScalarKind::I64),
            ChType::Decimal { precision, .. } => match precision {
                0..=9 => Self::scalar(ScalarKind::I32),
                10..=18 => Self::scalar(ScalarKind::I64),
                19..=38 => Self::scalar(ScalarKind::I128),
                _ => return Err(Self::unsupported(ty)),
            },
            ChType::Enum8 => Self::scalar(ScalarKind::I8),
            ChType::Enum16 => Self::scalar(ScalarKind::I16),
            ChType::Time64 { .. } | ChType::Other(_) => return Err(Self::unsupported(ty)),
        })
    }

    fn build_named(name: &str) -> Result<ColumnWriter, String> {
        Ok(match name {
            "Bool" => Self::scalar(ScalarKind::Bool),
            "Int8" => Self::scalar(ScalarKind::I8),
            "Int16" => Self::scalar(ScalarKind::I16),
            "Int32" => Self::scalar(ScalarKind::I32),
            "Int64" => Self::scalar(ScalarKind::I64),
            "Int128" => Self::scalar(ScalarKind::I128),
            "UInt8" => Self::scalar(ScalarKind::U8),
            "UInt16" => Self::scalar(ScalarKind::U16),
            "UInt32" => Self::scalar(ScalarKind::U32),
            "UInt64" => Self::scalar(ScalarKind::U64),
            "UInt128" => Self::scalar(ScalarKind::U128),
            "Float32" => Self::scalar(ScalarKind::F32),
            "Float64" => Self::scalar(ScalarKind::F64),
            "String" => ColumnWriter::Str(BytesMut::new()),
            "IPv4" => Self::scalar(ScalarKind::U32),
            "Date" => Self::scalar(ScalarKind::U16),
            "Date32" => Self::scalar(ScalarKind::I32),
            "UUID" | "IPv6" => ColumnWriter::RawExact {
                width: 16,
                buf: BytesMut::new(),
            },
            "Int256" | "UInt256" => ColumnWriter::RawExact {
                width: 32,
                buf: BytesMut::new(),
            },
            // Geo aliases (should the server report the named form rather
            // than the expanded Tuple/Array spelling): Point = Tuple(F64,F64),
            // Ring/LineString = Array(Point), Polygon/MultiLineString =
            // Array(Array(Point)), MultiPolygon = Array(Array(Array(Point))).
            "Point" => Self::point(),
            "Ring" | "LineString" => Self::array(Self::point()),
            "Polygon" | "MultiLineString" => Self::array(Self::array(Self::point())),
            "MultiPolygon" => Self::array(Self::array(Self::array(Self::point()))),
            other => return Err(other.to_string()),
        })
    }

    fn array(inner: ColumnWriter) -> ColumnWriter {
        ColumnWriter::Array {
            offsets: BytesMut::new(),
            cum: 0,
            inner: Box::new(inner),
        }
    }

    fn scalar(kind: ScalarKind) -> ColumnWriter {
        ColumnWriter::Scalar {
            kind,
            buf: BytesMut::new(),
        }
    }

    fn point() -> ColumnWriter {
        ColumnWriter::Tuple(vec![
            Self::scalar(ScalarKind::F64),
            Self::scalar(ScalarKind::F64),
        ])
    }

    fn unsupported(ty: &ChType) -> String {
        format!("{ty:?}")
    }

    /// Fold one record's value into this column's buffers.
    #[inline]
    pub(crate) fn append<V: serde::Serialize + ?Sized>(
        &mut self,
        value: &V,
    ) -> Result<(), NativeError> {
        match self {
            // Fixed-width blobs (UUID/IPv6/Int256/UInt256): the value's raw
            // LE bytes must total exactly `width`.
            ColumnWriter::RawExact { width, buf } => {
                let before = buf.len();
                value.serialize(super::dispatch::ByteSink { buf })?;
                let got = buf.len() - before;
                if got != *width {
                    buf.truncate(before);
                    return Err(NativeError::RawWidth {
                        expected: *width,
                        got,
                    });
                }
                Ok(())
            }
            // FixedString(N): accept up to N raw bytes, right-pad with NUL.
            ColumnWriter::Fixed { width, buf } => {
                let before = buf.len();
                value.serialize(super::dispatch::ByteSink { buf })?;
                let got = buf.len() - before;
                if got > *width {
                    buf.truncate(before);
                    return Err(NativeError::FixedTooLong { width: *width, got });
                }
                buf.put_bytes(0, *width - got);
                Ok(())
            }
            _ => value.serialize(super::dispatch::ColumnSerializer { writer: self }),
        }
    }

    /// Append one default-valued entry, the placeholder a `Nullable` dense
    /// stream writes at a NULL, recursively.
    pub(crate) fn push_default(&mut self) {
        match self {
            ColumnWriter::Scalar { kind, buf } => buf.put_bytes(0, kind.width()),
            ColumnWriter::Str(buf) => super::leaf::put_leb128(buf, 0),
            ColumnWriter::Fixed { width, buf } | ColumnWriter::RawExact { width, buf } => {
                buf.put_bytes(0, *width);
            }
            ColumnWriter::Nullable { nullmap, inner } => {
                // A default Nullable value is NULL.
                nullmap.put_u8(1);
                inner.push_default();
            }
            ColumnWriter::Array { offsets, cum, .. } => offsets.put_u64_le(*cum),
            ColumnWriter::Map { offsets, cum, .. } => offsets.put_u64_le(*cum),
            ColumnWriter::Tuple(elems) => elems.iter_mut().for_each(ColumnWriter::push_default),
            ColumnWriter::LowCard(lc) => lc.push_default(),
        }
    }

    /// Emit the column's serialization-state prefix (recursing inner-first),
    /// then reset for the next block. For a versioned inner (LowCardinality)
    /// this precedes the wrapper's own data, the ordering the spec requires.
    pub(crate) fn write_prefix(&self, out: &mut BytesMut) {
        match self {
            ColumnWriter::Scalar { .. }
            | ColumnWriter::Str(_)
            | ColumnWriter::Fixed { .. }
            | ColumnWriter::RawExact { .. } => {}
            ColumnWriter::Nullable { inner, .. } => inner.write_prefix(out),
            ColumnWriter::Array { inner, .. } => inner.write_prefix(out),
            ColumnWriter::Map { key, val, .. } => {
                key.write_prefix(out);
                val.write_prefix(out);
            }
            ColumnWriter::Tuple(elems) => elems.iter().for_each(|e| e.write_prefix(out)),
            ColumnWriter::LowCard(lc) => lc.write_prefix(out),
        }
    }

    /// Emit the column's data streams (after all prefixes).
    pub(crate) fn write_data(&self, out: &mut BytesMut) {
        match self {
            ColumnWriter::Scalar { buf, .. }
            | ColumnWriter::Str(buf)
            | ColumnWriter::Fixed { buf, .. }
            | ColumnWriter::RawExact { buf, .. } => out.put_slice(buf),
            ColumnWriter::Nullable { nullmap, inner } => {
                out.put_slice(nullmap);
                inner.write_data(out);
            }
            ColumnWriter::Array { offsets, inner, .. } => {
                out.put_slice(offsets);
                inner.write_data(out);
            }
            ColumnWriter::Map {
                offsets, key, val, ..
            } => {
                out.put_slice(offsets);
                key.write_data(out);
                val.write_data(out);
            }
            ColumnWriter::Tuple(elems) => elems.iter().for_each(|e| e.write_data(out)),
            ColumnWriter::LowCard(lc) => lc.write_data(out),
        }
    }

    /// Approximate bytes buffered so far (for the chunk-size threshold).
    pub(crate) fn byte_len(&self) -> usize {
        match self {
            ColumnWriter::Scalar { buf, .. }
            | ColumnWriter::Str(buf)
            | ColumnWriter::Fixed { buf, .. }
            | ColumnWriter::RawExact { buf, .. } => buf.len(),
            ColumnWriter::Nullable { nullmap, inner } => nullmap.len() + inner.byte_len(),
            ColumnWriter::Array { offsets, inner, .. } => offsets.len() + inner.byte_len(),
            ColumnWriter::Map {
                offsets, key, val, ..
            } => offsets.len() + key.byte_len() + val.byte_len(),
            ColumnWriter::Tuple(elems) => elems.iter().map(ColumnWriter::byte_len).sum(),
            ColumnWriter::LowCard(lc) => lc.byte_len(),
        }
    }

    /// Clear all accumulated data, readying the writer for the next block.
    pub(crate) fn reset(&mut self) {
        match self {
            ColumnWriter::Scalar { buf, .. }
            | ColumnWriter::Str(buf)
            | ColumnWriter::Fixed { buf, .. }
            | ColumnWriter::RawExact { buf, .. } => buf.clear(),
            ColumnWriter::Nullable { nullmap, inner } => {
                nullmap.clear();
                inner.reset();
            }
            ColumnWriter::Array {
                offsets,
                cum,
                inner,
            } => {
                offsets.clear();
                *cum = 0;
                inner.reset();
            }
            ColumnWriter::Map {
                offsets,
                cum,
                key,
                val,
            } => {
                offsets.clear();
                *cum = 0;
                key.reset();
                val.reset();
            }
            ColumnWriter::Tuple(elems) => elems.iter_mut().for_each(ColumnWriter::reset),
            ColumnWriter::LowCard(lc) => lc.reset(),
        }
    }
}
