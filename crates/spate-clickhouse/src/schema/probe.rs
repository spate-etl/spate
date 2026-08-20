//! A recording serializer: drives a row's `Serialize` impl once and
//! captures each field's *shape* (what the RowBinary serializer would
//! write) instead of bytes, plus a class-based compatibility check
//! between shapes and parsed ClickHouse types.
//!
//! Shape-vs-type checking is permissive: a `u32` field may feed `UInt32`,
//! `DateTime`, or `IPv4`, and unknown server types always pass. One hard
//! rule applies: the `Nullable` prefix byte is a wire-format difference, so
//! `Option`-ness must match exactly in both directions.

use super::typeparse::ChType;
use serde::ser::{self, Serialize};
use std::{fmt, ops::RangeInclusive};

/// What the RowBinary serializer would write for one value.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Shape {
    /// One byte, 0/1.
    Bool,
    /// Little-endian fixed-width integers.
    I8,
    /// See [`Shape::I8`].
    I16,
    /// See [`Shape::I8`].
    I32,
    /// See [`Shape::I8`].
    I64,
    /// See [`Shape::I8`].
    I128,
    /// See [`Shape::I8`].
    U8,
    /// See [`Shape::I8`].
    U16,
    /// See [`Shape::I8`].
    U32,
    /// See [`Shape::I8`].
    U64,
    /// See [`Shape::I8`].
    U128,
    /// Little-endian IEEE 754.
    F32,
    /// See [`Shape::F32`].
    F64,
    /// LEB128 length + bytes (a `String` column).
    Str,
    /// LEB128 length + bytes (a `String` column, via `serde_bytes`).
    Bytes,
    /// Null-prefix byte + inner value (`Nullable`).
    Option(Box<Shape>),
    /// LEB128 count + homogeneous elements (`Array`); inner shape is
    /// [`Shape::Unknown`] when the first record's sequence is empty.
    Seq(Box<Shape>),
    /// LEB128 count + alternating key, value (`Map`).
    Map(Box<Shape>, Box<Shape>),
    /// Elements back-to-back, no length (`Tuple`, `FixedString`, and the
    /// fixed-byte wire types).
    Tuple(Vec<Shape>),
    /// A nested struct, with the same wire as a `Tuple` and field names kept
    /// for diagnostics.
    Struct(Vec<FieldShape>),
    /// A newtype wrapper: transparent bytes, observable name (the hook
    /// for this crate's wire wrapper types).
    Newtype(&'static str, Box<Shape>),
    /// Nothing observable (empty containers, `None`, `Variant` values) —
    /// always compatible.
    Unknown,
}

/// One top-level row field: its name and recorded shape.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FieldShape {
    /// The struct field's name.
    pub(crate) name: &'static str,
    /// The recorded shape.
    pub(crate) shape: Shape,
}

/// Probing failed, so the row type cannot be validated (and could not be
/// encoded either).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum ProbeError {
    /// The payload is not a struct with named fields.
    #[error("row type must be a struct with named fields to validate against columns (got {0})")]
    NotAStruct(&'static str),
    /// A field's type has no RowBinary representation.
    #[error("type has no RowBinary representation: {0}")]
    Unsupported(&'static str),
    /// A sequence did not report its length.
    #[error("sequences must have a known length ahead of time")]
    SequenceMustHaveLength,
    /// `#[serde(flatten)]`, unsupported on row structs.
    #[error("#[serde(flatten)] is not supported by RowBinary rows")]
    Flatten,
    /// An error raised by a `Serialize` implementation.
    #[error("serialize error: {0}")]
    Custom(String),
}

impl ser::Error for ProbeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        ProbeError::Custom(msg.to_string())
    }
}

/// Record the top-level field shapes of one row value.
pub(crate) fn probe_row<T: Serialize + ?Sized>(row: &T) -> Result<Vec<FieldShape>, ProbeError> {
    match row.serialize(ShapeSer)? {
        Shape::Struct(fields) => Ok(fields),
        other => Err(ProbeError::NotAStruct(kind_name(&other))),
    }
}

fn kind_name(shape: &Shape) -> &'static str {
    match shape {
        Shape::Bool => "a bool",
        Shape::I8 | Shape::I16 | Shape::I32 | Shape::I64 | Shape::I128 => "an integer",
        Shape::U8 | Shape::U16 | Shape::U32 | Shape::U64 | Shape::U128 => "an integer",
        Shape::F32 | Shape::F64 => "a float",
        Shape::Str | Shape::Bytes => "a string",
        Shape::Option(_) => "an Option",
        Shape::Seq(_) => "a sequence",
        Shape::Map(..) => "a map",
        Shape::Tuple(_) => "a tuple",
        Shape::Struct(_) => "a struct",
        Shape::Newtype(..) => "a newtype",
        Shape::Unknown => "an opaque value",
    }
}

struct ShapeSer;

impl ser::Serializer for ShapeSer {
    type Ok = Shape;
    type Error = ProbeError;
    type SerializeSeq = SeqShape;
    type SerializeTuple = TupleShape;
    type SerializeTupleStruct = TupleShape;
    type SerializeTupleVariant = ser::Impossible<Shape, ProbeError>;
    type SerializeMap = MapShape;
    type SerializeStruct = StructShape;
    type SerializeStructVariant = ser::Impossible<Shape, ProbeError>;

    fn serialize_bool(self, _: bool) -> Result<Shape, ProbeError> {
        Ok(Shape::Bool)
    }
    fn serialize_i8(self, _: i8) -> Result<Shape, ProbeError> {
        Ok(Shape::I8)
    }
    fn serialize_i16(self, _: i16) -> Result<Shape, ProbeError> {
        Ok(Shape::I16)
    }
    fn serialize_i32(self, _: i32) -> Result<Shape, ProbeError> {
        Ok(Shape::I32)
    }
    fn serialize_i64(self, _: i64) -> Result<Shape, ProbeError> {
        Ok(Shape::I64)
    }
    fn serialize_i128(self, _: i128) -> Result<Shape, ProbeError> {
        Ok(Shape::I128)
    }
    fn serialize_u8(self, _: u8) -> Result<Shape, ProbeError> {
        Ok(Shape::U8)
    }
    fn serialize_u16(self, _: u16) -> Result<Shape, ProbeError> {
        Ok(Shape::U16)
    }
    fn serialize_u32(self, _: u32) -> Result<Shape, ProbeError> {
        Ok(Shape::U32)
    }
    fn serialize_u64(self, _: u64) -> Result<Shape, ProbeError> {
        Ok(Shape::U64)
    }
    fn serialize_u128(self, _: u128) -> Result<Shape, ProbeError> {
        Ok(Shape::U128)
    }
    fn serialize_f32(self, _: f32) -> Result<Shape, ProbeError> {
        Ok(Shape::F32)
    }
    fn serialize_f64(self, _: f64) -> Result<Shape, ProbeError> {
        Ok(Shape::F64)
    }
    fn serialize_char(self, _: char) -> Result<Shape, ProbeError> {
        Err(ProbeError::Unsupported("char"))
    }
    fn serialize_str(self, _: &str) -> Result<Shape, ProbeError> {
        Ok(Shape::Str)
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<Shape, ProbeError> {
        Ok(Shape::Bytes)
    }

    fn serialize_none(self) -> Result<Shape, ProbeError> {
        Ok(Shape::Option(Box::new(Shape::Unknown)))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Shape, ProbeError> {
        let inner = value.serialize(ShapeSer)?;
        if matches!(inner, Shape::Option(_)) {
            return Err(ProbeError::Unsupported("nested Option (Option<Option<T>>)"));
        }
        Ok(Shape::Option(Box::new(inner)))
    }

    fn serialize_unit(self) -> Result<Shape, ProbeError> {
        Err(ProbeError::Unsupported("()"))
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Shape, ProbeError> {
        Err(ProbeError::Unsupported("unit struct"))
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<Shape, ProbeError> {
        Err(ProbeError::Unsupported("unit enum variant"))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Shape, ProbeError> {
        Ok(Shape::Newtype(name, Box::new(value.serialize(ShapeSer)?)))
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Shape, ProbeError> {
        // A Variant column value: the column type parses to Other, which
        // always passes, so nothing further needs recording.
        Ok(Shape::Unknown)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<SeqShape, ProbeError> {
        if len.is_none() {
            return Err(ProbeError::SequenceMustHaveLength);
        }
        Ok(SeqShape { first: None })
    }

    fn serialize_tuple(self, _: usize) -> Result<TupleShape, ProbeError> {
        Ok(TupleShape { elems: Vec::new() })
    }

    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<TupleShape, ProbeError> {
        Ok(TupleShape { elems: Vec::new() })
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, ProbeError> {
        Err(ProbeError::Unsupported("tuple enum variant"))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<MapShape, ProbeError> {
        if len.is_none() {
            // A length-less map is serde's #[serde(flatten)] lowering.
            return Err(ProbeError::Flatten);
        }
        Ok(MapShape {
            key: None,
            value: None,
        })
    }

    fn serialize_struct(self, _: &'static str, len: usize) -> Result<StructShape, ProbeError> {
        Ok(StructShape {
            fields: Vec::with_capacity(len),
        })
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, ProbeError> {
        Err(ProbeError::Unsupported("struct enum variant"))
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

struct SeqShape {
    first: Option<Shape>,
}

impl ser::SerializeSeq for SeqShape {
    type Ok = Shape;
    type Error = ProbeError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), ProbeError> {
        if self.first.is_none() {
            self.first = Some(value.serialize(ShapeSer)?);
        }
        Ok(())
    }

    fn end(self) -> Result<Shape, ProbeError> {
        Ok(Shape::Seq(Box::new(self.first.unwrap_or(Shape::Unknown))))
    }
}

struct TupleShape {
    elems: Vec<Shape>,
}

impl ser::SerializeTuple for TupleShape {
    type Ok = Shape;
    type Error = ProbeError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), ProbeError> {
        self.elems.push(value.serialize(ShapeSer)?);
        Ok(())
    }

    fn end(self) -> Result<Shape, ProbeError> {
        Ok(Shape::Tuple(self.elems))
    }
}

impl ser::SerializeTupleStruct for TupleShape {
    type Ok = Shape;
    type Error = ProbeError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), ProbeError> {
        self.elems.push(value.serialize(ShapeSer)?);
        Ok(())
    }

    fn end(self) -> Result<Shape, ProbeError> {
        Ok(Shape::Tuple(self.elems))
    }
}

struct MapShape {
    key: Option<Shape>,
    value: Option<Shape>,
}

impl ser::SerializeMap for MapShape {
    type Ok = Shape;
    type Error = ProbeError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), ProbeError> {
        if self.key.is_none() {
            self.key = Some(key.serialize(ShapeSer)?);
        }
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), ProbeError> {
        if self.value.is_none() {
            self.value = Some(value.serialize(ShapeSer)?);
        }
        Ok(())
    }

    fn end(self) -> Result<Shape, ProbeError> {
        Ok(Shape::Map(
            Box::new(self.key.unwrap_or(Shape::Unknown)),
            Box::new(self.value.unwrap_or(Shape::Unknown)),
        ))
    }
}

struct StructShape {
    fields: Vec<FieldShape>,
}

impl ser::SerializeStruct for StructShape {
    type Ok = Shape;
    type Error = ProbeError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), ProbeError> {
        self.fields.push(FieldShape {
            name: key,
            shape: value.serialize(ShapeSer)?,
        });
        Ok(())
    }

    fn end(self) -> Result<Shape, ProbeError> {
        Ok(Shape::Struct(self.fields))
    }
}

/// Class-based compatibility between a recorded shape and a parsed
/// column type. Permissive except for the `Nullable` hard rule.
pub(crate) fn compatible(shape: &Shape, ty: &ChType) -> bool {
    // LowCardinality is wire-transparent on insert.
    if let ChType::LowCardinality(inner) = ty {
        return compatible(shape, inner);
    }

    // The Nullable null-prefix byte is a wire-format difference:
    // Option-ness must match exactly, both directions, before any
    // permissiveness applies.
    match (shape, ty) {
        (Shape::Option(inner), ChType::Nullable(t)) => return compatible(inner, t),
        (Shape::Option(_), _) | (_, ChType::Nullable(_)) => return false,
        _ => {}
    }

    if matches!(shape, Shape::Unknown) {
        return true;
    }

    // Unknown server types never fail validation, neither unparsed
    // constructors (Other) nor parameterless names outside the known
    // set. Failing on a type this parser has never heard of would be a
    // false positive.
    if is_unknown_type(ty) {
        return true;
    }

    // Wire wrapper newtypes carry their intended column class in the
    // name; unknown newtype names are transparent.
    if let Shape::Newtype(name, inner) = shape {
        if let Some((precision, scale)) = decimal_wrapper(name) {
            return matches!(ty, ChType::Decimal { precision: p, scale: s }
                if precision.contains(p) && *s == scale);
        }
        return match newtype_rule(name) {
            Some(rule) => rule(ty),
            None => compatible(inner, ty),
        };
    }

    // Geo columns are structural aliases over tuples and arrays.
    if let ChType::Named(n) = ty
        && let Some(equiv) = geo_equiv(n)
    {
        return compatible(shape, &equiv);
    }

    match shape {
        Shape::Bool => is_named(ty, &["Bool", "UInt8", "Int8"]),
        Shape::I8 => is_named(ty, &["Int8"]) || *ty == ChType::Enum8,
        Shape::I16 => is_named(ty, &["Int16"]) || *ty == ChType::Enum16,
        Shape::I32 => {
            is_named(ty, &["Int32", "Date32", "Time"])
                || matches!(ty, ChType::Decimal { precision, .. } if (1..=9).contains(precision))
        }
        Shape::I64 => {
            is_named(ty, &["Int64"])
                || matches!(ty, ChType::DateTime64 { .. } | ChType::Time64 { .. })
                || matches!(ty, ChType::Decimal { precision, .. } if (10..=18).contains(precision))
        }
        Shape::I128 => {
            is_named(ty, &["Int128"])
                || matches!(ty, ChType::Decimal { precision, .. } if (19..=38).contains(precision))
        }
        Shape::U8 => is_named(ty, &["UInt8", "Bool"]),
        Shape::U16 => is_named(ty, &["UInt16", "Date"]),
        Shape::U32 => is_named(ty, &["UInt32", "IPv4"]) || matches!(ty, ChType::DateTime { .. }),
        Shape::U64 => is_named(ty, &["UInt64"]),
        Shape::U128 => is_named(ty, &["UInt128"]),
        Shape::F32 => is_named(ty, &["Float32"]),
        Shape::F64 => is_named(ty, &["Float64"]),
        Shape::Str | Shape::Bytes => is_named(ty, &["String", "JSON"]),
        Shape::Seq(inner) => matches!(ty, ChType::Array(t) if compatible(inner, t)),
        Shape::Map(k, v) => {
            matches!(ty, ChType::Map(kt, vt) if compatible(k, kt) && compatible(v, vt))
        }
        Shape::Tuple(elems) => tuple_compatible(elems, ty),
        Shape::Struct(fields) => {
            let shapes: Vec<Shape> = fields.iter().map(|f| f.shape.clone()).collect();
            tuple_compatible(&shapes, ty)
        }
        // Handled by the early returns above.
        Shape::Option(_) | Shape::Newtype(..) | Shape::Unknown => true,
    }
}

fn tuple_compatible(elems: &[Shape], ty: &ChType) -> bool {
    let all_u8 = || elems.iter().all(|s| matches!(s, Shape::U8));
    match ty {
        ChType::Tuple(ts) => {
            elems.len() == ts.len() && elems.iter().zip(ts).all(|(s, t)| compatible(s, t))
        }
        // [u8; N] probes as N U8 elements, the same wire as FixedString(N).
        ChType::FixedString(n) => elems.len() == *n as usize && all_u8(),
        // Ipv6Addr's default impl probes as 16 U8s.
        ChType::Named(n) if n == "IPv6" => elems.len() == 16 && all_u8(),
        // serde::uuid writes the two 64-bit halves as a tuple.
        ChType::Named(n) if n == "UUID" => elems == [Shape::U64, Shape::U64],
        // Raw 32-byte arrays may feed the 256-bit integers.
        ChType::Named(n) if n == "Int256" || n == "UInt256" => elems.len() == 32 && all_u8(),
        _ => false,
    }
}

/// Parameterless type names the class matrix understands.
const KNOWN_NAMED: &[&str] = &[
    "Bool",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "Int256",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt128",
    "UInt256",
    "Float32",
    "Float64",
    "String",
    "UUID",
    "IPv4",
    "IPv6",
    "Date",
    "Date32",
    "Time",
    "JSON",
    "Point",
    "Ring",
    "LineString",
    "Polygon",
    "MultiLineString",
    "MultiPolygon",
];

fn is_unknown_type(ty: &ChType) -> bool {
    match ty {
        ChType::Other(_) => true,
        ChType::Named(n) => !KNOWN_NAMED.contains(&n.as_str()),
        _ => false,
    }
}

fn is_named(ty: &ChType, names: &[&str]) -> bool {
    matches!(ty, ChType::Named(n) if names.contains(&n.as_str()))
}

fn geo_equiv(name: &str) -> Option<ChType> {
    fn point() -> ChType {
        ChType::Tuple(vec![
            ChType::Named("Float64".into()),
            ChType::Named("Float64".into()),
        ])
    }
    fn array(t: ChType) -> ChType {
        ChType::Array(Box::new(t))
    }
    Some(match name {
        "Point" => point(),
        "Ring" | "LineString" => array(point()),
        "Polygon" | "MultiLineString" => array(array(point())),
        "MultiPolygon" => array(array(array(point()))),
        _ => return None,
    })
}

/// The registration table mapping this crate's wire wrapper names (as
/// written by their `serialize_newtype_struct` calls) to the column
/// classes they intend. New wrappers in `types.rs` register here, apart
/// from the decimal wrappers, whose names carry a scale and are matched
/// by [`decimal_wrapper`]. Unknown names stay transparent, so
/// user-defined newtypes keep working.
fn newtype_rule(name: &str) -> Option<fn(&ChType) -> bool> {
    Some(match name {
        "DateDays" => |ty| is_named(ty, &["Date", "UInt16"]),
        "Date32Days" => |ty| is_named(ty, &["Date32", "Int32"]),
        "DateTimeSeconds" => {
            |ty| is_named(ty, &["UInt32"]) || matches!(ty, ChType::DateTime { .. })
        }
        "DateTime64Secs" => {
            |ty| is_named(ty, &["Int64"]) || matches!(ty, ChType::DateTime64 { precision: 0, .. })
        }
        "DateTime64Millis" => {
            |ty| is_named(ty, &["Int64"]) || matches!(ty, ChType::DateTime64 { precision: 3, .. })
        }
        "DateTime64Micros" => {
            |ty| is_named(ty, &["Int64"]) || matches!(ty, ChType::DateTime64 { precision: 6, .. })
        }
        "DateTime64Nanos" => {
            |ty| is_named(ty, &["Int64"]) || matches!(ty, ChType::DateTime64 { precision: 9, .. })
        }
        "TimeSeconds" => |ty| is_named(ty, &["Time", "Int32"]),
        "Time64Secs" => {
            |ty| is_named(ty, &["Int64"]) || matches!(ty, ChType::Time64 { precision: 0 })
        }
        "Time64Millis" => {
            |ty| is_named(ty, &["Int64"]) || matches!(ty, ChType::Time64 { precision: 3 })
        }
        "Time64Micros" => {
            |ty| is_named(ty, &["Int64"]) || matches!(ty, ChType::Time64 { precision: 6 })
        }
        "Time64Nanos" => {
            |ty| is_named(ty, &["Int64"]) || matches!(ty, ChType::Time64 { precision: 9 })
        }
        "Int256" => |ty| is_named(ty, &["Int256"]),
        "UInt256" => |ty| is_named(ty, &["UInt256"]),
        _ => return None,
    })
}

/// A decimal wrapper's name carries its scale (`Decimal64<4>`): the backing
/// integer's precision band, and the scale the field declares.
fn decimal_wrapper(name: &str) -> Option<(RangeInclusive<u8>, u8)> {
    let (base, rest) = name.split_once('<')?;
    let scale: u8 = rest.strip_suffix('>')?.parse().ok()?;
    let precision = match base {
        "Decimal32" => 1..=9,
        "Decimal64" => 10..=18,
        "Decimal128" => 19..=38,
        _ => return None,
    };
    Some((precision, scale))
}

#[cfg(test)]
mod tests {
    use super::super::typeparse::parse;
    use super::*;
    use crate::types::{DateTime64Millis, Decimal32, Decimal64, Decimal128, Int256, UInt256};
    use serde::Serialize;

    fn ok(shape: &Shape, ty: &str) -> bool {
        compatible(shape, &parse(ty))
    }

    #[test]
    fn probes_a_kitchen_sink_struct() {
        #[derive(Serialize)]
        struct Nested {
            x: f64,
            y: f64,
        }
        #[derive(Serialize)]
        struct Row {
            id: u64,
            name: String,
            score: Option<f64>,
            tags: Vec<String>,
            attrs: std::collections::BTreeMap<String, u32>,
            when: DateTime64Millis,
            point: Nested,
            fixed: [u8; 4],
            empty: Vec<u8>,
        }
        let fields = probe_row(&Row {
            id: 1,
            name: "n".into(),
            score: None,
            tags: vec!["a".into()],
            attrs: [("k".to_string(), 1u32)].into(),
            when: DateTime64Millis(0),
            point: Nested { x: 0.0, y: 0.0 },
            fixed: [0; 4],
            empty: vec![],
        })
        .unwrap();

        let names: Vec<_> = fields.iter().map(|f| f.name).collect();
        assert_eq!(
            names,
            [
                "id", "name", "score", "tags", "attrs", "when", "point", "fixed", "empty"
            ]
        );
        assert_eq!(fields[0].shape, Shape::U64);
        assert_eq!(fields[1].shape, Shape::Str);
        assert_eq!(fields[2].shape, Shape::Option(Box::new(Shape::Unknown)));
        assert_eq!(fields[3].shape, Shape::Seq(Box::new(Shape::Str)));
        assert_eq!(
            fields[4].shape,
            Shape::Map(Box::new(Shape::Str), Box::new(Shape::U32))
        );
        assert_eq!(
            fields[5].shape,
            Shape::Newtype("DateTime64Millis", Box::new(Shape::I64))
        );
        assert!(matches!(fields[6].shape, Shape::Struct(_)));
        assert_eq!(fields[7].shape, Shape::Tuple(vec![Shape::U8; 4]));
        assert_eq!(fields[8].shape, Shape::Seq(Box::new(Shape::Unknown)));
    }

    #[test]
    fn skip_attributes_shorten_the_probed_row() {
        // The rowbinary serializer cannot detect #[serde(skip)]; the probe
        // sees the reduced field list, so the count check catches it.
        #[derive(Serialize)]
        struct Row {
            id: u8,
            #[serde(skip_serializing_if = "Option::is_none")]
            score: Option<u8>,
            name: String,
        }
        let fields = probe_row(&Row {
            id: 1,
            score: None,
            name: "x".into(),
        })
        .unwrap();
        assert_eq!(fields.len(), 2, "the skipped field is absent");
    }

    #[test]
    fn non_struct_rows_are_rejected() {
        assert!(matches!(
            probe_row(&42u64),
            Err(ProbeError::NotAStruct("an integer"))
        ));
        assert!(matches!(
            probe_row(&(1u8, 2u8)),
            Err(ProbeError::NotAStruct(_))
        ));
    }

    #[test]
    fn nullable_is_a_hard_rule_in_both_directions() {
        let opt_u32 = Shape::Option(Box::new(Shape::U32));
        assert!(ok(&opt_u32, "Nullable(UInt32)"));
        assert!(!ok(&opt_u32, "UInt32"), "Option vs plain column");
        assert!(!ok(&Shape::U32, "Nullable(UInt32)"), "plain vs Nullable");
        // Even against unknown types, Option-ness must not silently pass.
        assert!(!ok(&opt_u32, "SomeFutureType(1)"));
        // But Nullable of an unknown inner type is fine.
        assert!(ok(&opt_u32, "Nullable(SomeFutureType(1))"));
    }

    #[test]
    fn class_matrix_is_permissive_where_the_wire_matches() {
        assert!(ok(&Shape::U32, "UInt32"));
        assert!(ok(&Shape::U32, "DateTime"));
        assert!(ok(&Shape::U32, "DateTime('UTC')"));
        assert!(ok(&Shape::U32, "IPv4"));
        assert!(!ok(&Shape::U32, "Int32"));
        assert!(!ok(&Shape::I32, "DateTime"));
        assert!(ok(&Shape::I64, "DateTime64(3)"));
        assert!(ok(&Shape::I64, "Decimal(18, 4)"));
        assert!(
            !ok(&Shape::I64, "Decimal(9, 2)"),
            "Decimal(9,_) is Int32-wide"
        );
        assert!(ok(&Shape::I32, "Decimal(9, 2)"));
        assert!(ok(&Shape::I128, "Decimal(38, 10)"));
        assert!(ok(&Shape::U16, "Date"));
        assert!(ok(&Shape::I8, "Enum8('a' = 1)"));
        assert!(ok(&Shape::I16, "Enum16('a' = 1)"));
        assert!(!ok(&Shape::I8, "Enum16('a' = 1)"));
        assert!(ok(&Shape::Str, "String"));
        assert!(ok(&Shape::Str, "JSON"));
        assert!(ok(&Shape::Str, "LowCardinality(String)"));
        assert!(!ok(&Shape::Str, "FixedString(8)"), "Str is length-prefixed");
        assert!(ok(&Shape::Bool, "Bool"));
        assert!(ok(&Shape::Unknown, "AggregateFunction(sum, UInt64)"));
        assert!(
            ok(&Shape::U64, "SomeFutureType"),
            "unknown named types pass"
        );
    }

    #[test]
    fn containers_recurse() {
        let seq_str = Shape::Seq(Box::new(Shape::Str));
        assert!(ok(&seq_str, "Array(String)"));
        assert!(ok(&seq_str, "Array(LowCardinality(String))"));
        assert!(!ok(&seq_str, "Array(UInt32)"));
        assert!(!ok(&seq_str, "String"));

        let map = Shape::Map(Box::new(Shape::Str), Box::new(Shape::U64));
        assert!(ok(&map, "Map(String, UInt64)"));
        assert!(!ok(&map, "Map(String, Int64)"));

        let empty = Shape::Seq(Box::new(Shape::Unknown));
        assert!(
            ok(&empty, "Array(Tuple(UInt32, String))"),
            "empty seq passes"
        );

        let opt_in_array = Shape::Seq(Box::new(Shape::Option(Box::new(Shape::Str))));
        assert!(ok(&opt_in_array, "Array(Nullable(String))"));
        assert!(!ok(&opt_in_array, "Array(String)"));
    }

    #[test]
    fn fixed_byte_wire_types_match_tuples_of_u8() {
        let bytes16 = Shape::Tuple(vec![Shape::U8; 16]);
        assert!(ok(&bytes16, "IPv6"));
        assert!(ok(&bytes16, "FixedString(16)"));
        assert!(!ok(&bytes16, "FixedString(8)"));
        assert!(!ok(&Shape::Tuple(vec![Shape::U8; 15]), "IPv6"));

        let uuid = Shape::Tuple(vec![Shape::U64, Shape::U64]);
        assert!(ok(&uuid, "UUID"));
        assert!(!ok(&bytes16, "UUID"), "UUID is two u64 halves, not bytes");
    }

    #[test]
    fn wire_wrapper_newtypes_check_their_column_class() {
        let millis = probe_row(&{
            #[derive(Serialize)]
            struct R {
                t: DateTime64Millis,
            }
            R {
                t: DateTime64Millis(0),
            }
        })
        .unwrap();
        assert!(compatible(&millis[0].shape, &parse("DateTime64(3)")));
        assert!(compatible(&millis[0].shape, &parse("Int64")));
        assert!(
            !compatible(&millis[0].shape, &parse("DateTime64(6)")),
            "precision mismatch is exactly the bug this catches"
        );

        #[derive(Serialize)]
        struct D {
            d: Decimal64<4>,
            big: Int256,
            ubig: UInt256,
        }
        let fields = probe_row(&D {
            d: Decimal64::<4>(1),
            big: Int256::from_i128(1),
            ubig: UInt256::from_u128(1),
        })
        .unwrap();
        assert!(compatible(&fields[0].shape, &parse("Decimal(18, 4)")));
        assert!(!compatible(&fields[0].shape, &parse("Decimal(9, 4)")));
        assert!(compatible(&fields[1].shape, &parse("Int256")));
        assert!(!compatible(&fields[1].shape, &parse("UInt256")));
        assert!(compatible(&fields[2].shape, &parse("UInt256")));

        // Unknown newtype names fall through to the inner shape.
        #[derive(Serialize)]
        struct MyWrapper(u32);
        #[derive(Serialize)]
        struct R2 {
            w: MyWrapper,
        }
        let fields = probe_row(&R2 { w: MyWrapper(1) }).unwrap();
        assert!(compatible(&fields[0].shape, &parse("UInt32")));
        assert!(compatible(&fields[0].shape, &parse("DateTime")));
    }

    #[test]
    fn decimal_wrapper_scale_must_match_the_column() {
        #[derive(Serialize)]
        struct D {
            small: Decimal32<2>,
            medium: Decimal64<4>,
            large: Decimal128<10>,
        }
        let fields = probe_row(&D {
            small: Decimal32::<2>(1),
            medium: Decimal64::<4>(1),
            large: Decimal128::<10>(1),
        })
        .unwrap();

        assert!(compatible(&fields[1].shape, &parse("Decimal(18, 4)")));
        assert!(
            !compatible(&fields[1].shape, &parse("Decimal(18, 2)")),
            "the scale must agree, not just the width"
        );

        // The Decimal64(S) sugar carries the same precision.
        assert!(compatible(&fields[1].shape, &parse("Decimal64(4)")));
        assert!(!compatible(&fields[1].shape, &parse("Decimal64(2)")));

        assert!(compatible(&fields[0].shape, &parse("Decimal(9, 2)")));
        assert!(
            !compatible(&fields[0].shape, &parse("Decimal(18, 2)")),
            "Decimal32 is Int32-wide"
        );
        assert!(
            compatible(&fields[2].shape, &parse("Decimal(38, 10)")),
            "a two-digit scale survives the name"
        );
        assert!(
            !compatible(&fields[1].shape, &parse("Nullable(Decimal(18, 4))")),
            "the Nullable prefix byte needs an Option field"
        );
    }

    #[test]
    fn geo_columns_match_their_structural_shapes() {
        let point = Shape::Tuple(vec![Shape::F64, Shape::F64]);
        assert!(ok(&point, "Point"));
        let ring = Shape::Seq(Box::new(point.clone()));
        assert!(ok(&ring, "Ring"));
        assert!(ok(&ring, "LineString"));
        let polygon = Shape::Seq(Box::new(ring.clone()));
        assert!(ok(&polygon, "Polygon"));
        let multi = Shape::Seq(Box::new(polygon.clone()));
        assert!(ok(&multi, "MultiPolygon"));
        assert!(!ok(&point, "Ring"));
    }

    #[test]
    fn nested_structs_check_as_tuple_columns() {
        #[derive(Serialize)]
        struct Inner {
            a: u32,
            b: String,
        }
        #[derive(Serialize)]
        struct Row {
            n: Inner,
        }
        let fields = probe_row(&Row {
            n: Inner {
                a: 1,
                b: "x".into(),
            },
        })
        .unwrap();
        assert!(compatible(
            &fields[0].shape,
            &parse("Tuple(UInt32, String)")
        ));
        assert!(compatible(
            &fields[0].shape,
            &parse("Tuple(a UInt32, b String)")
        ));
        assert!(!compatible(&fields[0].shape, &parse("Tuple(UInt32)")));
    }
}
