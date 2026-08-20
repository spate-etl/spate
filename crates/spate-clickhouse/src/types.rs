//! Wire wrapper types for ClickHouse columns whose RowBinary encoding is a
//! plain integer or byte layout but whose *meaning* the Rust type system
//! should carry: dates, times, decimals, 256-bit integers, and geo shapes.
//!
//! Every wrapper here is a documentation-carrying newtype: the encoding is
//! transparently the inner value, written through
//! `serialize_newtype_struct` so the wrapper's name stays observable to
//! schema validation (see the crate's `schema` support) at zero wire cost.
//! A decimal wrapper's name carries its scale, `Decimal64<4>`, so schema
//! validation can check it against the column's.
//!
//! For `uuid`/`chrono`/`time` ecosystem types, use the field-attribute
//! modules under [`crate::serde`] instead.

use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

/// Defines a doc-carrying wire newtype: `Serialize` writes the inner value
/// through `serialize_newtype_struct` (transparent bytes, observable name),
/// `Deserialize` reads the inner value back.
macro_rules! wire_newtype {
    ($(#[$doc:meta])* $name:ident($inner:ty)) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $name(pub $inner);

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_newtype_struct(stringify!($name), &self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                <$inner as Deserialize<'de>>::deserialize(d).map($name)
            }
        }
    };
}

wire_newtype! {
    /// Days since the Unix epoch, matching a `Date` column's wire
    /// representation (`UInt16`, so 1970-01-01 through 2149-06-06).
    DateDays(u16)
}

wire_newtype! {
    /// Days since the Unix epoch (signed), matching a `Date32` column's
    /// wire representation (`Int32`; the server accepts 1900-01-01 through
    /// 2299-12-31).
    Date32Days(i32)
}

wire_newtype! {
    /// Seconds since the Unix epoch, matching a `DateTime` column's wire
    /// representation (`UInt32`).
    DateTimeSeconds(u32)
}

wire_newtype! {
    /// Seconds since the Unix epoch, matching a `DateTime64(0)` column's
    /// wire representation (`Int64`).
    DateTime64Secs(i64)
}

wire_newtype! {
    /// Milliseconds since the Unix epoch, matching a `DateTime64(3)`
    /// column's wire representation (`Int64`).
    DateTime64Millis(i64)
}

wire_newtype! {
    /// Microseconds since the Unix epoch, matching a `DateTime64(6)`
    /// column's wire representation (`Int64`).
    DateTime64Micros(i64)
}

wire_newtype! {
    /// Nanoseconds since the Unix epoch, matching a `DateTime64(9)`
    /// column's wire representation (`Int64`).
    DateTime64Nanos(i64)
}

wire_newtype! {
    /// Seconds, matching a `Time` column's wire representation (`Int32`;
    /// the server accepts -999:59:59 through 999:59:59).
    TimeSeconds(i32)
}

wire_newtype! {
    /// Seconds, matching a `Time64(0)` column's wire representation
    /// (`Int64`).
    Time64Secs(i64)
}

wire_newtype! {
    /// Milliseconds, matching a `Time64(3)` column's wire representation
    /// (`Int64`).
    Time64Millis(i64)
}

wire_newtype! {
    /// Microseconds, matching a `Time64(6)` column's wire representation
    /// (`Int64`).
    Time64Micros(i64)
}

wire_newtype! {
    /// Nanoseconds, matching a `Time64(9)` column's wire representation
    /// (`Int64`).
    Time64Nanos(i64)
}

/// Defines a pre-scaled decimal wire newtype over a fixed-width integer.
///
/// The scale is a const generic: `Decimal64<2>(150)` is `1.50` in a
/// `Decimal(18, 2)` column. Making the scale part of the *type* keeps
/// mixed-scale arithmetic from compiling; the wire format is the raw
/// little-endian scaled integer.
macro_rules! decimal_newtype {
    ($(#[$doc:meta])* $name:ident($inner:ty), max_scale = $max:literal) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $name<const SCALE: u32>(pub $inner);

        impl<const SCALE: u32> $name<SCALE> {
            /// The scale (fractional digits) this type carries: `raw =
            /// value × 10^SCALE`.
            pub const SCALE: u32 = {
                assert!(
                    SCALE <= $max,
                    concat!(
                        stringify!($name),
                        " scale exceeds the column type's maximum of ",
                        stringify!($max)
                    )
                );
                SCALE
            };

            const NAME_BUF: ([u8; stringify!($name).len() + 4], usize) = {
                // Reading `Self::SCALE` rather than the `SCALE` parameter is what
                // makes an out-of-range scale a compiler error, at monomorphization.
                let scale = Self::SCALE;
                let mut buf = [0u8; stringify!($name).len() + 4];
                let base = stringify!($name).as_bytes();
                let mut i = 0;
                while i < base.len() { buf[i] = base[i]; i += 1; }
                buf[i] = b'<'; i += 1;
                if scale >= 10 { buf[i] = b'0' + (scale / 10) as u8; i += 1; }
                buf[i] = b'0' + (scale % 10) as u8; i += 1;
                buf[i] = b'>'; i += 1;
                (buf, i)
            };

            const NAME: &'static str = {
                let (buf, len) = &Self::NAME_BUF;
                match core::str::from_utf8(buf.split_at(*len).0) {
                    Ok(s) => s,
                    Err(_) => panic!("the name is ASCII"),
                }
            };
        }

        impl<const SCALE: u32> Serialize for $name<SCALE> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_newtype_struct(Self::NAME, &self.0)
            }
        }

        impl<'de, const SCALE: u32> Deserialize<'de> for $name<SCALE> {
            fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let _ = Self::SCALE;
                <$inner as Deserialize<'de>>::deserialize(d).map($name)
            }
        }
    };
}

decimal_newtype! {
    /// A pre-scaled `Decimal32(S)` / `Decimal(P ≤ 9, S)` value: the inner
    /// `i32` is `value × 10^SCALE`, written little-endian.
    Decimal32(i32), max_scale = 9
}

decimal_newtype! {
    /// A pre-scaled `Decimal64(S)` / `Decimal(P ≤ 18, S)` value: the inner
    /// `i64` is `value × 10^SCALE`, written little-endian.
    Decimal64(i64), max_scale = 18
}

decimal_newtype! {
    /// A pre-scaled `Decimal128(S)` / `Decimal(P ≤ 38, S)` value: the
    /// inner `i128` is `value × 10^SCALE`, written little-endian.
    Decimal128(i128), max_scale = 38
}

/// A `rust_decimal::Decimal` could not be converted into a pre-scaled
/// decimal wrapper.
#[cfg(feature = "rust_decimal")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecimalConvertError {
    /// The value cannot be represented at the target scale —
    /// `rust_decimal`'s 96-bit mantissa ran out of precision. (This is
    /// also why `Decimal128` columns with large precision cannot always
    /// be filled from a `rust_decimal::Decimal`.)
    #[error("{value} cannot be rescaled to {scale} fractional digits")]
    Rescale {
        /// The original value.
        value: rust_decimal::Decimal,
        /// The wrapper's scale.
        scale: u32,
    },
    /// The rescaled mantissa overflows the column's integer width.
    #[error("scaled mantissa of {value} overflows the column's integer width")]
    Overflow {
        /// The original value.
        value: rust_decimal::Decimal,
    },
    /// A wrapper's raw value exceeds `rust_decimal`'s range (96-bit
    /// mantissa, scale ≤ 28) when converting back out of the wire type.
    #[error("raw decimal {raw} at scale {scale} exceeds rust_decimal's range")]
    Unrepresentable {
        /// The wrapper's raw pre-scaled integer.
        raw: i128,
        /// The wrapper's scale.
        scale: u32,
    },
}

/// Conversions between `rust_decimal::Decimal` and the pre-scaled
/// wrappers. Rescaling delegates to `rust_decimal::Decimal::rescale`,
/// which rounds midpoints away from zero (`1.505` at scale 2 → `1.51`);
/// conversions are checked, never panicking. Convert in operator code,
/// before the row struct, so the encode hot path stays a plain integer
/// write.
#[cfg(feature = "rust_decimal")]
mod rust_decimal_conv {
    use super::{Decimal32, Decimal64, Decimal128, DecimalConvertError};
    use rust_decimal::Decimal;

    macro_rules! decimal_conversions {
        ($wrapper:ident, $int:ty) => {
            impl<const SCALE: u32> TryFrom<Decimal> for $wrapper<SCALE> {
                type Error = DecimalConvertError;

                fn try_from(value: Decimal) -> Result<Self, Self::Error> {
                    // Compile-time scale bound of the wrapper itself.
                    let _ = Self::SCALE;
                    let mut scaled = value;
                    scaled.rescale(SCALE);
                    if scaled.scale() != SCALE {
                        // rescale clamps when the 96-bit mantissa cannot
                        // carry the requested fractional digits.
                        return Err(DecimalConvertError::Rescale {
                            value,
                            scale: SCALE,
                        });
                    }
                    <$int>::try_from(scaled.mantissa())
                        .map($wrapper)
                        .map_err(|_| DecimalConvertError::Overflow { value })
                }
            }

            impl<const SCALE: u32> TryFrom<$wrapper<SCALE>> for Decimal {
                type Error = DecimalConvertError;

                fn try_from(value: $wrapper<SCALE>) -> Result<Self, Self::Error> {
                    Decimal::try_from_i128_with_scale(i128::from(value.0), SCALE).map_err(|_| {
                        DecimalConvertError::Unrepresentable {
                            raw: i128::from(value.0),
                            scale: SCALE,
                        }
                    })
                }
            }
        };
    }

    decimal_conversions!(Decimal32, i32);
    decimal_conversions!(Decimal64, i64);
    decimal_conversions!(Decimal128, i128);
}

/// An `Int256` column value: 32 bytes, little-endian, two's complement.
///
/// Rust has no native 256-bit integer; this wrapper carries the raw wire
/// layout. Build one from an `i128` (sign-extended) or from little-endian
/// bytes produced by a big-integer crate. Also the documented escape hatch
/// for `Decimal256(S)` columns: store `value × 10^S` as an `Int256`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Int256(pub [u8; 32]);

impl Int256 {
    /// Sign-extend an `i128` into the full 256-bit range.
    #[must_use]
    pub const fn from_i128(v: i128) -> Self {
        let mut bytes = [if v < 0 { 0xff } else { 0x00 }; 32];
        let le = v.to_le_bytes();
        let mut i = 0;
        while i < 16 {
            bytes[i] = le[i];
            i += 1;
        }
        Int256(bytes)
    }

    /// Wrap raw little-endian two's-complement bytes.
    #[must_use]
    pub const fn from_le_bytes(bytes: [u8; 32]) -> Self {
        Int256(bytes)
    }

    /// The raw little-endian two's-complement bytes.
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl Serialize for Int256 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // The [u8; 32] array serializes as a fixed-width tuple: 32 raw
        // bytes, no length prefix (serialize_bytes would LEB128-prefix).
        serializer.serialize_newtype_struct("Int256", &self.0)
    }
}

impl<'de> Deserialize<'de> for Int256 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        <[u8; 32]>::deserialize(d).map(Int256)
    }
}

/// A `UInt256` column value: 32 bytes, little-endian.
///
/// See [`Int256`]; this is the unsigned counterpart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UInt256(pub [u8; 32]);

impl UInt256 {
    /// Zero-extend a `u128` into the full 256-bit range.
    #[must_use]
    pub const fn from_u128(v: u128) -> Self {
        let mut bytes = [0u8; 32];
        let le = v.to_le_bytes();
        let mut i = 0;
        while i < 16 {
            bytes[i] = le[i];
            i += 1;
        }
        UInt256(bytes)
    }

    /// Wrap raw little-endian bytes.
    #[must_use]
    pub const fn from_le_bytes(bytes: [u8; 32]) -> Self {
        UInt256(bytes)
    }

    /// The raw little-endian bytes.
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl Serialize for UInt256 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct("UInt256", &self.0)
    }
}

impl<'de> Deserialize<'de> for UInt256 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        <[u8; 32]>::deserialize(d).map(UInt256)
    }
}

/// A `Point` column: `(x, y)` as two `Float64`s.
pub type Point = (f64, f64);
/// A `Ring` column: a closed sequence of points (`Array(Point)`).
pub type Ring = Vec<Point>;
/// A `LineString` column: an open sequence of points (`Array(Point)`).
pub type LineString = Vec<Point>;
/// A `Polygon` column: an outer ring plus hole rings (`Array(Ring)`).
pub type Polygon = Vec<Ring>;
/// A `MultiLineString` column: `Array(LineString)`.
pub type MultiLineString = Vec<LineString>;
/// A `MultiPolygon` column: `Array(Polygon)`.
pub type MultiPolygon = Vec<Polygon>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowbinary::serialize_row;
    use bytes::BytesMut;

    fn enc<T: Serialize>(v: &T) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_row(v, &mut buf).expect("serialize");
        buf.to_vec()
    }

    #[test]
    fn date_and_time_newtypes_are_transparent_integers() {
        assert_eq!(enc(&DateDays(1)), 1u16.to_le_bytes());
        assert_eq!(enc(&Date32Days(-25567)), (-25567i32).to_le_bytes());
        assert_eq!(enc(&DateTimeSeconds(42)), 42u32.to_le_bytes());
        assert_eq!(enc(&DateTime64Secs(-1)), (-1i64).to_le_bytes());
        assert_eq!(enc(&DateTime64Millis(1_000)), 1_000i64.to_le_bytes());
        assert_eq!(enc(&DateTime64Micros(7)), 7i64.to_le_bytes());
        assert_eq!(enc(&DateTime64Nanos(7)), 7i64.to_le_bytes());
        assert_eq!(enc(&TimeSeconds(-3599)), (-3599i32).to_le_bytes());
        assert_eq!(enc(&Time64Nanos(1)), 1i64.to_le_bytes());
    }

    #[test]
    fn decimals_write_the_raw_scaled_integer() {
        assert_eq!(enc(&Decimal32::<2>(999)), 999i32.to_le_bytes());
        assert_eq!(enc(&Decimal64::<4>(-15_000)), (-15_000i64).to_le_bytes());
        assert_eq!(enc(&Decimal128::<10>(1)), 1i128.to_le_bytes());
        // Scale bounds are compile-time: Decimal32::<10> fails to build
        // (post-monomorphization const assert), so there is no runtime case
        // to test here.
    }

    #[test]
    fn decimal_names_carry_the_scale() {
        assert_eq!(Decimal32::<0>::NAME, "Decimal32<0>");
        assert_eq!(Decimal64::<4>::NAME, "Decimal64<4>");
        assert_eq!(Decimal64::<18>::NAME, "Decimal64<18>");
        assert_eq!(Decimal128::<10>::NAME, "Decimal128<10>");
    }

    #[test]
    fn int256_layouts() {
        assert_eq!(Int256::from_i128(-1).0, [0xff; 32]);
        let one = UInt256::from_u128(1);
        let mut expected = [0u8; 32];
        expected[0] = 1;
        assert_eq!(one.0, expected);

        // Sign extension keeps the i128 value's magnitude in the low half.
        let v = Int256::from_i128(i128::MIN);
        assert_eq!(&v.0[..16], &i128::MIN.to_le_bytes());
        assert_eq!(&v.0[16..], &[0xff; 16]);

        // Wire = the 32 raw bytes, no length prefix.
        assert_eq!(enc(&one), expected);
        assert_eq!(enc(&Int256::from_i128(-1)), [0xff; 32]);
    }

    #[cfg(feature = "rust_decimal")]
    #[test]
    fn rust_decimal_conversions_are_checked_and_round_trip() {
        use rust_decimal::Decimal;

        // 1.505 at scale 2: rescale rounds midpoints away from zero ->
        // 1.51 -> raw 151.
        let d = Decimal::new(1505, 3);
        assert_eq!(Decimal64::<2>::try_from(d), Ok(Decimal64::<2>(151)));

        // Round trip through the wrapper and back.
        let wrapped = Decimal64::<4>::try_from(Decimal::new(-15_000, 4)).unwrap();
        assert_eq!(wrapped, Decimal64::<4>(-15_000));
        assert_eq!(
            Decimal::try_from(wrapped).unwrap(),
            Decimal::new(-15_000, 4)
        );

        // Mantissa wider than the column's integer.
        assert!(matches!(
            Decimal32::<0>::try_from(Decimal::MAX),
            Err(DecimalConvertError::Overflow { .. })
        ));

        // 96-bit mantissa cannot take 10 more fractional digits.
        assert!(matches!(
            Decimal128::<10>::try_from(Decimal::MAX),
            Err(DecimalConvertError::Rescale { scale: 10, .. })
        ));

        // A raw i128 beyond rust_decimal's 96-bit range fails the back
        // conversion instead of panicking.
        assert!(matches!(
            Decimal::try_from(Decimal128::<2>(i128::MAX)),
            Err(DecimalConvertError::Unrepresentable { .. })
        ));
    }

    #[test]
    fn geo_shapes_encode_as_nested_arrays_of_points() {
        let p: Point = (1.0, 2.0);
        let mut expected = 1.0f64.to_le_bytes().to_vec();
        expected.extend_from_slice(&2.0f64.to_le_bytes());
        assert_eq!(enc(&p), expected);

        let ring: Ring = vec![(1.0, 2.0), (3.0, 4.0)];
        let bytes = enc(&ring);
        assert_eq!(bytes[0], 2, "LEB128 point count");
        assert_eq!(bytes.len(), 1 + 2 * 16);

        let poly: Polygon = vec![ring.clone()];
        let bytes = enc(&poly);
        assert_eq!(bytes[0], 1, "one ring");
        assert_eq!(bytes[1], 2, "two points");

        let multi: MultiPolygon = vec![poly];
        assert_eq!(enc(&multi)[0], 1);
    }
}
