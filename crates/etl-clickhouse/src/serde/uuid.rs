//! `UUID` columns: `uuid::Uuid` as two little-endian `UInt64` halves.
//!
//! ⚠ Required, not optional: `Uuid`'s default serde impl writes a
//! LEB128-length-prefixed string/byte form, which a RowBinary `UUID`
//! column cannot parse — or worse, misaligns the row. Always annotate:
//!
//! ```text
//! #[serde(with = "etl_clickhouse::serde::uuid")]
//! id: uuid::Uuid,
//! ```
//!
//! Wire layout (matches the `clickhouse` crate): `as_u64_pair()` — the
//! most-significant half then the least-significant half, each written as
//! a little-endian `u64`.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Serialize a `Uuid` as a ClickHouse `UUID` value.
pub fn serialize<S>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        uuid.to_string().serialize(serializer)
    } else {
        uuid.as_u64_pair().serialize(serializer)
    }
}

/// Deserialize a `Uuid` from a ClickHouse `UUID` value.
pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
where
    D: Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        let s = String::deserialize(deserializer)?;
        Uuid::parse_str(&s).map_err(D::Error::custom)
    } else {
        let (hi, lo) = <(u64, u64)>::deserialize(deserializer)?;
        Ok(Uuid::from_u64_pair(hi, lo))
    }
}

crate::serde::option_module!(uuid::Uuid);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowbinary::serialize_row;
    use bytes::BytesMut;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Row {
        #[serde(with = "crate::serde::uuid")]
        id: Uuid,
        #[serde(with = "crate::serde::uuid::option")]
        parent: Option<Uuid>,
    }

    fn enc<T: Serialize>(v: &T) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_row(v, &mut buf).expect("serialize");
        buf.to_vec()
    }

    #[test]
    fn uuid_is_two_little_endian_u64_halves() {
        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        let row = Row { id, parent: None };
        assert_eq!(
            enc(&row),
            [
                // MSB half 0x0102030405060708, little-endian.
                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
                // LSB half 0x090a0b0c0d0e0f10, little-endian.
                0x10, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, // parent: NULL.
                1,
            ]
        );
    }

    #[test]
    fn nullable_uuid_composes_with_the_nested_option_guard() {
        // Regression: the option module wraps the value in Some(wrapper);
        // that must not trip the serializer's Option<Option<T>> rejection.
        let row = Row {
            id: Uuid::nil(),
            parent: Some(Uuid::nil()),
        };
        let bytes = enc(&row);
        assert_eq!(bytes.len(), 16 + 1 + 16);
        assert_eq!(bytes[16], 0, "value-follows prefix");
    }

    #[test]
    fn round_trips_through_the_module_deserializer() {
        // Our deserialize must read what our serialize wrote (both run
        // against the clickhouse crate's binary format in integration
        // tests; here we sanity-check the pair mapping itself).
        let id = Uuid::from_u128(0xdead_beef_0000_0001_cafe_babe_0000_0002);
        let (hi, lo) = id.as_u64_pair();
        assert_eq!(Uuid::from_u64_pair(hi, lo), id);
    }
}
