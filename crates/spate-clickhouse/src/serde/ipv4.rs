//! `IPv4` columns: `std::net::Ipv4Addr` as a little-endian `UInt32`.
//!
//! ⚠ Required, not optional: `Ipv4Addr`'s default serde impl emits the
//! four octets big-endian, which a RowBinary `IPv4` column reads
//! successfully as a **wrong address**. Always annotate:
//!
//! ```text
//! #[serde(with = "spate_clickhouse::serde::ipv4")]
//! addr: std::net::Ipv4Addr,
//! ```

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::net::Ipv4Addr;

/// Serialize an `Ipv4Addr` as a ClickHouse `IPv4` (`UInt32`) value.
pub fn serialize<S>(ip: &Ipv4Addr, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        ip.to_string().serialize(serializer)
    } else {
        serializer.serialize_u32(u32::from(*ip))
    }
}

/// Deserialize an `Ipv4Addr` from a ClickHouse `IPv4` (`UInt32`) value.
pub fn deserialize<'de, D>(deserializer: D) -> Result<Ipv4Addr, D::Error>
where
    D: Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    } else {
        u32::deserialize(deserializer).map(Ipv4Addr::from)
    }
}

crate::serde::option_module!(std::net::Ipv4Addr);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowbinary::serialize_row;
    use bytes::BytesMut;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Row {
        #[serde(with = "crate::serde::ipv4")]
        ip: Ipv4Addr,
        #[serde(with = "crate::serde::ipv4::option")]
        opt: Option<Ipv4Addr>,
    }

    fn enc<T: Serialize>(v: &T) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_row(v, &mut buf).expect("serialize");
        buf.to_vec()
    }

    #[test]
    fn ipv4_is_a_little_endian_u32() {
        // 1.2.3.4 = 0x01020304 as a u32 -> LE bytes [4, 3, 2, 1].
        let row = Row {
            ip: Ipv4Addr::new(1, 2, 3, 4),
            opt: Some(Ipv4Addr::new(127, 0, 0, 1)),
        };
        assert_eq!(enc(&row), [4, 3, 2, 1, 0, 1, 0, 0, 127]);
    }

    #[test]
    fn nullable_ipv4_none_is_a_null_prefix() {
        let row = Row {
            ip: Ipv4Addr::new(0, 0, 0, 0),
            opt: None,
        };
        assert_eq!(enc(&row), [0, 0, 0, 0, 1]);
    }

    #[test]
    fn default_ipv4_serde_impl_is_documented_wrong_bytes() {
        // The footgun the module docs warn about: without the attribute,
        // 1.2.3.4 encodes as network-order octets, a valid 4-byte value
        // that an IPv4 column would read as 4.3.2.1.
        assert_eq!(enc(&Ipv4Addr::new(1, 2, 3, 4)), [1, 2, 3, 4]);
    }
}
