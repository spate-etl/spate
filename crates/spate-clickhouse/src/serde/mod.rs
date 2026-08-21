//! Field-attribute serde modules for ClickHouse column types, mirroring
//! the `clickhouse` crate's `clickhouse::serde::*` modules.
//!
//! A row struct written for the `clickhouse` crate's `Row` derive ports to
//! this sink by swapping the attribute path:
//!
//! ```text
//! #[serde(with = "clickhouse::serde::ipv4")]        // clickhouse crate
//! #[serde(with = "spate_clickhouse::serde::ipv4")]    // this crate
//! ```
//!
//! Every module provides `serialize`, `deserialize`, and a nested `option`
//! submodule for `Option<T>` fields against `Nullable(...)` columns:
//!
//! ```text
//! #[serde(with = "spate_clickhouse::serde::ipv4::option")]
//! addr: Option<std::net::Ipv4Addr>,
//! ```
//!
//! ## ⚠ Omitting these attributes is silent data corruption
//!
//! `uuid::Uuid`, `std::net::Ipv4Addr`, and the `chrono`/`time` date,
//! datetime and duration types all have *default* serde representations
//! that serialize **successfully into the wrong bytes** for their
//! ClickHouse columns. `Ipv4Addr` emits big-endian octets where `IPv4` is
//! a little-endian `UInt32`, and both date/time crates emit string forms
//! where every date and time column is a fixed-width integer. Omitting the
//! attribute is silent data corruption, not an error. The sink's opt-in
//! schema validation (`validate_schema: full`) catches the shape-level
//! cases; the docs table in [`crate::rowbinary`] is the authoritative
//! mapping.
//!
//! Wire semantics are rewritten from `clickhouse` 0.15.1's `src/serde.rs`,
//! which is `MIT OR Apache-2.0`; this crate takes the Apache-2.0 branch, the
//! same license it is itself distributed under. Compatibility is proven by
//! round-tripping through that crate's deserializer in the mock tests.

#[cfg(feature = "chrono")]
pub mod chrono;
pub mod ipv4;
#[cfg(feature = "time")]
pub mod time;
#[cfg(feature = "uuid")]
pub mod uuid;

/// Generates the `option` submodule of a with-module: `Option<T>` support
/// for `Nullable(...)` columns, delegating to the parent module's
/// `serialize`/`deserialize` for the inner value.
macro_rules! option_module {
    ($ty:ty) => {
        /// `Nullable(...)` counterpart of the parent module, for
        /// `Option<...>` fields.
        pub mod option {
            use ::serde::{Deserialize, Deserializer, Serialize, Serializer};

            /// Serialize an optional value via the parent module.
            pub fn serialize<S>(value: &Option<$ty>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                struct AsParent<'a>(&'a $ty);
                impl Serialize for AsParent<'_> {
                    fn serialize<S2>(&self, serializer: S2) -> Result<S2::Ok, S2::Error>
                    where
                        S2: Serializer,
                    {
                        super::serialize(self.0, serializer)
                    }
                }
                match value {
                    Some(v) => serializer.serialize_some(&AsParent(v)),
                    None => serializer.serialize_none(),
                }
            }

            /// Deserialize an optional value via the parent module.
            pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<$ty>, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FromParent($ty);
                impl<'de> Deserialize<'de> for FromParent {
                    fn deserialize<D2>(deserializer: D2) -> Result<Self, D2::Error>
                    where
                        D2: Deserializer<'de>,
                    {
                        super::deserialize(deserializer).map(FromParent)
                    }
                }
                Ok(Option::<FromParent>::deserialize(deserializer)?.map(|w| w.0))
            }
        }
    };
}
pub(crate) use option_module;
