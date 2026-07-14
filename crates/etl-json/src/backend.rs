//! The byte-slice → value decode seam.
//!
//! Every JSON document is decoded from an in-memory `&[u8]` slice — never
//! `from_reader`, which serde_json's own docs note is slower than reading to a
//! slice first and cannot borrow. Today the one backend is `serde_json`; a
//! future SIMD backend (`sonic-rs`, which also takes an immutable slice) would
//! swap in behind [`decode_one`] under a Cargo feature, leaving the framing and
//! emit logic in `deser.rs` untouched.

use serde::de::{self, DeserializeOwned, Deserializer, MapAccess, SeqAccess, Visitor};
use std::collections::HashSet;
use std::fmt;

/// Decode one complete JSON document from `bytes` into `T`.
#[inline]
pub(crate) fn decode_one<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Validate that no JSON object anywhere in `bytes` contains a duplicate key.
///
/// serde_json is silently last-value-wins on duplicate keys; this is the
/// opt-in guard behind `reject_duplicate_keys`. It is a separate structural
/// pass (a document is parsed twice when the guard is on — the documented
/// cost), independent of the decode backend.
pub(crate) fn check_no_duplicate_keys(bytes: &[u8]) -> Result<(), serde_json::Error> {
    // Deserializing into `DupGuard` walks the whole tree and errors on the
    // first repeated key; the value is discarded.
    serde_json::from_slice::<DupGuard>(bytes).map(|_| ())
}

/// A throwaway shape that accepts any JSON value but rejects an object with a
/// repeated key at any depth. It stores nothing — it exists only for its
/// [`Visitor`] side effect.
struct DupGuard;

impl<'de> serde::Deserialize<'de> for DupGuard {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DupVisitor)
    }
}

struct DupVisitor;

impl<'de> Visitor<'de> for DupVisitor {
    type Value = DupGuard;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value with unique object keys")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut seen: HashSet<String> = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !seen.insert(key.clone()) {
                return Err(de::Error::custom(format!("duplicate object key `{key}`")));
            }
            // Recurse so nested objects are guarded too.
            map.next_value::<DupGuard>()?;
        }
        Ok(DupGuard)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<DupGuard>()?.is_some() {}
        Ok(DupGuard)
    }

    // Scalars carry no keys — accept and ignore.
    fn visit_bool<E>(self, _v: bool) -> Result<Self::Value, E> {
        Ok(DupGuard)
    }
    fn visit_i64<E>(self, _v: i64) -> Result<Self::Value, E> {
        Ok(DupGuard)
    }
    fn visit_u64<E>(self, _v: u64) -> Result<Self::Value, E> {
        Ok(DupGuard)
    }
    fn visit_f64<E>(self, _v: f64) -> Result<Self::Value, E> {
        Ok(DupGuard)
    }
    fn visit_str<E>(self, _v: &str) -> Result<Self::Value, E> {
        Ok(DupGuard)
    }
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(DupGuard)
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(DupGuard)
    }
    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DupVisitor)
    }
}
