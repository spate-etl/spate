//! The byte-slice → value decode seam.
//!
//! Every JSON document is decoded from an in-memory `&[u8]` slice, never
//! `from_reader`, which serde_json's own docs note is slower than reading to a
//! slice first and cannot borrow. The default backend is `serde_json`; the
//! opt-in `simd` Cargo feature swaps [`decode_one`] to `simd-json`, leaving the
//! framing and emit logic in `deser.rs` untouched.
//!
//! `simd-json` parses a *mutable* buffer in place (it unescapes strings into the
//! buffer), so the borrowed payload is copied into a reused thread-local scratch
//! first, and the parser's own scratch [`Buffers`] are likewise reused across
//! calls. Both are per-message allocations a production integration avoids, so
//! the backend is charged only the unavoidable memcpy. The structural
//! [`check_no_duplicate_keys`] guard always stays on `serde_json`: an
//! off-by-default fidelity pass, not the hot path, keeping the duplicate-key
//! error classification identical across backends.
//!
//! Decode itself is **not** byte-for-byte identical across the two backends,
//! because `simd-json` is a different parser. It rejects integer literals outside the
//! `i64`/`u64` range that `serde_json` accepts (coercing to `f64`), so under
//! `simd` such a document surfaces as a `malformed` decode error where
//! `serde_json` would succeed; it normalizes `-0` to `0`; and, being a distinct
//! parser, it does not honor serde_json's `arbitrary_precision` / `raw_value` /
//! `float_roundtrip` cargo features. This is inherent to swapping parsers
//! rather than a bug to reconcile here; see the JSON connector guide's
//! Backends section.
//!
//! [`Buffers`]: https://docs.rs/simd-json

use serde::de::{self, DeserializeOwned, Deserializer, MapAccess, SeqAccess, Visitor};
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;

/// Identifier of the compiled decode backend, surfaced for benchmark and
/// telemetry labels so an arm is tagged from the compiled code rather than a
/// hand-passed label. The default backend is `serde_json`; the
/// opt-in `simd` Cargo feature overrides it.
#[cfg(feature = "simd")]
pub const BACKEND_ID: &str = "simd-json";
/// Identifier of the compiled decode backend (see the `simd` variant).
#[cfg(not(feature = "simd"))]
pub const BACKEND_ID: &str = "serde_json";

/// A backend-agnostic decode failure at the seam.
///
/// The framing/error-policy layer in `deser.rs` must classify a failure
/// (`is_data`, to label a metric `duplicate_key` vs `malformed`) and report it
/// (`Display`), but must not name a concrete backend's error type, so
/// swapping the decode backend does not ripple into `deser.rs`. Each backend maps its
/// native error into this on the way out of [`decode_one`] /
/// [`check_no_duplicate_keys`].
#[derive(Debug)]
pub(crate) struct DecodeError {
    /// True when the input was well-formed JSON but semantically rejected,
    /// making it a *data* error (a type mismatch, or the injected
    /// duplicate-key rejection) as opposed to a syntax/EOF error. Drives the
    /// `duplicate_key` vs `malformed` metric label.
    pub(crate) is_data: bool,
    msg: String,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl From<serde_json::Error> for DecodeError {
    fn from(e: serde_json::Error) -> Self {
        DecodeError {
            is_data: e.is_data(),
            msg: e.to_string(),
        }
    }
}

/// Decode one complete JSON document from `bytes` into `T` (serde_json backend).
///
/// serde_json borrows the immutable payload slice directly, with no copy.
#[cfg(not(feature = "simd"))]
#[inline]
pub(crate) fn decode_one<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    serde_json::from_slice(bytes).map_err(DecodeError::from)
}

// Reused per-thread scratch for the simd-json backend: the mutable copy target
// plus the parser's own `Buffers` (tape/string/structural indexes). simd-json
// parses destructively in place and allocates parser scratch per call by
// default; reusing both across calls charges the backend only the unavoidable
// memcpy, matching a production integration. `bytes` (the borrowed source
// payload) is never mutated, so at-least-once replay is safe.
#[cfg(feature = "simd")]
thread_local! {
    static SIMD: std::cell::RefCell<(Vec<u8>, simd_json::Buffers)> =
        std::cell::RefCell::new((Vec::new(), simd_json::Buffers::new(0)));
}

/// Decode one complete JSON document from `bytes` into `T` (simd-json backend).
///
/// Copies `bytes` into the reused thread-local scratch and parses that in place
/// with reused [`Buffers`](simd_json::Buffers). simd-json pads internally, so
/// no trailing SIMD padding is appended here. `T: DeserializeOwned`
/// borrows nothing out of the scratch, so it is free to be overwritten next
/// call.
#[cfg(feature = "simd")]
#[inline]
pub(crate) fn decode_one<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    SIMD.with(|cell| {
        let (buf, buffers) = &mut *cell.borrow_mut();
        buf.clear();
        buf.extend_from_slice(bytes);
        simd_json::serde::from_slice_with_buffers::<T>(buf.as_mut_slice(), buffers).map_err(|e| {
            DecodeError {
                is_data: false,
                msg: e.to_string(),
            }
        })
    })
}

/// Validate that no JSON object anywhere in `bytes` contains a duplicate key.
///
/// serde_json is silently last-value-wins on duplicate keys; this is the
/// opt-in guard behind `reject_duplicate_keys`. It is a separate structural
/// pass (a document is parsed twice when the guard is on, the documented
/// cost), independent of the decode backend, so it stays on `serde_json` even
/// when the decode backend is a SIMD parser.
pub(crate) fn check_no_duplicate_keys(bytes: &[u8]) -> Result<(), DecodeError> {
    // Deserializing into `DupGuard` walks the whole tree and errors on the
    // first repeated key; the value is discarded.
    serde_json::from_slice::<DupGuard>(bytes)
        .map(|_| ())
        .map_err(DecodeError::from)
}

/// A throwaway shape that accepts any JSON value but rejects an object with a
/// repeated key at any depth. It stores nothing and is used only for its
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

/// An object key, borrowed from the input when it carries no escapes and copied
/// when it does.
struct Key<'de>(Cow<'de, str>);

impl<'de> serde::Deserialize<'de> for Key<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(KeyVisitor)
    }
}

struct KeyVisitor;

impl<'de> Visitor<'de> for KeyVisitor {
    type Value = Key<'de>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an object key")
    }

    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
        Ok(Key(Cow::Borrowed(v)))
    }

    /// The deserializer unescapes into a scratch buffer it clears before the
    /// next key, so an escaped key has to be copied out.
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(Key(Cow::Owned(v.to_owned())))
    }
}

impl<'de> Visitor<'de> for DupVisitor {
    type Value = DupGuard;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value with unique object keys")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut seen: HashSet<Cow<'de, str>> = HashSet::new();
        while let Some(Key(key)) = map.next_key::<Key<'de>>()? {
            if let Some(dup) = seen.replace(key) {
                return Err(de::Error::custom(format!("duplicate object key `{dup}`")));
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

    // Scalars carry no keys, so accept and ignore.
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

#[cfg(test)]
mod tests {
    use super::{Key, check_no_duplicate_keys};
    use serde::Deserialize;
    use serde::de::{IgnoredAny, MapAccess, Visitor};
    use std::borrow::Cow;
    use std::fmt;

    struct ObjectKeys<'de>(Vec<Cow<'de, str>>);

    impl<'de> Deserialize<'de> for ObjectKeys<'de> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_map(ObjectKeysVisitor)
        }
    }

    struct ObjectKeysVisitor;

    impl<'de> Visitor<'de> for ObjectKeysVisitor {
        type Value = ObjectKeys<'de>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a JSON object")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut keys = Vec::new();
            while let Some(Key(key)) = map.next_key::<Key<'de>>()? {
                map.next_value::<IgnoredAny>()?;
                keys.push(key);
            }
            Ok(ObjectKeys(keys))
        }
    }

    #[test]
    fn an_unescaped_key_borrows_from_the_payload() {
        let ObjectKeys(keys) = serde_json::from_slice(br#"{"abc":1,"\u0061bc":2}"#).unwrap();
        assert!(matches!(keys[0], Cow::Borrowed("abc")));
        assert!(matches!(keys[1], Cow::Owned(_)));
    }

    #[test]
    fn duplicate_key_names_the_key() {
        let err = check_no_duplicate_keys(br#"{"id":1,"id":2}"#).unwrap_err();
        assert!(err.is_data, "{err}");
        assert!(
            err.to_string().starts_with("duplicate object key `id`"),
            "{err}"
        );
    }

    #[test]
    fn escaped_spelling_is_the_same_key() {
        let err = check_no_duplicate_keys(br#"{"\u0061":1,"a":2}"#).unwrap_err();
        assert!(err.is_data, "{err}");
        assert!(
            err.to_string().starts_with("duplicate object key `a`"),
            "{err}"
        );
    }

    /// The set holds the plain spelling and the escaped one probes it, the
    /// reverse of `escaped_spelling_is_the_same_key`. The message names the key
    /// the set already held.
    #[test]
    fn plain_spelling_matches_an_escaped_duplicate() {
        let err = check_no_duplicate_keys(br#"{"a":1,"\u0061":2}"#).unwrap_err();
        assert!(err.is_data, "{err}");
        assert!(
            err.to_string().starts_with("duplicate object key `a`"),
            "{err}"
        );
    }

    #[test]
    fn same_key_in_sibling_scopes_is_clean() {
        check_no_duplicate_keys(br#"{"a":{"a":1},"b":[{"a":1},{"a":2}]}"#).unwrap();
    }
}
