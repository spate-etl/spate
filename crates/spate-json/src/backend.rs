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
//! the backend is charged only the unavoidable memcpy (measured at ~1% of the
//! decode on flat and nested records; `decode_gungraun.rs`'s `large_string`
//! case is where a copy would show). The structural
//! [`check_no_duplicate_keys`] guard always stays on
//! `serde_json`: an off-by-default fidelity pass, not the hot path, keeping the
//! duplicate-key error classification identical across backends.
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
use std::collections::HashMap;
use std::collections::hash_map::Entry;
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

impl<'de> Visitor<'de> for DupVisitor {
    type Value = DupGuard;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value with unique object keys")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // `Cow` borrows the key straight out of the input when it contains no
        // escapes, so the happy path stores only references and allocates
        // nothing; an escaped key copies once, into the `Cow` itself.
        let mut seen: HashMap<Cow<'de, str>, ()> = HashMap::new();
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            match seen.entry(key) {
                Entry::Vacant(slot) => {
                    slot.insert(());
                }
                Entry::Occupied(occupied) => {
                    // The stored key is still borrowed from the input, so
                    // naming the offender costs no copy either.
                    return Err(de::Error::custom(format!(
                        "duplicate object key `{}`",
                        occupied.key()
                    )));
                }
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
    use super::check_no_duplicate_keys;

    fn duplicate_key_in(bytes: &[u8]) -> String {
        let err = check_no_duplicate_keys(bytes).unwrap_err();
        assert!(err.is_data, "a duplicate key must classify as a data error");
        err.to_string()
    }

    #[test]
    fn clean_document_passes_the_guard() {
        // Nested objects, arrays, and scalars with unique keys: the guard must
        // accept this without touching the heap for key storage.
        assert!(
            check_no_duplicate_keys(br#"{"a":1,"b":[true,null,{"c":"d"}],"e":{"f":2.5,"g":-3}}"#)
                .is_ok()
        );
    }

    #[test]
    fn duplicate_key_error_names_the_key() {
        let msg = duplicate_key_in(br#"{"k":1,"k":2}"#);
        assert!(msg.contains("duplicate object key `k`"), "{msg}");
    }

    #[test]
    fn escaped_keys_still_compare_decoded() {
        // Both keys decode to `ab`, but neither is borrowable from the input:
        // the guard must copy the escaped one rather than misclassify the
        // document as malformed.
        let msg = duplicate_key_in(br#"{"a\u0062":1,"ab":2}"#);
        assert!(msg.contains("duplicate object key `ab`"), "{msg}");
    }
}
