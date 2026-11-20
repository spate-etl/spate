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
//! `simd-json` rejects integer literals outside the `i64`/`u64` range that
//! `serde_json` accepts as `f64` and normalizes `-0` to `0`. Both backends reject
//! NUL bytes after numbers and underscore separators in numbers. `serde_json`
//! has a recursion limit of 128; `simd-json` limits nesting to 1024. With
//! `reject_duplicate_keys`, the `serde_json` guard applies its limit first.
//! `simd-json` does not honor serde_json's `arbitrary_precision` / `raw_value` /
//! `float_roundtrip` cargo features. See the JSON connector guide's Backends
//! section.
//!
//! [`Buffers`]: https://docs.rs/simd-json

use serde::de::{self, DeserializeOwned, Deserializer, MapAccess, SeqAccess, Visitor};
use spate_core::hash::QualitySet;
use std::borrow::Cow;
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

/// What a data error says in place of serde's message, which quotes the
/// value, key or variant it refused.
const MISMATCH: &str = "the document does not match the record type";

impl DecodeError {
    /// A `serde_json` error, with `data` as the text of a data error.
    fn from_serde_json(e: &serde_json::Error, data: &str) -> Self {
        let msg = match e.classify() {
            serde_json::error::Category::Syntax | serde_json::error::Category::Eof => e.to_string(),
            serde_json::error::Category::Data | serde_json::error::Category::Io => {
                format!("{data} at line {} column {}", e.line(), e.column())
            }
        };
        DecodeError {
            is_data: e.is_data(),
            msg,
        }
    }
}

/// Decode one complete JSON document from `bytes` into `T` (serde_json backend).
///
/// serde_json borrows the immutable payload slice directly, with no copy.
#[cfg(not(feature = "simd"))]
#[inline]
pub(crate) fn decode_one<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    serde_json::from_slice(bytes).map_err(|e| DecodeError::from_serde_json(&e, MISMATCH))
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
                msg: simd_text(&e),
            }
        })
    })
}

/// The text of a `simd-json` error: its kind and byte index.
///
/// `simd-json`'s own `Display` also prints the input character at the index.
#[cfg(feature = "simd")]
fn simd_text(e: &simd_json::Error) -> String {
    use simd_json::ErrorType as T;
    match e.error() {
        // serde's message, which quotes what it refused. simd-json reports
        // no index for it.
        T::Serde(_) => MISMATCH.to_owned(),
        // Kinds only simd-json's serde layer raises, for a value of the
        // wrong type. They hold no input and it reports no index for them.
        kind @ (T::Unexpected(..)
        | T::ExpectedArray
        | T::ExpectedBoolean
        | T::ExpectedEnum
        | T::ExpectedFloat
        | T::ExpectedMap
        | T::ExpectedSigned
        | T::ExpectedString
        | T::ExpectedUnsigned
        | T::KeyMustBeAString) => format!("{MISMATCH} ({kind:?})"),
        // Kinds that hold no input. A kind a later simd-json adds is not
        // printed until it is listed here.
        kind @ (T::InputTooLarge
        | T::InsufficientPadding
        | T::BadKeyType
        | T::ExpectedArrayComma
        | T::ExpectedInteger
        | T::ExpectedObjectColon
        | T::ExpectedMapComma
        | T::ExpectedMapEnd
        | T::ExpectedNull
        | T::ExpectedTrue
        | T::ExpectedFalse
        | T::ExpectedNumber
        | T::InternalError
        | T::InvalidEscape
        | T::InvalidExponent
        | T::InvalidNumber
        | T::InvalidUtf8
        | T::InvalidUnicodeEscape
        | T::InvalidUnicodeCodepoint
        | T::NoStructure
        | T::Parser
        | T::Eof
        | T::Syntax
        | T::TrailingData
        | T::UnexpectedCharacter
        | T::UnterminatedString
        | T::ExpectedArrayContent
        | T::ExpectedObjectContent
        | T::ExpectedObjectKey
        | T::Overflow
        | T::DepthLimitExceeded
        | T::SimdUnsupported) => format!("{kind:?} at character {}", e.index()),
        _ => format!("malformed JSON at character {}", e.index()),
    }
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
        .map_err(|e| DecodeError::from_serde_json(&e, "duplicate object key"))
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
        // Payload keys are chosen by the producer, so the set takes the
        // quality hasher, seeded afresh for each object.
        let mut seen: QualitySet<Cow<'de, str>> = QualitySet::default();
        while let Some(Key(key)) = map.next_key::<Key<'de>>()? {
            if seen.replace(key).is_some() {
                return Err(de::Error::custom("duplicate object key"));
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
    use super::{Key, check_no_duplicate_keys, decode_one};
    use serde::Deserialize;
    use serde::de::{IgnoredAny, MapAccess, Visitor};
    use std::borrow::Cow;
    use std::collections::BTreeMap;
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
    fn a_duplicate_key_is_reported_by_position_without_the_key() {
        let err = check_no_duplicate_keys(br#"{"id":1,"id":2}"#).unwrap_err();
        assert!(err.is_data, "{err}");
        assert_eq!(err.to_string(), "duplicate object key at line 1 column 12");
    }

    #[test]
    fn escaped_spelling_is_the_same_key() {
        let err = check_no_duplicate_keys(br#"{"\u0061":1,"a":2}"#).unwrap_err();
        assert!(err.is_data, "{err}");
        assert_eq!(err.to_string(), "duplicate object key at line 1 column 15");
    }

    /// The set holds the plain spelling and the escaped one probes it, the
    /// reverse of `escaped_spelling_is_the_same_key`.
    #[test]
    fn plain_spelling_matches_an_escaped_duplicate() {
        let err = check_no_duplicate_keys(br#"{"a":1,"\u0061":2}"#).unwrap_err();
        assert!(err.is_data, "{err}");
        assert_eq!(err.to_string(), "duplicate object key at line 1 column 15");
    }

    /// A syntax error in the guard's pass keeps the parser's text.
    #[test]
    fn the_guard_reports_a_syntax_error_as_the_parser_words_it() {
        let err = check_no_duplicate_keys(b"{not json").unwrap_err();
        assert!(!err.is_data, "{err}");
        assert_eq!(err.to_string(), "key must be a string at line 1 column 2");
    }

    #[derive(Debug, Deserialize)]
    enum Status {
        Active,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Strict {
        #[allow(dead_code)]
        n: u32,
    }

    #[derive(Debug, Deserialize)]
    struct Typed {
        #[allow(dead_code)]
        n: u32,
        #[allow(dead_code)]
        #[serde(default)]
        status: Option<Status>,
        #[allow(dead_code)]
        #[serde(default)]
        at: Option<std::net::IpAddr>,
        #[allow(dead_code)]
        #[serde(default)]
        small: Option<u8>,
    }

    fn typed(doc: &[u8]) -> String {
        decode_one::<Typed>(doc).unwrap_err().to_string()
    }

    /// serde's message for a document that does not fit the record type
    /// quotes the value, variant or key it refused. The text carries none of
    /// them, nor a message the record type's own `Deserialize` wrote.
    #[cfg(not(feature = "simd"))]
    #[test]
    fn a_data_error_carries_its_position_and_no_content() {
        const MISMATCH: &str = "the document does not match the record type";
        let cases: [(&[u8], usize); 4] = [
            (br#"{"n":"hunter2"}"#, 14),
            (br#"{"n":1,"status":"hunter2"}"#, 25),
            (br#"{"n":1,"at":"hunter2"}"#, 21),
            (br#"{"n":1,"small":4242}"#, 19),
        ];
        for (doc, column) in cases {
            assert_eq!(typed(doc), format!("{MISMATCH} at line 1 column {column}"));
        }
        let unknown_field = decode_one::<Strict>(br#"{"n":1,"hunter2":2}"#).unwrap_err();
        assert!(unknown_field.is_data, "{unknown_field}");
        assert_eq!(
            unknown_field.to_string(),
            format!("{MISMATCH} at line 1 column 16")
        );
    }

    #[cfg(not(feature = "simd"))]
    #[test]
    fn a_syntax_error_keeps_the_parser_text() {
        let bare_word = decode_one::<Typed>(br#"{"n":hunter2}"#).unwrap_err();
        assert!(!bare_word.is_data, "{bare_word}");
        assert_eq!(bare_word.to_string(), "expected value at line 1 column 6");
        assert_eq!(
            typed(br#"{"n":"hunter2"#),
            "EOF while parsing a string at line 1 column 13"
        );
    }

    /// `simd-json` gives no index for a document that does not fit the record
    /// type, so the text is the class.
    #[cfg(feature = "simd")]
    #[test]
    fn a_data_error_carries_no_content() {
        const MISMATCH: &str = "the document does not match the record type";
        for doc in [
            &br#"{"n":1,"status":"hunter2"}"#[..],
            br#"{"n":1,"at":"hunter2"}"#,
        ] {
            assert_eq!(typed(doc), MISMATCH);
        }
        let unknown_field = decode_one::<Strict>(br#"{"n":1,"hunter2":2}"#).unwrap_err();
        assert_eq!(unknown_field.to_string(), MISMATCH);
        for doc in [&br#"{"n":"hunter2"}"#[..], br#"{"n":1,"small":4242}"#] {
            assert_eq!(typed(doc), format!("{MISMATCH} (ExpectedUnsigned)"));
        }
    }

    /// `simd-json`'s own text ends with the input character at the index.
    #[cfg(feature = "simd")]
    #[test]
    fn a_syntax_error_names_its_kind_and_index_without_the_character() {
        assert_eq!(typed(br#"{"n":hunter2}"#), "InternalError at character 5");
        assert_eq!(
            typed(br#"{"n":"hun\qter2"}"#),
            "InvalidEscape at character 3"
        );
    }

    #[test]
    fn same_key_in_sibling_scopes_is_clean() {
        check_no_duplicate_keys(br#"{"a":{"a":1},"b":[{"a":1},{"a":2}]}"#).unwrap();
    }

    /// A `\u` escape naming half a surrogate pair is malformed, and never
    /// decodes to another character.
    #[test]
    fn a_lone_surrogate_escape_is_malformed() {
        let decoded = decode_one::<String>(br#""\ud800""#);
        assert!(decoded.is_err(), "{decoded:?}");
    }

    /// An array longer than the fixed-length type it decodes into is
    /// malformed, with no element dropped.
    #[test]
    fn an_array_longer_than_its_tuple_is_malformed() {
        #[derive(Debug, Deserialize)]
        struct Pair(#[allow(dead_code)] u8, #[allow(dead_code)] u8);
        #[derive(Debug, Deserialize)]
        struct Named {
            #[allow(dead_code)]
            a: u8,
        }
        let tuple = decode_one::<(u8, u8)>(b"[1,2,3]");
        assert!(tuple.is_err(), "{tuple:?}");
        let array = decode_one::<[u8; 2]>(b"[1,2,3]");
        assert!(array.is_err(), "{array:?}");
        let pair = decode_one::<Pair>(b"[1,2,3]");
        assert!(pair.is_err(), "{pair:?}");
        let named = decode_one::<Named>(b"[1,2]");
        assert!(named.is_err(), "{named:?}");
    }

    /// A number whose exponent is past the `f64` range is malformed, and
    /// never decodes to another number.
    #[test]
    fn an_exponent_past_the_f64_range_is_malformed() {
        for doc in [&b"1e4294967296"[..], b"1e4294967295"] {
            let decoded = decode_one::<f64>(doc);
            assert!(decoded.is_err(), "{decoded:?}");
        }
    }

    /// An integer map key is written in JSON number syntax: no leading `+`
    /// and no leading zero.
    #[test]
    fn an_integer_key_follows_json_number_syntax() {
        for doc in [&br#"{"+1":2}"#[..], br#"{"01":2}"#, br#"{"-01":2}"#] {
            let decoded = decode_one::<BTreeMap<i32, u8>>(doc);
            assert!(decoded.is_err(), "{decoded:?}");
        }
    }

    #[test]
    fn a_128_bit_integer_key_decodes() {
        let decoded: BTreeMap<i128, u8> = decode_one(br#"{"7":1}"#).unwrap();
        assert_eq!(decoded, BTreeMap::from([(7, 1)]));
    }
}
