//! Configuration and construction.
//!
//! The `deserializer: { json: { ... } }` section of the pipeline YAML is
//! handed here as an opaque
//! [`ComponentConfig`](spate_core::config::ComponentConfig):
//!
//! ```yaml
//! deserializer:
//!   json:
//!     on_error: skip               # skip | fail
//!     reject_duplicate_keys: false
//! ```
//!
//! How a payload splits into documents is set in code, with
//! [`JsonDeserializerBuilder::framing`], since it follows the producer's
//! format and the source the pipeline is built with.

use crate::backend::Backend;
use crate::deser::{DecoderCore, JsonSerdeDeserializer, JsonValueDeserializer};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use spate_core::config::{ComponentConfig, ConfigError};

/// How one payload is split into JSON documents, set with
/// [`JsonDeserializerBuilder::framing`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum JsonFraming {
    /// The whole payload is one JSON document → one record. An empty or
    /// whitespace-only payload is a tombstone (zero records).
    #[default]
    Single,
    /// Newline-delimited JSON (JSON Lines): one JSON value per `\n`-separated
    /// line → one record per line, with per-line error isolation under
    /// [`JsonOnError::Skip`].
    Lines,
    /// A top-level JSON array → one record per element, decoded in one pass
    /// (a malformed array is handled atomically by [`JsonOnError`]).
    Array,
}

/// What to do with a record that does not parse or match the target type.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum JsonOnError {
    /// Drop the record, count it in
    /// `spate_json_deser_records_dropped_total{reason}` on a chain built with
    /// `with_metrics`, and continue. Under [`JsonFraming::Array`] one bad
    /// element drops the whole array. The default for deserializers.
    #[default]
    Skip,
    /// Surface a decode error on the first bad record. What that error then
    /// *does* is the chain's deserializer
    /// [`ErrorPolicy`](spate_core::ErrorPolicy): `Fail` stops the pipeline and the
    /// batch replays under at-least-once; `Skip` (the default) drops the whole
    /// payload. Multi-record framings (`Lines`, `Array`) fail all-or-nothing,
    /// so a payload never emits a partial prefix before erroring.
    Fail,
}

/// The `deserializer: { json: ... }` section.
///
/// Every key has a default, so `json: {}` is a complete section. Build one in
/// code from [`Default`] and set the fields you need.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct JsonDeserializerConfig {
    /// Record-level error policy.
    pub on_error: JsonOnError,
    /// Reject (rather than silently last-value-wins) any JSON object with a
    /// duplicate key, at any depth. Off by default; enabling it parses each
    /// document a second time for the structural check.
    pub reject_duplicate_keys: bool,
}

impl JsonDeserializerConfig {
    /// Deserialize and validate from the pipeline's opaque component section.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] when the section does not deserialize into
    /// this type.
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError> {
        let cfg: JsonDeserializerConfig = section.deserialize_into()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation. It refuses nothing, because no key's values
    /// depend on another's.
    ///
    /// # Errors
    ///
    /// None.
    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }
}

/// The deserializer's component type, and the key its section sits under.
pub(crate) const COMPONENT_TYPE: &str = "json";

/// Builder produced from the config section; hands out either the typed or
/// the dynamically-typed deserializer.
#[derive(Clone, Debug)]
pub struct JsonDeserializerBuilder {
    config: JsonDeserializerConfig,
    framing: JsonFraming,
    backend: Backend,
}

impl JsonDeserializerBuilder {
    /// Build from a config, with [`JsonFraming::Single`] framing.
    ///
    /// # Errors
    ///
    /// As [`JsonDeserializerConfig::validate`].
    pub fn new(config: JsonDeserializerConfig) -> Result<JsonDeserializerBuilder, ConfigError> {
        config.validate()?;
        Ok(JsonDeserializerBuilder {
            config,
            framing: JsonFraming::Single,
            backend: Backend::SerdeJson,
        })
    }

    /// Build from the pipeline's `deserializer` component section.
    ///
    /// # Errors
    ///
    /// As [`JsonDeserializerConfig::from_component_config`].
    pub fn from_component_config(
        section: &ComponentConfig,
    ) -> Result<JsonDeserializerBuilder, ConfigError> {
        Self::new(JsonDeserializerConfig::from_component_config(section)?)
    }

    /// Split each payload as `framing`. The default, [`JsonFraming::Single`],
    /// decodes each payload as one document.
    ///
    /// A source that frames one record per payload, such as an S3 source with
    /// a `JsonLinesFramer`, has split the payload already. The deserializer
    /// then refuses any framing but `Single` when the pipeline starts.
    #[must_use]
    pub fn framing(mut self, framing: JsonFraming) -> Self {
        self.framing = framing;
        self
    }

    /// Decode with `simd-json`, which chooses its SIMD code at run time from
    /// the CPU's features. Each deserializer built from this builder uses it;
    /// any other keeps `serde_json`.
    ///
    /// It is faster on documents carrying long strings and slower on
    /// number-heavy ones, by an amount that depends on the CPU. The JSON
    /// connector guide's Backends section gives the measured figures.
    ///
    /// It accepts the same documents as `serde_json` and decodes them to the
    /// same values, with these differences:
    ///
    /// - An integer literal outside the `i64`/`u64` range is refused, where
    ///   `serde_json` reads it as an `f64`, and `-0` decodes as `0`.
    /// - Nesting is limited to 1024 levels, where `serde_json` stops at 128.
    ///   With `reject_duplicate_keys` on, the guard runs on `serde_json` first
    ///   and its limit applies.
    /// - `serde_json`'s `arbitrary_precision`, `raw_value` and
    ///   `float_roundtrip` features have no effect on this parser.
    /// - A refused document's error gives a character index for a syntax
    ///   error and no position for a document that does not match the record
    ///   type, where `serde_json` gives a line and column.
    #[cfg(feature = "simd-json")]
    #[must_use]
    pub fn with_simd_json(mut self) -> Self {
        self.backend = Backend::SimdJson;
        self
    }

    /// Decode each document into your own `T: serde::de::DeserializeOwned`.
    #[must_use]
    pub fn build_serde<T>(&self) -> JsonSerdeDeserializer<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        JsonSerdeDeserializer::new(self.core())
    }

    /// Decode each document into a dynamically-typed [`serde_json::Value`].
    #[must_use]
    pub fn build_value(&self) -> JsonValueDeserializer {
        JsonValueDeserializer::new(self.core())
    }

    fn core(&self) -> DecoderCore {
        DecoderCore {
            framing: self.framing,
            on_error: self.config.on_error,
            reject_duplicate_keys: self.config.reject_duplicate_keys,
            backend: self.backend,
            metrics: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::config::{ComponentConfig, YamlValue};

    fn component(yaml: &str) -> ComponentConfig {
        spate_test::component_config("json", yaml)
    }

    fn config(yaml: &str) -> Result<JsonDeserializerConfig, ConfigError> {
        JsonDeserializerConfig::from_component_config(&component(yaml))
    }

    fn builder(yaml: &str) -> JsonDeserializerBuilder {
        JsonDeserializerBuilder::from_component_config(&component(yaml)).unwrap()
    }

    #[test]
    fn defaults_are_skip_no_dedup_and_single_framing() {
        let c = JsonDeserializerConfig::default();
        assert_eq!(c.on_error, JsonOnError::Skip);
        assert!(!c.reject_duplicate_keys);
        let b = JsonDeserializerBuilder::new(c).expect("the default config validates");
        assert_eq!(b.framing, JsonFraming::Single);
    }

    #[test]
    fn parses_a_full_section() {
        let c = config("on_error: fail\nreject_duplicate_keys: true\n").unwrap();
        assert_eq!(c.on_error, JsonOnError::Fail);
        assert!(c.reject_duplicate_keys);
    }

    #[test]
    fn the_builder_takes_the_section_it_is_given() {
        let b = builder("on_error: fail\nreject_duplicate_keys: true\n");
        assert_eq!(b.config.on_error, JsonOnError::Fail);
        assert!(b.config.reject_duplicate_keys);
    }

    #[test]
    fn the_builder_takes_the_framing_it_is_given() {
        for framing in [JsonFraming::Single, JsonFraming::Lines, JsonFraming::Array] {
            let b = builder("{}").framing(framing);
            assert_eq!(b.framing, framing);
            assert_eq!(b.core().framing, framing);
        }
    }

    #[test]
    fn empty_section_uses_defaults() {
        // `json:` with no body → all defaults.
        let c = JsonDeserializerConfig::from_component_config(&ComponentConfig::new(
            "json",
            YamlValue::Null,
        ))
        .unwrap();
        assert_eq!(c, JsonDeserializerConfig::default());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        const REFUSAL: &str =
            "json.bogus: unknown field `bogus`, expected `on_error` or `reject_duplicate_keys`";
        let err = config("on_error: skip\nbogus: 1\n").unwrap_err();
        assert_eq!(err.to_string(), REFUSAL);
        let err =
            JsonDeserializerBuilder::from_component_config(&component("bogus: 1\n")).unwrap_err();
        assert_eq!(err.to_string(), REFUSAL);
    }

    /// Framing is set in code, so a `framing` key is refused like any other
    /// unknown key.
    #[test]
    fn a_framing_key_is_rejected() {
        let err = config("framing: lines\n").unwrap_err();
        assert_eq!(
            err.to_string(),
            "json.framing: unknown field `framing`, expected `on_error` or \
             `reject_duplicate_keys`"
        );
    }
}
