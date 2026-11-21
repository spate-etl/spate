//! Configuration and construction.
//!
//! The `deserializer: { json: { ... } }` section of the pipeline YAML is
//! handed here as an opaque
//! [`ComponentConfig`](spate_core::config::ComponentConfig):
//!
//! ```yaml
//! deserializer:
//!   json:
//!     framing: single              # single | ndjson | array
//!     on_error: skip               # skip | fail
//!     reject_duplicate_keys: false
//! ```

use crate::deser::{DecoderCore, JsonSerdeDeserializer, JsonValueDeserializer};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use spate_core::config::{ComponentConfig, ConfigError, SectionPath};
use spate_core::framing::FramingContract;

/// How one payload is split into JSON documents.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum JsonFraming {
    /// The whole payload is one JSON document → one record. An empty or
    /// whitespace-only payload is a tombstone (zero records).
    #[default]
    Single,
    /// Newline-delimited JSON (JSON Lines): one JSON value per `\n`-separated
    /// line → one record per line, with per-line error isolation under
    /// [`OnError::Skip`].
    Ndjson,
    /// A top-level JSON array → one record per element, decoded in one pass
    /// (a malformed array is handled atomically by [`OnError`]).
    Array,
}

impl JsonFraming {
    /// The value as the `framing` key spells it.
    fn yaml_name(self) -> &'static str {
        match self {
            JsonFraming::Single => "single",
            JsonFraming::Ndjson => "ndjson",
            JsonFraming::Array => "array",
        }
    }
}

/// What to do with a record that does not parse or match the target type.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnError {
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
    /// payload. Multi-record framings (`ndjson`, `array`) fail all-or-nothing,
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
    /// Payload framing.
    pub framing: JsonFraming,
    /// Record-level error policy.
    pub on_error: OnError,
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
    /// depend on another's. The framing a source already applies is checked
    /// by [`JsonDeserializerBuilder::for_source_framing`].
    ///
    /// # Errors
    ///
    /// None.
    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }
}

/// The deserializer's component type, and the tag its section sits under.
pub(crate) const TAG: &str = "json";

/// Builder produced from the config section; hands out either the typed or
/// the dynamically-typed deserializer.
#[derive(Clone, Debug)]
pub struct JsonDeserializerBuilder {
    config: JsonDeserializerConfig,
}

impl JsonDeserializerBuilder {
    /// Build from a config.
    #[must_use]
    pub fn new(config: JsonDeserializerConfig) -> JsonDeserializerBuilder {
        JsonDeserializerBuilder { config }
    }

    /// Build from the pipeline's `deserializer` component section.
    ///
    /// # Errors
    ///
    /// As [`JsonDeserializerConfig::from_component_config`].
    pub fn from_component_config(
        section: &ComponentConfig,
    ) -> Result<JsonDeserializerBuilder, ConfigError> {
        Ok(Self::new(JsonDeserializerConfig::from_component_config(
            section,
        )?))
    }

    /// Derive and validate framing against the source's
    /// [`FramingContract`](spate_core::framing::FramingContract), so the format
    /// is declared once (on the source) instead of coordinating the source's
    /// framing with `framing:` by hand.
    ///
    /// - [`PerRecord`](FramingContract::PerRecord): the source already frames
    ///   one record per payload, so the deserializer must decode a single
    ///   document. `framing: single` (the default) is used, and any other
    ///   `framing` is refused.
    /// - [`WholePayload`](FramingContract::WholePayload): the deserializer owns
    ///   framing, and the configured `framing` (`single` / `ndjson` / `array`)
    ///   is honored unchanged.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] at `deserializer.json.framing` for a
    /// `PerRecord` source with a `framing` other than `single`.
    pub fn for_source_framing(self, contract: FramingContract) -> Result<Self, ConfigError> {
        match contract {
            FramingContract::PerRecord if self.config.framing != JsonFraming::Single => {
                Err(SectionPath::deserializer(TAG).invalid(
                    "framing",
                    format!(
                        "is `{}`, but the source already frames one record per payload; \
                         remove `framing` or set it to `single`, or use a source format \
                         that leaves framing to the deserializer",
                        self.config.framing.yaml_name()
                    ),
                ))
            }
            _ => Ok(self),
        }
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
            framing: self.config.framing,
            on_error: self.config.on_error,
            reject_duplicate_keys: self.config.reject_duplicate_keys,
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
    fn defaults_are_single_skip_no_dedup() {
        let c = JsonDeserializerConfig::default();
        assert_eq!(c.framing, JsonFraming::Single);
        assert_eq!(c.on_error, OnError::Skip);
        assert!(!c.reject_duplicate_keys);
    }

    #[test]
    fn parses_a_full_section() {
        let c = config("framing: ndjson\non_error: fail\nreject_duplicate_keys: true\n").unwrap();
        assert_eq!(c.framing, JsonFraming::Ndjson);
        assert_eq!(c.on_error, OnError::Fail);
        assert!(c.reject_duplicate_keys);
    }

    #[test]
    fn the_builder_takes_the_section_it_is_given() {
        let b = builder("framing: array\non_error: fail\nreject_duplicate_keys: true\n");
        assert_eq!(b.config.framing, JsonFraming::Array);
        assert_eq!(b.config.on_error, OnError::Fail);
        assert!(b.config.reject_duplicate_keys);
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
        const REFUSAL: &str = "json.bogus: unknown field `bogus`, expected one of `framing`, \
                               `on_error`, `reject_duplicate_keys`";
        let err = config("framing: single\nbogus: 1\n").unwrap_err();
        assert_eq!(err.to_string(), REFUSAL);
        let err =
            JsonDeserializerBuilder::from_component_config(&component("bogus: 1\n")).unwrap_err();
        assert_eq!(err.to_string(), REFUSAL);
    }

    #[test]
    fn unknown_framing_is_rejected() {
        let err = config("framing: yaml\n").unwrap_err();
        assert_eq!(
            err.to_string(),
            "json.framing: unknown variant, expected one of `single`, `ndjson`, `array`"
        );
    }

    #[test]
    fn per_record_source_derives_single_and_rejects_double_framing() {
        // Omitted framing (defaults to single) under a per-record source is fine.
        let b = JsonDeserializerBuilder::new(JsonDeserializerConfig::default())
            .for_source_framing(FramingContract::PerRecord)
            .unwrap();
        assert_eq!(b.config.framing, JsonFraming::Single);

        // Explicit single is fine too.
        builder("framing: single\n")
            .for_source_framing(FramingContract::PerRecord)
            .unwrap();

        // ndjson / array double-frame an already-framed source.
        for framing in ["ndjson", "array"] {
            let err = builder(&format!("framing: {framing}\n"))
                .for_source_framing(FramingContract::PerRecord)
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                format!(
                    "deserializer.json.framing: is `{framing}`, but the source already frames \
                     one record per payload; remove `framing` or set it to `single`, or use a \
                     source format that leaves framing to the deserializer"
                ),
            );
        }
    }

    #[test]
    fn whole_payload_source_honors_configured_framing() {
        for framing in ["single", "ndjson", "array"] {
            let b = builder(&format!("framing: {framing}\n"))
                .for_source_framing(FramingContract::WholePayload)
                .unwrap();
            assert_eq!(b.config.framing.yaml_name(), framing);
        }
    }
}
