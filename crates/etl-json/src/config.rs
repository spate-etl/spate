//! Configuration and construction.
//!
//! The `deserializer: { json: { ... } }` section of the pipeline YAML is
//! handed here as an opaque
//! [`ComponentConfig`](etl_core::config::ComponentConfig):
//!
//! ```yaml
//! deserializer:
//!   json:
//!     framing: single              # single | ndjson | array
//!     on_error: skip               # skip | fail
//!     reject_duplicate_keys: false
//! ```

use crate::deser::{DecoderCore, JsonSerdeDeserializer, JsonValueDeserializer};
use crate::metrics::JsonDeserMetrics;
use etl_core::config::ComponentConfig;
use etl_core::framing::FramingContract;
use etl_core::metrics::{Meter, SharedString};
use serde::Deserialize;
use serde::de::DeserializeOwned;

/// How one payload is split into JSON documents.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JsonFraming {
    /// The whole payload is one JSON document → one record. An empty or
    /// whitespace-only payload is a tombstone (zero records).
    #[default]
    Single,
    /// Newline-delimited JSON (JSON Lines): one JSON value per `\n`-separated
    /// line → one record per line, with per-line error isolation.
    Ndjson,
    /// A top-level JSON array → one record per element, decoded in one pass
    /// (a malformed array is handled atomically by [`OnError`]).
    Array,
}

/// What to do with a record that does not parse or match the target type.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    /// Drop the record, count it in
    /// `etl_json_deser_records_dropped_total{reason}`, and continue. The
    /// default for deserializers.
    #[default]
    Skip,
    /// Surface a decode error on the first bad record. What that error then
    /// *does* is the chain's deserializer
    /// [`ErrorPolicy`](etl_core::ErrorPolicy): `Fail` stops the pipeline and the
    /// batch replays under at-least-once; `Skip` (the default) drops the whole
    /// payload. Multi-record framings (`ndjson`, `array`) fail all-or-nothing,
    /// so a payload never emits a partial prefix before erroring.
    Fail,
}

/// The `deserializer: { json: ... }` settings.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JsonSettings {
    /// Payload framing.
    pub framing: JsonFraming,
    /// Record-level error policy.
    pub on_error: OnError,
    /// Reject (rather than silently last-value-wins) any JSON object with a
    /// duplicate key, at any depth. Off by default; enabling it parses each
    /// document a second time for the structural check.
    pub reject_duplicate_keys: bool,
}

/// Construction errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum JsonConfigError {
    /// The component section did not deserialize.
    #[error(transparent)]
    Config(#[from] etl_core::config::ConfigError),
    /// The source already frames one record per payload, but the deserializer
    /// is configured to frame the payload too (double-framing).
    #[error(
        "the source frames one record per payload, but the JSON deserializer is set \
         to `framing: {framing:?}` — remove `framing` (or set it to `single`), or use \
         a whole-object source format that lets the deserializer frame"
    )]
    DoubleFraming {
        /// The offending deserializer framing.
        framing: JsonFraming,
    },
}

/// Builder produced from the opaque config section; hands out either the
/// typed or the dynamically-typed deserializer.
#[derive(Clone, Debug)]
pub struct JsonDeserializerBuilder {
    settings: JsonSettings,
    metrics: Option<JsonDeserMetrics>,
}

impl JsonDeserializerBuilder {
    /// Build from the pipeline's `deserializer` component section.
    pub fn from_component(cfg: &ComponentConfig) -> Result<Self, JsonConfigError> {
        let settings: JsonSettings = cfg.deserialize_into()?;
        Ok(Self::from_settings(settings))
    }

    /// Build from already-parsed settings.
    #[must_use]
    pub fn from_settings(settings: JsonSettings) -> Self {
        JsonDeserializerBuilder {
            settings,
            metrics: None,
        }
    }

    /// Derive and validate framing against the source's
    /// [`FramingContract`](etl_core::framing::FramingContract), so the format
    /// is declared once (on the source) instead of coordinating the source's
    /// framing with `framing:` by hand.
    ///
    /// - [`PerRecord`](FramingContract::PerRecord): the source already frames
    ///   one record per payload, so the deserializer must decode a single
    ///   document. `framing: single` (the default) is used; any other
    ///   `framing` is a [`DoubleFraming`](JsonConfigError::DoubleFraming)
    ///   error.
    /// - [`WholePayload`](FramingContract::WholePayload): the deserializer owns
    ///   framing — the configured `framing` (`single` / `ndjson` / `array`) is
    ///   honored unchanged.
    pub fn for_source_framing(self, contract: FramingContract) -> Result<Self, JsonConfigError> {
        match contract {
            FramingContract::PerRecord if self.settings.framing != JsonFraming::Single => {
                Err(JsonConfigError::DoubleFraming {
                    framing: self.settings.framing,
                })
            }
            _ => Ok(self),
        }
    }

    /// Scope the connector-owned `etl_json_deser_*` metrics to this component.
    /// Reuse the `pipeline` / `component` values the framework was given so the
    /// families join against the generic `etl_deser_*` stage metrics. Without
    /// this, per-record skips are logged (rate-limited) but not counted.
    #[must_use]
    pub fn with_metrics(
        mut self,
        pipeline: impl Into<SharedString>,
        component: impl Into<SharedString>,
    ) -> Self {
        let meter = Meter::with_namespace("json_deser", pipeline, component, "deserializer");
        self.metrics = Some(JsonDeserMetrics::new(&meter));
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
            framing: self.settings.framing,
            on_error: self.settings.on_error,
            reject_duplicate_keys: self.settings.reject_duplicate_keys,
            metrics: self.metrics.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_core::config::{ComponentConfig, YamlValue};

    fn component(yaml: &str) -> ComponentConfig {
        let raw: YamlValue = serde_yaml::from_str(yaml).unwrap();
        ComponentConfig::new("json", raw)
    }

    #[test]
    fn defaults_are_single_skip_no_dedup() {
        let s = JsonSettings::default();
        assert_eq!(s.framing, JsonFraming::Single);
        assert_eq!(s.on_error, OnError::Skip);
        assert!(!s.reject_duplicate_keys);
    }

    #[test]
    fn parses_a_full_section() {
        let cfg = component("framing: ndjson\non_error: fail\nreject_duplicate_keys: true\n");
        let b = JsonDeserializerBuilder::from_component(&cfg).unwrap();
        assert_eq!(b.settings.framing, JsonFraming::Ndjson);
        assert_eq!(b.settings.on_error, OnError::Fail);
        assert!(b.settings.reject_duplicate_keys);
    }

    #[test]
    fn empty_section_uses_defaults() {
        // `json:` with no body → all defaults.
        let cfg = ComponentConfig::new("json", YamlValue::Null);
        let b = JsonDeserializerBuilder::from_component(&cfg).unwrap();
        assert_eq!(b.settings.framing, JsonFraming::Single);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let cfg = component("framing: single\nbogus: 1\n");
        let err = JsonDeserializerBuilder::from_component(&cfg).unwrap_err();
        assert!(err.to_string().contains("bogus"), "{err}");
    }

    #[test]
    fn unknown_framing_is_rejected() {
        let cfg = component("framing: yaml\n");
        let err = JsonDeserializerBuilder::from_component(&cfg).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("framing"), "{err}");
    }

    #[test]
    fn per_record_source_derives_single_and_rejects_double_framing() {
        // Omitted framing (defaults to single) under a per-record source is fine.
        let b =
            JsonDeserializerBuilder::from_component(&ComponentConfig::new("json", YamlValue::Null))
                .unwrap()
                .for_source_framing(FramingContract::PerRecord)
                .unwrap();
        assert_eq!(b.settings.framing, JsonFraming::Single);

        // Explicit single is fine too.
        JsonDeserializerBuilder::from_component(&component("framing: single\n"))
            .unwrap()
            .for_source_framing(FramingContract::PerRecord)
            .unwrap();

        // ndjson / array double-frame an already-framed source.
        for framing in ["ndjson", "array"] {
            let err = JsonDeserializerBuilder::from_component(&component(&format!(
                "framing: {framing}\n"
            )))
            .unwrap()
            .for_source_framing(FramingContract::PerRecord)
            .unwrap_err();
            assert!(
                matches!(err, JsonConfigError::DoubleFraming { .. }),
                "{framing}: {err}"
            );
        }
    }

    #[test]
    fn whole_payload_source_honors_configured_framing() {
        for framing in ["single", "ndjson", "array"] {
            JsonDeserializerBuilder::from_component(&component(&format!("framing: {framing}\n")))
                .unwrap()
                .for_source_framing(FramingContract::WholePayload)
                .unwrap();
        }
    }
}
