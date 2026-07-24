//! Opaque per-component configuration passthrough.

use super::ConfigError;
use super::chunk::ChunkSection;
use crate::ops::ChunkConfig;
use serde::Deserialize;
use serde::de::DeserializeOwned;

/// An opaque component section: `{ <type_tag>: { ...connector config... } }`.
///
/// The framework records which component type the section selects (`kafka`,
/// `clickhouse`, `memory`, ...) and hands the body to that component's
/// factory, which deserializes it into its own typed config via
/// [`deserialize_into`](Self::deserialize_into). One key is the exception to
/// that opacity: **`chunk` is framework-reserved** — it configures the chain
/// terminal, not the connector, and is peeled out of the body at construction.
/// A connector must not declare its own `chunk` field: it would never receive
/// a value (on a sink section the framework consumes the key; on a
/// `source`/`deserializer` section the key is rejected outright).
///
/// The nested-block shape (exactly one key) is what lets every typed struct
/// in the tree keep `deny_unknown_fields` — a flattened shape would disable
/// that check.
#[derive(Debug, Clone, PartialEq)]
pub struct ComponentConfig {
    type_tag: String,
    raw: serde_yaml::Value,
    /// The framework-reserved `chunk:` block, peeled out of `raw` at
    /// construction so the connector's `deny_unknown_fields` never sees it.
    /// Stored unparsed and resolved lazily by [`resolved_chunk`](Self::resolved_chunk)
    /// (construction — including the infallible [`new`](Self::new) — must not
    /// fail on a malformed block; it surfaces at assembly/validation instead).
    chunk: Option<serde_yaml::Value>,
    /// Where this section sits in the pipeline config (`source`, `sink`,
    /// `deserializer`) — set after parsing, used to prefix error paths.
    section: Option<&'static str>,
}

/// Remove the framework-reserved `chunk` key from a component body so the
/// connector never sees it. A non-mapping body (a `memory:` null, a scalar)
/// has nothing to peel.
fn peel_chunk(raw: &mut serde_yaml::Value) -> Option<serde_yaml::Value> {
    raw.as_mapping_mut().and_then(|m| m.remove("chunk"))
}

impl ComponentConfig {
    /// Which component implementation this section selects.
    #[must_use]
    pub fn type_tag(&self) -> &str {
        &self.type_tag
    }

    /// Deserialize the opaque body into the component's typed config.
    ///
    /// Errors carry the full dotted path from the pipeline config root,
    /// e.g. `source.kafka.brokers: missing field \`brokers\``.
    pub fn deserialize_into<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        serde_path_to_error::deserialize(self.raw.clone())
            .map_err(|e| self.component_error(None, e))
    }

    /// Resolve the framework-reserved `chunk:` block, if present, into a
    /// runtime [`ChunkConfig`]. `Ok(None)` when the section declared no
    /// `chunk:`. Errors carry the dotted path (`sink.clickhouse.chunk.…`).
    ///
    /// Chunk configures the chain terminal, so this is only meaningful on a
    /// sink section; [`PipelineConfig::validate`](super::PipelineConfig::validate)
    /// rejects a stray `chunk:` on a `source`/`deserializer` section.
    pub(crate) fn resolved_chunk(&self) -> Result<Option<ChunkConfig>, ConfigError> {
        let Some(value) = &self.chunk else {
            return Ok(None);
        };
        let section: ChunkSection = serde_path_to_error::deserialize(value.clone())
            .map_err(|e| self.component_error(Some("chunk"), e))?;
        section.resolve().map(Some)
    }

    /// Whether this section declared a `chunk:` block (without resolving it) —
    /// lets assembly warn about a block that nothing will ever read.
    pub(crate) fn has_chunk(&self) -> bool {
        self.chunk.is_some()
    }

    /// Dotted location of this component in the config, for error messages.
    fn prefix(&self) -> String {
        match self.section {
            Some(section) => format!("{section}.{}", self.type_tag),
            None => self.type_tag.clone(),
        }
    }

    /// A [`ConfigError::Component`] anchored at this section's dotted path,
    /// with an optional sub-block `suffix` (e.g. `chunk`) and the error's own
    /// inner path appended.
    fn component_error(
        &self,
        suffix: Option<&str>,
        e: serde_path_to_error::Error<serde_yaml::Error>,
    ) -> ConfigError {
        let mut context = self.prefix();
        if let Some(suffix) = suffix {
            context.push('.');
            context.push_str(suffix);
        }
        let inner_path = e.path().to_string();
        if inner_path != "." && !inner_path.is_empty() {
            context.push('.');
            context.push_str(&inner_path);
        }
        ConfigError::Component {
            context,
            message: e.into_inner().to_string(),
        }
    }

    pub(super) fn set_section(&mut self, section: &'static str) {
        self.section = Some(section);
    }

    /// Build a component config programmatically (primarily for tests and
    /// `etl-test` pipelines that skip YAML).
    ///
    /// `raw` is the opaque connector body as a [`YamlValue`](super::YamlValue)
    /// (an `etl-core` re-export of `serde_yaml::Value` — see its docs for the
    /// dependency-policy exemption). The framework-reserved `chunk` key is
    /// peeled out of `raw` here, exactly as on the YAML path — see the struct
    /// docs.
    pub fn new(type_tag: impl Into<String>, mut raw: super::YamlValue) -> Self {
        let chunk = peel_chunk(&mut raw);
        ComponentConfig {
            type_tag: type_tag.into(),
            raw,
            chunk,
            section: None,
        }
    }
}

impl<'de> Deserialize<'de> for ComponentConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        let mapping = serde_yaml::Mapping::deserialize(deserializer)?;
        if mapping.len() != 1 {
            return Err(D::Error::custom(format!(
                "a component section must be a single-key mapping selecting the \
                 component type, e.g. `kafka: {{ ... }}` — found {} keys",
                mapping.len()
            )));
        }
        let (key, mut value) = mapping.into_iter().next().expect("len checked above");
        let type_tag = key.as_str().ok_or_else(|| {
            D::Error::custom("component type tag must be a string, e.g. `kafka:`")
        })?;
        let chunk = peel_chunk(&mut value);
        Ok(ComponentConfig {
            type_tag: type_tag.to_owned(),
            raw: value,
            chunk,
            section: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FakeKafkaConfig {
        brokers: String,
        topic: String,
        #[serde(default)]
        batch: Batch,
    }

    #[derive(Debug, PartialEq, Deserialize, Default)]
    #[serde(deny_unknown_fields)]
    struct Batch {
        max_rows: Option<u64>,
    }

    fn parse(yaml: &str) -> Result<ComponentConfig, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn parses_single_key_section_and_deserializes_body() {
        let cc = parse("kafka:\n  brokers: k1:9092\n  topic: orders\n").unwrap();
        assert_eq!(cc.type_tag(), "kafka");
        let typed: FakeKafkaConfig = cc.deserialize_into().unwrap();
        assert_eq!(typed.brokers, "k1:9092");
        assert_eq!(typed.topic, "orders");
    }

    #[test]
    fn rejects_zero_and_multiple_keys() {
        let err = parse("{}").unwrap_err().to_string();
        assert!(err.contains("single-key mapping"), "{err}");
        assert!(err.contains("0 keys"), "{err}");

        let err = parse("kafka: {}\nmemory: {}\n").unwrap_err().to_string();
        assert!(err.contains("2 keys"), "{err}");
    }

    #[test]
    fn rejects_non_string_tag() {
        let err = parse("7: {}").unwrap_err().to_string();
        assert!(err.contains("type tag must be a string"), "{err}");
    }

    #[test]
    fn error_paths_include_section_tag_and_field() {
        let mut cc = parse("kafka:\n  brokers: k1:9092\n").unwrap();
        cc.set_section("source");
        let err = cc.deserialize_into::<FakeKafkaConfig>().unwrap_err();
        let text = err.to_string();
        assert!(text.starts_with("source.kafka"), "{text}");
        assert!(text.contains("topic"), "{text}");
    }

    #[test]
    fn nested_error_paths_point_at_the_field() {
        let mut cc =
            parse("kafka:\n  brokers: b\n  topic: t\n  batch:\n    max_rows: lots\n").unwrap();
        cc.set_section("source");
        let err = cc.deserialize_into::<FakeKafkaConfig>().unwrap_err();
        let text = err.to_string();
        assert!(text.contains("source.kafka.batch.max_rows"), "{text}");
    }

    #[test]
    fn unknown_field_in_component_body_is_rejected_with_path() {
        let cc = parse("kafka:\n  brokers: b\n  topic: t\n  bogus: 1\n").unwrap();
        let err = cc.deserialize_into::<FakeKafkaConfig>().unwrap_err();
        assert!(err.to_string().contains("bogus"), "{err}");
    }

    #[test]
    fn empty_body_deserializes_into_defaultable_types() {
        #[derive(Debug, Deserialize, Default)]
        struct Empty {}
        let cc = parse("memory:\n").unwrap();
        assert_eq!(cc.type_tag(), "memory");
        // `memory:` with no body is a null value; struct with no required
        // fields must accept it.
        let _typed: Option<Empty> = cc.deserialize_into().unwrap();
    }

    #[test]
    fn reserved_chunk_is_peeled_so_the_connector_never_sees_it() {
        // A `chunk:` block sits beside the connector's own keys. It must be
        // stripped before the body reaches the (deny_unknown_fields) connector
        // config, and be resolvable by the framework.
        let cc = parse(
            "kafka:\n  brokers: b\n  topic: t\n  chunk:\n    target_bytes: 256KiB\n    encode_policy: fail\n",
        )
        .unwrap();
        assert_eq!(cc.type_tag(), "kafka");
        // The connector's strict config still deserializes — `chunk` is gone.
        let typed: FakeKafkaConfig = cc.deserialize_into().unwrap();
        assert_eq!(typed.brokers, "b");
        // The framework resolves the peeled block.
        let chunk = cc.resolved_chunk().unwrap().expect("chunk present");
        assert_eq!(chunk.target_bytes, 256 * 1024);
        assert_eq!(chunk.encode_policy, crate::error::ErrorPolicy::Fail);
    }

    #[test]
    fn programmatic_new_also_peels_chunk() {
        let body: super::super::YamlValue =
            serde_yaml::from_str("brokers: b\ntopic: t\nchunk:\n  target_bytes: 32KiB\n").unwrap();
        let cc = ComponentConfig::new("kafka", body);
        let _typed: FakeKafkaConfig = cc.deserialize_into().unwrap();
        let chunk = cc.resolved_chunk().unwrap().expect("chunk present");
        assert_eq!(chunk.target_bytes, 32 * 1024);
    }

    #[test]
    fn no_chunk_block_resolves_to_none() {
        let cc = parse("kafka:\n  brokers: b\n  topic: t\n").unwrap();
        assert!(cc.resolved_chunk().unwrap().is_none());
        // A non-mapping body has nothing to peel.
        let cc = parse("memory:\n").unwrap();
        assert!(cc.resolved_chunk().unwrap().is_none());
    }

    #[test]
    fn bare_chunk_key_resolves_to_the_defaults() {
        // `chunk:` with no body (a natural "defaults, please") — the null
        // value deserializes as an empty mapping, i.e. 64 KiB / Skip, matching
        // `chunk: {}`. Pinned so a serde change can't turn it into an error.
        let cc = parse("clickhouse:\n  chunk:\n").unwrap();
        let chunk = cc.resolved_chunk().unwrap().expect("chunk present");
        assert_eq!(chunk.target_bytes, 64 * 1024);
        assert_eq!(chunk.encode_policy, crate::error::ErrorPolicy::Skip);
    }

    #[test]
    fn malformed_chunk_surfaces_at_resolve_with_a_dotted_path() {
        let mut cc = parse("clickhouse:\n  chunk:\n    target_bytes: 0B\n").unwrap();
        cc.set_section("sink");
        let err = cc.resolved_chunk().unwrap_err().to_string();
        assert!(err.contains("chunk.target_bytes"), "{err}");
    }
}
