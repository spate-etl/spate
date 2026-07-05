//! Opaque per-component configuration passthrough.

use super::ConfigError;
use serde::Deserialize;
use serde::de::DeserializeOwned;

/// An opaque component section: `{ <type_tag>: { ...connector config... } }`.
///
/// The framework never interprets the body — it records which component
/// type the section selects (`kafka`, `clickhouse`, `memory`, ...) and hands
/// the raw YAML to that component's factory, which deserializes it into its
/// own typed config via [`deserialize_into`](Self::deserialize_into).
///
/// The nested-block shape (exactly one key) is what lets every typed struct
/// in the tree keep `deny_unknown_fields` — a flattened shape would disable
/// that check.
#[derive(Debug, Clone, PartialEq)]
pub struct ComponentConfig {
    type_tag: String,
    raw: serde_yaml::Value,
    /// Where this section sits in the pipeline config (`source`, `sink`,
    /// `deserializer`) — set after parsing, used to prefix error paths.
    section: Option<&'static str>,
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
        serde_path_to_error::deserialize(self.raw.clone()).map_err(|e| {
            let inner_path = e.path().to_string();
            let mut context = self.prefix();
            if inner_path != "." && !inner_path.is_empty() {
                context.push('.');
                context.push_str(&inner_path);
            }
            ConfigError::Component {
                context,
                message: e.into_inner().to_string(),
            }
        })
    }

    /// Dotted location of this component in the config, for error messages.
    fn prefix(&self) -> String {
        match self.section {
            Some(section) => format!("{section}.{}", self.type_tag),
            None => self.type_tag.clone(),
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
    /// dependency-policy exemption).
    pub fn new(type_tag: impl Into<String>, raw: super::YamlValue) -> Self {
        ComponentConfig {
            type_tag: type_tag.into(),
            raw,
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
        let (key, value) = mapping.into_iter().next().expect("len checked above");
        let type_tag = key.as_str().ok_or_else(|| {
            D::Error::custom("component type tag must be a string, e.g. `kafka:`")
        })?;
        Ok(ComponentConfig {
            type_tag: type_tag.to_owned(),
            raw: value,
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
}
