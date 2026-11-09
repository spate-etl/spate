//! Opaque per-component configuration passthrough.

use super::ConfigError;
use super::chunk::ChunkSection;
use super::redact;
use crate::ops::ChunkConfig;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::fmt;

/// An opaque component section: `{ <type_tag>: { ...connector config... } }`.
///
/// The framework records which component type the section selects (`kafka`,
/// `clickhouse`, `memory`, ...) and hands the body to that component's
/// factory, which deserializes it into its own typed config via
/// [`deserialize_into`](Self::deserialize_into). **`chunk` is
/// framework-reserved** and is the one exception to that opacity. It
/// configures the chain terminal, not the connector, and is peeled out of the
/// body at construction. A connector must not declare its own `chunk` field;
/// it would never receive a value (on a sink section the framework consumes
/// the key, and on a `source`/`deserializer` section the key is rejected
/// outright).
///
/// The nested-block shape (exactly one key) lets every typed struct in the
/// tree keep `deny_unknown_fields`. A flattened shape would disable that
/// check.
#[derive(Clone, PartialEq)]
pub struct ComponentConfig {
    type_tag: String,
    raw: serde_yaml::Value,
    /// The framework-reserved `chunk:` block, peeled out of `raw` at
    /// construction so the connector's `deny_unknown_fields` never sees it.
    /// Stored unparsed and resolved lazily by
    /// [`resolved_chunk`](Self::resolved_chunk). Construction, including the
    /// infallible [`new`](Self::new), must not fail on a malformed block; it
    /// surfaces at assembly/validation instead.
    chunk: Option<serde_yaml::Value>,
    /// Where this section sits in the pipeline config (`source`, `sink`,
    /// `sinks.<name>`, `deserializer`, `feed`). Set after parsing, used to
    /// prefix error paths.
    section: Option<Cow<'static, str>>,
}

/// The dotted path of a component section from the config root, such as
/// `source.kafka` or `sinks.eu.clickhouse`, for building a config error that
/// names where the section sits.
///
/// [`ComponentConfig::path`] gives the path of a loaded section, which names
/// the entry under `sinks:`. The constructors give the path for a config built
/// in code, which has no section.
///
/// # Examples
///
/// ```
/// use spate_core::config::SectionPath;
///
/// let err = SectionPath::source("kafka").invalid("brokers", "must not be empty");
/// assert_eq!(err.to_string(), "source.kafka.brokers: must not be empty");
///
/// let err = SectionPath::sink("kafka").invalid("topic", "must not be empty");
/// assert_eq!(err.to_string(), "sink.kafka.topic: must not be empty");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SectionPath(String);

impl SectionPath {
    /// The path of the `source:` section selecting `tag`.
    #[must_use]
    pub fn source(tag: &str) -> SectionPath {
        SectionPath(format!("source.{tag}"))
    }

    /// The path of the single `sink:` section selecting `tag`.
    #[must_use]
    pub fn sink(tag: &str) -> SectionPath {
        SectionPath(format!("sink.{tag}"))
    }

    /// The path of the backend `tag` in the `feed:` section.
    #[must_use]
    pub fn feed(tag: &str) -> SectionPath {
        SectionPath(format!("feed.{tag}"))
    }

    /// A [`ConfigError::Component`] for a rule `field` breaks, with context
    /// `<path>.<field>`. `field` is the key's path inside the section, such as
    /// `brokers` or `chunk.target_bytes`.
    #[must_use]
    pub fn invalid(&self, field: &str, message: impl Into<String>) -> ConfigError {
        ConfigError::Component {
            context: format!("{}.{field}", self.0),
            message: message.into(),
        }
    }

    /// A [`ConfigError::Component`] about the whole section, with context
    /// `<path>`. It serves an error no single field owns, and an error whose
    /// text already starts at its field, such as a
    /// [`ReservedKeyError`](super::ReservedKeyError).
    ///
    /// ```
    /// use spate_core::config::{Denylist, SectionPath};
    ///
    /// let owned = Denylist::new(&[("acks", "forced to all")]);
    /// let refusal = owned.check("rdkafka", ["acks"]).expect_err("owned key");
    /// let err = SectionPath::sink("kafka").error(refusal.to_string());
    /// assert_eq!(
    ///     err.to_string(),
    ///     "sink.kafka: rdkafka.\"acks\" cannot be overridden: forced to all"
    /// );
    /// ```
    #[must_use]
    pub fn error(&self, message: impl Into<String>) -> ConfigError {
        ConfigError::Component {
            context: self.0.clone(),
            message: message.into(),
        }
    }
}

// Hand-written: `raw` holds interpolated credentials. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl fmt::Debug for ComponentConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ComponentConfig {
            type_tag,
            raw,
            chunk,
            section,
        } = self;
        f.debug_struct("ComponentConfig")
            .field("type_tag", type_tag)
            .field("raw", &redact::yaml(raw))
            .field("chunk", &chunk.as_ref().map(redact::yaml))
            .field("section", section)
            .finish()
    }
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
    /// e.g. `source.kafka.brokers: missing field \`brokers\``, and name the
    /// expected type without the value.
    pub fn deserialize_into<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        serde_path_to_error::deserialize(self.raw.clone())
            .map_err(|e| component_error(self.path(), None, e))
    }

    /// Resolve the framework-reserved `chunk:` block, if present, into a
    /// runtime [`ChunkConfig`]. `Ok(None)` when the section declared no
    /// `chunk:`. Errors carry the dotted path (`sink.clickhouse.chunk.…`).
    ///
    /// Chunk configures the chain terminal, so this is only meaningful on a
    /// sink section; [`PipelineConfig::validate`](super::PipelineConfig::validate)
    /// rejects a stray `chunk:` on a `source`/`deserializer` section.
    pub(crate) fn resolved_chunk(&self) -> Result<Option<ChunkConfig>, ConfigError> {
        self.chunk_at(self.path())
    }

    /// [`resolved_chunk`](Self::resolved_chunk) for the sink at `address`
    /// (`sink` or `sinks.<name>`), so its errors name the sink even when the
    /// section was placed in the config without a label.
    pub(crate) fn resolved_sink_chunk(
        &self,
        address: &str,
    ) -> Result<Option<ChunkConfig>, ConfigError> {
        self.chunk_at(SectionPath(format!("{address}.{}", self.type_tag)))
    }

    fn chunk_at(&self, at: SectionPath) -> Result<Option<ChunkConfig>, ConfigError> {
        let Some(value) = &self.chunk else {
            return Ok(None);
        };
        let section: ChunkSection = serde_path_to_error::deserialize(value.clone())
            .map_err(|e| component_error(at.clone(), Some("chunk"), e))?;
        section.resolve(&at).map(Some)
    }

    /// Whether this section declared a `chunk:` block (without resolving it).
    /// Lets assembly warn about a block that nothing will ever read.
    pub(crate) fn has_chunk(&self) -> bool {
        self.chunk.is_some()
    }

    /// This section's path from the config root, such as `source.kafka` or
    /// `sinks.eu.clickhouse`. A section built with [`new`](Self::new) and
    /// never placed in a [`PipelineConfig`](super::PipelineConfig) has no
    /// position, and its path is the type tag alone.
    #[must_use]
    pub fn path(&self) -> SectionPath {
        match &self.section {
            Some(section) => SectionPath(format!("{section}.{}", self.type_tag)),
            None => SectionPath(self.type_tag.clone()),
        }
    }

    pub(super) fn set_section(&mut self, section: impl Into<Cow<'static, str>>) {
        self.section = Some(section.into());
    }

    /// Build a component config programmatically (primarily for tests and
    /// `spate-test` pipelines that skip YAML).
    ///
    /// `raw` is the opaque connector body as a [`YamlValue`](super::YamlValue)
    /// (an `spate-core` re-export of `serde_yaml::Value`; see its docs for the
    /// dependency-policy exemption). The framework-reserved `chunk` key is
    /// peeled out of `raw` here, as on the YAML path. See the struct docs.
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

/// A [`ConfigError::Component`] anchored at `at`, with an optional sub-block
/// `suffix` (e.g. `chunk`) and the error's own inner path appended.
fn component_error(
    at: SectionPath,
    suffix: Option<&str>,
    e: serde_path_to_error::Error<serde_yaml::Error>,
) -> ConfigError {
    let SectionPath(mut context) = at;
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
        message: redact::error_message(&e.into_inner().to_string()).into_owned(),
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

    /// `Debug` shows the section's keys and shape and none of its values.
    #[test]
    fn debug_never_prints_component_values() {
        let cc = parse(
            "clickhouse:\n  password: hunter2\n  shards: [{replicas: [http://u:hunter2@ch]}]\n  \
             chunk: {max_bytes: 1MiB}\n",
        )
        .unwrap();
        let printed = format!("{cc:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(!printed.contains("1MiB"), "{printed}");
        for visible in ["clickhouse", "password", "replicas", "max_bytes"] {
            assert!(printed.contains(visible), "{visible}: {printed}");
        }
    }

    /// Each role's path renders `<role>.<tag>.<field>: <message>` as a
    /// `Component` error.
    #[test]
    fn a_section_path_names_role_tag_and_field() {
        for (path, want) in [
            (SectionPath::source("kafka"), "source.kafka.brokers"),
            (SectionPath::sink("kafka"), "sink.kafka.brokers"),
            (SectionPath::feed("kafka"), "feed.kafka.brokers"),
        ] {
            let err = path.invalid("brokers", "must not be empty");
            assert!(
                matches!(&err, ConfigError::Component { context, message }
                    if context == want && message == "must not be empty"),
                "{err:?}"
            );
            assert_eq!(err.to_string(), format!("{want}: must not be empty"));
        }
    }

    /// An error about a whole section is anchored at the section's path,
    /// with no field after it.
    #[test]
    fn a_whole_section_error_names_the_section() {
        let err = SectionPath::sink("fake").error("producer creation failed");
        assert!(
            matches!(&err, ConfigError::Component { context, message }
                if context == "sink.fake" && message == "producer creation failed"),
            "{err:?}"
        );
        assert_eq!(err.to_string(), "sink.fake: producer creation failed");
    }

    /// A section placed in a config starts at its position; one built in
    /// code and never placed is its type tag alone.
    #[test]
    fn a_component_path_starts_at_its_section() {
        let mut cc = parse("kafka:\n  brokers: k1:9092\n").unwrap();
        assert_eq!(cc.path(), SectionPath("kafka".into()));
        cc.set_section("source");
        assert_eq!(cc.path(), SectionPath::source("kafka"));
    }

    /// A body that fails as a whole, such as a scalar where a mapping is
    /// expected, is anchored at the section with nothing appended.
    #[test]
    fn a_whole_body_error_is_anchored_at_the_section() {
        let mut cc = parse("kafka: 7\n").unwrap();
        cc.set_section("source");
        let err = cc.deserialize_into::<FakeKafkaConfig>().unwrap_err();
        assert!(
            matches!(&err, ConfigError::Component { context, .. } if context == "source.kafka"),
            "{err:?}"
        );
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

    /// A type error names the field and the expected type and does not quote
    /// the value. Regression for #764.
    #[test]
    fn type_errors_omit_the_value() {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Mechanism {
            Plain,
            Scram,
        }

        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Credentials {
            password: Option<String>,
            port: Option<u16>,
            mechanism: Option<Mechanism>,
            #[serde(default, with = "humantime_serde")]
            timeout: Option<std::time::Duration>,
        }

        for (body, secret, path, expected) in [
            (
                "password: 918273645",
                "918273645",
                "password",
                "expected a string",
            ),
            ("password: true", "true", "password", "expected a string"),
            ("password: 1.5e3", "1500", "password", "expected a string"),
            ("port: hunter2", "hunter2", "port", "expected u16"),
            ("port: 918273645", "918273645", "port", "expected u16"),
            (
                "mechanism: hunter2",
                "hunter2",
                "mechanism",
                "expected `plain` or `scram`",
            ),
            (
                "mechanism: !hunter2 ~",
                "hunter2",
                "mechanism",
                "expected `plain` or `scram`",
            ),
            (
                "timeout: hunter2",
                "hunter2",
                "timeout",
                "expected a duration",
            ),
        ] {
            let mut cc = parse(&format!("clickhouse:\n  {body}\n")).unwrap();
            cc.set_section("sink");
            let err = cc.deserialize_into::<Credentials>().unwrap_err();
            for shown in [err.to_string(), format!("{err:?}")] {
                assert!(!shown.contains(secret), "{body}: {shown}");
                assert!(
                    shown.contains(&format!("sink.clickhouse.{path}")),
                    "{body}: {shown}"
                );
                assert!(shown.contains(expected), "{body}: {shown}");
            }
        }
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
        // The connector's strict config still deserializes; `chunk` is gone.
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
        // `chunk:` with no body (a natural "defaults, please"). The null value
        // deserializes as an empty mapping, i.e. 64 KiB / Skip, matching
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
