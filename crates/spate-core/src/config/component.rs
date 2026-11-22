//! Opaque per-component configuration passthrough.

use super::ConfigError;
use super::redact;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::fmt;

/// An opaque component section: `{ <type_tag>: { ...connector config... } }`.
///
/// The framework records which component type the section selects (`kafka`,
/// `clickhouse`, `memory`, ...) and hands the body to that component's
/// factory, which deserializes it into its own typed config via
/// [`deserialize_into`](Self::deserialize_into). The body is passed through
/// untouched. `chunk` is reserved: the framework refuses it inside any body,
/// so a connector must not declare a `chunk` field of its own.
///
/// The nested-block shape (exactly one key) lets every typed struct in the
/// tree keep `deny_unknown_fields`. A flattened shape would disable that
/// check.
#[derive(Clone, PartialEq)]
pub struct ComponentConfig {
    type_tag: String,
    raw: serde_yaml::Value,
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

    /// The path of the `deserializer:` section selecting `tag`.
    ///
    /// ```
    /// use spate_core::config::SectionPath;
    ///
    /// let err = SectionPath::deserializer("avro").invalid("mode", "is not a mode");
    /// assert_eq!(err.to_string(), "deserializer.avro.mode: is not a mode");
    /// ```
    #[must_use]
    pub fn deserializer(tag: &str) -> SectionPath {
        SectionPath(format!("deserializer.{tag}"))
    }

    /// The path `path` from the config root, such as `sinks.eu`.
    pub(crate) fn at(path: impl Into<String>) -> SectionPath {
        SectionPath(path.into())
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

    /// Logs a warning that the credential at `field` is sent without TLS.
    ///
    /// `reason` names the setting that leaves the connection unencrypted. The
    /// line reads `<path>.<field>: <reason>; the credential is sent without
    /// TLS`. Call it once, after the section's own checks pass and before the
    /// client is created.
    ///
    /// Both arguments are `'static`, so a value read from the config, such as
    /// a password or a URL, cannot reach the line.
    ///
    /// ```
    /// use spate_core::config::SectionPath;
    ///
    /// // Logs `sink.example.password: endpoint uses http://; the credential is
    /// // sent without TLS`.
    /// SectionPath::sink("example").warn_cleartext_credential("password", "endpoint uses http://");
    /// ```
    ///
    /// A string read at runtime does not compile:
    ///
    /// ```compile_fail
    /// use spate_core::config::SectionPath;
    ///
    /// let password = String::from("hunter2");
    /// SectionPath::sink("example").warn_cleartext_credential(&password, "endpoint uses http://");
    /// ```
    pub fn warn_cleartext_credential(&self, field: &'static str, reason: &'static str) {
        tracing::warn!(
            "{}.{field}: {reason}; the credential is sent without TLS",
            self.0
        );
    }
}

// Hand-written: `raw` holds interpolated credentials. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl fmt::Debug for ComponentConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ComponentConfig {
            type_tag,
            raw,
            section,
        } = self;
        f.debug_struct("ComponentConfig")
            .field("type_tag", type_tag)
            .field("raw", &redact::yaml(raw))
            .field("section", section)
            .finish()
    }
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

    /// Whether the body is a mapping holding `key`. A non-mapping body (a
    /// `memory:` null, a scalar) holds no key.
    pub(crate) fn has_body_key(&self, key: &str) -> bool {
        self.raw.as_mapping().is_some_and(|m| m.contains_key(key))
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
    /// (a `spate-core` re-export of `yaml_serde`'s `Value`, an INV-6
    /// exception), passed through untouched as on the YAML path.
    pub fn new(type_tag: impl Into<String>, raw: super::YamlValue) -> Self {
        ComponentConfig {
            type_tag: type_tag.into(),
            raw,
            section: None,
        }
    }
}

/// A [`ConfigError::Component`] anchored at `at`, with an optional sub-block
/// `suffix` (e.g. `chunk`) and the error's own inner path appended.
pub(super) fn component_error(
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

/// Split a role section (`feed:`, a sink section) into the values of its
/// `common` keys, in order, and its one backend. Every key left after the
/// common ones names a backend, so a misspelled common key shows up as a
/// second backend. `role` names the section in an error, and `example`
/// shows a backend key.
pub(super) fn split_backend<E: serde::de::Error, const N: usize>(
    mut keys: serde_yaml::Mapping,
    role: &str,
    common: [&str; N],
    example: &str,
) -> Result<([Option<serde_yaml::Value>; N], ComponentConfig), E> {
    let values = common.map(|key| keys.remove(key));
    if keys.len() != 1 {
        let found: Vec<String> = keys
            .keys()
            .map(|k| match k.as_str() {
                Some(name) => format!("`{name}`"),
                None => "a non-string key".to_owned(),
            })
            .collect();
        return Err(E::custom(if found.is_empty() {
            format!("{role} names no backend; add one, e.g. {example}")
        } else {
            format!(
                "{role} must name exactly one backend beside {}; found {}",
                key_list(&common),
                found.join(", ")
            )
        }));
    }
    let backend = ComponentConfig::deserialize(serde_yaml::Value::Mapping(keys)).map_err(E::custom)?;
    Ok((values, backend))
}

/// `` `a`, `b` and `c` ``.
fn key_list(keys: &[&str]) -> String {
    let quoted: Vec<String> = keys.iter().map(|k| format!("`{k}`")).collect();
    match quoted.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
        _ => quoted.concat(),
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
            "clickhouse:\n  password: hunter2\n  shards: [{replicas: [http://u:hunter2@ch]}]\n",
        )
        .unwrap();
        let printed = format!("{cc:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        for visible in ["clickhouse", "password", "replicas"] {
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
            (
                SectionPath::deserializer("kafka"),
                "deserializer.kafka.brokers",
            ),
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

    /// The cleartext warning is one WARN line that names the credential's
    /// key and the reason.
    #[test]
    fn a_cleartext_credential_logs_one_warning_naming_the_key() {
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            SectionPath::sink("example")
                .warn_cleartext_credential("password", "endpoint uses http://");
        });
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("WARN"), "{lines:?}");
        assert!(
            lines[0].contains(
                "sink.example.password: endpoint uses http://; the credential is sent without TLS"
            ),
            "{lines:?}"
        );
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

    /// The body reaches the connector as written, a `chunk` key included;
    /// the pipeline config refuses that key, not the section.
    #[test]
    fn the_body_is_passed_through_untouched() {
        let body: super::super::YamlValue =
            serde_yaml::from_str("brokers: b\ntopic: t\nchunk: { target_bytes: 32KiB }\n").unwrap();
        let cc = ComponentConfig::new("kafka", body);
        assert!(cc.has_body_key("chunk"));
        let err = cc.deserialize_into::<FakeKafkaConfig>().unwrap_err();
        assert!(err.to_string().contains("chunk"), "{err}");
        assert!(!parse("memory:\n").unwrap().has_body_key("chunk"));
    }

    /// The key list in a split error reads as prose for one, two and more keys.
    #[test]
    fn a_key_list_joins_the_last_with_and() {
        assert_eq!(key_list(&["a"]), "`a`");
        assert_eq!(key_list(&["a", "b"]), "`a` and `b`");
        assert_eq!(key_list(&["a", "b", "c"]), "`a`, `b` and `c`");
    }
}
