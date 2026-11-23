//! Configuration and construction.
//!
//! The `deserializer: { avro: { ... } }` section of the pipeline YAML is
//! handed here as an opaque
//! [`ComponentConfig`](spate_core::config::ComponentConfig):
//!
//! ```yaml
//! deserializer:
//!   avro:
//!     mode: confluent                # confluent | raw | single_object
//!     registry:                      # required for confluent
//!       url: ${SCHEMA_REGISTRY_URL}
//!       username: ${SR_USER}         # optional basic auth
//!       password: ${SR_PASSWORD}
//!       tls:
//!         root_ca: /etc/spate/registry-ca.pem  # optional: a private CA
//!     prewarm_subjects: [orders-value]
//!     negative_cache_ttl: 30s
//!     reader_schema:                 # optional: pin the resolved shape
//!       path: /etc/spate/orders.avsc   # (or `inline: '{"type": ...}'`)
//!     # schema: { inline | path }    # required for raw / single_object
//! ```

use crate::cache::CompiledSchema;
use crate::datum::AvroDatumDeserializer;
use crate::de::ReaderSide;
use crate::deser::{AvroSerdeDeserializer, AvroValueDeserializer, DecoderCore, SchemaSourceMode};
use crate::registry::{Endpoint, Rejection, http_client, registry_url, spawn_fetcher};
use apache_avro::Schema;
use apache_avro::rabin::Rabin;
use reqwest::Url;
use serde::Deserialize;
use spate_core::clock::SyncClock;
use spate_core::config::{
    ComponentConfig, ConfigError, DurationBounds, SectionPath, check_duration, redact,
};
use spate_core::tls::TlsSection;
use std::sync::Arc;
use std::time::Duration;

/// The deserializer's component type, and the tag its section sits under.
pub(crate) const TAG: &str = "avro";

/// The path every refusal of this section starts at, `deserializer.avro`.
pub(crate) fn section_path() -> SectionPath {
    SectionPath::deserializer(TAG)
}

/// Payload framing mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AvroMode {
    /// Confluent wire format with a schema registry.
    #[default]
    Confluent,
    /// Bare datums with a fixed schema.
    Raw,
    /// Avro single-object encoding with a fixed schema.
    SingleObject,
}

/// A schema provided inline or from a file. Set exactly one field.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct AvroSchemaSection {
    /// The schema JSON itself.
    pub inline: Option<String>,
    /// Path to a `.avsc` file (Kubernetes: a mounted ConfigMap).
    pub path: Option<std::path::PathBuf>,
}

impl AvroSchemaSection {
    /// An inline schema.
    #[must_use]
    pub fn inline(schema: impl Into<String>) -> Self {
        AvroSchemaSection {
            inline: Some(schema.into()),
            path: None,
        }
    }

    /// A schema loaded from a file.
    #[must_use]
    pub fn path(path: impl Into<std::path::PathBuf>) -> Self {
        AvroSchemaSection {
            inline: None,
            path: Some(path.into()),
        }
    }

    /// Refuses a source with both `inline` and `path` set, or neither, at
    /// `field`.
    fn check_shape(&self, field: &str) -> Result<(), ConfigError> {
        if self.inline.is_some() == self.path.is_some() {
            return Err(section_path().invalid(field, "needs exactly one of `inline` or `path`"));
        }
        Ok(())
    }

    /// Load the schema's original JSON source (`CompiledSchema::compile`
    /// needs the source text, not a re-rendered canonical form). The caller
    /// has run [`check_shape`](Self::check_shape), so one field is set.
    fn load_text(&self, field: &str) -> Result<String, ConfigError> {
        match &self.path {
            Some(path) => std::fs::read_to_string(path)
                .map_err(|e| section_path().invalid(field, format!("{}: {e}", path.display()))),
            None => Ok(self.inline.clone().unwrap_or_default()),
        }
    }

    /// Load and parse the schema with `apache-avro` (used for the optional
    /// `reader_schema`).
    fn load(&self, field: &str) -> Result<Schema, ConfigError> {
        let text = self.load_text(field)?;
        Schema::parse_str(&text)
            .map_err(|e| section_path().invalid(field, format!("failed to parse: {e}")))
    }
}

/// Registry connection section.
///
/// Construct with [`AvroRegistrySection::new`] and set the optional fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct AvroRegistrySection {
    /// Base URL of the Confluent-compatible schema registry, without credentials.
    /// [`AvroDeserializerConfig::validate`] refuses a URL that carries
    /// credentials.
    pub url: String,
    /// Basic-auth username (optional).
    #[serde(default)]
    pub username: Option<String>,
    /// Basic-auth password (optional; use `${VAR}` interpolation). Requires
    /// `username`.
    #[serde(default)]
    pub password: Option<String>,
    /// Trust settings: `registry.tls.root_ca`, which requires an `https://`
    /// URL.
    #[serde(default)]
    pub tls: TlsSection,
}

impl AvroRegistrySection {
    /// A registry at `url`, without basic auth or extra trust roots.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        AvroRegistrySection {
            url: url.into(),
            username: None,
            password: None,
            tls: TlsSection::default(),
        }
    }
}

// Hand-written: `password` and the URL userinfo are credentials. The
// destructure lists every field so a new one cannot reach `Debug` unredacted.
impl std::fmt::Debug for AvroRegistrySection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let AvroRegistrySection {
            url,
            username,
            password,
            tls,
        } = self;
        f.debug_struct("AvroRegistrySection")
            .field("url", &redact::url(url))
            .field("username", username)
            .field("password", &redact::option(password))
            .field("tls", tls)
            .finish()
    }
}

/// The `deserializer: { avro: ... }` section.
///
/// Build one in code with the constructor for its mode,
/// [`confluent`](Self::confluent), [`raw`](Self::raw) or
/// [`single_object`](Self::single_object), then set the optional fields.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct AvroDeserializerConfig {
    /// Payload framing mode.
    pub mode: AvroMode,
    /// Registry connection (required in `confluent` mode).
    pub registry: Option<AvroRegistrySection>,
    /// Subjects whose latest schemas are fetched at startup. The schema cache
    /// holds at most 1,024 compiled schemas and an estimated 64 MiB of them,
    /// so a list above either bound keeps the subjects listed last. An evicted
    /// schema is fetched again by id on its next record.
    pub prewarm_subjects: Vec<String>,
    /// The longest a failed schema id stays negatively cached before a
    /// refetch is allowed. The cache holds at most 1,024 failed ids and drops
    /// the oldest for a new one, so a dropped id is refetched on its next
    /// record. Must be at least `100ms`;
    /// [`validate`](Self::validate) refuses a shorter value in every mode.
    #[serde(with = "humantime_serde")]
    pub negative_cache_ttl: Duration,
    /// Optional reader schema pinning the resolved record shape
    /// (`confluent` and `raw` modes). One that recurses through a union
    /// limits how deep a datum may recurse: see
    /// [`AvroValueDeserializer`](crate::AvroValueDeserializer).
    pub reader_schema: Option<AvroSchemaSection>,
    /// The writer schema (required in `raw` and `single_object` modes).
    pub schema: Option<AvroSchemaSection>,
}

/// The shortest `negative_cache_ttl` a build accepts.
const MIN_NEGATIVE_CACHE_TTL: Duration = Duration::from_millis(100);

impl Default for AvroDeserializerConfig {
    fn default() -> Self {
        AvroDeserializerConfig {
            mode: AvroMode::Confluent,
            registry: None,
            prewarm_subjects: Vec::new(),
            negative_cache_ttl: Duration::from_secs(30),
            reader_schema: None,
            schema: None,
        }
    }
}

/// What a valid section resolves to before anything is loaded.
enum Checked<'a> {
    /// `confluent`, with the registry and its checked URL.
    Confluent {
        registry: &'a AvroRegistrySection,
        url: Url,
    },
    /// `raw` or `single_object`, with the writer schema's source.
    Fixed { schema: &'a AvroSchemaSection },
}

impl AvroDeserializerConfig {
    /// A `confluent` section reading writer schemas from `registry`.
    #[must_use]
    pub fn confluent(registry: AvroRegistrySection) -> Self {
        // The default mode is `confluent`.
        AvroDeserializerConfig {
            registry: Some(registry),
            ..AvroDeserializerConfig::default()
        }
    }

    /// A `raw` section decoding bare datums with `schema`.
    #[must_use]
    pub fn raw(schema: AvroSchemaSection) -> Self {
        AvroDeserializerConfig {
            mode: AvroMode::Raw,
            schema: Some(schema),
            ..AvroDeserializerConfig::default()
        }
    }

    /// A `single_object` section decoding single-object encoded datums with
    /// `schema`.
    #[must_use]
    pub fn single_object(schema: AvroSchemaSection) -> Self {
        AvroDeserializerConfig {
            mode: AvroMode::SingleObject,
            schema: Some(schema),
            ..AvroDeserializerConfig::default()
        }
    }

    /// Deserialize and validate from the pipeline's opaque component section.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] when the section does not deserialize into
    /// this type, or when [`validate`](Self::validate) refuses it.
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError> {
        let cfg: AvroDeserializerConfig = section.deserialize_into()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Checks every rule that reads no file, parses no schema and makes no
    /// network call. [`AvroDeserializerBuilder::new`] runs it before loading
    /// and parsing schemas and building the registry client.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] at `deserializer.avro` for the first rule
    /// the section breaks: in `confluent` mode a `registry.url` that is not
    /// an `http` or `https` URL or carries userinfo, or a `registry.password`
    /// without a `registry.username`; a `negative_cache_ttl` under `100ms`;
    /// a `registry.tls.root_ca` beside a `registry.url` that is not
    /// `https://`; a section the mode does not use or a missing one it needs;
    /// and a `schema` or `reader_schema` with both `inline` and `path`, or
    /// neither.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.checked().map(|_| ())
    }

    fn checked(&self) -> Result<Checked<'_>, ConfigError> {
        let at = section_path();
        let confluent = match self.registry.as_ref() {
            Some(registry) if self.mode == AvroMode::Confluent => {
                let url = registry_url(&registry.url)?;
                if registry.password.is_some() && registry.username.is_none() {
                    return Err(
                        at.invalid("registry.password", "is set, but registry.username is not")
                    );
                }
                Some((registry, url))
            }
            _ => None,
        };
        // A negative entry has to outlive the replay of the record it blocks, or
        // each replay refetches the id and the record never reaches its policy.
        const NEGATIVE_CACHE_TTL: DurationBounds = DurationBounds::at_least(MIN_NEGATIVE_CACHE_TTL);
        check_duration(self.negative_cache_ttl, NEGATIVE_CACHE_TTL).map_err(|e| {
            at.invalid(
                "negative_cache_ttl",
                format!(
                    "{e}: a shorter entry can expire before the blocked record is replayed, \
                     and each replay then fetches the schema id again"
                ),
            )
        })?;
        let checked = match self.mode {
            AvroMode::Confluent => {
                let Some((registry, url)) = confluent else {
                    return Err(at.invalid("registry", "is required in mode `confluent`"));
                };
                if self.schema.is_some() {
                    return Err(at.invalid(
                        "schema",
                        "is only used in modes `raw` and `single_object`; in mode `confluent` \
                         writer schemas come from the registry (use `reader_schema` to pin the \
                         resolved shape)",
                    ));
                }
                if registry.tls.root_ca.is_some() && url.scheme() != "https" {
                    return Err(at.invalid(
                        "registry.tls.root_ca",
                        "is set, but registry.url is not https://",
                    ));
                }
                Checked::Confluent { registry, url }
            }
            AvroMode::Raw | AvroMode::SingleObject => {
                let mode = if self.mode == AvroMode::Raw {
                    "raw"
                } else {
                    "single_object"
                };
                let Some(schema) = self.schema.as_ref() else {
                    return Err(at.invalid(
                        "schema",
                        format!("is required in mode `{mode}` (inline or path)"),
                    ));
                };
                if self.registry.is_some() {
                    return Err(at.invalid("registry", format!("is not used in mode `{mode}`")));
                }
                if !self.prewarm_subjects.is_empty() {
                    return Err(at.invalid("prewarm_subjects", "requires mode `confluent`"));
                }
                if self.mode == AvroMode::SingleObject && self.reader_schema.is_some() {
                    return Err(at.invalid(
                        "reader_schema",
                        "is not supported in mode `single_object`, which decodes with the \
                         configured `schema` directly",
                    ));
                }
                Checked::Fixed { schema }
            }
        };
        for (field, source) in [
            ("schema", &self.schema),
            ("reader_schema", &self.reader_schema),
        ] {
            if let Some(source) = source {
                source.check_shape(field)?;
            }
        }
        Ok(checked)
    }
}

/// Builder produced from the config section; hands out either the
/// dynamically-typed or the serde-typed deserializer.
#[derive(Clone, Debug)]
pub struct AvroDeserializerBuilder {
    core: DecoderCore,
}

impl AvroDeserializerBuilder {
    /// Build from a config.
    ///
    /// `runtime` hosts the registry fetcher task and the startup pre-warm
    /// (Confluent mode); pass the pipeline's I/O runtime handle. In
    /// `confluent` mode this reads `registry.tls.root_ca`, and on Linux and
    /// other non-Apple Unix the system trust store. Unlike the JSON builder,
    /// this one can fail after [`AvroDeserializerConfig::validate`] passes,
    /// because it reads and parses the schemas and builds an HTTP client.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] at `deserializer.avro` for every refusal of
    /// [`AvroDeserializerConfig::validate`], a `schema` or `reader_schema`
    /// that cannot be read or parsed, and a registry client that cannot be
    /// built, which on Linux and other non-Apple Unix includes an `https://`
    /// registry when neither the system trust store nor
    /// `registry.tls.root_ca` holds a usable certificate.
    pub fn new(
        config: AvroDeserializerConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<AvroDeserializerBuilder, ConfigError> {
        Self::new_on(&config, runtime, SyncClock::system())
    }

    /// Build from the pipeline's `deserializer` component section.
    ///
    /// # Errors
    ///
    /// As [`AvroDeserializerConfig::from_component_config`] and
    /// [`new`](Self::new).
    pub fn from_component_config(
        section: &ComponentConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<AvroDeserializerBuilder, ConfigError> {
        Self::new(
            AvroDeserializerConfig::from_component_config(section)?,
            runtime,
        )
    }

    /// Test seam: time the negative cache, the fetch backoff and the
    /// unknown-id warning's rate limit on `clock`.
    ///
    /// Behind the off-by-default `testing` feature, outside the semver
    /// surface.
    ///
    /// # Errors
    ///
    /// As [`new`](Self::new).
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn new_with_clock(
        config: AvroDeserializerConfig,
        runtime: &tokio::runtime::Handle,
        clock: spate_core::clock::SharedClock,
    ) -> Result<AvroDeserializerBuilder, ConfigError> {
        Self::new_on(&config, runtime, SyncClock::new(clock))
    }

    fn new_on(
        config: &AvroDeserializerConfig,
        runtime: &tokio::runtime::Handle,
        clock: SyncClock,
    ) -> Result<AvroDeserializerBuilder, ConfigError> {
        let checked = config.checked()?;
        let at = section_path();
        let reader_side = config
            .reader_schema
            .as_ref()
            .map(|s| {
                let schema = Arc::new(s.load("reader_schema")?);
                ReaderSide::compile(schema)
                    .map(Arc::new)
                    .map_err(|e| at.invalid("reader_schema", e))
            })
            .transpose()?;
        let mode = match checked {
            Checked::Confluent { registry, url } => {
                let basic_auth = registry
                    .username
                    .as_ref()
                    .map(|u| (u.clone(), registry.password.clone()));
                if url.scheme() == "http"
                    && registry.password.as_ref().is_some_and(|p| !p.is_empty())
                {
                    at.warn_cleartext_credential("registry.password", "registry.url uses http://");
                }
                let rejection = Rejection::default();
                let http = http_client(registry.tls.root_ca.as_deref(), url.scheme())?;
                let endpoint = Arc::new(Endpoint::new(url, basic_auth));
                let handle = spawn_fetcher(
                    http.clone(),
                    Arc::clone(&endpoint),
                    Arc::clone(&rejection),
                    config.negative_cache_ttl,
                    clock,
                    runtime,
                );
                if !config.prewarm_subjects.is_empty() {
                    let subjects = config.prewarm_subjects.clone();
                    let cache = Arc::clone(&handle.cache);
                    let prewarm = async move {
                        crate::registry::prewarm(&http, &endpoint, &subjects, &cache, &rejection)
                            .await;
                    };
                    // Detached: the pre-warm ends once it has walked the subject list.
                    spate_core::task::spawn_on("avro-registry-prewarm", prewarm, runtime).detach();
                }
                SchemaSourceMode::Confluent {
                    registry: handle,
                    memo: crate::cache::SchemaCache::empty_snapshot(),
                }
            }
            Checked::Fixed { schema } => {
                let schema = Self::fixed_schema(schema)?;
                if config.mode == AvroMode::SingleObject {
                    // The single-object header fingerprint is computed from
                    // the parsed schema, which `fixed_schema` has confirmed.
                    let apache = schema.schema.as_ref().map_err(|reason| {
                        at.invalid(
                            "schema",
                            format!(
                                "mode `single_object` computes the header fingerprint with the \
                                 apache parser, which rejected the schema: {reason}"
                            ),
                        )
                    })?;
                    let fp = apache.fingerprint::<Rabin>();
                    let bytes: [u8; 8] =
                        fp.bytes.as_slice().try_into().map_err(|_| {
                            at.invalid("schema", "unexpected Rabin fingerprint width")
                        })?;
                    SchemaSourceMode::SingleObject {
                        schema,
                        fingerprint: u64::from_le_bytes(bytes),
                    }
                } else {
                    SchemaSourceMode::Raw { schema }
                }
            }
        };
        Ok(AvroDeserializerBuilder {
            core: DecoderCore::new(mode, reader_side),
        })
    }

    /// Compile the writer schema of a fixed-schema mode. A schema that cannot
    /// compile is refused here: built anyway, every record would surface as
    /// `SchemaUnavailable`, which under the default Skip policy drops and
    /// acks 100% of the input while watermarks advance.
    fn fixed_schema(source: &AvroSchemaSection) -> Result<Arc<CompiledSchema>, ConfigError> {
        let json = source.load_text("schema")?;
        // Compiled with apache-avro's parse panic caught.
        let compiled = CompiledSchema::compile(0, &json);
        if let Some(reason) = compiled.unusable_reason() {
            return Err(section_path().invalid("schema", reason));
        }
        Ok(Arc::new(compiled))
    }

    /// The dynamically-typed deserializer (emits [`crate::AvroValue`]).
    /// It refuses a datum past the limits on [`AvroValueDeserializer`].
    #[must_use]
    pub fn build_value(&self) -> AvroValueDeserializer {
        AvroValueDeserializer::new(self.core.clone())
    }

    /// The serde-typed deserializer (emits `T`). It refuses a datum past the
    /// limits on [`AvroValueDeserializer`].
    #[must_use]
    pub fn build_serde<T>(&self) -> AvroSerdeDeserializer<T>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        AvroSerdeDeserializer::new(self.core.clone())
    }

    /// The fixed schema's datum-path failure, if any. The single-pass spec
    /// can reject a schema the other paths accept (`duration`,
    /// `big-decimal`).
    fn fixed_datum_error(&self) -> Option<String> {
        match &self.core.mode {
            SchemaSourceMode::Raw { schema } | SchemaSourceMode::SingleObject { schema, .. } => {
                schema.datum.as_ref().err().cloned()
            }
            SchemaSourceMode::Confluent { .. } => None,
        }
    }

    /// The single-pass datum deserializer for any record family, owned or
    /// borrowed (see [`AvroDatumDeserializer`]'s zero-copy notes). For a
    /// plain owned `T`, [`Self::build_serde_datum`] reads better.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] for a configured `reader_schema`, because
    /// this path decodes in the writer schema's shape only (use
    /// `#[serde(default)]`/`#[serde(alias)]` on the record type for additive
    /// evolution, or [`Self::build_serde`] for Avro's full resolution rules),
    /// and for a fixed schema the datum path cannot decode
    /// (`duration`/`big-decimal` logical types). Deferring either would
    /// surface every record as `SchemaUnavailable` and drop the whole stream
    /// under the default Skip policy.
    pub fn build_datum<F>(&self) -> Result<AvroDatumDeserializer<F>, ConfigError>
    where
        F: spate_core::deser::RecFamily,
        for<'buf> F::Rec<'buf>: serde::Deserialize<'buf>,
    {
        if self.core.reader_schema.is_some() {
            return Err(section_path().invalid(
                "reader_schema",
                "is not applied by the datum deserializer (records decode in the writer \
                 schema's shape; use `#[serde(default)]`/`#[serde(alias)]` on the record type, \
                 or `build_serde` for Avro schema resolution)",
            ));
        }
        if let Some(reason) = self.fixed_datum_error() {
            return Err(section_path().invalid("schema", reason));
        }
        Ok(AvroDatumDeserializer::new(self.core.clone()))
    }

    /// The single-pass datum deserializer emitting an owned `T`, the
    /// convenience form of [`Self::build_datum`] for the common case.
    ///
    /// # Errors
    ///
    /// As [`Self::build_datum`].
    pub fn build_serde_datum<T>(
        &self,
    ) -> Result<AvroDatumDeserializer<spate_core::deser::Owned<T>>, ConfigError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.build_datum::<spate_core::deser::Owned<T>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::config::ComponentConfig;

    const SCHEMA: &str = r#"{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}"#;

    fn component(yaml: &str) -> ComponentConfig {
        spate_test::component_config("avro", yaml)
    }

    /// The section as serde reads it, before `validate`.
    fn parsed(yaml: &str) -> AvroDeserializerConfig {
        component(yaml).deserialize_into().unwrap()
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn build(
        config: &AvroDeserializerConfig,
        rt: &tokio::runtime::Runtime,
    ) -> Result<AvroDeserializerBuilder, ConfigError> {
        AvroDeserializerBuilder::new(config.clone(), rt.handle())
    }

    fn build_yaml(
        yaml: &str,
        rt: &tokio::runtime::Runtime,
    ) -> Result<AvroDeserializerBuilder, ConfigError> {
        AvroDeserializerBuilder::from_component_config(&component(yaml), rt.handle())
    }

    fn refusal(result: Result<AvroDeserializerBuilder, ConfigError>) -> String {
        result.map(|_| ()).unwrap_err().to_string()
    }

    fn assert_credential_error(error: &ConfigError) {
        for printed in [error.to_string(), format!("{error:?}")] {
            for field in ["registry.url", "registry.username", "registry.password"] {
                assert!(printed.contains(field), "{printed}");
            }
            for secret in ["urluser", "urlsecret", "configuredsecret", "https://"] {
                assert!(!printed.contains(secret), "{printed}");
            }
        }
    }

    /// Parsed URL credentials fail `validate` and the build with a redacted
    /// error. Regression for #876.
    #[test]
    fn registry_url_credentials_are_rejected_from_a_config() {
        let rt = runtime();
        for url in [
            "https://urluser:urlsecret@sr",
            "https://urluser@sr",
            "https://:urlsecret@sr",
            "https://url%75ser:url%73ecret@sr",
            "https:///urluser:urlsecret@sr",
            "https://urluser:@sr",
        ] {
            if url == "https://urluser:@sr" {
                let parsed = reqwest::Url::parse(url).unwrap();
                assert_eq!(parsed.username(), "urluser");
                assert_eq!(parsed.password(), None);
            }
            if url == "https://:urlsecret@sr" {
                let parsed = reqwest::Url::parse(url).unwrap();
                assert_eq!(parsed.username(), "");
                assert_eq!(parsed.password(), Some("urlsecret"));
            }
            for configured in [false, true] {
                let mut registry = AvroRegistrySection::new(url);
                if configured {
                    registry.username = Some("svc".into());
                    registry.password = Some("configuredsecret".into());
                }
                let config = AvroDeserializerConfig::confluent(registry);
                assert_credential_error(&config.validate().unwrap_err());
                assert_credential_error(&build(&config, &rt).map(|_| ()).unwrap_err());
            }
        }
    }

    /// Opaque component loading rejects both URL-only and dual-source credentials.
    /// Regression for #876.
    #[test]
    fn registry_url_credentials_are_rejected_from_component() {
        let rt = runtime();
        for fields in ["", ", username: svc, password: configuredsecret"] {
            let yaml = format!("registry: {{url: 'https://urluser:urlsecret@sr'{fields}}}");
            assert_credential_error(&build_yaml(&yaml, &rt).map(|_| ()).unwrap_err());
        }
    }

    /// Credential rejection precedes reader-schema loading with prewarm enabled.
    /// Regression for #876.
    #[test]
    fn registry_url_credentials_reject_before_reader_schema() {
        let rt = runtime();
        let yaml = "registry: {url: 'https://urluser:urlsecret@sr'}\n\
                    reader_schema: {inline: invalid-schema}\nprewarm_subjects: [orders-value]";
        assert_credential_error(&build_yaml(yaml, &rt).map(|_| ()).unwrap_err());
    }

    /// Credential rejection precedes TLS/client construction with prewarm enabled.
    /// Regression for #876.
    #[test]
    fn registry_url_credentials_reject_before_tls_client() {
        let rt = runtime();
        let yaml = "registry: {url: 'https://urluser:urlsecret@sr', \
                    tls: {root_ca: /nonexistent-spate-876/ca.pem}}\n\
                    prewarm_subjects: [orders-value]";
        assert_credential_error(&build_yaml(yaml, &rt).map(|_| ()).unwrap_err());
    }

    /// A password without a username, an empty one included, fails ahead of
    /// reader-schema loading and TLS client construction, with an error that
    /// names both fields and never the password. Regression for #1034.
    #[test]
    fn a_registry_password_without_a_username_is_rejected() {
        let rt = runtime();
        let config = |password: &str| {
            let mut registry = AvroRegistrySection::new("http://127.0.0.1:1");
            registry.password = Some(password.into());
            AvroDeserializerConfig::confluent(registry)
        };
        let yaml = "registry: {url: 'https://sr', password: s3cret, \
                    tls: {root_ca: /nonexistent-spate-1034/ca.pem}}\n\
                    reader_schema: {inline: invalid-schema}\nprewarm_subjects: [orders-value]";
        for error in [
            build(&config("s3cret"), &rt).map(|_| ()).unwrap_err(),
            build(&config(""), &rt).map(|_| ()).unwrap_err(),
            build_yaml(yaml, &rt).map(|_| ()).unwrap_err(),
        ] {
            assert_eq!(
                error.to_string(),
                "deserializer.avro.registry.password: is set, but registry.username is not"
            );
            assert!(!format!("{error:?}").contains("s3cret"), "{error:?}");
        }
    }

    /// A fixed-schema mode rejects a password-only `registry` section, or one
    /// whose URL carries userinfo, as a section the mode does not use.
    #[test]
    fn a_fixed_schema_mode_rejects_a_password_only_registry_as_unused() {
        let rt = runtime();
        for (mode, registry) in [
            ("raw", "{url: http://sr, password: s3cret}"),
            ("single_object", "{url: http://sr, password: s3cret}"),
            ("raw", "{url: 'http://u:s3cret@sr'}"),
            ("single_object", "{url: 'http://u:s3cret@sr'}"),
        ] {
            let yaml =
                format!("mode: {mode}\nschema: {{inline: '{SCHEMA}'}}\nregistry: {registry}");
            assert_eq!(
                refusal(build_yaml(&yaml, &rt)),
                format!("deserializer.avro.registry: is not used in mode `{mode}`"),
            );
        }
    }

    /// Credential-free parsed URLs accept configured auth and `@` in path/query data.
    #[test]
    fn credential_free_registry_urls_are_accepted() {
        let rt = runtime();
        for url in [
            "https://sr",
            "https://sr/path@data?value=@data",
            "https://sr/path%40data?value=%40data",
            "https://:@sr",
        ] {
            if url == "https://:@sr" {
                let parsed = reqwest::Url::parse(url).unwrap();
                assert_eq!(parsed.username(), "");
                assert_eq!(parsed.password(), None);
            }
            for configured in [false, true] {
                let mut registry = AvroRegistrySection::new(url);
                if configured {
                    registry.username = Some("svc".into());
                    registry.password = Some("configuredsecret".into());
                }
                build(&AvroDeserializerConfig::confluent(registry), &rt).unwrap();
            }
        }
    }

    const CLEARTEXT: &str = "the credential is sent without TLS";

    /// The cleartext-credential lines `build` logs, and every line it logs.
    fn cleartext_lines(build: impl FnOnce()) -> (Vec<String>, Vec<String>) {
        let lines = spate_test::capture_logs(tracing::Level::INFO, build);
        let cleartext = lines
            .iter()
            .filter(|line| line.contains(CLEARTEXT))
            .cloned()
            .collect();
        (cleartext, lines)
    }

    fn registry_config(url: &str, password: Option<&str>) -> AvroDeserializerConfig {
        let mut registry = AvroRegistrySection::new(url);
        registry.username = Some("svc".into());
        registry.password = password.map(Into::into);
        AvroDeserializerConfig::confluent(registry)
    }

    /// A registry password sent to an `http://` registry logs one warning that
    /// names the key and never the password. An `https://` registry, an empty
    /// password and no password log none.
    #[test]
    fn a_registry_password_over_http_logs_one_warning() {
        let rt = runtime();
        for (case, url, secret, warns) in [
            ("http", "http://127.0.0.1:1", Some("hunter2"), true),
            (
                "upper-case scheme",
                "HTTP://127.0.0.1:1",
                Some("hunter2"),
                true,
            ),
            ("https", "https://127.0.0.1:1", Some("hunter2"), false),
            ("empty", "http://127.0.0.1:1", Some(""), false),
            ("absent", "http://127.0.0.1:1", None, false),
        ] {
            let config = registry_config(url, secret);
            let (cleartext, lines) = cleartext_lines(|| {
                build(&config, &rt).unwrap();
            });
            let expected: &[&str] = if warns {
                &[
                    "deserializer.avro.registry.password: registry.url uses http://; \
                   the credential is sent without TLS",
                ]
            } else {
                &[]
            };
            assert_eq!(cleartext.len(), expected.len(), "{case}: {lines:?}");
            for (line, want) in cleartext.iter().zip(expected) {
                assert!(line.contains("WARN") && line.contains(want), "{line}");
            }
            assert!(!lines.iter().any(|l| l.contains("hunter2")), "{lines:?}");
        }
    }

    /// A config the builder refuses logs no warning, even when its password
    /// would cross `http://`: the warning follows the section's own checks.
    #[test]
    fn a_refused_registry_logs_no_cleartext_warning() {
        let rt = runtime();
        let mut config = registry_config("http://127.0.0.1:1", Some("hunter2"));
        if let Some(registry) = config.registry.as_mut() {
            registry.tls.root_ca = Some(std::path::PathBuf::from("/nonexistent-spate-1647/ca.pem"));
        }
        let (cleartext, lines) = cleartext_lines(|| {
            let err = refusal(build(&config, &rt));
            assert!(err.contains("registry.tls.root_ca"), "{err}");
        });
        assert!(cleartext.is_empty(), "{lines:?}");
    }

    /// `Debug` on the config hides the registry password and URL userinfo
    /// and keeps the host and username. Regression for #754.
    #[test]
    fn debug_never_prints_registry_credentials() {
        let config = parsed(
            "registry: {url: 'https://svc:hunter2@sr:8081', username: svc, password: hunter2, \
             tls: {root_ca: /etc/ca.pem}}",
        );
        let printed = format!("{config:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        for visible in ["sr:8081", "\"svc\"", "Some(<redacted>)", "/etc/ca.pem"] {
            assert!(printed.contains(visible), "{visible}: {printed}");
        }
    }

    #[test]
    fn full_confluent_section_parses() {
        let section = component(
            r#"
            mode: confluent
            registry:
              url: http://sr:8081
              username: svc
              password: secret
            prewarm_subjects: [orders-value, users-value]
            negative_cache_ttl: 45s
            reader_schema:
              inline: '{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}'
            "#,
        );
        let config = AvroDeserializerConfig::from_component_config(&section).unwrap();
        assert_eq!(config.mode, AvroMode::Confluent);
        assert_eq!(config.prewarm_subjects.len(), 2);
        assert_eq!(config.negative_cache_ttl, Duration::from_secs(45));
        let rt = runtime();
        build(&config, &rt).unwrap();
    }

    /// `registry.tls.root_ca` parses, rejects unknown keys beside it, and
    /// requires an `https://` registry URL.
    #[test]
    fn registry_tls_root_ca_parses_and_requires_https() {
        let config = parsed("registry: {url: https://sr, tls: {root_ca: /etc/ca.pem}}");
        let registry = config.registry.unwrap();
        assert_eq!(
            registry.tls.root_ca,
            Some(std::path::PathBuf::from("/etc/ca.pem"))
        );

        let err = component("registry: {url: https://sr, tls: {bogus: 1}}")
            .deserialize_into::<AvroDeserializerConfig>()
            .unwrap_err();
        assert!(err.to_string().contains("bogus"), "{err}");

        let config = parsed("registry: {url: http://sr, tls: {root_ca: /etc/ca.pem}}");
        let rt = runtime();
        assert_eq!(
            refusal(build(&config, &rt)),
            "deserializer.avro.registry.tls.root_ca: is set, but registry.url is not https://"
        );
    }

    #[test]
    fn build_datum_rejects_a_reader_schema_at_build_time() {
        let rt = runtime();
        let yaml = format!(
            "mode: raw\nschema: {{inline: '{SCHEMA}'}}\nreader_schema: {{inline: '{SCHEMA}'}}"
        );
        let builder = build_yaml(&yaml, &rt).unwrap();
        // The Value/serde paths accept a reader schema; the datum path
        // gates it at build time, not per record.
        let _ = builder.build_value();
        let err = builder
            .build_serde_datum::<TestRec>()
            .map(|_| ())
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "deserializer.avro.reader_schema: is not applied by the datum deserializer \
             (records decode in the writer schema's shape; use \
             `#[serde(default)]`/`#[serde(alias)]` on the record type, or `build_serde` for \
             Avro schema resolution)"
        );
    }

    #[test]
    fn build_datum_rejects_a_duration_schema_the_other_paths_accept() {
        let rt = runtime();
        let duration = r#"{"type":"record","name":"R","fields":[{"name":"d",
            "type":{"type":"fixed","name":"F","size":12,"logicalType":"duration"}}]}"#
            .replace('\n', " ");
        let builder =
            build_yaml(&format!("mode: raw\nschema: {{inline: '{duration}'}}"), &rt).unwrap();
        let _ = builder.build_value();
        let err = builder
            .build_serde_datum::<TestRec>()
            .map(|_| ())
            .unwrap_err();
        let err = err.to_string();
        assert!(
            err.starts_with("deserializer.avro.schema: schema 0 is not usable on the datum path: "),
            "{err}"
        );
        assert!(err.contains("does not support"), "{err}");
    }

    #[test]
    fn build_serde_datum_happy_path() {
        let rt = runtime();
        let builder = build(
            &AvroDeserializerConfig::raw(AvroSchemaSection::inline(SCHEMA)),
            &rt,
        )
        .unwrap();
        builder.build_serde_datum::<TestRec>().unwrap();
        builder
            .build_datum::<spate_core::deser::Owned<TestRec>>()
            .unwrap();
    }

    #[derive(Debug, serde::Deserialize)]
    #[expect(dead_code, reason = "builder-gate shape only")]
    struct TestRec {
        id: i64,
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let section = component("mode: raw\nschema: {inline: '\"long\"'}\nturbo: true\n");
        let err = AvroDeserializerConfig::from_component_config(&section).unwrap_err();
        assert_eq!(
            err.to_string(),
            "avro.turbo: unknown field `turbo`, expected one of `mode`, `registry`, \
             `prewarm_subjects`, `negative_cache_ttl`, `reader_schema`, `schema`"
        );
    }

    /// Each rule `validate` holds, with its whole refusal, on a config built
    /// in code and checked with no runtime.
    #[test]
    fn validate_refuses_each_rule_without_a_runtime() {
        let schema = || AvroSchemaSection::inline(SCHEMA);
        let both = AvroSchemaSection {
            inline: Some(SCHEMA.into()),
            path: Some("/etc/e.avsc".into()),
        };
        let mut password_only = AvroRegistrySection::new("http://sr");
        password_only.password = Some("s3cret".into());
        let mut http_root_ca = AvroRegistrySection::new("http://sr");
        http_root_ca.tls.root_ca = Some("/etc/ca.pem".into());
        let fixed = |mode: AvroMode| AvroDeserializerConfig {
            mode,
            ..AvroDeserializerConfig::raw(schema())
        };
        let cases: Vec<(&str, AvroDeserializerConfig, &str)> = vec![
            (
                "url userinfo",
                AvroDeserializerConfig::confluent(AvroRegistrySection::new("http://u:p@sr")),
                "deserializer.avro.registry.url: carries userinfo; set them in \
                 registry.username and registry.password",
            ),
            (
                "password without username",
                AvroDeserializerConfig::confluent(password_only),
                "deserializer.avro.registry.password: is set, but registry.username is not",
            ),
            (
                "ttl floor",
                AvroDeserializerConfig {
                    negative_cache_ttl: Duration::from_millis(99),
                    ..AvroDeserializerConfig::raw(schema())
                },
                "deserializer.avro.negative_cache_ttl: must be at least 100ms; found 99ms: a \
                 shorter entry can expire before the blocked record is replayed, and each \
                 replay then fetches the schema id again",
            ),
            (
                "confluent without registry",
                AvroDeserializerConfig::default(),
                "deserializer.avro.registry: is required in mode `confluent`",
            ),
            (
                "confluent with schema",
                AvroDeserializerConfig {
                    schema: Some(schema()),
                    ..AvroDeserializerConfig::confluent(AvroRegistrySection::new("http://sr"))
                },
                "deserializer.avro.schema: is only used in modes `raw` and `single_object`; in \
                 mode `confluent` writer schemas come from the registry (use `reader_schema` to \
                 pin the resolved shape)",
            ),
            (
                "root_ca over http",
                AvroDeserializerConfig::confluent(http_root_ca),
                "deserializer.avro.registry.tls.root_ca: is set, but registry.url is not https://",
            ),
            (
                "raw without schema",
                AvroDeserializerConfig {
                    schema: None,
                    ..fixed(AvroMode::Raw)
                },
                "deserializer.avro.schema: is required in mode `raw` (inline or path)",
            ),
            (
                "single_object without schema",
                AvroDeserializerConfig {
                    schema: None,
                    ..fixed(AvroMode::SingleObject)
                },
                "deserializer.avro.schema: is required in mode `single_object` (inline or path)",
            ),
            (
                "raw with registry",
                AvroDeserializerConfig {
                    registry: Some(AvroRegistrySection::new("http://sr")),
                    ..fixed(AvroMode::Raw)
                },
                "deserializer.avro.registry: is not used in mode `raw`",
            ),
            (
                "raw with prewarm",
                AvroDeserializerConfig {
                    prewarm_subjects: vec!["x".into()],
                    ..fixed(AvroMode::Raw)
                },
                "deserializer.avro.prewarm_subjects: requires mode `confluent`",
            ),
            (
                "single_object with prewarm",
                AvroDeserializerConfig {
                    prewarm_subjects: vec!["x".into()],
                    ..fixed(AvroMode::SingleObject)
                },
                "deserializer.avro.prewarm_subjects: requires mode `confluent`",
            ),
            (
                "single_object with reader_schema",
                AvroDeserializerConfig {
                    reader_schema: Some(schema()),
                    ..fixed(AvroMode::SingleObject)
                },
                "deserializer.avro.reader_schema: is not supported in mode `single_object`, \
                 which decodes with the configured `schema` directly",
            ),
            (
                "schema with both",
                AvroDeserializerConfig::raw(both.clone()),
                "deserializer.avro.schema: needs exactly one of `inline` or `path`",
            ),
            (
                "schema with neither",
                AvroDeserializerConfig::raw(AvroSchemaSection::default()),
                "deserializer.avro.schema: needs exactly one of `inline` or `path`",
            ),
            (
                "reader_schema with both",
                AvroDeserializerConfig {
                    reader_schema: Some(both),
                    ..fixed(AvroMode::Raw)
                },
                "deserializer.avro.reader_schema: needs exactly one of `inline` or `path`",
            ),
            (
                "reader_schema with neither",
                AvroDeserializerConfig {
                    reader_schema: Some(AvroSchemaSection::default()),
                    ..fixed(AvroMode::Raw)
                },
                "deserializer.avro.reader_schema: needs exactly one of `inline` or `path`",
            ),
        ];
        for (case, config, expected) in cases {
            let err = config.validate().expect_err(case);
            assert_eq!(err.to_string(), expected, "{case}");
        }
    }

    /// A `schema` or `reader_schema` loaded from YAML with both `inline` and
    /// `path`, or neither, is refused by name.
    #[test]
    fn a_yaml_schema_source_needs_exactly_one_of_inline_or_path() {
        for (yaml, expected) in [
            (
                "mode: raw\nschema: {}",
                "deserializer.avro.schema: needs exactly one of `inline` or `path`",
            ),
            (
                "mode: raw\nschema: {inline: x, path: /etc/e.avsc}",
                "deserializer.avro.schema: needs exactly one of `inline` or `path`",
            ),
            (
                "mode: raw\nschema: {inline: x}\nreader_schema: {}",
                "deserializer.avro.reader_schema: needs exactly one of `inline` or `path`",
            ),
            (
                "mode: raw\nschema: {inline: x}\nreader_schema: {inline: x, path: /etc/e.avsc}",
                "deserializer.avro.reader_schema: needs exactly one of `inline` or `path`",
            ),
        ] {
            let err = AvroDeserializerConfig::from_component_config(&component(yaml)).unwrap_err();
            assert_eq!(err.to_string(), expected, "{yaml}");
        }
    }

    /// The config's own constructor validates: the builder validating again
    /// would hide a constructor that skipped it.
    #[test]
    fn config_from_component_config_validates() {
        let err =
            AvroDeserializerConfig::from_component_config(&component("mode: raw")).unwrap_err();
        assert_eq!(
            err.to_string(),
            "deserializer.avro.schema: is required in mode `raw` (inline or path)"
        );
    }

    /// A config built in code is refused by `new` exactly as `validate`
    /// refuses it.
    #[test]
    fn new_applies_validate() {
        let rt = runtime();
        let config = AvroDeserializerConfig::raw(AvroSchemaSection::default());
        assert_eq!(
            refusal(build(&config, &rt)),
            config.validate().unwrap_err().to_string()
        );
    }

    /// Each mode's constructor sets its mode and required field, equals the
    /// same section loaded from YAML, and validates and builds.
    #[test]
    fn mode_constructors_set_their_required_field() {
        let rt = runtime();
        let raw = AvroDeserializerConfig::raw(AvroSchemaSection::inline(SCHEMA));
        assert_eq!(raw.mode, AvroMode::Raw);
        assert_eq!(
            raw.schema.as_ref().and_then(|s| s.inline.as_deref()),
            Some(SCHEMA)
        );
        let single = AvroDeserializerConfig::single_object(AvroSchemaSection::inline(SCHEMA));
        assert_eq!(single.mode, AvroMode::SingleObject);
        assert_eq!(
            single.schema.as_ref().and_then(|s| s.inline.as_deref()),
            Some(SCHEMA)
        );
        let confluent =
            AvroDeserializerConfig::confluent(AvroRegistrySection::new("http://127.0.0.1:1"));
        assert_eq!(confluent.mode, AvroMode::Confluent);
        assert_eq!(
            confluent.registry.as_ref().map(|r| r.url.as_str()),
            Some("http://127.0.0.1:1")
        );
        assert_eq!(
            parsed(&format!("mode: raw\nschema: {{inline: '{SCHEMA}'}}")),
            raw
        );
        assert_eq!(
            parsed(&format!(
                "mode: single_object\nschema: {{inline: '{SCHEMA}'}}"
            )),
            single
        );
        assert_eq!(parsed("registry: {url: 'http://127.0.0.1:1'}"), confluent);
        for config in [raw, single, confluent] {
            config.validate().unwrap();
            build(&config, &rt).unwrap();
        }
    }

    /// The mode checks run before the reader schema is read.
    #[test]
    fn mode_checks_precede_the_reader_schema_load() {
        let rt = runtime();
        let config = AvroDeserializerConfig {
            mode: AvroMode::Raw,
            reader_schema: Some(AvroSchemaSection::path(
                "/nonexistent-spate-1600/reader.avsc",
            )),
            ..AvroDeserializerConfig::default()
        };
        assert_eq!(
            refusal(build(&config, &rt)),
            "deserializer.avro.schema: is required in mode `raw` (inline or path)"
        );
    }

    /// `prewarm_subjects` and the single-object `reader_schema` refusals run
    /// before the writer schema is read.
    #[test]
    fn fixed_mode_rules_precede_the_schema_load() {
        let rt = runtime();
        let missing = || AvroSchemaSection::path("/nonexistent-spate-1600/writer.avsc");
        let prewarm = AvroDeserializerConfig {
            prewarm_subjects: vec!["orders-value".into()],
            ..AvroDeserializerConfig::raw(missing())
        };
        assert_eq!(
            refusal(build(&prewarm, &rt)),
            "deserializer.avro.prewarm_subjects: requires mode `confluent`"
        );
        let reader = AvroDeserializerConfig {
            reader_schema: Some(AvroSchemaSection::inline(SCHEMA)),
            ..AvroDeserializerConfig::single_object(missing())
        };
        assert_eq!(
            refusal(build(&reader, &rt)),
            "deserializer.avro.reader_schema: is not supported in mode `single_object`, which \
             decodes with the configured `schema` directly"
        );
    }

    fn ttl_refusal(found: &str) -> String {
        format!(
            "deserializer.avro.negative_cache_ttl: must be at least 100ms; found {found}: \
             a shorter entry can expire before the blocked record is replayed, and each \
             replay then fetches the schema id again"
        )
    }

    /// Pins the negative cache TTL's floor at 100ms, inclusive and on the
    /// whole duration, in every mode, and the refusal's whole text.
    /// Regression for #1555.
    #[test]
    fn negative_cache_ttl_loads_at_its_floor_and_is_refused_a_nanosecond_under() {
        let rt = runtime();
        let modes = [
            "registry: {url: http://sr}".to_owned(),
            format!("mode: raw\nschema: {{inline: '{SCHEMA}'}}"),
            format!("mode: single_object\nschema: {{inline: '{SCHEMA}'}}"),
        ];
        for mode in &modes {
            let build = |ttl: &str| build_yaml(&format!("{mode}\nnegative_cache_ttl: {ttl}"), &rt);
            build("100ms").unwrap_or_else(|err| panic!("{mode}: the floor builds: {err}"));
            for (ttl, found) in [("0s", "0s"), ("99ms 999us 999ns", "99ms 999us 999ns")] {
                assert_eq!(refusal(build(ttl)), ttl_refusal(found), "{mode}");
            }
        }
        let raw = |ttl: Duration| AvroDeserializerConfig {
            negative_cache_ttl: ttl,
            ..AvroDeserializerConfig::raw(AvroSchemaSection::inline(SCHEMA))
        };
        assert_eq!(
            refusal(build(
                &raw(Duration::from_millis(100) - Duration::from_nanos(1)),
                &rt
            )),
            ttl_refusal("99ms 999us 999ns")
        );
        build(&raw(Duration::MAX), &rt).expect("no ceiling");
    }

    /// Pins that the TTL floor is checked ahead of the mode arms' own
    /// refusals, where the registry tasks are spawned.
    #[test]
    fn negative_cache_ttl_is_refused_before_the_mode_checks() {
        let rt = runtime();
        let yaml = format!(
            "registry: {{url: http://sr}}\nschema: {{inline: '{SCHEMA}'}}\nnegative_cache_ttl: 0s"
        );
        assert_eq!(refusal(build_yaml(&yaml, &rt)), ttl_refusal("0s"));
    }

    /// Pins that ten of the engine's default blocked-batch replays fit inside
    /// the TTL floor, so a held record reads its 404 verdict before the
    /// verdict expires and reaches its policy.
    #[test]
    fn the_default_blocked_batch_replay_fits_inside_the_ttl_floor() {
        let replay = spate_core::pipeline::RuntimeOptions::default().blocked_retry;
        assert!(
            replay.saturating_mul(10) <= MIN_NEGATIVE_CACHE_TTL,
            "a {replay:?} replay against a {MIN_NEGATIVE_CACHE_TTL:?} floor"
        );
    }

    /// Pins that the two credential refusals come before the TTL floor, so a
    /// section wrong in both ways names its credentials first.
    #[test]
    fn credential_refusals_precede_the_ttl_floor() {
        let rt = runtime();
        let with_registry = |registry: AvroRegistrySection| AvroDeserializerConfig {
            negative_cache_ttl: Duration::ZERO,
            ..AvroDeserializerConfig::confluent(registry)
        };
        let mut password_only = AvroRegistrySection::new("http://sr");
        password_only.password = Some("s3cret".into());
        for (config, expected) in [
            (
                with_registry(AvroRegistrySection::new("http://urluser:urlsecret@sr")),
                "deserializer.avro.registry.url: carries userinfo; set them in \
                 registry.username and registry.password",
            ),
            (
                with_registry(password_only),
                "deserializer.avro.registry.password: is set, but registry.username is not",
            ),
        ] {
            assert_eq!(refusal(build(&config, &rt)), expected);
        }
    }

    /// A fixed schema that does not compile fails the build in both fixed
    /// modes, since built anyway every record would be dropped under the
    /// default Skip policy. A schema read from a file builds.
    #[test]
    fn an_unusable_fixed_schema_fails_the_build() {
        let rt = runtime();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("e.avsc");
        std::fs::write(&path, SCHEMA).unwrap();
        build(
            &AvroDeserializerConfig::raw(AvroSchemaSection::path(&path)),
            &rt,
        )
        .unwrap();

        for config in [
            AvroDeserializerConfig::raw(AvroSchemaSection::inline("not json")),
            AvroDeserializerConfig::single_object(AvroSchemaSection::inline("not json")),
        ] {
            assert_eq!(
                refusal(build(&config, &rt)),
                "deserializer.avro.schema: schema 0 is not usable: Failed to parse schema from JSON"
            );
        }
    }
}
