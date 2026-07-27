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
//!     prewarm_subjects: [orders-value]
//!     negative_cache_ttl: 30s
//!     reader_schema:                 # optional: pin the resolved shape
//!       path: /etc/spate/orders.avsc   # (or `inline: '{"type": ...}'`)
//!     # schema: { inline | path }    # required for raw / single_object
//! ```

use crate::cache::CompiledSchema;
use crate::deser::{AvroSerdeDeserializer, AvroValueDeserializer, DecoderCore, SchemaSourceMode};
use crate::registry::{RegistryConfig, spawn_fetcher};
use apache_avro::Schema;
use apache_avro::rabin::Rabin;
use serde::Deserialize;
use spate_core::config::ComponentConfig;
use std::sync::Arc;
use std::time::Duration;

/// Payload framing mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AvroMode {
    /// Confluent wire format with a schema registry.
    #[default]
    Confluent,
    /// Bare datums with a fixed schema.
    Raw,
    /// Avro single-object encoding with a fixed schema.
    SingleObject,
}

/// A schema provided inline or from a file — set exactly one field.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SchemaSource {
    /// The schema JSON itself.
    pub inline: Option<String>,
    /// Path to a `.avsc` file (Kubernetes: a mounted ConfigMap).
    pub path: Option<std::path::PathBuf>,
}

impl SchemaSource {
    /// An inline schema.
    #[must_use]
    pub fn inline(schema: impl Into<String>) -> Self {
        SchemaSource {
            inline: Some(schema.into()),
            path: None,
        }
    }

    /// A schema loaded from a file.
    #[must_use]
    pub fn path(path: impl Into<std::path::PathBuf>) -> Self {
        SchemaSource {
            inline: None,
            path: Some(path.into()),
        }
    }

    /// Load the schema's original JSON source (backend compiles need the
    /// source text, not a re-rendered canonical form).
    fn load_text(&self) -> Result<String, AvroConfigError> {
        match (&self.inline, &self.path) {
            (Some(s), None) => Ok(s.clone()),
            (None, Some(p)) => {
                std::fs::read_to_string(p).map_err(|e| AvroConfigError::SchemaLoad {
                    detail: format!("{}: {e}", p.display()),
                })
            }
            _ => Err(AvroConfigError::Invalid {
                detail: "a schema source needs exactly one of `inline` or `path`".into(),
            }),
        }
    }

    /// Load and parse the schema with `apache-avro` (the reader-schema
    /// path, which only exists on the apache backend).
    fn load(&self) -> Result<Schema, AvroConfigError> {
        let text = self.load_text()?;
        Schema::parse_str(&text).map_err(|e| AvroConfigError::SchemaLoad {
            detail: format!("schema failed to parse: {e}"),
        })
    }
}

/// Registry connection section.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrySection {
    /// Base URL of the Confluent-compatible schema registry.
    pub url: String,
    /// Basic-auth username (optional).
    #[serde(default)]
    pub username: Option<String>,
    /// Basic-auth password (optional; use `${VAR}` interpolation).
    #[serde(default)]
    pub password: Option<String>,
}

/// The `avro` component configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AvroSettings {
    /// Payload framing mode.
    pub mode: AvroMode,
    /// Registry connection (required in `confluent` mode).
    pub registry: Option<RegistrySection>,
    /// Subjects whose latest schemas are fetched at startup.
    pub prewarm_subjects: Vec<String>,
    /// How long a failed schema id stays negatively cached before a
    /// refetch is allowed.
    #[serde(with = "humantime_serde")]
    pub negative_cache_ttl: Duration,
    /// Optional reader schema pinning the resolved record shape
    /// (`confluent` and `raw` modes).
    pub reader_schema: Option<SchemaSource>,
    /// The writer schema (required in `raw` and `single_object` modes).
    pub schema: Option<SchemaSource>,
}

impl Default for AvroSettings {
    fn default() -> Self {
        AvroSettings {
            mode: AvroMode::Confluent,
            registry: None,
            prewarm_subjects: Vec::new(),
            negative_cache_ttl: Duration::from_secs(30),
            reader_schema: None,
            schema: None,
        }
    }
}

/// Construction errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AvroConfigError {
    /// The component section did not deserialize.
    #[error(transparent)]
    Config(#[from] spate_core::config::ConfigError),
    /// A configured schema could not be loaded or parsed.
    #[error("avro schema: {detail}")]
    SchemaLoad {
        /// What went wrong.
        detail: String,
    },
    /// The settings are inconsistent for the chosen mode.
    #[error("avro configuration: {detail}")]
    Invalid {
        /// What went wrong.
        detail: String,
    },
}

/// Builder produced from the opaque config section; hands out either the
/// dynamically-typed or the serde-typed deserializer.
#[derive(Clone, Debug)]
pub struct AvroDeserializerBuilder {
    core: DecoderCore,
}

impl AvroDeserializerBuilder {
    /// Build from the pipeline's `deserializer` component section.
    ///
    /// `runtime` hosts the registry fetcher task and the startup pre-warm
    /// (Confluent mode); pass the pipeline's I/O runtime handle.
    pub fn from_component(
        cfg: &ComponentConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, AvroConfigError> {
        let settings: AvroSettings = cfg.deserialize_into()?;
        Self::from_settings(&settings, runtime)
    }

    /// Build from already-parsed settings.
    pub fn from_settings(
        settings: &AvroSettings,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, AvroConfigError> {
        let reader_schema = settings
            .reader_schema
            .as_ref()
            .map(|s| s.load().map(Arc::new))
            .transpose()?;
        let mode = match settings.mode {
            AvroMode::Confluent => {
                let registry = settings.registry.as_ref().ok_or(AvroConfigError::Invalid {
                    detail: "mode `confluent` requires a `registry` section".into(),
                })?;
                if settings.schema.is_some() {
                    return Err(AvroConfigError::Invalid {
                        detail: "`schema` is only used in `raw`/`single_object` modes; \
                                 in `confluent` mode writer schemas come from the registry \
                                 (use `reader_schema` to pin the resolved shape)"
                            .into(),
                    });
                }
                let registry_cfg = RegistryConfig {
                    url: registry.url.clone(),
                    basic_auth: registry
                        .username
                        .as_ref()
                        .map(|u| (u.clone(), registry.password.clone())),
                    negative_cache_ttl: settings.negative_cache_ttl,
                };
                let handle = spawn_fetcher(registry_cfg.clone(), runtime);
                if !settings.prewarm_subjects.is_empty() {
                    let subjects = settings.prewarm_subjects.clone();
                    let cache = Arc::clone(&handle.cache);
                    runtime.spawn(async move {
                        crate::registry::prewarm(&registry_cfg, &subjects, &cache).await;
                    });
                }
                SchemaSourceMode::Confluent {
                    registry: handle,
                    memo: crate::cache::SchemaCache::empty_snapshot(),
                }
            }
            AvroMode::Raw => {
                let schema = Self::fixed_schema(settings, "raw")?;
                if !settings.prewarm_subjects.is_empty() {
                    return Err(AvroConfigError::Invalid {
                        detail: "`prewarm_subjects` requires mode `confluent`".into(),
                    });
                }
                SchemaSourceMode::Raw { schema }
            }
            AvroMode::SingleObject => {
                let schema = Self::fixed_schema(settings, "single_object")?;
                if !settings.prewarm_subjects.is_empty() {
                    return Err(AvroConfigError::Invalid {
                        detail: "`prewarm_subjects` requires mode `confluent`".into(),
                    });
                }
                if settings.reader_schema.is_some() {
                    return Err(AvroConfigError::Invalid {
                        detail: "single_object mode decodes with the configured schema \
                                 directly; `reader_schema` is not supported"
                            .into(),
                    });
                }
                // The single-object header fingerprint is computed from the
                // parsed schema, so this mode needs the parse to succeed at
                // settings-load time (unlike `raw`).
                let apache =
                    schema
                        .schema
                        .as_ref()
                        .map_err(|reason| AvroConfigError::SchemaLoad {
                            detail: format!(
                                "mode `single_object` computes the header fingerprint \
                                 with the apache parser, which rejected the schema: {reason}"
                            ),
                        })?;
                let fp = apache.fingerprint::<Rabin>();
                let bytes: [u8; 8] =
                    fp.bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| AvroConfigError::Invalid {
                            detail: "unexpected Rabin fingerprint width".into(),
                        })?;
                SchemaSourceMode::SingleObject {
                    schema,
                    fingerprint: u64::from_le_bytes(bytes),
                }
            }
        };
        Ok(AvroDeserializerBuilder {
            core: DecoderCore {
                mode,
                reader_schema,
            },
        })
    }

    fn fixed_schema(
        settings: &AvroSettings,
        mode: &str,
    ) -> Result<Arc<CompiledSchema>, AvroConfigError> {
        let source = settings.schema.as_ref().ok_or(AvroConfigError::Invalid {
            detail: format!("mode `{mode}` requires a `schema` (inline or path)"),
        })?;
        if settings.registry.is_some() {
            return Err(AvroConfigError::Invalid {
                detail: format!("mode `{mode}` does not use a `registry` section"),
            });
        }
        let json = source.load_text()?;
        // Per-backend compile (with apache-avro's parse-panic caught): a
        // fixed schema only one backend accepts still builds, and each
        // backend's builder gates on its own side below. Only a schema no
        // enabled backend accepts is a load error.
        let compiled = CompiledSchema::compile(0, &json);
        if let Some(reason) = compiled.unusable_reason() {
            return Err(AvroConfigError::SchemaLoad { detail: reason });
        }
        Ok(Arc::new(compiled))
    }

    /// The fixed schema's compile failure, if any. Fixed-schema modes
    /// compile eagerly at build time, so an unusable schema fails the
    /// builders *here* rather than surfacing per record as
    /// `SchemaUnavailable` — which under the default Skip policy would drop
    /// and ack 100% of the input while watermarks advance. Confluent writer
    /// schemas arrive per id at runtime and cannot be checked until then.
    fn fixed_schema_error(&self) -> Option<String> {
        match &self.core.mode {
            SchemaSourceMode::Raw { schema } | SchemaSourceMode::SingleObject { schema, .. } => {
                schema.schema.as_ref().err().cloned()
            }
            SchemaSourceMode::Confluent { .. } => None,
        }
    }

    /// The dynamically-typed deserializer (emits [`crate::AvroValue`]).
    ///
    /// # Errors
    ///
    /// Rejects a fixed schema (`raw`/`single_object`) that cannot be
    /// parsed — deferring it would surface every record as
    /// `SchemaUnavailable` and drop it under the default Skip policy.
    pub fn build_value(&self) -> Result<AvroValueDeserializer, AvroConfigError> {
        if let Some(reason) = self.fixed_schema_error() {
            return Err(AvroConfigError::Invalid { detail: reason });
        }
        Ok(AvroValueDeserializer::new(self.core.clone()))
    }

    /// The serde-typed deserializer (emits `T`).
    ///
    /// # Errors
    ///
    /// Rejects a fixed schema that cannot be parsed — see
    /// [`Self::build_value`].
    pub fn build_serde<T>(&self) -> Result<AvroSerdeDeserializer<T>, AvroConfigError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        if let Some(reason) = self.fixed_schema_error() {
            return Err(AvroConfigError::Invalid { detail: reason });
        }
        Ok(AvroSerdeDeserializer::new(self.core.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::config::ComponentConfig;

    const SCHEMA: &str = r#"{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}"#;

    fn component(yaml: &str) -> ComponentConfig {
        ComponentConfig::new("avro", serde_yaml::from_str(yaml).unwrap())
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn full_confluent_section_parses() {
        let cfg = component(
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
        let settings: AvroSettings = cfg.deserialize_into().unwrap();
        assert_eq!(settings.mode, AvroMode::Confluent);
        assert_eq!(settings.prewarm_subjects.len(), 2);
        assert_eq!(settings.negative_cache_ttl, Duration::from_secs(45));
        let rt = runtime();
        AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let cfg = component("mode: raw\nschema: {inline: '\"long\"'}\nturbo: true\n");
        let err = cfg.deserialize_into::<AvroSettings>().unwrap_err();
        assert!(err.to_string().contains("turbo"), "{err}");
    }

    #[test]
    fn mode_requirements_are_validated() {
        let rt = runtime();
        let cases: &[(&str, &str)] = &[
            ("mode: confluent", "requires a `registry`"),
            ("mode: raw", "requires a `schema`"),
            ("mode: single_object", "requires a `schema`"),
        ];
        for (yaml, needle) in cases {
            let settings: AvroSettings = component(yaml).deserialize_into().unwrap();
            let err = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap_err();
            assert!(err.to_string().contains(needle), "{yaml}: {err}");
        }
    }

    #[test]
    fn conflicting_sections_are_rejected() {
        let rt = runtime();
        let confluent_with_schema = format!(
            "mode: confluent\nregistry: {{url: http://sr}}\nschema: {{inline: '{SCHEMA}'}}"
        );
        let raw_with_registry =
            format!("mode: raw\nschema: {{inline: '{SCHEMA}'}}\nregistry: {{url: http://sr}}");
        let raw_with_prewarm =
            format!("mode: raw\nschema: {{inline: '{SCHEMA}'}}\nprewarm_subjects: [x]");
        let so_with_prewarm =
            format!("mode: single_object\nschema: {{inline: '{SCHEMA}'}}\nprewarm_subjects: [x]");
        let so_with_reader = format!(
            "mode: single_object\nschema: {{inline: '{SCHEMA}'}}\nreader_schema: {{inline: '{SCHEMA}'}}"
        );
        for yaml in [
            confluent_with_schema,
            raw_with_registry,
            raw_with_prewarm,
            so_with_prewarm,
            so_with_reader,
        ] {
            let settings: AvroSettings = component(&yaml).deserialize_into().unwrap();
            assert!(
                AvroDeserializerBuilder::from_settings(&settings, rt.handle()).is_err(),
                "must reject: {yaml}"
            );
        }
    }

    #[test]
    fn schema_from_file_and_bad_schema_errors() {
        let rt = runtime();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("e.avsc");
        std::fs::write(&path, SCHEMA).unwrap();
        let settings: AvroSettings =
            component(&format!("mode: raw\nschema: {{path: {}}}", path.display()))
                .deserialize_into()
                .unwrap();
        AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();

        let settings: AvroSettings = component("mode: raw\nschema: {inline: 'not json'}")
            .deserialize_into()
            .unwrap();
        let err = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap_err();
        assert!(matches!(err, AvroConfigError::SchemaLoad { .. }), "{err}");
    }
}
