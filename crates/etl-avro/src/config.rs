//! Configuration and construction.
//!
//! The `deserializer: { avro: { ... } }` section of the pipeline YAML is
//! handed here as an opaque
//! [`ComponentConfig`](etl_core::config::ComponentConfig):
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
//!       path: /etc/etl/orders.avsc   # (or `inline: '{"type": ...}'`)
//!     # schema: { inline | path }    # required for raw / single_object
//! ```

use crate::cache::CompiledSchema;
#[cfg(feature = "fast")]
use crate::deser::AvroFastDeserializer;
use crate::deser::{AvroSerdeDeserializer, AvroValueDeserializer, DecoderCore, SchemaSourceMode};
use crate::registry::{RegistryConfig, spawn_fetcher};
use apache_avro::Schema;
use apache_avro::rabin::Rabin;
use etl_core::config::ComponentConfig;
use serde::Deserialize;
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
    Config(#[from] etl_core::config::ConfigError),
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
                // The single-object header fingerprint is computed by
                // apache-avro, so this mode needs the apache parse to
                // succeed even for a fast-only pipeline (unlike `raw`).
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

    /// The fixed schema's apache-side failure, if any. Fixed-schema modes
    /// compile every backend eagerly at build time, so a schema this
    /// backend cannot use fails the apache builders *here* rather than
    /// surfacing per record as `SchemaUnavailable` — which under the
    /// default Skip policy would drop and ack 100% of the input while
    /// watermarks advance (the same gate `build_fast` applies to the fast
    /// side). Confluent writer schemas arrive per id at runtime and cannot
    /// be checked until then.
    fn fixed_apache_error(&self) -> Option<String> {
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
    /// Rejects a fixed schema (`raw`/`single_object`) that the apache
    /// backend cannot parse — deferring it would surface every record as
    /// `SchemaUnavailable` and drop it under the default Skip policy. The
    /// fast builders gate on their own backend the same way.
    pub fn build_value(&self) -> Result<AvroValueDeserializer, AvroConfigError> {
        if let Some(reason) = self.fixed_apache_error() {
            return Err(AvroConfigError::Invalid { detail: reason });
        }
        Ok(AvroValueDeserializer::new(self.core.clone()))
    }

    /// The serde-typed deserializer (emits `T`).
    ///
    /// # Errors
    ///
    /// Rejects a fixed schema the apache backend cannot parse — see
    /// [`Self::build_value`].
    pub fn build_serde<T>(&self) -> Result<AvroSerdeDeserializer<T>, AvroConfigError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        if let Some(reason) = self.fixed_apache_error() {
            return Err(AvroConfigError::Invalid { detail: reason });
        }
        Ok(AvroSerdeDeserializer::new(self.core.clone()))
    }

    /// The fast single-pass deserializer for **owned** records (emits `T`).
    /// The `Owned<T>` counterpart of [`Self::build_fast`] — see
    /// [`AvroFastDeserializer`] for the backend's semantics.
    ///
    /// # Errors
    ///
    /// Rejects a configured `reader_schema`: the fast backend resolves each
    /// writer schema directly into `T` and has no reader-schema resolution —
    /// evolution is expressed with serde attributes instead (see the
    /// [`AvroFastDeserializer`] docs). Also rejects a fixed schema this
    /// backend cannot compile, mirroring [`Self::build_value`]'s gate on
    /// the apache side.
    #[cfg(feature = "fast")]
    pub fn build_serde_fast<T>(
        &self,
    ) -> Result<AvroFastDeserializer<etl_core::deser::Owned<T>>, AvroConfigError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.build_fast::<etl_core::deser::Owned<T>>()
    }

    /// The fast single-pass deserializer for an arbitrary record family —
    /// including **borrowed** (zero-copy) families whose records point into
    /// the payload buffer. See [`AvroFastDeserializer`] for the family
    /// pattern and the backend's semantics.
    ///
    /// # Errors
    ///
    /// Rejects a configured `reader_schema` — see
    /// [`Self::build_serde_fast`].
    #[cfg(feature = "fast")]
    pub fn build_fast<F>(&self) -> Result<AvroFastDeserializer<F>, AvroConfigError>
    where
        F: etl_core::deser::RecFamily,
        for<'buf> F::Rec<'buf>: serde::Deserialize<'buf>,
    {
        if self.core.reader_schema.is_some() {
            return Err(AvroConfigError::Invalid {
                detail: "the fast backend resolves each writer schema directly into \
                         the target type; `reader_schema` is not supported — use \
                         serde attributes (`#[serde(default)]`, `#[serde(alias)]`) \
                         for evolution, or the apache backend (`build_serde`) for \
                         Avro schema resolution"
                    .into(),
            });
        }
        // Fixed-schema modes compile the fast form eagerly at build time, so a
        // schema `apache-avro` accepts but `serde_avro_fast` rejects is a
        // build-time error here rather than a per-record `SchemaUnavailable`
        // that would drop every record under the default Skip policy while
        // watermarks advance. Confluent writer schemas are compiled per id as
        // they arrive from the registry and cannot be checked until then.
        match &self.core.mode {
            SchemaSourceMode::Raw { schema } | SchemaSourceMode::SingleObject { schema, .. } => {
                if let Err(reason) = &schema.fast {
                    return Err(AvroConfigError::Invalid {
                        detail: reason.clone(),
                    });
                }
            }
            SchemaSourceMode::Confluent { .. } => {}
        }
        Ok(AvroFastDeserializer::new(self.core.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_core::config::ComponentConfig;

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

    #[cfg(feature = "fast")]
    #[test]
    fn reader_schema_is_rejected_by_the_fast_builders() {
        // The fast backend has no reader-schema resolution; the rejection
        // happens at build time (the backend choice postdates the settings),
        // mirroring the single_object + reader_schema rejection above.
        let rt = runtime();
        let yaml = format!(
            "mode: raw\nschema: {{inline: '{SCHEMA}'}}\nreader_schema: {{inline: '{SCHEMA}'}}"
        );
        let settings: AvroSettings = component(&yaml).deserialize_into().unwrap();
        let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();

        #[derive(serde::Deserialize)]
        struct E {
            #[expect(dead_code, reason = "shape only")]
            id: i64,
        }
        let err = builder.build_serde_fast::<E>().unwrap_err();
        assert!(
            matches!(&err, AvroConfigError::Invalid { detail } if detail.contains("reader_schema")),
            "{err}"
        );
        // The apache builders accept the same settings.
        builder.build_serde::<E>().unwrap();
        builder.build_value().unwrap();
    }

    #[cfg(feature = "fast")]
    #[test]
    fn fast_incompatible_fixed_schema_is_rejected_at_build_time() {
        // A fixed schema apache-avro accepts but serde_avro_fast 2.1 rejects
        // (here a `bytes` decimal logical type missing `precision`) must fail
        // the fast builders at *build* time. Deferring it would surface every
        // record as SchemaUnavailable and drop it under the default Skip
        // policy while watermarks advance. The apache builders, which do not
        // use the fast form, accept the very same settings.
        let rt = runtime();
        let bad = r#"{"type":"record","name":"D","fields":[{"name":"amt","type":{"type":"bytes","logicalType":"decimal","scale":2}}]}"#;
        let yaml = format!("mode: raw\nschema: {{inline: '{bad}'}}");
        let settings: AvroSettings = component(&yaml).deserialize_into().unwrap();
        let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();

        #[derive(serde::Deserialize)]
        struct D {
            #[expect(dead_code, reason = "shape only")]
            amt: Vec<u8>,
        }
        let err = builder.build_serde_fast::<D>().unwrap_err();
        assert!(
            matches!(&err, AvroConfigError::Invalid { detail }
                if detail.contains("fast backend") && detail.contains("precision")),
            "{err}"
        );
        // The apache builders accept the same settings.
        builder.build_serde::<D>().unwrap();
        builder.build_value().unwrap();
    }

    #[cfg(feature = "fast")]
    #[test]
    fn apache_incompatible_fixed_schema_builds_only_the_fast_backend() {
        // The mirror image: a raw-mode schema apache-avro rejects (it panics
        // on the dashed record name) but serde_avro_fast accepts. The fast
        // builders work — this used to be impossible, the load poisoned the
        // whole builder — while the apache builders reject at build time
        // with the stored reason instead of draining records at runtime.
        let rt = runtime();
        let bad = r#"{"type":"record","name":"my-record","fields":[{"name":"id","type":"long"}]}"#;
        let yaml = format!("mode: raw\nschema: {{inline: '{bad}'}}");
        let settings: AvroSettings = component(&yaml).deserialize_into().unwrap();
        let builder = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap();

        #[derive(Debug, serde::Deserialize)]
        struct E {
            #[expect(dead_code, reason = "shape only")]
            id: i64,
        }
        builder.build_serde_fast::<E>().unwrap();

        let err = builder.build_value().unwrap_err();
        assert!(
            matches!(&err, AvroConfigError::Invalid { detail } if detail.contains("apache backend")),
            "{err}"
        );
        let err = builder.build_serde::<E>().unwrap_err();
        assert!(
            matches!(&err, AvroConfigError::Invalid { detail } if detail.contains("apache backend")),
            "{err}"
        );
    }

    #[cfg(feature = "fast")]
    #[test]
    fn single_object_requires_the_apache_parse_for_its_fingerprint() {
        // single_object frames with the Rabin fingerprint, which apache-avro
        // computes — so unlike `raw`, an apache-rejected schema fails the
        // settings even though the fast backend could decode the datum.
        let rt = runtime();
        let bad = r#"{"type":"record","name":"my-record","fields":[{"name":"id","type":"long"}]}"#;
        let yaml = format!("mode: single_object\nschema: {{inline: '{bad}'}}");
        let settings: AvroSettings = component(&yaml).deserialize_into().unwrap();
        let err = AvroDeserializerBuilder::from_settings(&settings, rt.handle()).unwrap_err();
        assert!(
            matches!(&err, AvroConfigError::SchemaLoad { detail } if detail.contains("fingerprint")),
            "{err}"
        );
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
