//! S3 source configuration: typed fields plus a validated raw
//! `object_store` option passthrough.
//!
//! `url`, `compression` and `split_target_bytes` shape the **work itself**:
//! they feed a bounded feed job's fingerprint, which every instance must
//! agree on. `prefetch_bytes`, `chunk_bytes` and `store` are how *this*
//! instance reads that work, and may differ per replica.

use bytesize::ByteSize;
use object_store::ObjectStoreScheme;
use object_store::aws::AmazonS3ConfigKey;
use serde::{Deserialize, Serialize};
use spate_core::config::{ComponentConfig, ConfigError, redact};
use std::collections::BTreeMap;
use url::Url;

/// `object_store` option keys the framework owns. Setting them through the
/// passthrough is rejected at load time with an explanation.
///
/// `object_store` accepts several spellings for the same underlying option
/// (its config keys parse with and without the `aws_` prefix and with
/// historic aliases), and lowercases keys before parsing, so the check
/// here is case-insensitive over every spelling. Because the store is
/// built from the `url` field, a passthrough bucket key could never take
/// effect (the URL's bucket wins at build time); rejecting it keeps the
/// config honest instead of silently ignoring it.
const DENYLIST: &[(&str, &str)] = &[
    ("bucket", "owned by the typed `url` field"),
    ("bucket_name", "owned by the typed `url` field"),
    ("aws_bucket", "owned by the typed `url` field"),
    ("aws_bucket_name", "owned by the typed `url` field"),
];

fn default_prefetch_bytes() -> ByteSize {
    ByteSize::mib(8)
}

fn default_chunk_bytes() -> ByteSize {
    ByteSize::kib(512)
}

fn default_split_target_bytes() -> ByteSize {
    ByteSize::mib(64)
}

/// Floor for `split_target_bytes`: below ~1 MiB a large object is cut into so
/// many ranges that the per-unit cost outweighs the read parallelism gained.
const SPLIT_TARGET_FLOOR: u64 = 1024 * 1024;

/// Compression codec of the objects under the prefix.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Compression {
    /// Decide per object by key extension: `.gz`/`.gzip` → gzip,
    /// `.zst`/`.zstd` → zstd, anything else → uncompressed.
    #[default]
    Auto,
    /// Every object is uncompressed.
    None,
    /// Every object is gzip (multi-member streams are fully read).
    Gzip,
    /// Every object is zstd (multi-frame streams are fully read).
    Zstd,
}

/// Configuration of an `S3Source`, deserialized from the pipeline's opaque
/// `source: { s3: ... }` section.
///
/// Construct with [`S3SourceConfig::new`] and set the optional fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct S3SourceConfig {
    /// Bucket and prefix to backfill, e.g. `s3://my-bucket/exports/2026/`.
    /// Every object under the prefix is read; the planned key set is
    /// pinned per split (see the crate docs). `file://` URLs work for
    /// infrastructure-free runs and tests.
    pub url: String,
    /// Compression codec of the objects.
    #[serde(default)]
    pub compression: Compression,
    /// Size above which an uncompressed object with an ETag, at most 50,000
    /// GiB, is cut into byte ranges of at most this size, one unit each, when
    /// the framer declares a resync delimiter. Any other object is one unit.
    /// Part of the bounded feed job fingerprint: every instance of a bounded
    /// job must agree on it. Default 64 MiB.
    #[serde(default = "default_split_target_bytes")]
    pub split_target_bytes: ByteSize,
    /// Target GET window size for ETag-pinned reads. Queued chunks retain
    /// backing windows while a later GET is fetched; decoded records require
    /// additional memory. Byte-range boundary windows can add `chunk_bytes`
    /// to this size. Whole-object reads at or below this size use a single GET.
    ///
    /// Keep this modest. A large window is read as one long-held GET, which
    /// works against the connection release this source relies on, and a
    /// single window must complete inside the client request timeout (the
    /// `store` `timeout` option, 30s by default), so an oversized window on a
    /// slow link degrades into repeated whole-window retries. The 8 MiB default
    /// suits typical objects.
    #[serde(default = "default_prefetch_bytes")]
    pub prefetch_bytes: ByteSize,
    /// Target size of a single buffered chunk.
    #[serde(default = "default_chunk_bytes")]
    pub chunk_bytes: ByteSize,
    /// Raw `object_store` options, applied when building the store from
    /// `url` (credentials, region, endpoint, timeouts, ...). Bucket keys
    /// are rejected; the bucket comes from `url`.
    #[serde(default)]
    pub store: BTreeMap<String, String>,
}

// Hand-written: the `store` map carries credentials. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl std::fmt::Debug for S3SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let S3SourceConfig {
            url,
            compression,
            split_target_bytes,
            prefetch_bytes,
            chunk_bytes,
            store,
        } = self;
        f.debug_struct("S3SourceConfig")
            .field("url", url)
            .field("compression", compression)
            .field("split_target_bytes", split_target_bytes)
            .field("prefetch_bytes", prefetch_bytes)
            .field("chunk_bytes", chunk_bytes)
            .field("store", &redact::map(store.keys()))
            .finish()
    }
}

impl S3SourceConfig {
    /// A config for the bucket and prefix at `url`. Every other field
    /// starts at its YAML default.
    #[must_use]
    pub fn new(url: impl Into<String>) -> S3SourceConfig {
        S3SourceConfig {
            url: url.into(),
            compression: Compression::default(),
            split_target_bytes: default_split_target_bytes(),
            prefetch_bytes: default_prefetch_bytes(),
            chunk_bytes: default_chunk_bytes(),
            store: BTreeMap::new(),
        }
    }

    /// Deserialize and validate from the pipeline's opaque component
    /// section.
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError> {
        let cfg: S3SourceConfig = section.deserialize_into()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation, including the passthrough denylist.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let url = parse_url("source.s3.url", &self.url)?;
        if self.chunk_bytes.as_u64() == 0 {
            return Err(ConfigError::Validation(
                "source.s3.chunk_bytes must not be zero".into(),
            ));
        }
        if self.prefetch_bytes.as_u64() < self.chunk_bytes.as_u64() {
            return Err(ConfigError::Validation(format!(
                "source.s3.prefetch_bytes ({}) must be at least chunk_bytes ({})",
                self.prefetch_bytes, self.chunk_bytes
            )));
        }
        if self.split_target_bytes.as_u64() < SPLIT_TARGET_FLOOR {
            return Err(ConfigError::Validation(format!(
                "source.s3.split_target_bytes ({}) must be at least 1MiB",
                self.split_target_bytes
            )));
        }
        // Case-insensitive: object_store lowercases option keys before
        // parsing, so `Bucket:` would otherwise slip past the check.
        for key in self.store.keys() {
            let lowered = key.to_ascii_lowercase();
            if let Some((_, why)) = DENYLIST.iter().find(|(denied, _)| *denied == lowered) {
                return Err(ConfigError::Validation(format!(
                    "source.s3.store.\"{key}\" cannot be overridden: {why}"
                )));
            }
        }
        // Only the S3 client's keys are checked. `file://` reads no options,
        // and with this crate's object_store features any other scheme fails
        // when the source starts.
        if matches!(
            ObjectStoreScheme::parse(&url),
            Ok((ObjectStoreScheme::AmazonS3, _))
        ) && let Some(key) = self.store.keys().find(|k| s3_store_key(k).is_none())
        {
            return Err(ConfigError::Validation(format!(
                "source.s3.store.\"{key}\" is not an object_store S3 option"
            )));
        }
        Ok(())
    }
}

/// The S3 client option a `store` key names, under any spelling and case
/// `object_store` accepts.
pub(crate) fn s3_store_key(key: &str) -> Option<AmazonS3ConfigKey> {
    key.to_ascii_lowercase().parse().ok()
}

/// Parse and reject URLs the source cannot honor.
fn parse_url(field: &str, value: &str) -> Result<Url, ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::Validation(format!(
            "{field} must not be empty"
        )));
    }
    let url = Url::parse(value)
        .map_err(|e| ConfigError::Validation(format!("{field} is not a valid URL: {e}")))?;
    // `memory://` builds a fresh empty store on every parse, so the source
    // and any test setup would each see a different store.
    if url.scheme() == "memory" {
        return Err(ConfigError::Validation(format!(
            "{field}: memory:// creates a new empty store per component and cannot be \
             shared; use file:// for infrastructure-free runs"
        )));
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(body: &str) -> ComponentConfig {
        spate_test::component_config("s3", body)
    }

    fn minimal() -> String {
        "  url: s3://bucket/exports/\n".to_string()
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = S3SourceConfig::from_component_config(&section(&minimal())).unwrap();
        assert_eq!(cfg.compression, Compression::Auto);
        assert_eq!(cfg.prefetch_bytes, ByteSize::mib(8));
        assert_eq!(cfg.chunk_bytes, ByteSize::kib(512));
        assert_eq!(cfg.split_target_bytes, ByteSize::mib(64));
        assert!(cfg.store.is_empty());
    }

    /// `new` and the YAML defaults are two spellings of one config, so a
    /// knob added to the struct has to reach both.
    #[test]
    fn new_matches_the_yaml_defaults() {
        assert_eq!(
            S3SourceConfig::new("s3://bucket/exports/"),
            S3SourceConfig::from_component_config(&section(&minimal())).unwrap()
        );
    }

    #[test]
    fn split_target_parses_and_floors() {
        let body = format!("{}  split_target_bytes: 32MB\n", minimal());
        let cfg = S3SourceConfig::from_component_config(&section(&body)).unwrap();
        assert_eq!(cfg.split_target_bytes, ByteSize::mb(32));

        let low = format!("{}  split_target_bytes: 64KB\n", minimal());
        let err = S3SourceConfig::from_component_config(&section(&low)).unwrap_err();
        assert!(err.to_string().contains("split_target_bytes"), "{err}");
    }

    #[test]
    fn removed_knobs_are_rejected() {
        // None of these are keys of the source body.
        for extra in [
            "  lanes: 4\n",
            "  refresh_listing: true\n",
            "  checkpoint:\n    url: s3://bucket/_etl/b.json\n",
            "  coordination:\n    nats:\n      servers: [\"nats://n:4222\"]\n",
        ] {
            let body = format!("{}{extra}", minimal());
            assert!(
                S3SourceConfig::from_component_config(&section(&body)).is_err(),
                "must reject removed knob: {extra}"
            );
        }
    }

    #[test]
    fn debug_never_prints_store_values() {
        let body = format!(
            "{}  store:\n    aws_secret_access_key: hunter2\n",
            minimal()
        );
        let cfg = S3SourceConfig::from_component_config(&section(&body)).unwrap();
        let printed = format!("{cfg:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(
            printed.contains("aws_secret_access_key") && printed.contains("<redacted>"),
            "keys stay visible for debugging: {printed}"
        );
    }

    #[test]
    fn bucket_aliases_are_rejected_in_the_passthrough() {
        // object_store lowercases option keys before parsing, so the
        // denylist must catch every case spelling too.
        for key in [
            "bucket",
            "bucket_name",
            "aws_bucket",
            "aws_bucket_name",
            "Bucket",
            "AWS_BUCKET_NAME",
        ] {
            let body = format!("{}  store:\n    {key}: other\n", minimal());
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            assert!(err.to_string().contains(key), "error names the key: {err}");
        }
    }

    /// A misspelled key would leave the client on its default, so a typo in
    /// `endpoint` sends requests signed with the configured credentials to
    /// AWS. Regression for #1118.
    #[test]
    fn unrecognized_store_keys_are_rejected() {
        for key in ["endpont", "endpoint_unused", "Endpont"] {
            let body = format!("{}  store:\n    {key}: http://127.0.0.1:9000\n", minimal());
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            assert!(
                err.to_string().contains(&format!("store.\"{key}\"")),
                "error names the key as written: {err}"
            );
        }
    }

    /// Every key is checked, including one that sorts after recognized keys,
    /// as in the #1118 configuration.
    #[test]
    fn an_unrecognized_key_after_recognized_ones_is_rejected() {
        let body = format!(
            "{}  store:\n    access_key_id: k\n    allow_http: \"true\"\n    endpont: x\n",
            minimal()
        );
        let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
        assert!(err.to_string().contains("store.\"endpont\""), "{err}");
    }

    /// The local client reads no `store` options, so a `file://` URL loads
    /// with a key the S3 client would not recognize.
    #[test]
    fn file_url_store_keys_are_not_checked() {
        let body = "  url: file:///tmp/objects/\n  store:\n    endpont: x\n";
        S3SourceConfig::from_component_config(&section(body)).unwrap();
    }

    /// An `https://` URL on an S3 host is checked like `s3://`.
    #[test]
    fn https_s3_host_store_keys_are_checked() {
        for url in [
            "https://s3.us-east-1.amazonaws.com/bucket/exports/",
            "https://acct.r2.cloudflarestorage.com/bucket/exports/",
        ] {
            let body = format!("  url: {url}\n  store:\n    endpont: x\n");
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            assert!(
                err.to_string().contains("store.\"endpont\""),
                "{url}: {err}"
            );
        }
    }

    #[test]
    fn recognized_store_spellings_load() {
        for key in [
            "endpoint",
            "AWS_ENDPOINT_URL",
            "aws_region",
            "allow_http",
            "pool_idle_timeout",
            "aws_container_credentials_full_uri",
            "aws_container_authorization_token_file",
        ] {
            let body = format!("{}  store:\n    {key}: x\n", minimal());
            S3SourceConfig::from_component_config(&section(&body))
                .unwrap_or_else(|e| panic!("{key} is an object_store S3 option: {e}"));
        }
    }

    #[test]
    fn memory_scheme_is_rejected_with_guidance() {
        let err = S3SourceConfig::from_component_config(&section("  url: memory:///data/\n"))
            .unwrap_err();
        assert!(err.to_string().contains("file://"), "actionable: {err}");
    }

    #[test]
    fn bad_buffer_sizes_are_rejected() {
        for extra in [
            "  chunk_bytes: 0\n",
            "  prefetch_bytes: 4KiB\n  chunk_bytes: 1MiB\n",
        ] {
            let body = format!("{}{extra}", minimal());
            assert!(
                S3SourceConfig::from_component_config(&section(&body)).is_err(),
                "must reject: {extra}"
            );
        }
    }

    #[test]
    fn unknown_fields_and_unknown_variants_are_rejected() {
        for extra in ["  prefix: also-here\n", "  compression: xz\n"] {
            let body = format!("{}{extra}", minimal());
            assert!(
                S3SourceConfig::from_component_config(&section(&body)).is_err(),
                "must reject: {extra}"
            );
        }
    }

    #[test]
    fn file_urls_are_accepted() {
        S3SourceConfig::from_component_config(&section("  url: file:///tmp/objects/\n")).unwrap();
    }
}
