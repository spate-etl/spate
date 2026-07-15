//! S3 source configuration: typed fields plus a validated raw
//! `object_store` option passthrough.

use bytesize::ByteSize;
use etl_core::config::{ComponentConfig, ConfigError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

/// `object_store` option keys the framework owns. Setting them through the
/// passthrough is rejected at load time with an explanation.
///
/// `object_store` accepts several spellings for the same underlying option
/// (its config keys parse with and without the `aws_` prefix and with
/// historic aliases), and lowercases keys before parsing — so the check
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

fn default_lanes() -> u32 {
    4
}

fn default_prefetch_bytes() -> ByteSize {
    ByteSize::mib(8)
}

fn default_chunk_bytes() -> ByteSize {
    ByteSize::kib(512)
}

fn default_checkpoint_timeout() -> Duration {
    Duration::from_secs(10)
}

/// Debug view of a raw option map: keys are configuration, values may be
/// credentials (`aws_secret_access_key`, session tokens) — never print
/// them.
struct Redacted<'a>(&'a BTreeMap<String, String>);

impl std::fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.0.keys().map(|k| (k, "<redacted>")))
            .finish()
    }
}

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

/// Where the source persists its committed watermarks (the manifest
/// object). Object storage has no broker-side commit, so this is what makes
/// a backfill resumable.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointStoreConfig {
    /// Full URL of the manifest object, e.g.
    /// `s3://my-bucket/_etl_checkpoints/dns-backfill.json`. Must not live
    /// under the source prefix (it would appear in the listing).
    pub url: String,
    /// Raw `object_store` options for the checkpoint store. When empty and
    /// the manifest lives on the same scheme and host as the source, the
    /// source's `store` options are reused.
    #[serde(default)]
    pub store: BTreeMap<String, String>,
    /// Bound on each manifest read/write. A commit slower than this is
    /// retried on the next commit tick.
    #[serde(with = "humantime_serde", default = "default_checkpoint_timeout")]
    pub timeout: Duration,
}

// Hand-written: the `store` map carries credentials; `{:?}` must never
// print them.
impl std::fmt::Debug for CheckpointStoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointStoreConfig")
            .field("url", &self.url)
            .field("timeout", &self.timeout)
            .field("store", &Redacted(&self.store))
            .finish()
    }
}

/// Configuration of an `S3Source`, deserialized from the pipeline's opaque
/// `source: { s3: ... }` section.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct S3SourceConfig {
    /// Bucket and prefix to backfill, e.g. `s3://my-bucket/exports/2026/`.
    /// Every object under the prefix is read; the key set must not change
    /// for the lifetime of the backfill (see the crate docs). `file://`
    /// URLs work for infrastructure-free runs and tests.
    pub url: String,
    /// Number of lanes (framework partitions). The listing is dealt
    /// round-robin across lanes; each lane streams its slice sequentially,
    /// so this bounds read parallelism.
    #[serde(default = "default_lanes")]
    pub lanes: u32,
    /// Compression codec of the objects.
    #[serde(default)]
    pub compression: Compression,
    /// Durable watermark storage (required — this is what makes the
    /// backfill resumable).
    pub checkpoint: CheckpointStoreConfig,
    /// Per-lane prefetch budget: bytes buffered between the async fetcher
    /// and the pipeline thread.
    #[serde(default = "default_prefetch_bytes")]
    pub prefetch_bytes: ByteSize,
    /// Target size of a single buffered chunk.
    #[serde(default = "default_chunk_bytes")]
    pub chunk_bytes: ByteSize,
    /// Raw `object_store` options, applied when building the store from
    /// `url` (credentials, region, endpoint, timeouts, ...). Bucket keys
    /// are rejected — the bucket comes from `url`.
    #[serde(default)]
    pub store: BTreeMap<String, String>,
}

// Hand-written: the `store` map carries credentials; `{:?}` must never
// print them.
impl std::fmt::Debug for S3SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3SourceConfig")
            .field("url", &self.url)
            .field("lanes", &self.lanes)
            .field("compression", &self.compression)
            .field("checkpoint", &self.checkpoint)
            .field("prefetch_bytes", &self.prefetch_bytes)
            .field("chunk_bytes", &self.chunk_bytes)
            .field("store", &Redacted(&self.store))
            .finish()
    }
}

impl S3SourceConfig {
    /// Deserialize and validate from the pipeline's opaque component
    /// section.
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError> {
        let cfg: S3SourceConfig = section.deserialize_into()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation, including the passthrough denylist.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let source = parse_url("source.s3.url", &self.url)?;
        let checkpoint = parse_url("source.s3.checkpoint.url", &self.checkpoint.url)?;
        if self.lanes == 0 {
            return Err(ConfigError::Validation(
                "source.s3.lanes must be at least 1".into(),
            ));
        }
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
        for (map, section) in [
            (&self.store, "source.s3.store"),
            (&self.checkpoint.store, "source.s3.checkpoint.store"),
        ] {
            // Case-insensitive: object_store lowercases option keys before
            // parsing, so `Bucket:` would otherwise slip past the check.
            for key in map.keys() {
                let lowered = key.to_ascii_lowercase();
                if let Some((_, why)) = DENYLIST.iter().find(|(denied, _)| *denied == lowered) {
                    return Err(ConfigError::Validation(format!(
                        "{section}.\"{key}\" cannot be overridden: {why}"
                    )));
                }
            }
        }
        // The manifest must never appear in the backfill's own listing: a
        // checkpoint under the source prefix would be read back as data (and
        // mutate the "frozen" key set on every commit).
        if same_store(&source, &checkpoint)
            && checkpoint.path().starts_with(&normalized_prefix(&source))
        {
            return Err(ConfigError::Validation(format!(
                "source.s3.checkpoint.url ({}) must not live under the source prefix ({}); \
                 the manifest would show up in the backfill's own listing",
                self.checkpoint.url, self.url
            )));
        }
        Ok(())
    }
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
    // `memory://` builds a fresh empty store on every parse, so the source,
    // its checkpoint, and any test setup would each see a different store.
    if url.scheme() == "memory" {
        return Err(ConfigError::Validation(format!(
            "{field}: memory:// creates a new empty store per component and cannot be \
             shared; use file:// for infrastructure-free runs"
        )));
    }
    Ok(url)
}

/// Whether two URLs address the same underlying store (scheme + authority).
fn same_store(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme() && a.authority() == b.authority()
}

/// The source path as a prefix: normalized to end in exactly one `/` so
/// `starts_with` cannot match a sibling key that merely shares leading
/// characters (`exports/2026-old` vs `exports/2026/`).
fn normalized_prefix(source: &Url) -> String {
    let path = source.path().trim_end_matches('/');
    format!("{path}/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(body: &str) -> ComponentConfig {
        let yaml = format!("s3:\n{body}");
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        ComponentConfig::new("s3", value["s3"].clone())
    }

    fn minimal() -> String {
        "  url: s3://bucket/exports/\n  checkpoint:\n    url: s3://bucket/_etl/backfill.json\n"
            .to_string()
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = S3SourceConfig::from_component_config(&section(&minimal())).unwrap();
        assert_eq!(cfg.lanes, 4);
        assert_eq!(cfg.compression, Compression::Auto);
        assert_eq!(cfg.prefetch_bytes, ByteSize::mib(8));
        assert_eq!(cfg.chunk_bytes, ByteSize::kib(512));
        assert_eq!(cfg.checkpoint.timeout, Duration::from_secs(10));
        assert!(cfg.store.is_empty());
        assert!(cfg.checkpoint.store.is_empty());
    }

    #[test]
    fn debug_never_prints_store_values() {
        let body = format!(
            "{}  store:\n    aws_secret_access_key: hunter2\n  checkpoint:\n    url: s3://bucket/_etl/backfill.json\n    store:\n      aws_session_token: hunter3\n",
            "  url: s3://bucket/exports/\n"
        );
        let cfg = S3SourceConfig::from_component_config(&section(&body)).unwrap();
        let printed = format!("{cfg:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(!printed.contains("hunter3"), "{printed}");
        assert!(
            printed.contains("aws_secret_access_key") && printed.contains("<redacted>"),
            "keys stay visible for debugging: {printed}"
        );
    }

    #[test]
    fn bucket_aliases_are_rejected_in_both_passthroughs() {
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

            let body = format!(
                "  url: s3://bucket/exports/\n  checkpoint:\n    url: s3://bucket/_etl/b.json\n    store:\n      {key}: other\n"
            );
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            assert!(
                err.to_string().contains("checkpoint.store"),
                "error names the section: {err}"
            );
        }
    }

    #[test]
    fn checkpoint_under_the_source_prefix_is_rejected() {
        let body =
            "  url: s3://bucket/exports/\n  checkpoint:\n    url: s3://bucket/exports/state.json\n";
        let err = S3SourceConfig::from_component_config(&section(body)).unwrap_err();
        assert!(err.to_string().contains("listing"), "{err}");
    }

    #[test]
    fn checkpoint_on_a_sibling_prefix_or_other_store_is_accepted() {
        for body in [
            // Sibling prefix in the same bucket.
            "  url: s3://bucket/exports/\n  checkpoint:\n    url: s3://bucket/_etl/state.json\n",
            // Prefix that shares leading characters but is a different key.
            "  url: s3://bucket/exports\n  checkpoint:\n    url: s3://bucket/exports-meta/state.json\n",
            // Different bucket entirely, same path.
            "  url: s3://bucket/exports/\n  checkpoint:\n    url: s3://other/exports/state.json\n",
        ] {
            S3SourceConfig::from_component_config(&section(body)).unwrap();
        }
    }

    #[test]
    fn memory_scheme_is_rejected_with_guidance() {
        let body =
            "  url: memory:///data/\n  checkpoint:\n    url: memory:///state.json\n".to_string();
        let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
        assert!(err.to_string().contains("file://"), "actionable: {err}");
    }

    #[test]
    fn zero_lanes_and_bad_buffer_sizes_are_rejected() {
        for extra in [
            "  lanes: 0\n",
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
        let body = "  url: file:///tmp/objects/\n  checkpoint:\n    url: file:///tmp/state/backfill.json\n";
        S3SourceConfig::from_component_config(&section(body)).unwrap();
    }
}
