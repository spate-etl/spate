//! S3 source configuration: typed fields plus a validated raw
//! `object_store` option passthrough.
//!
//! Deliberately absent: any coordination-backend configuration. The
//! source takes a fully built coordinator at assembly time
//! ([`S3Source::with_coordinator`](crate::S3Source::with_coordinator)) —
//! which backend to use, and its tuning, is the deployer's wiring, not
//! this connector's.
//!
//! What is here splits in two. `url`, `compression`, `split_target_bytes`
//! and `refresh_listing` shape the **work itself**: they feed the job
//! fingerprint and must be byte-equal across every instance of a
//! coordinated job. `prefetch_bytes`, `chunk_bytes` and `store` are how
//! *this* instance reads that work, and may differ per replica.

use bytesize::ByteSize;
use etl_core::config::{ComponentConfig, ConfigError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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

fn default_prefetch_bytes() -> ByteSize {
    ByteSize::mib(8)
}

fn default_chunk_bytes() -> ByteSize {
    ByteSize::kib(512)
}

fn default_split_target_bytes() -> ByteSize {
    ByteSize::mib(64)
}

/// Floor for `split_target_bytes`: below ~1 MiB the per-object open-cost
/// floor collapses and packing degenerates into thousands of one-object
/// splits, taxing the coordination store for no read-parallelism gain.
const SPLIT_TARGET_FLOOR: u64 = 1024 * 1024;

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

/// Configuration of an `S3Source`, deserialized from the pipeline's opaque
/// `source: { s3: ... }` section.
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
    /// Target size of one split — the unit of work distribution, and the
    /// unit the leader balances. Objects are packed toward it in listing
    /// order with a
    /// per-object cost floor of a sixteenth of the target, so a split
    /// holds at most ~16 objects; an object at or above the target gets a
    /// split of its own. Part of the job fingerprint: every instance of a
    /// coordinated job must agree on it. Default 64 MiB.
    #[serde(default = "default_split_target_bytes")]
    pub split_target_bytes: ByteSize,
    /// Re-list the prefix on the coordinator's replan interval, planning
    /// newly arrived objects as additional splits (an **open** plan: the
    /// job never self-terminates). Default `false`: one listing, a
    /// bounded backfill that completes.
    #[serde(default)]
    pub refresh_listing: bool,
    /// Per-lane read-ahead budget: the size of each lane's in-memory read
    /// window. A lane fetches one window at a time as a bounded ranged GET and
    /// drains it before fetching the next, so peak per-lane *read-ahead* is
    /// ~one window and total source read-ahead is
    /// `in-flight splits × prefetch_bytes` (the decoded records a lane holds
    /// while framing are a separate, smaller budget). An object at or below
    /// this size is read in a single GET.
    ///
    /// Keep this modest. A large window is read as one long-held GET — which
    /// works against the connection release this source relies on — and a
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
            .field("compression", &self.compression)
            .field("split_target_bytes", &self.split_target_bytes)
            .field("refresh_listing", &self.refresh_listing)
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
        parse_url("source.s3.url", &self.url)?;
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
        let yaml = format!("s3:\n{body}");
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        ComponentConfig::new("s3", value["s3"].clone())
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
        assert!(!cfg.refresh_listing);
        assert!(cfg.store.is_empty());
    }

    #[test]
    fn planner_knobs_parse_and_floor() {
        let body = format!(
            "{}  split_target_bytes: 32MB\n  refresh_listing: true\n",
            minimal()
        );
        let cfg = S3SourceConfig::from_component_config(&section(&body)).unwrap();
        assert_eq!(cfg.split_target_bytes, ByteSize::mb(32));
        assert!(cfg.refresh_listing);

        let low = format!("{}  split_target_bytes: 64KB\n", minimal());
        let err = S3SourceConfig::from_component_config(&section(&low)).unwrap_err();
        assert!(err.to_string().contains("split_target_bytes"), "{err}");
    }

    #[test]
    fn removed_knobs_are_rejected() {
        // `lanes`, `checkpoint:`, and a source-level `coordination:` all
        // died with the manifest checkpoint and the assembly-only
        // coordinator wiring; deny_unknown_fields turns a stale config
        // into a load error.
        for extra in [
            "  lanes: 4\n",
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
