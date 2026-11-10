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
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use spate_core::config::{ComponentConfig, ConfigError, Denylist, SectionPath, redact};
use std::borrow::Cow;
use std::collections::BTreeMap;
use url::Url;

use crate::location::{BAD_PREFIX, STORE_URL, offline_store};

/// The source's component type, and the tag its section sits under.
pub(crate) const TAG: &str = "s3";

/// `object_store` option keys the framework owns, refused in the `store`
/// passthrough at load. The store is built from the `url` field, so a
/// passthrough bucket could never take effect; refusing it keeps the
/// configuration honest instead of silently ignoring it.
///
/// Keys compare by [`store_key`], so the one entry refuses every spelling and
/// case `object_store` accepts for the bucket option.
const DENYLIST: Denylist<'static> = Denylist::new(&[("bucket", "owned by the typed `url` field")])
    .normalized_by(store_key, "object_store option");

/// The name `object_store` gives the S3 option `key` sets, or `key`
/// lowercased when it is not one.
fn store_key(key: &str) -> Cow<'_, str> {
    match s3_store_key(key) {
        Some(option) => Cow::Owned(option.as_ref().to_owned()),
        None => Cow::Owned(key.to_ascii_lowercase()),
    }
}

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
    /// the framer declares a resync delimiter. The lister packs objects read
    /// whole into shared units of at most this many bytes and 16 objects, in
    /// listing order; an object read whole above it is one unit. Part of the
    /// bounded feed job fingerprint: every instance of a bounded job must
    /// agree on it. Default 64 MiB.
    #[serde(default = "default_split_target_bytes")]
    pub split_target_bytes: ByteSize,
    /// Target GET window size for ETag-pinned reads. A lane reads its next
    /// window while it hands on the current one, and queued chunks retain the
    /// window before that, so a lane holds three windows when each spans at
    /// least four `chunk_bytes`, and more when windows are shorter; decoded
    /// records require additional memory. Byte-range boundary windows can add `chunk_bytes`
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
    /// Target size of a single buffered chunk. The next GET starts when a
    /// window's chunks start moving, so read-ahead does not depend on this
    /// size.
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
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] when the section does not deserialize into
    /// this type, or when [`validate`](Self::validate) refuses it.
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError> {
        let cfg: S3SourceConfig = section.deserialize_into()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation, including the passthrough denylist.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] naming the refused key under `source.s3`,
    /// such as `source.s3.chunk_bytes: must not be zero`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let at = SectionPath::source(TAG);
        let url = parse_url(&at, "url", &self.url)?;
        if self.chunk_bytes.as_u64() == 0 {
            return Err(at.invalid("chunk_bytes", "must not be zero"));
        }
        if self.prefetch_bytes.as_u64() < self.chunk_bytes.as_u64() {
            return Err(at.invalid(
                "prefetch_bytes",
                format!(
                    "({}) must be at least chunk_bytes ({})",
                    self.prefetch_bytes, self.chunk_bytes
                ),
            ));
        }
        if self.split_target_bytes.as_u64() < SPLIT_TARGET_FLOOR {
            return Err(at.invalid(
                "split_target_bytes",
                format!("({}) must be at least 1MiB", self.split_target_bytes),
            ));
        }
        DENYLIST
            .check("store", self.store.keys())
            .map_err(|e| at.error(e.to_string()))?;
        // The request URIs `object_store` quotes in its errors start with the
        // endpoint.
        for (key, value) in &self.store {
            if matches!(
                s3_store_key(key),
                Some(AmazonS3ConfigKey::Endpoint | AmazonS3ConfigKey::S3Endpoint)
            ) {
                STORE_URL
                    .check(value)
                    .map_err(|e| at.invalid(&format!("store.\"{key}\""), e.to_string()))?;
            }
        }
        // Only the S3 client's keys are checked: `file://` reads no options.
        if matches!(
            ObjectStoreScheme::parse(&url),
            Ok((ObjectStoreScheme::AmazonS3, _))
        ) && let Some(key) = self.store.keys().find(|k| s3_store_key(k).is_none())
        {
            return Err(at.invalid(
                &format!("store.\"{key}\""),
                "is not an object_store S3 option",
            ));
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
fn parse_url(at: &SectionPath, field: &str, value: &str) -> Result<Url, ConfigError> {
    if value.trim().is_empty() {
        return Err(at.invalid(field, "must not be empty"));
    }
    let url = STORE_URL
        .check(value)
        .map_err(|e| at.invalid(field, e.to_string()))?;
    // The store reads no query, and its errors quote the URL whole.
    if url.query().is_some() {
        return Err(at.invalid(
            field,
            format!(
                "{} carries a query, which the store does not read; set credentials under \
                 the source's `store` or the environment",
                redact::url(value)
            ),
        ));
    }
    // `memory://` builds a fresh empty store on every parse, so the source
    // and any test setup would each see a different store.
    if url.scheme() == "memory" {
        return Err(at.invalid(
            field,
            "memory:// creates a new empty store per component and cannot be shared; use \
             file:// for infrastructure-free runs",
        ));
    }
    // `ObjectStoreScheme::parse` also fails on the path, which the scheme check
    // below would misreport.
    if Path::from_url_path(url.path()).is_err() {
        return Err(at.invalid(field, format!("{} {BAD_PREFIX}", redact::url(value))));
    }
    // The crate enables object_store's `aws` and `fs` features alone, so any
    // other scheme builds no client.
    if !matches!(
        ObjectStoreScheme::parse(&url),
        Ok((ObjectStoreScheme::AmazonS3 | ObjectStoreScheme::Local, _))
    ) {
        return Err(at.invalid(
            field,
            format!(
                "the source reads s3://, s3a://, file://, and https:// on an S3 or R2 \
                 endpoint host; for another S3-compatible store, write \
                 s3://<bucket>/<prefix> with the endpoint under the source's `store`, got {}",
                redact::url(value)
            ),
        ));
    }
    // The scheme check passes every `amazonaws.com` and
    // `r2.cloudflarestorage.com` host, and the client parses only some of
    // them.
    if url.scheme() == "https" && offline_store(&url).is_err() {
        return Err(at.invalid(
            field,
            format!(
                "{} is on a host form the store does not read; write the host as the S3 \
                 source's `url` documentation lists it, or s3://<bucket>/<prefix> with the \
                 region or endpoint under the source's `store`",
                redact::url(value)
            ),
        ));
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

    /// A scheme the crate builds no client for fails at load and names the
    /// field, and every scheme it reads passes. Regression for #1239.
    #[test]
    fn a_scheme_the_source_cannot_read_is_refused_at_load() {
        for url in [
            "gs://bucket/data/",
            "az://container/data/",
            "https://storage.example.com/bucket/data/",
            "ftp://bucket/data/",
        ] {
            let body = format!("  url: {url}\n");
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            let err = err.to_string();
            assert!(err.contains("source.s3.url"), "{url}: {err}");
            assert!(err.contains("file://"), "{url}: {err}");
            assert!(err.contains("`store`"), "{url}: {err}");
        }
        for url in [
            "s3://bucket/data/",
            "s3a://bucket/data/",
            "https://s3.us-east-1.amazonaws.com/bucket/data/",
            "file:///tmp/data/",
        ] {
            let body = format!("  url: {url}\n");
            S3SourceConfig::from_component_config(&section(&body)).expect(url);
        }
    }

    /// An S3 or R2 host the client does not parse fails at load and names
    /// the field, and each host form it parses loads. Regression for #1821.
    #[test]
    fn an_s3_host_the_client_cannot_parse_is_refused_at_load() {
        for url in [
            "https://s3.amazonaws.com/bucket/data/",
            "https://s3-us-west-2.amazonaws.com/bucket/data/",
            "https://bucket.s3-us-west-2.amazonaws.com/data/",
            "https://sqs.us-east-1.amazonaws.com/bucket/data/",
            "https://r2.cloudflarestorage.com/bucket/data/",
        ] {
            let body = format!("  url: {url}\n");
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            let err = err.to_string();
            assert!(err.starts_with("source.s3.url"), "{url}: {err}");
            assert!(
                err.contains("host form the store does not read"),
                "{url}: {err}"
            );
            assert!(err.contains(url), "{url}: {err}");
        }
        for url in [
            "https://s3.us-east-1.amazonaws.com/bucket/data/",
            "https://s3.dualstack.us-east-1.amazonaws.com/bucket/data/",
            "https://bucket.s3.amazonaws.com/data/",
            "https://bucket.s3.us-east-1.amazonaws.com/data/",
            "https://bucket.s3.dualstack.us-east-1.amazonaws.com/data/",
            "https://account.r2.cloudflarestorage.com/bucket/data/",
            "https://account.eu.r2.cloudflarestorage.com/bucket/data/",
        ] {
            let body = format!("  url: {url}\n");
            S3SourceConfig::from_component_config(&section(&body)).expect(url);
        }
    }

    /// Pins that a refused URL's userinfo and signed query stay out of the
    /// load error.
    #[test]
    fn a_refused_url_keeps_its_credentials_out_of_the_error() {
        for url in [
            "https://urluser:urlsecret@storage.example.com/bucket/data/",
            "https://acct.blob.core.windows.net/container/data/?sig=urlsecret",
            "s3://urluser:urlsecret@bucket/a//b/",
        ] {
            let body = format!("  url: {url}\n");
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            let err = err.to_string();
            assert!(err.contains("source.s3.url"), "{err}");
            assert!(!err.contains("urlsecret"), "{err}");
        }
    }

    /// Userinfo or a query in the URL, and userinfo in the store endpoint
    /// under any spelling of its key, fail at load without the password in
    /// the error. Regression for #1739.
    #[test]
    fn credentials_in_the_url_or_endpoint_are_refused_at_load() {
        for url in [
            "s3://u:urlsecret@bucket/data/",
            "s3://urlsecret@bucket/data/",
            "s3://bucket/data/?token=urlsecret",
        ] {
            let body = format!("  url: \"{url}\"\n");
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            let err = err.to_string();
            assert!(err.contains("source.s3.url"), "{url}: {err}");
            assert!(err.contains("`store`"), "{url}: {err}");
            assert!(!err.contains("urlsecret"), "{url}: {err}");
        }
        for key in [
            "endpoint",
            "aws_endpoint",
            "AWS_ENDPOINT_URL",
            "aws_endpoint_url_s3",
        ] {
            let body = format!(
                "  url: s3://bucket/data/\n  store:\n    {key}: \"http://u:urlsecret@127.0.0.1:9000\"\n"
            );
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            let err = err.to_string();
            assert!(
                err.contains(&format!("source.s3.store.\"{key}\"")),
                "{key}: {err}"
            );
            assert!(!err.contains("urlsecret"), "{key}: {err}");
        }
        let body = "  url: s3://bucket/data/\n  store:\n    endpoint: \"http://127.0.0.1:9000\"\n";
        S3SourceConfig::from_component_config(&section(body)).unwrap();
    }

    /// Pins that an S3 URL with a bad prefix is told about the prefix, as
    /// `ObjectStoreScheme::parse` fails on it as it does on a scheme.
    #[test]
    fn a_bad_prefix_is_not_reported_as_a_wrong_scheme() {
        let body = "  url: s3://bucket/exports//2026/\n";
        let err = S3SourceConfig::from_component_config(&section(body)).unwrap_err();
        let err = err.to_string();
        assert!(err.contains(BAD_PREFIX), "{err}");
        assert!(err.contains("source.s3.url"), "{err}");
    }

    /// The text of a refusal, which must be a [`ConfigError::Component`].
    fn refusal<T: std::fmt::Debug>(result: Result<T, ConfigError>) -> String {
        let err = result.expect_err("must be refused");
        assert!(matches!(err, ConfigError::Component { .. }), "{err:?}");
        err.to_string()
    }

    /// Each rule names the key it refuses under `source.s3`.
    #[test]
    fn each_rule_names_the_key_it_refuses() {
        let scheme = "the source reads s3://, s3a://, file://, and https:// on an S3 or R2 \
                      endpoint host; for another S3-compatible store, write \
                      s3://<bucket>/<prefix> with the endpoint under the source's `store`, \
                      got gs://bucket/data/";
        let cases = [
            (
                "  url: \"\"\n".to_owned(),
                "url: must not be empty".to_owned(),
            ),
            (
                "  url: data\n".to_owned(),
                "url: is not a URL: relative URL without a base".to_owned(),
            ),
            (
                "  url: \"s3://bucket/data/?token=t\"\n".to_owned(),
                format!(
                    "url: {} carries a query, which the store does not read; set credentials \
                     under the source's `store` or the environment",
                    redact::url("s3://bucket/data/?token=t")
                ),
            ),
            (
                "  url: \"s3://u:p@bucket/data/\"\n".to_owned(),
                "url: carries userinfo; the store does not read it; set credentials under the \
                 source's `store` or the environment"
                    .to_owned(),
            ),
            (
                "  url: memory:///data/\n".to_owned(),
                "url: memory:// creates a new empty store per component and cannot be \
                 shared; use file:// for infrastructure-free runs"
                    .to_owned(),
            ),
            (
                "  url: s3://bucket/a//b/\n".to_owned(),
                format!("url: s3://bucket/a//b/ {BAD_PREFIX}"),
            ),
            (
                "  url: gs://bucket/data/\n".to_owned(),
                format!("url: {scheme}"),
            ),
            (
                format!("{}  chunk_bytes: 0\n", minimal()),
                "chunk_bytes: must not be zero".to_owned(),
            ),
            (
                format!("{}  chunk_bytes: 2MiB\n  prefetch_bytes: 1MiB\n", minimal()),
                "prefetch_bytes: (1.0 MiB) must be at least chunk_bytes (2.0 MiB)".to_owned(),
            ),
            (
                format!("{}  split_target_bytes: 64KiB\n", minimal()),
                "split_target_bytes: (64.0 KiB) must be at least 1MiB".to_owned(),
            ),
            (
                format!(
                    "{}  store:\n    endpoint: \"http://u:p@127.0.0.1:9000\"\n",
                    minimal()
                ),
                "store.\"endpoint\": carries userinfo; the store does not read it; set \
                 credentials under the source's `store` or the environment"
                    .to_owned(),
            ),
            (
                format!(
                    "{}  store:\n    endpoint: \"http://u:p@exa mple:9000\"\n",
                    minimal()
                ),
                "store.\"endpoint\": is not a URL: invalid international domain name".to_owned(),
            ),
            (
                format!(
                    "{}  store:\n    endpoint: \"u:p@127.0.0.1:9000\"\n",
                    minimal()
                ),
                "store.\"endpoint\": has no `//` after its scheme".to_owned(),
            ),
            (
                format!(
                    "{}  store:\n    aws_endpoint_url_s3: \"u:p@127.0.0.1:9000\"\n",
                    minimal()
                ),
                "store.\"aws_endpoint_url_s3\": has no `//` after its scheme".to_owned(),
            ),
            (
                format!(
                    "{}  store:\n    endpont: http://127.0.0.1:9000\n",
                    minimal()
                ),
                "store.\"endpont\": is not an object_store S3 option".to_owned(),
            ),
        ];
        for (body, want) in cases {
            let msg = refusal(S3SourceConfig::from_component_config(&section(&body)));
            assert_eq!(msg, format!("source.s3.{want}"), "{body}");
        }
    }

    /// A section loaded from a pipeline file reports at the path
    /// [`S3SourceConfig::validate`] gives a config built in code.
    #[test]
    fn a_loaded_section_and_a_built_config_name_the_same_path() {
        let pipeline = spate_core::config::PipelineConfig::from_str(
            "pipeline: { name: demo }\nsource:\n  s3:\n    url: s3://bucket/exports/\n    \
             chunk_bytes: 0\nsink: { memory: {} }\n",
        )
        .expect("loads");
        let loaded = refusal(S3SourceConfig::from_component_config(&pipeline.source));
        let mut built = S3SourceConfig::new("s3://bucket/exports/");
        built.chunk_bytes = ByteSize::b(0);
        assert_eq!(loaded, refusal(built.validate()));
        assert_eq!(loaded, "source.s3.chunk_bytes: must not be zero");
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

    /// A bucket key written another way names `bucket`, the option it sets.
    #[test]
    fn a_bucket_alias_names_the_option_it_sets() {
        for key in ["Bucket", "aws_bucket_name"] {
            let body = format!("{}  store:\n    {key}: other\n", minimal());
            let err = S3SourceConfig::from_component_config(&section(&body)).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!(
                    "source.s3: store.\"{key}\" cannot be overridden: \
                     owned by the typed `url` field (`{key}` sets the same object_store \
                     option as `bucket`)"
                )
            );
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
            // An endpoint key is checked as a URL.
            let value = if key.to_ascii_lowercase().contains("endpoint") {
                "http://127.0.0.1:9000"
            } else {
                "x"
            };
            let body = format!("{}  store:\n    {key}: {value}\n", minimal());
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
