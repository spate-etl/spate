//! Opaque-section configuration and the sink factory.
//!
//! The `sink: { clickhouse: { ... } }` YAML section deserializes into
//! [`ClickHouseSinkConfig`];
//! [`ClickHouseSinkBuilder::from_component_config`] validates it and produces
//! a [`ClickHouseSinkBuilder`] holding per-shard replica endpoints.
//! [`ClickHouseSinkBuilder::connect`] supplies
//! the row type and produces the runnable [`ClickHouseSink`], including its
//! writer.

use crate::codec::{BlockCodec, BlockMethod};
use crate::distributed::{self, DistributedCheckError};
use crate::encoder::ClickHouseFormatEncoder;
use crate::http::StartupBound;
use crate::native::NativeSchema;
use crate::router::{ClickHouseDistributedRouter, KeyExtractor};
use crate::schema::{self, RowSchema, SchemaError};
use crate::writer::{ClickHouseEndpoint, ClickHouseWriter};
use bytes::Bytes;
use rustls::ClientConfig;
use serde::{Deserialize, Deserializer, de};
use spate_core::config::{ComponentConfig, ConfigError, SectionPath, UrlRule, redact};
use spate_core::deser::RecFamily;
use spate_core::sink::{
    FrameCodecFactory, SinkBundle, SinkParts, SinkPoolConfig, SinkProbeFn, endpoint_probe,
};
use spate_core::tls::{SystemRoots, TlsSection};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

/// The sink's component type key, under `sink:` or a `sinks:` entry.
pub(crate) const TAG: &str = "clickhouse";

/// The `clickhouse` sink section.
///
/// ```yaml
/// sink:
///   clickhouse:
///     table: orders_local            # or db.orders_local
///     shards:
///       - replicas: ["https://ch-0-0:8443", "https://ch-0-1:8443"]
///       - replicas: ["https://ch-1-0:8443", "https://ch-1-1:8443"]
///     user: default
///     password: ${CLICKHOUSE_PASSWORD}
///     timeouts: { send: 30s, end: 180s, startup: 30s }
///     compression: lz4               # none | lz4 | zstd | zstd:<1-22>
///     settings: { insert_quorum: "auto" }   # extra per-insert settings
/// ```
///
/// Construct with [`ClickHouseSinkConfig::new`] and set the optional
/// fields. The struct is `#[non_exhaustive]` so new knobs can be added
/// without breaking callers.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ClickHouseSinkConfig {
    /// Target table, optionally `database.table`-qualified.
    pub table: String,
    /// Shard topology: one entry per shard, each with its replica URLs.
    /// Writes go directly to shard-local tables; replicas of a shard are
    /// rotated per batch.
    pub shards: Vec<ClickHouseShardSection>,
    /// Default database for unqualified tables.
    #[serde(default)]
    pub database: Option<String>,
    /// Username (interpolate secrets upstream via `${VAR}`).
    #[serde(default)]
    pub user: Option<String>,
    /// Password (interpolate secrets upstream via `${VAR}`).
    #[serde(default)]
    pub password: Option<String>,
    /// Trust settings: `tls.root_ca`, which requires at least one `https://`
    /// URL. The URL scheme decides whether a connection uses TLS.
    #[serde(default)]
    pub tls: TlsSection,
    /// Extra per-insert ClickHouse settings (beyond the deduplication
    /// settings this sink always sets).
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
    /// Client-side timeouts for one insert, and for one startup request.
    #[serde(default)]
    pub timeouts: TimeoutSection,
    /// Transport compression of insert bodies. `lz4` by
    /// default (see [`ClickHouseCompression`]).
    #[serde(default)]
    pub compression: ClickHouseCompression,
    /// Insert wire format (see [`ClickHouseFormat`]). `rowbinary` by default;
    /// `native` selects the columnar block format.
    #[serde(default)]
    pub format: ClickHouseFormat,
    /// Opt-in startup parity check against the cluster topology and a
    /// `Distributed` table's DDL (see [`ClickHouseDistributedCheckSection`]).
    /// Absent by default: no queries issued.
    #[serde(default)]
    pub distributed_check: Option<ClickHouseDistributedCheckSection>,
}

// Hand-written: `password` is a credential, and a `settings` value can be one.
// ClickHouse reads `user` and `password` URL parameters as a login, and
// `format_avro_schema_registry_url` can carry userinfo. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl std::fmt::Debug for ClickHouseSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ClickHouseSinkConfig {
            table,
            shards,
            database,
            user,
            password,
            tls,
            settings,
            timeouts,
            compression,
            format,
            distributed_check,
        } = self;
        f.debug_struct("ClickHouseSinkConfig")
            .field("table", table)
            .field("shards", shards)
            .field("database", database)
            .field("user", user)
            .field("password", &redact::option(password))
            .field("tls", tls)
            .field("settings", &redact::map(settings.keys()))
            .field("timeouts", timeouts)
            .field("compression", compression)
            .field("format", format)
            .field("distributed_check", distributed_check)
            .finish()
    }
}

/// The `INSERT` wire format.
///
/// ```yaml
/// sink:
///   clickhouse:
///     format: native   # rowbinary | native
/// ```
///
/// `rowbinary` (default) streams rows; `native` transposes each chunk into a
/// columnar block. Both are self-describing on the wire: `rowbinary` sends
/// `FORMAT RowBinaryWithNamesAndTypes` with a header naming each column and
/// its type, and a Native block carries the same per column, so the server
/// checks the schema on every insert rather than mapping bytes by position.
/// [`ClickHouseSink::encoder`] hands out the encoder that writes the
/// configured format.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ClickHouseFormat {
    /// Row-wise RowBinary (default).
    // Spelled as one word, as ClickHouse names the format.
    #[serde(rename = "rowbinary")]
    #[default]
    RowBinary,
    /// Columnar Native (self-describing blocks).
    Native,
}

impl ClickHouseFormat {
    /// The `FORMAT` keyword for the `INSERT` statement.
    fn keyword(self) -> &'static str {
        match self {
            ClickHouseFormat::RowBinary => "RowBinaryWithNamesAndTypes",
            ClickHouseFormat::Native => "Native",
        }
    }
}

/// Transport compression of insert bodies.
///
/// This is wire-level compression, unrelated to on-disk column `CODEC`s
/// declared in table DDL, which stay the caller's responsibility.
/// Deserialized from a scalar string:
///
/// ```yaml
/// compression: lz4         # none | lz4 | zstd | zstd:<1-22>
/// ```
///
/// `lz4` is fast and low-CPU (the default); `zstd`
/// trades CPU for a better ratio and accepts an explicit level (`zstd` alone
/// uses level 3). `none` disables compression.
///
/// Insert bodies are compressed on the pipeline threads: the sink's frame
/// codec ([`ClickHouseSink::frame_codec`]) turns each chunk into one
/// compressed block when the chunk seals, reusing one zstd context per
/// pipeline thread, and the writer sends the blocks with `decompress=1`. A
/// retry resends the stored blocks. Responses to the sink's own queries
/// arrive uncompressed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClickHouseCompression {
    /// No transport compression.
    None,
    /// LZ4 (default): fast, low CPU, moderate ratio.
    #[default]
    Lz4,
    /// ZSTD at the given level (`1..=22`): higher ratio, more CPU.
    Zstd(i32),
}

/// Default ZSTD level, matching the `clickhouse`/`zstd` crate default.
const ZSTD_DEFAULT_LEVEL: i32 = 3;

/// The ZSTD levels the sink accepts, from YAML and from a config built in code.
const ZSTD_LEVELS: std::ops::RangeInclusive<i32> = 1..=22;

impl FromStr for ClickHouseCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(ClickHouseCompression::None),
            "lz4" => Ok(ClickHouseCompression::Lz4),
            "zstd" => Ok(ClickHouseCompression::Zstd(ZSTD_DEFAULT_LEVEL)),
            other => {
                let (lo, hi) = (ZSTD_LEVELS.start(), ZSTD_LEVELS.end());
                let raw = other.strip_prefix("zstd:").ok_or_else(|| {
                    format!("unknown compression: expected none, lz4, zstd, or zstd:<{lo}-{hi}>")
                })?;
                let level: i32 = raw.parse().map_err(|_| {
                    format!("invalid zstd level: expected an integer in [{lo}, {hi}]")
                })?;
                if !ZSTD_LEVELS.contains(&level) {
                    return Err(format!("zstd level must be in [{lo}, {hi}]"));
                }
                Ok(ClickHouseCompression::Zstd(level))
            }
        }
    }
}

impl<'de> Deserialize<'de> for ClickHouseCompression {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // A scalar with an embedded level (`zstd:9`), so parse from the string
        // rather than deriving a tagged enum; serde attaches the field path.
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(de::Error::custom)
    }
}

impl ClickHouseSinkConfig {
    /// A config for `table` and the shard topology. Every other field
    /// starts at its YAML default.
    #[must_use]
    pub fn new(
        table: impl Into<String>,
        shards: Vec<ClickHouseShardSection>,
    ) -> ClickHouseSinkConfig {
        ClickHouseSinkConfig {
            table: table.into(),
            shards,
            database: None,
            user: None,
            password: None,
            tls: TlsSection::default(),
            settings: BTreeMap::new(),
            timeouts: TimeoutSection::default(),
            compression: ClickHouseCompression::default(),
            format: ClickHouseFormat::default(),
            distributed_check: None,
        }
    }

    /// Deserialize and validate from the pipeline's opaque component section.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] when the section does not deserialize into
    /// this type, or when [`validate`](Self::validate) refuses it. Every error
    /// starts at the section's [`path`](ComponentConfig::path), such as
    /// `sinks.eu.clickhouse` for a `sinks:` entry named `eu`, or `clickhouse`
    /// for a section built with [`ComponentConfig::new`] and never placed in a
    /// pipeline config.
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError> {
        let cfg: ClickHouseSinkConfig = section.deserialize_into()?;
        cfg.validate_at(&section.path())?;
        Ok(cfg)
    }

    /// Checks the configuration rules: the shard topology, each replica URL's
    /// scheme and credentials, identifiers, the compression level, reserved
    /// settings, `distributed_check`, and a `tls.root_ca` set with no
    /// `https://` URL to use it. It reads no file and builds no client, so a
    /// `tls.root_ca` file that cannot be loaded is refused by
    /// [`ClickHouseSinkBuilder::new`].
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] for the first setting it refuses, starting
    /// at `sink.clickhouse`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_at(&SectionPath::sink(TAG))
    }

    /// [`validate`](Self::validate), with each error starting at `at`.
    fn validate_at(&self, at: &SectionPath) -> Result<(), ConfigError> {
        if self.shards.is_empty() {
            return Err(at.invalid("shards", "at least one shard is required"));
        }
        for (i, shard) in self.shards.iter().enumerate() {
            if shard.replicas.is_empty() {
                return Err(at.invalid(&format!("shards[{i}].replicas"), "must not be empty"));
            }
            for (j, url) in shard.replicas.iter().enumerate() {
                ENDPOINT.check(url).map_err(|e| {
                    at.invalid(&format!("shards[{i}].replicas[{j}]"), e.to_string())
                })?;
            }
            // Weights are ClickHouse interval widths: a zero-weight shard
            // receives nothing under Distributed parity and breaks the
            // prefix-sum selection.
            if shard.weight == 0 {
                return Err(at.invalid(&format!("shards[{i}].weight"), "must be at least 1"));
            }
        }
        let table_parts: Vec<&str> = self.table.split('.').collect();
        if self.table.is_empty()
            || table_parts.len() > 2
            || !table_parts.iter().all(|p| is_identifier(p))
        {
            return Err(at.invalid(
                "table",
                format!(
                    "`{}` is not a valid (optionally database-qualified) identifier",
                    self.table
                ),
            ));
        }
        if let Some(db) = &self.database
            && !is_identifier(db)
        {
            return Err(at.invalid("database", format!("`{db}` is not a valid identifier")));
        }

        // Compression: the string parser bounds the level, and a config built
        // in code can hold `Zstd(level)` directly. The same bound here makes a
        // config built in code accept the levels YAML accepts.
        if let ClickHouseCompression::Zstd(level) = self.compression
            && !ZSTD_LEVELS.contains(&level)
        {
            return Err(at.invalid(
                "compression",
                format!(
                    "zstd level must be in [{}, {}] (got {level})",
                    ZSTD_LEVELS.start(),
                    ZSTD_LEVELS.end()
                ),
            ));
        }

        if let Err(e) = crate::writer::RESERVED.check("settings", self.settings.keys()) {
            return Err(at.error(e.to_string()));
        }

        if let Some(check) = &self.distributed_check {
            if !is_identifier(&check.cluster) {
                return Err(at.invalid(
                    "distributed_check.cluster",
                    format!("`{}` is not a valid identifier", check.cluster),
                ));
            }
            let parts: Vec<&str> = check.table.split('.').collect();
            if check.table.is_empty() || parts.len() > 2 || !parts.iter().all(|p| is_identifier(p))
            {
                return Err(at.invalid(
                    "distributed_check.table",
                    format!(
                        "`{}` is not a valid (optionally database-qualified) identifier",
                        check.table
                    ),
                ));
            }
            match (&check.sharding_key, &check.sharding_expr) {
                (Some(_), Some(_)) | (None, None) => {
                    return Err(at.invalid(
                        "distributed_check",
                        "set exactly one of sharding_key or sharding_expr",
                    ));
                }
                (Some(key), None) if !is_identifier(key) => {
                    return Err(at.invalid(
                        "distributed_check.sharding_key",
                        format!("`{key}` is not a valid identifier"),
                    ));
                }
                (None, Some(expr)) if expr.trim().is_empty() => {
                    return Err(at.invalid("distributed_check.sharding_expr", "must not be empty"));
                }
                _ => {}
            }
            if let Some(url) = &check.endpoint {
                ENDPOINT
                    .check(url)
                    .map_err(|e| at.invalid("distributed_check.endpoint", e.to_string()))?;
            }
        }
        if self.tls.root_ca.is_some() && !Schemes::of(self).https {
            return Err(at.invalid(
                "tls.root_ca",
                "is set, but no replica or distributed_check.endpoint is https://",
            ));
        }
        Ok(())
    }
}

/// One shard's replica endpoints.
///
/// Construct with [`ClickHouseShardSection::new`] and set the optional fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ClickHouseShardSection {
    /// HTTP(S) URLs of this shard's replicas. A URL with credentials is
    /// rejected; set them in `user` and `password`.
    pub replicas: Vec<String>,
    /// Distributed-parity weight: must equal this shard's `<weight>` in
    /// the cluster's `remote_servers` entry (ClickHouse's default is 1).
    /// Consumed by [`ClickHouseSink::router`]; irrelevant otherwise.
    #[serde(default = "default_weight")]
    pub weight: u32,
}

// Hand-written: replica URLs can carry credentials. The destructure lists every
// field so a new one cannot reach `Debug` unredacted.
impl std::fmt::Debug for ClickHouseShardSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ClickHouseShardSection { replicas, weight } = self;
        f.debug_struct("ClickHouseShardSection")
            .field(
                "replicas",
                &replicas.iter().map(|r| redact::url(r)).collect::<Vec<_>>(),
            )
            .field("weight", weight)
            .finish()
    }
}

fn default_weight() -> u32 {
    1
}

impl ClickHouseShardSection {
    /// A shard served by `replicas`. The weight starts at its YAML
    /// default.
    #[must_use]
    pub fn new(replicas: Vec<String>) -> ClickHouseShardSection {
        ClickHouseShardSection {
            replicas,
            weight: default_weight(),
        }
    }
}

/// The opt-in `distributed_check:` block, a startup guard verifying that
/// the sink config, the cluster topology, and the `Distributed` table's
/// DDL agree (see [`ClickHouseSink::validate_distributed`]).
///
/// ```yaml
/// sink:
///   clickhouse:
///     distributed_check:
///       cluster: storefront
///       table: analytics.order_lines_dist  # db-qualified, or bare like `table`
///       sharding_key: order_id             # expected DDL = xxHash64(order_id)
///       # sharding_expr: "xxHash64(order_id)"  # escape hatch — exactly one
///       # endpoint: "http://ch-front:8123"     # default: shard 0, replica 0
/// ```
///
/// Construct with [`ClickHouseDistributedCheckSection::new`] and set the optional
/// fields. The struct is `#[non_exhaustive]` so new knobs can be added
/// without breaking callers.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ClickHouseDistributedCheckSection {
    /// The cluster the `Distributed` table is defined over.
    pub cluster: String,
    /// The `Distributed` table to check, optionally `db.table`-qualified
    /// (an unqualified name resolves like the sink `table`).
    pub table: String,
    /// The sharding key column; the expected DDL expression becomes
    /// `xxHash64(<sharding_key>)`. Exactly one of this or `sharding_expr`.
    #[serde(default)]
    pub sharding_key: Option<String>,
    /// Escape hatch: the full expected sharding expression, compared
    /// textually after normalization (whitespace and identifier quoting
    /// stripped), which is more brittle than `sharding_key`.
    #[serde(default)]
    pub sharding_expr: Option<String>,
    /// Endpoint to query for `system.clusters` / `system.tables`. The
    /// `Distributed` table may live on a front node outside the `shards:`
    /// list; defaults to the first replica of shard 0. Rejected like a replica
    /// URL when it carries credentials.
    #[serde(default)]
    pub endpoint: Option<String>,
}

// Hand-written: the endpoint URL can carry credentials. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl std::fmt::Debug for ClickHouseDistributedCheckSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ClickHouseDistributedCheckSection {
            cluster,
            table,
            sharding_key,
            sharding_expr,
            endpoint,
        } = self;
        f.debug_struct("ClickHouseDistributedCheckSection")
            .field("cluster", cluster)
            .field("table", table)
            .field("sharding_key", sharding_key)
            .field("sharding_expr", sharding_expr)
            .field("endpoint", &endpoint.as_deref().map(redact::url))
            .finish()
    }
}

impl ClickHouseDistributedCheckSection {
    /// A check of `table` on `cluster`, sharded by `sharding_key`. Every
    /// other field starts at its YAML default.
    ///
    /// To compare against a full expression instead, clear `sharding_key`
    /// and set `sharding_expr`: [`ClickHouseSinkBuilder::new`] takes exactly one
    /// of the two.
    #[must_use]
    pub fn new(
        cluster: impl Into<String>,
        table: impl Into<String>,
        sharding_key: impl Into<String>,
    ) -> ClickHouseDistributedCheckSection {
        ClickHouseDistributedCheckSection {
            cluster: cluster.into(),
            table: table.into(),
            sharding_key: Some(sharding_key.into()),
            sharding_expr: None,
            endpoint: None,
        }
    }
}

/// Client-side timeouts for one insert, and for one startup request.
///
/// Construct with [`TimeoutSection::default`] and set the fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct TimeoutSection {
    /// Per-`send` timeout (one frame reaching the socket).
    #[serde(with = "humantime_serde")]
    pub send: Option<Duration>,
    /// `end` timeout: the server fully processing the insert,
    /// materialized views included.
    #[serde(with = "humantime_serde")]
    pub end: Option<Duration>,
    /// Timeout of each request the sink sends a replica while it is built:
    /// the schema read, the settings check and the `distributed_check`
    /// queries.
    #[serde(with = "humantime_serde")]
    pub startup: Option<Duration>,
}

impl Default for TimeoutSection {
    fn default() -> Self {
        TimeoutSection {
            send: Some(Duration::from_secs(30)),
            end: Some(Duration::from_secs(180)),
            startup: Some(Duration::from_secs(30)),
        }
    }
}

/// A validated sink configuration with no row type yet.
///
/// The sink is built in two steps because the configuration alone cannot
/// finish it. Its `INSERT` names the row type's columns, and its writer
/// describes each row with the types read from every replica, so
/// [`connect`](Self::connect) takes the row type and reads the table's schema
/// before it produces the runnable [`ClickHouseSink`].
#[derive(Debug)]
#[must_use = "await connect::<F>() to get a runnable ClickHouseSink"]
pub struct ClickHouseSinkBuilder {
    cfg: ClickHouseSinkConfig,
    endpoints: Vec<Vec<ClickHouseEndpoint>>,
    probe_endpoints: Arc<Vec<Vec<ClickHouseEndpoint>>>,
    shard_weights: Arc<[u32]>,
    distributed: Option<distributed::DistributedCheck>,
    startup: StartupBound,
}

impl ClickHouseSinkBuilder {
    /// A builder from a config made in code.
    ///
    /// Reads the system trust store and `tls.root_ca` when any replica or
    /// `distributed_check.endpoint` is `https://`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spate_clickhouse::{ClickHouseSinkBuilder, ClickHouseSinkConfig, config::ClickHouseShardSection};
    ///
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let shards = vec![ClickHouseShardSection::new(vec!["http://ch-0-0:8123".into()])];
    /// let builder = ClickHouseSinkBuilder::new(ClickHouseSinkConfig::new("orders_local", shards))?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`], starting at `sink.clickhouse`, when `cfg`
    /// breaks a configuration rule, when `tls.root_ca` cannot be loaded, or
    /// when a URL is `https://` and neither the system trust store nor
    /// `tls.root_ca` holds a usable certificate.
    pub fn new(cfg: ClickHouseSinkConfig) -> Result<ClickHouseSinkBuilder, ConfigError> {
        build_with(cfg, &SectionPath::sink(TAG), SystemRoots::load)
    }

    /// A builder from the opaque `sink: { clickhouse: ... }` component section.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spate_clickhouse::ClickHouseSinkBuilder;
    /// use spate_core::config::ComponentConfig;
    ///
    /// # fn run(section: &ComponentConfig) -> Result<(), Box<dyn std::error::Error>> {
    /// let builder = ClickHouseSinkBuilder::from_component_config(section)?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// As [`ClickHouseSinkConfig::from_component_config`], and otherwise as
    /// [`new`](Self::new). Every error starts at the section's
    /// [`path`](ComponentConfig::path).
    pub fn from_component_config(
        section: &ComponentConfig,
    ) -> Result<ClickHouseSinkBuilder, ConfigError> {
        build_with(
            ClickHouseSinkConfig::from_component_config(section)?,
            &section.path(),
            SystemRoots::load,
        )
    }

    /// Supplies the row type and fetches the table's schema, producing the
    /// runnable sink.
    ///
    /// The `INSERT` column list comes from
    /// [`ClickHouseRowFamily::COLUMNS`](crate::ClickHouseRowFamily::COLUMNS).
    /// `system.columns` is read from every replica of every shard and checked
    /// against that list, failing with a readable diff for a missing or
    /// non-insertable column, replica drift, or a missing table. The fetched
    /// types describe the rows on the wire in both formats, so there is no
    /// sink without them. Every replica is then sent `deduplicate_insert` and
    /// the `settings` on a `SELECT 1`, and a setting one refuses fails the
    /// build with [`SchemaError::Settings`] and the server's message. That
    /// covers a constraint pinning `deduplicate_insert` and a server before
    /// 26.2, which does not know it.
    ///
    /// Run it on the builder's I/O runtime before the chain exists, via
    /// `pipeline.block_on`. `F` is not inferable from context, so name it:
    /// `builder.connect::<Owned<OrderRow>>()`.
    ///
    /// # Errors
    ///
    /// [`SchemaError::Columns`] when `F::COLUMNS` cannot produce an `INSERT`
    /// or its header cannot be compressed, [`SchemaError::Fetch`] when a
    /// replica's columns cannot be read, [`SchemaError::Mismatch`] when the
    /// table is missing, the replicas disagree, a declared column does not
    /// fit it, or, under `format: native`, a declared column has a type the
    /// Native format does not support, and [`SchemaError::Settings`] when a
    /// replica answers the
    /// settings check with an error. A replica that gives a request no
    /// answer within `timeouts.startup` fails that request's variant,
    /// [`SchemaError::Fetch`] or [`SchemaError::Settings`].
    pub async fn connect<F: crate::ClickHouseRowFamily>(
        self,
    ) -> Result<ClickHouseSink, SchemaError> {
        validate_columns(F::COLUMNS)
            .map_err(|why| SchemaError::Columns(format!("sink.clickhouse: {why}")))?;

        let check = schema::SchemaCheck {
            wire: match self.cfg.format {
                ClickHouseFormat::RowBinary => schema::Wire::RowBinary,
                ClickHouseFormat::Native => schema::Wire::Native,
            },
            database: self.cfg.database.clone(),
            table: self.cfg.table.clone(),
            columns: F::COLUMNS,
        };
        let schema = schema::validate(&check, &self.endpoints, &self.startup).await?;
        let encoding = match self.cfg.format {
            ClickHouseFormat::RowBinary => Encoding::RowBinary(Arc::clone(&schema)),
            ClickHouseFormat::Native => Encoding::Native(
                NativeSchema::from_row_schema(&schema)
                    .expect("schema validation refuses a column Native cannot lay out"),
            ),
        };

        let insert_sql = insert_statement(&self.cfg.table, F::COLUMNS, self.cfg.format);
        let frame_codec = BlockCodec::factory(self.cfg.compression);
        // A Native block names its own columns and types; a RowBinary body
        // carries them once, ahead of the rows, in the same blocks as the rows.
        let header = match (self.cfg.format, &frame_codec) {
            (ClickHouseFormat::Native, _) => None,
            (ClickHouseFormat::RowBinary, None) => Some(schema.header()),
            (ClickHouseFormat::RowBinary, Some(codec)) => {
                let block = codec
                    .make()
                    .encode_frame(&schema.header())
                    .map(Bytes::copy_from_slice)
                    .map_err(|e| SchemaError::Columns(format!("sink.clickhouse: {e}")))?;
                Some(block)
            }
        };
        let writer = ClickHouseWriter::new(
            insert_sql,
            header,
            BlockMethod::of(self.cfg.compression),
            self.cfg.settings.clone().into_iter().collect(),
            self.cfg.timeouts.send,
            self.cfg.timeouts.end,
        );
        for endpoint in self.endpoints.iter().flatten() {
            self.startup
                .request(writer.check_settings(endpoint))
                .await
                .map_err(|reason| SchemaError::Settings {
                    url: endpoint.display_url().to_string(),
                    reason,
                })?;
        }
        tracing::info!(
            table = %self.cfg.table,
            columns = ?F::COLUMNS,
            "clickhouse insert columns",
        );

        Ok(ClickHouseSink {
            writer,
            endpoints: self.endpoints,
            frame_codec,
            encoding,
            probe_endpoints: self.probe_endpoints,
            shard_weights: self.shard_weights,
            distributed: self.distributed,
        })
    }

    /// A [`ClickHouseDistributedRouter`] over the configured shard topology,
    /// before the schema fetch. Same placement as [`ClickHouseSink::router`],
    /// which is the one to use once the sink exists.
    ///
    /// `F` is not inferable from the extractor fn item, so name it:
    /// `builder.router::<Owned<OrderLineRow>>(order_key)`.
    #[must_use]
    pub fn router<F: RecFamily>(&self, extract: KeyExtractor<F>) -> ClickHouseDistributedRouter<F> {
        ClickHouseDistributedRouter::new(extract, &self.shard_weights)
            .expect("config validation guarantees at least one shard and weights >= 1")
    }
}

/// Everything the framework needs to run this sink.
#[derive(Debug)]
pub struct ClickHouseSink {
    /// The `ShardWriter` implementation.
    pub writer: ClickHouseWriter,
    /// Per-shard replica endpoints, `shards[i][j]` = shard `i`, replica `j`.
    pub endpoints: Vec<Vec<ClickHouseEndpoint>>,
    /// Codes each chunk at seal under `lz4` or `zstd`; `None` under
    /// `compression: none`.
    frame_codec: Option<FrameCodecFactory>,
    /// The configured wire format, with the live table's columns fetched at
    /// `connect`.
    encoding: Encoding,
    /// An independent client set for readiness probing: sharing the insert
    /// clients would report the write path healthy because probing keeps
    /// its connections warm.
    probe_endpoints: Arc<Vec<Vec<ClickHouseEndpoint>>>,
    /// Per-shard weights in config order, for [`router`](Self::router).
    shard_weights: Arc<[u32]>,
    /// The captured `distributed_check` block, if configured.
    distributed: Option<distributed::DistributedCheck>,
}

/// What the sink's encoders are built from, fixed at `connect`.
#[derive(Debug)]
enum Encoding {
    RowBinary(Arc<RowSchema>),
    Native(Arc<NativeSchema>),
}

impl ClickHouseSink {
    /// The configured insert wire format.
    #[must_use]
    pub fn format(&self) -> ClickHouseFormat {
        match self.encoding {
            Encoding::RowBinary(_) => ClickHouseFormat::RowBinary,
            Encoding::Native(_) => ClickHouseFormat::Native,
        }
    }

    /// The encoder for this sink's chain. It writes the configured `format`
    /// and checks each pipeline thread's first record against the table's
    /// columns as [`connect`](ClickHouseSinkBuilder::connect) fetched them.
    ///
    /// Take each sink's encoder from that sink: a chain whose encoder came
    /// from a sink with another `format` sends bodies its writer cannot
    /// insert.
    ///
    /// `F` is not inferable where the encoder is passed to the chain, so
    /// name it: `sink.encoder::<Owned<OrderRow>>()`.
    #[must_use]
    pub fn encoder<F>(&self) -> ClickHouseFormatEncoder<F>
    where
        F: RecFamily,
        for<'b> F::Rec<'b>: serde::Serialize,
    {
        match &self.encoding {
            Encoding::RowBinary(schema) => ClickHouseFormatEncoder::row_binary(Arc::clone(schema)),
            Encoding::Native(schema) => ClickHouseFormatEncoder::native(Arc::clone(schema)),
        }
    }

    /// The frame codec that turns each sealed chunk into one compressed
    /// block, or `None` under `compression: none`.
    ///
    /// Under `lz4` or `zstd` the writer sends only blocks this codec made, and
    /// fails `Fatal` on any other frame. [`SinkBundle::into_parts`] attaches
    /// it as `SinkParts::frame_codec`, and the pipeline builder hands it to
    /// every chain. A manually assembled pipeline passes it to
    /// `ShardQueues::with_frame_codec` before the queues reach any chain. A
    /// caller of [`ClickHouseWriter`]'s `write_batch` outside a pipeline codes
    /// each frame with it first. Take it before `into_parts` consumes the
    /// sink.
    #[must_use]
    pub fn frame_codec(&self) -> Option<FrameCodecFactory> {
        self.frame_codec.clone()
    }

    /// A readiness probe over every replica of every shard, using the
    /// sink's independent probe client set (never the insert clients).
    /// This is the probe [`SinkBundle::into_parts`] attaches; manual
    /// assemblies hand it to `SinkRuntime.probe` directly.
    pub fn probe_fn(&self) -> SinkProbeFn {
        endpoint_probe(self.writer.clone(), Arc::clone(&self.probe_endpoints))
    }

    /// A [`ClickHouseDistributedRouter`] over this sink's shard topology and
    /// configured weights. Its placement matches a `Distributed` table with
    /// sharding expression `xxHash64(<key column>)`. Infallible: the weights
    /// were validated at [`ClickHouseSinkBuilder::new`].
    ///
    /// `F` is not inferable from the extractor fn item (`Rec<'buf>`
    /// projections are not injective), so name it:
    /// `sink.router::<Owned<OrderLineRow>>(order_key)`.
    #[must_use]
    pub fn router<F: RecFamily>(&self, extract: KeyExtractor<F>) -> ClickHouseDistributedRouter<F> {
        ClickHouseDistributedRouter::new(extract, &self.shard_weights)
            .expect("config validation guarantees at least one shard and weights >= 1")
    }

    /// Opt-in startup DDL-parity guard. Instant `Ok(())` when no
    /// `distributed_check` block is configured. Otherwise verifies shard
    /// count, per-shard weights, and the `Distributed` table's sharding
    /// expression against the live cluster, failing fast with a readable
    /// diff. Placement/DDL drift does not error at query time; it silently
    /// returns wrong results under `optimize_skip_unused_shards`.
    ///
    /// Call **after** [`connect`](ClickHouseSinkBuilder::connect) and
    /// **before** the pipeline consumes the sink.
    pub async fn validate_distributed(&self) -> Result<(), DistributedCheckError> {
        match &self.distributed {
            None => Ok(()),
            Some(check) => check.verify().await,
        }
    }
}

// ANCHOR: bundle
impl SinkBundle for ClickHouseSink {
    type Writer = ClickHouseWriter;

    fn into_parts(self) -> SinkParts<ClickHouseWriter> {
        let probe = self.probe_fn();
        let replica_labels = self
            .endpoints
            .iter()
            .map(|shard| shard.iter().map(|e| e.display_url().to_string()).collect())
            .collect();
        let parts = SinkParts::new(self.writer, self.endpoints, SinkPoolConfig::default())
            .with_component_type(TAG)
            .with_replica_labels(replica_labels)
            .with_probe(probe);
        match self.frame_codec {
            Some(codec) => parts.with_frame_codec(codec),
            None => parts,
        }
    }
}
// ANCHOR_END: bundle

fn build_with(
    cfg: ClickHouseSinkConfig,
    at: &SectionPath,
    system_roots: impl FnOnce() -> SystemRoots,
) -> Result<ClickHouseSinkBuilder, ConfigError> {
    cfg.validate_at(at)?;
    let schemes = Schemes::of(&cfg);
    let roots = crate::http::root_store(&cfg.tls, schemes.https, system_roots)
        .map_err(|why| at.invalid("tls.root_ca", why.to_string()))?;
    warn_cleartext_credentials(&cfg, &schemes, at);
    let tls = crate::http::client_config(roots);

    // Two independent client sets: inserts and readiness probes must not
    // share connection pools (see `ClickHouseSink::probe_endpoints`).
    let endpoints = make_endpoints(&cfg, &tls);
    let probe_endpoints = Arc::new(make_endpoints(&cfg, &tls));

    let shard_weights: Arc<[u32]> = cfg.shards.iter().map(|s| s.weight).collect();
    let startup = StartupBound::new(cfg.timeouts.startup);
    let distributed = cfg.distributed_check.as_ref().map(|section| {
        let url = section
            .endpoint
            .clone()
            .unwrap_or_else(|| cfg.shards[0].replicas[0].clone());
        let endpoint = ClickHouseEndpoint::new(client_for(&url, &cfg, &tls), url);
        let expected_expr = match (&section.sharding_key, &section.sharding_expr) {
            (Some(key), None) => format!("xxHash64({key})"),
            (None, Some(expr)) => distributed::normalize(expr),
            _ => unreachable!("validated: exactly one of sharding_key/sharding_expr"),
        };
        distributed::DistributedCheck {
            endpoint,
            startup: startup.clone(),
            cluster: section.cluster.clone(),
            database: cfg.database.clone(),
            table: section.table.clone(),
            expected_expr,
            weights: Arc::clone(&shard_weights),
            replica_hosts: cfg
                .shards
                .iter()
                .map(|s| {
                    s.replicas
                        .iter()
                        .filter_map(|u| distributed::host_of(u))
                        .collect()
                })
                .collect(),
        }
    });

    Ok(ClickHouseSinkBuilder {
        cfg,
        endpoints,
        probe_endpoints,
        shard_weights,
        distributed,
        startup,
    })
}

/// One configured client for `url`. Private: the `clickhouse` crate's 0.x
/// `Client` type must never surface in this crate's public API.
fn client_for(url: &str, cfg: &ClickHouseSinkConfig, tls: &ClientConfig) -> clickhouse::Client {
    let mut client = crate::http::client(tls).with_url(url);
    if let Some(db) = &cfg.database {
        client = client.with_database(db);
    }
    if let Some(user) = &cfg.user {
        client = client.with_user(user);
    }
    if let Some(password) = &cfg.password {
        client = client.with_password(password);
    }
    // The sink's frame codec compresses insert bodies itself. Explicit because
    // the client's default codec follows its features, and another crate in
    // the build can turn them on.
    client.with_compression(clickhouse::Compression::None)
}

/// One connected client per replica, `[shard][replica]`.
fn make_endpoints(cfg: &ClickHouseSinkConfig, tls: &ClientConfig) -> Vec<Vec<ClickHouseEndpoint>> {
    cfg.shards
        .iter()
        .map(|shard| {
            shard
                .replicas
                .iter()
                .map(|url| ClickHouseEndpoint::new(client_for(url, cfg, tls), url.clone()))
                .collect()
        })
        .collect()
}

/// The rule each replica and `distributed_check.endpoint` URL meets. The
/// `clickhouse` client parses its URL with the same parser.
const ENDPOINT: UrlRule<'static> =
    UrlRule::new("set them in user and password").schemes(&["http", "https"]);

/// The schemes of the replica and `distributed_check.endpoint` URLs, in any
/// case. A `distributed_check` with no endpoint queries the first replica,
/// which the replica walk already covers.
#[derive(Default)]
struct Schemes {
    /// Any replica or `distributed_check.endpoint` is `https://`.
    https: bool,
    /// Any replica is `http://`.
    http_replica: bool,
    /// `distributed_check.endpoint` is `http://`.
    http_check: bool,
}

impl Schemes {
    fn of(cfg: &ClickHouseSinkConfig) -> Self {
        let mut schemes = Schemes::default();
        let replicas = cfg.shards.iter().flat_map(|shard| &shard.replicas);
        let check = cfg
            .distributed_check
            .as_ref()
            .and_then(|c| c.endpoint.as_ref());
        let urls = replicas
            .map(|url| (url, true))
            .chain(check.map(|url| (url, false)));
        for (url, is_replica) in urls {
            let Ok(url) = ENDPOINT.check(url) else {
                continue;
            };
            match url.scheme() {
                "https" => schemes.https = true,
                "http" if is_replica => schemes.http_replica = true,
                "http" => schemes.http_check = true,
                _ => {}
            }
        }
        schemes
    }
}

/// Logs one warning per credential setting that crosses an `http://`
/// connection. `password` goes to every replica and to
/// `distributed_check.endpoint` in a header; `settings` go to replicas only,
/// as URL parameters, and ClickHouse reads a `password` parameter as a login.
fn warn_cleartext_credentials(cfg: &ClickHouseSinkConfig, schemes: &Schemes, at: &SectionPath) {
    let set = |value: Option<&String>| value.is_some_and(|v| !v.is_empty());
    if set(cfg.password.as_ref()) && (schemes.http_replica || schemes.http_check) {
        at.warn_cleartext_credential(
            "password",
            "a replica or distributed_check.endpoint uses http://",
        );
    }
    if set(cfg.settings.get("password")) && schemes.http_replica {
        at.warn_cleartext_credential("settings.password", "a replica uses http://");
    }
}

/// Strict identifier: `[A-Za-z_][A-Za-z0-9_]*`. Validated before being
/// backtick-quoted into SQL, so no escaping is ever needed.
fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A column name: one or more `is_identifier` segments joined by `.`.
/// ClickHouse names a flattened `Nested` column `outer.inner`, and the
/// whole name is backtick-quoted as a single identifier, so no escaping
/// is needed.
fn is_column_name(s: &str) -> bool {
    s.split('.').all(is_identifier)
}

/// The runtime backstop for [`ClickHouseSinkBuilder::connect`]:
/// `#[derive(ClickHouseRow)]` rejects a duplicate, empty, or malformed
/// column list at compile time, but a hand-written `ClickHouseRowFamily`
/// impl (for a borrowed record family, which the blanket impl over
/// [`Owned`](spate_core::deser::Owned) does not cover) bypasses the derive
/// entirely. The error is the reason alone.
fn validate_columns(columns: &[&str]) -> Result<(), String> {
    if columns.is_empty() {
        return Err("ClickHouseRow::COLUMNS must list at least one column".into());
    }
    let mut seen =
        spate_core::hash::FastSet::with_capacity_and_hasher(columns.len(), Default::default());
    for &col in columns {
        if !is_column_name(col) {
            return Err(format!(
                "column `{col}` is not a valid identifier (or dotted `outer.inner` name)"
            ));
        }
        // Duplicate columns emit e.g. `INSERT INTO t (`id`, `id`)`, which
        // ClickHouse rejects with DUPLICATE_COLUMN, a code the writer
        // classifies retryable, so it would loop forever. Reject at load.
        if !seen.insert(col) {
            return Err(format!("column `{col}` is listed more than once"));
        }
    }
    Ok(())
}

fn insert_statement(table: &str, columns: &[&str], format: ClickHouseFormat) -> String {
    let table = table
        .split('.')
        .map(|p| format!("`{p}`"))
        .collect::<Vec<_>>()
        .join(".");
    let cols = columns
        .iter()
        .map(|c| format!("`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} ({cols}) FORMAT {}", format.keyword())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClickHouseRow;
    use crate::test_startup;
    use clickhouse::test::{Mock, handlers};
    use serde::Serialize;
    use spate_core::clock::SyncClock;
    use spate_core::config::ComponentConfig;
    use spate_core::deser::Owned;

    /// `body` as the `sink: { clickhouse: ... }` section of a loaded
    /// pipeline config, so its errors start at `sink.clickhouse`.
    fn component(body: &str) -> ComponentConfig {
        loaded("sink:\n  clickhouse:\n", "    ", body)
            .sink_config("default")
            .expect("declared")
            .clone()
    }

    /// `body` as the `sinks: { eu: { clickhouse: ... } }` entry of a loaded
    /// pipeline config.
    fn sinks_entry(body: &str) -> ComponentConfig {
        loaded("sinks:\n  eu:\n    clickhouse:\n", "      ", body)
            .sink_config("eu")
            .expect("declared")
            .clone()
    }

    fn loaded(header: &str, indent: &str, body: &str) -> spate_core::config::PipelineConfig {
        let body: String = body.lines().map(|l| format!("{indent}{l}\n")).collect();
        spate_core::config::PipelineConfig::from_str(&format!(
            "pipeline: {{ name: demo }}\nsource: {{ memory: {{}} }}\n{header}{body}"
        ))
        .expect("loads")
    }

    /// The text of a refusal, which must be a [`ConfigError::Component`].
    fn refusal<T: std::fmt::Debug>(result: Result<T, ConfigError>) -> String {
        let err = result.expect_err("must be refused");
        assert!(matches!(err, ConfigError::Component { .. }), "{err:?}");
        err.to_string()
    }

    #[derive(Serialize, ClickHouseRow)]
    struct IdOnly {
        id: u64,
    }

    #[derive(Serialize, ClickHouseRow)]
    struct IdName {
        id: u64,
        name: String,
    }

    #[derive(Serialize, ClickHouseRow)]
    struct NestedTags {
        id: u64,
        #[serde(rename = "tags.key")]
        tags_key: Vec<String>,
        #[serde(rename = "tags.value")]
        tags_value: Vec<String>,
    }

    const MINIMAL: &str = r#"
table: orders
shards:
  - replicas: ["http://a:8123"]
"#;

    /// Replica and distributed-check endpoint URLs print without userinfo
    /// or query, in `Debug` and in the scheme error.
    #[test]
    fn replica_url_credentials_never_reach_debug_or_config_errors() {
        let cfg: ClickHouseSinkConfig = component(
            "table: orders\nshards: [{replicas: ['http://svc:hunter2@a:8123/?password=hunter2']}]\n\
             distributed_check: {cluster: c, table: d, sharding_key: id, endpoint: 'http://svc:hunter2@a:8123'}\n",
        )
        .deserialize_into()
        .unwrap();
        let printed = format!("{cfg:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(
            printed.contains("http://<redacted>@a:8123/?<redacted>"),
            "{printed}"
        );
        assert!(
            printed.contains(r#"endpoint: Some("http://<redacted>@a:8123")"#),
            "{printed}"
        );

        let builder = ClickHouseSinkBuilder::from_component_config(&component(
            "table: orders\nshards: [{replicas: ['http://a:8123/?password=hunter2']}]\n",
        ))
        .unwrap();
        let printed = format!("{builder:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(printed.contains("http://a:8123/?<redacted>"), "{printed}");

        let err = refusal(ClickHouseSinkBuilder::from_component_config(&component(
            "table: orders\nshards: [{replicas: ['svc:hunter2@a:8123']}]\n",
        )));
        assert_eq!(
            err,
            "sink.clickhouse.shards[0].replicas[0]: has no `//` after its scheme"
        );
    }

    /// A replica or `distributed_check.endpoint` URL with userinfo fails at load
    /// with an error that names `user` and `password` and omits the secret.
    /// Regression for #765.
    #[test]
    fn url_credentials_are_rejected_at_load() {
        let check = "distributed_check: {cluster: c, table: d, sharding_key: id, endpoint: ";
        let cases = [
            (
                "shards: [{replicas: ['http://svc:hunter2@a:8123']}]\n".to_owned(),
                "shards[0].replicas[0]",
            ),
            (
                "shards: [{replicas: ['HTTP://svc:hunter2@a:8123']}]\n".to_owned(),
                "shards[0].replicas[0]",
            ),
            (
                "shards: [{replicas: ['http://a', 'https://svc@b']}]\n".to_owned(),
                "shards[0].replicas[1]",
            ),
            (
                "shards: [{replicas: ['http:///svc:hunter2@a:8123']}]\n".to_owned(),
                "shards[0].replicas[0]",
            ),
            (
                "shards: [{replicas: ['http://:hunter2@a:8123']}]\n".to_owned(),
                "shards[0].replicas[0]",
            ),
            (
                format!("shards: [{{replicas: ['http://a']}}]\n{check}'http://svc:hunter2@b'}}\n"),
                "distributed_check.endpoint",
            ),
        ];
        for (yaml, key) in cases {
            let err = refusal(ClickHouseSinkBuilder::from_component_config(&component(
                &format!("table: orders\n{yaml}"),
            )));
            assert_eq!(
                err,
                format!("sink.clickhouse.{key}: carries userinfo; set them in user and password")
            );
        }

        let builder = ClickHouseSinkBuilder::from_component_config(&component(
            "table: orders\nshards: [{replicas: ['http://a:8123/?x=a@b']}]\n",
        ))
        .expect("an `@` after the host is not userinfo");
        assert_eq!(builder.endpoints[0][0].url(), "http://a:8123/?x=a@b");
    }

    /// `Debug` on the builder hides the password and every `settings` value
    /// and keeps the user and setting names. Regression for #754.
    #[test]
    fn debug_never_prints_password_or_settings_values() {
        let yaml =
            format!("{MINIMAL}user: svc\npassword: hunter2\nsettings: {{ password: hunter2 }}\n");
        let builder = ClickHouseSinkBuilder::from_component_config(&component(&yaml)).unwrap();
        let printed = format!("{builder:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        for visible in ["\"svc\"", "Some(<redacted>)", "\"password\": <redacted>"] {
            assert!(printed.contains(visible), "{visible}: {printed}");
        }
        let writer = ClickHouseWriter::new(
            "INSERT".into(),
            None,
            None,
            vec![("password".into(), "hunter2".into())],
            None,
            None,
        );
        assert!(!format!("{writer:?}").contains("hunter2"), "{writer:?}");
    }

    #[test]
    fn minimal_config_builds_with_framework_defaults() {
        let builder = ClickHouseSinkBuilder::from_component_config(&component(MINIMAL)).unwrap();
        assert_eq!(
            insert_statement("orders", IdName::COLUMNS, ClickHouseFormat::RowBinary),
            "INSERT INTO `orders` (`id`, `name`) FORMAT RowBinaryWithNamesAndTypes"
        );
        assert_eq!(builder.endpoints.len(), 1);
        assert_eq!(builder.endpoints[0].len(), 1);
    }

    #[test]
    fn qualified_table_and_knobs_map_through() {
        let sink = ClickHouseSinkBuilder::from_component_config(&component(
            r#"
table: analytics.orders
shards:
  - replicas: ["http://a:8123", "http://b:8123"]
  - replicas: ["http://c:8123"]
timeouts: { send: 5s, end: 60s }
settings: { insert_quorum: "auto" }
"#,
        ))
        .unwrap();
        assert_eq!(
            insert_statement(
                "analytics.orders",
                IdOnly::COLUMNS,
                ClickHouseFormat::RowBinary
            ),
            "INSERT INTO `analytics`.`orders` (`id`) FORMAT RowBinaryWithNamesAndTypes"
        );
        assert_eq!(sink.endpoints.len(), 2);
        assert_eq!(sink.endpoints[0].len(), 2);
    }

    /// The sink sets `decompress` on every compressed insert, so a configured
    /// value for it fails validation.
    #[test]
    fn decompress_is_a_reserved_setting() {
        let err = refusal(ClickHouseSinkBuilder::from_component_config(&component(
            &format!("{MINIMAL}settings: {{decompress: \"0\"}}\n"),
        )));
        assert_eq!(
            err,
            "sink.clickhouse: settings.\"decompress\" cannot be \
             overridden: set to `1` on every compressed insert: the sink sends blocks it \
             compressed itself, and the server decodes them only with it on"
        );
    }

    /// The server reads the last `database` in an insert's URL, and the
    /// client writes the typed one before the settings, so a `database` key in
    /// `settings` fails validation.
    #[test]
    fn database_is_a_reserved_setting() {
        let err = refusal(ClickHouseSinkBuilder::from_component_config(&component(
            &format!("{MINIMAL}database: analytics\nsettings: {{database: \"default\"}}\n"),
        )));
        assert_eq!(
            err,
            "sink.clickhouse: settings.\"database\" cannot be overridden: owned by the typed \
             `database` field: the startup checks read the table in that database, and inserts \
             go to the table they checked"
        );
    }

    /// `deduplicate_insert` decides deduplication from ClickHouse 26.2, so a
    /// configured `disable` fails validation.
    #[test]
    fn deduplicate_insert_is_a_reserved_setting() {
        let err = refusal(ClickHouseSinkBuilder::from_component_config(&component(
            &format!("{MINIMAL}settings: {{deduplicate_insert: disable}}\n"),
        )));
        assert_eq!(
            err,
            "sink.clickhouse: settings.\"deduplicate_insert\" cannot be overridden: forced to \
             `enable`, so the server deduplicates a retried batch by its token whatever the \
             server or profile default"
        );
    }

    #[test]
    fn every_setting_an_insert_carries_is_refused_in_settings() {
        for (name, _) in crate::writer::owned_settings("token", true) {
            let err = refusal(ClickHouseSinkBuilder::from_component_config(&component(
                &format!("{MINIMAL}settings: {{{name}: \"0\"}}\n"),
            )));
            let refused = format!("sink.clickhouse: settings.\"{name}\" cannot be overridden: ");
            assert!(err.starts_with(&refused), "{name}: {err}");
        }
    }

    #[test]
    fn validation_rejects_bad_configs() {
        let cases = [
            ("table: orders\nshards: []", "shard"),
            ("table: orders\nshards: [{replicas: []}]", "replicas"),
            ("table: orders\nshards: [{replicas: [\"tcp://x\"]}]", "http"),
            (
                "table: \"or`ders\"\nshards: [{replicas: [\"http://a\"]}]",
                "identifier",
            ),
            (
                "table: a.b.c\nshards: [{replicas: [\"http://a\"]}]",
                "identifier",
            ),
            (
                "table: orders\nshards: [{replicas: [\"http://a\"]}]\nsettings: {insert_deduplication_token: \"x\"}",
                "settings.\"insert_deduplication_token\" cannot be overridden",
            ),
        ];
        for (yaml, needle) in cases {
            let err = ClickHouseSinkBuilder::from_component_config(&component(yaml)).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains(needle),
                "expected `{needle}` in error for {yaml}: {msg}"
            );
        }
    }

    #[test]
    fn a_flattened_nested_column_is_accepted_and_quoted_as_one_identifier() {
        validate_columns(NestedTags::COLUMNS).expect("dotted column names are valid");
        assert_eq!(
            insert_statement("events", NestedTags::COLUMNS, ClickHouseFormat::RowBinary),
            "INSERT INTO `events` (`id`, `tags.key`, `tags.value`) \
             FORMAT RowBinaryWithNamesAndTypes"
        );
    }

    /// `builder` with its startup bound, and its `distributed_check`'s, on
    /// `clock`.
    fn on_clock(mut builder: ClickHouseSinkBuilder, clock: &SyncClock) -> ClickHouseSinkBuilder {
        builder.startup.clock = clock.clone();
        if let Some(check) = &mut builder.distributed {
            check.startup.clock = clock.clone();
        }
        builder
    }

    /// A builder for table `t` on one replica at `url`, with a 7 s startup
    /// bound on `clock` and `extra` appended to its section.
    fn bounded_builder(url: &str, clock: &SyncClock, extra: &str) -> ClickHouseSinkBuilder {
        let section = format!(
            "table: t\nshards: [{{replicas: [\"{url}\"]}}]\ntimeouts: {{ startup: 7s }}\n{extra}"
        );
        on_clock(
            ClickHouseSinkBuilder::from_component_config(&component(&section)).unwrap(),
            clock,
        )
    }

    const STARTUP: Duration = Duration::from_secs(7);
    const NO_ANSWER: &str = "no answer within 7s, the `timeouts.startup` bound";

    fn id_column() -> Vec<crate::schema::ColumnRow> {
        crate::testing::system_columns(&[("id", "UInt64", "")])
    }

    /// A replica that accepts the schema read and never answers it fails the
    /// build once `timeouts.startup` has passed, with an error naming the
    /// replica and the bound. Regression for #1953.
    #[test]
    fn a_replica_that_never_answers_fails_the_build_at_the_startup_bound() {
        let io = test_startup::runtime();
        let (clock, given) = test_startup::frozen();
        let _in = io.enter();
        let mock = Mock::new();
        let (url, held) = test_startup::silent_after(&io, &mock, 0);
        let build = bounded_builder(&url, &given, "").connect::<spate_core::deser::Owned<IdOnly>>();
        let err = test_startup::past_the_bound(&io, &clock, STARTUP, &held, build).unwrap_err();
        let SchemaError::Fetch {
            url: named, reason, ..
        } = err
        else {
            panic!("expected the schema read to fail: {err}");
        };
        assert_eq!((named, reason.as_str()), (url, NO_ANSWER));
    }

    /// A replica that answers the schema read and never answers the settings
    /// check fails the build at the startup bound.
    #[test]
    fn a_settings_check_with_no_answer_fails_the_build_at_the_startup_bound() {
        let io = test_startup::runtime();
        let (clock, given) = test_startup::frozen();
        let _in = io.enter();
        let mock = Mock::new();
        mock.add(handlers::provide(id_column()));
        let (url, held) = test_startup::silent_after(&io, &mock, 1);
        let build = bounded_builder(&url, &given, "").connect::<spate_core::deser::Owned<IdOnly>>();
        let err = test_startup::past_the_bound(&io, &clock, STARTUP, &held, build).unwrap_err();
        let SchemaError::Settings { url: named, reason } = err else {
            panic!("expected the settings check to fail: {err}");
        };
        assert_eq!((named, reason.as_str()), (url, NO_ANSWER));
    }

    /// Each `distributed_check` query that gets no answer fails the check at
    /// the startup bound, naming the query.
    #[test]
    fn a_distributed_check_query_with_no_answer_fails_at_the_startup_bound() {
        #[derive(Serialize, clickhouse::Row)]
        struct ClusterRow {
            shard_num: u32,
            shard_weight: u32,
            host_name: String,
        }
        const CHECK: &str = "distributed_check: { cluster: prod, table: t_dist, sharding_key: id }";
        for (answered, query) in [(0, "cluster topology"), (1, "table engine")] {
            let io = test_startup::runtime();
            let (clock, given) = test_startup::frozen();
            let _in = io.enter();
            let mock = Mock::new();
            mock.add(handlers::provide(id_column()));
            mock.add(handlers::provide::<u8>([1u8]));
            if answered == 1 {
                mock.add(handlers::provide([ClusterRow {
                    shard_num: 1,
                    shard_weight: 1,
                    host_name: "127.0.0.1".into(),
                }]));
            }
            let (url, held) = test_startup::silent_after(&io, &mock, 2 + answered);
            let sink = io
                .block_on(
                    bounded_builder(&url, &given, CHECK)
                        .connect::<spate_core::deser::Owned<IdOnly>>(),
                )
                .expect("the replica answers the build");
            let checked = async move { sink.validate_distributed().await };
            let err =
                test_startup::past_the_bound(&io, &clock, STARTUP, &held, checked).unwrap_err();
            let DistributedCheckError::Fetch {
                what,
                url: named,
                reason,
            } = err
            else {
                panic!("expected the {query} query to fail: {err}");
            };
            assert_eq!((what, named, reason.as_str()), (query, url, NO_ANSWER));
        }
    }

    /// A build on a runtime with no timer returns the replica's error.
    #[test]
    fn a_build_on_a_runtime_without_a_timer_returns_the_replicas_error() {
        let refusing = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = refusing.local_addr().unwrap();
        drop(refusing);
        let io = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        let section = format!("table: t\nshards: [{{replicas: [\"http://{addr}\"]}}]");
        let err = io
            .block_on(
                ClickHouseSinkBuilder::from_component_config(&component(&section))
                    .unwrap()
                    .connect::<spate_core::deser::Owned<IdOnly>>(),
            )
            .unwrap_err();
        assert!(matches!(err, SchemaError::Fetch { .. }), "{err}");
    }

    /// The startup bound is 30 s unless `timeouts.startup` sets it.
    #[test]
    fn the_startup_bound_defaults_to_thirty_seconds() {
        assert_eq!(
            TimeoutSection::default().startup,
            Some(Duration::from_secs(30))
        );
        let sink = ClickHouseSinkBuilder::from_component_config(&component(
            "table: t\nshards: [{replicas: [\"http://a:8123\"]}]\ntimeouts: { startup: 5s }",
        ))
        .unwrap();
        assert_eq!(sink.cfg.timeouts.startup, Some(Duration::from_secs(5)));
        assert_eq!(sink.cfg.timeouts.send, Some(Duration::from_secs(30)));
    }

    /// `ClickHouseRowFamily` can be hand-written for a borrowed record
    /// family, bypassing the derive's compile-time checks entirely, so
    /// `connect` re-runs them at runtime, before it reaches the server.
    #[tokio::test]
    async fn connect_rejects_a_hand_written_familys_bad_columns() {
        use spate_core::deser::RecFamily;

        struct BadColumns;
        impl RecFamily for BadColumns {
            type Rec<'buf> = Vec<u8>;
        }
        impl crate::ClickHouseRowFamily for BadColumns {
            const COLUMNS: &'static [&'static str] = &["id", "id"];
        }

        // The replica is unroutable, so reaching it would surface as `Fetch`.
        let err = ClickHouseSinkBuilder::from_component_config(&component(
            "table: t\nshards: [{replicas: [\"http://127.0.0.1:1\"]}]",
        ))
        .unwrap()
        .connect::<BadColumns>()
        .await
        .unwrap_err();
        let SchemaError::Columns(msg) = err else {
            panic!("the column check runs before the fetch: {err}");
        };
        assert_eq!(msg, "sink.clickhouse: column `id` is listed more than once");
    }

    /// The framework's pool blocks sit beside `clickhouse:` in the sink
    /// section, so the body refuses each one by name.
    #[test]
    fn a_pool_block_in_the_body_is_refused() {
        for key in ["batch", "inflight", "retry", "breaker"] {
            let yaml = format!("{MINIMAL}{key}: {{}}\n");
            let msg = refusal(ClickHouseSinkBuilder::from_component_config(&component(
                &yaml,
            )));
            assert!(msg.contains(&format!("unknown field `{key}`")), "{msg}");
        }
    }

    /// A configuration written against the three-mode `validate_schema` key
    /// fails to load naming the key, rather than starting with the setting
    /// quietly ignored.
    #[test]
    fn a_stale_validate_schema_key_fails_to_parse() {
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        for stale in ["off", "names", "full"] {
            let err = serde_yaml::from_str::<ClickHouseSinkConfig>(&format!(
                "{base}validate_schema: {stale}\n"
            ))
            .expect_err("the key is gone");
            assert!(
                err.to_string().contains("validate_schema"),
                "the error must name the key: {err}"
            );
        }
    }

    /// The two header settings are managed per insert, so a configuration
    /// cannot turn off the check the wire format exists for.
    #[test]
    fn the_header_settings_cannot_be_overridden() {
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        for key in [
            "input_format_with_names_use_header",
            "input_format_with_types_use_header",
        ] {
            let err = ClickHouseSinkBuilder::from_component_config(&component(&format!(
                "{base}settings: {{ {key}: \"0\" }}"
            )))
            .expect_err("a managed setting");
            assert!(err.to_string().contains(key), "{err}");
        }
    }

    #[test]
    fn unknown_fields_are_rejected_with_a_path() {
        let err = ClickHouseSinkBuilder::from_component_config(&component(
            "table: orders\nshards: [{replicas: [\"http://a\"]}]\ntimeouts: {sendd: 5s}",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("sendd"), "{err}");
    }

    /// A YAML still holding the removed `columns` key fails to load, naming
    /// the key, rather than silently ignoring it or shadowing the derived
    /// column list.
    #[test]
    fn a_stale_columns_key_is_rejected_with_a_path() {
        let err = ClickHouseSinkBuilder::from_component_config(&component(
            "table: orders\ncolumns: [id]\nshards: [{replicas: [\"http://a\"]}]",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("columns"), "{err}");
    }

    #[test]
    fn compression_parses_and_defaults_to_lz4() {
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        for (yaml, expected) in [
            ("", ClickHouseCompression::Lz4),
            ("compression: none\n", ClickHouseCompression::None),
            ("compression: lz4\n", ClickHouseCompression::Lz4),
            (
                "compression: zstd\n",
                ClickHouseCompression::Zstd(ZSTD_DEFAULT_LEVEL),
            ),
            ("compression: \"zstd:9\"\n", ClickHouseCompression::Zstd(9)),
            ("compression: \"zstd:1\"\n", ClickHouseCompression::Zstd(1)),
            (
                "compression: \"zstd:22\"\n",
                ClickHouseCompression::Zstd(22),
            ),
        ] {
            let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!("{base}{yaml}")).unwrap();
            assert_eq!(cfg.compression, expected, "for `{yaml}`");
        }
    }

    #[test]
    fn compression_rejects_invalid_strings_with_a_path() {
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        for (value, input, needle) in [
            ("gzip", "gzip", "unknown compression"),
            // The off value is `none`, as for every other key that turns a
            // feature off, and the message names it.
            ("off", "off", "expected none, lz4"),
            ("\"zstd:0\"", "zstd:0", "[1, 22]"),
            ("\"zstd:23\"", "zstd:23", "[1, 22]"),
            ("\"zstd:99\"", "99", "[1, 22]"),
            ("\"zstd:hunter2\"", "hunter2", "invalid zstd level"),
        ] {
            let err = serde_yaml::from_str::<ClickHouseSinkConfig>(&format!(
                "{base}compression: {value}\n"
            ))
            .unwrap_err()
            .to_string();
            assert!(
                err.contains(needle),
                "expected `{needle}` for `{value}`: {err}"
            );
            assert!(
                !err.contains(input),
                "`{input}` echoed for `{value}`: {err}"
            );
        }
    }

    #[test]
    fn format_parses_and_defaults_to_rowbinary() {
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        for (yaml, expected) in [
            ("", ClickHouseFormat::RowBinary),
            ("format: rowbinary\n", ClickHouseFormat::RowBinary),
            ("format: native\n", ClickHouseFormat::Native),
        ] {
            let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!("{base}{yaml}")).unwrap();
            assert_eq!(cfg.format, expected, "for `{yaml}`");
        }
    }

    /// Each format names itself in the `INSERT`, and only RowBinary carries
    /// its schema separately: a Native block already names its own columns.
    #[test]
    fn each_format_names_itself_in_the_insert_statement() {
        assert_eq!(
            insert_statement("t", IdName::COLUMNS, ClickHouseFormat::Native),
            "INSERT INTO `t` (`id`, `name`) FORMAT Native"
        );
        assert_eq!(
            insert_statement("t", IdName::COLUMNS, ClickHouseFormat::RowBinary),
            "INSERT INTO `t` (`id`, `name`) FORMAT RowBinaryWithNamesAndTypes"
        );
    }

    #[test]
    fn validation_rejects_out_of_range_programmatic_zstd_level() {
        let mut cfg = ClickHouseSinkConfig::new(
            "t",
            vec![ClickHouseShardSection::new(vec!["http://a".into()])],
        );
        cfg.compression = ClickHouseCompression::Zstd(99);
        let err = ClickHouseSinkBuilder::new(cfg).unwrap_err();
        assert!(err.to_string().contains("[1, 22]"), "{err}");
    }

    /// A config built in code accepts the zstd levels the YAML parser
    /// accepts, and no others.
    #[test]
    fn new_accepts_zstd_levels_one_through_twenty_two_only() {
        let zstd = |level| {
            let mut cfg = ClickHouseSinkConfig::new(
                "t",
                vec![ClickHouseShardSection::new(vec!["http://a".into()])],
            );
            cfg.compression = ClickHouseCompression::Zstd(level);
            cfg
        };
        for level in [1, 22] {
            if let Err(e) = ClickHouseSinkBuilder::new(zstd(level)) {
                panic!("zstd level {level} must build: {e}");
            }
        }
        for level in [0, 23] {
            let msg = refusal(ClickHouseSinkBuilder::new(zstd(level)));
            assert_eq!(
                msg,
                format!("sink.clickhouse.compression: zstd level must be in [1, 22] (got {level})")
            );
        }
    }

    /// `new` and the YAML defaults are two spellings of one config, so a
    /// knob added to the struct has to reach both.
    #[test]
    fn new_matches_the_yaml_defaults() {
        let from_yaml: ClickHouseSinkConfig = serde_yaml::from_str(MINIMAL).unwrap();
        assert_eq!(
            ClickHouseSinkConfig::new(
                "orders",
                vec![ClickHouseShardSection::new(vec!["http://a:8123".into()])],
            ),
            from_yaml
        );
    }

    /// The nested section has the same two spellings, and `new` must
    /// produce one `ClickHouseSinkBuilder::new` accepts rather than one it
    /// rejects.
    #[test]
    fn distributed_check_new_matches_the_yaml_defaults_and_builds() {
        let from_yaml: ClickHouseDistributedCheckSection =
            serde_yaml::from_str("cluster: prod\ntable: db.t_dist\nsharding_key: id\n").unwrap();
        assert_eq!(
            ClickHouseDistributedCheckSection::new("prod", "db.t_dist", "id"),
            from_yaml
        );

        let mut cfg = ClickHouseSinkConfig::new(
            "t",
            vec![ClickHouseShardSection::new(vec!["http://a:8123".into()])],
        );
        cfg.distributed_check = Some(ClickHouseDistributedCheckSection::new(
            "prod",
            "db.t_dist",
            "id",
        ));
        let _ = ClickHouseSinkBuilder::new(cfg)
            .expect("a config built entirely from `new` passes validation");
    }

    /// An `https://` replica signed by the CA that `SSL_CERT_FILE` names is
    /// trusted through `ClickHouseSinkBuilder::new`. Regression for #619.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_trusts_a_ca_from_the_system_trust_store() {
        const NAME: &str = "config::tests::new_trusts_a_ca_from_the_system_trust_store";
        const URL: &str = "SPATE_TEST_CLICKHOUSE_TLS_URL";
        if let Ok(url) = std::env::var(URL) {
            let builder = ClickHouseSinkBuilder::new(ClickHouseSinkConfig::new(
                "t",
                vec![ClickHouseShardSection::new(vec![url])],
            ))
            .unwrap();
            builder.endpoints[0][0]
                .client()
                .query("SELECT 1")
                .execute()
                .await
                .expect("the CA in SSL_CERT_FILE is trusted");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let ca = crate::test_tls::TestCa::new("system");
        let url = crate::test_tls::serve(&ca, None).await;
        let ca_file = ca.write(dir.path());
        tokio::task::block_in_place(|| {
            spate_test_support::run_in_child(NAME, |child| {
                child
                    .env_remove("SSL_CERT_DIR")
                    .env("SSL_CERT_FILE", ca_file)
                    .env(URL, url)
            });
        });
    }

    /// The trust store is read when a replica or the `distributed_check`
    /// endpoint is `https://`, and only then.
    #[test]
    fn the_trust_store_is_read_only_for_https() {
        let http = ClickHouseSinkConfig::new(
            "t",
            vec![ClickHouseShardSection::new(vec!["http://a".into()])],
        );
        let _ = build_with(http.clone(), &SectionPath::sink(TAG), || {
            panic!("the trust store was read")
        })
        .unwrap();

        let mut front = http;
        let mut check = ClickHouseDistributedCheckSection::new("prod", "db.t_dist", "id");
        check.endpoint = Some("https://front:8443".into());
        front.distributed_check = Some(check);
        let https = ClickHouseSinkConfig::new(
            "t",
            vec![ClickHouseShardSection::new(vec!["https://a".into()])],
        );
        let system = crate::test_tls::TestCa::new("system");
        for cfg in [front, https] {
            let mut read = false;
            let _ = build_with(cfg, &SectionPath::sink(TAG), || {
                read = true;
                SystemRoots::from_certs(vec![system.der()])
            })
            .unwrap();
            assert!(read);
        }
    }

    /// An `https://` sink whose system trust store holds no usable root, and
    /// with no `tls.root_ca`, fails at that key with the fixes named.
    #[test]
    fn an_https_sink_with_no_trust_root_fails_at_root_ca() {
        let https = ClickHouseSinkConfig::new(
            "t",
            vec![ClickHouseShardSection::new(vec!["https://a".into()])],
        );
        let err = refusal(build_with(https, &SectionPath::sink(TAG), || {
            SystemRoots::from_certs(Vec::new())
        }));
        assert_eq!(
            err,
            "sink.clickhouse.tls.root_ca: the system trust store has no certificates; install \
             a CA bundle, point SSL_CERT_FILE at one, or set this key to the issuing CA"
        );
    }

    const CLEARTEXT: &str = "the credential is sent without TLS";

    /// The cleartext-credential lines building `cfg` logs, and every line it
    /// logs. The trust store is a stub, so no host store adds a line.
    fn cleartext_lines(cfg: ClickHouseSinkConfig) -> (Vec<String>, Vec<String>) {
        let system = crate::test_tls::TestCa::new("system");
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            let _ = build_with(cfg, &SectionPath::sink(TAG), || {
                SystemRoots::from_certs(vec![system.der()])
            })
            .unwrap();
        });
        let cleartext = lines
            .iter()
            .filter(|l| l.contains(CLEARTEXT))
            .cloned()
            .collect();
        (cleartext, lines)
    }

    fn shards(urls: &[&[&str]]) -> ClickHouseSinkConfig {
        let shards = urls
            .iter()
            .map(|replicas| {
                ClickHouseShardSection::new(replicas.iter().map(|&u| u.into()).collect())
            })
            .collect();
        ClickHouseSinkConfig::new("t", shards)
    }

    fn with_check_endpoint(mut cfg: ClickHouseSinkConfig, url: &str) -> ClickHouseSinkConfig {
        let mut check = ClickHouseDistributedCheckSection::new("prod", "db.t_dist", "id");
        check.endpoint = Some(url.into());
        cfg.distributed_check = Some(check);
        cfg
    }

    /// `password` sent over an `http://` replica or `distributed_check`
    /// endpoint logs one warning however many URLs it crosses. All-`https`
    /// URLs, an empty password and no password log none.
    #[test]
    fn a_password_over_http_logs_one_warning() {
        const LINE: &str = "sink.clickhouse.password: a replica or distributed_check.endpoint \
                            uses http://; the credential is sent without TLS";
        let https = || shards(&[&["https://a"]]);
        let four_http = || shards(&[&["http://a", "http://b"], &["http://c", "HTTP://d"]]);
        for (case, cfg, secret, warns) in [
            (
                "one http replica",
                shards(&[&["http://a"]]),
                Some("hunter2"),
                true,
            ),
            ("four http replicas", four_http(), Some("hunter2"), true),
            (
                "http check endpoint",
                with_check_endpoint(https(), "http://front"),
                Some("hunter2"),
                true,
            ),
            (
                "all https",
                with_check_endpoint(https(), "https://front"),
                Some("hunter2"),
                false,
            ),
            ("empty", four_http(), Some(""), false),
            ("absent", four_http(), None, false),
        ] {
            let mut cfg = cfg;
            cfg.password = secret.map(Into::into);
            let (cleartext, lines) = cleartext_lines(cfg);
            assert_eq!(cleartext.len(), usize::from(warns), "{case}: {lines:?}");
            for line in &cleartext {
                assert!(
                    line.contains("WARN") && line.contains(LINE),
                    "{case}: {line}"
                );
            }
            assert!(
                !lines.iter().any(|l| l.contains("hunter2")),
                "{case}: {lines:?}"
            );
        }
    }

    /// `settings.password` sent to an `http://` replica logs one warning.
    /// Settings never reach `distributed_check.endpoint`, so an `http://`
    /// endpoint in front of `https://` replicas logs none for it.
    #[test]
    fn a_settings_password_over_an_http_replica_logs_one_warning() {
        const LINE: &str = "sink.clickhouse.settings.password: a replica uses http://; \
                            the credential is sent without TLS";
        let https = || shards(&[&["https://a"]]);
        for (case, cfg, secret, warns) in [
            (
                "http replicas",
                shards(&[&["http://a", "http://b"]]),
                "hunter2",
                true,
            ),
            ("empty", shards(&[&["http://a"]]), "", false),
            ("all https", https(), "hunter2", false),
            (
                "http check endpoint only",
                with_check_endpoint(https(), "http://front"),
                "hunter2",
                false,
            ),
        ] {
            let mut cfg = cfg;
            cfg.settings.insert("password".into(), secret.into());
            let (cleartext, lines) = cleartext_lines(cfg);
            assert_eq!(cleartext.len(), usize::from(warns), "{case}: {lines:?}");
            for line in &cleartext {
                assert!(
                    line.contains("WARN") && line.contains(LINE),
                    "{case}: {line}"
                );
            }
            assert!(
                !lines.iter().any(|l| l.contains("hunter2")),
                "{case}: {lines:?}"
            );
        }
    }

    /// A `sinks:` entry names its own path in the warning.
    #[test]
    fn a_sinks_entry_names_its_path_in_the_warning() {
        let section =
            sinks_entry("table: t\nshards: [{ replicas: [\"http://a\"] }]\npassword: hunter2\n");
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            let _ = ClickHouseSinkBuilder::from_component_config(&section).unwrap();
        });
        let cleartext: Vec<_> = lines.iter().filter(|l| l.contains(CLEARTEXT)).collect();
        assert_eq!(cleartext.len(), 1, "{lines:?}");
        assert!(
            cleartext[0].contains("sinks.eu.clickhouse.password: "),
            "{cleartext:?}"
        );
    }

    /// A config the builder refuses logs no warning, even when its password
    /// would cross `http://`: the warning follows the section's own checks.
    #[test]
    fn a_refused_config_logs_no_cleartext_warning() {
        let system = crate::test_tls::TestCa::new("system");
        // Refused by `validate`, then by the trust-store load.
        for replicas in [&["http://a"][..], &["http://a", "https://b"]] {
            let mut cfg = shards(&[replicas]);
            cfg.password = Some("hunter2".into());
            cfg.tls.root_ca = Some("/nonexistent-spate-1647/ca.pem".into());
            let lines = spate_test::capture_logs(tracing::Level::INFO, || {
                let err = refusal(build_with(cfg, &SectionPath::sink(TAG), || {
                    SystemRoots::from_certs(vec![system.der()])
                }));
                assert!(err.contains("tls.root_ca"), "{err}");
            });
            assert!(
                !lines.iter().any(|l| l.contains(CLEARTEXT)),
                "{replicas:?}: {lines:?}"
            );
        }
    }

    #[test]
    fn tls_root_ca_parses_and_requires_an_https_url() {
        // The scheme is read in any case, as the client reads it.
        let upper =
            "table: t\nshards: [{replicas: [\"HTTPS://a\"]}]\ntls: {root_ca: /etc/ca.pem}\n";
        let cfg: ClickHouseSinkConfig = serde_yaml::from_str(upper).unwrap();
        cfg.validate().expect("an HTTPS:// replica is https");

        let yaml = "table: t\nshards: [{replicas: [\"https://a\"]}]\ntls: {root_ca: /etc/ca.pem}\n";
        let cfg: ClickHouseSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            cfg.tls.root_ca,
            Some(std::path::PathBuf::from("/etc/ca.pem"))
        );

        let err = ClickHouseSinkBuilder::from_component_config(&component(
            &yaml.replace("https", "http"),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("tls.root_ca"), "{err}");
    }

    #[test]
    fn shard_weights_parse_and_default_to_one() {
        let cfg: ClickHouseSinkConfig = serde_yaml::from_str(
            "table: t\nshards:\n\
             \x20 - replicas: [\"http://a\"]\n\
             \x20 - replicas: [\"http://b\"]\n\
             \x20   weight: 9\n",
        )
        .unwrap();
        assert_eq!(cfg.shards[0].weight, 1, "weight defaults to 1");
        assert_eq!(cfg.shards[1].weight, 9);
    }

    #[test]
    fn zero_shard_weight_is_rejected() {
        let err = ClickHouseSinkBuilder::from_component_config(&component(
            "table: t\nshards: [{replicas: [\"http://a\"], weight: 0}]",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("weight"), "{err}");
    }

    #[test]
    fn sink_router_captures_config_weights_in_order() {
        use crate::router::ShardKey;

        let sink = ClickHouseSinkBuilder::from_component_config(&component(
            "table: t\nshards:\n\
             \x20 - replicas: [\"http://a\"]\n\
             \x20   weight: 9\n\
             \x20 - replicas: [\"http://b\"]\n\
             \x20   weight: 10\n",
        ))
        .unwrap();
        // `&Vec<u8>` (not `&[u8]`) is forced by the KeyExtractor fn-pointer
        // type: its argument is `&'a Rec<'buf>` = `&'a Vec<u8>`.
        #[allow(clippy::ptr_arg)]
        fn key(rec: &Vec<u8>) -> ShardKey<'_> {
            ShardKey::Bytes(rec)
        }
        let router = sink.router::<Owned<Vec<u8>>>(key);
        assert_eq!(router.shard_count(), 2);
        // The docs' 9/10 example: remainder 8 → shard 0, remainder 9 → shard 1.
        assert_eq!(router.shard_for_hash(8), 0);
        assert_eq!(router.shard_for_hash(9), 1);
    }

    #[test]
    fn distributed_check_parses_with_endpoint_defaulting_to_first_replica() {
        let sink = ClickHouseSinkBuilder::from_component_config(&component(
            "table: t\nshards: [{replicas: [\"http://a:8123\"]}]\n\
             distributed_check: { cluster: prod, table: db.t_dist, sharding_key: id }",
        ))
        .unwrap();
        let check = sink.distributed.as_ref().expect("check captured");
        assert_eq!(check.cluster, "prod");
        assert_eq!(check.table, "db.t_dist");
        assert_eq!(check.expected_expr, "xxHash64(id)");
        assert_eq!(check.endpoint.url(), "http://a:8123");
    }

    #[test]
    fn distributed_check_requires_exactly_one_of_key_or_expr() {
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        for check in [
            "distributed_check: { cluster: c, table: t_dist }",
            "distributed_check: { cluster: c, table: t_dist, sharding_key: id, sharding_expr: \"xxHash64(id)\" }",
        ] {
            let err =
                ClickHouseSinkBuilder::from_component_config(&component(&format!("{base}{check}")))
                    .unwrap_err();
            assert!(
                err.to_string().contains("exactly one"),
                "for `{check}`: {err}"
            );
        }
    }

    #[test]
    fn distributed_check_rejects_bad_cluster_table_key_and_endpoint() {
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        let cases = [
            (
                "distributed_check: { cluster: \"pr od\", table: t_dist, sharding_key: id }",
                "cluster",
            ),
            (
                "distributed_check: { cluster: c, table: a.b.c, sharding_key: id }",
                "table",
            ),
            (
                "distributed_check: { cluster: c, table: t_dist, sharding_key: \"id; DROP\" }",
                "sharding_key",
            ),
            (
                "distributed_check: { cluster: c, table: t_dist, sharding_expr: \"  \" }",
                "sharding_expr",
            ),
            (
                "distributed_check: { cluster: c, table: t_dist, sharding_key: id, endpoint: \"tcp://x\" }",
                "endpoint",
            ),
        ];
        for (check, needle) in cases {
            let err =
                ClickHouseSinkBuilder::from_component_config(&component(&format!("{base}{check}")))
                    .unwrap_err();
            assert!(
                err.to_string().contains(needle),
                "expected `{needle}` for `{check}`: {err}"
            );
        }
    }

    /// A sink query sends no `Accept-Encoding` whatever `compression` names,
    /// so the server answers it uncompressed.
    #[tokio::test]
    #[expect(
        clippy::disallowed_methods,
        reason = "a test task ends when the test's runtime drops"
    )]
    async fn a_sink_query_asks_for_an_uncompressed_response() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut buf = [0u8; 1024];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = tcp.read(&mut buf).await.unwrap();
                assert_ne!(n, 0, "the request ended inside its head");
                head.extend_from_slice(&buf[..n]);
            }
            tcp.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8(head).unwrap().to_ascii_lowercase()
        });
        let mut cfg = ClickHouseSinkConfig::new("t", vec![ClickHouseShardSection::new(vec![url])]);
        cfg.compression = ClickHouseCompression::Zstd(3);
        let builder = ClickHouseSinkBuilder::new(cfg).unwrap();
        builder.endpoints[0][0]
            .client()
            .query("SELECT 1")
            .execute()
            .await
            .unwrap();
        let head = server.await.unwrap();
        assert!(head.starts_with("post "), "{head}");
        assert!(!head.contains("accept-encoding"), "{head}");
    }

    /// A sink built from a `sinks:` entry reports at the entry, as its decode
    /// errors do.
    #[test]
    fn an_error_in_a_sinks_entry_names_the_entry() {
        let err = refusal(ClickHouseSinkBuilder::from_component_config(&sinks_entry(
            "table: t\nshards: []\n",
        )));
        assert_eq!(
            err,
            "sinks.eu.clickhouse.shards: at least one shard is required"
        );

        let err = refusal(ClickHouseSinkBuilder::from_component_config(&sinks_entry(
            &format!("{MINIMAL}settings: {{insert_deduplicate: \"0\"}}\n"),
        )));
        assert!(
            err.starts_with(
                "sinks.eu.clickhouse: settings.\"insert_deduplicate\" cannot be overridden: "
            ),
            "{err}"
        );
    }

    /// An unusable `tls.root_ca` is refused at that key of the section it
    /// came from. The config constructor does not load the file, so only the
    /// builder's own path reaches this refusal.
    #[test]
    fn an_unusable_root_ca_in_a_sinks_entry_names_the_entry() {
        let section = sinks_entry(
            "table: t\nshards: [{replicas: [\"https://a\"]}]\ntls: {root_ca: /nonexistent/ca.pem}\n",
        );
        let err = refusal(ClickHouseSinkBuilder::from_component_config(&section));
        assert!(
            err.starts_with("sinks.eu.clickhouse.tls.root_ca: `/nonexistent/ca.pem`: "),
            "{err}"
        );
    }

    /// A section built with `ComponentConfig::new` and never placed reports
    /// at its tag alone.
    #[test]
    fn an_unplaced_section_reports_at_its_tag() {
        let section = spate_test::component_config(TAG, "table: t\nshards: []\n");
        assert_eq!(
            refusal(ClickHouseSinkBuilder::from_component_config(&section)),
            "clickhouse.shards: at least one shard is required"
        );
    }

    /// The config constructor refuses at the section's place in the pipeline,
    /// as the builder does.
    #[test]
    fn config_from_component_config_refuses_at_the_section_path() {
        let unplaced = spate_test::component_config(TAG, "table: t\nshards: []\n");
        assert_eq!(
            refusal(ClickHouseSinkConfig::from_component_config(&unplaced)),
            "clickhouse.shards: at least one shard is required"
        );
        assert_eq!(
            refusal(ClickHouseSinkConfig::from_component_config(&sinks_entry(
                "table: t\nshards: []\n"
            ))),
            "sinks.eu.clickhouse.shards: at least one shard is required"
        );
    }

    /// `validate` checks the configuration rules at `sink.clickhouse` and
    /// leaves the CA file to the builder.
    #[test]
    fn validate_refuses_without_building() {
        let no_shards = ClickHouseSinkConfig::new("t", Vec::new());
        assert_eq!(
            refusal(no_shards.validate()),
            "sink.clickhouse.shards: at least one shard is required"
        );

        let mut missing_ca = ClickHouseSinkConfig::new(
            "t",
            vec![ClickHouseShardSection::new(vec!["https://a".into()])],
        );
        missing_ca.tls.root_ca = Some("/nonexistent/ca.pem".into());
        missing_ca
            .validate()
            .expect("the CA file is the builder's to load");
        assert!(
            refusal(ClickHouseSinkBuilder::new(missing_ca))
                .starts_with("sink.clickhouse.tls.root_ca: `/nonexistent/ca.pem`: "),
        );
    }

    /// Each rule names the key it refuses, with a shard element by its index
    /// in the form decode errors use.
    #[test]
    fn each_rule_names_the_key_it_refuses() {
        let check = |rest: &str| {
            format!("shards: [{{replicas: [\"http://a\"]}}]\ndistributed_check: {{{rest}}}\n")
        };
        let cases = [
            (
                "shards:\n  - replicas: [\"http://a\"]\n  - replicas: []\n".to_owned(),
                "shards[1].replicas: must not be empty",
            ),
            (
                "shards:\n  - replicas: [\"http://a\"]\n  - replicas: [\"http://b\", \"ftp://c\"]\n"
                    .to_owned(),
                "shards[1].replicas[1]: must be a URL with scheme http or https; found ftp",
            ),
            (
                "shards:\n  - replicas: [\"http://a\"]\n  - replicas: [\"http://b\"]\n    weight: 0\n"
                    .to_owned(),
                "shards[1].weight: must be at least 1",
            ),
            (
                "shards: [{replicas: [\"http://a\"]}]\ndatabase: \"bad-db\"\n".to_owned(),
                "database: `bad-db` is not a valid identifier",
            ),
            (
                check("cluster: \"bad-c\", table: d, sharding_key: id"),
                "distributed_check.cluster: `bad-c` is not a valid identifier",
            ),
            (
                check("cluster: c, table: \"a.b.c\", sharding_key: id"),
                "distributed_check.table: `a.b.c` is not a valid (optionally database-qualified) identifier",
            ),
            (
                check("cluster: c, table: d, sharding_key: id, sharding_expr: id"),
                "distributed_check: set exactly one of sharding_key or sharding_expr",
            ),
            (
                check("cluster: c, table: d, sharding_key: \"bad-k\""),
                "distributed_check.sharding_key: `bad-k` is not a valid identifier",
            ),
            (
                check("cluster: c, table: d, sharding_expr: \" \""),
                "distributed_check.sharding_expr: must not be empty",
            ),
            (
                check("cluster: c, table: d, sharding_key: id, endpoint: \"ftp://e\""),
                "distributed_check.endpoint: must be a URL with scheme http or https; found ftp",
            ),
            (
                "shards: [{replicas: [\"http://a\"]}]\ntls: {root_ca: /etc/ca.pem}\n".to_owned(),
                "tls.root_ca: is set, but no replica or distributed_check.endpoint is https://",
            ),
            (
                "shards: [{replicas: [\"http://u:hunter2@exa mple:8123\"]}]\n".to_owned(),
                "shards[0].replicas[0]: is not a URL: invalid international domain name",
            ),
        ];
        for (body, want) in cases {
            let err = refusal(ClickHouseSinkBuilder::from_component_config(&component(
                &format!("table: t\n{body}"),
            )));
            assert_eq!(err, format!("sink.clickhouse.{want}"), "{body}");
        }
        let expr = check("cluster: c, table: d, sharding_expr: \"cityHash64(id)\"");
        if let Err(e) =
            ClickHouseSinkBuilder::from_component_config(&component(&format!("table: t\n{expr}")))
        {
            panic!("a non-empty sharding_expr must build: {e}");
        }
        let err = refusal(ClickHouseSinkBuilder::from_component_config(&component(
            "table: \"a.b.c\"\nshards: [{replicas: [\"http://a\"]}]\n",
        )));
        assert_eq!(
            err,
            "sink.clickhouse.table: `a.b.c` is not a valid (optionally database-qualified) identifier"
        );
    }
}
