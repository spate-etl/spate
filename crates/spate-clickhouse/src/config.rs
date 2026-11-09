//! Opaque-section configuration and the sink factory.
//!
//! The `sink: { clickhouse: { ... } }` YAML section deserializes into
//! [`ClickHouseSinkConfig`]; [`from_component_config`] validates it and
//! produces a [`ClickHouseSinkBuilder`], per-shard replica endpoints, and the
//! framework's [`SinkPoolConfig`]. [`ClickHouseSinkBuilder::with_row`]
//! supplies the row type and produces the runnable [`ClickHouseSink`],
//! including its writer.

use crate::codec::{BlockCodec, BlockMethod};
use crate::distributed::{self, DistributedCheckError};
use crate::router::{DistributedRouter, KeyExtractor};
use crate::schema::{self, RowSchema, SchemaError};
use crate::writer::{ClickHouseEndpoint, ClickHouseWriter};
use bytes::Bytes;
use rustls::ClientConfig;
use rustls_native_certs::CertificateResult;
use serde::{Deserialize, Deserializer, de};
use spate_core::config::{ComponentConfig, ConfigError, SectionPath, redact};
use spate_core::deser::RecFamily;
use spate_core::sink::{
    BatchConfig, BreakerConfig, FrameCodecFactory, InflightConfig, RetryConfig, SinkBundle,
    SinkParts, SinkPoolConfig, SinkProbeFn, endpoint_probe,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
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
///       - replicas: ["http://ch-0-0:8123", "http://ch-0-1:8123"]
///       - replicas: ["http://ch-1-0:8123", "http://ch-1-1:8123"]
///     user: default
///     password: ${CLICKHOUSE_PASSWORD}
///     batch: { max_rows: 500000, max_bytes: 128MiB, linger: 1s }
///     inflight: { max_per_shard: 2 }
///     retry: { initial: 100ms, max: 10s, multiplier: 2.0, jitter: 0.2, max_attempts: 0 }
///     breaker: { failure_threshold: 3, open_for: 5s, half_open_probes: 1 }
///     timeouts: { send: 30s, end: 180s }
///     compression: lz4               # off | lz4 | zstd | zstd:<1-22>
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
    pub shards: Vec<ShardConfig>,
    /// Default database for unqualified tables.
    #[serde(default)]
    pub database: Option<String>,
    /// Username (interpolate secrets upstream via `${VAR}`).
    #[serde(default)]
    pub user: Option<String>,
    /// Password (interpolate secrets upstream via `${VAR}`).
    #[serde(default)]
    pub password: Option<String>,
    /// Trust settings for `https://` replicas (see [`TlsSection`]).
    #[serde(default)]
    pub tls: TlsSection,
    /// Extra per-insert ClickHouse settings (beyond the deduplication
    /// settings this sink always sets).
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
    /// Batch sealing thresholds.
    #[serde(default)]
    pub batch: BatchConfig,
    /// Concurrent in-flight batches per shard.
    #[serde(default)]
    pub inflight: InflightConfig,
    /// Retry/backoff policy for failed writes.
    #[serde(default)]
    pub retry: RetryConfig,
    /// Per-replica circuit breaker.
    #[serde(default)]
    pub breaker: BreakerConfig,
    /// Client-side send/end timeouts.
    #[serde(default)]
    pub timeouts: TimeoutSection,
    /// Transport compression of insert bodies. `lz4` by
    /// default (see [`Compression`]).
    #[serde(default)]
    pub compression: Compression,
    /// Insert wire format (see [`Format`]). `rowbinary` by default;
    /// `native` selects the columnar block format.
    #[serde(default)]
    pub format: Format,
    /// Opt-in startup parity check against the cluster topology and a
    /// `Distributed` table's DDL (see [`DistributedCheckSection`]).
    /// Absent by default: no queries issued.
    #[serde(default)]
    pub distributed_check: Option<DistributedCheckSection>,
}

// Hand-written: `password` and the `settings` values are credentials. The
// destructure lists every field so a new one cannot reach `Debug` unredacted.
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
            batch,
            inflight,
            retry,
            breaker,
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
            .field("batch", batch)
            .field("inflight", inflight)
            .field("retry", retry)
            .field("breaker", breaker)
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
/// Pair Native with [`crate::NativeEncoder`] on the chain (via
/// [`ClickHouseSink::native_schema`]); RowBinary pairs with
/// [`crate::ClickHouseEncoder`].
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Format {
    /// Row-wise RowBinary (default).
    #[default]
    RowBinary,
    /// Columnar Native (self-describing blocks).
    Native,
}

impl Format {
    /// The `FORMAT` keyword for the `INSERT` statement.
    fn keyword(self) -> &'static str {
        match self {
            Format::RowBinary => "RowBinaryWithNamesAndTypes",
            Format::Native => "Native",
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
/// compression: lz4         # off | none | lz4 | zstd | zstd:<1-22>
/// ```
///
/// `lz4` is fast and low-CPU (the default); `zstd`
/// trades CPU for a better ratio and accepts an explicit level (`zstd` alone
/// uses level 3). `off` disables compression.
///
/// Insert bodies are compressed on the pipeline threads: the sink's frame
/// codec ([`ClickHouseSink::frame_codec`]) turns each chunk into one
/// compressed block when the chunk seals, reusing one zstd context per
/// pipeline thread, and the writer sends the blocks with `decompress=1`. A
/// retry resends the stored blocks. Responses to the sink's own queries
/// arrive uncompressed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Compression {
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

impl FromStr for Compression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "off" | "none" => Ok(Compression::None),
            "lz4" => Ok(Compression::Lz4),
            "zstd" => Ok(Compression::Zstd(ZSTD_DEFAULT_LEVEL)),
            other => {
                let (lo, hi) = (ZSTD_LEVELS.start(), ZSTD_LEVELS.end());
                let raw = other.strip_prefix("zstd:").ok_or_else(|| {
                    format!("unknown compression: expected off, lz4, zstd, or zstd:<{lo}-{hi}>")
                })?;
                let level: i32 = raw.parse().map_err(|_| {
                    format!("invalid zstd level: expected an integer in [{lo}, {hi}]")
                })?;
                if !ZSTD_LEVELS.contains(&level) {
                    return Err(format!("zstd level must be in [{lo}, {hi}]"));
                }
                Ok(Compression::Zstd(level))
            }
        }
    }
}

impl<'de> Deserialize<'de> for Compression {
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
    pub fn new(table: impl Into<String>, shards: Vec<ShardConfig>) -> ClickHouseSinkConfig {
        ClickHouseSinkConfig {
            table: table.into(),
            shards,
            database: None,
            user: None,
            password: None,
            tls: TlsSection::default(),
            settings: BTreeMap::new(),
            batch: BatchConfig::default(),
            inflight: InflightConfig::default(),
            retry: RetryConfig::default(),
            breaker: BreakerConfig::default(),
            timeouts: TimeoutSection::default(),
            compression: Compression::default(),
            format: Format::default(),
            distributed_check: None,
        }
    }

    /// The worker-pool tuning the four pool sections describe.
    fn pool(&self) -> SinkPoolConfig {
        SinkPoolConfig::new(self.batch, self.inflight, self.retry, self.breaker)
    }
}

/// One shard's replica endpoints.
///
/// Construct with [`ShardConfig::new`] and set the optional fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ShardConfig {
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
impl std::fmt::Debug for ShardConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ShardConfig { replicas, weight } = self;
        f.debug_struct("ShardConfig")
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

impl ShardConfig {
    /// A shard served by `replicas`. The weight starts at its YAML
    /// default.
    #[must_use]
    pub fn new(replicas: Vec<String>) -> ShardConfig {
        ShardConfig {
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
/// Construct with [`DistributedCheckSection::new`] and set the optional
/// fields. The struct is `#[non_exhaustive]` so new knobs can be added
/// without breaking callers.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DistributedCheckSection {
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
impl std::fmt::Debug for DistributedCheckSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let DistributedCheckSection {
            cluster,
            table,
            sharding_key,
            sharding_expr,
            endpoint,
        } = self;
        f.debug_struct("DistributedCheckSection")
            .field("cluster", cluster)
            .field("table", table)
            .field("sharding_key", sharding_key)
            .field("sharding_expr", sharding_expr)
            .field("endpoint", &endpoint.as_deref().map(redact::url))
            .finish()
    }
}

impl DistributedCheckSection {
    /// A check of `table` on `cluster`, sharded by `sharding_key`. Every
    /// other field starts at its YAML default.
    ///
    /// To compare against a full expression instead, clear `sharding_key`
    /// and set `sharding_expr`: [`build`] takes exactly one of the two.
    #[must_use]
    pub fn new(
        cluster: impl Into<String>,
        table: impl Into<String>,
        sharding_key: impl Into<String>,
    ) -> DistributedCheckSection {
        DistributedCheckSection {
            cluster: cluster.into(),
            table: table.into(),
            sharding_key: Some(sharding_key.into()),
            sharding_expr: None,
            endpoint: None,
        }
    }
}

/// Trust settings for `https://` replicas. The URL scheme decides whether a
/// connection uses TLS.
///
/// ```yaml
/// sink:
///   clickhouse:
///     tls:
///       root_ca: /etc/spate/clickhouse-ca.pem
/// ```
///
/// Construct with [`TlsSection::default`] and set the fields. The struct is
/// `#[non_exhaustive]` so new knobs can be added without breaking callers.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TlsSection {
    /// PEM bundle of root CAs trusted in addition to the system trust store.
    /// Requires at least one `https://` URL.
    #[serde(default)]
    pub root_ca: Option<PathBuf>,
}

/// Client-side timeouts for one insert.
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
}

impl Default for TimeoutSection {
    fn default() -> Self {
        TimeoutSection {
            send: Some(Duration::from_secs(30)),
            end: Some(Duration::from_secs(180)),
        }
    }
}

/// A validated sink configuration with no row type yet.
/// [`with_row`](Self::with_row) supplies it, fetches the table's schema, and
/// produces the runnable [`ClickHouseSink`].
#[derive(Debug)]
#[must_use = "await with_row::<F>() to get a runnable ClickHouseSink"]
pub struct ClickHouseSinkBuilder {
    cfg: ClickHouseSinkConfig,
    endpoints: Vec<Vec<ClickHouseEndpoint>>,
    probe_endpoints: Arc<Vec<Vec<ClickHouseEndpoint>>>,
    shard_weights: Arc<[u32]>,
    pool: SinkPoolConfig,
    distributed: Option<distributed::DistributedCheck>,
}

impl ClickHouseSinkBuilder {
    /// Supplies the row type and fetches the table's schema, producing the
    /// runnable sink.
    ///
    /// The `INSERT` column list comes from
    /// [`ClickHouseRowFamily::COLUMNS`](crate::ClickHouseRowFamily::COLUMNS).
    /// `system.columns` is read from every replica of every shard and checked
    /// against that list, failing with a readable diff for a missing or
    /// non-insertable column, replica drift, or a missing table. The fetched
    /// types describe the rows on the wire in both formats, so there is no
    /// sink without them.
    ///
    /// Run it on the builder's I/O runtime before the chain exists, via
    /// `pipeline.block_on`. `F` is not inferable from context, so name it:
    /// `builder.with_row::<Owned<OrderRow>>()`.
    pub async fn with_row<F: crate::ClickHouseRowFamily>(
        self,
    ) -> Result<ClickHouseSink, SchemaError> {
        validate_columns(F::COLUMNS)
            .map_err(|why| SchemaError::Columns(format!("sink.clickhouse: {why}")))?;

        let check = schema::SchemaCheck {
            wire: match self.cfg.format {
                Format::RowBinary => schema::Wire::RowBinary,
                Format::Native => schema::Wire::Native,
            },
            database: self.cfg.database.clone(),
            table: self.cfg.table.clone(),
            columns: F::COLUMNS,
        };
        let schema = schema::validate(&check, &self.endpoints).await?;

        let insert_sql = insert_statement(&self.cfg.table, F::COLUMNS, self.cfg.format);
        let frame_codec = BlockCodec::factory(self.cfg.compression);
        // A Native block names its own columns and types; a RowBinary body
        // carries them once, ahead of the rows, in the same blocks as the rows.
        let header = match (self.cfg.format, &frame_codec) {
            (Format::Native, _) => None,
            (Format::RowBinary, None) => Some(schema.header()),
            (Format::RowBinary, Some(codec)) => {
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
        tracing::info!(
            table = %self.cfg.table,
            columns = ?F::COLUMNS,
            "clickhouse insert columns",
        );

        Ok(ClickHouseSink {
            writer,
            endpoints: self.endpoints,
            pool: self.pool,
            frame_codec,
            format: self.cfg.format,
            schema,
            probe_endpoints: self.probe_endpoints,
            shard_weights: self.shard_weights,
            distributed: self.distributed,
        })
    }

    /// A [`DistributedRouter`] over the configured shard topology, before the
    /// schema fetch. Same placement as [`ClickHouseSink::router`], which is
    /// the one to use once the sink exists.
    ///
    /// `F` is not inferable from the extractor fn item, so name it:
    /// `builder.router::<Owned<OrderLineRow>>(order_key)`.
    #[must_use]
    pub fn router<F: RecFamily>(&self, extract: KeyExtractor<F>) -> DistributedRouter<F> {
        DistributedRouter::new(extract, &self.shard_weights)
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
    /// Pool knobs mapped onto the framework's configuration.
    pub pool: SinkPoolConfig,
    /// The configured insert wire format.
    format: Format,
    /// Codes each chunk at seal under `lz4` or `zstd`; `None` under `off`.
    frame_codec: Option<FrameCodecFactory>,
    /// The live table's columns, fetched at `with_row`.
    schema: Arc<RowSchema>,
    /// An independent client set for readiness probing: sharing the insert
    /// clients would report the write path healthy because probing keeps
    /// its connections warm.
    probe_endpoints: Arc<Vec<Vec<ClickHouseEndpoint>>>,
    /// Per-shard weights in config order, for [`router`](Self::router).
    shard_weights: Arc<[u32]>,
    /// The captured `distributed_check` block, if configured.
    distributed: Option<distributed::DistributedCheck>,
}

impl ClickHouseSink {
    /// The live table's columns, in the row struct's field order, as
    /// [`with_row`](ClickHouseSinkBuilder::with_row) fetched them. Hand it to
    /// [`crate::ClickHouseEncoder::with_schema`] for the first-record struct
    /// check.
    #[must_use]
    pub fn schema(&self) -> Arc<RowSchema> {
        Arc::clone(&self.schema)
    }

    /// The configured insert wire format.
    #[must_use]
    pub fn format(&self) -> Format {
        self.format
    }

    /// The frame codec that turns each sealed chunk into one compressed
    /// block, or `None` under `compression: off`.
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

    /// A [`NativeSchema`](crate::native::NativeSchema) over the fetched
    /// columns, for a [`crate::NativeEncoder`]. Fails for a column type the
    /// Native encoder cannot lay out, before any row is encoded.
    pub fn native_schema(&self) -> Result<Arc<crate::native::NativeSchema>, crate::NativeError> {
        crate::native::NativeSchema::from_row_schema(&self.schema)
    }

    /// A readiness probe over every replica of every shard, using the
    /// sink's independent probe client set (never the insert clients).
    /// This is the probe [`SinkBundle::into_parts`] attaches; manual
    /// assemblies hand it to `SinkRuntime.probe` directly.
    pub fn probe_fn(&self) -> SinkProbeFn {
        endpoint_probe(self.writer.clone(), Arc::clone(&self.probe_endpoints))
    }

    /// A [`DistributedRouter`] over this sink's shard topology and
    /// configured weights. Its placement matches a `Distributed` table with
    /// sharding expression `xxHash64(<key column>)`. Infallible: the weights
    /// were validated at [`build`].
    ///
    /// `F` is not inferable from the extractor fn item (`Rec<'buf>`
    /// projections are not injective), so name it:
    /// `sink.router::<Owned<OrderLineRow>>(order_key)`.
    #[must_use]
    pub fn router<F: RecFamily>(&self, extract: KeyExtractor<F>) -> DistributedRouter<F> {
        DistributedRouter::new(extract, &self.shard_weights)
            .expect("config validation guarantees at least one shard and weights >= 1")
    }

    /// Opt-in startup DDL-parity guard. Instant `Ok(())` when no
    /// `distributed_check` block is configured. Otherwise verifies shard
    /// count, per-shard weights, and the `Distributed` table's sharding
    /// expression against the live cluster, failing fast with a readable
    /// diff. Placement/DDL drift does not error at query time; it silently
    /// returns wrong results under `optimize_skip_unused_shards`.
    ///
    /// Call **after** [`with_row`](ClickHouseSinkBuilder::with_row) and
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
        let parts = SinkParts::new(self.writer, self.endpoints, self.pool)
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

/// Build a [`ClickHouseSinkBuilder`] from the opaque `sink: { clickhouse: ... }`
/// component section.
///
/// Errors start at the section's path, such as `sinks.eu.clickhouse.table`
/// for a `sinks:` entry, or `clickhouse.table` for a section built with
/// [`ComponentConfig::new`] and never placed in a pipeline config.
///
/// # Errors
///
/// [`ConfigError::Component`] when the section does not deserialize into
/// [`ClickHouseSinkConfig`], and otherwise as [`build`].
pub fn from_component_config(
    section: &ComponentConfig,
) -> Result<ClickHouseSinkBuilder, ConfigError> {
    let cfg: ClickHouseSinkConfig = section.deserialize_into()?;
    build_with(cfg, &section.path(), rustls_native_certs::load_native_certs)
}

/// Build from an already-deserialized config (programmatic use).
///
/// Reads the system trust store and `tls.root_ca` when any replica or
/// `distributed_check.endpoint` is `https://`.
///
/// Errors start at `sink.clickhouse`, such as `sink.clickhouse.table`.
///
/// # Errors
///
/// [`ConfigError::Component`] when `cfg` breaks a configuration rule, or when
/// `tls.root_ca` cannot be loaded.
pub fn build(cfg: ClickHouseSinkConfig) -> Result<ClickHouseSinkBuilder, ConfigError> {
    build_with(
        cfg,
        &SectionPath::sink(TAG),
        rustls_native_certs::load_native_certs,
    )
}

fn build_with(
    cfg: ClickHouseSinkConfig,
    at: &SectionPath,
    system_roots: impl FnOnce() -> CertificateResult,
) -> Result<ClickHouseSinkBuilder, ConfigError> {
    validate(&cfg, at)?;
    let roots = crate::http::root_store(&cfg.tls, uses_https(&cfg), system_roots)
        .map_err(|why| at.invalid("tls.root_ca", why))?;
    let tls = crate::http::client_config(roots);

    // Two independent client sets: inserts and readiness probes must not
    // share connection pools (see `ClickHouseSink::probe_endpoints`).
    let endpoints = make_endpoints(&cfg, &tls);
    let probe_endpoints = Arc::new(make_endpoints(&cfg, &tls));

    let shard_weights: Arc<[u32]> = cfg.shards.iter().map(|s| s.weight).collect();
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

    let pool = cfg.pool();

    Ok(ClickHouseSinkBuilder {
        cfg,
        endpoints,
        probe_endpoints,
        shard_weights,
        pool,
        distributed,
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

fn validate(cfg: &ClickHouseSinkConfig, at: &SectionPath) -> Result<(), ConfigError> {
    if cfg.shards.is_empty() {
        return Err(at.invalid("shards", "at least one shard is required"));
    }
    for (i, shard) in cfg.shards.iter().enumerate() {
        if shard.replicas.is_empty() {
            return Err(at.invalid(&format!("shards[{i}].replicas"), "must not be empty"));
        }
        for (j, url) in shard.replicas.iter().enumerate() {
            let key = format!("shards[{i}].replicas[{j}]");
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(at.invalid(
                    &key,
                    format!("`{}` is not an http(s) URL", redact::url(url)),
                ));
            }
            if has_credentials(url) {
                return Err(at.invalid(
                    &key,
                    "URL contains credentials; set them in user and password",
                ));
            }
        }
        // Weights are ClickHouse interval widths: a zero-weight shard
        // receives nothing under Distributed parity and breaks the
        // prefix-sum selection.
        if shard.weight == 0 {
            return Err(at.invalid(&format!("shards[{i}].weight"), "must be at least 1"));
        }
    }
    let table_parts: Vec<&str> = cfg.table.split('.').collect();
    if cfg.table.is_empty()
        || table_parts.len() > 2
        || !table_parts.iter().all(|p| is_identifier(p))
    {
        return Err(at.invalid(
            "table",
            format!(
                "`{}` is not a valid (optionally database-qualified) identifier",
                cfg.table
            ),
        ));
    }
    if let Some(db) = &cfg.database
        && !is_identifier(db)
    {
        return Err(at.invalid("database", format!("`{db}` is not a valid identifier")));
    }
    if let Err(why) = cfg.pool().validate() {
        return Err(at.error(why.to_string()));
    }

    // Compression: the string parser bounds the level, and a programmatic
    // `build()` caller can construct `Zstd(level)` directly. The same bound
    // here makes a config built in code accept the levels YAML accepts.
    if let Compression::Zstd(level) = cfg.compression
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

    if let Err(e) = crate::writer::RESERVED.check("settings", cfg.settings.keys()) {
        return Err(at.error(e.to_string()));
    }

    if let Some(check) = &cfg.distributed_check {
        if !is_identifier(&check.cluster) {
            return Err(at.invalid(
                "distributed_check.cluster",
                format!("`{}` is not a valid identifier", check.cluster),
            ));
        }
        let parts: Vec<&str> = check.table.split('.').collect();
        if check.table.is_empty() || parts.len() > 2 || !parts.iter().all(|p| is_identifier(p)) {
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
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(at.invalid(
                    "distributed_check.endpoint",
                    format!("`{}` is not an http(s) URL", redact::url(url)),
                ));
            }
            if has_credentials(url) {
                return Err(at.invalid(
                    "distributed_check.endpoint",
                    "URL contains credentials; set them in user and password",
                ));
            }
        }
    }
    if cfg.tls.root_ca.is_some() && !uses_https(cfg) {
        return Err(at.invalid(
            "tls.root_ca",
            "is set, but no replica or distributed_check.endpoint is https://",
        ));
    }
    Ok(())
}

/// Whether `url` carries userinfo, as the `clickhouse` client's URL parser
/// reads it.
fn has_credentials(url: &str) -> bool {
    url::Url::parse(url).is_ok_and(|u| !u.username().is_empty() || u.password().is_some())
}

/// Whether any replica or the `distributed_check` endpoint is `https://`.
fn uses_https(cfg: &ClickHouseSinkConfig) -> bool {
    let check = cfg.distributed_check.as_ref();
    cfg.shards
        .iter()
        .flat_map(|shard| &shard.replicas)
        .chain(check.and_then(|c| c.endpoint.as_ref()))
        .any(|url| url.starts_with("https://"))
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

/// The runtime backstop for [`ClickHouseSinkBuilder::with_row`]:
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

fn insert_statement(table: &str, columns: &[&str], format: Format) -> String {
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
    use serde::Serialize;
    use spate_core::config::ComponentConfig;
    use spate_core::deser::Owned;
    use spate_core::sink::{BreakerConfigError, RetryConfigError, SinkPoolConfigError};

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

        let builder = from_component_config(&component(
            "table: orders\nshards: [{replicas: ['http://a:8123/?password=hunter2']}]\n",
        ))
        .unwrap();
        let printed = format!("{builder:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(printed.contains("http://a:8123/?<redacted>"), "{printed}");

        let err = refusal(from_component_config(&component(
            "table: orders\nshards: [{replicas: ['svc:hunter2@a:8123']}]\n",
        )));
        assert_eq!(
            err,
            "sink.clickhouse.shards[0].replicas[0]: `<redacted>@a:8123` is not an http(s) URL"
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
            let err = refusal(from_component_config(&component(&format!(
                "table: orders\n{yaml}"
            ))));
            assert_eq!(
                err,
                format!(
                    "sink.clickhouse.{key}: URL contains credentials; set them in user and password"
                )
            );
        }

        let builder = from_component_config(&component(
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
        let builder = from_component_config(&component(&yaml)).unwrap();
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
        let builder = from_component_config(&component(MINIMAL)).unwrap();
        assert_eq!(
            insert_statement("orders", IdName::COLUMNS, Format::RowBinary),
            "INSERT INTO `orders` (`id`, `name`) FORMAT RowBinaryWithNamesAndTypes"
        );
        assert_eq!(builder.endpoints.len(), 1);
        assert_eq!(builder.endpoints[0].len(), 1);
        assert_eq!(builder.pool, SinkPoolConfig::default());
    }

    #[test]
    fn qualified_table_and_knobs_map_through() {
        let sink = from_component_config(&component(
            r#"
table: analytics.orders
shards:
  - replicas: ["http://a:8123", "http://b:8123"]
  - replicas: ["http://c:8123"]
batch: { max_rows: 1000, max_bytes: 1MiB, linger: 250ms }
inflight: { max_per_shard: 4 }
retry: { initial: 50ms, max: 2s, multiplier: 3.0, jitter: 0.5, max_attempts: 7 }
breaker: { failure_threshold: 9, open_for: 30s, half_open_probes: 2 }
timeouts: { send: 5s, end: 60s }
settings: { insert_quorum: "auto" }
"#,
        ))
        .unwrap();
        assert_eq!(
            insert_statement("analytics.orders", IdOnly::COLUMNS, Format::RowBinary),
            "INSERT INTO `analytics`.`orders` (`id`) FORMAT RowBinaryWithNamesAndTypes"
        );
        assert_eq!(sink.endpoints.len(), 2);
        assert_eq!(sink.endpoints[0].len(), 2);
        assert_eq!(sink.pool.batch.max_rows, 1000);
        assert_eq!(sink.pool.batch.max_bytes, 1024 * 1024);
        assert_eq!(sink.pool.batch.linger, Duration::from_millis(250));
        assert_eq!(sink.pool.inflight.max_per_shard, 4);
        assert_eq!(sink.pool.retry.max_attempts, 7);
        assert_eq!(sink.pool.breaker.failure_threshold, 9);
    }

    /// The sink sets `decompress` on every compressed insert, so a configured
    /// value for it fails validation.
    #[test]
    fn decompress_is_a_reserved_setting() {
        let err = refusal(from_component_config(&component(&format!(
            "{MINIMAL}settings: {{decompress: \"0\"}}\n"
        ))));
        assert_eq!(
            err,
            "sink.clickhouse: settings.\"decompress\" cannot be \
             overridden: set to `1` on every compressed insert: the sink sends blocks it \
             compressed itself, and the server decodes them only with it on"
        );
    }

    #[test]
    fn every_setting_an_insert_carries_is_refused_in_settings() {
        for (name, _) in crate::writer::owned_settings("token", true) {
            let err = refusal(from_component_config(&component(&format!(
                "{MINIMAL}settings: {{{name}: \"0\"}}\n"
            ))));
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
            let err = from_component_config(&component(yaml)).unwrap_err();
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
            insert_statement("events", NestedTags::COLUMNS, Format::RowBinary),
            "INSERT INTO `events` (`id`, `tags.key`, `tags.value`) \
             FORMAT RowBinaryWithNamesAndTypes"
        );
    }

    /// `ClickHouseRowFamily` can be hand-written for a borrowed record
    /// family, bypassing the derive's compile-time checks entirely, so
    /// `with_row` re-runs them at runtime, before it reaches the server.
    #[tokio::test]
    async fn with_row_rejects_a_hand_written_familys_bad_columns() {
        use spate_core::deser::RecFamily;

        struct BadColumns;
        impl RecFamily for BadColumns {
            type Rec<'buf> = Vec<u8>;
        }
        impl crate::ClickHouseRowFamily for BadColumns {
            const COLUMNS: &'static [&'static str] = &["id", "id"];
        }

        // The replica is unroutable, so reaching it would surface as `Fetch`.
        let err = from_component_config(&component(
            "table: t\nshards: [{replicas: [\"http://127.0.0.1:1\"]}]",
        ))
        .unwrap()
        .with_row::<BadColumns>()
        .await
        .unwrap_err();
        let SchemaError::Columns(msg) = err else {
            panic!("the column check runs before the fetch: {err}");
        };
        assert_eq!(msg, "sink.clickhouse: column `id` is listed more than once");
    }

    #[test]
    fn validation_rejects_bad_retry_and_breaker() {
        // Every one of these reaches the write loop (or a broken breaker) at
        // runtime rather than failing at load if the rules are dropped. The
        // rules live in `SinkPoolConfig::validate`; this asserts the
        // ClickHouse sink still applies them.
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        let cases = [
            ("retry: { multiplier: 0.5 }", "multiplier"),
            ("retry: { multiplier: -2.0 }", "multiplier"),
            ("retry: { multiplier: .nan }", "multiplier"),
            ("retry: { multiplier: .inf }", "multiplier"),
            ("retry: { jitter: 1.5 }", "jitter"),
            ("retry: { jitter: -0.1 }", "jitter"),
            ("retry: { jitter: .nan }", "jitter"),
            ("retry: { initial: 0s }", "non-zero"),
            ("retry: { max: 0s }", "non-zero"),
            ("retry: { initial: 10s, max: 1s }", "must not exceed"),
            ("breaker: { failure_threshold: 0 }", "failure_threshold"),
            ("breaker: { half_open_probes: 0 }", "half_open_probes"),
        ];
        for (extra, needle) in cases {
            let yaml = format!("{base}{extra}");
            let err = from_component_config(&component(&yaml)).unwrap_err();
            assert!(
                err.to_string().contains(needle),
                "expected `{needle}` for `{extra}`: {err}"
            );
        }
    }

    #[test]
    fn valid_retry_and_breaker_still_build() {
        let sink = from_component_config(&component(
            "table: t\nshards: [{replicas: [\"http://a\"]}]\n\
             retry: { initial: 100ms, max: 10s, multiplier: 1.0, jitter: 0.0 }\n\
             breaker: { failure_threshold: 1, open_for: 5s, half_open_probes: 1 }",
        ));
        assert!(sink.is_ok(), "boundary-valid config must build: {sink:?}");
    }

    /// Each pool section is refused by the framework's rule, with the
    /// framework's message under the sink's prefix.
    #[test]
    fn pool_rules_are_core_rules_under_the_sink_prefix() {
        let cases = [
            ("batch: { max_rows: 0 }", SinkPoolConfigError::ZeroMaxRows),
            ("batch: { max_bytes: 0 }", SinkPoolConfigError::ZeroMaxBytes),
            (
                "batch: { max_rows: 0, max_bytes: 0 }",
                SinkPoolConfigError::ZeroMaxRows,
            ),
            (
                "batch: { max_rows: 0 }\ninflight: { max_per_shard: 0 }",
                SinkPoolConfigError::ZeroMaxRows,
            ),
            (
                "inflight: { max_per_shard: 0 }",
                SinkPoolConfigError::ZeroMaxPerShard,
            ),
            (
                "retry: { multiplier: 0.5 }",
                SinkPoolConfigError::Retry(RetryConfigError::Multiplier(0.5)),
            ),
            (
                "breaker: { failure_threshold: 0 }",
                SinkPoolConfigError::Breaker(BreakerConfigError::ZeroFailureThreshold),
            ),
            (
                "breaker: { half_open_probes: 0 }",
                SinkPoolConfigError::Breaker(BreakerConfigError::ZeroHalfOpenProbes),
            ),
            (
                "breaker: { failure_threshold: 0, half_open_probes: 0 }",
                SinkPoolConfigError::Breaker(BreakerConfigError::ZeroFailureThreshold),
            ),
        ];
        for (extra, want) in cases {
            let yaml = format!("{MINIMAL}{extra}\n");
            let msg = refusal(from_component_config(&component(&yaml)));
            assert_eq!(msg, format!("sink.clickhouse: {want}"), "for `{extra}`");
        }
    }

    /// The pool check runs after the `database` check and before the
    /// reserved-settings check, so a config with two faults reports the
    /// same one whichever way the pool rules are written.
    #[test]
    fn a_pool_refusal_keeps_its_place_in_validate() {
        let earlier = format!("{MINIMAL}database: \"bad-db\"\nbatch: {{ max_rows: 0 }}\n");
        let msg = refusal(from_component_config(&component(&earlier)));
        assert!(msg.starts_with("sink.clickhouse.database: "), "{msg}");

        let later =
            format!("{MINIMAL}settings: {{ decompress: \"0\" }}\nbatch: {{ max_rows: 0 }}\n");
        let msg = refusal(from_component_config(&component(&later)));
        assert_eq!(
            msg,
            format!("sink.clickhouse: {}", SinkPoolConfigError::ZeroMaxRows)
        );
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
            let err =
                from_component_config(&component(&format!("{base}settings: {{ {key}: \"0\" }}")))
                    .expect_err("a managed setting");
            assert!(err.to_string().contains(key), "{err}");
        }
    }

    #[test]
    fn unknown_fields_are_rejected_with_a_path() {
        let err = from_component_config(&component(
            "table: orders\nshards: [{replicas: [\"http://a\"]}]\nbatch: {max_rowz: 5}",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("max_rowz"), "{err}");
    }

    /// A YAML still holding the removed `columns` key fails to load, naming
    /// the key, rather than silently ignoring it or shadowing the derived
    /// column list.
    #[test]
    fn a_stale_columns_key_is_rejected_with_a_path() {
        let err = from_component_config(&component(
            "table: orders\ncolumns: [id]\nshards: [{replicas: [\"http://a\"]}]",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("columns"), "{err}");
    }

    #[test]
    fn compression_parses_and_defaults_to_lz4() {
        let base = "table: t\nshards: [{replicas: [\"http://a\"]}]\n";
        for (yaml, expected) in [
            ("", Compression::Lz4),
            ("compression: off\n", Compression::None),
            ("compression: none\n", Compression::None),
            ("compression: lz4\n", Compression::Lz4),
            ("compression: zstd\n", Compression::Zstd(ZSTD_DEFAULT_LEVEL)),
            ("compression: \"zstd:9\"\n", Compression::Zstd(9)),
            ("compression: \"zstd:1\"\n", Compression::Zstd(1)),
            ("compression: \"zstd:22\"\n", Compression::Zstd(22)),
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
            ("", Format::RowBinary),
            ("format: rowbinary\n", Format::RowBinary),
            ("format: native\n", Format::Native),
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
            insert_statement("t", IdName::COLUMNS, Format::Native),
            "INSERT INTO `t` (`id`, `name`) FORMAT Native"
        );
        assert_eq!(
            insert_statement("t", IdName::COLUMNS, Format::RowBinary),
            "INSERT INTO `t` (`id`, `name`) FORMAT RowBinaryWithNamesAndTypes"
        );
    }

    #[test]
    fn validation_rejects_out_of_range_programmatic_zstd_level() {
        let mut cfg =
            ClickHouseSinkConfig::new("t", vec![ShardConfig::new(vec!["http://a".into()])]);
        cfg.compression = Compression::Zstd(99);
        let err = build(cfg).unwrap_err();
        assert!(err.to_string().contains("[1, 22]"), "{err}");
    }

    /// A config built in code accepts the zstd levels the YAML parser
    /// accepts, and no others.
    #[test]
    fn build_accepts_zstd_levels_one_through_twenty_two_only() {
        let zstd = |level| {
            let mut cfg =
                ClickHouseSinkConfig::new("t", vec![ShardConfig::new(vec!["http://a".into()])]);
            cfg.compression = Compression::Zstd(level);
            cfg
        };
        for level in [1, 22] {
            if let Err(e) = build(zstd(level)) {
                panic!("zstd level {level} must build: {e}");
            }
        }
        for level in [0, 23] {
            let msg = refusal(build(zstd(level)));
            assert_eq!(
                msg,
                format!("sink.clickhouse.compression: zstd level must be in [1, 22] (got {level})")
            );
        }
    }

    #[test]
    fn build_refuses_a_zero_batch_threshold_in_a_programmatic_config() {
        let mut cfg =
            ClickHouseSinkConfig::new("t", vec![ShardConfig::new(vec!["http://a".into()])]);
        cfg.batch.max_bytes = 0;
        let msg = refusal(build(cfg));
        assert_eq!(
            msg,
            format!("sink.clickhouse: {}", SinkPoolConfigError::ZeroMaxBytes)
        );
    }

    /// The zstd level check runs after the pool rules: a config built in
    /// code with a bad level and a refused pool value reports the pool key.
    #[test]
    fn a_zstd_level_refusal_keeps_its_place_against_the_pool_rules() {
        let zstd_99 = || {
            let mut cfg =
                ClickHouseSinkConfig::new("t", vec![ShardConfig::new(vec!["http://a".into()])]);
            cfg.compression = Compression::Zstd(99);
            cfg
        };

        let mut cfg = zstd_99();
        cfg.breaker.failure_threshold = 0;
        let msg = refusal(build(cfg));
        let want = SinkPoolConfigError::Breaker(BreakerConfigError::ZeroFailureThreshold);
        assert_eq!(msg, format!("sink.clickhouse: {want}"));

        let mut cfg = zstd_99();
        cfg.retry.multiplier = 0.5;
        let msg = refusal(build(cfg));
        let want = SinkPoolConfigError::Retry(RetryConfigError::Multiplier(0.5));
        assert_eq!(msg, format!("sink.clickhouse: {want}"));
    }

    /// `new` and the YAML defaults are two spellings of one config, so a
    /// knob added to the struct has to reach both.
    #[test]
    fn new_matches_the_yaml_defaults() {
        let from_yaml: ClickHouseSinkConfig = serde_yaml::from_str(MINIMAL).unwrap();
        assert_eq!(
            ClickHouseSinkConfig::new(
                "orders",
                vec![ShardConfig::new(vec!["http://a:8123".into()])],
            ),
            from_yaml
        );
    }

    /// The nested section has the same two spellings, and `new` must
    /// produce one `build` accepts rather than one it rejects.
    #[test]
    fn distributed_check_new_matches_the_yaml_defaults_and_builds() {
        let from_yaml: DistributedCheckSection =
            serde_yaml::from_str("cluster: prod\ntable: db.t_dist\nsharding_key: id\n").unwrap();
        assert_eq!(
            DistributedCheckSection::new("prod", "db.t_dist", "id"),
            from_yaml
        );

        let mut cfg =
            ClickHouseSinkConfig::new("t", vec![ShardConfig::new(vec!["http://a:8123".into()])]);
        cfg.distributed_check = Some(DistributedCheckSection::new("prod", "db.t_dist", "id"));
        let _ = build(cfg).expect("a config built entirely from `new` passes validation");
    }

    /// An `https://` replica signed by the CA that `SSL_CERT_FILE` names is
    /// trusted through `build`. Regression for #619.
    #[tokio::test(flavor = "multi_thread")]
    async fn build_trusts_a_ca_from_the_system_trust_store() {
        const NAME: &str = "config::tests::build_trusts_a_ca_from_the_system_trust_store";
        const URL: &str = "SPATE_TEST_CLICKHOUSE_TLS_URL";
        if let Ok(url) = std::env::var(URL) {
            let builder = build(ClickHouseSinkConfig::new(
                "t",
                vec![ShardConfig::new(vec![url])],
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
        let http = ClickHouseSinkConfig::new("t", vec![ShardConfig::new(vec!["http://a".into()])]);
        let _ = build_with(http.clone(), &SectionPath::sink(TAG), || {
            panic!("the trust store was read")
        })
        .unwrap();

        let mut front = http;
        let mut check = DistributedCheckSection::new("prod", "db.t_dist", "id");
        check.endpoint = Some("https://front:8443".into());
        front.distributed_check = Some(check);
        let https =
            ClickHouseSinkConfig::new("t", vec![ShardConfig::new(vec!["https://a".into()])]);
        for cfg in [front, https] {
            let mut read = false;
            let _ = build_with(cfg, &SectionPath::sink(TAG), || {
                read = true;
                CertificateResult::default()
            })
            .unwrap();
            assert!(read);
        }
    }

    #[test]
    fn tls_root_ca_parses_and_requires_an_https_url() {
        let yaml = "table: t\nshards: [{replicas: [\"https://a\"]}]\ntls: {root_ca: /etc/ca.pem}\n";
        let cfg: ClickHouseSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.tls.root_ca, Some(PathBuf::from("/etc/ca.pem")));

        let err = from_component_config(&component(&yaml.replace("https", "http"))).unwrap_err();
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
        let err = from_component_config(&component(
            "table: t\nshards: [{replicas: [\"http://a\"], weight: 0}]",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("weight"), "{err}");
    }

    #[test]
    fn sink_router_captures_config_weights_in_order() {
        use crate::router::ShardKey;

        let sink = from_component_config(&component(
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
        let sink = from_component_config(&component(
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
            let err = from_component_config(&component(&format!("{base}{check}"))).unwrap_err();
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
            let err = from_component_config(&component(&format!("{base}{check}"))).unwrap_err();
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
        let mut cfg = ClickHouseSinkConfig::new("t", vec![ShardConfig::new(vec![url])]);
        cfg.compression = Compression::Zstd(3);
        let builder = build(cfg).unwrap();
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
        let err = refusal(from_component_config(&sinks_entry(
            "table: t\nshards: []\n",
        )));
        assert_eq!(
            err,
            "sinks.eu.clickhouse.shards: at least one shard is required"
        );

        let err = refusal(from_component_config(&sinks_entry(&format!(
            "{MINIMAL}settings: {{insert_deduplicate: \"0\"}}\n"
        ))));
        assert!(
            err.starts_with(
                "sinks.eu.clickhouse: settings.\"insert_deduplicate\" cannot be overridden: "
            ),
            "{err}"
        );
    }

    /// An unusable `tls.root_ca` is refused at that key of the section it
    /// came from.
    #[test]
    fn an_unusable_root_ca_in_a_sinks_entry_names_the_entry() {
        let section = sinks_entry(
            "table: t\nshards: [{replicas: [\"https://a\"]}]\ntls: {root_ca: /nonexistent/ca.pem}\n",
        );
        let cfg: ClickHouseSinkConfig = section.deserialize_into().unwrap();
        let err = refusal(build_with(cfg, &section.path(), CertificateResult::default));
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
            refusal(from_component_config(&section)),
            "clickhouse.shards: at least one shard is required"
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
                "shards[1].replicas[1]: `ftp://c` is not an http(s) URL",
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
                "distributed_check.endpoint: `ftp://e` is not an http(s) URL",
            ),
            (
                "shards: [{replicas: [\"http://a\"]}]\ntls: {root_ca: /etc/ca.pem}\n".to_owned(),
                "tls.root_ca: is set, but no replica or distributed_check.endpoint is https://",
            ),
        ];
        for (body, want) in cases {
            let err = refusal(from_component_config(&component(&format!(
                "table: t\n{body}"
            ))));
            assert_eq!(err, format!("sink.clickhouse.{want}"), "{body}");
        }
        let expr = check("cluster: c, table: d, sharding_expr: \"cityHash64(id)\"");
        if let Err(e) = from_component_config(&component(&format!("table: t\n{expr}"))) {
            panic!("a non-empty sharding_expr must build: {e}");
        }
        let err = refusal(from_component_config(&component(
            "table: \"a.b.c\"\nshards: [{replicas: [\"http://a\"]}]\n",
        )));
        assert_eq!(
            err,
            "sink.clickhouse.table: `a.b.c` is not a valid (optionally database-qualified) identifier"
        );
    }
}
