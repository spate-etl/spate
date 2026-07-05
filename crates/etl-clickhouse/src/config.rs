//! Opaque-section configuration and the sink factory.
//!
//! The `sink: { clickhouse: { ... } }` YAML section deserializes into
//! [`ClickHouseSinkConfig`]; [`from_component_config`] validates it and
//! produces the writer, per-shard replica endpoints, and the framework's
//! [`SinkPoolConfig`].

use crate::writer::{ClickHouseEndpoint, ClickHouseWriter};
use bytesize::ByteSize;
use etl_core::config::{ComponentConfig, ConfigError};
use etl_core::sink::{BatchConfig, BreakerConfig, InflightConfig, RetryConfig, SinkPoolConfig};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;

/// The `clickhouse` sink section.
///
/// ```yaml
/// sink:
///   clickhouse:
///     table: orders_local            # or db.orders_local
///     columns: [id, name, amount]    # MUST match the row struct's field order
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
///     settings: { insert_quorum: "auto" }   # extra per-insert settings
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSinkConfig {
    /// Target table, optionally `database.table`-qualified.
    pub table: String,
    /// Column list for the `INSERT`. **Order is the wire contract**: it
    /// must match the row struct's field declaration order.
    pub columns: Vec<String>,
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
    /// Extra per-insert ClickHouse settings (beyond the deduplication
    /// settings this sink always sets).
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
    /// Batch sealing thresholds.
    #[serde(default)]
    pub batch: BatchSection,
    /// Concurrent in-flight batches per shard.
    #[serde(default)]
    pub inflight: InflightSection,
    /// Retry/backoff policy for failed writes.
    #[serde(default)]
    pub retry: RetrySection,
    /// Per-replica circuit breaker.
    #[serde(default)]
    pub breaker: BreakerSection,
    /// Client-side send/end timeouts.
    #[serde(default)]
    pub timeouts: TimeoutSection,
}

/// One shard's replica endpoints.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ShardConfig {
    /// HTTP(S) URLs of this shard's replicas.
    pub replicas: Vec<String>,
}

/// Batch sealing thresholds (defaults match the framework's).
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct BatchSection {
    /// Seal after this many rows.
    pub max_rows: u64,
    /// Seal after this many (uncompressed, encoded) bytes.
    pub max_bytes: ByteSize,
    /// Seal a partial batch after this long.
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
}

impl Default for BatchSection {
    fn default() -> Self {
        let d = BatchConfig::default();
        BatchSection {
            max_rows: d.max_rows,
            max_bytes: ByteSize(d.max_bytes),
            linger: d.linger,
        }
    }
}

/// In-flight batch limit per shard.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct InflightSection {
    /// Concurrent writes per shard (to different replicas).
    pub max_per_shard: usize,
}

impl Default for InflightSection {
    fn default() -> Self {
        InflightSection {
            max_per_shard: InflightConfig::default().max_per_shard,
        }
    }
}

/// Retry/backoff policy.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct RetrySection {
    /// First backoff delay.
    #[serde(with = "humantime_serde")]
    pub initial: Duration,
    /// Backoff cap.
    #[serde(with = "humantime_serde")]
    pub max: Duration,
    /// Backoff growth factor.
    pub multiplier: f64,
    /// Jitter fraction (`0..1`).
    pub jitter: f64,
    /// Attempt cap; `0` = unbounded (yields to the drain deadline).
    pub max_attempts: u32,
}

impl Default for RetrySection {
    fn default() -> Self {
        let d = RetryConfig::default();
        RetrySection {
            initial: d.initial,
            max: d.max,
            multiplier: d.multiplier,
            jitter: d.jitter,
            max_attempts: d.max_attempts,
        }
    }
}

/// Per-replica circuit breaker.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct BreakerSection {
    /// Consecutive failures before the replica is quarantined.
    pub failure_threshold: u32,
    /// Quarantine duration before a half-open probe.
    #[serde(with = "humantime_serde")]
    pub open_for: Duration,
    /// Probe writes allowed while half-open.
    pub half_open_probes: u32,
}

impl Default for BreakerSection {
    fn default() -> Self {
        let d = BreakerConfig::default();
        BreakerSection {
            failure_threshold: d.failure_threshold,
            open_for: d.open_for,
            half_open_probes: d.half_open_probes,
        }
    }
}

/// Client-side timeouts for one insert.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
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

/// Everything the framework needs to run this sink.
#[derive(Debug)]
pub struct ClickHouseSink {
    /// The `ShardWriter` implementation.
    pub writer: ClickHouseWriter,
    /// Per-shard replica endpoints, `shards[i][j]` = shard `i`, replica `j`.
    pub endpoints: Vec<Vec<ClickHouseEndpoint>>,
    /// Pool knobs mapped onto the framework's configuration.
    pub pool: SinkPoolConfig,
}

/// Build a [`ClickHouseSink`] from the opaque `sink: { clickhouse: ... }`
/// component section.
pub fn from_component_config(section: &ComponentConfig) -> Result<ClickHouseSink, ConfigError> {
    let cfg: ClickHouseSinkConfig = section.deserialize_into()?;
    build(cfg)
}

/// Build from an already-deserialized config (programmatic use).
pub fn build(cfg: ClickHouseSinkConfig) -> Result<ClickHouseSink, ConfigError> {
    validate(&cfg)?;

    let insert_sql = insert_statement(&cfg.table, &cfg.columns);
    let writer = ClickHouseWriter::new(
        insert_sql,
        cfg.settings.clone().into_iter().collect(),
        cfg.timeouts.send,
        cfg.timeouts.end,
    );

    let endpoints = cfg
        .shards
        .iter()
        .map(|shard| {
            shard
                .replicas
                .iter()
                .map(|url| {
                    let mut client = clickhouse::Client::default().with_url(url);
                    if let Some(db) = &cfg.database {
                        client = client.with_database(db);
                    }
                    if let Some(user) = &cfg.user {
                        client = client.with_user(user);
                    }
                    if let Some(password) = &cfg.password {
                        client = client.with_password(password);
                    }
                    ClickHouseEndpoint::new(client, url.clone())
                })
                .collect()
        })
        .collect();

    let pool = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: cfg.batch.max_rows,
            max_bytes: cfg.batch.max_bytes.as_u64(),
            linger: cfg.batch.linger,
        },
        inflight: InflightConfig {
            max_per_shard: cfg.inflight.max_per_shard,
        },
        retry: RetryConfig {
            initial: cfg.retry.initial,
            max: cfg.retry.max,
            multiplier: cfg.retry.multiplier,
            jitter: cfg.retry.jitter,
            max_attempts: cfg.retry.max_attempts,
        },
        breaker: BreakerConfig {
            failure_threshold: cfg.breaker.failure_threshold,
            open_for: cfg.breaker.open_for,
            half_open_probes: cfg.breaker.half_open_probes,
        },
    };

    Ok(ClickHouseSink {
        writer,
        endpoints,
        pool,
    })
}

fn validate(cfg: &ClickHouseSinkConfig) -> Result<(), ConfigError> {
    let fail = |msg: String| Err(ConfigError::Validation(format!("sink.clickhouse: {msg}")));

    if cfg.shards.is_empty() {
        return fail("at least one shard is required".into());
    }
    for (i, shard) in cfg.shards.iter().enumerate() {
        if shard.replicas.is_empty() {
            return fail(format!("shard {i} has no replicas"));
        }
        for url in &shard.replicas {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return fail(format!("replica `{url}` is not an http(s) URL"));
            }
        }
    }
    if cfg.columns.is_empty() {
        return fail("`columns` must list the insert columns in field order".into());
    }
    let mut seen = std::collections::HashSet::with_capacity(cfg.columns.len());
    for col in &cfg.columns {
        if !is_identifier(col) {
            return fail(format!("column `{col}` is not a valid identifier"));
        }
        // Duplicate columns emit e.g. `INSERT INTO t (`id`, `id`)`, which
        // ClickHouse rejects with DUPLICATE_COLUMN — a code the writer
        // classifies retryable, so it would loop forever. Reject at load.
        if !seen.insert(col.as_str()) {
            return fail(format!("column `{col}` is listed more than once"));
        }
    }
    let table_parts: Vec<&str> = cfg.table.split('.').collect();
    if cfg.table.is_empty()
        || table_parts.len() > 2
        || !table_parts.iter().all(|p| is_identifier(p))
    {
        return fail(format!(
            "table `{}` is not a valid (optionally database-qualified) identifier",
            cfg.table
        ));
    }
    if let Some(db) = &cfg.database
        && !is_identifier(db)
    {
        return fail(format!("database `{db}` is not a valid identifier"));
    }
    if cfg.batch.max_rows == 0 || cfg.batch.max_bytes.as_u64() == 0 {
        return fail("batch thresholds must be non-zero".into());
    }
    if cfg.inflight.max_per_shard == 0 {
        return fail("inflight.max_per_shard must be at least 1".into());
    }

    // Retry policy: these values flow unchecked into `Duration::mul_f64` in
    // the backoff, which panics on non-finite, negative, or overflowing
    // results — and a sub-1.0 multiplier or a zero delay yields a zero-delay
    // hot-retry loop hammering an already-failing replica. Validate at load.
    let retry = &cfg.retry;
    if !retry.multiplier.is_finite() || !(1.0..=1e9).contains(&retry.multiplier) {
        return fail(format!(
            "retry.multiplier must be a finite number in [1.0, 1e9] (got {})",
            retry.multiplier
        ));
    }
    if !retry.jitter.is_finite() || !(0.0..=1.0).contains(&retry.jitter) {
        return fail(format!(
            "retry.jitter must be a finite fraction in [0.0, 1.0] (got {})",
            retry.jitter
        ));
    }
    if retry.initial.is_zero() || retry.max.is_zero() {
        return fail("retry.initial and retry.max must be non-zero".into());
    }
    if retry.initial > retry.max {
        return fail(format!(
            "retry.initial ({:?}) must not exceed retry.max ({:?})",
            retry.initial, retry.max
        ));
    }

    // Circuit breaker: a zero failure threshold opens on the first outcome
    // and zero half-open probes never lets a replica recover.
    if cfg.breaker.failure_threshold == 0 {
        return fail("breaker.failure_threshold must be at least 1".into());
    }
    if cfg.breaker.half_open_probes == 0 {
        return fail("breaker.half_open_probes must be at least 1".into());
    }

    for reserved in [
        "insert_deduplication_token",
        "insert_deduplicate",
        "wait_end_of_query",
    ] {
        if cfg.settings.contains_key(reserved) {
            return fail(format!(
                "setting `{reserved}` is managed by the sink and cannot be overridden"
            ));
        }
    }
    Ok(())
}

/// Strict identifier: `[A-Za-z_][A-Za-z0-9_]*`. Validated before being
/// backtick-quoted into SQL, so no escaping is ever needed.
fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn insert_statement(table: &str, columns: &[String]) -> String {
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
    format!("INSERT INTO {table} ({cols}) FORMAT RowBinary")
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_core::config::ComponentConfig;

    fn component(yaml: &str) -> ComponentConfig {
        let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        ComponentConfig::new("clickhouse", value)
    }

    const MINIMAL: &str = r#"
table: orders
columns: [id, name]
shards:
  - replicas: ["http://a:8123"]
"#;

    #[test]
    fn minimal_config_builds_with_framework_defaults() {
        let sink = from_component_config(&component(MINIMAL)).unwrap();
        assert_eq!(
            sink.writer.insert_sql(),
            "INSERT INTO `orders` (`id`, `name`) FORMAT RowBinary"
        );
        assert_eq!(sink.endpoints.len(), 1);
        assert_eq!(sink.endpoints[0].len(), 1);
        assert_eq!(sink.pool, SinkPoolConfig::default());
    }

    #[test]
    fn qualified_table_and_knobs_map_through() {
        let sink = from_component_config(&component(
            r#"
table: analytics.orders
columns: [id]
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
            sink.writer.insert_sql(),
            "INSERT INTO `analytics`.`orders` (`id`) FORMAT RowBinary"
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

    #[test]
    fn validation_rejects_bad_configs() {
        let cases = [
            ("table: orders\ncolumns: [id]\nshards: []", "shard"),
            (
                "table: orders\ncolumns: [id]\nshards: [{replicas: []}]",
                "replicas",
            ),
            (
                "table: orders\ncolumns: [id]\nshards: [{replicas: [\"tcp://x\"]}]",
                "http",
            ),
            (
                "table: orders\ncolumns: []\nshards: [{replicas: [\"http://a\"]}]",
                "columns",
            ),
            (
                "table: orders\ncolumns: [\"id; DROP\"]\nshards: [{replicas: [\"http://a\"]}]",
                "identifier",
            ),
            (
                "table: \"or`ders\"\ncolumns: [id]\nshards: [{replicas: [\"http://a\"]}]",
                "identifier",
            ),
            (
                "table: a.b.c\ncolumns: [id]\nshards: [{replicas: [\"http://a\"]}]",
                "identifier",
            ),
            (
                "table: orders\ncolumns: [id]\nshards: [{replicas: [\"http://a\"]}]\nsettings: {insert_deduplication_token: \"x\"}",
                "managed by the sink",
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
    fn validation_rejects_bad_retry_and_breaker() {
        // Every one of these previously reached `Duration::mul_f64` (or a
        // broken breaker) at runtime instead of failing at load.
        let base = "table: t\ncolumns: [id]\nshards: [{replicas: [\"http://a\"]}]\n";
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
            "table: t\ncolumns: [id]\nshards: [{replicas: [\"http://a\"]}]\n\
             retry: { initial: 100ms, max: 10s, multiplier: 1.0, jitter: 0.0 }\n\
             breaker: { failure_threshold: 1, open_for: 5s, half_open_probes: 1 }",
        ));
        assert!(sink.is_ok(), "boundary-valid config must build: {sink:?}");
    }

    #[test]
    fn validation_rejects_duplicate_columns() {
        let err = from_component_config(&component(
            "table: t\ncolumns: [id, name, id]\nshards: [{replicas: [\"http://a\"]}]",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("more than once"), "{err}");
    }

    #[test]
    fn unknown_fields_are_rejected_with_a_path() {
        let err = from_component_config(&component(
            "table: orders\ncolumns: [id]\nshards: [{replicas: [\"http://a\"]}]\nbatch: {max_rowz: 5}",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("max_rowz"), "{err}");
    }
}
