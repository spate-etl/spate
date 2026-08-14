//! Opt-in startup DDL-parity guard for `Distributed`-table deployments.
//!
//! When the sink config carries a `distributed_check` block, startup
//! verifies that the cluster topology and the `Distributed` table's DDL
//! agree with the sink config, covering shard count, per-shard weights, and
//! the sharding expression. Placement/DDL drift does not error at query time:
//! under `optimize_skip_unused_shards=1` it silently returns wrong results.
//!
//! What is verified: shard count, weights, the sharding expression, and
//! the DDL's cluster argument. What is documented-only: that config shard
//! `i` IS the cluster's `shard_num = i + 1`. HTTP replica URLs
//! are not reliably mappable onto the cluster's native `host:port`
//! entries (proxies, DNS aliases, the 8123/9000 port split), so ordering
//! stays the operator's contract. A best-effort hostname cross-check
//! warns (never fails) on apparent cross-wiring.

use crate::writer::ClickHouseEndpoint;
use serde::Deserialize;
use std::fmt::Write as _;
use std::sync::Arc;

/// Startup DDL-parity guard failed. Fatal: fix the config or the DDL.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DistributedCheckError {
    /// A system-table query failed. The user opted into fail-fast
    /// verification, so connectivity problems fail startup too —
    /// distinguishably from mismatches.
    #[error("sink.clickhouse.distributed_check: could not fetch {what} from {url}: {reason}")]
    Fetch {
        /// What was being fetched (`cluster topology` / `table engine`).
        what: &'static str,
        /// The endpoint URL queried.
        url: String,
        /// Human-readable cause.
        reason: String,
    },
    /// Config and cluster/DDL disagree (pre-formatted diff).
    #[error("{0}")]
    Mismatch(String),
}

/// One replica row of `system.clusters`, crate-private.
#[derive(Clone, Debug, clickhouse::Row, Deserialize)]
pub(crate) struct ClusterReplicaRow {
    pub(crate) shard_num: u32,
    pub(crate) shard_weight: u32,
    pub(crate) host_name: String,
}

/// The engine of the checked table, crate-private.
#[derive(Clone, Debug, clickhouse::Row, Deserialize)]
pub(crate) struct TableEngineRow {
    pub(crate) engine: String,
    pub(crate) engine_full: String,
}

/// What `build()` captures for a later `validate_distributed()` call.
#[derive(Debug)]
pub(crate) struct DistributedCheck {
    /// The endpoint to query (`distributed_check.endpoint`, defaulting to
    /// the first replica of shard 0).
    pub(crate) endpoint: ClickHouseEndpoint,
    /// The cluster named by the `Distributed` DDL.
    pub(crate) cluster: String,
    /// Default database for an unqualified `table`.
    pub(crate) database: Option<String>,
    /// The `Distributed` table, optionally `db.table`-qualified.
    pub(crate) table: String,
    /// The expected sharding expression, already normalized.
    pub(crate) expected_expr: String,
    /// Configured per-shard weights, config order.
    pub(crate) weights: Arc<[u32]>,
    /// Configured replica URL hostnames per shard, for the advisory
    /// cross-check.
    pub(crate) replica_hosts: Vec<Vec<String>>,
}

impl DistributedCheck {
    fn target(&self) -> (Option<&str>, &str) {
        match self.table.split_once('.') {
            Some((db, tbl)) => (Some(db), tbl),
            None => (self.database.as_deref(), &self.table),
        }
    }

    fn display_table(&self) -> String {
        match self.target() {
            (Some(db), tbl) => format!("`{db}`.`{tbl}`"),
            (None, tbl) => format!("`{tbl}`"),
        }
    }

    fn mismatch(&self, detail: String) -> DistributedCheckError {
        DistributedCheckError::Mismatch(format!("sink.clickhouse.distributed_check: {detail}"))
    }

    /// Run the guard. Query order is fixed at cluster topology then table
    /// engine, and the mock tests depend on it.
    pub(crate) async fn verify(&self) -> Result<(), DistributedCheckError> {
        let replicas = self
            .endpoint
            .client()
            .query(
                "SELECT shard_num, shard_weight, host_name FROM system.clusters \
                 WHERE cluster = ? ORDER BY shard_num, replica_num",
            )
            .bind(&self.cluster)
            .fetch_all::<ClusterReplicaRow>()
            .await
            .map_err(|e| DistributedCheckError::Fetch {
                what: "cluster topology",
                url: self.endpoint.url().to_string(),
                reason: e.to_string(),
            })?;
        if replicas.is_empty() {
            return Err(self.mismatch(format!(
                "cluster `{}` not found in system.clusters on {}",
                self.cluster,
                self.endpoint.url()
            )));
        }

        // Collapse replica rows to per-shard weights; shard_num is 1-based
        // and consecutive in cluster-config order.
        let mut shard_weights: Vec<u32> = Vec::new();
        for row in &replicas {
            let idx = row.shard_num as usize;
            if idx == 0 || idx > replicas.len() {
                return Err(self.mismatch(format!(
                    "cluster `{}` reports an unexpected shard_num {} — cannot map \
                     shards positionally",
                    self.cluster, row.shard_num
                )));
            }
            if idx == shard_weights.len() + 1 {
                shard_weights.push(row.shard_weight);
            } else if idx <= shard_weights.len() {
                // Another replica of an already-seen shard.
                if shard_weights[idx - 1] != row.shard_weight {
                    return Err(self.mismatch(format!(
                        "cluster `{}` shard_num {} reports inconsistent weights",
                        self.cluster, row.shard_num
                    )));
                }
            } else {
                return Err(self.mismatch(format!(
                    "cluster `{}` shard numbering is not consecutive at shard_num {}",
                    self.cluster, row.shard_num
                )));
            }
        }

        if shard_weights.len() != self.weights.len() {
            return Err(self.mismatch(format!(
                "the sink config has {} shard(s) but cluster `{}` has {}",
                self.weights.len(),
                self.cluster,
                shard_weights.len()
            )));
        }
        for (i, (&configured, &live)) in self.weights.iter().zip(shard_weights.iter()).enumerate() {
            if configured != live {
                return Err(self.mismatch(format!(
                    "config shard {i} has weight {configured} but cluster shard_num {} \
                     has weight {live}",
                    i + 1
                )));
            }
        }

        // Advisory only: exact-hostname hits under the wrong shard_num
        // suggest cross-wiring.
        for (i, hosts) in self.replica_hosts.iter().enumerate() {
            for host in hosts {
                let expected_num = (i + 1) as u32;
                let under_expected = replicas
                    .iter()
                    .any(|r| r.shard_num == expected_num && r.host_name == *host);
                let elsewhere: Vec<u32> = replicas
                    .iter()
                    .filter(|r| r.host_name == *host && r.shard_num != expected_num)
                    .map(|r| r.shard_num)
                    .collect();
                if !under_expected && !elsewhere.is_empty() {
                    tracing::warn!(
                        host,
                        config_shard = i,
                        cluster_shard_nums = ?elsewhere,
                        "sink.clickhouse.distributed_check: config shard {i} replica host \
                         appears under a different cluster shard_num — the shards: list \
                         may not be in remote_servers order",
                    );
                }
            }
        }

        // The Distributed table's DDL.
        let (db, table) = self.target();
        let query = match db {
            Some(db) => self
                .endpoint
                .client()
                .query(
                    "SELECT engine, engine_full FROM system.tables \
                     WHERE database = ? AND name = ?",
                )
                .bind(db)
                .bind(table),
            None => self
                .endpoint
                .client()
                .query(
                    "SELECT engine, engine_full FROM system.tables \
                     WHERE database = currentDatabase() AND name = ?",
                )
                .bind(table),
        };
        let engines = query.fetch_all::<TableEngineRow>().await.map_err(|e| {
            DistributedCheckError::Fetch {
                what: "table engine",
                url: self.endpoint.url().to_string(),
                reason: e.to_string(),
            }
        })?;
        let Some(row) = engines.first() else {
            return Err(self.mismatch(format!(
                "table {} not found (or not visible to this user) on {}",
                self.display_table(),
                self.endpoint.url()
            )));
        };
        if row.engine != "Distributed" {
            return Err(self.mismatch(format!(
                "table {} exists but its engine is {}, not Distributed",
                self.display_table(),
                row.engine
            )));
        }

        let Some(args) = engine_args(&row.engine_full) else {
            return Err(self.mismatch(format!(
                "could not parse the Distributed engine arguments of {}; raw \
                 engine_full: {}",
                self.display_table(),
                row.engine_full
            )));
        };
        if args.len() < 4 {
            return Err(self.mismatch(format!(
                "the Distributed table {} declares no sharding key (inserts through it \
                 spray randomly); shard pruning requires one. Raw engine_full: {}",
                self.display_table(),
                row.engine_full
            )));
        }
        let ddl_cluster = unquote(&args[0]);
        if ddl_cluster != self.cluster {
            return Err(self.mismatch(format!(
                "the Distributed table {} is defined over cluster `{ddl_cluster}`, but \
                 the check is configured for cluster `{}`",
                self.display_table(),
                self.cluster
            )));
        }
        let ddl_expr = normalize(&args[3]);
        if ddl_expr != self.expected_expr {
            let mut msg = String::new();
            let _ = write!(
                msg,
                "the sharding expression of {} does not match the sink config:\n  \
                 DDL (normalized):      {ddl_expr}\n  \
                 expected (normalized): {}\n  \
                 raw engine_full:       {}",
                self.display_table(),
                self.expected_expr,
                row.engine_full
            );
            return Err(self.mismatch(msg));
        }
        Ok(())
    }
}

/// Split the top-level arguments of `Distributed(...)` out of an
/// `engine_full` string: a quote- and paren-aware scanner (not a regex) so
/// nested calls in the sharding expression, 5-argument policy forms, and a
/// trailing ` SETTINGS ...` suffix all parse. Returns `None` on any shape
/// that is not `Distributed(...)` with balanced delimiters.
fn engine_args(engine_full: &str) -> Option<Vec<String>> {
    let rest = engine_full.trim().strip_prefix("Distributed")?.trim_start();
    let rest = rest.strip_prefix('(')?;
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut depth = 0usize;
    let mut in_str = false;
    let mut chars = rest.chars().peekable();
    while let Some(c) = chars.next() {
        if in_str {
            cur.push(c);
            match c {
                '\\' => {
                    // Backslash escape: consume the escaped char verbatim.
                    if let Some(next) = chars.next() {
                        cur.push(next);
                    }
                }
                '\'' => {
                    // `''` is an escaped quote, anything else ends the string.
                    if chars.peek() == Some(&'\'') {
                        cur.push(chars.next().expect("peeked"));
                    } else {
                        in_str = false;
                    }
                }
                _ => {}
            }
            continue;
        }
        match c {
            '\'' => {
                in_str = true;
                cur.push(c);
            }
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                if depth == 0 {
                    // The matching close of `Distributed(`. Anything after
                    // (e.g. ` SETTINGS fsync_after_insert = 1`) is ignored.
                    args.push(cur.trim().to_string());
                    return Some(args);
                }
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => {
                args.push(cur.trim().to_string());
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    None
}

/// Strip one level of single quotes (with `''`/`\'` unescaping) from a
/// quoted engine argument; bare identifiers pass through trimmed.
fn unquote(arg: &str) -> String {
    let t = arg.trim();
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        t[1..t.len() - 1]
            .replace("\\'", "'")
            .replace("''", "'")
            .replace("\\\\", "\\")
    } else {
        t.to_string()
    }
}

/// Character-level expression normalization: remove ASCII whitespace and
/// strip backtick/double-quote identifier quoting, case preserved —
/// **outside string literals only**. Literal contents are compared
/// verbatim (`'x y'` ≠ `'xy'`): filtering inside literals would let a
/// drifted expression normalize equal and false-PASS the guard. Exact for
/// the `xxHash64(identifier)` form this crate ships; the `sharding_expr`
/// escape hatch inherits the textual (non-AST) comparison and its
/// documented brittleness (escape style inside literals included).
pub(crate) fn normalize(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.chars().peekable();
    let mut in_str = false;
    while let Some(c) = chars.next() {
        if in_str {
            out.push(c);
            match c {
                '\\' => {
                    // Backslash escape: keep the escaped char verbatim.
                    if let Some(next) = chars.next() {
                        out.push(next);
                    }
                }
                '\'' => {
                    // `''` is an escaped quote, anything else ends the string.
                    if chars.peek() == Some(&'\'') {
                        out.push(chars.next().expect("peeked"));
                    } else {
                        in_str = false;
                    }
                }
                _ => {}
            }
            continue;
        }
        match c {
            '\'' => {
                in_str = true;
                out.push(c);
            }
            c if c.is_ascii_whitespace() || c == '`' || c == '"' => {}
            c => out.push(c),
        }
    }
    out
}

/// The hostname of an `http(s)://host[:port][/...]` replica URL, for the
/// advisory cross-check. Bracketed IPv6 hosts yield the bare address (no
/// brackets), matching `system.clusters.host_name` formatting.
pub(crate) fn host_of(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    if let Some(v6) = rest.strip_prefix('[') {
        let host = &v6[..v6.find(']')?];
        return (!host.is_empty()).then(|| host.to_string());
    }
    let end = rest.find([':', '/']).unwrap_or(rest.len());
    let host = &rest[..end];
    (!host.is_empty()).then(|| host.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_full_fourth_argument_is_the_sharding_expression() {
        let args =
            engine_args("Distributed('prod', 'analytics', 'events_local', xxHash64(sensor))")
                .unwrap();
        assert_eq!(args.len(), 4);
        assert_eq!(unquote(&args[0]), "prod");
        assert_eq!(unquote(&args[1]), "analytics");
        assert_eq!(unquote(&args[2]), "events_local");
        assert_eq!(normalize(&args[3]), "xxHash64(sensor)");
    }

    #[test]
    fn engine_full_with_policy_arg_and_settings_suffix_still_parses() {
        let args = engine_args(
            "Distributed('prod', 'db', 't', xxHash64(id), 'default') \
             SETTINGS fsync_after_insert = 1",
        )
        .unwrap();
        assert_eq!(args.len(), 5);
        assert_eq!(normalize(&args[3]), "xxHash64(id)");
        assert_eq!(unquote(&args[4]), "default");
    }

    #[test]
    fn engine_full_with_quoted_and_bare_arguments_unquotes() {
        // Both AST formattings ClickHouse has used: quoted and bare.
        for full in [
            "Distributed('prod', 'db', 't', xxHash64(sensor))",
            "Distributed(prod, db, t, xxHash64(sensor))",
        ] {
            let args = engine_args(full).unwrap();
            assert_eq!(unquote(&args[0]), "prod", "for {full}");
            assert_eq!(normalize(&args[3]), "xxHash64(sensor)", "for {full}");
        }
    }

    #[test]
    fn engine_full_without_a_sharding_key_is_parsed_as_three_arguments() {
        let args = engine_args("Distributed('prod', 'db', 't')").unwrap();
        assert_eq!(args.len(), 3, "no sharding key → fewer than 4 args");
    }

    #[test]
    fn engine_full_with_nested_calls_and_quoted_commas_stays_top_level() {
        let args =
            engine_args("Distributed('pr,od', 'db', 't', cityHash64(concat(a, ',', b)))").unwrap();
        assert_eq!(args.len(), 4);
        assert_eq!(unquote(&args[0]), "pr,od", "commas inside quotes survive");
        assert_eq!(
            normalize(&args[3]),
            "cityHash64(concat(a,',',b))",
            "nested parens stay one argument"
        );
    }

    #[test]
    fn engine_full_that_is_not_distributed_or_unbalanced_returns_none() {
        assert!(engine_args("MergeTree ORDER BY id").is_none());
        assert!(engine_args("Distributed('prod', 'db', 't'").is_none());
    }

    #[test]
    fn normalization_strips_whitespace_backticks_and_double_quotes() {
        assert_eq!(normalize("xxHash64( `sensor` )"), "xxHash64(sensor)");
        assert_eq!(normalize("xxHash64(\"sensor\")"), "xxHash64(sensor)");
        assert_eq!(normalize("xxHash64(Sensor)"), "xxHash64(Sensor)");
    }

    #[test]
    fn normalization_preserves_string_literal_contents_verbatim() {
        // Whitespace (and quoting chars) inside a literal are significant:
        // stripping them would let a drifted expression false-PASS the guard.
        assert_eq!(
            normalize("xxHash64(concat(a, 'x y'))"),
            "xxHash64(concat(a,'x y'))"
        );
        assert_ne!(
            normalize("concat(a, 'x y')"),
            normalize("concat(a, 'xy')"),
            "a space inside a literal must distinguish the expressions"
        );
        assert_ne!(
            normalize("concat(a, '`')"),
            normalize("concat(a, '')"),
            "a backtick inside a literal must survive"
        );
        // Escapes stay verbatim; the literal ends at the right quote.
        assert_eq!(normalize("concat('it''s', ` x `)"), "concat('it''s',x)");
        assert_eq!(normalize(r"concat('a\' b', c )"), r"concat('a\' b',c)");
    }

    #[test]
    fn host_of_extracts_the_hostname_from_replica_urls() {
        assert_eq!(host_of("http://ch-0:8123").as_deref(), Some("ch-0"));
        assert_eq!(
            host_of("https://ch.example.com/x").as_deref(),
            Some("ch.example.com")
        );
        assert_eq!(host_of("http://ch-1").as_deref(), Some("ch-1"));
        assert_eq!(host_of("not-a-url"), None);
    }

    #[test]
    fn host_of_unwraps_bracketed_ipv6_hosts() {
        assert_eq!(host_of("http://[::1]:8123").as_deref(), Some("::1"));
        assert_eq!(
            host_of("https://[2001:db8::2]/db").as_deref(),
            Some("2001:db8::2")
        );
        assert_eq!(host_of("http://[]:8123"), None, "empty brackets");
        assert_eq!(host_of("http://[::1"), None, "unclosed bracket");
    }
}
