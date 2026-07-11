//! ClickHouse-side observability for the sink-saturation rig.
//!
//! Throughput measured at the ETL boundary (`etl_sink_records_total`) tells us
//! how fast we *pushed* rows; it can't tell us whether the server wrote them at
//! a healthy *shape*. ClickHouse wants large parts, so a sink that inserts fast
//! but produces many tiny parts is quietly storing up merge pressure. These
//! helpers read the server's own accounting after a run — inserted rows, part
//! sizes, merges, server CPU, and async-insert flushes — so the report can say
//! whether the ETL batch size and the resulting CH part size are jointly good.
//!
//! Every aggregate is scoped to one table and to inserts at or after a captured
//! `since` timestamp (ClickHouse's own clock, so there is no host/container
//! skew), and read through a non-panicking path: a missing or empty system log
//! (e.g. `asynchronous_insert_log` when `async_insert=0`) yields zeros rather
//! than aborting the run.

/// A snapshot of ClickHouse-side counters for one run. Only fields the report
/// emits are kept; the queries fetch nothing else.
#[derive(Debug, Clone, Default)]
pub struct ChStats {
    /// `SELECT version()` — recorded so results carry the server version.
    pub version: String,
    /// Rows the server accepted across all INSERTs in the window.
    pub written_rows: f64,
    /// Total server CPU (microseconds) charged to those INSERTs.
    pub cpu_us: f64,
    /// Parts written to storage (`NewPart`); 0 for `ENGINE = Null`.
    pub parts_created: f64,
    /// Mean rows per written part — the "are parts big enough?" signal.
    pub avg_part_rows: f64,
    /// Mean bytes per written part.
    pub avg_part_bytes: f64,
    /// Background merges observed in the window (write amplification).
    pub merges: f64,
    /// Async-insert buffer flushes (only when `async_insert=1`).
    pub async_flushes: f64,
    /// Mean rows per async flush — shows whether the server re-batched us.
    pub async_avg_rows: f64,

    // ── Multi-table (MV-vs-split) fields; filled by `capture_multi` ──────
    /// Whole-server INSERT-query CPU (µs) across the window — for the
    /// `Null`+MV arm this *includes* the materialized views' fan-out CPU,
    /// which the direct-split arm never pays. The headline offload signal.
    pub server_insert_cpu_us: f64,
    /// Rows actually stored across all target tables (`NewPart` rows) —
    /// engine-agnostic to how they arrived (direct insert or via an MV).
    pub target_rows: f64,
    /// Materialized-view execution CPU (µs) — the work the MV arm does that
    /// the split arm offloads to the ETL tier. Zero for the split arm.
    pub mv_cpu_us: f64,
    /// Materialized-view executions observed (`system.query_views_log`).
    pub mv_executions: f64,
    /// Rows merged in the window (`MergeParts`) — merge *cost*, not just count.
    pub merged_rows: f64,
    /// Total merge duration (ms) in the window.
    pub merge_duration_ms: f64,
}

impl ChStats {
    /// Server CPU microseconds charged per accepted row (0 when no rows).
    #[must_use]
    pub fn cpu_us_per_row(&self) -> f64 {
        if self.written_rows > 0.0 {
            self.cpu_us / self.written_rows
        } else {
            0.0
        }
    }

    /// Whole-server INSERT CPU (incl. MV fan-out) per stored target row — the
    /// metric the MV-vs-split gate reads (0 when no rows landed).
    #[must_use]
    pub fn server_cpu_us_per_row(&self) -> f64 {
        if self.target_rows > 0.0 {
            self.server_insert_cpu_us / self.target_rows
        } else {
            0.0
        }
    }
}

/// The server's current wall clock as a millisecond-precision
/// `'YYYY-MM-DD hh:mm:ss.fff'` string, to scope later
/// `event_time_microseconds >=` filters without host/container clock skew.
/// Capture this and the pipeline's `text0` metrics render back-to-back at the
/// window start; the residual sliver between the two is a few milliseconds of
/// setup, negligible against the multi-second measurement window.
pub fn now(host: &str, port: u16, user: &str, password: &str) -> String {
    crate::docker::clickhouse_sql(host, port, user, password, "SELECT toString(now64(3))")
        .expect("clickhouse now()")
        .trim()
        .to_owned()
}

/// Flush the system logs and read the per-run counters for `table`, counting
/// only activity at or after `since` (from [`now`]).
pub fn capture(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    table: &str,
    since: &str,
) -> ChStats {
    // The logs buffer in memory; force them to the tables before reading. This
    // is best-effort observability, so a flush failure must not abort the run.
    let _ = crate::docker::try_clickhouse_sql(host, port, user, password, "SYSTEM FLUSH LOGS");

    let version = crate::docker::clickhouse_sql(host, port, user, password, "SELECT version()")
        .map(|v| v.trim().to_owned())
        .unwrap_or_default();

    let mut s = ChStats {
        version,
        ..ChStats::default()
    };

    // INSERT-side accounting (both engines; Null still records written_rows).
    // `event_time_microseconds` gives sub-second scoping against the millisecond
    // `since`; all three system logs carry the column on 26.3.
    if let Some(r) = scalar_row(
        host,
        port,
        user,
        password,
        &format!(
            "SELECT sum(written_rows), \
             sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) \
             FROM system.query_log \
             WHERE type = 'QueryFinish' AND query_kind = 'Insert' \
             AND event_time_microseconds >= toDateTime64('{since}', 3) \
             AND arrayExists(t -> position(t, '{table}') > 0, tables)"
        ),
    ) {
        s.written_rows = at(&r, 0);
        s.cpu_us = at(&r, 1);
    }

    // Part shape and merge pressure (MergeTree only; empty for Null).
    if let Some(r) = scalar_row(
        host,
        port,
        user,
        password,
        &format!(
            "SELECT countIf(event_type = 'NewPart'), \
             round(avgIf(rows, event_type = 'NewPart'), 1), \
             round(avgIf(size_in_bytes, event_type = 'NewPart'), 1), \
             countIf(event_type = 'MergeParts') \
             FROM system.part_log \
             WHERE table = '{table}' \
             AND event_time_microseconds >= toDateTime64('{since}', 3)"
        ),
    ) {
        s.parts_created = at(&r, 0);
        s.avg_part_rows = at(&r, 1);
        s.avg_part_bytes = at(&r, 2);
        s.merges = at(&r, 3);
    }

    // Async-insert flushes — present only when async_insert batched us
    // server-side; the log may not exist on some builds, hence the optional read.
    if let Some(r) = scalar_row(
        host,
        port,
        user,
        password,
        &format!(
            "SELECT count(), round(avg(rows), 1) \
             FROM system.asynchronous_insert_log \
             WHERE table = '{table}' \
             AND event_time_microseconds >= toDateTime64('{since}', 3)"
        ),
    ) {
        s.async_flushes = at(&r, 0);
        s.async_avg_rows = at(&r, 1);
    }

    s
}

/// Capture the MV-vs-split counters across a *set* of target tables: the
/// whole-server INSERT CPU (which folds in materialized-view fan-out for the
/// `Null`+MV arm), the rows/parts/merges actually stored across every target,
/// and the materialized views' own execution cost. Same non-panicking,
/// `since`-scoped contract as [`capture`].
pub fn capture_multi(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    tables: &[String],
    since: &str,
) -> ChStats {
    let _ = crate::docker::try_clickhouse_sql(host, port, user, password, "SYSTEM FLUSH LOGS");
    let version = crate::docker::clickhouse_sql(host, port, user, password, "SELECT version()")
        .map(|v| v.trim().to_owned())
        .unwrap_or_default();
    let mut s = ChStats {
        version,
        ..ChStats::default()
    };
    let table_list = tables
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");

    // Whole-server INSERT CPU. No table filter: for the MV arm the fan-out
    // runs *inside* the Null-table insert query, so its cost lands here.
    if let Some(r) = scalar_row(
        host,
        port,
        user,
        password,
        &format!(
            "SELECT sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) \
             FROM system.query_log \
             WHERE type = 'QueryFinish' AND query_kind = 'Insert' \
             AND event_time_microseconds >= toDateTime64('{since}', 3)"
        ),
    ) {
        s.server_insert_cpu_us = at(&r, 0);
    }

    // Rows/parts/merges stored across every target table (engine-agnostic to
    // how they arrived — direct split insert or MV fan-out).
    if let Some(r) = scalar_row(
        host,
        port,
        user,
        password,
        &format!(
            "SELECT sumIf(rows, event_type = 'NewPart'), \
             countIf(event_type = 'NewPart'), \
             round(avgIf(rows, event_type = 'NewPart'), 1), \
             round(avgIf(size_in_bytes, event_type = 'NewPart'), 1), \
             countIf(event_type = 'MergeParts'), \
             sumIf(rows, event_type = 'MergeParts'), \
             sumIf(duration_ms, event_type = 'MergeParts') \
             FROM system.part_log \
             WHERE table IN ({table_list}) \
             AND event_time_microseconds >= toDateTime64('{since}', 3)"
        ),
    ) {
        s.target_rows = at(&r, 0);
        s.parts_created = at(&r, 1);
        s.avg_part_rows = at(&r, 2);
        s.avg_part_bytes = at(&r, 3);
        s.merges = at(&r, 4);
        s.merged_rows = at(&r, 5);
        s.merge_duration_ms = at(&r, 6);
    }
    // Keep the single-table fields consistent for shared report code.
    s.written_rows = s.target_rows;
    s.cpu_us = s.server_insert_cpu_us;

    // Materialized-view execution cost — the work the split arm offloads.
    // Absent (all zero) for the split arm, which runs no views.
    if let Some(r) = scalar_row(
        host,
        port,
        user,
        password,
        &format!(
            "SELECT count(), \
             sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) \
             FROM system.query_views_log \
             WHERE event_time_microseconds >= toDateTime64('{since}', 3)"
        ),
    ) {
        s.mv_executions = at(&r, 0);
        s.mv_cpu_us = at(&r, 1);
    }

    s
}

/// `slot` of a parsed row, or 0.0 when short.
fn at(row: &[f64], slot: usize) -> f64 {
    row.get(slot).copied().unwrap_or(0.0)
}

/// Run a one-row aggregate query and parse its tab-separated columns. Returns
/// `None` on any transport error or server exception (a missing system table,
/// an empty log), so optional observability never aborts a benchmark run.
/// ClickHouse prints an empty `avgIf` as `nan`; those become 0.0.
fn scalar_row(host: &str, port: u16, user: &str, password: &str, sql: &str) -> Option<Vec<f64>> {
    let body = crate::docker::try_clickhouse_sql(host, port, user, password, sql).ok()?;
    if body.contains("DB::Exception") {
        return None;
    }
    let line = body.lines().next()?;
    Some(
        line.split('\t')
            .map(|c| {
                let v = c.trim().parse::<f64>().unwrap_or(0.0);
                if v.is_nan() { 0.0 } else { v }
            })
            .collect(),
    )
}
