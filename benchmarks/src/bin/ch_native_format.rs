//! ClickHouse Native-vs-RowBinary format A/B — the spike's go/no-go rigs.
//!
//! Holds batching constant and isolates the *format* delta on identical
//! seeded rows across four representative schemas:
//! - `events`  — realistic mixed analytics row (every Native mechanism)
//! - `metrics` — fixed-width numeric row (the common case; regression guard)
//! - `dims`    — LowCardinality-heavy row (the columnar win case)
//! - `dims_hc` — `dims` with a ~50k-distinct LowCardinality column, guarding
//!   dictionary-interner hash-collision pathologies (client-only, not gated)
//!
//! Rig A (client encode): RowBinary vs Native, each through its real
//!   [`RowEncoder`] impl (`ClickHouseEncoder` / `NativeEncoder`) so the call
//!   shape is symmetric; median of per-iteration total-ns samples over
//!   `ITERS` iterations, divided by rows once. A bare `serialize_row`
//!   reference line exposes the encoder-wrapper overhead.
//! Rig B (wire size): compressed body size (lz4 + zstd level 3) per format.
//! Rig C (server parse CPU): with a running ClickHouse (Docker), insert each
//!   format into an `ENGINE = Null` table (isolates parse+block-form from
//!   merge) and read `system.query_log` ProfileEvents.
//!
//! Env: ROWS (200000) ITERS (25) REPS (15, server reps) SERVER (1 = run rig C)
//! RESULTS (JSONL path). Methodology + recorded results in
//! docs/benchmarks/clickhouse-format.mdx.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::{docker, env_u64, http_post_bytes, percentile};
use bytes::BytesMut;
use serde::Serialize;
use spate_clickhouse::{
    ClickHouseEncoder, DateTime64Millis, Decimal64, NativeEncoder, NativeSchema, serialize_row,
};
use spate_core::checkpoint::AckRef;
use spate_core::deser::Owned;
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::RowEncoder;
use std::sync::Arc;
use std::time::Instant;

// ---- rows -----------------------------------------------------------------

fn record<T>(payload: T) -> Record<T> {
    let (ack, rx) = AckRef::test_pair();
    std::mem::forget(rx);
    Record {
        payload,
        meta: RecordMeta {
            partition: PartitionId(0),
            offset: 0,
            event_time_ms: 0,
            key_hash: None,
        },
        ack,
    }
}

const EVENT_TYPES: &[&str] = &[
    "click", "view", "purchase", "signup", "logout", "search", "share", "error",
];
const COUNTRIES: &[&str] = &["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA", "AU", "NL"];

#[derive(Clone, Serialize)]
struct EventRow {
    event_id: u64,
    event_time: DateTime64Millis,
    user_id: u64,
    event_type: String,
    country: String,
    city: String,
    url: String,
    referrer: Option<String>,
    status: u16,
    duration_ms: u32,
    revenue: Decimal64<4>,
    tags: Vec<String>,
    is_conversion: bool,
}

fn event_columns() -> Vec<(&'static str, &'static str)> {
    vec![
        ("event_id", "UInt64"),
        ("event_time", "DateTime64(3)"),
        ("user_id", "UInt64"),
        ("event_type", "LowCardinality(String)"),
        ("country", "LowCardinality(String)"),
        ("city", "LowCardinality(String)"),
        ("url", "String"),
        ("referrer", "Nullable(String)"),
        ("status", "UInt16"),
        ("duration_ms", "UInt32"),
        ("revenue", "Decimal(18, 4)"),
        ("tags", "Array(LowCardinality(String))"),
        ("is_conversion", "Bool"),
    ]
}

fn gen_events(n: usize) -> Vec<EventRow> {
    (0..n)
        .map(|i| {
            let tag_n = i % 5;
            EventRow {
                event_id: i as u64,
                event_time: DateTime64Millis(1_700_000_000_000 + (i as i64) * 137),
                user_id: (i as u64 * 2_654_435_761) % 1_000_000,
                event_type: EVENT_TYPES[i % EVENT_TYPES.len()].to_string(),
                country: COUNTRIES[i % COUNTRIES.len()].to_string(),
                city: format!("city{}", i % 5000),
                url: format!("https://example.com/p/{}/item/{}", i % 1000, i % 97),
                referrer: (i % 7 != 0).then(|| format!("https://ref.example/{}", i % 300)),
                status: [200u16, 200, 200, 404, 500][i % 5],
                duration_ms: (i as u32 * 7) % 5000,
                revenue: Decimal64((i as i64 % 1000) * 10),
                tags: (0..tag_n)
                    .map(|t| EVENT_TYPES[(i + t) % EVENT_TYPES.len()].to_string())
                    .collect(),
                is_conversion: i % 3 == 0,
            }
        })
        .collect()
}

#[derive(Clone, Serialize)]
struct MetricRow {
    ts: DateTime64Millis,
    host_id: u32,
    cpu: f64,
    mem: f64,
    disk: f64,
    net_in: u64,
    net_out: u64,
    load1: f32,
    load5: f32,
    load15: f32,
    reqs: u64,
    errors: u32,
    latency_p99: f64,
    uptime: u64,
}

fn metric_columns() -> Vec<(&'static str, &'static str)> {
    vec![
        ("ts", "DateTime64(3)"),
        ("host_id", "UInt32"),
        ("cpu", "Float64"),
        ("mem", "Float64"),
        ("disk", "Float64"),
        ("net_in", "UInt64"),
        ("net_out", "UInt64"),
        ("load1", "Float32"),
        ("load5", "Float32"),
        ("load15", "Float32"),
        ("reqs", "UInt64"),
        ("errors", "UInt32"),
        ("latency_p99", "Float64"),
        ("uptime", "UInt64"),
    ]
}

fn gen_metrics(n: usize) -> Vec<MetricRow> {
    (0..n)
        .map(|i| {
            let f = i as f64;
            MetricRow {
                ts: DateTime64Millis(1_700_000_000_000 + (i as i64) * 1000),
                host_id: (i as u32) % 500,
                cpu: (f * 0.013) % 100.0,
                mem: (f * 0.027) % 100.0,
                disk: (f * 0.041) % 100.0,
                net_in: (i as u64 * 131) % 10_000_000,
                net_out: (i as u64 * 197) % 10_000_000,
                load1: (f as f32 * 0.01) % 8.0,
                load5: (f as f32 * 0.007) % 8.0,
                load15: (f as f32 * 0.003) % 8.0,
                reqs: (i as u64 * 53) % 1_000_000,
                errors: (i as u32 * 3) % 1000,
                latency_p99: (f * 0.5) % 2000.0,
                uptime: i as u64 * 60,
            }
        })
        .collect()
}

#[derive(Clone, Serialize)]
struct DimRow {
    id: u64,
    c1: String,
    c2: String,
    c3: String,
    c4: String,
    c5: String,
    c6: String,
    c_high: String,
}

fn dim_columns() -> Vec<(&'static str, &'static str)> {
    vec![
        ("id", "UInt64"),
        ("c1", "LowCardinality(String)"),
        ("c2", "LowCardinality(String)"),
        ("c3", "LowCardinality(String)"),
        ("c4", "LowCardinality(String)"),
        ("c5", "LowCardinality(String)"),
        ("c6", "LowCardinality(String)"),
        ("c_high", "LowCardinality(String)"),
    ]
}

fn gen_dims(n: usize) -> Vec<DimRow> {
    (0..n)
        .map(|i| DimRow {
            id: i as u64,
            c1: EVENT_TYPES[i % 4].to_string(),
            c2: COUNTRIES[i % 10].to_string(),
            c3: format!("seg{}", i % 30),
            c4: format!("plan{}", i % 6),
            c5: format!("src{}", i % 15),
            c6: format!("dev{}", i % 4),
            c_high: format!("v{}", i % 5000), // > 255 distinct -> u16 dictionary keys
        })
        .collect()
}

/// Same shape and schema as [`gen_dims`], but `c_high` spans ~50_000 distinct
/// values to stress the dictionary interner's hashing under high cardinality.
fn gen_dims_hc(n: usize) -> Vec<DimRow> {
    (0..n)
        .map(|i| DimRow {
            id: i as u64,
            c1: EVENT_TYPES[i % 4].to_string(),
            c2: COUNTRIES[i % 10].to_string(),
            c3: format!("seg{}", i % 30),
            c4: format!("plan{}", i % 6),
            c5: format!("src{}", i % 15),
            c6: format!("dev{}", i % 4),
            c_high: format!("city{}", i % 50_000),
        })
        .collect()
}

// ---- encoding -------------------------------------------------------------

/// The bare `serialize_row` free function — the wrapper-overhead reference.
fn encode_rowbinary_bare<T: Serialize>(records: &[Record<T>], buf: &mut BytesMut) {
    buf.clear();
    for r in records {
        serialize_row(&r.payload, buf).expect("rowbinary encode");
    }
}

fn encode_rowbinary<T: Serialize + Send + 'static>(
    enc: &mut ClickHouseEncoder<Owned<T>>,
    records: &[Record<T>],
    buf: &mut BytesMut,
) {
    buf.clear();
    for r in records {
        enc.encode(r, buf).expect("rowbinary encode");
        // `buffered_bytes`/`finish_chunk` are row-format no-ops; call them
        // anyway so the measured shape matches the Native pipeline exactly.
        std::hint::black_box(enc.buffered_bytes());
    }
    enc.finish_chunk(buf).expect("rowbinary finish");
}

fn encode_native<T: Serialize + Send + 'static>(
    enc: &mut NativeEncoder<Owned<T>>,
    records: &[Record<T>],
    buf: &mut BytesMut,
) {
    buf.clear();
    for r in records {
        enc.encode(r, buf).expect("native encode");
        // The terminal stage checks the seal threshold per record; include it
        // so the measurement reflects the real pipeline's per-row cost.
        std::hint::black_box(enc.buffered_bytes());
    }
    enc.finish_chunk(buf).expect("native finish");
}

/// Median of per-iteration *total* nanoseconds — dividing to a per-row rate
/// is the caller's job, once, to keep sub-integer resolution at low ns/row.
fn median_total_ns(mut f: impl FnMut() -> usize, iters: u64) -> (u64, usize) {
    let mut last_bytes = 0;
    let mut samples = Vec::with_capacity(iters as usize);
    for _ in 0..iters {
        let t = Instant::now();
        last_bytes = f();
        samples.push(t.elapsed().as_nanos() as u64);
        std::hint::black_box(last_bytes);
    }
    (percentile(&mut samples, 0.5), last_bytes)
}

struct FormatResult {
    ns_per_row: f64,
    raw_bytes: usize,
    lz4_bytes: usize,
    zstd_bytes: usize,
}

fn measure<T: Serialize + Send + 'static + Clone>(
    schema: Arc<NativeSchema>,
    rows: Vec<T>,
    iters: u64,
) -> (FormatResult, FormatResult, f64) {
    let records: Vec<Record<T>> = rows.into_iter().map(record).collect();
    let rows_f = records.len() as f64;

    // Rig A: encode timing, both formats through their real `RowEncoder` impl.
    let mut buf = BytesMut::new();
    let mut rb_enc = ClickHouseEncoder::<Owned<T>>::new();
    let (rb_total, rb_raw) = median_total_ns(
        || {
            encode_rowbinary(&mut rb_enc, &records, &mut buf);
            buf.len()
        },
        iters,
    );
    let rb_bytes = buf.to_vec();

    let (bare_total, _) = median_total_ns(
        || {
            encode_rowbinary_bare(&records, &mut buf);
            buf.len()
        },
        iters,
    );

    let mut enc = NativeEncoder::<Owned<T>>::new(schema);
    let (nat_total, nat_raw) = median_total_ns(
        || {
            encode_native(&mut enc, &records, &mut buf);
            buf.len()
        },
        iters,
    );
    let nat_bytes = buf.to_vec();

    // Rig B: compressed wire size.
    let lz4 = |d: &[u8]| lz4_flex::compress(d).len();
    let zstd = |d: &[u8]| zstd::encode_all(d, 3).expect("zstd").len();

    (
        FormatResult {
            ns_per_row: rb_total as f64 / rows_f,
            raw_bytes: rb_raw,
            lz4_bytes: lz4(&rb_bytes),
            zstd_bytes: zstd(&rb_bytes),
        },
        FormatResult {
            ns_per_row: nat_total as f64 / rows_f,
            raw_bytes: nat_raw,
            lz4_bytes: lz4(&nat_bytes),
            zstd_bytes: zstd(&nat_bytes),
        },
        bare_total as f64 / rows_f,
    )
}

fn pct_lower(base: f64, other: f64) -> f64 {
    if base == 0.0 {
        0.0
    } else {
        (base - other) / base * 100.0
    }
}

fn run_client_ab(name: &str, rows: usize, iters: u64) -> (FormatResult, FormatResult) {
    let cols = match name {
        "events" => event_columns(),
        "metrics" => metric_columns(),
        "dims" | "dims_hc" => dim_columns(),
        _ => unreachable!(),
    };
    let schema = NativeSchema::from_columns(&cols).expect("native schema");
    let (rb, nat, bare_ns) = match name {
        "events" => measure(schema, gen_events(rows), iters),
        "metrics" => measure(schema, gen_metrics(rows), iters),
        "dims" => measure(schema, gen_dims(rows), iters),
        "dims_hc" => measure(schema, gen_dims_hc(rows), iters),
        _ => unreachable!(),
    };

    let rows_f = 1e9 / rb.ns_per_row;
    let nat_rows_f = 1e9 / nat.ns_per_row;
    println!(
        "\n[{name}]  encode ns/row: RowBinary {:.1} ({:.1}M rows/s) | Native {:.1} ({:.1}M rows/s)  \
         => Native is {:+.1}% on encode CPU",
        rb.ns_per_row,
        rows_f / 1e6,
        nat.ns_per_row,
        nat_rows_f / 1e6,
        pct_lower(rb.ns_per_row, nat.ns_per_row),
    );
    println!(
        "          RowBinary bare-fn: {bare_ns:.1} ns/row  (encoder-wrapper overhead reference)"
    );
    println!(
        "          raw bytes/row: RowBinary {} | Native {}",
        rb.raw_bytes / rows,
        nat.raw_bytes / rows
    );
    println!(
        "          lz4 wire:  RowBinary {} | Native {}  => {:+.1}% smaller",
        rb.lz4_bytes,
        nat.lz4_bytes,
        pct_lower(rb.lz4_bytes as f64, nat.lz4_bytes as f64)
    );
    println!(
        "          zstd wire: RowBinary {} | Native {}  => {:+.1}% smaller",
        rb.zstd_bytes,
        nat.zstd_bytes,
        pct_lower(rb.zstd_bytes as f64, nat.zstd_bytes as f64)
    );
    // One measurement per format on this schema (client-side encode). The
    // cross-format improvement percentages ride on the `native` arm they
    // describe, so no derived number is dropped.
    Report::measurement("ch_native_format")
        .variant("stage", "client")
        .variant("format", "rowbinary")
        .variant("schema", name)
        .variant("rows", rows as u64)
        .metric("ns_per_row", Metric::minimize(rb.ns_per_row, "ns"))
        .metric("raw_bytes", Metric::minimize(rb.raw_bytes as f64, "bytes"))
        .metric("lz4_bytes", Metric::minimize(rb.lz4_bytes as f64, "bytes"))
        .metric(
            "zstd_bytes",
            Metric::minimize(rb.zstd_bytes as f64, "bytes"),
        )
        .emit();
    Report::measurement("ch_native_format")
        .variant("stage", "client")
        .variant("format", "rowbinary_bare")
        .variant("schema", name)
        .variant("rows", rows as u64)
        .metric("ns_per_row", Metric::minimize(bare_ns, "ns"))
        .note("bare serialize_row reference (encoder-wrapper overhead baseline)")
        .emit();
    Report::measurement("ch_native_format")
        .variant("stage", "client")
        .variant("format", "native")
        .variant("schema", name)
        .variant("rows", rows as u64)
        .metric("ns_per_row", Metric::minimize(nat.ns_per_row, "ns"))
        .metric("raw_bytes", Metric::minimize(nat.raw_bytes as f64, "bytes"))
        .metric("lz4_bytes", Metric::minimize(nat.lz4_bytes as f64, "bytes"))
        .metric(
            "zstd_bytes",
            Metric::minimize(nat.zstd_bytes as f64, "bytes"),
        )
        .metric(
            "encode_cpu_pct_lower",
            Metric::maximize(pct_lower(rb.ns_per_row, nat.ns_per_row), "%"),
        )
        .metric(
            "lz4_pct_smaller",
            Metric::maximize(pct_lower(rb.lz4_bytes as f64, nat.lz4_bytes as f64), "%"),
        )
        .metric(
            "zstd_pct_smaller",
            Metric::maximize(pct_lower(rb.zstd_bytes as f64, nat.zstd_bytes as f64), "%"),
        )
        .emit();
    (rb, nat)
}

// ---- Rig C: server parse CPU (Null engine) --------------------------------

/// Minimal percent-encoding for a query passed in the URL.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn server_parse_cpu(rows: usize, reps: u64) -> Option<(f64, f64, String)> {
    let (host, port, user, password) = docker::ensure_clickhouse();
    let sql =
        |q: &str| docker::clickhouse_sql(&host, port, &user, &password, q).expect("clickhouse sql");
    let version = sql("SELECT version()").trim().to_owned();
    let cols = event_columns();
    let col_defs = cols
        .iter()
        .map(|(n, t)| format!("{n} {t}"))
        .collect::<Vec<_>>()
        .join(", ");
    let col_list = cols.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ");

    let schema = NativeSchema::from_columns(&cols).expect("schema");
    let records: Vec<Record<EventRow>> = gen_events(rows).into_iter().map(record).collect();
    let mut rb_buf = BytesMut::new();
    encode_rowbinary_bare(&records, &mut rb_buf);
    let rb_body = rb_buf.to_vec();
    let mut enc = NativeEncoder::<Owned<EventRow>>::new(schema);
    let mut nat_buf = BytesMut::new();
    encode_native(&mut enc, &records, &mut nat_buf);
    let nat_body = nat_buf.to_vec();

    let cpu_us = |query_id: &str| -> f64 {
        let q = format!(
            "SELECT ProfileEvents['OSCPUVirtualTimeMicroseconds'] FROM system.query_log \
             WHERE query_id = '{query_id}' AND type = 'QueryFinish' LIMIT 1"
        );
        sql(&q).trim().parse::<f64>().unwrap_or(0.0)
    };

    // Measure one engine. The query goes in the URL (percent-encoded); the
    // block/rows are the raw HTTP body (a body-embedded query reads as data —
    // "Empty query"). Interleave formats and take medians to average drift.
    let measure_engine = |table: &str, engine: &str| -> (f64, f64) {
        sql(&format!("DROP TABLE IF EXISTS {table}"));
        sql(&format!(
            "CREATE TABLE {table} ({col_defs}) ENGINE = {engine}"
        ));
        let insert = |fmt: &str, query_id: &str, body: &[u8]| {
            let query = format!("INSERT INTO {table} ({col_list}) FORMAT {fmt}");
            let path = format!(
                "/?query={}&user={user}&password={password}&query_id={query_id}\
                 &async_insert=0&insert_deduplicate=0&max_insert_threads=1&max_threads=1",
                percent_encode(&query)
            );
            let resp = http_post_bytes(&host, port, &path, body).expect("insert");
            assert!(!resp.contains("DB::Exception"), "insert failed: {resp}");
        };
        insert("RowBinary", &format!("warm_rb_{table}"), &rb_body);
        insert("Native", &format!("warm_nat_{table}"), &nat_body);
        let (mut rb_ids, mut nat_ids) = (Vec::new(), Vec::new());
        for i in 0..reps {
            let rb_id = format!("rb_{table}_{i}");
            let nat_id = format!("nat_{table}_{i}");
            insert("RowBinary", &rb_id, &rb_body);
            insert("Native", &nat_id, &nat_body);
            rb_ids.push(rb_id);
            nat_ids.push(nat_id);
        }
        sql("SYSTEM FLUSH LOGS");
        let mut rb: Vec<u64> = rb_ids.iter().map(|id| cpu_us(id) as u64).collect();
        let mut nat: Vec<u64> = nat_ids.iter().map(|id| cpu_us(id) as u64).collect();
        (
            percentile(&mut rb, 0.5) as f64,
            percentile(&mut nat, 0.5) as f64,
        )
    };

    // Null isolates parse + block-form; MergeTree adds format-independent
    // sort/compress/write — the cross-check shows the honest end-to-end
    // dilution of the format delta.
    let (null_rb, null_nat) = measure_engine("events_null", "Null");
    let (mt_rb, mt_nat) = measure_engine("events_mt", "MergeTree ORDER BY user_id");

    println!(
        "\n[events]  server CPU (median over {reps} reps, {rows} rows, single-threaded):\n\
         \tENGINE=Null (parse+form only): RowBinary {null_rb:.0} us | Native {null_nat:.0} us  => Native {:+.1}% CPU\n\
         \tENGINE=MergeTree (end-to-end): RowBinary {mt_rb:.0} us | Native {mt_nat:.0} us  => Native {:+.1}% CPU\n\
         \t(MergeTree-Null ~ format-independent work: RowBinary {:.0} us, Native {:.0} us)",
        pct_lower(null_rb, null_nat),
        pct_lower(mt_rb, mt_nat),
        (mt_rb - null_rb).max(0.0),
        (mt_nat - null_nat).max(0.0),
    );
    // One measurement per (format, engine): server-side parse CPU. The
    // native arm carries its own improvement over rowbinary.
    let emit_engine = |engine: &str, rb: f64, nat: f64| {
        Report::measurement("ch_native_format")
            .variant("stage", "server")
            .variant("format", "rowbinary")
            .variant("schema", "events")
            .variant("engine", engine)
            .variant("rows", rows as u64)
            .metric("cpu_us", Metric::minimize(rb, "us").with_n(reps))
            .emit();
        Report::measurement("ch_native_format")
            .variant("stage", "server")
            .variant("format", "native")
            .variant("schema", "events")
            .variant("engine", engine)
            .variant("rows", rows as u64)
            .metric("cpu_us", Metric::minimize(nat, "us").with_n(reps))
            .metric(
                "native_pct_lower",
                Metric::maximize(pct_lower(rb, nat), "%"),
            )
            .emit();
    };
    emit_engine("null", null_rb, null_nat);
    emit_engine("mergetree", mt_rb, mt_nat);
    // The gate uses the Null-engine parse-isolation number.
    Some((null_rb, null_nat, version))
}

fn main() {
    // Validates BENCH_TRIGGER before any work: it is otherwise read when the
    // first report is built, which is after the measurement.
    benchmarks::preflight();
    let rows = env_u64("ROWS", 200_000) as usize;
    let iters = env_u64("ITERS", 25);
    let reps = env_u64("REPS", 15);
    let run_server = env_u64("SERVER", 1) == 1;

    println!("=== ClickHouse Native vs RowBinary — spike go/no-go ===");
    println!("rows={rows} encode-iters={iters} server-reps={reps}");

    let (ev_rb, ev_nat) = run_client_ab("events", rows, iters);
    let (mt_rb, mt_nat) = run_client_ab("metrics", rows, iters);
    let (_dim_rb, _dim_nat) = run_client_ab("dims", rows, iters);
    // Client-only stress line: not gated, not run against the server rig.
    let (_dhc_rb, _dhc_nat) = run_client_ab("dims_hc", rows, iters);

    let server = if run_server {
        server_parse_cpu(rows.min(200_000), reps)
    } else {
        None
    };

    // ---- GO/NO-GO gate ----
    println!("\n=== GATE ===");
    let metrics_regression = pct_lower(mt_rb.ns_per_row, mt_nat.ns_per_row);
    let client_ok = metrics_regression >= -5.0; // Native not >5% slower on fixed-width
    let wire_ok = pct_lower(ev_rb.lz4_bytes as f64, ev_nat.lz4_bytes as f64) >= 8.0
        || pct_lower(ev_rb.zstd_bytes as f64, ev_nat.zstd_bytes as f64) >= 8.0;
    let server_ok = match &server {
        Some((rb, nat, _)) => pct_lower(*rb, *nat) >= 20.0,
        None => false,
    };
    println!(
        "  client (metrics no >5% regression): {}  [{:+.1}%]",
        if client_ok { "PASS" } else { "FAIL" },
        metrics_regression
    );
    println!(
        "  wire (events >=8% smaller):          {}  [lz4 {:+.1}%, zstd {:+.1}%]",
        if wire_ok { "PASS" } else { "FAIL" },
        pct_lower(ev_rb.lz4_bytes as f64, ev_nat.lz4_bytes as f64),
        pct_lower(ev_rb.zstd_bytes as f64, ev_nat.zstd_bytes as f64),
    );
    println!(
        "  server (events parse CPU >=20% lower): {}",
        match &server {
            Some((rb, nat, _)) => format!(
                "{}  [{:+.1}%]",
                if server_ok { "PASS" } else { "FAIL" },
                pct_lower(*rb, *nat)
            ),
            None => "SKIPPED (set SERVER=1 with Docker)".to_string(),
        }
    );
    let verdict = if client_ok && wire_ok && server_ok {
        "GO — flip default to Native"
    } else {
        "NO-GO — ship Native opt-in, keep RowBinary default"
    };
    println!("\n  VERDICT: {verdict}");
    // Server inserts pin async_insert=0; record the server version so the
    // recorded environment is explicit (server present only under SERVER=1).
    let note = match &server {
        Some((_, _, version)) => format!("{verdict}; async_insert=0; ch_version={version}"),
        None => verdict.to_owned(),
    };
    Report::verdict("ch_native_format")
        .variant("client_ok", client_ok)
        .variant("wire_ok", wire_ok)
        .variant("server_ok", server_ok)
        .note(note)
        .emit();
}
