//! Row fixtures for the encoder bench: two schemas chosen because they
//! exercise different halves of the encoder.
//!
//! `events` is the wide, string-heavy shape — LowCardinality dictionaries,
//! a Nullable null-map, an Array with its offsets, and a Decimal. `metrics`
//! is narrow and fixed-width, almost entirely numeric. A change that speeds
//! up dictionary handling and slows down fixed-width columns moves the two in
//! opposite directions, which one schema alone could not show.
//!
//! Deliberately the same shapes the wall-clock `ch_native_format` rig uses, so
//! a count here and a throughput number there describe the same rows.
//!
//! Row values are pure functions of the index — no random source — because an
//! instruction count is only comparable when both legs encoded identical
//! bytes. The distributions matter beyond determinism: `country` and
//! `event_type` repeat from a small set, so the LowCardinality dictionaries
//! stay small and hit, while `city` and `url` vary widely, which is what
//! makes the dictionary path do real work rather than degenerate.

use serde::Serialize;
use spate_clickhouse::{DateTime64Millis, Decimal64};

const EVENT_TYPES: &[&str] = &[
    "click", "view", "purchase", "signup", "logout", "search", "share", "error",
];
const COUNTRIES: &[&str] = &["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA", "AU", "NL"];

/// Rows per encoded chunk. A ClickHouse insert is a block, not a row, so the
/// per-row cost is only meaningful amortised over one — and the dictionary
/// columns only behave realistically once a block holds repeats.
pub(crate) const ROWS: usize = 1_000;

#[derive(Clone, Serialize)]
pub(crate) struct EventRow {
    pub(crate) event_id: u64,
    pub(crate) event_time: DateTime64Millis,
    pub(crate) user_id: u64,
    pub(crate) event_type: String,
    pub(crate) country: String,
    pub(crate) city: String,
    pub(crate) url: String,
    pub(crate) referrer: Option<String>,
    pub(crate) status: u16,
    pub(crate) duration_ms: u32,
    pub(crate) revenue: Decimal64<4>,
    pub(crate) tags: Vec<String>,
    pub(crate) is_conversion: bool,
}

#[derive(Clone, Serialize)]
pub(crate) struct MetricRow {
    pub(crate) ts: DateTime64Millis,
    pub(crate) host_id: u32,
    pub(crate) cpu: f64,
    pub(crate) mem: f64,
    pub(crate) disk: f64,
    pub(crate) net_in: u64,
    pub(crate) net_out: u64,
    pub(crate) load1: f32,
    pub(crate) load5: f32,
    pub(crate) load15: f32,
    pub(crate) reqs: u64,
    pub(crate) errors: u32,
    pub(crate) latency_p99: f64,
    pub(crate) uptime: u64,
}

pub(crate) fn event_columns() -> Vec<(&'static str, &'static str)> {
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

pub(crate) fn metric_columns() -> Vec<(&'static str, &'static str)> {
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

pub(crate) fn events(n: usize) -> Vec<EventRow> {
    (0..n)
        .map(|i| EventRow {
            event_id: i as u64,
            event_time: DateTime64Millis(1_700_000_000_000 + (i as i64) * 137),
            user_id: (i as u64).wrapping_mul(2_654_435_761) % 1_000_000,
            event_type: EVENT_TYPES[i % EVENT_TYPES.len()].to_owned(),
            country: COUNTRIES[i % COUNTRIES.len()].to_owned(),
            city: format!("city{}", i % 5000),
            url: format!("https://example.com/p/{}/item/{}", i % 1000, i % 97),
            referrer: (i % 7 != 0).then(|| format!("https://ref.example/{}", i % 300)),
            status: [200u16, 200, 200, 404, 500][i % 5],
            duration_ms: (i as u32 * 13) % 9_000,
            revenue: Decimal64::<4>(((i as i64) * 251) % 1_000_000),
            tags: (0..i % 5)
                .map(|t| EVENT_TYPES[(i + t) % EVENT_TYPES.len()].to_owned())
                .collect(),
            is_conversion: i % 11 == 0,
        })
        .collect()
}

pub(crate) fn metrics(n: usize) -> Vec<MetricRow> {
    (0..n)
        .map(|i| {
            let f = i as f64;
            MetricRow {
                ts: DateTime64Millis(1_700_000_000_000 + (i as i64) * 1000),
                host_id: (i as u32) % 500,
                cpu: f % 100.0,
                mem: (f * 1.5) % 100.0,
                disk: (f * 0.25) % 100.0,
                net_in: (i as u64) * 1_024,
                net_out: (i as u64) * 512,
                load1: (f % 8.0) as f32,
                load5: (f % 6.0) as f32,
                load15: (f % 4.0) as f32,
                reqs: (i as u64) * 7,
                errors: (i as u32) % 23,
                latency_p99: (f % 250.0) + 1.0,
                uptime: 86_400 + (i as u64),
            }
        })
        .collect()
}
