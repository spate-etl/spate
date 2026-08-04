//! Row fixtures for the encoder bench: three schemas chosen because they
//! exercise different halves of the encoder.
//!
//! `events` is the wide, string-heavy shape — LowCardinality dictionaries,
//! a Nullable null-map, an Array with its offsets, and a Decimal. `metrics`
//! is narrow and fixed-width, almost entirely numeric. A change that speeds
//! up dictionary handling and slows down fixed-width columns moves the two in
//! opposite directions, which one schema alone could not show.
//!
//! `exotic` is the third: every column type whose writer the other two never
//! reach. `Map` and `Tuple` are the composite writers with their own offset
//! and per-element streams; `FixedString` and the fixed-width blobs (`UUID`,
//! `IPv6`, `Int256`, `UInt256`) route through the raw byte sink rather than
//! the column serializer at all; `Enum8`/`Enum16` are width-selected integer
//! leaves; and `LowCardinality(Nullable(String))` is the dictionary's other
//! seeding rule, where index 0 is reserved for NULL. It is a separate schema
//! rather than more columns on `events` because the encoded size of `events`
//! is pinned: growing it would silently re-baseline every count ever recorded
//! against it.
//!
//! The shapes are chosen to be representative of an ingest schema rather than
//! minimal — the same columns and types a wall-clock measurement of this path
//! would want — so a count taken here and a throughput number taken elsewhere
//! describe comparable work.
//!
//! Row values are pure functions of the index — no random source — because an
//! instruction count is only comparable when both legs encoded identical
//! bytes. The distributions matter beyond determinism, and they deliberately
//! cover both dictionary regimes: `event_type`, `country` and `tags` repeat
//! from small sets, so those dictionaries stay small and hit, while `city` is
//! distinct on every row at this block size — a dictionary that grows one
//! entry per row, which is the cost of the opposite case. `url` is a plain
//! `String` and pays no dictionary at all.

use serde::Serialize;
use serde_repr::Serialize_repr;
use spate_clickhouse::{DateTime64Millis, Decimal64, Int256, UInt256};
use std::collections::BTreeMap;
use std::net::Ipv6Addr;

const EVENT_TYPES: &[&str] = &[
    "click", "view", "purchase", "signup", "logout", "search", "share", "error",
];
const COUNTRIES: &[&str] = &["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA", "AU", "NL"];

/// `FixedString(8)` values, deliberately of mixed length: the writer pads a
/// short value with NUL to the column width, and only `jp-tokyo` fills it
/// exactly, so both sides of that branch run within one block.
const CODES: &[&str] = &[
    "us", "gb-1", "de-lon", "fr", "jp-tokyo", "br-sp", "in-blr", "ca",
];

/// `LowCardinality(Nullable(String))` values. A small repeating set so the
/// dictionary hits, and one row in seven is NULL — the reserved index-0 slot
/// a nullable dictionary seeds and a plain one does not.
const TIERS: &[&str] = &["free", "pro", "enterprise"];

/// Rows per encoded chunk. A ClickHouse insert is a block, not a row, so the
/// per-row cost is only meaningful amortised over one — and a dictionary
/// column only shows its hit path once a block holds repeats, which at this
/// size `event_type`, `country` and `tags` provide.
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

/// An `Enum8` column value. ClickHouse stores an `Enum8` as its `Int8`
/// ordinal, which is what a `#[repr(i8)]` enum plus `serde_repr` produces —
/// the shape a user of this sink writes, rather than a bare `i8` that would
/// measure the same leaf without the derive in front of it.
#[derive(Clone, Copy, Serialize_repr)]
#[repr(i8)]
pub(crate) enum Level {
    Debug = -1,
    Info = 0,
    Warn = 1,
    Error = 2,
}

/// An `Enum16` column value: the same idea one width up, so a change that
/// mis-sizes an enum leaf shows on one of the two.
#[derive(Clone, Copy, Serialize_repr)]
#[repr(i16)]
pub(crate) enum Kind {
    Ingest = 300,
    Query = 301,
    Merge = 302,
}

#[derive(Clone, Serialize)]
pub(crate) struct ExoticRow {
    pub(crate) id: u64,
    pub(crate) code: String,
    pub(crate) session: (u64, u64),
    pub(crate) peer: Ipv6Addr,
    pub(crate) balance: Int256,
    pub(crate) supply: UInt256,
    pub(crate) level: Level,
    pub(crate) kind: Kind,
    pub(crate) tier: Option<String>,
    pub(crate) attrs: BTreeMap<String, u64>,
    pub(crate) span: (String, u32),
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

/// The `Tuple`, `Map`, blob and enum column types, none of which the other
/// two schemas reach.
///
/// `session` is declared `UUID` and carried as a `(u64, u64)` pair rather
/// than a `uuid::Uuid`: the wire layout is identical — the two halves of the
/// value as little-endian `u64`s, which is exactly what `serde::uuid` emits —
/// and the bench builds under the crate's default features, where the
/// optional `uuid` dependency is absent.
pub(crate) fn exotic_columns() -> Vec<(&'static str, &'static str)> {
    vec![
        ("id", "UInt64"),
        ("code", "FixedString(8)"),
        ("session", "UUID"),
        ("peer", "IPv6"),
        ("balance", "Int256"),
        ("supply", "UInt256"),
        (
            "level",
            "Enum8('debug' = -1, 'info' = 0, 'warn' = 1, 'error' = 2)",
        ),
        (
            "kind",
            "Enum16('ingest' = 300, 'query' = 301, 'merge' = 302)",
        ),
        ("tier", "LowCardinality(Nullable(String))"),
        ("attrs", "Map(String, UInt64)"),
        ("span", "Tuple(String, UInt32)"),
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

/// Every value a pure function of the index, as above. Two distributions are
/// deliberate: `attrs` carries the same three keys on every row, so the Map's
/// key stream is the repetitive shape a real attribute map has, and `tier` is
/// NULL on one row in seven, so the nullable dictionary writes both a NULL
/// key and interned hits within one block.
pub(crate) fn exotic(n: usize) -> Vec<ExoticRow> {
    (0..n)
        .map(|i| {
            let seg = i as u16;
            ExoticRow {
                id: i as u64,
                code: CODES[i % CODES.len()].to_owned(),
                session: ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15), i as u64),
                peer: Ipv6Addr::new(0x2001, 0x0db8, seg, 0, 0, 0x8a2e, 0x0370, seg ^ 0x7334),
                balance: Int256::from_i128((i as i128) * -1_000_003),
                supply: UInt256::from_u128((i as u128) * 7_919),
                level: match i % 4 {
                    0 => Level::Debug,
                    1 => Level::Info,
                    2 => Level::Warn,
                    _ => Level::Error,
                },
                kind: match i % 3 {
                    0 => Kind::Ingest,
                    1 => Kind::Query,
                    _ => Kind::Merge,
                },
                tier: (i % 7 != 0).then(|| TIERS[i % TIERS.len()].to_owned()),
                attrs: BTreeMap::from([
                    ("bytes".to_owned(), (i as u64) * 1_024),
                    ("rows".to_owned(), (i as u64) % 997),
                    ("ms".to_owned(), (i as u64) % 250),
                ]),
                span: (format!("stage-{}", i % 16), (i as u32 * 31) % 5_000),
            }
        })
        .collect()
}
