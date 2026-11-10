//! A generated corpus of log event lines: newline-delimited JSON of about 600
//! bytes a line, nested two levels deep and mostly strings, the shape a log
//! or event pipeline decodes.
//!
//! Line `i` is a pure function of `i`, so a benchmark and the test that pins
//! it agree on every byte. Each line decodes into [`Event`]. Strings are ASCII
//! and need no escapes.

#![expect(
    clippy::disallowed_types,
    reason = "`labels` is the std map a user's own struct declares, and decoding into it is what the corpus prices"
)]

use serde::Deserialize;
use spate_bench::rng::SplitMix64;
use std::collections::HashMap;

/// One log event, as a line of the corpus decodes.
#[derive(Debug, Deserialize)]
pub struct Event {
    /// RFC 3339 timestamp with milliseconds.
    pub ts: String,
    /// The emitting host's name.
    pub host: String,
    /// The emitting service.
    pub service: String,
    /// Severity: `debug`, `info`, `warn` or `error`.
    pub level: String,
    /// Free text of 30 to 100 bytes.
    pub message: String,
    /// 32 hex digits.
    pub trace_id: String,
    /// The request the event describes.
    pub http: Http,
    /// Who made the request.
    pub client: Client,
    /// One to four tags.
    pub tags: Vec<String>,
    /// Two to four labels.
    pub labels: HashMap<String, String>,
}

/// The request half of an [`Event`].
#[derive(Debug, Deserialize)]
pub struct Http {
    /// Request method.
    pub method: String,
    /// Request path.
    pub path: String,
    /// Response status.
    pub status: u16,
    /// The client's `User-Agent`.
    pub user_agent: String,
    /// The `Referer` the request carried.
    pub referrer: String,
}

/// The client half of an [`Event`].
#[derive(Debug, Deserialize)]
pub struct Client {
    /// Client address, dotted quad.
    pub ip: String,
    /// Where the address resolves to.
    pub geo: Geo,
}

/// Where a [`Client`] address resolves to.
#[derive(Debug, Deserialize)]
pub struct Geo {
    /// ISO 3166-1 alpha-2 code.
    pub country: String,
    /// Region name.
    pub region: String,
    /// City name.
    pub city: String,
}

const SERVICES: &[&str] = &[
    "checkout-api",
    "payments-gateway",
    "search-indexer",
    "inventory-sync",
    "identity-provider",
    "recommendations",
    "order-router",
    "media-transcoder",
];

/// Weighted toward `info`, as production logs are.
const LEVELS: &[&str] = &[
    "info", "info", "info", "info", "info", "warn", "debug", "error",
];

const METHODS: &[&str] = &["GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"];

const RESOURCES: &[&str] = &[
    "orders",
    "customers",
    "products",
    "carts",
    "invoices",
    "shipments",
];

const STATUSES: &[u16] = &[200, 200, 200, 200, 201, 204, 304, 400, 404, 500];

const AGENTS: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "okhttp/4.12.0",
    "python-requests/2.32.3",
];

const PLACES: &[(&str, &str, &str)] = &[
    ("GB", "England", "London"),
    ("GB", "Scotland", "Edinburgh"),
    ("DE", "Bavaria", "Munich"),
    ("FR", "Ile-de-France", "Paris"),
    ("US", "Virginia", "Ashburn"),
    ("US", "California", "San Jose"),
    ("JP", "Tokyo", "Tokyo"),
    ("BR", "Sao Paulo", "Sao Paulo"),
];

const WORDS: &[&str] = &[
    "request",
    "completed",
    "upstream",
    "timeout",
    "retrying",
    "cache",
    "miss",
    "for",
    "customer",
    "session",
    "token",
    "refreshed",
    "after",
    "validation",
    "failed",
    "on",
    "field",
    "quantity",
    "payment",
    "authorised",
    "shipment",
    "scheduled",
    "warehouse",
    "connection",
    "pool",
    "exhausted",
    "slow",
    "query",
];

const TAGS: &[&str] = &[
    "canary",
    "beta",
    "mobile",
    "internal",
    "batch",
    "retry",
    "cold-start",
    "eu",
];

/// Label keys, in the order a line writes them.
const LABEL_KEYS: &[&str] = &["env", "zone", "team", "version"];

const ZONES: &[&str] = &["euw2-az1", "euw2-az2", "euw2-az3"];

const TEAMS: &[&str] = &["payments", "storefront", "platform", "search"];

/// The draws of one line, from [`SplitMix64`] seeded with the line index.
struct Draw(SplitMix64);

impl Draw {
    fn next(&mut self) -> u64 {
        self.0.next_u64()
    }

    /// A value in `0..n`.
    fn below(&mut self, n: usize) -> usize {
        // `n` is a table length or a small bound, so both casts are lossless.
        self.0.below(n as u64) as usize
    }

    fn pick<'a, T>(&mut self, from: &'a [T]) -> &'a T {
        &from[self.below(from.len())]
    }
}

/// Appends line `i` of the corpus to `out`, its newline included.
pub fn event_line(i: u64, out: &mut Vec<u8>) {
    let mut d = Draw(SplitMix64::new(i));
    let mut line = String::with_capacity(768);
    let ms = 1_793_000_000_000 + i * 7;
    let (secs, millis) = (ms / 1_000, ms % 1_000);
    line.push_str(&format!(
        "{{\"ts\":\"2026-11-{:02}T{:02}:{:02}:{:02}.{millis:03}Z\"",
        1 + secs / 86_400 % 28,
        secs / 3_600 % 24,
        secs / 60 % 60,
        secs % 60,
    ));
    line.push_str(&format!(
        ",\"host\":\"ip-10-{}-{}-{}.internal\"",
        d.below(4),
        d.below(256),
        d.below(256)
    ));
    line.push_str(&format!(",\"service\":\"{}\"", d.pick(SERVICES)));
    line.push_str(&format!(",\"level\":\"{}\"", d.pick(LEVELS)));
    line.push_str(",\"message\":\"");
    let start = line.len();
    let target = 30 + d.below(61);
    while line.len() - start < target {
        if line.len() > start {
            line.push(' ');
        }
        line.push_str(d.pick(WORDS));
    }
    line.push('"');
    line.push_str(&format!(
        ",\"trace_id\":\"{:016x}{:016x}\"",
        d.next(),
        d.next()
    ));
    line.push_str(&format!(
        ",\"http\":{{\"method\":\"{}\",\"path\":\"/api/v2/{}/{}\",\"status\":{},\"user_agent\":\"{}\",\"referrer\":\"https://shop.example.com/{}?page={}\"}}",
        d.pick(METHODS),
        d.pick(RESOURCES),
        d.below(1_000_000),
        d.pick(STATUSES),
        d.pick(AGENTS),
        d.pick(RESOURCES),
        d.below(50),
    ));
    let (country, region, city) = d.pick(PLACES);
    line.push_str(&format!(
        ",\"client\":{{\"ip\":\"{}.{}.{}.{}\",\"geo\":{{\"country\":\"{country}\",\"region\":\"{region}\",\"city\":\"{city}\"}}}}",
        1 + d.below(223),
        d.below(256),
        d.below(256),
        d.below(256),
    ));
    line.push_str(",\"tags\":[");
    for t in 0..=d.below(4) {
        if t > 0 {
            line.push(',');
        }
        line.push_str(&format!("\"{}\"", d.pick(TAGS)));
    }
    line.push_str("],\"labels\":{");
    let labels = 2 + d.below(3);
    for (n, key) in LABEL_KEYS.iter().take(labels).enumerate() {
        if n > 0 {
            line.push(',');
        }
        let value = match *key {
            "env" => (*d.pick(&["prod", "staging"])).to_owned(),
            "zone" => (*d.pick(ZONES)).to_owned(),
            "team" => (*d.pick(TEAMS)).to_owned(),
            _ => format!("1.{}.{}", d.below(60), d.below(10)),
        };
        line.push_str(&format!("\"{key}\":\"{value}\""));
    }
    line.push_str("}}\n");
    out.extend_from_slice(line.as_bytes());
}
