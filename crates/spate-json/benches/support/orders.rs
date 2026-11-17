//! Decode fixtures shared by `benches/decode_wall.rs` (wall time) and
//! `benches/decode_gungraun.rs` (instruction counts): a flat 15-field record,
//! a seven-field order line, the three payload framings the deserializer
//! supports, clean and corrupted, and a nested event line of the kind a log
//! or event pipeline decodes.
//!
//! Included with `#[path]` rather than imported: a bench target is its own
//! crate, so two targets can only agree on a workload by compiling the same
//! source. If the two ever encoded different documents, a wall-clock result
//! and an instruction count would not be talking about the same bytes.
//!
//! Corpora only. The rig that decodes them is `decode_rig.rs`.

// Each bench target compiles this module separately and uses a different
// subset of it: the counted tier measures a case the wall tier leaves out.
// So an item is legitimately dead in one target while live in the other,
// which is a module-wide `allow` rather than per-item `expect`: an `expect`
// would itself go unfulfilled in whichever target does use the item.
#![allow(dead_code, reason = "each bench target uses a different subset")]

use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::hash::{BuildHasherDefault, DefaultHasher};

#[derive(Serialize, Deserialize)]
pub(crate) struct Order {
    pub(crate) id: i64,
    pub(crate) sku: String,
    pub(crate) customer: String,
    pub(crate) qty: i32,
    pub(crate) price: f64,
    pub(crate) currency: String,
    pub(crate) ts_ms: i64,
    pub(crate) region: String,
    pub(crate) priority: i32,
    pub(crate) discount: f64,
    pub(crate) notes: String,
    pub(crate) paid: bool,
    pub(crate) channel: String,
    pub(crate) warehouse: String,
    pub(crate) coupon: Option<String>,
}

/// One line of an order, the many-small-records workload the framing cases
/// batch. Do not change the field *types* (string, int, string, int, bool,
/// float, string array) or the record's encoded size: both are what the
/// counted tier compares against, and `tests/bench_fixtures.rs` pins the
/// corpus byte-for-byte.
#[derive(Serialize, Deserialize)]
pub(crate) struct LineItem {
    pub(crate) sku: String,
    pub(crate) qty: i64,
    pub(crate) unit: String,
    pub(crate) ts_ms: i64,
    pub(crate) ok: bool,
    pub(crate) ratio: f64,
    pub(crate) tags: Vec<String>,
}

pub(crate) fn sample_order() -> Order {
    Order {
        id: 918_273_645,
        sku: "SKU-000917".to_owned(),
        customer: "cust-4471".to_owned(),
        qty: 7,
        price: 1499.95,
        currency: "USD".to_owned(),
        ts_ms: 1_772_000_000_000,
        region: "emea".to_owned(),
        priority: 2,
        discount: 0.15,
        notes: "expedited; gift wrap".to_owned(),
        paid: true,
        channel: "web".to_owned(),
        warehouse: "wh-eu-3".to_owned(),
        coupon: Some("SAVE15".to_owned()),
    }
}

pub(crate) fn sample_line(i: u64) -> LineItem {
    LineItem {
        sku: format!("SKU-{i:06}"),
        qty: (i as i64) * 37,
        unit: "each".to_owned(),
        ts_ms: 1_772_000_000_000 + i as i64,
        ok: !i.is_multiple_of(5),
        ratio: (i as f64) / 100.0,
        tags: vec!["gift".to_owned(), "eu".to_owned(), format!("bin-{}", i % 8)],
    }
}

pub(crate) fn sample_lines(n: u64) -> Vec<LineItem> {
    (0..n).map(sample_line).collect()
}

pub(crate) fn ndjson<T: Serialize>(lines: &[T]) -> Vec<u8> {
    let mut out = Vec::new();
    for r in lines {
        out.extend_from_slice(&serde_json::to_vec(r).unwrap());
        out.push(b'\n');
    }
    out
}

/// One flat record as a single JSON document, the `single` framing's payload.
pub(crate) fn order_document() -> Vec<u8> {
    serde_json::to_vec(&sample_order()).expect("encode an order")
}

/// `n` lines as a top-level JSON array, the `array` framing's payload.
pub(crate) fn lines_array(n: u64) -> Vec<u8> {
    serde_json::to_vec(&sample_lines(n)).expect("encode an array")
}

/// `n` lines newline-delimited, the `ndjson` framing's payload.
pub(crate) fn lines_ndjson(n: u64) -> Vec<u8> {
    ndjson(&sample_lines(n))
}

// ---------------------------------------------------------------------------
// Event lines.
//
// The records above are flat and their strings are short. A log or event
// pipeline decodes lines of a few hundred bytes, nested two or three levels,
// made mostly of strings, into a struct with `String`, `Vec<String>` and map
// fields. Allocating those fields and validating those strings is most of
// what such a decode costs, so it is the shape a choice of decode backend has
// to be measured on.
// ---------------------------------------------------------------------------

/// std's map with std's SipHash-1-3, keyed by a fixed key in place of a fresh
/// random one.
///
/// The hashing and allocation are those of the `HashMap<String, String>` a
/// user's record declares. A random key makes the counted case vary by up to
/// 0.8% from run to run under the `simd` backend, all of it in glibc malloc.
/// A fixed key keeps it repeating to the instruction.
#[expect(
    clippy::disallowed_types,
    reason = "the bench measures decoding the std map a user's record declares"
)]
pub(crate) type Labels =
    std::collections::HashMap<String, String, BuildHasherDefault<DefaultHasher>>;

/// One event line: a request log entry with its host, request and client.
///
/// Three objects deep (`Event`, `Request`, `Client`), with one integer among
/// nineteen string values. `tests/bench_fixtures.rs` pins the corpus bytes and
/// the line shape.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct Event {
    pub(crate) id: String,
    pub(crate) ts: String,
    pub(crate) level: String,
    pub(crate) service: String,
    pub(crate) message: String,
    pub(crate) host: Host,
    pub(crate) request: Request,
    pub(crate) tags: Vec<String>,
    #[serde(serialize_with = "in_key_order")]
    pub(crate) labels: Labels,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct Host {
    pub(crate) name: String,
    pub(crate) zone: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct Request {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) status: u16,
    pub(crate) user_agent: String,
    pub(crate) client: Client,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct Client {
    pub(crate) ip: String,
    pub(crate) city: String,
}

/// Writes a map's entries sorted by key. A `HashMap` iterates in an order its
/// per-process seed decides, and the corpus has to be the same bytes on every
/// run for `tests/bench_fixtures.rs` to pin it.
fn in_key_order<S: Serializer>(map: &Labels, s: S) -> Result<S::Ok, S::Error> {
    map.iter().collect::<BTreeMap<_, _>>().serialize(s)
}

// Table lengths are primes from 2 to 13, so the table values on a line repeat
// together only every 30,030 lines, well past the corpus.

const LEVELS: [&str; 3] = ["info", "warn", "error"];

const METHODS: [&str; 2] = ["GET", "POST"];

const SERVICES: [&str; 5] = [
    "checkout-api",
    "inventory-worker",
    "auth-gateway",
    "search-frontend",
    "billing-reconciler",
];

/// One message carries an escaped quote, so both backends leave their fast
/// string path on some lines.
const MESSAGES: [&str; 7] = [
    "request completed for customer \"{id}\" after a cache miss",
    "upstream returned a retryable status; retrying in 200 ms",
    "order {id} moved from pending to fulfilled",
    "token refresh succeeded for session {id}",
    "slow query on orders_by_customer exceeded the 250 ms budget",
    "cart {id} merged with the anonymous cart",
    "rate limit reached for api key {id}; rejected with 429",
];

const PATHS: [&str; 11] = [
    "/api/v2/orders/{id}/items",
    "/api/v2/customers/{id}/addresses/default",
    "/api/v1/search?q=wireless+headphones&page=2",
    "/static/js/app.3f9c2e1b.min.js",
    "/api/v2/payments/{id}/capture",
    "/api/v2/carts/{id}/merge",
    "/api/v1/recommendations?customer={id}",
    "/api/v2/inventory/sku/{id}/reserve",
    "/auth/oauth2/token",
    "/api/v2/orders/{id}/cancel",
    "/api/v1/catalog/categories/audio/products",
];

const AGENTS: [&str; 5] = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
    "okhttp/4.12.0 (Android 14; Pixel 8)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) Mobile/15E148",
    "python-requests/2.32.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) Gecko/20100101 Firefox/132.0",
];

/// Several carry multi-byte UTF-8, which both backends validate.
const CITIES: [&str; 13] = [
    "London, United Kingdom",
    "São Paulo, Brazil",
    "Zürich, Switzerland",
    "Kraków, Poland",
    "東京, Japan",
    "Montréal, Canada",
    "Dublin, Ireland",
    "Frankfurt, Germany",
    "Málaga, Spain",
    "Singapore, Singapore",
    "Göteborg, Sweden",
    "Austin, United States",
    "Reykjavík, Iceland",
];

const TEAMS: [&str; 7] = [
    "payments-core",
    "platform-infra",
    "search-relevance",
    "growth-experiments",
    "fulfilment-ops",
    "identity-access",
    "catalog-quality",
];

const ROLLOUTS: [&str; 3] = ["edge-cache-hit", "canary-release", "batch-replay"];

/// Line `i` of the event corpus.
pub(crate) fn sample_event(i: u64) -> Event {
    let id = format!("{i:08x}");
    let pick = |table: &[&str]| {
        let n = table.len() as u64;
        table[usize::try_from(i % n).expect("an index below the table length")].replace("{id}", &id)
    };
    let service = pick(&SERVICES);
    let labels = Labels::from_iter([
        ("team".to_owned(), pick(&TEAMS)),
        ("deployment".to_owned(), format!("{service}-{}", i % 3)),
        ("release".to_owned(), format!("release-2026.11.{}", i % 17)),
    ]);
    Event {
        id: format!("0f8e7d6c-5b4a-4392-8170-{i:012x}"),
        ts: format!(
            "2026-03-01T12:{:02}:{:02}.{:03}Z",
            (i / 60) % 60,
            i % 60,
            (i * 37) % 1000
        ),
        level: pick(&LEVELS),
        message: pick(&MESSAGES),
        host: Host {
            name: format!("web-{:03}.prod.internal", i % 211),
            zone: format!("eu-west-2{}", ["a", "b", "c"][(i % 3) as usize]),
        },
        request: Request {
            method: pick(&METHODS),
            path: pick(&PATHS),
            status: [200, 201, 304, 404, 503][(i % 5) as usize],
            user_agent: pick(&AGENTS),
            client: Client {
                ip: format!("203.0.113.{}", i % 256),
                city: pick(&CITIES),
            },
        },
        tags: vec![
            pick(&ROLLOUTS),
            format!("partition-{:02}", i % 16),
            format!("region-{}", ["eu-west", "us-east"][(i % 2) as usize]),
        ],
        service,
        labels,
    }
}

/// `n` event lines newline-delimited, the `ndjson` framing's payload.
pub(crate) fn events_ndjson(n: u64) -> Vec<u8> {
    ndjson(&(0..n).map(sample_event).collect::<Vec<_>>())
}

// ---------------------------------------------------------------------------
// Poison-pill corpora.
//
// The deserializer's error isolation (drop a record and keep going, or fail
// the payload without emitting a prefix) runs only on input that does not
// decode. These build the same records with a known fraction of them broken,
// in a known way, at a known position.
// ---------------------------------------------------------------------------

/// Records in the error-policy corpora.
///
/// Forty times the fifty-record batch the reference cases carry, from this
/// same builder, which puts those cases in the 10^6–10^8 instruction band the
/// rest of this workspace's counter benches sit in while leaving the clean
/// case an exact scale-up of the reference one rather than a different
/// workload at a different size.
///
/// Shared with the test that pins the corpora: two copies of this would let
/// the bench's corpus drift while the test went on passing against its own.
pub(crate) const RECORDS: u64 = 2_000;

/// One record in this many is broken in the partial-poison corpora.
///
/// Ten per cent is a rate a real stream reaches, such as a schema drift on
/// one producer of ten, and it is far enough from both ends that the count is
/// neither the clean path with a rounding error on top nor the all-poison case
/// under another name.
pub(crate) const BAD_EVERY: u64 = 10;

/// How a poison record is broken.
///
/// The two kinds reach the failure from opposite ends of the parser, which is
/// why both are worth a case: one stops the parser before it has a value, the
/// other hands `serde` a value it cannot use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Corruption {
    /// The document with its closing brace removed. Every field parses, and
    /// then the input ends, giving a *syntax* error (`is_data` false) reached
    /// after the parser has done the whole document's work.
    Syntax,
    /// A well-formed document whose `sku` is a number where [`LineItem`] wants
    /// a string. The parse succeeds and the *mapping* fails, giving a data
    /// error, the one a schema drift produces rather than a truncated write.
    ///
    /// Where in the record it fails is part of the workload, and it is not the
    /// declaration order: this document is built with `json!`, whose map is a
    /// `BTreeMap`, so its keys are emitted **alphabetically**. `sku` sits
    /// fourth, and `ok`, `qty` and `ratio` decode successfully before the
    /// rejection. Renaming a field can therefore move the failure position
    /// without touching this code.
    TypeMismatch,
}

/// One record, encoded and then broken.
pub(crate) fn bad_line(i: u64, how: Corruption) -> Vec<u8> {
    let r = sample_line(i);
    match how {
        Corruption::Syntax => {
            let mut line = serde_json::to_vec(&r).expect("encode an order line");
            let brace = line.pop();
            assert_eq!(brace, Some(b'}'), "a record does not end in a brace");
            line
        }
        Corruption::TypeMismatch => serde_json::to_vec(&serde_json::json!({
            "sku": i,
            "qty": r.qty,
            "unit": r.unit,
            "ts_ms": r.ts_ms,
            "ok": r.ok,
            "ratio": r.ratio,
            "tags": r.tags,
        }))
        .expect("encode a mismatched order line"),
    }
}

/// `records` lines newline-delimited, with every `bad_every`-th record
/// broken (`bad_every` of 0 breaks none, 1 breaks all).
pub(crate) fn lines_ndjson_bad_every(records: u64, bad_every: u64, how: Corruption) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..records {
        if bad_every != 0 && (i + 1).is_multiple_of(bad_every) {
            out.extend_from_slice(&bad_line(i, how));
        } else {
            out.extend_from_slice(&serde_json::to_vec(&sample_line(i)).unwrap());
        }
        out.push(b'\n');
    }
    out
}

/// How many records a corpus built by [`lines_ndjson_bad_every`] must
/// yield. Derived from the same arithmetic the builder uses, so the two cannot
/// disagree about which lines are broken.
pub(crate) fn good_lines(records: u64, bad_every: u64) -> u64 {
    if bad_every == 0 {
        return records;
    }
    records
        - (1..=records)
            .filter(|i| i.is_multiple_of(bad_every))
            .count() as u64
}

/// `records` lines newline-delimited with only the **last** one broken.
///
/// Late on purpose. Under `on_error: fail` the ndjson path decodes every line
/// into a holding buffer before emitting any of them, so a payload that fails
/// on its last line is the one that pays for the whole decode and then throws
/// all of it away. That is the atomic path's worst case, and the one that
/// says what atomicity costs.
pub(crate) fn lines_ndjson_bad_last(records: u64, how: Corruption) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..records - 1 {
        out.extend_from_slice(&serde_json::to_vec(&sample_line(i)).unwrap());
        out.push(b'\n');
    }
    out.extend_from_slice(&bad_line(records - 1, how));
    out.push(b'\n');
    out
}

/// `records` lines as a top-level array with only the **last** element
/// broken, the array framing's counterpart to
/// [`lines_ndjson_bad_last`], and broken the same way at the same position
/// so the pair differs in framing and nothing else.
///
/// A syntax error here would end the array parse early rather than late, so
/// this takes the [`TypeMismatch`](Corruption::TypeMismatch) kind: the parser
/// builds every element before `serde` rejects the last, and the whole vector
/// is discarded.
pub(crate) fn lines_array_bad_last(records: u64) -> Vec<u8> {
    let mut out = Vec::from(b"[");
    for i in 0..records - 1 {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(&serde_json::to_vec(&sample_line(i)).unwrap());
    }
    out.push(b',');
    out.extend_from_slice(&bad_line(records - 1, Corruption::TypeMismatch));
    out.push(b']');
    out
}
