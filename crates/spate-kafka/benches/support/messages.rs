//! Record corpora and the header-stamping encoder for the message-encode
//! bench.
//!
//! An instruction count only compares across two legs that encoded
//! byte-identical input, so nothing here may vary between runs: payload bytes
//! come from a fixed linear congruential generator rather than a random
//! source, and every field is a pure function of the record index. That also
//! means no `rand` dependency, which a bench-only corpus does not justify.
//!
//! Two corpora, because the two encoders take different record types.
//! [`payloads`] is the opaque-bytes shape a Kafka-to-Kafka passthrough
//! carries, at one fixed length: the encode cost is a function of payload
//! length, and holding it constant is what makes the keyless case a usable
//! denominator for the keyed and header-stamped ones — the only thing that
//! differs between the three is work the connector itself does. [`events`] is
//! the typed shape a JSON encoder serializes, sized so its documents land near
//! [`PAYLOAD_LEN`].
//!
//! [`HeaderStamp`] lives here rather than in the bench because the fixture
//! test drives it too: the guard-trip case rests on the claim that the headers
//! — not the payload — are what push a record past the limit, and that claim
//! is checked wherever `cargo test` runs rather than only under valgrind.

// The bench and the fixture test each compile this module separately and use a
// different subset of it — the declared header total and the typed key
// extractor exist for the test to check, and no case measures them. So an item
// is legitimately dead in one target while live in the other, which is a
// module-wide `allow` rather than a per-item `expect`: an `expect` would itself
// go unfulfilled in whichever target does use the item.
#![allow(dead_code, reason = "each target uses a different subset")]

use serde::Serialize;
use spate_core::deser::Owned;
use spate_core::error::SinkError;
use spate_core::record::Record;
use spate_kafka::sink::{KafkaMessage, MessageEncoder};

/// Records per case.
///
/// One encode is a few hundred instructions, three orders of magnitude below
/// what a counter can resolve against run-to-run codegen jitter, so the
/// measured region has to be a run of records rather than one call. Ten
/// thousand is roughly eighty sealed chunks at the shipped 64 KiB
/// `ChunkConfig::target_bytes` for this payload size — long enough that
/// steady-state per-record encoding dominates and the scratch buffers'
/// one-time growth to their high-water mark is a rounding error, which is the
/// regime a running pipeline spends all of its time in. A single chunk's ~130
/// records would measure the growth instead.
pub(crate) const RECORDS: usize = 10_000;

/// Every passthrough payload is exactly this long.
///
/// Fixed rather than drawn from a range: the guard-trip case needs its trip to
/// be a property of the header bytes at every record, not an accident of where
/// a length landed, and a varying length would buy nothing else — the copy
/// cost is linear in it, so a spread only averages back to the same number.
pub(crate) const PAYLOAD_LEN: usize = 512;

/// Leading bytes of a passthrough payload that [`key_prefix`] lifts out as the
/// message key: a fixed-width identifier, the shape a Kafka-to-Kafka pipeline
/// re-derives when it has to preserve partitioning (a record carries only the
/// source key's *hash*, never the key).
pub(crate) const KEY_LEN: usize = 16;

/// Header bytes [`HeaderStamp`] adds per message — names plus values, which is
/// what the size guard sums.
///
/// Restated as a constant so [`GUARD_LIMIT`] can be reasoned about here; the
/// fixture test fails if the stamp and this ever disagree.
pub(crate) const HEADER_BYTES: usize = 86;

/// The `max_message_bytes` the guard-trip case runs under.
///
/// Exactly [`PAYLOAD_LEN`], so a payload on its own sits *at* the limit and
/// passes, while the same payload carrying [`HeaderStamp`]'s headers is over
/// it and fails. That is the property the case exists to exercise: this guard
/// counts header bytes, unlike librdkafka's client-side key-plus-payload
/// check, so an oversized-with-headers record stays under the record-level
/// Skip/Fail policy instead of failing a whole batch at the broker.
pub(crate) const GUARD_LIMIT: usize = PAYLOAD_LEN;

// The relation the guard case rests on, checked where it is stated: a payload
// fits and a payload plus the stamp's headers does not. It is the constants'
// half of the claim — that the stamp really adds [`HEADER_BYTES`] is the
// fixture test's half, since `guarded_size` is private.
const _: () = assert!(
    PAYLOAD_LEN <= GUARD_LIMIT && PAYLOAD_LEN + HEADER_BYTES > GUARD_LIMIT,
    "the declared header bytes no longer decide the guard case"
);

/// A linear congruential generator with Knuth's MMIX constants. Reproducible
/// across platforms and architectures, which `DefaultHasher` and `rand` are
/// explicitly not.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Lcg {
        Lcg(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

/// The message key a passthrough payload carries: its fixed-width leading
/// identifier.
///
/// A slice of the payload rather than a scan for a delimiter, deliberately.
/// What the keyed case measures is the connector's own extra work — the copy
/// into the accumulator's key buffer and the extra framed section — and a key
/// function that walked the payload would fold the caller's cost into that
/// difference.
pub(crate) fn key_prefix(payload: &[u8]) -> Option<&[u8]> {
    payload.get(..KEY_LEN)
}

/// The identifier at the head of every record, in both corpora: exactly
/// [`KEY_LEN`] printable bytes, so a payload's leading slice is a whole key
/// rather than a truncated one.
fn user_id(index: usize) -> String {
    format!("user-{index:011}")
}

/// `n` opaque payloads of exactly [`PAYLOAD_LEN`] bytes, each opening with a
/// [`KEY_LEN`]-byte printable identifier and filled with generator bytes
/// after it.
///
/// The body is generated rather than repeated because nothing here should
/// depend on believing that a copy costs the same for a constant run of bytes
/// as for arbitrary ones.
pub(crate) fn payloads(n: usize) -> Vec<Vec<u8>> {
    let mut lcg = Lcg::new(0x5EED_0011);
    (0..n)
        .map(|i| {
            let mut payload = Vec::with_capacity(PAYLOAD_LEN);
            payload.extend_from_slice(user_id(i).as_bytes());
            assert_eq!(payload.len(), KEY_LEN, "the identifier is not a whole key");
            while payload.len() < PAYLOAD_LEN {
                payload.extend_from_slice(&lcg.next().to_le_bytes());
            }
            payload.truncate(PAYLOAD_LEN);
            payload
        })
        .collect()
}

const KINDS: &[&str] = &["click", "view", "purchase", "signup", "search", "share"];
const COUNTRIES: &[&str] = &["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA"];

/// The typed record the JSON encoder serializes: a flat page-view event whose
/// documents land near [`PAYLOAD_LEN`], so its count and the passthrough
/// counts describe a comparable quantity of payload.
///
/// Flat and string-heavy on purpose. `serde_json`'s cost is dominated by
/// escaping and copying string bytes and by formatting numbers, and a nested
/// shape would add container bookkeeping that the sink's own encode path has
/// no part in.
#[derive(Serialize)]
pub(crate) struct Event {
    pub(crate) id: u64,
    pub(crate) session: String,
    pub(crate) user: String,
    pub(crate) kind: &'static str,
    pub(crate) ts_ms: i64,
    pub(crate) url: String,
    pub(crate) referrer: String,
    pub(crate) country: &'static str,
    pub(crate) city: String,
    pub(crate) duration_ms: u32,
    pub(crate) revenue: f64,
    pub(crate) ok: bool,
    pub(crate) tags: Vec<String>,
    pub(crate) campaign: String,
    pub(crate) agent: String,
}

/// The message key an [`Event`] carries: the user identifier, which is what a
/// keyed topic partitions on.
///
/// Unused by the cases as they stand — the key axis is measured once, on the
/// cheaper passthrough encoder, where the difference is not buried under
/// serialization — but it is the extractor the fixture test uses to prove the
/// typed corpus can be keyed at all.
pub(crate) fn event_key(event: &Event) -> Option<&[u8]> {
    Some(event.user.as_bytes())
}

/// `n` events, every field a pure function of the index.
pub(crate) fn events(n: usize) -> Vec<Event> {
    let mut lcg = Lcg::new(0x5EED_0012);
    (0..n)
        .map(|i| {
            let entropy = lcg.next();
            Event {
                id: 900_000_000 + i as u64,
                session: format!("sess-{entropy:016x}"),
                user: user_id(i),
                kind: KINDS[i % KINDS.len()],
                ts_ms: 1_772_000_000_000 + i as i64,
                url: format!("https://example.com/catalogue/{}/item-{i:08}", i % 64),
                referrer: format!("https://search.example.net/q?p={}", i % 997),
                country: COUNTRIES[i % COUNTRIES.len()],
                city: format!("city-{:04}", i % 512),
                duration_ms: (entropy % 30_000) as u32,
                revenue: (i % 10_000) as f64 / 100.0,
                ok: i % 5 != 0,
                tags: vec![
                    "prod".to_owned(),
                    format!("rack-{}", i % 8),
                    format!("cohort-{}", i % 32),
                ],
                campaign: format!("spring-sale-{:04}-{}", i % 400, KINDS[i % KINDS.len()]),
                agent: format!(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like \
                     Gecko) Chrome/141.0.0.0 Safari/537.36 build-{i:06}"
                ),
            }
        })
        .collect()
}

/// A passthrough encoder that also stamps the provenance headers a real
/// Kafka-to-Kafka hop carries.
///
/// Three of the four are constants and one varies with the record, which is
/// the mix production produces and the mix the accumulator's header-slot reuse
/// has to handle: a name is rewritten into the slot it already owns every
/// time, so steady-state header stamping must not allocate.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HeaderStamp;

impl MessageEncoder<Owned<Vec<u8>>> for HeaderStamp {
    fn encode<'buf>(
        &mut self,
        rec: &Record<Vec<u8>>,
        msg: &mut KafkaMessage,
    ) -> Result<(), SinkError> {
        msg.set_payload(&rec.payload);
        msg.add_header("trace-id", &rec.payload[..KEY_LEN]);
        msg.add_header("origin", b"spate");
        msg.add_header("content-type", b"application/octet-stream");
        msg.add_header("schema-version", b"3");
        Ok(())
    }
}
