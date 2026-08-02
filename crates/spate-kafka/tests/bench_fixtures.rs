//! The encode bench's corpora are reproducible, and encode the way the bench
//! claims.
//!
//! An instruction count only means something if both legs of a comparison ran
//! on byte-identical input, so "the corpus is a pure function of nothing" is a
//! property worth a test rather than an assumption. The bench itself cannot
//! carry it: it needs Linux, valgrind and a matching runner, and it only runs
//! when a pull request selects the bench stage. This runs everywhere
//! `cargo test` does.
//!
//! The guard-trip case needs more than a stable corpus. It rests on a claim
//! about *why* it fails — that the header bytes are what push a record past
//! the limit, not the payload — and a limit nudged either way would leave a
//! case that still ran and still produced a number while measuring the
//! opposite thing. That claim is checked here.

use bytes::BytesMut;
use spate_core::checkpoint::AckRef;
use spate_core::deser::Owned;
use spate_core::error::{ErrorClass, SinkError};
use spate_core::record::{PartitionId, Record, RecordMeta};
use spate_core::sink::RowEncoder;
use spate_kafka::sink::{KafkaBytesEncoder, KafkaEncoder, KafkaJsonEncoder, MessageEncoder};
use std::collections::HashSet;

#[path = "../benches/support/messages.rs"]
mod messages;

use messages::{Event, GUARD_LIMIT, HEADER_BYTES, HeaderStamp, KEY_LEN, PAYLOAD_LEN, RECORDS};

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

/// What one corpus through one encoder produced: how many records were
/// accepted, how many bytes were framed, and the first error if any. The same
/// loop the bench measures.
struct Run {
    accepted: usize,
    framed: usize,
    digest: u64,
    first_err: Option<SinkError>,
}

fn drive<T, M>(mut enc: KafkaEncoder<Owned<T>, M>, payloads: Vec<T>) -> Run
where
    T: Send + 'static,
    M: MessageEncoder<Owned<T>>,
{
    let mut buf = BytesMut::new();
    let mut run = Run {
        accepted: 0,
        framed: 0,
        digest: 0,
        first_err: None,
    };
    for payload in payloads {
        match enc.encode(&record(payload), &mut buf) {
            Ok(()) => run.accepted += 1,
            Err(e) => run.first_err = run.first_err.or(Some(e)),
        }
    }
    run.framed = buf.len();
    run.digest = digest(&buf);
    run
}

/// The framed length and a digest of the framed bytes, for a corpus that must
/// encode cleanly.
///
/// The length alone is not enough to pin a corpus. Every passthrough payload
/// is exactly `PAYLOAD_LEN` bytes, so re-seeding the generator changes every
/// byte the encoder copies and moves no length at all — the pin would pass
/// over a corpus that is not the one any recorded count was measured against.
/// The digest is what closes that: it folds the frame the encoder actually
/// produced, so a changed seed, a changed value formula and a reordered corpus
/// all fail alike.
fn framed<T, M>(enc: KafkaEncoder<Owned<T>, M>, payloads: Vec<T>) -> (usize, u64)
where
    T: Send + 'static,
    M: MessageEncoder<Owned<T>>,
{
    let want = payloads.len();
    let run = drive(enc, payloads);
    assert!(
        run.first_err.is_none(),
        "a record failed to encode: {:?}",
        run.first_err
    );
    assert_eq!(run.accepted, want);
    (run.framed, run.digest)
}

/// FNV-1a over the framed bytes.
///
/// Written out rather than taken from `DefaultHasher`, whose output is
/// explicitly not stable across releases — and a pin that could change under a
/// toolchain bump is not a pin.
fn digest(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

fn keyless() -> KafkaEncoder<Owned<Vec<u8>>, KafkaBytesEncoder> {
    KafkaEncoder::new(KafkaBytesEncoder::new())
}

fn keyed() -> KafkaEncoder<Owned<Vec<u8>>, KafkaBytesEncoder> {
    KafkaEncoder::new(KafkaBytesEncoder::with_key_fn(messages::key_prefix))
}

fn json() -> KafkaEncoder<Owned<Event>, KafkaJsonEncoder<Owned<Event>>> {
    KafkaEncoder::new(KafkaJsonEncoder::<Owned<Event>>::new())
}

#[test]
fn the_corpora_are_reproducible() {
    assert_eq!(messages::payloads(RECORDS), messages::payloads(RECORDS));
    // `Event` is not `PartialEq`, and what the encoder makes of it is the
    // stronger statement anyway, since that is what a leg measures.
    assert_eq!(
        framed(json(), messages::events(RECORDS)),
        framed(json(), messages::events(RECORDS))
    );
}

/// The framed bytes of each corpus, pinned by length and digest.
///
/// Two calls in one process only prove the generators are pure. The property
/// the bench needs is stronger — that the corpus is the same *across
/// revisions*, since a merge-base leg and a head leg run different builds. A
/// one-character edit to a value formula, a seed, or a record count would
/// otherwise re-baseline every comparison with nothing to say it happened.
/// These numbers are what makes that edit fail here instead. They are what the
/// pull request that added this bench published, and changing one is a
/// deliberate act: re-record it, and treat every count from before the change
/// as measuring a different corpus.
#[test]
fn the_corpora_are_pinned_across_revisions() {
    assert_eq!(
        framed(keyless(), messages::payloads(RECORDS)),
        (5_210_000, 0xebab_6615_da4d_44ce),
        "bytes_keyless"
    );
    assert_eq!(
        framed(keyed(), messages::payloads(RECORDS)),
        (5_410_000, 0x6a5f_a371_4fc3_80be),
        "bytes_keyed"
    );
    assert_eq!(
        framed(KafkaEncoder::new(HeaderStamp), messages::payloads(RECORDS)),
        (6_390_000, 0x85b5_6f1d_2074_b3ea),
        "stamped_headers"
    );
    assert_eq!(
        framed(json(), messages::events(RECORDS)),
        (5_033_806, 0x6b0e_b104_eac6_1f2d),
        "json_typed"
    );
}

/// The typed documents are the same size as the passthrough payloads, to
/// within a few percent.
///
/// That is what lets the JSON count be read against the passthrough ones as
/// "what serialisation costs over copying the same quantity of payload". A
/// corpus that drifted to half or twice the size would still run, still be
/// pinned, and quietly stop supporting that reading — so the comparability is
/// asserted rather than assumed.
#[test]
fn the_typed_documents_match_the_payload_length() {
    // Nine bytes of frame per keyless, headerless message: the flags byte, the
    // header count, and the payload's length prefix.
    const FRAME_OVERHEAD: usize = 9;
    let mean = framed(json(), messages::events(RECORDS)).0 / RECORDS - FRAME_OVERHEAD;
    let drift = mean.abs_diff(PAYLOAD_LEN) * 100 / PAYLOAD_LEN;
    assert!(
        drift <= 5,
        "the mean document is {mean} bytes against a {PAYLOAD_LEN}-byte payload \
         ({drift}% apart); the JSON count no longer describes the same quantity \
         of payload as the passthrough ones"
    );
}

/// The guard-trip case fails **because of the headers**.
///
/// The same corpus at the same limit through a plain passthrough encoder must
/// be accepted in full: that is what makes the payload innocent and the
/// headers the whole difference. Without this, a `GUARD_LIMIT` that had
/// drifted below the payload length would leave the bench measuring an
/// oversized *payload* rejection — a different path from the one the case is
/// named for, and one librdkafka's own key-plus-payload check already catches.
#[test]
fn the_guard_trips_on_the_header_bytes_not_the_payload() {
    let run = drive(
        KafkaEncoder::with_max_message_bytes(HeaderStamp, GUARD_LIMIT),
        messages::payloads(RECORDS),
    );
    assert_eq!(run.accepted, 0, "the size guard accepted a stamped record");
    assert_eq!(run.framed, 0, "a rejected record was still framed");
    match run.first_err.expect("a rejected record reports an error") {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::RecordLevel, "the guard is not fatal");
            assert!(reason.contains("max_message_bytes"), "actionable: {reason}");
        }
        other => panic!("unexpected error shape: {other:?}"),
    }

    assert!(
        framed(
            KafkaEncoder::with_max_message_bytes(KafkaBytesEncoder::new(), GUARD_LIMIT),
            messages::payloads(RECORDS)
        )
        .0 > 0,
        "the payload alone does not fit the limit the headers are supposed to break"
    );
}

/// The stamp adds exactly the header bytes `GUARD_LIMIT` is reasoned against.
///
/// `guarded_size` is private, so the sum is read through the guard itself: at
/// a limit of `PAYLOAD_LEN + HEADER_BYTES` every stamped record must pass (the
/// guard rejects on strictly greater), and one byte below it every stamped
/// record must fail. Those two together pin the guarded size exactly. A fifth
/// header, or a renamed one, moves the total — and the guard case's margin
/// with it.
#[test]
fn the_header_stamp_adds_the_declared_byte_count() {
    let exact = PAYLOAD_LEN + HEADER_BYTES;
    assert!(
        framed(
            KafkaEncoder::with_max_message_bytes(HeaderStamp, exact),
            messages::payloads(RECORDS)
        )
        .0 > 0,
        "a stamped message is larger than {exact} bytes"
    );
    let run = drive(
        KafkaEncoder::with_max_message_bytes(HeaderStamp, exact - 1),
        messages::payloads(RECORDS),
    );
    assert_eq!(
        run.accepted, 0,
        "a stamped message is smaller than {exact} bytes"
    );
}

/// Every payload is exactly the length the guard limit is derived from, and
/// opens with a whole key.
#[test]
fn the_payloads_have_the_declared_shape() {
    let payloads = messages::payloads(RECORDS);
    assert_eq!(payloads.len(), RECORDS);
    for payload in &payloads {
        assert_eq!(
            payload.len(),
            PAYLOAD_LEN,
            "a payload is not the declared length, so the guard case's trip \
             is no longer a property of the header bytes"
        );
    }

    let keys: Vec<&[u8]> = payloads
        .iter()
        .map(|p| messages::key_prefix(p).expect("every payload carries a key"))
        .collect();
    assert!(keys.iter().all(|k| k.len() == KEY_LEN));
    assert!(
        keys.iter().all(|k| k.is_ascii()),
        "a key is not the printable identifier the corpus documents"
    );
    assert_eq!(
        keys.iter().collect::<HashSet<_>>().len(),
        RECORDS,
        "the corpus repeats a key, so it does not partition the way a real one would"
    );
}

/// The typed corpus can be keyed, which is the extractor the corpus documents
/// but no case measures.
#[test]
fn the_typed_corpus_carries_a_key() {
    let events = messages::events(RECORDS);
    assert_eq!(events.len(), RECORDS);
    assert!(
        framed(
            KafkaEncoder::new(KafkaJsonEncoder::<Owned<Event>>::with_key_fn(
                messages::event_key
            )),
            events
        )
        .0 > 0
    );
}
