//! The encode bench's corpora are reproducible, and encode the way the bench
//! claims.
//!
//! An instruction count only means something if both legs of a comparison ran
//! on byte-identical input, so "the corpus is a pure function of nothing" is a
//! property worth a test rather than an assumption. The bench itself cannot
//! carry it: it needs Linux, valgrind and a matching runner. This runs
//! everywhere `cargo test` does.
//!
//! The guard-trip case needs more than a stable corpus. It rests on a claim
//! about *why* it fails: the header bytes are what push a record past the
//! limit, not the payload, and a limit nudged either way would leave a case
//! that still ran and still produced a number while measuring the opposite
//! thing. That claim is checked here.

use bytes::BytesMut;
use spate_core::deser::Owned;
use spate_core::error::{ErrorClass, SinkError};
use spate_core::hash::FastSet;
use spate_core::sink::RowEncoder;
use spate_kafka::sink::{KafkaBytesEncoder, KafkaEncoder, KafkaJsonEncoder, MessageEncoder};
use spate_test::record;
use spate_test_support::fnv1a;

#[path = "../benches/support/messages.rs"]
mod messages;
#[allow(dead_code, reason = "only the rigs that frame a batch are used here")]
#[path = "../benches/support/encode_rig.rs"]
mod encode_rig;
#[allow(dead_code, reason = "the batches are written, not prepared")]
#[path = "../benches/support/prepare_rig.rs"]
mod prepare_rig;
#[path = "../benches/support/poll_rig.rs"]
mod poll_rig;
#[path = "../benches/support/produce_rig.rs"]
mod produce_rig;
mod support;

use messages::{
    Article, Event, GUARD_LIMIT, HEADER_BYTES, HeaderStamp, KEY_LEN, PAYLOAD_LEN, RECORDS,
    TEXT_BODY_ESCAPES, TEXT_BODY_LEN,
};

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
    run.digest = fnv1a(&buf);
    run
}

/// The framed length and a digest of the framed bytes, for a corpus that must
/// encode cleanly.
///
/// The length alone is not enough to pin a corpus. Every passthrough payload
/// is exactly `PAYLOAD_LEN` bytes, so re-seeding the generator changes every
/// byte the encoder copies and moves no length at all, so the pin would pass
/// over a corpus that is not the one any recorded count was measured against.
/// The digest closes that: it folds the frame the encoder produced, so a
/// changed seed, a changed value formula and a reordered corpus all fail
/// alike.
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

fn keyless() -> KafkaEncoder<Owned<Vec<u8>>, KafkaBytesEncoder> {
    KafkaEncoder::new(KafkaBytesEncoder::new())
}

fn keyed() -> KafkaEncoder<Owned<Vec<u8>>, KafkaBytesEncoder> {
    KafkaEncoder::new(KafkaBytesEncoder::with_key_fn(messages::key_prefix))
}

fn json() -> KafkaEncoder<Owned<Event>, KafkaJsonEncoder<Owned<Event>>> {
    KafkaEncoder::new(KafkaJsonEncoder::<Owned<Event>>::new())
}

fn json_simd() -> KafkaEncoder<Owned<Event>, KafkaJsonEncoder<Owned<Event>>> {
    KafkaEncoder::new(KafkaJsonEncoder::<Owned<Event>>::new().with_simd_json())
}

fn text() -> KafkaEncoder<Owned<Article>, KafkaJsonEncoder<Owned<Article>>> {
    KafkaEncoder::new(KafkaJsonEncoder::<Owned<Article>>::new())
}

fn text_simd() -> KafkaEncoder<Owned<Article>, KafkaJsonEncoder<Owned<Article>>> {
    KafkaEncoder::new(KafkaJsonEncoder::<Owned<Article>>::new().with_simd_json())
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
/// the bench needs is stronger: the corpus must be the same *across
/// revisions*, since a merge-base leg and a head leg run different builds. A
/// one-character edit to a value formula, a seed, or a record count would
/// otherwise re-baseline every comparison with nothing to say it happened.
/// These numbers make that edit fail here instead. They are what the pull
/// request that added this bench published; changing one means re-recording
/// it and treating every count from before the change as measuring a
/// different corpus.
#[test]
fn the_corpora_are_pinned_across_revisions() {
    assert_eq!(
        framed(keyless(), messages::payloads(RECORDS)),
        (5_210_000, 0x433a_8a15_da4d_44ce),
        "bytes_keyless"
    );
    assert_eq!(
        framed(keyed(), messages::payloads(RECORDS)),
        (5_410_000, 0x90db_7971_4fc3_80be),
        "bytes_keyed"
    );
    assert_eq!(
        framed(KafkaEncoder::new(HeaderStamp), messages::payloads(RECORDS)),
        (6_390_000, 0x121c_5f1d_2074_b3ea),
        "stamped_headers"
    );
    assert_eq!(
        framed(json(), messages::events(RECORDS)),
        (5_033_806, 0x83f1_ed04_eac6_1f2d),
        "json_typed"
    );
    assert_eq!(
        framed(json_simd(), messages::events(RECORDS)),
        (5_033_806, 0x83f1_ed04_eac6_1f2d),
        "json_typed_simd"
    );
    assert_eq!(
        framed(text(), messages::articles(RECORDS)),
        (6_240_000, 0x06c3_6460_9394_d41c),
        "json_text"
    );
    assert_eq!(
        framed(text_simd(), messages::articles(RECORDS)),
        (6_240_000, 0x06c3_6460_9394_d41c),
        "json_text_simd"
    );
}

/// The typed documents are the same size as the passthrough payloads, to
/// within a few percent.
///
/// That is what lets the JSON count be read against the passthrough ones as
/// "what serialization costs over copying the same quantity of payload". A
/// corpus that drifted to half or twice the size would still run, still be
/// pinned, and stop supporting that reading, so the comparability is
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
/// oversized *payload* rejection, a different path from the one the case is
/// named for, and one librdkafka's own key-plus-payload check already catches.
#[test]
fn the_guard_trips_on_the_header_bytes_not_the_payload() {
    let run = drive(
        KafkaEncoder::with_max_message_bytes(HeaderStamp, GUARD_LIMIT),
        messages::payloads(RECORDS),
    );
    assert_eq!(run.accepted, 0, "the size guard accepted a stamped record");
    assert_eq!(run.framed, 0, "a rejected record was still framed");
    let err = run.first_err.expect("a rejected record reports an error");
    assert_eq!(
        err.class(),
        ErrorClass::RecordLevel,
        "the guard is not fatal"
    );
    assert!(
        err.reason().contains("max_message_bytes"),
        "actionable: {err}"
    );

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
/// header, or a renamed one, moves the total, and the guard case's margin
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
        keys.iter().collect::<FastSet<_>>().len(),
        RECORDS,
        "the corpus repeats a key, so it does not partition the way a real one would"
    );
}

/// Every article body is the declared length and carries the declared number
/// of characters JSON escapes, so the text cases measure a scan that stops on
/// every line.
#[test]
fn the_article_bodies_have_the_declared_shape() {
    let articles = messages::articles(RECORDS);
    assert_eq!(articles.len(), RECORDS);
    for article in &articles {
        assert_eq!(article.body.len(), TEXT_BODY_LEN);
        assert!(article.body.is_ascii(), "a body is not ASCII");
        let escapes = article
            .body
            .bytes()
            .filter(|&b| b == b'"' || b == b'\\' || b < 0x20)
            .count();
        assert_eq!(escapes, TEXT_BODY_ESCAPES, "{:?}", article.body);
    }
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

/// The produce bench's setup and write work for every case, and each case
/// writes the message shape its name says.
///
/// `produce_wall.rs` builds its sink against the mock cluster in setup, so a
/// setting the builder refused or a batch the cluster rejected would fail
/// minutes into a bench run. The read-back checks each case against its own
/// batch: a key only when keyed, four headers only when stamped.
#[test]
fn the_produce_bench_writes_each_case_shape() {
    for case in &produce_rig::CASES {
        let id = case.id;
        let rig = produce_rig::Rig::new((case.batch)());
        assert_eq!(rig.write(), RECORDS as u64, "{id}");
        assert_eq!(rig.write(), RECORDS as u64, "{id}");
        let brokers = rig.cluster.bootstrap_servers();
        assert_eq!(
            support::log_end(&brokers, produce_rig::TOPIC, 0),
            3 * RECORDS as i64,
            "{id}: the setup write and two more"
        );
        let read = support::consume(&brokers, produce_rig::TOPIC, id, 1, |message| {
            Some(support::Read::of(message))
        });
        assert_eq!(read[0].key.is_some(), case.keyed, "{id}: key");
        assert_eq!(read[0].headers.len(), case.headers, "{id}: headers");
    }
}

/// Each drain of the poll bench reads exactly the records one feed produced,
/// and a region leaves nothing behind for the next.
///
/// The lane here polls up to 500 records, which does not divide a drain's
/// count, so a drain that asked for more than it had left would read into the
/// next drain's records.
#[test]
fn each_poll_bench_drain_reads_exactly_one_feed() {
    let (mut rig, setup) = poll_rig::Rig::new(500);
    let first: Vec<i64> = setup.iter().map(|(_, _, offset)| *offset).collect();
    assert_eq!(first, (0..poll_rig::RECORDS as i64).collect::<Vec<_>>());

    let mut offsets = Vec::new();
    for _region in 0..2 {
        rig.settle();
        for _ in 0..poll_rig::DRAINS {
            rig.feed();
        }
        for _ in 0..poll_rig::DRAINS {
            let mut drained = Vec::new();
            let read = rig.drain(|payload| drained.push(payload.offset));
            assert_eq!(read, poll_rig::RECORDS);
            assert_eq!(drained.len(), poll_rig::RECORDS);
            offsets.extend(drained);
        }
    }
    let start = poll_rig::RECORDS as i64;
    let end = start + (2 * poll_rig::DRAINS * poll_rig::RECORDS) as i64;
    assert_eq!(offsets, (start..end).collect::<Vec<_>>());
}
