//! The counter benches' corpora are reproducible, and are the corpora the
//! cases claim they are.
//!
//! An instruction count only means something if both legs of a comparison ran
//! on byte-identical input, so "the corpus is a pure function of nothing" is a
//! property worth a test rather than an assumption. The benches themselves
//! cannot carry it: they need Linux, valgrind and a matching runner. This runs
//! everywhere `cargo test` does.
//!
//! Reproducibility is the smaller half. Most of these cases rest on a claim
//! about *what the corpus does*: that a poison corpus really fails, at the
//! rate and in the place the case is named for; that a stream frames the same
//! records however it is chunked; that the duplicate-key document really has a
//! duplicate and its twin really has not. Every one of those could drift into
//! its opposite while the bench went on running, reporting a plausible number
//! for the wrong path. That is what is checked here.

use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, Owned};
use spate_core::framing::RecordFramer;
use spate_core::record::{PartitionId, RawPayload};
use spate_json::{JsonDeserializerBuilder, JsonFraming, JsonSettings, NdjsonFramer, OnError};

#[path = "../benches/support/decode_rig.rs"]
mod decode_rig;
#[path = "../benches/support/frame_rig.rs"]
mod frame_rig;
#[path = "../benches/support/lines.rs"]
mod lines;
#[path = "../benches/support/orders.rs"]
mod orders;
#[path = "../benches/support/shapes.rs"]
mod shapes;

use decode_rig::Sink;
use lines::Eol;
use orders::{BAD_EVERY, Corruption, LineItem, RECORDS};

/// FNV-1a over a corpus.
///
/// Written out rather than taken from `DefaultHasher`, whose output is
/// explicitly not stable across releases. A pin that could change under a
/// toolchain bump is not a pin.
fn digest(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// A corpus's length and digest.
///
/// The length alone is not enough to pin any of these. A re-seeded filler, a
/// changed value formula or a reordered field list can leave every length
/// untouched while changing every byte the decoder reads, and the pin would
/// then pass over a corpus no recorded count was measured against. The digest
/// closes that.
fn pin(bytes: &[u8]) -> (usize, u64) {
    (bytes.len(), digest(bytes))
}

fn raw(bytes: &[u8]) -> RawPayload<'_> {
    RawPayload {
        bytes,
        key: None,
        partition: PartitionId(0),
        offset: 1,
        timestamp_ms: 0,
    }
}

/// What one payload through one deserializer produced: whether the call
/// failed, and how many records reached the sink. The same two quantities
/// every bench case asserts.
fn drive(settings: JsonSettings, payload: &[u8]) -> (bool, u64) {
    let mut deser = JsonDeserializerBuilder::from_settings(settings).build_serde::<LineItem>();
    let (ack, _rx) = AckRef::test_pair();
    let mut sink = Sink(0);
    let failed = deser.deserialize(&raw(payload), &ack, &mut sink).is_err();
    (failed, sink.0)
}

fn drive_value(settings: JsonSettings, payload: &[u8]) -> (bool, u64) {
    let mut deser = JsonDeserializerBuilder::from_settings(settings).build_value();
    let (ack, _rx) = AckRef::test_pair();
    let mut sink = Sink(0);
    let failed = deser.deserialize(&raw(payload), &ack, &mut sink).is_err();
    (failed, sink.0)
}

fn settings(framing: JsonFraming, on_error: OnError, reject_duplicate_keys: bool) -> JsonSettings {
    JsonSettings {
        framing,
        on_error,
        reject_duplicate_keys,
    }
}

/// Feed `stream` through a fresh framer in `chunk`-sized pieces, popping as a
/// source does, and return the record count and the framed byte total, the
/// pair `framing_gungraun.rs` asserts.
fn frame(stream: &[u8], chunk: usize) -> (usize, usize) {
    let mut framer = NdjsonFramer::new(frame_rig::MAX_RECORD_BYTES);
    let (mut records, mut bytes) = (0usize, 0usize);
    for piece in stream.chunks(chunk) {
        framer.push(piece).expect("inside the record cap");
        while let Some(record) = framer.pop() {
            records += 1;
            bytes += record.len();
        }
    }
    framer.finish().expect("the stream frames cleanly");
    while let Some(record) = framer.pop() {
        records += 1;
        bytes += record.len();
    }
    (records, bytes)
}

// ---------------------------------------------------------------------------
// Reproducibility and pins
// ---------------------------------------------------------------------------

#[test]
fn the_corpora_are_reproducible() {
    assert_eq!(orders::lines_ndjson(RECORDS), orders::lines_ndjson(RECORDS));
    assert_eq!(
        orders::lines_ndjson_bad_every(RECORDS, BAD_EVERY, Corruption::TypeMismatch),
        orders::lines_ndjson_bad_every(RECORDS, BAD_EVERY, Corruption::TypeMismatch)
    );
    assert_eq!(shapes::wide_flat(), shapes::wide_flat());
    assert_eq!(shapes::deep_nested(), shapes::deep_nested());
    assert_eq!(shapes::numeric_array(), shapes::numeric_array());
    assert_eq!(shapes::large_string(), shapes::large_string());
    assert_eq!(
        lines::stream(lines::RECORDS, lines::LINE_BYTES, Eol::Lf, 0),
        lines::stream(lines::RECORDS, lines::LINE_BYTES, Eol::Lf, 0)
    );
}

/// The record's field names and JSON kinds, pinned separately from the bytes.
///
/// A length-and-digest pin moves whenever any byte moves and cannot say what
/// moved; this names the part of the shape that did, and fails on a `qty` that
/// silently became a float while the corpus happened to keep its size. It also
/// catches an added, removed or renamed field, and a `#[serde(rename)]`.
///
/// What it does **not** catch: `i64` becoming `u64`, or `String` becoming
/// `Box<str>`. Those keep both the JSON kind and the encoded bytes, so neither
/// this nor the digests below would notice; the guard there is the
/// do-not-change contract on the fixture itself.
///
/// Keys come back sorted, because `serde_json::Value` is a map, so this pins
/// name/kind pairs and not their declaration order. Order is pinned by the
/// clean corpora's digests: it is what the encoder writes.
#[test]
fn the_line_item_shape_is_the_measured_workload() {
    let doc = serde_json::to_value(orders::sample_line(7)).expect("encode an order line");
    let obj = doc.as_object().expect("an order line is a JSON object");
    let shape: Vec<(&str, &str)> = obj
        .iter()
        .map(|(k, v)| {
            let kind = match v {
                serde_json::Value::String(_) => "string",
                serde_json::Value::Bool(_) => "bool",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Number(n) if n.is_f64() => "float",
                serde_json::Value::Number(_) => "int",
                // Reported rather than panicked, so an accidental `Option`
                // field arrives as a readable shape diff.
                serde_json::Value::Null => "null",
                serde_json::Value::Object(_) => "object",
            };
            (k.as_str(), kind)
        })
        .collect();
    assert_eq!(
        shape,
        [
            ("ok", "bool"),
            ("qty", "int"),
            ("ratio", "float"),
            ("sku", "string"),
            ("tags", "array"),
            ("ts_ms", "int"),
            ("unit", "string"),
        ]
    );
}

/// Every corpus, pinned by length and digest.
///
/// Two calls in one process only prove the generators are pure. The property
/// the benches need is stronger: that a corpus is the same *across
/// revisions*, since a merge-base leg and a head leg run different builds. A
/// one-character edit to a value formula, a seed, or a record count would
/// otherwise re-baseline every comparison with nothing to say it happened.
/// These numbers make that edit fail here instead. Changing one is a
/// deliberate act: re-record it, and treat every count from before the change
/// as measuring a different corpus.
#[test]
fn the_corpora_are_pinned_across_revisions() {
    assert_eq!(
        pin(&orders::order_document()),
        (265, 0xed39_44fc_72ba_fb50),
        "single_typed / single_value"
    );
    assert_eq!(
        pin(&orders::lines_ndjson(RECORDS)),
        (242_897, 0xa3d6_4abc_50c3_2286),
        "ndjson_clean / ndjson_fail_clean"
    );
    assert_eq!(
        pin(&orders::lines_array(RECORDS)),
        (242_898, 0x0c28_c24d_f9f9_05a4),
        "array_clean"
    );
    assert_eq!(
        pin(&orders::lines_ndjson_bad_every(
            RECORDS,
            BAD_EVERY,
            Corruption::Syntax
        )),
        (242_697, 0x2c32_faca_f2c0_1388),
        "ndjson_syntax_10pct"
    );
    assert_eq!(
        pin(&orders::lines_ndjson_bad_every(
            RECORDS,
            BAD_EVERY,
            Corruption::TypeMismatch
        )),
        (241_186, 0x699a_8e82_2d17_1b30),
        "ndjson_type_10pct"
    );
    assert_eq!(
        pin(&orders::lines_ndjson_bad_every(
            RECORDS,
            1,
            Corruption::Syntax
        )),
        (240_897, 0x4be5_a663_e642_cb62),
        "ndjson_syntax_all"
    );
    assert_eq!(
        pin(&orders::lines_ndjson_bad_last(
            RECORDS,
            Corruption::TypeMismatch
        )),
        (242_889, 0x8576_1234_48d1_cc3c),
        "ndjson_fail_bad_last"
    );
    assert_eq!(
        pin(&orders::lines_array_bad_last(RECORDS)),
        (242_890, 0xe02a_92e8_bb84_e836),
        "array_bad_last"
    );
    assert_eq!(
        pin(&shapes::wide_flat()),
        (74_287, 0x075b_13f6_2104_f0db),
        "wide_flat / dup_guard_wide"
    );
    assert_eq!(
        pin(&shapes::wide_flat_duplicate_key()),
        (74_287, 0x6f95_ab21_4ce9_0f31),
        "dup_guard_hit"
    );
    assert_eq!(
        pin(&shapes::deep_nested()),
        (143_958, 0x0941_11aa_484c_8fcb),
        "deep_nested / dup_guard_deep"
    );
    assert_eq!(
        pin(&shapes::numeric_array()),
        (388_106, 0xe2a9_d100_9c4f_0be4),
        "numeric_array"
    );
    assert_eq!(
        pin(&shapes::large_string()),
        (532_518, 0x27fc_30d7_f65d_8cd5),
        "large_string"
    );
    assert_eq!(
        pin(&lines::stream(
            lines::RECORDS,
            lines::LINE_BYTES,
            Eol::Lf,
            0
        )),
        (1_608_000, 0x0d34_2f26_6fec_1036),
        "lf_fetch_chunks / lf_line_chunks / lf_split_chunks"
    );
    assert_eq!(
        pin(&lines::stream(
            lines::RECORDS,
            lines::LINE_BYTES,
            Eol::Crlf,
            0
        )),
        (1_616_000, 0xb870_694c_3a68_f4be),
        "crlf_fetch_chunks"
    );
    assert_eq!(
        pin(&lines::stream(
            lines::RECORDS,
            lines::LINE_BYTES,
            Eol::Lf,
            1
        )),
        (1_616_000, 0x86ad_0a8e_5f37_61d2),
        "lf_blank_interleaved"
    );
    assert_eq!(
        pin(&lines::stream(
            lines::WIDE_RECORDS,
            lines::WIDE_LINE_BYTES,
            Eol::Lf,
            0
        )),
        (1_601_000, 0x1470_cca1_b2c4_3f60),
        "lf_wide_lines"
    );
}

// ---------------------------------------------------------------------------
// The poison corpora really poison
// ---------------------------------------------------------------------------

/// Each error-policy case emits exactly what its bench asserts.
///
/// A corruption that stopped corrupting, such as a truncation the parser
/// tolerated or a type the record could hold after all, would leave every one
/// of these cases measuring the happy path under an error-path name, running
/// clean and reporting a smaller number that reads like an improvement.
#[test]
fn the_error_cases_emit_what_they_claim() {
    let skip_ndjson = settings(JsonFraming::Ndjson, OnError::Skip, false);
    assert_eq!(
        drive(skip_ndjson.clone(), &orders::lines_ndjson(RECORDS)),
        (false, RECORDS),
        "ndjson_clean"
    );
    for how in [Corruption::Syntax, Corruption::TypeMismatch] {
        assert_eq!(
            drive(
                skip_ndjson.clone(),
                &orders::lines_ndjson_bad_every(RECORDS, BAD_EVERY, how)
            ),
            (false, orders::good_lines(RECORDS, BAD_EVERY)),
            "ndjson_*_10pct with {how:?}"
        );
    }
    assert_eq!(
        drive(
            skip_ndjson,
            &orders::lines_ndjson_bad_every(RECORDS, 1, Corruption::Syntax)
        ),
        (false, 0),
        "ndjson_syntax_all"
    );

    let fail_ndjson = settings(JsonFraming::Ndjson, OnError::Fail, false);
    assert_eq!(
        drive(fail_ndjson.clone(), &orders::lines_ndjson(RECORDS)),
        (false, RECORDS),
        "ndjson_fail_clean"
    );
    assert_eq!(
        drive(
            fail_ndjson,
            &orders::lines_ndjson_bad_last(RECORDS, Corruption::TypeMismatch)
        ),
        (true, 0),
        "ndjson_fail_bad_last: the payload fails and emits no prefix"
    );

    let skip_array = settings(JsonFraming::Array, OnError::Skip, false);
    assert_eq!(
        drive(skip_array.clone(), &orders::lines_array(RECORDS)),
        (false, RECORDS),
        "array_clean"
    );
    assert_eq!(
        drive(skip_array, &orders::lines_array_bad_last(RECORDS)),
        (false, 0),
        "array_bad_last: one bad element drops the whole payload"
    );
}

/// The two corruptions really are the two *kinds* the cases are named for.
///
/// If both failed the same way the error-kind axis would be one case measured
/// twice, and the pair would be paying for a comparison it could not make. The
/// distinction is structural rather than textual: the truncated record is not
/// JSON at all, so the parser stops without ever producing a value, while the
/// mismatched one is well-formed JSON that cannot become a
/// [`LineItem`], so the parser does all of its work and `serde` rejects the
/// result. Asserted that way rather than on the message, which the two decode
/// backends word differently and which the `simd` arm reaches from a different
/// position in the input.
#[test]
fn the_two_corruptions_fail_for_different_reasons() {
    let syntax = orders::bad_line(0, Corruption::Syntax);
    let mismatch = orders::bad_line(0, Corruption::TypeMismatch);

    assert!(
        serde_json::from_slice::<serde_json::Value>(&syntax).is_err(),
        "the truncated record is still well-formed JSON, so it is not a syntax \
         corruption at all"
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&mismatch).is_ok(),
        "the mismatched record no longer parses, so it is a second syntax case \
         rather than the type-mismatch one"
    );

    // And both still fail the decode, whichever backend is compiled in.
    let one = |bytes: &[u8]| {
        let mut deser = JsonDeserializerBuilder::from_settings(settings(
            JsonFraming::Single,
            OnError::Fail,
            false,
        ))
        .build_serde::<LineItem>();
        let (ack, _rx) = AckRef::test_pair();
        let mut sink = Sink(0);
        deser
            .deserialize(&raw(bytes), &ack, &mut sink)
            .expect_err("a poison record decodes cleanly")
            .to_string()
    };
    assert_ne!(
        one(&syntax),
        one(&mismatch),
        "the two corruptions produce the same failure, so the error-kind axis \
         is one case measured twice"
    );
}

// ---------------------------------------------------------------------------
// The duplicate-key corpora
// ---------------------------------------------------------------------------

/// The guard corpora differ in exactly the way the three guard cases need:
/// the clean pair passes the guard, the duplicated one does not, and the
/// duplicated one would have decoded fine without it.
#[test]
fn the_duplicate_key_corpus_is_the_only_one_the_guard_rejects() {
    let guarded = settings(JsonFraming::Single, OnError::Skip, true);
    let unguarded = settings(JsonFraming::Single, OnError::Skip, false);

    assert_eq!(
        drive_value(guarded.clone(), &shapes::wide_flat()),
        (false, 1),
        "dup_guard_wide: clean input still decodes with the guard on"
    );
    assert_eq!(
        drive_value(guarded.clone(), &shapes::deep_nested()),
        (false, 1),
        "dup_guard_deep: clean input still decodes with the guard on"
    );
    assert_eq!(
        drive_value(guarded, &shapes::wide_flat_duplicate_key()),
        (false, 0),
        "dup_guard_hit: the guard drops the duplicated document"
    );
    assert_eq!(
        drive_value(unguarded, &shapes::wide_flat_duplicate_key()),
        (false, 1),
        "the duplicated document decodes fine with the guard off, so the \
         guard case is measuring the guard and not a malformed payload"
    );
}

/// The duplicate is the **last** key, not an early one.
///
/// Position is what the case rests on: an early duplicate is found before the
/// guard has walked anything, and would leave `dup_guard_hit` reporting the
/// cost of rejecting a document rather than of checking one. Read as the
/// difference between the guard's two corpora: the duplicated document has
/// one fewer distinct key, and its first key is the repeated one.
#[test]
fn the_duplicate_is_the_last_key_in_the_object() {
    let clean: serde_json::Value = serde_json::from_slice(&shapes::wide_flat()).unwrap();
    let duplicated: serde_json::Value =
        serde_json::from_slice(&shapes::wide_flat_duplicate_key()).unwrap();
    let clean = clean.as_object().expect("an object");
    let duplicated = duplicated.as_object().expect("an object");

    assert_eq!(clean.len(), shapes::WIDE_FIELDS);
    assert_eq!(
        duplicated.len(),
        shapes::WIDE_FIELDS - 1,
        "the duplicated document does not repeat exactly one key"
    );
    let last = format!("f{:06}", shapes::WIDE_FIELDS - 1);
    assert!(
        clean.contains_key(&last),
        "the clean document has no last field to have been repeated"
    );
    assert!(
        !duplicated.contains_key(&last),
        "the repeated key is not the last one, so the guard finds it early"
    );
}

// ---------------------------------------------------------------------------
// The payload shapes really have their shapes
// ---------------------------------------------------------------------------

/// Each shape corpus is the shape its case is named for.
///
/// Every one of these could drift into an ordinary document while the bench
/// went on reporting a number: a depth that fell below the decoder's recursion
/// limit for a different reason, an array that acquired non-numeric elements,
/// a "large" string that stopped being large. The axis is the whole point of
/// the four cases, so it is asserted rather than assumed.
#[test]
fn the_shape_corpora_have_the_shapes_their_cases_name() {
    let deep: serde_json::Value = serde_json::from_slice(&shapes::deep_nested()).unwrap();
    let docs = deep.as_array().expect("an array of documents");
    assert_eq!(docs.len(), shapes::DEEP_DOCS);
    for doc in docs {
        let mut level = doc;
        let mut depth = 1;
        while let Some(next) = level.as_object().expect("an object").get("n") {
            level = next;
            depth += 1;
        }
        assert_eq!(
            depth,
            shapes::DEEP_DEPTH,
            "a document is not as deep as claimed"
        );
        assert_eq!(
            level.as_object().expect("an object").len(),
            shapes::DEEP_WIDTH,
            "the innermost level carries a recursive field it should not"
        );
    }

    let numbers: serde_json::Value = serde_json::from_slice(&shapes::numeric_array()).unwrap();
    let numbers = numbers.as_array().expect("an array");
    assert_eq!(numbers.len(), shapes::NUMBERS);
    assert!(
        numbers.iter().all(serde_json::Value::is_number),
        "the numeric array carries something that is not a number"
    );

    let large: serde_json::Value = serde_json::from_slice(&shapes::large_string()).unwrap();
    let text = large["text"].as_str().expect("a text field");
    assert_eq!(
        text.len(),
        shapes::TEXT_BYTES,
        "the decoded text is not the declared length, so the escapes are not \
         being counted the way the corpus builder counts them"
    );
    let others: usize = large
        .as_object()
        .expect("an object")
        .iter()
        .filter(|(key, _)| key.as_str() != "text")
        .map(|(key, value)| key.len() + value.to_string().len())
        .sum();
    assert!(
        text.len() > 1000 * others,
        "the text field ({} bytes) no longer dwarfs its neighbors ({others} \
         bytes), which is the whole premise of the case",
        text.len()
    );
}

// ---------------------------------------------------------------------------
// The framing streams
// ---------------------------------------------------------------------------

/// Every stream the framing bench feeds frames the record count and the byte
/// total that bench asserts, under every chunking rather than only the one
/// its case happens to use.
///
/// The bench asserts a pair per case, and this makes that pair the *right*
/// pair: the framer's contract is that the framing is a pure function of the
/// bytes, so a stream that framed differently at one chunk size than another
/// would mean the bench's chunk-size axis was varying the output rather than
/// only the work. The chunk sizes span every regime the cases run in (many
/// lines per chunk, exactly one, several chunks per line) plus the whole
/// stream in one push, which no case uses and which is the strongest form of
/// the same statement.
#[test]
fn every_stream_frames_its_declared_records_under_every_chunking() {
    for (records, width) in [
        (lines::RECORDS, lines::LINE_BYTES),
        (lines::WIDE_RECORDS, lines::WIDE_LINE_BYTES),
    ] {
        for eol in [Eol::Lf, Eol::Crlf] {
            for blank_every in [0, 1] {
                let stream = lines::stream(records, width, eol, blank_every);
                let want = (records, lines::expect_bytes(records, width));
                for chunk in [
                    lines::FETCH_CHUNK_BYTES,
                    width + 1,
                    lines::SPLIT_CHUNK_BYTES,
                    stream.len(),
                ] {
                    assert_eq!(
                        frame(&stream, chunk),
                        want,
                        "{records}x{width} eol={eol:?} blank_every={blank_every} chunk={chunk}"
                    );
                }
            }
        }
    }
}

/// Every framed line is a JSON document of exactly the declared width.
///
/// The framer is not JSON-aware, so nothing in it would notice if the corpus
/// stopped being JSON, but a framing bench fed something no JSON source would
/// carry is measuring a stream production does not have. The exact width is
/// the other half: it makes "the chunk is smaller than a line" a statement
/// about the framer rather than about the line generator.
#[test]
fn every_framed_line_is_a_json_document_of_the_declared_width() {
    for (records, width) in [
        (lines::RECORDS, lines::LINE_BYTES),
        (lines::WIDE_RECORDS, lines::WIDE_LINE_BYTES),
    ] {
        let stream = lines::stream(records, width, Eol::Lf, 0);
        let mut framer = NdjsonFramer::new(frame_rig::MAX_RECORD_BYTES);
        framer.push(&stream).unwrap();
        framer.finish().unwrap();
        let mut seen = 0;
        while let Some(record) = framer.pop() {
            assert_eq!(record.len(), width, "a line is not the declared width");
            serde_json::from_slice::<serde_json::Value>(&record)
                .expect("a framed line is not a JSON document");
            seen += 1;
        }
        assert_eq!(seen, records);
    }
}

/// The wide-line stream carries the same number of bytes as the standard one.
///
/// That equality is what makes `lf_wide_lines` a controlled comparison against
/// `lf_fetch_chunks`: the same quantity of bytes through the same chunking,
/// with only the number of record boundaries changed. Constants nudged apart
/// would leave the pair silently comparing two different-sized corpora and
/// attributing the difference to line width.
#[test]
fn the_wide_stream_is_the_same_quantity_of_bytes() {
    assert_eq!(
        lines::expect_bytes(lines::RECORDS, lines::LINE_BYTES),
        lines::expect_bytes(lines::WIDE_RECORDS, lines::WIDE_LINE_BYTES),
    );
}

// ---------------------------------------------------------------------------
// Repeatability
// ---------------------------------------------------------------------------

/// Every rig emits the same records on its third drive as on its first.
///
/// The counted tier never needed this: gungraun drives a rig once and throws
/// it away. The wall harness calls a routine hundreds of times against one
/// piece of state, so a rig that drifts reports a case whose name has stopped
/// describing what it measures, and reports it as a stable number, because
/// the drift settles long before the measured region opens.
///
/// What is asserted is the emitted count, which is what every case asserts and
/// what a drifting sink or a fixture that stopped being broken would move. It
/// is not the *whole* of "the same work": the rate limiter behind the skip
/// warning admits five events per window, so a poison rig's first drive emits
/// log events its later ones suppress. That difference is bounded, converges
/// inside the harness's warm-up, and is the steady state a running pipeline is
/// in; `warm_rig`'s own documentation accounts for it.
///
/// A distinct label pair per rig, because this is one process and
/// identically-labeled counters would be summed together.
#[test]
fn a_second_drive_emits_what_the_first_did() {
    // Both framings, since only ndjson decodes line by line.
    let mut ndjson = decode_rig::batch_rig::<LineItem>(
        JsonFraming::Ndjson,
        OnError::Skip,
        orders::lines_ndjson(RECORDS),
        RECORDS,
        ("fixtures-ndjson", "json"),
    );
    let mut array = decode_rig::batch_rig::<LineItem>(
        JsonFraming::Array,
        OnError::Skip,
        orders::lines_array(RECORDS),
        RECORDS,
        ("fixtures-array", "json"),
    );
    // Both poison rates: one record in ten, and every record. The second is
    // the case that drops two thousand times per drive, which is where a
    // limiter or a counter that accumulated would show first.
    let mut poisoned = decode_rig::batch_rig::<LineItem>(
        JsonFraming::Ndjson,
        OnError::Skip,
        orders::lines_ndjson_bad_every(RECORDS, BAD_EVERY, Corruption::Syntax),
        orders::good_lines(RECORDS, BAD_EVERY),
        ("fixtures-poisoned", "json"),
    );
    let mut storm = decode_rig::batch_rig::<LineItem>(
        JsonFraming::Ndjson,
        OnError::Skip,
        orders::lines_ndjson_bad_every(RECORDS, 1, Corruption::Syntax),
        0,
        ("fixtures-storm", "json"),
    );
    for drive in 1..=3 {
        assert_eq!(
            decode_rig::decode_run::<Owned<LineItem>, _>(&mut ndjson),
            RECORDS,
            "ndjson drive {drive}"
        );
        assert_eq!(
            decode_rig::decode_run::<Owned<LineItem>, _>(&mut array),
            RECORDS,
            "array drive {drive}"
        );
        assert_eq!(
            decode_rig::decode_run::<Owned<LineItem>, _>(&mut poisoned),
            orders::good_lines(RECORDS, BAD_EVERY),
            "poisoned drive {drive}"
        );
        assert_eq!(
            decode_rig::decode_run::<Owned<LineItem>, _>(&mut storm),
            0,
            "storm drive {drive}"
        );
    }

    // The failing arm too: a rig whose call must return `Err` has to keep
    // returning it, or the case silently starts measuring the happy path.
    let mut failing = decode_rig::batch_rig::<LineItem>(
        JsonFraming::Ndjson,
        OnError::Fail,
        orders::lines_ndjson_bad_last(RECORDS, Corruption::TypeMismatch),
        0,
        ("fixtures-failing", "json"),
    );
    for drive in 1..=3 {
        assert_eq!(
            decode_rig::decode_run_err::<Owned<LineItem>, _>(&mut failing),
            0,
            "failing drive {drive}"
        );
    }

    // Every shape the wall tier measures, guard off. These decode into a
    // value rather than a struct, and `large_string` is the one whose backend
    // scratch is resized by its own payload.
    for (label, payload) in [
        ("fixtures-wide", shapes::wide_flat as fn() -> Vec<u8>),
        ("fixtures-deep", shapes::deep_nested),
        ("fixtures-numeric", shapes::numeric_array),
        ("fixtures-large", shapes::large_string),
    ] {
        let mut rig = decode_rig::shape_rig(payload(), false, 1, (label, "json"));
        for drive in 1..=3 {
            assert_eq!(
                decode_rig::decode_run::<Owned<serde_json::Value>, _>(&mut rig),
                1,
                "{label} drive {drive}"
            );
        }
    }

    // And both guard cases: the one that passes the guard and decodes, and
    // the one the guard rejects before the decode runs at all.
    let mut guarded =
        decode_rig::shape_rig(shapes::wide_flat(), true, 1, ("fixtures-guard", "json"));
    let mut rejected = decode_rig::shape_rig(
        shapes::wide_flat_duplicate_key(),
        true,
        0,
        ("fixtures-reject", "json"),
    );
    for drive in 1..=3 {
        assert_eq!(
            decode_rig::decode_run::<Owned<serde_json::Value>, _>(&mut guarded),
            1,
            "guarded drive {drive}"
        );
        assert_eq!(
            decode_rig::decode_run::<Owned<serde_json::Value>, _>(&mut rejected),
            0,
            "rejected drive {drive}"
        );
    }

    // Framing carries no state between drives at all, because `frame_stream`
    // builds its own framer, so the claim is the stronger one that every drive
    // returns the identical pair.
    let stream = lines::stream(lines::RECORDS, lines::LINE_BYTES, Eol::Lf, 0);
    let rig = frame_rig::Rig {
        chunks: lines::chunks(&stream, lines::FETCH_CHUNK_BYTES),
        expect_records: lines::RECORDS,
        expect_bytes: lines::expect_bytes(lines::RECORDS, lines::LINE_BYTES),
    };
    let first = frame_rig::frame_stream(&rig);
    assert_eq!(
        first,
        (rig.expect_records, rig.expect_bytes),
        "frame drive 1"
    );
    for drive in 2..=3 {
        assert_eq!(frame_rig::frame_stream(&rig), first, "frame drive {drive}");
    }
}

/// The sink reset inside `decode_run` must not become a no-op.
///
/// The mirror of the test above: driving the deserializer directly, without
/// going through `decode_run`, must accumulate. If this ever reports `RECORDS`
/// rather than twice it, the sink has started resetting itself somewhere else
/// and the reset in `decode_run` has stopped being the thing that makes the
/// wall tier's thousandth iteration the same as its first.
#[test]
fn the_decode_rig_would_accumulate_without_its_reset() {
    let mut rig = decode_rig::batch_rig::<LineItem>(
        JsonFraming::Ndjson,
        OnError::Skip,
        orders::lines_ndjson(RECORDS),
        RECORDS,
        ("fixtures-noreset", "json"),
    );
    for _ in 0..2 {
        let payload = decode_rig::raw_payload(&rig.payload);
        rig.deser
            .deserialize(&payload, &rig.ack, &mut rig.sink)
            .expect("the corpus is clean");
    }
    assert_eq!(
        rig.sink.0,
        RECORDS * 2,
        "two drives without a reset should have accumulated"
    );
}
