//! Wall time for this crate's decode paths, and for the parser underneath
//! them.
//!
//! The `decode_*` cases measure **this crate**: the framing the deserializer
//! applies to a payload, the record emission, and the per-record error
//! isolation that comes with it. The `decode_floor_*` cases measure the
//! compiled parser directly over the same bytes. The first is what the
//! framework adds and the second is what the library costs, and reading the
//! first without the second turns a parser's regression into an unexplained
//! framework one.
//!
//! Both halves drive `decode_run` from `support/decode_rig.rs`, which
//! `decode_gungraun.rs` also drives — so a counted regression and a wall-clock
//! one are statements about one region rather than two that have drifted.
//!
//! ## The backend axis
//!
//! `simd` swaps the byte-slice → value seam to a different parser, which makes
//! this the one crate where a report could silently pair two implementations.
//! Two independent tripwires stop it. The harness guards `features`, which
//! records what was passed to cargo; every case here also absorbs
//! [`BACKEND_ID`] into its corpus, which records what actually *compiled*.
//! Only the second catches a change that makes `simd` a default feature — the
//! two legs would then agree on `features` while decoding through different
//! parsers.
//!
//! So comparing the backends is a deliberate act rather than an accident. It
//! is two legs of one commit, not one A/B run. A leg directory has to sit
//! outside the repository, which is where the driver keeps its own:
//!
//! ```sh
//! legs="${TMPDIR:-/tmp}/spate-json-backends"
//! bench run --out "$legs/serde" --leg base
//! bench run --out "$legs/simd"  --leg head --features simd
//! bench compare "$legs/serde" "$legs/simd" --allow features --allow digest
//! ```
//!
//! and the report says in its header which guards were waived. Both waivers
//! are needed: `--allow features` alone leaves every shared case demoted on
//! its corpus digest, which is the second tripwire doing its job.
//!
//! A leg built with `--features simd` also carries that backend's own floors,
//! so the library margin reads inside one leg without waiving anything —
//! `decode_floor_simd_ndjson` against `decode_floor_serde_ndjson` is two
//! libraries over one corpus in one binary.
//!
//! ## Which cases carry a floor
//!
//! One floor per parser entry point, over the same bytes as its framework
//! partner, in whichever backend the leg compiled:
//!
//! | Framework case | Floor |
//! |---|---|
//! | `decode_ndjson_typed` | `decode_floor_*_ndjson` |
//! | `decode_array_typed` | `decode_floor_*_array` |
//! | `decode_wide_flat` | `decode_floor_*_wide_flat` |
//!
//! The error-policy cases are read against `decode_ndjson_typed` rather than
//! against a floor, because what they price is this crate's isolation and not
//! the parser's. The other three shape cases vary the document rather than the
//! entry point, and `decode_wide_flat` is the pair that prices that entry
//! point; they have no floor of their own.
//!
//! ## What is left out
//!
//! Fourteen cases where `decode_gungraun.rs` declares nineteen. `single_typed`
//! and `single_value` are dropped because one 265-byte document decodes in
//! about 360 ns, and at that size a build's code layout moves the figure by
//! more than the 5% floor — an A/A comparison of one commit against itself
//! produced verdicts in both directions on it. The counted tier measures that
//! shape deterministically, which is where it belongs. `ndjson_batch50` and
//! `array_batch50` go with them: they are the same framings at a fortieth of
//! the size, and the batch-scale cases already walk that code. `dup_guard_deep`
//! is dropped because `dup_guard_wide` prices the guard on clean input and a
//! second shape does not change what it prices. `array_bad_last` is dropped
//! because the array framing's atomicity lives inside `serde` rather than in
//! this crate, and `ndjson_fail_bad_last` already prices the
//! decode-everything-emit-nothing shape.
//!
//! Nothing here pins an iteration count. The harness calibrates every case to
//! its `--target-ms`, and its degenerate-region guard resolves an empty loop
//! at those counts with several orders of magnitude to spare — 64 iterations
//! measure 291 ns against a clock that never failed to resolve one in two
//! hundred passes. A case wanting a longer region wants `--target-ms`, which
//! is the harness's knob for it and moves every case together.
//!
//! Run it with `make bench-ab REF=main FILTER=decode_`.
//!
//! [`BACKEND_ID`]: spate_json::BACKEND_ID

use serde_json::Value;
use spate_bench::{Corpus, Suite, bench_main};
use spate_core::deser::Owned;
use spate_json::{JsonFraming, JsonSerdeDeserializer, JsonValueDeserializer, OnError};
use std::cell::RefCell;

#[path = "support/decode_rig.rs"]
mod decode_rig;
#[path = "support/orders.rs"]
mod orders;
#[path = "support/shapes.rs"]
mod shapes;

use decode_rig::{Rig, batch_rig, decode_run, decode_run_err, shape_rig};
use orders::{BAD_EVERY, Corruption, RECORDS, Reading};

/// The component label a case's drop counters carry; the pipeline label is the
/// case id.
///
/// The harness runs one process per case per replicate, so nothing in a bench
/// run could collide. `tests/bench_fixtures.rs` is the one process that builds
/// several rigs at once, and distinct label sets are what keep one rig's
/// counters from being summed into another's there.
const COMPONENT: &str = "json";

fn absorb<D>(corpus: &mut Corpus, rig: &Rig<D>) {
    corpus.absorb("payload", &rig.payload);
    corpus.absorb("backend", spate_json::BACKEND_ID.as_bytes());
}

fn payload_bytes<D>(rig: &RefCell<Rig<D>>) -> u64 {
    rig.borrow().payload.len() as u64
}

/// A case over [`RECORDS`] records that decodes under its error policy.
///
/// The state is a `RefCell` because a routine receives its state by shared
/// reference and `decode_run` takes `&mut self`. Resetting the sink is inside
/// `decode_run` rather than here, so the counted tier cannot drift from this
/// one on the one thing that decides whether a second drive is the same work
/// as the first.
///
/// The emitted count is asserted and returned: asserted so a fixture that
/// stopped being broken cannot pass as a fast one, returned so `black_box` has
/// something to hold and the decode cannot be optimised away.
///
/// No resident-set figure reaches these records. `warm_rig` drives a full pass
/// before the region opens, so the process's high-water mark is already set by
/// the time the harness starts watching, and it reports the metric absent
/// rather than as a figure about the warm-up.
fn batch_case(
    suite: Suite,
    id: &'static str,
    framing: JsonFraming,
    on_error: OnError,
    expect: u64,
    payload: impl Fn() -> Vec<u8> + 'static,
) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig =
                    batch_rig::<Reading>(framing, on_error, payload(), expect, (id, COMPONENT));
                absorb(corpus, &rig);
                RefCell::new(rig)
            },
            |b, rig: &RefCell<Rig<JsonSerdeDeserializer<Reading>>>| {
                b.iter(|| {
                    let mut rig = rig.borrow_mut();
                    let got = decode_run::<Owned<Reading>, _>(&mut rig);
                    assert_eq!(got, rig.expect, "the framing emitted a different count");
                    got
                });
            },
        )
        // Records *attempted*, not emitted, and so identical across the clean,
        // partly-poisoned and wholly-poisoned members of this family. That is
        // what makes `records_per_s` read as input throughput and the family
        // directly comparable; counting emissions would leave the poison-storm
        // case declaring no throughput at all.
        .items(RECORDS)
        .bytes_of(payload_bytes)
        .done()
}

/// The same, for a payload that must fail the whole call.
///
/// Separate rather than folded in with a flag, for the reason `decode_run_err`
/// is separate from `decode_run`: an `Ok` here is a fixture that stopped being
/// broken, and it would otherwise report as a fast version of its clean twin.
fn batch_fail_case(
    suite: Suite,
    id: &'static str,
    framing: JsonFraming,
    payload: impl Fn() -> Vec<u8> + 'static,
) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig =
                    batch_rig::<Reading>(framing, OnError::Fail, payload(), 0, (id, COMPONENT));
                absorb(corpus, &rig);
                RefCell::new(rig)
            },
            |b, rig: &RefCell<Rig<JsonSerdeDeserializer<Reading>>>| {
                b.iter(|| {
                    let mut rig = rig.borrow_mut();
                    let got = decode_run_err::<Owned<Reading>, _>(&mut rig);
                    assert_eq!(got, 0, "an atomic framing emitted a record before failing");
                    got
                });
            },
        )
        .items(RECORDS)
        .bytes_of(payload_bytes)
        .done()
}

/// A case over one document of an arbitrary shape, decoded into a value,
/// optionally through the duplicate-key guard.
///
/// `items` is one document, where the batch family counts records — so
/// `records_per_s` here is documents per second and the two families are not
/// comparable in that column. Wall time and the allocation totals are.
fn shape_case(
    suite: Suite,
    id: &'static str,
    payload: fn() -> Vec<u8>,
    guard: bool,
    expect: u64,
) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = shape_rig(payload(), guard, expect, (id, COMPONENT));
                absorb(corpus, &rig);
                RefCell::new(rig)
            },
            |b, rig: &RefCell<Rig<JsonValueDeserializer>>| {
                b.iter(|| {
                    let mut rig = rig.borrow_mut();
                    let got = decode_run::<Owned<Value>, _>(&mut rig);
                    assert_eq!(got, rig.expect, "the document emitted a different count");
                    got
                });
            },
        )
        .items(1)
        .bytes_of(payload_bytes)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-json");

    // The framing and typing axes at batch scale. `decode_ndjson_typed` is the
    // headline, and the denominator the error-policy cases below are read
    // against.
    let suite = batch_case(
        suite,
        "decode_ndjson_typed",
        JsonFraming::Ndjson,
        OnError::Skip,
        RECORDS,
        || orders::readings_ndjson(RECORDS),
    );
    let suite = batch_case(
        suite,
        "decode_array_typed",
        JsonFraming::Array,
        OnError::Skip,
        RECORDS,
        || orders::readings_array(RECORDS),
    );

    // The `Fail` policy on input that never needed it. This framing decodes
    // every line into a holding buffer before emitting any of them, so that a
    // payload failing part-way emits no prefix; against `decode_ndjson_typed`
    // this is what that guarantee costs.
    let suite = batch_case(
        suite,
        "decode_ndjson_fail_clean",
        JsonFraming::Ndjson,
        OnError::Fail,
        RECORDS,
        || orders::readings_ndjson(RECORDS),
    );

    // The error axis under `Skip`: one record in `BAD_EVERY` broken from
    // opposite ends of the parser, then every record broken.
    let suite = batch_case(
        suite,
        "decode_ndjson_syntax_10pct",
        JsonFraming::Ndjson,
        OnError::Skip,
        orders::good_lines(RECORDS, BAD_EVERY),
        || orders::readings_ndjson_bad_every(RECORDS, BAD_EVERY, Corruption::Syntax),
    );
    let suite = batch_case(
        suite,
        "decode_ndjson_type_10pct",
        JsonFraming::Ndjson,
        OnError::Skip,
        orders::good_lines(RECORDS, BAD_EVERY),
        || orders::readings_ndjson_bad_every(RECORDS, BAD_EVERY, Corruption::TypeMismatch),
    );
    let suite = batch_case(
        suite,
        "decode_ndjson_syntax_all",
        JsonFraming::Ndjson,
        OnError::Skip,
        0,
        || orders::readings_ndjson_bad_every(RECORDS, 1, Corruption::Syntax),
    );

    // Everything decoded, nothing emitted, the holding buffer discarded.
    let suite = batch_fail_case(
        suite,
        "decode_ndjson_fail_bad_last",
        JsonFraming::Ndjson,
        || orders::readings_ndjson_bad_last(RECORDS, Corruption::TypeMismatch),
    );

    // The shape axis, all under the dynamically-typed target so that shape is
    // the only thing varying.
    let suite = shape_case(suite, "decode_wide_flat", shapes::wide_flat, false, 1);
    let suite = shape_case(suite, "decode_deep_nested", shapes::deep_nested, false, 1);
    let suite = shape_case(
        suite,
        "decode_numeric_array",
        shapes::numeric_array,
        false,
        1,
    );
    let suite = shape_case(suite, "decode_large_string", shapes::large_string, false, 1);

    // The duplicate-key guard, a documented second parse over the whole
    // document.
    //
    // `decode_dup_guard_wide` shares `wide_flat` with the guard-off case
    // above, so the difference between them is the guard on clean input.
    // `decode_dup_guard_hit` has no such partner and is not read as one: the
    // guard rejects before `decode_one` runs at all (`src/deser.rs`), so the
    // case is the guard walking to the duplicate and nothing else, and
    // subtracting a guard-off case from it yields a negative number rather
    // than a cost.
    //
    // Neither is marked erratic, and the counted tier's caveat about them does
    // not transfer. There the guard's `HashSet` is built once and the case is
    // one drive, so that construction's seed decides the count — a spread of
    // up to 1.74% between processes. `check_no_duplicate_keys` builds a fresh
    // set per object per document, and `RandomState::new()` reseeds on every
    // construction rather than once per process, so a region running hundreds
    // of iterations averages over hundreds of seeds. Measured A/A, these are
    // the quietest cases in the run at ±0.2%.
    let suite = shape_case(suite, "decode_dup_guard_wide", shapes::wide_flat, true, 1);
    let suite = shape_case(
        suite,
        "decode_dup_guard_hit",
        shapes::wide_flat_duplicate_key,
        true,
        0,
    );

    floors(suite)
}

// ---------------------------------------------------------------------------
// The parser's own floor
// ---------------------------------------------------------------------------

/// A floor case over borrowed bytes.
///
/// Neither warmed nor instrumented, unlike every framework case above: there
/// is no deserializer to warm and no counter for the library to increment.
/// What the pair still shares is the corpus, which is the comparison.
///
/// The parsed value is returned rather than discarded, so it is dropped inside
/// the region — which is parity with the crate's path, where the record
/// reaches a sink and is dropped there.
fn serde_floor<T: 'static>(
    suite: Suite,
    id: &'static str,
    items: u64,
    payload: fn() -> Vec<u8>,
    routine: fn(&[u8]) -> T,
) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let bytes = payload();
                corpus.absorb("payload", &bytes);
                corpus.absorb("backend", spate_json::BACKEND_ID.as_bytes());
                bytes
            },
            move |b, bytes: &Vec<u8>| {
                b.iter(|| routine(bytes));
            },
        )
        .items(items)
        .bytes_of(|bytes: &Vec<u8>| bytes.len() as u64)
        .done()
}

fn serde_array(bytes: &[u8]) -> Vec<Reading> {
    serde_json::from_slice(bytes).expect("the fixture is a valid array")
}

fn serde_value(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("the fixture is a valid document")
}

/// The parser over a newline-delimited batch, split the way the crate's
/// `Skip` path splits it — `is_blank` is all-ASCII-whitespace, not just empty.
///
/// Counting rather than collecting is the parity choice: the crate's sink
/// increments a counter and drops the record, so this does the same. The count
/// is also what keeps the loop alive.
fn serde_ndjson(bytes: &[u8]) -> u64 {
    let mut decoded = 0u64;
    for line in bytes.split(|&b| b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let _: Reading = serde_json::from_slice(line).expect("the fixture is a valid reading");
        decoded += 1;
    }
    decoded
}

#[cfg(not(feature = "simd"))]
fn floors(suite: Suite) -> Suite {
    serde_floors(suite)
}

/// Under `simd` the crate decodes through a different parser, so the leg
/// carries both floor sets: `serde_json`, which is still the library the
/// duplicate-key guard uses whatever the backend, and the compiled backend,
/// which is what every framework case is read against. Both are present for
/// each entry point, so no pairing crosses two parsers.
#[cfg(feature = "simd")]
fn floors(suite: Suite) -> Suite {
    simd_floors(serde_floors(suite))
}

fn serde_floors(suite: Suite) -> Suite {
    let suite = serde_floor(
        suite,
        "decode_floor_serde_ndjson",
        RECORDS,
        || orders::readings_ndjson(RECORDS),
        serde_ndjson,
    );
    let suite = serde_floor(
        suite,
        "decode_floor_serde_array",
        RECORDS,
        || orders::readings_array(RECORDS),
        serde_array,
    );
    serde_floor(
        suite,
        "decode_floor_serde_wide_flat",
        1,
        shapes::wide_flat,
        serde_value,
    )
}

/// One payload with the scratch the simd backend reuses.
///
/// Both halves mirror `src/backend.rs`: simd-json parses a *mutable* buffer in
/// place, so the payload is copied into a reused buffer, and the parser's own
/// tape and string indexes are reused across calls. A floor that allocated a
/// fresh copy per iteration would price a memcpy and an allocation the crate's
/// path does not pay, and would read as the framework being faster than the
/// library it calls.
///
/// The crate reaches the same pair through a `thread_local!`, where this holds
/// it in a struct field. That leaves the floor a lazy-initialisation check and
/// a thread-local read per document lighter than the crate's path — a sliver
/// of the measured margin that is the access, not the framework. Holding it in
/// a real thread-local here would mean either a `const`-initialised one, which
/// `simd_json::Buffers` is not, or reproducing the crate's lazy cell, at which
/// point the floor stops being the library and starts being a copy of the
/// backend module.
#[cfg(feature = "simd")]
struct SimdFloor {
    payload: Vec<u8>,
    scratch: RefCell<(Vec<u8>, simd_json::Buffers)>,
}

#[cfg(feature = "simd")]
fn simd_floor<T: 'static>(
    suite: Suite,
    id: &'static str,
    items: u64,
    payload: fn() -> Vec<u8>,
    routine: fn(&SimdFloor) -> T,
) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let bytes = payload();
                corpus.absorb("payload", &bytes);
                corpus.absorb("backend", spate_json::BACKEND_ID.as_bytes());
                let state = SimdFloor {
                    payload: bytes,
                    scratch: RefCell::new((Vec::new(), simd_json::Buffers::new(0))),
                };
                // Warm the scratch on the case's own payload, for the reason
                // `warm_rig` does: both buffers size to the input, so a first
                // measured pass would otherwise be charged for growing them.
                // `setup` runs on the thread that later runs the routine.
                let _ = routine(&state);
                state
            },
            move |b, state: &SimdFloor| {
                b.iter(|| routine(state));
            },
        )
        .items(items)
        .bytes_of(|state: &SimdFloor| state.payload.len() as u64)
        .done()
}

#[cfg(feature = "simd")]
fn simd_decode<T: serde::de::DeserializeOwned>(state: &SimdFloor, bytes: &[u8]) -> T {
    let (buf, buffers) = &mut *state.scratch.borrow_mut();
    buf.clear();
    buf.extend_from_slice(bytes);
    simd_json::serde::from_slice_with_buffers(buf.as_mut_slice(), buffers)
        .expect("the fixture is valid JSON")
}

#[cfg(feature = "simd")]
fn simd_array(state: &SimdFloor) -> Vec<Reading> {
    simd_decode(state, &state.payload)
}

#[cfg(feature = "simd")]
fn simd_value(state: &SimdFloor) -> Value {
    simd_decode(state, &state.payload)
}

#[cfg(feature = "simd")]
fn simd_ndjson(state: &SimdFloor) -> u64 {
    let mut decoded = 0u64;
    for line in state.payload.split(|&b| b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let _: Reading = simd_decode(state, line);
        decoded += 1;
    }
    decoded
}

#[cfg(feature = "simd")]
fn simd_floors(suite: Suite) -> Suite {
    let suite = simd_floor(
        suite,
        "decode_floor_simd_ndjson",
        RECORDS,
        || orders::readings_ndjson(RECORDS),
        simd_ndjson,
    );
    let suite = simd_floor(
        suite,
        "decode_floor_simd_array",
        RECORDS,
        || orders::readings_array(RECORDS),
        simd_array,
    );
    simd_floor(
        suite,
        "decode_floor_simd_wide_flat",
        1,
        shapes::wide_flat,
        simd_value,
    )
}

bench_main!(suite);
