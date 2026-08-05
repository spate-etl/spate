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
//! is two legs of one commit, not one A/B run:
//!
//! ```sh
//! bench run --out legs/serde --leg base
//! bench run --out legs/simd  --leg head --features simd
//! bench compare legs/serde legs/simd --allow features --allow digest
//! ```
//!
//! and the report says in its header which guards were waived. A run built
//! with `--features simd` also carries a second floor, so the library margin
//! can be read inside one leg without waiving anything at all.
//!
//! ## What is left out
//!
//! Fewer cases than `decode_gungraun.rs` runs, and the omissions are where
//! wall time cannot resolve what the counted tier can. `single_value` is
//! dropped because the four shape cases already decode into a value; the array
//! error path is dropped because its atomicity lives inside `serde` rather
//! than in this crate, and `ndjson_fail_bad_last` already prices the
//! decode-everything-emit-nothing shape. The typing, framing, error and shape
//! axes are each still exercised, and none is crossed with another: a case
//! that re-measures the product of two effects two others already fix costs a
//! person several minutes of an afternoon.
//!
//! Run it with `make bench-ab REF=main FILTER=decode_`.
//!
//! [`BACKEND_ID`]: spate_json::BACKEND_ID

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

use decode_rig::{Rig, batch_rig, decode_run, decode_run_err, rig, shape_rig};
use orders::{BAD_EVERY, Corruption, Order, RECORDS, Reading};

/// The component label every case's drop counters carry. The pipeline label is
/// the case id, which is what keeps one case's counters out of another's: one
/// bench binary runs every case in one process, and the recorder is global to
/// it.
const COMPONENT: &str = "json";

/// The iteration count every case over [`RECORDS`] records is pinned to.
///
/// Calibrating these to the harness's 50 ms target lands on a few dozen
/// iterations, because one drive decodes a quarter of a megabyte. The
/// degenerate-region guard times an empty loop at whatever count the case
/// settled on, and an empty loop of thirty iterations is a few tens of
/// nanoseconds — at or under the clock's own granularity, which the guard
/// reports as a case it cannot judge. Pinning keeps the reference loop well
/// clear of that, at the cost of a region closer to half a second than to
/// fifty milliseconds.
const BATCH_ITERS: u64 = 512;

/// Why the guard cases can never reach the significant-changes table.
const GUARD_ERRATIC: &str = "the duplicate-key guard collects keys into a HashSet whose hasher is \
     seeded per process, and every replicate is a fresh process, so the same document hashes into \
     a different number of probes and potentially a different rehash schedule";

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
        .iters(BATCH_ITERS)
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
        .iters(BATCH_ITERS)
        .done()
}

/// A case over one document of an arbitrary shape, decoded into a value,
/// optionally through the duplicate-key guard.
fn shape_case(
    suite: Suite,
    id: &'static str,
    payload: fn() -> Vec<u8>,
    guard: bool,
    expect: u64,
) -> Suite {
    let case = suite
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
                    let got = decode_run::<Owned<serde_json::Value>, _>(&mut rig);
                    assert_eq!(got, rig.expect, "the document emitted a different count");
                    got
                });
            },
        )
        .items(1)
        .bytes_of(payload_bytes)
        .iters(BATCH_ITERS);
    // A `CaseBuilder` cannot be branched on after `.done()`, so the guard
    // cases take the mark here.
    if guard {
        case.erratic(GUARD_ERRATIC).done()
    } else {
        case.done()
    }
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-json");

    // The per-record cost every payload pays, under the framing that has one
    // record to split. Unwarmed and without metrics, which is what its counted
    // twin `single_typed` is; every case below is warmed and instrumented,
    // which is what theirs are.
    let suite = suite
        .case(
            "decode_single_typed",
            |corpus, _seed| {
                let rig = rig(JsonFraming::Single, orders::order_document(), 1, |b| {
                    b.build_serde::<Order>()
                });
                absorb(corpus, &rig);
                RefCell::new(rig)
            },
            |b, rig: &RefCell<Rig<JsonSerdeDeserializer<Order>>>| {
                // `decode_once`, which the counted twin calls, asserts inside
                // and returns nothing — so it cannot be the routine here,
                // where the return value is what `black_box` holds. The work
                // either side of that is the same call.
                b.iter(|| {
                    let mut rig = rig.borrow_mut();
                    let got = decode_run::<Owned<Order>, _>(&mut rig);
                    assert_eq!(got, 1, "the single framing emitted a different count");
                    got
                });
            },
        )
        .items(1)
        .bytes_of(payload_bytes)
        .done();

    // The framing and typing axes at batch scale. `ndjson_typed` is the
    // headline; `ndjson_value` is the allocation-heavy arm, and the pair is
    // where `alloc_count_per_iter` earns its 1% floor.
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

    // The duplicate-key guard: what it costs on clean input, and what
    // rejecting costs. Both share a corpus with a guard-off case above, so the
    // difference is the guard alone.
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
/// The parsed value is returned rather than discarded, so it is dropped inside
/// the region — which is parity with the crate's path, where the record
/// reaches a sink and is dropped there.
fn serde_floor<T: 'static>(
    suite: Suite,
    id: &'static str,
    items: u64,
    payload: fn() -> Vec<u8>,
    routine: fn(&[u8]) -> T,
    iters: Option<u64>,
) -> Suite {
    let case = suite
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
        .bytes_of(|bytes: &Vec<u8>| bytes.len() as u64);
    match iters {
        Some(n) => case.iters(n).done(),
        None => case.done(),
    }
}

fn serde_one_order(bytes: &[u8]) -> Order {
    serde_json::from_slice(bytes).expect("the fixture is a valid order")
}

fn serde_array(bytes: &[u8]) -> Vec<Reading> {
    serde_json::from_slice(bytes).expect("the fixture is a valid array")
}

/// The parser over a newline-delimited batch, split the way the crate's
/// `Skip` path splits it.
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
/// carries both floors: `serde_json`, which is still the library the guard and
/// the value arm use, and the compiled backend, which is what the framework
/// cases are read against. The margin between the two is then readable inside
/// one leg, without waiving a guard to compare across legs.
#[cfg(feature = "simd")]
fn floors(suite: Suite) -> Suite {
    simd_floors(serde_floors(suite))
}

fn serde_floors(suite: Suite) -> Suite {
    let suite = serde_floor(
        suite,
        "decode_floor_serde_typed",
        1,
        orders::order_document,
        serde_one_order,
        None,
    );
    let suite = serde_floor(
        suite,
        "decode_floor_serde_ndjson",
        RECORDS,
        || orders::readings_ndjson(RECORDS),
        serde_ndjson,
        Some(BATCH_ITERS),
    );
    serde_floor(
        suite,
        "decode_floor_serde_array",
        RECORDS,
        || orders::readings_array(RECORDS),
        serde_array,
        Some(BATCH_ITERS),
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
    iters: Option<u64>,
) -> Suite {
    let case = suite
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
        .bytes_of(|state: &SimdFloor| state.payload.len() as u64);
    match iters {
        Some(n) => case.iters(n).done(),
        None => case.done(),
    }
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
fn simd_one_order(state: &SimdFloor) -> Order {
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
        "decode_floor_simd_typed",
        1,
        orders::order_document,
        simd_one_order,
        None,
    );
    simd_floor(
        suite,
        "decode_floor_simd_ndjson",
        RECORDS,
        || orders::readings_ndjson(RECORDS),
        simd_ndjson,
        Some(BATCH_ITERS),
    )
}

bench_main!(suite);
