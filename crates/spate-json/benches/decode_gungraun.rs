//! Instruction counts for this crate's decode paths (gungraun).
//!
//! The sibling `benches/decode_wall.rs` carries the floor any JSON decoder is
//! read against — cases calling the compiled parser directly. These measure
//! **this crate**: the framing the deserializer applies to a payload, the
//! record emission, and the per-record error isolation that comes with it. A
//! regression in `JsonSerdeDeserializer` would not show up in the floor.
//!
//! The rig itself is `support/decode_rig.rs`, which both tiers compile, so the
//! region counted here is the region the wall tier times.
//!
//! One shape — one payload through one deserializer — parameterised by the
//! framing the payload carries, the target it decodes into, the shape of the
//! document, and what the deserializer is asked to do when a record does not
//! decode.
//!
//! ## The reference cases
//!
//! Four small cases, kept at the size they were first recorded at because
//! resizing them would re-baseline every count taken against them:
//!
//! - `single_typed` / `single_value` — one flat 15-field document, decoded
//!   into a struct and into a dynamically-typed value. The per-record cost
//!   every payload pays, and the two ends of the typing axis.
//! - `ndjson_batch50` — fifty records newline-delimited. This framing isolates
//!   errors per line, so it does per-record bookkeeping the others do not; the
//!   gap against `array_batch50` is what that isolation costs.
//! - `array_batch50` — the same fifty records as one top-level array, decoded
//!   in a single pass and handled atomically on error.
//!
//! ## The error policy
//!
//! The reference cases all decode valid input under the default `Skip`, so
//! nothing in them reaches the error path at all. These do, over
//! [`RECORDS`] records — the same corpus builder at forty times the size, so
//! `ndjson_clean` is a scale-up of `ndjson_batch50` rather than a different
//! workload:
//!
//! - `ndjson_clean` — the denominator: valid input under `Skip`.
//! - `ndjson_syntax_10pct` / `ndjson_type_10pct` — one record in
//!   [`BAD_EVERY`] broken, by truncation and by a type the record cannot hold.
//!   The two reach the failure from opposite ends of the parser, and the pair
//!   is what says whether that matters.
//! - `ndjson_syntax_all` — every record broken: the poison storm, where the
//!   drop counter, the rate limiter's clock read and the discarded parse are
//!   the whole cost and nothing is emitted.
//! - `ndjson_fail_clean` — valid input under `Fail`. This framing decodes
//!   every line into a holding buffer before emitting any of them, so that a
//!   payload that fails part-way emits no prefix; against `ndjson_clean` this
//!   is what that guarantee costs on input that never needed it.
//! - `ndjson_fail_bad_last` — the same corpus with its last record broken:
//!   everything decoded, nothing emitted, the buffer discarded.
//! - `array_clean` / `array_bad_last` — the same pair under the array framing,
//!   broken the same way at the same position. Array error handling is atomic
//!   inside `serde` rather than across a holding buffer, and the pair against
//!   the ndjson one is the only place that difference is priced.
//!
//! ## Document shape
//!
//! Every case above is a flat record of seven to fifteen fields. These are the
//! rest of the shape axis, all decoded into a dynamically-typed value so that
//! shape is the only thing varying, and all anchored by `single_value` at the
//! small-and-flat end:
//!
//! - `wide_flat` — a flat object far wider than any struct.
//! - `deep_nested` — documents nested to the decoder's practical depth.
//! - `numeric_array` — almost nothing but number conversion.
//! - `large_string` — one field three orders of magnitude larger than its
//!   neighbours. This is the case the `simd` backend's mandatory copy of the
//!   payload into its scratch buffer is large enough to show in; the crate's
//!   claim that the copy is negligible rests on flat records, which is not
//!   where a copy would ever have been visible.
//!
//! ## The duplicate-key guard
//!
//! `reject_duplicate_keys` parses each document a second time through a
//! structural visitor that recurses over objects and arrays. It is off in
//! every case above, which keeps them measuring the decode — and leaves the
//! guard, a documented second parse, unmeasured. These three turn it on over
//! corpora two shape cases already fix, so each guard count has a guard-off
//! twin and the difference is the guard alone:
//!
//! - `dup_guard_wide` / `dup_guard_deep` — clean input over `wide_flat` and
//!   `deep_nested`: the guard pays in full and the decode still succeeds.
//! - `dup_guard_hit` — `wide_flat` with its *last* key repeating its first, so
//!   the guard walks the whole object before rejecting. Nothing is emitted;
//!   the decode would have succeeded, since the parser is last-value-wins.
//!
//! These three are the only cases in this file observed **not** to be
//! bit-identical from run to run, and the reason is worth knowing before a
//! delta on one of them is read as a regression. The guard's visitor collects
//! the keys it has seen into a `HashSet`, whose default hasher is seeded per
//! process from the operating system, so the same document hashes into a
//! different number of probes — and, at four thousand keys, potentially a
//! different rehash schedule — on every run.
//!
//! The spread is usually tiny and occasionally is not. Over three runs of one
//! revision on two architectures — twelve guard-to-guard deltas — eleven were
//! under a tenth of a per cent and the twelfth was **1.74%**: `dup_guard_wide`
//! read 17,164,046 and then 17,463,060 with nothing between them but a new
//! process. Every other case in this file repeated to the instruction across
//! those same runs, which is what attributes the movement to the seed rather
//! than to the runner.
//!
//! So a guard row is not evidence on its own. A change that moved the guard
//! would move `dup_guard_wide` and `dup_guard_deep` together and in proportion,
//! and would leave a mark on the `wide_flat` and `deep_nested` twins these
//! share their corpora with; one guard row moving by a per cent while its twin
//! and its guard-off pair sit still is the hasher.
//!
//! One other thing could in principle move a count without the code moving,
//! and it is worth naming rather than leaving to be rediscovered. The rate
//! limiter behind the skip warning allows five events per ten *seconds*, so a
//! poison case whose warm pass and measured pass together straddled a window
//! roll would emit up to five extra log events on one run and not the next.
//! Nothing observed comes near it: the whole nineteen-case binary takes 36
//! seconds on the runner and 15 on a development machine, which bounds any one
//! case's process — valgrind startup, corpus construction, warm pass and
//! measured region together — at a small multiple of a second. A poison row
//! that moved by a few hundred instructions between runs would be this, and
//! would mean the runner had become several times slower rather than that the
//! decode had changed.
//!
//! ## What is left out
//!
//! The matrix is sparse in three directions. The typing axis is exercised only
//! on a single document, because it moves with the payload rather than with
//! the framing. The error axis is exercised only on `ndjson` and `array`,
//! because `single` framing has one record to fail and its error path is the
//! per-record one these already charge, run once. And the shape axis is
//! exercised only under the dynamically-typed target, because a struct can
//! only be given one of these shapes at a time. Callgrind runs the workload
//! under emulation, and a case that re-measures a combination two others
//! already fix costs real time on every pull request.
//!
//! Needs valgrind and a same-version `gungraun-runner`, neither of which
//! exists on every developer machine: run it with `make bench-gungraun`.

// `library_benchmark` and `library_benchmark_group` expand to public modules,
// functions and constants of their own, none of which carry documentation, so
// the workspace's `missing_docs` lint has nothing to bite on here.
#![expect(missing_docs, reason = "items are generated by gungraun macros")]

use gungraun::{Dhat, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main};
use serde_json::Value;
use spate_core::deser::Owned;
use spate_json::{JsonFraming, OnError};

#[path = "support/decode_rig.rs"]
mod decode_rig;
#[path = "support/orders.rs"]
mod orders;
#[path = "support/shapes.rs"]
mod shapes;

use decode_rig::{Rig, batch_rig, decode_once, decode_run, decode_run_err, rig, shape_rig};
use orders::{BAD_EVERY, Corruption, Order, RECORDS, Reading};

/// The number of records the batch framings carry. Fifty is the same batch
/// size the Avro decode bench uses, so the two are comparable per element.
const BATCH: u64 = 50;

/// Labels for the connector-owned drop counters. Each case runs in its own
/// process, so one pair serves them all.
const LABELS: (&str, &str) = ("bench", "json");

/// [`batch_rig`] and [`shape_rig`] under this binary's labels. The shared rig
/// takes them per call because the wall tier runs every case in one process
/// and needs a distinct pair each; here there is nothing to separate.
fn batch(
    framing: JsonFraming,
    on_error: OnError,
    payload: Vec<u8>,
    expect: u64,
) -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    batch_rig(framing, on_error, payload, expect, LABELS)
}

fn shape(payload: Vec<u8>, guard: bool, expect: u64) -> Rig<spate_json::JsonValueDeserializer> {
    shape_rig(payload, guard, expect, LABELS)
}

fn single_typed_rig() -> Rig<spate_json::JsonSerdeDeserializer<Order>> {
    rig(JsonFraming::Single, orders::order_document(), 1, |b| {
        b.build_serde::<Order>()
    })
}

fn single_value_rig() -> Rig<spate_json::JsonValueDeserializer> {
    rig(JsonFraming::Single, orders::order_document(), 1, |b| {
        b.build_value()
    })
}

fn ndjson_batch_rig() -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    rig(
        JsonFraming::Ndjson,
        orders::readings_ndjson(BATCH),
        BATCH,
        |b| b.build_serde::<Reading>(),
    )
}

fn array_batch_rig() -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    rig(
        JsonFraming::Array,
        orders::readings_array(BATCH),
        BATCH,
        |b| b.build_serde::<Reading>(),
    )
}

fn ndjson_clean_rig() -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    batch(
        JsonFraming::Ndjson,
        OnError::Skip,
        orders::readings_ndjson(RECORDS),
        RECORDS,
    )
}

fn ndjson_bad_rig(
    bad_every: u64,
    how: Corruption,
) -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    batch(
        JsonFraming::Ndjson,
        OnError::Skip,
        orders::readings_ndjson_bad_every(RECORDS, bad_every, how),
        orders::good_lines(RECORDS, bad_every),
    )
}

fn ndjson_fail_clean_rig() -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    batch(
        JsonFraming::Ndjson,
        OnError::Fail,
        orders::readings_ndjson(RECORDS),
        RECORDS,
    )
}

fn ndjson_fail_bad_last_rig() -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    batch(
        JsonFraming::Ndjson,
        OnError::Fail,
        orders::readings_ndjson_bad_last(RECORDS, Corruption::TypeMismatch),
        0,
    )
}

fn array_clean_rig() -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    batch(
        JsonFraming::Array,
        OnError::Skip,
        orders::readings_array(RECORDS),
        RECORDS,
    )
}

fn array_bad_last_rig() -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    batch(
        JsonFraming::Array,
        OnError::Skip,
        orders::readings_array_bad_last(RECORDS),
        0,
    )
}

// Each case returns its rig rather than dropping it: a value moved into the
// benchmark function is dropped inside the collected region, which would
// charge the count for tearing the deserializer down. A `///` comment is a
// `#[doc]` attribute, which `#[library_benchmark]` rejects.
#[library_benchmark]
#[bench::single_typed(single_typed_rig())]
fn decode_typed(
    mut rig: Rig<spate_json::JsonSerdeDeserializer<Order>>,
) -> Rig<spate_json::JsonSerdeDeserializer<Order>> {
    decode_once::<Owned<Order>, _>(&mut rig);
    rig
}

#[library_benchmark]
#[bench::single_value(single_value_rig())]
fn decode_value(
    mut rig: Rig<spate_json::JsonValueDeserializer>,
) -> Rig<spate_json::JsonValueDeserializer> {
    decode_once::<Owned<Value>, _>(&mut rig);
    rig
}

#[library_benchmark]
#[bench::ndjson_batch50(ndjson_batch_rig())]
#[bench::array_batch50(array_batch_rig())]
fn decode_framed(
    mut rig: Rig<spate_json::JsonSerdeDeserializer<Reading>>,
) -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    decode_once::<Owned<Reading>, _>(&mut rig);
    rig
}

// The error-policy cases that return `Ok`: valid input, partially poisoned
// input under `Skip`, and the array framing dropping a payload whole.
#[library_benchmark]
#[bench::ndjson_clean(ndjson_clean_rig())]
#[bench::ndjson_syntax_10pct(ndjson_bad_rig(BAD_EVERY, Corruption::Syntax))]
#[bench::ndjson_type_10pct(ndjson_bad_rig(BAD_EVERY, Corruption::TypeMismatch))]
#[bench::ndjson_syntax_all(ndjson_bad_rig(1, Corruption::Syntax))]
#[bench::ndjson_fail_clean(ndjson_fail_clean_rig())]
#[bench::array_clean(array_clean_rig())]
#[bench::array_bad_last(array_bad_last_rig())]
fn decode_batch(
    mut rig: Rig<spate_json::JsonSerdeDeserializer<Reading>>,
) -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    assert_eq!(
        decode_run::<Owned<Reading>, _>(&mut rig),
        rig.expect,
        "the framing emitted a different count"
    );
    rig
}

// The one case whose payload must fail the whole call. Split out because the
// assertion is the opposite one, and because a `Fail` fixture that decoded
// cleanly would otherwise pass as a fast version of `ndjson_fail_clean`.
#[library_benchmark]
#[bench::ndjson_fail_bad_last(ndjson_fail_bad_last_rig())]
fn decode_batch_failing(
    mut rig: Rig<spate_json::JsonSerdeDeserializer<Reading>>,
) -> Rig<spate_json::JsonSerdeDeserializer<Reading>> {
    assert_eq!(
        decode_run_err::<Owned<Reading>, _>(&mut rig),
        rig.expect,
        "an atomic framing emitted a record before failing"
    );
    rig
}

// The shape axis, and the duplicate-key guard over two of the same corpora.
#[library_benchmark]
#[bench::wide_flat(shape(shapes::wide_flat(), false, 1))]
#[bench::deep_nested(shape(shapes::deep_nested(), false, 1))]
#[bench::numeric_array(shape(shapes::numeric_array(), false, 1))]
#[bench::large_string(shape(shapes::large_string(), false, 1))]
#[bench::dup_guard_wide(shape(shapes::wide_flat(), true, 1))]
#[bench::dup_guard_deep(shape(shapes::deep_nested(), true, 1))]
#[bench::dup_guard_hit(shape(shapes::wide_flat_duplicate_key(), true, 0))]
fn decode_shape(
    mut rig: Rig<spate_json::JsonValueDeserializer>,
) -> Rig<spate_json::JsonValueDeserializer> {
    assert_eq!(
        decode_run::<Owned<Value>, _>(&mut rig),
        rig.expect,
        "the document emitted a different count"
    );
    rig
}

library_benchmark_group!(
    name = decode;
    benchmarks =
        decode_typed,
        decode_value,
        decode_framed,
        decode_batch,
        decode_batch_failing,
        decode_shape
);

// DHAT is scoped as an extra tool rather than a callgrind argument: the
// callgrind invocation — and so every `Ir` baseline — is bit-identical with
// and without it. `--num-callers=500` (the maximum) keeps allocation stacks
// deep enough to attribute to the decode under measurement rather than to
// whichever frame the default depth of 4 happens to cut at.
main!(
    config = LibraryBenchmarkConfig::default().tool(Dhat::with_args(["--num-callers=500"])),
    library_benchmark_groups = decode
);
