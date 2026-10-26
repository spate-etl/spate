//! Allocation snapshot of this crate's decode paths.
//!
//! Each case drives the same rig and the same region the counted tier in
//! `decode_gungraun.rs` measures, under the same case names, and prints its
//! allocation, reallocation and byte counts for `cargo xtask bench alloc` to
//! compare with `benches/alloc/`. Harness-free, so
//! every case runs on the main thread and nothing else allocates while one is
//! counted. Each case is counted once after one uncounted pass, so lazily
//! built state such as the `simd` backend's thread-local scratch is already in
//! place. The four reference cases are counted warm here, where the counted
//! tier counts them cold.
//!
//! The skip-path cases (`ndjson_syntax_10pct`, `ndjson_type_10pct`,
//! `ndjson_syntax_all`, `array_bad_last`, `dup_guard_hit`) are left out. Each
//! drop under `on_error: skip` passes the skip warning's rate limiter, which
//! reads the wall clock, so whether a pass emits an event depends on time.

use serde_json::Value;
use spate_core::deser::Owned;
use spate_json::{JsonFraming, OnError};
use spate_test_support::alloc::{Counting, Report, main_with};

#[path = "support/decode_rig.rs"]
mod decode_rig;
#[path = "support/orders.rs"]
mod orders;
#[path = "support/shapes.rs"]
mod shapes;

use decode_rig::{Rig, batch_rig, decode_once, decode_run, decode_run_err, rig, shape_rig};
use orders::{Corruption, LineItem, Order, RECORDS};

#[global_allocator]
static ALLOC: Counting = Counting;

/// The batch size of `decode_gungraun.rs`'s reference cases.
const BATCH: u64 = 50;

const LABELS: (&str, &str) = ("snapshot", "json");

fn main() {
    main_with(|report| {
        report.key("backend", spate_json::BACKEND_ID);
        // The x86 implementation is chosen at run time from the host's CPU, so
        // a runner on another CPU family is named rather than silently
        // compared.
        #[cfg(feature = "simd")]
        report.key("simd-json", simd_json::Deserializer::algorithm());
        reference(report);
        error_policy(report);
        shape(report);
    });
}

fn reference(report: &mut Report) {
    let mut r = rig(JsonFraming::Single, orders::order_document(), 1, |b| {
        b.build_serde::<Order>()
    });
    report.case("single_typed", || decode_once::<Owned<Order>, _>(&mut r));

    let mut r = rig(JsonFraming::Single, orders::order_document(), 1, |b| {
        b.build_value()
    });
    report.case("single_value", || decode_once::<Owned<Value>, _>(&mut r));

    let mut r = rig(
        JsonFraming::Ndjson,
        orders::lines_ndjson(BATCH),
        BATCH,
        |b| b.build_serde::<LineItem>(),
    );
    report.case("ndjson_batch50", || {
        decode_once::<Owned<LineItem>, _>(&mut r);
    });

    let mut r = rig(JsonFraming::Array, orders::lines_array(BATCH), BATCH, |b| {
        b.build_serde::<LineItem>()
    });
    report.case("array_batch50", || {
        decode_once::<Owned<LineItem>, _>(&mut r);
    });
}

fn error_policy(report: &mut Report) {
    let cases = [
        (
            "ndjson_clean",
            JsonFraming::Ndjson,
            OnError::Skip,
            orders::lines_ndjson(RECORDS),
            RECORDS,
        ),
        (
            "ndjson_fail_clean",
            JsonFraming::Ndjson,
            OnError::Fail,
            orders::lines_ndjson(RECORDS),
            RECORDS,
        ),
        (
            "array_clean",
            JsonFraming::Array,
            OnError::Skip,
            orders::lines_array(RECORDS),
            RECORDS,
        ),
    ];
    for (name, framing, on_error, payload, expect) in cases {
        let mut r = batch_rig::<LineItem>(framing, on_error, payload, expect, LABELS);
        report.case(name, || batch_case(&mut r));
    }

    let mut r = batch_rig::<LineItem>(
        JsonFraming::Ndjson,
        OnError::Fail,
        orders::lines_ndjson_bad_last(RECORDS, Corruption::TypeMismatch),
        0,
        LABELS,
    );
    report.case("ndjson_fail_bad_last", || {
        assert_eq!(
            decode_run_err::<Owned<LineItem>, _>(&mut r),
            0,
            "an atomic framing emitted a record before failing"
        );
    });
}

fn batch_case(r: &mut Rig<spate_json::JsonSerdeDeserializer<LineItem>>) {
    assert_eq!(
        decode_run::<Owned<LineItem>, _>(r),
        r.expect,
        "the framing emitted a different count"
    );
}

fn shape(report: &mut Report) {
    let cases = [
        ("wide_flat", shapes::wide_flat(), false),
        ("deep_nested", shapes::deep_nested(), false),
        ("numeric_array", shapes::numeric_array(), false),
        ("large_string", shapes::large_string(), false),
        ("dup_guard_wide", shapes::wide_flat(), true),
        ("dup_guard_deep", shapes::deep_nested(), true),
    ];
    for (name, payload, guard) in cases {
        let mut r = shape_rig(payload, guard, 1, LABELS);
        report.case(name, || {
            assert_eq!(
                decode_run::<Owned<Value>, _>(&mut r),
                1,
                "the document emitted a different count"
            );
        });
    }
}
