//! Allocation snapshot of this crate's decode paths.
//!
//! Each case drives a rig the counted tier in `decode_gungraun.rs` measures,
//! under the same case names, and prints its allocation, reallocation and
//! byte counts for `cargo xtask bench alloc` to compare with `benches/alloc/`.
//! Harness-free, so every case runs on the main
//! thread. The Confluent rigs stop their stub registry before they are handed
//! over, and every rig's runtime is current-thread, so nothing else allocates
//! while a case is counted. Each case is counted once after one uncounted
//! pass, so the schema memo and the held reader are already in place. The
//! counted tier counts its single-payload cases cold, so it prices building
//! the held reader; these rows price the steady-state decode only.
//!
//! `poisoned_schema_id` is left out. Each lookup of a negative-cached id
//! compares the entry's age with the cache's TTL, so it reads the wall clock.

use spate_test_support::alloc::{Counting, Report, main_with};

#[path = "support/batches.rs"]
mod batches;
#[path = "support/corpora.rs"]
mod corpora;
#[path = "support/decode_rig.rs"]
mod decode_rig;
#[path = "support/orders.rs"]
mod orders;
#[path = "support/registry_stub.rs"]
mod registry_stub;

use decode_rig::{
    batch_datum_rig, batch_value_rig, confluent_cached_rig, confluent_unknown_rig, decode_batch,
    decode_once, decode_once_err, evolution_rig, flat_datum_rig, flat_serde_rig,
    flat_value_malformed_rig, flat_value_rig, map_heavy_rig, recursive_rig, shapes_rig,
    single_object_rig, stale_fingerprint_rig, truncated_datum_rig, truncated_serde_rig,
};

#[global_allocator]
static ALLOC: Counting = Counting;

fn main() {
    main_with(|report| {
        single(report);
        batches(report);
    });
}

fn single(report: &mut Report) {
    let mut r = flat_value_rig();
    report.case("decode_value.flat_record", || decode_once(&mut r));
    let mut r = batch_value_rig();
    report.case("decode_value.batch50", || decode_once(&mut r));
    let mut r = flat_serde_rig();
    report.case("decode_serde_typed.flat_record", || decode_once(&mut r));
    let mut r = flat_datum_rig();
    report.case("decode_datum_typed.flat_record", || decode_once(&mut r));
    let mut r = flat_value_malformed_rig();
    report.case("decode_value_malformed.flat_record_malformed", || {
        decode_once_err(&mut r);
    });
    let mut r = batch_datum_rig();
    report.case("decode_batch_datum_typed.batch50", || decode_once(&mut r));
}

fn batches(report: &mut Report) {
    let mut r = confluent_cached_rig();
    report.case("decode_confluent.cached_schema", || {
        decode_batch(&mut r);
    });
    let mut r = confluent_unknown_rig();
    report.case("decode_confluent.unknown_schema_id", || {
        decode_batch(&mut r);
    });
    for (name, reader) in [
        ("decode_resolved.writer_schema_only", None),
        (
            "decode_resolved.reordered_fields",
            Some(corpora::EVENT_REORDERED),
        ),
        (
            "decode_resolved.promoted_numerics",
            Some(corpora::EVENT_PROMOTED),
        ),
        (
            "decode_resolved.added_default_field",
            Some(corpora::EVENT_DEFAULTED),
        ),
    ] {
        let mut r = evolution_rig(reader);
        report.case(name, || {
            decode_batch(&mut r);
        });
    }
    let mut r = shapes_rig();
    report.case("decode_shapes.logical_types", || {
        decode_batch(&mut r);
    });
    let mut r = map_heavy_rig();
    report.case("decode_map_heavy.map_heavy", || {
        decode_batch(&mut r);
    });
    let mut r = recursive_rig();
    report.case("decode_recursive.recursive_refs", || {
        decode_batch(&mut r);
    });
    let mut r = truncated_datum_rig();
    report.case("decode_malformed_datum_typed.truncated_datum", || {
        decode_batch(&mut r);
    });
    let mut r = truncated_serde_rig();
    report.case("decode_malformed_serde_typed.truncated_datum", || {
        decode_batch(&mut r);
    });
    let mut r = single_object_rig();
    report.case("decode_single_object.matching_fingerprint", || {
        decode_batch(&mut r);
    });
    let mut r = stale_fingerprint_rig();
    report.case("decode_single_object.stale_fingerprint", || {
        decode_batch(&mut r);
    });
}
