//! A datum whose arrays claim more items than its bytes carry does not grow
//! the pipeline's memory with the claim on the serde path.
//!
//! One test in this binary: the allocator's high-water mark is process-wide.

mod common;

#[global_allocator]
static ALLOC: common::HighWater = common::HighWater;

#[derive(Debug, serde::Deserialize)]
struct Tick {}

#[derive(Debug, serde::Deserialize)]
struct Batch {
    groups: Vec<Vec<Tick>>,
}

#[test]
fn claimed_items_do_not_grow_memory_on_the_serde_path() {
    common::assert_bounded(&common::run(
        |b| b.build_serde::<Batch>().unwrap(),
        |batch: &Batch| batch.groups[0].len() as u64,
    ));
}
