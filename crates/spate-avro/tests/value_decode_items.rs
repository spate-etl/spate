//! A datum whose arrays claim more items than its bytes carry does not grow
//! the pipeline's memory with the claim on the value path.
//!
//! One test in this binary: the allocator's high-water mark is process-wide.

mod common;

use spate_avro::AvroValue;

#[global_allocator]
static ALLOC: common::HighWater = common::HighWater;

/// The tick count of the first group.
fn ticks(value: &AvroValue) -> u64 {
    let AvroValue::Record(fields) = value else {
        panic!("datum is {value:?}")
    };
    let AvroValue::Array(groups) = &fields[0].1 else {
        panic!("groups field is {:?}", fields[0].1)
    };
    let AvroValue::Array(ticks) = &groups[0] else {
        panic!("group is {:?}", groups[0])
    };
    ticks.len() as u64
}

#[test]
fn claimed_items_do_not_grow_memory_on_the_value_path() {
    common::assert_bounded(&common::run(|b| b.build_value(), ticks));
}
