//! Wall-clock cases for the Kafka source's lane: records polled off a
//! partition queue and handed out one payload at a time.
//!
//! The source is a `KafkaSource` with one lane, built against librdkafka's
//! in-process mock cluster by `support/poll_rig.rs`. Setup opens it, waits for
//! the assignment, and absorbs the payloads and keys of a first drain into the
//! corpus. Each input of a region produces 2,048 records before the region
//! opens, and each iteration polls the lane until it has handed out those
//! 2,048 payloads. The client fetches them in the background while the inputs
//! are built, so most wait in the partition queue when the region opens.
//!
//! # Reading these numbers
//!
//! - The region holds the lane's poll: librdkafka's queue pop, the lane holding
//!   each message for its batch, the batch's ack, and `next_payload` for each
//!   record. It also holds releasing each message when the next poll clears
//!   the batch, and any fetch still in flight when the region opens.
//! - `cpu_ns_per_iter` reads the whole process, so it carries librdkafka's
//!   threads and the mock cluster's on both legs.
//! - The records are small (about 20 bytes of payload and key). The lane hands
//!   out slices of librdkafka's memory, so its cost is per record and not per
//!   byte.
//! - These are numbers for the client and Spate's code over loopback. No
//!   Apache Kafka or Redpanda broker is involved.
//!
//! # Cases
//!
//! - `poll_default_batch` polls up to 512 records at a time, the
//!   `RuntimeOptions` default.
//! - `poll_small_batch` polls up to 32, so the per-poll work (the ack and
//!   clearing the batch) weighs more against the per-record work.
//!
//! The iteration count is pinned at `DRAINS`, because a region's records are
//! produced before it opens. `poll_rig.rs` holds the count under the client's
//! queue limit and the mock cluster's log limit.
//!
//! Run: `cargo xtask bench ab --ref main --package spate-kafka --filter poll_`

use spate_bench::{Suite, bench_main};
use std::cell::RefCell;

#[path = "support/poll_rig.rs"]
mod poll_rig;
#[path = "../tests/support/mod.rs"]
mod support;

use poll_rig::{DRAINS, RECORDS, Rig};

fn case(suite: Suite, id: &str, max_records: usize) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let (rig, read) = Rig::new(max_records);
                for (payload, key, _offset) in &read {
                    corpus.absorb("payload", payload);
                    corpus.absorb("key", key);
                }
                RefCell::new(rig)
            },
            |b, rig: &RefCell<Rig>| {
                b.iter_batched(
                    |i| {
                        let mut rig = rig.borrow_mut();
                        if i == 0 {
                            rig.settle();
                        }
                        rig.feed();
                    },
                    |()| {
                        let mut bytes = 0usize;
                        let read = rig
                            .borrow_mut()
                            .drain(|payload| bytes += payload.bytes.len());
                        (read, bytes)
                    },
                );
            },
        )
        .iters(DRAINS as u64)
        .items(RECORDS as u64)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-kafka");
    let suite = case(suite, "poll_default_batch", 512);
    case(suite, "poll_small_batch", 32)
}

bench_main!(suite);
