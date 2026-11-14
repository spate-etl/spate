//! Wall-clock cases for the Kafka sink's send: one sealed batch through
//! `KafkaWriter::write_batch`.
//!
//! The sink is built by `spate_kafka::sink::build` against librdkafka's
//! in-process mock cluster, as a pipeline builds it, by
//! `support/produce_rig.rs`. The batch is the frame `support/prepare_rig.rs`
//! builds for the preparation benches: 10,000 messages of 512-byte payloads.
//! Setup writes it once, so the idempotent producer's start and the topic's
//! metadata stay out of the region.
//!
//! One iteration writes the batch and waits for every delivery report. That
//! is the writer's whole `write_batch`: parsing the frame and building each
//! header list, which `prepare_gungraun.rs` also counts, then building each
//! record, handing it to librdkafka, and the countdown the delivery reports
//! complete. The writer runs on a current-thread runtime.
//!
//! # Reading these numbers
//!
//! - `wall_ns_per_iter` is the batch's latency on one thread.
//! - `cpu_ns_per_iter` reads the whole process, so it carries librdkafka's
//!   threads and the mock cluster's on both legs.
//! - The sink runs with `linger.ms` 0, where librdkafka's default is 5 ms. At
//!   the default, every write waits on that timer for its last partial message
//!   set, the same whatever the code does.
//! - These are numbers for the client and Spate's code over loopback. No
//!   Apache Kafka or Redpanda broker is involved.
//!
//! # Cases
//!
//! - `produce_keyless`: payload only.
//! - `produce_stamped`: adds four headers to each record, which the writer
//!   copies into a librdkafka header list. It took 37% longer than
//!   `produce_keyless` when the cases were chosen.
//!
//! A keyed case is left out. With a 16-byte key per record it took 4.9% longer
//! than `produce_keyless`, under the driver's 5% floor (both legs of a
//! 6-replicate A/A run, Apple M5 Max).
//!
//! Run: `cargo xtask bench ab --ref main --package spate-kafka --filter produce_`

use spate_bench::{Suite, bench_main};

#[expect(dead_code, reason = "only the rigs that frame a batch are used here")]
#[path = "support/encode_rig.rs"]
mod encode_rig;
#[path = "support/messages.rs"]
mod messages;
#[expect(dead_code, reason = "the batch is written, not prepared")]
#[path = "support/prepare_rig.rs"]
mod prepare_rig;
#[path = "support/produce_rig.rs"]
mod produce_rig;
#[path = "../tests/support/mod.rs"]
mod support;

use produce_rig::{CASES, Case, Rig};

fn case(suite: Suite, case: &Case) -> Suite {
    let batch = case.batch;
    suite
        .case(
            case.id,
            move |corpus, _seed| {
                let rig = Rig::new(batch());
                for frame in &rig.batch.frames {
                    corpus.absorb("frame", frame);
                }
                rig
            },
            |b, rig: &Rig| b.iter(|| rig.write()),
        )
        .items_of(|rig: &Rig| rig.batch.rows)
        .bytes_of(|rig: &Rig| rig.batch.bytes)
        .done()
}

fn suite() -> Suite {
    CASES.iter().fold(spate_bench::suite("spate-kafka"), case)
}

bench_main!(suite);
