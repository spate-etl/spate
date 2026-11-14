//! The sink rig `produce_wall.rs` measures and `tests/bench_fixtures.rs`
//! checks: a Kafka sink built as a pipeline builds it, against librdkafka's
//! in-process mock cluster, and one sealed batch it writes.
//!
//! Included with `#[path]`. Each includer also declares `mod support` from
//! `tests/support/mod.rs`, and `mod encode_rig`, `mod messages` and
//! `mod prepare_rig` from this directory, since the batch is the one the
//! preparation benches parse.

use spate_core::sink::{SealedBatch, ShardWriter as _};
use spate_kafka::sink::{KafkaSink, KafkaSinkConfig};
use std::time::Duration;
use tokio::runtime::Runtime;

use crate::prepare_rig::Batch;
use crate::support::MockKafka;

/// The topic every case writes to.
pub(crate) const TOPIC: &str = "bench-produce";

/// One case: its id, the batch it writes, whether each message carries a key,
/// and how many headers each carries.
pub(crate) struct Case {
    pub(crate) id: &'static str,
    pub(crate) batch: fn() -> Batch,
    #[allow(dead_code, reason = "the fixture test reads it, the bench does not")]
    pub(crate) keyed: bool,
    #[allow(dead_code, reason = "the fixture test reads it, the bench does not")]
    pub(crate) headers: usize,
}

/// Every case `produce_wall.rs` runs. The fixture test writes each one and
/// reads back its shape.
pub(crate) const CASES: [Case; 2] = [
    Case {
        id: "produce_keyless",
        batch: crate::prepare_rig::keyless,
        keyed: false,
        headers: 0,
    },
    Case {
        id: "produce_stamped",
        batch: crate::prepare_rig::stamped,
        keyed: false,
        headers: 4,
    },
];

/// A sink, the batch it writes, and the cluster it writes to.
pub(crate) struct Rig {
    runtime: Runtime,
    sink: KafkaSink,
    pub(crate) batch: SealedBatch,
    // Last, so the producer is destroyed while its cluster is still up.
    #[allow(dead_code, reason = "the fixture test reads it, the bench does not")]
    pub(crate) cluster: MockKafka,
}

impl Rig {
    /// A sink on a fresh one-partition topic that has already written `batch`
    /// once, so the idempotent producer's start and the topic's metadata stay
    /// out of every later write.
    ///
    /// `linger.ms` is 0. At librdkafka's default of 5 ms, every write would
    /// wait on that timer for its last partial message set, and the wait is
    /// the same whatever the code does.
    pub(crate) fn new(batch: Batch) -> Rig {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let mut config = KafkaSinkConfig::new(cluster.bootstrap_servers(), TOPIC);
        config.statistics_interval = Duration::ZERO;
        config
            .rdkafka
            .insert("linger.ms".to_owned(), "0".to_owned());
        let sink = spate_kafka::sink::build(config).expect("sink build");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the writer runtime builds");
        let batch = SealedBatch {
            bytes: batch.frames.iter().map(|f| f.len() as u64).sum(),
            frames: batch.frames,
            rows: batch.rows,
            dedup_token: "unused-by-kafka".to_owned(),
        };
        let rig = Rig {
            runtime,
            sink,
            batch,
            cluster,
        };
        rig.write();
        rig
    }

    /// The measured work: write the batch and wait for every delivery report.
    /// Returns the rows written.
    pub(crate) fn write(&self) -> u64 {
        self.runtime
            .block_on(
                self.sink
                    .writer
                    .write_batch(&self.sink.endpoints[0][0], &self.batch),
            )
            .map(|()| self.batch.rows)
            .expect("the mock cluster accepts the batch")
    }
}
