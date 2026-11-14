//! The source rig `poll_wall.rs` measures and `tests/bench_fixtures.rs`
//! checks: a Kafka source with one lane, against librdkafka's in-process mock
//! cluster, and the records it is fed.
//!
//! Included with `#[path]`. Each includer also declares `mod support` from
//! `tests/support/mod.rs`.

use spate_core::checkpoint::Checkpointer;
use spate_core::record::RawPayload;
use spate_core::source::{PayloadBatch as _, Source as _, SourceCtx, SourceLane as _};
use spate_kafka::{KafkaLane, KafkaSource, KafkaSourceConfig};
use spate_test::HANG;
use std::collections::BTreeMap;
use std::time::Duration;

use crate::support::{MockKafka, await_assignment, drain_lane, produce, serve_events};

/// The topic the source reads.
pub(crate) const TOPIC: &str = "bench-poll";

/// Records one drain reads, and one feed produces.
pub(crate) const RECORDS: usize = 2_048;

/// Drains in one measured region. Pinned, because the records a region reads
/// are produced before it opens and sit in the cluster and the client until
/// it drains them.
pub(crate) const DRAINS: usize = 16;

/// An upper bound on one record's bytes in the cluster's log: a payload
/// `bench-p0-{i}` and a key `k0-{i}` from `support::produce`, 20 bytes at the
/// largest index here, plus the record's own varint fields.
const RECORD_BYTES_MAX: usize = 64;

// A region stays under half of each limit that would change what it measures.
// At librdkafka's `queued.min.messages` (100,000) the client stops fetching
// and backs off. Past 5 MiB the mock cluster drops the oldest records, which
// the client may not have fetched yet.
const _: () = assert!(DRAINS * RECORDS <= 50_000, "a region fills the client's queue");
const _: () = assert!(
    DRAINS * RECORDS * RECORD_BYTES_MAX <= 5 * 1024 * 1024 / 2,
    "a region outruns the mock cluster's log"
);

/// How long one poll waits on an empty queue.
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// The payload tag of every record produced.
const TAG: &str = "bench";

/// The payload, key and offset of each record a drain read, as
/// `support::drain_lane` returns them.
pub(crate) type Drained = Vec<(Vec<u8>, Vec<u8>, i64)>;

/// A source with its one lane, the checkpointer its acks reach, and the
/// cluster it reads.
pub(crate) struct Rig {
    // Declared in drop order: the lane before its source, the source before
    // its cluster.
    lane: KafkaLane,
    source: KafkaSource,
    checkpointer: Checkpointer,
    brokers: String,
    max_records: usize,
    _cluster: MockKafka,
}

impl Rig {
    /// A source assigned the one partition of a fresh topic, polling at most
    /// `max_records` per batch, with the payload, key and offset of the first
    /// [`RECORDS`] records it read.
    ///
    /// `fetch.wait.max.ms` is 1. The mock cluster holds an empty fetch for the
    /// whole wait and does not end it when records arrive, so the default
    /// 500 ms would leave a drain idle. At 0 a client with nothing to fetch
    /// asks again at once, and that loop competes with the lane for a core.
    pub(crate) fn new(max_records: usize) -> (Rig, Drained) {
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let mut config = KafkaSourceConfig::new(&brokers, TOPIC, "bench-poll");
        config.statistics_interval = Duration::ZERO;
        config.rdkafka = BTreeMap::from([
            ("auto.offset.reset".to_owned(), "earliest".to_owned()),
            ("fetch.wait.max.ms".to_owned(), "1".to_owned()),
        ]);
        let mut checkpointer = Checkpointer::new();
        let mut source = KafkaSource::new(config);
        source
            .open(SourceCtx::new(checkpointer.handle()))
            .expect("open");
        let mut lanes = await_assignment(&mut source, HANG);
        assert_eq!(lanes.len(), 1, "one lane for the one partition");
        let mut lane = lanes.pop().expect("one lane");
        checkpointer.begin_epoch(&[lane.partition()], 1);

        produce(&brokers, TOPIC, RECORDS, 1, TAG);
        let read = drain_lane(&mut source, &mut lane, RECORDS, HANG);
        let mut rig = Rig {
            lane,
            source,
            checkpointer,
            brokers,
            max_records,
            _cluster: cluster,
        };
        rig.settle();
        (rig, read)
    }

    /// Serve the source's control plane once and take the acks the lane's
    /// batches sent, so neither grows across regions.
    pub(crate) fn settle(&mut self) {
        let mut errors = Vec::new();
        serve_events(&mut self.source, &mut errors);
        assert!(errors.is_empty(), "the source failed: {errors:?}");
        self.checkpointer.drain();
        drop(self.checkpointer.take_watermarks());
    }

    /// Produce the [`RECORDS`] records one drain reads.
    pub(crate) fn feed(&self) {
        produce(&self.brokers, TOPIC, RECORDS, 1, TAG);
    }

    /// The measured work: poll the lane until it has handed out [`RECORDS`]
    /// payloads, passing each to `each`. Each poll asks for no more than what
    /// is left, so a drain never reads records fed for the next one. Returns
    /// the payloads read.
    ///
    /// # Panics
    ///
    /// When the lane stays empty for [`HANG`].
    pub(crate) fn drain(&mut self, mut each: impl FnMut(&RawPayload<'_>)) -> usize {
        let empty_limit = HANG.as_millis() / POLL_TIMEOUT.as_millis();
        let mut read = 0;
        let mut empty = 0;
        while read < RECORDS {
            let want = self.max_records.min(RECORDS - read);
            match self.lane.poll(want, POLL_TIMEOUT).expect("lane poll") {
                Some(mut batch) => {
                    empty = 0;
                    while let Some(payload) = batch.next_payload() {
                        each(&payload);
                        read += 1;
                    }
                }
                None => {
                    empty += 1;
                    assert!(
                        empty < empty_limit,
                        "the lane read {read} of {RECORDS} records and then stayed empty"
                    );
                }
            }
        }
        read
    }
}
