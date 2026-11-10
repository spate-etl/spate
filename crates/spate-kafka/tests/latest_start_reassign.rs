//! Starts under `latest` where one consumer's offer arrives after the other's
//! was accepted alone, so the source drains and reassigns, against a group the
//! test leads on rdkafka's in-process MockCluster.
//!
//! Each test asserts the source's log lines for the path it takes. The source
//! logs from the pipeline's controller thread, which no subscriber scoped to
//! the test thread reaches, so this binary captures the whole process's log.

mod support;

use spate_test::LogCapture;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

/// The line the source logs when an offer arrives while other consumers hold
/// lanes, before it drains them to reassign.
const DRAIN: &str = "draining them to admit every consumer's partitions together";
/// The line the source logs when an accept's start fails and is retried.
const RETRY: &str = "could not settle a start position";

/// Every line the process logs at `INFO` and above.
static LOGS: LazyLock<LogCapture> = LazyLock::new(|| {
    let capture = LogCapture::new();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(capture.clone())
        .init();
    capture
});

/// The tests here share [`LOGS`], and `cargo test` runs a binary's tests on
/// several threads, so each test holds this while it runs.
static SERIAL: Mutex<()> = Mutex::new(());

/// The lines logged since the test started, read while it holds [`SERIAL`].
struct Logged {
    _serial: MutexGuard<'static, ()>,
    from: usize,
}

impl Logged {
    fn start() -> Logged {
        let serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        Logged {
            _serial: serial,
            from: LOGS.lines().len(),
        }
    }

    fn lines(&self) -> Vec<String> {
        LOGS.lines().split_off(self.from)
    }

    /// Where the first line containing `needle` is, at or after `from`.
    fn find(&self, needle: &str, from: usize) -> Option<usize> {
        self.lines()
            .iter()
            .skip(from)
            .position(|l| l.contains(needle))
            .map(|i| i + from)
    }

    /// Asserts the source drained its lanes for a reassign, and that a start
    /// failed after the drain when `retried`, or never failed otherwise.
    fn assert_reassigned(&self, retried: bool) {
        let lines = self.lines();
        let drain = self
            .find(DRAIN, 0)
            .unwrap_or_else(|| panic!("no reassign: {lines:#?}"));
        if retried {
            assert!(
                self.find(RETRY, drain).is_some(),
                "no failed start after the reassign: {lines:#?}"
            );
        } else {
            assert!(self.find(RETRY, 0).is_none(), "a start failed: {lines:#?}");
        }
    }
}

/// A start that fails during a reassign must not split the reassign.
mod retried_reassign {
    use crate::Logged;
    use crate::support;
    use crate::support::MockKafka;
    use rdkafka::Offset;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use spate_test::{HANG, unique_name, wait_until};
    use support::latest::{Run, config};
    use support::{committed_by, produce_to};

    const TOPIC: &str = "orders";
    const PARTITIONS: i32 = 4;

    /// One consumer of a two-consumer source gets its offer past the hold, so the
    /// other is accepted alone and then drained for a reassign whose first lookup
    /// fails. Every partition is read once the retry has run. The test leads the
    /// group, so neither consumer's `SyncGroup` is refused.
    #[test]
    fn a_retried_reassign_reads_every_partition() {
        let logged = Logged::start();
        let cluster = MockKafka::new();
        cluster
            .create_topic(TOPIC, PARTITIONS, 1)
            .expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let group = unique_name("split");
        let _leader = cluster.lead_group(&group, TOPIC, PARTITIONS);
        // SAFETY: the pointer is valid while `cluster` lives. The list holds one
        // error and one round trip in the C types the variadic reads.
        unsafe {
            rdkafka::bindings::rd_kafka_mock_broker_push_request_error_rtts(
                cluster.native(),
                1,
                RDKafkaApiKey::SyncGroup as i16,
                1,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                5000 as std::ffi::c_int,
            );
        }
        let mut cfg = config(&brokers, TOPIC, &group, "latest");
        cfg.consumers = 2;
        let run = Run::start(cfg);

        // The first consumer's start commit shows it was accepted alone. The next
        // lookup is the reassign's, once the second consumer's offer arrives.
        wait_until(HANG, "the first accept", || {
            committed_by(&brokers, &group, TOPIC, PARTITIONS)
                .iter()
                .any(|o| matches!(o, Offset::Offset(_)))
        });
        cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
        );

        // Each round is one record per partition: the first run's start moved past
        // nothing here, so any partition with a reader writes its round.
        let mut next = 0;
        let mut tick = 0;
        wait_until(HANG, "a write from every partition", || {
            if tick % 200 == 0 {
                for p in 0..PARTITIONS {
                    produce_to(&brokers, TOPIC, p, next..next + 1);
                }
                next += 1;
            }
            tick += 1;
            (0..PARTITIONS).all(|p| !run.written(p).is_empty())
        });
        run.stop();
        logged.assert_reassigned(true);
    }
}

/// A retried re-accept that comes due while a poll waits accepts the held offer
/// alone, and the runtime then drops those lanes for the next assignment.
mod late_offers {
    use crate::Logged;
    use crate::support;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use spate_test::{HANG, wait_until};
    use support::latest::{self, Run};
    use support::{MockKafka, produce_to};

    const TOPIC: &str = "orders";

    /// Run two consumers on four partitions, one consumer's `SyncGroup` answered
    /// past the hold, and fail the lookups named by `offset_fetch`. Every partition
    /// must then reach the sink, after a reassign. The test leads the group, so
    /// neither consumer's `SyncGroup` is refused.
    fn every_partition_reads(group: &str, offset_fetch: &[RDKafkaRespErr]) {
        let logged = Logged::start();
        let cluster = MockKafka::new();
        cluster.create_topic(TOPIC, 4, 1).expect("create topic");
        let _leader = cluster.lead_group(group, TOPIC, 4);
        // SAFETY: the pointer is valid while `cluster` lives, and the arguments
        // match the (error, rtt in ms) pairs the variadic reads.
        unsafe {
            let err = rdkafka::bindings::rd_kafka_mock_broker_push_request_error_rtts(
                cluster.native(),
                1,
                RDKafkaApiKey::SyncGroup as i16,
                1,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                4_500i32,
            );
            assert_eq!(err, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
        }
        if !offset_fetch.is_empty() {
            cluster.request_errors(RDKafkaApiKey::OffsetFetch, offset_fetch);
        }
        let brokers = cluster.bootstrap_servers();
        let mut cfg = latest::config(&brokers, TOPIC, group, "latest");
        cfg.consumers = 2;
        let run = Run::start(cfg);
        let mut next = 1_000;
        let mut tick = 0;
        wait_until(HANG, "every partition to reach the sink", || {
            if tick % 20 == 0 {
                for p in 0..4 {
                    produce_to(&brokers, TOPIC, p, next..next + 1);
                }
                next += 1;
            }
            tick += 1;
            let unread: Vec<i32> = (0..4).filter(|p| run.written(*p).is_empty()).collect();
            unread.is_empty()
        });
        run.stop();
        logged.assert_reassigned(!offset_fetch.is_empty());
    }

    /// The control: with no failed lookup the late offer's re-accept covers every
    /// partition.
    #[test]
    fn a_late_offer_reads_every_partition() {
        every_partition_reads("late-offer", &[]);
    }

    /// The re-accept's first lookup fails once. Its retry must still accept every
    /// consumer's partitions together.
    #[test]
    fn a_retried_reaccept_reads_every_partition() {
        every_partition_reads(
            "late-offer-retried",
            &[
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN,
            ],
        );
    }
}
