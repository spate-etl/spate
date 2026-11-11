//! The librdkafka mock cluster every Kafka mock test builds, with the mock
//! delays a test does not exercise removed.

// A per-item `expect` goes unfulfilled in whichever target uses the item.
#![allow(dead_code, reason = "each target uses a different subset")]

use rdkafka::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::mocking::{MockCluster, MockCoordinator};
use rdkafka::producer::{BaseProducer, DefaultProducerContext, Producer as _};
use rdkafka::types::{RDKafkaApiKey, RDKafkaMockCluster, RDKafkaRespErr};
use std::sync::Arc;
use std::time::Duration;

/// The mock's default hold on an empty group's first rebalance, in
/// milliseconds.
const INITIAL_REBALANCE_DELAY_MS: i32 = 3000;

/// A librdkafka mock cluster whose consumer groups form at once.
///
/// The mock holds an empty group's first rebalance for 3 s by default, so that
/// members starting together join one generation. A cluster from here has no
/// such hold: the first member to join forms the group alone, and each later
/// member rebalances it after `session.timeout.ms - 1000`. A test that starts
/// several members of one group restores the hold with
/// [`MockKafka::hold_first_rebalance`].
///
/// A group of several members can lose a race whenever it forms. The mock
/// refuses a follower's `SyncGroup` that arrives after the leader's, and the
/// follower's rejoin holds the group for `session.timeout.ms - 1000`. A test
/// gives its source one consumer unless several members are what it tests.
///
/// Its methods are [`MockCluster`]'s. The cluster lives in a client of its
/// own, which every clone shares, and is destroyed with the last clone.
#[derive(Clone)]
pub(crate) struct MockKafka {
    client: Arc<BaseProducer>,
}

impl MockKafka {
    /// A cluster of one broker.
    ///
    /// # Panics
    ///
    /// If librdkafka cannot start the cluster.
    pub(crate) fn new() -> Self {
        Self::with_brokers(1)
    }

    /// A cluster of `brokers` brokers.
    ///
    /// # Panics
    ///
    /// If librdkafka cannot start the cluster.
    pub(crate) fn with_brokers(brokers: i32) -> Self {
        // librdkafka sets the delay only through a cluster's native handle, and
        // rdkafka hands one out only for a cluster a client config starts.
        #[expect(
            clippy::disallowed_methods,
            reason = "the client only holds the mock cluster, outside every role"
        )]
        let client: BaseProducer = ClientConfig::new()
            .set("test.mock.num.brokers", brokers.to_string())
            .create()
            .expect("mock cluster");
        let kafka = Self {
            client: Arc::new(client),
        };
        kafka.set_initial_rebalance_delay(0);
        kafka
    }

    /// Holds an empty group's first rebalance for the mock's default 3 s, for a
    /// test whose members start together and must join one generation. It
    /// applies to each group that forms after the call.
    pub(crate) fn hold_first_rebalance(&self) {
        self.set_initial_rebalance_delay(INITIAL_REBALANCE_DELAY_MS);
    }

    /// Queues one accepted `JoinGroup` and three refused after it, behind any
    /// `JoinGroup` errors already queued, so one consumer of a group forms it
    /// alone and the next joins about 3 s later, one librdkafka join interval
    /// per refusal.
    pub(crate) fn hold_back_the_second_join(&self) {
        let refused = RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS;
        self.request_errors(
            RDKafkaApiKey::JoinGroup,
            &[
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                refused,
                refused,
                refused,
            ],
        );
    }

    fn cluster(&self) -> MockCluster<'_, DefaultProducerContext> {
        self.client
            .client()
            .mock_cluster()
            .expect("the client was built with a mock cluster")
    }

    /// See [`MockCluster::bootstrap_servers`].
    pub(crate) fn bootstrap_servers(&self) -> String {
        self.cluster().bootstrap_servers()
    }

    /// See [`MockCluster::create_topic`].
    pub(crate) fn create_topic(
        &self,
        topic: &str,
        partition_count: i32,
        replication_factor: i32,
    ) -> KafkaResult<()> {
        self.cluster()
            .create_topic(topic, partition_count, replication_factor)
    }

    /// See [`MockCluster::request_errors`].
    pub(crate) fn request_errors(&self, api_key: RDKafkaApiKey, errors: &[RDKafkaRespErr]) {
        self.cluster().request_errors(api_key, errors);
    }

    /// See [`MockCluster::clear_request_errors`].
    pub(crate) fn clear_request_errors(&self, api_key: RDKafkaApiKey) {
        self.cluster().clear_request_errors(api_key);
    }

    /// See [`MockCluster::topic_error`].
    pub(crate) fn topic_error(&self, topic: &str, error: RDKafkaRespErr) -> KafkaResult<()> {
        self.cluster().topic_error(topic, error)
    }

    /// See [`MockCluster::partition_leader`].
    pub(crate) fn partition_leader(
        &self,
        topic: &str,
        partition: i32,
        broker_id: Option<i32>,
    ) -> KafkaResult<()> {
        self.cluster().partition_leader(topic, partition, broker_id)
    }

    /// See [`MockCluster::broker_down`].
    pub(crate) fn broker_down(&self, broker_id: i32) -> KafkaResult<()> {
        self.cluster().broker_down(broker_id)
    }

    /// See [`MockCluster::broker_up`].
    pub(crate) fn broker_up(&self, broker_id: i32) -> KafkaResult<()> {
        self.cluster().broker_up(broker_id)
    }

    /// See [`MockCluster::broker_round_trip_time`].
    pub(crate) fn broker_round_trip_time(
        &self,
        broker_id: i32,
        delay: Duration,
    ) -> KafkaResult<()> {
        self.cluster().broker_round_trip_time(broker_id, delay)
    }

    /// See [`MockCluster::coordinator`].
    pub(crate) fn coordinator(
        &self,
        coordinator: MockCoordinator,
        broker_id: i32,
    ) -> KafkaResult<()> {
        self.cluster().coordinator(coordinator, broker_id)
    }

    /// See [`MockCluster::apiversion`].
    pub(crate) fn apiversion(
        &self,
        api_key: RDKafkaApiKey,
        min_version: Option<i16>,
        max_version: Option<i16>,
    ) -> KafkaResult<()> {
        self.cluster().apiversion(api_key, min_version, max_version)
    }

    /// Hold broker `broker`'s reply to its next `api` request for `held`.
    ///
    /// # Panics
    ///
    /// When the cluster refuses the hold.
    pub(crate) fn hold_next_reply(&self, broker: i32, api: RDKafkaApiKey, held: Duration) {
        let millis = i32::try_from(held.as_millis()).expect("a hold that fits in i32 ms");
        // SAFETY: the pointer is valid while `self` lives, and the variadic
        // reads one (error, round-trip ms) pair, as the count says.
        let err = unsafe {
            rdkafka::bindings::rd_kafka_mock_broker_push_request_error_rtts(
                self.native(),
                broker,
                api as i16,
                1,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                millis,
            )
        };
        assert_eq!(err, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
    }

    /// The cluster's native handle, for the mock calls rdkafka does not wrap. It
    /// is valid while `self` lives.
    pub(crate) fn native(&self) -> *mut RDKafkaMockCluster {
        // SAFETY: the client handle is valid while `self.client` lives, and the
        // call only reads the cluster pointer the client holds.
        unsafe {
            rdkafka::bindings::rd_kafka_handle_mock_cluster(self.client.client().native_ptr())
        }
    }

    /// Record each request the cluster receives from here on, as it arrives and
    /// before any reply delay, for [`MockKafka::received`] to count.
    pub(crate) fn track_requests(&self) {
        // SAFETY: the pointer is valid while `self` lives.
        unsafe { rdkafka::bindings::rd_kafka_mock_start_request_tracking(self.native()) };
    }

    /// How many `api` requests the cluster has received since
    /// [`MockKafka::track_requests`].
    pub(crate) fn received(&self, api: RDKafkaApiKey) -> usize {
        self.count_requests(|key, _| key == api as i16)
    }

    /// How many `api` requests broker `broker` has received since
    /// [`MockKafka::track_requests`].
    pub(crate) fn received_from(&self, api: RDKafkaApiKey, broker: i32) -> usize {
        self.count_requests(|key, id| key == api as i16 && id == broker)
    }

    /// The API key of each request the cluster has received since
    /// [`MockKafka::track_requests`], in the order it received them.
    pub(crate) fn received_keys(&self) -> Vec<i16> {
        self.tracked().into_iter().map(|(key, _)| key).collect()
    }

    /// How many tracked requests `keep` accepts, given each one's API key and
    /// the id of the broker that received it.
    fn count_requests(&self, keep: impl Fn(i16, i32) -> bool) -> usize {
        self.tracked()
            .into_iter()
            .filter(|(key, broker)| keep(*key, *broker))
            .count()
    }

    /// Each tracked request's API key and the id of the broker that received
    /// it, in the order received.
    fn tracked(&self) -> Vec<(i16, i32)> {
        let mut count = 0;
        // SAFETY: the pointer is valid while `self` lives, and the array holds
        // `count` requests until it is destroyed.
        unsafe {
            let requests =
                rdkafka::bindings::rd_kafka_mock_get_requests(self.native(), &raw mut count);
            let seen = (0..count)
                .map(|i| {
                    let request = *requests.add(i);
                    (
                        rdkafka::bindings::rd_kafka_mock_request_api_key(request),
                        rdkafka::bindings::rd_kafka_mock_request_id(request),
                    )
                })
                .collect();
            rdkafka::bindings::rd_kafka_mock_request_destroy_array(requests, count);
            seen
        }
    }

    fn set_initial_rebalance_delay(&self, delay_ms: i32) {
        // SAFETY: `native` is valid while `self` lives, and the call writes the
        // cluster default under the cluster's own lock.
        unsafe {
            rdkafka::bindings::rd_kafka_mock_group_initial_rebalance_delay_ms(
                self.native(),
                delay_ms,
            );
        }
    }
}
