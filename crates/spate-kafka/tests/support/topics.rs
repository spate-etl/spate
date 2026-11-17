//! The feed's topics as the container tests create them. The library's tests
//! include this file too, since they cannot reach the rest of the support
//! module.

use rdkafka::admin::{NewTopic, TopicReplication};

pub(crate) const WORK: &str = "units";
pub(crate) const RETRY: &str = "units-retry";
pub(crate) const DEAD: &str = "units-dead";
pub(crate) const ATTEMPT: &str = "units-attempts";

/// The attempt topic's partition count on the MockCluster and the broker.
pub(crate) const ATTEMPT_PARTITIONS: i32 = 2;

/// The work, retry and dead-letter topics, one partition each, the work topic
/// stamped on append, and the compacted attempt topic. The feed never creates
/// a topic.
pub(crate) fn feed_topics() -> Vec<NewTopic<'static>> {
    vec![
        NewTopic::new(WORK, 1, TopicReplication::Fixed(1))
            .set("message.timestamp.type", "LogAppendTime"),
        NewTopic::new(RETRY, 1, TopicReplication::Fixed(1)),
        NewTopic::new(DEAD, 1, TopicReplication::Fixed(1)),
        NewTopic::new(ATTEMPT, ATTEMPT_PARTITIONS, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact"),
    ]
}
