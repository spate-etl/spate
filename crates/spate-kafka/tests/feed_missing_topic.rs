//! The Kafka feed against a dead-letter topic one metadata response calls
//! missing, on rdkafka's in-process MockCluster. A binary of its own: the
//! test installs a process-global log subscriber, which librdkafka's threads
//! log through.

mod support;

use rdkafka::types::RDKafkaRespErr;
use spate_core::feed::{Feed as _, FeedErrorKind};
use spate_test::{HANG, LogCapture, wait_until};
use support::MockKafka;
use support::feed::{
    ATTEMPT, ATTEMPT_PARTITIONS, DEAD, RETRY, WORK, direct, feed_yaml, high_watermark, produce_raw,
};

/// Pins that a dead-letter topic one metadata response calls missing does
/// not stop the feed: librdkafka marks the topic missing for a window, a
/// write in that window is sent again, and it lands once the window ends.
#[test]
fn a_topic_reported_missing_once_does_not_stop_the_feed() {
    let capture = LogCapture::new();
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .init();
    let cluster = MockKafka::new();
    for topic in [WORK, RETRY, DEAD] {
        cluster.create_topic(topic, 1, 1).expect("create topic");
    }
    cluster
        .create_topic(ATTEMPT, ATTEMPT_PARTITIONS, 1)
        .expect("create topic");
    let brokers = cluster.bootstrap_servers();
    let group = spate_test::unique_name("transient-missing");
    let worker = direct(&feed_yaml(
        &brokers,
        &group,
        "",
        "      topic.metadata.propagation.max.ms: \"1000\"\n      \
         topic.metadata.refresh.interval.ms: \"100\"\n      \
         debug: topic\n",
    ));
    produce_raw(&brokers, WORK, &[(Some(&[0xff, 0xfe][..]), &b"first"[..])]);
    wait_until(HANG, "the first message is dead-lettered", || {
        high_watermark(&brokers, DEAD) == 1
    });
    cluster
        .topic_error(
            DEAD,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
        )
        .unwrap();
    // librdkafka marks the topic missing once it has existed for longer than
    // the propagation window, and logs the change at debug `topic`.
    let missing = format!("Topic {DEAD} changed state exists -> notexists");
    wait_until(HANG, "the dead-letter topic is marked missing", || {
        capture.lines().iter().any(|line| line.contains(&missing))
    });
    cluster
        .topic_error(DEAD, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR)
        .unwrap();
    produce_raw(&brokers, WORK, &[(Some(&[0xff, 0xfe][..]), &b"second"[..])]);
    let mut fatal = None;
    wait_until(
        HANG,
        "the second message is dead-lettered or the feed stops",
        || {
            if let Err(e) = worker.feed.receive(1)
                && e.kind == FeedErrorKind::Fatal
            {
                fatal = Some(e);
            }
            fatal.is_some() || high_watermark(&brokers, DEAD) == 2
        },
    );
    assert!(
        fatal.is_none(),
        "the feed stopped though the topic exists: {fatal:?}"
    );
}
