//! What a test does to a broker through a Kafka client: create topics and
//! wait until the broker lists them, produce records, and read a group's
//! committed offsets, a partition's high watermark, a topic's partition count
//! or a topic from its start. A reader named `_once` reads once, for a
//! caller whose own wait reads again.

use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _, ProducerContext};
use rdkafka::{Offset, TopicPartitionList};

/// How long one metadata request waits on a cluster that may be electing.
pub(crate) const METADATA_TIMEOUT: Duration = Duration::from_secs(5);

/// The longest one offset or watermark request waits before it is sent
/// again, and the bound of a `_once` offset or watermark read. A broker can
/// drop a request while a coordinator or leader moves.
const READ_EVERY: Duration = Duration::from_secs(10);

/// How long [`send`] serves delivery reports before it asks a full queue
/// again.
const DRAIN_EVERY: Duration = Duration::from_millis(50);

/// A metadata reader's settings. librdkafka lets a producer's metadata read
/// create the topic it names, and an admin client is a producer, so the read
/// goes through a consumer with auto-creation pinned off.
const METADATA: &[(&str, &str)] = &[("allow.auto.create.topics", "false")];

/// A producer's settings: a record the broker has not taken within 30 s
/// fails.
const PRODUCER: &[(&str, &str)] = &[("message.timeout.ms", "30000")];

/// What a transactional producer adds. Kafka's 60 s default can abort an open
/// transaction under a slow suite.
const TRANSACTIONAL: &[(&str, &str)] = &[("transaction.timeout.ms", "600000")];

/// A client of `bootstrap` with `settings`, outside every connector role.
#[expect(
    clippy::disallowed_methods,
    reason = "test helpers build their clients outside spate-kafka's factory"
)]
fn client<T: FromClientConfig>(bootstrap: &str, settings: &[(&str, &str)]) -> Result<T, String> {
    config(bootstrap, settings)
        .create()
        .map_err(|e| format!("Kafka client: {e}"))
}

/// The configuration of every client this module builds: `bootstrap` with
/// `settings`. OpenSSL is initialised first, as
/// [`leave_openssl_running`](crate::leave_openssl_running) says.
#[expect(
    clippy::disallowed_methods,
    reason = "test helpers build their clients outside spate-kafka's factory"
)]
fn config(bootstrap: &str, settings: &[(&str, &str)]) -> rdkafka::ClientConfig {
    crate::leave_openssl_running();
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", bootstrap);
    for &(key, value) in settings {
        config.set(key, value);
    }
    config
}

/// A client that reads `bootstrap`'s metadata.
pub(crate) fn metadata_probe(bootstrap: &str) -> Result<BaseConsumer, String> {
    consumer(bootstrap, &[])
}

/// A consumer of `bootstrap` with `settings`, outside every connector role,
/// for a test's own reads. Auto-creation is pinned off, as for a metadata
/// read; `settings` add the group, the isolation level and the rest.
///
/// # Errors
///
/// Fails when librdkafka refuses the settings.
pub fn consumer(bootstrap: &str, settings: &[(&str, &str)]) -> Result<BaseConsumer, String> {
    client(bootstrap, &[METADATA, settings].concat())
}

/// A [`consumer`] with `settings`, assigned `partitions` of `topic` from each
/// one's first record, outside any group's coordination.
///
/// # Errors
///
/// Fails when librdkafka refuses the settings or the assignment.
pub fn from_start(
    bootstrap: &str,
    topic: &str,
    partitions: &[i32],
    settings: &[(&str, &str)],
) -> Result<BaseConsumer, String> {
    let reader = consumer(bootstrap, settings)?;
    let mut list = TopicPartitionList::new();
    for &partition in partitions {
        list.add_partition_offset(topic, partition, Offset::Beginning)
            .map_err(|e| format!("assign {topic}/{partition}: {e}"))?;
    }
    reader
        .assign(&list)
        .map_err(|e| format!("assign {topic}: {e}"))?;
    Ok(reader)
}

/// What `consumer`'s group has committed on partitions `0..partitions` of
/// `topic`, as the broker answers it. A failed answer is read again until
/// `bound` passes, since a broker refuses the read while the group's
/// coordinator moves.
///
/// # Errors
///
/// Fails with the last answer's error once `bound` passes. Each request waits
/// at most 10 s or the time left, so the call ends within `bound`.
pub fn committed<C, K>(
    consumer: &K,
    topic: &str,
    partitions: i32,
    bound: Duration,
) -> Result<TopicPartitionList, String>
where
    C: ConsumerContext,
    K: Consumer<C>,
{
    let list = partition_list(topic, partitions);
    read_again(&committed_read(topic), bound, |timeout| {
        committed_within(consumer, &list, timeout)
    })
}

/// [`committed`] as one request of at most 10 s, for a caller whose own wait
/// reads again.
///
/// # Errors
///
/// Fails with the request's error, naming the read.
pub fn committed_once<C, K>(
    consumer: &K,
    topic: &str,
    partitions: i32,
) -> Result<TopicPartitionList, String>
where
    C: ConsumerContext,
    K: Consumer<C>,
{
    let list = partition_list(topic, partitions);
    named(
        &committed_read(topic),
        committed_within(consumer, &list, READ_EVERY),
    )
}

/// The offsets of [`committed`], in partition order. A partition with no
/// commit reads as `Offset::Invalid`.
///
/// # Errors
///
/// As [`committed`].
pub fn committed_offsets<C, K>(
    consumer: &K,
    topic: &str,
    partitions: i32,
    bound: Duration,
) -> Result<Vec<Offset>, String>
where
    C: ConsumerContext,
    K: Consumer<C>,
{
    committed(consumer, topic, partitions, bound).map(|list| offsets(&list))
}

/// The offsets of [`committed_once`], in partition order. A partition with no
/// commit reads as `Offset::Invalid`.
///
/// # Errors
///
/// As [`committed_once`].
pub fn committed_offsets_once<C, K>(
    consumer: &K,
    topic: &str,
    partitions: i32,
) -> Result<Vec<Offset>, String>
where
    C: ConsumerContext,
    K: Consumer<C>,
{
    committed_once(consumer, topic, partitions).map(|list| offsets(&list))
}

/// One committed-offset request for `list`, waiting at most `timeout`.
fn committed_within<C, K>(
    consumer: &K,
    list: &TopicPartitionList,
    timeout: Duration,
) -> KafkaResult<TopicPartitionList>
where
    C: ConsumerContext,
    K: Consumer<C>,
{
    consumer.committed_offsets(list.clone(), timeout)
}

/// What a committed-offset read of `topic` is called in its error.
fn committed_read(topic: &str) -> String {
    format!("a committed-offset read of {topic}")
}

/// The high watermark of `partition` of `topic`, at the isolation level of
/// `consumer`'s settings, each read asking for the topic's metadata first. A
/// failed answer is read again until `bound` passes.
///
/// # Errors
///
/// As [`committed`].
pub fn high_watermark<C, K>(
    consumer: &K,
    topic: &str,
    partition: i32,
    bound: Duration,
) -> Result<i64, String>
where
    C: ConsumerContext,
    K: Consumer<C>,
{
    read_again(&watermark_read(topic, partition), bound, |timeout| {
        high_within(consumer, topic, partition, timeout)
    })
}

/// [`high_watermark`] as one read of at most 10 s, for a caller whose own
/// wait reads again: a metadata request for the topic, then one watermark
/// call. librdkafka sends a request for each watermark in that call and
/// retries some refusals inside it.
///
/// # Errors
///
/// Fails with the call's error, naming the read.
pub fn high_watermark_once<C, K>(consumer: &K, topic: &str, partition: i32) -> Result<i64, String>
where
    C: ConsumerContext,
    K: Consumer<C>,
{
    named(
        &watermark_read(topic, partition),
        high_within(consumer, topic, partition, READ_EVERY),
    )
}

/// The high watermark of one watermark call, after a metadata request for
/// `topic`, the two together waiting at most `timeout`. The metadata answer
/// is not read: any failure comes from the watermark call.
fn high_within<C, K>(
    consumer: &K,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> KafkaResult<i64>
where
    C: ConsumerContext,
    K: Consumer<C>,
{
    let deadline = Instant::now() + timeout;
    // librdkafka answers a partition its cache holds with no leader from that
    // cache, without asking the broker again.
    let _ = consumer.fetch_metadata(Some(topic), timeout);
    consumer
        .fetch_watermarks(topic, partition, request_bound(deadline, Instant::now()))
        .map(|(_, high)| high)
}

/// What a watermark read of `partition` of `topic` is called in its error.
fn watermark_read(topic: &str, partition: i32) -> String {
    format!("a watermark read of {topic}/{partition}")
}

/// `answer`, with a failure named as `what`'s.
fn named<T>(what: &str, answer: KafkaResult<T>) -> Result<T, String> {
    answer.map_err(|e| format!("{what}: {e}"))
}

/// Partitions `0..partitions` of `topic`, with no offset.
fn partition_list(topic: &str, partitions: i32) -> TopicPartitionList {
    let mut list = TopicPartitionList::new();
    for partition in 0..partitions {
        list.add_partition(topic, partition);
    }
    list
}

/// Each element's offset, in the list's order.
fn offsets(list: &TopicPartitionList) -> Vec<Offset> {
    list.elements().iter().map(|e| e.offset()).collect()
}

/// Calls `read` until it answers, or `bound` passes. Each call is handed the
/// longest it may wait: 10 s, or the time left if that is shorter.
fn read_again<T>(
    what: &str,
    bound: Duration,
    mut read: impl FnMut(Duration) -> KafkaResult<T>,
) -> Result<T, String> {
    let deadline = Instant::now() + bound;
    let mut answer = None;
    crate::started::wait(
        what,
        bound,
        || {
            answer =
                Some(read(request_bound(deadline, Instant::now())).map_err(|e| e.to_string())?);
            Ok(())
        },
        || None,
    )?;
    answer.ok_or_else(|| format!("{what} returned no answer"))
}

/// How long a request sent at `now` may wait before `deadline`.
fn request_bound(deadline: Instant, now: Instant) -> Duration {
    READ_EVERY.min(deadline.saturating_duration_since(now))
}

/// Creates `topics` on `bootstrap` and waits until its metadata lists each
/// one with every partition. `bound` limits the request and the wait
/// together. A metadata read under way when it passes finishes first, which
/// can take up to 5 s more.
///
/// A broker can report a topic it has just created as missing for a while,
/// so a test that reads or writes it straight away waits here first.
///
/// # Errors
///
/// Fails when the request fails, when the broker refuses a topic, naming it
/// and the error, and when `bound` passes before every topic is listed, with
/// the last metadata read's error or what it listed.
pub fn create_topics(
    bootstrap: &str,
    topics: &[NewTopic<'_>],
    bound: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + bound;
    create(bootstrap, topics, bound)?;
    wait_listed(bootstrap, topics, deadline)
}

/// Sends the request that creates `topics`, bounded by `bound`.
fn create(bootstrap: &str, topics: &[NewTopic<'_>], bound: Duration) -> Result<(), String> {
    let admin: AdminClient<DefaultClientContext> = client(bootstrap, &[])?;
    let options = AdminOptions::new().request_timeout(Some(bound));
    let results = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|e| format!("runtime for the topic request: {e}"))?
        .block_on(admin.create_topics(topics, &options))
        .map_err(|e| format!("create topics: {e}"))?;
    for result in results {
        result.map_err(|(topic, code)| format!("create topic {topic}: {code}"))?;
    }
    Ok(())
}

/// Waits until `bootstrap`'s metadata lists each of `topics` with every
/// partition, or `deadline` passes.
fn wait_listed(bootstrap: &str, topics: &[NewTopic<'_>], deadline: Instant) -> Result<(), String> {
    let probe = metadata_probe(bootstrap)?;
    let wanted: Vec<(&str, usize)> = topics
        .iter()
        .map(|t| (t.name, usize::try_from(t.num_partitions).unwrap_or(0)))
        .collect();
    let names = wanted.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    crate::started::wait(
        &format!("topics {}", names.join(", ")),
        deadline.saturating_duration_since(Instant::now()),
        || listed(read(&probe), &wanted),
        || None,
    )
}

/// One topic as a metadata read lists it.
#[derive(Debug)]
struct Listed {
    name: String,
    error: Option<String>,
    partitions: usize,
}

/// Every topic one metadata read lists.
fn read(probe: &BaseConsumer) -> Result<Vec<Listed>, String> {
    let metadata = probe
        .fetch_metadata(None, METADATA_TIMEOUT)
        .map_err(|e| e.to_string())?;
    Ok(metadata
        .topics()
        .iter()
        .map(|t| Listed {
            name: t.name().to_owned(),
            error: t.error().map(|e| RDKafkaErrorCode::from(e).to_string()),
            partitions: t.partitions().len(),
        })
        .collect())
}

/// Whether `read` lists each `(name, partitions)` of `wanted` with no error
/// and at least that many partitions. A failed read lists nothing yet.
fn listed(read: Result<Vec<Listed>, String>, wanted: &[(&str, usize)]) -> Result<(), String> {
    let topics = read.map_err(metadata_failure)?;
    for &(name, partitions) in wanted {
        let t = usable(&topics, name)?;
        if t.partitions < partitions {
            return Err(format!(
                "metadata lists {} of {name}'s {partitions} partitions",
                t.partitions
            ));
        }
    }
    Ok(())
}

/// `name` as `topics` lists it, when it is listed with no error.
fn usable<'a>(topics: &'a [Listed], name: &str) -> Result<&'a Listed, String> {
    match topics.iter().find(|t| t.name == name) {
        None => Err(format!("metadata does not list {name}")),
        Some(Listed { error: Some(e), .. }) => Err(format!("metadata lists {name} with {e}")),
        Some(t) => Ok(t),
    }
}

/// A failed metadata read, as a listing reports it.
fn metadata_failure(e: String) -> String {
    format!("metadata read: {e}")
}

/// How many partitions one metadata read of at most 5 s through `probe`
/// lists for `topic`, for a caller whose own wait reads again. `None` when
/// the read does not list `topic`, or lists it with an error.
///
/// # Errors
///
/// Fails with the read's error.
pub fn topic_partitions_once(probe: &BaseConsumer, topic: &str) -> Result<Option<usize>, String> {
    let topics = read(probe).map_err(metadata_failure)?;
    Ok(usable(&topics, topic).ok().map(|t| t.partitions))
}

/// A producer on `bootstrap` that gives up on a record after 30 s.
///
/// # Errors
///
/// Fails when librdkafka refuses the settings.
pub fn producer(bootstrap: &str) -> Result<BaseProducer, String> {
    client(bootstrap, PRODUCER)
}

/// A [`producer`] on `bootstrap` initialised for transactions under `id`, with
/// a 600 s transaction timeout. `bound` limits the initialisation.
///
/// # Errors
///
/// Fails when librdkafka refuses the settings, and when initialisation fails
/// or does not finish within `bound`.
pub fn transactional_producer(
    bootstrap: &str,
    id: &str,
    bound: Duration,
) -> Result<BaseProducer, String> {
    let producer: BaseProducer = client(bootstrap, &transactional_settings(id))?;
    producer
        .init_transactions(bound)
        .map_err(|e| format!("initialise transactions for {id}: {e}"))?;
    Ok(producer)
}

/// A transactional producer's settings under `id`.
fn transactional_settings(id: &str) -> Vec<(&str, &str)> {
    let mut settings = [PRODUCER, TRANSACTIONAL].concat();
    settings.push(("transactional.id", id));
    settings
}

/// Hands `record` to `producer`. While the producer's queue is full, it
/// serves delivery reports, which free the queue, and asks again until
/// `deadline`. A test that sends many records passes one deadline for all of
/// them.
///
/// # Errors
///
/// Fails, naming the topic, on any other refusal at once, and with the
/// queue-full error once `deadline` has passed.
pub fn send<C, K, P>(
    producer: &BaseProducer<C>,
    record: BaseRecord<'_, K, P>,
    deadline: Instant,
) -> Result<(), String>
where
    C: ProducerContext<DeliveryOpaque = ()>,
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    send_until(producer, record, || Instant::now() >= deadline)
}

/// [`send`], asking `expired` after each full-queue refusal whether to stop.
fn send_until<C, K, P>(
    producer: &BaseProducer<C>,
    mut record: BaseRecord<'_, K, P>,
    mut expired: impl FnMut() -> bool,
) -> Result<(), String>
where
    C: ProducerContext<DeliveryOpaque = ()>,
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    loop {
        match producer.send(record) {
            Ok(()) => return Ok(()),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back))
                if !expired() =>
            {
                producer.poll(DRAIN_EVERY);
                record = back;
            }
            Err((e, back)) => return Err(format!("send to {}: {e}", back.topic)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, PoisonError};

    use rdkafka::ClientContext;
    use rdkafka::consumer::CommitMode;
    use rdkafka::message::{DeliveryResult, Message as _};
    use rdkafka::producer::DefaultProducerContext;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};

    use super::*;

    /// Bounds a hang only.
    const HANG: Duration = Duration::from_secs(60);

    /// An address nothing listens on.
    const NOWHERE: &str = "127.0.0.1:1";

    #[expect(
        clippy::disallowed_methods,
        reason = "a test producer with its own context, outside spate-kafka's factory"
    )]
    fn one_slot<C: ProducerContext>(settings: &[(&str, &str)], context: C) -> BaseProducer<C> {
        let mut all = vec![("queue.buffering.max.messages", "1")];
        all.extend_from_slice(settings);
        config(NOWHERE, &all).create_with_context(context).unwrap()
    }

    /// Records the payload of each delivery report the producer serves.
    #[derive(Default)]
    struct Reports(Mutex<Vec<Vec<u8>>>);

    impl ClientContext for Reports {}

    impl ProducerContext for Reports {
        type DeliveryOpaque = ();

        fn delivery(&self, report: &DeliveryResult<'_>, (): ()) {
            let (Ok(message) | Err((_, message))) = report;
            // The producer's drop serves reports while a failed test unwinds,
            // where a second panic aborts the run.
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(message.payload().unwrap_or_default().to_vec());
        }
    }

    /// Pins that a client built from `config` leaves OpenSSL's exit handler
    /// unregistered. Every client this module builds starts there.
    #[cfg(feature = "kafka-tls")]
    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "the client is built straight from `config`, as each builder here does"
    )]
    fn a_client_leaves_openssl_running_through_process_exit() {
        crate::assert_openssl_runs_through_exit(
            "kafka_client::tests::a_client_leaves_openssl_running_through_process_exit",
            || {
                let client: BaseProducer = config(NOWHERE, &[]).create().expect("producer");
                drop(client);
            },
        );
    }

    #[test]
    fn a_refusal_other_than_a_full_queue_returns_without_asking_the_deadline() {
        let producer: BaseProducer = client(NOWHERE, &[("message.max.bytes", "1000")]).unwrap();
        let payload = vec![0_u8; 5000];
        let refused = send_until(
            &producer,
            BaseRecord::<(), _>::to("t").payload(&payload),
            || panic!("the deadline was asked on a refusal other than a full queue"),
        )
        .unwrap_err();
        assert!(refused.starts_with("send to t: "), "{refused}");
        assert!(refused.contains("MessageSizeTooLarge"), "{refused}");
    }

    #[test]
    fn a_full_queue_past_the_deadline_returns_its_error() {
        let producer = one_slot(&[], DefaultProducerContext);
        send(
            &producer,
            BaseRecord::<(), _>::to("t").payload("a"),
            Instant::now() + HANG,
        )
        .unwrap();
        let refused = send(
            &producer,
            BaseRecord::<(), _>::to("t").payload("b"),
            Instant::now(),
        )
        .unwrap_err();
        assert!(refused.contains("QueueFull"), "{refused}");
    }

    #[test]
    fn a_full_queue_is_sent_to_once_a_delivery_report_frees_it() {
        // The first record fails within about a second, and serving its
        // report frees the one slot.
        let producer = one_slot(&[("message.timeout.ms", "1")], Reports::default());
        send(
            &producer,
            BaseRecord::<(), _>::to("t").payload("a"),
            Instant::now() + HANG,
        )
        .unwrap();
        let hang = Instant::now() + HANG;
        send_until(&producer, BaseRecord::<(), _>::to("t").payload("b"), || {
            Instant::now() >= hang
        })
        .unwrap();
        let reports = producer.context().0.lock().unwrap().clone();
        assert_eq!(reports, [b"a"], "the first record's report freed the slot");
        let refused = send(
            &producer,
            BaseRecord::<(), _>::to("t").payload("c"),
            Instant::now(),
        )
        .unwrap_err();
        assert!(
            refused.contains("QueueFull"),
            "the second record holds the slot: {refused}"
        );
    }

    fn topic(name: &str, error: Option<&str>, partitions: usize) -> Listed {
        Listed {
            name: name.to_owned(),
            error: error.map(str::to_owned),
            partitions,
        }
    }

    #[test]
    fn a_topic_is_listed_once_metadata_holds_every_partition_with_no_error() {
        let wanted = [("a", 2), ("b", 1)];
        let cases = [
            (Err("timed out".to_owned()), Err("metadata read: timed out")),
            (
                Ok(vec![topic("a", None, 2)]),
                Err("metadata does not list b"),
            ),
            (
                Ok(vec![
                    topic("a", None, 2),
                    topic("b", Some("UnknownTopic"), 0),
                ]),
                Err("metadata lists b with UnknownTopic"),
            ),
            (
                Ok(vec![topic("a", None, 1), topic("b", None, 1)]),
                Err("metadata lists 1 of a's 2 partitions"),
            ),
            (Ok(vec![topic("a", None, 2), topic("b", None, 1)]), Ok(())),
        ];
        for (read, want) in cases {
            assert_eq!(listed(read, &wanted), want.map_err(str::to_owned));
        }
    }

    #[test]
    fn a_wait_past_its_deadline_names_the_topics() {
        let topics = [NewTopic::new(
            "a",
            1,
            rdkafka::admin::TopicReplication::Fixed(1),
        )];
        let failed = wait_listed(NOWHERE, &topics, Instant::now()).unwrap_err();
        assert!(
            failed.starts_with("topics a was not ready within"),
            "{failed}"
        );
        assert!(failed.contains("metadata read: "), "{failed}");
    }

    #[test]
    fn a_transactional_producer_keeps_every_producer_setting() {
        let settings = transactional_settings("tx");
        for setting in [
            ("message.timeout.ms", "30000"),
            ("transaction.timeout.ms", "600000"),
            ("transactional.id", "tx"),
        ] {
            assert!(settings.contains(&setting), "{setting:?} in {settings:?}");
        }
    }

    #[test]
    fn a_metadata_read_cannot_create_a_topic() {
        assert!(METADATA.contains(&("allow.auto.create.topics", "false")));
    }

    #[test]
    fn a_transactional_producer_that_cannot_initialise_returns_the_error() {
        let failed = transactional_producer(NOWHERE, "tx", Duration::from_secs(1))
            .map(drop)
            .unwrap_err();
        assert!(
            failed.starts_with("initialise transactions for tx: "),
            "{failed}"
        );
    }

    /// An answer a broker gives while a group's coordinator moves.
    fn refused() -> KafkaError {
        KafkaError::OffsetFetch(RDKafkaErrorCode::NotCoordinator)
    }

    #[test]
    fn a_read_refused_once_is_read_again() {
        let mut reads = 0;
        let answer = read_again("a read", HANG, |_| {
            reads += 1;
            if reads == 1 { Err(refused()) } else { Ok(7) }
        });
        assert_eq!(answer, Ok(7));
        assert_eq!(reads, 2);
    }

    #[test]
    fn a_read_past_its_bound_names_what_and_the_last_error() {
        let failed =
            read_again::<()>("a read of t", Duration::ZERO, |_| Err(refused())).unwrap_err();
        assert!(
            failed.starts_with("a read of t was not ready within 0ns: "),
            "{failed}"
        );
        assert!(failed.ends_with(&refused().to_string()), "{failed}");
    }

    #[test]
    fn each_request_waits_at_most_ten_seconds_or_the_time_left() {
        let now = Instant::now();
        assert_eq!(request_bound(now + HANG, now), READ_EVERY);
        assert_eq!(
            request_bound(now + Duration::from_secs(3), now),
            Duration::from_secs(3)
        );
        assert_eq!(
            request_bound(now, now + Duration::from_secs(1)),
            Duration::ZERO
        );
        let mut handed = None;
        read_again("a read", HANG, |timeout| {
            handed = Some(timeout);
            Ok(())
        })
        .unwrap();
        assert_eq!(handed, Some(READ_EVERY));
    }

    #[test]
    fn a_request_waits_no_longer_than_the_bound() {
        let bound = Duration::from_secs(3);
        let mut handed = None;
        read_again("a read", bound, |timeout| {
            handed = Some(timeout);
            Ok(())
        })
        .unwrap();
        let handed = handed.unwrap();
        assert!(handed > Duration::ZERO && handed <= bound, "{handed:?}");
    }

    #[test]
    fn the_list_names_each_partition_of_the_topic() {
        let list = partition_list("t", 3);
        let named: Vec<(String, i32)> = list
            .elements()
            .iter()
            .map(|e| (e.topic().to_owned(), e.partition()))
            .collect();
        assert_eq!(
            named,
            [
                ("t".to_owned(), 0),
                ("t".to_owned(), 1),
                ("t".to_owned(), 2)
            ]
        );
    }

    #[test]
    fn no_commit_reads_as_invalid() {
        let mut list = partition_list("t", 1);
        list.add_partition_offset("t", 1, Offset::Offset(5))
            .unwrap();
        assert_eq!(offsets(&list), [Offset::Invalid, Offset::Offset(5)]);
    }

    /// A client that holds a mock cluster of one broker, with `topic` of
    /// `partitions` partitions on it, and the cluster's bootstrap address.
    fn mock_with(topic: &str, partitions: i32) -> (BaseProducer, String) {
        let holder: BaseProducer = client("", &[("test.mock.num.brokers", "1")]).unwrap();
        let brokers = {
            let cluster = holder.client().mock_cluster().unwrap();
            cluster.create_topic(topic, partitions, 1).unwrap();
            cluster.bootstrap_servers()
        };
        (holder, brokers)
    }

    #[test]
    fn a_committed_read_refused_once_returns_the_refusal_then_the_offset() {
        let (holder, brokers) = mock_with("units", 1);
        let reader = consumer(&brokers, &[("group.id", "once")]).unwrap();
        let mut at = TopicPartitionList::new();
        at.add_partition_offset("units", 0, Offset::Offset(3))
            .unwrap();
        reader.commit(&at, CommitMode::Sync).unwrap();
        // librdkafka answers a refused OffsetFetch with its error, unretried.
        holder.client().mock_cluster().unwrap().request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED],
        );
        let refused = committed_offsets_once(&reader, "units", 1).unwrap_err();
        assert!(
            refused.starts_with("a committed-offset read of units: "),
            "{refused}"
        );
        assert!(refused.contains("GroupAuthorizationFailed"), "{refused}");
        assert_eq!(
            committed_offsets_once(&reader, "units", 1),
            Ok(vec![Offset::Offset(3)])
        );
    }

    #[test]
    fn a_read_again_answers_past_a_refusal_with_the_offset_and_the_high_mark() {
        let (holder, brokers) = mock_with("units", 1);
        let producer = producer(&brokers).unwrap();
        for payload in ["one", "two"] {
            send(
                &producer,
                BaseRecord::<(), _>::to("units")
                    .partition(0)
                    .payload(payload),
                Instant::now() + HANG,
            )
            .unwrap();
        }
        producer.flush(HANG).unwrap();
        let reader = consumer(&brokers, &[("group.id", "again")]).unwrap();
        let mut at = TopicPartitionList::new();
        at.add_partition_offset("units", 0, Offset::Offset(3))
            .unwrap();
        reader.commit(&at, CommitMode::Sync).unwrap();
        holder.client().mock_cluster().unwrap().request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED],
        );
        assert_eq!(
            committed_offsets(&reader, "units", 1, HANG),
            Ok(vec![Offset::Offset(3)])
        );
        assert_eq!(high_watermark(&reader, "units", 0, HANG), Ok(2));
    }

    #[test]
    fn a_read_again_answers_once_a_leaderless_partition_has_a_leader() {
        let (holder, brokers) = mock_with("units", 1);
        let cluster = holder.client().mock_cluster().unwrap();
        cluster.partition_leader("units", 0, None).unwrap();
        let reader = consumer(&brokers, &[]).unwrap();
        let leaderless = high_watermark_once(&reader, "units", 0).unwrap_err();
        assert!(leaderless.contains("UnknownPartition"), "{leaderless}");
        cluster.partition_leader("units", 0, Some(1)).unwrap();
        assert_eq!(high_watermark(&reader, "units", 0, HANG), Ok(0));
    }

    #[test]
    fn a_watermark_read_returns_the_high_mark_or_the_error() {
        let (_holder, brokers) = mock_with("units", 1);
        let producer = producer(&brokers).unwrap();
        send(
            &producer,
            BaseRecord::<(), _>::to("units").partition(0).payload("one"),
            Instant::now() + HANG,
        )
        .unwrap();
        producer.flush(HANG).unwrap();
        let reader = consumer(&brokers, &[]).unwrap();
        assert_eq!(high_watermark_once(&reader, "units", 0), Ok(1));
        let missing = high_watermark_once(&reader, "units", 5).unwrap_err();
        assert!(
            missing.starts_with("a watermark read of units/5: "),
            "{missing}"
        );
    }

    #[test]
    fn a_topic_is_counted_by_its_partitions_once_listed() {
        let (_holder, brokers) = mock_with("units", 2);
        let probe = consumer(&brokers, &[]).unwrap();
        assert_eq!(topic_partitions_once(&probe, "units"), Ok(Some(2)));
        assert_eq!(topic_partitions_once(&probe, "never-created"), Ok(None));
    }
}
