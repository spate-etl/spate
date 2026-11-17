//! What a test does to a broker through a Kafka client: create topics and
//! wait until the broker lists them, and produce records.

use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _};

/// How long one metadata request waits on a cluster that may be electing.
pub(crate) const METADATA_TIMEOUT: Duration = Duration::from_secs(5);

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
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", bootstrap);
    for &(key, value) in settings {
        config.set(key, value);
    }
    config.create().map_err(|e| format!("Kafka client: {e}"))
}

/// A client that reads `bootstrap`'s metadata.
pub(crate) fn metadata_probe(bootstrap: &str) -> Result<BaseConsumer, String> {
    client(bootstrap, METADATA)
}

/// Creates `topics` on `bootstrap` and waits until its metadata lists each
/// one with every partition, all within `bound`.
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
    let topics = read.map_err(|e| format!("metadata read: {e}"))?;
    for &(name, partitions) in wanted {
        match topics.iter().find(|t| t.name == name) {
            None => return Err(format!("metadata does not list {name}")),
            Some(Listed { error: Some(e), .. }) => {
                return Err(format!("metadata lists {name} with {e}"));
            }
            Some(t) if t.partitions < partitions => {
                return Err(format!(
                    "metadata lists {} of {name}'s {partitions} partitions",
                    t.partitions
                ));
            }
            Some(_) => {}
        }
    }
    Ok(())
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
pub fn send<K, P>(
    producer: &BaseProducer,
    record: BaseRecord<'_, K, P>,
    deadline: Instant,
) -> Result<(), String>
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    send_until(producer, record, || Instant::now() >= deadline)
}

/// [`send`], asking `expired` after each full-queue refusal whether to stop.
fn send_until<K, P>(
    producer: &BaseProducer,
    mut record: BaseRecord<'_, K, P>,
    mut expired: impl FnMut() -> bool,
) -> Result<(), String>
where
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
    use super::*;

    /// Bounds a hang only.
    const HANG: Duration = Duration::from_secs(60);

    /// An address nothing listens on.
    const NOWHERE: &str = "127.0.0.1:1";

    fn one_slot(settings: &[(&str, &str)]) -> BaseProducer {
        let mut all = vec![("queue.buffering.max.messages", "1")];
        all.extend_from_slice(settings);
        client(NOWHERE, &all).unwrap()
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
        let producer = one_slot(&[]);
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
        let producer = one_slot(&[("message.timeout.ms", "1")]);
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
        assert_eq!(producer.in_flight_count(), 1, "the second record is queued");
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
}
