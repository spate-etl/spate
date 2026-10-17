//! The attempt record: how many times a unit was read, kept on a compacted
//! attempt topic, and the follower thread that reads that topic into
//! memory.
//!
//! # Identity
//!
//! A count is keyed by the publish identity of the unit's message,
//! `<topic>/<partition>/<offset>` of the work-topic message a publish wrote.
//! Kafka topic names cannot contain `/`, so the string parses back
//! unambiguously. A retry copy carries its identity in the `spate.origin`
//! header. A work-topic message always counts under its own coordinates, so
//! each publish starts its own count.
//!
//! # Record
//!
//! An attempt record is keyed by the identity and lives on partition
//! `(murmur2(identity) & 0x7fffffff) % partitions` of the attempt topic, as
//! Kafka's Java default partitioner places it. Its value is the JSON object
//! `{"v":1,"attempts":n}`, holding the absolute count, so a write sent again
//! after an unknown outcome writes the same value. A null value is a
//! tombstone: the unit has no attempts recorded. A reader refuses a value at
//! any other `v`, with a field it does not know, or that is not that object.

use super::config::KafkaFeedConfig;
use super::consumer::{POLL_TIMEOUT, poll_class};
use super::{Core, KafkaFeedError, LatchOnPanic, Topics};
use crate::context::SourceContext;
use crate::error::redacted_client_error;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use spate_core::error::ErrorClass;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

/// The header that carries a retry copy's publish identity.
pub(crate) const ORIGIN_HEADER: &str = "spate.origin";

/// The record version this release writes, and the only one it reads.
const VERSION: u32 = 1;

/// The publish identity a unit's attempts are counted under:
/// `<topic>/<partition>/<offset>` of the work-topic message a publish wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishId(String);

impl PublishId {
    fn new(topic: &str, partition: i32, offset: i64) -> PublishId {
        PublishId(format!("{topic}/{partition}/{offset}"))
    }

    /// The identity a message read from `topic` counts under. A work-topic
    /// message uses its own coordinates whatever its headers say. Any other
    /// message uses its last `spate.origin` header, or its own coordinates,
    /// with a WARN, when that header is missing or not an identity.
    pub(crate) fn of_message(
        topic: &str,
        partition: i32,
        offset: i64,
        headers: &[(String, Option<Vec<u8>>)],
        topics: &Topics,
    ) -> PublishId {
        let own = PublishId::new(topic, partition, offset);
        if topic == &*topics.work {
            return own;
        }
        let origin = headers
            .iter()
            .rev()
            .find(|(name, _)| name == ORIGIN_HEADER)
            .and_then(|(_, value)| value.as_deref());
        if let Some(id) = origin.and_then(PublishId::parse) {
            return id;
        }
        tracing::warn!(
            topic,
            partition,
            offset,
            "a retried message carries no readable `spate.origin` header; its attempts are \
             counted from its own position"
        );
        own
    }

    /// `origin` as an identity, written in canonical form, or `None` when
    /// it is not `<topic>/<partition>/<offset>` with a non-empty topic and
    /// a partition and offset of zero or more.
    fn parse(origin: &[u8]) -> Option<PublishId> {
        let text = std::str::from_utf8(origin).ok()?;
        let mut parts = text.split('/');
        let (topic, partition, offset) = (parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() || topic.is_empty() {
            return None;
        }
        let partition: i32 = partition.parse().ok().filter(|p| *p >= 0)?;
        let offset: i64 = offset.parse().ok().filter(|o| *o >= 0)?;
        Some(PublishId::new(topic, partition, offset))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::fmt::Display for PublishId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Kafka's murmur2 hash, as its Java client and librdkafka compute it.
pub(crate) fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    // Truncated to 32 bits, as both reference implementations take it.
    let mut h = SEED ^ data.len() as u32;
    let (words, tail) = data.as_chunks::<4>();
    for word in words {
        let mut k = u32::from_le_bytes(*word);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    if tail.len() == 3 {
        h ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail[1]) << 8;
    }
    if let Some(first) = tail.first() {
        h ^= u32::from(*first);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

/// The attempt-topic partition of `identity`, out of `partitions`: Kafka's
/// Java default partitioner, and librdkafka's `murmur2` partitioner.
pub(crate) fn partition_of(identity: &[u8], partitions: NonZeroU32) -> i32 {
    let partition = (murmur2(identity) & 0x7fff_ffff) % partitions.get();
    i32::try_from(partition).expect("a value under 2^31 fits an i32")
}

/// What one attempt record says about its identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Entry {
    /// The unit has been read this many times.
    Count(u32),
    /// A tombstone: the unit has no attempts recorded.
    Removed,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    v: u32,
    attempts: u32,
}

/// The version field alone, read first so that a record of another version
/// is refused by its version whatever its other fields.
#[derive(Deserialize)]
struct Version {
    v: u64,
}

/// The value of an attempt record holding `attempts`.
pub(crate) fn encode(attempts: u32) -> Vec<u8> {
    serde_json::to_vec(&Record {
        v: VERSION,
        attempts,
    })
    .expect("a record of two integers serializes")
}

/// An attempt record this release cannot read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "the attempt record at offset {offset} of `{topic}` partition {partition} cannot be read \
     ({reason}); a newer release or another producer wrote it"
)]
pub(crate) struct RecordError {
    topic: String,
    partition: i32,
    offset: i64,
    reason: String,
}

/// Decode the value of the attempt record at `offset` of `topic`'s
/// `partition`. A null value is a tombstone.
///
/// # Errors
///
/// A value that is not a version-1 record with exactly its known fields.
pub(crate) fn decode(
    topic: &str,
    partition: i32,
    offset: i64,
    value: Option<&[u8]>,
) -> Result<Entry, RecordError> {
    let refuse = |reason: String| RecordError {
        topic: topic.to_owned(),
        partition,
        offset,
        reason,
    };
    let Some(value) = value else {
        return Ok(Entry::Removed);
    };
    let version: Version =
        serde_json::from_slice(value).map_err(|e| refuse(format!("not an attempt record: {e}")))?;
    if version.v != u64::from(VERSION) {
        return Err(refuse(format!("version {}", version.v)));
    }
    let record: Record =
        serde_json::from_slice(value).map_err(|e| refuse(format!("not an attempt record: {e}")))?;
    Ok(Entry::Count(record.attempts))
}

/// The identity and entry of the attempt record at `offset` of `topic`'s
/// `partition`.
///
/// # Errors
///
/// A record with no key, or one [`decode`] refuses.
fn record_of<'a>(
    topic: &str,
    partition: i32,
    offset: i64,
    identity: Option<&'a [u8]>,
    value: Option<&[u8]>,
) -> Result<(&'a [u8], Entry), RecordError> {
    let Some(identity) = identity else {
        return Err(RecordError {
            topic: topic.to_owned(),
            partition,
            offset,
            reason: "no key".to_owned(),
        });
    };
    Ok((identity, decode(topic, partition, offset, value)?))
}

/// The attempt counts the follower has read, and how far it has read each
/// attempt partition.
#[derive(Debug)]
pub(crate) struct Ledger {
    /// Per partition, one past the last offset read.
    next: Vec<Option<i64>>,
    counts: HashMap<Box<[u8]>, u32>,
}

/// The ledger and the condvar the follower notifies after each record.
pub(crate) type SharedLedger = Arc<(Mutex<Ledger>, Condvar)>;

/// Lock the ledger, whatever a panicking holder left: each `apply` leaves
/// it whole.
pub(crate) fn lock(ledger: &SharedLedger) -> MutexGuard<'_, Ledger> {
    ledger.0.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Ledger {
    pub(crate) fn new(partitions: NonZeroU32) -> Ledger {
        Ledger {
            next: vec![None; partitions.get() as usize],
            counts: HashMap::new(),
        }
    }

    /// Take in the record at `offset` of `partition`.
    pub(crate) fn apply(&mut self, partition: i32, offset: i64, identity: &[u8], entry: Entry) {
        if let Some(next) = usize::try_from(partition)
            .ok()
            .and_then(|p| self.next.get_mut(p))
        {
            *next = Some(offset + 1);
        }
        match entry {
            Entry::Count(n) => {
                self.counts.insert(identity.into(), n);
            }
            Entry::Removed => {
                self.counts.remove(identity);
            }
        }
    }

    /// The attempts recorded for `identity`, 0 when none are.
    pub(crate) fn count(&self, identity: &[u8]) -> u32 {
        self.counts.get(identity).copied().unwrap_or(0)
    }

    /// Whether the ledger holds every record of `partition` below `hwm`,
    /// given the partition's watermarks `(lwm, hwm)`. Partition EOF carries
    /// no offset, so the offsets read are the only measure.
    pub(crate) fn caught_up(&self, partition: i32, lwm: i64, hwm: i64) -> bool {
        let next = usize::try_from(partition)
            .ok()
            .and_then(|p| self.next.get(p).copied())
            .flatten();
        hwm <= lwm || next >= Some(hwm)
    }
}

/// An assign-only consumer in the feed's group that reads every attempt
/// partition from its start into the ledger.
pub(crate) struct Follower {
    core: Arc<Core>,
    consumer: BaseConsumer<SourceContext>,
    ledger: SharedLedger,
}

impl Follower {
    /// A follower of the `partitions` partitions of `topic`, assigned at
    /// their start.
    ///
    /// # Errors
    ///
    /// [`KafkaFeedError::Refused`] when librdkafka refuses the client
    /// configuration or the assignment.
    pub(crate) fn new(
        config: &KafkaFeedConfig,
        core: Arc<Core>,
        topic: &str,
        partitions: NonZeroU32,
    ) -> Result<Follower, KafkaFeedError> {
        let openssl_env = crate::security::openssl_env_overrides();
        let consumer = BaseConsumer::from_config_and_context(
            &config.follower_config(openssl_env),
            SourceContext::default(),
        )
        .map_err(|e| {
            KafkaFeedError::Refused(format!(
                "creating the attempt follower: {}",
                redacted_client_error(&e)
            ))
        })?;
        let mut tpl = TopicPartitionList::new();
        // Kafka numbers partitions with an i32, so no topic has more.
        for partition in (0..=i32::MAX).take(partitions.get() as usize) {
            tpl.add_partition_offset(topic, partition, Offset::Beginning)
                .map_err(|e| KafkaFeedError::Refused(format!("assigning `{topic}`: {e}")))?;
        }
        consumer
            .assign(&tpl)
            .map_err(|e| KafkaFeedError::Refused(format!("assigning `{topic}`: {e}")))?;
        Ok(Follower {
            core,
            consumer,
            ledger: Arc::new((Mutex::new(Ledger::new(partitions)), Condvar::new())),
        })
    }

    pub(crate) fn ledger(&self) -> SharedLedger {
        Arc::clone(&self.ledger)
    }

    /// Start the follower on a thread of its own, which runs until the feed
    /// stops or latches.
    ///
    /// # Errors
    ///
    /// When the thread cannot be started.
    pub(crate) fn spawn(self) -> std::io::Result<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("spate-kafka-feed-follower".to_owned())
            .spawn(move || self.run())
    }

    fn run(self) {
        let _guard = LatchOnPanic {
            core: Arc::clone(&self.core),
            thread: "follower",
        };
        while !self.core.stopped() && !self.core.latched() {
            match self.consumer.poll(POLL_TIMEOUT) {
                None => {}
                Some(Ok(message)) => {
                    if let Err(e) = self.on_message(&message) {
                        // Latched with no ledger guard held.
                        self.core.latch(&e.to_string());
                    }
                }
                Some(Err(error)) => self.on_error(&error),
            }
        }
    }

    fn on_message(&self, message: &BorrowedMessage<'_>) -> Result<(), RecordError> {
        let (partition, offset) = (message.partition(), message.offset());
        let (identity, entry) = record_of(
            message.topic(),
            partition,
            offset,
            message.key(),
            message.payload(),
        )?;
        lock(&self.ledger).apply(partition, offset, identity, entry);
        self.ledger.1.notify_all();
        Ok(())
    }

    fn on_error(&self, error: &KafkaError) {
        let code = error.rdkafka_error_code();
        let text = code.and_then(|code| self.consumer.context().take_error(code));
        let reason = match &text {
            Some(text) => format!("the attempt follower's poll: {error}: {text}"),
            None => format!("the attempt follower's poll: {error}"),
        };
        match poll_class(error, true, text.as_deref()) {
            ErrorClass::Fatal => {
                self.core.latch(&reason);
            }
            _ => tracing::warn!(error = %reason, "the attempt follower's poll failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics() -> Topics {
        Topics {
            work: Arc::from("units"),
            retry: Arc::from("units-retry"),
            dead_letter: Arc::from("units-dead"),
        }
    }

    fn origin(value: &[u8]) -> (String, Option<Vec<u8>>) {
        (ORIGIN_HEADER.to_owned(), Some(value.to_vec()))
    }

    /// Pins the hash against librdkafka's Java-compatible vectors
    /// (`rdmurmur2.c`), and the partition against the mask and the modulo:
    /// an unmasked hash puts `kafka` on 0, and `abs` of the signed hash puts
    /// `giberish123456789` on 0.
    #[test]
    fn murmur2_matches_kafkas_vectors() {
        let unaligned = b"PreAmbleWillBeRemoved,ThePrePartThatIs";
        for (data, hash) in [
            (&b"kafka"[..], 0xd067_cf64_u32),
            (b"giberish123456789", 0x8f55_2b0c),
            (b"1234", 0x9fc9_7b14),
            (b"234", 0xe7c0_09ca),
            (b"34", 0x8739_30da),
            (b"4", 0x5a4b_5ca1),
            (unaligned, 0x7842_4f1c),
            (&unaligned[1..], 0x4a62_b377),
            (&unaligned[2..], 0xe0e4_e09e),
            (&unaligned[3..], 0x62b8_b43f),
            (b"", 0x106e_08d9),
        ] {
            assert_eq!(murmur2(data), hash, "{}", String::from_utf8_lossy(data));
        }
        let three = NonZeroU32::new(3).unwrap();
        assert_eq!(partition_of(b"kafka", three), 1);
        assert_eq!(partition_of(b"giberish123456789", three), 2);
        assert_eq!(partition_of(b"units/0/0", NonZeroU32::new(2).unwrap()), 1);
        assert_eq!(partition_of(b"kafka", NonZeroU32::MIN), 0);
    }

    /// Pins the version-1 bytes: the literal value reads as its count, a
    /// null value as a tombstone, and `encode` writes that literal.
    #[test]
    fn an_attempt_record_at_version_1_reads_as_its_count() {
        let literal = br#"{"v":1,"attempts":3}"#;
        assert_eq!(decode("a", 0, 0, Some(literal)), Ok(Entry::Count(3)));
        assert_eq!(decode("a", 0, 0, None), Ok(Entry::Removed));
        assert_eq!(encode(3), literal);
        assert_eq!(decode("a", 0, 0, Some(&encode(7))), Ok(Entry::Count(7)));
        // Member order carries no meaning.
        assert_eq!(
            decode("a", 0, 0, Some(br#"{"attempts":4,"v":1}"#)),
            Ok(Entry::Count(4))
        );
    }

    /// Pins the refusals: another version, an unknown field, a value that
    /// is not JSON, a negative or missing count and a keyless record are
    /// each an error naming the record's topic, partition and offset.
    #[test]
    fn an_attempt_record_this_release_cannot_read_is_refused() {
        for (value, needle) in [
            (&br#"{"v":2,"attempts":3}"#[..], "version 2"),
            (br#"{"v":0,"attempts":3}"#, "version 0"),
            (br#"{"v":2,"attempts":3,"next":1}"#, "version 2"),
            (br#"{"v":1,"attempts":3,"next":1}"#, "unknown field"),
            (br#"{"v":1,"attempts":-1}"#, "not an attempt record"),
            (br#"{"v":1}"#, "not an attempt record"),
            (br#"{"attempts":3}"#, "not an attempt record"),
            (b"3", "not an attempt record"),
            (b"\xff", "not an attempt record"),
            (b"[1,3]", "not an attempt record"),
        ] {
            let err = decode("units-attempts", 1, 42, Some(value))
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "{err}");
            assert!(
                err.contains("offset 42 of `units-attempts` partition 1"),
                "{err}"
            );
            assert!(
                err.contains("a newer release or another producer wrote it"),
                "{err}"
            );
        }
        let err = record_of("units-attempts", 0, 5, None, Some(&encode(1)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("offset 5") && err.contains("no key"), "{err}");
        assert_eq!(
            record_of("t", 0, 5, Some(b"units/0/0"), Some(&encode(1))),
            Ok((&b"units/0/0"[..], Entry::Count(1)))
        );
    }

    /// Pins the catch-up rule at its boundaries: an empty partition is
    /// caught up, and a partition with records is caught up once the
    /// ledger has read up to the high watermark.
    #[test]
    fn the_follower_is_caught_up_at_the_high_watermark() {
        let mut ledger = Ledger::new(NonZeroU32::new(2).unwrap());
        assert!(ledger.caught_up(0, 0, 0));
        assert!(!ledger.caught_up(0, 0, 1));
        assert!(ledger.caught_up(0, 5, 5));
        ledger.apply(0, 3, b"a", Entry::Count(1));
        assert!(!ledger.caught_up(0, 0, 5));
        ledger.apply(0, 4, b"a", Entry::Count(2));
        assert!(ledger.caught_up(0, 0, 5));
        // Each partition is measured alone.
        assert!(!ledger.caught_up(1, 0, 5));
        assert!(!ledger.caught_up(2, 0, 5));
    }

    /// Pins that a tombstone removes the count, and that the read position
    /// follows every record, tombstones included.
    #[test]
    fn a_tombstone_removes_the_count() {
        let mut ledger = Ledger::new(NonZeroU32::MIN);
        ledger.apply(0, 0, b"a", Entry::Count(1));
        assert_eq!((ledger.count(b"a"), ledger.next[0]), (1, Some(1)));
        ledger.apply(0, 1, b"a", Entry::Count(2));
        ledger.apply(0, 2, b"b", Entry::Count(5));
        assert_eq!((ledger.count(b"a"), ledger.next[0]), (2, Some(3)));
        ledger.apply(0, 3, b"a", Entry::Removed);
        assert_eq!((ledger.count(b"a"), ledger.next[0]), (0, Some(4)));
        assert_eq!(ledger.count(b"b"), 5);
    }

    /// Pins which identity a message counts under: a work message its own
    /// coordinates whatever its headers, a retry copy its last
    /// `spate.origin` header, and a retry copy with no readable header its
    /// own coordinates.
    #[test]
    fn a_work_message_counts_under_its_own_coordinates_and_a_retry_copy_under_its_origin() {
        let topics = topics();
        let id = |topic: &str, headers: &[(String, Option<Vec<u8>>)]| {
            PublishId::of_message(topic, 2, 9, headers, &topics).to_string()
        };
        assert_eq!(
            PublishId::of_message("units", 2, 9, &[], &topics).as_bytes(),
            b"units/2/9"
        );
        assert_eq!(id("units", &[origin(b"units/0/1")]), "units/2/9");
        assert_eq!(id("units-retry", &[origin(b"units/0/1")]), "units/0/1");
        assert_eq!(id("units-retry", &[origin(b"units/0/0")]), "units/0/0");
        assert_eq!(
            id(
                "units-retry",
                &[
                    origin(b"units/0/1"),
                    ("other".to_owned(), Some(b"units/5/5".to_vec())),
                    origin(b"units/3/4"),
                ]
            ),
            "units/3/4"
        );
        assert_eq!(
            id(
                "units-retry",
                &[
                    origin(b"units/0/1"),
                    ("other".to_owned(), Some(b"units/5/5".to_vec())),
                ]
            ),
            "units/0/1"
        );
        // Parsed, so the identity is written in one form.
        assert_eq!(id("units-retry", &[origin(b"units/01/+4")]), "units/1/4");
        for malformed in [
            &b""[..],
            b"units",
            b"units/0",
            b"units/0/1/2",
            b"/0/1",
            b"units/x/1",
            b"units/0/x",
            b"units/-1/1",
            b"units/0/-1",
            b"\xff/0/1",
        ] {
            assert_eq!(
                id("units-retry", &[origin(malformed)]),
                "units-retry/2/9",
                "{}",
                String::from_utf8_lossy(malformed)
            );
        }
        assert_eq!(id("units-retry", &[]), "units-retry/2/9");
        assert_eq!(
            id(
                "units-retry",
                &[origin(b"units/0/1"), (ORIGIN_HEADER.to_owned(), None)]
            ),
            "units-retry/2/9"
        );
    }

    fn config() -> KafkaFeedConfig {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(
            "kafka:\n  brokers: 127.0.0.1:1\n  group_id: workers\n  topic: units\n  \
             retry_topic: units-retry\n  dead_letter_topic: units-dead\n",
        )
        .unwrap();
        KafkaFeedConfig::from_section(&section).unwrap()
    }

    fn follower(config: &KafkaFeedConfig) -> Result<Follower, KafkaFeedError> {
        let core = Arc::new(Core::new(config));
        Follower::new(config, core, "units-attempts", NonZeroU32::MIN)
    }

    /// Pins that a follower librdkafka will not build is refused, and that
    /// one in the feed's group assigns with no broker reachable.
    #[test]
    fn a_follower_librdkafka_will_not_build_is_refused() {
        assert!(follower(&config()).is_ok());

        let mut invalid = config();
        invalid
            .rdkafka
            .insert("session.timeout.ms".to_owned(), "soon".to_owned());
        let err = follower(&invalid).err().unwrap().to_string();
        assert!(err.contains("creating the attempt follower"), "{err}");
    }

    /// Pins the follower's poll errors: a Fatal one stops the feed, and a
    /// transient one does not.
    #[test]
    fn only_a_fatal_poll_error_stops_the_feed() {
        use rdkafka::error::RDKafkaErrorCode as C;
        let Ok(follower) = follower(&config()) else {
            panic!("the follower is refused");
        };
        follower.on_error(&KafkaError::MessageConsumption(C::BrokerTransportFailure));
        assert!(!follower.core.latched());
        follower.on_error(&KafkaError::MessageConsumption(C::TopicAuthorizationFailed));
        let reason = follower.core.lock().fatal.clone().unwrap().reason;
        assert!(reason.contains("the attempt follower's poll"), "{reason}");
    }

    /// Pins that `UnknownTopicOrPartition` from a poll latches the feed.
    #[test]
    fn an_unknown_attempt_topic_latches_the_feed() {
        use rdkafka::error::RDKafkaErrorCode as C;
        let Ok(follower) = follower(&config()) else {
            panic!("the follower is refused");
        };
        follower.on_error(&KafkaError::MessageConsumption(C::UnknownTopicOrPartition));
        assert!(follower.core.latched());
    }

    /// Pins that a TLS rejection reported through the error callback
    /// latches the feed.
    #[test]
    fn a_tls_rejection_latches_the_feed() {
        use rdkafka::ClientContext;
        use rdkafka::error::RDKafkaErrorCode as C;
        let Ok(follower) = follower(&config()) else {
            panic!("the follower is refused");
        };
        follower.consumer.context().error(
            KafkaError::MessageConsumption(C::SSL),
            "ssl handshake failed: certificate verify failed",
        );
        follower.on_error(&KafkaError::MessageConsumption(C::SSL));
        assert!(follower.core.latched());
    }

    /// Pins that `lock` hands out the ledger after a holder panicked.
    #[test]
    fn a_poisoned_ledger_still_locks() {
        let ledger: SharedLedger =
            Arc::new((Mutex::new(Ledger::new(NonZeroU32::MIN)), Condvar::new()));
        let held = Arc::clone(&ledger);
        let _ = std::thread::spawn(move || {
            let _held = held.0.lock().unwrap();
            panic!("poisoning the ledger");
        })
        .join();
        lock(&ledger).apply(0, 0, b"a", Entry::Count(1));
        assert_eq!(lock(&ledger).count(b"a"), 1);
    }

    mod cluster {
        //! The follower against rdkafka's in-process MockCluster.

        use super::super::*;
        use rdkafka::ClientConfig;
        use rdkafka::mocking::MockCluster;
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        use spate_core::config::FeedSection;
        use spate_test::wait_until;
        use std::sync::atomic::Ordering;
        use std::thread::JoinHandle;
        use std::time::Duration;

        const DEADLINE: Duration = Duration::from_secs(30);
        const ATTEMPTS: &str = "units-attempts";
        const TWO: NonZeroU32 = NonZeroU32::new(2).expect("non-zero");

        struct Rig {
            producer: BaseProducer,
            config: KafkaFeedConfig,
            // Declared last, so dropped after the producer that uses it.
            cluster: MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
        }

        fn rig() -> Rig {
            let cluster = MockCluster::new(1).expect("mock cluster");
            for topic in ["units", "units-retry", "units-dead"] {
                cluster.create_topic(topic, 1, 1).unwrap();
            }
            cluster.create_topic(ATTEMPTS, 2, 1).unwrap();
            let brokers = cluster.bootstrap_servers();
            let producer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("acks", "all")
                .create()
                .unwrap();
            let section: FeedSection = serde_yaml::from_str(&format!(
                "kafka:\n  brokers: \"{brokers}\"\n  group_id: {}\n  topic: units\n  \
                 retry_topic: units-retry\n  dead_letter_topic: units-dead\n",
                spate_test::unique_name("follower")
            ))
            .unwrap();
            let config = KafkaFeedConfig::from_section(&section).unwrap();
            Rig {
                producer,
                config,
                cluster,
            }
        }

        impl Rig {
            /// Write `value` for `identity` on its attempt partition, and
            /// wait for the write's report.
            fn write(&self, identity: &[u8], value: Option<&[u8]>) {
                let mut record = BaseRecord::<[u8], [u8]>::to(ATTEMPTS)
                    .partition(partition_of(identity, TWO))
                    .key(identity);
                if let Some(value) = value {
                    record = record.payload(value);
                }
                self.producer.send(record).map_err(|(e, _)| e).unwrap();
                self.producer.flush(Duration::from_secs(10)).unwrap();
            }

            /// Wait until `ledger` holds every record of both partitions.
            fn caught_up(&self, ledger: &SharedLedger) {
                wait_until(DEADLINE, "the follower reaches the high watermarks", || {
                    (0..2).all(|p| {
                        let (lwm, hwm) = self
                            .producer
                            .client()
                            .fetch_watermarks(ATTEMPTS, p, Duration::from_secs(10))
                            .unwrap();
                        lock(ledger).caught_up(p, lwm, hwm)
                    })
                });
            }
        }

        /// A started follower, stopped and joined on drop, also when the
        /// test fails: a cluster dropped while a client is still connected
        /// waits on it.
        struct Running {
            core: Arc<Core>,
            thread: Option<JoinHandle<()>>,
        }

        impl Running {
            fn start(rig: &Rig) -> (Running, SharedLedger) {
                let core = Arc::new(Core::new(&rig.config));
                let follower =
                    Follower::new(&rig.config, Arc::clone(&core), ATTEMPTS, TWO).unwrap();
                let ledger = follower.ledger();
                let thread = Some(follower.spawn().unwrap());
                (Running { core, thread }, ledger)
            }
        }

        impl Drop for Running {
            fn drop(&mut self) {
                self.core.stop.store(true, Ordering::Release);
                if let Some(thread) = self.thread.take() {
                    let _ = thread.join();
                }
            }
        }

        /// Pins that the follower, assigned in the feed's group, reads the
        /// records written before it started and those written after, a
        /// tombstone included, into the counts.
        #[test]
        fn the_follower_reads_the_attempt_topic_from_its_start() {
            let rig = rig();
            // `units/0/0` hashes to partition 1 of 2, `units/0/2` to 0.
            let (a, b) = (&b"units/0/0"[..], &b"units/0/2"[..]);
            rig.write(a, Some(&encode(1)));
            rig.write(a, Some(&encode(2)));
            rig.write(b, Some(&encode(1)));

            let (running, ledger) = Running::start(&rig);
            rig.caught_up(&ledger);
            let counts = {
                let ledger = lock(&ledger);
                (ledger.count(a), ledger.count(b))
            };
            assert_eq!(counts, (2, 1));

            rig.write(b, None);
            rig.write(a, Some(&encode(3)));
            rig.caught_up(&ledger);
            let counts = {
                let ledger = lock(&ledger);
                (ledger.count(a), ledger.count(b))
            };
            assert_eq!(counts, (3, 0));
            let core = Arc::clone(&running.core);
            drop(running);
            assert!(!core.latched(), "{:?}", core.lock().fatal);
        }

        /// Pins that a Fatal error the follower's poll returns stops the
        /// feed.
        #[test]
        fn a_fatal_error_reading_the_attempt_topic_stops_the_feed() {
            let rig = rig();
            rig.cluster.request_errors(
                rdkafka::types::RDKafkaApiKey::Fetch,
                &[rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED; 4],
            );
            let (running, _) = Running::start(&rig);
            wait_until(DEADLINE, "the feed latches", || running.core.latched());
            let reason = running.core.lock().fatal.clone().unwrap().reason;
            assert!(reason.contains("the attempt follower's poll"), "{reason}");
        }

        /// Pins that a record the follower cannot read stops the feed with
        /// an error naming the record's offset.
        #[test]
        fn an_unreadable_attempt_record_stops_the_feed() {
            let rig = rig();
            let a = b"units/0/0";
            rig.write(a, Some(&encode(1)));
            rig.write(a, Some(br#"{"v":9}"#));

            let (running, _) = Running::start(&rig);
            wait_until(DEADLINE, "the feed latches", || running.core.latched());
            let reason = running.core.lock().fatal.clone().unwrap().reason;
            assert!(
                reason.contains("offset 1 of `units-attempts` partition 1"),
                "{reason}"
            );
            assert!(reason.contains("version 9"), "{reason}");
        }

        /// Pins that the follower's thread ends once the feed latches while
        /// the feed is not stopped.
        #[test]
        fn the_follower_exits_once_the_feed_latches() {
            let rig = rig();
            let (running, _) = Running::start(&rig);
            running.core.latch("probe");
            let thread = running.thread.as_ref().unwrap();
            wait_until(DEADLINE, "the follower exits once the feed latches", || {
                thread.is_finished()
            });
        }
    }
}
