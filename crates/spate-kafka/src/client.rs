//! The one constructor for every Kafka client this crate builds: the shared
//! librdkafka configuration, the passthrough denylist derived from it, and
//! the context every client runs under.
//!
//! A client's configuration is built in one order: the `rdkafka:`
//! passthrough, then the defaults it may replace, then the properties the
//! framework owns. An owned property wins over the same key string. Its
//! aliases and `topic.`-prefixed twins are refused at load instead, because
//! `ClientConfig` applies its keys in hash order and either spelling could
//! land last.
//!
//! Each role declares its owned properties once, through [`Keys`]. The same
//! declarations set the properties on the client and decide which
//! passthrough keys [`Base::denied`] refuses, so a role cannot set a property
//! without refusing it in the passthrough.

use crate::error::redacted_client_error;
use rdkafka::client::{Client, ClientContext};
use rdkafka::config::{FromClientConfigAndContext, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer as _, ConsumerContext};
use rdkafka::error::{IsError as _, KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::TopicPartitionListElem;
use rdkafka::types::RDKafkaRespErr;
use rdkafka::util::IntoOpaque;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use spate_core::config::{Denylist, DurationBounds, ReservedKeyError, check_duration, redact};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

/// The longest the committed-offset read before an assignment may take.
pub(crate) const COMMITTED_LOOKUP_MAX: Duration = Duration::from_secs(10);

/// librdkafka's default `max.poll.interval.ms`.
const DEFAULT_MAX_POLL_INTERVAL: Duration = Duration::from_secs(300);

/// The bound on the committed-offset read made before each assignment, for
/// a consumer configured by `cc`: a quarter of its `max.poll.interval.ms`,
/// at most 10 s. librdkafka's poll-interval clock keeps running during the
/// read, and a member that outlasts the interval leaves the group.
///
/// The interval is read back from librdkafka, which takes spellings a plain
/// parse refuses, such as `8000ms`.
pub(crate) fn committed_lookup_timeout(cc: &ClientConfig) -> Duration {
    let max_poll = cc
        .create_native_config()
        .ok()
        .and_then(|native| native.get("max.poll.interval.ms").ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(DEFAULT_MAX_POLL_INTERVAL, Duration::from_millis);
    (max_poll / 4).min(COMMITTED_LOOKUP_MAX)
}

/// Global librdkafka properties whose names start with `topic.`. librdkafka
/// looks a name up as a global property before it strips `topic.`, so these
/// keep their prefix.
const GLOBAL_TOPIC_PROPERTIES: [&str; 6] = [
    "topic.metadata.refresh.interval.ms",
    "topic.metadata.refresh.fast.interval.ms",
    "topic.metadata.refresh.fast.cnt",
    "topic.metadata.refresh.sparse",
    "topic.metadata.propagation.max.ms",
    "topic.blacklist",
];

/// librdkafka's aliases of the properties a role owns, each with the name
/// librdkafka writes it to.
const ALIASES: [(&str, &str); 4] = [
    ("bootstrap.servers", "metadata.broker.list"),
    ("acks", "request.required.acks"),
    ("delivery.timeout.ms", "message.timeout.ms"),
    ("compression.type", "compression.codec"),
];

/// The longest topic name librdkafka takes. It refuses a longer one with a NULL
/// handle, which rdkafka's `fetch_metadata` unwraps, panicking the thread.
const MAX_TOPIC_NAME_BYTES: usize = 512;

/// Checks `topic` against librdkafka's length limit.
///
/// # Errors
///
/// The reason, without the key, for the caller to anchor at its topic key.
pub(crate) fn check_topic_name(topic: &str) -> Result<(), String> {
    if topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(format!("is longer than {MAX_TOPIC_NAME_BYTES} bytes"));
    }
    Ok(())
}

/// The librdkafka property `key` writes, under one spelling per property:
/// `topic.` stripped from a topic property and an alias replaced by its
/// target. Names match case-sensitively, as librdkafka's do.
pub(crate) fn canonical(key: &str) -> &str {
    if GLOBAL_TOPIC_PROPERTIES.contains(&key) {
        return key;
    }
    let name = key.strip_prefix("topic.").unwrap_or(key);
    // In topic scope `enable.auto.commit` is librdkafka's alias of the topic
    // property `auto.commit.enable`, distinct from the global one.
    if name != key && name == "enable.auto.commit" {
        return "auto.commit.enable";
    }
    ALIASES
        .iter()
        .find(|(alias, _)| *alias == name)
        .map_or(name, |(_, target)| target)
}

/// Whether `timeout` is in librdkafka's `message.timeout.ms` range of 1ms to
/// `i32::MAX` milliseconds.
///
/// librdkafka reads the timeout in whole milliseconds and reads `0` as no
/// timeout, so a positive value under a millisecond would remove the bound.
///
/// # Errors
///
/// The reason, without the key, for the caller to anchor at its timeout key.
pub(crate) fn check_message_timeout(timeout: Duration) -> Result<(), String> {
    const MESSAGE_TIMEOUT: DurationBounds = DurationBounds::at_least(Duration::from_millis(1))
        .at_most(Duration::from_millis(i32::MAX as u64));
    check_duration(timeout, MESSAGE_TIMEOUT).map_err(|e| {
        format!("{e}: librdkafka reads whole milliseconds up to i32::MAX and reads 0 as no timeout")
    })
}

/// One client's place in a connector: its `client.id` suffix, whether it
/// reports statistics, and the properties it owns.
pub(crate) trait Role {
    /// The role's name in the default `client.id`, such as `consumer`.
    fn name(&self) -> &'static str;

    /// Whether the client emits statistics at the section's
    /// `statistics_interval`.
    fn statistics(&self) -> bool;

    /// The client's index among identical clients of one section, appended to
    /// its `client.id` as `-<k>`. `None` for a role built once.
    fn instance(&self) -> Option<usize> {
        None
    }

    /// librdkafka properties this role sets unless the passthrough sets them.
    fn defaults(&self) -> &'static [(&'static str, &'static str)] {
        &[]
    }

    /// Declare the properties this role owns beyond the base.
    fn declare(&self, keys: &mut Keys<'_>);
}

/// The recorder a [`Role`] declares its owned properties through. Building a
/// client sets each property; validating a passthrough records only which
/// keys are owned.
pub(crate) struct Keys<'c> {
    config: Option<&'c mut ClientConfig>,
    owned: Vec<(&'static str, &'static str)>,
}

impl Keys<'_> {
    /// Own `key`: set it to `value` after the passthrough, and refuse it,
    /// with `why`, in the passthrough.
    pub(crate) fn own(&mut self, key: &'static str, value: &str, why: &'static str) {
        self.own_if(true, key, || value.to_owned(), why);
    }

    /// Own `key` whether or not `set` holds, and set it only when it does.
    pub(crate) fn own_if(
        &mut self,
        set: bool,
        key: &'static str,
        value: impl FnOnce() -> String,
        why: &'static str,
    ) {
        self.owned.push((key, why));
        if set && let Some(cc) = self.config.as_deref_mut() {
            cc.set(key, value());
        }
    }

    /// Refuse `key` in the passthrough without setting it.
    pub(crate) fn forbid(&mut self, key: &'static str, why: &'static str) {
        self.owned.push((key, why));
    }

    /// Set `key` without owning it, from a typed field or as a rewrite of a
    /// passthrough value. The passthrough may still name it, and the section
    /// checks that conflict itself.
    pub(crate) fn set(&mut self, key: &str, value: &str) {
        if let Some(cc) = self.config.as_deref_mut() {
            cc.set(key, value);
        }
    }
}

/// librdkafka's `statistics.interval.ms` range, with `0s` for off.
const STATISTICS_INTERVAL: DurationBounds = DurationBounds::at_least(Duration::from_millis(1))
    .at_most(Duration::from_hours(24))
    .or_zero();

/// The key that selects Kafka in a `source:`, `sink:` or `feed:` section.
pub(crate) const TAG: &str = "kafka";

/// What every client of one connector section shares: its brokers, its
/// passthrough and its statistics interval.
pub(crate) struct Base<'a> {
    /// The section's path in the pipeline configuration, such as
    /// `source.kafka`.
    pub(crate) section: &'static str,
    pub(crate) brokers: &'a str,
    pub(crate) rdkafka: &'a BTreeMap<String, String>,
    pub(crate) statistics_interval: Duration,
    /// Whether `SSL_CERT_FILE` or `SSL_CERT_DIR` is set, read once by the
    /// caller. On Linux, cargo exports both to every process it runs, so a
    /// test passes the value instead of reading it.
    pub(crate) openssl_env: bool,
}

// Hand-written: the `rdkafka` map carries credentials. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl fmt::Debug for Base<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Base {
            section,
            brokers,
            rdkafka,
            statistics_interval,
            openssl_env,
        } = self;
        f.debug_struct("Base")
            .field("section", section)
            .field("brokers", brokers)
            .field("rdkafka", &redact::map(rdkafka.keys()))
            .field("statistics_interval", statistics_interval)
            .field("openssl_env", openssl_env)
            .finish()
    }
}

impl Base<'_> {
    fn declare(&self, keys: &mut Keys<'_>, role: &dyn Role) {
        keys.own(
            "bootstrap.servers",
            self.brokers,
            "owned by the typed `brokers` field",
        );
        keys.own(
            "allow.auto.create.topics",
            "false",
            "forced off: a write to, or a metadata read of, a missing topic \
             must fail, and never create the topic with the broker's defaults",
        );
        keys.own_if(
            role.statistics() && !self.statistics_interval.is_zero(),
            "statistics.interval.ms",
            || self.statistics_interval.as_millis().to_string(),
            "owned by the typed `statistics_interval` field",
        );
        role.declare(keys);
    }

    /// The librdkafka configuration of `role`'s client.
    pub(crate) fn config(&self, role: &dyn Role) -> ClientConfig {
        let mut cc = new_config();
        // rdkafka reads the log facade by default; tracing filters these events.
        cc.set_log_level(RDKafkaLogLevel::Debug);
        // Before the passthrough, which replaces any of them it sets.
        for (key, value) in role.defaults() {
            cc.set(*key, *value);
        }
        for (k, v) in self.rdkafka {
            cc.set(k, v);
        }
        crate::security::apply_ca_default(&mut cc, self.rdkafka, self.openssl_env);
        // librdkafka reads an empty value as unset and sends `rdkafka`.
        let passed = self.rdkafka.get("client.id").filter(|v| !v.is_empty());
        let id = match (passed, role.instance()) {
            (Some(_), None) => None,
            (Some(id), Some(k)) => Some(format!("{id}-{k}")),
            (None, instance) => {
                let kind = self.section.split('.').next().unwrap_or(self.section);
                let id = format!("spate-{kind}-{}", role.name());
                Some(match instance {
                    Some(k) => format!("{id}-{k}"),
                    None => id,
                })
            }
        };
        if let Some(id) = id {
            cc.set("client.id", id);
        }
        self.declare(
            &mut Keys {
                config: Some(&mut cc),
                owned: Vec::new(),
            },
            role,
        );
        cc
    }

    /// The first passthrough key that any of `roles` owns, under any
    /// spelling.
    ///
    /// # Errors
    ///
    /// The [`ReservedKeyError`] naming that key and why the role owns it.
    pub(crate) fn denied(&self, roles: &[&dyn Role]) -> Result<(), ReservedKeyError> {
        let mut keys = Keys {
            config: None,
            owned: Vec::new(),
        };
        for role in roles {
            self.declare(&mut keys, *role);
        }
        Denylist::new(&keys.owned)
            .normalized_by(|key| Cow::Borrowed(canonical(key)), "librdkafka property")
            .check("rdkafka", self.rdkafka.keys())
    }

    /// Whether `statistics_interval` is `0s` or in librdkafka's range of 1ms
    /// to 24h.
    ///
    /// librdkafka reads the interval in whole milliseconds and reads `0` as
    /// off, so a positive value under a millisecond would silently disable
    /// statistics.
    ///
    /// # Errors
    ///
    /// The reason, without the key, for the caller to anchor at
    /// `statistics_interval`.
    pub(crate) fn check_statistics_interval(&self) -> Result<(), String> {
        check_duration(self.statistics_interval, STATISTICS_INTERVAL).map_err(|e| {
            format!(
                "{e}: 0s disables statistics, and librdkafka reads whole milliseconds up to 24h"
            )
        })
    }

    /// Build `role`'s client under `hooks`.
    ///
    /// # Errors
    ///
    /// librdkafka's refusal, rendered without any property value.
    pub(crate) fn create<T, H>(&self, role: &dyn Role, hooks: H) -> Result<T, String>
    where
        H: Hooks,
        T: FromClientConfigAndContext<SpateContext<H>>,
    {
        construct(&self.config(role), SpateContext(hooks, role.instance()))
            .map_err(|e| redacted_client_error(&e))
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "the one place a Kafka client configuration starts"
)]
fn new_config() -> ClientConfig {
    ClientConfig::new()
}

#[expect(
    clippy::disallowed_methods,
    reason = "the one place a Kafka client is built"
)]
fn construct<T, C>(config: &ClientConfig, context: C) -> Result<T, KafkaError>
where
    C: ClientContext,
    T: FromClientConfigAndContext<C>,
{
    // Ahead of the first OpenSSL call, this leaves OpenSSL's exit handler
    // unregistered: it frees state a broker thread uses in a handshake, which
    // kills a process that exits with a client alive.
    #[cfg(feature = "tls")]
    openssl_sys::init();
    T::from_config_and_context(config, context)
}

/// The callbacks a role adds to its client's context.
pub(crate) trait Hooks: Send + Sync + 'static {
    /// A statistics snapshot, on the client's polling thread.
    fn stats(&self, _statistics: Statistics) {}

    /// An error event from the client numbered `instance` among its role's
    /// clients, on the client's polling thread.
    fn error(&self, error: KafkaError, reason: &str, instance: Option<usize>) {
        log_error(&error, reason, instance);
    }
}

impl Hooks for () {}

/// The next offset the application reads on the one partition in `list`, or
/// [`Offset::Invalid`] until a served op sets it.
///
/// Takes the client, topic and partition locks and makes no broker call. It
/// reads through the list's `elems` pointer because `elements()` allocates.
pub(crate) fn read_position<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    list: &mut TopicPartitionList,
) -> Offset {
    debug_assert_eq!(list.count(), 1, "one partition per position read");
    // SAFETY: the client pointer is valid while `consumer` is borrowed, and
    // `list` holds the one element the read below dereferences, written only
    // under this exclusive borrow.
    unsafe {
        rdkafka::bindings::rd_kafka_position(consumer.client().native_ptr(), list.ptr());
        Offset::from_raw((*(*list.ptr()).elems).offset)
    }
}

/// The last offset no record holds when the position reads `position` and
/// everything below `covered_to` is accounted for, or `None` when nothing lies
/// between them. A logical position means no served op has set it, and with
/// nothing accounted for an offset of 0 has nothing below it.
pub(crate) fn offset_to_retire(position: Offset, covered_to: Option<i64>) -> Option<i64> {
    let Offset::Offset(position) = position else {
        return None;
    };
    (position > covered_to.unwrap_or(0)).then(|| position - 1)
}

/// Writes the offset each element of `tpl` names into that element, with one
/// ListOffsets request per leader: the log start for `Offset::Beginning`, the
/// end for `Offset::End`, as `isolation.level` reads it. Blocks for up to
/// `timeout`. A refusal is reported, where a client's own lookup retries it.
///
/// # Errors
///
/// When any partition's answer is a refusal, with nothing written to the list.
pub(crate) fn list_offsets<C: ClientContext>(
    client: &Client<C>,
    tpl: &mut TopicPartitionList,
    timeout: Duration,
) -> KafkaResult<()> {
    let timeout = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    // SAFETY: the client pointer is valid while `client` is borrowed, and `tpl`
    // owns its list across the call. `rd_kafka_offsets_for_times` writes each
    // result into the list and keeps neither pointer past the call.
    let code = unsafe {
        rdkafka::bindings::rd_kafka_offsets_for_times(client.native_ptr(), tpl.ptr(), timeout)
    };
    if code.is_error() {
        return Err(KafkaError::MetadataFetch(code.into()));
    }
    Ok(())
}

/// A commit sent by [`send_commit`], waiting for the group's answer on a queue of
/// its own. Dropping it abandons the wait, and a commit left unanswered may still
/// reach the broker, which checks its generation.
pub(crate) struct PendingCommit<C: ConsumerContext + 'static> {
    /// Keeps the client alive for the queue's whole life.
    _consumer: Arc<BaseConsumer<C>>,
    queue: *mut rdkafka::bindings::rd_kafka_queue_t,
}

impl<C: ConsumerContext + 'static> PendingCommit<C> {
    /// Waits up to `timeout` for the group's answer.
    ///
    /// # Errors
    ///
    /// The commit's failure, or `OperationTimedOut` when no answer came in time.
    pub(crate) fn wait(&self, timeout: Duration) -> KafkaResult<()> {
        let timeout = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        // SAFETY: `queue` is valid until `drop`. The event is owned here and
        // destroyed before the block ends.
        let code = unsafe {
            let event = rdkafka::bindings::rd_kafka_queue_poll(self.queue, timeout);
            if event.is_null() {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__TIMED_OUT
            } else {
                let code = rdkafka::bindings::rd_kafka_event_error(event);
                rdkafka::bindings::rd_kafka_event_destroy(event);
                code
            }
        };
        if code.is_error() {
            return Err(KafkaError::ConsumerCommit(code.into()));
        }
        Ok(())
    }
}

impl<C: ConsumerContext + 'static> Drop for PendingCommit<C> {
    fn drop(&mut self) {
        // SAFETY: `queue` came from `rd_kafka_queue_new` on `consumer`, which is
        // still alive, and nothing uses it after this. librdkafka drops a reply
        // that arrives once the queue is gone.
        unsafe { rdkafka::bindings::rd_kafka_queue_destroy(self.queue) };
    }
}

/// Sends a commit of exactly `tpl`, or with `None` of every offset stored for
/// the consumer's current assignment, without waiting for its answer, so several
/// clients' commits can be in flight together. `rd_kafka_commit` in its sync form
/// has no bound: a request queued to a broker that has stopped answering is
/// requeued for as long as the broker stays silent.
///
/// # Errors
///
/// When the commit cannot be queued.
pub(crate) fn send_commit<C: ConsumerContext + 'static>(
    consumer: &Arc<BaseConsumer<C>>,
    tpl: Option<&TopicPartitionList>,
) -> KafkaResult<PendingCommit<C>> {
    let offsets = tpl.map_or(std::ptr::null_mut(), TopicPartitionList::ptr);
    // SAFETY: the client pointer is valid while `consumer` is borrowed, and
    // `rd_kafka_commit_queue` copies `offsets`, which may be null. The queue is
    // owned by the returned value, or destroyed here when nothing was queued.
    unsafe {
        let rk = consumer.client().native_ptr();
        let queue = rdkafka::bindings::rd_kafka_queue_new(rk);
        let code = rdkafka::bindings::rd_kafka_commit_queue(
            rk,
            offsets,
            queue,
            None,
            std::ptr::null_mut(),
        );
        if code.is_error() {
            rdkafka::bindings::rd_kafka_queue_destroy(queue);
            return Err(KafkaError::ConsumerCommit(code.into()));
        }
        Ok(PendingCommit {
            _consumer: Arc::clone(consumer),
            queue,
        })
    }
}

/// The offset [`list_offsets`] wrote into `elem`.
///
/// # Errors
///
/// The element's own failure, or `OffsetNotAvailable` for anything that is not
/// a concrete offset.
pub(crate) fn listed_offset(elem: &TopicPartitionListElem<'_>) -> KafkaResult<i64> {
    elem.error()?;
    match elem.offset() {
        Offset::Offset(offset) => Ok(offset),
        _ => Err(KafkaError::MetadataFetch(
            RDKafkaErrorCode::OffsetNotAvailable,
        )),
    }
}

/// The generation id `consumer` holds, or `-1` when it holds none: it has not
/// joined, the group has told it its generation is gone, or it has no group. The
/// call blocks on the client's main thread, as `assign` does.
pub(crate) fn group_generation<C: ConsumerContext>(consumer: &BaseConsumer<C>) -> i32 {
    // SAFETY: the client pointer is valid while `consumer` is borrowed. The
    // metadata is owned here, is checked for NULL before it is read, and is
    // destroyed before the block ends.
    unsafe {
        let metadata =
            rdkafka::bindings::rd_kafka_consumer_group_metadata(consumer.client().native_ptr());
        if metadata.is_null() {
            return -1;
        }
        let generation =
            rdkafka::bindings::rd_kafka_consumer_group_metadata_generation_id(metadata);
        rdkafka::bindings::rd_kafka_consumer_group_metadata_destroy(metadata);
        generation
    }
}

/// Log a librdkafka error event under the `librdkafka` target, with the
/// client's `instance` as the `consumer` field when it has one.
pub(crate) fn log_error(error: &KafkaError, reason: &str, instance: Option<usize>) {
    tracing::warn!(target: "librdkafka", consumer = instance, %error, "{reason}");
}

/// A consumer role's rebalance callback.
pub(crate) trait ConsumerHooks: Hooks + Sized {
    /// librdkafka's rebalance event, run inside the poll that delivers it.
    fn rebalance(
        &self,
        consumer: &BaseConsumer<SpateContext<Self>>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    );
}

/// A producer role's delivery callback.
pub(crate) trait ProducerHooks: Hooks {
    type DeliveryOpaque: IntoOpaque;

    /// One message's delivery report, on the producer's poll thread.
    fn delivery(&self, result: &DeliveryResult<'_>, opaque: Self::DeliveryOpaque);
}

impl ProducerHooks for () {
    type DeliveryOpaque = ();

    fn delivery(&self, _result: &DeliveryResult<'_>, _opaque: ()) {}
}

/// The context every client runs under: librdkafka's log lines go to
/// `tracing` under the `librdkafka` target, and everything else goes to the
/// role's hooks. It carries the role's instance, which names the consumer on
/// each line it forwards.
pub(crate) struct SpateContext<H>(H, Option<usize>);

impl<H> Deref for SpateContext<H> {
    type Target = H;

    fn deref(&self) -> &H {
        &self.0
    }
}

impl<H: Hooks> ClientContext for SpateContext<H> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        use RDKafkaLogLevel as L;
        match level {
            L::Emerg | L::Alert | L::Critical | L::Error => {
                tracing::error!(target: "librdkafka", consumer = self.1, fac, "{log_message}");
            }
            L::Warning => {
                tracing::warn!(target: "librdkafka", consumer = self.1, fac, "{log_message}");
            }
            L::Notice | L::Info => {
                tracing::info!(target: "librdkafka", consumer = self.1, fac, "{log_message}");
            }
            L::Debug => {
                tracing::debug!(target: "librdkafka", consumer = self.1, fac, "{log_message}");
            }
        }
    }

    fn stats(&self, statistics: Statistics) {
        self.0.stats(statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.0.error(error, reason, self.1);
    }
}

impl<H: ConsumerHooks> ConsumerContext for SpateContext<H> {
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        self.0.rebalance(base_consumer, err, tpl);
    }
}

impl<H: ProducerHooks> ProducerContext for SpateContext<H> {
    type DeliveryOpaque = H::DeliveryOpaque;

    fn delivery(&self, result: &DeliveryResult<'_>, opaque: H::DeliveryOpaque) {
        self.0.delivery(result, opaque);
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// A client over exactly `properties`, outside every role, under
    /// `hooks`: for a test that needs a configuration no role builds.
    pub(crate) fn raw<T, H>(properties: &[(&str, &str)], hooks: H) -> T
    where
        H: Hooks,
        T: FromClientConfigAndContext<SpateContext<H>>,
    {
        let mut cc = new_config();
        for (key, value) in properties {
            cc.set(*key, *value);
        }
        construct(&cc, SpateContext(hooks, None)).expect("client")
    }

    /// A configuration a test builds itself, outside every role.
    pub(crate) fn raw_config() -> ClientConfig {
        new_config()
    }

    /// Builds a client from a [`raw_config`].
    pub(crate) trait Raw {
        fn build<T: rdkafka::config::FromClientConfig>(&self) -> Result<T, KafkaError>;

        fn build_with<C, T>(&self, context: C) -> Result<T, KafkaError>
        where
            C: ClientContext,
            T: FromClientConfigAndContext<C>;
    }

    impl Raw for ClientConfig {
        #[expect(
            clippy::disallowed_methods,
            reason = "a test client is built outside every role"
        )]
        fn build<T: rdkafka::config::FromClientConfig>(&self) -> Result<T, KafkaError> {
            self.create()
        }

        #[expect(
            clippy::disallowed_methods,
            reason = "a test client is built outside every role"
        )]
        fn build_with<C, T>(&self, context: C) -> Result<T, KafkaError>
        where
            C: ClientContext,
            T: FromClientConfigAndContext<C>,
        {
            self.create_with_context(context)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KafkaSourceConfig;
    use crate::client::testing::raw_config;
    use crate::config::Consumer;
    use crate::context::SourceHooks;
    use crate::feed::{FeedRole, KafkaFeedConfig};
    use crate::security::tests::assert_ca_cases;
    use crate::sink::{KafkaSinkConfig, SinkRole};
    use rdkafka::admin::AdminClient;
    use rdkafka::consumer::BaseConsumer;
    use rdkafka::producer::BaseProducer;
    use spate_core::config::ConfigError;

    /// The position rule: only a concrete position past what is covered
    /// retires offsets, and it retires through the offset below the position.
    /// Regression for #1709.
    #[test]
    fn the_position_rule_advances_only_past_what_is_covered() {
        for logical in [
            Offset::Invalid,
            Offset::Stored,
            Offset::End,
            Offset::Beginning,
            Offset::OffsetTail(1),
        ] {
            assert_eq!(offset_to_retire(logical, None), None, "{logical:?}");
            assert_eq!(offset_to_retire(logical, Some(5)), None, "{logical:?}");
        }
        assert_eq!(offset_to_retire(Offset::Offset(4), Some(5)), None);
        assert_eq!(offset_to_retire(Offset::Offset(5), Some(5)), None);
        assert_eq!(offset_to_retire(Offset::Offset(6), Some(5)), Some(5));
        assert_eq!(offset_to_retire(Offset::Offset(0), None), None);
        assert_eq!(offset_to_retire(Offset::Offset(1), None), Some(0));
    }

    /// A consumer with no group holds no group metadata, and reads no
    /// generation.
    #[test]
    fn a_consumer_with_no_group_reads_no_generation() {
        let consumer: BaseConsumer<crate::context::SourceContext> = testing::raw(
            &[("bootstrap.servers", "127.0.0.1:1")],
            SourceHooks::default(),
        );
        assert_eq!(group_generation(&consumer), -1);
    }

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    /// `message.timeout.ms` takes whole milliseconds from 1 to `i32::MAX`,
    /// and a refusal gives librdkafka's reason for the bound.
    #[test]
    fn check_message_timeout_takes_1ms_to_i32_max_milliseconds() {
        const DAY: u64 = 24 * 60 * 60;
        for timeout in [
            Duration::ZERO,
            Duration::from_micros(999),
            Duration::from_nanos(999_999),
            Duration::from_millis(i32::MAX as u64 + 1),
            Duration::from_millis(i32::MAX as u64) + Duration::from_nanos(1),
            Duration::from_secs(25 * DAY),
        ] {
            assert!(check_message_timeout(timeout).is_err(), "{timeout:?}");
        }
        assert_eq!(
            check_message_timeout(Duration::ZERO).unwrap_err(),
            "must be from 1ms to 24days 20h 31m 23s 647ms; found 0s: librdkafka reads whole \
             milliseconds up to i32::MAX and reads 0 as no timeout"
        );
        for timeout in [
            Duration::from_millis(1),
            Duration::from_micros(1000),
            Duration::from_secs(24 * DAY),
            Duration::from_millis(i32::MAX as u64),
        ] {
            check_message_timeout(timeout).unwrap_or_else(|why| panic!("{timeout:?}: {why}"));
        }
    }

    /// One config per section, each over `rdkafka`, unvalidated.
    struct Sections {
        source: KafkaSourceConfig,
        sink: KafkaSinkConfig,
        feed: KafkaFeedConfig,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Section {
        Source,
        Sink,
        Feed,
    }

    impl Sections {
        fn over(rdkafka: &BTreeMap<String, String>) -> Sections {
            let mut source = KafkaSourceConfig::new("localhost:9092", "orders", "spate");
            source.rdkafka = rdkafka.clone();
            let mut sink = KafkaSinkConfig::new("localhost:9092", "orders-out");
            sink.rdkafka = rdkafka.clone();
            let section: spate_core::config::FeedSection = serde_yaml::from_str(
                "kafka:\n  brokers: localhost:9092\n  group_id: workers\n  topic: units\n  \
                 retry_topic: units-retry\n  dead_letter_topic: units-dead\n  \
                 attempt_topic: units-attempts\n",
            )
            .unwrap();
            let mut feed = KafkaFeedConfig::from_section(&section).unwrap();
            feed.rdkafka = rdkafka.clone();
            Sections { source, sink, feed }
        }

        /// Every role, with its section and its base.
        fn roles(&self, openssl_env: bool) -> Vec<(Section, Base<'_>, Box<dyn Role + '_>)> {
            let mut roles: Vec<(Section, Base<'_>, Box<dyn Role + '_>)> = vec![
                (
                    Section::Source,
                    self.source.base(openssl_env),
                    Box::new(Consumer(&self.source, None)),
                ),
                (
                    Section::Sink,
                    self.sink.base(openssl_env),
                    Box::new(SinkRole {
                        config: &self.sink,
                        probe: false,
                    }),
                ),
                (
                    Section::Sink,
                    self.sink.base(openssl_env),
                    Box::new(SinkRole {
                        config: &self.sink,
                        probe: true,
                    }),
                ),
            ];
            for role in FeedRole::ALL {
                roles.push((
                    Section::Feed,
                    self.feed.base(openssl_env),
                    Box::new(self.feed.client(role)),
                ));
            }
            roles
        }

        /// Every role's configuration, labelled `<section>-<role>`.
        fn configs(&self, openssl_env: bool) -> Vec<(String, ClientConfig)> {
            self.roles(openssl_env)
                .into_iter()
                .map(|(_, base, role)| {
                    let label = format!("{}-{}", base.section, role.name());
                    (label, base.config(&*role))
                })
                .collect()
        }

        fn validate(&self, section: Section) -> Result<(), ConfigError> {
            match section {
                Section::Source => self.source.validate(),
                Section::Sink => self.sink.validate(),
                Section::Feed => self.feed.validate(),
            }
        }
    }

    /// Keys a role carries that the passthrough may still set: the CA
    /// default, the `client.id` default, each role's [`Role::defaults`], and
    /// the sink's typed compression, which is a conflict rule of its own.
    const NOT_OWNED: [&str; 4] = [
        "ssl.ca.location",
        "client.id",
        "fetch.queue.backoff.ms",
        "compression.codec",
    ];

    /// Whether `section`'s roles carry `key` with the passthrough free to set
    /// it: [`NOT_OWNED`], and the source's reset policy, which its consumer
    /// rewrites only from `latest`. A feed consumer owns the reset policy.
    fn not_owned(section: Section, key: &str) -> bool {
        NOT_OWNED.contains(&key) || (section == Section::Source && key == "auto.offset.reset")
    }

    /// Every spelling librdkafka accepts for `key`: itself, its
    /// `topic.`-prefixed form, and each alias of the same property.
    fn spellings(key: &str) -> Vec<String> {
        let mut out = vec![key.to_owned(), format!("topic.{key}")];
        for (alias, target) in ALIASES {
            for name in [alias, target] {
                if name != key && canonical(name) == canonical(key) {
                    out.push(name.to_owned());
                }
            }
        }
        out
    }

    /// For each pair, librdkafka writes the alias to the property `canonical`
    /// names: the value set under the alias reads back under the target, and
    /// the `topic.` form maps to the same name.
    #[test]
    fn canonical_names_match_librdkafka_aliases() {
        for (alias, target) in ALIASES {
            assert_eq!(canonical(alias), canonical(target), "{alias}");
            assert_eq!(canonical(alias), target, "{alias}");
            assert_eq!(
                canonical(&format!("topic.{alias}")),
                target,
                "topic.{alias}"
            );
            let value = match target {
                "metadata.broker.list" => "broker-a:9092",
                "request.required.acks" => "-1",
                "message.timeout.ms" => "12345",
                _ => "gzip",
            };
            let mut cc = raw_config();
            cc.set(alias, value);
            let native = cc.create_native_config().expect("native config");
            assert_eq!(
                native.get(target).as_deref(),
                Ok(value),
                "{alias} writes {target}"
            );
        }
        assert_eq!(
            canonical("topic.metadata.propagation.max.ms"),
            "topic.metadata.propagation.max.ms"
        );
        assert_eq!(canonical("topic.request.required.acks"), canonical("acks"));
    }

    /// `auto.commit.enable` is librdkafka's legacy topic property, distinct
    /// from the global `enable.auto.commit` the source owns, and
    /// `topic.enable.auto.commit` is the topic property's alias.
    #[test]
    fn auto_commit_enable_is_not_an_alias_of_enable_auto_commit() {
        for key in ["auto.commit.enable", "topic.enable.auto.commit"] {
            let mut cc = raw_config();
            cc.set(key, "false");
            let native = cc.create_native_config().expect("native config");
            assert_eq!(
                native.get("enable.auto.commit").as_deref(),
                Ok("true"),
                "{key}"
            );
            assert_eq!(canonical(key), "auto.commit.enable", "{key}");
        }
        assert_ne!(
            canonical("auto.commit.enable"),
            canonical("enable.auto.commit")
        );
    }

    /// Each key a role sets is refused in its section's passthrough under
    /// every spelling, so no role sets a property the passthrough could race.
    #[test]
    fn every_key_a_role_sets_is_refused_in_the_passthrough() {
        let empty = Sections::over(&BTreeMap::new());
        for (section, base, role) in empty.roles(false) {
            let cc = base.config(&*role);
            for key in cc.config_map().keys() {
                if not_owned(section, key) {
                    continue;
                }
                for spelling in spellings(key) {
                    let sections = Sections::over(&map(&[(&spelling, "x")]));
                    let err = sections
                        .validate(section)
                        .expect_err(&format!("{section:?} {} accepts {spelling}", role.name()))
                        .to_string();
                    assert!(
                        err.contains(&format!("rdkafka.\"{spelling}\" cannot be overridden")),
                        "{err}"
                    );
                }
            }
        }
    }

    /// An owned key wins over the same key in an unvalidated passthrough, and
    /// a producer role's durability reaches librdkafka.
    #[test]
    fn owned_values_win_over_an_unvalidated_passthrough() {
        let empty = Sections::over(&BTreeMap::new());
        for (section, base, role) in empty.roles(false) {
            let expected = base.config(&*role);
            let wrong: BTreeMap<String, String> = expected
                .config_map()
                .keys()
                .filter(|k| !not_owned(section, k))
                .map(|k| ((*k).to_owned(), "wrong".to_owned()))
                .collect();
            let wronged = Sections::over(&wrong);
            let (_, wronged_base, wronged_role) = wronged
                .roles(false)
                .into_iter()
                .find(|(s, _, r)| *s == section && r.name() == role.name())
                .unwrap();
            let cc = wronged_base.config(&*wronged_role);
            for (key, value) in expected.config_map() {
                if !not_owned(section, key) {
                    assert_eq!(
                        cc.get(key),
                        Some(value),
                        "{section:?} {} {key}",
                        role.name()
                    );
                }
            }
            if matches!(role.name(), "producer" | "probe") {
                let native = cc.create_native_config().expect("native config");
                assert_eq!(native.get("request.required.acks").as_deref(), Ok("-1"));
                assert_eq!(native.get("enable.idempotence").as_deref(), Ok("true"));
            }
        }
    }

    /// The wrapper logs each librdkafka level under the `librdkafka` target
    /// with the facility, at the mapped level, and with the `consumer` field
    /// only when the client has an instance.
    #[test]
    fn the_wrapper_logs_every_level_to_the_librdkafka_target() {
        use RDKafkaLogLevel as L;
        for instance in [None, Some(1)] {
            let context = SpateContext((), instance);
            for (level, expected) in [
                (L::Emerg, "ERROR"),
                (L::Alert, "ERROR"),
                (L::Critical, "ERROR"),
                (L::Error, "ERROR"),
                (L::Warning, "WARN"),
                (L::Notice, "INFO"),
                (L::Info, "INFO"),
                (L::Debug, "DEBUG"),
            ] {
                let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
                    context.log(level, "FACIL", "the message");
                });
                assert_eq!(lines.len(), 1, "{level:?}: {lines:#?}");
                let line = &lines[0];
                assert!(line.contains(expected), "{level:?}: {line}");
                assert!(line.contains("librdkafka"), "{level:?}: {line}");
                assert!(line.contains("fac=\"FACIL\""), "{level:?}: {line}");
                assert!(line.contains("the message"), "{level:?}: {line}");
                assert_eq!(
                    line.contains("consumer=1"),
                    instance.is_some(),
                    "{instance:?} {level:?}: {line}"
                );
            }
        }
    }

    /// A role with no error hook of its own, such as the admin client, logs
    /// each error event as a warning under the `librdkafka` target.
    #[test]
    fn the_default_error_hook_logs_a_warning() {
        let context = SpateContext((), None);
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            context.error(
                KafkaError::Global(rdkafka::error::RDKafkaErrorCode::AllBrokersDown),
                "1/1 brokers are down",
            );
        });
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert!(lines[0].contains("WARN"), "{}", lines[0]);
        assert!(lines[0].contains("librdkafka"), "{}", lines[0]);
        assert!(lines[0].contains("1/1 brokers are down"), "{}", lines[0]);
    }

    /// A source running several consumers suffixes each one's `client.id`
    /// with its index, its own id or the passthrough's, and changes nothing
    /// else. One consumer keeps the unsuffixed id.
    #[test]
    fn consumer_client_ids_are_suffixed_above_one() {
        let configs = |count: usize, pairs: &[(&str, &str)]| -> Vec<ClientConfig> {
            let mut source = KafkaSourceConfig::new("localhost:9092", "orders", "spate");
            source.consumers = count;
            source.rdkafka = map(pairs);
            (0..count)
                .map(|k| source.base(false).config(&Consumer::member(&source, k)))
                .collect()
        };
        let ids = |configs: &[ClientConfig]| -> Vec<String> {
            configs
                .iter()
                .map(|cc| cc.get("client.id").unwrap_or_default().to_owned())
                .collect()
        };
        let tls = [
            ("security.protocol", "SASL_SSL"),
            ("ssl.ca.location", "/etc/ca.pem"),
        ];

        assert_eq!(ids(&configs(1, &tls)), ["spate-source-consumer"]);
        let three = configs(3, &tls);
        assert_eq!(
            ids(&three),
            [
                "spate-source-consumer-0",
                "spate-source-consumer-1",
                "spate-source-consumer-2"
            ]
        );
        let mut rest: Vec<_> = three
            .iter()
            .map(|cc| {
                let mut keys = cc.config_map();
                keys.remove("client.id");
                keys.into_iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect::<Vec<_>>()
            })
            .collect();
        rest.dedup();
        assert_eq!(rest.len(), 1, "only client.id differs: {rest:#?}");
        assert!(
            rest[0].contains(&("ssl.ca.location".to_owned(), "/etc/ca.pem".to_owned())),
            "{rest:#?}"
        );

        assert_eq!(ids(&configs(1, &[("client.id", "app")])), ["app"]);
        assert_eq!(
            ids(&configs(3, &[("client.id", "app")])),
            ["app-0", "app-1", "app-2"]
        );
    }

    /// The source's consumers back a full partition queue off for 10 ms
    /// unless the passthrough sets `fetch.queue.backoff.ms`, and no other
    /// client sets it.
    #[test]
    fn only_the_source_shortens_the_fetch_queue_backoff() {
        let defaults = Sections::over(&BTreeMap::new());
        for (section, base, role) in defaults.roles(false) {
            let expected = matches!(section, Section::Source).then_some("10");
            assert_eq!(
                base.config(&*role).get("fetch.queue.backoff.ms"),
                expected,
                "{} {}",
                base.section,
                role.name()
            );
        }
        let passed = Sections::over(&map(&[("fetch.queue.backoff.ms", "1000")]));
        passed
            .validate(Section::Source)
            .expect("the backoff is not owned");
        for (_, base, role) in passed.roles(false) {
            assert_eq!(
                base.config(&*role).get("fetch.queue.backoff.ms"),
                Some("1000"),
                "{} {}",
                base.section,
                role.name()
            );
        }
    }

    /// Each role sends its own `client.id` unless the passthrough names one,
    /// which then reaches every client unchanged.
    #[test]
    fn each_role_sends_its_client_id() {
        let ids: Vec<(String, Option<String>)> = Sections::over(&BTreeMap::new())
            .configs(false)
            .into_iter()
            .map(|(label, cc)| (label, cc.get("client.id").map(str::to_owned)))
            .collect();
        let expected = [
            "spate-source-consumer",
            "spate-sink-producer",
            "spate-sink-probe",
            "spate-feed-consumer",
            "spate-feed-producer",
            "spate-feed-depth",
            "spate-feed-admin",
            "spate-feed-reader",
            "spate-feed-follower",
        ];
        assert_eq!(
            ids.iter().map(|(_, id)| id.as_deref()).collect::<Vec<_>>(),
            expected.map(Some).to_vec(),
        );

        let quota = Sections::over(&map(&[("client.id", "quota-key")]));
        for (label, cc) in quota.configs(false) {
            assert_eq!(cc.get("client.id"), Some("quota-key"), "{label}");
        }
        for section in [Section::Source, Section::Sink, Section::Feed] {
            quota.validate(section).expect("client.id is not owned");
        }

        let oauth = Sections::over(&map(&[("sasl.oauthbearer.client.id", "svc")]));
        for (_, base, role) in oauth.roles(false) {
            assert!(base.denied(&[&*role]).is_ok(), "{}", role.name());
            assert_eq!(
                base.config(&*role).get("client.id"),
                Some(
                    format!(
                        "spate-{}-{}",
                        base.section.split('.').next().unwrap(),
                        role.name()
                    )
                    .as_str()
                )
            );
        }
    }

    /// An empty `client.id`, which librdkafka reads as unset, leaves the
    /// default in place.
    #[test]
    fn an_empty_client_id_keeps_the_spate_default() {
        let sections = Sections::over(&map(&[("client.id", "")]));
        for (label, cc) in sections.configs(false) {
            let native = cc.create_native_config().expect("native config");
            let expected = format!("spate-{}", label.replacen(".kafka-", "-", 1));
            assert_eq!(native.get("client.id"), Ok(expected), "{label}");
        }
    }

    /// Every role's configuration carries `probe` only in a vendored-OpenSSL
    /// `tls` build with no CA named and no OpenSSL environment override, and
    /// keeps a named CA as given. Regression for #825.
    #[test]
    fn every_role_reads_the_openssl_environment() {
        assert_ca_cases(
            "client::tests::every_role_reads_the_openssl_environment",
            |rdkafka| {
                let sections = Sections::over(&rdkafka);
                sections
                    .configs(crate::security::openssl_env_overrides())
                    .into_iter()
                    .map(|(label, cc)| (&*label.leak(), cc))
                    .collect()
            },
        );
    }

    /// A value librdkafka refuses names its key and never itself.
    #[test]
    fn every_role_withholds_a_rejected_value() {
        let sections = Sections::over(&map(&[("debug", "all,hunter2")]));
        for (_, base, role) in sections.roles(false) {
            let err = match role.name() {
                "consumer" | "depth" | "follower" => base
                    .create::<BaseConsumer<SpateContext<SourceHooks>>, _>(
                        &*role,
                        SourceHooks::default(),
                    )
                    .err(),
                "admin" => base
                    .create::<AdminClient<SpateContext<()>>, _>(&*role, ())
                    .err(),
                _ => base
                    .create::<BaseProducer<SpateContext<()>>, _>(&*role, ())
                    .err(),
            }
            .unwrap_or_else(|| panic!("{} accepted the value", role.name()));
            assert!(err.contains("debug"), "{err}");
            assert!(!err.contains("unter2"), "{err}");
        }
    }

    /// `Debug` on a base shows every passthrough key and none of the values.
    #[test]
    fn base_debug_never_prints_passthrough_values() {
        let rdkafka = map(&[
            ("ssl.key.pem", "PRIVATE-KEY"),
            (
                "sasl.oauthbearer.client.credentials.client.secret",
                "hunter2",
            ),
        ]);
        let base = Base {
            section: "source.kafka",
            brokers: "localhost:9092",
            rdkafka: &rdkafka,
            statistics_interval: Duration::from_secs(5),
            openssl_env: false,
        };
        let printed = format!("{base:?}");
        assert!(!printed.contains("PRIVATE-KEY"), "{printed}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(printed.contains("\"ssl.key.pem\": <redacted>"), "{printed}");
    }

    /// Pins that building a TLS client leaves OpenSSL's exit handler
    /// unregistered. A child process registers its own handler before any
    /// OpenSSL call, so that handler runs after OpenSSL's, and aborts if
    /// OpenSSL refuses to initialise because its cleanup has run. Regression
    /// for #1752.
    #[cfg(feature = "tls")]
    #[test]
    fn a_tls_client_leaves_openssl_running_through_process_exit() {
        use rdkafka::producer::{BaseProducer, DefaultProducerContext};
        use std::ffi::c_int;

        const NAME: &str =
            "client::tests::a_tls_client_leaves_openssl_running_through_process_exit";
        const CHILD: &str = "SPATE_TEST_OPENSSL_EXIT";

        unsafe extern "C" {
            fn atexit(handler: extern "C" fn()) -> c_int;
        }

        extern "C" fn abort_after_openssl_cleanup() {
            // SAFETY: a null settings pointer selects OpenSSL's defaults.
            if unsafe { openssl_sys::OPENSSL_init_ssl(0, std::ptr::null()) } != 1 {
                std::process::abort();
            }
        }

        if std::env::var_os(CHILD).is_none() {
            spate_test_support::run_in_child(NAME, |child| child.env(CHILD, "1"));
            return;
        }
        // SAFETY: the handler is a function item, valid for the whole process.
        assert_eq!(unsafe { atexit(abort_after_openssl_cleanup) }, 0);
        let mut config = new_config();
        config
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("security.protocol", "ssl")
            .set("ssl.ca.location", "probe");
        let client: BaseProducer<DefaultProducerContext> =
            construct(&config, DefaultProducerContext).unwrap();
        drop(client);
    }

    /// Pins that librdkafka narrows a `statistics.interval.ms` of 2^32 to
    /// `0`, which it reads as off, so the 24h ceiling of
    /// `STATISTICS_INTERVAL` also keeps such a value out.
    #[test]
    fn librdkafka_wraps_a_statistics_interval_at_2_pow_32() {
        let mut cc = new_config();
        cc.set("statistics.interval.ms", "4294967296");
        let native = cc.create_native_config().expect("native config");
        assert_eq!(native.get("statistics.interval.ms"), Ok("0".to_owned()));
    }
}
