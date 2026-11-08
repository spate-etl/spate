//! The checks [`KafkaFeed::connect`](crate::feed::KafkaFeed::connect) and
//! [`KafkaFeedPublisher::connect`](crate::feed::KafkaFeedPublisher::connect)
//! run on the feed's topics before the feed starts.
//!
//! The work, retry, dead-letter and attempt topics must exist. The work,
//! retry and dead-letter topics may not be compacted: a compacted topic
//! keeps only the latest message per key, and a unit's id is its key, so
//! compaction can delete a copy of a unit nobody has read. The attempt
//! topic must be compacted and nothing else, since time or size retention
//! would delete the counts of units still in flight, and it must report at
//! least one partition. The work topic must stamp each message as the broker
//! appends it: a store can write storage notifications to it with timestamp
//! 0, which time retention deletes at its next check, read or not. The
//! checks run once, at connect.
//!
//! A credential or TLS handshake rejected on the way to the cluster refuses
//! connecting with the rejection's text, as soon as the client reports it.
//! A read waiting on a request it sent finishes that wait first, within the
//! checks' bound.

use super::Topics;
use super::config::NAME;
use crate::client::{SpateContext, check_topic_name};
use crate::sink::SinkContext;
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{Producer, ThreadedProducer};
use spate_core::clock::SyncClock;
use spate_core::feed::ConnectError;
use spate_core::hash::FastMap;
use spate_core::task::{BoundError, ThreadBound, ThreadHandle, spawn_thread};
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// How long the checks may take, every step included.
pub(crate) const CONNECT_BOUND: Duration = Duration::from_secs(30);

/// How long a metadata read's calls wait while no broker is usable; see
/// [`next_slice`].
const SLICE: Duration = Duration::from_millis(100);

/// The configuration entry naming a topic's cleanup policies.
const CLEANUP_POLICY: &str = "cleanup.policy";

/// The configuration entry naming which timestamp a topic keeps for each
/// message.
const TIMESTAMP_TYPE: &str = "message.timestamp.type";

/// The [`TIMESTAMP_TYPE`] under which the broker stamps each message as it
/// appends it.
const LOG_APPEND_TIME: &str = "LogAppendTime";

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A failed admin request, or a topic the request reported an error for.
#[derive(Clone, Debug)]
pub(crate) struct AdminError {
    pub(crate) code: RDKafkaErrorCode,
    pub(crate) message: String,
    /// The checks' bound, when a metadata read ended at its deadline past it.
    pub(crate) bound: Option<Duration>,
}

impl AdminError {
    pub(crate) fn new(code: RDKafkaErrorCode, message: impl Into<String>) -> AdminError {
        AdminError {
            code,
            message: message.into(),
            bound: None,
        }
    }
}

/// Reads topic metadata and configuration from the cluster.
pub(crate) trait TopicAdmin: Send + Sync {
    /// How many partitions `topic` has, if it exists. An error the broker
    /// reports for the topic, `UnknownTopicOrPartition` for a missing one,
    /// is an [`AdminError`] with its code.
    fn metadata<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Result<usize, AdminError>>;

    /// `topic`'s configuration entries, by name. An entry the broker does
    /// not report is absent, and one it reports with no value is `None`. A
    /// topic the cluster describes no entry for, as when it refuses the
    /// read, is an empty map.
    fn configs<'a>(
        &'a self,
        topic: &'a str,
    ) -> BoxFuture<'a, Result<FastMap<String, Option<String>>, AdminError>>;

    /// The text of a credential or TLS handshake rejection the client
    /// recorded since it was built, if any.
    fn rejection(&self) -> Option<String>;
}

/// [`TopicAdmin`] over rdkafka clients: metadata through `reader`,
/// configuration through `admin`.
///
/// An admin client never serves librdkafka's main queue, so its error
/// callback never runs and a rejection it meets goes unreported. `reader`
/// polls that queue on a thread of its own, and its context records the
/// rejection. Every topic's metadata is read before any configuration, so a
/// rejection is met first on `reader`.
///
/// Each metadata read runs on a thread of its own and ends at most two
/// [`SLICE`]s past `limit` from when `Brokers` is built. Once nothing awaits
/// its answer, it stops within one [`SLICE`] while no broker is usable, and
/// when the call under way ends while a request it sent is outstanding. A
/// read that ends at its deadline ends the checks with the bound's error, so
/// they end the same way whichever passes first. Dropping `Brokers` joins
/// those threads, so both clients are closed when the drop returns.
pub(crate) struct Brokers {
    // Declared first, so the reads are joined before the fields below drop
    // and the last reference to `reader` drops on the thread dropping
    // `Brokers`.
    reads: Mutex<Vec<ThreadHandle<()>>>,
    admin: Arc<AdminClient<SpateContext<()>>>,
    reader: Arc<ThreadedProducer<SinkContext>>,
    limit: Duration,
    deadline: Instant,
}

impl Brokers {
    /// Wraps `admin` and `reader`, for checks that end `limit` from now.
    pub(crate) fn new(
        admin: AdminClient<SpateContext<()>>,
        reader: ThreadedProducer<SinkContext>,
        limit: Duration,
    ) -> Brokers {
        Brokers {
            reads: Mutex::new(Vec::new()),
            admin: Arc::new(admin),
            reader: Arc::new(reader),
            limit,
            // One slice past the limit, so the limit has passed on the system
            // clock by the time a read gives up as the bound would.
            deadline: Instant::now() + limit + SLICE,
        }
    }
}

/// What a metadata read does after a failed call.
#[derive(Debug, PartialEq, Eq)]
enum Next {
    /// Call again, waiting this long.
    Call(Duration),
    /// End at the deadline, with an error the read would have asked again.
    Expired,
    /// End with the call's error.
    Fail,
}

/// What a metadata read does after a call that failed with `code`, with
/// `left` before the read's deadline.
///
/// librdkafka answers `BrokerTransportFailure` when no broker became usable
/// within the call, having sent nothing, so the next call is short and a
/// read that nothing awaits stops soon. `OperationTimedOut` means a request
/// went out and its answer did not come in time. That answer is lost, so the
/// next call sends one more request and waits for half the time left, and
/// at least one [`SLICE`]. Half, because librdkafka waits for a usable
/// broker again, for the call's full length, when the broker a call asked
/// is decommissioned mid-call.
fn next_slice(code: RDKafkaErrorCode, left: Duration) -> Next {
    let slice = match code {
        RDKafkaErrorCode::BrokerTransportFailure => SLICE.min(left),
        RDKafkaErrorCode::OperationTimedOut => (left / 2).max(SLICE).min(left),
        _ => return Next::Fail,
    };
    if left.is_zero() {
        Next::Expired
    } else {
        Next::Call(slice)
    }
}

fn admin_error(e: &KafkaError) -> AdminError {
    let code = e.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail);
    AdminError::new(code, e.to_string())
}

impl TopicAdmin for Brokers {
    fn metadata<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Result<usize, AdminError>> {
        if let Err(why) = check_topic_name(topic) {
            return Box::pin(std::future::ready(Err(AdminError::new(
                RDKafkaErrorCode::InvalidTopic,
                format!("the topic name {why}"),
            ))));
        }
        let reader = Arc::clone(&self.reader);
        let name = topic.to_owned();
        let (limit, deadline) = (self.limit, self.deadline);
        let (tx, rx) = oneshot::channel();
        // `fetch_metadata` blocks and cannot be cancelled, so the read runs
        // on a thread of its own, and stops between calls once the future
        // below, and with it `rx`, has been dropped.
        let spawned = spawn_thread("spate-kafka-feed-check", move || {
            let mut slice = SLICE.min(deadline.saturating_duration_since(Instant::now()));
            let read = loop {
                match reader.client().fetch_metadata(Some(&name), slice) {
                    Err(e) => {
                        let code = e.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail);
                        let left = deadline.saturating_duration_since(Instant::now());
                        match next_slice(code, left) {
                            Next::Call(next) if !tx.is_closed() => slice = next,
                            Next::Expired => {
                                break Err(AdminError {
                                    bound: Some(limit),
                                    ..admin_error(&e)
                                });
                            }
                            _ => break Err(admin_error(&e)),
                        }
                    }
                    Ok(metadata) => {
                        let found = metadata.topics().iter().find(|t| t.name() == name);
                        break match found.map(|t| (t.error(), t.partitions().len())) {
                            None => Err(AdminError::new(
                                RDKafkaErrorCode::UnknownTopicOrPartition,
                                "the metadata response does not list the topic",
                            )),
                            Some((None, partitions)) => Ok(partitions),
                            Some((Some(err), _)) => {
                                let code = RDKafkaErrorCode::from(err);
                                Err(AdminError::new(code, code.to_string()))
                            }
                        };
                    }
                }
            };
            let _ = tx.send(read);
        });
        let spawned = spawned
            .map(|thread| {
                self.reads
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(thread);
            })
            .map_err(|e| {
                AdminError::new(RDKafkaErrorCode::Fail, format!("starting a thread: {e}"))
            });
        Box::pin(async move {
            spawned?;
            rx.await.unwrap_or_else(|_| {
                Err(AdminError::new(
                    RDKafkaErrorCode::Fail,
                    "the metadata read ended without an answer",
                ))
            })
        })
    }

    fn configs<'a>(
        &'a self,
        topic: &'a str,
    ) -> BoxFuture<'a, Result<FastMap<String, Option<String>>, AdminError>> {
        Box::pin(async move {
            let options = AdminOptions::new().request_timeout(Some(CONNECT_BOUND));
            let results = self
                .admin
                .describe_configs(&[ResourceSpecifier::Topic(topic)], &options)
                .await
                .map_err(|e| admin_error(&e))?;
            // rdkafka returns the error the cluster reports for one resource,
            // such as a refused `DescribeConfigs`, as that resource with no
            // entries, and never as an `Err`.
            Ok(results
                .into_iter()
                .flatten()
                .next()
                .map(|resource| {
                    resource
                        .entries
                        .into_iter()
                        .map(|e| (e.name, e.value))
                        .collect()
                })
                .unwrap_or_default())
        })
    }

    fn rejection(&self) -> Option<String> {
        self.reader.context().latest_rejection()
    }
}

/// Which checks [`check`] runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Scope {
    /// Every check.
    All,
    /// Only that each topic exists.
    Existence,
}

/// What the checks read, for the feed and the log line at connect. `None`
/// where the broker reported no value, or the scope read no configuration.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Checked {
    pub(crate) cleanup_policy: Option<String>,
    pub(crate) retry_cleanup_policy: Option<String>,
    pub(crate) dead_letter_cleanup_policy: Option<String>,
    pub(crate) attempt_cleanup_policy: Option<String>,
    /// The work topic's `message.timestamp.type`.
    pub(crate) timestamp_type: Option<String>,
    /// The attempt topic's partition count, which places each record.
    pub(crate) attempt_partitions: NonZeroU32,
}

/// The refusal for a rejection recorded while `step` was under way.
fn rejected(step: &str, text: &str) -> ConnectError {
    ConnectError::refused(
        NAME,
        format!("{step}: the client's credential or TLS handshake was rejected: {text}"),
    )
}

/// The error past the checks' bound of `limit`, while on `step`.
fn elapsed(limit: Duration, step: &str) -> ConnectError {
    ConnectError::unavailable(
        NAME,
        format!("the startup checks took longer than {limit:?}, while {step}"),
    )
}

/// `e`, from the step `doing`, as the error `connect` returns: a refusal
/// when `admin` recorded a rejection, whatever the read's own error, the
/// bound's error for a read that ended at its deadline, and otherwise the
/// class of `e`'s code by the table the source classifies its consumer
/// errors with.
///
/// A read never fails with a bad credential's code or a rejected TLS
/// handshake: librdkafka keeps a broker that failed either out of use, and
/// the read waits for a usable one. The client's error callback reports the
/// rejection instead, which [`TopicAdmin::rejection`] returns.
fn classified(admin: &dyn TopicAdmin, doing: &str, e: &AdminError) -> ConnectError {
    if let Some(text) = admin.rejection() {
        return rejected(doing, &text);
    }
    if let Some(limit) = e.bound {
        return elapsed(limit, doing);
    }
    let class = crate::error::classify_consumer_error(e.code, false);
    ConnectError::from_class(NAME, class, format!("{doing}: {}", e.message))
}

/// Run the startup checks on `topics`. `step` names the read under way, for
/// the connect bound's message.
///
/// # Errors
///
/// [`ConnectError::Refused`] for a missing topic, a compacted work, retry
/// or dead-letter topic, an attempt topic whose `cleanup.policy` is anything
/// but `compact` or that reports no partitions, a work topic whose
/// `message.timestamp.type` is anything but `LogAppendTime`, a read that
/// fails the same way when asked again, a failed read after the client
/// recorded a rejected credential or TLS handshake, and a topic the cluster
/// describes no configuration entry for; [`ConnectError::Unavailable`] for
/// a read that fails otherwise.
pub(crate) async fn check(
    admin: &dyn TopicAdmin,
    topics: &Topics,
    scope: Scope,
    step: &Mutex<String>,
) -> Result<Checked, ConnectError> {
    let set = |s: String| *step.lock().unwrap_or_else(PoisonError::into_inner) = s;
    let named = [
        ("work", &*topics.work),
        ("retry", &*topics.retry),
        ("dead-letter", &*topics.dead_letter),
        ("attempt", &*topics.attempt),
    ];
    let mut attempt_partitions = 0;
    for (role, topic) in named {
        let doing = format!("reading the metadata of the {role} topic `{topic}`");
        set(doing.clone());
        match admin.metadata(topic).await {
            Ok(partitions) => {
                if role == "attempt" {
                    attempt_partitions = partitions;
                }
            }
            Err(e) if e.code == RDKafkaErrorCode::UnknownTopicOrPartition => {
                return Err(ConnectError::refused(
                    NAME,
                    format!(
                        "the {role} topic `{topic}` does not exist; the feed never creates a \
                     topic, so create it before the first worker starts"
                    ),
                ));
            }
            Err(e) => return Err(classified(admin, &doing, &e)),
        }
    }

    let Some(attempt_partitions) = u32::try_from(attempt_partitions)
        .ok()
        .and_then(NonZeroU32::new)
    else {
        return Err(ConnectError::refused(
            NAME,
            format!(
                "the attempt topic `{}` reports no partitions",
                topics.attempt
            ),
        ));
    };

    if scope == Scope::Existence {
        return Ok(Checked {
            cleanup_policy: None,
            retry_cleanup_policy: None,
            dead_letter_cleanup_policy: None,
            attempt_cleanup_policy: None,
            timestamp_type: None,
            attempt_partitions,
        });
    }

    let mut read = Vec::with_capacity(named.len());
    for (role, topic) in named {
        let doing = format!("reading the configuration of the {role} topic `{topic}`");
        set(doing.clone());
        let configs = admin
            .configs(topic)
            .await
            .map_err(|e| classified(admin, &doing, &e))?;
        // Every existing topic has configuration entries, so a topic with
        // none is a read the cluster refused.
        if configs.is_empty() {
            return Err(ConnectError::refused(
                NAME,
                format!(
                    "{doing}: the cluster described no configuration entry for the topic, as it \
                 does for a client without the `DescribeConfigs` operation on it"
                ),
            ));
        }
        read.push(configs);
    }
    let entry = |i: usize, name: &str| read[i].get(name).cloned().flatten();

    let checked = Checked {
        cleanup_policy: entry(0, CLEANUP_POLICY),
        retry_cleanup_policy: entry(1, CLEANUP_POLICY),
        dead_letter_cleanup_policy: entry(2, CLEANUP_POLICY),
        attempt_cleanup_policy: entry(3, CLEANUP_POLICY),
        timestamp_type: entry(0, TIMESTAMP_TYPE),
        attempt_partitions,
    };
    let policies = [
        &checked.cleanup_policy,
        &checked.retry_cleanup_policy,
        &checked.dead_letter_cleanup_policy,
    ];
    for ((role, topic), policy) in named.into_iter().zip(policies) {
        let Some(policy) = policy else {
            tracing::warn!(
                topic,
                "the {role} topic reported no `{CLEANUP_POLICY}`; the compaction check is \
                 skipped for it"
            );
            continue;
        };
        if policy.split(',').any(|p| p.trim() == "compact") {
            return Err(ConnectError::refused(
                NAME,
                format!(
                    "the {role} topic `{topic}` is compacted (`{CLEANUP_POLICY}` is `{policy}`). \
                 Compaction keeps only the latest message for each key, and a unit's id is \
                 its key, so it can delete a unit nobody has read; set `{CLEANUP_POLICY}` \
                 to `delete`"
                ),
            ));
        }
    }
    match &checked.attempt_cleanup_policy {
        None => tracing::warn!(
            topic = &*topics.attempt,
            "the attempt topic reported no `{CLEANUP_POLICY}`; the compaction check is skipped \
             for it"
        ),
        Some(policy) if policy.trim() == "compact" => {}
        Some(policy) => {
            return Err(ConnectError::refused(
                NAME,
                format!(
                    "the attempt topic `{}` must be compacted only (`{CLEANUP_POLICY}` is \
                 `{policy}`). Time or size retention would delete the attempt counts of units \
                 still in flight; create it with `{CLEANUP_POLICY}=compact`",
                    topics.attempt
                ),
            ));
        }
    }
    match &checked.timestamp_type {
        None => tracing::warn!(
            topic = &*topics.work,
            "the work topic reported no `{TIMESTAMP_TYPE}`; the timestamp check is skipped"
        ),
        Some(kind) if kind == LOG_APPEND_TIME => {}
        Some(kind) => {
            return Err(ConnectError::refused(
                NAME,
                format!(
                    "the work topic `{}` keeps each message's own timestamp (`{TIMESTAMP_TYPE}` is \
                 `{kind}`). A store can write storage notifications to it with timestamp 0, \
                 as RustFS does, and time retention deletes such messages at its next check, \
                 read or not; create it with `{TIMESTAMP_TYPE}={LOG_APPEND_TIME}`",
                    topics.work
                ),
            ));
        }
    }
    Ok(checked)
}

/// Run [`check`] on `topics` through `brokers`, on the runtime `io`, under
/// [`bounded`] with `brokers`' limit on `clock` and its rejection watch.
/// Both clients in `brokers` are closed when it returns.
///
/// # Errors
///
/// As [`check`] and [`bounded`].
///
/// # Panics
///
/// When called from inside an async context, as
/// [`Handle::block_on`](tokio::runtime::Handle::block_on) does.
pub(crate) fn run_checks(
    brokers: Brokers,
    topics: &Topics,
    io: &tokio::runtime::Handle,
    scope: Scope,
    clock: SyncClock,
) -> Result<Checked, ConnectError> {
    let step = Mutex::new("starting".to_owned());
    let limit = brokers.limit;
    let brokers = Arc::new(brokers);
    let watched = Arc::clone(&brokers);
    io.block_on(bounded(
        &step,
        limit,
        clock,
        move || watched.rejection(),
        check(&*brokers, topics, scope, &step),
    ))
}

/// `checks` under a bound of `limit` on `clock`, ended early by a rejection
/// `rejection` returns. Past the bound, an [`ConnectError::Unavailable`]
/// naming the step `step` holds.
///
/// [`ThreadBound`] times the bound on a thread of its own, so it fires on any
/// runtime, calls `rejection` there once a tick, and joins that thread
/// before returning, so whatever `rejection` holds has been dropped by then.
///
/// # Errors
///
/// `checks`' own error; [`ConnectError::Refused`] naming the step and the
/// rejection once `rejection` returns one; [`ConnectError::Unavailable`]
/// past the bound, or when the system cannot start the thread.
pub(crate) async fn bounded<T>(
    step: &Mutex<String>,
    limit: Duration,
    clock: SyncClock,
    rejection: impl Fn() -> Option<String> + Send + 'static,
    checks: impl Future<Output = Result<T, ConnectError>>,
) -> Result<T, ConnectError> {
    let current = || step.lock().unwrap_or_else(PoisonError::into_inner).clone();
    ThreadBound::new("spate-kafka-feed-bound", limit)
        .with_clock(clock)
        .with_rejection(rejection)
        .run(checks)
        .await
        .unwrap_or_else(|stopped| {
            Err(match stopped {
                BoundError::Rejected(text) => rejected(&current(), &text),
                BoundError::Elapsed => elapsed(limit, &current()),
                other => ConnectError::unavailable(NAME, other.to_string()),
            })
        })
}

#[expect(
    clippy::disallowed_methods,
    reason = "a test thread ends with the call it runs, or serves a stub until the test process exits"
)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::config::KafkaFeedConfig;
    use RDKafkaErrorCode as C;
    use spate_core::clock::TestClock;
    use spate_test::{HANG, wait_until};
    use std::sync::mpsc;

    const WORK: &str = "units";
    const RETRY: &str = "units-retry";
    const DEAD: &str = "units-dead";
    const ATTEMPTS: &str = "units-attempts";

    fn topics() -> Topics {
        Topics {
            work: Arc::from(WORK),
            retry: Arc::from(RETRY),
            dead_letter: Arc::from(DEAD),
            attempt: Arc::from(ATTEMPTS),
        }
    }

    /// The configuration Apache Kafka describes for a topic created with no
    /// overrides, in part: every entry, its default included, with a value.
    fn broker_defaults() -> FastMap<String, Option<String>> {
        [
            ("cleanup.policy", "delete"),
            ("message.timestamp.type", "CreateTime"),
            ("retention.ms", "604800000"),
            ("retention.bytes", "-1"),
            ("min.insync.replicas", "1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), Some(v.to_owned())))
        .collect()
    }

    /// Topics by name, each with its configuration, or the error reading
    /// it fails with. Like a broker, it answers a topic it does not have
    /// with `UnknownTopicOrPartition`, for metadata and configuration alike.
    #[derive(Default)]
    struct Scripted {
        topics: FastMap<String, FastMap<String, Option<String>>>,
        /// Partition counts by topic; one where a topic has none here.
        partitions: FastMap<String, usize>,
        metadata_errors: FastMap<String, AdminError>,
        config_errors: FastMap<String, AdminError>,
        /// A topic whose metadata read (`true`) or configuration read
        /// (`false`) never answers.
        hangs: Option<(&'static str, bool)>,
        /// The rejection the client recorded.
        rejection: Option<String>,
    }

    impl Scripted {
        /// The four topics, each with the broker's defaults, the attempt
        /// topic compacted and the work topic stamped on append.
        fn all() -> Scripted {
            let mut s = Scripted::default();
            for t in [WORK, RETRY, DEAD, ATTEMPTS] {
                s.topics.insert(t.to_owned(), broker_defaults());
            }
            s.entry(ATTEMPTS, CLEANUP_POLICY, Some(Some("compact")))
                .entry(WORK, TIMESTAMP_TYPE, Some(Some(LOG_APPEND_TIME)))
        }

        fn without(mut self, topic: &str) -> Scripted {
            self.topics.remove(topic);
            self
        }

        /// Set `entry` of `topic`: `Some(Some(v))` to `v`, `Some(None)` to
        /// no value, `None` to absent.
        fn entry(mut self, topic: &str, entry: &str, value: Option<Option<&str>>) -> Scripted {
            let configs = self.topics.get_mut(topic).unwrap();
            match value {
                Some(v) => {
                    configs.insert(entry.to_owned(), v.map(str::to_owned));
                }
                None => {
                    configs.remove(entry);
                }
            }
            self
        }

        fn missing(topic: &str) -> AdminError {
            AdminError::new(C::UnknownTopicOrPartition, format!("{topic}: unknown"))
        }
    }

    impl TopicAdmin for Scripted {
        fn metadata<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Result<usize, AdminError>> {
            let hang = self.hangs == Some((topic, true));
            let answer = match self.metadata_errors.get(topic) {
                Some(e) => Err(e.clone()),
                None if self.topics.contains_key(topic) => {
                    Ok(self.partitions.get(topic).copied().unwrap_or(1))
                }
                None => Err(Scripted::missing(topic)),
            };
            Box::pin(async move {
                if hang {
                    std::future::pending::<()>().await;
                }
                answer
            })
        }

        fn configs<'a>(
            &'a self,
            topic: &'a str,
        ) -> BoxFuture<'a, Result<FastMap<String, Option<String>>, AdminError>> {
            let hang = self.hangs == Some((topic, false));
            let answer = match self.config_errors.get(topic) {
                Some(e) => Err(e.clone()),
                None => self
                    .topics
                    .get(topic)
                    .cloned()
                    .ok_or_else(|| Scripted::missing(topic)),
            };
            Box::pin(async move {
                if hang {
                    std::future::pending::<()>().await;
                }
                answer
            })
        }

        fn rejection(&self) -> Option<String> {
            self.rejection.clone()
        }
    }

    /// The section the topics above come from.
    fn config() -> KafkaFeedConfig {
        let section = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: localhost:9092\n  group_id: workers\n  topic: {WORK}\n  \
             retry_topic: {RETRY}\n  dead_letter_topic: {DEAD}\n  attempt_topic: {ATTEMPTS}\n"
        ))
        .unwrap();
        KafkaFeedConfig::from_section(&section).unwrap()
    }

    fn run(admin: &Scripted) -> Result<Checked, ConnectError> {
        let step = Mutex::new("starting".to_owned());
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(check(admin, &topics(), Scope::All, &step))
    }

    fn refusal(result: Result<Checked, ConnectError>) -> String {
        match result {
            Err(ConnectError::Refused {
                reason: message, ..
            }) => message,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// Pins that each missing topic is refused by its role and name, and
    /// that topics the feed can run on pass with what was read.
    #[test]
    fn a_missing_topic_is_refused_by_its_role() {
        for (topic, role) in [
            (WORK, "work"),
            (RETRY, "retry"),
            (DEAD, "dead-letter"),
            (ATTEMPTS, "attempt"),
        ] {
            let message = refusal(run(&Scripted::all().without(topic)));
            assert!(
                message.contains(&format!("the {role} topic `{topic}` does not exist")),
                "{message}"
            );
        }
        let checked = run(&Scripted::all()).unwrap();
        assert_eq!(
            checked,
            Checked {
                cleanup_policy: Some("delete".into()),
                retry_cleanup_policy: Some("delete".into()),
                dead_letter_cleanup_policy: Some("delete".into()),
                attempt_cleanup_policy: Some("compact".into()),
                timestamp_type: Some("LogAppendTime".into()),
                attempt_partitions: NonZeroU32::MIN,
            }
        );
    }

    /// Pins the work topic's timestamp check: `CreateTime` on the work topic
    /// is refused naming the topic, the value and the setting to use, while
    /// the retry, dead-letter and attempt topics pass at the broker's
    /// `CreateTime` default; an unreported value skips the check with a
    /// warning, and the line logged at connect shows it as `unreported`.
    #[test]
    fn a_work_topic_not_stamped_on_append_is_refused() {
        let admin = Scripted::all().entry(WORK, TIMESTAMP_TYPE, Some(Some("CreateTime")));
        let message = refusal(run(&admin));
        assert!(
            message.contains(&format!("the work topic `{WORK}`")),
            "{message}"
        );
        assert!(
            message.contains("`message.timestamp.type` is `CreateTime`"),
            "{message}"
        );
        assert!(
            message.contains("message.timestamp.type=LogAppendTime"),
            "{message}"
        );

        let others = Scripted::all();
        for topic in [RETRY, DEAD, ATTEMPTS] {
            assert_eq!(
                others.topics[topic][TIMESTAMP_TYPE].as_deref(),
                Some("CreateTime"),
                "{topic}"
            );
        }
        let checked = run(&others).unwrap();
        assert_eq!(checked.timestamp_type.as_deref(), Some("LogAppendTime"));

        for value in [None, Some(None)] {
            let admin = Scripted::all().entry(WORK, TIMESTAMP_TYPE, value);
            let mut result = None;
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                result = Some(run(&admin));
            });
            let checked = result.unwrap().unwrap();
            assert_eq!(checked.timestamp_type, None);
            assert_eq!(lines.len(), 1, "{lines:#?}");
            assert!(lines[0].contains("the work topic"), "{}", lines[0]);
            let started = spate_test::capture_logs(tracing::Level::INFO, || {
                crate::feed::log_started(&config(), &checked);
            });
            assert!(
                started[0].contains(" timestamp_type=unreported"),
                "{}",
                started[0]
            );
        }
    }

    /// Pins the attempt topic's policy check: `compact` alone passes, with
    /// spaces around it; `delete`, and `compact` beside `delete` in either
    /// order, are refused naming the topic and the value; an unreported
    /// policy skips the check with a warning, and the line logged at
    /// connect shows it as `unreported`.
    #[test]
    fn an_attempt_topic_that_is_not_only_compacted_is_refused() {
        for passing in ["compact", " compact "] {
            let admin = Scripted::all().entry(ATTEMPTS, CLEANUP_POLICY, Some(Some(passing)));
            run(&admin).unwrap_or_else(|e| panic!("{passing:?}: {e}"));
        }
        for policy in ["delete", "compact,delete", "delete,compact"] {
            let admin = Scripted::all().entry(ATTEMPTS, CLEANUP_POLICY, Some(Some(policy)));
            let message = refusal(run(&admin));
            assert!(
                message.contains(&format!("the attempt topic `{ATTEMPTS}`")),
                "{message}"
            );
            assert!(
                message.contains(&format!("`cleanup.policy` is `{policy}`")),
                "{message}"
            );
            assert!(message.contains("cleanup.policy=compact"), "{message}");
        }
        for value in [None, Some(None)] {
            let admin = Scripted::all().entry(ATTEMPTS, CLEANUP_POLICY, value);
            let mut result = None;
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                result = Some(run(&admin));
            });
            let checked = result.unwrap().unwrap();
            assert_eq!(checked.attempt_cleanup_policy, None);
            assert_eq!(lines.len(), 1, "{lines:#?}");
            assert!(lines[0].contains("the attempt topic"), "{}", lines[0]);
            let started = spate_test::capture_logs(tracing::Level::INFO, || {
                crate::feed::log_started(&config(), &checked);
            });
            assert!(
                started[0].contains(" attempt_cleanup_policy=unreported"),
                "{}",
                started[0]
            );
        }
    }

    /// Pins that the attempt topic's partition count is read, under both
    /// scopes, and that a topic reporting none is refused.
    #[test]
    fn startup_reads_the_attempt_topics_partition_count() {
        let mut admin = Scripted::all();
        admin.partitions.insert(ATTEMPTS.to_owned(), 2);
        // The work topic's count is not the attempt topic's.
        admin.partitions.insert(WORK.to_owned(), 5);
        assert_eq!(run(&admin).unwrap().attempt_partitions.get(), 2);
        let step = Mutex::new("starting".to_owned());
        let existence = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(check(&admin, &topics(), Scope::Existence, &step))
            .unwrap();
        assert_eq!(existence.attempt_partitions.get(), 2);
        assert_eq!(existence.timestamp_type, None);

        admin.partitions.insert(ATTEMPTS.to_owned(), 0);
        let message = refusal(run(&admin));
        assert!(
            message.contains(&format!(
                "the attempt topic `{ATTEMPTS}` reports no partitions"
            )),
            "{message}"
        );
    }

    /// Pins that `compact` anywhere in a topic's cleanup policies refuses
    /// it, by role, and that `delete` alone passes.
    #[test]
    fn a_compacted_topic_is_refused_by_its_role() {
        for (topic, role) in [(WORK, "work"), (RETRY, "retry"), (DEAD, "dead-letter")] {
            for policy in ["compact", "delete,compact", "compact,delete"] {
                let admin = Scripted::all().entry(topic, CLEANUP_POLICY, Some(Some(policy)));
                let message = refusal(run(&admin));
                assert!(
                    message.contains(&format!("the {role} topic `{topic}` is compacted")),
                    "{message}"
                );
            }
        }
    }

    /// Pins that an entry the broker does not report, or reports with no
    /// value, skips its check with one warning naming the topic and the
    /// entry, that the checks the broker did answer still run, and that the
    /// line logged at connect shows the entry as `unreported`. No
    /// Apache Kafka broker leaves out `cleanup.policy`, which has a
    /// default; an absent entry stands for a
    /// Kafka-compatible broker that describes only some of a topic's
    /// configuration, and an entry with no value for one that withholds it,
    /// as Kafka does for a sensitive entry.
    #[test]
    fn an_unreported_entry_skips_its_check_with_a_warning() {
        for value in [None, Some(None)] {
            // The work topic's policy goes unreported; the retry topic stays
            // compacted, and is refused.
            let admin = Scripted::all().entry(WORK, CLEANUP_POLICY, value).entry(
                RETRY,
                CLEANUP_POLICY,
                Some(Some("compact")),
            );
            let mut result = None;
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                result = Some(run(&admin));
            });
            assert!(refusal(result.unwrap()).contains("the retry topic"));
            assert_eq!(lines.len(), 1, "{lines:#?}");
            for needle in [
                "WARN",
                WORK,
                "`cleanup.policy`",
                "compaction check is skipped",
            ] {
                assert!(lines[0].contains(needle), "{needle}: {}", lines[0]);
            }

            let admin = Scripted::all().entry(WORK, CLEANUP_POLICY, value);
            let mut result = None;
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                result = Some(run(&admin));
            });
            let checked = result.unwrap().unwrap();
            assert_eq!(checked.cleanup_policy, None);
            assert_eq!(checked.retry_cleanup_policy, Some("delete".into()));
            assert_eq!(lines.len(), 1, "{lines:#?}");

            let started = spate_test::capture_logs(tracing::Level::INFO, || {
                crate::feed::log_started(&config(), &checked);
            });
            assert_eq!(started.len(), 1, "{started:#?}");
            for needle in [
                " cleanup_policy=unreported",
                " retry_cleanup_policy=delete",
                " attempt_cleanup_policy=compact",
                " attempt_partitions=1",
            ] {
                assert!(started[0].contains(needle), "{needle}: {}", started[0]);
            }
        }
    }

    /// Pins how a failed read maps: a read that fails the same way when
    /// asked again, as unauthorized or for an invalid topic name, is a
    /// refusal, anything else unavailable, each naming the read and the
    /// topic.
    #[test]
    fn a_failed_read_is_refused_when_unauthorized_and_unavailable_otherwise() {
        for code in [
            C::TopicAuthorizationFailed,
            C::ClusterAuthorizationFailed,
            C::InvalidTopic,
        ] {
            let mut admin = Scripted::all();
            admin
                .metadata_errors
                .insert(RETRY.to_owned(), AdminError::new(code, "denied"));
            let message = refusal(run(&admin));
            assert!(
                message.contains("reading the metadata of the retry topic `units-retry`: denied"),
                "{message}"
            );
        }
        for code in [
            C::LeaderNotAvailable,
            C::RequestTimedOut,
            C::BrokerTransportFailure,
        ] {
            let mut admin = Scripted::all();
            admin
                .config_errors
                .insert(WORK.to_owned(), AdminError::new(code, "slow"));
            match run(&admin) {
                Err(ConnectError::Unavailable {
                    reason: message, ..
                }) => assert!(
                    message.contains("reading the configuration of the work topic `units`: slow"),
                    "{message}"
                ),
                other => panic!("{code:?}: expected unavailable, got {other:?}"),
            }
        }
    }

    /// Pins the text a refused and an unavailable read render, with the
    /// feed's name in it.
    #[test]
    fn a_failed_read_renders_the_kafka_text() {
        let mut admin = Scripted::all();
        admin.metadata_errors.insert(
            RETRY.to_owned(),
            AdminError::new(C::TopicAuthorizationFailed, "denied"),
        );
        assert_eq!(
            run(&admin).unwrap_err().to_string(),
            "the Kafka feed cannot start: reading the metadata of the retry topic `units-retry`: denied"
        );

        let mut admin = Scripted::all();
        admin
            .config_errors
            .insert(WORK.to_owned(), AdminError::new(C::RequestTimedOut, "slow"));
        assert_eq!(
            run(&admin).unwrap_err().to_string(),
            "Kafka is unavailable: reading the configuration of the work topic `units`: slow"
        );
    }

    /// The bound the tests advance a frozen clock past.
    const SHORT: Duration = Duration::from_millis(20);

    /// The checks under [`SHORT`] on a frozen clock, as `connect` runs them:
    /// through `Handle::block_on` on `io`, with the read of `hangs` never
    /// answering. The clock moves past the bound once the checks are on
    /// `reading`. `None` when nothing came back within [`HANG`].
    fn bounded_on(
        io: tokio::runtime::Handle,
        hangs: (&'static str, bool),
        reading: &str,
    ) -> Option<Result<Checked, ConnectError>> {
        let clock = TestClock::frozen();
        let run = bounded_with(io, hangs, SHORT, &clock, || None);
        run.wait_for(reading);
        clock.advance(SHORT);
        run.done.recv_timeout(HANG).ok()
    }

    /// Checks running under [`bounded`] on a thread of their own.
    struct Bounded {
        /// The step the checks are on.
        step: Arc<Mutex<String>>,
        done: mpsc::Receiver<Result<Checked, ConnectError>>,
    }

    impl Bounded {
        /// Wait until the checks are on `reading`.
        fn wait_for(&self, reading: &str) {
            wait_until(HANG, reading, || {
                *self.step.lock().unwrap_or_else(PoisonError::into_inner) == reading
            });
        }
    }

    /// The checks with the read of `hangs` never answering, under a bound of
    /// `limit` on `clock`, with `rejection` as the client's rejection watch.
    fn bounded_with(
        io: tokio::runtime::Handle,
        hangs: (&'static str, bool),
        limit: Duration,
        clock: &Arc<TestClock>,
        rejection: impl Fn() -> Option<String> + Send + 'static,
    ) -> Bounded {
        let (tx, done) = mpsc::channel();
        let step = Arc::new(Mutex::new("starting".to_owned()));
        let (running, clock) = (Arc::clone(&step), SyncClock::new(Arc::clone(clock) as _));
        std::thread::spawn(move || {
            let admin = Scripted {
                hangs: Some(hangs),
                ..Scripted::all()
            };
            let topics = topics();
            let checks = check(&admin, &topics, Scope::All, &running);
            let _ = tx.send(io.block_on(bounded(&running, limit, clock, rejection, checks)));
        });
        Bounded { step, done }
    }

    fn multi_thread() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    /// Pins that the connect bound names the read it was on, for a metadata
    /// read and for a configuration read.
    #[test]
    fn the_connect_bound_names_the_read_it_was_on() {
        let io = multi_thread();
        for (hangs, step) in [
            (
                (RETRY, true),
                "while reading the metadata of the retry topic `units-retry`",
            ),
            (
                (DEAD, false),
                "while reading the configuration of the dead-letter topic `units-dead`",
            ),
        ] {
            let reading = step.trim_start_matches("while ");
            match bounded_on(io.handle().clone(), hangs, reading) {
                Some(Err(ConnectError::Unavailable {
                    reason: message, ..
                })) => {
                    assert!(message.ends_with(step), "{message}");
                }
                other => panic!("expected unavailable, got {other:?}"),
            }
        }
    }

    /// Pins that the connect bound fires through `Handle::block_on` on a
    /// current-thread runtime, which drives no timer there, on a runtime
    /// with timers disabled, and on one that has shut down.
    #[test]
    fn the_connect_bound_fires_on_any_runtime() {
        let current = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let untimed = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let stopped = multi_thread().handle().clone();
        for (name, io) in [
            ("current-thread", current.handle().clone()),
            ("timers disabled", untimed.handle().clone()),
            ("shut down", stopped),
        ] {
            match bounded_on(
                io,
                (WORK, true),
                "reading the metadata of the work topic `units`",
            ) {
                Some(Err(ConnectError::Unavailable {
                    reason: message, ..
                })) => assert!(
                    message.ends_with("while reading the metadata of the work topic `units`"),
                    "{name}: {message}"
                ),
                other => panic!("{name}: expected unavailable, got {other:?}"),
            }
        }
    }

    /// Pins that a topic the cluster describes with no configuration entry
    /// is refused, naming the read and the topic.
    #[test]
    fn a_topic_described_with_no_entries_is_refused() {
        for (topic, role) in [
            (WORK, "work"),
            (RETRY, "retry"),
            (DEAD, "dead-letter"),
            (ATTEMPTS, "attempt"),
        ] {
            let mut admin = Scripted::all();
            admin.topics.insert(topic.to_owned(), FastMap::default());
            let message = refusal(run(&admin));
            assert!(
                message.contains(&format!(
                    "reading the configuration of the {role} topic `{topic}`"
                )),
                "{message}"
            );
            assert!(message.contains("no configuration entry"), "{message}");
        }
    }

    /// librdkafka's text for a handshake the broker refused, as its error
    /// callback reports it.
    const ALERT_40: &str = "ssl://127.0.0.1:9093/bootstrap: SSL handshake failed: \
        ssl/tls alert handshake failure: SSL alert number 40 (after 0ms in state SSL_HANDSHAKE)";

    /// Pins that a failed read refuses connecting, naming the read and the
    /// rejection, once the client recorded a rejected credential or TLS
    /// handshake, and that the same read with none recorded is unavailable.
    #[test]
    fn a_failed_read_after_a_rejection_is_refused_with_its_text() {
        let failure = AdminError::new(C::BrokerTransportFailure, "transport");
        let mut admin = Scripted::all();
        admin
            .metadata_errors
            .insert(WORK.to_owned(), failure.clone());
        match run(&admin) {
            Err(ConnectError::Unavailable {
                reason: message, ..
            }) => {
                assert!(message.ends_with("`units`: transport"), "{message}");
            }
            other => panic!("expected unavailable, got {other:?}"),
        }
        admin.rejection = Some(ALERT_40.to_owned());
        let message = refusal(run(&admin));
        assert!(
            message.starts_with("reading the metadata of the work topic `units`: "),
            "{message}"
        );
        assert!(message.ends_with(ALERT_40), "{message}");

        let mut admin = Scripted {
            rejection: Some(ALERT_40.to_owned()),
            ..Scripted::all()
        };
        admin.config_errors.insert(DEAD.to_owned(), failure);
        let message = refusal(run(&admin));
        assert!(
            message.starts_with("reading the configuration of the dead-letter topic"),
            "{message}"
        );
        assert!(message.ends_with(ALERT_40), "{message}");
    }

    /// Pins that a read that ended at its deadline ends the checks with the
    /// bound's error, unless the client recorded a rejection, which refuses
    /// them with its text.
    #[test]
    fn a_read_ended_at_its_deadline_gives_the_bounds_error_unless_rejected() {
        let expired = AdminError {
            bound: Some(Duration::from_secs(30)),
            ..AdminError::new(C::BrokerTransportFailure, "transport")
        };
        let mut admin = Scripted::all();
        admin.metadata_errors.insert(RETRY.to_owned(), expired);
        match run(&admin) {
            Err(ConnectError::Unavailable {
                reason: message, ..
            }) => assert_eq!(
                message,
                "the startup checks took longer than 30s, while reading the metadata of the \
                 retry topic `units-retry`"
            ),
            other => panic!("expected unavailable, got {other:?}"),
        }
        admin.rejection = Some(ALERT_40.to_owned());
        let message = refusal(run(&admin));
        assert!(
            message.starts_with("reading the metadata of the retry topic `units-retry`: "),
            "{message}"
        );
        assert!(message.ends_with(ALERT_40), "{message}");
    }

    /// The connect check reads the metadata client's latest rejection
    /// however long ago it arrived.
    #[test]
    fn a_rejection_refuses_the_connect_check_at_any_age() {
        use crate::client::testing::raw;
        let clock = spate_core::clock::TestClock::frozen();
        let hooks = crate::sink::SinkHooks::detached(SyncClock::new(Arc::clone(&clock) as _));
        let broker = crate::testutil::refused_broker();
        let properties = [("bootstrap.servers", broker.as_str())];
        let brokers = Brokers::new(raw(&properties, ()), raw(&properties, hooks), CONNECT_BOUND);
        brokers.reader.context().note_error(C::SSL, ALERT_40);
        clock.advance(Duration::from_secs(3600));
        assert_eq!(brokers.rejection().as_deref(), Some(ALERT_40));
    }

    /// Pins that a rejection the client records ends the checks with a
    /// refusal naming the read under way and the rejection, long before
    /// the bound.
    #[test]
    fn a_recorded_rejection_ends_the_checks_before_the_bound() {
        let io = multi_thread();
        let limit = Duration::from_secs(600);
        let clock = TestClock::frozen();
        let run = bounded_with(io.handle().clone(), (RETRY, true), limit, &clock, || {
            Some(ALERT_40.to_owned())
        });
        match run.done.recv_timeout(HANG).ok() {
            Some(Err(ConnectError::Refused {
                reason: message, ..
            })) => {
                assert!(
                    message.starts_with("reading the metadata of the retry topic `units-retry`: "),
                    "{message}"
                );
                assert!(message.ends_with(ALERT_40), "{message}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// Pins that the connect bound fires once `limit` has passed on its
    /// clock, and not before.
    #[test]
    fn the_connect_bound_fires_at_its_limit_on_the_given_clock() {
        let io = multi_thread();
        let limit = Duration::from_secs(600);
        let clock = TestClock::frozen();
        let asked = Arc::new(Mutex::new(0_usize));
        let counted = Arc::clone(&asked);
        let run = bounded_with(
            io.handle().clone(),
            (RETRY, true),
            limit,
            &clock,
            move || {
                *counted.lock().unwrap() += 1;
                None
            },
        );
        run.wait_for("reading the metadata of the retry topic `units-retry`");
        // The bound thread asks for a rejection before each deadline check,
        // so a second ask after the advance follows a check that read it.
        let sample = {
            let asked = asked.lock().unwrap();
            clock.advance(limit - Duration::from_nanos(1));
            *asked
        };
        wait_until(HANG, "the bound checks the advanced clock", || {
            *asked.lock().unwrap() >= sample + 2
        });
        assert!(run.done.try_recv().is_err(), "fired before its limit");
        clock.advance(Duration::from_nanos(1));
        match run.done.recv_timeout(HANG) {
            Ok(Err(ConnectError::Unavailable {
                reason: message, ..
            })) => assert!(
                message.starts_with("the startup checks took longer than 600s"),
                "{message}"
            ),
            other => panic!("expected unavailable, got {other:?}"),
        }
    }

    /// Pins that `connect` times its bound on the clock it is given: with a
    /// broker that accepts connections and never answers, the checks end
    /// once the clock passes the bound.
    #[test]
    fn the_connect_bound_runs_on_the_clock_connect_is_given() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let brokers = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            let held: Vec<_> = listener.incoming().collect();
            drop(held);
        });
        let section: spate_core::config::FeedSection = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: workers\n  topic: {WORK}\n  \
             retry_topic: {RETRY}\n  dead_letter_topic: {DEAD}\n  attempt_topic: {ATTEMPTS}\n  \
             statistics_interval: 0s\n"
        ))
        .unwrap();
        let limit = Duration::from_secs(600);
        let clock = TestClock::frozen();
        let given = SyncClock::new(Arc::clone(&clock) as _);
        let (tx, done) = mpsc::channel();
        std::thread::spawn(move || {
            let io = multi_thread();
            let connected = crate::feed::KafkaFeed::connect_with(
                &section,
                io.handle(),
                Scope::Existence,
                limit,
                given,
            );
            let _ = tx.send(connected.err());
        });
        // The bound reads its deadline once the clients are built, which no
        // signal marks, so the clock moves on until the bound fires.
        let mut fired = None;
        wait_until(HANG, "the connect bound fires", || {
            clock.advance(limit);
            fired = done.try_recv().ok();
            fired.is_some()
        });
        match fired.flatten() {
            Some(ConnectError::Unavailable {
                reason: message, ..
            }) => assert!(
                message.starts_with("the startup checks took longer than 600s"),
                "{message}"
            ),
            other => panic!("expected unavailable, got {other:?}"),
        }
    }

    /// `KafkaFeed::connect` against `brokers` with the passthrough
    /// `rdkafka`, under a bound of `limit`, on a thread of its own. `None`
    /// when nothing came back within [`HANG`].
    ///
    /// On the system clock: the tests that use it pin what librdkafka does
    /// in real time inside the bound.
    fn connect_to(brokers: &str, rdkafka: &str, limit: Duration) -> Option<ConnectError> {
        connect_with_to(brokers, rdkafka, limit, |section, io, limit| {
            crate::feed::KafkaFeed::connect_with(
                section,
                io,
                Scope::All,
                limit,
                SyncClock::system(),
            )
            .err()
        })
    }

    /// `KafkaFeedPublisher::connect` as [`connect_to`] runs the feed's.
    #[cfg(feature = "tls")]
    fn connect_publisher_to(brokers: &str, rdkafka: &str, limit: Duration) -> Option<ConnectError> {
        connect_with_to(brokers, rdkafka, limit, |section, io, limit| {
            crate::feed::KafkaFeedPublisher::connect_with(section, io, Scope::All, limit).err()
        })
    }

    /// [`connect_to`] with the constructor `connect`.
    fn connect_with_to(
        brokers: &str,
        rdkafka: &str,
        limit: Duration,
        connect: fn(
            &spate_core::config::FeedSection,
            &tokio::runtime::Handle,
            Duration,
        ) -> Option<ConnectError>,
    ) -> Option<ConnectError> {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: workers\n  topic: {WORK}\n  \
             retry_topic: {RETRY}\n  dead_letter_topic: {DEAD}\n  attempt_topic: {ATTEMPTS}\n  \
             statistics_interval: 0s\n  rdkafka:\n{rdkafka}"
        ))
        .unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let io = multi_thread();
            let _ = tx.send(connect(&section, io.handle(), limit));
        });
        rx.recv_timeout(HANG)
            .unwrap_or_else(|_| panic!("connect did not return within {:?}", HANG))
    }

    /// Pins that a broker that refuses the connection is unavailable at the
    /// bound, and records no rejection.
    #[test]
    fn a_refused_connection_is_unavailable_at_the_bound() {
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let brokers = closed.local_addr().unwrap().to_string();
        drop(closed);
        match connect_to(&brokers, "", Duration::from_secs(2)) {
            Some(ConnectError::Unavailable {
                reason: message, ..
            }) => assert!(
                message.starts_with("the startup checks took longer than 2s"),
                "{message}"
            ),
            other => panic!("expected unavailable, got {other:?}"),
        }
    }

    /// [`run_checks`] through clients built with `properties`, under
    /// `limit` on a frozen clock, on a thread of its own. Asserts that the
    /// metadata client is gone once the result is back within [`HANG`].
    fn checks_closing_their_clients(
        properties: &[(&str, &str)],
        limit: Duration,
    ) -> Result<Checked, ConnectError> {
        use crate::client::testing::raw;
        let clock = SyncClock::new(TestClock::frozen() as _);
        let hooks = crate::sink::SinkHooks::detached(clock.clone());
        let brokers = Brokers::new(raw(properties, ()), raw(properties, hooks), limit);
        let reader = Arc::downgrade(&brokers.reader);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let io = multi_thread();
            let _ = tx.send(run_checks(
                brokers,
                &topics(),
                io.handle(),
                Scope::All,
                clock,
            ));
        });
        let got = rx
            .recv_timeout(HANG)
            .unwrap_or_else(|_| panic!("the checks did not return within {:?}", HANG));
        assert!(
            reader.upgrade().is_none(),
            "the metadata client outlived the checks"
        );
        got
    }

    /// Pins that checks ended by the bound, with a metadata read still
    /// waiting for a usable broker, have closed the metadata client by the
    /// time they return. Regression for #1809.
    #[test]
    fn checks_ended_by_the_bound_close_their_clients_before_they_return() {
        let broker = crate::testutil::refused_broker();
        let got =
            checks_closing_their_clients(&[("bootstrap.servers", broker.as_str())], Duration::ZERO);
        match got {
            Err(ConnectError::Unavailable {
                reason: message, ..
            }) => assert!(
                message.starts_with("the startup checks took longer than"),
                "{message}"
            ),
            other => panic!("expected unavailable, got {other:?}"),
        }
    }

    /// A metadata read that reaches its deadline before the bound fires
    /// ends the checks as the bound does, naming the read under way.
    #[test]
    fn a_read_past_its_deadline_ends_the_checks_as_the_bound_does() {
        let broker = crate::testutil::refused_broker();
        // The bound's clock never moves, so only the read's deadline ends the
        // checks.
        let got = checks_closing_their_clients(
            &[("bootstrap.servers", broker.as_str())],
            Duration::from_millis(1),
        );
        match got {
            Err(ConnectError::Unavailable {
                reason: message, ..
            }) => assert_eq!(
                message,
                "the startup checks took longer than 1ms, while reading the metadata of the \
                 work topic `units`"
            ),
            other => panic!("expected unavailable, got {other:?}"),
        }
    }

    /// Pins how a metadata read's next call waits after each error: one
    /// [`SLICE`] while no broker is usable, half the time left and at least
    /// one [`SLICE`] after a request went unanswered, never past the
    /// deadline. Once the deadline has passed, either error ends the read
    /// as expired, and any other error ends it with that error whatever the
    /// time left.
    #[test]
    fn a_metadata_read_paces_its_calls_by_the_error() {
        let ms = Duration::from_millis;
        let left = Duration::from_secs(30);
        let unusable = C::BrokerTransportFailure;
        let unanswered = C::OperationTimedOut;
        assert_eq!(next_slice(unusable, left), Next::Call(SLICE));
        assert_eq!(next_slice(unusable, ms(30)), Next::Call(ms(30)));
        assert_eq!(next_slice(unanswered, left), Next::Call(left / 2));
        assert_eq!(next_slice(unanswered, ms(150)), Next::Call(SLICE));
        assert_eq!(next_slice(unanswered, ms(30)), Next::Call(ms(30)));
        for code in [unusable, unanswered] {
            assert_eq!(next_slice(code, Duration::ZERO), Next::Expired, "{code:?}");
        }
        for code in [C::Fail, C::TopicAuthorizationFailed, C::LeaderNotAvailable] {
            for left in [left, Duration::ZERO] {
                assert_eq!(next_slice(code, left), Next::Fail, "{code:?} {left:?}");
            }
        }
    }

    /// Pins that a metadata read ends by the deadline its [`Brokers`] was
    /// built with, while its answer is still awaited, so no read outlasts
    /// the checks' bound. Regression for #1809.
    #[test]
    fn a_metadata_read_ends_by_the_deadline_of_its_brokers() {
        use crate::client::testing::raw;
        let broker = crate::testutil::refused_broker();
        let properties = [("bootstrap.servers", broker.as_str())];
        let hooks = crate::sink::SinkHooks::detached(SyncClock::new(TestClock::frozen() as _));
        let brokers = Brokers::new(
            raw(&properties, ()),
            raw(&properties, hooks),
            Duration::ZERO,
        );
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let io = multi_thread();
            let _ = tx.send(io.block_on(brokers.metadata(WORK)).map_err(|e| e.code));
        });
        match rx.recv_timeout(HANG) {
            Ok(Err(C::BrokerTransportFailure)) => {}
            other => panic!(
                "expected a transport failure within {:?}, got {other:?}",
                HANG
            ),
        }
    }

    /// Pins that a metadata read ends at its deadline, with the bound's
    /// error, when the slow broker it asked is replaced, mid-call, by one the
    /// client cannot reach. librdkafka then waits for a usable broker again
    /// for the call's full length.
    /// [`a_metadata_read_paces_its_calls_by_the_error`] pins the pacing that
    /// keeps each call inside the deadline.
    #[test]
    #[expect(unsafe_code, reason = "moves a mock broker's advertised listener")]
    fn a_metadata_read_ends_by_its_deadline_when_its_broker_is_replaced() {
        use crate::client::testing::raw;
        use crate::testutil::mock::MockKafka;
        use std::ffi::CString;
        let limit = Duration::from_secs(3);
        let cluster = MockKafka::new();
        cluster.create_topic(WORK, 1, 1).expect("create topic");
        cluster
            .broker_round_trip_time(1, Duration::from_secs(1))
            .expect("round trip time");
        let servers = cluster.bootstrap_servers();
        let unreachable = crate::testutil::refused_broker();
        let port = unreachable
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .expect("a port");
        let host = CString::new("127.0.0.1").expect("host");
        // SAFETY: `native` is valid while `cluster` lives. The bootstrap
        // address still reaches the broker; only the listener its metadata
        // advertises moves.
        unsafe {
            rdkafka::bindings::rd_kafka_mock_broker_set_host_port(
                cluster.native(),
                1,
                host.as_ptr(),
                port,
            );
        }
        let properties = [("bootstrap.servers", servers.as_str())];
        let hooks = crate::sink::SinkHooks::detached(SyncClock::new(TestClock::frozen() as _));
        let brokers = Brokers::new(raw(&properties, ()), raw(&properties, hooks), limit);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let io = multi_thread();
            let read = io.block_on(brokers.metadata(WORK));
            let _ = tx.send(read.map_err(|e| (e.code, e.bound)));
        });
        // Either code is one an expired read asks again on.
        match rx.recv_timeout(HANG) {
            Ok(Err((C::BrokerTransportFailure | C::OperationTimedOut, Some(bound))))
                if bound == limit => {}
            other => panic!("expected the bound's error within {HANG:?}, got {other:?}"),
        }
    }

    /// Pins that checks ending on their own, before any rejection or the
    /// bound, return at once on a clock that never moves, with the
    /// rejection watch and what it holds already dropped.
    #[test]
    fn checks_that_end_first_return_with_the_watch_dropped() {
        let clock = SyncClock::new(TestClock::frozen() as _);
        let held = Arc::new(());
        let watched = Arc::downgrade(&held);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let io = multi_thread();
            let step = Mutex::new("starting".to_owned());
            let got = io.block_on(bounded(
                &step,
                Duration::from_secs(600),
                clock,
                move || {
                    let _ = &held;
                    None
                },
                async { Ok::<_, ConnectError>(()) },
            ));
            let _ = tx.send((got, watched.upgrade().is_none()));
        });
        match rx.recv_timeout(HANG) {
            Ok((Ok(()), true)) => {}
            other => panic!(
                "expected the checks' result within {:?}, got {other:?}",
                HANG
            ),
        }
    }

    /// A broker that answers ApiVersions and drops the connection once the
    /// SASL mechanism handshake request arrives, as a broker or a proxy in
    /// front of it does when it restarts mid-handshake. Returns its address.
    #[cfg(feature = "tls")]
    fn drops_during_sasl_handshake() -> std::net::SocketAddr {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { return };
                std::thread::spawn(move || {
                    loop {
                        let mut len = [0u8; 4];
                        if s.read_exact(&mut len).is_err() {
                            return;
                        }
                        let mut req = vec![0u8; u32::from_be_bytes(len) as usize];
                        if s.read_exact(&mut req).is_err() {
                            return;
                        }
                        let key = i16::from_be_bytes([req[0], req[1]]);
                        let ver = i16::from_be_bytes([req[2], req[3]]);
                        let mut body = req[4..8].to_vec();
                        match (key, ver) {
                            (18, v) if v > 0 => {
                                body.extend_from_slice(&35i16.to_be_bytes());
                                body.extend_from_slice(&0i32.to_be_bytes());
                            }
                            (18, _) => {
                                body.extend_from_slice(&0i16.to_be_bytes());
                                let apis: [(i16, i16, i16); 3] =
                                    [(18, 0, 0), (17, 0, 0), (3, 0, 0)];
                                body.extend_from_slice(&(apis.len() as i32).to_be_bytes());
                                for (k, lo, hi) in apis {
                                    body.extend_from_slice(&k.to_be_bytes());
                                    body.extend_from_slice(&lo.to_be_bytes());
                                    body.extend_from_slice(&hi.to_be_bytes());
                                }
                            }
                            // SaslHandshake, or anything else: drop the connection.
                            _ => return,
                        }
                        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
                        frame.extend_from_slice(&body);
                        if s.write_all(&frame).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    /// Pins that a connection closed during the SASL mechanism handshake
    /// leaves connecting unavailable at the bound. ADR-0081 keeps that case
    /// retryable.
    #[cfg(feature = "tls")]
    #[test]
    fn a_connection_dropped_during_the_sasl_handshake_is_unavailable() {
        let addr = drops_during_sasl_handshake();
        let got = connect_to(
            &addr.to_string(),
            "    security.protocol: sasl_plaintext\n    sasl.mechanism: PLAIN\n    \
             sasl.username: u\n    sasl.password: p\n",
            Duration::from_secs(5),
        );
        match got {
            Some(ConnectError::Unavailable {
                reason: message, ..
            }) => {
                assert!(
                    message.starts_with("the startup checks took longer than"),
                    "{message}"
                )
            }
            other => panic!("expected unavailable, got {other:?}"),
        }
    }

    /// The passthrough of a client that logs in with SASL `PLAIN`.
    #[cfg(feature = "tls")]
    const SASL_PLAIN: &str = "    security.protocol: sasl_plaintext\n    sasl.mechanism: PLAIN\n    \
                              sasl.username: u\n    sasl.password: wrong\n";

    #[cfg(feature = "tls")]
    mod rejected_handshake {
        use super::*;

        /// A bound no test waits out.
        const LONG: Duration = Duration::from_secs(600);

        fn refused_within_deadline(brokers: &str, rdkafka: &str) -> String {
            match connect_to(brokers, rdkafka, LONG) {
                Some(ConnectError::Refused {
                    reason: message, ..
                }) => {
                    assert!(
                        message.starts_with("reading the metadata of the work topic `units`: "),
                        "{message}"
                    );
                    message
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        }

        fn refused_with_text(got: Option<ConnectError>) -> String {
            match got {
                Some(ConnectError::Refused {
                    reason: message, ..
                }) => {
                    assert!(
                        message.starts_with("reading the metadata of the work topic `units`: "),
                        "{message}"
                    );
                    message
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        }

        /// Pins that a broker refusing the SASL login refuses connecting the
        /// feed with the broker's text, without waiting out the bound.
        #[test]
        fn a_wrong_sasl_password_refuses_the_feed_at_connect() {
            let brokers = crate::testutil::refuses_sasl_login().to_string();
            let message = refused_with_text(connect_to(&brokers, SASL_PLAIN, LONG));
            assert!(message.contains("Authentication failed"), "{message}");
        }

        /// Pins that a broker refusing the SASL login refuses connecting the
        /// publisher with the broker's text, without waiting out the bound.
        #[test]
        fn a_wrong_sasl_password_refuses_the_publisher_at_connect() {
            let brokers = crate::testutil::refuses_sasl_login().to_string();
            let message = refused_with_text(connect_publisher_to(&brokers, SASL_PLAIN, LONG));
            assert!(message.contains("Authentication failed"), "{message}");
        }

        /// Pins that `KafkaFeedPublisher::connect` checks under the connect
        /// bound: a broker refusing the SASL login refuses it with the
        /// broker's text.
        #[test]
        fn the_public_publisher_connect_refuses_a_wrong_sasl_password() {
            let brokers = crate::testutil::refuses_sasl_login().to_string();
            let message = refused_with_text(connect_with_to(
                &brokers,
                SASL_PLAIN,
                LONG,
                |section, io, _| crate::feed::KafkaFeedPublisher::connect(section, io).err(),
            ));
            assert!(message.contains("Authentication failed"), "{message}");
        }

        /// Pins that connecting to a broker that answers the handshake with
        /// `handshake_failure` refuses with librdkafka's text, without
        /// waiting out the bound.
        #[test]
        fn a_handshake_the_broker_refuses_is_refused() {
            let brokers = spate_test::tls_alert_server(b"", 40).to_string();
            let message = refused_within_deadline(&brokers, "    security.protocol: ssl\n");
            assert!(message.contains("SSL alert number 40"), "{message}");
        }

        /// Pins that connecting to a broker whose certificate the client
        /// fails to verify refuses with librdkafka's text, without waiting
        /// out the bound.
        #[test]
        fn a_certificate_the_client_rejects_is_refused() {
            let server = spate_test_support::TestCa::new("feed-server-ca");
            let other = spate_test_support::TestCa::new("feed-other-ca");
            let dir = tempfile::tempdir().unwrap();
            let ca = other.write(dir.path());
            let io = multi_thread();
            let addr = io.block_on(spate_test_support::serve_tls(
                server.server_config(None),
                |_| async {},
            ));
            let message = refused_within_deadline(
                &addr.to_string(),
                &format!(
                    "    security.protocol: ssl\n    ssl.ca.location: \"{}\"\n",
                    ca.display()
                ),
            );
            assert!(message.contains("certificate verify failed"), "{message}");
        }

        /// Pins that checks refused by a rejected handshake have closed the
        /// metadata client by the time they return. Regression for #1809.
        #[test]
        fn refused_checks_close_their_clients_before_they_return() {
            let broker = spate_test::tls_alert_server(b"", 40).to_string();
            let got = checks_closing_their_clients(
                &[
                    ("bootstrap.servers", broker.as_str()),
                    ("security.protocol", "ssl"),
                ],
                LONG,
            );
            match got {
                Err(ConnectError::Refused {
                    reason: message, ..
                }) => {
                    assert!(message.contains("SSL alert number 40"), "{message}");
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        }
    }
}
