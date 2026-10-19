//! The checks [`KafkaFeed::connect`](crate::feed::KafkaFeed::connect) runs
//! on the feed's topics before the feed starts.
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

use super::{KafkaFeedError, Topics};
use crate::sink::SinkContext;
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{Producer, ThreadedProducer};
use spate_core::error::ErrorClass;
use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::{Duration, Instant};

/// How long the checks may take, every step included.
pub(crate) const CONNECT_BOUND: Duration = Duration::from_secs(30);

/// How often [`bounded`] asks whether the client recorded a rejection.
const REJECTION_TICK: Duration = Duration::from_millis(50);

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
}

impl AdminError {
    pub(crate) fn new(code: RDKafkaErrorCode, message: impl Into<String>) -> AdminError {
        AdminError {
            code,
            message: message.into(),
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
    ) -> BoxFuture<'a, Result<HashMap<String, Option<String>>, AdminError>>;

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
pub(crate) struct Brokers {
    admin: Arc<AdminClient<DefaultClientContext>>,
    reader: Arc<ThreadedProducer<SinkContext>>,
}

impl Brokers {
    /// Wraps `admin` and `reader`, which must both be built with
    /// `allow.auto.create.topics` off: with it on, a metadata read of a
    /// missing topic creates the topic with the broker's defaults.
    pub(crate) fn new(
        admin: AdminClient<DefaultClientContext>,
        reader: ThreadedProducer<SinkContext>,
    ) -> Brokers {
        Brokers {
            admin: Arc::new(admin),
            reader: Arc::new(reader),
        }
    }
}

fn admin_error(e: &KafkaError) -> AdminError {
    let code = e.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail);
    AdminError::new(code, e.to_string())
}

impl TopicAdmin for Brokers {
    fn metadata<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Result<usize, AdminError>> {
        // librdkafka refuses a topic name longer than 512 bytes with a NULL
        // handle, which rdkafka's `fetch_metadata` unwraps, panicking the
        // read's thread.
        if topic.len() > 512 {
            return Box::pin(std::future::ready(Err(AdminError::new(
                RDKafkaErrorCode::InvalidTopic,
                "the topic name is longer than 512 bytes",
            ))));
        }
        let reader = Arc::clone(&self.reader);
        let name = topic.to_owned();
        let (tx, rx) = tokio::sync::oneshot::channel();
        // `fetch_metadata` blocks, so it runs on a thread of its own, which
        // ends with the read at most `CONNECT_BOUND` later even when the
        // connect bound has already passed.
        let spawned = std::thread::Builder::new()
            .name("spate-kafka-feed-check".to_owned())
            .spawn(move || {
                let read = reader
                    .client()
                    .fetch_metadata(Some(&name), CONNECT_BOUND)
                    .map_err(|e| admin_error(&e))
                    .and_then(|metadata| {
                        let found = metadata.topics().iter().find(|t| t.name() == name);
                        match found.map(|t| (t.error(), t.partitions().len())) {
                            None => Err(AdminError::new(
                                RDKafkaErrorCode::UnknownTopicOrPartition,
                                "the metadata response does not list the topic",
                            )),
                            Some((None, partitions)) => Ok(partitions),
                            Some((Some(err), _)) => {
                                let code = RDKafkaErrorCode::from(err);
                                Err(AdminError::new(code, code.to_string()))
                            }
                        }
                    });
                let _ = tx.send(read);
            });
        Box::pin(async move {
            spawned.map_err(|e| {
                AdminError::new(RDKafkaErrorCode::Fail, format!("starting a thread: {e}"))
            })?;
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
    ) -> BoxFuture<'a, Result<HashMap<String, Option<String>>, AdminError>> {
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
        self.reader
            .context()
            .rejection_within(Instant::now(), Duration::MAX)
            .map(|(_, text)| text)
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

/// Whether `code` means asking again fails the same way, by the table the
/// source classifies its consumer errors with: an authorization or
/// authentication failure, an invalid topic name, or a version the broker
/// does not support.
///
/// A read never fails with a bad credential's code or a rejected TLS
/// handshake: librdkafka keeps a broker that failed either out of use, and
/// the read waits for a usable one. The client's error callback reports the
/// rejection instead, which [`TopicAdmin::rejection`] returns.
fn refused(code: RDKafkaErrorCode) -> bool {
    crate::error::classify_consumer_error(code, false) == ErrorClass::Fatal
}

/// The refusal for a rejection recorded while `step` was under way.
fn rejected(step: &str, text: &str) -> KafkaFeedError {
    KafkaFeedError::Refused(format!(
        "{step}: the client's credential or TLS handshake was rejected: {text}"
    ))
}

/// `e`, from the step `doing`, as the error `connect` returns: a refusal
/// when `admin` recorded a rejection, whatever the read's own error.
fn classified(admin: &dyn TopicAdmin, doing: &str, e: &AdminError) -> KafkaFeedError {
    if let Some(text) = admin.rejection() {
        return rejected(doing, &text);
    }
    let message = format!("{doing}: {}", e.message);
    if refused(e.code) {
        KafkaFeedError::Refused(message)
    } else {
        KafkaFeedError::Unavailable(message)
    }
}

/// Run the startup checks on `topics`. `step` names the read under way, for
/// the connect bound's message.
///
/// # Errors
///
/// [`KafkaFeedError::Refused`] for a missing topic, a compacted work, retry
/// or dead-letter topic, an attempt topic whose `cleanup.policy` is anything
/// but `compact` or that reports no partitions, a work topic whose
/// `message.timestamp.type` is anything but `LogAppendTime`, a read that
/// fails the same way when asked again, a failed read after the client
/// recorded a rejected credential or TLS handshake, and a topic the cluster
/// describes no configuration entry for; [`KafkaFeedError::Unavailable`] for
/// a read that fails otherwise.
pub(crate) async fn check(
    admin: &dyn TopicAdmin,
    topics: &Topics,
    scope: Scope,
    step: &Mutex<String>,
) -> Result<Checked, KafkaFeedError> {
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
                return Err(KafkaFeedError::Refused(format!(
                    "the {role} topic `{topic}` does not exist; the feed never creates a \
                     topic, so create it before the first worker starts"
                )));
            }
            Err(e) => return Err(classified(admin, &doing, &e)),
        }
    }

    let Some(attempt_partitions) = u32::try_from(attempt_partitions)
        .ok()
        .and_then(NonZeroU32::new)
    else {
        return Err(KafkaFeedError::Refused(format!(
            "the attempt topic `{}` reports no partitions",
            topics.attempt
        )));
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
            return Err(KafkaFeedError::Refused(format!(
                "{doing}: the cluster described no configuration entry for the topic, as it \
                 does for a client without the `DescribeConfigs` operation on it"
            )));
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
            return Err(KafkaFeedError::Refused(format!(
                "the {role} topic `{topic}` is compacted (`{CLEANUP_POLICY}` is `{policy}`). \
                 Compaction keeps only the latest message for each key, and a unit's id is \
                 its key, so it can delete a unit nobody has read; set `{CLEANUP_POLICY}` \
                 to `delete`"
            )));
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
            return Err(KafkaFeedError::Refused(format!(
                "the attempt topic `{}` must be compacted only (`{CLEANUP_POLICY}` is \
                 `{policy}`). Time or size retention would delete the attempt counts of units \
                 still in flight; create it with `{CLEANUP_POLICY}=compact`",
                topics.attempt
            )));
        }
    }
    match &checked.timestamp_type {
        None => tracing::warn!(
            topic = &*topics.work,
            "the work topic reported no `{TIMESTAMP_TYPE}`; the timestamp check is skipped"
        ),
        Some(kind) if kind == LOG_APPEND_TIME => {}
        Some(kind) => {
            return Err(KafkaFeedError::Refused(format!(
                "the work topic `{}` keeps each message's own timestamp (`{TIMESTAMP_TYPE}` is \
                 `{kind}`). A store can write storage notifications to it with timestamp 0, \
                 as RustFS does, and time retention deletes such messages at its next check, \
                 read or not; create it with `{TIMESTAMP_TYPE}={LOG_APPEND_TIME}`",
                topics.work
            )));
        }
    }
    Ok(checked)
}

/// `checks` under a bound of `limit`, ended early by a rejection
/// `rejection` returns. Past the bound, an [`KafkaFeedError::Unavailable`]
/// naming the step `step` holds.
///
/// The bound is timed on a thread of its own, so it fires on any runtime:
/// one whose timer nothing drives, as under `Handle::block_on` on a
/// current-thread runtime, one with timers disabled, and one that has shut
/// down. The thread calls `rejection` every [`REJECTION_TICK`], and ends
/// when `checks` does.
///
/// # Errors
///
/// `checks`' own error; [`KafkaFeedError::Refused`] naming the step and the
/// rejection once `rejection` returns one; [`KafkaFeedError::Unavailable`]
/// past the bound, or when the system cannot start the thread.
pub(crate) async fn bounded<T>(
    step: &Mutex<String>,
    limit: Duration,
    rejection: impl Fn() -> Option<String> + Send + 'static,
    checks: impl Future<Output = Result<T, KafkaFeedError>>,
) -> Result<T, KafkaFeedError> {
    // `Some` carries a rejection's text, `None` the bound passing.
    let (fire, mut fired) = tokio::sync::oneshot::channel::<Option<String>>();
    // Held until `checks` ends: dropping it ends the timing thread.
    let (_finished, done) = std::sync::mpsc::channel::<()>();
    let deadline = Instant::now() + limit;
    std::thread::Builder::new()
        .name("spate-kafka-feed-bound".to_owned())
        .spawn(move || {
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                if done.recv_timeout(left.min(REJECTION_TICK)) != Err(RecvTimeoutError::Timeout) {
                    return;
                }
                // Asked for first, so a rejection recorded by the bound
                // refuses.
                if let Some(text) = rejection() {
                    let _ = fire.send(Some(text));
                    return;
                }
                if Instant::now() >= deadline {
                    let _ = fire.send(None);
                    return;
                }
            }
        })
        .map_err(|e| KafkaFeedError::Unavailable(format!("starting a thread: {e}")))?;
    let mut checks = std::pin::pin!(checks);
    std::future::poll_fn(|cx| {
        if let Poll::Ready(result) = checks.as_mut().poll(cx) {
            return Poll::Ready(result);
        }
        Pin::new(&mut fired).poll(cx).map(|fired| {
            let step = step.lock().unwrap_or_else(PoisonError::into_inner).clone();
            match fired.ok().flatten() {
                Some(text) => Err(rejected(&step, &text)),
                None => Err(KafkaFeedError::Unavailable(format!(
                    "the startup checks took longer than {limit:?}, while {step}"
                ))),
            }
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use RDKafkaErrorCode as C;

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
    fn broker_defaults() -> HashMap<String, Option<String>> {
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
        topics: HashMap<String, HashMap<String, Option<String>>>,
        /// Partition counts by topic; one where a topic has none here.
        partitions: HashMap<String, usize>,
        metadata_errors: HashMap<String, AdminError>,
        config_errors: HashMap<String, AdminError>,
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
        ) -> BoxFuture<'a, Result<HashMap<String, Option<String>>, AdminError>> {
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
    fn config() -> crate::feed::config::KafkaFeedConfig {
        let section = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: localhost:9092\n  group_id: workers\n  topic: {WORK}\n  \
             retry_topic: {RETRY}\n  dead_letter_topic: {DEAD}\n  attempt_topic: {ATTEMPTS}\n"
        ))
        .unwrap();
        crate::feed::config::KafkaFeedConfig::from_section(&section).unwrap()
    }

    fn run(admin: &Scripted) -> Result<Checked, KafkaFeedError> {
        let step = Mutex::new("starting".to_owned());
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(check(admin, &topics(), Scope::All, &step))
    }

    fn refusal(result: Result<Checked, KafkaFeedError>) -> String {
        match result {
            Err(KafkaFeedError::Refused(message)) => message,
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
                Err(KafkaFeedError::Unavailable(message)) => assert!(
                    message.contains("reading the configuration of the work topic `units`: slow"),
                    "{message}"
                ),
                other => panic!("{code:?}: expected unavailable, got {other:?}"),
            }
        }
    }

    /// A bound short enough that a test waits on it, never on the real one.
    const SHORT: Duration = Duration::from_millis(20);

    /// How long a test waits for the short bound to fire.
    const DEADLINE: Duration = Duration::from_secs(10);

    /// The checks under [`SHORT`] on a thread of their own, as `connect`
    /// runs them: through `Handle::block_on` on `io`, with the read of
    /// `hangs` never answering. `None` when nothing came back within
    /// [`DEADLINE`].
    fn bounded_on(
        io: tokio::runtime::Handle,
        hangs: (&'static str, bool),
    ) -> Option<Result<Checked, KafkaFeedError>> {
        bounded_with(io, hangs, SHORT, None)
    }

    /// [`bounded_on`] under a bound of `limit`, with `rejection` as the
    /// rejection the client recorded.
    fn bounded_with(
        io: tokio::runtime::Handle,
        hangs: (&'static str, bool),
        limit: Duration,
        rejection: Option<&'static str>,
    ) -> Option<Result<Checked, KafkaFeedError>> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let admin = Scripted {
                hangs: Some(hangs),
                ..Scripted::all()
            };
            let step = Mutex::new("starting".to_owned());
            let topics = topics();
            let checks = check(&admin, &topics, Scope::All, &step);
            let rejection = move || rejection.map(str::to_owned);
            let _ = tx.send(io.block_on(bounded(&step, limit, rejection, checks)));
        });
        rx.recv_timeout(DEADLINE).ok()
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
            match bounded_on(io.handle().clone(), hangs) {
                Some(Err(KafkaFeedError::Unavailable(message))) => {
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
            match bounded_on(io, (WORK, true)) {
                Some(Err(KafkaFeedError::Unavailable(message))) => assert!(
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
            admin.topics.insert(topic.to_owned(), HashMap::new());
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
            Err(KafkaFeedError::Unavailable(message)) => {
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

    /// Pins that a rejection the client records ends the checks with a
    /// refusal naming the read under way and the rejection, long before
    /// the bound.
    #[test]
    fn a_recorded_rejection_ends_the_checks_before_the_bound() {
        let io = multi_thread();
        let limit = Duration::from_secs(600);
        match bounded_with(io.handle().clone(), (RETRY, true), limit, Some(ALERT_40)) {
            Some(Err(KafkaFeedError::Refused(message))) => {
                assert!(
                    message.starts_with("reading the metadata of the retry topic `units-retry`: "),
                    "{message}"
                );
                assert!(message.ends_with(ALERT_40), "{message}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// `KafkaFeed::connect` against `brokers` with the passthrough
    /// `rdkafka`, under a bound of `limit`, on a thread of its own. `None`
    /// when nothing came back within [`DEADLINE`].
    fn connect_to(brokers: &str, rdkafka: &str, limit: Duration) -> Option<KafkaFeedError> {
        let section: spate_core::config::FeedSection = serde_yaml::from_str(&format!(
            "kafka:\n  brokers: \"{brokers}\"\n  group_id: workers\n  topic: {WORK}\n  \
             retry_topic: {RETRY}\n  dead_letter_topic: {DEAD}\n  attempt_topic: {ATTEMPTS}\n  \
             statistics_interval: 0s\n  rdkafka:\n{rdkafka}"
        ))
        .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let io = multi_thread();
            let connected =
                crate::feed::KafkaFeed::connect_with(&section, io.handle(), Scope::All, limit);
            let _ = tx.send(connected.err());
        });
        rx.recv_timeout(DEADLINE)
            .unwrap_or_else(|_| panic!("connect did not return within {DEADLINE:?}"))
    }

    /// Pins that a broker that refuses the connection is unavailable at the
    /// bound, and records no rejection.
    #[test]
    fn a_refused_connection_is_unavailable_at_the_bound() {
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let brokers = closed.local_addr().unwrap().to_string();
        drop(closed);
        match connect_to(&brokers, "", Duration::from_secs(2)) {
            Some(KafkaFeedError::Unavailable(message)) => assert!(
                message.starts_with("the startup checks took longer than 2s"),
                "{message}"
            ),
            other => panic!("expected unavailable, got {other:?}"),
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
    /// leaves connecting unavailable at the bound. ADR-0047 keeps that case
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
            Some(KafkaFeedError::Unavailable(message)) => {
                assert!(
                    message.starts_with("the startup checks took longer than"),
                    "{message}"
                )
            }
            other => panic!("expected unavailable, got {other:?}"),
        }
    }

    #[cfg(feature = "tls")]
    mod rejected_handshake {
        use super::*;

        /// A bound no test waits out.
        const LONG: Duration = Duration::from_secs(600);

        fn refused_within_deadline(brokers: &str, rdkafka: &str) -> String {
            match connect_to(brokers, rdkafka, LONG) {
                Some(KafkaFeedError::Refused(message)) => {
                    assert!(
                        message.starts_with("reading the metadata of the work topic `units`: "),
                        "{message}"
                    );
                    message
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
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
    }
}
