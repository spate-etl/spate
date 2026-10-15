//! The checks [`SqsFeed::connect`](crate::SqsFeed::connect) runs on the queue
//! before the feed starts.
//!
//! The queue must be a standard queue with a redrive policy, and the
//! dead-letter queue the policy names must be readable and keep messages
//! longer than the queue does. SQS counts a message's retention from its
//! first send and keeps that count when the message moves to the dead-letter
//! queue, so a dead-letter queue that does not outlive the source can delete
//! a unit as soon as it arrives. The checks run once, at connect.

use crate::error::{CallError, Class, SqsError};
use crate::queue::BoxFuture;
use serde::Deserialize;
use spate_core::config::ConfigError;
use std::collections::HashMap;
use url::Url;

/// The attributes read from the queue the feed reads. SQS refuses the whole
/// read when it names an attribute the queue does not have, so this names
/// none of a FIFO queue's own, such as `FifoQueue`.
const SOURCE_ATTRIBUTES: &[&str] = &[
    "QueueArn",
    "MessageRetentionPeriod",
    "VisibilityTimeout",
    "RedrivePolicy",
];

/// The attributes read from its dead-letter queue.
const DEAD_LETTER_ATTRIBUTES: &[&str] = &["MessageRetentionPeriod"];

/// Reads a queue's attributes by URL.
pub(crate) trait QueueAttributes: Send + Sync {
    /// The attributes `names` of the queue at `queue_url`, keyed by name. An
    /// attribute the queue does not report is absent.
    fn attributes<'a>(
        &'a self,
        queue_url: &'a str,
        names: &'static [&'static str],
    ) -> BoxFuture<'a, Result<HashMap<String, String>, CallError>>;
}

/// What the checks read, for the log line at connect.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Checked {
    pub(crate) arn: String,
    /// Seconds, as the queue reports it.
    pub(crate) retention: Option<u64>,
    /// The queue's own default, which each receive overrides.
    pub(crate) visibility_timeout: Option<String>,
    pub(crate) max_receive_count: u32,
    pub(crate) dead_letter_arn: String,
    pub(crate) dead_letter_retention: Option<u64>,
}

/// The queue's `RedrivePolicy` attribute.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RedrivePolicy {
    dead_letter_target_arn: String,
    max_receive_count: Count,
}

/// `maxReceiveCount`, which SQS accepts as a string and returns as a number.
#[derive(Deserialize)]
#[serde(untagged)]
enum Count {
    Number(u32),
    Text(String),
}

/// Run the startup checks on the queue at `queue_url`. `step` names the read
/// under way, for the connect bound's message.
///
/// # Errors
///
/// [`SqsError::Refused`] for a FIFO queue, a missing or unreadable redrive
/// policy, a dead-letter ARN that is not an SQS queue's, an unreadable
/// dead-letter queue, or one that keeps messages no longer than the queue;
/// [`SqsError::Config`] for a `queue_url` that is not a URL;
/// [`SqsError::Unavailable`] when a read keeps failing transiently.
pub(crate) async fn check(
    queue: &dyn QueueAttributes,
    queue_url: &str,
    step: &std::sync::Mutex<&'static str>,
) -> Result<Checked, SqsError> {
    let set = |s| {
        *step
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = s
    };
    set("reading the queue's attributes");
    let source = queue
        .attributes(queue_url, SOURCE_ATTRIBUTES)
        .await
        .map_err(|e| match e.class {
            Class::Fatal => SqsError::Refused(e.message),
            Class::Retryable => SqsError::Unavailable(e.message),
        })?;
    let arn = source.get("QueueArn").cloned().unwrap_or_default();
    // SQS requires a FIFO queue's name to end in `.fifo`.
    if arn.ends_with(".fifo") {
        return Err(SqsError::Refused(format!(
            "the queue {arn} is a FIFO queue; the SQS feed reads standard queues only"
        )));
    }
    let Some(policy) = source.get("RedrivePolicy").filter(|p| !p.is_empty()) else {
        return Err(SqsError::Refused(format!(
            "the queue {arn} has no redrive policy: give it a dead-letter queue, so a unit \
             that keeps failing moves there after `maxReceiveCount` receives"
        )));
    };
    let policy: RedrivePolicy = serde_json::from_str(policy).map_err(|e| {
        SqsError::Refused(format!(
            "the queue {arn} has a redrive policy the feed cannot read: {e}: {policy}"
        ))
    })?;
    let max_receive_count = match &policy.max_receive_count {
        Count::Number(n) => Some(*n),
        Count::Text(s) => s.parse().ok(),
    }
    .ok_or_else(|| {
        SqsError::Refused(format!(
            "the queue {arn} has a redrive policy whose `maxReceiveCount` is not a count"
        ))
    })?;
    let dead_letter_arn = policy.dead_letter_target_arn;
    let source_url = Url::parse(queue_url).map_err(|e| {
        SqsError::Config(ConfigError::Component {
            context: "feed.sqs".into(),
            message: format!("`queue_url` is not a URL: {e}"),
        })
    })?;
    let dead_letter = dead_letter_url(&source_url, &dead_letter_arn).ok_or_else(|| {
        SqsError::Refused(format!(
            "the queue {arn} has a redrive policy that names `{dead_letter_arn}`, which is \
             not an SQS queue ARN (arn:aws:sqs:<region>:<account>:<queue>)"
        ))
    })?;

    set("reading the dead-letter queue's attributes");
    let dead = queue
        .attributes(dead_letter.as_str(), DEAD_LETTER_ATTRIBUTES)
        .await
        .map_err(|e| match e.class {
            Class::Fatal => SqsError::Refused(format!(
                "cannot read the dead-letter queue {dead_letter_arn}: {}",
                e.message
            )),
            Class::Retryable => SqsError::Unavailable(format!(
                "reading the dead-letter queue {dead_letter_arn}: {}",
                e.message
            )),
        })?;

    let retention = seconds(&source);
    let dead_letter_retention = seconds(&dead);
    let unreported = match (retention, dead_letter_retention) {
        (Some(source), Some(dead)) if dead <= source => {
            return Err(SqsError::Refused(format!(
                "the dead-letter queue {dead_letter_arn} keeps messages for {dead} s, no longer \
                 than the queue {arn} keeps them ({source} s). SQS counts retention from a \
                 message's first send, so a unit moved there could be deleted at once; give \
                 the dead-letter queue a longer `MessageRetentionPeriod`, or the queue a \
                 shorter one"
            )));
        }
        (Some(_), Some(_)) => None,
        (None, Some(_)) => Some("the queue reported no MessageRetentionPeriod"),
        (Some(_), None) => Some("the dead-letter queue reported no MessageRetentionPeriod"),
        (None, None) => Some("neither queue reported a MessageRetentionPeriod"),
    };
    if let Some(unreported) = unreported {
        tracing::warn!(
            queue = %arn,
            dead_letter_queue = %dead_letter_arn,
            "{unreported}; the dead-letter retention check is skipped"
        );
    }
    Ok(Checked {
        arn,
        retention,
        visibility_timeout: source.get("VisibilityTimeout").cloned(),
        max_receive_count,
        dead_letter_arn,
        dead_letter_retention,
    })
}

/// `MessageRetentionPeriod` in seconds, or `None` when the queue does not
/// report it as a number.
fn seconds(attributes: &HashMap<String, String>) -> Option<u64> {
    attributes.get("MessageRetentionPeriod")?.parse().ok()
}

/// The URL of the dead-letter queue `arn` names: the source queue URL with
/// its last path segment replaced by the dead-letter queue's name, or `None`
/// when `arn` is not an SQS queue ARN of six `:`-separated fields.
///
/// SQS requires a dead-letter queue in the source queue's account and
/// region, and routes a read by the client's endpoint, so the source's
/// endpoint serves it and no `sqs:GetQueueUrl` is needed.
pub(crate) fn dead_letter_url(source: &Url, arn: &str) -> Option<Url> {
    let fields: Vec<&str> = arn.split(':').collect();
    let [arn_tag, _partition, service, _region, account, name] = fields.as_slice() else {
        return None;
    };
    if *arn_tag != "arn" || *service != "sqs" || account.is_empty() || name.is_empty() {
        return None;
    }
    // The dead-letter queue shares the source's account, so any account
    // segment ahead of the queue name already names it.
    let mut segments: Vec<&str> = source
        .path_segments()
        .map(|s| s.filter(|s| !s.is_empty()).collect())
        .unwrap_or_default();
    segments.pop();
    segments.push(name);
    let path: String = segments.iter().map(|s| format!("/{s}")).collect();
    let mut url = source.clone();
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Some(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const SOURCE: &str = "http://127.0.0.1:9324/000000000000/jobs";
    const DEAD_URL: &str = "http://127.0.0.1:9324/000000000000/jobs-dlq";
    const DEAD_ARN: &str = "arn:aws:sqs:elasticmq:000000000000:jobs-dlq";

    /// The attribute names `GetQueueAttributes` accepts on a standard queue.
    const STANDARD_NAMES: &[&str] = &[
        "All",
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesDelayed",
        "ApproximateNumberOfMessagesNotVisible",
        "CreatedTimestamp",
        "DelaySeconds",
        "KmsDataKeyReusePeriodSeconds",
        "KmsMasterKeyId",
        "LastModifiedTimestamp",
        "MaximumMessageSize",
        "MessageRetentionPeriod",
        "Policy",
        "QueueArn",
        "ReceiveMessageWaitTimeSeconds",
        "RedriveAllowPolicy",
        "RedrivePolicy",
        "SqsManagedSseEnabled",
        "VisibilityTimeout",
    ];

    /// The attribute names only a FIFO queue accepts.
    const FIFO_NAMES: &[&str] = &[
        "ContentBasedDeduplication",
        "DeduplicationScope",
        "FifoQueue",
        "FifoThroughputLimit",
    ];

    /// The error SQS answers a read naming an attribute the queue does not
    /// have with, classified as the feed classifies a real answer.
    fn invalid_attribute_name(name: &str) -> CallError {
        use aws_sdk_sqs::config::http::HttpResponse;
        use aws_sdk_sqs::error::{ErrorMetadata, SdkError};
        use aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesError;
        let meta = ErrorMetadata::builder()
            .code("InvalidAttributeName")
            .message(format!("Unknown Attribute {name}."))
            .build();
        let raw = HttpResponse::new(400.try_into().unwrap(), "".into());
        let e = SdkError::service_error(GetQueueAttributesError::generic(meta), raw);
        crate::error::classify("GetQueueAttributes", &e)
    }

    /// Attributes per queue URL, or the error reading that queue fails with.
    /// Like SQS, it fails a read that names an attribute the queue does not
    /// have, and a queue is FIFO when its `QueueArn` ends in `.fifo`.
    #[derive(Default)]
    struct Scripted {
        queues: HashMap<String, Result<HashMap<String, String>, CallError>>,
        read: Mutex<Vec<String>>,
    }

    impl Scripted {
        fn queue(mut self, url: &str, attributes: &[(&str, &str)]) -> Scripted {
            let attributes = attributes
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect();
            self.queues.insert(url.to_owned(), Ok(attributes));
            self
        }

        fn failing(mut self, url: &str, error: CallError) -> Scripted {
            self.queues.insert(url.to_owned(), Err(error));
            self
        }

        /// A source with a redrive policy to [`DEAD_ARN`], and the
        /// dead-letter queue, each with the retention given.
        fn pair(source_retention: Option<&str>, dead_retention: Option<&str>) -> Scripted {
            let policy = format!(r#"{{"deadLetterTargetArn":"{DEAD_ARN}","maxReceiveCount":5}}"#);
            let mut source = vec![
                ("QueueArn", "arn:aws:sqs:elasticmq:000000000000:jobs"),
                ("RedrivePolicy", policy.as_str()),
            ];
            source.extend(source_retention.map(|r| ("MessageRetentionPeriod", r)));
            let dead: Vec<_> = dead_retention
                .map(|r| ("MessageRetentionPeriod", r))
                .into_iter()
                .collect();
            Scripted::default()
                .queue(SOURCE, &source)
                .queue(DEAD_URL, &dead)
        }
    }

    impl QueueAttributes for Scripted {
        fn attributes<'a>(
            &'a self,
            queue_url: &'a str,
            names: &'static [&'static str],
        ) -> BoxFuture<'a, Result<HashMap<String, String>, CallError>> {
            self.read.lock().unwrap().push(queue_url.to_owned());
            let answer = match self.queues.get(queue_url) {
                Some(Ok(all)) => {
                    let fifo = all.get("QueueArn").is_some_and(|a| a.ends_with(".fifo"));
                    let accepted = |name: &&str| {
                        STANDARD_NAMES.contains(name) || (fifo && FIFO_NAMES.contains(name))
                    };
                    let unknown = names.iter().find(|name| !accepted(name));
                    match unknown {
                        Some(name) => Err(invalid_attribute_name(name)),
                        None => Ok(all
                            .iter()
                            .filter(|(k, _)| names.contains(&k.as_str()))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect()),
                    }
                }
                Some(Err(e)) => Err(e.clone()),
                None => Err(CallError::fatal("QueueDoesNotExist")),
            };
            Box::pin(async move { answer })
        }
    }

    fn run(queue: &Scripted) -> Result<Checked, SqsError> {
        let step = Mutex::new("starting");
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(check(queue, SOURCE, &step))
    }

    fn refusal(result: Result<Checked, SqsError>) -> String {
        match result {
            Err(SqsError::Refused(message)) => message,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// Pins that the dead-letter URL is the source queue URL with its last
    /// segment replaced by the dead-letter queue's name, with or without a
    /// path prefix or an account segment, and refuses an ARN that is not an
    /// SQS queue's.
    #[test]
    fn the_dead_letter_url_keeps_the_source_scheme_and_host() {
        let url = |s: &str| Url::parse(s).unwrap();
        assert_eq!(
            dead_letter_url(&url(SOURCE), DEAD_ARN).unwrap().as_str(),
            DEAD_URL
        );
        assert_eq!(
            dead_letter_url(
                &url("https://vpce-0abc-xyz.sqs.eu-west-2.vpce.amazonaws.com/111122223333/jobs"),
                "arn:aws:sqs:eu-west-2:111122223333:jobs-dlq"
            )
            .unwrap()
            .as_str(),
            "https://vpce-0abc-xyz.sqs.eu-west-2.vpce.amazonaws.com/111122223333/jobs-dlq"
        );
        assert_eq!(
            dead_letter_url(
                &url("http://127.0.0.1:9324/ctx/000000000000/jobs"),
                DEAD_ARN
            )
            .unwrap()
            .as_str(),
            "http://127.0.0.1:9324/ctx/000000000000/jobs-dlq"
        );
        assert_eq!(
            dead_letter_url(&url("http://127.0.0.1:9324/ctx/jobs"), DEAD_ARN)
                .unwrap()
                .as_str(),
            "http://127.0.0.1:9324/ctx/jobs-dlq"
        );
        assert_eq!(
            dead_letter_url(&url("http://127.0.0.1:9324/jobs"), DEAD_ARN)
                .unwrap()
                .as_str(),
            "http://127.0.0.1:9324/jobs-dlq"
        );
        for arn in [
            "arn:aws:sns:eu-west-2:111122223333:jobs-dlq",
            "arn:aws:sqs:eu-west-2:jobs-dlq",
            "arn:aws:sqs:eu-west-2:111122223333:jobs-dlq:extra",
            "arn:aws:sqs:eu-west-2::jobs-dlq",
        ] {
            assert!(dead_letter_url(&url(SOURCE), arn).is_none(), "{arn}");
        }
    }

    /// Pins the retention comparison: a dead-letter queue that keeps messages
    /// as long as the source, or less, is refused; a longer one passes. The
    /// refusal offers a shorter source retention too, the only fix when both
    /// queues keep messages for SQS's 14-day maximum.
    #[test]
    fn startup_refuses_a_dead_letter_queue_that_does_not_outlive_the_source() {
        let message = refusal(run(&Scripted::pair(Some("345600"), Some("345600"))));
        assert!(message.contains("no longer"), "{message}");
        let message = refusal(run(&Scripted::pair(Some("1209600"), Some("1209600"))));
        assert!(message.contains("or the queue a shorter one"), "{message}");
        let message = refusal(run(&Scripted::pair(Some("345600"), Some("345599"))));
        assert!(message.contains("no longer"), "{message}");
        let checked = run(&Scripted::pair(Some("345600"), Some("1209600"))).unwrap();
        assert_eq!(checked.retention, Some(345_600));
        assert_eq!(checked.dead_letter_retention, Some(1_209_600));
        assert_eq!(checked.max_receive_count, 5);
        assert_eq!(checked.dead_letter_arn, DEAD_ARN);
    }

    /// Pins that a queue that reports no retention skips the comparison, on
    /// either side.
    #[test]
    fn a_missing_retention_skips_the_comparison() {
        for (source, dead) in [(None, Some("60")), (Some("1209600"), None), (None, None)] {
            let checked = run(&Scripted::pair(source, dead)).unwrap();
            assert_eq!(checked.retention, source.map(|s| s.parse().unwrap()));
        }
    }

    /// Pins the warning a missing retention logs: one line, naming the queue
    /// that reported none and both ARNs, saying the comparison is skipped.
    #[test]
    fn a_missing_retention_warns_naming_the_queue_that_reported_none() {
        for (source, dead, reported) in [
            (
                None,
                Some("60"),
                "the queue reported no MessageRetentionPeriod",
            ),
            (
                Some("1209600"),
                None,
                "the dead-letter queue reported no MessageRetentionPeriod",
            ),
            (
                None,
                None,
                "neither queue reported a MessageRetentionPeriod",
            ),
        ] {
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                run(&Scripted::pair(source, dead)).unwrap();
            });
            assert_eq!(lines.len(), 1, "{lines:?}");
            let line = &lines[0];
            for needle in [
                "WARN",
                reported,
                "the dead-letter retention check is skipped",
                "arn:aws:sqs:elasticmq:000000000000:jobs",
                DEAD_ARN,
            ] {
                assert!(line.contains(needle), "{needle}: {line}");
            }
        }
    }

    /// Pins the refusals of the source queue, each by its own message: FIFO,
    /// told by the `.fifo` name in its ARN, no redrive policy, a policy the
    /// feed cannot read, a `maxReceiveCount` that is not a count, and a
    /// dead-letter ARN that is not an SQS queue's. `maxReceiveCount` is read
    /// as a number or as a string.
    #[test]
    fn startup_refuses_a_queue_the_feed_cannot_dead_letter_from() {
        let arn = ("QueueArn", "arn:aws:sqs:elasticmq:000000000000:jobs");
        let fifo = Scripted::default().queue(
            SOURCE,
            &[
                ("QueueArn", "arn:aws:sqs:elasticmq:000000000000:jobs.fifo"),
                ("FifoQueue", "true"),
            ],
        );
        assert!(refusal(run(&fifo)).contains("FIFO"));

        let none = Scripted::default().queue(SOURCE, &[arn]);
        assert!(refusal(run(&none)).contains("no redrive policy"));

        for (policy, refused) in [
            ("not json", "a redrive policy the feed cannot read"),
            (
                r#"{"maxReceiveCount":5}"#,
                "a redrive policy the feed cannot read",
            ),
            (
                r#"{"deadLetterTargetArn":"arn:aws:sqs:r:1:q","maxReceiveCount":"five"}"#,
                "`maxReceiveCount` is not a count",
            ),
            (
                r#"{"deadLetterTargetArn":"arn:aws:sns:r:1:q","maxReceiveCount":5}"#,
                "not an SQS queue ARN",
            ),
        ] {
            let bad = Scripted::default().queue(SOURCE, &[arn, ("RedrivePolicy", policy)]);
            let message = refusal(run(&bad));
            assert!(message.contains(refused), "{policy}: {message}");
        }

        for count in ["5", r#""5""#] {
            let policy =
                format!(r#"{{"deadLetterTargetArn":"{DEAD_ARN}","maxReceiveCount":{count}}}"#);
            let queue = Scripted::default()
                .queue(SOURCE, &[arn, ("RedrivePolicy", &policy)])
                .queue(DEAD_URL, &[]);
            assert_eq!(run(&queue).unwrap().max_receive_count, 5, "{count}");
        }
    }

    /// Pins how a failed read of each queue maps: Fatal is a refusal, and a
    /// read of the dead-letter queue names it; Retryable is unavailable.
    #[test]
    fn a_failed_read_is_refused_when_fatal_and_unavailable_when_retryable() {
        let source = Scripted::default().failing(SOURCE, CallError::fatal("AccessDenied"));
        assert!(refusal(run(&source)).contains("AccessDenied"));

        let slow_source = Scripted::default().failing(SOURCE, CallError::retryable("throttled"));
        match run(&slow_source) {
            Err(SqsError::Unavailable(message)) => assert!(message.contains("throttled")),
            other => panic!("expected unavailable, got {other:?}"),
        }

        let dead = Scripted::pair(None, None).failing(DEAD_URL, CallError::fatal("AccessDenied"));
        let message = refusal(run(&dead));
        assert!(
            message.contains(&format!("cannot read the dead-letter queue {DEAD_ARN}")),
            "{message}"
        );
        assert_eq!(*dead.read.lock().unwrap(), [SOURCE, DEAD_URL]);

        let slow = Scripted::pair(None, None).failing(DEAD_URL, CallError::retryable("throttled"));
        match run(&slow) {
            Err(SqsError::Unavailable(message)) => assert!(message.contains("throttled")),
            other => panic!("expected unavailable, got {other:?}"),
        }
    }
}
