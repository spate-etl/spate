//! The I/O half of the Kafka sink: produce a sealed batch and await every
//! delivery report.
//!
//! `write_batch` runs in two phases. First it parses **all** frames back
//! into messages, validating the connector's framing before anything is
//! enqueued and fixing the delivery countdown's total up front (counting
//! per send would race reports arriving mid-loop). Then it produces each
//! message, absorbing librdkafka queue-full pushback with a bounded async
//! backoff, and awaits the countdown under a deadline derived from the
//! configured delivery timeout. Only when every report confirmed does it
//! return `Ok`, which is the framework's durable-ack point.
//!
//! Failure semantics are honest at-least-once: a retryable error (report
//! timeout, broker transport failure) makes the framework retry the whole
//! sealed batch, re-producing any already-delivered prefix, so duplicates
//! are possible and loss is not. Errors that idempotence or configuration cannot heal
//! (authorization, unknown topic, a fenced idempotent producer) are fatal:
//! the batch is abandoned, the watermark stalls, and the pipeline fails
//! fast instead of spinning. A timed-out delivery is fatal too while the
//! producer's latest SASL or TLS rejection by a broker is inside the
//! rejection window, and its reason carries the rejection's text.

use crate::sink::context::{BatchInflight, SinkContext};
use crate::sink::frame::{FrameParser, MessageRef};
use crate::sink::metrics::KafkaSinkStatsMetrics;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use spate_core::error::{ErrorClass, SinkError};
use spate_core::metrics::Meter;
use spate_core::sink::{SealedBatch, ShardWriter};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Extra time granted past `delivery_timeout` for the countdown await:
/// `delivery.timeout.ms` bounds time-to-report only from each message's
/// *enqueue*, and the send loop itself may have spent time in queue-full
/// backoff.
pub(crate) const DELIVERY_GRACE: Duration = Duration::from_secs(5);

/// Queue-full backoff bounds: librdkafka signals queue-full synchronously
/// from `send`; the writer sleeps and retries that message while the
/// producer's poll thread drains the queue.
const QUEUE_FULL_BACKOFF_INITIAL: Duration = Duration::from_millis(10);
const QUEUE_FULL_BACKOFF_MAX: Duration = Duration::from_millis(250);

/// How long a readiness probe waits for topic metadata.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// One connected producer endpoint. The sink's shards all clone the same
/// underlying producer (see the config module: statistics soundness and
/// librdkafka's own broker routing both want exactly one client), so this
/// is a cheap `Arc`-backed handle.
pub struct KafkaEndpoint {
    producer: ThreadedProducer<SinkContext>,
    label: String,
}

impl KafkaEndpoint {
    pub(crate) fn new(producer: ThreadedProducer<SinkContext>, label: String) -> Self {
        KafkaEndpoint { producer, label }
    }

    pub(crate) fn label(&self) -> &str {
        &self.label
    }
}

impl Clone for KafkaEndpoint {
    fn clone(&self) -> Self {
        KafkaEndpoint {
            producer: self.producer.clone(),
            label: self.label.clone(),
        }
    }
}

impl std::fmt::Debug for KafkaEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaEndpoint")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

/// The Kafka sink's [`ShardWriter`]: parses the connector's framed
/// messages out of a sealed batch, produces them to the configured topic,
/// and treats "every delivery report confirmed" as the durable write.
/// Built by the sink's config factory; cheap to clone.
#[derive(Clone, Debug)]
pub struct KafkaWriter {
    /// How long a rejection by a broker makes a timed-out delivery fatal.
    rejection_window: Duration,
    topic: String,
    delivery_timeout: Duration,
    /// Shared with the main producer's context, which publishes through
    /// it; filled by [`attach_metrics`](ShardWriter::attach_metrics).
    stats_slot: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>,
    statistics_enabled: bool,
}

impl KafkaWriter {
    pub(crate) fn new(
        rejection_window: Duration,
        topic: String,
        delivery_timeout: Duration,
        stats_slot: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>,
        statistics_enabled: bool,
    ) -> Self {
        KafkaWriter {
            rejection_window,
            topic,
            delivery_timeout,
            stats_slot,
            statistics_enabled,
        }
    }

    /// The latest rejection recorded by `endpoint`'s producer, when it is
    /// inside the rejection window now.
    fn rejection(&self, endpoint: &KafkaEndpoint) -> Option<(Duration, String)> {
        endpoint
            .producer
            .context()
            .rejection_within(Instant::now(), self.rejection_window)
    }

    fn parse_messages<'a>(&self, batch: &'a SealedBatch) -> Result<Vec<MessageRef<'a>>, SinkError> {
        let mut messages = Vec::with_capacity(batch.rows as usize);
        for frame in &batch.frames {
            for message in FrameParser::new(frame) {
                messages.push(message.map_err(|e| SinkError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!("corrupted sink frame (framework bug): {e}"),
                })?);
            }
        }
        if messages.len() as u64 != batch.rows {
            return Err(SinkError::Client {
                class: ErrorClass::Fatal,
                reason: format!(
                    "corrupted sink frame (framework bug): batch claims {} rows \
                     but frames parse to {} messages",
                    batch.rows,
                    messages.len()
                ),
            });
        }
        Ok(messages)
    }
}

impl ShardWriter for KafkaWriter {
    type Endpoint = KafkaEndpoint;

    /// Resolve the `spate_kafka_sink_*` statistics handles into the slot the
    /// producer context publishes through. No-op when `statistics_interval`
    /// is zero; disabled statistics register no families.
    fn attach_metrics(&mut self, meter: Option<Meter>) {
        if !self.statistics_enabled {
            return;
        }
        if let Some(meter) = meter {
            *self.stats_slot.lock().expect("stats slot lock") =
                Some(KafkaSinkStatsMetrics::new(meter));
        }
    }

    async fn write_batch(
        &self,
        endpoint: &KafkaEndpoint,
        batch: &SealedBatch,
    ) -> Result<(), SinkError> {
        // Phase 1: validate framing and fix the countdown total before any
        // message is enqueued.
        let messages = self.parse_messages(batch)?;
        let total = messages.len();
        let inflight = Arc::new(BatchInflight::new(total));
        let send_deadline = Instant::now() + self.delivery_timeout + DELIVERY_GRACE;

        // Phase 2: produce, absorbing queue-full pushback bounded by the
        // deadline. On a definitive send error librdkafka returns the
        // record (and its opaque) without a future report, so the batch
        // fails immediately; already-enqueued messages report into a
        // countdown nothing awaits, which is harmless.
        for message in &messages {
            let mut record: BaseRecord<'_, [u8], [u8], Arc<BatchInflight>> =
                BaseRecord::with_opaque_to(&self.topic, Arc::clone(&inflight));
            if let Some(key) = message.key {
                record = record.key(key);
            }
            if let Some(payload) = message.payload {
                record = record.payload(payload);
            }
            if !message.headers.is_empty() {
                let mut headers = OwnedHeaders::new_with_capacity(message.headers.len());
                for (name, value) in &message.headers {
                    headers = headers.insert(Header {
                        key: name,
                        value: Some(*value),
                    });
                }
                record = record.headers(headers);
            }

            let mut backoff = QUEUE_FULL_BACKOFF_INITIAL;
            loop {
                match endpoint.producer.send(record) {
                    Ok(()) => break,
                    Err((error, returned))
                        if error.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull) =>
                    {
                        if Instant::now() >= send_deadline {
                            return Err(SinkError::Client {
                                class: ErrorClass::Retryable,
                                reason: format!(
                                    "producer queue stayed full past the delivery \
                                     deadline ({:?} + {:?} grace); the batch will be \
                                     retried",
                                    self.delivery_timeout, DELIVERY_GRACE
                                ),
                            });
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(QUEUE_FULL_BACKOFF_MAX);
                        record = returned;
                    }
                    Err((error, _returned)) => {
                        return Err(classify(
                            error.rdkafka_error_code(),
                            &format!("produce to \"{}\" failed: {error}", self.topic),
                        ));
                    }
                }
            }
        }

        // Phase 3: the durable-ack point, where every report must confirm.
        // Re-anchor the deadline: `delivery.timeout.ms` bounds each message's
        // time-to-report from *its own* enqueue, and the send loop above may
        // have spent the whole send window in queue-full backoff, so the last
        // messages were only just enqueued. A shared start-anchored deadline
        // would time the wait out while they were still legitimately in flight
        // and force a duplicate-producing retry under sustained queue-full.
        let wait_deadline = Instant::now() + self.delivery_timeout + DELIVERY_GRACE;
        if !inflight.wait(wait_deadline).await {
            return Err(report_error(
                Some(RDKafkaErrorCode::MessageTimedOut),
                &format!(
                    "delivery reports missing past the deadline ({:?} + {:?} \
                     grace)",
                    self.delivery_timeout, DELIVERY_GRACE
                ),
                self.rejection(endpoint),
            ));
        }
        if let Some((code, reason)) = inflight.first_error() {
            return Err(report_error(
                *code,
                &format!(
                    "{} of {total} delivery reports failed; first: {reason}",
                    inflight.failed()
                ),
                self.rejection(endpoint),
            ));
        }
        Ok(())
    }

    /// Readiness: fetch the topic's metadata on a blocking thread (the
    /// underlying call blocks) and fail fast on an unknown topic. Runs
    /// against a probe-only producer; see the config module.
    async fn probe(&self, endpoint: &KafkaEndpoint) -> Result<(), SinkError> {
        let producer = endpoint.producer.clone();
        let topic = self.topic.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let metadata = producer
                .client()
                .fetch_metadata(Some(&topic), PROBE_TIMEOUT)
                .map_err(|e| classify(e.rdkafka_error_code(), &format!("metadata fetch: {e}")))?;
            let Some(meta) = metadata.topics().iter().find(|t| t.name() == topic) else {
                return Err(SinkError::Client {
                    class: ErrorClass::Retryable,
                    reason: format!("metadata response omitted topic \"{topic}\""),
                });
            };
            if let Some(err) = meta.error() {
                let code: RDKafkaErrorCode = err.into();
                return Err(classify(
                    Some(code),
                    &format!("topic \"{topic}\" metadata error: {code}"),
                ));
            }
            if meta.partitions().is_empty() {
                return Err(SinkError::Client {
                    class: ErrorClass::Retryable,
                    reason: format!("topic \"{topic}\" reports no partitions yet"),
                });
            }
            Ok(())
        })
        .await;
        let outcome = outcome.unwrap_or_else(|join_error| {
            Err(SinkError::Client {
                class: ErrorClass::Retryable,
                reason: format!("probe task failed: {join_error}"),
            })
        });
        match (outcome, self.rejection(endpoint)) {
            (Err(SinkError::Client { class, reason }), Some(rejection)) => Err(SinkError::Client {
                class,
                reason: with_rejection(&reason, &rejection),
            }),
            (outcome, _) => outcome,
        }
    }
}

/// Map a failed delivery report onto the framework taxonomy.
///
/// A `MessageTimedOut` report is [`ErrorClass::Fatal`] when `rejection`, the
/// age and text of a rejection by a broker, is present, and its reason ends
/// with that rejection. Any other report defers to [`classify`].
fn report_error(
    code: Option<RDKafkaErrorCode>,
    reason: &str,
    rejection: Option<(Duration, String)>,
) -> SinkError {
    match rejection {
        Some(rejection) if code == Some(RDKafkaErrorCode::MessageTimedOut) => SinkError::Client {
            class: ErrorClass::Fatal,
            reason: with_rejection(reason, &rejection),
        },
        _ => classify(code, reason),
    }
}

/// `reason` followed by a rejection's age and text.
fn with_rejection(reason: &str, (age, text): &(Duration, String)) -> String {
    format!("{reason}; a broker rejected the connection {age:?} ago: {text}")
}

/// Map a produce/report error onto the framework taxonomy.
///
/// Fatal covers what retrying cannot heal: authorization and configuration
/// errors, an unknown topic, a message the broker's limits reject, and the
/// idempotent producer's fenced/fatal states (retrying those would spin
/// until the stalled-watermark deadline instead of surfacing the cause).
/// Everything else (transport failures, timeouts, purges, and unknown codes)
/// is retryable: with idempotence enabled a re-produce is safe in-session,
/// and a batch replay costs duplicates, never loss.
fn classify(code: Option<RDKafkaErrorCode>, reason: &str) -> SinkError {
    use RDKafkaErrorCode as C;
    let class = match code {
        Some(
            C::UnknownTopicOrPartition
            | C::TopicAuthorizationFailed
            | C::ClusterAuthorizationFailed
            | C::Authentication
            | C::SaslAuthenticationFailed
            | C::InvalidRequiredAcks
            | C::MessageSizeTooLarge
            | C::PolicyViolation
            | C::Fatal
            | C::OutOfOrderSequenceNumber
            | C::InvalidProducerEpoch
            | C::InvalidProducerIdMapping,
        ) => ErrorClass::Fatal,
        _ => ErrorClass::Retryable,
    };
    let reason = match code {
        Some(C::UnknownTopicOrPartition) => format!(
            "{reason} — the topic does not exist (create it, or check the \
             sink's `topic` field)"
        ),
        Some(C::MessageSizeTooLarge) => format!(
            "{reason} — a message passed the sink's `max_message_bytes` \
             guard but exceeded the broker/topic `message.max.bytes`; align \
             the two limits"
        ),
        _ => reason.to_string(),
    };
    SinkError::Client { class, reason }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_writer(statistics_enabled: bool) -> KafkaWriter {
        KafkaWriter::new(
            Duration::from_secs(1),
            "t".to_string(),
            Duration::from_secs(1),
            Arc::new(Mutex::new(None)),
            statistics_enabled,
        )
    }

    fn class_of(err: &SinkError) -> ErrorClass {
        match err {
            SinkError::Client { class, .. } => *class,
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    #[test]
    fn classify_table() {
        use RDKafkaErrorCode as C;
        let fatal = [
            C::UnknownTopicOrPartition,
            C::TopicAuthorizationFailed,
            C::ClusterAuthorizationFailed,
            C::Authentication,
            C::SaslAuthenticationFailed,
            C::InvalidRequiredAcks,
            C::MessageSizeTooLarge,
            C::PolicyViolation,
            C::Fatal,
            C::OutOfOrderSequenceNumber,
            C::InvalidProducerEpoch,
            C::InvalidProducerIdMapping,
        ];
        for code in fatal {
            assert_eq!(
                class_of(&classify(Some(code), "x")),
                ErrorClass::Fatal,
                "{code:?} must be fatal"
            );
        }
        let retryable = [
            C::MessageTimedOut,
            C::BrokerTransportFailure,
            C::AllBrokersDown,
            C::NotEnoughReplicas,
            C::NotEnoughReplicasAfterAppend,
            C::QueueFull,
            C::PurgeQueue,
            C::PurgeInflight,
            C::InvalidMessage,
            C::RequestTimedOut,
        ];
        for code in retryable {
            assert_eq!(
                class_of(&classify(Some(code), "x")),
                ErrorClass::Retryable,
                "{code:?} must be retryable"
            );
        }
        // Unknown / uncoded errors stay retryable (conservative: replays
        // duplicate, never lose).
        assert_eq!(class_of(&classify(None, "x")), ErrorClass::Retryable);
    }

    #[test]
    fn classify_actionable_messages() {
        let err = classify(Some(RDKafkaErrorCode::UnknownTopicOrPartition), "ctx");
        let SinkError::Client { reason, .. } = err else {
            unreachable!()
        };
        assert!(reason.contains("topic"), "actionable: {reason}");

        let err = classify(Some(RDKafkaErrorCode::MessageSizeTooLarge), "ctx");
        let SinkError::Client { reason, .. } = err else {
            unreachable!()
        };
        assert!(reason.contains("max_message_bytes"), "actionable: {reason}");
        assert!(reason.contains("message.max.bytes"), "actionable: {reason}");
    }

    /// A timed-out report is fatal while a rejection is in the window, from
    /// the failed-reports exit and the missing-reports exit alike, and the
    /// reason ends with the rejection's age and text. Without one it stays
    /// retryable.
    #[test]
    fn a_timed_out_report_after_a_rejection_is_fatal() {
        use crate::sink::context::tests::SASL_REFUSED;
        let ctx = SinkContext::detached();
        let t0 = Instant::now();
        ctx.note_error(RDKafkaErrorCode::Authentication, SASL_REFUSED, t0);
        let rejection = ctx.rejection_within(t0 + Duration::from_secs(2), Duration::from_secs(75));

        for reason in [
            "1 of 1 delivery reports failed; first: Message production error: MessageTimedOut \
             (Local: Message timed out)",
            "delivery reports missing past the deadline (1s + 5s grace)",
        ] {
            let timed_out = Some(RDKafkaErrorCode::MessageTimedOut);
            let SinkError::Client { class, reason: got } =
                report_error(timed_out, reason, rejection.clone())
            else {
                unreachable!()
            };
            assert_eq!(class, ErrorClass::Fatal, "{got}");
            assert_eq!(
                got,
                format!("{reason}; a broker rejected the connection 2s ago: {SASL_REFUSED}")
            );
            assert_eq!(
                class_of(&report_error(timed_out, reason, None)),
                ErrorClass::Retryable
            );
        }
    }

    /// A report other than a timeout keeps its code's class while a
    /// rejection is in the window.
    #[test]
    fn other_reports_keep_their_class_after_a_rejection() {
        let rejection = Some((Duration::from_secs(2), "rejected".to_owned()));
        for (code, class) in [
            (
                RDKafkaErrorCode::BrokerTransportFailure,
                ErrorClass::Retryable,
            ),
            (RDKafkaErrorCode::MessageSizeTooLarge, ErrorClass::Fatal),
        ] {
            assert_eq!(
                class_of(&report_error(Some(code), "x", rejection.clone())),
                class,
                "{code:?}"
            );
        }
        assert_eq!(
            class_of(&report_error(None, "x", rejection)),
            ErrorClass::Retryable
        );
    }

    #[test]
    fn parse_messages_rejects_row_count_mismatch() {
        use bytes::BytesMut;
        let mut frame = BytesMut::new();
        crate::sink::frame::write_message(&mut frame, None, std::iter::empty(), Some(b"p"))
            .unwrap();
        let batch = SealedBatch {
            frames: vec![frame.freeze()],
            rows: 2, // claims two, frame holds one
            bytes: 0,
            dedup_token: "t".to_string(),
        };
        let writer = test_writer(false);
        let err = writer.parse_messages(&batch).unwrap_err();
        assert_eq!(class_of(&err), ErrorClass::Fatal);

        let ok_batch = SealedBatch { rows: 1, ..batch };
        assert_eq!(writer.parse_messages(&ok_batch).unwrap().len(), 1);
    }

    /// Disabled statistics must register no families, and a missing Meter
    /// (custom/reserved component_type) must leave the slot empty, so the
    /// producer context publishes nothing.
    #[test]
    fn attach_metrics_gates_on_statistics_and_meter() {
        let mut disabled = test_writer(false);
        disabled.attach_metrics(Some(Meter::with_namespace(
            "kafka", "orders", "out", "kafka",
        )));
        assert!(disabled.stats_slot.lock().unwrap().is_none());

        let mut no_meter = test_writer(true);
        no_meter.attach_metrics(None);
        assert!(no_meter.stats_slot.lock().unwrap().is_none());

        let mut enabled = test_writer(true);
        enabled.attach_metrics(Some(Meter::with_namespace(
            "kafka", "orders", "out", "kafka",
        )));
        assert!(enabled.stats_slot.lock().unwrap().is_some());
    }

    #[cfg(feature = "tls")]
    mod tls_rejection {
        use super::*;
        use crate::sink::config::{KafkaSinkConfig, build};

        fn one_row() -> SealedBatch {
            let mut frame = bytes::BytesMut::new();
            crate::sink::frame::write_message(&mut frame, None, std::iter::empty(), Some(b"p"))
                .unwrap();
            SealedBatch {
                frames: vec![frame.freeze()],
                rows: 1,
                bytes: 0,
                dedup_token: "t".to_string(),
            }
        }

        /// A batch written to a broker that answers the TLS handshake with a
        /// listed alert fails `Fatal`, and the error carries librdkafka's
        /// text. A live 116 arrives under the `SSL` code.
        #[tokio::test]
        async fn a_rejected_handshake_fails_the_batch() {
            for alert in [40, 48, 70, 116] {
                let brokers = spate_test::tls_alert_server(b"", alert).to_string();
                let mut cfg = KafkaSinkConfig::new(brokers, "t");
                cfg.delivery_timeout = Duration::from_secs(1);
                cfg.statistics_interval = Duration::ZERO;
                cfg.rdkafka.insert("security.protocol".into(), "ssl".into());
                let sink = build(cfg).expect("build");

                let err = sink
                    .writer
                    .write_batch(&sink.endpoints[0][0], &one_row())
                    .await
                    .expect_err("no broker accepts the batch");
                let SinkError::Client { class, reason } = err else {
                    panic!("alert {alert}: unexpected error shape: {err:?}");
                };
                assert!(
                    reason.contains(&format!("SSL alert number {alert} ")),
                    "alert {alert}: {reason}"
                );
                assert_eq!(class, ErrorClass::Fatal, "alert {alert}: {reason}");
            }
        }
    }
}
