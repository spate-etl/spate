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
//! fast instead of spinning.

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
const DELIVERY_GRACE: Duration = Duration::from_secs(5);

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
    topic: String,
    delivery_timeout: Duration,
    /// Shared with the main producer's context, which publishes through
    /// it; filled by [`attach_metrics`](ShardWriter::attach_metrics).
    stats_slot: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>,
    statistics_enabled: bool,
}

impl KafkaWriter {
    pub(crate) fn new(
        topic: String,
        delivery_timeout: Duration,
        stats_slot: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>,
        statistics_enabled: bool,
    ) -> Self {
        KafkaWriter {
            topic,
            delivery_timeout,
            stats_slot,
            statistics_enabled,
        }
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
            return Err(SinkError::Client {
                class: ErrorClass::Retryable,
                reason: format!(
                    "delivery reports missing past the deadline ({:?} + {:?} \
                     grace); the batch will be retried (already-delivered \
                     messages will duplicate — at-least-once)",
                    self.delivery_timeout, DELIVERY_GRACE
                ),
            });
        }
        if let Some((code, reason)) = inflight.first_error() {
            return Err(classify(
                *code,
                &format!(
                    "{} of {total} delivery reports failed; first: {reason}",
                    inflight.failed()
                ),
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
        outcome.unwrap_or_else(|join_error| {
            Err(SinkError::Client {
                class: ErrorClass::Retryable,
                reason: format!("probe task failed: {join_error}"),
            })
        })
    }
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
}
