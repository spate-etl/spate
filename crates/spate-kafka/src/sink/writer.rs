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
use bytes::Bytes;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use spate_core::backoff::Backoff;
use spate_core::clock::tokio::Clock as IoClock;
use spate_core::error::{ErrorClass, SinkError};
use spate_core::sink::{SealedBatch, ShardWriter, SinkMetricsCtx};
use std::panic::RefUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Extra time granted past `delivery_timeout` for the countdown await:
/// `delivery.timeout.ms` bounds time-to-report only from each message's
/// *enqueue*, and the send loop itself may have spent time in queue-full
/// backoff.
pub(crate) const DELIVERY_GRACE: Duration = Duration::from_secs(5);

/// Queue-full backoff schedule: librdkafka signals queue-full synchronously
/// from `send`; the writer sleeps and retries that message while the
/// producer's poll thread drains the queue.
const QUEUE_FULL_BACKOFF: Backoff =
    Backoff::new(Duration::from_millis(10), Duration::from_millis(250));

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
    /// The clock the send and report deadlines run on.
    io_clock: Arc<dyn IoClock + RefUnwindSafe>,
}

impl KafkaWriter {
    pub(crate) fn new(
        rejection_window: Duration,
        topic: String,
        delivery_timeout: Duration,
        stats_slot: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>,
        statistics_enabled: bool,
        io_clock: Arc<dyn IoClock + RefUnwindSafe>,
    ) -> Self {
        KafkaWriter {
            rejection_window,
            topic,
            delivery_timeout,
            stats_slot,
            statistics_enabled,
            io_clock,
        }
    }

    /// The latest rejection recorded by `endpoint`'s producer, when it is
    /// inside the rejection window now.
    fn rejection(&self, endpoint: &KafkaEndpoint) -> Option<(Duration, String)> {
        endpoint
            .producer
            .context()
            .rejection_within(self.rejection_window)
    }
}

/// Parse every frame of a batch into its messages, failing on any corrupt
/// frame or when the message count differs from `rows`.
pub(crate) fn parse_messages(
    frames: &[Bytes],
    rows: u64,
) -> Result<Vec<MessageRef<'_>>, SinkError> {
    let mut messages = Vec::with_capacity(rows as usize);
    for frame in frames {
        for message in FrameParser::new(frame) {
            messages.push(message.map_err(|e| {
                SinkError::fatal(format!("corrupted sink frame (framework bug): {e}"))
            })?);
        }
    }
    if messages.len() as u64 != rows {
        return Err(SinkError::fatal(format!(
            "corrupted sink frame (framework bug): batch claims {rows} rows \
                 but frames parse to {} messages",
            messages.len()
        )));
    }
    Ok(messages)
}

/// The librdkafka header list for `message`, or `None` when it has no
/// headers.
pub(crate) fn message_headers(message: &MessageRef<'_>) -> Option<OwnedHeaders> {
    if message.headers.is_empty() {
        return None;
    }
    Some(owned_headers(
        message
            .headers
            .iter()
            .map(|(name, value)| (name, Some(value))),
    ))
}

/// Copy `headers` into a librdkafka header list, in order. A `None` value is
/// a null header value.
pub(crate) fn owned_headers<'a, I>(headers: I) -> OwnedHeaders
where
    I: ExactSizeIterator<Item = (&'a str, Option<&'a [u8]>)>,
{
    let mut owned = OwnedHeaders::new_with_capacity(headers.len());
    for (key, value) in headers {
        owned = owned.insert(Header { key, value });
    }
    owned
}

impl ShardWriter for KafkaWriter {
    type Endpoint = KafkaEndpoint;

    /// Resolve the `spate_kafka_sink_*` statistics handles into the slot the
    /// producer context publishes through. No-op when `statistics_interval`
    /// is zero; disabled statistics register no families.
    fn attach_metrics(&mut self, cx: SinkMetricsCtx) {
        if !self.statistics_enabled {
            return;
        }
        if let Some(meter) = cx.meter {
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
        let messages = parse_messages(&batch.frames, batch.rows)?;
        let total = messages.len();
        let inflight = Arc::new(BatchInflight::new(total));
        let send_deadline = self.io_clock.now() + self.delivery_timeout + DELIVERY_GRACE;

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
            if let Some(headers) = message_headers(message) {
                record = record.headers(headers);
            }

            let mut backoff = QUEUE_FULL_BACKOFF;
            loop {
                match endpoint.producer.send(record) {
                    Ok(()) => break,
                    Err((error, returned))
                        if error.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull) =>
                    {
                        if self.io_clock.now() >= send_deadline {
                            return Err(SinkError::retryable(format!(
                                "producer queue stayed full past the delivery \
                                     deadline ({:?} + {:?} grace); the batch will be \
                                     retried",
                                self.delivery_timeout, DELIVERY_GRACE
                            )));
                        }
                        // Real time: it paces librdkafka's queue drain, which
                        // runs on the poll thread's own clock.
                        tokio::time::sleep(backoff.next_delay()).await;
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
        let wait_deadline = self.io_clock.now() + self.delivery_timeout + DELIVERY_GRACE;
        if !inflight.wait(wait_deadline, &*self.io_clock).await {
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
        #[expect(
            clippy::disallowed_methods,
            reason = "the metadata read ends within `PROBE_TIMEOUT`, and a blocking task cannot be aborted"
        )]
        let outcome = tokio::task::spawn_blocking(move || {
            let metadata = producer
                .client()
                .fetch_metadata(Some(&topic), PROBE_TIMEOUT)
                .map_err(|e| classify(e.rdkafka_error_code(), &format!("metadata fetch: {e}")))?;
            let Some(meta) = metadata.topics().iter().find(|t| t.name() == topic) else {
                return Err(SinkError::retryable(format!(
                    "metadata response omitted topic \"{topic}\""
                )));
            };
            if let Some(err) = meta.error() {
                let code: RDKafkaErrorCode = err.into();
                return Err(classify(
                    Some(code),
                    &format!("topic \"{topic}\" metadata error: {code}"),
                ));
            }
            if meta.partitions().is_empty() {
                return Err(SinkError::retryable(format!(
                    "topic \"{topic}\" reports no partitions yet"
                )));
            }
            Ok(())
        })
        .await;
        let outcome = outcome.unwrap_or_else(|join_error| {
            Err(SinkError::retryable(format!(
                "probe task failed: {join_error}"
            )))
        });
        match self.rejection(endpoint) {
            Some(rejection) => {
                outcome.map_err(|e| e.map_reason(|reason| with_rejection(&reason, &rejection)))
            }
            None => outcome,
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
        Some(rejection) if code == Some(RDKafkaErrorCode::MessageTimedOut) => {
            SinkError::fatal(with_rejection(reason, &rejection))
        }
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
    let class = report_class(code);
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
    SinkError::new(class, reason)
}

/// The class of a failed produce or delivery report, by [`classify`]'s
/// table.
pub(crate) fn report_class(code: Option<RDKafkaErrorCode>) -> ErrorClass {
    use RDKafkaErrorCode as C;
    match code {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::context::tests::on_test_clock;
    use crate::sink::frame::write_message;
    use spate_core::clock::tokio::SystemClock as IoSystemClock;
    use spate_core::metrics::Meter;
    use spate_test::HANG;

    fn test_writer(statistics_enabled: bool) -> KafkaWriter {
        KafkaWriter::new(
            Duration::from_secs(1),
            "t".to_string(),
            Duration::from_secs(1),
            Arc::new(Mutex::new(None)),
            statistics_enabled,
            Arc::new(IoSystemClock),
        )
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
                classify(Some(code), "x").class(),
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
                classify(Some(code), "x").class(),
                ErrorClass::Retryable,
                "{code:?} must be retryable"
            );
        }
        // Unknown / uncoded errors stay retryable (conservative: replays
        // duplicate, never lose).
        assert_eq!(classify(None, "x").class(), ErrorClass::Retryable);
    }

    #[test]
    fn classify_actionable_messages() {
        let err = classify(Some(RDKafkaErrorCode::UnknownTopicOrPartition), "ctx");
        assert!(err.reason().contains("topic"), "actionable: {err}");

        let err = classify(Some(RDKafkaErrorCode::MessageSizeTooLarge), "ctx");
        assert!(
            err.reason().contains("max_message_bytes"),
            "actionable: {err}"
        );
        assert!(
            err.reason().contains("message.max.bytes"),
            "actionable: {err}"
        );
    }

    /// `report_error` makes a timed-out report fatal while a rejection is
    /// recorded, with either exit's reason, and the reason ends with the
    /// rejection's age and text. Without one it stays retryable.
    #[test]
    fn a_timed_out_report_after_a_rejection_is_fatal() {
        use crate::sink::context::tests::SASL_REFUSED;
        let (ctx, clock) = on_test_clock();
        ctx.note_error(RDKafkaErrorCode::Authentication, SASL_REFUSED);
        clock.advance(Duration::from_secs(2));
        let rejection = ctx.rejection_within(Duration::from_secs(75));

        for reason in [
            "1 of 1 delivery reports failed; first: Message production error: MessageTimedOut \
             (Local: Message timed out)",
            "delivery reports missing past the deadline (1s + 5s grace)",
        ] {
            let timed_out = Some(RDKafkaErrorCode::MessageTimedOut);
            let err = report_error(timed_out, reason, rejection.clone());
            assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
            assert_eq!(
                err.reason(),
                format!("{reason}; a broker rejected the connection 2s ago: {SASL_REFUSED}")
            );
            assert_eq!(
                report_error(timed_out, reason, None).class(),
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
                report_error(Some(code), "x", rejection.clone()).class(),
                class,
                "{code:?}"
            );
        }
        assert_eq!(
            report_error(None, "x", rejection).class(),
            ErrorClass::Retryable
        );
    }

    #[test]
    fn parse_messages_rejects_row_count_mismatch() {
        use bytes::BytesMut;
        let mut frame = BytesMut::new();
        write_message(&mut frame, None, std::iter::empty(), Some(b"p")).unwrap();
        let batch = SealedBatch {
            frames: vec![frame.freeze()],
            rows: 2, // claims two, frame holds one
            bytes: 0,
            dedup_token: "t".to_string(),
        };
        let err = parse_messages(&batch.frames, batch.rows).unwrap_err();
        assert_eq!(err.class(), ErrorClass::Fatal);

        let ok_batch = SealedBatch { rows: 1, ..batch };
        assert_eq!(
            parse_messages(&ok_batch.frames, ok_batch.rows)
                .unwrap()
                .len(),
            1
        );
    }

    /// The header list keeps every pair in order, a null value included.
    #[test]
    fn owned_headers_keeps_order_and_null_values() {
        use rdkafka::message::Headers as _;
        let pairs = [("a", Some(&b"1"[..])), ("b", None), ("c", Some(&b""[..]))];
        let owned = owned_headers(pairs.iter().copied());
        let built: Vec<_> = (0..owned.count())
            .map(|i| {
                let header = owned.get(i);
                (header.key, header.value)
            })
            .collect();
        assert_eq!(built, pairs);
    }

    /// A message without headers carries no header list.
    #[test]
    fn message_headers_is_none_without_headers() {
        use bytes::BytesMut;
        let mut frame = BytesMut::new();
        write_message(&mut frame, None, std::iter::empty(), Some(b"p")).unwrap();
        write_message(&mut frame, None, [("h", &b"v"[..])].into_iter(), Some(b"p")).unwrap();
        let frames = [frame.freeze()];
        let messages = parse_messages(&frames, 2).unwrap();
        assert!(message_headers(&messages[0]).is_none());
        let headers = message_headers(&messages[1]).expect("one header");
        assert_eq!(rdkafka::message::Headers::count(&headers), 1);
    }

    /// A frame that ends inside a message is a fatal framework bug.
    #[test]
    fn parse_messages_rejects_a_corrupt_frame() {
        use bytes::BytesMut;
        let mut frame = BytesMut::new();
        write_message(&mut frame, None, std::iter::empty(), Some(b"p")).unwrap();
        frame.truncate(frame.len() - 1);
        let batch = SealedBatch {
            frames: vec![frame.freeze()],
            rows: 1,
            bytes: 0,
            dedup_token: "t".to_string(),
        };
        let err = parse_messages(&batch.frames, batch.rows).unwrap_err();
        assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
        assert!(
            err.reason()
                .starts_with("corrupted sink frame (framework bug): "),
            "{err}"
        );
    }

    /// Disabled statistics must register no families, and a missing Meter
    /// (custom/reserved component_type) must leave the slot empty, so the
    /// producer context publishes nothing.
    #[test]
    fn attach_metrics_gates_on_statistics_and_meter() {
        let mut disabled = test_writer(false);
        disabled.attach_metrics(SinkMetricsCtx::new(Some(Meter::with_namespace(
            "kafka", "orders", "out", "kafka",
        ))));
        assert!(disabled.stats_slot.lock().unwrap().is_none());

        let mut no_meter = test_writer(true);
        no_meter.attach_metrics(SinkMetricsCtx::new(None));
        assert!(no_meter.stats_slot.lock().unwrap().is_none());

        let mut enabled = test_writer(true);
        enabled.attach_metrics(SinkMetricsCtx::new(Some(Meter::with_namespace(
            "kafka", "orders", "out", "kafka",
        ))));
        assert!(enabled.stats_slot.lock().unwrap().is_some());
    }

    #[cfg(feature = "tls")]
    mod tls_rejection {
        use super::*;
        use crate::sink::config::{KafkaSinkConfig, build};

        fn one_row() -> SealedBatch {
            let mut frame = bytes::BytesMut::new();
            write_message(&mut frame, None, std::iter::empty(), Some(b"p")).unwrap();
            SealedBatch {
                frames: vec![frame.freeze()],
                rows: 1,
                bytes: 0,
                dedup_token: "t".to_string(),
            }
        }

        /// A batch written to a broker that answers the TLS handshake with a
        /// listed alert fails `Fatal`, and the error carries librdkafka's
        /// text. A live 116 arrives under the `SSL` code. A probe on the same
        /// producer then carries the rejection in its reason.
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
                assert!(
                    err.reason().contains(&format!("SSL alert number {alert} ")),
                    "alert {alert}: {err}"
                );
                assert_eq!(err.class(), ErrorClass::Fatal, "alert {alert}: {err}");

                let err = sink
                    .writer
                    .probe(&sink.endpoints[0][0])
                    .await
                    .expect_err("no broker answers the probe");
                let reason = err.to_string();
                assert_eq!(
                    err.class(),
                    ErrorClass::Retryable,
                    "alert {alert}: probe: {reason}"
                );
                assert!(
                    reason.contains("a broker rejected the connection")
                        && reason.contains(&format!("SSL alert number {alert} ")),
                    "alert {alert}: probe: {reason}"
                );
            }
        }
    }

    /// `build_on_clocks` stamps and ages a broker's rejection on the clock
    /// it is given.
    #[cfg(feature = "tls")]
    #[test]
    fn build_on_clocks_ages_a_rejection_on_the_given_clock() {
        let brokers = crate::testutil::refuses_sasl_login();
        let body = format!(
            "  brokers: {brokers}\n  topic: orders\n  rdkafka:\n    security.protocol: \
             sasl_plaintext\n    sasl.mechanism: PLAIN\n    sasl.username: u\n    \
             sasl.password: wrong\n"
        );
        let cfg: crate::sink::KafkaSinkConfig = spate_test::component_config("kafka", &body)
            .deserialize_into()
            .expect("config");
        let clock = spate_core::clock::TestClock::frozen();
        let sink =
            crate::sink::build_on_clocks(cfg, Arc::clone(&clock) as _, Arc::new(IoSystemClock))
                .expect("sink");
        let endpoint = &sink.endpoints[0][0];
        spate_test::wait_until(HANG, "the rejection", || {
            endpoint.producer.context().latest_rejection().is_some()
        });
        assert!(sink.writer.rejection(endpoint).is_some());
        clock.advance(sink.writer.rejection_window + Duration::from_nanos(1));
        assert_eq!(sink.writer.rejection(endpoint), None);
    }
}
