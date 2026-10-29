//! Producer context: the delivery-report → batch-countdown bridge, and the
//! latest rejection by a broker.
//!
//! The framework acks a batch when [`write_batch`] returns `Ok`, so the
//! writer must not return until every message of the batch has a confirmed
//! delivery report. Reports arrive asynchronously on the producer's poll
//! thread; each produced message carries an `Arc<BatchInflight>` as its
//! librdkafka opaque, and the callback decrements the batch's countdown,
//! recording the first failure, then wakes the awaiting writer on the
//! last report. The callback never blocks: it is atomics plus one
//! `Notify::notify_one`.
//!
//! Abort safety: librdkafka holds one `Arc` reference per outstanding
//! message and delivers exactly one report for each (success, error, or
//! `Purge*` at teardown), so a `write_batch` future aborted at the drain
//! deadline leaks nothing; late reports decrement a countdown nothing
//! awaits and the last `Arc` drop frees the state.
//!
//! [`write_batch`]: spate_core::sink::ShardWriter::write_batch

use crate::clock::SyncClock;
use crate::sink::metrics::KafkaSinkStatsMetrics;
use rdkafka::client::ClientContext;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::ProducerContext;
use rdkafka::statistics::Statistics;
use spate_core::clock::tokio::Clock as IoClock;
use std::future::Future as _;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Per-batch delivery countdown. Created by `write_batch` after parsing
/// the batch (so the total is known up front; incrementing per send would
/// race reports arriving mid-loop), cloned into every produced message's
/// delivery opaque.
#[derive(Debug)]
pub(crate) struct BatchInflight {
    remaining: AtomicUsize,
    failed: AtomicUsize,
    first_error: OnceLock<(Option<RDKafkaErrorCode>, String)>,
    done: Notify,
}

impl BatchInflight {
    pub(crate) fn new(messages: usize) -> Self {
        BatchInflight {
            remaining: AtomicUsize::new(messages),
            failed: AtomicUsize::new(0),
            first_error: OnceLock::new(),
            done: Notify::new(),
        }
    }

    /// Record a failed report. The first error is kept verbatim (it
    /// classifies the batch); later ones only count.
    pub(crate) fn record_failure(&self, code: Option<RDKafkaErrorCode>, reason: String) {
        let _ = self.first_error.set((code, reason));
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    /// One message resolved (delivered or failed). Wakes the waiter on the
    /// last one. Failure recording must happen *before* this call so the
    /// waiter's acquire on the final decrement observes it.
    pub(crate) fn complete_one(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.done.notify_one();
        }
    }

    /// Await all reports until `clock` reaches `deadline`. Returns `false`
    /// on timeout. Lost-wakeup-safe: the `Notify` future is created before
    /// each re-check of the countdown, so a `notify_one` racing the check is
    /// stored as a permit and observed by the next await.
    pub(crate) async fn wait(&self, deadline: tokio::time::Instant, clock: &dyn IoClock) -> bool {
        let mut timeout = clock.sleep_until(deadline);
        loop {
            let notified = self.done.notified();
            if self.remaining.load(Ordering::Acquire) == 0 {
                return true;
            }
            let mut notified = pin!(notified);
            // The report first, so a countdown finished as the deadline
            // comes due still counts.
            let reported = std::future::poll_fn(|cx| {
                if notified.as_mut().poll(cx).is_ready() {
                    Poll::Ready(true)
                } else if timeout.as_mut().poll(cx).is_ready() {
                    Poll::Ready(false)
                } else {
                    Poll::Pending
                }
            })
            .await;
            if !reported {
                return self.remaining.load(Ordering::Acquire) == 0;
            }
        }
    }

    /// The first failed report, when any report failed.
    pub(crate) fn first_error(&self) -> Option<&(Option<RDKafkaErrorCode>, String)> {
        self.first_error.get()
    }

    /// How many reports failed.
    pub(crate) fn failed(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
    }
}

/// Client context for the sink's producers and the client the feed's
/// startup checks read metadata through: routes librdkafka logs to
/// `tracing` (mirroring the source's context), translates delivery
/// reports into batch countdowns, publishes statistics snapshots
/// through the slot shared with the writer, and records the latest
/// rejection by a broker.
#[derive(Debug)]
pub(crate) struct SinkContext {
    /// Filled by the writer's `attach_metrics` (main producer only, and
    /// only when statistics are enabled); the `stats` callback publishes
    /// through it from the producer's poll thread.
    stats: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>,
    /// When the latest SASL or TLS rejection arrived, on `clock`, and its
    /// text.
    rejection: Mutex<Option<(Instant, String)>>,
    /// The clock rejections are stamped and aged on.
    clock: SyncClock,
    /// The last error callback, until the identical call that repeats it.
    unpaired_error: Mutex<Option<(Option<RDKafkaErrorCode>, String)>>,
}

impl SinkContext {
    /// The main producer's context, sharing the writer's statistics slot.
    pub(crate) fn new(stats: Arc<Mutex<Option<KafkaSinkStatsMetrics>>>, clock: SyncClock) -> Self {
        SinkContext {
            stats,
            rejection: Mutex::new(None),
            clock,
            unpaired_error: Mutex::new(None),
        }
    }

    /// A context for probe producers: its slot is never filled (and the
    /// probe client config sets no statistics interval), so it publishes
    /// nothing. The single main producer stays the sole statistics source,
    /// which keeps the absolute-counter mapping sound.
    pub(crate) fn detached(clock: SyncClock) -> Self {
        SinkContext::new(Arc::new(Mutex::new(None)), clock)
    }

    /// Record an error callback's `text` as the latest rejection, stamped on
    /// the context's clock, when `code` and `text` report a
    /// [`rejection`](crate::error::rejection). Any other error leaves the
    /// record as it is.
    pub(crate) fn note_error(&self, code: RDKafkaErrorCode, text: &str) {
        if crate::error::rejection(code, text) {
            *self.rejection.lock().expect("rejection lock") =
                Some((self.clock.now(), text.to_owned()));
        }
    }

    /// The latest rejection's age on the context's clock and its text, when
    /// that age is at most `window`.
    pub(crate) fn rejection_within(&self, window: Duration) -> Option<(Duration, String)> {
        let rejection = self.rejection.lock().expect("rejection lock");
        let (at, text) = rejection.as_ref()?;
        let age = self.clock.now().saturating_duration_since(*at);
        (age <= window).then(|| (age, text.clone()))
    }

    /// The latest rejection's text, however long ago it arrived.
    pub(crate) fn latest_rejection(&self) -> Option<String> {
        let rejection = self.rejection.lock().expect("rejection lock");
        rejection.as_ref().map(|(_, text)| text.clone())
    }

    /// Whether this error callback repeats the previous one, which it then
    /// consumes. rdkafka's producer poll hands each error event to the
    /// context twice in a row on the poll thread;
    /// `rdkafka_delivers_each_producer_error_twice` pins that.
    fn repeats_previous(&self, code: Option<RDKafkaErrorCode>, reason: &str) -> bool {
        let mut unpaired = self.unpaired_error.lock().expect("unpaired error lock");
        if unpaired
            .as_ref()
            .is_some_and(|(c, r)| *c == code && r == reason)
        {
            *unpaired = None;
            return true;
        }
        *unpaired = Some((code, reason.to_owned()));
        false
    }
}

impl ClientContext for SinkContext {
    /// Runs on the producer's poll thread, once per statistics interval —
    /// never on the record path.
    fn stats(&self, statistics: Statistics) {
        if let Some(metrics) = self.stats.lock().expect("stats slot lock").as_mut() {
            metrics.update(&statistics);
        }
    }

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        use rdkafka::config::RDKafkaLogLevel as L;
        match level {
            L::Emerg | L::Alert | L::Critical | L::Error => {
                tracing::error!(target: "librdkafka", fac, "{log_message}");
            }
            L::Warning => tracing::warn!(target: "librdkafka", fac, "{log_message}"),
            L::Notice | L::Info => tracing::info!(target: "librdkafka", fac, "{log_message}"),
            L::Debug => tracing::debug!(target: "librdkafka", fac, "{log_message}"),
        }
    }

    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        if self.repeats_previous(error.rdkafka_error_code(), reason) {
            return;
        }
        tracing::warn!(target: "librdkafka", %error, "{reason}");
        if let Some(code) = error.rdkafka_error_code() {
            self.note_error(code, reason);
        }
    }
}

impl ProducerContext for SinkContext {
    type DeliveryOpaque = Arc<BatchInflight>;

    /// Runs on the producer's poll thread. Never block here.
    fn delivery(&self, delivery_result: &DeliveryResult<'_>, inflight: Arc<BatchInflight>) {
        if let Err((error, _message)) = delivery_result {
            inflight.record_failure(error.rdkafka_error_code(), error.to_string());
        }
        inflight.complete_one();
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use spate_core::clock::TestClock;
    use spate_core::clock::tokio::{SystemClock as IoSystemClock, TestClock as IoTestClock};
    use std::time::Duration;

    /// A detached context on a frozen clock, and that clock.
    pub(crate) fn on_test_clock() -> (SinkContext, Arc<TestClock>) {
        let clock = TestClock::frozen();
        let ctx = SinkContext::detached(SyncClock::new(Arc::clone(&clock) as _));
        (ctx, clock)
    }

    /// Yields until `clock` has a sleep armed for `deadline`.
    async fn armed(clock: &IoTestClock, deadline: tokio::time::Instant) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !clock.has_slept_until(deadline) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the wait arms its deadline");
    }

    #[tokio::test]
    async fn countdown_completes_on_last_delivery() {
        let inflight = Arc::new(BatchInflight::new(3));
        let waiter = {
            let inflight = Arc::clone(&inflight);
            tokio::spawn(async move {
                let clock = IoSystemClock;
                inflight
                    .wait(clock.now() + Duration::from_secs(5), &clock)
                    .await
            })
        };
        // Reports fire from a foreign (non-tokio) thread, as librdkafka's
        // poll thread would.
        let reporter = {
            let inflight = Arc::clone(&inflight);
            std::thread::spawn(move || {
                for _ in 0..3 {
                    inflight.complete_one();
                }
            })
        };
        assert!(waiter.await.unwrap(), "all reports in → wait resolves true");
        reporter.join().unwrap();
    }

    /// The countdown waits until its clock reaches the deadline: a report one
    /// nanosecond before it completes the batch, and a report still missing
    /// at it times the wait out.
    #[tokio::test]
    async fn a_missing_report_times_out_at_the_deadline() {
        let clock = IoTestClock::frozen();
        let window = Duration::from_secs(5);

        let inflight = Arc::new(BatchInflight::new(2));
        inflight.complete_one();
        let deadline = clock.now() + window;
        let waiter = tokio::spawn({
            let (inflight, clock) = (Arc::clone(&inflight), Arc::clone(&clock));
            async move { inflight.wait(deadline, &*clock).await }
        });
        armed(&clock, deadline).await;
        clock.advance(window - Duration::from_nanos(1));
        tokio::task::yield_now().await;
        inflight.complete_one();
        assert!(waiter.await.unwrap(), "timed out before the deadline");

        let inflight = Arc::new(BatchInflight::new(1));
        let deadline = clock.now() + window;
        let waiter = tokio::spawn({
            let (inflight, clock) = (Arc::clone(&inflight), Arc::clone(&clock));
            async move { inflight.wait(deadline, &*clock).await }
        });
        armed(&clock, deadline).await;
        clock.advance(window);
        assert!(!waiter.await.unwrap(), "one report missing → false");
    }

    #[tokio::test]
    async fn first_error_wins_and_is_visible_after_countdown() {
        let inflight = Arc::new(BatchInflight::new(2));
        let reporter = {
            let inflight = Arc::clone(&inflight);
            std::thread::spawn(move || {
                inflight
                    .record_failure(Some(RDKafkaErrorCode::MessageTimedOut), "first".to_string());
                inflight.complete_one();
                inflight.record_failure(
                    Some(RDKafkaErrorCode::MessageSizeTooLarge),
                    "second".to_string(),
                );
                inflight.complete_one();
            })
        };
        let clock = IoSystemClock;
        assert!(
            inflight
                .wait(clock.now() + Duration::from_secs(5), &clock)
                .await
        );
        reporter.join().unwrap();
        let (code, reason) = inflight.first_error().expect("an error was recorded");
        assert_eq!(*code, Some(RDKafkaErrorCode::MessageTimedOut));
        assert_eq!(reason, "first");
        assert_eq!(inflight.failed(), 2, "later failures still count");
    }

    #[tokio::test]
    async fn abandoned_countdown_reclaims_arcs() {
        // A write_batch future aborted at the drain deadline drops its
        // waiter; late reports must still decrement harmlessly and release
        // their references.
        let clock = IoSystemClock;
        let inflight = Arc::new(BatchInflight::new(2));
        let opaques = [Arc::clone(&inflight), Arc::clone(&inflight)];
        {
            // Waiter is created and dropped without resolving (the abort).
            let _abandoned = inflight.wait(clock.now(), &clock);
        }
        for opaque in opaques {
            opaque.record_failure(None, "purged at teardown".to_string());
            opaque.complete_one();
            drop(opaque);
        }
        assert_eq!(
            Arc::strong_count(&inflight),
            1,
            "all opaque references reclaimed"
        );
        // A late waiter (none exists in practice) would still see completion.
        assert!(inflight.wait(clock.now(), &clock).await);
    }

    /// librdkafka's text for a SASL PLAIN login the broker refused.
    pub(crate) const SASL_REFUSED: &str = "sasl_plaintext://127.0.0.1:29192/bootstrap: SASL \
        authentication error: Authentication failed: Invalid username or password (after 301ms \
        in state AUTH_REQ)";

    const CERTIFICATE_UNVERIFIED: &str = "ssl://127.0.0.1:29193/bootstrap: SSL handshake failed: \
        error:0A000086:SSL routines::certificate verify failed: broker certificate could not be \
        verified (after 6ms in state SSL_HANDSHAKE)";

    /// A SASL failure and a certificate the client failed to verify are
    /// recorded, the latest wins, and it holds for exactly the window after
    /// its arrival on the context's clock.
    #[test]
    fn a_rejection_is_recorded_for_the_window() {
        use RDKafkaErrorCode as C;
        let (ctx, clock) = on_test_clock();
        let window = Duration::from_secs(75);

        ctx.note_error(C::Authentication, SASL_REFUSED);
        clock.advance(window);
        assert_eq!(
            ctx.rejection_within(window),
            Some((window, SASL_REFUSED.to_owned()))
        );
        clock.advance(Duration::from_nanos(1));
        assert_eq!(ctx.rejection_within(window), None);

        ctx.note_error(C::SSL, CERTIFICATE_UNVERIFIED);
        ctx.note_error(C::AllBrokersDown, "1/1 brokers are down");
        assert_eq!(
            ctx.rejection_within(window),
            Some((Duration::ZERO, CERTIFICATE_UNVERIFIED.to_owned())),
            "a later error that is not a rejection leaves the record"
        );
    }

    /// A rejection the producer's error callback reports is stamped on the
    /// context's clock and holds for the window from there.
    #[test]
    fn a_rejection_from_the_callback_holds_for_the_window() {
        use rdkafka::error::KafkaError;
        let (ctx, clock) = on_test_clock();
        let window = Duration::from_secs(75);

        clock.advance(Duration::from_secs(3600));
        ctx.error(
            KafkaError::Global(RDKafkaErrorCode::Authentication),
            SASL_REFUSED,
        );
        clock.advance(window);
        assert_eq!(
            ctx.rejection_within(window),
            Some((window, SASL_REFUSED.to_owned()))
        );
        clock.advance(Duration::from_nanos(1));
        assert_eq!(ctx.rejection_within(window), None);
        assert_eq!(
            ctx.latest_rejection().as_deref(),
            Some(SASL_REFUSED),
            "an aged rejection is still the latest"
        );
    }

    /// Pins that a SASL mechanism handshake ended by a dropped connection
    /// records nothing under `Authentication`, while a refused login under
    /// the same code is recorded.
    #[test]
    fn a_handshake_dropped_locally_is_not_a_rejection() {
        const DROPPED: &str = "sasl_plaintext://127.0.0.1:63121/bootstrap: SASL PLAIN mechanism \
            handshake failed: Local: Broker transport failure: broker's supported mechanisms: \
            (n/a) (after 0ms in state DOWN)";
        let (ctx, _clock) = on_test_clock();
        ctx.note_error(RDKafkaErrorCode::Authentication, DROPPED);
        assert_eq!(ctx.latest_rejection(), None);
        ctx.note_error(RDKafkaErrorCode::Authentication, SASL_REFUSED);
        assert_eq!(
            ctx.rejection_within(Duration::MAX),
            Some((Duration::ZERO, SASL_REFUSED.to_owned()))
        );
    }

    /// A refused connection, `AllBrokersDown`, a `decode error` alert and a
    /// malformed record are not rejections.
    #[test]
    fn other_errors_record_nothing() {
        use RDKafkaErrorCode as C;
        let (ctx, _clock) = on_test_clock();
        for (code, text) in [
            (
                C::BrokerTransportFailure,
                "127.0.0.1:1/bootstrap: Connect to ipv4#127.0.0.1:1 failed: Connection refused \
                 (after 0ms in state CONNECT)",
            ),
            (C::AllBrokersDown, "1/1 brokers are down"),
            (
                C::SSL,
                "ssl://127.0.0.1:54448/bootstrap: SSL handshake failed: error:0A00041A:SSL \
                 routines::tlsv1 alert decode error: SSL alert number 50 (after 0ms in state \
                 SSL_HANDSHAKE)",
            ),
            (
                C::SSL,
                "ssl://127.0.0.1:9093/bootstrap: SSL handshake failed: error:0A00010B:SSL \
                 routines::wrong version number (after 0ms in state SSL_HANDSHAKE)",
            ),
        ] {
            ctx.note_error(code, text);
            assert_eq!(ctx.latest_rejection(), None, "{code:?}: {text}");
        }
    }

    /// Each error event logs one warning when it arrives as the identical pair
    /// of callbacks rdkafka makes, and a SASL failure is still recorded.
    /// Regression for #747.
    #[test]
    fn an_error_event_is_logged_once() {
        use RDKafkaErrorCode as C;
        use rdkafka::error::KafkaError;
        const DOWN: &str = "1/1 brokers are down";
        let (ctx, _clock) = on_test_clock();
        let calls = [(C::AllBrokersDown, DOWN); 4]
            .into_iter()
            .chain([(C::Authentication, SASL_REFUSED); 2]);
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            for (code, text) in calls {
                ctx.error(KafkaError::Global(code), text);
            }
        });
        let count = |text: &str| lines.iter().filter(|line| line.contains(text)).count();
        assert_eq!(count(DOWN), 2, "two events, one line each: {lines:#?}");
        assert_eq!(count(SASL_REFUSED), 1, "one event, one line: {lines:#?}");
        assert!(ctx.latest_rejection().is_some());
    }

    /// Records every error callback a producer receives.
    #[derive(Default)]
    struct ErrorCalls(Mutex<Vec<(Option<RDKafkaErrorCode>, String)>>);

    impl ClientContext for ErrorCalls {
        fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
            self.0
                .lock()
                .expect("calls lock")
                .push((error.rdkafka_error_code(), reason.to_owned()));
        }
    }

    impl ProducerContext for ErrorCalls {
        type DeliveryOpaque = ();

        fn delivery(&self, _: &DeliveryResult<'_>, _: ()) {}
    }

    /// rdkafka's producer hands each error event to its context twice in a
    /// row, which `SinkContext::repeats_previous` and the feed's
    /// `FeedProducerContext::repeats_previous` depend on.
    #[test]
    fn rdkafka_delivers_each_producer_error_twice() {
        use rdkafka::ClientConfig;
        use rdkafka::producer::{Producer, ThreadedProducer};
        let producer: ThreadedProducer<ErrorCalls> = ClientConfig::new()
            .set("bootstrap.servers", crate::testutil::refused_broker())
            .create_with_context(ErrorCalls::default())
            .expect("producer");
        let context = Arc::clone(producer.context());
        spate_test::wait_until(Duration::from_secs(10), "two error callbacks", || {
            context.0.lock().expect("calls lock").len() >= 2
        });
        // Dropping the producer joins its poll thread, so every pair is complete.
        drop(producer);
        let calls = context.0.lock().expect("calls lock");
        assert!(
            calls.len() % 2 == 0 && calls.chunks(2).all(|pair| pair[0] == pair[1]),
            "rdkafka no longer repeats error callbacks: delete \
             `SinkContext::repeats_previous`, `FeedProducerContext::repeats_previous` \
             and this test. Calls: {calls:#?}"
        );
    }
}
