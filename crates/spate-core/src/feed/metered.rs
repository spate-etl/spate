//! The runtime's wrapper around the feed it hands a source.
//!
//! [`MeteredFeed`] forwards every call to the backend unchanged and records
//! the calls that receive, renew, settle, hand back and publish units under
//! `spate_feed_*`, so every backend is counted the same way with no code of
//! its own. [`DeadLetterSampler`] publishes the depth of the feed's
//! dead-letter destination.

use super::{
    Confirmation, Delivery, DepthOf, DepthReading, Feed, FeedError, FeedErrorKind, FeedMetricsCtx,
    FeedWaker, Receipt, Unit,
};
use crate::clock::{SharedClock, Stopwatch};
use crate::metrics::{FeedMetrics, FeedOp};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The default for
/// [`RuntimeOptions::dead_letter_read_interval`](crate::pipeline::RuntimeOptions::dead_letter_read_interval):
/// once a minute.
pub(crate) const DEAD_LETTER_READ_INTERVAL: Duration = Duration::from_secs(60);

/// A feed that records the unit and publish calls made through it.
///
/// Each call reaches the backend once, with the same arguments, and returns
/// what the backend returned.
pub(crate) struct MeteredFeed {
    inner: Arc<dyn Feed>,
    metrics: Arc<FeedMetrics>,
    clock: SharedClock,
}

impl MeteredFeed {
    /// Wrap `inner`, timing its calls on `clock`.
    pub(crate) fn new(
        inner: Arc<dyn Feed>,
        metrics: FeedMetrics,
        clock: SharedClock,
    ) -> MeteredFeed {
        MeteredFeed {
            inner,
            metrics: Arc::new(metrics),
            clock,
        }
    }

    fn call<T>(
        &self,
        op: FeedOp,
        call: impl FnOnce() -> Result<T, FeedError>,
    ) -> Result<T, FeedError> {
        let watch = Stopwatch::start(&*self.clock);
        let result = call();
        self.metrics.call(op, watch.elapsed(&*self.clock));
        if let Err(e) = &result {
            self.metrics.error(op, e.kind);
        }
        result
    }

    /// Record a call that settles a delivery. The source lets go of it once
    /// the feed accepts the call or answers Gone, and the ack rule has it make
    /// no later call with that receipt, so a plain count stays exact.
    fn settle(
        &self,
        op: FeedOp,
        accepted: fn(&FeedMetrics),
        call: impl FnOnce() -> Result<(), FeedError>,
    ) -> Result<(), FeedError> {
        let result = self.call(op, call);
        match &result {
            Ok(()) => {
                accepted(&self.metrics);
                self.metrics.released();
            }
            Err(e) if e.kind == FeedErrorKind::Gone => self.metrics.released(),
            Err(_) => {}
        }
        result
    }
}

impl Feed for MeteredFeed {
    fn backend(&self) -> &str {
        self.inner.backend()
    }

    fn set_waker(&self, waker: FeedWaker) {
        self.inner.set_waker(waker);
    }

    fn attach_metrics(&self, cx: FeedMetricsCtx) {
        self.inner.attach_metrics(cx);
    }

    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        let deliveries = self.call(FeedOp::Receive, || self.inner.receive(max))?;
        self.metrics.received(deliveries.len());
        Ok(deliveries)
    }

    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        let result = self.call(FeedOp::Renew, || self.inner.renew(receipt));
        match &result {
            Ok(()) => self.metrics.renewed(),
            Err(e) if e.kind == FeedErrorKind::Gone => self.metrics.released(),
            Err(_) => {}
        }
        result
    }

    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.settle(FeedOp::Acknowledge, FeedMetrics::acknowledged, || {
            self.inner.acknowledge(receipt)
        })
    }

    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.settle(FeedOp::Fail, FeedMetrics::failed, || {
            self.inner.fail(receipt, reason)
        })
    }

    fn publish(&self, unit: Unit) -> Confirmation {
        let watch = Stopwatch::start(&*self.clock);
        let confirmation = self.inner.publish(unit);
        self.metrics
            .call(FeedOp::Publish, watch.elapsed(&*self.clock));
        let metrics = Arc::clone(&self.metrics);
        let clock = Arc::clone(&self.clock);
        confirmation.on_settle(move |outcome| {
            metrics.publish_settled(outcome, watch.elapsed(&*clock));
        })
    }

    fn read_depth(&self, of: DepthOf) -> Option<DepthReading> {
        self.inner.read_depth(of)
    }

    fn hands_back(&self) -> bool {
        self.inner.hands_back()
    }

    fn stop_receiving(&self) {
        self.inner.stop_receiving();
    }

    fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.settle(FeedOp::HandBack, FeedMetrics::handed_back, || {
            self.inner.hand_back(receipt)
        })
    }

    fn close(&self, within: Duration) -> Option<Confirmation> {
        self.inner.close(within)
    }
}

/// Reads the depth of a feed's dead-letter destination every interval and
/// publishes each reading that settles as `spate_feed_dead_letter_units`.
///
/// A read that fails leaves the gauge at its last reading. A backend that
/// cannot count leaves it absent.
#[derive(Debug)]
pub(crate) struct DeadLetterSampler {
    interval: Duration,
    // `None` once the next read would fall past what an `Instant` holds.
    next: Option<Instant>,
    pending: Option<DepthReading>,
}

impl DeadLetterSampler {
    /// A sampler whose first read is due at `now`.
    pub(crate) fn new(interval: Duration, now: Instant) -> DeadLetterSampler {
        DeadLetterSampler {
            interval,
            next: Some(now),
            pending: None,
        }
    }

    /// Publish the pending reading if it has settled, and start the next one
    /// when it is due and none is pending. Never waits.
    pub(crate) fn sample(&mut self, feed: &MeteredFeed, now: Instant) {
        if let Some(reading) = &mut self.pending {
            match reading.try_outcome() {
                None => return,
                Some(Ok(depth)) => feed.metrics.set_dead_letters(
                    depth
                        .visible
                        .saturating_add(depth.delayed)
                        .saturating_add(depth.in_flight),
                ),
                Some(Err(e)) => tracing::warn!(
                    feed = feed.backend(),
                    error = %e,
                    "could not read the depth of the feed's dead-letter destination; \
                     `spate_feed_dead_letter_units` keeps its last reading"
                ),
            }
            self.pending = None;
        }
        if self.next.is_some_and(|due| now >= due) {
            self.next = now.checked_add(self.interval);
            self.pending = feed.read_depth(DepthOf::DeadLetters);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use crate::feed::{Confirmer, Depth, DepthReporter, confirmation, depth_reading};
    use crate::metrics::{ComponentLabels, Meter};
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusRecorder};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// A backend that logs every call, answers the calls that return a
    /// `Result` from a script of refusals, and leaves each publish, close and,
    /// when it `counts`, depth read for the test to settle.
    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<String>>,
        refusals: Mutex<VecDeque<FeedErrorKind>>,
        deliveries: Mutex<Vec<Delivery>>,
        confirmers: Mutex<Vec<Confirmer>>,
        reporters: Mutex<Vec<DepthReporter>>,
        counts: bool,
        // Moved by each renewal and publish, so a test can see what the call
        // took.
        call_takes: Mutex<Option<(Arc<TestClock>, Duration)>>,
    }

    impl Fake {
        fn log(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }

        fn answer(&self) -> Result<(), FeedError> {
            match self.refusals.lock().unwrap().pop_front() {
                Some(kind) => Err(FeedError::new(kind, "refused by the fake")),
                None => Ok(()),
            }
        }

        fn refuse(&self, kind: FeedErrorKind) {
            self.refusals.lock().unwrap().push_back(kind);
        }

        fn deliver(&self, receipts: impl IntoIterator<Item = u64>) {
            self.deliveries.lock().unwrap().extend(
                receipts
                    .into_iter()
                    .map(|n| Delivery::new(Unit::new(format!("u{n}"), Vec::new()), Receipt(n), 1)),
            );
        }

        fn take_time(&self) {
            if let Some((clock, by)) = &*self.call_takes.lock().unwrap() {
                clock.advance(*by);
            }
        }

        fn take_confirmer(&self) -> Confirmer {
            self.confirmers.lock().unwrap().remove(0)
        }

        fn take_reporter(&self) -> DepthReporter {
            self.reporters.lock().unwrap().remove(0)
        }
    }

    impl Feed for Fake {
        fn backend(&self) -> &str {
            "fake"
        }

        fn set_waker(&self, _waker: FeedWaker) {
            self.log("set_waker".into());
        }

        fn attach_metrics(&self, cx: FeedMetricsCtx) {
            self.log(format!(
                "attach_metrics {} {}",
                cx.meter.is_some(),
                cx.per_partition_detail
            ));
        }

        fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
            self.log(format!("receive {max}"));
            self.answer()?;
            Ok(std::mem::take(&mut *self.deliveries.lock().unwrap()))
        }

        fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.log(format!("renew {}", receipt.0));
            self.take_time();
            self.answer()
        }

        fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.log(format!("acknowledge {}", receipt.0));
            self.answer()
        }

        fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
            self.log(format!("fail {} {reason}", receipt.0));
            self.answer()
        }

        fn publish(&self, unit: Unit) -> Confirmation {
            self.log(format!("publish {}", unit.id));
            self.take_time();
            let (confirmer, confirmation) = confirmation();
            self.confirmers.lock().unwrap().push(confirmer);
            confirmation
        }

        fn read_depth(&self, of: DepthOf) -> Option<DepthReading> {
            self.log(format!("read_depth {of:?}"));
            self.counts.then(|| {
                let (reporter, reading) = depth_reading();
                self.reporters.lock().unwrap().push(reporter);
                reading
            })
        }

        fn hands_back(&self) -> bool {
            self.log("hands_back".into());
            true
        }

        fn stop_receiving(&self) {
            self.log("stop_receiving".into());
        }

        fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.log(format!("hand_back {}", receipt.0));
            self.answer()
        }

        fn close(&self, within: Duration) -> Option<Confirmation> {
            self.log(format!("close {within:?}"));
            let (confirmer, confirmation) = confirmation();
            self.confirmers.lock().unwrap().push(confirmer);
            Some(confirmation)
        }
    }

    /// A metered fake whose handles record into a recorder of the test's
    /// own, under `pipeline`.
    struct Rig {
        feed: MeteredFeed,
        fake: Arc<Fake>,
        clock: Arc<TestClock>,
        recorder: PrometheusRecorder,
        pipeline: &'static str,
    }

    impl Rig {
        fn new(pipeline: &'static str) -> Rig {
            Rig::with_fake(pipeline, Fake::default())
        }

        fn with_fake(pipeline: &'static str, fake: Fake) -> Rig {
            let recorder = PrometheusBuilder::new().build_recorder();
            let fake = Arc::new(fake);
            let clock = TestClock::frozen();
            let feed = metrics::with_local_recorder(&recorder, || {
                let labels = ComponentLabels::new(pipeline, "feed", "fake");
                let metrics = FeedMetrics::try_new(&labels).expect("free series");
                MeteredFeed::new(
                    Arc::clone(&fake) as Arc<dyn Feed>,
                    metrics,
                    Arc::clone(&clock) as SharedClock,
                )
            });
            Rig {
                feed,
                fake,
                clock,
                recorder,
                pipeline,
            }
        }

        /// The rendered value of `name` carrying the standard labels and
        /// then `extra`, such as `,op="renew"`; `None` when absent.
        fn value(&self, name: &str, extra: &str) -> Option<f64> {
            let series = format!(
                "{name}{{pipeline=\"{}\",component=\"feed\",component_type=\"fake\"{extra}}} ",
                self.pipeline
            );
            let text = self.recorder.handle().render();
            text.lines()
                .find_map(|line| line.strip_prefix(&series))
                .map(|value| value.parse().expect("a number"))
        }

        fn counter(&self, name: &str) -> f64 {
            self.value(name, "").unwrap_or(0.0)
        }

        fn errors(&self, op: &str, error_type: &str) -> f64 {
            let extra = format!(",op=\"{op}\",error_type=\"{error_type}\"");
            self.value("spate_feed_errors_total", &extra).unwrap_or(0.0)
        }

        fn held(&self) -> Option<f64> {
            self.value("spate_feed_held_units", "")
        }

        fn calls_timed(&self, op: &str) -> f64 {
            let extra = format!(",op=\"{op}\"");
            self.value("spate_feed_call_duration_seconds_count", &extra)
                .unwrap_or(0.0)
        }
    }

    /// Pins that every call reaches the backend once, with its arguments,
    /// and returns the backend's answer unchanged, including the answers of
    /// methods the trait gives defaults for.
    #[test]
    fn every_call_reaches_the_backend_once_with_its_answer() {
        let rig = Rig::new("metered-forwards");
        let (waker, _park) = crate::feed::wake_channel();
        rig.fake.deliver([7]);

        assert_eq!(rig.feed.backend(), "fake");
        rig.feed.attach_metrics(
            FeedMetricsCtx::new(Some(Meter::new("p", "feed", "fake"))).with_partition_detail(true),
        );
        rig.feed.set_waker(waker);
        let delivered = rig.feed.receive(4).unwrap();
        assert_eq!(delivered[0].receipt, Receipt(7));
        rig.fake.refuse(FeedErrorKind::Retryable);
        let refused = rig.feed.renew(Receipt(7)).unwrap_err();
        assert_eq!(
            refused,
            FeedError::new(FeedErrorKind::Retryable, "refused by the fake")
        );
        rig.feed.acknowledge(Receipt(7)).unwrap();
        rig.feed.fail(Receipt(8), "unreadable").unwrap();
        drop(rig.feed.publish(Unit::new("day-1", Vec::new())));
        assert!(rig.feed.read_depth(DepthOf::Feed).is_none());
        assert!(rig.feed.hands_back());
        rig.feed.stop_receiving();
        rig.feed.hand_back(Receipt(9)).unwrap();
        assert!(rig.feed.close(Duration::from_secs(2)).is_some());

        assert_eq!(
            *rig.fake.calls.lock().unwrap(),
            [
                "attach_metrics true true",
                "set_waker",
                "receive 4",
                "renew 7",
                "acknowledge 7",
                "fail 8 unreadable",
                "publish day-1",
                "read_depth Feed",
                "hands_back",
                "stop_receiving",
                "hand_back 9",
                "close 2s",
            ]
        );
    }

    /// Pins that the held gauge counts each delivery from its receipt until
    /// the feed accepts a call that settles it or answers Gone.
    #[test]
    fn the_held_gauge_counts_each_delivery_until_it_settles() {
        let rig = Rig::new("metered-held");
        assert_eq!(rig.held(), Some(0.0));
        assert!(rig.feed.receive(8).unwrap().is_empty());
        assert_eq!(rig.counter("spate_feed_units_received_total"), 0.0);

        rig.fake.deliver(1..=5);
        rig.feed.receive(8).unwrap();
        assert_eq!(rig.counter("spate_feed_units_received_total"), 5.0);
        assert_eq!(rig.held(), Some(5.0));

        rig.feed.acknowledge(Receipt(1)).unwrap();
        rig.feed.renew(Receipt(2)).unwrap();
        assert_eq!(rig.held(), Some(4.0), "a renewal keeps the delivery");
        rig.fake.refuse(FeedErrorKind::Gone);
        rig.feed.renew(Receipt(2)).unwrap_err();
        assert_eq!(rig.held(), Some(3.0));

        rig.feed.fail(Receipt(3), "unreadable").unwrap();
        rig.feed.hand_back(Receipt(4)).unwrap();
        assert_eq!(rig.held(), Some(1.0));

        assert_eq!(rig.counter("spate_feed_units_acknowledged_total"), 1.0);
        assert_eq!(rig.counter("spate_feed_units_failed_total"), 1.0);
        assert_eq!(rig.counter("spate_feed_units_handed_back_total"), 1.0);
        assert_eq!(rig.counter("spate_feed_renewals_total"), 1.0);
        assert_eq!(rig.errors("renew", "gone"), 1.0);
    }

    /// Pins that an answer other than Gone keeps the delivery held and counts
    /// nothing as settled, and that each error lands under its call and
    /// kind.
    #[test]
    fn a_refused_call_keeps_the_delivery_and_counts_its_kind() {
        let rig = Rig::new("metered-refused");
        rig.fake.deliver([1]);
        rig.feed.receive(8).unwrap();

        rig.fake.refuse(FeedErrorKind::Retryable);
        rig.feed.acknowledge(Receipt(1)).unwrap_err();
        rig.fake.refuse(FeedErrorKind::Refused);
        rig.feed.fail(Receipt(1), "unreadable").unwrap_err();
        rig.fake.refuse(FeedErrorKind::Fatal);
        rig.feed.hand_back(Receipt(1)).unwrap_err();
        rig.fake.refuse(FeedErrorKind::Retryable);
        rig.feed.renew(Receipt(1)).unwrap_err();
        rig.fake.refuse(FeedErrorKind::Fatal);
        rig.feed.receive(8).unwrap_err();

        assert_eq!(rig.held(), Some(1.0));
        assert_eq!(rig.errors("acknowledge", "retryable"), 1.0);
        assert_eq!(rig.errors("fail", "refused"), 1.0);
        assert_eq!(rig.errors("hand_back", "fatal"), 1.0);
        assert_eq!(rig.errors("renew", "retryable"), 1.0);
        assert_eq!(rig.errors("receive", "fatal"), 1.0);
        assert_eq!(rig.errors("receive", "retryable"), 0.0);
        for counter in [
            "spate_feed_units_acknowledged_total",
            "spate_feed_units_failed_total",
            "spate_feed_units_handed_back_total",
            "spate_feed_renewals_total",
        ] {
            assert_eq!(rig.counter(counter), 0.0, "{counter}");
        }

        rig.fake.refuse(FeedErrorKind::Gone);
        rig.feed.hand_back(Receipt(1)).unwrap_err();
        assert_eq!(rig.held(), Some(0.0), "Gone lets go of the delivery");
        assert_eq!(rig.counter("spate_feed_units_handed_back_total"), 0.0);
    }

    /// Pins that every recorded call is timed under its own `op`, across the
    /// backend's call, and that a publish dropped unseen counts nothing.
    #[test]
    fn every_call_is_timed_under_its_op() {
        let rig = Rig::new("metered-timed");
        *rig.fake.call_takes.lock().unwrap() =
            Some((Arc::clone(&rig.clock), Duration::from_millis(2)));
        rig.feed.receive(1).unwrap();
        rig.feed.receive(1).unwrap();
        rig.feed.renew(Receipt(1)).unwrap();
        rig.feed.acknowledge(Receipt(1)).unwrap();
        rig.feed.fail(Receipt(2), "x").unwrap();
        rig.feed.hand_back(Receipt(3)).unwrap();
        drop(rig.feed.publish(Unit::new("u", Vec::new())));

        assert_eq!(rig.calls_timed("receive"), 2.0);
        for op in ["renew", "acknowledge", "fail", "hand_back", "publish"] {
            assert_eq!(rig.calls_timed(op), 1.0, "{op}");
        }
        for op in ["renew", "publish"] {
            let extra = format!(",op=\"{op}\"");
            assert_eq!(
                rig.value("spate_feed_call_duration_seconds_sum", &extra),
                Some(0.002),
                "{op}"
            );
        }
        assert_eq!(rig.counter("spate_feed_units_published_total"), 0.0);
    }

    /// Pins that a publish whose backend drops its confirmer unsettled counts
    /// as a retryable publish error, on a check and on an await.
    #[test]
    fn a_publish_the_backend_drops_counts_as_a_retryable_error() {
        let rig = Rig::new("metered-dropped");
        let mut checked = rig.feed.publish(Unit::new("a", Vec::new()));
        drop(rig.fake.take_confirmer());
        let err = checked.try_outcome().expect("settled").unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Retryable);
        assert_eq!(rig.errors("publish", "retryable"), 1.0);

        let awaited = rig.feed.publish(Unit::new("b", Vec::new()));
        drop(rig.fake.take_confirmer());
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(awaited).unwrap_err();
        assert_eq!(rig.errors("publish", "retryable"), 2.0);
    }

    /// Pins that a publish is counted once, by outcome, when a check or an
    /// await first finds its confirmation settled, timed on the feed's clock,
    /// and that the caller sees the backend's outcome.
    #[test]
    fn a_publish_is_counted_when_its_confirmation_settles() {
        let rig = Rig::new("metered-publish");
        let mut checked = rig.feed.publish(Unit::new("a", Vec::new()));
        assert!(checked.try_outcome().is_none());
        assert_eq!(
            rig.value("spate_feed_publish_duration_seconds_count", ""),
            Some(0.0)
        );
        rig.clock.advance(Duration::from_secs(3));
        rig.fake.take_confirmer().confirm();
        assert!(matches!(checked.try_outcome(), Some(Ok(()))));
        assert!(matches!(checked.try_outcome(), Some(Ok(()))));
        assert_eq!(rig.counter("spate_feed_units_published_total"), 1.0);
        assert_eq!(
            rig.value("spate_feed_publish_duration_seconds_sum", ""),
            Some(3.0)
        );

        let awaited = rig.feed.publish(Unit::new("b", Vec::new()));
        rig.fake
            .take_confirmer()
            .reject(FeedError::new(FeedErrorKind::Refused, "too large"));
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let err = rt.block_on(awaited).unwrap_err();
        assert_eq!(err, FeedError::new(FeedErrorKind::Refused, "too large"));

        assert_eq!(rig.counter("spate_feed_units_published_total"), 1.0);
        assert_eq!(rig.errors("publish", "refused"), 1.0);
        assert_eq!(
            rig.value("spate_feed_publish_duration_seconds_count", ""),
            Some(2.0)
        );
    }
    fn dead_letters(n: u64) -> Depth {
        let mut depth = Depth::new("the dead letters", true);
        depth.visible = n;
        depth.delayed = 2;
        depth.in_flight = 1;
        depth.unsent = 100;
        depth
    }

    /// A metered fake that counts and leaves its depth reads pending.
    fn counting(pipeline: &'static str) -> Rig {
        Rig::with_fake(
            pipeline,
            Fake {
                counts: true,
                ..Fake::default()
            },
        )
    }

    /// Run one sample at `t0 + at` under the rig's recorder, where the gauge
    /// registers on its first reading.
    fn sample(rig: &Rig, sampler: &mut DeadLetterSampler, t0: Instant, at: Duration) {
        metrics::with_local_recorder(&rig.recorder, || sampler.sample(&rig.feed, t0 + at));
    }

    fn dead_letter_reads(rig: &Rig) -> usize {
        rig.fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| *call == "read_depth DeadLetters")
            .count()
    }

    /// Pins the sampler's cadence: a read starts when due and only while
    /// none is pending, and a settled reading is published as the visible,
    /// delayed and in-flight units it counts.
    #[test]
    fn the_dead_letter_sampler_publishes_each_settled_reading() {
        let rig = counting("metered-dead-letters");
        let gauge = || rig.value("spate_feed_dead_letter_units", "");
        let t0 = Instant::now();
        let mut sampler = DeadLetterSampler::new(Duration::from_secs(60), t0);

        sample(&rig, &mut sampler, t0, Duration::ZERO);
        assert_eq!(dead_letter_reads(&rig), 1);
        sample(&rig, &mut sampler, t0, Duration::from_secs(61));
        assert_eq!(
            dead_letter_reads(&rig),
            1,
            "no second read while one is pending"
        );
        assert_eq!(gauge(), None, "absent until a reading settles");

        rig.fake.take_reporter().report(Ok(dead_letters(4)));
        sample(&rig, &mut sampler, t0, Duration::from_secs(62));
        assert_eq!(gauge(), Some(7.0));
        assert_eq!(
            dead_letter_reads(&rig),
            2,
            "the overdue read starts once the first settles"
        );

        rig.fake
            .take_reporter()
            .report(Err(FeedError::new(FeedErrorKind::Retryable, "throttled")));
        sample(&rig, &mut sampler, t0, Duration::from_secs(63));
        assert_eq!(gauge(), Some(7.0), "a failed read keeps the last reading");
        sample(&rig, &mut sampler, t0, Duration::from_secs(121));
        assert_eq!(
            dead_letter_reads(&rig),
            2,
            "the next read waits out the interval"
        );
        sample(&rig, &mut sampler, t0, Duration::from_secs(122));
        assert_eq!(dead_letter_reads(&rig), 3);
    }

    /// Pins that an interval too long for an `Instant` reads once, at
    /// startup, and never again, rather than overflowing, and that a zero
    /// interval reads again whenever no read is pending.
    #[test]
    fn the_interval_bounds_the_dead_letter_reads() {
        let rig = counting("metered-dead-letters-bounds");
        let t0 = Instant::now();
        let mut once = DeadLetterSampler::new(Duration::MAX, t0);
        sample(&rig, &mut once, t0, Duration::ZERO);
        rig.fake.take_reporter().report(Ok(dead_letters(0)));
        sample(&rig, &mut once, t0, Duration::from_secs(1));
        sample(&rig, &mut once, t0, Duration::from_secs(86_400));
        assert_eq!(rig.value("spate_feed_dead_letter_units", ""), Some(3.0));
        assert_eq!(dead_letter_reads(&rig), 1, "one read, at startup");

        let mut always = DeadLetterSampler::new(Duration::ZERO, t0);
        for _ in 0..3 {
            sample(&rig, &mut always, t0, Duration::ZERO);
            rig.fake.take_reporter().report(Ok(dead_letters(0)));
        }
        assert_eq!(dead_letter_reads(&rig), 4);
    }

    /// Pins that a backend that cannot count leaves the gauge absent, and
    /// is asked again each interval.
    #[test]
    fn a_feed_that_cannot_count_leaves_the_dead_letter_gauge_absent() {
        let rig = Rig::new("metered-no-depth");
        let t0 = Instant::now();
        let mut sampler = DeadLetterSampler::new(Duration::from_secs(60), t0);
        for at in [0, 1, 60] {
            sample(&rig, &mut sampler, t0, Duration::from_secs(at));
        }
        assert_eq!(dead_letter_reads(&rig), 2);
        assert_eq!(rig.value("spate_feed_dead_letter_units", ""), None);
    }
}
