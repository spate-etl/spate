//! Feed handles (`spate_feed_*`): what the runtime records about the calls a
//! source makes through its feed.
//!
//! One handle set per pipeline, resolved by the runtime when it wraps the feed
//! it hands the source. Every label takes its values from a fixed set: the
//! recorded feed calls, the [`FeedErrorKind`]s, and the backend's key as the
//! `component_type`.

use super::MetricsError;
use super::labels::ComponentLabels;
use super::names;
use super::ownership::{SeriesClaim, series_key};
use crate::feed::{FeedError, FeedErrorKind};
use metrics::{Counter, Gauge, Histogram};
use std::sync::OnceLock;
use std::time::Duration;

/// A feed call, as the `op` label of the `spate_feed_*` families names it.
///
/// A feed backend labels its own per-call families with [`label`](FeedOp::label),
/// so they join the framework's on `op`.
///
/// # Examples
///
/// ```
/// use spate_core::metrics::{FeedOp, Meter};
///
/// let meter = Meter::with_namespace("queue_feed", "orders", "feed", "queue");
/// let retries = FeedOp::ALL.map(|op| meter.counter("retries_total", &[("op", op.label().into())]));
/// retries[FeedOp::Publish as usize].increment(1);
/// assert_eq!(FeedOp::HandBack.label(), "hand_back");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FeedOp {
    /// [`Feed::receive`](crate::feed::Feed::receive).
    Receive,
    /// [`Feed::renew`](crate::feed::Feed::renew).
    Renew,
    /// [`Feed::acknowledge`](crate::feed::Feed::acknowledge).
    Acknowledge,
    /// [`Feed::fail`](crate::feed::Feed::fail).
    Fail,
    /// [`Feed::hand_back`](crate::feed::Feed::hand_back).
    HandBack,
    /// [`Feed::publish`](crate::feed::Feed::publish).
    Publish,
}

impl FeedOp {
    /// Every call, in discriminant order, so `op as usize` indexes a table
    /// built from it.
    pub const ALL: [FeedOp; 6] = [
        FeedOp::Receive,
        FeedOp::Renew,
        FeedOp::Acknowledge,
        FeedOp::Fail,
        FeedOp::HandBack,
        FeedOp::Publish,
    ];

    /// The `op` label value.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            FeedOp::Receive => "receive",
            FeedOp::Renew => "renew",
            FeedOp::Acknowledge => "acknowledge",
            FeedOp::Fail => "fail",
            FeedOp::HandBack => "hand_back",
            FeedOp::Publish => "publish",
        }
    }
}

/// The `error_type` label values, in the order [`kind_index`] numbers them.
const ERROR_TYPES: [&str; 4] = ["gone", "refused", "retryable", "fatal"];

fn kind_index(kind: FeedErrorKind) -> usize {
    match kind {
        FeedErrorKind::Gone => 0,
        FeedErrorKind::Refused => 1,
        FeedErrorKind::Retryable => 2,
        FeedErrorKind::Fatal => 3,
    }
}

/// Feed handles (`spate_feed_*`).
#[derive(Debug)]
pub(crate) struct FeedMetrics {
    labels: ComponentLabels,
    received: Counter,
    acknowledged: Counter,
    failed: Counter,
    handed_back: Counter,
    published: Counter,
    renewals: Counter,
    errors: [[Counter; ERROR_TYPES.len()]; FeedOp::ALL.len()],
    call_duration: [Histogram; FeedOp::ALL.len()],
    publish_duration: Histogram,
    held_units: Gauge,
    // Registered by the first reading, so the series is absent until a depth
    // has been measured rather than reading as an empty destination.
    dead_letter_units: OnceLock<Gauge>,
    _claim: SeriesClaim,
}

impl FeedMetrics {
    /// Resolve the feed handles, failing when another live handle set already
    /// owns the series.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub(crate) fn try_new(labels: &ComponentLabels) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(series_key("feed", labels, ""))?;
        let held_units = labels.gauge(names::FEED_HELD_UNITS);
        held_units.set(0.0);
        Ok(FeedMetrics {
            labels: labels.clone(),
            received: labels.counter(names::FEED_UNITS_RECEIVED_TOTAL),
            acknowledged: labels.counter(names::FEED_UNITS_ACKNOWLEDGED_TOTAL),
            failed: labels.counter(names::FEED_UNITS_FAILED_TOTAL),
            handed_back: labels.counter(names::FEED_UNITS_HANDED_BACK_TOTAL),
            published: labels.counter(names::FEED_UNITS_PUBLISHED_TOTAL),
            renewals: labels.counter(names::FEED_RENEWALS_TOTAL),
            errors: FeedOp::ALL.map(|op| {
                ERROR_TYPES.map(|error_type| {
                    labels.counter2(
                        names::FEED_ERRORS_TOTAL,
                        names::L_OP,
                        op.label(),
                        names::L_ERROR_TYPE,
                        error_type,
                    )
                })
            }),
            call_duration: FeedOp::ALL.map(|op| {
                labels.histogram1(names::FEED_CALL_DURATION_SECONDS, names::L_OP, op.label())
            }),
            publish_duration: labels.histogram(names::FEED_PUBLISH_DURATION_SECONDS),
            held_units,
            dead_letter_units: OnceLock::new(),
            _claim: claim,
        })
    }

    pub(crate) fn received(&self, units: usize) {
        self.received.increment(units as u64);
        self.held_units.increment(units as f64);
    }

    /// The source let go of one delivery it held.
    pub(crate) fn released(&self) {
        self.held_units.decrement(1.0);
    }

    pub(crate) fn set_dead_letters(&self, units: u64) {
        self.dead_letter_units
            .get_or_init(|| self.labels.gauge(names::FEED_DEAD_LETTER_UNITS))
            .set(units as f64);
    }

    pub(crate) fn acknowledged(&self) {
        self.acknowledged.increment(1);
    }

    pub(crate) fn failed(&self) {
        self.failed.increment(1);
    }

    pub(crate) fn handed_back(&self) {
        self.handed_back.increment(1);
    }

    pub(crate) fn renewed(&self) {
        self.renewals.increment(1);
    }

    pub(crate) fn error(&self, op: FeedOp, kind: FeedErrorKind) {
        self.errors[op as usize][kind_index(kind)].increment(1);
    }

    pub(crate) fn call(&self, op: FeedOp, elapsed: Duration) {
        self.call_duration[op as usize].record(elapsed);
    }

    /// Record a publish that settled `elapsed` after it was made.
    pub(crate) fn publish_settled(&self, outcome: &Result<(), FeedError>, elapsed: Duration) {
        self.publish_duration.record(elapsed);
        match outcome {
            Ok(()) => self.published.increment(1),
            Err(e) => self.error(FeedOp::Publish, e.kind),
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// Pins that `ALL` lists the calls in discriminant order, which the
    /// handle tables are indexed by.
    #[test]
    fn every_feed_op_sits_at_its_discriminant() {
        for (i, op) in FeedOp::ALL.into_iter().enumerate() {
            assert_eq!(op as usize, i, "{op:?}");
        }
    }

    /// Pins that a second handle set on the same labels is refused, so the
    /// held and dead-letter gauges have one owner (INV-10).
    #[test]
    fn a_second_feed_handle_set_is_refused() {
        let labels = ComponentLabels::new("feed-duplicate", "feed", "memory");
        let owner = FeedMetrics::try_new(&labels).unwrap();
        let err = FeedMetrics::try_new(&labels).unwrap_err();
        assert!(matches!(err, MetricsError::DuplicateSeries(_)), "{err}");
        drop(owner);
        FeedMetrics::try_new(&labels).expect("the claim frees with its owner");
    }
}
