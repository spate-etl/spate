//! The feed's countable events. Each passes through one function here, so a
//! metric family for it has one place to count.

use super::Topics;
use super::inflight::Tp;
use super::write::{Destination, Move};
use crate::metrics::L_TOPIC;
use spate_core::clock::Clock;
use spate_core::metrics::names::L_REASON;
use spate_core::metrics::{Counter, Histogram, Meter};
use std::time::Duration;

const ATTEMPT_WRITE_DURATION_SECONDS: &str = "attempt_write_duration_seconds";
const RETRY_WRITES_TOTAL: &str = "retry_writes_total";
const DEAD_LETTER_WRITES_TOTAL: &str = "dead_letter_writes_total";
const OFFSETS_SKIPPED_TOTAL: &str = "offsets_skipped_total";

/// The feed's own write families, resolved once from the feed's `Meter`.
#[derive(Debug)]
pub(crate) struct KafkaFeedMetrics {
    /// From an attempt record's first send to its confirmation; its count is
    /// the attempt records written.
    attempt_write_duration: Histogram,
    retry_writes: Counter,
    dead_letter_attempt_cap: Counter,
    dead_letter_no_unit: Counter,
    offsets_skipped_work: Counter,
    offsets_skipped_retry: Counter,
}

impl KafkaFeedMetrics {
    /// Resolve every handle, one dead-letter series per [`Cause`] and one
    /// skipped-offsets series per topic the feed reads, so each renders from
    /// the attach on.
    pub(crate) fn new(meter: &Meter, topics: &Topics) -> Self {
        let skipped = |topic: &str| {
            meter.counter(OFFSETS_SKIPPED_TOTAL, &[(L_TOPIC, topic.to_owned().into())])
        };
        let dead_letter = |cause: Cause| {
            meter.counter(
                DEAD_LETTER_WRITES_TOTAL,
                &[(L_REASON, cause.label().into())],
            )
        };
        KafkaFeedMetrics {
            attempt_write_duration: meter.histogram(ATTEMPT_WRITE_DURATION_SECONDS, &[]),
            retry_writes: meter.counter(RETRY_WRITES_TOTAL, &[]),
            dead_letter_attempt_cap: dead_letter(Cause::AttemptCap),
            dead_letter_no_unit: dead_letter(Cause::NoUnit),
            offsets_skipped_work: skipped(&topics.work),
            offsets_skipped_retry: skipped(&topics.retry),
        }
    }

    fn offsets_skipped(&self, tp: &Tp, topics: &Topics) -> &Counter {
        if tp.topic == topics.work {
            &self.offsets_skipped_work
        } else {
            &self.offsets_skipped_retry
        }
    }

    fn dead_letter_writes(&self, cause: Cause) -> &Counter {
        match cause {
            Cause::AttemptCap => &self.dead_letter_attempt_cap,
            Cause::NoUnit => &self.dead_letter_no_unit,
        }
    }
}

/// Why a message went to the dead-letter topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cause {
    /// A unit failed on a delivery at or past the attempt cap, or a unit
    /// whose recorded attempts had reached the cap, moved without being
    /// read.
    AttemptCap,
    /// A message whose key is not UTF-8.
    NoUnit,
}

impl Cause {
    fn label(self) -> &'static str {
        match self {
            Cause::AttemptCap => "attempt_cap",
            Cause::NoUnit => "no_unit",
        }
    }
}

/// A delivery report arrived, or librdkafka refused a write at enqueue.
pub(crate) fn delivery_report(ok: bool) {
    tracing::trace!(ok, "a Kafka feed write settled");
}

/// A unit's attempt record was confirmed `confirmed_in` after its first
/// send.
pub(crate) fn attempt_written(metrics: Option<&KafkaFeedMetrics>, confirmed_in: Duration) {
    tracing::trace!(?confirmed_in, "a unit's attempt was recorded");
    if let Some(m) = metrics {
        m.attempt_write_duration.record(confirmed_in.as_secs_f64());
    }
}

/// A tombstone of a unit's attempt record was confirmed, and the unit's
/// offset finished.
pub(crate) fn tombstoned() {
    tracing::trace!("a unit's attempt record was tombstoned");
}

/// Offsets `from..to` of `tp` were below its log start when its assignment
/// started reading at `from`: retention deleted them before the group
/// finished them.
pub(crate) fn offsets_skipped(
    metrics: Option<&KafkaFeedMetrics>,
    topics: &Topics,
    tp: &Tp,
    from: i64,
    to: i64,
) {
    if let Some(m) = metrics {
        m.offsets_skipped(tp, topics)
            .increment(u64::try_from(to.saturating_sub(from)).unwrap_or(0));
    }
    tracing::warn!(
        topic = &*tp.topic,
        partition = tp.partition,
        from,
        to,
        "offsets the feed's group had not finished were deleted before this worker could read \
         them; a unit there was either never read or lost when a rebalance moved it"
    );
}

/// A move to the retry or dead-letter topic was confirmed.
pub(crate) fn moved(metrics: Option<&KafkaFeedMetrics>, moving: &Move, clock: &dyn Clock) {
    let confirmed_in = moving.started.elapsed(clock);
    match moving.to {
        Destination::Retry => retry_written(metrics, moving, confirmed_in),
        Destination::DeadLetter(cause) => dead_lettered(metrics, moving, cause, confirmed_in),
    }
}

fn retry_written(metrics: Option<&KafkaFeedMetrics>, moving: &Move, confirmed_in: Duration) {
    if let Some(m) = metrics {
        m.retry_writes.increment(1);
    }
    tracing::debug!(
        unit = moving.unit.as_deref().unwrap_or_default(),
        deliveries = moving.deliveries,
        ?confirmed_in,
        "a failed unit was written to the retry topic"
    );
}

fn dead_lettered(
    metrics: Option<&KafkaFeedMetrics>,
    moving: &Move,
    cause: Cause,
    confirmed_in: Duration,
) {
    if let Some(m) = metrics {
        m.dead_letter_writes(cause).increment(1);
    }
    tracing::info!(
        unit = moving.unit.as_deref().unwrap_or_default(),
        deliveries = moving.deliveries,
        cause = cause.label(),
        ?confirmed_in,
        "a message was written to the dead-letter topic"
    );
}
