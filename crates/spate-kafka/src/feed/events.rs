//! The feed's countable events. Each passes through one function here, so a
//! metric family for it has one place to count.

use super::write::{Destination, Move};
use std::time::Duration;

/// Why a message went to the dead-letter topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cause {
    /// A unit failed on a delivery at or past the attempt cap, or a unit
    /// whose recorded attempts had reached the cap, moved without being
    /// read.
    AttemptCap,
    /// A message with no key, or a key that is not UTF-8.
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
pub(crate) fn attempt_written(confirmed_in: Duration) {
    tracing::trace!(?confirmed_in, "a unit's attempt was recorded");
}

/// A tombstone of a unit's attempt record was confirmed, and the unit's
/// offset finished.
pub(crate) fn tombstoned() {
    tracing::trace!("a unit's attempt record was tombstoned");
}

/// A move to the retry or dead-letter topic was confirmed.
pub(crate) fn moved(moving: &Move) {
    let confirmed_in = moving.started.elapsed();
    match moving.to {
        Destination::Retry => retry_written(moving, confirmed_in),
        Destination::DeadLetter(cause) => dead_lettered(moving, cause, confirmed_in),
    }
}

fn retry_written(moving: &Move, confirmed_in: Duration) {
    tracing::debug!(
        unit = moving.unit.as_deref().unwrap_or_default(),
        deliveries = moving.deliveries,
        ?confirmed_in,
        "a failed unit was written to the retry topic"
    );
}

fn dead_lettered(moving: &Move, cause: Cause, confirmed_in: Duration) {
    tracing::info!(
        unit = moving.unit.as_deref().unwrap_or_default(),
        deliveries = moving.deliveries,
        cause = cause.label(),
        ?confirmed_in,
        "a message was written to the dead-letter topic"
    );
}
