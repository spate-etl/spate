//! Backpressure handles (`etl_backpressure_*`).

use super::labels::ComponentLabels;
use super::names;
use metrics::{Counter, Gauge};
use std::time::Duration;

/// Backpressure handles (`etl_backpressure_*`).
#[derive(Debug)]
pub struct BackpressureMetrics {
    paused: Gauge,
    paused_seconds: Gauge,
    pause_events: Counter,
    inflight_bytes: Gauge,
}

impl BackpressureMetrics {
    /// Resolve all backpressure handles.
    pub fn new(labels: &ComponentLabels) -> Self {
        BackpressureMetrics {
            paused: labels.gauge(names::BACKPRESSURE_PAUSED),
            paused_seconds: labels.gauge(names::BACKPRESSURE_PAUSED_SECONDS_TOTAL),
            pause_events: labels.counter(names::BACKPRESSURE_PAUSE_EVENTS_TOTAL),
            inflight_bytes: labels.gauge(names::BACKPRESSURE_INFLIGHT_BYTES),
        }
    }

    /// Record a pause transition.
    pub fn pause_started(&self) {
        self.paused.set(1.0);
        self.pause_events.increment(1);
    }

    /// Record a resume transition and the time spent paused.
    pub fn pause_ended(&self, paused_for: Duration) {
        self.paused.set(0.0);
        // Monotonic accumulator; gauge because the facade counter is
        // integer-only (see names.rs).
        self.paused_seconds.increment(paused_for.as_secs_f64());
    }

    /// Set the current in-flight byte budget usage.
    #[inline]
    pub fn set_inflight_bytes(&self, bytes: usize) {
        self.inflight_bytes.set(bytes as f64);
    }
}
