//! Backpressure handles (`spate_backpressure_*`).

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use metrics::Counter;
use std::time::Duration;

/// Backpressure handles (`spate_backpressure_*`).
#[derive(Debug)]
pub struct BackpressureMetrics {
    paused: OwnedGauge,
    paused_seconds: OwnedGauge,
    pause_events: Counter,
    inflight_bytes: OwnedGauge,
    _claim: Option<SeriesClaim>,
}

impl BackpressureMetrics {
    /// Resolve all backpressure handles.
    ///
    /// Claims the `spate_backpressure_*` series for these labels; if another
    /// live handle set already owns them this one logs and becomes a shadow,
    /// counting but publishing no gauge. Each pipeline thread's controller
    /// gets its own `component` label, so they own separate series.
    pub fn new(labels: &ComponentLabels) -> Self {
        Self::build(labels, SeriesClaim::claim_or_shadow(Self::key(labels)))
    }

    /// Resolve all backpressure handles, failing when another live handle set
    /// already owns the series. The pipeline runtime's path.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub fn try_new(labels: &ComponentLabels) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(Self::key(labels))?;
        Ok(Self::build(labels, Some(claim)))
    }

    fn key(labels: &ComponentLabels) -> String {
        series_key("backpressure", labels, "")
    }

    fn build(labels: &ComponentLabels, claim: Option<SeriesClaim>) -> Self {
        let owned = claim.is_some();
        BackpressureMetrics {
            paused: OwnedGauge::new(labels.gauge(names::BACKPRESSURE_PAUSED), owned),
            paused_seconds: OwnedGauge::new(
                labels.gauge(names::BACKPRESSURE_PAUSED_SECONDS_TOTAL),
                owned,
            ),
            pause_events: labels.counter(names::BACKPRESSURE_PAUSE_EVENTS_TOTAL),
            inflight_bytes: OwnedGauge::new(
                labels.gauge(names::BACKPRESSURE_INFLIGHT_BYTES),
                owned,
            ),
            _claim: claim,
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
