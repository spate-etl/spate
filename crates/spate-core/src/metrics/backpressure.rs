//! Backpressure handles (`spate_backpressure_*`).

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use metrics::{Counter, Gauge};
use std::time::Duration;

/// Backpressure handles (`spate_backpressure_*`).
#[derive(Debug)]
pub struct BackpressureMetrics {
    paused: OwnedGauge,
    paused_seconds: OwnedGauge,
    pause_events: Counter,
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
}

/// The in-flight byte budget's gauge (`spate_backpressure_inflight_bytes`).
///
/// The budget is one counter per pipeline, shared by every driver and every
/// sink, so this series is one per pipeline too.
///
/// It is separate from [`BackpressureMetrics`] rather than a field on it, and
/// must stay separate: a pipeline builds one of those per driver thread, so
/// folding the gauge back in would publish the same pipeline-wide number under
/// one label set per thread, and a `sum()` over the family would report the
/// thread count times the real usage.
#[derive(Debug)]
pub(crate) struct InflightBudgetMetrics {
    inflight_bytes: Gauge,
    _claim: SeriesClaim,
}

impl InflightBudgetMetrics {
    /// Resolve the budget gauge, failing when another live handle set already
    /// owns the series. Every instance owns its series, so there is no
    /// shadowing constructor.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub(crate) fn try_new(labels: &ComponentLabels) -> Result<Self, MetricsError> {
        // The claim carries a `budget` segment, so a handle set on the labels a
        // `BackpressureMetrics` already owns claims a separate key rather than
        // shadowing against it.
        let claim = SeriesClaim::try_claim(series_key("backpressure", labels, "budget"))?;
        Ok(InflightBudgetMetrics {
            inflight_bytes: labels.gauge(names::BACKPRESSURE_INFLIGHT_BYTES),
            _claim: claim,
        })
    }

    /// Publish the budget's current usage.
    #[inline]
    pub(crate) fn set_inflight_bytes(&self, bytes: usize) {
        self.inflight_bytes.set(bytes as f64);
    }
}
