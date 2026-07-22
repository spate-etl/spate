//! Queue-edge handles (`etl_queue_*`).

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use metrics::{Counter, SharedString};

/// Queue-edge handles (`etl_queue_*`).
#[derive(Debug)]
pub struct QueueMetrics {
    depth: OwnedGauge,
    full_events: Counter,
    _claim: Option<SeriesClaim>,
}

impl QueueMetrics {
    /// Resolve handles for one queue edge (e.g. `chain->sink/shard-3`) and
    /// publish its configured capacity.
    ///
    /// Claims this edge's series (the labels plus the queue name); a second
    /// live handle set for the same edge logs and becomes a shadow, counting
    /// but publishing neither depth nor capacity (see "Series ownership" in
    /// `docs/METRICS.md`).
    pub fn new(labels: &ComponentLabels, queue: &str, capacity: usize) -> Self {
        let claim = SeriesClaim::claim_or_shadow(Self::key(labels, queue));
        Self::build(labels, queue, capacity, claim)
    }

    /// Resolve handles for one queue edge, failing when another live handle
    /// set already owns it. The pipeline builder's path.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub fn try_new(
        labels: &ComponentLabels,
        queue: &str,
        capacity: usize,
    ) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(Self::key(labels, queue))?;
        Ok(Self::build(labels, queue, capacity, Some(claim)))
    }

    fn key(labels: &ComponentLabels, queue: &str) -> String {
        series_key("queue", labels, &format!("queue={queue}"))
    }

    fn build(
        labels: &ComponentLabels,
        queue: &str,
        capacity: usize,
        claim: Option<SeriesClaim>,
    ) -> Self {
        let owned = claim.is_some();
        let queue: SharedString = queue.to_owned().into();
        OwnedGauge::new(
            labels.gauge1(names::QUEUE_CAPACITY, names::L_QUEUE, queue.clone()),
            owned,
        )
        .set(capacity as f64);
        QueueMetrics {
            depth: OwnedGauge::new(
                labels.gauge1(names::QUEUE_DEPTH, names::L_QUEUE, queue.clone()),
                owned,
            ),
            full_events: labels.counter1(names::QUEUE_FULL_EVENTS_TOTAL, names::L_QUEUE, queue),
            _claim: claim,
        }
    }

    /// Set the current queue depth.
    #[inline]
    pub fn set_depth(&self, depth: usize) {
        self.depth.set(depth as f64);
    }

    /// Count `try_send` rejections.
    #[inline]
    pub fn full_events(&self, n: u64) {
        self.full_events.increment(n);
    }
}
