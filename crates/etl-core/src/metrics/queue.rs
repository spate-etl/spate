//! Queue-edge handles (`etl_queue_*`).

use super::labels::ComponentLabels;
use super::names;
use metrics::{Counter, Gauge, SharedString};

/// Queue-edge handles (`etl_queue_*`).
#[derive(Debug)]
pub struct QueueMetrics {
    depth: Gauge,
    full_events: Counter,
}

impl QueueMetrics {
    /// Resolve handles for one queue edge (e.g. `chain->sink/shard-3`) and
    /// publish its configured capacity.
    pub fn new(labels: &ComponentLabels, queue: &str, capacity: usize) -> Self {
        let queue: SharedString = queue.to_owned().into();
        labels
            .gauge1(names::QUEUE_CAPACITY, names::L_QUEUE, queue.clone())
            .set(capacity as f64);
        QueueMetrics {
            depth: labels.gauge1(names::QUEUE_DEPTH, names::L_QUEUE, queue.clone()),
            full_events: labels.counter1(names::QUEUE_FULL_EVENTS_TOTAL, names::L_QUEUE, queue),
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
