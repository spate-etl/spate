//! Checkpointer handles (`etl_checkpoint_*`).

use super::labels::{ComponentLabels, PartitionGauges};
use super::names;
use crate::record::PartitionId;
use metrics::{Counter, Gauge, Histogram};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// Checkpointer handles (`etl_checkpoint_*`).
#[derive(Debug)]
pub struct CheckpointMetrics {
    pending_max: Gauge,
    commits_ok: Counter,
    commits_err: Counter,
    commit_duration: Histogram,
    watermark_age: Gauge,
    partition_pending: Option<PartitionGauges>,
}

impl CheckpointMetrics {
    /// Resolve all checkpointer handles.
    pub fn new(labels: &ComponentLabels, per_partition_detail: bool) -> Self {
        CheckpointMetrics {
            pending_max: labels.gauge(names::CHECKPOINT_PENDING_BATCHES),
            commits_ok: labels.counter1(names::CHECKPOINT_COMMITS_TOTAL, names::L_OUTCOME, "ok"),
            commits_err: labels.counter1(
                names::CHECKPOINT_COMMITS_TOTAL,
                names::L_OUTCOME,
                "error",
            ),
            commit_duration: labels.histogram(names::CHECKPOINT_COMMIT_DURATION_SECONDS),
            watermark_age: labels.gauge(names::CHECKPOINT_WATERMARK_AGE_SECONDS),
            partition_pending: per_partition_detail.then(|| PartitionGauges {
                name: names::CHECKPOINT_PENDING_BATCHES,
                labels: labels.clone(),
                gauges: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Set the max pending-batch count across partitions.
    pub fn set_pending_max(&self, pending: usize) {
        self.pending_max.set(pending as f64);
    }

    /// Set one partition's pending count. No-op unless
    /// `per_partition_detail`.
    pub fn set_partition_pending(&self, partition: PartitionId, pending: usize) {
        if let Some(pg) = &self.partition_pending {
            pg.set(partition, pending as f64);
        }
    }

    /// Drop per-partition series for revoked partitions.
    pub fn retain_partitions(&self, keep: &[PartitionId]) {
        if let Some(pg) = &self.partition_pending {
            pg.retain(keep);
        }
    }

    /// Record one source commit call.
    pub fn commit(&self, ok: bool, d: Duration) {
        if ok {
            self.commits_ok.increment(1);
        } else {
            self.commits_err.increment(1);
        }
        self.commit_duration.record(d.as_secs_f64());
    }

    /// Set the age of the oldest unacknowledged batch.
    pub fn set_watermark_age(&self, age: Duration) {
        self.watermark_age.set(age.as_secs_f64());
    }
}
