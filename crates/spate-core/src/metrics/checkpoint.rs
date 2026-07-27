//! Checkpointer handles (`spate_checkpoint_*`).

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge, PartitionGauges};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use crate::record::PartitionId;
use metrics::{Counter, Histogram};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// Checkpointer handles (`spate_checkpoint_*`).
#[derive(Debug)]
pub struct CheckpointMetrics {
    pending_max: OwnedGauge,
    commits_ok: Counter,
    commits_err: Counter,
    commit_duration: Histogram,
    watermark_age: OwnedGauge,
    partition_pending: Option<PartitionGauges>,
    _claim: Option<SeriesClaim>,
}

impl CheckpointMetrics {
    /// Resolve all checkpointer handles.
    ///
    /// Claims the `spate_checkpoint_*` series for these labels; a second live
    /// handle set logs and becomes a shadow, counting commits but publishing
    /// no gauge (see "Series ownership" in `docs/METRICS.md`).
    pub fn new(labels: &ComponentLabels, per_partition_detail: bool) -> Self {
        let claim = SeriesClaim::claim_or_shadow(Self::key(labels));
        Self::build(labels, per_partition_detail, claim)
    }

    /// Resolve all checkpointer handles, failing when another live handle set
    /// already owns the series. The pipeline runtime's path.
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub fn try_new(
        labels: &ComponentLabels,
        per_partition_detail: bool,
    ) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(Self::key(labels))?;
        Ok(Self::build(labels, per_partition_detail, Some(claim)))
    }

    fn key(labels: &ComponentLabels) -> String {
        series_key("checkpoint", labels, "")
    }

    fn build(
        labels: &ComponentLabels,
        per_partition_detail: bool,
        claim: Option<SeriesClaim>,
    ) -> Self {
        let owned = claim.is_some();
        CheckpointMetrics {
            pending_max: OwnedGauge::new(labels.gauge(names::CHECKPOINT_PENDING_BATCHES), owned),
            commits_ok: labels.counter1(names::CHECKPOINT_COMMITS_TOTAL, names::L_OUTCOME, "ok"),
            commits_err: labels.counter1(
                names::CHECKPOINT_COMMITS_TOTAL,
                names::L_OUTCOME,
                "error",
            ),
            commit_duration: labels.histogram(names::CHECKPOINT_COMMIT_DURATION_SECONDS),
            watermark_age: OwnedGauge::new(
                labels.gauge(names::CHECKPOINT_WATERMARK_AGE_SECONDS),
                owned,
            ),
            partition_pending: per_partition_detail.then(|| PartitionGauges {
                name: names::CHECKPOINT_PENDING_BATCHES,
                labels: labels.clone(),
                gauges: Mutex::new(HashMap::new()),
                owned,
            }),
            _claim: claim,
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
