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
    /// no gauge.
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

    /// Whether this handle set publishes the per-partition series.
    ///
    /// False when `per_partition_detail` was not set, and false for a shadow,
    /// which publishes no gauge.
    /// [`set_partition_pending`](Self::set_partition_pending) and
    /// [`retain_partitions`](Self::retain_partitions) no-op in both cases. A
    /// caller reads this to skip gathering the counts at all.
    #[must_use]
    pub fn publishes_partition_detail(&self) -> bool {
        self.partition_pending.as_ref().is_some_and(|pg| pg.owned)
    }

    /// Set one partition's pending count. No-op unless
    /// `per_partition_detail`.
    pub fn set_partition_pending(&self, partition: PartitionId, pending: usize) {
        if let Some(pg) = &self.partition_pending {
            pg.set(partition, pending as f64);
        }
    }

    /// Drop per-partition series for revoked partitions.
    ///
    /// Two things differ from
    /// [`SourceMetrics::retain_partitions`](super::SourceMetrics::retain_partitions),
    /// and both follow from what this family counts.
    ///
    /// This takes the live set on every commit cycle rather than once a new
    /// assignment is known. Zeroing a partition the runtime is about to be
    /// handed back costs nothing, because the unlabeled series reads the same
    /// `0` from the same empty tracker map in the same cycle. The lag family
    /// has no aggregate to agree with, so it waits instead.
    ///
    /// This also zeroes a partition no other member will publish, where a lag
    /// gauge holds its last value. `0` here is the count of what the
    /// checkpointer tracks, and it tracks nothing for a partition it has
    /// dropped, so the reading stays true. A lag of `0` instead claims the
    /// partition is caught up, which is a claim about the world rather than
    /// about the handle, and false for a partition still being consumed
    /// somewhere.
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
