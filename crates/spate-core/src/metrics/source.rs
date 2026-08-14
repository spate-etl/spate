//! Source-stage handles (`spate_source_*`).
//!
//! Every handle the record loop touches is resolved once at build time, and
//! its methods take per-batch aggregates, as [the metrics reference]
//! specifies. The per-partition lag series is resolved lazily instead, on the
//! control plane, so a partition whose lag has never been measured is absent
//! rather than a `0` that reads as "caught up".
//!
//! [the metrics reference]: https://spate.kainth.dev/docs/METRICS

use super::MetricsError;
use super::labels::{ComponentLabels, OwnedGauge, PartitionGauges};
use super::names;
use super::ownership::{SeriesClaim, series_key};
use crate::record::PartitionId;
use metrics::{Counter, Histogram};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// Source-stage handles (`spate_source_*`).
#[derive(Debug)]
pub struct SourceMetrics {
    records: Counter,
    bytes: Counter,
    poll_duration: Histogram,
    rebalance_assign: Counter,
    rebalance_revoke: Counter,
    lanes_active: OwnedGauge,
    partition_lag: PartitionGauges,
    _claim: Option<SeriesClaim>,
}

impl SourceMetrics {
    /// Resolve all source handles, claiming the `spate_source_*` series for
    /// these labels.
    ///
    /// A pipeline builds several of these on identical labels (one per
    /// pipeline thread plus the controller's) because every thread counts
    /// records it polled. Only *one* of them may publish the source gauges,
    /// and it must be the controller's. The controller holds the assignment
    /// and hands its clone to the source, which is the only thing that can
    /// measure lag. The per-thread instances are therefore built with
    /// [`shadow`](Self::shadow), not this constructor. A collision here logs
    /// and shadows rather than panicking.
    ///
    /// Consumer lag is not gated by `per_partition_detail`. The per-partition
    /// series is the *only* representation of a golden signal, so a
    /// cardinality knob must not be able to delete it. The lag handles are
    /// also not resolved here; `PartitionGauges` registers a partition's
    /// series on its first known value, so a partition whose lag has never
    /// been measured is absent rather than reporting a `0` that reads as
    /// "caught up".
    pub fn new(labels: &ComponentLabels) -> Self {
        Self::build(labels, SeriesClaim::claim_or_shadow(Self::key(labels)))
    }

    /// Resolve all source handles, failing when another live handle set
    /// already owns the series. The pipeline runtime's path for the
    /// controller's instance (the one that owns lag and lanes).
    ///
    /// # Errors
    ///
    /// [`MetricsError::DuplicateSeries`] on a collision.
    pub fn try_new(labels: &ComponentLabels) -> Result<Self, MetricsError> {
        let claim = SeriesClaim::try_claim(Self::key(labels))?;
        Ok(Self::build(labels, Some(claim)))
    }

    /// Resolve source handles that **do not** own their series. Counters and
    /// the poll histogram record as normal (they aggregate across instances);
    /// gauge writes are dropped.
    ///
    /// A pipeline thread counts its own polls this way without competing for
    /// `spate_source_lag_records` and `spate_source_lanes_active`,
    /// which only the controller can populate correctly. Use it when a second
    /// instance on the same labels is intended; anything else should use
    /// [`new`](Self::new) or [`try_new`](Self::try_new) and hear about the
    /// collision.
    #[must_use]
    pub fn shadow(labels: &ComponentLabels) -> Self {
        Self::build(labels, None)
    }

    fn key(labels: &ComponentLabels) -> String {
        series_key("source", labels, "")
    }

    fn build(labels: &ComponentLabels, claim: Option<SeriesClaim>) -> Self {
        let owned = claim.is_some();
        SourceMetrics {
            records: labels.counter(names::SOURCE_RECORDS_TOTAL),
            bytes: labels.counter(names::SOURCE_BYTES_TOTAL),
            poll_duration: labels.histogram(names::SOURCE_POLL_DURATION_SECONDS),
            rebalance_assign: labels.counter1(
                names::SOURCE_REBALANCES_TOTAL,
                names::L_EVENT,
                "assign",
            ),
            rebalance_revoke: labels.counter1(
                names::SOURCE_REBALANCES_TOTAL,
                names::L_EVENT,
                "revoke",
            ),
            lanes_active: OwnedGauge::new(labels.gauge(names::SOURCE_LANES_ACTIVE), owned),
            partition_lag: PartitionGauges {
                name: names::SOURCE_LAG_RECORDS,
                labels: labels.clone(),
                gauges: Mutex::new(HashMap::new()),
                owned,
            },
            _claim: claim,
        }
    }

    /// Record one polled batch.
    #[inline]
    pub fn batch(&self, records: u64, bytes: u64) {
        self.records.increment(records);
        self.bytes.increment(bytes);
    }

    /// Observe one `poll` call's duration.
    #[inline]
    pub fn poll_duration(&self, d: Duration) {
        self.poll_duration.record(d.as_secs_f64());
    }

    /// Publish one partition's consumer lag.
    ///
    /// Only call this with a lag the client measured. The series is registered
    /// on the first such call, so never publishing is how "lag unknown" is
    /// expressed. A `0` would be indistinguishable from a consumer that has
    /// caught up.
    pub fn set_partition_lag(&self, partition: PartitionId, lag: u64) {
        self.partition_lag.set(partition, lag as f64);
    }

    /// Zero and drop the lag series for partitions this member no longer
    /// owns.
    ///
    /// The exporter has no deletion, so a partition that moved to another
    /// member would keep rendering this member's last lag forever and every
    /// reader that sums across partitions would count it twice. Zeroing first
    /// makes the sum correct. The member that now owns the partition publishes
    /// the real figure, and this one contributes the `0` it holds.
    ///
    /// Call this once the *new* assignment is known, not while partitions are
    /// still draining. A member that is about to be handed a partition back
    /// should never publish a zero for it.
    pub fn retain_partitions(&self, keep: &[PartitionId]) {
        self.partition_lag.retain(keep);
    }

    /// Count a rebalance assignment event.
    pub fn rebalance_assigned(&self) {
        self.rebalance_assign.increment(1);
    }

    /// Count a rebalance revocation event.
    pub fn rebalance_revoked(&self) {
        self.rebalance_revoke.increment(1);
    }

    /// Set the number of currently assigned lanes.
    pub fn set_lanes_active(&self, lanes: usize) {
        self.lanes_active.set(lanes as f64);
    }
}
