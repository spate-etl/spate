//! Source-stage handles (`etl_source_*`).
//!
//! Resolved once at build time; the record loop only touches the resolved
//! handles and methods take per-batch aggregates (see `docs/METRICS.md`).

use super::labels::{ComponentLabels, PartitionGauges};
use super::names;
use crate::record::PartitionId;
use metrics::{Counter, Gauge, Histogram};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// Source-stage handles (`etl_source_*`).
#[derive(Debug)]
pub struct SourceMetrics {
    records: Counter,
    bytes: Counter,
    poll_duration: Histogram,
    lag_max: Gauge,
    rebalance_assign: Counter,
    rebalance_revoke: Counter,
    lanes_active: Gauge,
    partition_lag: Option<PartitionGauges>,
}

impl SourceMetrics {
    /// Resolve all source handles. `per_partition_detail` gates the
    /// cardinality-sensitive per-partition lag series.
    pub fn new(labels: &ComponentLabels, per_partition_detail: bool) -> Self {
        SourceMetrics {
            records: labels.counter(names::SOURCE_RECORDS_TOTAL),
            bytes: labels.counter(names::SOURCE_BYTES_TOTAL),
            poll_duration: labels.histogram(names::SOURCE_POLL_DURATION_SECONDS),
            lag_max: labels.gauge(names::SOURCE_LAG_RECORDS),
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
            lanes_active: labels.gauge(names::SOURCE_LANES_ACTIVE),
            partition_lag: per_partition_detail.then(|| PartitionGauges {
                name: names::SOURCE_LAG_RECORDS,
                labels: labels.clone(),
                gauges: Mutex::new(HashMap::new()),
            }),
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

    /// Set the max consumer lag across partitions.
    pub fn set_lag_max(&self, lag: u64) {
        self.lag_max.set(lag as f64);
    }

    /// Set one partition's lag. No-op unless `per_partition_detail`.
    pub fn set_partition_lag(&self, partition: PartitionId, lag: u64) {
        if let Some(pg) = &self.partition_lag {
            pg.set(partition, lag as f64);
        }
    }

    /// Drop per-partition series for revoked partitions.
    pub fn retain_partitions(&self, keep: &[PartitionId]) {
        if let Some(pg) = &self.partition_lag {
            pg.retain(keep);
        }
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
