//! Source-stage handles (`etl_source_*`).
//!
//! Every handle the record loop touches is resolved once at build time, and
//! its methods take per-batch aggregates (see `docs/METRICS.md`). The
//! per-partition lag series is the exception: it is resolved lazily, on the
//! control plane, so that a partition whose lag has never been measured is
//! absent rather than a `0` that reads as "caught up".

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
    rebalance_assign: Counter,
    rebalance_revoke: Counter,
    lanes_active: Gauge,
    partition_lag: PartitionGauges,
}

impl SourceMetrics {
    /// Resolve all source handles.
    ///
    /// Consumer lag is deliberately not gated by `per_partition_detail`: the
    /// per-partition series is the *only* representation of a golden signal,
    /// so a cardinality knob must not be able to delete it. The lag handles
    /// are also not resolved here — `PartitionGauges` registers a partition's
    /// series on its first known value, so a partition whose lag has never
    /// been measured is absent rather than reporting a `0` that reads as
    /// "caught up".
    pub fn new(labels: &ComponentLabels) -> Self {
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
            lanes_active: labels.gauge(names::SOURCE_LANES_ACTIVE),
            partition_lag: PartitionGauges {
                name: names::SOURCE_LAG_RECORDS,
                labels: labels.clone(),
                gauges: Mutex::new(HashMap::new()),
            },
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
    /// Only call this with a lag the client actually measured. The series is
    /// registered on the first such call, so never publishing is how "lag
    /// unknown" is expressed — a `0` would be indistinguishable from a
    /// consumer that has caught up.
    pub fn set_partition_lag(&self, partition: PartitionId, lag: u64) {
        self.partition_lag.set(partition, lag as f64);
    }

    /// Zero and drop the lag series for partitions this member no longer
    /// owns.
    ///
    /// Load-bearing, but not by deleting anything: the exporter has no
    /// deletion, so a partition that moved to another member would keep
    /// rendering this member's last lag forever and every reader that sums
    /// across partitions would count it twice. Zeroing first makes the sum
    /// correct — the member that now owns the partition publishes the real
    /// figure, and this one contributes the `0` it truthfully has.
    ///
    /// Call this once the *new* assignment is known, not while partitions are
    /// still draining: a member that is about to be handed a partition back
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
