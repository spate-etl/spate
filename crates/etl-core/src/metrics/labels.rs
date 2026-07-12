//! Shared building blocks for the per-stage handle structs: the standard
//! label set, its typed `Counter`/`Gauge`/`Histogram` constructors, and the
//! dynamic per-partition gauge family.
//!
//! The constructors are `pub(crate)` so each stage module (`source`, `sink`,
//! `checkpoint`, ...) resolves its handles through one code path that always
//! attaches the three standard labels.

use super::names;
use crate::error::ErrorClass;
use crate::record::PartitionId;
use metrics::{Counter, Gauge, Histogram, SharedString, counter, gauge, histogram};
use std::collections::HashMap;
use std::sync::Mutex;

/// The standard label set attached to every framework metric.
#[derive(Clone, Debug)]
pub struct ComponentLabels {
    /// Pipeline name.
    pub pipeline: SharedString,
    /// Component instance id from config/builder (e.g. `orders_kafka`).
    pub component: SharedString,
    /// Component implementation (e.g. `kafka`, `clickhouse`, `map`).
    pub component_type: SharedString,
}

impl ComponentLabels {
    /// Build the standard label set.
    pub fn new(
        pipeline: impl Into<SharedString>,
        component: impl Into<SharedString>,
        component_type: impl Into<SharedString>,
    ) -> Self {
        ComponentLabels {
            pipeline: pipeline.into(),
            component: component.into(),
            component_type: component_type.into(),
        }
    }

    pub(crate) fn counter(&self, name: &'static str) -> Counter {
        counter!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
        )
    }

    pub(crate) fn counter1(
        &self,
        name: &'static str,
        k: &'static str,
        v: impl Into<SharedString>,
    ) -> Counter {
        counter!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k => v.into(),
        )
    }

    pub(crate) fn counter2(
        &self,
        name: &'static str,
        k1: &'static str,
        v1: impl Into<SharedString>,
        k2: &'static str,
        v2: impl Into<SharedString>,
    ) -> Counter {
        counter!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k1 => v1.into(),
            k2 => v2.into(),
        )
    }

    pub(crate) fn gauge(&self, name: &'static str) -> Gauge {
        gauge!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
        )
    }

    pub(crate) fn gauge1(
        &self,
        name: &'static str,
        k: &'static str,
        v: impl Into<SharedString>,
    ) -> Gauge {
        gauge!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k => v.into(),
        )
    }

    pub(crate) fn gauge2(
        &self,
        name: &'static str,
        k1: &'static str,
        v1: impl Into<SharedString>,
        k2: &'static str,
        v2: impl Into<SharedString>,
    ) -> Gauge {
        gauge!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k1 => v1.into(),
            k2 => v2.into(),
        )
    }

    pub(crate) fn histogram(&self, name: &'static str) -> Histogram {
        histogram!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
        )
    }

    pub(crate) fn histogram1(
        &self,
        name: &'static str,
        k: &'static str,
        v: impl Into<SharedString>,
    ) -> Histogram {
        histogram!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k => v.into(),
        )
    }
}

impl ErrorClass {
    pub(crate) fn label(self) -> &'static str {
        match self {
            ErrorClass::Retryable => "retryable",
            ErrorClass::RecordLevel => "record_level",
            ErrorClass::Fatal => "fatal",
        }
    }
}

/// Dynamic per-partition gauge family, gated by `per_partition_detail`.
/// Registration happens on the control plane (rebalance/commit paths), so a
/// mutex is acceptable; the hot path never touches this.
#[derive(Debug)]
pub(crate) struct PartitionGauges {
    pub(crate) name: &'static str,
    pub(crate) labels: ComponentLabels,
    pub(crate) gauges: Mutex<HashMap<u32, Gauge>>,
}

impl PartitionGauges {
    pub(crate) fn set(&self, partition: PartitionId, value: f64) {
        let mut gauges = self.gauges.lock().expect("partition gauge lock");
        gauges
            .entry(partition.0)
            .or_insert_with(|| {
                self.labels
                    .gauge1(self.name, names::L_PARTITION, partition.0.to_string())
            })
            .set(value);
    }

    /// Drops handles for revoked partitions so they are no longer updated
    /// and don't accumulate across rebalances. The exporter may keep
    /// rendering the last value of a dropped series until its own idle
    /// timeout; that staleness is harmless and expected.
    pub(crate) fn retain(&self, keep: &[PartitionId]) {
        let mut gauges = self.gauges.lock().expect("partition gauge lock");
        gauges.retain(|p, _| keep.iter().any(|k| k.0 == *p));
    }
}
