//! Shared building blocks for the per-stage handle structs: the standard
//! label set, its typed `Counter`/`Gauge`/`Histogram` constructors, and the
//! dynamic per-partition gauge family.
//!
//! The stage constructors (`counter`, `counter1`, ...) are `pub(crate)` so
//! each stage module (`source`, `sink`, `checkpoint`, ...) resolves its
//! handles through one code path that always attaches the three standard
//! labels. The dynamic-arity `register_*` path backing the public
//! [`Meter`](super::Meter) lives here too (also `pub(crate)`), so
//! connector- and user-owned metric families inherit the same labels.

use super::names;
use crate::error::ErrorClass;
use crate::record::PartitionId;
use metrics::{
    Counter, Gauge, Histogram, Key, Label, Level, Metadata, SharedString, counter, gauge,
    histogram, with_recorder,
};
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

    pub(crate) fn histogram2(
        &self,
        name: &'static str,
        k1: &'static str,
        v1: impl Into<SharedString>,
        k2: &'static str,
        v2: impl Into<SharedString>,
    ) -> Histogram {
        histogram!(name,
            names::L_PIPELINE => self.pipeline.clone(),
            names::L_COMPONENT => self.component.clone(),
            names::L_COMPONENT_TYPE => self.component_type.clone(),
            k1 => v1.into(),
            k2 => v2.into(),
        )
    }

    /// Build the metric key for a dynamic-arity family. The three standard
    /// labels come first (so every family joins cleanly against the
    /// framework's series), then the caller's `extra` labels in order.
    ///
    /// This mirrors what the `counter!`/`gauge!`/`histogram!` macros lower to
    /// for runtime label values (`Key::from_parts` over a `Vec<Label>`). The
    /// stage constructors above use the macro form; [`Meter`](super::Meter)
    /// needs a runtime-sized slice and a name assembled from its namespace at
    /// build time.
    fn family_key(&self, name: SharedString, extra: &[(&'static str, SharedString)]) -> Key {
        validate_extra_labels(&name, extra);
        let mut labels = Vec::with_capacity(3 + extra.len());
        labels.push(Label::new(names::L_PIPELINE, self.pipeline.clone()));
        labels.push(Label::new(names::L_COMPONENT, self.component.clone()));
        labels.push(Label::new(
            names::L_COMPONENT_TYPE,
            self.component_type.clone(),
        ));
        for (k, v) in extra {
            labels.push(Label::new(*k, v.clone()));
        }
        Key::from_parts(name, labels)
    }

    /// Resolve a counter carrying the three standard labels plus `extra`.
    /// `name` is the fully-qualified `spate_<namespace>_...` name the
    /// [`Meter`](super::Meter) assembled. Build-time (cold path) only.
    pub(crate) fn register_counter(
        &self,
        name: SharedString,
        extra: &[(&'static str, SharedString)],
    ) -> Counter {
        let key = self.family_key(name, extra);
        with_recorder(|recorder| recorder.register_counter(&key, &FAMILY_METADATA))
    }

    /// Resolve a gauge carrying the three standard labels plus `extra`.
    pub(crate) fn register_gauge(
        &self,
        name: SharedString,
        extra: &[(&'static str, SharedString)],
    ) -> Gauge {
        let key = self.family_key(name, extra);
        with_recorder(|recorder| recorder.register_gauge(&key, &FAMILY_METADATA))
    }

    /// Resolve a histogram carrying the three standard labels plus `extra`.
    pub(crate) fn register_histogram(
        &self,
        name: SharedString,
        extra: &[(&'static str, SharedString)],
    ) -> Histogram {
        let key = self.family_key(name, extra);
        with_recorder(|recorder| recorder.register_histogram(&key, &FAMILY_METADATA))
    }
}

/// A gauge that publishes only for the handle set that **owns** its series.
///
/// Registration still happens for a shadow. The key is the same, so it is the
/// same series and nothing extra renders, but every write is dropped, leaving
/// the owner's reading intact. Ownership is decided once, at construction (see
/// [`ownership`](super::ownership)); the check is a plain bool test, not a lock.
///
/// The handle is wrapped rather than each setter gated. A stage struct's
/// initial publishes run in its constructor, and those are the writes that
/// clobber a live owner.
#[derive(Clone, Debug)]
pub(crate) struct OwnedGauge {
    gauge: Gauge,
    owned: bool,
}

impl OwnedGauge {
    pub(crate) fn new(gauge: Gauge, owned: bool) -> Self {
        OwnedGauge { gauge, owned }
    }

    #[inline]
    pub(crate) fn set(&self, value: f64) {
        if self.owned {
            self.gauge.set(value);
        }
    }

    #[inline]
    pub(crate) fn increment(&self, value: f64) {
        if self.owned {
            self.gauge.increment(value);
        }
    }
}

/// Metadata attached to connector- and user-owned metric families. The
/// framework's own stage metrics register through the `metrics` macros, which
/// stamp `module_path!()` here; families registered through
/// [`Meter`](super::Meter) inherit this module's path. The exporters this
/// framework installs do not surface metadata.
const FAMILY_METADATA: Metadata<'static> =
    Metadata::new(module_path!(), Level::INFO, Some(module_path!()));

/// Guard the caller's `extra` labels at build time (cold path). None may
/// shadow a standard label key (those are attached automatically) or repeat
/// another `extra` key. Panics on a violation, at startup before any data
/// flows. The name's namespace is validated up front by
/// [`Meter`](super::Meter), so it needs no check here.
fn validate_extra_labels(name: &str, extra: &[(&'static str, SharedString)]) {
    for (i, (k, _)) in extra.iter().enumerate() {
        assert!(
            *k != names::L_PIPELINE && *k != names::L_COMPONENT && *k != names::L_COMPONENT_TYPE,
            "custom label `{k}` on `{name}` shadows a standard label \
             (pipeline/component/component_type are attached automatically)"
        );
        assert!(
            !extra[..i].iter().any(|(prev, _)| prev == k),
            "custom label `{k}` is repeated on `{name}`"
        );
    }
}

/// Why a `Meter` namespace token was rejected. The panicking constructor path
/// ([`validate_namespace`]) and the non-panicking runtime path
/// (`Meter::for_component`) share one rule set and phrase the outcome
/// differently. An explicit author call gets a hard error; a component default
/// gets a silent opt-out or a warning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NamespaceRejection {
    /// Empty string.
    Empty,
    /// Not a lowercase `[a-z][a-z0-9_]*` segment, and so not a legal
    /// metric-name segment. For example it contains an uppercase letter or a
    /// hyphen, or leads with a digit.
    Malformed,
    /// A framework stage root (`source`, `sink`, …); a custom family here would
    /// collide with the taxonomy.
    Reserved,
}

impl NamespaceRejection {
    /// A short reason phrase for a diagnostic (`… because {reason}`).
    pub(crate) fn reason(self) -> &'static str {
        match self {
            NamespaceRejection::Empty => "it is empty",
            NamespaceRejection::Malformed => "it is not a lowercase `[a-z][a-z0-9_]*` segment",
            NamespaceRejection::Reserved => "it is a reserved framework stage root",
        }
    }
}

/// Classify a `Meter` namespace token (the `<ns>` in the `spate_<ns>_` prefix
/// every one of its metrics gets). Returns `Ok(())` if it is a usable,
/// non-reserved segment, else why it was rejected. Both the panicking
/// [`validate_namespace`] and the non-panicking `Meter::for_component` resolve
/// through this function; rejecting a reserved root keeps custom names from
/// colliding with the framework taxonomy.
pub(crate) fn classify_namespace(namespace: &str) -> Result<(), NamespaceRejection> {
    if namespace.is_empty() {
        return Err(NamespaceRejection::Empty);
    }
    let well_formed = namespace
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && namespace.as_bytes()[0].is_ascii_lowercase();
    if !well_formed {
        return Err(NamespaceRejection::Malformed);
    }
    if names::RESERVED_ROOTS.contains(&namespace) {
        return Err(NamespaceRejection::Reserved);
    }
    Ok(())
}

/// Validate a `Meter` namespace token, panicking with a specific message on
/// rejection. The construction-time wiring check behind
/// [`Meter::with_namespace`](super::Meter::with_namespace).
pub(crate) fn validate_namespace(namespace: &str) {
    match classify_namespace(namespace) {
        Ok(()) => {}
        Err(NamespaceRejection::Empty) => panic!(
            "Meter namespace must not be empty (it becomes the `spate_<namespace>_` \
             segment on every metric); use `\"custom\"` or your connector's name"
        ),
        Err(NamespaceRejection::Malformed) => panic!(
            "Meter namespace `{namespace}` must be a lowercase `[a-z][a-z0-9_]*` \
             segment (it becomes part of the `spate_<namespace>_` metric prefix)"
        ),
        Err(NamespaceRejection::Reserved) => panic!(
            "Meter namespace `{namespace}` is a reserved framework root; custom \
             families would collide with `spate_{namespace}_*`. Use `\"custom\"` or \
             a connector segment like `\"kafka\"`."
        ),
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
/// Registration happens on the control plane (rebalance/commit paths); the hot
/// path never touches this.
#[derive(Debug)]
pub(crate) struct PartitionGauges {
    pub(crate) name: &'static str,
    pub(crate) labels: ComponentLabels,
    pub(crate) gauges: Mutex<HashMap<u32, Gauge>>,
    /// Whether the owning handle set owns this series (see [`OwnedGauge`]).
    /// A shadow registers nothing here and publishes nothing.
    pub(crate) owned: bool,
}

impl PartitionGauges {
    pub(crate) fn set(&self, partition: PartitionId, value: f64) {
        if !self.owned {
            return;
        }
        let mut gauges = self.gauges.lock().expect("partition gauge lock");
        gauges
            .entry(partition.0)
            .or_insert_with(|| {
                self.labels
                    .gauge1(self.name, names::L_PARTITION, partition.0.to_string())
            })
            .set(value);
    }

    /// Zeroes and then drops the handles for partitions this component no
    /// longer owns.
    ///
    /// The `metrics` facade has no deletion and no idle timeout is configured
    /// (see `configured_builder`), so dropping a handle is invisible to the
    /// exporter. The series keeps rendering its last value for the life of the
    /// process. Without the zeroing, a reader that aggregates across members
    /// counts a partition twice, once frozen here and once live on the member
    /// that now owns it.
    ///
    /// Absence and `0` therefore mean different things for a per-partition
    /// series. Absent is "never measured"; `0` here is "measured, not ours".
    pub(crate) fn retain(&self, keep: &[PartitionId]) {
        // Symmetric with `set`. A shadow's map is empty today, so this only
        // guards against a future path that populates one, where zeroing here
        // would write over the owner's series.
        if !self.owned {
            return;
        }
        let mut gauges = self.gauges.lock().expect("partition gauge lock");
        gauges.retain(|p, gauge| {
            let kept = keep.iter().any(|k| k.0 == *p);
            if !kept {
                gauge.set(0.0);
            }
            kept
        });
    }
}
