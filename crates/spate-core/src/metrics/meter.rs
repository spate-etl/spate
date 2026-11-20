//! [`Meter`], the public instrumentation scope for connector- and
//! user-owned metric families.
//!
//! A `Meter` is bound to one component's three standard labels
//! (`pipeline`, `component`, `component_type`) and a **namespace**. It mints
//! `Counter`/`Gauge`/`Histogram` handles that carry those labels
//! automatically and whose names are auto-prefixed `spate_<namespace>_`, so a
//! connector's or pipeline author's own series sit under the same `spate_`
//! umbrella as the framework's and join cleanly in a query.
//!
//! You pass the metric's **local name** (`"schema_fetches_total"`, not
//! `"spate_kafka_schema_fetches_total"`); the `Meter` adds the umbrella and
//! namespace for you. Operators get one greppable `spate_` root, and there is
//! no way to typo the prefix or collide with a framework metric.
//!
//! The framework's own stages don't use `Meter`; they resolve fixed handle
//! structs ([`SourceMetrics`](super::SourceMetrics) and friends) directly.
//! `Meter` is the seam for metrics the framework can't measure from the
//! outside, such as a connector statistic or a pipeline author's business
//! counter.
//!
//! # Namespaces
//!
//! - [`Meter::new`] uses the `custom` namespace → `spate_custom_*`. This is the
//!   bucket for pipeline-author metrics.
//! - [`Meter::with_namespace`] takes a segment a connector owns (`"kafka"` →
//!   `spate_kafka_*`). The runtime keeps a connector's roles apart when it
//!   injects a component's `Meter`, under `spate_kafka_source_*`,
//!   `spate_kafka_sink_*` and `spate_kafka_feed_*`, and a chain hands its
//!   deserializer one under `spate_<component_type>_deser_*`. The role is
//!   derived from the wiring position, not set by hand.
//!
//! The namespace is validated once, at construction. It must be a lowercase
//! `[a-z][a-z0-9_]*` segment and must not be one of the framework's reserved
//! stage roots (`source`, `sink`, …).
//!
//! # Where a `Meter` comes from
//!
//! - **Pipeline authors** get one from the chain factory's
//!   [`ChainCtx::meter`](crate::pipeline::ChainCtx::meter) (the `custom`
//!   namespace) and close the resolved handle over an operator closure
//!   (`.inspect` / `.map`).
//! - **A source, sink or feed backend** receives one from the runtime, scoped
//!   by its role. **A deserializer** receives one from the chain's
//!   [`with_metrics`](crate::ops::ChainBuilder::with_metrics).
//! - **Standalone** (a tool, a test, an example) constructs one with
//!   [`Meter::new`] / [`Meter::with_namespace`].
//!
//! # Hot-path discipline
//!
//! Resolve every handle **once, at build time**, and touch only the resolved
//! `Counter`/`Gauge`/`Histogram` on the per-record path. The framework's own
//! handles follow the same rule, and [the metrics reference] states it for
//! the taxonomy as a whole. The `Meter::counter` / `gauge` / `histogram`
//! calls belong in construction, not the record loop.
//!
//! [the metrics reference]: https://spate.kainth.dev/docs/METRICS

use super::labels::{ComponentLabels, NamespaceRejection, classify_namespace, validate_namespace};
use metrics::{Counter, Gauge, Histogram, SharedString};

// Declares each role with its segment once, and builds `MetricRole::SEGMENTS`
// from the same entries.
macro_rules! metric_roles {
    ($(#[$enum_doc:meta])* $( $role:ident => $segment:literal, )+) => {
        $(#[$enum_doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) enum MetricRole {
            $( $role, )+
        }

        impl MetricRole {
            /// Every role's segment, in declaration order.
            pub(crate) const SEGMENTS: &'static [&'static str] = &[$($segment),+];

            fn segment(self) -> &'static str {
                match self {
                    $( MetricRole::$role => $segment, )+
                }
            }
        }
    };
}

metric_roles! {
    /// The part a component plays in the pipeline, appended to its namespace so
    /// a connector that plays several roles keeps its families apart
    /// (`spate_kafka_source_*`, `spate_kafka_sink_*`, `spate_kafka_feed_*`).
    /// The framework derives it from the wiring position when it builds a
    /// component's [`Meter`](Meter::for_component), so connector code never
    /// names a role.
    Source => "source",
    Sink => "sink",
    Feed => "feed",
    Deser => "deser",
}

/// An instrumentation scope: one component's three standard labels plus an
/// `spate_<namespace>_` name prefix applied to every metric it mints.
///
/// Cheap to clone; the handles it mints are the `metrics` facade's own
/// `Arc`-backed handles, safe to clone across pipeline threads. Every clone
/// feeds the same series.
///
/// ```
/// use spate_core::metrics::Meter;
///
/// // Build-time: resolve the handle once. Pass the LOCAL name — the Meter
/// // prepends `spate_custom_`, so this registers `spate_custom_orders_enriched_total`.
/// let meter = Meter::new("orders", "enrich", "map");
/// let enriched = meter.counter("orders_enriched_total", &[("region", "eu".into())]);
///
/// // Hot path: touch only the handle, count per batch.
/// enriched.increment(512);
/// ```
#[derive(Clone, Debug)]
pub struct Meter {
    labels: ComponentLabels,
    /// The full `spate_<namespace>_` prefix prepended to every metric name.
    prefix: String,
}

impl Meter {
    /// A scope for **pipeline-author custom metrics**. Names land under the
    /// `spate_custom_` bucket. `component` is the instance id (e.g. `enrich`);
    /// `component_type` the implementation label. Reuse the `pipeline` /
    /// `component` values the framework was given so your series join against
    /// its.
    pub fn new(
        pipeline: impl Into<SharedString>,
        component: impl Into<SharedString>,
        component_type: impl Into<SharedString>,
    ) -> Self {
        Self::with_namespace(
            super::names::CUSTOM_NAMESPACE,
            pipeline,
            component,
            component_type,
        )
    }

    /// A scope under a specific `spate_<namespace>_` bucket, for a connector
    /// that owns a segment (e.g. `"kafka"` → `spate_kafka_*`).
    ///
    /// # Panics
    ///
    /// Panics if `namespace` is empty, is not a lowercase `[a-z][a-z0-9_]*`
    /// segment, or is one of the framework's reserved stage roots (`source`,
    /// `sink`, …). This is a construction-time wiring check.
    pub fn with_namespace(
        namespace: &str,
        pipeline: impl Into<SharedString>,
        component: impl Into<SharedString>,
        component_type: impl Into<SharedString>,
    ) -> Self {
        validate_namespace(namespace);
        Meter {
            labels: ComponentLabels::new(pipeline, component, component_type),
            prefix: format!("{}{namespace}_", super::names::PREFIX),
        }
    }

    /// The scope the framework hands a component (a `Source`, a sink
    /// `ShardWriter`, a `Feed` backend or a chain's `Deserializer`). The
    /// namespace is the component's `component_type`, which for a feed backend
    /// is its `Feed::backend` key, and
    /// `role` (derived from the wiring position) is appended, giving
    /// `spate_<component_type>_<role>_*`. The `component_type` is also the
    /// standard `component_type` label, so a family joins the component's stage
    /// metrics.
    ///
    /// Returns `None` (rather than panicking, unlike [`Meter::with_namespace`])
    /// when `component_type` cannot scope a family:
    ///
    /// - a **reserved root** (a source's default `"source"`, a sink set to
    ///   `"sink"`) or the **`custom`** author bucket (a sink's default
    ///   `component_type`). A silent opt-out, so an undeclared component gets
    ///   no custom `Meter`. `custom` is reserved for pipeline-author
    ///   metrics ([`Meter::new`]); a component keeps its families out of that
    ///   shared bucket by declaring a distinct `component_type`.
    /// - a deserializer's default `"deserializer"`
    ///   ([`DESERIALIZER_DEFAULT`](super::names::DESERIALIZER_DEFAULT)), under
    ///   the deserializer role only. A silent opt-out, so an undeclared
    ///   deserializer gets no `Meter`. The same string in another role scopes a
    ///   family as usual.
    /// - a **malformed** string (not a `[a-z][a-z0-9_]*` segment). The same
    ///   `None`, but logged at `warn`. A `component_type` that is a legal label
    ///   yet an illegal metric-name segment (e.g. `"clickhouse-v2"`) is almost
    ///   always a wiring mistake that would otherwise drop the component's
    ///   families silently while its framework stage metrics keep emitting.
    pub(crate) fn for_component(
        component_type: &str,
        role: MetricRole,
        pipeline: impl Into<SharedString>,
        component: impl Into<SharedString>,
    ) -> Option<Meter> {
        // The `custom` author bucket is off-limits to component scoping, so a
        // component's families never share the `spate_custom_` space with a
        // pipeline author's. A silent opt-out, like a reserved root.
        if component_type == super::names::CUSTOM_NAMESPACE {
            return None;
        }
        // An undeclared deserializer's default type, like a source's reserved
        // `"source"`, is no declaration of a family.
        if role == MetricRole::Deser && component_type == super::names::DESERIALIZER_DEFAULT {
            return None;
        }
        match classify_namespace(component_type) {
            Ok(()) => Some(Meter {
                labels: ComponentLabels::new(pipeline, component, component_type.to_string()),
                prefix: format!(
                    "{}{component_type}_{}_",
                    super::names::PREFIX,
                    role.segment()
                ),
            }),
            // A reserved root is a legitimate default (a source's `"source"`).
            // No `Meter`, and the component's stage metrics stay intact.
            Err(NamespaceRejection::Reserved) => None,
            // Empty/malformed is a wiring mistake. Surface it rather than drop
            // the component's families silently.
            Err(reason) => {
                tracing::warn!(
                    component_type,
                    role = role.segment(),
                    reason = reason.reason(),
                    "component `component_type` is unusable as a metric namespace, \
                     so this component's custom Meter families are disabled (its \
                     framework stage metrics are unaffected); use a lowercase \
                     [a-z][a-z0-9_]* segment to enable them"
                );
                None
            }
        }
    }

    /// Fully-qualify a local metric name under this scope's `spate_<namespace>_`
    /// prefix. Panics if the caller already included an `spate_` prefix. Pass
    /// the local name only (`"schema_fetches_total"`, not
    /// `"spate_kafka_schema_fetches_total"`).
    fn qualify(&self, name: &str) -> SharedString {
        assert!(
            !name.starts_with(super::names::PREFIX),
            "pass the metric's local name without the `{}` prefix — the Meter \
             adds `{}` for you (got `{name}`)",
            super::names::PREFIX,
            self.prefix
        );
        // A local name may not lead with a role segment. The runtime injects
        // one between the namespace and the name (`spate_<ns>_<role>_*`), so a
        // hand-written name leading with one could alias a role-scoped family.
        let first = name.split('_').next().unwrap_or_default();
        assert!(
            !super::names::ROLE_SEGMENTS.contains(&first),
            "metric local name `{name}` must not begin with the role segment \
             `{first}_`; the runtime reserves {:?} to scope a component's families \
             by role",
            super::names::ROLE_SEGMENTS
        );
        format!("{}{name}", self.prefix).into()
    }

    /// Resolve a counter under this scope. `name` is the **local** name
    /// (auto-prefixed `spate_<namespace>_`); it carries the three standard
    /// labels, then `extra`. Pass `&[]` for the standard labels alone.
    /// Build-time only.
    ///
    /// Follow the taxonomy rules: `_total` suffix on counters, and push
    /// per-instance identity (a topic, a shard) into `extra` labels rather
    /// than the name, keeping the name low-cardinality.
    ///
    /// # Panics
    ///
    /// Panics if `name` starts with `spate_`, or if its first `_`-separated
    /// segment is a role segment in
    /// [`ROLE_SEGMENTS`](super::names::ROLE_SEGMENTS) (`source`, `sink`,
    /// `feed`, `deser`), in every namespace. Panics if `extra` shadows a standard label
    /// or repeats a key.
    pub fn counter(&self, name: &str, extra: &[(&'static str, SharedString)]) -> Counter {
        self.labels.register_counter(self.qualify(name), extra)
    }

    /// Resolve a gauge under this scope (local name, auto-prefixed).
    /// Build-time only.
    ///
    /// # Panics
    ///
    /// Panics if `name` starts with `spate_`, or if its first `_`-separated
    /// segment is a role segment in
    /// [`ROLE_SEGMENTS`](super::names::ROLE_SEGMENTS) (`source`, `sink`,
    /// `feed`, `deser`), in every namespace. Panics if `extra` shadows a standard label
    /// or repeats a key.
    pub fn gauge(&self, name: &str, extra: &[(&'static str, SharedString)]) -> Gauge {
        self.labels.register_gauge(self.qualify(name), extra)
    }

    /// Resolve a histogram under this scope (local name, auto-prefixed).
    /// Build-time only. Give it a unit suffix (`_seconds` / `_bytes` /
    /// `_rows`).
    ///
    /// # Panics
    ///
    /// Panics if `name` starts with `spate_`, or if its first `_`-separated
    /// segment is a role segment in
    /// [`ROLE_SEGMENTS`](super::names::ROLE_SEGMENTS) (`source`, `sink`,
    /// `feed`, `deser`), in every namespace. Panics if `extra` shadows a standard label
    /// or repeats a key.
    pub fn histogram(&self, name: &str, extra: &[(&'static str, SharedString)]) -> Histogram {
        self.labels.register_histogram(self.qualify(name), extra)
    }

    /// The underlying standard-label set, for building a framework stage
    /// handle from the same scope (e.g.
    /// [`DeserMetrics::new`](super::DeserMetrics::new)).
    #[must_use]
    pub fn labels(&self) -> &ComponentLabels {
        &self.labels
    }
}
