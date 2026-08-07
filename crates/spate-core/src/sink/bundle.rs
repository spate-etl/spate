//! A ready-to-run sink, decomposed: what a connector's config factory
//! hands the pipeline builder.
//!
//! Connector crates implement [`SinkBundle`] on their config-built sink
//! type (e.g. a ClickHouse sink, a capturing test sink); hand-rolled sinks
//! construct [`SinkParts`] directly — it implements the trait itself, so
//! `Pipeline::sink` accepts either. The only bound is [`ShardWriter`]:
//! connector-specific types appear solely as the implementor's associated
//! types, keeping 0.x dependencies out of this crate's public bounds
//! (INV-6; ADR-0011).

use super::{ShardWriter, SinkPoolConfig, SinkProbeFn};

/// A sink ready for assembly: the writer, the shard/replica topology, pool
/// tuning, and optional metadata (metric labels, readiness probe).
///
/// Consumed once by the pipeline builder, which turns it into shard
/// queues, per-shard metrics, and a spawned
/// [`SinkPool`](crate::sink::SinkPool).
pub trait SinkBundle {
    /// The connector's [`ShardWriter`] implementation.
    type Writer: ShardWriter;

    /// Decompose into the parts the builder wires up. Consuming: the
    /// endpoints move into the sink pool.
    fn into_parts(self) -> SinkParts<Self::Writer>;
}

/// The decomposed sink. Construct with [`SinkParts::new`] and refine with
/// the `with_*` methods (the struct is `#[non_exhaustive]`; fields may be
/// added without breaking implementors).
///
/// `shard_endpoints` is indexed `[shard][replica]` and must be non-empty
/// with every shard holding at least one replica — the builder rejects
/// ragged or empty topologies before anything spawns.
#[non_exhaustive]
pub struct SinkParts<W: ShardWriter> {
    /// The connector's writer, shared by every shard worker.
    pub writer: W,
    /// Per-shard replica endpoints, `[shard][replica]`.
    pub shard_endpoints: Vec<Vec<W::Endpoint>>,
    /// Pool tuning (batching, inflight, retry, breaker).
    pub pool: SinkPoolConfig,
    /// The `component_type` metric label (e.g. `"clickhouse"`).
    pub component_type: String,
    /// Per-replica display labels for shard metrics, same shape as
    /// `shard_endpoints`. Defaults to `"{component_type}-{shard}-{replica}"`.
    pub replica_labels: Vec<Vec<String>>,
    /// Optional readiness probe. Probes should use their own client set —
    /// sharing the writer's connections would report the insert path
    /// healthy simply because probing keeps it warm.
    pub probe: Option<SinkProbeFn>,
}

impl<W: ShardWriter> SinkParts<W> {
    /// Minimal parts: `component_type` defaults to `"custom"`, replica
    /// labels to `"{component_type}-{shard}-{replica}"`, no probe.
    pub fn new(writer: W, shard_endpoints: Vec<Vec<W::Endpoint>>, pool: SinkPoolConfig) -> Self {
        SinkParts {
            writer,
            shard_endpoints,
            pool,
            component_type: "custom".to_string(),
            replica_labels: Vec::new(),
            probe: None,
        }
    }

    /// Set the `component_type` metric label.
    #[must_use]
    pub fn with_component_type(mut self, component_type: impl Into<String>) -> Self {
        self.component_type = component_type.into();
        self
    }

    /// Set per-replica display labels (same `[shard][replica]` shape as
    /// the endpoints).
    #[must_use]
    pub fn with_replica_labels(mut self, labels: Vec<Vec<String>>) -> Self {
        self.replica_labels = labels;
        self
    }

    /// Attach a readiness probe.
    #[must_use]
    pub fn with_probe(mut self, probe: SinkProbeFn) -> Self {
        self.probe = Some(probe);
        self
    }

    /// The replica labels to use: the configured ones, or the
    /// `"{component_type}-{shard}-{replica}"` defaults. Manual assemblies
    /// can feed these to
    /// [`SinkShardMetrics::new`](crate::metrics::SinkShardMetrics::new).
    pub fn effective_replica_labels(&self) -> Vec<Vec<String>> {
        if self.replica_labels.is_empty() {
            self.shard_endpoints
                .iter()
                .enumerate()
                .map(|(shard, replicas)| {
                    (0..replicas.len())
                        .map(|replica| format!("{}-{shard}-{replica}", self.component_type))
                        .collect()
                })
                .collect()
        } else {
            self.replica_labels.clone()
        }
    }
}

impl<W: ShardWriter> std::fmt::Debug for SinkParts<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkParts")
            .field("component_type", &self.component_type)
            .field(
                "shards",
                &self
                    .shard_endpoints
                    .iter()
                    .map(Vec::len)
                    .collect::<Vec<_>>(),
            )
            .field("pool", &self.pool)
            .field("probe", &self.probe.is_some())
            .finish_non_exhaustive()
    }
}

impl<W: ShardWriter> SinkBundle for SinkParts<W> {
    type Writer = W;

    fn into_parts(self) -> SinkParts<W> {
        self
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::error::SinkError;
    use crate::sink::SealedBatch;

    struct NullWriter;
    impl ShardWriter for NullWriter {
        type Endpoint = ();
        async fn write_batch(&self, (): &(), _batch: &SealedBatch) -> Result<(), SinkError> {
            Ok(())
        }
    }

    #[test]
    fn default_replica_labels_follow_topology_shape() {
        let parts = SinkParts::new(
            NullWriter,
            vec![vec![(), ()], vec![()]],
            SinkPoolConfig::default(),
        )
        .with_component_type("stdout");
        assert_eq!(
            parts.effective_replica_labels(),
            vec![
                vec!["stdout-0-0".to_string(), "stdout-0-1".to_string()],
                vec!["stdout-1-0".to_string()]
            ]
        );
    }

    #[test]
    fn explicit_replica_labels_win() {
        let parts = SinkParts::new(NullWriter, vec![vec![()]], SinkPoolConfig::default())
            .with_replica_labels(vec![vec!["primary".to_string()]]);
        assert_eq!(
            parts.effective_replica_labels(),
            vec![vec!["primary".to_string()]]
        );
    }

    #[test]
    fn sink_parts_round_trips_through_the_trait() {
        let parts = SinkParts::new(NullWriter, vec![vec![()]], SinkPoolConfig::default())
            .with_component_type("capture");
        let parts = SinkBundle::into_parts(parts);
        assert_eq!(parts.component_type, "capture");
        assert_eq!(parts.shard_endpoints.len(), 1);
    }
}
