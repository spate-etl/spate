//! The sink pool: one worker task per shard on the I/O runtime.

use super::config::SinkPoolConfig;
use super::worker::{ShardWorker, WorkerReport};
use super::{EncodedChunk, ShardWriter};
use crate::backpressure::InflightBudget;
use crate::error::SinkError;
use crate::metrics::SinkShardMetrics;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// What a full-pool drain accomplished.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DrainReport {
    /// Batches durably written over the pool's lifetime.
    pub flushed: u64,
    /// Batches abandoned (failed acknowledgements; replay after restart).
    pub abandoned: u64,
}

/// Shard workers plus the handles to probe and drain them.
///
/// Construction wiring: build the queues with
/// [`shard_queues`](super::shard_queues), hand the [`ShardQueues`]
/// (senders) to the pipeline threads' terminal stages, and the receivers to
/// [`SinkPool::spawn`].
#[derive(Debug)]
pub struct SinkPool<W: ShardWriter> {
    writer: Arc<W>,
    endpoints: Vec<Arc<Vec<W::Endpoint>>>,
    workers: Vec<JoinHandle<WorkerReport>>,
    drain_tx: watch::Sender<Option<Instant>>,
}

impl<W: ShardWriter> SinkPool<W> {
    /// Spawn one worker per shard onto `runtime`.
    ///
    /// `shard_endpoints[s]` are shard `s`'s replica endpoints;
    /// `receivers[s]` its chunk queue; `metrics[s]` its pre-registered
    /// handles. All three must have equal length, with at least one replica
    /// per shard.
    ///
    /// # Panics
    ///
    /// Panics when the lengths disagree or a shard has no replicas —
    /// construction-time configuration errors.
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "construction-time wiring call, used once by the pipeline runtime"
    )]
    pub fn spawn(
        writer: Arc<W>,
        shard_endpoints: Vec<Vec<W::Endpoint>>,
        receivers: Vec<mpsc::Receiver<EncodedChunk>>,
        config: SinkPoolConfig,
        budget: Arc<InflightBudget>,
        metrics: Vec<SinkShardMetrics>,
        pipeline_name: &str,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        assert_eq!(
            shard_endpoints.len(),
            receivers.len(),
            "one receiver per shard"
        );
        assert_eq!(
            shard_endpoints.len(),
            metrics.len(),
            "one metrics set per shard"
        );
        assert!(
            shard_endpoints.iter().all(|r| !r.is_empty()),
            "every shard needs at least one replica"
        );

        let (drain_tx, drain_rx) = watch::channel(None);
        let endpoints: Vec<Arc<Vec<W::Endpoint>>> =
            shard_endpoints.into_iter().map(Arc::new).collect();

        let workers = receivers
            .into_iter()
            .zip(metrics)
            .enumerate()
            .map(|(shard, (rx, shard_metrics))| {
                let worker = ShardWorker {
                    writer: Arc::clone(&writer),
                    endpoints: Arc::clone(&endpoints[shard]),
                    rx,
                    cfg: config,
                    budget: Arc::clone(&budget),
                    metrics: Arc::new(shard_metrics),
                    drain_deadline: drain_rx.clone(),
                    token_prefix: format!("{pipeline_name}-{shard}-"),
                };
                runtime.spawn(worker.run())
            })
            .collect();

        SinkPool {
            writer,
            endpoints,
            workers,
            drain_tx,
        }
    }

    /// Probe every replica of every shard (readiness). Fails on the first
    /// unhealthy endpoint.
    pub async fn probe_all(&self) -> Result<(), SinkError> {
        for shard in &self.endpoints {
            for endpoint in shard.iter() {
                self.writer.probe(endpoint).await?;
            }
        }
        Ok(())
    }

    /// Drain the pool: workers force-seal partial batches, then in-flight
    /// writes get until `deadline` before being aborted and abandoned.
    ///
    /// Contract: the caller must have dropped every [`ShardQueues`]
    /// (super::ShardQueues) clone first — workers only enter their drain
    /// phase once their queue closes.
    pub async fn drain(self, deadline: Duration) -> DrainReport {
        let _ = self.drain_tx.send(Some(Instant::now() + deadline));
        let mut report = WorkerReport::default();
        for handle in self.workers {
            match handle.await {
                Ok(r) => report.absorb(r),
                Err(join_err) => {
                    tracing::error!(error = %join_err, "sink shard worker panicked");
                }
            }
        }
        DrainReport {
            flushed: report.flushed,
            abandoned: report.abandoned,
        }
    }
}
