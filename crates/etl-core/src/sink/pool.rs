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

        // Warn once per pool, from the seam every sink passes through, so no
        // connector has to mirror the check.
        if config.retry.stalls_indefinitely() {
            tracing::warn!(
                retry_max = ?config.retry.max,
                "retry.max_attempts is 0 (unbounded) and retry.max is over 5m: once a \
                 shard backs off to its ceiling it sleeps that long between attempts and \
                 never abandons the batch, so a stalled shard looks identical to a \
                 healthy idle one. Bound it with retry.max_attempts, or lower retry.max."
            );
        }

        let (drain_tx, drain_rx) = watch::channel(None);
        let endpoints: Vec<Arc<Vec<W::Endpoint>>> =
            shard_endpoints.into_iter().map(Arc::new).collect();

        let nonce = run_nonce();
        let workers = receivers
            .into_iter()
            .zip(metrics)
            .enumerate()
            .map(|(shard, (rx, shard_metrics))| {
                let worker = ShardWorker {
                    shard: u32::try_from(shard).unwrap_or(u32::MAX),
                    writer: Arc::clone(&writer),
                    endpoints: Arc::clone(&endpoints[shard]),
                    rx,
                    cfg: config,
                    budget: Arc::clone(&budget),
                    metrics: Arc::new(shard_metrics),
                    drain_deadline: drain_rx.clone(),
                    token_prefix: format!("{pipeline_name}-{nonce}-{shard}-"),
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

/// A short id unique across process runs (boot time, pid, and an in-process
/// counter), embedded in every deduplication token.
///
/// Without it, tokens are `{pipeline}-{shard}-{seq}` with `seq` restarting
/// at 0 on every start: a restarted (or same-named concurrent) pipeline
/// reuses tokens still inside the server's deduplication window, and the
/// sink silently discards **new** rows while acknowledging them — data
/// loss precisely when server-side dedup is enabled. With the nonce,
/// in-session retries still share their batch's token (idempotent), while
/// cross-run collisions are impossible; crash replay lands duplicate rows
/// instead of losing them, which is the documented at-least-once contract.
fn run_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = u64::from(std::process::id());
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:x}", nanos ^ (pid << 48) ^ (n << 40))
}

#[cfg(all(test, not(loom)))]
mod nonce_tests {
    use super::run_nonce;

    #[test]
    fn nonces_differ_within_and_across_calls() {
        let a = run_nonce();
        let b = run_nonce();
        assert_ne!(a, b, "two pools in one process must not share tokens");
        assert!(!a.is_empty() && a.len() <= 16);
    }
}
