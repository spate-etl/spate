//! The sink pool: one worker task per shard on the I/O runtime.

use super::config::SinkPoolConfig;
use super::worker::{ShardWorker, WorkerReport};
use super::{EncodedChunk, ShardWriter, SinkFailures};
use crate::backpressure::InflightBudget;
use crate::clock::tokio::{Clock, SystemClock};
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
    /// Batches abandoned (failed acknowledgments; replay after restart).
    pub abandoned: u64,
}

/// Shard workers plus the handles to probe and drain them.
///
/// Construction wiring: build the queues with
/// [`shard_queues`](super::shard_queues), hand the
/// [`ShardQueues`](super::ShardQueues) (senders) to the pipeline threads'
/// terminal stages, and the receivers to [`SinkPool::spawn`].
#[derive(Debug)]
pub struct SinkPool<W: ShardWriter> {
    writer: Arc<W>,
    endpoints: Vec<Arc<Vec<W::Endpoint>>>,
    workers: Vec<JoinHandle<WorkerReport>>,
    drain_tx: watch::Sender<Option<Instant>>,
    metrics: Vec<Arc<SinkShardMetrics>>,
    /// The clock every deadline in the pool and its workers runs on.
    clock: Arc<dyn Clock>,
}

/// How long past the drain deadline a shard worker gets before `drain` gives
/// up on it and force-aborts it.
///
/// The deadline itself is cooperative. Workers watch it, abort their in-flight
/// writes and abandon what is left. This constant is the backstop under that,
/// so a worker that cannot honor the deadline degrades shutdown to a lost
/// drain report rather than an unbounded hang.
///
/// The tightest constraint is the controller rather than the pod's
/// `terminationGracePeriodSeconds`. The controller waits only `deadline + 2s`
/// for the drain to report before giving up and running the final commit
/// anyway (`pipeline::controller`). At an equal 2s the backstop could never
/// fire *before* that, so the commit would race a pool still resolving.
/// Ordering, tightest first: the worker's own `ABORT_GRACE` (500ms) < this <
/// the controller's 2s. The ordering holds on real time, where the stop clock
/// and the pool's `SystemClock` agree.
const BACKSTOP_GRACE: Duration = Duration::from_secs(1);

impl<W: ShardWriter> SinkPool<W> {
    /// Spawn one worker per shard onto `runtime`.
    ///
    /// `shard_endpoints[s]` are shard `s`'s replica endpoints;
    /// `receivers[s]` its chunk queue; `metrics[s]` its pre-registered
    /// handles. All three must have equal length, with at least one replica
    /// per shard. A worker that abandons a batch records the reason in
    /// `failures` under `sink_name`.
    ///
    /// # Panics
    ///
    /// Panics when the lengths disagree or a shard has no replicas
    /// (construction-time configuration errors).
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
        sink_name: &str,
        failures: SinkFailures,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        Self::spawn_on_clock(
            writer,
            shard_endpoints,
            receivers,
            config,
            budget,
            metrics,
            pipeline_name,
            sink_name,
            failures,
            runtime,
            Arc::new(SystemClock),
        )
    }

    /// [`spawn`](Self::spawn), with every deadline in the pool on `clock`.
    #[expect(
        clippy::too_many_arguments,
        reason = "construction-time wiring call, used once by the pipeline runtime"
    )]
    pub(crate) fn spawn_on_clock(
        writer: Arc<W>,
        shard_endpoints: Vec<Vec<W::Endpoint>>,
        receivers: Vec<mpsc::Receiver<EncodedChunk>>,
        config: SinkPoolConfig,
        budget: Arc<InflightBudget>,
        metrics: Vec<SinkShardMetrics>,
        pipeline_name: &str,
        sink_name: &str,
        failures: SinkFailures,
        runtime: &tokio::runtime::Handle,
        clock: Arc<dyn Clock>,
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
        // Connectors validate this in their own config parsing, but a
        // programmatically built `SinkPoolConfig` reaches here unchecked. A
        // zero-permit semaphore means every sealed batch parks forever
        // instead of failing at construction.
        assert!(
            config.inflight.max_per_shard > 0,
            "inflight.max_per_shard must be greater than zero"
        );

        // Warn once per pool, from the seam every sink passes through, so no
        // connector has to mirror the check.
        if config.retry.stalls_indefinitely() {
            tracing::warn!(
                retry_max = ?config.retry.max,
                "retry.max_attempts is 0 (unbounded) and retry.max is over 5m: once a \
                 shard backs off to its ceiling it sleeps that long between attempts and \
                 never abandons the batch, so a stalled shard looks identical to a \
                 healthy idle one. Bound it with retry.max_attempts, or lower retry.max. \
                 If this is deliberate, watch spate_sink_retry_backoff_seconds — it reads \
                 the backoff a shard is sleeping between attempts right now."
            );
        }

        let (drain_tx, drain_rx) = watch::channel(None);
        let endpoints: Vec<Arc<Vec<W::Endpoint>>> =
            shard_endpoints.into_iter().map(Arc::new).collect();

        let nonce = run_nonce();
        let sink: Arc<str> = Arc::from(sink_name);
        // Shared with the workers rather than moved into them; `drain` needs
        // the handles too, to report a shard that overruns its deadline.
        let metrics: Vec<Arc<SinkShardMetrics>> = metrics.into_iter().map(Arc::new).collect();
        let workers = receivers
            .into_iter()
            .zip(metrics.iter().map(Arc::clone))
            .enumerate()
            .map(|(shard, (rx, shard_metrics))| {
                let worker = ShardWorker {
                    sink: Arc::clone(&sink),
                    failures: failures.clone(),
                    shard: u32::try_from(shard).unwrap_or(u32::MAX),
                    writer: Arc::clone(&writer),
                    endpoints: Arc::clone(&endpoints[shard]),
                    rx,
                    cfg: config,
                    budget: Arc::clone(&budget),
                    metrics: shard_metrics,
                    drain_deadline: drain_rx.clone(),
                    token_prefix: format!("{pipeline_name}-{nonce}-{shard}-"),
                    clock: Arc::clone(&clock),
                };
                runtime.spawn(worker.run())
            })
            .collect();

        SinkPool {
            writer,
            endpoints,
            workers,
            drain_tx,
            metrics,
            clock,
        }
    }

    /// A pool around arbitrary worker handles, so the drain backstop can be
    /// tested against a worker that ignores its deadline.
    #[cfg(test)]
    pub(crate) fn from_workers(
        writer: Arc<W>,
        workers: Vec<JoinHandle<WorkerReport>>,
        drain_tx: watch::Sender<Option<Instant>>,
        metrics: Vec<Arc<SinkShardMetrics>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        SinkPool {
            writer,
            endpoints: Vec::new(),
            workers,
            drain_tx,
            metrics,
            clock,
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

    /// Drain the pool. Workers force-seal partial batches, then in-flight
    /// writes get until `deadline` before being aborted and abandoned.
    ///
    /// `deadline` runs on the pool's clock. Always returns. A worker that does not stop by `deadline` is
    /// force-aborted `BACKSTOP_GRACE` later; its acknowledgments fail with
    /// it (so at-least-once holds and the data replays), but its counts are
    /// missing from the returned report.
    ///
    /// Contract: the caller must have dropped every
    /// [`ShardQueues`](super::ShardQueues) clone first. Workers only enter
    /// their drain phase once their queue closes.
    pub async fn drain(self, deadline: Duration) -> DrainReport {
        let deadline_at = self.clock.now() + deadline;
        let _ = self.drain_tx.send(Some(deadline_at));
        // Absolute, so joining the shards in sequence still bounds the whole
        // drain. One wedged shard spends the budget once, and the shards
        // behind it (long since finished) are joined immediately after.
        let hard_at = deadline_at + BACKSTOP_GRACE;
        let mut report = WorkerReport::default();
        let mut forced = 0usize;
        for (shard, mut handle) in self.workers.into_iter().enumerate() {
            // Biased with the handle first, so a shard that finished as the
            // backstop came due still reports.
            let joined = tokio::select! {
                biased;
                joined = &mut handle => Some(joined),
                () = self.clock.sleep_until(hard_at) => None,
            };
            match joined {
                Some(Ok(r)) => report.absorb(r),
                Some(Err(join_err)) => {
                    tracing::error!(shard, error = %join_err, "sink shard worker panicked");
                }
                None => {
                    handle.abort();
                    self.metrics[shard].drain_overrun();
                    // `hard_at` is absolute, which bounds the whole sequential
                    // join. It also means the first overrun spends the budget
                    // and every shard behind it times out instantly, however
                    // healthy. Only the first can be diagnosed as the culprit;
                    // the rest are collateral. The realistic cause (a writer
                    // blocking runtime threads) starves them all.
                    if forced == 0 {
                        tracing::error!(
                            shard,
                            grace = ?BACKSTOP_GRACE,
                            "sink shard worker did not return by the drain deadline and was \
                             force-aborted — a framework bug, not an operating condition. Its \
                             acknowledgments fail and that data replays after restart, but its \
                             flushed/abandoned counts are missing from the drain report."
                        );
                    } else {
                        tracing::error!(
                            shard,
                            "sink shard worker force-aborted: the drain budget was already spent \
                             by an earlier shard, so this one was given no time of its own. It \
                             may have been healthy. Same consequence — its acknowledgments fail \
                             and that data replays — but diagnose the first shard reported above."
                        );
                    }
                    forced += 1;
                }
            }
        }
        DrainReport {
            flushed: report.flushed,
            abandoned: report.abandoned,
        }
    }
}

/// A short id unique across process runs (the wall-clock time, pid, and an
/// in-process counter), embedded in every deduplication token.
///
/// Without it, tokens are `{pipeline}-{shard}-{seq}` with `seq` restarting
/// at 0 on every start. A restarted (or same-named concurrent) pipeline
/// reuses tokens still inside the server's deduplication window, and the
/// sink silently discards **new** rows while acknowledging them, losing data
/// wherever server-side dedup is enabled. With the nonce, in-session retries
/// still share their batch's token (idempotent), while cross-run collisions
/// are impossible; crash replay lands duplicate rows instead of losing them,
/// as at-least-once requires.
fn run_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = crate::clock::wall_now()
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
