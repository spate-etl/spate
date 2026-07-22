//! Bounded per-shard chunk queues: the pipeline→sink handoff.
//!
//! Senders live on pipeline threads and only ever `try_send` (the
//! backpressure invariant — never block a poll loop); receivers live in
//! shard worker tasks on the I/O runtime.

use super::EncodedChunk;
use crate::metrics::{ComponentLabels, MetricsError, QueueMetrics};
use std::sync::Arc;
use tokio::sync::mpsc;

/// A rejected chunk, handed back so the terminal stage can park it and
/// report `Blocked` upstream.
#[derive(Debug)]
pub struct ChunkSendError(pub EncodedChunk);

/// Sending side of every shard queue, shared by pipeline threads.
#[derive(Clone, Debug)]
pub struct ShardQueues {
    senders: Vec<mpsc::Sender<EncodedChunk>>,
    capacity: usize,
    /// Per-shard `etl_queue_*` handles, shared across every producer clone of
    /// this queue. `None` until `attach_metrics` runs (and in bare transport
    /// tests), so the no-metrics path stays untouched.
    metrics: Option<Arc<Vec<QueueMetrics>>>,
}

impl ShardQueues {
    /// Number of shards.
    #[must_use]
    pub fn num_shards(&self) -> usize {
        self.senders.len()
    }

    /// Configured per-shard capacity (in chunks).
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Non-blocking send to `shard`. On `Err` the chunk comes back and the
    /// caller applies backpressure. A closed queue (sink shut down) also
    /// returns the chunk; the driver observes shutdown separately.
    pub fn try_send(&self, shard: usize, chunk: EncodedChunk) -> Result<(), ChunkSendError> {
        let (result, live) = match self.senders[shard].try_send(chunk) {
            Ok(()) => (Ok(()), true),
            Err(mpsc::error::TrySendError::Full(c)) => {
                // A full queue is a backpressure signal; count it. The queue is
                // still live, so its depth is meaningful (and reads as full).
                if let Some(m) = &self.metrics {
                    m[shard].full_events(1);
                }
                (Err(ChunkSendError(c)), true)
            }
            // A closed queue is shutdown, not backpressure — don't count it, and
            // don't sample a depth from a torn-down channel.
            Err(mpsc::error::TrySendError::Closed(c)) => (Err(ChunkSendError(c)), false),
        };
        // Sample depth from the live channel, mirroring the `all_below` fill
        // calculation. `capacity()` is the free slots remaining.
        if let Some(m) = self.metrics.as_ref().filter(|_| live) {
            m[shard].set_depth(self.capacity - self.senders[shard].capacity());
        }
        result
    }

    /// Pre-register one `QueueMetrics` per shard, resolved through `labels` so
    /// the `etl_queue_*` series inherit the standard component labels. Call
    /// once, before this handle is cloned into pipeline terminals, so every
    /// producer clone shares the same handles.
    ///
    /// Fails when another live handle set already owns one of these queue
    /// edges — two pipelines with the same names in one process. The depth
    /// gauge cannot be shared, so the caller refuses to build rather than
    /// letting two writers alternate readings.
    pub(crate) fn attach_metrics(&mut self, labels: &ComponentLabels) -> Result<(), MetricsError> {
        let metrics = (0..self.senders.len())
            .map(|i| {
                QueueMetrics::try_new(labels, &format!("chain->sink/shard-{i}"), self.capacity)
            })
            .collect::<Result<_, _>>()?;
        self.metrics = Some(Arc::new(metrics));
        Ok(())
    }

    /// Whether every shard queue is below `ratio` of its capacity —
    /// the resume condition the backpressure controller asks about.
    #[must_use]
    pub fn all_below(&self, ratio: f64) -> bool {
        let threshold = (self.capacity as f64 * ratio) as usize;
        self.senders
            .iter()
            .all(|s| self.capacity - s.capacity() <= threshold)
    }
}

/// Build the queues: one bounded channel per shard. Returns the shared
/// sender handle and the per-shard receivers for the workers.
#[must_use]
pub fn shard_queues(
    num_shards: usize,
    capacity: usize,
) -> (ShardQueues, Vec<mpsc::Receiver<EncodedChunk>>) {
    assert!(num_shards > 0, "a sink needs at least one shard");
    assert!(capacity > 0, "shard queues need non-zero capacity");
    let (senders, receivers): (Vec<_>, Vec<_>) =
        (0..num_shards).map(|_| mpsc::channel(capacity)).unzip();
    (
        ShardQueues {
            senders,
            capacity,
            metrics: None,
        },
        receivers,
    )
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn chunk() -> EncodedChunk {
        EncodedChunk {
            oldest_ingest: std::time::Instant::now(),
            oldest_event_ms: 0,
            frame: Bytes::from_static(b"x"),
            rows: 1,
            acks: crate::checkpoint::AckSet::new(),
        }
    }

    #[test]
    fn try_send_never_blocks_and_returns_the_chunk_when_full() {
        let (q, mut rx) = shard_queues(1, 2);
        assert!(q.try_send(0, chunk()).is_ok());
        assert!(q.try_send(0, chunk()).is_ok());
        let ChunkSendError(returned) = q.try_send(0, chunk()).unwrap_err();
        assert_eq!(returned.rows, 1);
        assert!(rx[0].try_recv().is_ok());
        assert!(q.try_send(0, chunk()).is_ok(), "capacity freed");
        let _ = rx;
    }

    #[test]
    fn dropping_a_receiver_with_queued_chunks_fails_their_acks() {
        use crate::checkpoint::{AckRef, AckStatus};
        let (q, rx) = shard_queues(1, 4);
        let (ack, ack_rx) = AckRef::test_pair();
        let mut c = chunk();
        c.acks = vec![ack.clone()].into();
        drop(ack);
        q.try_send(0, c).expect("queued");
        drop(rx); // sink torn down with the chunk still queued
        assert_eq!(
            ack_rx.try_recv().expect("resolved").status,
            AckStatus::Failed,
            "chunks lost in a dropped queue must fail their batches"
        );
    }

    #[test]
    fn closed_queue_hands_the_chunk_back() {
        let (q, rx) = shard_queues(1, 1);
        drop(rx);
        assert!(q.try_send(0, chunk()).is_err());
    }

    #[test]
    fn all_below_reflects_fill_ratio() {
        let (q, _rx) = shard_queues(2, 4);
        assert!(q.all_below(0.5));
        q.try_send(0, chunk()).unwrap();
        q.try_send(0, chunk()).unwrap();
        q.try_send(0, chunk()).unwrap();
        assert!(!q.all_below(0.5), "shard 0 is 75% full");
    }

    #[test]
    fn attached_metrics_emit_the_documented_queue_family() {
        use metrics_exporter_prometheus::PrometheusBuilder;

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let (mut q, rx) = shard_queues(1, 2);
            q.attach_metrics(&ComponentLabels::new(
                "orders",
                "queue-family-test",
                "clickhouse",
            ))
            .expect("free series");
            q.try_send(0, chunk()).expect("first send fits"); // depth -> 1
            q.try_send(0, chunk()).expect("second send fills"); // depth -> 2
            q.try_send(0, chunk()).expect_err("full"); // Full: full_events -> 1, depth -> 2
            drop(rx); // tear the sink down
            q.try_send(0, chunk()).expect_err("closed"); // Closed: must NOT count, no resample
        });
        let rendered = handle.render();

        let series = r#"{pipeline="orders",component="queue-family-test",component_type="clickhouse",queue="chain->sink/shard-0"}"#;
        for needle in [
            format!("etl_queue_capacity{series} 2"),
            // A single Full rejection; the following Closed rejection is excluded.
            format!("etl_queue_full_events_total{series} 1"),
            // Depth was last sampled on the Full send, reading the full queue.
            format!("etl_queue_depth{series} 2"),
        ] {
            assert!(
                rendered.contains(&needle),
                "rendered output missing `{needle}`:\n{rendered}"
            );
        }
    }
}
