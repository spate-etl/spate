//! Bounded per-shard chunk queues: the pipeline→sink handoff.
//!
//! Senders live on pipeline threads and only ever `try_send` (the
//! backpressure invariant — never block a poll loop); receivers live in
//! shard worker tasks on the I/O runtime.

use super::EncodedChunk;
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
        match self.senders[shard].try_send(chunk) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(c) | mpsc::error::TrySendError::Closed(c)) => {
                Err(ChunkSendError(c))
            }
        }
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
    (ShardQueues { senders, capacity }, receivers)
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn chunk() -> EncodedChunk {
        EncodedChunk {
            frame: Bytes::from_static(b"x"),
            rows: 1,
            acks: Vec::new(),
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
}
