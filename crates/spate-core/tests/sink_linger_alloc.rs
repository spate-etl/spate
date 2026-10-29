//! Allocation bound for the sink worker's intake loop while a batch lingers:
//! an intake pass that parks on the linger timer does not allocate.

use bytes::Bytes;
use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::AckSet;
use spate_core::error::SinkError;
use spate_core::metrics::{ComponentLabels, E2eBasis, SinkShardMetrics};
use spate_core::sink::{
    EncodedChunk, SealedBatch, ShardWriter, SinkFailures, SinkPool, SinkPoolConfig, shard_queues,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

struct CountingAlloc;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static COUNTING: AtomicBool = AtomicBool::new(false);

// SAFETY: delegates to `System`, adding a relaxed counter increment.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: caller's contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: caller's contract.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: caller's contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

struct Noop;
impl ShardWriter for Noop {
    type Endpoint = ();
    async fn write_batch(&self, _: &(), _: &SealedBatch) -> Result<(), SinkError> {
        Ok(())
    }
}

fn chunk() -> EncodedChunk {
    EncodedChunk {
        oldest_ingest: std::time::Instant::now(),
        oldest_event_ms: 0,
        frame: Bytes::from_static(b"x"),
        raw_len: 1,
        rows: 1,
        acks: AckSet::new(),
    }
}

/// Thousands of single-chunk intake passes under one lingering batch
/// allocate far fewer times than there are passes.
#[test]
fn intake_passes_under_a_lingering_batch_do_not_allocate_each() {
    const N: usize = 4096;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        let (queues, receivers) = shard_queues(1, N + 8);
        let labels = ComponentLabels::new("sink_linger_alloc", "sink", "mock");
        let metrics = vec![SinkShardMetrics::new(
            &labels,
            0,
            &["r0".into()],
            E2eBasis::Ingest,
        )];
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.max_rows = u64::MAX;
        cfg.batch.max_bytes = u64::MAX;
        cfg.batch.linger = Duration::from_secs(3600);
        let pool = SinkPool::spawn(
            Arc::new(Noop),
            vec![vec![()]],
            receivers,
            cfg,
            Arc::new(InflightBudget::new()),
            metrics,
            "sink_linger_alloc",
            "sink",
            SinkFailures::new(),
            &tokio::runtime::Handle::current(),
        );
        let mut chunks: Vec<EncodedChunk> = (0..N + 1).map(|_| chunk()).collect();
        // Warm up: the first chunk opens the batch, so linger is armed after.
        queues.try_send(0, chunks.pop().unwrap()).ok().unwrap();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        COUNTING.store(true, Ordering::SeqCst);
        for c in chunks.drain(..) {
            queues.try_send(0, c).ok().unwrap();
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
        }
        COUNTING.store(false, Ordering::SeqCst);
        let allocs = ALLOCS.load(Ordering::SeqCst);
        drop(queues);
        let _ = pool.drain(Duration::from_secs(5)).await;
        assert!(
            allocs < (N as u64) / 2,
            "{allocs} allocations over {N} intake passes while a batch lingers"
        );
    });
}
