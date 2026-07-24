//! Behavioural tests for the sink worker pool, driven through a scriptable
//! mock writer.

use super::*;
use crate::backpressure::InflightBudget;
use crate::checkpoint::{AckMsg, AckRef, AckStatus};
use crate::error::ErrorClass;
use crate::metrics::{ComponentLabels, E2eBasis, SinkShardMetrics};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
enum Outcome {
    Write(Duration),
    Fail(ErrorClass, Duration),
    Hang,
    /// Panic the write task (simulates a writer bug / poisoned lock).
    Panic,
    /// Block the runtime thread outright, so an abort cannot land until it
    /// returns (simulates a writer doing synchronous work between awaits).
    BlockThread(Duration),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MockEndpoint {
    shard: usize,
    replica: usize,
}

#[derive(Clone, Debug)]
struct Call {
    replica: usize,
    token: String,
    rows: u64,
}

#[derive(Debug)]
struct MockWriter {
    script: Mutex<HashMap<(usize, usize), std::collections::VecDeque<Outcome>>>,
    global_default: Mutex<Outcome>,
    log: Mutex<Vec<Call>>,
    concurrent: AtomicUsize,
    max_concurrent: AtomicUsize,
    /// Cuts an `Outcome::BlockThread` short, so a test that has proved its
    /// point does not pay for the rest of the block at runtime teardown.
    release: std::sync::atomic::AtomicBool,
}

impl MockWriter {
    fn new() -> Arc<Self> {
        Arc::new(MockWriter {
            script: Mutex::new(HashMap::new()),
            global_default: Mutex::new(Outcome::Write(Duration::ZERO)),
            log: Mutex::new(Vec::new()),
            concurrent: AtomicUsize::new(0),
            max_concurrent: AtomicUsize::new(0),
            release: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Let any in-progress [`Outcome::BlockThread`] return early.
    fn release_blocked(&self) {
        self.release.store(true, Ordering::SeqCst);
    }

    fn script(&self, shard: usize, replica: usize, outcomes: impl IntoIterator<Item = Outcome>) {
        self.script
            .lock()
            .unwrap()
            .entry((shard, replica))
            .or_default()
            .extend(outcomes);
    }

    fn set_default(&self, outcome: Outcome) {
        *self.global_default.lock().unwrap() = outcome;
    }

    fn calls(&self) -> Vec<Call> {
        self.log.lock().unwrap().clone()
    }
}

impl ShardWriter for MockWriter {
    type Endpoint = MockEndpoint;

    async fn write_batch(&self, ep: &MockEndpoint, batch: &SealedBatch) -> Result<(), SinkError> {
        let outcome = {
            let mut script = self.script.lock().unwrap();
            script
                .get_mut(&(ep.shard, ep.replica))
                .and_then(std::collections::VecDeque::pop_front)
                .unwrap_or_else(|| self.global_default.lock().unwrap().clone())
        };
        self.log.lock().unwrap().push(Call {
            replica: ep.replica,
            token: batch.dedup_token.clone(),
            rows: batch.rows,
        });
        let now = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_concurrent.fetch_max(now, Ordering::SeqCst);
        let result = match outcome {
            Outcome::Write(delay) => {
                tokio::time::sleep(delay).await;
                Ok(())
            }
            Outcome::Fail(class, delay) => {
                tokio::time::sleep(delay).await;
                Err(SinkError::Client {
                    class,
                    reason: "scripted failure".into(),
                })
            }
            Outcome::Hang => {
                // Held "concurrent" on purpose; aborted at drain deadline.
                std::future::pending::<()>().await;
                unreachable!()
            }
            Outcome::BlockThread(d) => {
                // A blocking loop, not an await: the task never reaches a
                // yield point, so `abort` cannot land on it.
                let until = std::time::Instant::now() + d;
                while std::time::Instant::now() < until && !self.release.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Ok(())
            }
            Outcome::Panic => panic!("scripted sink write panic"),
        };
        self.concurrent.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

struct Fixture {
    writer: Arc<MockWriter>,
    queues: ShardQueues,
    pool: SinkPool<MockWriter>,
    budget: Arc<InflightBudget>,
}

fn next_component() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "sink-{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

fn fixture(shards: usize, replicas: usize, cfg: SinkPoolConfig, queue_cap: usize) -> Fixture {
    let writer = MockWriter::new();
    let (queues, receivers) = shard_queues(shards, queue_cap);
    let budget = Arc::new(InflightBudget::new());
    // A distinct component per fixture: the shard gauges are owned by one live
    // handle set per series (`metrics::ownership`), and under `cargo test`
    // these fixtures are alive concurrently in one process. Sharing a label
    // set would make all but the first a shadow that publishes no gauge.
    let labels = ComponentLabels::new("test", next_component(), "mock");
    let endpoints: Vec<Vec<MockEndpoint>> = (0..shards)
        .map(|s| {
            (0..replicas)
                .map(|r| MockEndpoint {
                    shard: s,
                    replica: r,
                })
                .collect()
        })
        .collect();
    let replica_names: Vec<String> = (0..replicas).map(|r| format!("r{r}")).collect();
    let metrics: Vec<SinkShardMetrics> = (0..shards)
        .map(|s| {
            SinkShardMetrics::new(
                &labels,
                u32::try_from(s).unwrap(),
                &replica_names,
                E2eBasis::Ingest,
            )
        })
        .collect();
    let pool = SinkPool::spawn(
        Arc::clone(&writer),
        endpoints,
        receivers,
        cfg,
        Arc::clone(&budget),
        metrics,
        "test",
        &tokio::runtime::Handle::current(),
    );
    Fixture {
        writer,
        queues,
        pool,
        budget,
    }
}

/// A chunk of `rows` rows, `bytes_per_row * rows` bytes, carrying `ack`.
fn chunk(rows: u32, bytes_per_row: usize, ack: &AckRef) -> EncodedChunk {
    EncodedChunk {
        oldest_ingest: std::time::Instant::now(),
        oldest_event_ms: 0,
        frame: Bytes::from(vec![0u8; bytes_per_row * rows as usize]),
        rows,
        acks: vec![ack.clone()].into(),
    }
}

fn small_batches() -> SinkPoolConfig {
    SinkPoolConfig {
        batch: BatchConfig {
            max_rows: 1,
            max_bytes: u64::MAX,
            linger: Duration::from_secs(3600),
        },
        retry: RetryConfig {
            initial: Duration::from_millis(1),
            max: Duration::from_millis(4),
            multiplier: 2.0,
            jitter: 0.0,
            max_attempts: 0,
        },
        ..SinkPoolConfig::default()
    }
}

#[tokio::test]
async fn chunks_from_many_senders_merge_into_one_big_batch() {
    let cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: 200,
            max_bytes: u64::MAX,
            linger: Duration::from_secs(3600),
        },
        ..SinkPoolConfig::default()
    };
    let f = fixture(1, 1, cfg, 64);
    let (ack, _rx) = AckRef::test_pair();

    // Two concurrent senders, 10 chunks of 10 rows each.
    let q1 = f.queues.clone();
    let q2 = f.queues.clone();
    let a1 = ack.clone();
    let a2 = ack.clone();
    let s1 = tokio::spawn(async move {
        for _ in 0..10 {
            q1.try_send(0, chunk(10, 1, &a1)).unwrap();
            tokio::task::yield_now().await;
        }
    });
    let s2 = tokio::spawn(async move {
        for _ in 0..10 {
            q2.try_send(0, chunk(10, 1, &a2)).unwrap();
            tokio::task::yield_now().await;
        }
    });
    s1.await.unwrap();
    s2.await.unwrap();
    drop(ack);
    drop(f.queues);
    let report = f.pool.drain(Duration::from_secs(5)).await;

    let calls = f.writer.calls();
    assert_eq!(
        report,
        DrainReport {
            flushed: 1,
            abandoned: 0
        }
    );
    assert_eq!(calls.len(), 1, "20 chunks merged into one batch");
    assert_eq!(calls[0].rows, 200, "batch spans both senders' chunks");
}

#[tokio::test(start_paused = true)]
async fn linger_seals_a_partial_batch() {
    let cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: u64::MAX,
            linger: Duration::from_secs(1),
        },
        ..SinkPoolConfig::default()
    };
    let f = fixture(1, 1, cfg, 16);
    let (ack, ack_rx) = AckRef::test_pair();
    for _ in 0..3 {
        f.queues.try_send(0, chunk(1, 1, &ack)).unwrap();
    }
    drop(ack);

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let calls = f.writer.calls();
    assert_eq!(calls.len(), 1, "linger sealed without hitting thresholds");
    assert_eq!(calls[0].rows, 3);
    assert_eq!(
        ack_rx.try_recv().unwrap().status,
        AckStatus::Delivered,
        "acks resolve on write success"
    );

    drop(f.queues);
    f.pool.drain(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn byte_threshold_seals_independently_of_rows() {
    let cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: 1024,
            linger: Duration::from_secs(3600),
        },
        ..SinkPoolConfig::default()
    };
    let f = fixture(1, 1, cfg, 16);
    let (ack, _rx) = AckRef::test_pair();
    // Each chunk is 2 KiB > max_bytes: every chunk seals its own batch.
    for _ in 0..3 {
        f.queues.try_send(0, chunk(2, 1024, &ack)).unwrap();
    }
    drop(ack);
    drop(f.queues);
    let report = f.pool.drain(Duration::from_secs(5)).await;
    assert_eq!(report.flushed, 3);
    assert_eq!(f.writer.calls().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn failover_rotates_replicas_and_reuses_the_dedup_token() {
    let f = fixture(1, 3, small_batches(), 16);
    f.writer
        .script(0, 0, [Outcome::Fail(ErrorClass::Retryable, Duration::ZERO)]);
    f.writer
        .script(0, 1, [Outcome::Fail(ErrorClass::Retryable, Duration::ZERO)]);

    let (ack, ack_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(1, 1, &ack)).unwrap();
    drop(ack);
    drop(f.queues);
    let report = f.pool.drain(Duration::from_secs(30)).await;

    let calls = f.writer.calls();
    assert_eq!(
        report,
        DrainReport {
            flushed: 1,
            abandoned: 0
        }
    );
    assert_eq!(calls.len(), 3);
    assert_eq!(
        (calls[0].replica, calls[1].replica, calls[2].replica),
        (0, 1, 2),
        "rotation advances on failure"
    );
    assert!(
        calls.iter().all(|c| c.token == calls[0].token),
        "retries reuse the sealed batch's token"
    );
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Delivered);
}

#[tokio::test(start_paused = true)]
async fn open_breakers_are_skipped_by_later_batches() {
    let mut cfg = small_batches();
    cfg.breaker.failure_threshold = 1;
    cfg.breaker.open_for = Duration::from_secs(60);
    let f = fixture(1, 2, cfg, 16);
    f.writer
        .script(0, 0, [Outcome::Fail(ErrorClass::Retryable, Duration::ZERO)]);

    let (ack, _rx) = AckRef::test_pair();
    // Batch 1 fails on replica 0 (opens its breaker), succeeds on 1.
    f.queues.try_send(0, chunk(1, 1, &ack)).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Batch 2 must go straight to replica 1.
    f.queues.try_send(0, chunk(1, 1, &ack)).unwrap();
    drop(ack);
    drop(f.queues);
    f.pool.drain(Duration::from_secs(5)).await;

    let calls = f.writer.calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[2].replica, 1, "open breaker skipped");
    assert_ne!(calls[1].token, calls[2].token, "distinct batches");
}

#[tokio::test(start_paused = true)]
async fn single_replica_recovers_through_half_open_probe() {
    let mut cfg = small_batches();
    cfg.breaker.failure_threshold = 1;
    cfg.breaker.open_for = Duration::from_secs(5);
    let f = fixture(1, 1, cfg, 16);
    f.writer
        .script(0, 0, [Outcome::Fail(ErrorClass::Retryable, Duration::ZERO)]);

    let (ack, ack_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(1, 1, &ack)).unwrap();
    drop(ack);
    drop(f.queues);
    let report = f.pool.drain(Duration::from_secs(30)).await;

    assert_eq!(
        report,
        DrainReport {
            flushed: 1,
            abandoned: 0
        }
    );
    let calls = f.writer.calls();
    assert_eq!(
        calls.len(),
        2,
        "one failure, one half-open probe after open_for"
    );
    assert_eq!(calls[0].token, calls[1].token);
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Delivered);
}

#[tokio::test(start_paused = true)]
async fn inflight_cap_bounds_concurrent_writes() {
    let mut cfg = small_batches();
    cfg.inflight.max_per_shard = 2;
    let f = fixture(1, 1, cfg, 64);
    f.writer
        .set_default(Outcome::Write(Duration::from_millis(100)));

    let (ack, _rx) = AckRef::test_pair();
    for _ in 0..6 {
        f.queues.try_send(0, chunk(1, 1, &ack)).unwrap();
    }
    drop(ack);
    drop(f.queues);
    let report = f.pool.drain(Duration::from_secs(30)).await;

    assert_eq!(report.flushed, 6);
    assert!(
        f.writer.max_concurrent.load(Ordering::SeqCst) <= 2,
        "at most max_inflight writes ran concurrently, saw {}",
        f.writer.max_concurrent.load(Ordering::SeqCst)
    );
}

#[tokio::test(start_paused = true)]
async fn exhausted_attempts_abandon_and_fail_acks() {
    let mut cfg = small_batches();
    cfg.retry.max_attempts = 3;
    let f = fixture(1, 2, cfg, 16);
    f.writer
        .set_default(Outcome::Fail(ErrorClass::Retryable, Duration::ZERO));

    let (ack, ack_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(1, 1, &ack)).unwrap();
    drop(ack);
    drop(f.queues);
    let report = f.pool.drain(Duration::from_secs(30)).await;

    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 1
        }
    );
    assert_eq!(f.writer.calls().len(), 3, "exactly max_attempts writes");
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
}

#[tokio::test(start_paused = true)]
async fn fatal_errors_abandon_without_retry() {
    let f = fixture(1, 2, small_batches(), 16);
    f.writer
        .script(0, 0, [Outcome::Fail(ErrorClass::Fatal, Duration::ZERO)]);

    let (ack, ack_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(1, 1, &ack)).unwrap();
    drop(ack);
    drop(f.queues);
    let report = f.pool.drain(Duration::from_secs(5)).await;

    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 1
        }
    );
    assert_eq!(f.writer.calls().len(), 1, "no rotation on fatal errors");
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
}

#[tokio::test(start_paused = true)]
async fn drain_flushes_partials_and_abandons_hung_writes() {
    let cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: u64::MAX,
            linger: Duration::from_secs(3600),
        },
        ..SinkPoolConfig::default()
    };
    let f = fixture(2, 1, cfg, 16);
    f.writer.script(1, 0, [Outcome::Hang]);

    let (ok_ack, ok_rx) = AckRef::test_pair();
    let (hung_ack, hung_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(4, 8, &ok_ack)).unwrap();
    f.queues.try_send(1, chunk(4, 8, &hung_ack)).unwrap();
    f.budget.add(32);
    f.budget.add(32);
    drop(ok_ack);
    drop(hung_ack);
    drop(f.queues);

    let report = f.pool.drain(Duration::from_millis(200)).await;
    assert_eq!(
        report,
        DrainReport {
            flushed: 1,
            abandoned: 1
        }
    );
    assert_eq!(ok_rx.try_recv().unwrap().status, AckStatus::Delivered);
    assert_eq!(
        hung_rx.try_recv().unwrap().status,
        AckStatus::Failed,
        "aborted write fails its acks — never silently delivers"
    );
    assert_eq!(f.budget.usage(), 0, "budget released for both outcomes");
}

/// A panicking write task must abandon *its own* batch — not the oldest
/// pending one. Two batches are in flight on one shard: the first (started
/// first) hangs briefly then succeeds; the second panics immediately. The
/// panicked batch's acks must resolve Failed and the healthy one Delivered.
/// The old handler abandoned the minimum-`started` batch, so it failed the
/// wrong (healthy) batch and stranded the panicked one.
#[tokio::test(start_paused = true)]
async fn write_task_panic_abandons_exactly_the_panicked_batch() {
    let mut cfg = small_batches();
    cfg.inflight.max_per_shard = 2;
    let f = fixture(1, 1, cfg, 16);
    // First write (seq 0) hangs 50ms then succeeds; second (seq 1) panics.
    f.writer.script(
        0,
        0,
        [Outcome::Write(Duration::from_millis(50)), Outcome::Panic],
    );

    let (ok_ack, ok_rx) = AckRef::test_pair();
    let (panic_ack, panic_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(1, 8, &ok_ack)).unwrap();
    f.queues.try_send(0, chunk(1, 8, &panic_ack)).unwrap();
    drop(ok_ack);
    drop(panic_ack);
    drop(f.queues);

    let report = f.pool.drain(Duration::from_secs(5)).await;
    assert_eq!(
        report,
        DrainReport {
            flushed: 1,
            abandoned: 1
        }
    );
    assert_eq!(
        ok_rx.try_recv().unwrap().status,
        AckStatus::Delivered,
        "the healthy batch must not be failed for another task's panic"
    );
    assert_eq!(
        panic_rx.try_recv().unwrap().status,
        AckStatus::Failed,
        "the panicked batch's acks must resolve Failed, not leak"
    );
}

/// The drain deadline may be published *after* a worker has already parked
/// on a hung write in its drain phase (queues close when drivers drop their
/// chains, strictly before `SinkPool::drain` runs). The worker must still
/// observe the late deadline and abort — the old once-per-join deadline read
/// deadlocked here.
#[tokio::test(start_paused = true)]
async fn drain_deadline_published_after_worker_parks_is_observed() {
    let f = fixture(1, 1, small_batches(), 16);
    f.writer.script(0, 0, [Outcome::Hang]);

    let (ack, ack_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(4, 8, &ack)).unwrap();
    f.budget.add(32);
    drop(ack);
    // Close intake and let the worker reach its drain phase and park on the
    // hung write with no deadline published yet.
    drop(f.queues);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Only now is the deadline published — the exact ordering that wedged
    // graceful shutdown before the fix.
    let report = f.pool.drain(Duration::from_millis(200)).await;
    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 1
        }
    );
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(f.budget.usage(), 0, "budget released for the aborted write");
}

/// A `ShardQueues` clone leaked past shutdown (stashed outside the chain
/// factory) must not wedge `drain` forever: the published deadline itself
/// breaks the worker out of intake into the bounded drain, force-sealing
/// and flushing the partial batch on the way. The old intake loop only
/// exited when the queue closed, so this drain never returned.
#[tokio::test(start_paused = true)]
async fn drain_with_a_leaked_sender_completes_within_the_deadline() {
    let cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: u64::MAX,
            linger: Duration::from_secs(3600),
        },
        ..SinkPoolConfig::default()
    };
    let f = fixture(1, 1, cfg, 16);

    let (ack, ack_rx) = AckRef::test_pair();
    // Below every seal threshold: sits in the accumulator until drain.
    f.queues.try_send(0, chunk(4, 8, &ack)).unwrap();
    f.budget.add(32);
    drop(ack);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // The clone is deliberately KEPT alive across the drain call.
    let leaked = f.queues.clone();
    drop(f.queues);
    let report = tokio::time::timeout(
        Duration::from_secs(30),
        f.pool.drain(Duration::from_millis(200)),
    )
    .await
    .expect("drain must complete despite the leaked sender");

    assert_eq!(
        report,
        DrainReport {
            flushed: 1,
            abandoned: 0
        },
        "the partial batch force-seals and flushes on the way out"
    );
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Delivered);
    assert_eq!(f.budget.usage(), 0);
    drop(leaked);
}

/// With every in-flight permit held by a hung write, the drain-phase
/// force-seal of a partial batch must not block on the semaphore: it parks
/// the sealed batch and lets the deadline loop abort everything. The old
/// force-seal awaited `acquire_owned()` here forever and never reached the
/// deadline. Both the hung in-flight batch and the partial are abandoned.
#[tokio::test(start_paused = true)]
async fn drain_force_seal_does_not_block_on_a_held_permit() {
    let mut cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: 32,
            linger: Duration::from_secs(3600),
        },
        ..SinkPoolConfig::default()
    };
    cfg.inflight.max_per_shard = 1;
    let f = fixture(1, 1, cfg, 16);
    f.writer.script(0, 0, [Outcome::Hang]);

    let (a_ack, a_rx) = AckRef::test_pair();
    let (b_ack, b_rx) = AckRef::test_pair();
    // Chunk A hits max_bytes → seals, grabs the only permit, and hangs.
    f.queues.try_send(0, chunk(4, 8, &a_ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Chunk B stays partial in the accumulator (below max_bytes).
    f.queues.try_send(0, chunk(1, 8, &b_ack)).unwrap();
    f.budget.add(8);
    drop(a_ack);
    drop(b_ack);
    drop(f.queues);

    let report = f.pool.drain(Duration::from_millis(200)).await;
    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 2
        },
        "the hung batch and the force-sealed partial both abandon"
    );
    assert_eq!(a_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(b_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(f.budget.usage(), 0);
}

/// The same hazard as the force-seal above, on the *intake* path (#83).
/// Chunk B reaches `max_bytes` and so dispatches rather than sitting partial
/// in the accumulator. `dispatch` used to await `acquire_owned()`, which
/// parked the whole worker future outside both `select!`s: the drain deadline
/// was never polled, `tasks.shutdown()` never ran, no permit was ever
/// released, and `drain` waited forever with `drain_timeout` having no effect.
#[tokio::test(start_paused = true)]
async fn a_held_permit_does_not_wedge_dispatch_at_shutdown() {
    let mut cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: 32,
            linger: Duration::from_secs(3600),
        },
        ..SinkPoolConfig::default()
    };
    cfg.inflight.max_per_shard = 1;
    let f = fixture(1, 1, cfg, 16);
    f.writer.set_default(Outcome::Hang);

    let (a_ack, a_rx) = AckRef::test_pair();
    let (b_ack, b_rx) = AckRef::test_pair();
    // A: seals on max_bytes, takes the only permit, hangs.
    f.queues.try_send(0, chunk(4, 8, &a_ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    // B: also reaches max_bytes, so it dispatches with no permit free.
    f.queues.try_send(0, chunk(4, 8, &b_ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(a_ack);
    drop(b_ack);
    drop(f.queues);

    let report = tokio::time::timeout(
        Duration::from_secs(120),
        f.pool.drain(Duration::from_millis(200)),
    )
    .await
    .expect("drain must honour its deadline, not wait on the held permit");

    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 2
        },
        "the hung batch and the one that never got a permit both abandon"
    );
    assert_eq!(a_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(b_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(f.budget.usage(), 0);
}

/// Intake is gated off while a sealed batch waits for a permit, so the drain
/// can begin with the shard queue **closed but not empty**. Those chunks must
/// still be sealed and accounted: dropped unsealed with the receiver they
/// would fail their acknowledgements with no `abandoned` count, no log and no
/// `DrainReport` entry — a silent loss of work, and the one regression the
/// `waiting` gate introduced.
#[tokio::test(start_paused = true)]
async fn chunks_left_in_a_gated_queue_are_sealed_at_drain() {
    let mut cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: 32,
            linger: Duration::from_secs(3600),
        },
        ..SinkPoolConfig::default()
    };
    cfg.inflight.max_per_shard = 1;
    let f = fixture(1, 1, cfg, 16);
    f.writer.set_default(Outcome::Hang);

    let (ack, ack_rx) = AckRef::test_pair();
    // A: takes the only permit and hangs.
    f.queues.try_send(0, chunk(4, 8, &ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    // B: seals with no permit free, so it parks in `waiting` — which is what
    // gates intake off from here on.
    f.queues.try_send(0, chunk(4, 8, &ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    // C and D therefore sit unread in the queue across the whole drain.
    f.queues.try_send(0, chunk(4, 8, &ack)).unwrap();
    f.budget.add(32);
    f.queues.try_send(0, chunk(4, 8, &ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(ack);
    drop(f.queues);

    let report = tokio::time::timeout(
        Duration::from_secs(120),
        f.pool.drain(Duration::from_millis(200)),
    )
    .await
    .expect("drain must return");

    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 4
        },
        "every chunk handed over must be accounted, including the two the \
         gated intake never read"
    );
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(f.budget.usage(), 0);
}

/// The routine reachability of #83, with no hung write anywhere: the default
/// `retry.max_attempts` is 0 (unbounded), so against a sink that is merely
/// *down* every write task retries forever and never releases its permit.
/// A shutdown during a sink outage — the case the guide names explicitly —
/// therefore hit the same wedge, on the default configuration.
#[tokio::test(start_paused = true)]
async fn a_down_sink_retrying_forever_does_not_wedge_dispatch_at_shutdown() {
    let mut cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: 32,
            linger: Duration::from_secs(3600),
        },
        // Retry policy left at its default: unbounded attempts.
        ..SinkPoolConfig::default()
    };
    cfg.inflight.max_per_shard = 1;
    assert_eq!(
        cfg.retry.max_attempts, 0,
        "the default is what is under test"
    );
    let f = fixture(1, 1, cfg, 16);
    f.writer
        .set_default(Outcome::Fail(ErrorClass::Retryable, Duration::ZERO));

    let (ack, ack_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(4, 8, &ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    f.queues.try_send(0, chunk(4, 8, &ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(ack);
    drop(f.queues);

    let report = tokio::time::timeout(
        Duration::from_secs(120),
        f.pool.drain(Duration::from_millis(200)),
    )
    .await
    .expect("drain must honour its deadline against an endlessly retrying sink");

    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 2
        }
    );
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(f.budget.usage(), 0);
}

/// The third way into `dispatch`: the linger timer rather than a size
/// threshold. Same wedge, same fix — the sealed batch parks instead of
/// blocking.
#[tokio::test(start_paused = true)]
async fn a_held_permit_does_not_wedge_the_linger_dispatch() {
    let mut cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: u64::MAX,
            max_bytes: 32,
            linger: Duration::from_millis(50),
        },
        ..SinkPoolConfig::default()
    };
    cfg.inflight.max_per_shard = 1;
    let f = fixture(1, 1, cfg, 16);
    f.writer.set_default(Outcome::Hang);

    let (a_ack, a_rx) = AckRef::test_pair();
    let (b_ack, b_rx) = AckRef::test_pair();
    // A: seals on max_bytes, takes the only permit, hangs.
    f.queues.try_send(0, chunk(4, 8, &a_ack)).unwrap();
    f.budget.add(32);
    tokio::time::sleep(Duration::from_millis(10)).await;
    // B: stays under max_bytes, so only the linger timer can seal it.
    f.queues.try_send(0, chunk(1, 8, &b_ack)).unwrap();
    f.budget.add(8);
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(a_ack);
    drop(b_ack);
    drop(f.queues);

    let report = tokio::time::timeout(
        Duration::from_secs(120),
        f.pool.drain(Duration::from_millis(200)),
    )
    .await
    .expect("a linger-sealed batch must not block the worker on the semaphore");

    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 2
        }
    );
    assert_eq!(a_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(b_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(f.budget.usage(), 0);
}

/// The general property the three tests above are instances of: whatever the
/// sink is doing and however the in-flight window is saturated, `drain`
/// returns and every reservation is released. Asserted over a matrix rather
/// than a single scenario so the next unbounded await on this path is caught
/// as a class, not left for the next reviewer to notice.
#[tokio::test(start_paused = true)]
async fn drain_returns_under_any_permit_pressure() {
    let deadline = Duration::from_millis(200);
    for permits in [1usize, 2] {
        for shards in [1usize, 2] {
            for outcome in [
                Outcome::Hang,
                Outcome::Fail(ErrorClass::Retryable, Duration::ZERO),
                Outcome::Write(Duration::from_secs(60)),
            ] {
                let mut cfg = SinkPoolConfig {
                    batch: BatchConfig {
                        max_rows: u64::MAX,
                        max_bytes: 32,
                        linger: Duration::from_secs(3600),
                    },
                    ..SinkPoolConfig::default()
                };
                cfg.inflight.max_per_shard = permits;
                let f = fixture(shards, 1, cfg, 64);
                f.writer.set_default(outcome.clone());

                let (ack, _rx) = AckRef::test_pair();
                for s in 0..shards {
                    // Saturate every permit, then one more sealing batch with
                    // nowhere to go, then a partial for the drain force-seal.
                    for _ in 0..=permits {
                        f.queues.try_send(s, chunk(4, 8, &ack)).unwrap();
                        f.budget.add(32);
                    }
                    f.queues.try_send(s, chunk(1, 8, &ack)).unwrap();
                    f.budget.add(8);
                }
                drop(ack);
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(f.queues);

                let case = format!("permits={permits} shards={shards} outcome={outcome:?}");
                tokio::time::timeout(deadline + Duration::from_secs(60), f.pool.drain(deadline))
                    .await
                    .unwrap_or_else(|_| panic!("drain never returned: {case}"));
                assert_eq!(f.budget.usage(), 0, "reservations leaked: {case}");
            }
        }
    }
}

/// A write that blocks its runtime thread cannot be aborted — `abort` only
/// lands at a yield point. The drain sweep bounds that wait rather than
/// inheriting it, so the worker still runs its abandon accounting and still
/// returns its own report instead of being force-aborted by the pool.
///
/// Real time and a multi-threaded runtime on purpose: `std::thread::sleep`
/// does not participate in paused time, and the worker needs a thread of its
/// own while the write task holds one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unabortable_write_does_not_hold_the_worker_past_its_grace() {
    let mut cfg = SinkPoolConfig {
        batch: BatchConfig {
            max_rows: 1,
            max_bytes: u64::MAX,
            linger: Duration::from_secs(3600),
        },
        ..SinkPoolConfig::default()
    };
    cfg.inflight.max_per_shard = 1;
    let f = fixture(1, 1, cfg, 16);
    // Far longer than the sweep's abort grace, so the test fails loudly if the
    // sweep ever waits this out instead of bounding it.
    f.writer
        .set_default(Outcome::BlockThread(Duration::from_secs(30)));

    let (ack, ack_rx) = AckRef::test_pair();
    f.queues.try_send(0, chunk(1, 8, &ack)).unwrap();
    f.budget.add(8);
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(ack);
    drop(f.queues);

    let started = std::time::Instant::now();
    let report = tokio::time::timeout(
        Duration::from_secs(20),
        f.pool.drain(Duration::from_millis(100)),
    )
    .await
    .expect("the sweep must bound its wait for aborts it cannot land");
    let elapsed = started.elapsed();
    // The point is proved; don't pay the rest of the block at teardown.
    f.writer.release_blocked();

    assert_eq!(
        report,
        DrainReport {
            flushed: 0,
            abandoned: 1
        },
        "the unabortable batch is abandoned by the worker itself"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "drain took {elapsed:?}; it should return about one abort grace after \
         the deadline, not wait out the blocking write"
    );
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
    assert_eq!(f.budget.usage(), 0);
}

/// The pool's backstop under the cooperative deadline. A real worker cannot
/// reach this any more — that is the whole point of the restructured intake
/// loop — so the wedge is injected directly: `drain` must still return, and
/// must say so on `etl_sink_drain_overrun_total` rather than only in a log.
#[test]
fn a_worker_that_ignores_the_deadline_is_force_aborted() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        rt.block_on(async {
            let labels = ComponentLabels::new("test", next_component(), "mock");
            let metrics = vec![Arc::new(SinkShardMetrics::new(
                &labels,
                0,
                &["r0".into()],
                E2eBasis::Ingest,
            ))];
            let (drain_tx, _drain_rx) = tokio::sync::watch::channel(None);
            let wedged = tokio::spawn(async {
                std::future::pending::<()>().await;
                unreachable!()
            });
            let pool = SinkPool::from_workers(MockWriter::new(), vec![wedged], drain_tx, metrics);

            let report = tokio::time::timeout(
                Duration::from_secs(600),
                pool.drain(Duration::from_millis(200)),
            )
            .await
            .expect("the backstop must make drain return regardless");
            assert_eq!(
                report,
                DrainReport::default(),
                "a force-aborted worker contributes no counts"
            );
        });
    });

    let rendered = handle.render();
    assert_eq!(
        metric_value(&rendered, "etl_sink_drain_overrun_total", "shard=\"0\""),
        1.0
    );
}

#[test]
fn random_streams_conserve_rows_and_resolve_every_ack() {
    use proptest::prelude::*;

    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: 16,
        ..Default::default()
    });
    runner
        .run(
            &(
                proptest::collection::vec((1u32..40, 1usize..64), 1..60),
                2u64..80,
            ),
            |(chunks, max_rows)| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let cfg = SinkPoolConfig {
                        batch: BatchConfig {
                            max_rows,
                            max_bytes: u64::MAX,
                            linger: Duration::from_millis(5),
                        },
                        ..SinkPoolConfig::default()
                    };
                    let f = fixture(1, 2, cfg, chunks.len() + 1);
                    let mut receivers = Vec::new();
                    let mut sent_rows: u64 = 0;
                    for (rows, bpr) in &chunks {
                        let (ack, rx) = AckRef::test_pair();
                        sent_rows += u64::from(*rows);
                        f.budget.add(*bpr * *rows as usize);
                        f.queues.try_send(0, chunk(*rows, *bpr, &ack)).unwrap();
                        receivers.push(rx);
                    }
                    drop(f.queues);
                    let report = f.pool.drain(Duration::from_secs(10)).await;

                    let written: u64 = f.writer.calls().iter().map(|c| c.rows).sum();
                    prop_assert_eq!(written, sent_rows, "row conservation");
                    prop_assert!(report.abandoned == 0);
                    for rx in receivers {
                        let msg: AckMsg = rx.try_recv().expect("every ack resolves");
                        prop_assert_eq!(msg.status, AckStatus::Delivered);
                        prop_assert!(rx.try_recv().is_err(), "exactly once");
                    }
                    prop_assert_eq!(f.budget.usage(), 0);
                    Ok(())
                })
            },
        )
        .unwrap();
}

/// Teardown without drain (a Failed exit dropping the I/O runtime) must
/// never resolve un-written data as delivered: pending batches and queued
/// chunks hold their acknowledgements in fail-on-drop sets.
#[test]
fn runtime_teardown_without_drain_fails_unwritten_acks() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (in_flight_ack, in_flight_rx) = AckRef::test_pair();
    let (queued_ack, queued_rx) = AckRef::test_pair();

    let fx = rt.block_on(async {
        let fx = fixture(1, 1, small_batches(), 8);
        // First chunk seals immediately (max_rows = 1) and its write hangs
        // forever: the batch stays pending in the worker's ledger.
        fx.writer.set_default(Outcome::Hang);
        fx.queues
            .try_send(0, chunk(1, 8, &in_flight_ack))
            .expect("send in-flight");
        // Give the worker a moment to seal and dispatch.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Second chunk sits in the shard queue, never picked up (the
        // worker is awaiting the semaphore/inflight join).
        fx.queues
            .try_send(0, chunk(1, 8, &queued_ack))
            .expect("send queued");
        fx
    });
    drop(in_flight_ack);
    drop(queued_ack);

    // Tear the runtime down without draining the pool: worker futures and
    // the queued chunk are dropped wherever they stand.
    rt.shutdown_background();
    drop(fx);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut statuses = Vec::new();
    for rx in [in_flight_rx, queued_rx] {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let msg: AckMsg = rx
            .recv_timeout(remaining)
            .expect("ack resolves at teardown");
        statuses.push(msg.status);
    }
    assert_eq!(
        statuses,
        vec![AckStatus::Failed, AckStatus::Failed],
        "unwritten data must fail, never deliver, at teardown"
    );
}

/// Metric-level row conservation: `etl_sink_records_total` counts exactly
/// the rows of durably written batches — no double-counting across chunk
/// merging, sealing, or retries. Uses a current-thread runtime so the
/// thread-local recorder sees the worker's increments.
#[test]
fn sink_records_metric_matches_rows_written_exactly() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let cfg = SinkPoolConfig {
                batch: BatchConfig {
                    max_rows: 5,
                    max_bytes: u64::MAX,
                    linger: Duration::from_secs(3600),
                },
                ..SinkPoolConfig::default()
            };
            let fx = fixture(1, 1, cfg, 64);
            let (ack, _rx) = AckRef::test_pair();
            // 12 rows across chunks of 3. Sealing is chunk-granular: the
            // accumulator crosses max_rows=5 at the second and fourth
            // chunk, so two 6-row batches flush and nothing remains for
            // the drain.
            for _ in 0..4 {
                fx.queues.try_send(0, chunk(3, 4, &ack)).expect("send");
            }
            drop(ack);
            drop(fx.queues);
            let report = fx.pool.drain(Duration::from_secs(5)).await;
            assert_eq!(report.flushed, 2);
            assert_eq!(report.abandoned, 0);
            let written: u64 = fx.writer.calls().iter().map(|c| c.rows).sum();
            assert_eq!(written, 12, "writer saw every row exactly once");
        });
    });
    let rendered = handle.render();
    let value = metric_value(&rendered, "etl_sink_records_total", "shard=\"0\"");
    assert!(
        (value - 12.0).abs() < f64::EPSILON,
        "metric must equal rows written exactly, got {value}"
    );
}

/// Pull one rendered Prometheus value by metric name plus a distinguishing
/// label fragment. The value is the last space-separated token of the line
/// (this exporter renders no trailing timestamp).
///
/// This is the **first** matching line, not a sum across series. A family
/// split by a label the caller does not pin (say `shard`, on a multi-shard
/// fixture) returns whichever series the exporter happened to emit first, so
/// pin every label that varies — otherwise the assertion silently covers one
/// series out of several and passes for the wrong reason.
fn metric_value(rendered: &str, name: &str, label: &str) -> f64 {
    let line = rendered
        .lines()
        .find(|l| l.starts_with(name) && l.contains(label))
        .unwrap_or_else(|| panic!("`{name}{{{label}}}` not rendered:\n{rendered}"));
    line.rsplit(' ').next().unwrap().parse().expect("value")
}

/// End-to-end wiring of the whole-shard health signal: with every replica of
/// a shard failing, `etl_sink_shard_healthy` drops to 0 once every breaker is
/// open. Drives the real worker/breaker path (not the breaker unit tests)
/// through the Prometheus recorder. Per-replica error *attribution* is pinned
/// by `replica_errors_attribute_failures_asymmetrically`, whose counts differ
/// per replica — this scenario is symmetric (one failure each) and could not
/// tell swapped indices apart.
#[test]
fn shard_healthy_drops_to_zero_when_every_breaker_opens() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut cfg = small_batches();
            // One failure quarantines a replica; keep it open (no probe during
            // the test) and cap attempts so the batch abandons after both
            // replicas have failed exactly once.
            cfg.breaker.failure_threshold = 1;
            cfg.breaker.open_for = Duration::from_secs(3600);
            cfg.retry.max_attempts = 2;
            let fx = fixture(1, 2, cfg, 16);
            fx.writer
                .set_default(Outcome::Fail(ErrorClass::Retryable, Duration::ZERO));

            let (ack, ack_rx) = AckRef::test_pair();
            fx.queues.try_send(0, chunk(1, 1, &ack)).expect("send");
            drop(ack);
            drop(fx.queues);
            let report = fx.pool.drain(Duration::from_secs(30)).await;

            assert_eq!(
                report,
                DrainReport {
                    flushed: 0,
                    abandoned: 1
                }
            );
            // One attempt per replica before attempts are exhausted.
            let calls = fx.writer.calls();
            assert_eq!(calls.len(), 2, "one attempt on each replica");
            assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
        });
    });

    let rendered = handle.render();
    // Both breakers open → no circuit-closed replica → shard unhealthy.
    assert_eq!(
        metric_value(&rendered, "etl_sink_shard_healthy", "shard=\"0\""),
        0.0,
        "every replica quarantined ⇒ shard_healthy == 0"
    );
}

/// Per-replica error attribution, falsifiably: with `failure_threshold: 2`
/// and three attempts, round-robin rotation lands two failures on replica 0
/// and one on replica 1, so any index miswiring (swap, off-by-one) breaks
/// the 2-vs-1 assertion below. Replica 1 stays circuit-closed, which also
/// pins `etl_sink_shard_healthy` staying 1 while a healthy replica remains.
#[test]
fn replica_errors_attribute_failures_asymmetrically() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut cfg = small_batches();
            // Two failures quarantine a replica; keep it open (no probe
            // during the test). Three attempts: r0, r1, r0 — the third
            // opens replica 0's breaker and exhausts the attempt budget.
            cfg.breaker.failure_threshold = 2;
            cfg.breaker.open_for = Duration::from_secs(3600);
            cfg.retry.max_attempts = 3;
            let fx = fixture(1, 2, cfg, 16);
            fx.writer
                .set_default(Outcome::Fail(ErrorClass::Retryable, Duration::ZERO));

            let (ack, ack_rx) = AckRef::test_pair();
            fx.queues.try_send(0, chunk(1, 1, &ack)).expect("send");
            drop(ack);
            drop(fx.queues);
            let report = fx.pool.drain(Duration::from_secs(30)).await;

            assert_eq!(
                report,
                DrainReport {
                    flushed: 0,
                    abandoned: 1
                }
            );
            let calls = fx.writer.calls();
            assert_eq!(
                (calls[0].replica, calls[1].replica, calls[2].replica),
                (0, 1, 0),
                "round-robin rotation: two attempts on r0, one on r1"
            );
            assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
        });
    });

    let rendered = handle.render();
    assert_eq!(
        metric_value(&rendered, "etl_sink_replica_errors_total", "replica=\"r0\""),
        2.0,
        "replica 0 took two failures"
    );
    assert_eq!(
        metric_value(&rendered, "etl_sink_replica_errors_total", "replica=\"r1\""),
        1.0,
        "replica 1 took one failure"
    );
    // Replica 1 is below threshold and still circuit-closed.
    assert_eq!(
        metric_value(&rendered, "etl_sink_shard_healthy", "shard=\"0\""),
        1.0,
        "a circuit-closed replica remains ⇒ shard_healthy == 1"
    );
}

/// The scenario behind #79, reproduced: two batches, one sink speed, two very
/// different flush durations. `etl_sink_flush_duration_seconds` spans seal to
/// settle, so the second batch — which sealed at t=0 and could not start until
/// the first released the shard's only in-flight permit at t=2 — reads twice
/// what the first did against an identically fast writer. The split families
/// are what tell the two apart: write duration is flat at 2s across both, and
/// the whole difference lands in the permit-wait histogram.
///
/// Paused time makes every figure exact rather than approximate: the only
/// timers are the mock's own sleeps, so the runtime advances to them and
/// nothing else.
#[test]
fn permit_wait_separates_queueing_from_a_slow_write() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut cfg = small_batches();
            // One permit: the second batch provably queues behind the first.
            cfg.inflight.max_per_shard = 1;
            let fx = fixture(1, 1, cfg, 16);
            fx.writer
                .set_default(Outcome::Write(Duration::from_secs(2)));

            let (ack, _rx) = AckRef::test_pair();
            // max_rows = 1, so each chunk seals its own batch on arrival.
            fx.queues.try_send(0, chunk(1, 1, &ack)).expect("send");
            fx.queues.try_send(0, chunk(1, 1, &ack)).expect("send");
            drop(ack);
            drop(fx.queues);
            let report = fx.pool.drain(Duration::from_secs(600)).await;
            assert_eq!(
                report,
                DrainReport {
                    flushed: 2,
                    abandoned: 0
                }
            );
        });
    });

    let rendered = handle.render();
    let value = |name: &str, label: &str| metric_value(&rendered, name, label);

    // Both writes took exactly 2s, and the sink never failed one.
    assert_eq!(
        value("etl_sink_write_duration_seconds_count", "outcome=\"ok\""),
        2.0
    );
    assert_eq!(
        value("etl_sink_write_duration_seconds_sum", "outcome=\"ok\""),
        4.0,
        "the sink was equally fast for both batches"
    );

    // The first batch got the permit for free; the second waited out the
    // first's whole write.
    assert_eq!(
        value("etl_sink_permit_wait_duration_seconds_count", "shard=\"0\""),
        2.0,
        "every sealed batch is observed, including the one that waited 0"
    );
    assert_eq!(
        value("etl_sink_permit_wait_duration_seconds_sum", "shard=\"0\""),
        2.0
    );

    // And the family the issue is about: 2s + 4s, against a writer that never
    // varied. Reading this as sink latency is the misdiagnosis.
    assert_eq!(
        value("etl_sink_flush_duration_seconds_sum", "shard=\"0\""),
        6.0,
        "seal-to-settle carries the queueing the write histogram excludes"
    );
}

/// The drain's force-sealed partial batch lands in the permit-wait histogram
/// like any other, so a shutdown that queues behind a slow write is visible to
/// someone sizing `drain_timeout`. This drives the drain loop's *late*
/// acquisition: the force-seal cannot block on the semaphore (that would
/// deadlock shutdown), so it parks in `waiting` with its stamp and is observed
/// only once a permit frees.
///
/// Two rows seal batch A on arrival and it takes the shard's only permit for
/// 2s; the third row stays partial in the accumulator and is force-sealed at
/// drain, where it must wait out A's whole write.
#[test]
fn drain_force_seal_observes_the_permit_wait_it_queued_for() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut cfg = small_batches();
            cfg.batch.max_rows = 2;
            cfg.inflight.max_per_shard = 1;
            let fx = fixture(1, 1, cfg, 16);
            fx.writer
                .set_default(Outcome::Write(Duration::from_secs(2)));

            let (ack, _rx) = AckRef::test_pair();
            fx.queues.try_send(0, chunk(2, 1, &ack)).expect("send");
            fx.queues.try_send(0, chunk(1, 1, &ack)).expect("send");
            drop(ack);
            drop(fx.queues);
            let report = fx.pool.drain(Duration::from_secs(600)).await;
            assert_eq!(
                report,
                DrainReport {
                    flushed: 2,
                    abandoned: 0
                }
            );
        });
    });

    let rendered = handle.render();
    let value = |name: &str, label: &str| metric_value(&rendered, name, label);

    assert_eq!(
        value("etl_sink_permit_wait_duration_seconds_count", "shard=\"0\""),
        2.0,
        "the dispatched batch and the drain's force-sealed one are both observed"
    );
    assert_eq!(
        value("etl_sink_permit_wait_duration_seconds_sum", "shard=\"0\""),
        2.0,
        "the force-sealed batch waited out the in-flight write; the first waited 0"
    );
    // The sink was equally fast for both, so the drain's cost was queueing.
    assert_eq!(
        value("etl_sink_write_duration_seconds_sum", "outcome=\"ok\""),
        4.0
    );
}

/// The other drain branch: a partial batch force-sealed with the permit free
/// is observed too, at ~0. Registering an observation only when the drain
/// happens to queue would make the family read as absent exactly when an
/// operator wants to confirm a clean shutdown did not queue at all.
#[test]
fn drain_force_seal_observes_a_free_permit_as_no_wait() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut cfg = small_batches();
            // Nothing reaches max_rows, so the only seal is the drain's.
            cfg.batch.max_rows = 2;
            let fx = fixture(1, 1, cfg, 16);
            fx.writer
                .set_default(Outcome::Write(Duration::from_secs(2)));

            let (ack, _rx) = AckRef::test_pair();
            fx.queues.try_send(0, chunk(1, 1, &ack)).expect("send");
            drop(ack);
            drop(fx.queues);
            let report = fx.pool.drain(Duration::from_secs(600)).await;
            assert_eq!(
                report,
                DrainReport {
                    flushed: 1,
                    abandoned: 0
                }
            );
        });
    });

    let rendered = handle.render();
    let value = |name: &str, label: &str| metric_value(&rendered, name, label);

    assert_eq!(
        value("etl_sink_permit_wait_duration_seconds_count", "shard=\"0\""),
        1.0,
        "the force-sealed batch is observed even though it never waited"
    );
    assert_eq!(
        value("etl_sink_permit_wait_duration_seconds_sum", "shard=\"0\""),
        0.0
    );
    assert_eq!(
        value("etl_sink_write_duration_seconds_sum", "outcome=\"ok\""),
        2.0,
        "the write itself is unaffected by the drain path"
    );
}

/// A failed attempt and a successful one are both attempts, and they are
/// separated by the `outcome` label rather than averaged together — a fast
/// reject would otherwise flatter the distribution and a slow timeout wreck
/// it. Retries are observed like any other attempt, so the counts here are
/// per attempt, not per batch.
#[test]
fn write_duration_separates_failed_attempts_from_successful_ones() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut cfg = small_batches();
            // Keep the breaker closed, so the second attempt follows the
            // ordinary retry backoff and not an all-replicas-quarantined
            // probe wait (which is outside both new families).
            cfg.breaker.failure_threshold = u32::MAX;
            let fx = fixture(1, 1, cfg, 16);
            // A slow failure, then a fast success.
            fx.writer.script(
                0,
                0,
                [
                    Outcome::Fail(ErrorClass::Retryable, Duration::from_secs(1)),
                    Outcome::Write(Duration::from_millis(100)),
                ],
            );

            let (ack, _rx) = AckRef::test_pair();
            fx.queues.try_send(0, chunk(1, 1, &ack)).expect("send");
            drop(ack);
            drop(fx.queues);
            let report = fx.pool.drain(Duration::from_secs(600)).await;
            assert_eq!(
                report,
                DrainReport {
                    flushed: 1,
                    abandoned: 0
                }
            );
        });
    });

    let rendered = handle.render();
    let value = |name: &str, label: &str| metric_value(&rendered, name, label);

    assert_eq!(
        value("etl_sink_write_duration_seconds_count", "outcome=\"error\""),
        1.0
    );
    assert_eq!(
        value("etl_sink_write_duration_seconds_sum", "outcome=\"error\""),
        1.0,
        "the attempt that failed took a second to do it"
    );
    assert_eq!(
        value("etl_sink_write_duration_seconds_count", "outcome=\"ok\""),
        1.0
    );
    assert!(
        (value("etl_sink_write_duration_seconds_sum", "outcome=\"ok\"") - 0.1).abs() < 1e-6,
        "the successful attempt is 100ms, not the 1.1s the batch spent flushing"
    );
    // One batch, one flush observation — spanning both attempts and the 1ms
    // backoff between them.
    assert_eq!(
        value("etl_sink_flush_duration_seconds_count", "shard=\"0\""),
        1.0
    );
    assert!(
        (value("etl_sink_flush_duration_seconds_sum", "shard=\"0\"") - 1.101).abs() < 1e-6,
        "seal-to-settle is both attempts plus the backoff between them"
    );
}

/// End-to-end wiring of `etl_sink_retry_backoff_seconds` through the real
/// write loop: a shard parked between attempts publishes the step it is
/// sleeping, and stops publishing it when the sleep ends — here by the drain
/// deadline *aborting* the task mid-sleep, the one exit that runs none of the
/// write loop's own code. Registering the family proves nothing; a scrape
/// taken while the shard is actually asleep does.
///
/// Paused time makes the observation deterministic: it only advances when
/// nothing is ready, so the first attempt has failed and parked in its 60s
/// backoff before the 1s observation sleep below returns.
#[test]
fn retry_backoff_gauge_reads_the_step_a_sleeping_shard_is_serving() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut cfg = small_batches();
            // A flat, patient policy: every attempt sleeps exactly 60s, and
            // nothing ever abandons the batch — the shape the startup warning
            // fires on, and the one this gauge exists to make visible.
            cfg.retry.initial = Duration::from_secs(60);
            cfg.retry.max = Duration::from_secs(60);
            cfg.retry.jitter = 0.0;
            cfg.retry.max_attempts = 0;
            // Keep the breaker closed throughout, so the task is provably in
            // the retry sleep and not in the all-replicas-quarantined wait
            // (which this gauge deliberately does not cover).
            cfg.breaker.failure_threshold = u32::MAX;
            let fx = fixture(1, 1, cfg, 16);
            fx.writer
                .set_default(Outcome::Fail(ErrorClass::Retryable, Duration::ZERO));

            let (ack, ack_rx) = AckRef::test_pair();
            fx.queues.try_send(0, chunk(1, 1, &ack)).expect("send");
            tokio::time::sleep(Duration::from_secs(1)).await;

            assert_eq!(
                fx.writer.calls().len(),
                1,
                "one attempt made, and the shard is asleep before the next"
            );
            assert_eq!(
                metric_value(
                    &handle.render(),
                    "etl_sink_retry_backoff_seconds",
                    "shard=\"0\""
                ),
                60.0,
                "a shard sleeping between attempts must say how long for"
            );

            drop(ack);
            drop(fx.queues);
            // A zero deadline: the sweep aborts the write task where it is
            // parked, inside the sleep.
            let report = fx.pool.drain(Duration::ZERO).await;
            assert_eq!(
                report,
                DrainReport {
                    flushed: 0,
                    abandoned: 1
                }
            );
            assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
        });
    });

    assert_eq!(
        metric_value(
            &handle.render(),
            "etl_sink_retry_backoff_seconds",
            "shard=\"0\""
        ),
        0.0,
        "an aborted sleep must not strand the gauge at its last step"
    );
}
