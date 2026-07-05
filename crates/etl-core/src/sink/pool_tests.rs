//! Behavioural tests for the sink worker pool, driven through a scriptable
//! mock writer.

use super::*;
use crate::backpressure::InflightBudget;
use crate::checkpoint::{AckMsg, AckRef, AckStatus};
use crate::error::ErrorClass;
use crate::metrics::{ComponentLabels, SinkShardMetrics};
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
}

impl MockWriter {
    fn new() -> Arc<Self> {
        Arc::new(MockWriter {
            script: Mutex::new(HashMap::new()),
            global_default: Mutex::new(Outcome::Write(Duration::ZERO)),
            log: Mutex::new(Vec::new()),
            concurrent: AtomicUsize::new(0),
            max_concurrent: AtomicUsize::new(0),
        })
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

fn fixture(shards: usize, replicas: usize, cfg: SinkPoolConfig, queue_cap: usize) -> Fixture {
    let writer = MockWriter::new();
    let (queues, receivers) = shard_queues(shards, queue_cap);
    let budget = Arc::new(InflightBudget::new());
    let labels = ComponentLabels::new("test", "sink", "mock");
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
        .map(|s| SinkShardMetrics::new(&labels, u32::try_from(s).unwrap(), &replica_names))
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
        frame: Bytes::from(vec![0u8; bytes_per_row * rows as usize]),
        rows,
        acks: vec![ack.clone()],
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
