//! Runtime integration tests over scripted fakes: a controllable Source
//! with real `AckIssuer` plumbing, a counting chain, and a mock sink
//! bundle. The Checkpointer, DrainBarrier, and WatermarkController in the
//! loop are the real implementations.

use super::*;
use crate::checkpoint::AckIssuer;
use crate::config::{
    BackpressureSection, CheckpointSection, ComponentConfig, MetricsExporter, MetricsSection,
    PinningMode, PipelineConfig, PipelineSection,
};
use crate::error::{ErrorClass, FatalError, SourceError};
use crate::ops::{BlockReason, PushOutcome, RunnableChain};
use crate::pipeline::runtime::{PipelineRuntime, RuntimeOptions};
use crate::record::{PartitionId, RawPayload};
use crate::sink::shard_queues;
use crate::source::{
    DrainBarrier, LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------- fakes --

#[derive(Default)]
struct SourceLog {
    committed: BTreeMap<PartitionId, i64>,
    pauses: Vec<Vec<LaneId>>,
    resumes: Vec<Vec<LaneId>>,
    flush_commits: usize,
    opened: bool,
    /// Interleaved ordering log shared with the chain fake.
    log: Vec<String>,
}

enum Script {
    Assign(Vec<LaneSpec>),
    Revoke(Vec<LaneId>),
}

struct LaneSpec {
    id: LaneId,
    partition: PartitionId,
    batches: Vec<Vec<(i64, Vec<u8>)>>,
}

type SharedLog = Arc<Mutex<SourceLog>>;
type SharedScript = Arc<Mutex<VecDeque<Script>>>;

struct FakeSource {
    shared: SharedLog,
    script: SharedScript,
    issuer: Option<AckIssuer>,
}

impl FakeSource {
    fn new() -> (Self, SharedLog, SharedScript) {
        let shared = Arc::new(Mutex::new(SourceLog::default()));
        let script = Arc::new(Mutex::new(VecDeque::new()));
        (
            FakeSource {
                shared: Arc::clone(&shared),
                script: Arc::clone(&script),
                issuer: None,
            },
            shared,
            script,
        )
    }
}

impl Source for FakeSource {
    type Lane = FakeLane;

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        self.shared.lock().unwrap().opened = true;
        self.issuer = Some(ctx.issuer);
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<FakeLane>, SourceError> {
        let next = self.script.lock().unwrap().pop_front();
        match next {
            Some(Script::Assign(specs)) => {
                let issuer = self.issuer.as_ref().expect("open before assign");
                let lanes = specs
                    .into_iter()
                    .map(|s| FakeLane {
                        id: s.id,
                        partition: s.partition,
                        batches: s.batches.into(),
                        current: Vec::new(),
                        issuer: issuer.clone(),
                    })
                    .collect();
                Ok(SourceEvent::LanesAssigned(lanes))
            }
            Some(Script::Revoke(ids)) => {
                let mut log = self.shared.lock().unwrap();
                log.log.push("revoke-delivered".into());
                Ok(SourceEvent::LanesRevoked {
                    barrier: DrainBarrier::new(ids.len()),
                    lanes: ids,
                })
            }
            None => {
                std::thread::sleep(timeout.min(Duration::from_millis(5)));
                Ok(SourceEvent::Idle)
            }
        }
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        let mut log = self.shared.lock().unwrap();
        for &(p, o) in watermarks {
            let slot = log.committed.entry(p).or_insert(o);
            *slot = (*slot).max(o);
        }
        log.log.push("commit".into());
        Ok(())
    }

    fn flush_commits(&mut self) -> Result<(), SourceError> {
        let mut log = self.shared.lock().unwrap();
        log.flush_commits += 1;
        log.log.push("flush_commits".into());
        Ok(())
    }

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        self.shared.lock().unwrap().pauses.push(lanes.to_vec());
        Ok(())
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        self.shared.lock().unwrap().resumes.push(lanes.to_vec());
        Ok(())
    }
}

struct FakeLane {
    id: LaneId,
    partition: PartitionId,
    batches: VecDeque<Vec<(i64, Vec<u8>)>>,
    current: Vec<(i64, Vec<u8>)>,
    issuer: AckIssuer,
}

impl SourceLane for FakeLane {
    type Batch<'a> = FakeBatch<'a>;

    fn id(&self) -> LaneId {
        self.id
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        _max_records: usize,
        timeout: Duration,
    ) -> Result<Option<FakeBatch<'_>>, SourceError> {
        match self.batches.pop_front() {
            Some(batch) => {
                self.current = batch;
                let last = self.current.last().expect("non-empty batch").0;
                let ack = self.issuer.issue(self.partition, last);
                Ok(Some(FakeBatch {
                    payloads: &self.current,
                    idx: 0,
                    partition: self.partition,
                    ack,
                }))
            }
            None => {
                std::thread::sleep(timeout.min(Duration::from_millis(2)));
                Ok(None)
            }
        }
    }
}

struct FakeBatch<'a> {
    payloads: &'a [(i64, Vec<u8>)],
    idx: usize,
    partition: PartitionId,
    ack: crate::checkpoint::AckRef,
}

impl<'a> PayloadBatch<'a> for FakeBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let (offset, bytes) = self.payloads.get(self.idx)?;
        self.idx += 1;
        Some(RawPayload {
            bytes,
            key: None,
            partition: self.partition,
            offset: *offset,
            timestamp_ms: *offset,
        })
    }

    fn ack(&self) -> &crate::checkpoint::AckRef {
        &self.ack
    }
}

#[derive(Default)]
struct ChainShared {
    consumed: AtomicUsize,
    flushes: AtomicUsize,
}

enum ChainMode {
    Ok,
    BlockOnce(AtomicBool),
    BlockForever,
    FatalAtBatch(usize),
    PanicAtBatch(usize),
    /// Fail the ack of the n-th batch (1-based) after consuming it, stalling
    /// that partition's watermark permanently — as a fatal sink write does.
    FailAckAtBatch(usize),
    /// Clone-and-stash every batch's ack so watermarks never advance and the
    /// checkpointer's pending count climbs. Setting `release` stops stashing;
    /// the test then drops the stash to resolve the batches Delivered.
    HoldAcks {
        held: Arc<Mutex<Vec<crate::checkpoint::AckRef>>>,
        release: Arc<AtomicBool>,
    },
}

struct FakeChain {
    shared: Arc<ChainShared>,
    log: Arc<Mutex<SourceLog>>,
    mode: ChainMode,
    batches_seen: usize,
}

impl RunnableChain for FakeChain {
    fn push_batch<'buf>(&mut self, batch: &mut dyn PayloadBatch<'buf>, from: usize) -> PushOutcome {
        if from == 0 {
            self.batches_seen += 1;
        }
        match &self.mode {
            ChainMode::BlockOnce(done) if !done.swap(true, Ordering::Relaxed) => {
                return PushOutcome::Blocked {
                    resume_at: from,
                    reason: BlockReason::Capacity,
                };
            }
            ChainMode::BlockForever => {
                return PushOutcome::Blocked {
                    resume_at: from,
                    reason: BlockReason::Capacity,
                };
            }
            ChainMode::FatalAtBatch(n) if self.batches_seen == *n => {
                return PushOutcome::Fatal(FatalError {
                    component: "fake-chain".into(),
                    reason: "scripted fatal".into(),
                });
            }
            ChainMode::PanicAtBatch(n) if self.batches_seen == *n => {
                panic!("scripted panic in operator chain");
            }
            _ => {}
        }
        while let Some(_p) = batch.next_payload() {
            self.shared.consumed.fetch_add(1, Ordering::Relaxed);
        }
        match &self.mode {
            ChainMode::FailAckAtBatch(n) if self.batches_seen == *n => batch.ack().fail(),
            ChainMode::HoldAcks { held, release } if !release.load(Ordering::Relaxed) => {
                held.lock().unwrap().push(batch.ack().clone());
            }
            _ => {}
        }
        PushOutcome::Done
    }

    fn flush(&mut self) -> PushOutcome {
        self.shared.flushes.fetch_add(1, Ordering::Relaxed);
        self.log.lock().unwrap().log.push("flush".into());
        PushOutcome::Done
    }
}

fn test_sink() -> (SinkRuntime, Arc<AtomicBool>) {
    let (queues, receivers) = shard_queues(1, 8);
    let drained = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&drained);
    let drain: super::SinkDrainFn = Box::new(move |_budget| {
        Box::pin(async move {
            let _receivers = receivers;
            flag.store(true, Ordering::Relaxed);
            DrainReport::default()
        })
    });
    (
        SinkRuntime {
            queues,
            drain,
            probe: None,
        },
        drained,
    )
}

fn test_config(threads: usize) -> PipelineConfig {
    PipelineConfig {
        pipeline: PipelineSection {
            name: "test".into(),
            threads: Some(threads),
            io_threads: 1,
            pinning: PinningMode::Off,
        },
        checkpoint: CheckpointSection {
            interval: Duration::from_millis(20),
            max_pending_batches: 1024,
            drain_timeout: Duration::from_secs(3),
            stalled_fail_after: Duration::from_secs(120),
        },
        backpressure: BackpressureSection {
            min_pause: Duration::from_millis(10),
            ..Default::default()
        },
        metrics: MetricsSection {
            exporter: MetricsExporter::None,
            listen: "127.0.0.1:0".parse().expect("addr"),
            ..Default::default()
        },
        source: ComponentConfig::new("fake", serde_yaml::Value::Null),
        deserializer: None,
        sink: ComponentConfig::new("fake", serde_yaml::Value::Null),
    }
}

fn test_options() -> RuntimeOptions {
    RuntimeOptions {
        handle_signals: false,
        max_records: 512,
        poll_timeout: Duration::from_millis(2),
        idle_flush: Duration::from_millis(20),
        blocked_retry: Duration::from_millis(1),
        event_poll_timeout: Duration::from_millis(5),
        version: "test".into(),
    }
}

fn batches(ranges: &[std::ops::Range<i64>]) -> Vec<Vec<(i64, Vec<u8>)>> {
    ranges
        .iter()
        .map(|r| r.clone().map(|o| (o, vec![0u8; 8])).collect())
        .collect()
}

/// Spin until `cond` holds or the timeout elapses; panic on timeout.
fn wait_for(what: &str, timeout: Duration, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

struct Harness {
    shared: Arc<Mutex<SourceLog>>,
    script: Arc<Mutex<VecDeque<Script>>>,
    chain: Arc<ChainShared>,
    drained: Arc<AtomicBool>,
    shutdown: super::runtime::ShutdownHandle,
    join: std::thread::JoinHandle<Result<ExitReport, super::runtime::StartError>>,
}

fn start(
    mode_factory: impl Fn(Arc<ChainShared>, Arc<Mutex<SourceLog>>) -> FakeChain + Send + 'static,
) -> Harness {
    start_with_config(test_config(1), mode_factory)
}

fn start_with_config(
    config: PipelineConfig,
    mode_factory: impl Fn(Arc<ChainShared>, Arc<Mutex<SourceLog>>) -> FakeChain + Send + 'static,
) -> Harness {
    let (source, shared, script) = FakeSource::new();
    let chain_shared = Arc::new(ChainShared::default());
    let (sink, drained) = test_sink();
    let budget = Arc::new(crate::backpressure::InflightBudget::new());
    let cs = Arc::clone(&chain_shared);
    let log = Arc::clone(&shared);
    let runtime = PipelineRuntime::new(
        config,
        source,
        move |_thread| {
            Box::new(mode_factory(Arc::clone(&cs), Arc::clone(&log))) as Box<dyn RunnableChain>
        },
        sink,
        budget,
    )
    .with_options(test_options());
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());
    Harness {
        shared,
        script,
        chain: chain_shared,
        drained,
        shutdown,
        join,
    }
}

fn assign_one_lane(h: &Harness, ranges: &[std::ops::Range<i64>]) {
    h.script
        .lock()
        .unwrap()
        .push_back(Script::Assign(vec![LaneSpec {
            id: LaneId(0),
            partition: PartitionId(0),
            batches: batches(ranges),
        }]));
}

// ---------------------------------------------------------------- tests --

#[test]
fn happy_path_consumes_and_commits() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20, 20..30]);
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 30
    });
    wait_for("watermark committed", Duration::from_secs(5), || {
        h.shared.lock().unwrap().committed.get(&PartitionId(0)) == Some(&30)
    });
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.final_watermarks, vec![(PartitionId(0), 30)]);
    assert!(h.drained.load(Ordering::Relaxed), "sink drain must run");
    let log = h.shared.lock().unwrap();
    assert!(log.opened);
    assert!(log.flush_commits >= 1, "shutdown must flush commits");
}

#[test]
fn revocation_drains_flushes_and_commits_in_order() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20, 20..30]);
    wait_for("first batch consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) >= 10
    });
    h.script
        .lock()
        .unwrap()
        .push_back(Script::Revoke(vec![LaneId(0)]));
    wait_for("revocation processed", Duration::from_secs(5), || {
        let log = h.shared.lock().unwrap();
        log.log.iter().any(|e| e == "revoke-delivered") && log.flush_commits >= 1
    });
    let consumed_at_revoke = h.chain.consumed.load(Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(
        h.chain.consumed.load(Ordering::Relaxed),
        consumed_at_revoke,
        "no consumption after lanes were revoked"
    );
    {
        let log = h.shared.lock().unwrap();
        let revoke_at = log
            .log
            .iter()
            .position(|e| e == "revoke-delivered")
            .unwrap();
        let flush_after = log.log[revoke_at..].iter().position(|e| e == "flush");
        let commit_after = log.log[revoke_at..].iter().position(|e| e == "commit");
        assert!(
            flush_after.is_some(),
            "chain must flush during revocation drain"
        );
        assert!(
            commit_after.is_some(),
            "revocation must commit acknowledged offsets"
        );
        assert!(flush_after < commit_after, "flush precedes the commit");
        // Everything consumed was committed — nothing consumed was lost.
        assert_eq!(
            log.committed.get(&PartitionId(0)),
            Some(&(consumed_at_revoke as i64))
        );
    }
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

#[test]
fn blocked_chain_pauses_then_resumes() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::BlockOnce(AtomicBool::new(false)),
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20]);
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 20
    });
    wait_for("pause and resume observed", Duration::from_secs(5), || {
        let log = h.shared.lock().unwrap();
        !log.pauses.is_empty() && !log.resumes.is_empty()
    });
    {
        let log = h.shared.lock().unwrap();
        assert_eq!(log.pauses[0], vec![LaneId(0)]);
        assert_eq!(log.resumes[0], vec![LaneId(0)]);
    }
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// A batch that can never unblock must not hold shutdown hostage until the
/// barrier deadline: the driver's retry loop observes the shutdown flag,
/// abandons the batch (failing its acknowledgement so the data replays),
/// and exits promptly.
#[test]
fn shutdown_during_permanently_blocked_batch_exits_promptly_and_fails_the_batch() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::BlockForever,
        batches_seen: 0,
    });
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    // Let the driver enter the blocked-retry loop.
    std::thread::sleep(Duration::from_millis(100));
    let begun = std::time::Instant::now();
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert!(
        begun.elapsed() < Duration::from_secs(5),
        "shutdown must not wait for the barrier deadline"
    );
    // The abandoned batch's offsets never became committable: nothing was
    // consumed, so no watermark advanced and no commit happened.
    assert_eq!(h.chain.consumed.load(Ordering::Relaxed), 0);
    let log = h.shared.lock().unwrap();
    assert!(
        log.committed.is_empty(),
        "a blocked, abandoned batch must not commit: {:?}",
        log.committed
    );
}

#[test]
fn fatal_chain_fails_pipeline_and_stalls_watermark() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::FatalAtBatch(2),
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20, 20..30]);
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("pipeline must fail");
    };
    assert_eq!(failure.component, "fake-chain");
    let log = h.shared.lock().unwrap();
    // Batch 1 delivered → committable to 10. The failed batch 2 stalls the
    // watermark: 20 and 30 must never be committed.
    let committed = log.committed.get(&PartitionId(0)).copied();
    assert!(
        committed == Some(10) || committed.is_none(),
        "watermark must not pass the failed batch (got {committed:?})"
    );
    assert!(
        h.drained.load(Ordering::Relaxed),
        "failure path drains sink"
    );
}

#[test]
fn panicking_chain_fails_pipeline() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::PanicAtBatch(2),
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20]);
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("pipeline must fail");
    };
    assert!(failure.reason.contains("panicked"), "{}", failure.reason);
    let log = h.shared.lock().unwrap();
    let committed = log.committed.get(&PartitionId(0)).copied();
    assert!(committed == Some(10) || committed.is_none());
}

/// A watermark permanently stalled behind a failed batch (as a fatal sink
/// write produces) must fail the pipeline once it has been stalled past
/// `stalled_fail_after`. Otherwise a broken sink leg leaves the pipeline
/// Running forever, consuming the source but committing nothing.
#[test]
fn permanent_watermark_stall_fails_pipeline_as_checkpoint() {
    let mut cfg = test_config(1);
    cfg.checkpoint.stalled_fail_after = Duration::from_millis(50);
    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::FailAckAtBatch(1),
        batches_seen: 0,
    });
    // The first batch's ack fails, stalling the partition; later batches
    // deliver but their watermark can never pass the failure.
    assign_one_lane(&h, &[0..10, 10..20, 20..30]);
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("a permanent stall must fail the pipeline");
    };
    assert_eq!(failure.component, "checkpoint");
    assert!(failure.reason.contains("stalled"), "{}", failure.reason);
}

/// When per-partition pending batches exceed `max_pending_batches`, the
/// controller pauses the assigned lanes; once acknowledgements drain the
/// pending count below half the limit, it resumes them.
#[test]
fn pending_batch_limit_pauses_then_resumes_lanes() {
    let held: Arc<Mutex<Vec<crate::checkpoint::AckRef>>> = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(AtomicBool::new(false));
    let held_c = Arc::clone(&held);
    let release_c = Arc::clone(&release);
    let mut cfg = test_config(1);
    cfg.checkpoint.max_pending_batches = 3;
    let h = start_with_config(cfg, move |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::HoldAcks {
            held: Arc::clone(&held_c),
            release: Arc::clone(&release_c),
        },
        batches_seen: 0,
    });
    // Six batches whose acks are all withheld: pending climbs past the
    // limit of 3 and the controller pauses the lane.
    assign_one_lane(&h, &[0..10, 10..20, 20..30, 30..40, 40..50, 50..60]);
    wait_for(
        "controller pauses under pending pressure",
        Duration::from_secs(5),
        || {
            h.shared
                .lock()
                .unwrap()
                .pauses
                .iter()
                .any(|p| p.contains(&LaneId(0)))
        },
    );
    // Stop withholding and resolve everything held: pending drains to zero
    // and the controller resumes the lane.
    release.store(true, Ordering::Relaxed);
    held.lock().unwrap().clear();
    wait_for(
        "controller resumes after pending clears",
        Duration::from_secs(5),
        || {
            h.shared
                .lock()
                .unwrap()
                .resumes
                .iter()
                .any(|r| r.contains(&LaneId(0)))
        },
    );
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

#[test]
fn shutdown_flushes_chain_before_exit() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, std::slice::from_ref(&(0..5)));
    wait_for("payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 5
    });
    let flushes_before = h.chain.flushes.load(Ordering::Relaxed);
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert!(
        h.chain.flushes.load(Ordering::Relaxed) > flushes_before,
        "shutdown must flush the chain"
    );
    assert!(h.drained.load(Ordering::Relaxed));
    assert_eq!(report.final_watermarks, vec![(PartitionId(0), 5)]);
}

#[test]
fn source_error_classification() {
    let retryable = SourceError::Client {
        class: ErrorClass::Retryable,
        reason: "hiccup".into(),
    };
    let fatal = SourceError::Client {
        class: ErrorClass::Fatal,
        reason: "broken".into(),
    };
    // Compile-time association test of the taxonomy used by the driver.
    assert!(matches!(
        retryable,
        SourceError::Client {
            class: ErrorClass::Retryable,
            ..
        }
    ));
    assert!(matches!(
        fatal,
        SourceError::Client {
            class: ErrorClass::Fatal,
            ..
        }
    ));
}
