//! Scripted fakes shared by the runtime and builder test suites: a
//! controllable Source with real `AckIssuer` plumbing and a counting
//! chain. The Checkpointer, DrainBarrier, and WatermarkController in the
//! loop are always the real implementations.

use crate::checkpoint::AckIssuer;
use crate::config::{
    BackpressureSection, CheckpointSection, ComponentConfig, MetricsExporter, MetricsSection,
    PinningMode, PipelineConfig, PipelineSection,
};
use crate::error::{ErrorClass, FatalError, SourceError};
use crate::ops::{BlockReason, PushOutcome, RunnableChain};
use crate::pipeline::runtime::RuntimeOptions;
use crate::record::{PartitionId, RawPayload};
use crate::source::{
    DrainBarrier, LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(crate) struct SourceLog {
    pub(crate) committed: BTreeMap<PartitionId, i64>,
    pub(crate) pauses: Vec<Vec<LaneId>>,
    pub(crate) resumes: Vec<Vec<LaneId>>,
    pub(crate) flush_commits: usize,
    pub(crate) opened: bool,
    /// Whether `open` received the framework's source-stage handles. Guards
    /// the seam a source publishes consumer lag through: the handles were
    /// once reachable only via a builder nothing called, so the lag gauge
    /// rendered a permanent zero on every pipeline.
    pub(crate) stage_metrics_attached: bool,
    /// When set, `commit` and `flush_commits` fail retryably — models a
    /// checkpoint store outage (nothing is recorded as committed).
    pub(crate) fail_commits: bool,
    /// Interleaved ordering log shared with the chain fake.
    pub(crate) log: Vec<String>,
}

pub(crate) enum Script {
    Assign(Vec<LaneSpec>),
    /// Additive gain — lanes join the *current* epoch, as a coordinated
    /// source emits for an incremental unit of work.
    Add(Vec<LaneSpec>),
    Revoke(Vec<LaneId>),
    /// Report the source permanently exhausted — models a bounded source
    /// (backfill) requesting the graceful completion drain.
    Drained,
    /// Panic inside `poll_events` — models a source bug that kills the
    /// controller thread outside its drain choreography.
    PanicPoll,
}

pub(crate) struct LaneSpec {
    pub(crate) id: LaneId,
    pub(crate) partition: PartitionId,
    pub(crate) batches: Vec<Vec<(i64, Vec<u8>)>>,
}

pub(crate) type SharedLog = Arc<Mutex<SourceLog>>;
pub(crate) type SharedScript = Arc<Mutex<VecDeque<Script>>>;

/// The lag `FakeSource` publishes for partition 0 at `open`. Distinctive so a
/// test can prove it came from this source and not a registration default.
pub(crate) const FAKE_SOURCE_LAG: u64 = 4242;

pub(crate) struct FakeSource {
    shared: SharedLog,
    script: SharedScript,
    issuer: Option<AckIssuer>,
}

impl FakeSource {
    pub(crate) fn new() -> (Self, SharedLog, SharedScript) {
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

    // A distinctive non-default value so tests can prove the source's own
    // contract (not the trait default) reaches `ChainCtx`.
    fn framing_contract(&self) -> crate::framing::FramingContract {
        crate::framing::FramingContract::PerRecord
    }

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        {
            let mut log = self.shared.lock().unwrap();
            log.opened = true;
            log.stage_metrics_attached = ctx.stage_metrics.is_some();
        }
        // Publish a lag figure like a real source would, so a test can tell
        // whether the handle set it was given actually owns the series. Only
        // the owner registers a per-partition gauge; a shadow's write is
        // dropped and the series never appears.
        if let Some(m) = &ctx.stage_metrics {
            m.set_partition_lag(PartitionId(0), FAKE_SOURCE_LAG);
        }
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
            Some(Script::Add(specs)) => {
                let issuer = self.issuer.as_ref().expect("open before add");
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
                Ok(SourceEvent::LanesAdded(lanes))
            }
            Some(Script::Revoke(ids)) => {
                let mut log = self.shared.lock().unwrap();
                log.log.push("revoke-delivered".into());
                Ok(SourceEvent::LanesRevoked {
                    barrier: DrainBarrier::new(ids.len()),
                    lanes: ids,
                })
            }
            Some(Script::Drained) => {
                self.shared
                    .lock()
                    .unwrap()
                    .log
                    .push("drained-delivered".into());
                Ok(SourceEvent::Drained)
            }
            Some(Script::PanicPoll) => panic!("scripted poll_events panic"),
            None => {
                std::thread::sleep(timeout.min(Duration::from_millis(5)));
                Ok(SourceEvent::Idle)
            }
        }
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        let mut log = self.shared.lock().unwrap();
        if log.fail_commits {
            log.log.push("commit-failed".into());
            return Err(SourceError::Client {
                class: ErrorClass::Retryable,
                reason: "scripted commit failure".into(),
            });
        }
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
        if log.fail_commits {
            return Err(SourceError::Client {
                class: ErrorClass::Retryable,
                reason: "scripted flush failure".into(),
            });
        }
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

pub(crate) struct FakeLane {
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

pub(crate) struct FakeBatch<'a> {
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
pub(crate) struct ChainShared {
    pub(crate) consumed: AtomicUsize,
    pub(crate) flushes: AtomicUsize,
}

pub(crate) enum ChainMode {
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

pub(crate) struct FakeChain {
    pub(crate) shared: Arc<ChainShared>,
    pub(crate) log: Arc<Mutex<SourceLog>>,
    pub(crate) mode: ChainMode,
    pub(crate) batches_seen: usize,
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

/// A distinct pipeline name per config.
///
/// Metric gauge series are owned by exactly one live handle set per process
/// (`metrics::ownership`), and the pipeline name is part of every key. Under
/// nextest each test is its own process and a fixed name would do, but
/// `cargo test` runs a binary's tests concurrently in one process, where two
/// pipelines called `test` are the very collision the ownership check exists
/// to reject — the second would fail to start. Naming them apart keeps the
/// two runners equivalent, and matches production, where two pipelines in one
/// process are two different pipelines.
pub(crate) fn test_pipeline_name() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "test-{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

pub(crate) fn test_config(threads: usize) -> PipelineConfig {
    PipelineConfig {
        pipeline: PipelineSection {
            name: test_pipeline_name(),
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
        sink: Some(ComponentConfig::new("fake", serde_yaml::Value::Null)),
        sinks: None,
    }
}

pub(crate) fn test_options() -> RuntimeOptions {
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

pub(crate) fn batches(ranges: &[std::ops::Range<i64>]) -> Vec<Vec<(i64, Vec<u8>)>> {
    ranges
        .iter()
        .map(|r| r.clone().map(|o| (o, vec![0u8; 8])).collect())
        .collect()
}

/// Spin until `cond` holds or the timeout elapses; panic on timeout.
pub(crate) fn wait_for(what: &str, timeout: Duration, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}
