//! Measurable entry points into work this crate does not otherwise expose.
//!
//! A pipeline thread's poll loop runs inside the runtime, on a thread the
//! runtime spawns, behind a source and a sink that need their own I/O. None of
//! that is countable, but the loop itself is synchronous, so it is exported
//! here over scripted lanes and a chain that only drains.
//!
//! The seams follow the rules `spate-s3`'s `bench_seams` module states: behind
//! the off-by-default `testing` feature and `#[doc(hidden)]`, functions and
//! aliases over `std` types only, and one whole unit of a stage's work per
//! function.

use crate::admin::{HealthState, HealthThresholds};
use crate::backpressure::{InflightBudget, WatermarkController};
use crate::checkpoint::{AckMsg, AckRef, AdvanceCounter, PendingGate};
use crate::clock::{Clock, TestClock};
use crate::config::{BackpressureSection, CheckpointSection};
use crate::error::SourceError;
use crate::metrics::{BackpressureMetrics, ComponentLabels, SourceMetrics};
use crate::ops::{PushOutcome, RunnableChain};
use crate::pipeline::driver::{
    DriverContext, DriverExit, DriverParams, backpressure_params, run_driver,
};
use crate::pipeline::{DriverEvent, RuntimeOptions, ThreadControl};
use crate::record::{PartitionId, RawPayload};
use crate::sink::{EncodedChunk, ShardQueues, shard_queues};
use crate::source::{DrainBarrier, LaneId, PayloadBatch, SourceLane};
use std::any::Any;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// A built pipeline thread, run by calling it.
///
/// It returns `(polls, min_lane_polls, batches, payloads)` and the state the
/// build kept alive, which the caller drops once it has stopped measuring.
pub type PollLoop = Box<dyn FnOnce() -> ((u64, u64, u64, u64), Box<dyn Any + Send>) + Send>;

/// Every payload a scripted batch yields.
const PAYLOAD: &[u8] = &[b'x'; 64];

/// Shard-queue capacity in chunks. Nothing is sent, so only the fill reading
/// the queue scan takes depends on it.
const QUEUE_CAPACITY: usize = 64;

/// Build one pipeline thread that runs `polls` iterations of its poll loop over
/// `lanes` lanes and the queues of a sink with `shards` shards, then stops.
///
/// With `payloads` set to `None` every poll returns nothing. With `Some(n)` every
/// poll returns a batch of `n` payloads, which the chain drains. The thread
/// runs on the shipped runtime options and the default backpressure and
/// checkpoint settings, with each lane carrying a pending-ceiling gate.
///
/// Everything is built here, outside whatever the caller measures. The lane
/// assignments and the stop are handled by the loop itself, so calling the
/// returned closure still counts them. The counts it returns are the polls the
/// lanes saw in total and the fewest any one lane saw, and the batches and
/// payloads the chain received.
///
/// # Panics
///
/// When `polls`, `lanes` or `shards` is zero. With no lanes the loop parks
/// until a control message arrives, and none would.
pub fn poll_loop(polls: usize, lanes: usize, shards: usize, payloads: Option<usize>) -> PollLoop {
    assert!(polls > 0, "a run needs at least one poll");
    assert!(
        lanes > 0,
        "a thread with no lanes never polls, so never stops"
    );
    let options = RuntimeOptions::default();
    let backpressure = BackpressureSection::default();
    let checkpoint = CheckpointSection::default();
    let clock = TestClock::frozen();

    let (control_tx, control_rx) = crossbeam_channel::unbounded();
    let (events_tx, events_rx) = crossbeam_channel::unbounded();
    let (queues, receivers) = shard_queues(shards, QUEUE_CAPACITY);
    let barrier = DrainBarrier::new(1);
    let stop = Arc::new(Mutex::new(Some(ThreadControl::Shutdown {
        barrier: barrier.clone(),
        deadline: clock.now(),
    })));
    let remaining = Arc::new(AtomicUsize::new(polls));
    // Every lane's batches share this handle. Holding it here keeps its final
    // drop, and the acknowledgment that drop sends, out of the run.
    let (ack, acks) = AckRef::test_pair();
    let mut lane_polls = Vec::with_capacity(lanes);
    for i in 0..lanes {
        let index = u32::try_from(i).expect("lane count fits in u32");
        let polled = Arc::new(AtomicU64::new(0));
        lane_polls.push(Arc::clone(&polled));
        let lane = ScriptedLane {
            id: LaneId(index),
            partition: PartitionId(index),
            ack: ack.clone(),
            payloads,
            polled,
            remaining: Arc::clone(&remaining),
            stop: Arc::clone(&stop),
            control: control_tx.clone(),
        };
        let gate = PendingGate {
            epoch: 0,
            advanced: AdvanceCounter::new(),
        };
        control_tx
            .send(ThreadControl::AddLane {
                lane,
                gate: Some(gate),
            })
            .expect("the receiver is held below");
    }

    let batches = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let labels = ComponentLabels::new("bench", "driver-0", "driver");
    let ctx = DriverContext {
        params: DriverParams::new(0, &options, &backpressure, &checkpoint),
        control: control_rx,
        events: events_tx,
        chain: Box::new(DrainingChain {
            batches: Arc::clone(&batches),
            payloads: Arc::clone(&received),
        }),
        bp: WatermarkController::new(backpressure_params(&backpressure)),
        budget: Arc::new(InflightBudget::new()),
        queues: vec![queues.clone()],
        health: HealthState::with_clock(1, HealthThresholds::default(), clock.clone()),
        bp_metrics: BackpressureMetrics::new(&labels),
        source_metrics: SourceMetrics::shadow(&labels),
        shutdown: Arc::new(AtomicBool::new(false)),
        clock: clock.clone(),
        stop_clock: clock,
    };
    // Boxed now so that returning it allocates nothing.
    let kept = Box::new(Kept {
        events: events_rx,
        lane_polls,
        batches,
        payloads: received,
        _control: control_tx,
        _queues: queues,
        _receivers: receivers,
        _ack: ack,
        _acks: acks,
        _barrier: barrier,
    });

    Box::new(move || {
        let exit = run_driver(ctx);
        assert_eq!(
            exit,
            DriverExit::Completed,
            "the thread did not stop cleanly"
        );
        assert!(
            kept.events.try_recv().is_err(),
            "the thread raised a pause, a resume or a fatal, so it left the path the run measures"
        );
        let polls = kept
            .lane_polls
            .iter()
            .map(|p| p.load(Ordering::Relaxed))
            .sum();
        let min_lane_polls = kept
            .lane_polls
            .iter()
            .map(|p| p.load(Ordering::Relaxed))
            .min()
            .unwrap_or_default();
        let counts = (
            polls,
            min_lane_polls,
            kept.batches.load(Ordering::Relaxed),
            kept.payloads.load(Ordering::Relaxed),
        );
        (counts, kept as Box<dyn Any + Send>)
    })
}

/// What [`poll_loop`] keeps alive across the run, so it is freed after it.
struct Kept {
    events: crossbeam_channel::Receiver<DriverEvent>,
    lane_polls: Vec<Arc<AtomicU64>>,
    batches: Arc<AtomicU64>,
    payloads: Arc<AtomicU64>,
    _control: crossbeam_channel::Sender<ThreadControl<ScriptedLane>>,
    _queues: ShardQueues,
    _receivers: Vec<mpsc::Receiver<EncodedChunk>>,
    _ack: AckRef,
    _acks: crossbeam_channel::Receiver<AckMsg>,
    _barrier: DrainBarrier,
}

/// A lane whose every poll returns at once, with nothing or with a batch of
/// the same payloads.
///
/// It returns without waiting out the poll timeout, which `SourceLane::poll`
/// asks of a real lane. Waiting would put the clock in the measurement, and
/// the loop's own head-of-line timeout is still computed and passed.
struct ScriptedLane {
    id: LaneId,
    partition: PartitionId,
    ack: AckRef,
    payloads: Option<usize>,
    polled: Arc<AtomicU64>,
    /// Polls left across every lane; the one that takes the last sends `stop`.
    remaining: Arc<AtomicUsize>,
    stop: Arc<Mutex<Option<ThreadControl<ScriptedLane>>>>,
    control: crossbeam_channel::Sender<ThreadControl<ScriptedLane>>,
}

impl SourceLane for ScriptedLane {
    type Batch<'a> = ScriptedBatch<'a>;

    fn id(&self) -> LaneId {
        self.id
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        _max_records: usize,
        _timeout: Duration,
    ) -> Result<Option<ScriptedBatch<'_>>, SourceError> {
        self.polled.fetch_add(1, Ordering::Relaxed);
        if self.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            let stop = self
                .stop
                .lock()
                .expect("the stop slot is never poisoned")
                .take()
                .expect("only the last poll takes the stop");
            self.control
                .send(stop)
                .expect("the driver holds the receiver");
        }
        Ok(self.payloads.map(|left| ScriptedBatch {
            ack: &self.ack,
            partition: self.partition,
            left,
        }))
    }
}

/// `left` copies of [`PAYLOAD`] under the lane's acknowledgment handle.
struct ScriptedBatch<'a> {
    ack: &'a AckRef,
    partition: PartitionId,
    left: usize,
}

impl<'a> PayloadBatch<'a> for ScriptedBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        self.left = self.left.checked_sub(1)?;
        Some(RawPayload {
            bytes: PAYLOAD,
            key: None,
            partition: self.partition,
            offset: 0,
            timestamp_ms: 0,
        })
    }

    fn ack(&self) -> &AckRef {
        self.ack
    }
}

/// A chain that reads every payload it is handed and keeps nothing.
struct DrainingChain {
    batches: Arc<AtomicU64>,
    payloads: Arc<AtomicU64>,
}

impl RunnableChain for DrainingChain {
    fn push_batch<'buf>(
        &mut self,
        batch: &mut dyn PayloadBatch<'buf>,
        _from: usize,
    ) -> PushOutcome {
        self.batches.fetch_add(1, Ordering::Relaxed);
        while let Some(payload) = batch.next_payload() {
            black_box(payload);
            self.payloads.fetch_add(1, Ordering::Relaxed);
        }
        PushOutcome::Done
    }

    fn flush(&mut self) -> PushOutcome {
        PushOutcome::Done
    }
}
