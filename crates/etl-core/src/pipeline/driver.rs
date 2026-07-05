//! The pipeline-thread driver loop.
//!
//! One driver owns a set of source lanes, one erased operator chain, and a
//! backpressure controller. Everything here is synchronous; the loop never
//! blocks on a channel send (the backpressure invariant) and heartbeats the
//! liveness probe on every iteration, including while paused, while
//! retrying a blocked batch, and while draining.

use super::{DriverEvent, ThreadControl};
use crate::admin::HealthState;
use crate::backpressure::{InflightBudget, Transition, WatermarkController};
use crate::checkpoint::AckRef;
use crate::error::{ErrorClass, FatalError, SourceError};
use crate::metrics::{BackpressureMetrics, SourceMetrics};
use crate::ops::{BlockReason, PushOutcome, RunnableChain};
use crate::record::RawPayload;
use crate::sink::ShardQueues;
use crate::source::{LaneId, PayloadBatch, SourceLane};
use crate::telemetry::RateLimit;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static POLL_ERROR_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));

/// Tuning for one driver thread. Constructed by the runtime; exposed to
/// tests.
#[derive(Clone, Debug)]
pub(crate) struct DriverParams {
    /// This thread's index (labels, heartbeats).
    pub thread: usize,
    /// Max payloads per lane poll.
    pub max_records: usize,
    /// Lane poll timeout; also the paused-loop sleep.
    pub poll_timeout: Duration,
    /// Flush the chain after this long without new data (drives partial
    /// encoder chunks out of idle pipelines).
    pub idle_flush: Duration,
    /// Sleep between retries of a blocked batch.
    pub blocked_retry: Duration,
    /// Queue fill ratio below which resume is allowed (mirrors the
    /// backpressure low watermark).
    pub queue_low_ratio: f64,
}

/// How the driver loop ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DriverExit {
    /// Shutdown barrier arrived at; thread done.
    Completed,
    /// A fatal error was reported; thread done.
    Failed,
}

/// Everything a driver thread owns for its lifetime.
pub(crate) struct DriverContext<L> {
    pub params: DriverParams,
    pub control: crossbeam_channel::Receiver<ThreadControl<L>>,
    pub events: crossbeam_channel::Sender<DriverEvent>,
    pub chain: Box<dyn RunnableChain>,
    pub bp: WatermarkController,
    pub budget: Arc<InflightBudget>,
    pub queues: ShardQueues,
    pub health: Arc<HealthState>,
    pub bp_metrics: BackpressureMetrics,
    pub source_metrics: SourceMetrics,
    /// The process-wide shutdown flag: checked inside long-running inner
    /// loops (blocked-batch retries) so a wedged batch cannot defer
    /// shutdown to the barrier timeout.
    pub shutdown: Arc<AtomicBool>,
}

/// Run one driver thread to completion.
pub(crate) fn run_driver<L: SourceLane>(ctx: DriverContext<L>) -> DriverExit {
    let DriverContext {
        params,
        control,
        events,
        mut chain,
        mut bp,
        budget,
        queues,
        health,
        bp_metrics,
        source_metrics,
        shutdown,
    } = ctx;

    let mut lanes: Vec<L> = Vec::new();
    let mut next_lane = 0usize;
    let mut last_data = Instant::now();
    let mut flushed_since_data = false;
    let mut pause_started: Option<Instant> = None;

    loop {
        health.heartbeat(params.thread);

        // 1. Control messages (never block).
        while let Ok(msg) = control.try_recv() {
            match msg {
                ThreadControl::AddLane(lane) => lanes.push(lane),
                ThreadControl::StopLanes {
                    lanes: stop,
                    barrier,
                    deadline,
                } => {
                    let mut stopped = 0usize;
                    lanes.retain(|l| {
                        let goes = stop.contains(&l.id());
                        stopped += usize::from(goes);
                        !goes
                    });
                    if stopped > 0 {
                        flush_until(
                            chain.as_mut(),
                            deadline,
                            &mut bp,
                            &events,
                            &health,
                            params.thread,
                        );
                    }
                    // One arrival per stopped lane: the source sized the
                    // barrier by lane count (it cannot know how lanes were
                    // distributed across threads).
                    for _ in 0..stopped {
                        barrier.arrive();
                    }
                }
                ThreadControl::Shutdown { barrier, deadline } => {
                    flush_until(
                        chain.as_mut(),
                        deadline,
                        &mut bp,
                        &events,
                        &health,
                        params.thread,
                    );
                    lanes.clear();
                    // Dropping the chain closes this thread's shard-queue
                    // senders; the terminal stage fails the acks of any
                    // records it still parks (its documented Drop
                    // contract), so nothing unwritten can be committed.
                    drop(chain);
                    barrier.arrive();
                    return DriverExit::Completed;
                }
            }
        }

        // 2. Backpressure transitions. Pause/resume are *requests* — only
        // the controller thread touches the Source.
        let queues_low = queues.all_below(params.queue_low_ratio);
        if let Some(t) = bp.tick(&budget, queues_low) {
            let owned: Vec<LaneId> = lanes.iter().map(SourceLane::id).collect();
            apply_transition(t, &owned, &events, &bp_metrics, &mut pause_started);
        }
        if bp.is_paused() {
            std::thread::sleep(params.poll_timeout);
            continue;
        }

        // 3. Poll one lane (round-robin), or idle.
        if lanes.is_empty() {
            std::thread::sleep(params.poll_timeout);
            idle_flush(
                chain.as_mut(),
                &mut last_data,
                &mut flushed_since_data,
                params.idle_flush,
                &mut bp,
                &events,
                params.thread,
            );
            continue;
        }
        next_lane %= lanes.len();
        let lane_idx = next_lane;
        next_lane += 1;

        let owned_ids: Vec<LaneId> = lanes.iter().map(SourceLane::id).collect();
        let poll_started = Instant::now();
        let polled = lanes[lane_idx].poll(params.max_records, params.poll_timeout);
        source_metrics.poll_duration(poll_started.elapsed());

        match polled {
            Ok(Some(mut batch)) => {
                last_data = Instant::now();
                flushed_since_data = false;
                let mut counting = CountingBatch::new(&mut batch);
                let outcome = drive_batch(
                    chain.as_mut(),
                    &mut counting,
                    &mut bp,
                    &budget,
                    &queues,
                    &params,
                    &events,
                    &owned_ids,
                    &health,
                    &bp_metrics,
                    &mut pause_started,
                    &shutdown,
                );
                source_metrics.batch(counting.records, counting.bytes);
                if let Err(error) = outcome {
                    let _ = events.send(DriverEvent::Fatal {
                        thread: params.thread,
                        error,
                    });
                    return DriverExit::Failed;
                }
            }
            Ok(None) => {
                idle_flush(
                    chain.as_mut(),
                    &mut last_data,
                    &mut flushed_since_data,
                    params.idle_flush,
                    &mut bp,
                    &events,
                    params.thread,
                );
            }
            Err(e) if is_fatal(&e) => {
                let _ = events.send(DriverEvent::Fatal {
                    thread: params.thread,
                    error: FatalError {
                        component: format!("driver-{}", params.thread),
                        reason: format!("source poll failed: {e}"),
                    },
                });
                return DriverExit::Failed;
            }
            Err(e) => {
                crate::rate_limited_warn!(
                    POLL_ERROR_WARN,
                    thread = params.thread,
                    error = %e,
                    "retryable source poll error"
                );
            }
        }
    }
}

fn is_fatal(e: &SourceError) -> bool {
    let SourceError::Client { class, .. } = e;
    *class == ErrorClass::Fatal
}

/// Push one batch through the chain, retrying blocked pushes with the
/// resume cursor until the batch completes.
///
/// The batch borrows the lane's buffers, so the retry loop must hold it —
/// it cannot be stashed. While blocked, the loop keeps ticking the
/// backpressure controller (raising a pause request the first time),
/// heartbeats, and sleeps briefly. The never-block invariant is about
/// channel sends; deferring this thread's *next* poll is exactly what
/// backpressure is supposed to do.
#[expect(
    clippy::too_many_arguments,
    reason = "free function over disjoint driver-state borrows"
)]
fn drive_batch(
    chain: &mut dyn RunnableChain,
    batch: &mut dyn PayloadBatch<'_>,
    bp: &mut WatermarkController,
    budget: &InflightBudget,
    queues: &ShardQueues,
    params: &DriverParams,
    events: &crossbeam_channel::Sender<DriverEvent>,
    owned: &[LaneId],
    health: &HealthState,
    bp_metrics: &BackpressureMetrics,
    pause_started: &mut Option<Instant>,
    shutdown: &AtomicBool,
) -> Result<(), FatalError> {
    // Cloned before the first push: if the chain panics the batch may be
    // in an arbitrary state, but this handle can still fail it.
    let ack: AckRef = batch.ack().clone();
    let mut from = 0usize;
    loop {
        health.heartbeat(params.thread);
        let pushed = std::panic::catch_unwind(AssertUnwindSafe(|| chain.push_batch(batch, from)));
        match pushed {
            Ok(PushOutcome::Done) => return Ok(()),
            Ok(PushOutcome::Blocked { resume_at, reason }) => {
                debug_assert!(resume_at >= from, "resume cursor must not go backwards");
                from = resume_at;
                // A batch that can never unblock must not hold shutdown
                // hostage until the barrier deadline: abandon it (fail its
                // acknowledgement — the data replays after restart) and
                // hand control back so the Shutdown message is processed.
                if shutdown.load(Ordering::Relaxed) {
                    tracing::warn!(
                        thread = params.thread,
                        "shutdown during a blocked batch; abandoning it for replay"
                    );
                    ack.fail();
                    // Discard the chain's mid-batch cursor / not-ready stash;
                    // otherwise the Shutdown-triggered flush (or any stray
                    // poll before the Shutdown message arrives) trips the
                    // resume asserts or replays the stale payload.
                    chain.abandon_batch();
                    return Ok(());
                }
                // Only genuine sink pressure engages the backpressure
                // controller; a not-ready wait (schema fetch in flight) is
                // counted by the chain and simply retried.
                if reason == BlockReason::Capacity {
                    bp.on_send_rejected();
                    if let Some(t) = bp.tick(budget, queues.all_below(params.queue_low_ratio)) {
                        apply_transition(t, owned, events, bp_metrics, pause_started);
                    }
                }
                std::thread::sleep(params.blocked_retry);
            }
            Ok(PushOutcome::Fatal(error)) => {
                ack.fail();
                return Err(error);
            }
            Err(panic) => {
                ack.fail();
                return Err(FatalError {
                    component: format!("driver-{}", params.thread),
                    reason: format!("operator chain panicked: {}", panic_message(panic.as_ref())),
                });
            }
        }
    }
}

/// Wraps a lane batch to count payloads and payload bytes as the chain
/// consumes them — the `etl_source_records_total` / `etl_source_bytes_total`
/// feed. Counting happens per payload (not per derived record), and each
/// payload is yielded exactly once even across blocked-batch retries, so
/// the totals are exact.
struct CountingBatch<'a, 'buf> {
    inner: &'a mut dyn PayloadBatch<'buf>,
    records: u64,
    bytes: u64,
}

impl<'a, 'buf> CountingBatch<'a, 'buf> {
    fn new(inner: &'a mut dyn PayloadBatch<'buf>) -> Self {
        CountingBatch {
            inner,
            records: 0,
            bytes: 0,
        }
    }
}

impl<'buf> PayloadBatch<'buf> for CountingBatch<'_, 'buf> {
    fn next_payload(&mut self) -> Option<RawPayload<'buf>> {
        let payload = self.inner.next_payload()?;
        self.records += 1;
        self.bytes +=
            payload.bytes.len() as u64 + payload.key.map(<[u8]>::len).unwrap_or_default() as u64;
        Some(payload)
    }

    fn ack(&self) -> &AckRef {
        self.inner.ack()
    }
}

/// Best-effort chain flush with a deadline (revocation and shutdown).
/// A flush still blocked at the deadline is abandoned: the terminal
/// stage's Drop contract fails any parked acknowledgements, so abandoned
/// data replays instead of being committed.
fn flush_until(
    chain: &mut dyn RunnableChain,
    deadline: Instant,
    bp: &mut WatermarkController,
    events: &crossbeam_channel::Sender<DriverEvent>,
    health: &HealthState,
    thread: usize,
) {
    loop {
        health.heartbeat(thread);
        match chain.flush() {
            PushOutcome::Done => return,
            PushOutcome::Blocked { .. } => {
                bp.on_send_rejected();
                if Instant::now() >= deadline {
                    tracing::error!(
                        thread,
                        "drain deadline exceeded with the chain still blocked; \
                         abandoning parked records for replay"
                    );
                    return;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            PushOutcome::Fatal(error) => {
                let _ = events.send(DriverEvent::Fatal { thread, error });
                return;
            }
        }
    }
}

/// Flush partial terminal state once per data lull.
fn idle_flush(
    chain: &mut dyn RunnableChain,
    last_data: &mut Instant,
    flushed_since_data: &mut bool,
    after: Duration,
    bp: &mut WatermarkController,
    events: &crossbeam_channel::Sender<DriverEvent>,
    thread: usize,
) {
    if *flushed_since_data || last_data.elapsed() < after {
        return;
    }
    match chain.flush() {
        PushOutcome::Done => *flushed_since_data = true,
        PushOutcome::Blocked { .. } => {
            // Full queues while idle: note the rejection; the main-loop
            // tick raises the pause request and the flush retries on the
            // next lull check.
            bp.on_send_rejected();
        }
        PushOutcome::Fatal(error) => {
            let _ = events.send(DriverEvent::Fatal { thread, error });
        }
    }
}

fn apply_transition(
    t: Transition,
    owned: &[LaneId],
    events: &crossbeam_channel::Sender<DriverEvent>,
    bp_metrics: &BackpressureMetrics,
    pause_started: &mut Option<Instant>,
) {
    match t {
        Transition::Pause => {
            *pause_started = Some(Instant::now());
            bp_metrics.pause_started();
            let _ = events.send(DriverEvent::PauseLanes {
                lanes: owned.to_vec(),
            });
        }
        Transition::Resume => {
            let paused_for = pause_started
                .take()
                .map(|s| s.elapsed())
                .unwrap_or_default();
            bp_metrics.pause_ended(paused_for);
            let _ = events.send(DriverEvent::ResumeLanes {
                lanes: owned.to_vec(),
            });
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
