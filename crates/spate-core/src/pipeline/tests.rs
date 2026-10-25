//! Runtime integration tests over scripted fakes: a controllable Source
//! with real `AckIssuer` plumbing, a counting chain, and a mock sink
//! bundle. The Checkpointer, DrainBarrier, and WatermarkController in the
//! loop are the real implementations.

use super::fakes::*;
use super::*;
use crate::config::PipelineConfig;
use crate::error::{ErrorClass, SourceError};
use crate::feed::memory::MemoryFeed;
use crate::metrics::names;
use crate::ops::RunnableChain;
use crate::pipeline::runtime::{PipelineRuntime, default_thread_count};
use crate::record::PartitionId;
use crate::sink::shard_queues;
use crate::source::{LaneId, Source, SourceCtx, SourceEvent};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

fn test_sink() -> (SinkRuntime, Arc<AtomicBool>) {
    test_sink_recording(Arc::default())
}

/// [`test_sink`], recording the budget each drain is given into `budgets`.
fn test_sink_recording(budgets: Arc<Mutex<Vec<Duration>>>) -> (SinkRuntime, Arc<AtomicBool>) {
    let (queues, receivers) = shard_queues(1, 8);
    let drained = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&drained);
    let drain: super::SinkDrainFn = Box::new(move |budget| {
        budgets.lock().unwrap().push(budget);
        Box::pin(async move {
            let _receivers = receivers;
            flag.store(true, Ordering::Relaxed);
            DrainReport::default()
        })
    });
    (
        SinkRuntime {
            queues: vec![queues],
            drain,
            probe: None,
            failures: SinkFailures::new(),
        },
        drained,
    )
}

struct Harness {
    /// The pipeline's in-flight budget, cloned before the runtime takes it.
    /// A test charges it directly, with no sink involved.
    budget: Arc<crate::backpressure::InflightBudget>,
    shared: Arc<Mutex<SourceLog>>,
    script: Arc<Mutex<VecDeque<Script>>>,
    chain: Arc<ChainShared>,
    drained: Arc<AtomicBool>,
    /// The register the runtime reads; a test records into it in place of a
    /// sink worker.
    sink_failures: SinkFailures,
    shutdown: super::runtime::ShutdownHandle,
    /// Every WARN `run` logged on its own thread.
    warnings: Warnings,
    /// The budget each sink drain was given.
    sink_budgets: Arc<Mutex<Vec<Duration>>>,
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
    start_with_options(config, test_options(), mode_factory)
}

fn start_with_options(
    config: PipelineConfig,
    options: RuntimeOptions,
    mode_factory: impl Fn(Arc<ChainShared>, Arc<Mutex<SourceLog>>) -> FakeChain + Send + 'static,
) -> Harness {
    start_with(config, options, |runtime| runtime, mode_factory)
}

/// [`start_with_options`], with `customize` applied to the assembled runtime
/// before it runs.
fn start_with(
    config: PipelineConfig,
    options: RuntimeOptions,
    customize: impl FnOnce(PipelineRuntime<FakeSource>) -> PipelineRuntime<FakeSource>,
    mode_factory: impl Fn(Arc<ChainShared>, Arc<Mutex<SourceLog>>) -> FakeChain + Send + 'static,
) -> Harness {
    start_prepared(
        config,
        options,
        |_| {},
        |runtime, _| customize(runtime),
        mode_factory,
    )
}

/// [`start_with`], with `prepare` run on the source's log before `run`
/// starts, for settings `run` reads at startup.
fn start_prepared(
    config: PipelineConfig,
    options: RuntimeOptions,
    prepare: impl FnOnce(&mut SourceLog),
    customize: impl FnOnce(PipelineRuntime<FakeSource>, &SharedLog) -> PipelineRuntime<FakeSource>,
    mode_factory: impl Fn(Arc<ChainShared>, Arc<Mutex<SourceLog>>) -> FakeChain + Send + 'static,
) -> Harness {
    let (source, shared, script) = FakeSource::new();
    prepare(&mut shared.lock().unwrap());
    let chain_shared = Arc::new(ChainShared::default());
    let sink_budgets = Arc::new(Mutex::new(Vec::new()));
    let (sink, drained) = test_sink_recording(Arc::clone(&sink_budgets));
    let sink_failures = sink.failures.clone();
    let budget = Arc::new(crate::backpressure::InflightBudget::new());
    let budget_for_test = Arc::clone(&budget);
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
    .with_options(options);
    let runtime = customize(runtime, &shared);
    let shutdown = runtime.shutdown_handle();
    let warnings = Warnings::default();
    let layer = warnings.clone();
    std::sync::LazyLock::force(&BYSTANDER);
    let join = std::thread::spawn(move || {
        use tracing_subscriber::layer::SubscriberExt;
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || runtime.run())
    });
    Harness {
        budget: budget_for_test,
        shared,
        script,
        chain: chain_shared,
        drained,
        sink_failures,
        shutdown,
        warnings,
        sink_budgets,
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

/// The controller broadcasts `FlushNow` on every commit tick, so a partial
/// terminal buffer can never hold acknowledgments (and with them the
/// partition watermark) longer than the checkpoint interval just because
/// polls keep returning data. The idle flush is pushed out of reach (60s
/// against a 20ms commit interval), so every flush observed here can only
/// have come from the tick broadcast.
#[test]
fn commit_tick_flushes_chains_without_an_idle_lull() {
    let options = RuntimeOptions {
        idle_flush: Duration::from_secs(60),
        ..test_options()
    };
    let h = start_with_options(test_config(1), options, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20]);
    wait_for("batches consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) >= 20
    });
    let flushes_before = h.chain.flushes.load(Ordering::Relaxed);
    wait_for("commit-tick flushes", Duration::from_secs(5), || {
        h.chain.flushes.load(Ordering::Relaxed) >= flushes_before + 3
    });
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

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

/// A tick commit that fails because the stop began during it leaves its
/// positions pending, and the final commit, run with the stop signal clear,
/// stores them.
#[test]
fn a_tick_commit_cut_by_the_stop_goes_out_in_the_final_commit() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    h.shared.lock().unwrap().trigger_in_commit = Some(h.shutdown.clone());
    // One batch, so the first position committed is the last one.
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    let report = h.join.join().unwrap().unwrap();

    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.final_watermarks, vec![(PartitionId(0), 10)]);
    let log = h.shared.lock().unwrap();
    assert_eq!(log.stopped_commits, vec![vec![(PartitionId(0), 10)]]);
    assert_eq!(
        log.log.iter().filter(|entry| *entry == "commit").count(),
        1,
        "{:?}",
        log.log
    );
    assert_eq!(log.committed.get(&PartitionId(0)), Some(&10));
}

/// Revocation stops the lane's consumption and commits every consumed offset
/// from the revocation drain. Regression for #664.
#[test]
fn revocation_drains_flushes_and_commits_in_order() {
    let mut config = test_config(1);
    // No commit tick fires during the test, so only the revocation drain can
    // commit.
    config.checkpoint.interval = Duration::from_secs(60);
    let h = start_with_config(config, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20, 20..30]);
    wait_for("first batch consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) >= 10
    });
    assert!(
        h.shared.lock().unwrap().committed.is_empty(),
        "nothing may commit before the revocation drain"
    );
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
        // This chain acks on consume, so flush-before-commit ordering is
        // pinned by `revocation_flushes_parked_acks_before_committing`.
        assert_eq!(
            log.committed.get(&PartitionId(0)),
            Some(&(consumed_at_revoke as i64))
        );
    }
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// Revocation must flush the chain *before* committing. The drain turns
/// parked acknowledgments into committable watermarks. The chain here
/// parks every batch's ack until it is flushed, as a real terminal's partial
/// chunk buffer does, and both periodic flush paths are out of reach (60s
/// commit interval, 60s idle flush, sub-second test), so the only thing that
/// can resolve the acks before the revocation commit is the `StopLanes`
/// drain itself. Deleting `flush_until` from the `StopLanes` arm fails this
/// test; `revocation_drains_flushes_and_commits_in_order` cannot see that.
#[test]
fn revocation_flushes_parked_acks_before_committing() {
    let mut config = test_config(1);
    config.checkpoint.interval = Duration::from_secs(60);
    let options = RuntimeOptions {
        idle_flush: Duration::from_secs(60),
        ..test_options()
    };
    let held = Arc::new(Mutex::new(Vec::new()));
    let h = start_with_options(config, options, move |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::HoldAcksUntilFlush {
            held: Arc::clone(&held),
        },
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20]);
    wait_for("batches consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) >= 20
    });
    assert!(
        h.shared.lock().unwrap().committed.is_empty(),
        "acks are parked and no commit tick is due; nothing may commit \
         before the revocation drain"
    );
    h.script
        .lock()
        .unwrap()
        .push_back(Script::Revoke(vec![LaneId(0)]));
    // StopLanes → flush_until resolves the parked acks; the commit that
    // follows inside `revoke_lanes` must then cover everything consumed.
    wait_for(
        "revocation committed the drained acks",
        Duration::from_secs(5),
        || h.shared.lock().unwrap().committed.get(&PartitionId(0)) == Some(&20),
    );
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
/// barrier deadline. The driver's retry loop observes the shutdown flag,
/// abandons the batch (failing its acknowledgment so the data replays),
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
    // A block pauses the lane, so the driver is inside its retry loop.
    wait_for(
        "the blocked batch pauses the lane",
        Duration::from_secs(5),
        || !h.shared.lock().unwrap().pauses.is_empty(),
    );
    let begun = std::time::Instant::now();
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert!(
        begun.elapsed() < Duration::from_secs(5),
        "shutdown must not wait for the barrier deadline"
    );
    // The abandoned batch's offsets never became committable. Nothing was
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
    // watermark. 20 and 30 must never be committed.
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

/// `max_pending_batches` is a hard per-partition bound, enforced at the
/// driver's poll boundary (the regression #196 pins). With every ack
/// withheld, exactly `limit` batches enter the chain and no more, however
/// long the pipeline runs. The bound holds across commit ticks rather than
/// being re-measured by them.
#[test]
fn pending_batch_ceiling_is_a_hard_bound() {
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
    // Six batches whose acks are all withheld. The gate closes after the
    // third and the remaining three never leave the lane.
    assign_one_lane(&h, &[0..10, 10..20, 20..30, 30..40, 40..50, 50..60]);
    wait_for("the ceiling fills", Duration::from_secs(5), || {
        held.lock().unwrap().len() == 3
    });
    // Many commit ticks later (20ms interval), the count has not moved.
    // The bound holds instead of overshooting on the next tick's slack.
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        held.lock().unwrap().len(),
        3,
        "pending exceeded max_pending_batches"
    );
    // The bound is the driver's, not a controller pause. The source was
    // never asked to pause for checkpoint pressure.
    assert!(
        h.shared.lock().unwrap().pauses.is_empty(),
        "the ceiling must not be enforced via source pauses"
    );
    // Releasing the acks retires the held batches; the gate reopens and
    // the remainder flows to completion.
    release.store(true, Ordering::Relaxed);
    held.lock().unwrap().clear();
    wait_for("everything commits", Duration::from_secs(5), || {
        h.shared
            .lock()
            .unwrap()
            .committed
            .get(&PartitionId(0))
            .is_some_and(|&w| w >= 60)
    });
    h.shutdown.trigger();
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// A permanently failed batch stalls its partition's watermark, so the
/// gate pins that partition at the ceiling. The replay a restart faces is
/// bounded by `max_pending_batches`, not by fetch rate x
/// `stalled_fail_after`.
#[test]
fn a_stalled_partition_plateaus_at_the_ceiling() {
    let mut cfg = test_config(1);
    cfg.checkpoint.max_pending_batches = 3;
    let h = start_with_config(cfg, move |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::FailAckAtBatch(1),
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20, 20..30, 30..40, 40..50, 50..60]);
    // The failed head blocks advancement forever; the gate lets exactly
    // the ceiling through and the other three batches stay in the lane.
    wait_for("the ceiling fills", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 30
    });
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        h.chain.consumed.load(Ordering::Relaxed),
        30,
        "a stalled partition must plateau at the ceiling"
    );
    h.shutdown.trigger();
    let _ = h.join.join().unwrap();
}

/// The ceiling is per partition. A lane added while a sibling partition
/// sits at its ceiling runs immediately, gated only by its own count, as
/// `checkpoint.max_pending_batches` documents.
#[test]
fn a_partition_at_the_ceiling_gates_only_its_own_lanes() {
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
    assign_one_lane(&h, &[0..10, 10..20, 20..30, 30..40, 40..50, 50..60]);
    wait_for(
        "partition 0 reaches its ceiling",
        Duration::from_secs(5),
        || held.lock().unwrap().len() == 3,
    );

    // A fresh lane on a fresh partition joins while partition 0 is gated.
    // Its batch flows immediately; the newcomer's gate counts only its own
    // partition's pending.
    h.script
        .lock()
        .unwrap()
        .push_back(Script::Add(vec![LaneSpec {
            id: LaneId(1),
            partition: PartitionId(1),
            batches: batches(std::slice::from_ref(&(100..110))),
        }]));
    wait_for(
        "the added partition's batch is delivered beside the gated sibling",
        Duration::from_secs(5),
        || held.lock().unwrap().len() == 4,
    );
    assert!(
        h.shared.lock().unwrap().pauses.is_empty(),
        "no source pause is involved in gating"
    );

    // Releasing drains both partitions to completion.
    release.store(true, Ordering::Relaxed);
    held.lock().unwrap().clear();
    wait_for("both partitions commit", Duration::from_secs(5), || {
        let shared = h.shared.lock().unwrap();
        shared
            .committed
            .get(&PartitionId(0))
            .is_some_and(|&w| w >= 60)
            && shared
                .committed
                .get(&PartitionId(1))
                .is_some_and(|&w| w >= 110)
    });
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

/// A bounded source reporting `Drained` completes the pipeline on its own,
/// with no external shutdown trigger, through the ordinary drain
/// choreography: chain flush, sink drain, final synchronous commit.
#[test]
fn drained_source_completes_pipeline_without_external_trigger() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20, 20..30]);
    // Model the bounded-source contract: Drained only after every lane's
    // data has been polled and pushed.
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 30
    });
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.final_watermarks, vec![(PartitionId(0), 30)]);
    assert!(h.drained.load(Ordering::Relaxed), "sink drain must run");
    let log = h.shared.lock().unwrap();
    assert!(
        log.flush_commits >= 1,
        "drained completion must flush commits synchronously"
    );
}

/// An empty bounded source (nothing to assign, e.g. an empty backfill
/// listing) still completes cleanly via `Drained`.
#[test]
fn drained_before_any_data_completes_with_empty_watermarks() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert!(report.final_watermarks.is_empty());
    assert!(h.drained.load(Ordering::Relaxed), "sink drain must run");
}

/// `Completed` after `Drained` must mean every batch was acknowledged and
/// committed. If acknowledgments never resolve (a wedged sink abandoning
/// batches at the drain deadline), the backstop converts the exit into a
/// failure instead of reporting a silently incomplete backfill as done.
#[test]
fn drained_with_unacknowledged_batches_fails_instead_of_completing() {
    let held: Arc<Mutex<Vec<crate::checkpoint::AckRef>>> = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(AtomicBool::new(false));
    let held_c = Arc::clone(&held);
    let release_c = Arc::clone(&release);
    let h = start(move |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::HoldAcks {
            held: Arc::clone(&held_c),
            release: Arc::clone(&release_c),
        },
        batches_seen: 0,
    });
    assign_one_lane(&h, &[0..10, 10..20]);
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 20
    });
    // Acks for both batches are held (never delivered, never failed) for the
    // life of the test, so the final commit cycle still sees them pending.
    h.sink_failures.record("orders", "write timed out".into());
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("drained exit with pending acks must fail, not complete");
    };
    assert_eq!(failure.component, "source");
    assert!(
        failure.reason.contains("unacknowledged")
            && failure
                .reason
                .contains("sink `orders` abandoned a batch: write timed out"),
        "{}",
        failure.reason
    );
    assert!(report.final_watermarks.is_empty(), "nothing was committed");
    drop(held);
}

/// `Completed` after `Drained` must also mean the final watermark commit
/// persisted. When the checkpoint store is down for the whole run (every
/// commit and the final flush fail retryably), all batches acknowledge
/// fine, but nothing was durably committed, and there is no next tick to
/// retry into. The backstop must fail the run instead of reporting a
/// completion whose checkpoint holds stale offsets.
#[test]
fn drained_with_failing_commit_fails_instead_of_completing() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    h.shared.lock().unwrap().fail_commits = true;
    assign_one_lane(&h, &[0..10, 10..20]);
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 20
    });
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("drained exit with an unpersisted commit must fail, not complete");
    };
    assert_eq!(failure.component, "source");
    assert!(
        failure.reason.contains("did not persist"),
        "{}",
        failure.reason
    );
    assert!(
        report.final_watermarks.is_empty(),
        "nothing was durably committed"
    );
}

/// The last commit of a stop goes through `Source::commit`, once, after the
/// drain.
#[test]
fn the_final_commit_runs_after_the_drain_through_commit() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    h.shared.lock().unwrap().fail_commits = true;
    assign_one_lane(&h, &[0..10, 10..20]);
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 20
    });
    wait_for("a tick commit", Duration::from_secs(5), || {
        h.shared
            .lock()
            .unwrap()
            .log
            .iter()
            .any(|e| e == "commit-failed")
    });
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();

    let log = h.shared.lock().unwrap().log.clone();
    let drained = log
        .iter()
        .position(|e| e == "drained-delivered")
        .expect("the drain was delivered");
    let finals = log[drained..]
        .iter()
        .filter(|e| *e == "commit-failed")
        .count();
    assert_eq!(finals, 1, "one final commit after the drain: {log:?}");
    assert!(report.final_watermarks.is_empty(), "nothing was committed");
    let ExitState::Failed(failure) = report.state else {
        panic!("a drained exit with nothing committed must fail");
    };
    assert!(
        failure.reason.contains("did not persist"),
        "{}",
        failure.reason
    );
}

/// A failed final commit records one error outcome, the same as a failed
/// tick commit, and leaves every position to the drained-exit backstop.
#[test]
fn a_failed_final_commit_counts_one_error_outcome() {
    let mut cfg = test_config(1);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = crate::metrics::install(&crate::metrics::MetricsSettings {
        exporter: crate::metrics::Exporter::Prometheus,
        per_partition_detail: cfg.metrics.per_partition_detail,
        e2e_basis: crate::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter");

    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    h.shared.lock().unwrap().fail_commits = true;
    h.script.lock().unwrap().push_back(Script::Assign(vec![
        LaneSpec {
            id: LaneId(0),
            partition: PartitionId(0),
            batches: batches(&[0..10, 10..20]),
        },
        LaneSpec {
            id: LaneId(1),
            partition: PartitionId(1),
            batches: batches(&[0..10, 10..20]),
        },
    ]));
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 40
    });
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();

    let log = h.shared.lock().unwrap();
    assert!(report.final_watermarks.is_empty());
    let ExitState::Failed(failure) = report.state else {
        panic!("a drained exit with nothing committed must fail");
    };
    assert!(
        failure.reason.contains("(2 partition(s) uncommitted"),
        "{}",
        failure.reason
    );

    let rendered = handle.render();
    let failed = log.log.iter().filter(|e| *e == "commit-failed").count();
    assert_eq!(
        commits_counter(&rendered, &pipeline_name, "error"),
        Some(failed as f64),
        "{rendered}"
    );
    assert!(
        commits_counter(&rendered, &pipeline_name, "ok").is_none_or(|n| n == 0.0),
        "{rendered}"
    );
}

/// A stored commit records an `ok` outcome and no `error` outcome.
#[test]
fn a_stored_commit_counts_an_ok_outcome() {
    let mut cfg = test_config(1);
    cfg.pipeline.name = "ok-commit-outcome".into();
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = crate::metrics::install(&crate::metrics::MetricsSettings {
        exporter: crate::metrics::Exporter::Prometheus,
        per_partition_detail: cfg.metrics.per_partition_detail,
        e2e_basis: crate::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter");
    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });
    h.shutdown.trigger();
    h.join.join().unwrap().unwrap();

    let rendered = handle.render();
    assert!(
        commits_counter(&rendered, &pipeline_name, "ok").is_some_and(|n| n >= 1.0),
        "{rendered}"
    );
    assert!(
        commits_counter(&rendered, &pipeline_name, "error").is_none_or(|n| n == 0.0),
        "{rendered}"
    );
}

/// A fatal commit error in the final pass fails the run with the commit's own
/// reason.
#[test]
fn a_fatal_final_commit_fails_the_run_with_its_reason() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    h.shared.lock().unwrap().fail_commits = true;
    assign_one_lane(&h, &[0..10, 10..20]);
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 20
    });
    wait_for("a tick commit", Duration::from_secs(5), || {
        h.shared
            .lock()
            .unwrap()
            .log
            .iter()
            .any(|e| e == "commit-failed")
    });
    h.shared.lock().unwrap().fatal_commits = true;
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("a fatal final commit must fail the run");
    };
    assert!(
        failure.reason.contains("commit failed fatally"),
        "{}",
        failure.reason
    );
}

/// A commit-ready hint commits the named partitions ahead of the tick and
/// leaves the others to it.
#[test]
fn a_commit_ready_hint_commits_only_the_chased_partitions() {
    let mut cfg = test_config(1);
    cfg.checkpoint.interval = Duration::from_secs(60);
    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    h.script.lock().unwrap().push_back(Script::Assign(vec![
        LaneSpec {
            id: LaneId(0),
            partition: PartitionId(0),
            batches: batches(std::slice::from_ref(&(0..10))),
        },
        LaneSpec {
            id: LaneId(1),
            partition: PartitionId(1),
            batches: batches(std::slice::from_ref(&(0..10))),
        },
    ]));
    wait_for("all payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 20
    });
    {
        let mut script = h.script.lock().unwrap();
        script.push_back(Script::CommitReady(vec![PartitionId(9)]));
        script.push_back(Script::CommitReady(vec![PartitionId(0)]));
    }
    wait_for(
        "the chased partition committed",
        Duration::from_secs(5),
        || {
            h.shared
                .lock()
                .unwrap()
                .committed
                .contains_key(&PartitionId(0))
        },
    );
    {
        let log = h.shared.lock().unwrap();
        assert!(
            !log.committed.contains_key(&PartitionId(1)),
            "{:?}",
            log.log
        );
        assert_eq!(
            log.log.iter().filter(|e| *e == "commit").count(),
            1,
            "{:?}",
            log.log
        );
    }
    h.shutdown.trigger();
    h.join.join().unwrap().unwrap();
}

/// A fatal commit error fails the run.
#[test]
fn a_fatal_commit_fails_the_run() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    h.shared.lock().unwrap().fatal_commits = true;
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("a fatal commit must fail the run");
    };
    assert!(
        failure.reason.contains("commit failed fatally"),
        "{}",
        failure.reason
    );
}

/// The `spate_checkpoint_commits_total` value for one pipeline and outcome.
fn commits_counter(rendered: &str, pipeline_name: &str, outcome: &str) -> Option<f64> {
    rendered
        .lines()
        .filter(|l| l.starts_with(&format!("{}{{", names::CHECKPOINT_COMMITS_TOTAL)))
        .filter(|l| l.contains(&format!(r#"pipeline="{pipeline_name}""#)))
        .find(|l| l.contains(&format!(r#"outcome="{outcome}""#)))
        .and_then(|l| l.rsplit(' ').next())
        .map(|v| v.parse().expect("a numeric sample"))
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

/// If the controller thread panics (here, a source whose `poll_events`
/// panics), it never runs the drain choreography, never tells the drivers to
/// stop, and never sets the shutdown flag. `run()` must detect the dead
/// controller, stop the drivers itself, and return `Failed` rather than
/// wedging on an untimed driver join.
#[test]
fn controller_panic_stops_drivers_and_fails_instead_of_hanging() {
    let h = start(|shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::BlockForever,
        batches_seen: 0,
    });
    {
        let mut script = h.script.lock().unwrap();
        // Assign a lane so the driver wedges on the never-unblocking batch,
        // then panic the next poll_events.
        script.push_back(Script::Assign(vec![LaneSpec {
            id: LaneId(0),
            partition: PartitionId(0),
            batches: batches(std::slice::from_ref(&(0..10))),
        }]));
        script.push_back(Script::PanicPoll);
    }
    // Bounded wait: without the fix, main's untimed driver join hangs here.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !h.join.is_finished() {
        assert!(
            Instant::now() < deadline,
            "run() hung after the controller thread panicked"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("a controller panic must fail the run");
    };
    assert_eq!(failure.component, "controller");
}

/// A caller-owned I/O runtime (`with_io_runtime`) must be the runtime `run()`
/// uses (tasks land on its worker threads) and must be shut down by `run()`
/// on exit like an internally built one.
#[test]
fn caller_owned_io_runtime_is_used_and_shut_down_by_run() {
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("custom-io")
        .enable_all()
        .build()
        .expect("runtime");

    // A parked task holding a drop guard: only a runtime shutdown drops it.
    struct SetOnDrop(Arc<AtomicBool>);
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let runtime_shut_down = Arc::new(AtomicBool::new(false));
    let guard = SetOnDrop(Arc::clone(&runtime_shut_down));
    io.spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });

    // A sink drain that records which runtime executes spawned work.
    let (queues, receivers) = shard_queues(1, 8);
    let drain_worker: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&drain_worker);
    let drain: super::SinkDrainFn = Box::new(move |_budget| {
        Box::pin(async move {
            let _receivers = receivers;
            let name = tokio::spawn(async { std::thread::current().name().map(String::from) })
                .await
                .expect("probe task");
            *seen.lock().unwrap() = name;
            DrainReport::default()
        })
    });
    let sink = SinkRuntime {
        queues: vec![queues],
        drain,
        probe: None,
        failures: SinkFailures::new(),
    };

    let (source, _shared, _script) = FakeSource::new();
    let chain_shared = Arc::new(ChainShared::default());
    let log = Arc::new(Mutex::new(SourceLog::default()));
    let cs = Arc::clone(&chain_shared);
    let runtime = PipelineRuntime::new(
        test_config(1),
        source,
        move |_thread| {
            Box::new(FakeChain {
                shared: Arc::clone(&cs),
                log: Arc::clone(&log),
                mode: ChainMode::Ok,
                batches_seen: 0,
            }) as Box<dyn RunnableChain>
        },
        sink,
        Arc::new(crate::backpressure::InflightBudget::new()),
    )
    .with_options(test_options())
    .with_io_runtime(io);
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());
    shutdown.trigger();
    let report = join.join().unwrap().unwrap();

    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(
        drain_worker.lock().unwrap().as_deref(),
        Some("custom-io"),
        "drain-spawned work must run on the caller's runtime"
    );
    assert!(
        runtime_shut_down.load(Ordering::Relaxed),
        "run() must shut the caller-owned runtime down on exit"
    );
}

/// A caller-owned current-thread I/O runtime fails `run` with
/// `StartError::Config` before any pipeline thread starts. Regression for
/// #1155.
#[test]
fn current_thread_io_runtime_fails_startup() {
    let io = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (queues, receivers) = shard_queues(1, 8);
    let drain: super::SinkDrainFn = Box::new(move |_budget| {
        Box::pin(async move {
            let _receivers = receivers;
            DrainReport::default()
        })
    });
    let sink = SinkRuntime {
        queues: vec![queues],
        drain,
        probe: None,
        failures: SinkFailures::new(),
    };

    let (source, shared, _script) = FakeSource::new();
    let chain_shared = Arc::new(ChainShared::default());
    let log = Arc::new(Mutex::new(SourceLog::default()));
    let chains_built = Arc::new(AtomicUsize::new(0));
    let chains_built_in = Arc::clone(&chains_built);
    let runtime = PipelineRuntime::new(
        test_config(1),
        source,
        move |_thread| {
            chains_built_in.fetch_add(1, Ordering::Relaxed);
            Box::new(FakeChain {
                shared: Arc::clone(&chain_shared),
                log: Arc::clone(&log),
                mode: ChainMode::Ok,
                batches_seen: 0,
            }) as Box<dyn RunnableChain>
        },
        sink,
        Arc::new(crate::backpressure::InflightBudget::new()),
    )
    .with_options(test_options())
    .with_io_runtime(io);
    // Without the check, a pre-triggered shutdown lets `run` drain and
    // return `Ok`, so the assertion below fails instead of hanging.
    runtime.shutdown_handle().trigger();
    let (result, rendered) = std::thread::spawn(move || {
        let mut result = None;
        let rendered = crate::metrics::render_local(|| result = Some(runtime.run()));
        (result.expect("run returned"), rendered)
    })
    .join()
    .unwrap();

    let Err(err @ StartError::Config(_)) = result else {
        panic!("expected StartError::Config, got {result:?}");
    };
    assert!(err.to_string().contains("multi-thread"), "{err}");
    assert!(!shared.lock().unwrap().opened, "the source must not open");
    assert_eq!(
        chains_built.load(Ordering::Relaxed),
        0,
        "no pipeline thread may start"
    );
    assert!(
        !rendered.contains(names::PIPELINE_STATE),
        "a refused runtime must register no series:\n{rendered}"
    );
}

/// The source drops while the io runtime still runs, so work its `Drop`
/// spawns there completes. Regression for #854.
#[test]
fn the_source_drops_before_the_io_runtime_stops() {
    struct NeedsIoOnDrop {
        inner: FakeSource,
        io: tokio::runtime::Handle,
        ran: Arc<AtomicBool>,
    }
    impl Source for NeedsIoOnDrop {
        type Lane = FakeLane;
        fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
            self.inner.open(ctx)
        }
        fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<FakeLane>, SourceError> {
            self.inner.poll_events(timeout)
        }
        fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
            self.inner.commit(watermarks)
        }
    }
    impl Drop for NeedsIoOnDrop {
        fn drop(&mut self) {
            let (tx, rx) = mpsc::channel();
            // Outlives the moment main would stop the runtime if it did not
            // wait for this thread. A stopped runtime drops the task, and
            // with it the sender.
            self.io.spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let _ = tx.send(());
            });
            let ran = rx.recv_timeout(Duration::from_secs(10)).is_ok();
            self.ran.store(ran, Ordering::Relaxed);
        }
    }

    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let ran = Arc::new(AtomicBool::new(false));
    let (inner, _shared, _script) = FakeSource::new();
    let source = NeedsIoOnDrop {
        inner,
        io: io.handle().clone(),
        ran: Arc::clone(&ran),
    };
    let (queues, receivers) = shard_queues(1, 8);
    let drain: super::SinkDrainFn = Box::new(move |_budget| {
        Box::pin(async move {
            let _receivers = receivers;
            DrainReport::default()
        })
    });
    let sink = SinkRuntime {
        queues: vec![queues],
        drain,
        probe: None,
        failures: SinkFailures::new(),
    };
    let chain_shared = Arc::new(ChainShared::default());
    let log = Arc::new(Mutex::new(SourceLog::default()));
    let runtime = PipelineRuntime::new(
        test_config(1),
        source,
        move |_thread| {
            Box::new(FakeChain {
                shared: Arc::clone(&chain_shared),
                log: Arc::clone(&log),
                mode: ChainMode::Ok,
                batches_seen: 0,
            }) as Box<dyn RunnableChain>
        },
        sink,
        Arc::new(crate::backpressure::InflightBudget::new()),
    )
    .with_options(test_options())
    .with_io_runtime(io);
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());
    shutdown.trigger();
    let report = join.join().unwrap().unwrap();

    assert_eq!(report.state, ExitState::Completed);
    assert!(
        ran.load(Ordering::Relaxed),
        "work the source's Drop spawns on the io runtime must run"
    );
}

/// A fallible startup step after the driver threads are spawned (here, the
/// admin bind, forced to fail by occupying its port) must stop the drivers
/// and return the error promptly, never leaking the running pinned threads.
#[test]
fn startup_error_after_driver_spawn_stops_drivers_and_returns_err() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe port");
    let addr = occupied.local_addr().unwrap();
    let mut cfg = test_config(1);
    cfg.admin.listen = Some(addr);
    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    // Prompt return proves the driver join completed (threads did not leak).
    let deadline = Instant::now() + Duration::from_secs(10);
    while !h.join.is_finished() {
        assert!(
            Instant::now() < deadline,
            "run() did not return promptly on a bind failure"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let result = h.join.join().unwrap();
    let Err(err @ StartError::AdminBind { .. }) = result else {
        panic!("expected StartError::AdminBind, got {result:?}");
    };
    // A bare I/O error leaves the reader guessing which of a config's
    // addresses the kernel refused.
    assert!(
        err.to_string().contains(&addr.to_string()),
        "the error must name the address it could not take: {err}"
    );
    drop(occupied);
}

/// The text of the `StartError::Config` a run returns before its source
/// opens. Waits for the run under a deadline, so a run that opens the source
/// and keeps going fails the test there.
fn config_error_before_open(h: Harness) -> String {
    wait_for("run() to return", Duration::from_secs(10), || {
        h.join.is_finished()
    });
    let result = h.join.join().unwrap();
    let Err(err @ StartError::Config(_)) = result else {
        panic!("expected StartError::Config, got {result:?}");
    };
    assert!(!h.shared.lock().unwrap().opened, "the source must not open");
    err.to_string()
}

fn ok_chain(shared: Arc<ChainShared>, log: Arc<Mutex<SourceLog>>) -> FakeChain {
    FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    }
}

fn feed_section(yaml: &str) -> Option<crate::config::FeedSection> {
    Some(serde_yaml::from_str(yaml).expect("section"))
}

/// Pins that the runtime hands a `feed:` section to the source, whose default
/// `configure_feed` refuses it before the source opens.
#[test]
fn a_source_without_a_feed_rejects_the_feed_section() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("memory: {}");
    let h = start_with_config(cfg, ok_chain);
    let err = config_error_before_open(h);
    assert!(err.contains("reads no feed"), "{err}");
}

/// Pins that `run` refuses a `location` on the in-process feed it builds,
/// before the source is offered the section.
#[test]
fn a_memory_feed_with_a_location_is_refused() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("memory: {}\nlocation: s3://ops/jobs/backfill-1/");
    let h = start_with_config(cfg, ok_chain);
    let err = config_error_before_open(h);
    assert!(
        err.contains("`feed.location`") && err.contains("`memory`"),
        "{err}"
    );
    assert!(
        !err.contains("s3://ops"),
        "the location must not print: {err}"
    );
}

/// Pins the defaults `spate-core` owns for the `feed:` section: no key set
/// leaves the source's own default in force, a bounded job settles for five
/// minutes, the in-process feed dead-letters a unit on its tenth failed
/// delivery, and a stop reserves five seconds of the 25 s drain. The SQS,
/// Kafka and S3 defaults are pinned in their crates.
#[test]
fn the_feed_section_defaults() {
    use crate::feed::Feed;

    for yaml in ["memory: {}", "sqs: {}"] {
        let section = feed_section(yaml).expect("section");
        assert_eq!(section.units_held, None, "{yaml}");
        assert_eq!(section.attempt_cap, None, "{yaml}");
        assert_eq!(section.location, None, "{yaml}");
        assert_eq!(section.settling_window, None, "{yaml}");
        assert_eq!(
            section.settling_window_or_default(),
            Duration::from_secs(300),
            "{yaml}"
        );
    }

    let section = feed_section("memory: {}").expect("section");
    let feed = MemoryFeed::from_section(&section).expect("feed");
    drop(feed.publish(crate::feed::Unit::new("poison", b"poison".to_vec())));
    // The cap is written out: a loop bound read from the constant would
    // follow a changed cap.
    for attempt in 1..=10 {
        let delivery = feed.receive(usize::MAX).unwrap().remove(0);
        assert_eq!(delivery.deliveries, attempt);
        feed.fail(delivery.receipt, "unreadable").unwrap();
        let counts = feed.counts();
        let dead = u64::from(attempt == 10);
        assert_eq!(counts.dead_lettered, dead, "after attempt {attempt}");
        assert_eq!(counts.visible, 1 - dead, "after attempt {attempt}");
    }

    let cfg = PipelineConfig::from_str(
        "pipeline: { name: demo }\nsource: { memory: {} }\nsink: { memory: {} }\n",
    )
    .expect("config");
    assert_eq!(cfg.checkpoint.drain_timeout, Duration::from_secs(25));
    assert_eq!(crate::feed::STOP_RESERVE, Duration::from_secs(5));
}

/// Pins that `run` refuses a `location` on an in-process feed passed in
/// through `Pipeline::feed`, which skips building the feed from the section.
#[test]
fn a_memory_feed_passed_in_with_a_location_is_refused() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("memory: {}\nlocation: s3://ops/jobs/backfill-1/");
    let h = start_with(
        cfg,
        test_options(),
        |runtime| runtime.with_feed(Arc::new(MemoryFeed::new())),
        ok_chain,
    );
    let err = config_error_before_open(h);
    assert!(
        err.contains("`feed.location`") && err.contains("`memory`"),
        "{err}"
    );
}

/// Pins that `run` refuses a `settling_window` on an in-process feed passed
/// in through `Pipeline::feed`, which skips building the feed from the
/// section.
#[test]
fn a_memory_feed_passed_in_with_a_settling_window_is_refused() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("memory: {}\nsettling_window: 1m");
    let h = start_with(
        cfg,
        test_options(),
        |runtime| runtime.with_feed(Arc::new(MemoryFeed::new())),
        ok_chain,
    );
    let err = config_error_before_open(h);
    assert!(
        err.contains("`feed.settling_window`") && err.contains("`memory`"),
        "{err}"
    );
}

/// Pins that `run` refuses a settling window below the floor set on a
/// section built in code, which skips the load-time check.
#[test]
fn a_zero_settling_window_passed_in_is_refused() {
    let mut cfg = test_config(1);
    let mut section = feed_section("sqs: {}\nlocation: s3://ops/jobs/backfill-1/").unwrap();
    section.settling_window = Some(Duration::ZERO);
    cfg.feed = Some(section);
    let h = start_with_config(cfg, ok_chain);
    let err = config_error_before_open(h);
    assert!(err.contains("`feed.settling_window`"), "{err}");
}

/// Pins that `Source::configure_feed`'s default refuses the section, naming
/// the source, so a source that reads no feed fails at startup.
#[test]
fn the_default_configure_feed_refuses_the_section() {
    struct NoFeed(FakeSource);
    impl Source for NoFeed {
        type Lane = FakeLane;
        fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
            self.0.open(ctx)
        }
        fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<FakeLane>, SourceError> {
            self.0.poll_events(timeout)
        }
        fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
            self.0.commit(watermarks)
        }
    }
    let (inner, _shared, _script) = FakeSource::new();
    let mut source = NoFeed(inner);
    let section = feed_section("memory: {}").unwrap();
    let err = source
        .configure_feed(
            &section,
            Arc::new(MemoryFeed::new()),
            crate::feed::FeedOrigin::Section,
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("reads no feed"), "{err}");
}

/// Pins the origin `run` hands `configure_feed`: `Section` for the in-process
/// feed it builds from the section, `Given` for a feed passed to
/// `Pipeline::feed`.
#[test]
fn the_source_learns_where_its_feed_came_from() {
    use crate::feed::FeedOrigin;
    for (given, origin) in [(false, FeedOrigin::Section), (true, FeedOrigin::Given)] {
        let mut cfg = test_config(1);
        cfg.feed = feed_section("memory: {}");
        let (source, shared, script) = FakeSource::new();
        shared.lock().unwrap().accept_feed = true;
        script.lock().unwrap().push_back(Script::Drained);
        let chain_shared = Arc::new(ChainShared::default());
        let (sink, _drained) = test_sink();
        let log = Arc::clone(&shared);
        let mut runtime = PipelineRuntime::new(
            cfg,
            source,
            move |_thread| {
                Box::new(ok_chain(Arc::clone(&chain_shared), Arc::clone(&log)))
                    as Box<dyn RunnableChain>
            },
            sink,
            Arc::new(crate::backpressure::InflightBudget::new()),
        )
        .with_options(test_options());
        if given {
            runtime = runtime.with_feed(Arc::new(MemoryFeed::new()));
        }
        let report = runtime.run().expect("the source accepts the section");
        assert_eq!(report.state, ExitState::Completed);
        assert_eq!(
            shared.lock().unwrap().feed_origin,
            Some(origin),
            "given: {given}"
        );
    }
}

/// Pins that `run` passes a `location` beside a backend other than the
/// in-process feed on to the feed checks that follow.
#[test]
fn a_location_beside_a_durable_backend_is_not_refused() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("sqs: {}\nlocation: s3://ops/jobs/backfill-1/");
    let h = start_with_config(cfg, ok_chain);
    let err = config_error_before_open(h);
    assert!(err.contains("pass it to `Pipeline::feed`"), "{err}");
}

/// Pins that a section naming a backend other than `memory`, with no feed
/// passed in, fails startup with the instruction to build it in code.
#[test]
fn a_backend_built_in_code_without_a_feed_fails_startup() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("sqs: {}");
    let h = start_with_config(cfg, ok_chain);
    let err = config_error_before_open(h);
    assert!(err.contains("pass it to `Pipeline::feed`"), "{err}");
}

/// Pins that a feed passed in with no `feed:` section fails startup.
#[test]
fn a_feed_given_without_a_feed_section_fails_startup() {
    let h = start_with(
        test_config(1),
        test_options(),
        |runtime| runtime.with_feed(Arc::new(MemoryFeed::new())),
        ok_chain,
    );
    let err = config_error_before_open(h);
    assert!(err.contains("no `feed:` section"), "{err}");
}

/// Pins that a feed passed in whose backend differs from the section's key
/// fails startup, naming both.
#[test]
fn a_feed_whose_backend_differs_from_the_section_fails_startup() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("sqs: {}");
    let h = start_with(
        cfg,
        test_options(),
        |runtime| runtime.with_feed(Arc::new(MemoryFeed::new())),
        ok_chain,
    );
    let err = config_error_before_open(h);
    assert!(err.contains("`memory`") && err.contains("`sqs`"), "{err}");
}

/// Pins that the runtime builds the in-process feed from the section, so a
/// key in the `memory` body fails startup naming its path.
#[test]
fn a_key_in_the_memory_body_fails_startup() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("memory: { depth: 4 }");
    let h = start_with_config(cfg, ok_chain);
    let err = config_error_before_open(h);
    assert!(
        err.contains("feed.memory") && err.contains("depth"),
        "{err}"
    );
}

/// Pins that the runtime offers the `feed:` section before any pipeline
/// thread is built, so a refused section starts no driver.
#[test]
fn a_refused_feed_section_builds_no_pipeline_thread() {
    let mut cfg = test_config(1);
    cfg.feed = feed_section("memory: {}");
    let built = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&built);
    let h = start_with_config(cfg, move |shared, log| {
        counter.fetch_add(1, Ordering::SeqCst);
        ok_chain(shared, log)
    });
    let err = config_error_before_open(h);
    assert!(err.contains("reads no feed"), "{err}");
    assert_eq!(
        built.load(Ordering::SeqCst),
        0,
        "no chain may be built before the section is accepted"
    );
}

/// A second registered dispatcher. With one live dispatcher, tracing-core
/// caches a callsite's interest from whichever thread reaches it first, and
/// other tests reach the feed arm on threads with no subscriber.
static BYSTANDER: std::sync::LazyLock<tracing::Dispatch> = std::sync::LazyLock::new(|| {
    use tracing_subscriber::layer::SubscriberExt;
    tracing::Dispatch::new(
        tracing_subscriber::registry().with(tracing_subscriber::filter::LevelFilter::OFF),
    )
});

/// Pins that a `feed: { memory: {} }` section the source refuses fails
/// startup with the error alone and logs no WARN.
#[test]
fn a_refused_feed_section_fails_startup_without_a_warning() {
    use tracing_subscriber::layer::SubscriberExt;
    std::sync::LazyLock::force(&BYSTANDER);
    let mut cfg = test_config(1);
    cfg.feed = feed_section("memory: {}");
    let (source, shared, _script) = FakeSource::new();
    let chain_shared = Arc::new(ChainShared::default());
    let (sink, _drained) = test_sink();
    let log = Arc::clone(&shared);
    let runtime = PipelineRuntime::new(
        cfg,
        source,
        move |_thread| {
            Box::new(ok_chain(Arc::clone(&chain_shared), Arc::clone(&log)))
                as Box<dyn RunnableChain>
        },
        sink,
        Arc::new(crate::backpressure::InflightBudget::new()),
    )
    .with_options(test_options());
    let warnings = Warnings::default();
    let subscriber = tracing_subscriber::registry().with(warnings.clone());
    let result = tracing::subscriber::with_default(subscriber, || runtime.run());
    let Err(err @ StartError::Config(_)) = result else {
        panic!("expected StartError::Config, got {result:?}");
    };
    assert!(err.to_string().contains("reads no feed"), "{err}");
    assert!(!shared.lock().unwrap().opened, "the source must not open");
    let warnings = warnings.0.lock().unwrap().clone();
    assert!(warnings.is_empty(), "{warnings:#?}");
}

/// Pins that a `feed: { memory: {} }` section the source accepts starts and
/// completes the run with no WARN from the runtime.
#[test]
fn an_accepted_in_process_feed_section_starts_without_a_warning() {
    use tracing_subscriber::layer::SubscriberExt;
    std::sync::LazyLock::force(&BYSTANDER);
    let mut cfg = test_config(1);
    cfg.feed = feed_section("memory: {}");
    let (source, shared, script) = FakeSource::new();
    shared.lock().unwrap().accept_feed = true;
    script.lock().unwrap().push_back(Script::Drained);
    let chain_shared = Arc::new(ChainShared::default());
    let (sink, _drained) = test_sink();
    let log = Arc::clone(&shared);
    let runtime = PipelineRuntime::new(
        cfg,
        source,
        move |_thread| {
            Box::new(ok_chain(Arc::clone(&chain_shared), Arc::clone(&log)))
                as Box<dyn RunnableChain>
        },
        sink,
        Arc::new(crate::backpressure::InflightBudget::new()),
    )
    .with_options(test_options());
    let warnings = Warnings::default();
    let subscriber = tracing_subscriber::registry().with(warnings.clone());
    let report = tracing::subscriber::with_default(subscriber, || runtime.run())
        .expect("the source accepts the section");
    assert_eq!(report.state, ExitState::Completed);
    assert!(shared.lock().unwrap().opened, "the source opens");
    let warnings = warnings.0.lock().unwrap().clone();
    assert!(warnings.is_empty(), "{warnings:#?}");
}

/// `admin.listen: none` runs and drains with no admin server. With no
/// listener there is no stop channel to signal, and the run has to reach its
/// drain and its controller join anyway.
#[test]
fn no_admin_listener_still_drains_and_completes() {
    let mut cfg = test_config(1);
    cfg.admin.listen = None;
    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for("payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// The controller's `SourceMetrics`, not one of the per-thread instances,
/// must own the source series.
///
/// A pipeline builds one handle set per driver thread plus the controller's,
/// all on identical labels, because every thread counts the records it polled.
/// Only the controller's can publish the gauges. It holds the assignment, and
/// its clone reaches the source, which is the only thing that can measure lag.
/// If a driver's instance claimed the series first, the controller's would be
/// a shadow and `spate_source_lag_records` would go silent again, as it did
/// for fourteen days.
///
/// `FakeSource` publishes `FAKE_SOURCE_LAG` for partition 0 at `open`, through
/// whatever handle set it was given. A per-partition gauge registers on its
/// first published value, so a shadowed controller leaves the series *absent*.
/// Flip `SourceMetrics::shadow` to `try_new` in `runtime.rs` and this test
/// fails on a missing series, not on a wrong number.
#[test]
fn the_controller_owns_the_source_series_not_the_per_thread_shadows() {
    let mut cfg = test_config(4); // four drivers → four shadows, one owner
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    // Installed before the pipeline builds. Handles bind to the recorder
    // present at construction. Idempotent, so the runtime's own install
    // reuses this one and renders through this handle.
    let handle = crate::metrics::install(&crate::metrics::MetricsSettings {
        exporter: crate::metrics::Exporter::Prometheus,
        per_partition_detail: cfg.metrics.per_partition_detail,
        e2e_basis: crate::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter");

    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for("payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);

    let rendered = handle.render();
    let needle = format!(
        r#"spate_source_lag_records{{pipeline="{pipeline_name}",component="source",component_type="source",partition="0"}} {FAKE_SOURCE_LAG}"#
    );
    assert!(
        rendered.contains(&needle),
        "the source's lag must reach the exposition — the handle set the \
         source was given owns the series:\n{rendered}"
    );
}

/// The in-flight budget's usage reaches the exposition as exactly one series.
///
/// Regression for #332.
///
/// This asserts the number of series as well as the value. The budget is one
/// counter per pipeline, so publishing it from each driver would give one
/// series per thread, all carrying the same number, and `sum()` over the
/// family would report the thread count times the usage.
#[test]
fn the_inflight_budget_reaches_the_exposition_as_one_series() {
    let mut cfg = test_config(4); // four drivers, so a per-driver regression shows up in the count
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    // Installed before the pipeline builds. Handles bind to the recorder
    // present at construction, and `install` is idempotent, so the runtime
    // reuses this one and renders through this handle.
    let handle = crate::metrics::install(&crate::metrics::MetricsSettings {
        exporter: crate::metrics::Exporter::Prometheus,
        per_partition_detail: cfg.metrics.per_partition_detail,
        e2e_basis: crate::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter");

    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });

    // The controller samples the budget once per loop pass.
    const CHARGED: usize = 7 << 20;
    h.budget.add(CHARGED);

    let series = |rendered: &str| -> Vec<String> {
        rendered
            .lines()
            .filter(|l| l.starts_with(&format!("{}{{", names::BACKPRESSURE_INFLIGHT_BYTES)))
            // Under `cargo test` other tests' pipelines record into this process-wide recorder.
            .filter(|l| l.contains(&format!(r#"pipeline="{pipeline_name}""#)))
            .map(str::to_owned)
            .collect()
    };

    wait_for(
        "the budget gauge is published",
        Duration::from_secs(5),
        || {
            series(&handle.render())
                .iter()
                .any(|l| l.ends_with(&format!(" {CHARGED}")))
        },
    );

    let rendered = handle.render();
    let published = series(&rendered);
    assert_eq!(
        published,
        vec![format!(
            r#"{}{{pipeline="{pipeline_name}",component="runtime",component_type="pipeline"}} {CHARGED}"#,
            names::BACKPRESSURE_INFLIGHT_BYTES
        )],
        "the budget publishes exactly one series, under the runtime's labels:\n{rendered}"
    );

    // A drain is visible too, so the series tracks the budget.
    h.budget.sub(CHARGED);
    wait_for("the budget gauge drains", Duration::from_secs(5), || {
        series(&handle.render()).iter().any(|l| l.ends_with(" 0"))
    });

    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// The value of one pipeline's `spate_pipeline_io_busy_ratio` line, if the
/// exposition carries it.
fn io_busy_ratio(rendered: &str, pipeline_name: &str) -> Option<f64> {
    let prefix = format!(
        r#"{}{{pipeline="{pipeline_name}",component="runtime",component_type="pipeline"}} "#,
        names::PIPELINE_IO_BUSY_RATIO
    );
    rendered
        .lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.parse().ok())
}

fn install_prometheus(cfg: &PipelineConfig) -> crate::metrics::MetricsHandle {
    // Installed before the pipeline builds. Handles bind to the recorder
    // present at construction, and `install` is idempotent, so the runtime
    // reuses this one and renders through this handle.
    crate::metrics::install(&crate::metrics::MetricsSettings {
        exporter: crate::metrics::Exporter::Prometheus,
        per_partition_detail: cfg.metrics.per_partition_detail,
        e2e_basis: crate::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter")
}

/// The I/O busy gauge is in the exposition, at 0, before its first sample.
#[test]
fn the_io_busy_ratio_is_published_from_start() {
    let mut cfg = test_config(1);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = install_prometheus(&cfg);

    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    wait_for(
        "the busy gauge is published",
        Duration::from_secs(5),
        || io_busy_ratio(&handle.render(), &pipeline_name) == Some(0.0),
    );

    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// A caller-owned I/O runtime kept busy by a task of its own reads as busy
/// in the exposition, sampled by the running pipeline.
#[test]
fn a_busy_io_runtime_reaches_the_exposition() {
    let mut cfg = test_config(1);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = install_prometheus(&cfg);

    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let stop = Arc::new(AtomicBool::new(false));
    let spin = Arc::clone(&stop);
    io.spawn(async move {
        // CPU work inside a poll is what tokio counts as busy.
        while !spin.load(Ordering::Relaxed) {
            let polled = Instant::now();
            while polled.elapsed() < Duration::from_millis(10) {
                std::hint::spin_loop();
            }
            tokio::task::yield_now().await;
        }
    });

    let h = start_with(
        cfg,
        test_options(),
        |runtime| {
            runtime
                .with_io_runtime(io)
                .with_io_busy_sampling(Duration::from_millis(50), Duration::from_secs(1))
        },
        |shared, log| FakeChain {
            shared,
            log,
            mode: ChainMode::Ok,
            batches_seen: 0,
        },
    );
    wait_for(
        "a busy sample is published",
        Duration::from_secs(10),
        || io_busy_ratio(&handle.render(), &pipeline_name).is_some_and(|r| r > 0.5),
    );

    stop.store(true, Ordering::Relaxed);
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// A running pipeline divides by its I/O runtime's worker count: one task
/// holding one of two workers reads about half in the exposition.
#[test]
fn one_busy_worker_of_two_reads_half_in_the_exposition() {
    const WINDOW: Duration = Duration::from_secs(2);

    let mut cfg = test_config(1);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = install_prometheus(&cfg);

    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let stop = Arc::new(AtomicBool::new(false));
    let spin = Arc::clone(&stop);
    io.spawn(async move {
        while !spin.load(Ordering::Relaxed) {
            let polled = Instant::now();
            while polled.elapsed() < Duration::from_millis(10) {
                std::hint::spin_loop();
            }
            tokio::task::yield_now().await;
        }
    });

    let h = start_with(
        cfg,
        test_options(),
        |runtime| {
            runtime
                .with_io_runtime(io)
                .with_io_busy_sampling(Duration::from_millis(50), WINDOW)
        },
        |shared, log| FakeChain {
            shared,
            log,
            mode: ChainMode::Ok,
            batches_seen: 0,
        },
    );
    // A reading counted against one worker sits near 1 once the window
    // fills; against two it stays near half for a whole window. The hold is
    // timed because one render of a shared process's exposition can take
    // tens of milliseconds, so a count of polls has no fixed duration.
    let in_band_since = std::cell::Cell::new(None::<Instant>);
    wait_for(
        "the gauge to hold near half",
        Duration::from_secs(15),
        || {
            let half = io_busy_ratio(&handle.render(), &pipeline_name)
                .is_some_and(|r| (0.3..0.7).contains(&r));
            in_band_since.set(half.then(|| in_band_since.get().unwrap_or_else(Instant::now)));
            in_band_since
                .get()
                .is_some_and(|since| since.elapsed() >= WINDOW)
        },
    );

    stop.store(true, Ordering::Relaxed);
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// One event's fields, as `(name, debug value)` pairs.
type Fields = Vec<(String, String)>;

/// The fields of every warning event.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<Fields>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warnings {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Record(Fields);
        impl tracing::field::Visit for Record {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push((field.name().to_owned(), format!("{value:?}")));
            }
        }
        if *event.metadata().level() == tracing::Level::WARN {
            let mut fields = Record(Vec::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }
}

/// Feeds `sample_io_busy` a sample every 5 s for ten minutes from a
/// three-worker runtime busy `share` of the time, under a captured subscriber,
/// and returns the fields of every warning it logged.
fn io_busy_warnings(pipeline: &'static str, share: f64) -> Vec<Fields> {
    use crate::metrics::{ComponentLabels, IoBusySampler, IoRuntimeMetrics};
    use crate::pipeline::runtime::sample_io_busy;
    use tracing_subscriber::layer::SubscriberExt;

    const WORKERS: usize = 3;
    let labels = ComponentLabels::new(pipeline, "runtime", "pipeline");
    let gauge = IoRuntimeMetrics::try_new(&labels).expect("free series");
    let t0 = Instant::now();
    let mut sampler = IoBusySampler::new(
        Duration::from_secs(5),
        IoBusySampler::WINDOW,
        t0,
        Duration::ZERO,
    );
    let warnings = Warnings::default();
    let subscriber = tracing_subscriber::registry().with(warnings.clone());
    tracing::subscriber::with_default(subscriber, || {
        for secs in (5..=600).step_by(5) {
            let busy = Duration::from_secs_f64(share * (WORKERS as u64 * secs) as f64);
            sample_io_busy(
                &mut sampler,
                &gauge,
                t0 + Duration::from_secs(secs),
                busy,
                WORKERS,
                pipeline,
            );
        }
    });
    warnings.0.lock().unwrap().clone()
}

/// Ten minutes of a saturated three-worker I/O runtime log one warning,
/// carrying the pipeline and the worker count. Half busy on three workers
/// logs nothing.
#[test]
fn a_saturated_io_runtime_logs_one_warning() {
    let logged = io_busy_warnings("io-busy-warning", 0.95);
    assert_eq!(logged.len(), 1, "{logged:?}");
    let field = |name: &str| {
        logged[0]
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    };
    assert_eq!(field("pipeline"), Some("\"io-busy-warning\""));
    assert_eq!(field("io_workers"), Some("3"));

    let half = io_busy_warnings("io-busy-half", 0.5);
    assert!(half.is_empty(), "half busy warned: {half:?}");
}

/// One pipeline's `spate_checkpoint_pending_batches` lines, sorted. The
/// pipeline label keeps the assertion independent of any other pipeline a
/// sibling test registered in this process.
fn pending_series(rendered: &str, pipeline_name: &str) -> Vec<String> {
    let mut out: Vec<String> = rendered
        .lines()
        .filter(|l| l.starts_with(&format!("{}{{", names::CHECKPOINT_PENDING_BATCHES)))
        .filter(|l| l.contains(&format!(r#"pipeline="{pipeline_name}""#)))
        .map(str::to_owned)
        .collect();
    out.sort();
    out
}

/// The checkpoint labels a pipeline of this name publishes under.
fn checkpoint_labels(pipeline_name: &str) -> String {
    format!(r#"pipeline="{pipeline_name}",component="checkpoint",component_type="checkpoint""#)
}

/// `metrics.per_partition_detail: true` publishes the checkpointer's
/// per-partition pending counts.
///
/// Regression for #334. `CheckpointMetrics::set_partition_pending` and
/// `retain_partitions` had no caller outside the metrics module's own tests,
/// so the flag bought a family that never gained a `partition`-labeled
/// series.
///
/// A labeled gauge registers on its first published value, so a missing call
/// leaves the series *absent* rather than `0`. Presence is what this asserts;
/// both lines render `0` because the run completed with every batch
/// acknowledged.
#[test]
fn per_partition_pending_reaches_the_exposition_when_detail_is_on() {
    let mut cfg = test_config(1);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    cfg.metrics.per_partition_detail = true;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = crate::metrics::install(&crate::metrics::MetricsSettings {
        exporter: crate::metrics::Exporter::Prometheus,
        per_partition_detail: cfg.metrics.per_partition_detail,
        e2e_basis: crate::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter");

    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));

    // Published on the commit tick, so it is there while the run is live and
    // not only on the final commit at shutdown.
    wait_for(
        "the per-partition series is published",
        Duration::from_secs(5),
        || {
            pending_series(&handle.render(), &pipeline_name)
                .iter()
                .any(|l| l.contains(r#"partition="0""#))
        },
    );

    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);

    let rendered = handle.render();
    let labels = checkpoint_labels(&pipeline_name);
    let mut expected = vec![
        format!("{}{{{labels}}} 0", names::CHECKPOINT_PENDING_BATCHES),
        format!(
            r#"{}{{{labels},partition="0"}} 0"#,
            names::CHECKPOINT_PENDING_BATCHES
        ),
    ];
    expected.sort();
    assert_eq!(
        pending_series(&rendered, &pipeline_name),
        expected,
        "the aggregate and the one assigned partition:\n{rendered}"
    );
}

/// Each partition's series carries that partition's own count, and a
/// partition that leaves the assignment is released.
///
/// These are the two assertions presence cannot make. A publisher writing a
/// constant satisfies presence, and so does a `commit_cycle` that never calls
/// `retain_partitions`. Both are caught here: the counts differ across the two
/// partitions, and partition 1 leaves while its batch is still unacknowledged,
/// so without the release its series freezes at `1` for the life of the
/// process rather than reading `0`.
#[test]
fn per_partition_pending_counts_each_partition_and_zeroes_a_revoked_one() {
    let held: Arc<Mutex<Vec<crate::checkpoint::AckRef>>> = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(AtomicBool::new(false));
    let held_c = Arc::clone(&held);
    let release_c = Arc::clone(&release);

    let mut cfg = test_config(1);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    cfg.metrics.per_partition_detail = true;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = crate::metrics::install(&crate::metrics::MetricsSettings {
        exporter: crate::metrics::Exporter::Prometheus,
        per_partition_detail: cfg.metrics.per_partition_detail,
        e2e_basis: crate::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter");

    let h = start_with_config(cfg, move |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::HoldAcks {
            held: Arc::clone(&held_c),
            release: Arc::clone(&release_c),
        },
        batches_seen: 0,
    });

    // Three withheld batches on partition 0 against one on partition 1, so
    // the two series must differ and the aggregate must follow the larger.
    h.script.lock().unwrap().push_back(Script::Assign(vec![
        LaneSpec {
            id: LaneId(0),
            partition: PartitionId(0),
            batches: batches(&[0..10, 10..20, 20..30]),
        },
        LaneSpec {
            id: LaneId(1),
            partition: PartitionId(1),
            batches: batches(std::slice::from_ref(&(0..10))),
        },
    ]));

    let labels = checkpoint_labels(&pipeline_name);
    let name = names::CHECKPOINT_PENDING_BATCHES;
    let p0 = format!(r#"{name}{{{labels},partition="0"}} 3"#);
    let p1 = format!(r#"{name}{{{labels},partition="1"}} 1"#);
    let aggregate = format!("{name}{{{labels}}} 3");
    wait_for(
        "each partition publishes its own count",
        Duration::from_secs(5),
        || {
            let rendered = handle.render();
            rendered.contains(&p0) && rendered.contains(&p1) && rendered.contains(&aggregate)
        },
    );

    // Partition 1 leaves with its batch still held, so the checkpointer drops
    // a tracker whose count is non-zero.
    h.script
        .lock()
        .unwrap()
        .push_back(Script::Revoke(vec![LaneId(1)]));
    let p1_released = format!(r#"{name}{{{labels},partition="1"}} 0"#);
    wait_for(
        "the revoked partition is released and the remaining one is not",
        Duration::from_secs(10),
        || {
            let rendered = handle.render();
            rendered.contains(&p1_released) && rendered.contains(&p0)
        },
    );

    // Releasing retires partition 0's batches so the drain can complete.
    release.store(true, Ordering::Relaxed);
    held.lock().unwrap().clear();
    wait_for("partition 0 commits", Duration::from_secs(5), || {
        h.shared
            .lock()
            .unwrap()
            .committed
            .get(&PartitionId(0))
            .is_some_and(|&w| w >= 30)
    });
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// With `per_partition_detail` off the family is the aggregate alone.
///
/// Asserting the whole list rather than the absence of a `partition=` line
/// also catches a second publisher of the aggregate.
#[test]
fn per_partition_pending_is_absent_when_detail_is_off() {
    let mut cfg = test_config(1);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    assert!(!cfg.metrics.per_partition_detail, "the default is off");
    let handle = crate::metrics::install(&crate::metrics::MetricsSettings {
        exporter: crate::metrics::Exporter::Prometheus,
        per_partition_detail: cfg.metrics.per_partition_detail,
        e2e_basis: crate::metrics::E2eBasis::Ingest,
    })
    .expect("install the exporter");

    let h = start_with_config(cfg, |shared, log| FakeChain {
        shared,
        log,
        mode: ChainMode::Ok,
        batches_seen: 0,
    });
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for("payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });

    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);

    let rendered = handle.render();
    assert_eq!(
        pending_series(&rendered, &pipeline_name),
        vec![format!(
            "{}{{{}}} 0",
            names::CHECKPOINT_PENDING_BATCHES,
            checkpoint_labels(&pipeline_name)
        )],
        "the aggregate alone:\n{rendered}"
    );
}

/// With `pipeline.threads` unset, the pipeline keeps one core per two I/O
/// workers, rounded up, plus one for the controller, and always runs at least
/// one thread.
#[test]
fn the_default_thread_count_reserves_half_a_core_per_io_worker() {
    for (cores, io_threads, threads) in [
        (1, 2, 1),
        (2, 2, 1),
        (4, 1, 2),
        (4, 2, 2),
        (8, 8, 3),
        (32, 2, 30),
        (32, 8, 27),
    ] {
        assert_eq!(
            default_thread_count(cores, io_threads),
            threads,
            "{cores} cores, io_threads: {io_threads}"
        );
    }
}

/// With `pipeline.threads` unset, the runtime starts as many drivers as
/// `default_thread_count` derives from `available_parallelism` and the
/// configured `io_threads`.
#[test]
fn an_unset_thread_count_starts_the_derived_number_of_drivers() {
    // From 4 cores up, `io_threads: 2` tells a reserve of one core per worker
    // apart. `io_threads: 4` tells apart a reserve that ignores the configured
    // count, since 1 and 2 workers both reserve one core.
    for io_threads in [2, 4] {
        let mut cfg = test_config(1);
        cfg.pipeline.threads = None;
        cfg.pipeline.io_threads = io_threads;
        let built = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&built);
        let h = start_with_config(cfg, move |shared, log| {
            counter.fetch_add(1, Ordering::Relaxed);
            FakeChain {
                shared,
                log,
                mode: ChainMode::Ok,
                batches_seen: 0,
            }
        });
        h.script.lock().unwrap().push_back(Script::Drained);
        let report = h.join.join().unwrap().unwrap();
        assert_eq!(report.state, ExitState::Completed);

        let cores = std::thread::available_parallelism().map_or(2, usize::from);
        assert_eq!(
            built.load(Ordering::Relaxed),
            default_thread_count(cores, io_threads),
            "{cores} cores, io_threads: {io_threads}"
        );
    }
}

// ------------------------------------------------------ graceful stop --

/// A config whose stop has `drain_timeout` to run in.
fn stop_config(drain_timeout: Duration) -> PipelineConfig {
    let mut cfg = test_config(1);
    cfg.checkpoint.drain_timeout = drain_timeout;
    cfg
}

/// Start a run whose source answers `reading_on_stop` with `read_on_stop`
/// and logs its lanes' drops, on the stop clock `clock`.
fn start_stopping(
    cfg: PipelineConfig,
    read_on_stop: bool,
    clock: &Arc<crate::clock::TestClock>,
    feed: impl FnOnce(&SharedLog) -> Option<Arc<dyn crate::feed::Feed>>,
    mode_factory: impl Fn(Arc<ChainShared>, Arc<Mutex<SourceLog>>) -> FakeChain + Send + 'static,
) -> Harness {
    let clock = Arc::clone(clock);
    start_prepared(
        cfg,
        test_options(),
        |log| {
            log.read_on_stop = read_on_stop;
            log.log_lane_drops = true;
            log.accept_feed = true;
        },
        move |runtime, log| {
            let runtime = runtime.with_stop_clock(clock);
            match feed(log) {
                Some(feed) => runtime.with_feed(feed),
                None => runtime,
            }
        },
        mode_factory,
    )
}

fn joined(h: &Harness) {
    wait_for("run() to return", Duration::from_secs(10), || {
        h.join.is_finished()
    });
}

/// Pins that startup refuses a drain timeout no longer than the stop reserve
/// for a feed that hands back, under any backend key, before the source
/// opens, and accepts it for one that does not.
#[test]
fn a_feed_refuses_a_drain_timeout_within_the_read_reserve() {
    for backend in ["sqs", "kafka"] {
        let mut cfg = stop_config(crate::feed::STOP_RESERVE);
        cfg.feed = feed_section(&format!("{backend}: {{}}"));
        let h = start_with(
            cfg,
            test_options(),
            |runtime| runtime.with_feed(Arc::new(HandingBack::new(backend))),
            ok_chain,
        );
        let err = config_error_before_open(h);
        assert!(
            err.contains("checkpoint.drain_timeout")
                && err.contains("5s")
                && err.contains(&format!("`{backend}`")),
            "{err}"
        );
    }

    let clock = crate::clock::TestClock::frozen();
    let completes = |cfg: PipelineConfig, feed: Option<Arc<dyn crate::feed::Feed>>| {
        let h = start_stopping(cfg, false, &clock, |_| feed, ok_chain);
        h.script.lock().unwrap().push_back(Script::Drained);
        joined(&h);
        let report = h.join.join().unwrap().expect("the run starts");
        assert_eq!(report.state, ExitState::Completed);
    };
    let mut cfg = stop_config(crate::feed::STOP_RESERVE + Duration::from_millis(1));
    cfg.feed = feed_section("sqs: {}");
    completes(cfg, Some(Arc::new(HandingBack::new("sqs"))));
    for drain_timeout in [crate::feed::STOP_RESERVE, Duration::from_secs(3)] {
        let mut cfg = stop_config(drain_timeout);
        cfg.feed = feed_section("memory: {}");
        completes(cfg, None);
    }
    let mut cfg = stop_config(crate::feed::STOP_RESERVE);
    cfg.feed = feed_section("memory: {}");
    completes(cfg, Some(Arc::new(MemoryFeed::new())));
}

/// Pins that a stop reads on while the source answers `reading_on_stop`
/// until the reserve before the deadline, in the draining state, and calls
/// `stop_reading` at the reserve, before the drivers drop their lanes.
#[test]
fn a_stop_reads_on_until_the_reserve_then_hands_back() {
    let drain_timeout = Duration::from_secs(600);
    let mut cfg = stop_config(drain_timeout);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = install_prometheus(&cfg);
    let clock = crate::clock::TestClock::frozen();
    let h = start_stopping(cfg, true, &clock, |_| None, ok_chain);
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for("payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });

    h.shutdown.trigger();
    wait_for("the read phase", Duration::from_secs(5), || {
        h.shared.lock().unwrap().reading_on_stop_calls > 0
    });
    clock.advance(drain_timeout - crate::feed::STOP_RESERVE - Duration::from_millis(1));
    let asked = h.shared.lock().unwrap().reading_on_stop_calls;
    wait_for("three more passes", Duration::from_secs(5), || {
        h.shared.lock().unwrap().reading_on_stop_calls >= asked + 3
    });
    assert_eq!(h.shared.lock().unwrap().stop_reading_calls, 0);
    let draining = format!(r#"spate_pipeline_state{{pipeline="{pipeline_name}","#);
    let rendered = handle.render();
    assert!(
        rendered.lines().any(|l| l.starts_with(&draining)
            && l.contains(r#"state="draining""#)
            && l.ends_with(" 1")),
        "{rendered}"
    );

    clock.advance(Duration::from_millis(1));
    joined(&h);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    let log = h.shared.lock().unwrap();
    assert_eq!(log.stop_reading_calls, 1);
    let at = |entry: &str| log.log.iter().position(|e| e == entry);
    assert!(
        at("stop_reading") < at("lane-dropped") && at("lane-dropped").is_some(),
        "{:?}",
        log.log
    );
    // With no feed that hands back, the sink drains to the deadline itself.
    assert_eq!(
        *h.sink_budgets.lock().unwrap(),
        vec![crate::feed::STOP_RESERVE]
    );
}

/// Pins that a source with nothing left to read ends the read phase on the
/// first pass, so the stop drains without waiting for the reserve.
#[test]
fn a_stop_with_nothing_left_to_read_drains_at_once() {
    let clock = crate::clock::TestClock::frozen();
    let h = start_stopping(
        stop_config(Duration::from_secs(600)),
        false,
        &clock,
        |_| None,
        ok_chain,
    );
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    h.shutdown.trigger();
    joined(&h);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    let log = h.shared.lock().unwrap();
    assert_eq!((log.reading_on_stop_calls, log.stop_reading_calls), (1, 1));
}

/// Pins that a batch blocked when the stop begins keeps retrying through the
/// read phase and is delivered, where the drain would abandon it for replay.
#[test]
fn a_batch_blocked_during_the_read_phase_is_not_abandoned() {
    let open = Arc::new(AtomicBool::new(false));
    let unblock = Arc::clone(&open);
    let clock = crate::clock::TestClock::frozen();
    let h = start_stopping(
        stop_config(Duration::from_secs(600)),
        true,
        &clock,
        |_| None,
        move |shared, log| FakeChain {
            shared,
            log,
            mode: ChainMode::BlockUntil(Arc::clone(&unblock)),
            batches_seen: 0,
        },
    );
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for(
        "the blocked batch pauses the lane",
        Duration::from_secs(5),
        || !h.shared.lock().unwrap().pauses.is_empty(),
    );
    h.shutdown.trigger();
    let asked = h.shared.lock().unwrap().reading_on_stop_calls;
    wait_for("the read phase", Duration::from_secs(5), || {
        h.shared.lock().unwrap().reading_on_stop_calls >= asked + 3
    });
    open.store(true, Ordering::Relaxed);
    wait_for("the batch is taken", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });
    h.shared.lock().unwrap().read_on_stop = false;
    joined(&h);
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.final_watermarks, vec![(PartitionId(0), 10)]);
}

/// Pins that a failed run reads nothing more and hands back at once.
#[test]
fn a_failure_drain_hands_back_without_reading_on() {
    let clock = crate::clock::TestClock::frozen();
    let h = start_stopping(
        stop_config(Duration::from_secs(600)),
        true,
        &clock,
        |_| None,
        |shared, log| FakeChain {
            shared,
            log,
            mode: ChainMode::FatalAtBatch(1),
            batches_seen: 0,
        },
    );
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    joined(&h);
    let report = h.join.join().unwrap().unwrap();
    assert!(matches!(report.state, ExitState::Failed(_)), "{report:?}");
    let log = h.shared.lock().unwrap();
    assert_eq!((log.reading_on_stop_calls, log.stop_reading_calls), (0, 1));
}

/// Pins that a source whose run ends drained hands nothing back, and that a
/// drain no stop began runs to `drain_timeout` from its own start: the sink
/// to a second before it, the feed's close to the end.
#[test]
fn a_drained_run_does_not_stop_reading() {
    let drain_timeout = Duration::from_secs(600);
    let clock = crate::clock::TestClock::frozen();
    let mut cfg = stop_config(drain_timeout);
    cfg.feed = feed_section("sqs: {}");
    let kept = Arc::new(Mutex::new(None::<Arc<HandingBack>>));
    let keep = Arc::clone(&kept);
    let h = start_stopping(
        cfg,
        true,
        &clock,
        move |_| {
            let feed = Arc::new(HandingBack::new("sqs"));
            *keep.lock().unwrap() = Some(Arc::clone(&feed));
            Some(feed)
        },
        ok_chain,
    );
    h.script.lock().unwrap().push_back(Script::Drained);
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    assert_eq!(h.shared.lock().unwrap().stop_reading_calls, 0);
    let feed = kept.lock().unwrap().take().unwrap();
    assert_eq!(*feed.closes.lock().unwrap(), vec![drain_timeout]);
    assert_eq!(
        *h.sink_budgets.lock().unwrap(),
        vec![drain_timeout - Duration::from_secs(1)]
    );
}

/// Pins that the runtime closes a feed it was given once, after the final
/// commit's flush.
#[test]
fn the_runtime_closes_its_feed_after_the_final_commit() {
    let clock = crate::clock::TestClock::frozen();
    let mut cfg = stop_config(Duration::from_secs(600));
    cfg.feed = feed_section("sqs: {}");
    let kept = Arc::new(Mutex::new(None::<Arc<HandingBack>>));
    let keep = Arc::clone(&kept);
    let h = start_stopping(
        cfg,
        false,
        &clock,
        move |log| {
            let feed = Arc::new(HandingBack::new("sqs").logging_into(Arc::clone(log)));
            *keep.lock().unwrap() = Some(Arc::clone(&feed));
            Some(feed)
        },
        ok_chain,
    );
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for("payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });
    h.shutdown.trigger();
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    let feed = kept.lock().unwrap().take().unwrap();
    assert_eq!(feed.closes.lock().unwrap().len(), 1);
    let log = h.shared.lock().unwrap();
    let close = log.log.iter().position(|e| e == "close");
    let last_flush = log.log.iter().rposition(|e| e == "flush_commits");
    assert!(last_flush.is_some() && close > last_flush, "{:?}", log.log);
    assert_eq!(
        log.log.last().map(String::as_str),
        Some("close"),
        "{:?}",
        log.log
    );
}

/// Pins that the close's bound is what is left of the stop's deadline, read
/// on the stop clock: the whole reserve when the read phase ran to it.
#[test]
fn the_close_gets_the_rest_of_the_deadline() {
    let drain_timeout = Duration::from_secs(600);
    let clock = crate::clock::TestClock::frozen();
    let mut cfg = stop_config(drain_timeout);
    cfg.feed = feed_section("sqs: {}");
    let kept = Arc::new(Mutex::new(None::<Arc<HandingBack>>));
    let keep = Arc::clone(&kept);
    let h = start_stopping(
        cfg,
        true,
        &clock,
        move |_| {
            let feed = Arc::new(HandingBack::new("sqs"));
            *keep.lock().unwrap() = Some(Arc::clone(&feed));
            Some(feed)
        },
        ok_chain,
    );
    h.shutdown.trigger();
    wait_for("the read phase", Duration::from_secs(5), || {
        h.shared.lock().unwrap().reading_on_stop_calls > 0
    });
    clock.advance(drain_timeout - crate::feed::STOP_RESERVE);
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    let feed = kept.lock().unwrap().take().unwrap();
    assert_eq!(
        *feed.closes.lock().unwrap(),
        vec![crate::feed::STOP_RESERVE]
    );
    // The sink drain ends a second early, which leaves that second to the
    // close when the sink drains slower than this one.
    assert_eq!(
        *h.sink_budgets.lock().unwrap(),
        vec![crate::feed::STOP_RESERVE - Duration::from_secs(1)]
    );
}

/// Pins that a close that never settles ends the run at the deadline, with
/// a warning, and leaves the exit state alone.
#[test]
fn a_close_that_never_settles_ends_at_the_deadline() {
    let drain_timeout = Duration::from_secs(600);
    let clock = crate::clock::TestClock::frozen();
    let mut cfg = stop_config(drain_timeout);
    cfg.feed = feed_section("sqs: {}");
    let h = start_stopping(
        cfg,
        true,
        &clock,
        |_| Some(Arc::new(HandingBack::new("sqs").holding_its_close())),
        ok_chain,
    );
    h.shutdown.trigger();
    wait_for("the read phase", Duration::from_secs(5), || {
        h.shared.lock().unwrap().reading_on_stop_calls > 0
    });
    clock.advance(drain_timeout + Duration::from_secs(1));
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    let warnings = h.warnings.0.lock().unwrap().clone();
    assert!(
        warnings
            .iter()
            .any(|fields| fields.iter().any(|(name, value)| {
                name == "message" && value.contains("did not close before the drain deadline")
            })),
        "{warnings:#?}"
    );
}

/// Pins that a source keeping `Source::reading_on_stop`'s default reads
/// nothing on after the stop, so it drains without waiting for the reserve.
#[test]
fn a_source_that_does_not_read_on_drains_at_once() {
    struct Plain(FakeSource);
    impl Source for Plain {
        type Lane = FakeLane;
        fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
            self.0.open(ctx)
        }
        fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<FakeLane>, SourceError> {
            self.0.poll_events(timeout)
        }
        fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
            self.0.commit(watermarks)
        }
    }
    let (inner, shared, _script) = FakeSource::new();
    let chain_shared = Arc::new(ChainShared::default());
    let (sink, _drained) = test_sink();
    let log = Arc::clone(&shared);
    let runtime = PipelineRuntime::new(
        stop_config(Duration::from_secs(600)),
        Plain(inner),
        move |_thread| {
            Box::new(ok_chain(Arc::clone(&chain_shared), Arc::clone(&log)))
                as Box<dyn RunnableChain>
        },
        sink,
        Arc::new(crate::backpressure::InflightBudget::new()),
    )
    .with_options(test_options())
    .with_stop_clock(crate::clock::TestClock::frozen());
    runtime.shutdown_handle().trigger();
    let join = std::thread::spawn(move || runtime.run());
    wait_for("run() to return", Duration::from_secs(10), || {
        join.is_finished()
    });
    assert_eq!(join.join().unwrap().unwrap().state, ExitState::Completed);
}

/// Pins that a source whose `open` failed is not asked to hand back.
#[test]
fn a_source_that_failed_to_open_is_not_asked_to_stop_reading() {
    let clock = crate::clock::TestClock::frozen();
    let h = start_prepared(
        stop_config(Duration::from_secs(600)),
        test_options(),
        |log| log.fail_open = true,
        move |runtime, _| runtime.with_stop_clock(clock),
        ok_chain,
    );
    joined(&h);
    let report = h.join.join().unwrap().unwrap();
    assert!(matches!(report.state, ExitState::Failed(_)), "{report:?}");
    assert_eq!(h.shared.lock().unwrap().stop_reading_calls, 0);
}

/// A lane whose batch blocks, assigned and revoked, with the revocation
/// waiting on the blocked driver when this returns.
fn revoke_a_blocked_lane(h: &Harness) {
    assign_one_lane(h, std::slice::from_ref(&(0..10)));
    wait_for(
        "the blocked batch pauses the lane",
        Duration::from_secs(5),
        || !h.shared.lock().unwrap().pauses.is_empty(),
    );
    h.script
        .lock()
        .unwrap()
        .push_back(Script::Revoke(vec![LaneId(0)]));
    wait_for(
        "the revocation is delivered",
        Duration::from_secs(5),
        || {
            h.shared
                .lock()
                .unwrap()
                .log
                .iter()
                .any(|e| e == "revoke-delivered")
        },
    );
}

fn blocked_chain(shared: Arc<ChainShared>, log: Arc<Mutex<SourceLog>>) -> FakeChain {
    FakeChain {
        shared,
        log,
        mode: ChainMode::BlockForever,
        batches_seen: 0,
    }
}

/// Pins that a stop which arrives while a revocation waits on a driver held
/// by a blocked batch reaches that driver when the source reads nothing on,
/// so the run drains without waiting out the revocation's budget.
#[test]
fn a_stop_during_a_blocked_revocation_drains_at_once() {
    let clock = crate::clock::TestClock::frozen();
    let h = start_stopping(
        stop_config(Duration::from_secs(600)),
        false,
        &clock,
        |_| None,
        blocked_chain,
    );
    revoke_a_blocked_lane(&h);
    h.shutdown.trigger();
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    assert_eq!(h.chain.consumed.load(Ordering::Relaxed), 0);
    assert_revoked_while_blocked(&h.shared);
}

/// The lane's batch blocked and its revocation reached the controller.
fn assert_revoked_while_blocked(shared: &SharedLog) {
    let log = shared.lock().unwrap();
    assert!(!log.pauses.is_empty(), "the batch never blocked");
    let revoked = log.log.iter().position(|e| e == "revoke-delivered");
    let stopped = log.log.iter().position(|e| e == "stop_reading");
    assert!(revoked.is_some() && revoked < stopped, "{:?}", log.log);
}

/// Pins that a stop request names whether it is set and when it was first
/// asked for.
#[test]
fn a_stop_request_reports_its_trigger() {
    let clock = crate::clock::TestClock::frozen();
    let runtime = PipelineRuntime::new(
        test_config(1),
        FakeSource::new().0,
        |_thread| unreachable!("the runtime does not run"),
        test_sink().0,
        Arc::new(crate::backpressure::InflightBudget::new()),
    )
    .with_stop_clock(Arc::clone(&clock) as _);
    let handle = runtime.shutdown_handle();
    assert!(
        format!("{handle:?}").contains("set: false, at: None"),
        "{handle:?}"
    );
    handle.trigger();
    clock.advance(Duration::from_secs(1));
    handle.trigger();
    let printed = format!("{handle:?}");
    assert!(printed.contains("set: true, at: Some("), "{printed}");
}

/// Pins that a revocation during the stop's reading, waiting on a blocked
/// batch, ends at the reserve, so the hand-back and the drain still run.
#[test]
fn a_revocation_during_the_read_phase_ends_at_the_reserve() {
    let drain_timeout = Duration::from_secs(600);
    let clock = crate::clock::TestClock::frozen();
    let h = start_stopping(
        stop_config(drain_timeout),
        true,
        &clock,
        |_| None,
        blocked_chain,
    );
    h.shutdown.trigger();
    wait_for("the read phase", Duration::from_secs(5), || {
        h.shared.lock().unwrap().reading_on_stop_calls > 0
    });
    revoke_a_blocked_lane(&h);
    clock.advance(drain_timeout - crate::feed::STOP_RESERVE);
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    assert_eq!(h.shared.lock().unwrap().stop_reading_calls, 1);
    assert_revoked_while_blocked(&h.shared);
}

/// Pins that the drain deadline counts from the trigger: a stop asked for
/// before the source finished opening leaves the close what is left after
/// the open.
#[test]
fn the_drain_deadline_counts_from_the_trigger() {
    let drain_timeout = Duration::from_secs(600);
    let opening = Duration::from_secs(10);
    let clock = crate::clock::TestClock::frozen();
    let mut cfg = stop_config(drain_timeout);
    cfg.feed = feed_section("sqs: {}");
    let kept = Arc::new(Mutex::new(None::<Arc<HandingBack>>));
    let keep = Arc::clone(&kept);
    let in_open = Arc::clone(&clock);
    let stop_clock = Arc::clone(&clock);
    let h = start_prepared(
        cfg,
        test_options(),
        |log| {
            log.accept_feed = true;
            log.advance_in_open = Some((in_open, opening));
        },
        move |runtime, _| {
            let feed = Arc::new(HandingBack::new("sqs"));
            *keep.lock().unwrap() = Some(Arc::clone(&feed));
            let runtime = runtime.with_stop_clock(stop_clock).with_feed(feed);
            runtime.shutdown_handle().trigger();
            runtime
        },
        ok_chain,
    );
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    let feed = kept.lock().unwrap().take().unwrap();
    assert_eq!(*feed.closes.lock().unwrap(), vec![drain_timeout - opening]);
}

/// Pins that a close the feed rejects ends the run completed, with a
/// warning that carries the feed's reason.
#[test]
fn a_rejected_close_warns_with_its_reason() {
    let clock = crate::clock::TestClock::frozen();
    let mut cfg = stop_config(Duration::from_secs(600));
    cfg.feed = feed_section("sqs: {}");
    let kept = Arc::new(Mutex::new(None::<Arc<HandingBack>>));
    let keep = Arc::clone(&kept);
    let h = start_stopping(
        cfg,
        false,
        &clock,
        move |_| {
            let feed = Arc::new(HandingBack::new("sqs").holding_its_close());
            *keep.lock().unwrap() = Some(Arc::clone(&feed));
            Some(feed)
        },
        ok_chain,
    );
    let feed = kept.lock().unwrap().clone().unwrap();
    h.shutdown.trigger();
    wait_for("the close", Duration::from_secs(5), || {
        !feed.closes.lock().unwrap().is_empty()
    });
    feed.reject_held_closes(&crate::feed::FeedError::new(
        crate::feed::FeedErrorKind::Retryable,
        "2 units not handed back",
    ));
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    let warnings = h.warnings.0.lock().unwrap().clone();
    assert!(
        warnings.iter().any(|fields| {
            fields
                .iter()
                .any(|(name, value)| name == "message" && value.contains("did not close cleanly"))
                && fields.iter().any(|(name, value)| {
                    name == "error" && value.contains("2 units not handed back")
                })
        }),
        "{warnings:#?}"
    );
}

/// Pins that the runtime waits for a close that settles after the wait
/// begins, with no warning.
#[test]
fn a_close_that_settles_later_is_waited_for() {
    let clock = crate::clock::TestClock::frozen();
    let mut cfg = stop_config(Duration::from_secs(600));
    cfg.feed = feed_section("sqs: {}");
    let kept = Arc::new(Mutex::new(None::<Arc<HandingBack>>));
    let keep = Arc::clone(&kept);
    let h = start_stopping(
        cfg,
        false,
        &clock,
        move |_| {
            let feed = Arc::new(HandingBack::new("sqs").holding_its_close());
            *keep.lock().unwrap() = Some(Arc::clone(&feed));
            Some(feed)
        },
        ok_chain,
    );
    let feed = kept.lock().unwrap().clone().unwrap();
    h.shutdown.trigger();
    wait_for("the close", Duration::from_secs(5), || {
        !feed.closes.lock().unwrap().is_empty()
    });
    feed.settle_held_closes();
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    let warnings = h.warnings.0.lock().unwrap().clone();
    assert!(
        !warnings.iter().any(|fields| fields
            .iter()
            .any(|(name, value)| name == "message" && value.contains("did not close"))),
        "{warnings:#?}"
    );
}

/// Pins that a feed that does not hand back leaves the sink drain the whole
/// deadline.
#[test]
fn a_feed_that_does_not_hand_back_leaves_the_sink_the_whole_deadline() {
    let drain_timeout = Duration::from_secs(600);
    let clock = crate::clock::TestClock::frozen();
    let mut cfg = stop_config(drain_timeout);
    cfg.feed = feed_section("memory: {}");
    let h = start_stopping(cfg, false, &clock, |_| None, ok_chain);
    h.script.lock().unwrap().push_back(Script::Drained);
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
    assert_eq!(*h.sink_budgets.lock().unwrap(), vec![drain_timeout]);
}

/// Pins that an assignment during the stop's reading leaves the pipeline in
/// the draining state.
#[test]
fn an_assignment_during_the_read_phase_keeps_the_pipeline_draining() {
    let drain_timeout = Duration::from_secs(600);
    let mut cfg = stop_config(drain_timeout);
    cfg.metrics.exporter = crate::config::MetricsExporter::Prometheus;
    let pipeline_name = cfg.pipeline.name.clone();
    let handle = install_prometheus(&cfg);
    let clock = crate::clock::TestClock::frozen();
    let h = start_stopping(cfg, true, &clock, |_| None, ok_chain);
    h.shutdown.trigger();
    wait_for("the read phase", Duration::from_secs(5), || {
        h.shared.lock().unwrap().reading_on_stop_calls > 0
    });
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for("payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });
    let state = |name: &str| {
        let line = format!(r#"pipeline="{pipeline_name}","#);
        handle
            .render()
            .lines()
            .find(|l| {
                l.starts_with("spate_pipeline_state{")
                    && l.contains(&line)
                    && l.contains(&format!(r#"state="{name}""#))
            })
            .map(str::to_owned)
    };
    assert!(
        state("draining").is_some_and(|l| l.ends_with(" 1")),
        "{:?}",
        state("draining")
    );
    assert!(
        state("running").is_some_and(|l| l.ends_with(" 0")),
        "{:?}",
        state("running")
    );
    clock.advance(drain_timeout);
    joined(&h);
    assert_eq!(h.join.join().unwrap().unwrap().state, ExitState::Completed);
}

/// Pins that SIGTERM begins the graceful stop, so the run drains and
/// returns.
#[cfg(unix)]
#[test]
fn a_sigterm_stops_the_run() {
    // A listener of the test's own replaces the default action first, so a
    // SIGTERM sent before the runtime listens does not end the test process.
    let signals = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _listener = signals.block_on(async { signal(SignalKind::terminate()).unwrap() });
    let mut options = test_options();
    options.handle_signals = true;
    let h = start_with(test_config(1), options, |runtime| runtime, ok_chain);
    assign_one_lane(&h, std::slice::from_ref(&(0..10)));
    wait_for("payloads consumed", Duration::from_secs(5), || {
        h.chain.consumed.load(Ordering::Relaxed) == 10
    });
    let pid = std::process::id().to_string();
    // The runtime's listener may not be installed yet, so the signal is
    // sent until the run ends.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !h.join.is_finished() {
        assert!(Instant::now() < deadline, "SIGTERM did not stop the run");
        let sent = std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap();
        assert!(sent.success());
        std::thread::sleep(Duration::from_millis(50));
    }
    let report = h.join.join().unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.final_watermarks, vec![(PartitionId(0), 10)]);
}
