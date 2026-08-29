//! Runtime integration tests over scripted fakes: a controllable Source
//! with real `AckIssuer` plumbing, a counting chain, and a mock sink
//! bundle. The Checkpointer, DrainBarrier, and WatermarkController in the
//! loop are the real implementations.

use super::fakes::*;
use super::*;
use crate::config::PipelineConfig;
use crate::error::{ErrorClass, SourceError};
use crate::metrics::names;
use crate::ops::RunnableChain;
use crate::pipeline::runtime::PipelineRuntime;
use crate::record::PartitionId;
use crate::sink::shard_queues;
use crate::source::LaneId;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
            queues: vec![queues],
            drain,
            probe: None,
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
    start_with_options(config, test_options(), mode_factory)
}

fn start_with_options(
    config: PipelineConfig,
    options: RuntimeOptions,
    mode_factory: impl Fn(Arc<ChainShared>, Arc<Mutex<SourceLog>>) -> FakeChain + Send + 'static,
) -> Harness {
    let (source, shared, script) = FakeSource::new();
    let chain_shared = Arc::new(ChainShared::default());
    let (sink, drained) = test_sink();
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
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());
    Harness {
        budget: budget_for_test,
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
        // No flush/commit *ordering* assertion here. The controller also
        // broadcasts FlushNow on every commit tick (20ms in tests), so the
        // log interleaves periodic flushes and commits nondeterministically,
        // and this chain resolves acks on consumption, so the equality below
        // cannot see whether revocation flushed before committing. That
        // ordering is pinned by `revocation_flushes_parked_acks_before_committing`,
        // whose chain parks acks until a flush. This test keeps the
        // ack-on-consume half: everything consumed was committed, and nothing
        // consumed was lost.
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
    h.script.lock().unwrap().push_back(Script::Drained);
    let report = h.join.join().unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("drained exit with pending acks must fail, not complete");
    };
    assert_eq!(failure.component, "source");
    assert!(
        failure.reason.contains("unacknowledged"),
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
    std::thread::sleep(Duration::from_millis(50));
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
            .filter(|l| l.starts_with(names::BACKPRESSURE_INFLIGHT_BYTES))
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
