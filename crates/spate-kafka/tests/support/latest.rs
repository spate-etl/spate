//! Whole runs over a partition with no committed offset, shared by the mock
//! cluster and real broker suites. Each body takes the policy, so `earliest` is
//! its control. The caller creates the topic, since the mock broker takes no
//! admin request: two partitions, or one for a removed consumer.

use super::{assert_no_gap, await_assignment, committed_by, produce_to};
use rdkafka::Offset;
use spate_core::checkpoint::Checkpointer;
use spate_core::config::PipelineConfig;
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, PipelineRuntime, RuntimeOptions, ShutdownHandle};
use spate_core::sink::{KeyHashRouter, SinkPoolConfig};
use spate_core::source::{PayloadBatch, Source, SourceCtx, SourceLane};
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test::{
    BytesPassthrough, CaptureSink, PipelineRun, ScriptedResult, SinkScript, TestEncoder,
    WriteOutcome, capture_sink, decode_rows, passthrough_runtime, unique_name, wait_until,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(120);
/// A burst of records on each tick of a wait that has to keep producing.
const ROUND: i64 = 20;
/// A sink that stays down outlasts any drain here.
const DOWN_WRITES: usize = 100_000;

/// A source on `policy` whose group gives up on a stalled member at 6 s.
pub(crate) fn config(brokers: &str, topic: &str, group: &str, policy: &str) -> KafkaSourceConfig {
    let mut cfg = KafkaSourceConfig::new(brokers, topic, group);
    cfg.commit_interval = Duration::from_millis(200);
    cfg.startup_timeout = DEADLINE;
    cfg.statistics_interval = Duration::ZERO;
    cfg.rdkafka = BTreeMap::from([
        ("auto.offset.reset".to_string(), policy.to_string()),
        ("session.timeout.ms".to_string(), "6000".to_string()),
        ("max.poll.interval.ms".to_string(), "6000".to_string()),
        ("heartbeat.interval.ms".to_string(), "2000".to_string()),
    ]);
    cfg
}

/// A pipeline run over a capture sink.
pub(crate) struct Run {
    shutdown: ShutdownHandle,
    run: PipelineRun,
    script: SinkScript,
}

fn pipeline() -> (Pipeline, CaptureSink, SinkScript) {
    let name = unique_name("latest");
    let yaml = format!(
        "pipeline: {{ name: {name}, threads: 2, io_threads: 1 }}\n\
         admin: {{ listen: none }}\n\
         checkpoint: {{ drain_timeout: 200ms }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ memory: {{}} }}\n\
         sink: {{ capture: {{ chunk: {{ target_bytes: 64 }} }} }}\n"
    );
    let mut pool = SinkPoolConfig::default();
    pool.batch.max_rows = 4;
    pool.batch.linger = Duration::from_millis(10);
    let (sink, script) = capture_sink(1, 1);
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(&yaml).expect("config")).expect("builder");
    (pipeline, sink.with_pool_config(pool), script)
}

fn options() -> RuntimeOptions {
    RuntimeOptions {
        max_records: 4,
        ..RuntimeOptions::default()
    }
}

impl Run {
    fn spawn(runtime: PipelineRuntime<KafkaSource>, script: SinkScript) -> Run {
        let shutdown = runtime.shutdown_handle();
        let run = PipelineRun::spawn(move || runtime.run());
        Run {
            shutdown,
            run,
            script,
        }
    }

    /// A run whose sink acknowledges every write.
    pub(crate) fn start(cfg: KafkaSourceConfig) -> Run {
        let (pipeline, sink, script) = pipeline();
        let source = KafkaSource::new(cfg);
        let runtime = passthrough_runtime(pipeline, source, sink, TestEncoder, options());
        Run::spawn(runtime, script)
    }

    /// A run whose sink refuses every write, so no watermark moves.
    pub(crate) fn start_with_sink_down(cfg: KafkaSourceConfig) -> Run {
        let (pipeline, sink, script) = pipeline();
        for _ in 0..DOWN_WRITES {
            script.enqueue_global(WriteOutcome::retryable("sink down"));
        }
        let source = KafkaSource::new(cfg);
        let runtime = passthrough_runtime(pipeline, source, sink, TestEncoder, options());
        Run::spawn(runtime, script)
    }

    /// A run whose operator stops the pipeline on a record whose payload begins
    /// with `poison`.
    pub(crate) fn start_stopping_on_poison(cfg: KafkaSourceConfig) -> Run {
        let (pipeline, sink, script) = pipeline();
        let runtime = pipeline
            .sink(sink)
            .expect("sink")
            .chains(|ctx| {
                let chunk = ctx.chunk();
                chain_owned::<Vec<u8>, _>(BytesPassthrough)
                    .try_map(
                        |payload: Vec<u8>| {
                            if payload.starts_with(b"poison") {
                                Err("poison".to_owned())
                            } else {
                                Ok(payload)
                            }
                        },
                        ErrorPolicy::Fail,
                    )
                    .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
                    .build()
            })
            .runtime_options(RuntimeOptions {
                handle_signals: false,
                ..options()
            })
            .into_runtime(KafkaSource::new(cfg))
            .expect("runtime");
        Run::spawn(runtime, script)
    }

    fn offsets(&self, partition: i32, only_written: bool) -> BTreeSet<i64> {
        let prefix = format!("{partition}:");
        let mut out = BTreeSet::new();
        for write in self.script.writes() {
            if only_written && write.result != ScriptedResult::Ok {
                continue;
            }
            for row in decode_rows(&write.payload) {
                let row = String::from_utf8(row).expect("utf-8 payload");
                if let Some(offset) = row.strip_prefix(&prefix) {
                    out.insert(offset.parse::<i64>().expect("an offset"));
                }
            }
        }
        out
    }

    /// The offsets of `partition` a sink durably wrote.
    pub(crate) fn written(&self, partition: i32) -> BTreeSet<i64> {
        self.offsets(partition, true)
    }

    /// Whether any write of `partition` was attempted, acknowledged or not.
    fn attempted(&self, partition: i32) -> bool {
        !self.offsets(partition, false).is_empty()
    }

    /// Stop the run and wait for it to exit.
    pub(crate) fn stop(self) {
        self.shutdown.trigger();
        self.run.wait_exit(DEADLINE).expect("the run stops").ok();
    }

    /// Wait up to `timeout` for a run that ends on its own.
    pub(crate) fn wait_exit(
        &self,
        timeout: Duration,
    ) -> Option<Result<spate_core::pipeline::ExitReport, spate_core::pipeline::StartError>> {
        self.run.wait_exit(timeout)
    }

    /// Wait for a run that stops on its own.
    fn stopped(&self) -> bool {
        self.run.wait_exit(Duration::from_millis(50)).is_some()
    }
}

/// Produce to `partition` from `next` until `run` has handed one of its records
/// to the sink, which shows the run's start is settled and it reads.
fn await_reading(run: &Run, brokers: &str, topic: &str, partition: i32, next: &mut i64) {
    let mut tick = 0;
    wait_until(DEADLINE, "the run to read", || {
        if tick % 40 == 0 {
            produce_to(brokers, topic, partition, *next..*next + 1);
            *next += 1;
        }
        tick += 1;
        run.attempted(partition)
    });
}

fn offset_of(offset: Offset) -> Option<i64> {
    match offset {
        Offset::Offset(offset) => Some(offset),
        _ => None,
    }
}

/// Wait until the group's commit on `partition` reaches `upto`, and return it.
fn await_commit(
    brokers: &str,
    group: &str,
    topic: &str,
    partitions: i32,
    partition: i32,
    upto: i64,
) -> i64 {
    let mut at = None;
    wait_until(
        DEADLINE,
        "the group's commit to reach the produced end",
        || {
            let committed = committed_by(brokers, group, topic, partitions);
            at = offset_of(committed[usize::try_from(partition).expect("partition")]);
            at.is_some_and(|at| at >= upto)
        },
    );
    at.expect("a commit")
}

/// Route: a quiet partition, a clean restart, records written while it is down.
/// Partition 1 shows each run reads; partition 0 stays quiet until the stop.
pub(crate) fn a_quiet_partition_across_a_restart(brokers: &str, topic: &str, policy: &str) {
    let group = unique_name("quiet");
    let mut sentinel = 0;
    let first = Run::start(config(brokers, topic, &group, policy));
    await_reading(&first, brokers, topic, 1, &mut sentinel);
    first.stop();

    produce_to(brokers, topic, 0, 0..ROUND);

    let second = Run::start(config(brokers, topic, &group, policy));
    await_reading(&second, brokers, topic, 1, &mut sentinel);
    produce_to(brokers, topic, 0, ROUND..2 * ROUND);
    let upto = await_commit(brokers, &group, topic, 2, 0, 2 * ROUND);
    let written = second.written(0);
    second.stop();
    assert_no_gap(0, 0, upto, &written);
}

/// Route: a new group with its sink down commits nothing past its start, and a
/// restart reads from that start.
pub(crate) fn a_sink_down_across_a_restart(brokers: &str, topic: &str, policy: &str) {
    let group = unique_name("sinkdown");
    let mut sentinel = 0;
    let first = Run::start_with_sink_down(config(brokers, topic, &group, policy));
    await_reading(&first, brokers, topic, 1, &mut sentinel);
    produce_to(brokers, topic, 0, 0..ROUND);
    first.stop();
    if policy == "latest" {
        assert_eq!(
            committed_by(brokers, &group, topic, 2)[0],
            Offset::Offset(0),
            "the first run's start is on the group"
        );
    }

    let second = Run::start(config(brokers, topic, &group, policy));
    await_reading(&second, brokers, topic, 1, &mut sentinel);
    produce_to(brokers, topic, 0, ROUND..2 * ROUND);
    let upto = await_commit(brokers, &group, topic, 2, 0, 2 * ROUND);
    let written = second.written(0);
    second.stop();
    assert_no_gap(0, 0, upto, &written);
}

/// Route: a second process joins while the first has no commit, since its sink
/// is down. No restart and no removal.
pub(crate) fn a_second_process_joining(brokers: &str, topic: &str, policy: &str) {
    let group = unique_name("joining");
    let mut next = [0_i64; 2];
    let first = Run::start_with_sink_down(config(brokers, topic, &group, policy));
    // Partition 0 shows the run reads. Records produced before its start are
    // what `latest` skips, so its first attempt is the bound below. Partition 1
    // has none, since the run has started by now.
    await_reading(&first, brokers, topic, 0, &mut next[0]);
    let starts = [
        first
            .offsets(0, false)
            .first()
            .copied()
            .expect("an attempt"),
        0,
    ];
    for (p, next) in next.iter_mut().enumerate() {
        let partition = i32::try_from(p).expect("partition");
        produce_to(brokers, topic, partition, *next..*next + ROUND);
        *next += ROUND;
    }

    let second = Run::start(config(brokers, topic, &group, policy));
    let mut moved = None;
    let mut tick = 0;
    wait_until(
        DEADLINE,
        "the second process to write a moved partition",
        || {
            if tick % 40 == 0 {
                for (p, next) in next.iter_mut().enumerate() {
                    let partition = i32::try_from(p).expect("partition");
                    produce_to(brokers, topic, partition, *next..*next + ROUND);
                    *next += ROUND;
                }
            }
            tick += 1;
            moved = (0..2).find(|p| {
                !second
                    .written(i32::try_from(*p).expect("partition"))
                    .is_empty()
            });
            moved.is_some()
        },
    );
    let moved = moved.expect("a moved partition");
    let partition = i32::try_from(moved).expect("partition");
    let upto = await_commit(brokers, &group, topic, 2, partition, next[moved]);
    let mut written = first.written(partition);
    written.extend(second.written(partition));
    first.stop();
    second.stop();
    assert_no_gap(partition, starts[moved], upto, &written);
}

/// Produce and read `source`'s one lane until it has delivered a record, and
/// return the first offset. Nothing read is acknowledged here.
fn first_read<L: SourceLane>(
    source: &mut KafkaSource,
    lane: &mut L,
    brokers: &str,
    topic: &str,
    next: &mut i64,
) -> i64
where
    KafkaSource: Source<Lane = L>,
{
    let mut first = None;
    let mut tick = 0;
    wait_until(DEADLINE, "the lane to deliver", || {
        if tick % 40 == 0 {
            produce_to(brokers, topic, 0, *next..*next + ROUND);
            *next += ROUND;
        }
        tick += 1;
        let _ = source.poll_events(Duration::from_millis(5));
        if let Some(mut batch) = lane.poll(64, Duration::from_millis(5)).expect("lane poll")
            && let Some(raw) = batch.next_payload()
        {
            first = Some(raw.offset);
        }
        first.is_some()
    });
    first.expect("a record")
}

/// Route: one consumer per process. The first stalls past
/// `max.poll.interval.ms` holding records it read and did not acknowledge, the
/// group removes it, and the next member is assigned the partition.
pub(crate) fn a_removed_single_consumer(brokers: &str, topic: &str, policy: &str) {
    let group = unique_name("removed");
    let mut next = 0;
    let first_cp = Checkpointer::new();
    let mut first = KafkaSource::new(config(brokers, topic, &group, policy));
    first.open(SourceCtx::new(first_cp.handle())).expect("open");
    let mut first_lanes = await_assignment(&mut first, DEADLINE);
    let first_at = first_read(&mut first, &mut first_lanes[0], brokers, topic, &mut next);

    // The first member polls no more from here.
    let second_cp = Checkpointer::new();
    let mut second = KafkaSource::new(config(brokers, topic, &group, policy));
    second
        .open(SourceCtx::new(second_cp.handle()))
        .expect("open");
    let mut second_lanes = await_assignment(&mut second, DEADLINE);
    let second_at = first_read(&mut second, &mut second_lanes[0], brokers, topic, &mut next);
    assert!(
        second_at <= first_at,
        "the next member started at {second_at}, past the record at {first_at} the first read \
         and did not acknowledge"
    );
    drop(second_lanes);
    drop(second);
    drop(first_lanes);
    drop(first);
}

/// A record the chain fails on stops the run, and a restart stops at the same
/// record with nothing committed past it.
pub(crate) fn a_failing_record_across_a_restart(brokers: &str, topic: &str) {
    let group = unique_name("failing");
    let mut sentinel = 0;
    let first = Run::start_stopping_on_poison(config(brokers, topic, &group, "latest"));
    await_reading(&first, brokers, topic, 1, &mut sentinel);

    super::produce_payload(brokers, topic, 0, b"poison");
    produce_to(brokers, topic, 0, 1..1 + ROUND);
    wait_until(DEADLINE, "the first run to stop", || first.stopped());

    let second = Run::start_stopping_on_poison(config(brokers, topic, &group, "latest"));
    wait_until(DEADLINE, "the second run to stop at the record", || {
        second.stopped()
    });
    assert_eq!(
        committed_by(brokers, &group, topic, 2)[0],
        Offset::Offset(0),
        "nothing is committed past the record the chain fails on"
    );
    assert!(second.written(0).is_empty());
}
