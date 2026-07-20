//! Coordinated S3 backfill: several pipeline instances share one bounded
//! object-storage backfill over an in-process coordination store, so the run
//! exercises work-stealing and cooperative split handoff. Local filesystem
//! store, real listing, real per-lane fetchers and framing, throttled null
//! sink. No network, no broker.
//!
//! This is a **rebalancing** rig, not a throughput rig. The sink is paced
//! ([`ThrottledNullWriter`]) so the backfill lasts tens of seconds and spans
//! dozens of heartbeat rounds; `wall_s` is therefore pacing-dominated and is
//! reported as a diagnostic, never a throughput. The headline numbers split
//! into the *cost* of rebalancing — `duplicate_rows` (rows re-read across a
//! move) — and its *speed*: `late_share` (how much of the job a late joiner
//! took) plus the two `handoff_*_p50_s` latencies, which decompose
//! time-to-balance into waiting for a victim to answer (`request`) and waiting
//! for its drain to finish (`drain`).
//!
//! Instance 0 starts alone and leases every split (`MAX_IN_FLIGHT` covers the
//! whole job). Late joiners arrive after `JOIN_DELAY_S` and must take work off
//! the leader — a replay-free move when the owner consents, a replaying steal
//! when it cannot. The rig reads every movement counter and latency from the
//! process's own Prometheus **text**, so the same bytes compile and run
//! unchanged on older commits where a family does not yet exist and reads as 0
//! (an absent histogram yields no quantile, reported as 0). That is what lets
//! one rig serve every arm of an A/B.
//!
//! Usage:
//!   s3_backfill_coordinated                      # defaults (see table)
//!   RECORDS_PER_OBJECT=3000 JOIN_DELAY_S=1 \
//!     THROTTLE_ROWS_PER_S=12000 s3_backfill_coordinated   # short smoke run
//!
//! Env (default):
//!   OBJECTS (12)              objects staged, one NDJSON file each
//!   RECORDS_PER_OBJECT (30000)
//!   PAYLOAD (256)             bytes per record
//!   SPLIT_TARGET_MB (1)       planner bin size; 1 MiB + big objects => 1 split/object
//!   INSTANCES (2)             concurrent pipelines sharing the job (>= 2)
//!   JOIN_DELAY_S (3)          late joiners start this long after instance 0
//!   THROTTLE_ROWS_PER_S (6000) per-instance sink pacing
//!   CHECKPOINT_MS (500)       commit interval (also the duplicate window)
//!   LEASE_MS (1000)           coordination lease TTL == store TTL
//!   OP_TIMEOUT_MS (250)       per store op
//!   REPLAN_MS (1000)          leader replan cadence (>= LEASE_MS)
//!   MAX_IN_FLIGHT (16)        splits one instance may lease at once
//!   THREADS (2)               pipeline lanes per instance
//!   HARD_TIMEOUT_S (300)      watchdog: exit(3) if the run wedges
//!   CODEC (none)              one codec only (none | gzip | zstd)
//!   LOG (warn)                telemetry level
//!   RESULTS                   append the JSON record to this path
//!
//! Aggregate medians per arm (keyed by the run's commit) with jq:
//!
//!   jq -s 'map(select(.bench=="s3_backfill_coordinated"))|group_by(.run.commit)|map({commit:.[0].run.commit,reps:length,wall_s:(map(.metrics.wall_s.value)|sort|.[(length-1)/2|floor]),dup_rows:(map(.metrics.duplicate_rows.value)|sort|.[(length-1)/2|floor]),late_share:(map(.metrics.late_share.value)|sort|.[(length-1)/2|floor])})' RESULTS.jsonl
//!
//!   jq -s 'map(select(.bench=="s3_backfill_coordinated"))|group_by(.run.commit)|map({commit:.[0].run.commit,steals:(map(.metrics.steals_total.value)|sort|.[(length-1)/2|floor]),handoffs:(map(.metrics.handoffs_total.value)|sort|.[(length-1)/2|floor])})' RESULTS.jsonl
//!
//!   jq -s 'map(select(.bench=="s3_backfill_coordinated"))|group_by(.run.commit)|map({commit:.[0].run.commit,request_p50:(map(.metrics.handoff_request_p50_s.value)|sort|.[(length-1)/2|floor]),drain_p50:(map(.metrics.handoff_drain_p50_s.value)|sort|.[(length-1)/2|floor])})' RESULTS.jsonl
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::s3data::stage;
use benchmarks::synthetic::ThrottledNullWriter;
use benchmarks::{env_str, env_u64, prom};
use bytes::BytesMut;
use etl_coordination::store::memory::MemoryStore;
use etl_coordination::{CoordinationConfig, StoreCoordinator};
use etl_core::backpressure::InflightBudget;
use etl_core::config::PipelineConfig;
use etl_core::deser::{BytesPassthrough, Owned};
use etl_core::error::SinkError;
use etl_core::metrics::{
    ComponentLabels, CoordinationMetrics, E2eBasis, Exporter, MetricsSettings, SinkShardMetrics,
};
use etl_core::ops::{ChunkConfig, chain_owned};
use etl_core::pipeline::{ExitState, PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl_core::record::Record;
use etl_core::sink::{KeyHashRouter, RowEncoder, SinkPool, SinkPoolConfig, shard_queues};
use etl_s3::S3Source;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Length-prefixed rows, like the solo rig's encoder. The rows are discarded
/// by the null sink, so only their count matters.
#[derive(Clone)]
struct OwnedBytesEncoder;

impl RowEncoder<Owned<Vec<u8>>> for OwnedBytesEncoder {
    fn encode<'buf>(&mut self, rec: &Record<Vec<u8>>, buf: &mut BytesMut) -> Result<(), SinkError> {
        use bytes::BufMut;
        buf.put_u32_le(u32::try_from(rec.payload.len()).unwrap_or(u32::MAX));
        buf.put_slice(&rec.payload);
        Ok(())
    }
}

/// Coordination + pacing knobs shared by every instance (byte-identical config
/// is a job-fingerprint requirement; only the instance id differs).
#[derive(Clone, Copy)]
struct Tuning {
    lease: Duration,
    op_timeout: Duration,
    replan: Duration,
    max_in_flight: u32,
    throttle_rows_per_s: f64,
}

/// One instance's terminal state and its durably-written row count.
struct InstanceOutcome {
    state: ExitState,
    rows: u64,
}

/// Run one coordinated pipeline instance to completion over the shared store.
fn run_instance(i: usize, yaml: &str, store: MemoryStore, t: Tuning) -> InstanceOutcome {
    let config = PipelineConfig::from_str(yaml).expect("config");
    let source_section = config.source.clone();
    let instance_id = format!("bench-{i}");

    let shards = 1usize;
    let writer = Arc::new(ThrottledNullWriter::new(t.throttle_rows_per_s));
    let endpoints: Vec<Vec<()>> = (0..shards).map(|_| vec![()]).collect();
    let (queues, receivers) = shard_queues(shards, 64);
    let budget = Arc::new(InflightBudget::new());
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("io runtime");

    // Coordination metrics bind to the Prometheus recorder installed in main;
    // per-instance labels keep the series distinct, and `prom::value` sums them.
    let coord_labels = ComponentLabels::new(
        "s3-backfill-coordinated",
        instance_id.clone(),
        "coordination",
    );
    let coordinator = StoreCoordinator::new(
        store,
        CoordinationConfig {
            lease_duration: t.lease,
            op_timeout: t.op_timeout,
            replan_interval: t.replan,
            instance_id: Some(instance_id.clone()),
            max_in_flight: t.max_in_flight,
            ..CoordinationConfig::default()
        },
        io.handle().clone(),
        Some(CoordinationMetrics::new(&coord_labels)),
    )
    .expect("coordinator builds");

    let sink_labels = ComponentLabels::new("s3-backfill-coordinated", format!("sink-{i}"), "null");
    let sink_metrics = vec![SinkShardMetrics::new(
        &sink_labels,
        0,
        &["null-0".to_string()],
        E2eBasis::Ingest,
    )];
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.max_rows = 1024;
        cfg.batch.linger = Duration::from_millis(5);
        cfg
    };
    let pool = SinkPool::spawn(
        Arc::clone(&writer),
        endpoints,
        receivers,
        pool_cfg,
        Arc::clone(&budget),
        sink_metrics,
        "s3-backfill-coordinated",
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: vec![queues.clone()],
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        probe: None,
    };

    let source = S3Source::from_component_config(&source_section, io.handle().clone())
        .expect("source")
        .with_framer(|| Box::new(etl_json::NdjsonFramer::new(64 << 20)))
        .with_coordinator(Box::new(coordinator));

    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| {
        chain_owned::<Vec<u8>, _>(BytesPassthrough)
            .sink(
                OwnedBytesEncoder,
                KeyHashRouter,
                ChunkConfig::default(),
                chain_queues.clone(),
                Arc::clone(&chain_budget),
            )
            .build()
    };

    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });
    let exit = runtime.run().expect("run");
    // `io` must outlive the run; it drops here, after the pool has drained.
    InstanceOutcome {
        state: exit.state,
        rows: writer.rows(),
    }
}

fn main() {
    etl_core::telemetry::init(
        etl_core::telemetry::LogFormat::Pretty,
        &env_str("LOG", "warn"),
    );

    // Install the Prometheus recorder FIRST: the coordination/sink metric
    // handles bind to the recorder present at construction. The pipeline YAML
    // keeps `exporter: none`, which installs no recorder and never claims the
    // once-per-process global slot, so this handle stays authoritative.
    let metrics = etl_core::metrics::install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        listen: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..MetricsSettings::default()
    })
    .expect("install prometheus recorder");

    let objects = env_u64("OBJECTS", 12) as usize;
    let records = env_u64("RECORDS_PER_OBJECT", 30_000) as usize;
    let payload = env_u64("PAYLOAD", 256) as usize;
    let split_target_mb = env_u64("SPLIT_TARGET_MB", 1);
    let instances = env_u64("INSTANCES", 2) as usize;
    let join_delay = Duration::from_secs(env_u64("JOIN_DELAY_S", 3));
    let throttle_rows_per_s = env_u64("THROTTLE_ROWS_PER_S", 6000) as f64;
    let checkpoint_ms = env_u64("CHECKPOINT_MS", 500);
    let lease_ms = env_u64("LEASE_MS", 1000);
    let op_timeout_ms = env_u64("OP_TIMEOUT_MS", 250);
    let replan_ms = env_u64("REPLAN_MS", 1000);
    let max_in_flight = env_u64("MAX_IN_FLIGHT", 16) as u32;
    let threads = env_u64("THREADS", 2) as usize;
    let hard_timeout = Duration::from_secs(env_u64("HARD_TIMEOUT_S", 300));
    let codec = env_str("CODEC", "none");

    assert!(
        instances >= 2,
        "coordination needs at least 2 instances to rebalance, got {instances}"
    );
    let total_records = (objects * records) as u64;

    // Stage the "bucket" once; every instance points at the same prefix.
    let dir = tempfile::tempdir().expect("tempdir");
    stage(dir.path(), &codec, objects, records, payload);

    let lease = Duration::from_millis(lease_ms);
    let store = MemoryStore::new(lease);
    let yaml = format!(
        r#"
pipeline: {{ name: s3-backfill-coordinated, threads: {threads} }}
checkpoint: {{ interval: {checkpoint_ms}ms }}
metrics: {{ exporter: none, listen: "127.0.0.1:0" }}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: {split_target_mb}MiB
sink: {{ nullsink: {{}} }}
"#,
        data = dir.path().join("data").display(),
    );

    let tuning = Tuning {
        lease,
        op_timeout: Duration::from_millis(op_timeout_ms),
        replan: Duration::from_millis(replan_ms),
        max_in_flight,
        throttle_rows_per_s,
    };

    // Watchdog: a pacing-dominated run finishes in tens of seconds; a wedged
    // coordinator would otherwise hang forever. Detached — main returning kills
    // it.
    std::thread::spawn(move || {
        std::thread::sleep(hard_timeout);
        eprintln!(
            "HARD_TIMEOUT_S exceeded ({hard_timeout:?}); the run did not complete — aborting"
        );
        std::process::exit(3);
    });

    let t0 = Instant::now();
    // Instance 0 starts alone and leases every split; late joiners arrive after
    // JOIN_DELAY_S and must take work off it.
    let mut handles = Vec::with_capacity(instances);
    {
        let store = store.clone();
        let yaml = yaml.clone();
        handles.push(std::thread::spawn(move || {
            run_instance(0, &yaml, store, tuning)
        }));
    }
    std::thread::sleep(join_delay);
    for i in 1..instances {
        let store = store.clone();
        let yaml = yaml.clone();
        handles.push(std::thread::spawn(move || {
            run_instance(i, &yaml, store, tuning)
        }));
    }

    let outcomes: Vec<InstanceOutcome> = handles
        .into_iter()
        .map(|h| h.join().expect("instance thread"))
        .collect();
    let wall = t0.elapsed().as_secs_f64();

    // ── Honest assertions ────────────────────────────────────────────────
    // A run that fails to exercise #58 must abort loudly, not emit a record
    // that reads like a pass.
    for (i, o) in outcomes.iter().enumerate() {
        assert_eq!(
            o.state,
            ExitState::Completed,
            "instance bench-{i} did not finish (state {:?})",
            o.state
        );
    }
    let rows_written: u64 = outcomes.iter().map(|o| o.rows).sum();
    assert!(
        rows_written >= total_records,
        "conservation: instances wrote {rows_written} rows but {total_records} were \
         staged — the union does not cover the job"
    );
    for (i, o) in outcomes.iter().enumerate().skip(1) {
        assert!(
            o.rows > 0,
            "late joiner bench-{i} wrote 0 rows: it never took work, so the run does \
             not exercise a rebalance — raise JOIN_DELAY_S or lower THROTTLE_ROWS_PER_S"
        );
    }
    let late_rows: u64 = outcomes[1..].iter().map(|o| o.rows).sum();

    // Movement counters from the process's own Prometheus text. Text parsing
    // only: this rig references no feature-branch Rust API, so the same bytes
    // compile and run on `main`, where the handoffs family is absent and reads 0.
    let text = metrics.render();
    // `reason="stolen"` on the acquisitions family, summed across label sets.
    let steals_total = prom::value(
        &text,
        "etl_coordination_acquisitions_total",
        r#"reason="stolen""#,
    )
    .unwrap_or(0.0);
    // A cooperative handoff completes as `outcome="granted"`; read that outcome
    // only (one granted move) so the count is comparable across arms and never
    // double-counts a request that later aborted into a fallback steal.
    let handoffs_total = prom::value(
        &text,
        "etl_coordination_handoffs_total",
        r#"outcome="granted""#,
    )
    .unwrap_or(0.0);
    assert!(
        steals_total + handoffs_total >= 1.0,
        "no split moved (steals={steals_total}, handoffs={handoffs_total}) — the run \
         did not exercise coordination; adjust the geometry so a late joiner must \
         take work"
    );
    // Time-to-balance, decomposed. `request` spans from going under target to
    // claiming a granted split — it absorbs every unanswered round, so it is
    // where request-admission pacing shows up. `drain` is the victim's
    // stop-commit-release, the term concurrent grants overlap. Reported as
    // medians of the in-process histogram; an arm whose build lacks the family
    // reports 0, exactly like the counters above.
    let handoff_request_p50 = prom::histogram_quantile_labeled(
        &text,
        "etl_coordination_handoff_duration_seconds",
        r#"phase="request""#,
        0.5,
    )
    .unwrap_or(0.0);
    let handoff_drain_p50 = prom::histogram_quantile_labeled(
        &text,
        "etl_coordination_handoff_duration_seconds",
        r#"phase="drain""#,
        0.5,
    )
    .unwrap_or(0.0);

    let duplicate_rows = rows_written - total_records;
    let duplicate_pct = if total_records > 0 {
        100.0 * duplicate_rows as f64 / total_records as f64
    } else {
        0.0
    };
    let late_share = if total_records > 0 {
        late_rows as f64 / total_records as f64
    } else {
        0.0
    };
    let exit_states: Vec<String> = outcomes.iter().map(|o| format!("{:?}", o.state)).collect();

    Report::measurement("s3_backfill_coordinated")
        .variant("instances", instances as u64)
        .variant("objects", objects as u64)
        .variant("records_per_object", records as u64)
        .variant("payload_bytes", payload as u64)
        .variant("split_target_mb", split_target_mb)
        .variant("threads", threads as u64)
        .variant("codec", codec.clone())
        .variant("max_in_flight", u64::from(max_in_flight))
        .variant("throttle_rows_per_s", throttle_rows_per_s as u64)
        .variant("join_delay_s", join_delay.as_secs())
        .variant("checkpoint_ms", checkpoint_ms)
        .variant("lease_ms", lease_ms)
        .metric("wall_s", Metric::minimize(wall, "s"))
        .metric(
            "duplicate_rows",
            Metric::minimize(duplicate_rows as f64, "rows"),
        )
        .metric("duplicate_pct", Metric::minimize(duplicate_pct, "%"))
        .metric("late_share", Metric::maximize(late_share, "ratio"))
        .metric(
            "steals_total",
            Metric::minimize(steals_total, "acquisitions"),
        )
        .metric(
            "handoffs_total",
            Metric::maximize(handoffs_total, "handoffs"),
        )
        .metric(
            "handoff_request_p50_s",
            Metric::minimize(handoff_request_p50, "s"),
        )
        .metric(
            "handoff_drain_p50_s",
            Metric::minimize(handoff_drain_p50, "s"),
        )
        .metric(
            "records_total",
            Metric::maximize(total_records as f64, "records"),
        )
        .metric(
            "rows_written_total",
            Metric::maximize(rows_written as f64, "rows"),
        )
        .note(format!(
            "throttled sink — wall is pacing-dominated, not throughput; \
             exit states: {}",
            exit_states.join(", ")
        ))
        .emit();
}
