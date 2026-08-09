//! One instance per process, sharing one backfill over the durable NATS
//! JetStream store — the same binary, run twice, in two terminals.
//!
//! The other coordinated examples put two instances in one process over
//! [`MemoryStore`](spate::coordination::store::memory::MemoryStore), which
//! shows the protocol but not the deployment. This is the deployment: the
//! process is the instance, its identity comes from the environment the
//! way a pod's does, and the fleet meets in a store that outlives every
//! member of it. Nothing in the code below knows how many peers exist.
//!
//! The work being divided is a bounded backfill of a `file://` prefix —
//! 96 small NDJSON objects staged into the temp directory on first run,
//! packed into six splits at the 1 MiB target — so the only thing to stand
//! up is NATS. Whichever instance holds leadership lists the prefix once
//! and writes the split table; every instance leases the splits it is
//! assigned, reads them straight from the split descriptors, and commits
//! fenced per-split progress. Each exits `Completed` once every split is
//! complete, and the union of the two covers the whole prefix — at-least-
//! once, so a forced handoff can replay a tail but never drop one.
//!
//! The chain paces itself on purpose (see `PACE`): without that the
//! backfill is over before you can reach the second terminal.
//!
//! # Run it
//!
//! A NATS server with JetStream enabled, version 2.11 or newer — the
//! store needs per-message age limits and KV limit markers, and the worker
//! refuses anything older at startup:
//!
//! ```sh
//! docker run --rm -p 4222:4222 nats:2.11 --jetstream
//! NATS_URL=nats://127.0.0.1:4222 POD_NAME=worker-a cargo run -p spate --features s3,json,coordination-nats --example nats_coordinated_backfill
//! NATS_URL=nats://127.0.0.1:4222 POD_NAME=worker-b cargo run -p spate --features s3,json,coordination-nats --example nats_coordinated_backfill
//! ```
//!
//! Start the second one while the first is still working. The leader
//! recomputes the assignment the moment the new member appears and revokes
//! the newcomer's share from the first instance, which drains those splits
//! cooperatively — it finishes the object it has open, cuts at that
//! boundary, and commits its tail before handing them back, so the move
//! replays nothing — and the second claims them. Each instance prints the
//! objects it covered.
//!
//! The first terminal narrates that: `peer joined` as the new member's
//! presence key lands, then `assignment published` naming how many splits
//! changed hands. The second reports the fleet it walked into. Per-split
//! detail — `split claimed`, `drain started`, `drain finished` — is a level
//! down, at `RUST_LOG=info,spate_coordination=debug`, which is the run to
//! make if you want to watch one object's worth of handover.
//!
//! Draining a paced chain takes time, which is why `drain_deadline` below
//! sits far above its default: a drain that outruns the deadline is
//! revoked outright and its uncommitted tail replays under the new owner
//! instead. Safe either way, but only one of the two is a clean handoff.
//!
//! # Killing one instance
//!
//! **Ctrl-C** is a graceful departure. The pipeline drains, the source is
//! dropped, and the coordinator releases: every split's owner field is
//! cleared, its lease key deleted, leadership handed back, and the presence
//! key dropped, so an instance that held splits leaves nothing to expire.
//! (One holding none when the signal lands sends no release at all, and its
//! presence key goes on the age limit like any other.) The survivor sees
//! the released records on its watch and picks them up as soon as it holds
//! the leadership that assigns them — seconds after the signal rather than
//! a lease after it — and because the departing instance commits its tail
//! before letting go, the handoff replays nothing.
//!
//! **`kill -9`** writes nothing. The dead instance's lease keys simply stop
//! being rewritten and expire on the bucket's age limit one lease after the
//! last successful heartbeat; heartbeats run at about a third of the lease
//! and are jittered, so the expiry lands within a lease of the death. It
//! reaches the survivor as a limit marker, and the leader then withholds
//! the dead instance's splits for `rebalance_delay` — the window that lets a
//! restarting worker reclaim its own work instead of the fleet churning
//! around a bounce — before assigning them. With the values below that is
//! at most twenty seconds.
//!
//! Either way the new owner resumes from the last committed watermark, so
//! records written after it are replayed: at-least-once, never loss.
//!
//! # Running it again
//!
//! Split records are durable, so a finished job stays finished: a later run
//! under the same job name finds every split complete and exits at once.
//! The demo's coordination state lives only inside the container above, and
//! `--rm` throws it away — so stopping that container and starting a fresh
//! one is the reset.

// The examples index renders these four fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  bounded-jobs
// INDEX-GOAL:  coordinate a fleet over the durable store
// INDEX-TECH:  NATS JetStream
// INDEX-NEEDS: a NATS server with JetStream; run the binary twice

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::coordination::store::nats::{NatsConfig, NatsStore};
use spate::coordination::{CoordinationConfig, StoreCoordinator};
use spate::json::NdjsonFramer;
use spate::prelude::*;
use spate::s3::S3Source;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, decode_rows};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

/// One constant drives both sides of the lease agreement: the store's TTL
/// (the bucket age limit that expires an unheartbeated key) and the
/// coordinator's `lease_duration` (the protocol's takeover ceiling). They
/// are separate mechanisms and the coordinator rejects a store whose TTL
/// diverges from its config, so they are built from the same value.
///
/// Demo-fast; the default is 30s and the NATS floor is 2s, below which
/// one-second marker granularity would dominate.
const LEASE: Duration = Duration::from_secs(10);

/// Job identity: it suffixes both KV bucket names, every instance of one
/// job uses it, and two different jobs must never share one.
const JOB: &str = "nats-backfill-demo";

const DEFAULT_NATS: &str = "nats://127.0.0.1:4222";

const OBJECTS: usize = 96;
const RECORDS_PER_OBJECT: usize = 250;

/// Per-record pacing. A real pipeline is paced by a sink doing something;
/// this one has an in-memory sink and nothing to wait for, so it would
/// finish in about a second and leave no window in which to start the
/// second instance. At this rate one instance takes roughly a minute and
/// two take roughly half of it.
const PACE: Duration = Duration::from_millis(2);

/// The instance identity, the way a real deployment supplies it: unique
/// per *live* worker, and stable across a restart — that stability is what
/// lets a bounced worker reclaim its own splits inside the rebalance
/// window. `POD_NAME` is the Kubernetes downward-API spelling, `HOSTNAME`
/// the equivalent elsewhere, and the last resort is unique but not stable.
///
/// Give each terminal its own `POD_NAME` when running both instances on one
/// host: `HOSTNAME` is stable but shared, and two live workers claiming one
/// id is detected and fatal.
///
/// The id must be 1..=128 bytes of `[A-Za-z0-9_-]`, so a hostname's dots are
/// rewritten rather than rejected at startup. A variable set to the empty
/// string falls through to the next rung, as an unset one does.
fn instance_id() -> String {
    let from_env = |key| std::env::var(key).ok().filter(|v: &String| !v.is_empty());
    let raw = from_env("POD_NAME")
        .or_else(|| from_env("HOSTNAME"))
        .unwrap_or_else(|| format!("worker-{}", std::process::id()));
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Stage the "bucket" at a fixed path — both processes must read the same
/// objects, so a per-process tempdir would give them different jobs.
///
/// Whoever gets there first builds the prefix in a private directory and
/// renames it into place, which is atomic: a peer either finds the
/// finished listing or stages its own byte-identical copy and throws it
/// away when the rename loses.
fn stage_bucket() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join("spate-nats-backfill-demo");
    let data = root.join("data");
    if data.is_dir() {
        return Ok(data);
    }
    let staging = root.join(format!("staging-{}", std::process::id()));
    std::fs::create_dir_all(&staging)?;
    for o in 0..OBJECTS {
        let mut body = String::new();
        for r in 0..RECORDS_PER_OBJECT {
            body.push_str(&format!("{{\"obj\":\"obj-{o:03}\",\"seq\":{r}}}\n"));
        }
        std::fs::write(staging.join(format!("obj-{o:03}.ndjson")), body)?;
    }
    if std::fs::rename(&staging, &data).is_err() {
        std::fs::remove_dir_all(&staging)?;
        // Losing the race is the only survivable failure, and it is the one
        // that leaves the finished listing behind. Anything else would hand
        // back a prefix that is not there, and the run would report covering
        // nothing rather than failing.
        if !data.is_dir() {
            return Err(format!("staging {} failed and no peer staged it", data.display()).into());
        }
    }
    Ok(data)
}

/// The `obj` field of one staged line, without a JSON parser for a
/// two-field record.
fn object_of(line: &str) -> Option<&str> {
    line.split_once("\"obj\":\"")?
        .1
        .split_once('"')
        .map(|(id, _)| id)
}

/// `split_target_bytes` at its 1 MiB floor charges each object a 64 KiB
/// open cost, so 96 small objects pack into six splits — enough for a
/// fleet to divide. Real deployments keep the 64 MiB default.
///
/// The pipeline name is not instance-scoped: each instance is its own
/// process, so the metric series a name claims has exactly one live owner
/// (INV-10) without any help. Neither instance is scraped, so neither asks
/// for an admin server; a real deployment names an address and gets
/// `/metrics`, `/healthz` and `/readyz` on it.
fn config_yaml(data: &std::path::Path) -> String {
    format!(
        r#"
pipeline: {{ name: nats-coordinated-backfill, threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
checkpoint: {{ interval: 500ms }}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: 1MiB
sink: {{ capture: {{}} }}
"#,
        data = data.display(),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    let instance = instance_id();
    let servers = vec![std::env::var("NATS_URL").unwrap_or_else(|_| DEFAULT_NATS.to_string())];
    let data = stage_bucket()?;
    println!(
        "instance {instance}: job {JOB} on {}, prefix {}",
        servers[0],
        data.display()
    );

    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&config_yaml(&data))?)?;

    let tuning = CoordinationConfig {
        lease_duration: LEASE,
        op_timeout: Duration::from_secs(2),
        instance_id: Some(instance.clone()),
        // Replanning faster than leadership can be observed to fail is
        // churn; the floor is the lease and the prefix never grows here.
        replan_interval: LEASE,
        // Shortened from its 20s default so a `kill -9` demo does not
        // outlast the reader's patience. See the module docs.
        rebalance_delay: LEASE,
        // Raised well above its 10s default, and deliberately above the
        // lease: a revoked split drains by pushing its tail through the
        // chain to a final commit, and this chain is paced. A draining
        // split is still owned and still heartbeated, so a long drain
        // costs a slow rebalance, never a race. The default leaves the
        // drain racing the deadline on this workload, and a drain that
        // loses is forced instead of cooperative — safe, since the tail
        // replays, but not what this example is here to show.
        drain_deadline: Duration::from_secs(60),
        ..CoordinationConfig::default()
    };
    // Construction is lazy: connecting and provisioning the two buckets
    // happen on the coordinator's startup probe, so a wrong URL rides the
    // startup retry budget and a wrong server version is fatal with an
    // actionable message rather than surfacing later.
    // ANCHOR: coordinator
    let store = NatsStore::new(NatsConfig::new(servers, JOB), tuning.lease_duration)?;
    let coordinator = StoreCoordinator::new(store, tuning, pipeline.io_handle(), None)?;
    // ANCHOR_END: coordinator

    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)))
        .with_coordinator(Box::new(coordinator));

    let (sink, script) = capture_sink(1, 1);
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50);
        cfg
    };
    // `handle_signals` stays at its default: Ctrl-C drains the pipeline,
    // which drops the source, which releases every split.
    let report = pipeline
        .sink(sink.with_pool_config(pool_cfg))?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::passthrough())
                .with_metrics(ctx.pipeline, "main")
                .map(|line: Vec<u8>| {
                    std::thread::sleep(PACE);
                    line
                })
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .run(source)?;

    // What this instance's share turned out to be. Set the two terminals
    // side by side: together the lists are the whole prefix. A split moved
    // cooperatively cuts between objects, so each object lands on one side
    // only; a split taken back after a forced drain, a Ctrl-C or a `kill -9`
    // resumes inside an object, and that one object shows up on both.
    let mut records = 0usize;
    let mut objects: BTreeSet<String> = BTreeSet::new();
    for write in script.writes() {
        for row in decode_rows(&write.payload) {
            records += 1;
            let line = String::from_utf8(row)?;
            if let Some(obj) = object_of(&line) {
                objects.insert(obj.to_string());
            }
        }
    }
    println!(
        "\n{instance}: {records} records, covering {} of {OBJECTS} objects",
        objects.len()
    );
    println!(
        "{instance} objects: {}",
        objects.iter().cloned().collect::<Vec<_>>().join(" ")
    );
    report.log();
    std::process::exit(report.exit_code());
}
