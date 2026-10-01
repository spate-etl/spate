//! One instance per process, sharing one backfill over the durable NATS
//! JetStream store: the same binary, run twice, in two terminals.
//!
//! The other coordinated examples put two instances in one process over
//! [`MemoryStore`](spate::coordination::store::memory::MemoryStore), which
//! shows the protocol but not the deployment. This is the deployment: the
//! process is the instance, its identity comes from the environment the
//! way a pod's does, and the fleet meets in a store that outlives every
//! member of it. Nothing in the code below knows how many peers exist.
//!
//! The work being divided is a bounded backfill of a `file://` prefix,
//! 96 small NDJSON objects staged into the temp directory on first run and
//! packed into six splits at the 1 MiB target, so the only thing to stand
//! up is NATS. Whichever instance holds leadership lists the prefix once
//! and writes the split table; every instance leases the splits it is
//! assigned, reads them straight from the split descriptors, and commits
//! fenced per-split progress. Each exits `Completed` once every split is
//! complete, and the union of the two covers the whole prefix. Delivery is
//! at-least-once, so a forced revocation can replay a tail but never drop one.
//!
//! The chain paces itself on purpose (see `PACE`): without that the
//! backfill is over before you can reach the second terminal.
//!
//! # Run it
//!
//! A NATS server with JetStream enabled, on a line the NATS store page lists
//! as supported. The worker refuses an older one at startup. This starts the
//! server CI runs:
//!
//! ```sh
//! docker run --rm -p 4222:4222 "$(cargo xtask container-image --pull nats)" -js
//! NATS_URL=nats://127.0.0.1:4222 POD_NAME=worker-a cargo run -p spate --features s3,json,coordination-nats --example nats_coordinated_backfill
//! NATS_URL=nats://127.0.0.1:4222 POD_NAME=worker-b cargo run -p spate --features s3,json,coordination-nats --example nats_coordinated_backfill
//! ```
//!
//! Start the second one while the first is still working. The leader
//! recomputes the assignment the moment the new member appears and revokes
//! the newcomer's share from the first instance, which drains those splits
//! cooperatively before the second claims them. It finishes the object it
//! has open, cuts at that boundary, and commits its tail before releasing
//! them, so the move replays nothing. Each instance prints the objects it
//! covered.
//!
//! The first terminal narrates that: `peer joined` as the new member's
//! presence key lands, then `assignment published` naming how many splits
//! changed hands. The second reports the fleet it walked into. Per-split
//! detail (`split claimed`, `drain started`, `drain finished`) is a level
//! down, at `RUST_LOG=info,spate_coordination=debug`, which is the run to
//! make to watch one object's worth of reassignment.
//!
//! Draining a paced chain takes time, so the `drain_deadline` in
//! [`COORDINATION`] sits far above its default. A drain that outruns the deadline is revoked outright
//! and its uncommitted tail replays under the new owner instead. Both are
//! safe; only the first is a clean revocation.
//!
//! # Killing one instance
//!
//! **Ctrl-C** is a graceful departure. The pipeline drains, the source is
//! dropped, and the coordinator departs: every split's owner field is
//! cleared, its lease key deleted, leadership handed back, and the presence
//! key dropped, so the instance leaves nothing to expire. The survivor sees
//! the released records on its watch and picks them up as soon as it holds
//! the leadership that assigns them, seconds after the signal rather than a
//! lease after it. Because the departing instance commits its tail before
//! letting go, the release replays nothing.
//!
//! **`kill -9`** writes nothing. The dead instance's lease keys stop
//! being rewritten and expire on the bucket's age limit one lease after the
//! last successful heartbeat; heartbeats run at about a third of the lease
//! and are jittered, so the expiry lands within a lease of the death. It
//! reaches the survivor as a limit marker, and the leader then withholds
//! the dead instance's splits for `rebalance_delay` before assigning them.
//! That window lets a restarting worker reclaim its own work instead of the
//! fleet churning around a bounce. With the values in [`COORDINATION`] it
//! is at most twenty seconds.
//!
//! Either way the new owner resumes from the last committed watermark, so
//! records written after it are replayed. Delivery is at-least-once.
//!
//! # Running it again
//!
//! Split records are durable, so a finished job stays finished: a later run
//! under the same job name finds every split complete and exits at once.
//! The demo's coordination state lives only inside the container above, and
//! `--rm` throws it away, so stopping that container and starting a fresh
//! one is the reset.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  bounded-jobs
// INDEX-GOAL:  coordinate a fleet over the durable store
// INDEX-TECH:  NATS JetStream
// INDEX-NEEDS: a NATS server with JetStream; run the binary twice

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::json::NdjsonFramer;
use spate::prelude::*;
use spate::s3::S3Source;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, decode_rows};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

/// The coordination section, as a deployment would write it. The store's
/// lease TTL and the coordinator's lease are both `lease_duration`.
///
/// `instance_id` must be unique per *live* worker and stable across a
/// restart, so a worker that crashed and comes back reclaims its own splits
/// inside the rebalance window: `POD_NAME` is the Kubernetes downward-API
/// spelling.
/// Two live workers claiming one id is detected and fatal.
///
/// The tuning is demo-fast. `lease_duration` defaults to 30s and the NATS
/// floor is 2s. `rebalance_delay` is shortened from 20s so a `kill -9` demo
/// is quick; `drain_deadline` is raised from 10s because a revoked split
/// drains by pushing its tail through this paced chain to a final commit.
const COORDINATION: &str = r#"
# ANCHOR: coordination
coordination:
  instance_id: "${POD_NAME}"
  lease_duration: 10s
  op_timeout: 2s
  replan_interval: 10s
  rebalance_delay: 10s
  drain_deadline: 60s
  store:
    nats:
      servers: ["${NATS_URL:-nats://127.0.0.1:4222}"]
      job: nats-backfill-demo
# ANCHOR_END: coordination
"#;

const OBJECTS: usize = 96;
const RECORDS_PER_OBJECT: usize = 250;

/// Per-record pacing. A real pipeline is paced by a sink doing something;
/// this one has an in-memory sink and nothing to wait for, so it would
/// finish in about a second and leave no window in which to start the
/// second instance. At this rate one instance takes roughly a minute and
/// two take roughly half of it.
const PACE: Duration = Duration::from_millis(2);

/// Stage the "bucket" at a fixed path. Both processes must read the same
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
/// open cost, so 96 small objects pack into six splits, which is enough for
/// a fleet to divide. Real deployments keep the 64 MiB default.
///
/// The pipeline name is not instance-scoped: each instance is its own
/// process, so the metric series a name claims has one live owner
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
{COORDINATION}"#,
        data = data.display(),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    let instance = std::env::var("POD_NAME").unwrap_or_default();
    let data = stage_bucket()?;
    println!("instance {instance}: prefix {}", data.display());

    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&config_yaml(&data))?)?;
    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)));

    let (sink, script) = capture_sink(1, 1);
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50);
        cfg
    };
    // `handle_signals` stays at its default: Ctrl-C drains the pipeline,
    // which drops the source, which departs the job.
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
