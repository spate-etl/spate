//! One instance per process, sharing one backfill over the DynamoDB store:
//! the same binary, run twice, in two terminals.
//!
//! Each process is one instance. Its identity comes from the environment
//! the way a pod's does, and the fleet meets in a table that outlives every
//! member of it. Nothing in the code below knows how many peers exist.
//!
//! The work being divided is a bounded backfill of a `file://` prefix,
//! 96 small NDJSON objects staged into the temp directory on first run, so
//! the only thing to stand up is DynamoDB Local. The planner puts at most 16
//! objects in a split, so they pack into six. Whichever instance holds leadership lists the
//! prefix once and writes the split table; every instance leases the splits
//! it is assigned, reads them straight from the split descriptors, and
//! commits fenced per-split progress. Each exits `Completed` once every
//! split is complete, and the union of the two covers the whole prefix.
//! Delivery is at-least-once, so a forced revocation can replay a tail but
//! never drop one.
//!
//! The chain paces itself on purpose (see `PACE`): without that the
//! backfill is over before you can reach the second terminal.
//!
//! # Run it
//!
//! DynamoDB Local, the emulator CI runs, in memory. It accepts any key pair,
//! and the store takes credentials and the region from the AWS provider
//! chain, so export a dummy pair and a region in each terminal:
//!
//! ```sh
//! docker run --rm -p 8000:8000 "$(cargo xtask container-image --pull dynamodb)" -jar DynamoDBLocal.jar -inMemory
//! export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
//! POD_NAME=worker-a cargo run -p spate --features s3,json,coordination-dynamodb --example dynamodb_coordinated_backfill
//! POD_NAME=worker-b cargo run -p spate --features s3,json,coordination-dynamodb --example dynamodb_coordinated_backfill
//! ```
//!
//! The first instance creates the table, since the section sets
//! `create_table`. `DYNAMODB_ENDPOINT` overrides the endpoint.
//!
//! Start the second one while the first is still working. The leader sees
//! the new member at its next poll, recomputes the assignment, and revokes
//! the newcomer's share from the first instance, which drains those splits
//! cooperatively before the second claims them. It finishes the object it
//! has open, cuts at that boundary, and commits its tail before releasing
//! them, so the move replays nothing. Each instance prints the objects it
//! covered.
//!
//! The first terminal narrates that: `peer joined` as the new member's
//! presence key reaches its poll, then `assignment published` naming how
//! many splits changed hands. Per-split detail (`split claimed`,
//! `drain started`, `drain finished`) is a level down, at
//! `RUST_LOG=info,spate_coordination=debug`.
//!
//! Draining a paced chain takes time, so the `drain_deadline` in
//! [`COORDINATION`] sits far above its default. A drain that outruns the
//! deadline is revoked outright and its uncommitted tail replays under the
//! new owner instead. Both are safe; only the first is a clean revocation.
//!
//! # Killing one instance
//!
//! **Ctrl-C** is a graceful departure. The pipeline drains, the source is
//! dropped, and the coordinator clears each held split's owner field and
//! deletes its lease item, the presence item and, if it leads, the leader
//! item. The survivor sees the departure at its next poll and picks the
//! splits up a poll or two later, with no lease to wait out. Because the
//! departing instance commits its tail before letting go, the release
//! replays nothing.
//!
//! **`kill -9`** writes nothing. The table enforces no expiry: the survivor
//! judges the dead instance's leases expired from its own polls, up to one
//! lease plus two poll intervals after the last heartbeat. The leader then
//! withholds the dead instance's splits for `rebalance_delay`, so a
//! restarting worker can reclaim its own work, and the survivor reads the
//! new assignment at a later poll. With the values in [`COORDINATION`],
//! takeover lands within about twenty-four seconds of the death.
//!
//! Either way the new owner resumes from the last committed watermark, so
//! records written after it are replayed. Delivery is at-least-once.
//!
//! # Running it again
//!
//! Split records are durable, so a finished job stays finished. A later run
//! under the same job name finds every split complete and exits. The finished
//! run left its `verdict` item and no leader item behind.
//! DynamoDB Local above keeps the table in memory, and `--rm` throws the
//! container away, so stopping it and starting a fresh one is the reset.

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  bounded-jobs
// INDEX-GOAL:  coordinate a fleet over a serverless store
// INDEX-TECH:  DynamoDB
// INDEX-NEEDS: DynamoDB Local; run the binary twice

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
/// The tuning suits a demo against DynamoDB Local. `lease_duration` (30s
/// by default), `poll_interval` (2s) and `rebalance_delay` (20s) are
/// shortened so a takeover after `kill -9` is quick, and `op_timeout` (10s)
/// and `replan_interval` (60s) shrink with the lease they are checked
/// against. `drain_deadline` is raised from 10s because a revoked split
/// drains by pushing its tail through this paced chain to a final commit.
/// `reconcile_interval` is raised from 30s to the store page's 5m, since
/// each reconcile reads the job's whole split history. `endpoint` and
/// `create_table` are there for DynamoDB Local; a deployment leaves both
/// unset.
const COORDINATION: &str = r#"
# ANCHOR: coordination
coordination:
  instance_id: "${POD_NAME}"
  lease_duration: 10s
  op_timeout: 2s
  replan_interval: 10s
  reconcile_interval: 5m
  rebalance_delay: 10s
  drain_deadline: 60s
  store:
    dynamodb:
      table: spate-coordination
      job: dynamodb-backfill-demo
      endpoint: "${DYNAMODB_ENDPOINT:-http://127.0.0.1:8000}"
      create_table: true
      poll_interval: 1s
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
    let root = std::env::temp_dir().join("spate-dynamodb-backfill-demo");
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
        // nothing without failing.
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

/// The pipeline name is not instance-scoped: each instance is its own
/// process, so the metric series a name claims has one live owner
/// (INV-10) without any help. Neither instance is scraped, so neither asks
/// for an admin server.
fn config_yaml(data: &std::path::Path) -> String {
    format!(
        r#"
pipeline: {{ name: dynamodb-coordinated-backfill, threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
checkpoint: {{ interval: 500ms }}
source:
  s3:
    url: "file://{data}/"
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

    // What this instance's share turned out to be. Together the two lists
    // are the whole prefix. A split moved cooperatively cuts between
    // objects, so each object lands on one side only; a split taken back
    // after a forced drain, a Ctrl-C or a `kill -9` resumes inside an
    // object, and that one object shows up on both.
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
