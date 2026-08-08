//! Two pipeline instances sharing one S3 backfill — the coordinated
//! deployment story end to end, runnable without any infrastructure.
//!
//! Both instances point their `S3Source` at the same prefix and inject a
//! coordinator over one shared store ([`S3Source::with_coordinator`] —
//! the assembly seam). The elected leader lists the prefix **once** and
//! packs it into splits; both workers lease splits, read them straight
//! from the split descriptors, and commit fenced per-split progress.
//! Neither replica duplicates the backfill and each exits `Completed`
//! once every split is committed complete.
//!
//! The shared in-process [`MemoryStore`] stands in for the durable
//! backend (e.g. `spate-coordination`'s NATS JetStream store) both
//! instances would point at in production — swap the store constructor
//! and this is the real multi-process deployment.
//!
//! ```sh
//! cargo run -p spate --features s3,json,coordination --example s3_coordinated_backfill
//! ```

// The examples index renders these four fields; see scripts/examples-index.sh.
// INDEX-TIER:  bounded-jobs
// INDEX-GOAL:  split one backfill across two instances without either duplicating it
// INDEX-TECH:  object storage and coordination
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::coordination::store::memory::MemoryStore;
use spate::coordination::{CoordinationConfig, StoreCoordinator};
use spate::json::NdjsonFramer;
use spate::prelude::*;
use spate::s3::S3Source;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, decode_rows};
use std::collections::BTreeSet;
use std::time::Duration;

/// Demo-fast takeover; production leases are far higher (see the
/// deployment guide). The store must be built from the SAME constant —
/// the coordinator rejects a store whose TTL diverges from its
/// `lease_duration`.
const LEASE: Duration = Duration::from_secs(1);

const OBJECTS: usize = 48;
const RECORDS_PER_OBJECT: usize = 50;

fn config_yaml(data: &std::path::Path, instance: &str) -> String {
    // `split_target_bytes` at its floor keeps the per-object cost at
    // 64KiB, so 48 small objects pack into three splits — enough for the
    // two workers to share. Real deployments keep the 64 MiB default.
    //
    // The pipeline name carries the instance because both run in *this*
    // process, and a gauge series has exactly one live owner per process
    // (INV-10). Two instances under one name is the same series claimed
    // twice. In production each instance is its own process and they share
    // a name; here the label has to do what the process boundary would.
    format!(
        r#"
pipeline: {{ name: s3-coordinated-demo-{instance}, threads: 2 }}
checkpoint: {{ interval: 100ms }}
metrics: {{ listen: "127.0.0.1:0" }}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: 1MiB
sink: {{ capture: {{}} }}
"#,
        data = data.display(),
    )
}

fn run_instance(
    instance: &str,
    yaml: String,
    store: MemoryStore,
) -> Result<(ExitState, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&yaml)?)?;

    let coordinator = StoreCoordinator::new(
        store,
        CoordinationConfig {
            lease_duration: LEASE,
            op_timeout: Duration::from_millis(250),
            instance_id: Some(instance.to_string()),
            replan_interval: Duration::from_secs(1),
            ..CoordinationConfig::default()
        },
        pipeline.io_handle(),
        None,
    )?;
    let source = S3Source::from_component_config(&pipeline.config().source, pipeline.io_handle())?
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)))
        .with_coordinator(Box::new(coordinator));

    let (sink, script) = capture_sink(1, 1);
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(20);
        cfg
    };
    let report = pipeline
        .sink(sink.with_pool_config(pool_cfg))?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::passthrough())
                .with_metrics(ctx.pipeline, "main")
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .run(source)?;

    let mut rows = Vec::new();
    for write in script.writes() {
        for row in decode_rows(&write.payload) {
            rows.push(String::from_utf8(row)?);
        }
    }
    Ok((report.state, rows))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    // Stage the "bucket": 48 small NDJSON objects in a tempdir.
    let root = tempfile::tempdir()?;
    let data = root.path().join("data");
    std::fs::create_dir_all(&data)?;
    let mut expected: BTreeSet<String> = BTreeSet::new();
    for o in 0..OBJECTS {
        let mut body = String::new();
        for r in 0..RECORDS_PER_OBJECT {
            let line = format!("{{\"k\":\"obj-{o:02}-{r}\"}}");
            body.push_str(&line);
            body.push('\n');
            expected.insert(line);
        }
        std::fs::write(data.join(format!("obj-{o:02}.ndjson")), body)?;
    }

    // The shared store stands in for the durable coordination backend
    // both instances would point at in production.
    let store = MemoryStore::new(LEASE);

    let workers: Vec<_> = ["instance-a", "instance-b"]
        .into_iter()
        .map(|instance| {
            let store = store.clone();
            let yaml = config_yaml(&data, instance);
            std::thread::spawn(move || run_instance(instance, yaml, store))
        })
        .collect();

    let mut union: BTreeSet<String> = BTreeSet::new();
    let mut total_captured = 0usize;
    for (instance, worker) in ["instance-a", "instance-b"].iter().zip(workers) {
        let (state, rows) = worker
            .join()
            .expect("instance thread")
            .map_err(|e| format!("{instance}: {e}"))?;
        println!("{instance}: exit={state:?}, rows={}", rows.len());
        assert!(
            matches!(state, ExitState::Completed),
            "{instance} must complete, got {state:?}"
        );
        total_captured += rows.len();
        union.extend(rows);
    }

    // At-least-once: the union is exactly the staged records; overlap
    // (replayed rows after a steal or takeover) is possible and fine.
    assert_eq!(union, expected, "the union must cover every staged record");
    println!(
        "union covers all {} records ({total_captured} captured across instances, {} duplicates)",
        expected.len(),
        total_captured - expected.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller, so the assertions above stop being decorative.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
