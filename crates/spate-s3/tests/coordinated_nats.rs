//! The coordinated backfill against the real production pair: a NATS
//! JetStream coordination store and a SeaweedFS S3 gateway — durable
//! resume across process "restarts" and multi-instance sharing, exactly
//! as a deployment would assemble them.
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo test -p spate-s3 --test coordinated_nats -- --ignored --test-threads=1
//! ```

mod support;

use spate_coordination::store::nats::{NatsConfig, NatsStore};
use spate_coordination::{CoordinationConfig, StoreCoordinator};
use spate_core::pipeline::ExitState;
use spate_test::{SinkScript, WriteOutcome, wait_until};
use std::time::Duration;
use support::seaweed::Gateway;
use support::{
    Launched, captured_rows, launch_customized, line_framer, lines_bytes, recs, sorted,
    test_options,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const NATS_IMAGE: &str = "nats";
const NATS_TAG: &str = "2.11-alpine";
const NATS_CLIENT_PORT: u16 = 4222;

/// The NATS floor for leases is 2s.
const LEASE: Duration = Duration::from_secs(2);

fn start_nats() -> (Container<GenericImage>, u16) {
    let container = GenericImage::new(NATS_IMAGE, NATS_TAG)
        .with_exposed_port(NATS_CLIENT_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .expect("start NATS (is Docker running? first run pulls the image)");
    let port = container
        .get_host_port_ipv4(NATS_CLIENT_PORT)
        .expect("mapped client port");
    (container, port)
}

fn config_yaml(gw: &Gateway) -> String {
    format!(
        r#"
pipeline: {{ name: s3-nats-test, threads: 2 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "s3://{bucket}/data/"
    split_target_bytes: 1MiB
    store:
{opts}
sink: {{ capture: {{}} }}
"#,
        bucket = gw.bucket,
        opts = gw.store_options_yaml(),
    )
}

/// Assemble one instance the way a deployment does: a `NatsStore` over
/// the shared server, a `StoreCoordinator` with this instance's identity,
/// injected via `with_coordinator`.
fn launch_nats_instance(
    yaml: &str,
    nats_port: u16,
    job: &str,
    instance: &str,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    let nats = NatsConfig::new(vec![format!("nats://127.0.0.1:{nats_port}")], job);
    let tuning = CoordinationConfig {
        lease_duration: LEASE,
        op_timeout: Duration::from_secs(1),
        instance_id: Some(instance.to_string()),
        replan_interval: LEASE,
        ..CoordinationConfig::default()
    };
    launch_customized(yaml, test_options(), pre, move |source, io| {
        let store = NatsStore::new(nats, LEASE).expect("nats store");
        let coordinator =
            StoreCoordinator::new(store, tuning, io, None).expect("coordinator builds");
        line_framer(source).with_coordinator(Box::new(coordinator))
    })
}

/// Durable resume across a process boundary: run 1 makes partial
/// progress and drains; run 2 — a fresh pipeline, fresh coordinator,
/// same NATS job — finishes the backfill without losing a record and
/// without re-reading what run 1 committed as complete.
#[test]
#[ignore = "requires Docker"]
fn durable_resume_across_restart_loses_nothing() {
    let gw = Gateway::start("spate-s3-nats-resume");
    let (_nats, nats_port) = start_nats();
    let objects: Vec<(String, Vec<u8>)> = (0..48)
        .map(|i| {
            (
                format!("data/part-{i:02}.ndjson"),
                lines_bytes(&recs(&format!("p{i:02}"), 50)),
            )
        })
        .collect();
    gw.seed(objects);
    let mut expected: Vec<String> = Vec::new();
    for i in 0..48 {
        expected.extend(recs(&format!("p{i:02}"), 50));
    }
    let yaml = config_yaml(&gw);

    // Run 1: paced writes hold the job open; stop after data flows. The
    // drain commits acked progress into NATS and releases the splits.
    let l1 = launch_nats_instance(&yaml, nats_port, "resume-job", "run-1", |sink| {
        for _ in 0..20 {
            sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(150)));
        }
    });
    wait_until(Duration::from_secs(60), "first rows captured", || {
        !captured_rows(&l1.script).is_empty()
    });
    l1.shutdown.trigger();
    let r1 = l1.run.join().expect("run 1 exits");
    assert_eq!(r1.state, ExitState::Completed, "signal shutdown drains");
    let rows1 = captured_rows(&l1.script);

    // Run 2: a brand-new pipeline against the same job must finish it.
    let l2 = launch_nats_instance(&yaml, nats_port, "resume-job", "run-2", |_| {});
    let r2 = l2
        .run
        .wait_exit(Duration::from_secs(120))
        .expect("run 2 completes on its own")
        .expect("no start error");
    assert_eq!(r2.state, ExitState::Completed);
    let rows2 = captured_rows(&l2.script);

    let total = expected.len();
    let mut union: Vec<String> = rows1.iter().chain(rows2.iter()).cloned().collect();
    union.sort();
    union.dedup();
    assert_eq!(union, sorted(expected), "no record lost across the restart");
    assert!(
        rows2.len() < total || rows1.is_empty(),
        "run 2 resumed durable progress instead of replaying everything \
         (run1={}, run2={}, total={total})",
        rows1.len(),
        rows2.len(),
    );
}

/// Two concurrent instances over real NATS complete the job collectively.
#[test]
#[ignore = "requires Docker"]
fn two_instances_over_real_nats_complete_collectively() {
    let gw = Gateway::start("spate-s3-nats-pair");
    let (_nats, nats_port) = start_nats();
    let objects: Vec<(String, Vec<u8>)> = (0..64)
        .map(|i| {
            (
                format!("data/part-{i:02}.ndjson"),
                lines_bytes(&recs(&format!("q{i:02}"), 25)),
            )
        })
        .collect();
    gw.seed(objects);
    let mut expected: Vec<String> = Vec::new();
    for i in 0..64 {
        expected.extend(recs(&format!("q{i:02}"), 25));
    }
    let yaml = config_yaml(&gw);

    let a = launch_nats_instance(&yaml, nats_port, "pair-job", "instance-a", |_| {});
    let b = launch_nats_instance(&yaml, nats_port, "pair-job", "instance-b", |_| {});
    let ra = a.run.wait_exit(Duration::from_secs(180)).unwrap().unwrap();
    let rb = b.run.wait_exit(Duration::from_secs(180)).unwrap().unwrap();
    assert_eq!(ra.state, ExitState::Completed);
    assert_eq!(rb.state, ExitState::Completed);

    let mut union: Vec<String> = captured_rows(&a.script)
        .into_iter()
        .chain(captured_rows(&b.script))
        .collect();
    union.sort();
    union.dedup();
    assert_eq!(union, sorted(expected), "the union must cover every record");
}
