//! The coordination protocol over a real NATS 2.11 server: validates the
//! whole `NatsStore` mapping — bucket provisioning, the startup probe,
//! revision CAS, marker-driven expiry surfacing through the watch, and
//! heartbeats keeping leases alive across many TTLs — by running the same
//! scenarios the in-memory suite proves, against the production backend.
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo test -p spate-coordination --test nats_integration -- --ignored
//! ```
#![cfg(feature = "nats")]

mod support;

use spate_coordination::store::nats::{NatsConfig, NatsStore};
use spate_coordination::{
    CoordinationConfig, CoordinationErrorKind, NatsCoordinator, SplitCoordinator, SplitProgress,
    StoreCoordinator,
};
use std::time::{Duration, Instant};
use support::{Held, PhasedPlanner, crash, drive, drive_pair, runtime, split_id};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const IMAGE: &str = "nats";
const TAG: &str = "2.11-alpine";
const OLD_TAG: &str = "2.10-alpine";
const CLIENT_PORT: u16 = 4222;

/// The NATS floor for leases is 2s; timing assertions scale from this.
const LEASE: Duration = Duration::from_secs(2);

fn start_nats(tag: &str) -> (Container<GenericImage>, u16) {
    let container = GenericImage::new(IMAGE, tag)
        .with_exposed_port(CLIENT_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .expect("start NATS (is Docker running? first run pulls the image)");
    let port = container
        .get_host_port_ipv4(CLIENT_PORT)
        .expect("mapped client port");
    (container, port)
}

fn nats_config(port: u16, job: &str) -> NatsConfig {
    NatsConfig::new(vec![format!("nats://127.0.0.1:{port}")], job)
}

fn worker(port: u16, job: &str, io: &tokio::runtime::Handle, instance_id: &str) -> NatsCoordinator {
    let store = NatsStore::new(nats_config(port, job), LEASE).expect("nats store");
    StoreCoordinator::new(
        store,
        CoordinationConfig {
            lease_duration: LEASE,
            op_timeout: Duration::from_secs(1),
            instance_id: Some(instance_id.to_string()),
            replan_interval: LEASE,
            reconcile_interval: Duration::from_secs(1),
            // A dead worker's splits flow back on lease expiry alone; the
            // grace window is for absorbing a restart, which this suite
            // does not exercise. Left at its 20s default it would delay
            // takeover past the test deadline.
            rebalance_delay: Duration::ZERO,
            drain_deadline: LEASE / 2,
            ..CoordinationConfig::default()
        },
        io.clone(),
        None,
    )
    .expect("coordinator")
}

#[test]
#[ignore = "needs Docker; run explicitly"]
fn partition_takeover_and_completion_over_real_nats() {
    let (_nats, port) = start_nats(TAG);
    let rt = runtime();
    let ids = ["n0", "n1", "n2", "n3"];
    let planner = || Box::new(PhasedPlanner::one_final("nats-smoke:v1", &ids));

    // Two workers provision the buckets, pass the probe, and partition
    // the plan 2/2 — claims arrive via real KV watches.
    let rt_a = runtime();
    let mut a = worker(port, "smoke", rt_a.handle(), "worker-a");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    let mut b = worker(port, "smoke", rt.handle(), "worker-b");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "partitioning over real NATS",
        |ha, hb| {
            ha.splits.len() + hb.splits.len() == ids.len()
                && ha.splits.keys().all(|k| !hb.splits.contains_key(k))
                && !ha.splits.is_empty()
                && !hb.splits.is_empty()
        },
    );

    // Held leases survive well past the TTL — heartbeat rewrites re-arm
    // the bucket's max_age per message, exactly as the spike pinned.
    let hold_until = Instant::now() + LEASE * 5 / 2;
    while Instant::now() < hold_until {
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
    }
    assert_eq!(
        held_a.splits.len() + held_b.splits.len(),
        ids.len(),
        "heartbeated leases must survive far past the TTL"
    );

    // A commits progress, then dies without releasing. Its leases expire
    // server-side; the Purge markers reach B's watch; B takes everything
    // over with the progress carried and epochs bumped.
    let a_split = held_a.splits.keys().next().unwrap().clone();
    a.commit(
        &split_id(&a_split),
        &SplitProgress::new(7, b"nats-resume".to_vec()),
    )
    .unwrap();
    let died_at = Instant::now();
    crash(rt_a, a);
    drive(
        &mut b,
        &mut held_b,
        "B taking over via marker expiry",
        |h| h.splits.len() == ids.len(),
    );
    // The lease expires one TTL after the LAST HEARTBEAT, which may be up
    // to one renewal interval (TTL/3) before the death — so the earliest
    // legitimate takeover is died_at + 2/3 TTL.
    assert!(
        died_at.elapsed() >= LEASE * 2 / 3,
        "takeover before the dead worker's lease could have expired: {:?}",
        died_at.elapsed()
    );
    let (_, progress) = &held_b.splits[&a_split];
    assert_eq!(
        progress.as_ref().map(|p| (p.watermark, p.state.clone())),
        Some((7, b"nats-resume".to_vec())),
        "committed progress carries across the takeover"
    );

    // B completes the job; AllComplete arrives from the durable records.
    let ids: Vec<String> = held_b.splits.keys().cloned().collect();
    for id in ids {
        b.commit(&split_id(&id), &SplitProgress::completed(100, vec![]))
            .unwrap();
    }
    drive(&mut b, &mut held_b, "completion over real NATS", |h| {
        h.all_complete
    });
}

#[test]
#[ignore = "needs Docker; run explicitly"]
fn resume_after_full_restart_reads_durable_records() {
    let (_nats, port) = start_nats(TAG);
    let planner = || Box::new(PhasedPlanner::one_final("nats-resume:v1", &["r0"]));

    // First incarnation commits progress and releases gracefully.
    {
        let rt = runtime();
        let mut w = worker(port, "resume", rt.handle(), "gen-1");
        w.start(planner()).unwrap();
        let mut held = Held::default();
        drive(&mut w, &mut held, "first incarnation claiming", |h| {
            h.splits.len() == 1
        });
        w.commit(&split_id("r0"), &SplitProgress::new(41, b"cursor".to_vec()))
            .unwrap();
        w.release(&[split_id("r0")]).unwrap();
    }

    // A brand-new process (fresh runtime, fresh connection) adopts the
    // durable state instantly — released work needs no lease wait.
    let rt = runtime();
    let started = Instant::now();
    let mut w = worker(port, "resume", rt.handle(), "gen-2");
    w.start(planner()).unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "second incarnation resuming", |h| {
        h.splits.len() == 1
    });
    assert!(
        started.elapsed() < LEASE,
        "released work must not wait out a lease: {:?}",
        started.elapsed()
    );
    assert_eq!(
        held.splits["r0"].1.as_ref().map(|p| p.watermark),
        Some(41),
        "progress read back from the durable record"
    );
}

#[test]
#[ignore = "needs Docker; run explicitly"]
fn servers_below_the_floor_are_rejected_actionably() {
    let (_nats, port) = start_nats(OLD_TAG);
    let rt = runtime();
    let mut w = worker(port, "floor", rt.handle(), "worker-a");
    let planner = Box::new(PhasedPlanner::one_final("nats-floor:v1", &["f0"]));
    // The lazy connection runs under start(); the version gate must fail
    // fatally (no silent degrade onto a server without limit markers).
    let error = match w.start(planner) {
        Err(e) => e,
        Ok(()) => {
            let deadline = Instant::now() + support::DEADLINE;
            loop {
                assert!(Instant::now() < deadline, "old server never rejected");
                if let Err(e) = w.poll() {
                    break e;
                }
            }
        }
    };
    assert_eq!(error.kind, CoordinationErrorKind::Fatal, "{error}");
    assert!(error.to_string().contains("2.11"), "{error}");
    assert!(error.to_string().contains("upgrade"), "{error}");
}
