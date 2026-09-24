//! The coordination protocol over a real NATS 2.11 server: validates the
//! whole `NatsStore` mapping, covering bucket provisioning, the startup
//! probe, revision CAS, marker-driven expiry surfacing through the watch,
//! and heartbeats keeping leases alive across many TTLs, by running the same
//! scenarios the in-memory suite proves against the production backend.
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo test -p spate-coordination --test nats_integration -- --ignored
//! ```
#![cfg(feature = "nats")]

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::nats::{NatsConfig, NatsCredentials, NatsStore, Secret};
use spate_coordination::store::{CasOutcome, CoordinationStore, Keyspace, StoreError, WatchEvent};
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
    worker_with(nats_config(port, job), io, Some(instance_id))
}

fn worker_with(
    nats: NatsConfig,
    io: &tokio::runtime::Handle,
    instance_id: Option<&str>,
) -> NatsCoordinator {
    let store = NatsStore::new(nats, LEASE).expect("nats store");
    let mut cfg = CoordinationConfig::default();
    cfg.lease_duration = LEASE;
    cfg.op_timeout = Duration::from_secs(1);
    cfg.instance_id = instance_id.map(str::to_string);
    cfg.replan_interval = LEASE;
    cfg.reconcile_interval = Duration::from_secs(1);
    // A dead worker's splits flow back on lease expiry alone; the grace
    // window is for absorbing a restart, which this suite does not
    // exercise. Left at its 20s default it would delay takeover past the
    // test deadline.
    cfg.rebalance_delay = Duration::ZERO;
    cfg.drain_deadline = LEASE / 2;
    StoreCoordinator::new(store, cfg, io.clone(), None).expect("coordinator")
}

#[test]
#[ignore = "needs Docker; run explicitly"]
fn partition_takeover_and_completion_over_real_nats() {
    let (_nats, port) = start_nats(TAG);
    let rt = runtime();
    let ids = ["n0", "n1", "n2", "n3"];
    let planner = || Box::new(PhasedPlanner::one_final("nats-smoke:v1", &ids));

    // Two workers provision the buckets, pass the probe, and partition
    // the plan 2/2; claims arrive via real KV watches.
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

    // Held leases survive well past the TTL. Heartbeat rewrites re-arm
    // the bucket's max_age per message, as the spike pinned.
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
    // to one renewal interval (TTL/3) before the death, so the earliest
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
    // durable state instantly; released work needs no lease wait.
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

/// `NatsStore::delete` returns the outcomes the trait documents, and a
/// guarded delete of an expired lease wins. Regression for #654.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn delete_outcomes_match_the_trait() {
    let (_nats, port) = start_nats(TAG);
    let rt = runtime();
    let store = NatsStore::new(nats_config(port, "delete"), LEASE).expect("nats store");
    rt.block_on(async {
        support::delete_contract(&store).await;

        let mut watch = store.watch(Keyspace::Ephemeral, "lease").await.unwrap();
        let rev = store
            .create(Keyspace::Ephemeral, "lease", b"v".to_vec())
            .await
            .unwrap()
            .won()
            .unwrap();
        let expired = async {
            while let Some(event) = watch.next().await {
                if let WatchEvent::Delete { key, .. } = event.unwrap()
                    && key == "lease"
                {
                    return;
                }
            }
            panic!("watch ended before the lease expired");
        };
        tokio::time::timeout(LEASE * 4, expired)
            .await
            .expect("lease expires within four TTLs");
        assert!(matches!(
            store
                .delete(Keyspace::Ephemeral, "lease", Some(rev))
                .await
                .unwrap(),
            CasOutcome::Won(_)
        ));
    });
}

/// Fills the job's lease bucket with more keys than a listing buffers
/// before flow control pauses delivery, all written within one lease.
async fn fill_lease_bucket(
    store: &NatsStore,
    port: u16,
    job: &str,
) -> async_nats::jetstream::stream::Stream {
    // Provisions both buckets.
    store
        .get(Keyspace::Ephemeral, "none")
        .await
        .expect("provision");
    let client = async_nats::connect(format!("nats://127.0.0.1:{port}"))
        .await
        .expect("connect");
    let js = async_nats::jetstream::new(client);
    let kv = js
        .get_key_value(format!("spate_coordination_{job}_lease"))
        .await
        .expect("lease bucket");
    let mut acks = Vec::new();
    for i in 0..40_000 {
        acks.push(kv.put(format!("k{i}"), "v".into()));
        if acks.len() == 1024 {
            futures_util::future::try_join_all(acks.drain(..))
                .await
                .expect("put");
        }
    }
    futures_util::future::try_join_all(acks).await.expect("put");
    js.get_stream(format!("KV_spate_coordination_{job}_lease"))
        .await
        .expect("lease stream")
}

/// A listing whose undelivered messages expire ends, as a result or as
/// Retryable. Regression for #661.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn a_listing_whose_undelivered_tail_expires_ends() {
    let (_nats, port) = start_nats(TAG);
    let rt = runtime();
    rt.block_on(async {
        let store = NatsStore::new(nats_config(port, "stall-list"), LEASE).expect("store");
        fill_lease_bucket(&store, port, "stall-list").await;
        // One point read per key keeps the listing busy past the lease.
        let listed =
            tokio::time::timeout(Duration::from_secs(60), store.list(Keyspace::Ephemeral, ""))
                .await
                .expect("listing never ended");
        if let Err(e) = listed {
            assert!(matches!(e, StoreError::Retryable(_)), "{e}");
        }
    });
}

/// A watch snapshot whose undelivered messages expire ends, with
/// `SnapshotDone` or as Retryable. Regression for #661.
#[test]
#[ignore = "needs Docker; run explicitly"]
#[allow(clippy::print_stderr)]
fn a_watch_snapshot_whose_undelivered_tail_expires_ends() {
    let (_nats, port) = start_nats(TAG);
    let rt = runtime();
    // The poll's timer and the watch's drop both need a runtime context.
    let _context = rt.enter();
    let store = NatsStore::new(nats_config(port, "stall-watch"), LEASE).expect("store");
    let (mut lease, mut watch) = rt.block_on(async {
        let lease = fill_lease_bucket(&store, port, "stall-watch").await;
        let mut watch = store.watch(Keyspace::Ephemeral, "").await.expect("watch");
        watch
            .next()
            .await
            .expect("first event")
            .expect("first event");
        (lease, watch)
    });
    let mut last = None;
    spate_test::wait_until(LEASE * 5, "the lease bucket holds no message", || {
        let Ok(info) = rt.block_on(tokio::time::timeout(LEASE, lease.info())) else {
            return false;
        };
        let messages = info.expect("stream info").state.messages;
        if last != Some(messages) {
            eprintln!("lease bucket holds {messages} messages");
            last = Some(messages);
        }
        messages == 0
    });
    rt.block_on(async {
        let ended = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match watch.next().await {
                    Some(Ok(WatchEvent::SnapshotDone)) | None => return Ok(()),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e),
                }
            }
        })
        .await
        .expect("snapshot never ended");
        if let Err(e) = ended {
            assert!(matches!(e, StoreError::Retryable(_)), "{e}");
        }
    });
}

fn state_bucket_name(job: &str) -> String {
    format!("spate_coordination_{job}_state")
}

async fn jetstream(port: u16) -> async_nats::jetstream::Context {
    let client = async_nats::connect(format!("nats://127.0.0.1:{port}"))
        .await
        .expect("connect");
    async_nats::jetstream::new(client)
}

/// The state bucket's message count, live keys, and subjects matching
/// `filter` under the bucket.
async fn state_bucket(
    js: &async_nats::jetstream::Context,
    job: &str,
    filter: &str,
) -> (u64, usize, usize) {
    let bucket = state_bucket_name(job);
    let kv = js.get_key_value(&bucket).await.expect("state bucket");
    let messages = kv.status().await.expect("status").info.state.messages;
    let live = kv.keys().await.expect("keys").count().await;
    let subjects = js
        .get_stream(format!("KV_{bucket}"))
        .await
        .expect("stream")
        .info_with_subjects(format!("$KV.{bucket}.{filter}"))
        .await
        .expect("subjects")
        .count()
        .await;
    (messages, live, subjects)
}

/// Starts a worker, claims the job's one split and releases it.
fn start_claim_release(nats: NatsConfig, instance_id: Option<&str>, what: &str) {
    let rt = runtime();
    let mut w = worker_with(nats, rt.handle(), instance_id);
    w.start(Box::new(PhasedPlanner::one_final("markers:v1", &["m0"])))
        .expect("start");
    let mut held = Held::default();
    drive(&mut w, &mut held, what, |h| h.splits.len() == 1);
    w.release(&[split_id("m0")]).expect("release");
}

/// Waits until `done` holds for the state bucket, or panics with its
/// last reading.
fn await_state_bucket(
    port: u16,
    job: &str,
    filter: &str,
    done: impl Fn(u64, usize, usize) -> bool,
) {
    runtime().block_on(async {
        let js = jetstream(port).await;
        let deadline = Instant::now() + LEASE * 5;
        loop {
            let (messages, live, subjects) = state_bucket(&js, job, filter).await;
            if done(messages, live, subjects) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "state bucket: {messages} messages, {live} live keys, {subjects} subjects \
                 matching {filter}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
}

/// Keys deleted from the state bucket leave no marker once a lease has
/// passed, across restarts with a random instance id. Regression for #657.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn durable_markers_expire_across_restarts() {
    let (_nats, port) = start_nats(TAG);
    for i in 0..4 {
        start_claim_release(nats_config(port, "markers"), None, &format!("start {i}"));
        // Lets the leader see the departed instance and delete its record.
        std::thread::sleep(LEASE + Duration::from_millis(500));
    }
    await_state_bucket(port, "markers", "_probe.>", |messages, live, probes| {
        probes == 0 && messages == live as u64
    });
}

/// A state bucket created without per-message TTLs gains them on the
/// next start and keeps the rest of its stream config.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn an_existing_state_bucket_gains_message_ttls() {
    let (_nats, port) = start_nats(TAG);
    let rt = runtime();
    let js = rt.block_on(jetstream(port));
    rt.block_on(async {
        js.create_key_value(async_nats::jetstream::kv::Config {
            bucket: state_bucket_name("upgrade"),
            description: "operator note".into(),
            history: 1,
            max_bytes: 1 << 30,
            ..Default::default()
        })
        .await
        .expect("old-style state bucket");
    });
    start_claim_release(nats_config(port, "upgrade"), Some("gen-1"), "first start");
    let config = rt.block_on(async {
        let deadline = Instant::now() + LEASE * 5;
        loop {
            let mut stream = js
                .get_stream(format!("KV_{}", state_bucket_name("upgrade")))
                .await
                .expect("stream");
            let config = stream.info().await.expect("info").config.clone();
            if config.allow_message_ttl {
                return config;
            }
            assert!(Instant::now() < deadline, "message TTLs never enabled");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    assert_eq!(config.description.as_deref(), Some("operator note"));
    assert_eq!(config.max_bytes, 1 << 30);
    assert_eq!(config.subject_delete_marker_ttl, Some(LEASE));

    start_claim_release(nats_config(port, "upgrade"), Some("gen-2"), "second start");
    await_state_bucket(port, "upgrade", "_probe.gen-2", |_, _, probes| probes == 0);
}

/// A worker whose credentials may not update the state bucket's stream
/// starts, and its deletes keep writing plain markers.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn a_state_bucket_that_cannot_be_updated_keeps_working() {
    const CONF: &str = r#"
jetstream: enabled
authorization {
  users: [
    {
      user: spate
      password: spate
      permissions: {
        publish: { allow: [">"], deny: ["$JS.API.STREAM.UPDATE.>"] }
        subscribe: ">"
      }
    }
  ]
}
"#;
    let nats = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(CLIENT_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_copy_to("/etc/nats/deny-update.conf", CONF.as_bytes().to_vec())
        .with_cmd(["-c", "/etc/nats/deny-update.conf"])
        .start()
        .expect("start NATS");
    let port = nats.get_host_port_ipv4(CLIENT_PORT).expect("mapped port");
    let mut config = nats_config(port, "denied");
    config.credentials = NatsCredentials::UserPassword {
        username: "spate".into(),
        password: Secret::new("spate"),
    };
    let rt = runtime();
    let js = rt.block_on(async {
        let client =
            async_nats::ConnectOptions::with_user_and_password("spate".into(), "spate".into())
                .connect(format!("nats://127.0.0.1:{port}"))
                .await
                .expect("connect");
        async_nats::jetstream::new(client)
    });
    rt.block_on(async {
        js.create_key_value(async_nats::jetstream::kv::Config {
            bucket: state_bucket_name("denied"),
            history: 1,
            ..Default::default()
        })
        .await
        .expect("old-style state bucket");
    });
    start_claim_release(config.clone(), Some("gen-1"), "first start");
    start_claim_release(config, Some("gen-2"), "second start");
    let allowed = rt.block_on(async {
        let mut stream = js
            .get_stream(format!("KV_{}", state_bucket_name("denied")))
            .await
            .expect("stream");
        stream.info().await.expect("info").config.allow_message_ttl
    });
    assert!(!allowed, "the update was denied");
}
