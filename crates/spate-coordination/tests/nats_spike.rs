//! Semantics spike against a real NATS 2.11 server: pins down the exact
//! JetStream KV behaviors the lease design builds on, BEFORE anything
//! builds on them. Each assertion pins one fact the design rests on:
//!
//! 1. CAS semantics. `create` is create-if-absent, `update` is
//!    compare-and-swap on revision, revisions are bucket-wide stream
//!    sequences (strictly increasing per key), and a deleted/expired key
//!    is re-creatable with `create`.
//! 2. Heartbeats. A bucket-level `max_age` applies per *message*, so a
//!    key that is CAS-rewritten inside the window survives indefinitely
//!    (this is the lease heartbeat), while an untouched key expires.
//! 3. Expiry visibility. With `limit_markers` configured, the expiry of
//!    an untouched key surfaces to a live watcher as a non-Put operation
//!    (the takeover trigger), and the asserted variant documents which.
//! 4. Watch snapshots. What a fresh watcher replays and how "caught up
//!    with current state" is detected.
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo test -p spate-coordination --test nats_spike -- --ignored
//! ```
#![cfg(feature = "nats")]

use async_nats::jetstream::kv;
use futures_util::StreamExt as _;
use std::time::{Duration, Instant};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};

const IMAGE: &str = "nats";
const TAG: &str = "2.11-alpine";
const CLIENT_PORT: u16 = 4222;

/// Lease TTL used by the expiry scenarios. Kept short so the whole spike
/// runs in well under a minute; production floors are far above this.
const MAX_AGE: Duration = Duration::from_secs(2);

#[test]
#[ignore = "needs Docker; run explicitly"]
fn kv_semantics() {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(CLIENT_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .expect("start NATS (is Docker running? first run pulls the image)");
    let port = container
        .get_host_port_ipv4(CLIENT_PORT)
        .expect("mapped client port");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let client = async_nats::connect(format!("nats://127.0.0.1:{port}"))
            .await
            .expect("connect");
        let js = async_nats::jetstream::new(client);

        cas_semantics(&js).await;
        heartbeat_and_expiry(&js).await;
        watch_snapshot(&js).await;
    });
}

/// Fact 1: create-if-absent, CAS-on-revision, monotone revisions,
/// re-creatable after delete.
async fn cas_semantics(js: &async_nats::jetstream::Context) {
    let state = js
        .create_key_value(kv::Config {
            bucket: "state".into(),
            history: 1,
            ..Default::default()
        })
        .await
        .expect("create state bucket");

    let r1 = state.create("split.a", "v1".into()).await.expect("create");
    let dup = state.create("split.a", "dup".into()).await;
    assert!(dup.is_err(), "second create must be rejected, got {dup:?}");

    let r2 = state
        .update("split.a", "v2".into(), r1)
        .await
        .expect("matched-revision update");
    assert!(r2 > r1, "update revision must advance: {r1} -> {r2}");

    let stale = state.update("split.a", "v3".into(), r1).await;
    assert!(
        stale.is_err(),
        "stale-revision update must be rejected, got {stale:?}"
    );

    // Revisions are stream sequences shared by the bucket: another key's
    // first revision continues the sequence rather than starting at 1.
    let r3 = state.create("split.b", "v1".into()).await.expect("create");
    assert!(
        r3 > r2,
        "revisions are bucket-wide monotone sequences: {r2} -> {r3}"
    );

    // Release-then-reclaim path: an explicitly deleted key must accept a
    // fresh `create` (claim after graceful release).
    state
        .delete_expect_revision("split.a", Some(r2))
        .await
        .expect("delete at current revision");
    // `entry()` surfaces the delete MARKER (operation Delete), not absence —
    // a store adapter must translate non-Put operations to "absent". The
    // value view `get()` does that filtering itself.
    let entry = state.entry("split.a").await.expect("entry after delete");
    assert!(
        matches!(entry, Some(ref e) if e.operation == kv::Operation::Delete),
        "entry() after delete returns the marker, got {entry:?}"
    );
    let value = state.get("split.a").await.expect("get after delete");
    assert!(value.is_none(), "get() reads a deleted key as absent");
    state
        .create("split.a", "v4".into())
        .await
        .expect("deleted key must be re-creatable");
}

/// Facts 2 + 3: a CAS-rewritten key outlives max_age (heartbeat); an
/// untouched key expires and the expiry is watcher-visible as a non-Put
/// entry (with `limit_markers` configured).
async fn heartbeat_and_expiry(js: &async_nats::jetstream::Context) {
    let lease = js
        .create_key_value(kv::Config {
            bucket: "lease".into(),
            history: 1,
            max_age: MAX_AGE,
            limit_markers: Some(Duration::from_secs(30)),
            ..Default::default()
        })
        .await
        .expect("create lease bucket (limit_markers requires server >= 2.11)");

    // Watcher starts before the keys exist so it sees the whole lifecycle.
    let mut watch = lease.watch_all().await.expect("watch lease bucket");

    // Heartbeated key: rewrite every MAX_AGE/4 for 2.5x MAX_AGE.
    let mut rev = lease.create("hb", "0".into()).await.expect("create hb");
    let hb_until = Instant::now() + MAX_AGE * 5 / 2;
    while Instant::now() < hb_until {
        tokio::time::sleep(MAX_AGE / 4).await;
        rev = lease
            .update("hb", "beat".into(), rev)
            .await
            .expect("heartbeat rewrite must keep the key alive past max_age");
    }
    assert!(
        lease.entry("hb").await.expect("entry").is_some(),
        "a key rewritten inside max_age survives it: heartbeat works"
    );

    // Untouched key: must expire and surface to the watcher.
    lease.create("dead", "0".into()).await.expect("create dead");
    let deadline = Instant::now() + MAX_AGE * 5;
    let expiry_op = loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .expect("lease expiry must reach a live watcher within ~5x max_age");
        let entry = tokio::time::timeout(remaining, watch.next())
            .await
            .expect("watch delivers the expiry before the deadline")
            .expect("watch stream stays open")
            .expect("watch entry");
        if entry.key == "dead" && entry.operation != kv::Operation::Put {
            break entry.operation;
        }
    };
    // Pinned observation: the max_age limit marker surfaces as Purge
    // (marker reason MaxAge), NOT Delete. Watchers distinguishing
    // graceful release (Delete) from expiry (Purge) can rely on this.
    assert_eq!(
        expiry_op,
        kv::Operation::Purge,
        "expiry surfaced as unexpected operation {expiry_op:?}"
    );

    let dead_entry = lease.entry("dead").await.expect("entry");
    assert!(
        matches!(dead_entry, Some(ref e) if e.operation != kv::Operation::Put),
        "entry() after expiry returns the marker, got {dead_entry:?}"
    );
    assert!(
        lease.get("dead").await.expect("get").is_none(),
        "get() reads an expired key as absent"
    );
    lease
        .create("dead", "1".into())
        .await
        .expect("expired key must be re-creatable (claim after death)");
}

/// Fact 4: what a fresh watcher replays, and how catch-up is detected.
async fn watch_snapshot(js: &async_nats::jetstream::Context) {
    let state = js.get_key_value("state").await.expect("state bucket");

    // Bucket currently holds split.a (recreated) and split.b. A fresh
    // watcher with history must replay the latest value per key, then go
    // live. Catch-up detection: `Entry::seen_current` flips true once the
    // snapshot is fully delivered (fallback if this field does not exist:
    // compare against the bucket status' last sequence).
    let mut watch = state.watch_with_history(">").await.expect("watch");
    let mut snapshot = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .expect("snapshot must be delivered promptly");
        let entry = tokio::time::timeout(remaining, watch.next())
            .await
            .expect("snapshot entry before deadline")
            .expect("watch stream stays open")
            .expect("watch entry");
        let caught_up = entry.seen_current;
        snapshot.push((entry.key, entry.revision, entry.operation));
        if caught_up {
            break;
        }
    }
    let keys: Vec<&str> = snapshot.iter().map(|(k, _, _)| k.as_str()).collect();
    assert!(
        keys.contains(&"split.a") && keys.contains(&"split.b"),
        "history replay must deliver the latest value of every key, got {snapshot:?}"
    );
    assert!(
        snapshot.len() <= 3,
        "history=1 must replay at most one entry per key, got {snapshot:?}"
    );
}
