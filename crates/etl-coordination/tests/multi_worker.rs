//! Multi-worker protocol suite over one shared in-memory store: several
//! real coordinators race through the public synchronous API, exactly as
//! pipeline instances would. Every scenario is a defect class the design
//! must hold against — fencing, takeover, reclaim, twins, poison,
//! collective completion.

mod support;

use etl_coordination::store::memory::MemoryStore;
use etl_coordination::store::{CasOutcome, CoordinationStore as _, Keyspace};
use etl_coordination::{CoordinationErrorKind, SplitCoordinator, SplitProgress};
use std::time::Instant;
use support::{
    Held, LEASE, PhasedPlanner, crash, drive, drive_pair, runtime, split_id, store, worker,
};

#[test]
fn racing_workers_partition_the_splits_and_complete_collectively() {
    let rt = runtime();
    let store = store();
    let ids = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"];
    let planner = || Box::new(PhasedPlanner::one_final("partition:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    a.start(planner()).unwrap();
    b.start(planner()).unwrap();
    let (mut held_a, mut held_b) = (Held::default(), Held::default());

    // Converge to a full, disjoint 4/4 partition (target = ceil(8/2)).
    //
    // Each round also checkpoints whatever each worker holds, standing in
    // for a running data plane. That is load-bearing, not decoration: a
    // split with no committed progress has no resume point and is
    // deliberately not stealable, so if one worker wins the whole plan
    // before its peer announces presence, only a commit makes the
    // rebalance possible.
    let deadline = Instant::now() + support::DEADLINE;
    while !(held_a.splits.len() + held_b.splits.len() == ids.len()
        && held_a.splits.keys().all(|k| !held_b.splits.contains_key(k))
        && held_a.splits.len() == 4)
    {
        assert!(
            Instant::now() < deadline,
            "timed out: waiting for a full disjoint partition"
        );
        for (coordinator, held) in [(&mut a, &mut held_a), (&mut b, &mut held_b)] {
            held.fold(coordinator.poll().unwrap());
            support::commit_held(coordinator, held);
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    // Settle: drain any steal still in flight from the pre-balance era,
    // then re-check — a balanced fleet must hold its assignment.
    let settle_until = Instant::now() + LEASE * 2;
    while Instant::now() < settle_until {
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
    }
    assert_eq!(held_a.splits.len(), 4, "balance must hold once reached");
    assert_eq!(held_b.splits.len(), 4);
    assert!(held_a.splits.keys().all(|k| !held_b.splits.contains_key(k)));

    // Complete every held split; both workers must observe AllComplete.
    // Commits tolerate `Fenced`: a lease can legally move between the
    // snapshot and the write (e.g. a takeover after a scheduling stall) —
    // at-least-once means the NEW owner completes it instead, and the
    // fleet still converges.
    //
    // A split gained fresh carries no progress; one that moved during
    // convergence carries the checkpoint its previous owner committed —
    // that resume point is precisely what bounds a steal's replay, so
    // seeing it here is the fix working, not a leak.
    for held in [&held_a, &held_b] {
        for (_, progress) in held.splits.values() {
            if let Some(progress) = progress {
                assert_eq!(progress.watermark, 1, "carried progress is the checkpoint");
            }
        }
    }
    let deadline = Instant::now() + support::DEADLINE;
    while !(held_a.all_complete && held_b.all_complete) {
        assert!(Instant::now() < deadline, "collective completion timed out");
        for (held, coordinator) in [(&mut held_a, &mut a), (&mut held_b, &mut b)] {
            let ids: Vec<String> = held.splits.keys().cloned().collect();
            for id in ids {
                match coordinator.commit(&split_id(&id), &SplitProgress::completed(100, vec![])) {
                    Ok(()) => {
                        held.splits.remove(&id);
                    }
                    Err(e) if e.kind == CoordinationErrorKind::Fenced => {
                        held.splits.remove(&id); // the new owner completes it
                    }
                    Err(e) => panic!("commit failed: {e}"),
                }
            }
            held.fold(coordinator.poll().unwrap());
        }
    }
}

#[test]
fn death_takeover_waits_out_the_lease_and_carries_progress() {
    let rt = runtime();
    let store = store();
    let planner = || Box::new(PhasedPlanner::one_final("takeover:v1", &["t0", "t1"]));

    // A runs alone on its own runtime, claims everything, commits real
    // progress on one split.
    let rt_a = runtime();
    let mut a = worker(&store, rt_a.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming both splits", |h| {
        h.splits.len() == 2
    });
    a.commit(
        &split_id("t0"),
        &SplitProgress::new(42, b"resume-here".to_vec()),
    )
    .unwrap();
    let first_epochs: Vec<u64> = held_a.splits.values().map(|(e, _)| *e).collect();

    // A dies without releasing.
    let died_at = Instant::now();
    crash(rt_a, a);

    // B must wait out the lease, then take over with progress carried
    // and the epoch bumped past A's tenancy.
    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive(
        &mut b,
        &mut held_b,
        "B taking over the dead worker's splits",
        |h| h.splits.len() == 2,
    );
    // The lease expires one TTL after the LAST SUCCESSFUL WRITE. Renewals
    // are skipped while `elapsed < TTL/3` and ticks are jittered up to
    // 1.2x, so consecutive successful writes can legitimately sit up to
    // ~2.2 * TTL/3 apart — the last one may precede the death by that
    // much, making the earliest legitimate takeover
    // died_at + TTL - 2.2*TTL/3 ≈ died_at + 0.27*TTL. Assert the safe
    // floor below it: an instant (lease-ignoring) takeover still fails.
    assert!(
        died_at.elapsed() >= LEASE / 4,
        "takeover before the dead worker's lease could have expired: {:?}",
        died_at.elapsed()
    );
    let (epoch, progress) = &held_b.splits["t0"];
    let progress = progress
        .as_ref()
        .expect("progress carried to the new owner");
    assert_eq!(progress.watermark, 42);
    assert_eq!(progress.state, b"resume-here");
    assert!(
        first_epochs.iter().all(|first| epoch > first),
        "takeover must bump the epoch: {first_epochs:?} -> {epoch}"
    );
    assert!(held_b.splits["t1"].1.is_none(), "t1 never committed");
}

#[test]
fn graceful_release_hands_off_without_waiting_out_the_lease() {
    let rt = runtime();
    let store = store();
    let planner = || Box::new(PhasedPlanner::one_final("release:v1", &["r0", "r1"]));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming both splits", |h| {
        h.splits.len() == 2
    });

    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    let released_at = Instant::now();
    a.release(&[split_id("r0"), split_id("r1")]).unwrap();
    drop(a);
    drive(&mut b, &mut held_b, "B claiming the released splits", |h| {
        h.splits.len() == 2
    });
    assert!(
        released_at.elapsed() < LEASE,
        "released splits must hand off without a lease wait: {:?}",
        released_at.elapsed()
    );
}

#[test]
fn stable_instance_id_reclaims_fast_after_a_restart() {
    let rt = runtime();
    let store = store();
    let planner = || Box::new(PhasedPlanner::one_final("reclaim:v1", &["m0"]));

    let rt_old = runtime();
    let mut old = worker(&store, rt_old.handle(), Some("pod-1"));
    old.start(planner()).unwrap();
    let mut held_old = Held::default();
    drive(&mut old, &mut held_old, "predecessor claiming", |h| {
        h.splits.len() == 1
    });
    old.commit(&split_id("m0"), &SplitProgress::new(7, vec![]))
        .unwrap();
    crash(rt_old, old);

    // The restarted pod reclaims while the dead lease is still live —
    // well inside one TTL, no expiry wait.
    let restarted_at = Instant::now();
    let mut new = worker(&store, rt.handle(), Some("pod-1"));
    new.start(planner()).unwrap();
    let mut held_new = Held::default();
    drive(
        &mut new,
        &mut held_new,
        "restart reclaiming its split",
        |h| h.splits.len() == 1,
    );
    assert!(
        restarted_at.elapsed() < LEASE,
        "reclaim must not wait out the lease: {:?}",
        restarted_at.elapsed()
    );
    assert_eq!(
        held_new.splits["m0"].1.as_ref().map(|p| p.watermark),
        Some(7),
        "progress survives the restart"
    );
}

#[test]
fn live_twins_sharing_an_instance_id_are_fatal() {
    let rt = runtime();
    let store = store();
    let planner = || Box::new(PhasedPlanner::one_final("twin:v1", &["w0"]));

    let mut first = worker(&store, rt.handle(), Some("pod-1"));
    first.start(planner()).unwrap();
    let mut held_first = Held::default();
    drive(&mut first, &mut held_first, "first twin claiming", |h| {
        h.splits.len() == 1
    });

    // A second LIVE process with the same instance id fast-reclaims the
    // split (it cannot tell itself from a dead predecessor). Whichever twin
    // sees its own id under a foreign nonce first reports it: Fatal, naming
    // the misconfiguration; the other survives. Which one gets there first is
    // a reclaim-ordering race, so we poll BOTH — pinning the report to a
    // single worker times out whenever the other twin is the reporter.
    let mut second = worker(&store, rt.handle(), Some("pod-1"));
    second.start(planner()).unwrap();
    let mut held_second = Held::default();
    drive(
        &mut second,
        &mut held_second,
        "second twin reclaiming",
        |h| h.splits.len() == 1,
    );

    let deadline = Instant::now() + support::DEADLINE;
    let error = 'outer: loop {
        assert!(
            Instant::now() < deadline,
            "neither twin reported the shared instance_id"
        );
        for (twin, held) in [
            (&mut first, &mut held_first),
            (&mut second, &mut held_second),
        ] {
            match twin.poll() {
                Ok(events) => held.fold(events),
                Err(e) => break 'outer e,
            }
        }
    };
    assert_eq!(error.kind, CoordinationErrorKind::Fatal);
    assert!(error.to_string().contains("instance_id"), "{error}");
    assert!(error.to_string().contains("pod-1"), "{error}");
}

#[test]
fn a_fenced_zombie_commit_writes_nothing() {
    let rt = runtime();
    let store = store();
    let planner = || Box::new(PhasedPlanner::one_final("fence:v1", &["z0"]));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming the split", |h| {
        h.splits.len() == 1
    });
    a.commit(&split_id("z0"), &SplitProgress::new(10, b"a-10".to_vec()))
        .unwrap();

    // A thief takes the split behind A's back: lease CAS, then record
    // CAS with a bumped epoch — exactly what a stealing peer writes.
    let thief_watermark = steal_as_thief(&rt, &store, "z0", "thief");

    // A's next commit must be rejected with NOTHING written, and the
    // Lost event must follow.
    let error = a
        .commit(&split_id("z0"), &SplitProgress::new(11, b"a-11".to_vec()))
        .unwrap_err();
    assert_eq!(error.kind, CoordinationErrorKind::Fenced, "{error}");
    drive(&mut a, &mut held_a, "A observing the loss", |h| {
        !h.splits.contains_key("z0")
    });

    // The store still holds the thief's record, byte-untouched by A.
    let record = rt
        .block_on(store.get(Keyspace::Durable, "split.z0"))
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
    assert_eq!(json["owner"], "thief");
    assert_eq!(json["watermark"], thief_watermark);
}

/// Manually execute a steal exactly as a peer's task would: CAS the lease
/// key, then CAS the record with the epoch bumped. Returns the watermark
/// the thief wrote.
fn steal_as_thief(rt: &tokio::runtime::Runtime, store: &MemoryStore, id: &str, thief: &str) -> i64 {
    rt.block_on(async {
        let lease_key = format!("split.{id}");
        let lease = store
            .get(Keyspace::Ephemeral, &lease_key)
            .await
            .unwrap()
            .expect("victim lease");
        let mut lease_val: serde_json::Value = serde_json::from_slice(&lease.value).unwrap();
        let record = store
            .get(Keyspace::Durable, &lease_key)
            .await
            .unwrap()
            .expect("record");
        let mut record_val: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        let epoch = record_val["epoch"].as_u64().unwrap() + 1;
        lease_val["owner"] = thief.into();
        lease_val["nonce"] = "thief-nonce".into();
        lease_val["epoch"] = epoch.into();
        assert!(matches!(
            store
                .update(
                    Keyspace::Ephemeral,
                    &lease_key,
                    serde_json::to_vec(&lease_val).unwrap(),
                    lease.revision,
                )
                .await
                .unwrap(),
            CasOutcome::Won(_)
        ));
        record_val["epoch"] = epoch.into();
        record_val["owner"] = thief.into();
        let watermark = record_val["watermark"].as_i64().unwrap();
        assert!(matches!(
            store
                .update(
                    Keyspace::Durable,
                    &lease_key,
                    serde_json::to_vec(&record_val).unwrap(),
                    record.revision,
                )
                .await
                .unwrap(),
            CasOutcome::Won(_)
        ));
        watermark
    })
}

#[test]
fn poison_splits_quarantine_and_stall_instead_of_false_success() {
    let rt = runtime();
    let store = store();
    let planner = || Box::new(PhasedPlanner::one_final("poison:v1", &["good", "bad"]));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held = Held::default();
    drive(&mut a, &mut held, "claiming both splits", |h| {
        h.splits.len() == 2
    });
    a.commit(&split_id("good"), &SplitProgress::completed(5, vec![]))
        .unwrap();

    // The bad split fails on every tenancy until the attempt budget
    // (max_attempts = 4) parks it.
    let mut failures = 0;
    let deadline = Instant::now() + support::DEADLINE;
    while held.quarantined.is_empty() {
        assert!(Instant::now() < deadline, "quarantine never happened");
        if held.splits.contains_key("bad") {
            a.fail(&split_id("bad"), "undecodable descriptor").unwrap();
            held.splits.remove("bad");
            failures += 1;
        }
        held.fold(a.poll().unwrap());
    }
    assert_eq!(held.quarantined[0].0, "bad");
    assert_eq!(
        (failures, held.quarantined[0].1),
        (4, 4),
        "exactly max_attempts failing tenancies ran"
    );

    // A Final plan with quarantined work must stall, never AllComplete.
    drive(&mut a, &mut held, "waiting for the stall verdict", |h| {
        h.stalled.is_some()
    });
    assert_eq!(held.stalled, Some((1, 1)));
    assert!(!held.all_complete, "quarantine must block AllComplete");
}

#[test]
fn all_complete_reaches_a_late_standby_that_never_owned_anything() {
    let rt = runtime();
    let store = store();
    let planner = || Box::new(PhasedPlanner::one_final("standby:v1", &["only"]));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming the split", |h| {
        h.splits.len() == 1
    });

    // The standby joins after all work is claimed and never gains.
    let mut standby = worker(&store, rt.handle(), Some("worker-s"));
    standby.start(planner()).unwrap();
    let mut held_s = Held::default();

    a.commit(&split_id("only"), &SplitProgress::completed(9, vec![]))
        .unwrap();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut standby, &mut held_s),
        "completion reaching both the owner and the standby",
        |ha, hs| ha.all_complete && hs.all_complete,
    );
    assert!(held_s.splits.is_empty(), "the standby never owned work");
}

#[test]
fn a_newcomer_steals_from_a_loaded_worker() {
    let rt = runtime();
    let store = store();
    // 4 splits, one worker: it holds all 4 (target = min(8, 4)).
    let ids = ["b0", "b1", "b2", "b3"];
    let planner = || Box::new(PhasedPlanner::one_final("balance:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });

    // The data plane checkpoints: only now is the work resumable, and so
    // only now is it cheap enough to move. Before this, a steal would
    // replay each split from the start.
    support::commit_held(&mut a, &held_a);

    // A newcomer with nothing claimable must steal toward balance: the
    // pairwise rule converges to a 2/2 split.
    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "stealing toward balance",
        |ha, hb| ha.splits.len() == 2 && hb.splits.len() == 2,
    );
    // Steady state: balanced fleets do not oscillate.
    let before: Vec<String> = held_b.splits.keys().cloned().collect();
    std::thread::sleep(LEASE * 2);
    held_a.fold(a.poll().unwrap());
    held_b.fold(b.poll().unwrap());
    let after: Vec<String> = held_b.splits.keys().cloned().collect();
    assert_eq!(before, after, "balance must not oscillate");
    assert_eq!(held_a.splits.len(), 2);
}
