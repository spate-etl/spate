//! Multi-worker protocol suite over one shared in-memory store: several
//! real coordinators race through the public synchronous API, exactly as
//! pipeline instances would. Every scenario is a defect class the design
//! must hold against — fencing, takeover, reclaim, twins, poison,
//! collective completion.

mod support;

use etl_coordination::store::memory::MemoryStore;
use etl_coordination::store::{CasOutcome, CoordinationStore as _, Keyspace};
use etl_coordination::{
    Clock, CoordinationErrorKind, CoordinationEvent, SplitCoordinator, SplitProgress,
    StoreCoordinator,
};
use std::sync::Arc;
use std::time::Instant;
use support::{
    Held, LEASE, PhasedPlanner, TestClock, config, crash, drive, drive_pair, runtime, split_id,
    store, worker, worker_handoff_rounds,
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

/// The cooperative path: an under-target newcomer asks a loaded owner to
/// hand a split over rather than stealing it. The owner drains it, commits
/// the tail, and releases — the newcomer resumes from that final watermark,
/// so nothing is replayed (unlike a steal, which re-reads the uncommitted
/// tail). The owner keeps the rest of its work and stays in the fleet.
#[test]
fn a_newcomer_requests_a_handoff_and_claims_it_with_carried_progress() {
    let rt = runtime();
    let store = store();
    let ids = ["h0", "h1", "h2", "h3"];
    let planner = || Box::new(PhasedPlanner::one_final("handoff:v1", &ids));

    // A claims all four and checkpoints them (a running data plane's first
    // commit).
    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });
    support::commit_held(&mut a, &held_a);

    // B is patient: its fallback steal cannot fire inside the test window,
    // so the move below can only be a consented handoff.
    let mut b = worker_handoff_rounds(&store, rt.handle(), Some("worker-b"), 1_000);
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    // Drive both. A plays its own embedder by hand: on `HandoffRequested`
    // it commits a fresh tail watermark W' (what a drained data plane would
    // commit) and releases the split cooperatively.
    const TAIL: i64 = 100;
    let mut handed: Option<String> = None;
    let mut a_lost_handed = false;
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        assert!(Instant::now() < deadline, "handoff did not complete");
        for event in a.poll().unwrap() {
            match &event {
                CoordinationEvent::HandoffRequested { split } if handed.is_none() => {
                    let id = split.as_str().to_string();
                    a.commit(&split_id(&id), &SplitProgress::new(TAIL, b"tail".to_vec()))
                        .unwrap();
                    a.release_handoff(&[split_id(&id)]).unwrap();
                    handed = Some(id);
                }
                CoordinationEvent::Lost { split } if handed.as_deref() == Some(split.as_str()) => {
                    a_lost_handed = true;
                }
                _ => {}
            }
            held_a.fold(vec![event]);
        }
        held_b.fold(b.poll().unwrap());
        if let Some(handed) = &handed
            && held_b.splits.contains_key(handed)
        {
            break;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let handed = handed.expect("A was asked to hand a split over");

    assert!(
        !a_lost_handed,
        "a graceful grant must never surface as a Lost on the victim"
    );
    let (_, progress) = &held_b.splits[&handed];
    assert_eq!(
        progress.as_ref().map(|p| p.watermark),
        Some(TAIL),
        "the handed split resumes from the victim's final commit: zero replay"
    );

    // A handed off one split of four, so it is NOT parting: its presence key
    // survives (a departure would have deleted it).
    let present = rt
        .block_on(store.get(Keyspace::Ephemeral, "worker.worker-a"))
        .unwrap();
    assert!(
        present.is_some(),
        "A gave up one split of four; it must remain in the fleet"
    );
}

/// A served request starts the next one on a fresh fallback clock. The
/// victim's grant-ack deletes the request key, which the requester cannot
/// tell apart from a TTL blip — and the blip rule preserves `since_round`
/// across recreates. Without closing the cycle on the granted claim, the
/// second request would be born already past due and every move after the
/// first would degrade to a fenced steal (surfacing as `Lost` on the
/// victim). Here a prompt victim grants three sequential moves under a
/// tight round budget, and none of them may be a steal.
#[test]
fn sequential_grants_each_get_a_fresh_fallback_clock() {
    let rt = runtime();
    let store = store();
    let ids = ["f0", "f1", "f2", "f3", "f4", "f5"];
    let planner = || Box::new(PhasedPlanner::one_final("fresh-clock:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 6
    });
    support::commit_held(&mut a, &held_a);

    // Four-round budget, and every grant below is answered only after a
    // deliberate ~two-round drain delay. A per-request clock absorbs the
    // delay with margin; only a clock carried over from the previous,
    // already-served request can expire mid-drain and turn a later move
    // into a steal (observable as a Lost on A, and as the stolen split
    // resuming from the old commit instead of the drained tail).
    let mut b = worker_handoff_rounds(&store, rt.handle(), Some("worker-b"), 4);
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    const TAIL: i64 = 500;
    let drain = support::LEASE * 2 / 3; // ~two heartbeat rounds
    let mut a_lost: Vec<String> = Vec::new();
    let mut pending: Option<(String, Instant)> = None;
    let deadline = Instant::now() + support::DEADLINE;
    // Fair share of six over two workers is three; the pairwise rule
    // stops granting at 3/3.
    while held_b.splits.len() < 3 {
        assert!(
            Instant::now() < deadline,
            "three sequential grants did not complete (B holds {}, A lost {:?})",
            held_b.splits.len(),
            a_lost,
        );
        for event in a.poll().unwrap() {
            match &event {
                CoordinationEvent::HandoffRequested { split } if pending.is_none() => {
                    pending = Some((split.as_str().to_string(), Instant::now() + drain));
                }
                CoordinationEvent::Lost { split } => {
                    a_lost.push(split.as_str().to_string());
                }
                _ => {}
            }
            held_a.fold(vec![event]);
        }
        if let Some((id, due)) = &pending
            && Instant::now() >= *due
        {
            a.commit(&split_id(id), &SplitProgress::new(TAIL, b"tail".to_vec()))
                .unwrap();
            a.release_handoff(&[split_id(id)]).unwrap();
            pending = None;
        }
        held_b.fold(b.poll().unwrap());
        std::thread::sleep(support::POLL_INTERVAL);
    }

    assert!(
        a_lost.is_empty(),
        "every move must be a consented grant, never a fenced steal: {a_lost:?}"
    );
    for (id, (_, progress)) in &held_b.splits {
        assert_eq!(
            progress.as_ref().map(|p| p.watermark),
            Some(TAIL),
            "{id} must resume from the victim's final commit"
        );
    }
}

/// A request the victim never answers must not wedge: after the round
/// budget elapses the requester falls back to today's fenced steal, which
/// bounds replay to the victim's last committed watermark.
#[test]
fn an_unanswered_request_falls_back_to_a_steal_after_n_rounds() {
    let rt = runtime();
    let store = store();
    let ids = ["u0", "u1", "u2", "u3"];
    let planner = || Box::new(PhasedPlanner::one_final("unanswered:v1", &ids));

    // A holds everything and commits (so the fallback steal has a resume
    // point). A's embedder never acts on `HandoffRequested`: an unresponsive
    // victim.
    let mut a = worker_handoff_rounds(&store, rt.handle(), Some("worker-a"), 1);
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });
    support::commit_held(&mut a, &held_a);

    // B: one unanswered round, then the fenced fallback steal. Both converge
    // to a 2/2 balance and A observes the fenced splits as Lost.
    let mut b = worker_handoff_rounds(&store, rt.handle(), Some("worker-b"), 1);
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "B falling back to a steal after an unanswered request",
        |ha, hb| hb.splits.len() == 2 && ha.splits.len() == 2,
    );
    for (_, progress) in held_b.splits.values() {
        assert_eq!(
            progress.as_ref().map(|p| p.watermark),
            Some(1),
            "the fallback steal resumes from A's last committed watermark"
        );
    }
}

/// G5: a handoff release that empties the working set is a grant, not a
/// departure. In production the split being drained can turn out to be the
/// last one held (a peer took the others mid-drain); the embedder then
/// grants its last split through `release_handoff`, which — unlike
/// `release` — must NOT retire the worker. Proven with a solo worker: it
/// re-claims its own still-unowned hand-back (a *parted* worker sits idle
/// forever and the split would strand).
#[test]
fn a_handoff_of_the_only_split_is_not_a_departure() {
    let rt = runtime();
    let store = store();
    let planner = || Box::new(PhasedPlanner::one_final("solo-handoff:v1", &["only"]));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming its only split", |h| {
        h.splits.len() == 1
    });
    a.commit(&split_id("only"), &SplitProgress::new(5, vec![]))
        .unwrap();
    let epoch_before = held_a.splits["only"].0;

    // A grants its LAST held split through the handoff path — working set
    // empties. `release` would retire A here; `release_handoff` must not.
    a.release_handoff(&[split_id("only")]).unwrap();
    drive(
        &mut a,
        &mut held_a,
        "A re-claiming its own hand-back (a handoff grant is not a departure)",
        |h| {
            h.splits
                .get("only")
                .is_some_and(|(epoch, _)| *epoch > epoch_before)
        },
    );

    // Belt and suspenders: a departure deletes the presence key; a grant
    // must leave it standing.
    let present = rt
        .block_on(store.get(Keyspace::Ephemeral, "worker.worker-a"))
        .unwrap();
    assert!(
        present.is_some(),
        "a handoff grant must not retire the worker"
    );
}

/// The per-victim request key is the fairness arbiter: two under-target
/// workers eyeing one loaded victim can never both hold an open request —
/// at most one `handoff.{victim}` key ever exists (create-if-absent).
#[test]
fn handoff_requests_are_serialized_per_victim() {
    let rt = runtime();
    let store = store();
    let ids = ["z0", "z1", "z2", "z3", "z4", "z5"];
    let planner = || Box::new(PhasedPlanner::one_final("serialize:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming all six", |h| {
        h.splits.len() == 6
    });
    support::commit_held(&mut a, &held_a);

    // Two under-target newcomers both eye the single loaded victim. A never
    // grants (its embedder ignores the requests) and the requesters are
    // patient, so requests stay outstanding — yet the store must show at
    // most one at a time.
    let mut b = worker_handoff_rounds(&store, rt.handle(), Some("worker-b"), 1_000);
    let mut c = worker_handoff_rounds(&store, rt.handle(), Some("worker-c"), 1_000);
    b.start(planner()).unwrap();
    c.start(planner()).unwrap();
    let (mut held_b, mut held_c) = (Held::default(), Held::default());

    let deadline = Instant::now() + LEASE * 4;
    let mut saw_request = false;
    while Instant::now() < deadline {
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
        held_c.fold(c.poll().unwrap());
        let keys = rt
            .block_on(store.list(Keyspace::Ephemeral, "handoff."))
            .unwrap();
        assert!(
            keys.len() <= 1,
            "at most one handoff request key may exist at once: {:?}",
            keys.iter().map(|e| e.key.clone()).collect::<Vec<_>>()
        );
        if let Some(entry) = keys.first() {
            saw_request = true;
            assert_eq!(entry.key, "handoff.worker-a", "A is the only viable victim");
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        saw_request,
        "at least one handoff request should have formed"
    );
}

/// H1: a victim's UNRELATED lease traffic must never be read as the grant.
/// A requester marks a split incoming ONLY from the victim's `granted`
/// annotation on its request key — so when the victim `fail`s a *different*
/// split (its lease vanishing for a reason that is not a grant), the
/// requester takes that split over WITHOUT closing its handoff cycle, and the
/// real grant still resolves afterwards. Pre-fix, the failed split's lease
/// delete looked like the grant: the request retired early and the annotated
/// split stranded.
#[test]
fn a_victims_unrelated_lease_traffic_is_not_a_grant() {
    let rt = runtime();
    let store = store();
    let ids = ["x0", "x1", "x2", "x3"];
    let planner = || Box::new(PhasedPlanner::one_final("unrelated-traffic:v1", &ids));

    // A claims all four and checkpoints them.
    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });
    support::commit_held(&mut a, &held_a);

    // B is patient: its fallback steal cannot fire inside the test window.
    let mut b = worker_handoff_rounds(&store, rt.handle(), Some("worker-b"), 1_000);
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    const TAIL: i64 = 100;
    let mut granted: Option<String> = None; // the annotated grant G
    let mut failed: Option<String> = None; // the unrelated split X we fail
    let mut request_survived = false;
    let mut released = false;
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        assert!(
            Instant::now() < deadline,
            "handoff never completed (granted={granted:?}, failed={failed:?})"
        );
        for event in a.poll().unwrap() {
            if let CoordinationEvent::HandoffRequested { split } = &event
                && granted.is_none()
            {
                let g = split.as_str().to_string();
                // Fail a DIFFERENT owned split: unrelated lease traffic. Its
                // lease delete must NOT be mistaken for the grant of G.
                let x = held_a
                    .splits
                    .keys()
                    .find(|id| **id != g)
                    .cloned()
                    .expect("A holds more than one split");
                a.fail(&split_id(&x), "unrelated poison").unwrap();
                granted = Some(g);
                failed = Some(x);
            }
            held_a.fold(vec![event]);
        }
        held_b.fold(b.poll().unwrap());

        // Once B has taken the failed split X over, the request key must
        // still exist — the unrelated fail did not close the cycle. Only then
        // grant G for real.
        if let (Some(g), Some(x)) = (granted.as_ref(), failed.as_ref())
            && !released
            && held_b.splits.contains_key(x)
        {
            let keys = rt
                .block_on(store.list(Keyspace::Ephemeral, "handoff."))
                .unwrap();
            assert!(
                keys.iter().any(|e| e.key == "handoff.worker-a"),
                "the request must survive an unrelated fail, not be retired by it: {:?}",
                keys.iter().map(|e| e.key.clone()).collect::<Vec<_>>()
            );
            request_survived = true;
            a.commit(&split_id(g), &SplitProgress::new(TAIL, b"tail".to_vec()))
                .unwrap();
            a.release_handoff(&[split_id(g)]).unwrap();
            released = true;
        }

        if let Some(g) = granted.as_ref()
            && held_b.splits.contains_key(g)
        {
            break;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        request_survived,
        "B never took the unrelated failed split over"
    );
    let g = granted.unwrap();
    let x = failed.unwrap();
    assert_ne!(g, x);
    assert_eq!(
        held_b.splits[&g].1.as_ref().map(|p| p.watermark),
        Some(TAIL),
        "the real grant still resolves: G resumes from the victim's drained tail"
    );
}

/// A declined grant must free the victim's one-grant-at-a-time slot so it can
/// offer a DIFFERENT split. On a refusal the victim cools the declined split
/// down for one round budget and re-picks; the requester ends up holding the
/// second split, never the first. Pre-fix, a silent decline pinned the slot
/// and rebalancing against that victim froze.
#[test]
fn a_declined_grant_frees_the_slot_for_another_split() {
    let rt = runtime();
    let store = store();
    let ids = ["c0", "c1", "c2", "c3"];
    let planner = || Box::new(PhasedPlanner::one_final("declined:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });
    support::commit_held(&mut a, &held_a);

    let mut b = worker_handoff_rounds(&store, rt.handle(), Some("worker-b"), 1_000);
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    const TAIL: i64 = 200;
    let mut declined: Option<String> = None; // S1 — refused
    let mut granted: Option<String> = None; // S2 — the different split, granted
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        assert!(
            Instant::now() < deadline,
            "the declined slot never freed for another split (declined={declined:?}, \
             granted={granted:?})"
        );
        for event in a.poll().unwrap() {
            if let CoordinationEvent::HandoffRequested { split } = &event {
                let id = split.as_str().to_string();
                if declined.is_none() {
                    // The first offer: refuse it (the source cannot serve it).
                    a.decline_handoff(&split_id(&id)).unwrap();
                    declined = Some(id);
                } else if granted.is_none() && declined.as_deref() != Some(id.as_str()) {
                    // A DIFFERENT split was offered: the slot freed. Grant it.
                    a.commit(&split_id(&id), &SplitProgress::new(TAIL, b"tail".to_vec()))
                        .unwrap();
                    a.release_handoff(&[split_id(&id)]).unwrap();
                    granted = Some(id);
                }
            }
            held_a.fold(vec![event]);
        }
        held_b.fold(b.poll().unwrap());
        if let Some(s2) = granted.as_ref()
            && held_b.splits.contains_key(s2)
        {
            break;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let s1 = declined.unwrap();
    let s2 = granted.unwrap();
    assert_ne!(
        s1, s2,
        "the victim must offer a different split after the decline"
    );
    assert_eq!(
        held_b.splits[&s2].1.as_ref().map(|p| p.watermark),
        Some(TAIL),
        "B resumes the second split from the victim's drained tail"
    );
    assert!(
        !held_b.splits.contains_key(&s1),
        "B must never receive the declined split"
    );
}

/// H2: a served grant that a peer wins must close the requester's cycle, not
/// resurrect the request. Once the victim has annotated the grant on the
/// request key (attribution), a vanished key is SERVED, and a foreign owner
/// appearing on the granted split closes the cycle — the requester never
/// re-creates the request for a move that already happened. Pre-fix, the
/// ack-deleted key looked like a TTL blip and the request was recreated
/// within a round, making the victim drain a second split for nothing.
///
/// The victim here is fully hand-driven (no live worker A). A throwaway
/// worker with instance id `worker-a` seeds the split records — so their
/// fingerprint pins are correct, the same shape `steal_as_thief` relies on —
/// then is crashed; a frozen clock keeps its seeded leases alive with no live
/// worker renewing them, so the scenario is deterministic and the annotate /
/// steal / ack are all applied by hand.
#[test]
fn a_served_grant_lost_to_a_peer_does_not_resurrect_the_request() {
    let rt = runtime();
    let rt_seed = runtime();
    // Frozen clock shared by the store and both coordinators: the hand-written
    // `worker-a` leases never expire with no worker renewing them, so B can
    // never take a split over through plain expiry and the only thing under
    // test is the served-grant bookkeeping.
    let clock: Arc<dyn Clock> = TestClock::frozen();
    let store = MemoryStore::with_clock(support::LEASE, clock.clone());
    let planner = || Box::new(PhasedPlanner::one_final("served-lost:v1", &["g0", "u0"]));

    // Seed: a throwaway `worker-a` claims both splits and commits ONLY g0
    // (u0 stays uncommitted), then crashes WITHOUT releasing — so g0/u0 stay
    // owned by `worker-a` with valid, fingerprint-pinned records.
    let mut seed = StoreCoordinator::with_clock(
        store.clone(),
        config(Some("worker-a")),
        rt_seed.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("seed coordinator");
    seed.start(planner()).unwrap();
    let mut held_seed = Held::default();
    drive(&mut seed, &mut held_seed, "worker-a claiming both", |h| {
        h.splits.len() == 2
    });
    seed.commit(&split_id("g0"), &SplitProgress::new(5, b"g0-tail".to_vec()))
        .unwrap();
    crash(rt_seed, seed);

    // Real worker B is patient: it opens a request against worker-a and never
    // falls back to a steal inside the window.
    let mut cfg_b = config(Some("worker-b"));
    cfg_b.handoff_rounds = 1_000;
    let mut b = StoreCoordinator::with_clock(
        store.clone(),
        cfg_b,
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator B");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    // B opens the request (worker-a holds 2, B holds 0 — the pairwise rule
    // justifies it).
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        assert!(
            Instant::now() < deadline,
            "B never opened a handoff request"
        );
        held_b.fold(b.poll().unwrap());
        if rt
            .block_on(store.get(Keyspace::Ephemeral, "handoff.worker-a"))
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }

    // Hand-annotate the request key with the grant attribution (granted=g0),
    // retrying against B's per-round refreshes. HandoffVal serializes
    // `granted: Option<String>` as the string or null.
    let annotate_rev = loop {
        let entry = rt
            .block_on(store.get(Keyspace::Ephemeral, "handoff.worker-a"))
            .unwrap()
            .expect("B's request key");
        let mut val: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        val["granted"] = serde_json::Value::from("g0");
        match rt
            .block_on(store.update(
                Keyspace::Ephemeral,
                "handoff.worker-a",
                serde_json::to_vec(&val).unwrap(),
                entry.revision,
            ))
            .unwrap()
        {
            CasOutcome::Won(rev) => break rev,
            CasOutcome::Lost => std::thread::sleep(support::POLL_INTERVAL),
        }
    };

    // Drive B until it has re-written the key while PRESERVING the grant — the
    // observable proof that it adopted the attribution (handoff_incoming={g0}).
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        assert!(
            Instant::now() < deadline,
            "B never adopted the grant annotation"
        );
        held_b.fold(b.poll().unwrap());
        if let Some(entry) = rt
            .block_on(store.get(Keyspace::Ephemeral, "handoff.worker-a"))
            .unwrap()
        {
            let val: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            if entry.revision > annotate_rev && val["granted"] == "g0" {
                break;
            }
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }

    // A peer (worker-c) wins the granted split, then the request key is
    // ack-deleted — exactly the served-grant-lost-to-a-peer shape.
    let _ = steal_as_thief(&rt, &store, "g0", "worker-c");
    let _ = rt
        .block_on(store.delete(Keyspace::Ephemeral, "handoff.worker-a", None))
        .expect("delete the request key");

    // Over the next several rounds B must NOT resurrect the request (the cycle
    // is closed), and must NOT claim or steal the split worker-c already won.
    // worker-a now holds only u0 and worker-c holds g0 — each ≤ B's own+1 — so
    // no victim justifies a fresh request, making the empty-prefix assertion
    // deterministic.
    let observe_until = Instant::now() + support::LEASE * 2;
    while Instant::now() < observe_until {
        held_b.fold(b.poll().unwrap());
        let handoff_keys = rt
            .block_on(store.list(Keyspace::Ephemeral, "handoff."))
            .unwrap();
        assert!(
            handoff_keys.is_empty(),
            "a served grant lost to a peer must not resurrect the request: {:?}",
            handoff_keys
                .iter()
                .map(|e| e.key.clone())
                .collect::<Vec<_>>()
        );
        let g_record = rt
            .block_on(store.get(Keyspace::Durable, "split.g0"))
            .unwrap()
            .expect("g0 record");
        let g_val: serde_json::Value = serde_json::from_slice(&g_record.value).unwrap();
        assert_eq!(
            g_val["owner"], "worker-c",
            "B must not claim or steal the split a peer already won"
        );
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        held_b.splits.is_empty(),
        "B took nothing over: the served grant went to worker-c"
    );
}
