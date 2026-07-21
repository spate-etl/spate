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
    worker_drain_deadline, worker_max_in_flight, worker_rebalance_delay,
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
    // for a running data plane — so that when the leader does move a split,
    // its new owner resumes from a real watermark instead of re-reading
    // from the start. The balancer itself does not consult the watermark;
    // this is here because the assertions below are about a partition that
    // a running pipeline could actually have reached.
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

// ----------------------------------------------------------------------
// Leader-computed assignment.

/// The scale-out case: one worker holds everything, a second joins, and
/// the leader moves it a share. The transfer must be cooperative — the
/// victim drains and commits before releasing — so the newcomer resumes
/// from a watermark rather than replaying.
#[test]
fn a_newcomer_is_assigned_a_share_of_a_loaded_fleet() {
    let rt = runtime();
    let store = store();
    let ids = ["s0", "s1", "s2", "s3"];
    let planner = || Box::new(PhasedPlanner::one_final("scaleout:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "worker-a takes the whole plan", |h| {
        h.splits.len() == ids.len()
    });
    support::commit_held(&mut a, &held_a);

    // B joins. The leader must take splits off A and give them to B.
    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    let deadline = Instant::now() + support::DEADLINE;
    while !(held_a.splits.len() == 2 && held_b.splits.len() == 2) {
        assert!(
            Instant::now() < deadline,
            "timed out balancing: a={:?} b={:?}",
            held_a.splits.keys().collect::<Vec<_>>(),
            held_b.splits.keys().collect::<Vec<_>>()
        );
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
        support::commit_held(&mut a, &held_a);
        // A consents to whatever the leader revoked; B just claims.
        support::consent_to_revocations(&mut a, &mut held_a);
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        held_a.splits.keys().all(|k| !held_b.splits.contains_key(k)),
        "the two halves must be disjoint"
    );
    // Every split B holds carries the progress A committed before
    // releasing: a cooperative move replays nothing.
    for (id, (_, progress)) in &held_b.splits {
        let watermark = progress
            .as_ref()
            .unwrap_or_else(|| panic!("{id} arrived without a resume point, so it will replay"))
            .watermark;
        assert_eq!(
            watermark,
            support::DRAINED_WATERMARK,
            "{id} resumed from the pre-rebalance watermark ({}), not the drained tail — \
             the move was forced, not cooperative",
            support::BASE_WATERMARK
        );
    }
}

/// KAFKA-12495 analogue. Kafka Connect's assignor assumed the member count
/// would not change between the revoking round and the assigning round; a
/// worker joining in between left work permanently stranded and the fleet
/// in a skew that never self-corrected (unfixed until 3.4.0). Continuous
/// reconciliation has no rounds to fall between, so a third worker
/// arriving mid-revocation must still converge.
#[test]
fn a_worker_joining_during_a_revocation_still_converges() {
    let rt = runtime();
    let store = store();
    let ids = ["s0", "s1", "s2", "s3", "s4", "s5"];
    let planner = || Box::new(PhasedPlanner::one_final("midjoin:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "worker-a takes the whole plan", |h| {
        h.splits.len() == ids.len()
    });
    support::commit_held(&mut a, &held_a);

    // B joins and the leader starts revoking — but B does NOT consent or
    // claim yet; we hold the transfer open deliberately.
    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive(&mut a, &mut held_a, "a revocation is requested", |h| {
        !h.revoke_requests.is_empty()
    });

    // C arrives in exactly that window.
    let mut c = worker(&store, rt.handle(), Some("worker-c"));
    c.start(planner()).unwrap();
    let mut held_c = Held::default();

    // All three must reach a balanced, disjoint partition: 6 splits, 3
    // workers, 2 each. A stranded split or a permanent skew fails here.
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        let counts = (
            held_a.splits.len(),
            held_b.splits.len(),
            held_c.splits.len(),
        );
        if counts == (2, 2, 2) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out converging: a/b/c = {counts:?}"
        );
        for (co, held) in [
            (&mut a, &mut held_a),
            (&mut b, &mut held_b),
            (&mut c, &mut held_c),
        ] {
            held.fold(co.poll().unwrap());
            support::commit_held(co, held);
            support::consent_to_revocations(co, held);
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let total: usize = held_a.splits.len() + held_b.splits.len() + held_c.splits.len();
    assert_eq!(
        total,
        ids.len(),
        "every split must still be held by someone"
    );
}

/// KAFKA-15693 analogue, arm 1 of 2. Connect's
/// `scheduled.rebalance.max.delay.ms=0` withheld a departed worker's
/// assignments *indefinitely* rather than reassigning them at once —
/// shipped broken from 2.3.0 through 3.7.0, because zero was a value
/// flowing through the general delay path instead of a case in its own
/// right. Zero here must mean immediately.
///
/// Note `rebalance_delay` governs the **leader**, so every worker carries
/// the same setting: configuring only the joiner would assert nothing,
/// since the surviving leader's own value is what decides. Paired with
/// [`a_departed_workers_splits_are_withheld_for_the_grace_window`] — one
/// arm alone cannot tell "prompt" from "no window at all".
#[test]
fn a_zero_rebalance_delay_reassigns_immediately() {
    assert!(
        reassignment_delay(std::time::Duration::ZERO) < LEASE * 3,
        "zero delay must not withhold the split"
    );
}

/// Arm 2: a non-zero window really does hold the work back, so arm 1 is
/// measuring the window rather than the lease expiry it sits on top of.
#[test]
fn a_departed_workers_splits_are_withheld_for_the_grace_window() {
    let withheld = reassignment_delay(LEASE * 5);
    assert!(
        withheld >= LEASE * 3,
        "a grace window must actually delay reassignment, took {withheld:?}"
    );
}

/// Crash one of two workers and return how long the survivor took to pick
/// up its split. Both workers share `delay`, because the leader's copy is
/// the one that governs.
fn reassignment_delay(delay: std::time::Duration) -> std::time::Duration {
    let rt = runtime();
    let store = store();
    let ids = ["s0", "s1"];
    let planner = || Box::new(PhasedPlanner::one_final("delay:v1", &ids));

    let mut a = worker_rebalance_delay(&store, rt.handle(), Some("worker-a"), delay);
    // B gets its own runtime so it can be killed outright rather than shut
    // down cleanly — a clean stop releases its split and would prove
    // nothing about reassignment.
    let rt_b = runtime();
    let mut b = worker_rebalance_delay(&store, rt_b.handle(), Some("worker-b"), delay);
    a.start(planner()).unwrap();
    b.start(planner()).unwrap();
    let (mut held_a, mut held_b) = (Held::default(), Held::default());
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "both workers hold a split",
        |x, y| x.splits.len() == 1 && y.splits.len() == 1,
    );
    support::commit_held(&mut a, &held_a);

    crash(rt_b, b);
    let died_at = Instant::now();
    drive(
        &mut a,
        &mut held_a,
        "the survivor picks up the departed worker's split",
        |h| h.splits.len() == 2,
    );
    died_at.elapsed()
}

/// A source that will not stop cleanly still has to give the split up —
/// a leader's revocation is a decision, not a request. The expensive path
/// (replay) is the price of declining, not an escape from it.
#[test]
fn a_drain_that_never_completes_is_forced_out() {
    let rt = runtime();
    let store = store();
    let ids = ["s0", "s1", "s2", "s3"];
    let planner = || Box::new(PhasedPlanner::one_final("forced:v1", &ids));

    // A short deadline so the force fires inside the test window.
    let mut a = worker_drain_deadline(&store, rt.handle(), Some("worker-a"), LEASE / 4);
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "worker-a takes the whole plan", |h| {
        h.splits.len() == ids.len()
    });
    support::commit_held(&mut a, &held_a);

    let mut b = worker_drain_deadline(&store, rt.handle(), Some("worker-b"), LEASE / 4);
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    let started = Instant::now();

    // A never consents: `Held::fold` records the request and does nothing,
    // which is exactly a declining source.
    let deadline = Instant::now() + support::DEADLINE;
    while held_b.splits.is_empty() {
        assert!(
            Instant::now() < deadline,
            "a declining source blocked the rebalance forever"
        );
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
        support::commit_held(&mut a, &held_a);
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        !held_a.revoke_requests.is_empty(),
        "the split should have been asked for before it was taken"
    );
    assert!(
        held_a.splits.len() < ids.len(),
        "the forced split must have left worker-a"
    );
    // The split has to leave because the DEADLINE fired, not because the
    // lease ran out — otherwise this test would still pass with the whole
    // forcing path deleted. `drain_deadline` is a quarter of the lease, so
    // nothing but a forced revocation can have moved a split this soon.
    assert!(
        started.elapsed() < LEASE,
        "a split moved only after a full lease ({:?}) — that is an expiry takeover, \
         not a forced revocation",
        started.elapsed()
    );
    // And what B holds must be something A was actually asked for.
    let moved: Vec<&String> = held_b.splits.keys().collect();
    assert!(
        moved.iter().all(|id| held_a.revoke_requests.contains(id)),
        "worker-b holds {moved:?} but the revocations asked for {:?}",
        held_a.revoke_requests
    );
}

/// **Absence of an assignment is not an instruction to hold nothing.**
///
/// A worker whose `assign.{instance}` record disappears — a leader gap, a
/// withdrawn assignment, a reconcile that finds the key gone — must keep
/// what it holds and wait to be told again. If that ever inverted, a
/// single leaderless moment would drain the entire fleet at once, and the
/// job would stall behind a rebalance nobody asked for. This is the most
/// dangerous inversion in the design, so it is asserted directly rather
/// than left to fall out of the happy path.
#[test]
fn a_withdrawn_assignment_record_does_not_release_anything() {
    let rt = runtime();
    let store = store();
    let ids = ["w0", "w1", "w2", "w3"];
    let planner = || Box::new(PhasedPlanner::one_final("withdrawn:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "both workers settle on a share",
        |x, y| !x.splits.is_empty() && !y.splits.is_empty(),
    );
    support::commit_held(&mut a, &held_a);
    support::commit_held(&mut b, &held_b);
    let before: Vec<String> = held_b.splits.keys().cloned().collect();

    // Delete B's assignment record out from under it. Whichever worker is
    // leader will republish eventually; the invariant is that B does not
    // give anything up in the meantime.
    rt.block_on(async {
        let outcome = store
            .delete(Keyspace::Durable, "assign.worker-b", None)
            .await
            .expect("delete");
        assert!(
            matches!(outcome, CasOutcome::Won(_)),
            "the record has to have existed, or this tests nothing"
        );
    });

    let until = Instant::now() + LEASE;
    while Instant::now() < until {
        for event in a.poll().expect("poll a") {
            held_a.fold(vec![event]);
        }
        for event in b.poll().expect("poll b") {
            assert!(
                !matches!(event, etl_coordination::CoordinationEvent::Lost { .. }),
                "worker-b released a split because its assignment record vanished: {event:?}"
            );
            held_b.fold(vec![event]);
        }
        assert!(
            held_b.revoke_requests.is_empty(),
            "an absent assignment record was read as an instruction to hold nothing: {:?}",
            held_b.revoke_requests
        );
        support::commit_held(&mut a, &held_a);
        support::commit_held(&mut b, &held_b);
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let after: Vec<String> = held_b.splits.keys().cloned().collect();
    assert_eq!(
        before, after,
        "worker-b's working set changed while it had no assignment record"
    );
}

/// Giving up the last split under a revocation is **not** a departure.
///
/// `release` doubles as the shutdown path: a worker that releases its last
/// split stops claiming, hands leadership back, and drops its presence
/// key. A revocation's hand-back must not trip that — otherwise revoking a
/// single-split worker's only split retires it from the fleet, and its
/// lane budget is lost for the rest of the job.
#[test]
fn a_revocation_of_the_last_split_is_not_a_departure() {
    let rt = runtime();
    let store = store();
    // Two splits, two workers, one lane each: A takes both, then must give
    // exactly one back — which is its last split only from B's arrival
    // onward, so the release path runs with `owned` about to empty.
    let ids = ["r0", "r1"];
    let planner = || Box::new(PhasedPlanner::one_final("lastsplit:v1", &ids));

    let mut a = worker_max_in_flight(&store, rt.handle(), Some("worker-a"), 1);
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "worker-a takes its one lane", |h| {
        h.splits.len() == 1
    });
    support::commit_held(&mut a, &held_a);

    let mut b = worker_max_in_flight(&store, rt.handle(), Some("worker-b"), 1);
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    // Drive until both hold one split each. A's hand-back empties its
    // working set momentarily; if that had been read as a departure it
    // would have dropped its presence key and never claimed again, so this
    // loop would time out.
    let deadline = Instant::now() + support::DEADLINE;
    while !(held_a.splits.len() == 1 && held_b.splits.len() == 1) {
        assert!(
            Instant::now() < deadline,
            "the fleet never reached one split each: a={:?} b={:?}",
            held_a.splits.keys().collect::<Vec<_>>(),
            held_b.splits.keys().collect::<Vec<_>>()
        );
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
        support::commit_held(&mut a, &held_a);
        support::commit_held(&mut b, &held_b);
        support::consent_to_revocations(&mut a, &mut held_a);
        std::thread::sleep(support::POLL_INTERVAL);
    }
    // Still a member: a departure deletes this key.
    let presence = rt.block_on(async {
        store
            .get(Keyspace::Ephemeral, "worker.worker-a")
            .await
            .expect("get")
    });
    assert!(
        presence.is_some(),
        "worker-a left the fleet after a revocation took its last split"
    );
}

/// The lane budget is a per-worker materialization limit, and the leader
/// honours each worker's own. Splits beyond the fleet's summed budget are
/// the queue: not assigned, not quarantined, not lost.
#[test]
fn splits_beyond_the_fleets_lane_budget_wait_in_the_queue() {
    let rt = runtime();
    let store = store();
    let ids = ["q0", "q1", "q2", "q3", "q4"];
    let planner = || Box::new(PhasedPlanner::one_final("queue:v1", &ids));

    let mut a = worker_max_in_flight(&store, rt.handle(), Some("worker-a"), 1);
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "worker-a takes its single lane", |h| {
        h.splits.len() == 1
    });

    // Hold there: one lane means one split, however much work is queued.
    let until = Instant::now() + LEASE;
    while Instant::now() < until {
        held_a.fold(a.poll().expect("poll a"));
        assert_eq!(
            held_a.splits.len(),
            1,
            "a one-lane worker took {} splits",
            held_a.splits.len()
        );
        support::commit_held(&mut a, &held_a);
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        held_a.quarantined.is_empty(),
        "queued splits were parked instead of waiting: {:?}",
        held_a.quarantined
    );

    // A second one-lane worker widens the fleet's budget by exactly one.
    let mut b = worker_max_in_flight(&store, rt.handle(), Some("worker-b"), 1);
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    drive_pair(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        "the second lane picks up queued work",
        |_, y| y.splits.len() == 1,
    );
}
