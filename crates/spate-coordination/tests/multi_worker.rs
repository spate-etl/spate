//! Multi-worker protocol suite over one shared in-memory store: several
//! real coordinators race through the public synchronous API, exactly as
//! pipeline instances would. Every scenario is a defect class the design
//! must hold against — fencing, takeover, reclaim, twins, poison,
//! collective completion.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{CasOutcome, CoordinationStore as _, Keyspace};
use spate_coordination::{CoordinationErrorKind, SplitCoordinator, SplitProgress};
use std::time::Instant;
use support::{
    Held, LEASE, PhasedPlanner, crash, drive, drive_pair, runtime, split_id, store, worker,
    worker_drain_deadline, worker_max_in_flight,
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

/// A round-based assignor computes revocations in one round and assignments
/// in the next, which assumes the member count does not change in between; a
/// worker joining between the two leaves work permanently stranded and the
/// fleet in a skew that never self-corrects. Continuous reconciliation has no
/// rounds to fall between, so a third worker arriving mid-revocation must
/// still converge.
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

/// Zero-delay reassignment, arm 1 of 2. A delay knob whose zero flows
/// through the general path as just another value is how "reassign a
/// departed worker's splits at once" silently becomes "withhold them
/// indefinitely" — the two differ by one comparison. Zero here must mean
/// immediately.
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

/// Crash one of two workers and return how much *clock time* the survivor
/// needed to pick up its split. Both workers share `delay`, because the
/// leader's copy is the one that governs.
///
/// Runs on a frozen clock the test steps itself: the grace window is a span
/// of clock time, so measuring it against wall time is exactly the flake
/// (#45's cousin) — a loaded CI scheduler stretches "how long it took" for
/// reasons that have nothing to do with the window. Stepping the clock makes
/// the measurement the window and nothing else, and collapses a ~10s
/// wall-clock wait to milliseconds.
fn reassignment_delay(delay: std::time::Duration) -> std::time::Duration {
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let ids = ["s0", "s1"];
    let planner = || Box::new(PhasedPlanner::one_final("delay:v1", &ids));

    let mut a = support::worker_rebalance_delay_clock(
        &store,
        rt.handle(),
        Some("worker-a"),
        delay,
        clock.clone(),
    );
    // B gets its own runtime so it can be killed outright rather than shut
    // down cleanly — a clean stop releases its split and would prove
    // nothing about reassignment.
    let rt_b = runtime();
    let mut b = support::worker_rebalance_delay_clock(
        &store,
        rt_b.handle(),
        Some("worker-b"),
        delay,
        clock.clone(),
    );
    a.start(planner()).unwrap();
    b.start(planner()).unwrap();
    let (mut held_a, mut held_b) = (Held::default(), Held::default());
    // Step the clock while both claim: the leader's first assignment can
    // hinge on a reconcile tick, which is clock-driven now, so a frozen clock
    // that never moves can leave the pair un-assigned. Both are alive, so the
    // step stays under a renew-interval (advance-to-settle).
    let step = LEASE / 6;
    let claim_deadline = Instant::now() + support::DEADLINE;
    while !(held_a.splits.len() == 1 && held_b.splits.len() == 1) {
        assert!(
            Instant::now() < claim_deadline,
            "both workers never held a split"
        );
        clock.advance(step);
        std::thread::sleep(support::POLL_INTERVAL);
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
    }
    support::commit_held(&mut a, &held_a);

    // Kill B: it can no longer renew, so its lease and presence expire only
    // as the clock advances. A is alive and must keep its own lease, so step
    // in fractions of a renew-interval — a single lease-sized jump would
    // expire A too. `advanced` is the clock time from the crash to the
    // hand-off: B's lease expiry plus, for a non-zero delay, the whole grace
    // window on top.
    crash(rt_b, b);
    let cap = LEASE * 10;
    let mut advanced = std::time::Duration::ZERO;
    while held_a.splits.len() < 2 {
        assert!(advanced < cap, "the survivor never picked up the split");
        clock.advance(step);
        advanced += step;
        std::thread::sleep(support::POLL_INTERVAL);
        held_a.fold(a.poll().unwrap());
        support::commit_held(&mut a, &held_a);
    }
    advanced
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
///
/// The baseline has to be a **settled** fleet, not merely a working one.
/// Until the leader's assignment has converged, a worker is legitimately
/// asked to give splits up — the first worker to claim takes the whole
/// plan, and the balancer moves half of it to the peer — and those
/// requests are exactly what this test reads as the inversion. So both
/// sides consent their way to the fixpoint first, and the window below
/// starts from an empty revocation slate.
#[test]
fn a_withdrawn_assignment_record_does_not_release_anything() {
    let rt = runtime();
    // Frozen clock: the window is a span of protocol time, and running it
    // in wall time both costs a full lease per run and lets a scheduler
    // stall expire the very leases the invariant is about.
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let ids = ["w0", "w1", "w2", "w3"];
    let planner = || Box::new(PhasedPlanner::one_final("withdrawn:v1", &ids));

    let mut a = support::worker_with_clock(&store, rt.handle(), Some("worker-a"), clock.clone());
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    let mut b = support::worker_with_clock(&store, rt.handle(), Some("worker-b"), clock.clone());
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    // Four splits, two workers, equal weights, a lane budget neither
    // reaches: the balancer's fixpoint is two each, and reaching it is what
    // makes "nothing changed" below mean anything. The helper settles to
    // that fixpoint *and* proves the fleet stopped moving — it only returns
    // after `QUIET_ROUNDS` rounds in which it drained nothing and nothing
    // was revoked. Asserting the slates are empty here instead would be
    // dead: `consent_to_revocations` empties them unconditionally, so it
    // would hold however hard the fleet was churning. The guarantee has to
    // come from rounds that drain nothing.
    support::settle_pair_clocked(
        (&mut a, &mut held_a),
        (&mut b, &mut held_b),
        &clock,
        "both workers settle on half the plan",
        |x, y| x.splits.len() == 2 && y.splits.len() == 2,
    );
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

    // A lease of protocol time, stepped so both workers keep renewing. The
    // reconcile tick reads the clock too, so the leader's republish still
    // happens inside this window — it is just not paid for in wall time.
    clock.advance_stepped(LEASE, LEASE / 12, || {
        std::thread::sleep(support::POLL_INTERVAL);
        for event in a.poll().expect("poll a") {
            held_a.fold(vec![event]);
        }
        for event in b.poll().expect("poll b") {
            assert!(
                !matches!(event, spate_coordination::CoordinationEvent::Lost { .. }),
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
    });
    let after: Vec<String> = held_b.splits.keys().cloned().collect();
    assert_eq!(
        before, after,
        "worker-b's working set changed while it had no assignment record"
    );
}

/// A revocation the leader takes back is **cancelled**, not forced.
///
/// `desired_assignment` is sticky on the current owner, and a draining
/// split still holds its lease — so any input that reverts (a peer leaving,
/// a spec landing, an improving move undone) names the split for the very
/// worker that is giving it up. Forcing it out at `drain_deadline` then
/// serves a move nobody wants any more, and charges a re-claim plus one
/// commit interval of replay for it.
///
/// The window here is the whole point: the source never answers the
/// request, so under the pre-fix code the deadline is the *only* thing that
/// can move the split, and it fires at half a lease. This test runs a full
/// lease of protocol time past that.
///
/// The worker keeps committing throughout, which is what a drain winding
/// down does as its tail acks. That is not incidental: a cancelled drain is
/// still bounded, by silence rather than by the deadline, so the split
/// staying put here is the *progressing* half of a pair with
/// [`a_stalled_cancelled_drain_is_still_released`].
#[test]
fn a_reassigned_split_cancels_its_own_revocation() {
    let rt = runtime();
    // Frozen clock: the assertion is that nothing happens across a span of
    // protocol time, and paying for that span in wall time both slows the
    // suite and lets a scheduler stall expire the leases it is about.
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let ids = ["c0", "c1", "c2", "c3"];
    let planner = || Box::new(PhasedPlanner::one_final("cancel:v1", &ids));

    // A alone takes the whole plan and commits, so its splits carry a
    // resume point a forced move would visibly replay from.
    let mut a = support::worker_with_clock(&store, rt.handle(), Some("worker-a"), clock.clone());
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    support::drive_clocked(
        &mut a,
        &clock,
        &mut held_a,
        "worker-a takes the plan",
        |h| h.splits.len() == ids.len(),
    );
    support::commit_held(&mut a, &held_a);

    // B joins on its own runtime so it can be killed outright. The leader
    // moves half the plan toward it; A is asked, and — like a source that
    // has stopped intake but not yet finished its tail — answers nothing.
    let rt_b = runtime();
    let mut b = support::worker_with_clock(&store, rt_b.handle(), Some("worker-b"), clock.clone());
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    let deadline = Instant::now() + support::DEADLINE;
    while held_a.revoke_requests.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the leader never revoked anything from worker-a"
        );
        clock.advance(LEASE / 12);
        std::thread::sleep(support::POLL_INTERVAL);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
        support::commit_held(&mut a, &held_a);
    }
    let asked = held_a.revoke_requests.clone();
    let before: Vec<String> = held_a.splits.keys().cloned().collect();
    assert_eq!(
        before.len(),
        ids.len(),
        "worker-a must still hold everything when the window opens: the drain \
         has been asked for, not answered"
    );

    // B dies mid-drain and its presence key goes with it. Deleting the key
    // rather than waiting out its TTL is what keeps the leader's change of
    // mind inside the drain deadline — which is the case under test.
    crash(rt_b, b);
    rt.block_on(async {
        let outcome = store
            .delete(Keyspace::Ephemeral, "worker.worker-b", None)
            .await
            .expect("delete");
        assert!(
            matches!(outcome, CasOutcome::Won(_)),
            "the presence key has to have existed, or this tests nothing"
        );
    });

    // A full lease of protocol time — twice the drain deadline the test
    // config sets. The leader (A itself) re-decides that every split stays
    // put, and the pending revocations must simply end.
    clock.advance_stepped(LEASE, LEASE / 12, || {
        std::thread::sleep(support::POLL_INTERVAL);
        for event in a.poll().expect("poll a") {
            if let spate_coordination::CoordinationEvent::Lost { split } = &event {
                assert!(
                    !asked.contains(&split.as_str().to_string()),
                    "worker-a gave up {split}, which the leader had already assigned \
                     back to it: the revocation was forced instead of cancelled"
                );
            }
            held_a.fold(vec![event]);
        }
        support::commit_held(&mut a, &held_a);
    });
    let after: Vec<String> = held_a.splits.keys().cloned().collect();
    assert_eq!(
        before, after,
        "worker-a's working set moved while every split was assigned to it"
    );
}

/// Cancelling a revocation drops the deadline, not the obligation to keep
/// the split **readable**.
///
/// The drain the cancelled revocation started is still out there, and a
/// source that has stopped intake at a safe boundary cannot be asked to
/// resume — that seam does not exist. So a drain that never finishes would
/// leave the split owned, leased, assigned, and read by nobody for the life
/// of the process, with `splits_draining` the only sign and a bounded job
/// containing it unable to ever complete. It is bounded by silence instead:
/// commit nothing at all for `drain_deadline` and the split is released
/// anyway, then re-claimed with a fresh lane that reads again.
///
/// The two phases are the point, and the first is what makes the second
/// mean anything. Phase 1 runs the full
/// [`a_reassigned_split_cancels_its_own_revocation`] window with the worker
/// committing, and proves the revocation really was cancelled — a live one
/// would have been forced at half a lease. Only then does phase 2 go quiet.
/// Without phase 1 a passing test could just be watching the ordinary drain
/// deadline fire.
#[test]
fn a_stalled_cancelled_drain_is_still_released() {
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let ids = ["w0", "w1", "w2", "w3"];
    let planner = || Box::new(PhasedPlanner::one_final("stalled-cancel:v1", &ids));

    let mut a = support::worker_with_clock(&store, rt.handle(), Some("worker-a"), clock.clone());
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    support::drive_clocked(
        &mut a,
        &clock,
        &mut held_a,
        "worker-a takes the plan",
        |h| h.splits.len() == ids.len(),
    );
    support::commit_held(&mut a, &held_a);

    // Same opening as the cancellation test: B joins, the leader moves work
    // toward it, A is asked and answers nothing — a source that stopped
    // intake and is still chasing its tail.
    let rt_b = runtime();
    let mut b = support::worker_with_clock(&store, rt_b.handle(), Some("worker-b"), clock.clone());
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    let deadline = Instant::now() + support::DEADLINE;
    while held_a.revoke_requests.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the leader never revoked anything from worker-a"
        );
        clock.advance(LEASE / 12);
        std::thread::sleep(support::POLL_INTERVAL);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
        support::commit_held(&mut a, &held_a);
    }
    let asked = held_a.revoke_requests.clone();
    crash(rt_b, b);
    rt.block_on(async {
        let outcome = store
            .delete(Keyspace::Ephemeral, "worker.worker-b", None)
            .await
            .expect("delete");
        assert!(
            matches!(outcome, CasOutcome::Won(_)),
            "the presence key has to have existed, or this tests nothing"
        );
    });

    // Phase 1 — the drain is progressing, so nothing is forced. A full
    // lease is twice the drain deadline: a revocation that had not been
    // cancelled would have fired inside this window.
    clock.advance_stepped(LEASE, LEASE / 12, || {
        std::thread::sleep(support::POLL_INTERVAL);
        for event in a.poll().expect("poll a") {
            if let spate_coordination::CoordinationEvent::Lost { split } = &event {
                assert!(
                    !asked.contains(&split.as_str().to_string()),
                    "worker-a gave up {split} while still committing it: the revocation \
                     was forced instead of cancelled, so phase 2 would prove nothing"
                );
            }
            held_a.fold(vec![event]);
        }
        support::commit_held(&mut a, &held_a);
    });
    assert_eq!(
        held_a.splits.len(),
        ids.len(),
        "worker-a must still hold everything before the stall begins"
    );

    // Phase 2 — the asked splits go quiet while the rest keep committing,
    // so this cannot pass by the worker looking dead. The stalled drain must
    // be released and then re-claimed: a split that comes back is a split
    // being read again, which is the whole point of releasing it.
    let mut lost: Option<String> = None;
    let deadline = Instant::now() + support::DEADLINE;
    while lost
        .as_ref()
        .is_none_or(|id| !held_a.splits.contains_key(id))
    {
        assert!(
            Instant::now() < deadline,
            "the stalled drain was never released: {asked:?} stayed owned with nothing \
             reading them"
        );
        clock.advance(LEASE / 12);
        std::thread::sleep(support::POLL_INTERVAL);
        for event in a.poll().expect("poll a") {
            if let spate_coordination::CoordinationEvent::Lost { split } = &event
                && asked.contains(&split.as_str().to_string())
                && lost.is_none()
            {
                lost = Some(split.as_str().to_string());
            }
            held_a.fold(vec![event]);
        }
        support::commit_held_except(&mut a, &held_a, &asked);
    }
    let lost = lost.expect("a stalled split was released");
    assert!(
        asked.contains(&lost),
        "the released split must be one the leader had asked for"
    );
    assert_eq!(
        held_a.splits.len(),
        ids.len(),
        "worker-a must be whole again: the stalled split was released and re-claimed"
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
/// honors each worker's own. Splits beyond the fleet's summed budget are
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
