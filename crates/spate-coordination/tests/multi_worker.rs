//! Multi-worker protocol suite over one shared in-memory store: several
//! real coordinators race through the public synchronous API, exactly as
//! pipeline instances would.
//!
//! The scenarios in `scenarios/` run here over the store directly, over
//! [`PolledStore`](support::polled::PolledStore), which delivers changes by
//! polling, and over the DynamoDB store on an in-memory table. The scenarios
//! below need a frozen clock and run here only.

mod support;
#[macro_use]
mod scenarios;

use spate_coordination::SplitCoordinator as _;
use spate_coordination::store::{CasOutcome, CoordinationStore as _, Keyspace};
use std::time::Instant;
use support::{Held, LEASE, PhasedPlanner, crash, runtime};

multi_worker_scenarios!(support::MemoryBackend::new());

mod polled {
    multi_worker_scenarios!(crate::support::polled::PolledBackend::new());
}

#[cfg(feature = "dynamodb")]
mod dynamodb {
    multi_worker_scenarios!(crate::support::dynamodb::DynamoDbFakeBackend::new());
}

// ----------------------------------------------------------------------
// Leader-computed assignment.

/// Zero-delay reassignment, arm 1 of 2. A delay knob whose zero flows
/// through the general path as another value is how "reassign a departed
/// worker's splits at once" silently becomes "withhold them indefinitely";
/// the two differ by one comparison. Zero here must mean immediately.
///
/// `rebalance_delay` governs the **leader**, so every worker carries the
/// same setting: configuring only the joiner would assert nothing, since
/// the surviving leader's own value decides. Paired with
/// [`a_departed_workers_splits_are_withheld_for_the_grace_window`], because
/// one arm alone cannot tell "prompt" from "no window at all".
#[test]
fn a_zero_rebalance_delay_reassigns_immediately() {
    assert!(
        reassignment_delay(std::time::Duration::ZERO, RECONCILE) < LEASE * 3,
        "zero delay must not withhold the split"
    );
}

/// Arm 2: a non-zero window really does hold the work back, so arm 1 is
/// measuring the window rather than the lease expiry it sits on top of.
#[test]
fn a_departed_workers_splits_are_withheld_for_the_grace_window() {
    let withheld = reassignment_delay(LEASE * 5, RECONCILE);
    assert!(
        withheld >= LEASE * 3,
        "a grace window must actually delay reassignment, took {withheld:?}"
    );
}

/// A grace window ends on its own clock: the leader reassigns a departed
/// worker's splits one window after the departure, not at the next reconcile.
#[test]
fn a_grace_window_ends_without_waiting_for_a_reconcile() {
    let taken = reassignment_delay(LEASE * 2, LEASE * 50);
    assert!(
        taken < LEASE * 4,
        "reassignment waited past the window, took {taken:?}"
    );
}

/// The suite's reconcile interval.
const RECONCILE: std::time::Duration = std::time::Duration::from_millis(300);

/// Crash one of two workers and return how much *clock time* the survivor
/// needed to pick up its split. Both workers share `delay` and `reconcile`,
/// because the leader's copy is the one that governs.
///
/// Runs on a frozen clock the test steps itself: the grace window is a span
/// of clock time, so measuring it against wall time flakes (#45's cousin),
/// because a loaded CI scheduler stretches "how long it took" for reasons
/// unrelated to the window. Stepping the clock measures the window and
/// nothing else, and collapses a ~10s wall-clock wait to milliseconds.
fn reassignment_delay(
    delay: std::time::Duration,
    reconcile: std::time::Duration,
) -> std::time::Duration {
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let ids = ["s0", "s1"];
    let planner = || Box::new(PhasedPlanner::one_final("delay:v1", &ids));

    let tune = |c: &mut spate_coordination::CoordinationConfig| {
        c.rebalance_delay = delay;
        c.reconcile_interval = reconcile;
    };
    let mut a = support::worker_tuned_clock(&store, rt.handle(), "worker-a", clock.clone(), tune);
    // B gets its own runtime so it can be killed outright rather than shut
    // down cleanly; a clean stop releases its split and proves nothing
    // about reassignment.
    let rt_b = runtime();
    let mut b = support::worker_tuned_clock(&store, rt_b.handle(), "worker-b", clock.clone(), tune);
    a.start(planner()).unwrap();
    b.start(planner()).unwrap();
    let mut fleet = support::Fleet::new(&store, rt.handle());
    fleet.join(&a);
    fleet.join(&b);
    let (mut held_a, mut held_b) = (Held::default(), Held::default());
    // Step the clock while both claim: the leader's first assignment can
    // hinge on a reconcile tick, which is clock-driven, so a frozen clock
    // that never moves can leave the pair un-assigned. Both are alive, so the
    // step stays under a renew-interval (advance-to-settle).
    let step = LEASE / 6;
    let claim_deadline = Instant::now() + support::DEADLINE;
    while !(held_a.splits.len() == 1 && held_b.splits.len() == 1) {
        assert!(
            Instant::now() < claim_deadline,
            "both workers never held a split"
        );
        fleet.step(&clock, step);
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
    }
    support::commit_held(&mut a, &held_a);

    // Kill B: it can no longer renew, so its lease and presence expire only
    // as the clock advances. A is alive and must keep its own lease, so step
    // in fractions of a renew-interval; a single lease-sized jump expires A
    // too. `advanced` is the clock time from the crash to the
    // takeover: B's lease expiry plus, for a non-zero delay, the whole grace
    // window on top.
    fleet.forget("worker-b");
    crash(rt_b, b);
    let cap = LEASE * 10;
    let mut advanced = std::time::Duration::ZERO;
    while held_a.splits.len() < 2 {
        assert!(advanced < cap, "the survivor never picked up the split");
        fleet.step(&clock, step);
        advanced += step;
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
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let ids = ["s0", "s1", "s2", "s3"];
    let planner = || Box::new(PhasedPlanner::one_final("forced:v1", &ids));
    let step = LEASE / 12;

    // A short deadline so the force fires inside the test window.
    let mut a = support::worker_drain_deadline_clock(
        &store,
        rt.handle(),
        Some("worker-a"),
        LEASE / 4,
        clock.clone(),
    );
    a.start(planner()).unwrap();
    let mut fleet = support::Fleet::new(&store, rt.handle());
    fleet.join(&a);
    let mut held_a = Held::default();
    let deadline = Instant::now() + support::DEADLINE;
    while held_a.splits.len() < ids.len() {
        assert!(
            Instant::now() < deadline,
            "worker-a never took the whole plan"
        );
        fleet.step(&clock, step);
        held_a.fold(a.poll().unwrap());
    }
    support::commit_held(&mut a, &held_a);

    let mut b = support::worker_drain_deadline_clock(
        &store,
        rt.handle(),
        Some("worker-b"),
        LEASE / 4,
        clock.clone(),
    );
    b.start(planner()).unwrap();
    fleet.join(&b);
    let mut held_b = Held::default();

    // A never consents: `Held::fold` records the request and does nothing,
    // which is exactly a declining source. `advanced` is the clock time from
    // B's start to its first split.
    let mut advanced = std::time::Duration::ZERO;
    while held_b.splits.is_empty() {
        assert!(
            advanced < LEASE * 10,
            "a declining source blocked the rebalance forever"
        );
        fleet.step(&clock, step);
        advanced += step;
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
        support::commit_held(&mut a, &held_a);
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
    // lease ran out; otherwise this test would still pass with the whole
    // forcing path deleted. `drain_deadline` is a quarter of the lease, so
    // nothing but a forced revocation can have moved a split this soon.
    assert!(
        advanced < LEASE,
        "a split moved only after a full lease ({advanced:?}) — that is an expiry \
         takeover, not a forced revocation"
    );
    // And what B holds must be something A was asked for.
    let moved: Vec<&String> = held_b.splits.keys().collect();
    assert!(
        moved.iter().all(|id| held_a.revoke_requests.contains(id)),
        "worker-b holds {moved:?} but the revocations asked for {:?}",
        held_a.revoke_requests
    );
}

/// **Absence of an assignment is not an instruction to hold nothing.**
///
/// A worker whose `assign.{instance}` record disappears (a leader gap, a
/// withdrawn assignment, a reconcile that finds the key gone) must keep
/// what it holds and wait to be told again. If that inverted, a single
/// leaderless moment would drain the entire fleet at once and the job would
/// stall behind an unrequested rebalance, so it is asserted directly rather
/// than left to fall out of the happy path.
///
/// The baseline has to be a **settled** fleet, not a working one. Until the
/// leader's assignment has converged, a worker is legitimately asked to
/// give splits up: the first worker to claim takes the whole plan, and the
/// balancer moves half of it to the peer. This test reads those requests as
/// the inversion, so both sides consent their way to the fixpoint first and
/// the window below starts from an empty revocation slate.
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
    let mut fleet = support::Fleet::new(&store, rt.handle());
    fleet.join(&a);
    fleet.join(&b);
    // Four splits, two workers, equal weights, a lane budget neither
    // reaches: the balancer's fixpoint is two each, and reaching it is what
    // makes "nothing changed" below mean anything. The helper settles to
    // that fixpoint and proves the fleet stopped moving, returning only
    // after `QUIET_ROUNDS` rounds in which it drained nothing and nothing
    // was revoked. Asserting the slates are empty here instead would be
    // dead: `consent_to_revocations` empties them unconditionally, so it
    // would hold however hard the fleet was churning.
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
    // happens inside this window without being paid for in wall time.
    clock.advance_stepped(LEASE, LEASE / 12, || {
        fleet.settle(&clock);
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
    let republished = rt
        .block_on(store.get(Keyspace::Durable, "assign.worker-b"))
        .expect("get");
    assert!(
        republished.is_some(),
        "the leader never republished, so the window proved nothing"
    );
}

/// A leader republishes an assignment record deleted under it without
/// waiting for a reconcile.
#[test]
fn a_withdrawn_assignment_record_is_republished_without_a_reconcile() {
    let rt = runtime();
    let clock = support::TestClock::frozen();
    let store = support::store_with_clock(clock.clone());
    let mut a = support::worker_tuned_clock(&store, rt.handle(), "worker-a", clock.clone(), |c| {
        c.reconcile_interval = LEASE * 50;
    });
    a.start(Box::new(PhasedPlanner::one_final("republish:v1", &["r0"])))
        .unwrap();
    let mut held = Held::default();
    support::drive_clocked(&mut a, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });
    let mut fleet = support::Fleet::new(&store, rt.handle());
    fleet.join(&a);
    fleet.settle(&clock);

    let outcome = rt
        .block_on(store.delete(Keyspace::Durable, "assign.worker-a", None))
        .expect("delete");
    assert!(matches!(outcome, CasOutcome::Won(_)));
    fleet.settle(&clock);
    let republished = rt
        .block_on(store.get(Keyspace::Durable, "assign.worker-a"))
        .expect("get");
    assert!(republished.is_some(), "the record was not republished");
}

/// A revocation the leader takes back is **cancelled**, not forced.
///
/// `desired_assignment` is sticky on the current owner, and a draining
/// split still holds its lease, so any input that reverts (a peer leaving,
/// a spec landing, an improving move undone) names the split for the worker
/// that is giving it up. Forcing it out at `drain_deadline` then serves a
/// move no longer wanted, and charges a re-claim plus one commit interval
/// of replay for it.
///
/// The source never answers the request, so the deadline is the only other
/// thing that can move the split, and it fires at half a lease. This test
/// runs a full lease of protocol time past that.
///
/// The worker keeps committing throughout, which is what a drain winding
/// down does as its tail acks. A cancelled drain is still bounded, by
/// silence rather than by the deadline, so the split staying put here is
/// the *progressing* half of a pair with
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
    // moves half the plan toward it; A is asked, and answers nothing, like
    // a source that has stopped intake but not yet finished its tail.
    let rt_b = runtime();
    let mut b = support::worker_with_clock(&store, rt_b.handle(), Some("worker-b"), clock.clone());
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    let mut fleet = support::Fleet::new(&store, rt.handle());
    fleet.join(&a);
    fleet.join(&b);
    let deadline = Instant::now() + support::DEADLINE;
    while held_a.revoke_requests.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the leader never revoked anything from worker-a"
        );
        fleet.step(&clock, LEASE / 12);
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
    // rather than waiting out its TTL keeps the leader's change of mind
    // inside the drain deadline, the case under test.
    fleet.forget("worker-b");
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

    // A full lease of protocol time, twice the drain deadline the test
    // config sets. The leader (A itself) re-decides that every split stays
    // put, and the pending revocations must end.
    clock.advance_stepped(LEASE, LEASE / 12, || {
        fleet.settle(&clock);
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
/// resume; that seam does not exist. A drain that never finishes would
/// leave the split owned, leased, assigned, and unread for the life of the
/// process, with `splits_draining` the only sign and a bounded job
/// containing it unable to complete. It is bounded by silence instead:
/// commit nothing at all for `drain_deadline` and the split is released
/// anyway, then re-claimed with a fresh lane that reads again.
///
/// Phase 1 runs the full
/// [`a_reassigned_split_cancels_its_own_revocation`] window with the worker
/// committing, and proves the revocation was cancelled; a live one would
/// have been forced at half a lease. Only then does phase 2 go quiet.
/// Without phase 1 a passing test could be watching the ordinary drain
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
    // toward it, A is asked and answers nothing, like a source that stopped
    // intake and is still chasing its tail.
    let rt_b = runtime();
    let mut b = support::worker_with_clock(&store, rt_b.handle(), Some("worker-b"), clock.clone());
    b.start(planner()).unwrap();
    let mut held_b = Held::default();
    let mut fleet = support::Fleet::new(&store, rt.handle());
    fleet.join(&a);
    fleet.join(&b);
    let deadline = Instant::now() + support::DEADLINE;
    while held_a.revoke_requests.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the leader never revoked anything from worker-a"
        );
        fleet.step(&clock, LEASE / 12);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
        support::commit_held(&mut a, &held_a);
    }
    let asked = held_a.revoke_requests.clone();
    fleet.forget("worker-b");
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
        fleet.settle(&clock);
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
    // being read again.
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
        fleet.step(&clock, LEASE / 12);
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
