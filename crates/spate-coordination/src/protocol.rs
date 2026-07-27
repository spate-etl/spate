//! The pure decision core of the coordination protocol.
//!
//! Everything here is a function of the observed store state — no I/O, no
//! channels, no clocks — so the assignment, claim, and quarantine rules
//! are unit- and property-testable in isolation. The task layer feeds
//! observations in and executes the decisions with conditional writes;
//! losing any race is always safe because the write, not the decision, is
//! what transfers ownership.
//!
//! [`desired_assignment`] is the balance decision in full, and lives here
//! rather than in the task deliberately: it is the part most likely to
//! change, and this is the half of the crate where a change is cheap to
//! verify. Its contract is specified normatively in
//! `docs/user-guide/02-concepts/08-work-assignment.mdx` — the numbered
//! invariants there name the property tests at the bottom of this file.
//!
//! Liveness discipline: a split is claimable exactly when its durable
//! progress record says `runnable` and no live lease key exists for it.
//! Lease keys expire server-side (single clock — the store's), so there
//! are no cross-machine clock comparisons anywhere; fencing (the progress
//! record CAS) remains the only *correctness* mechanism regardless.

use crate::records::{LeaseVal, SplitProgressRecord, SplitSpecRecord, SplitStatus};
use crate::store::Revision;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::BuildHasher as _;
use std::time::Duration;

/// Everything this worker knows about one split: the mutable progress
/// record (and the revision to CAS against), the immutable spec once
/// observed (created before the progress record, but snapshots may
/// deliver them in either order), and the live lease key, if any.
#[derive(Clone, Debug)]
pub(crate) struct SplitState {
    pub(crate) progress: SplitProgressRecord,
    pub(crate) progress_rev: Revision,
    pub(crate) spec: Option<SplitSpecRecord>,
    pub(crate) lease: Option<(LeaseVal, Revision)>,
}

/// How a claimable split became claimable, in claim-priority order:
/// never-owned work first, then instant revocations, then reclaims, then
/// expiry takeovers (the contended kind, tried last).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ClaimKind {
    /// Never owned (`epoch == 0`).
    Create,
    /// Gracefully released (`owner` cleared by the releasing worker).
    Released,
    /// The lease key is still live but held by this worker's own stable
    /// id under a foreign nonce: a restarted predecessor — reclaim fast,
    /// without waiting out the lease. (The same observation with OUR
    /// nonce on a split we do not hold is a live twin — Fatal, decided by
    /// the task layer.)
    Reclaim,
    /// The lease expired with `owner` still set: the owner died.
    Expired,
}

impl ClaimKind {
    /// Whether claiming consumes a delivery attempt: only takeovers from
    /// a non-graceful end do. Graceful releases and fresh work are not
    /// poison evidence.
    pub(crate) fn consumes_attempt(self) -> bool {
        matches!(self, ClaimKind::Reclaim | ClaimKind::Expired)
    }
}

/// What to do with a claimable split.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClaimAction {
    /// Claim it (lease write, then the progress-record CAS).
    Claim(ClaimKind),
    /// It is out of delivery attempts: park it instead.
    Quarantine(ClaimKind),
}

/// Hash a round through the worker's seed (tick decorrelation).
pub(crate) fn stable_hash(seed: u64, value: u64) -> u64 {
    foldhash::fast::FixedState::with_seed(seed).hash_one(value)
}

/// Hash a string through the same keyed hasher.
pub(crate) fn stable_hash_str(seed: u64, value: &str) -> u64 {
    foldhash::fast::FixedState::with_seed(seed).hash_one(value)
}

/// Jitter factor in `[0.8, 1.2)` for tick scheduling, keyed by round so
/// workers drift apart rather than herd.
pub(crate) fn jitter(seed: u64, round: u64, base: Duration) -> Duration {
    let h = stable_hash(seed, round) % 1024;
    base.mul_f64(0.8 + 0.4 * (h as f64) / 1024.0)
}

/// Fleet size from the explicit membership keys (self is always counted,
/// even before its own presence write lands).
pub(crate) fn live_workers(presence: &BTreeMap<String, Revision>, instance: &str) -> usize {
    presence.len() + usize::from(!presence.contains_key(instance))
}

/// The leader's desired assignment: which splits each live member should
/// hold. This is the whole balance decision, and the only place it is
/// made — workers reconcile toward what they are given and never choose
/// for themselves.
///
/// **Balance is on weight, not split count.** Split counts are a proxy,
/// and a bad one here: an object at or above the packing target lands
/// alone in its own split, so equal counts can mean wildly unequal bytes.
/// The lane budget still caps the *count* per member — it is a
/// materialization limit rather than a fairness one. Splits beyond the
/// fleet's summed budget stay unassigned: they are the queue, exactly as
/// unleased splits were before.
///
/// `caps` gives each member its own lane budget, as that member advertised
/// it on its presence key; `default_cap` covers a member whose presence
/// value predates the field. A member's budget must be *its own* rather
/// than the leader's, or a fleet of unequally-sized pods strands work: the
/// leader would keep handing splits to whichever member looks least loaded
/// while that member's own `max_in_flight` refuses to claim them, forever.
///
/// `reserved` names splits withheld by the rebalance delay — work whose
/// owner has departed but whose grace window has not elapsed. The window
/// itself is the leader's bookkeeping (it needs a clock); this function
/// only sees the resulting set, so it stays pure and replayable.
///
/// The three passes, in order:
///
/// 1. **Sticky.** Every split whose current owner is still a live member
///    stays put, subject to the lane cap. Stickiness is the point: a move
///    costs a drain, so an assignment that churns for a marginally better
///    balance is worse than one that does not.
/// 2. **Fill.** Unassigned splits go to the least-loaded member that has
///    lane budget, heaviest split first — longest-processing-time greedy,
///    which is where the approximation quality comes from.
/// 3. **Improve.** While some split can move from a heavier member to a
///    lighter one and strictly reduce imbalance, move it.
///
/// Pass 3's admission rule is `load(from) > load(to) + weight`, which is
/// exactly the condition under which the move reduces the sum of squared
/// loads — the standard balance potential. It is also the weight-valued
/// generalization of the pairwise `victim > own + 1` rule the previous
/// work-stealing protocol used, so the convergence argument carries over
/// unchanged: every move strictly improves, therefore none oscillate.
///
/// Because each move strictly decreases an integer potential bounded
/// below, pass 3 terminates on its own; [`MAX_IMPROVING_MOVES`] is a
/// belt-and-braces bound so a leader can never spin on pathological
/// input rather than publish.
///
/// The result is idempotent: feeding this function's own output back as
/// current ownership reproduces it exactly. Pass 1 restores every
/// placement, pass 2 finds nothing unassigned, and pass 3 finds no
/// improving move because it already ran to fixpoint. That property is
/// what makes a steady-state fleet publish an unchanging assignment and
/// therefore never drain anything.
///
/// `seed` keys the tie-breaks only (equal loads in pass 2, equal gains in
/// pass 3). It must be a property of the **job**, not of the leader, or
/// two leaders would break ties differently and a failover would churn the
/// fleet for no reason; the caller passes the job fingerprint hash.
pub(crate) fn desired_assignment(
    members: &BTreeSet<String>,
    splits: &BTreeMap<String, SplitState>,
    reserved: &BTreeSet<String>,
    caps: &BTreeMap<String, u32>,
    default_cap: u32,
    seed: u64,
) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> =
        members.iter().map(|m| (m.clone(), Vec::new())).collect();
    if members.is_empty() {
        return out;
    }
    // Per-member lane budget, and the member-name tie-break hashes, both
    // computed once: `lightest` runs per unplaced split and would otherwise
    // re-hash every member name on every call.
    let caps: BTreeMap<&str, usize> = members
        .iter()
        .map(|m| {
            let cap = caps.get(m).copied().unwrap_or(default_cap).max(1) as usize;
            (m.as_str(), cap)
        })
        .collect();
    let hashes: BTreeMap<&str, u64> = members
        .iter()
        .map(|m| (m.as_str(), stable_hash_str(seed, m)))
        .collect();
    let mut load: BTreeMap<&str, u64> = members.iter().map(|m| (m.as_str(), 0u64)).collect();

    // Assignable pool: runnable, spec observed (its weight is the balance
    // input, and a worker cannot start a split whose descriptor nobody
    // has), and not withheld by the rebalance delay.
    let mut pool: Vec<(&str, u64, Option<&str>)> = splits
        .iter()
        .filter(|(id, state)| {
            state.progress.status == SplitStatus::Runnable
                && state.spec.is_some()
                && !state.progress.completed
                && !reserved.contains(id.as_str())
        })
        .map(|(id, state)| {
            let weight = state.spec.as_ref().map_or(1, |s| s.weight.max(1));
            (id.as_str(), weight, current_owner(state))
        })
        .collect();
    // Heaviest first: pass 2 is LPT, and pass 1 needs a stable rule for
    // which splits an over-capacity owner keeps.
    pool.sort_by_key(|(id, weight, _)| (std::cmp::Reverse(*weight), *id));

    // Pass 1 — sticky. Loads are summed with `saturating_add`: a weight is
    // planner-supplied and unbounded (`spate-s3` reports bytes), and a leader
    // must publish a slightly-wrong assignment rather than panic on an
    // overflow it cannot influence.
    let mut unplaced: Vec<(&str, u64)> = Vec::new();
    for (id, weight, owner) in &pool {
        match owner {
            Some(owner) if members.contains(*owner) && out[*owner].len() < caps[*owner] => {
                out.get_mut(*owner)
                    .expect("live member")
                    .push(id.to_string());
                let l = load.get_mut(*owner).expect("live member");
                *l = l.saturating_add(*weight);
            }
            _ => unplaced.push((id, *weight)),
        }
    }

    // Pass 2 — fill, heaviest split to the least-loaded member with lane
    // budget. Member-name hash breaks load ties so a fleet of empty
    // members does not pile onto whichever id sorts first.
    for (id, weight) in unplaced {
        let Some(target) = lightest(&load, &out, &caps, &hashes) else {
            break; // every lane is full: the rest stay queued
        };
        out.get_mut(target)
            .expect("live member")
            .push(id.to_string());
        let l = load.get_mut(target).expect("live member");
        *l = l.saturating_add(weight);
    }

    // Pass 3 — improve.
    let weights: BTreeMap<&str, u64> = pool.iter().map(|(id, w, _)| (*id, *w)).collect();
    for _ in 0..MAX_IMPROVING_MOVES {
        let Some((from, to, split)) = best_move(&out, &load, &weights, &caps, seed) else {
            break;
        };
        let weight = weights[split.as_str()];
        out.get_mut(from.as_str())
            .expect("live member")
            .retain(|s| s != &split);
        out.get_mut(to.as_str()).expect("live member").push(split);
        let from_load = load.get_mut(from.as_str()).expect("live member");
        *from_load = from_load.saturating_sub(weight);
        let to_load = load.get_mut(to.as_str()).expect("live member");
        *to_load = to_load.saturating_add(weight);
    }

    for splits in out.values_mut() {
        splits.sort();
    }
    out
}

/// Termination backstop for the improving-move pass. Each accepted move
/// strictly decreases the sum of squared loads, so the pass converges
/// without this; the bound only guarantees a leader publishes *something*
/// on input we did not anticipate rather than spinning. Two moves per
/// split is far above what greedy needs in practice — if this ever binds,
/// the local-optimality property test is the thing that will say so.
const MAX_IMPROVING_MOVES: usize = 4096;

/// Who holds a split now. The live lease is authoritative; the durable
/// record's owner is the fallback for a split whose owner died and whose
/// lease has expired but which nobody has reclaimed yet. Either way the
/// caller only acts on it when the name is a live member, so a stale
/// owner simply reads as "unplaced".
fn current_owner(state: &SplitState) -> Option<&str> {
    state
        .lease
        .as_ref()
        .map(|(lease, _)| lease.owner.as_str())
        .or(state.progress.owner.as_deref())
}

/// The member with the least load that still has lane budget. `hashes`
/// carries each member name's tie-break hash, precomputed by the caller —
/// this runs once per unplaced split, and re-hashing every member name on
/// every call was the pass's dominant cost.
fn lightest<'a>(
    load: &BTreeMap<&'a str, u64>,
    out: &BTreeMap<String, Vec<String>>,
    caps: &BTreeMap<&str, usize>,
    hashes: &BTreeMap<&str, u64>,
) -> Option<&'a str> {
    load.iter()
        .filter(|(m, _)| out[**m].len() < caps[**m])
        .min_by_key(|(m, l)| (**l, hashes[**m]))
        .map(|(m, _)| *m)
}

/// The single move that most reduces imbalance, or `None` at a local
/// optimum. Admission is `load(from) > load(to) + weight`; among admitted
/// moves the one with the largest potential drop wins, a hash of the split
/// id breaking ties so the choice is deterministic.
///
/// The gain is computed in `u128`. Both factors are planner-supplied
/// weights — `spate-s3` reports bytes — so `weight * load` overflows `u64`
/// once two splits of a few GiB each sit on one member against an idle
/// peer, which would panic the coordination task under the debug profile's
/// overflow checks and silently mis-rank moves under release.
fn best_move(
    out: &BTreeMap<String, Vec<String>>,
    load: &BTreeMap<&str, u64>,
    weights: &BTreeMap<&str, u64>,
    caps: &BTreeMap<&str, usize>,
    seed: u64,
) -> Option<(String, String, String)> {
    // The incumbent's tie-break hash is carried rather than recomputed:
    // this is the innermost comparison of an O(m·n) scan.
    let mut best: Option<(u128, u64, String, String, String)> = None;
    for (from, held) in out {
        let from_load = load[from.as_str()];
        for (to, _) in out
            .iter()
            .filter(|(to, s)| *to != from && s.len() < caps[to.as_str()])
        {
            let to_load = load[to.as_str()];
            for split in held {
                let weight = weights[split.as_str()];
                // Saturating: a saturated bound is never exceeded, so an
                // absurd weight reads as "not worth moving" rather than
                // wrapping into a spuriously admitted move.
                if from_load <= to_load.saturating_add(weight) {
                    continue;
                }
                // Sum-of-squares reduction, scaled: 2w(from - to - w).
                let gain = u128::from(weight) * u128::from(from_load - to_load - weight);
                let hash = stable_hash_str(seed, split);
                let better = best
                    .as_ref()
                    .is_none_or(|(g, h, _, _, _)| (gain, hash) > (*g, *h));
                if better {
                    best = Some((gain, hash, from.clone(), to.clone(), split.clone()));
                }
            }
        }
    }
    best.map(|(_, _, from, to, split)| (from, to, split))
}

/// Splits this worker *may* act on, with the quarantine decision folded
/// in. Whether it *should* claim one is the assignment's call, not this
/// function's — it validates eligibility and nothing more.
///
/// A split is claimable when its progress record is `runnable`, this
/// worker does not hold it, and there is no live foreign lease (a live
/// lease under our own stable id is the fast-reclaim case). A claim also
/// requires the spec record to have been observed — a `Gained` event
/// carries the descriptor — while a quarantine does not (it writes only
/// the progress record).
///
/// Ordered by [`ClaimKind`] priority then split id. There is deliberately
/// no per-worker hash and no weight tie-break any more: both existed to
/// spread self-selecting workers across the pool and to start the heaviest
/// remainders first, and the leader's assignment now decides both. Two
/// workers racing for the same split is no longer a thing that happens.
pub(crate) fn claim_candidates(
    splits: &BTreeMap<String, SplitState>,
    owned: impl Fn(&str) -> bool,
    instance: &str,
    max_attempts: u32,
) -> Vec<(String, ClaimAction)> {
    let mut out: Vec<(String, ClaimAction)> = Vec::new();
    for (id, state) in splits {
        if state.progress.status != SplitStatus::Runnable || owned(id) {
            continue;
        }
        let kind = match &state.lease {
            Some((lease, _)) if lease.owner == instance => ClaimKind::Reclaim,
            Some(_) => continue, // live foreign lease: not ours to take
            None => match (&state.progress.owner, state.progress.epoch) {
                (None, 0) => ClaimKind::Create,
                (None, _) => ClaimKind::Released,
                (Some(_), _) => ClaimKind::Expired,
            },
        };
        let attempts = state.progress.attempts + u32::from(kind.consumes_attempt());
        let action = if kind.consumes_attempt() && attempts >= max_attempts {
            ClaimAction::Quarantine(kind)
        } else if state.spec.is_some() {
            ClaimAction::Claim(kind)
        } else {
            continue; // spec not observed yet: nothing to hand the source
        };
        out.push((id.clone(), action));
    }
    out.sort_by(|a, b| kind_of(a).cmp(&kind_of(b)).then_with(|| a.0.cmp(&b.0)));
    out
}

fn kind_of(entry: &(String, ClaimAction)) -> ClaimKind {
    match entry.1 {
        ClaimAction::Claim(kind) | ClaimAction::Quarantine(kind) => kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{SCHEMA, now_ms};
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn record(
        id: &str,
        status: SplitStatus,
        owner: Option<&str>,
        epoch: u64,
        attempts: u32,
    ) -> SplitProgressRecord {
        SplitProgressRecord {
            schema: SCHEMA,
            id: id.to_string(),
            fp: 0,
            epoch,
            status,
            owner: owner.map(str::to_string),
            attempts,
            watermark: None,
            state: None,
            completed: false,
            written_at_ms: now_ms(),
        }
    }

    fn spec_record(id: &str, weight: u64) -> SplitSpecRecord {
        SplitSpecRecord {
            schema: SCHEMA,
            id: id.to_string(),
            fp: 0,
            generation: 1,
            weight,
            descriptor: String::new(),
        }
    }

    fn lease(owner: &str, nonce: &str, epoch: u64) -> (LeaseVal, Revision) {
        (
            LeaseVal {
                schema: SCHEMA,
                owner: owner.to_string(),
                nonce: nonce.to_string(),
                epoch,
            },
            Revision(1),
        )
    }

    fn state(
        progress: SplitProgressRecord,
        weight: u64,
        lease: Option<(LeaseVal, Revision)>,
    ) -> SplitState {
        let spec = spec_record(&progress.id, weight);
        SplitState {
            progress,
            progress_rev: Revision(1),
            spec: Some(spec),
            lease,
        }
    }

    fn splits(states: Vec<SplitState>) -> BTreeMap<String, SplitState> {
        states
            .into_iter()
            .map(|s| (s.progress.id.clone(), s))
            .collect()
    }

    #[test]
    fn claim_kinds_follow_record_and_lease_state() {
        let map = splits(vec![
            state(record("fresh", SplitStatus::Runnable, None, 0, 0), 1, None),
            state(
                record("released", SplitStatus::Runnable, None, 3, 0),
                1,
                None,
            ),
            state(
                record("expired", SplitStatus::Runnable, Some("dead"), 2, 0),
                1,
                None,
            ),
            state(
                record("mine-restarted", SplitStatus::Runnable, Some("me"), 2, 0),
                1,
                Some(lease("me", "old-nonce", 2)),
            ),
            state(
                record("foreign", SplitStatus::Runnable, Some("peer"), 2, 0),
                1,
                Some(lease("peer", "n", 2)),
            ),
            state(record("done", SplitStatus::Completed, None, 2, 0), 1, None),
            state(
                record("parked", SplitStatus::Quarantined, Some("dead"), 2, 4),
                1,
                None,
            ),
        ]);
        let candidates = claim_candidates(&map, |_| false, "me", 4);
        let kinds: Vec<(&str, ClaimAction)> = candidates
            .iter()
            .map(|(id, action)| (id.as_str(), *action))
            .collect();
        // Priority order: Create < Released < Reclaim < Expired; foreign,
        // completed, and quarantined splits never appear.
        assert_eq!(
            kinds,
            vec![
                ("fresh", ClaimAction::Claim(ClaimKind::Create)),
                ("released", ClaimAction::Claim(ClaimKind::Released)),
                ("mine-restarted", ClaimAction::Claim(ClaimKind::Reclaim)),
                ("expired", ClaimAction::Claim(ClaimKind::Expired)),
            ]
        );
    }

    #[test]
    fn attempts_gate_flips_takeovers_to_quarantine() {
        // max_attempts = 3: the third non-graceful takeover quarantines.
        let map = splits(vec![
            state(
                record("dying", SplitStatus::Runnable, Some("dead"), 5, 2),
                1,
                None,
            ),
            state(
                record("fresh-heavily-failed", SplitStatus::Runnable, None, 9, 2),
                1,
                None,
            ),
        ]);
        let candidates = claim_candidates(&map, |_| false, "me", 3);
        let by_id: BTreeMap<&str, ClaimAction> =
            candidates.iter().map(|(id, a)| (id.as_str(), *a)).collect();
        assert_eq!(
            by_id["dying"],
            ClaimAction::Quarantine(ClaimKind::Expired),
            "2 recorded + this takeover = 3 >= max_attempts"
        );
        assert_eq!(
            by_id["fresh-heavily-failed"],
            ClaimAction::Claim(ClaimKind::Released),
            "graceful claims consume no attempt and never quarantine"
        );
    }

    #[test]
    fn spec_less_splits_are_quarantinable_but_not_claimable() {
        let mut map = splits(vec![
            state(
                record("no-spec", SplitStatus::Runnable, None, 0, 0),
                1,
                None,
            ),
            state(
                record("dying-no-spec", SplitStatus::Runnable, Some("dead"), 5, 3),
                1,
                None,
            ),
        ]);
        for state in map.values_mut() {
            state.spec = None;
        }
        let candidates = claim_candidates(&map, |_| false, "me", 4);
        assert_eq!(
            candidates,
            vec![(
                "dying-no-spec".to_string(),
                ClaimAction::Quarantine(ClaimKind::Expired)
            )],
            "a claim needs the descriptor; a quarantine writes only progress"
        );
    }

    #[test]
    fn candidates_order_by_claim_kind_then_id_and_ignore_weight() {
        // Weight used to break ties near the tail so the heaviest
        // remainders started first. The leader's LPT fill owns that now, so
        // this function must be weight-blind — otherwise two orderings
        // would be competing to decide the same thing.
        let map = splits(vec![
            state(
                record("aaa-heavy", SplitStatus::Runnable, None, 0, 0),
                1 << 30,
                None,
            ),
            state(
                record("bbb-light", SplitStatus::Runnable, None, 0, 0),
                1,
                None,
            ),
            // Released (kind priority 2) must still sort after both
            // Creates (priority 1) despite sorting first by id.
            state(
                record("aaa-released", SplitStatus::Runnable, None, 3, 0),
                1 << 30,
                None,
            ),
        ]);
        let ids: Vec<String> = claim_candidates(&map, |_| false, "me", 4)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, ["aaa-heavy", "bbb-light", "aaa-released"]);

        // Same splits, weights swapped: identical order.
        let swapped = splits(vec![
            state(
                record("aaa-heavy", SplitStatus::Runnable, None, 0, 0),
                1,
                None,
            ),
            state(
                record("bbb-light", SplitStatus::Runnable, None, 0, 0),
                1 << 30,
                None,
            ),
            state(
                record("aaa-released", SplitStatus::Runnable, None, 3, 0),
                1,
                None,
            ),
        ]);
        let swapped_ids: Vec<String> = claim_candidates(&swapped, |_| false, "me", 4)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(swapped_ids, ids, "ordering must not depend on weight");
    }

    #[test]
    fn jitter_stays_in_band_and_decorrelates() {
        let base = Duration::from_secs(10);
        for round in 0..64 {
            let j = jitter(7, round, base);
            assert!(j >= base.mul_f64(0.8) && j < base.mul_f64(1.2), "{j:?}");
        }
        assert_ne!(jitter(7, 1, base), jitter(8, 1, base));
    }

    #[test]
    fn membership_counts_self_exactly_once() {
        let mut presence = BTreeMap::new();
        assert_eq!(live_workers(&presence, "me"), 1);
        presence.insert("me".to_string(), Revision(1));
        assert_eq!(live_workers(&presence, "me"), 1);
        presence.insert("peer".to_string(), Revision(2));
        assert_eq!(live_workers(&presence, "me"), 2);
    }

    /// Build an assignment input: `(id, weight, current owner)`.
    fn assign_map(entries: &[(&str, u64, Option<&str>)]) -> BTreeMap<String, SplitState> {
        splits(
            entries
                .iter()
                .map(|(id, weight, owner)| {
                    let l = owner.map(|o| lease(o, "n", 1));
                    state(record(id, SplitStatus::Runnable, *owner, 1, 0), *weight, l)
                })
                .collect(),
        )
    }

    fn members(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    /// Total assigned weight per member.
    fn loads(
        assignment: &BTreeMap<String, Vec<String>>,
        map: &BTreeMap<String, SplitState>,
    ) -> BTreeMap<String, u64> {
        assignment
            .iter()
            .map(|(m, ids)| {
                let load = ids
                    .iter()
                    .map(|id| map[id].spec.as_ref().map_or(1, |s| s.weight.max(1)))
                    .fold(0u64, u64::saturating_add);
                (m.clone(), load)
            })
            .collect()
    }

    /// `(id, weight, owner slot, status)`. The owner slot is deliberately
    /// drawn wider than any fleet size the tests use, so a share of splits
    /// carry an owner that is *not* a live member — the departed-owner
    /// case, which is where reassignment has to happen.
    type AssignEntry = (String, u64, Option<u8>, u8);

    fn assignment_entries() -> impl Strategy<Value = Vec<AssignEntry>> {
        proptest::collection::vec(
            (
                "[a-z]{1,6}",
                // Two scales on purpose. Small weights explore the tie-break
                // and lane-cap logic; byte-scale ones are what an
                // object-store planner actually emits, and are where
                // `weight * load` left `u64`.
                prop_oneof![1u64..50, 1_000_000_000u64..8_000_000_000],
                proptest::option::of(0u8..7),
                0u8..3,
            ),
            0..14,
        )
    }

    /// Ids drawn from the same alphabet as the entries, so a share of them
    /// name a real split and the rest are inert. Invariant 1 names
    /// `reserved` as an input, so it has to be varied like every other one.
    fn reserved_ids() -> impl Strategy<Value = BTreeSet<String>> {
        proptest::collection::vec("[a-z]{1,6}", 0..4).prop_map(|v| v.into_iter().collect())
    }

    fn assignment_input(
        entries: Vec<AssignEntry>,
        fleet: usize,
    ) -> (BTreeMap<String, SplitState>, BTreeSet<String>) {
        let ms: BTreeSet<String> = (0..fleet).map(|i| format!("w{i}")).collect();
        let map = splits(
            entries
                .into_iter()
                .map(|(id, weight, owner, status)| {
                    let status = match status {
                        0 => SplitStatus::Runnable,
                        1 => SplitStatus::Completed,
                        _ => SplitStatus::Quarantined,
                    };
                    let owner = owner.map(|o| format!("w{o}"));
                    let l = owner.as_deref().map(|o| lease(o, "n", 1));
                    state(record(&id, status, owner.as_deref(), 1, 0), weight, l)
                })
                .collect(),
        );
        (map, ms)
    }

    /// [`desired_assignment`] with one lane budget shared by every member —
    /// the shape almost every test wants. Heterogeneous budgets, which are
    /// the interesting case, get their own test below.
    fn assign(
        members: &BTreeSet<String>,
        splits: &BTreeMap<String, SplitState>,
        reserved: &BTreeSet<String>,
        cap: u32,
        seed: u64,
    ) -> BTreeMap<String, Vec<String>> {
        desired_assignment(members, splits, reserved, &BTreeMap::new(), cap, seed)
    }

    #[test]
    fn an_empty_fleet_assigns_nothing() {
        let map = assign_map(&[("a", 1, None)]);
        assert!(assign(&members(&[]), &map, &BTreeSet::new(), 8, 7).is_empty());
    }

    #[test]
    fn a_members_own_lane_budget_bounds_what_it_is_given() {
        // A worker configured with a smaller `max_in_flight` than the
        // leader's must not be handed more than it will claim. It reports
        // its budget on its presence key; the leader honours it. Without
        // this, w2 looks permanently least-loaded (it holds one split
        // against w1's three), so the fill pass keeps assigning to a worker
        // whose own cap refuses the work and the splits never run.
        let map = assign_map(&[
            ("a", 1, Some("w1")),
            ("b", 1, Some("w1")),
            ("c", 1, Some("w1")),
            ("d", 1, Some("w2")),
            ("e", 1, None),
            ("f", 1, None),
        ]);
        let caps: BTreeMap<String, u32> = [("w2".to_string(), 1)].into_iter().collect();
        let out = desired_assignment(&members(&["w1", "w2"]), &map, &BTreeSet::new(), &caps, 8, 7);
        assert_eq!(out["w2"].len(), 1, "w2 advertised a single lane");
        assert_eq!(
            out["w1"].len(),
            5,
            "everything else goes where there is budget to run it"
        );
    }

    #[test]
    fn byte_scale_weights_do_not_overflow_the_improving_move() {
        // `spate-s3` reports weight in BYTES, and any object at or above the
        // packing target gets a split to itself — so multi-GiB weights are
        // the designed case, not an abuse. The improving-move gain is
        // `weight * (from - to - weight)`, which leaves `u64` at around two
        // 4.3 GB splits on one member against an idle peer: under the debug
        // profile's overflow checks that panicked the coordination task.
        const HUGE: u64 = 5_000_000_000;
        let map = assign_map(&[("a", HUGE, Some("w1")), ("b", HUGE, Some("w1"))]);
        let out = assign(&members(&["w1", "w2"]), &map, &BTreeSet::new(), 8, 7);
        assert_eq!(out["w1"].len(), 1, "one of the two heavy splits moves");
        assert_eq!(out["w2"].len(), 1);

        // And the extremes stay survivable rather than panicking: summed
        // load saturates instead of wrapping.
        let map = assign_map(&[
            ("a", u64::MAX, Some("w1")),
            ("b", u64::MAX, Some("w1")),
            ("c", u64::MAX, None),
        ]);
        let out = assign(&members(&["w1", "w2"]), &map, &BTreeSet::new(), 8, 7);
        assert_eq!(
            out.values().map(Vec::len).sum::<usize>(),
            3,
            "every split is still placed"
        );
    }

    #[test]
    fn a_lone_member_takes_everything_up_to_its_lane_cap() {
        let map = assign_map(&[("a", 1, None), ("b", 1, None), ("c", 1, None)]);
        let out = assign(&members(&["w1"]), &map, &BTreeSet::new(), 2, 7);
        assert_eq!(out["w1"].len(), 2, "lane cap bounds the working set");
    }

    #[test]
    fn splits_beyond_total_lane_budget_stay_queued() {
        let map = assign_map(&[
            ("a", 1, None),
            ("b", 1, None),
            ("c", 1, None),
            ("d", 1, None),
        ]);
        let out = assign(&members(&["w1", "w2"]), &map, &BTreeSet::new(), 1, 7);
        let assigned: usize = out.values().map(Vec::len).sum();
        assert_eq!(assigned, 2, "2 members x 1 lane; the rest are the queue");
    }

    #[test]
    fn a_held_split_stays_with_a_live_owner() {
        let map = assign_map(&[("a", 10, Some("w1")), ("b", 10, Some("w2"))]);
        let out = assign(&members(&["w1", "w2"]), &map, &BTreeSet::new(), 8, 7);
        assert_eq!(out["w1"], vec!["a".to_string()]);
        assert_eq!(out["w2"], vec!["b".to_string()]);
    }

    #[test]
    fn a_dead_owners_work_is_reassigned() {
        // w2 is gone from the membership set; its split must move.
        let map = assign_map(&[("a", 10, Some("w1")), ("b", 10, Some("w2"))]);
        let out = assign(&members(&["w1", "w3"]), &map, &BTreeSet::new(), 8, 7);
        assert_eq!(
            out["w3"],
            vec!["b".to_string()],
            "orphan goes to the empty member"
        );
    }

    #[test]
    fn reserved_splits_are_withheld_from_everyone() {
        let map = assign_map(&[("a", 10, Some("w1")), ("b", 10, Some("gone"))]);
        let reserved: BTreeSet<String> = ["b".to_string()].into_iter().collect();
        let out = assign(&members(&["w1", "w2"]), &map, &reserved, 8, 7);
        assert_eq!(
            out["w2"],
            Vec::<String>::new(),
            "still inside the grace window"
        );
        assert!(!out.values().flatten().any(|s| s == "b"));
    }

    #[test]
    fn a_newcomer_is_given_work_from_the_heaviest_member() {
        let map = assign_map(&[
            ("a", 10, Some("w1")),
            ("b", 10, Some("w1")),
            ("c", 10, Some("w1")),
            ("d", 10, Some("w1")),
        ]);
        let out = assign(&members(&["w1", "w2"]), &map, &BTreeSet::new(), 8, 7);
        assert_eq!(out["w1"].len(), 2);
        assert_eq!(out["w2"].len(), 2, "an idle newcomer is balanced into");
    }

    #[test]
    fn balance_is_on_weight_not_split_count() {
        // One 100-byte split against four 1-byte ones. Count-balancing
        // would split them 2/3; weight-balancing isolates the heavy one.
        let map = assign_map(&[
            ("heavy", 100, Some("w1")),
            ("t1", 1, Some("w1")),
            ("t2", 1, Some("w1")),
            ("t3", 1, Some("w1")),
            ("t4", 1, Some("w1")),
        ]);
        let out = assign(&members(&["w1", "w2"]), &map, &BTreeSet::new(), 8, 7);
        let by_load = loads(&out, &map);
        let heavy_holder = out
            .iter()
            .find(|(_, ids)| ids.iter().any(|s| s == "heavy"))
            .map(|(m, _)| m.clone())
            .expect("heavy assigned");
        assert_eq!(
            out[&heavy_holder].len(),
            1,
            "the heavy split is alone; the four light ones sit together"
        );
        assert_eq!(by_load.values().sum::<u64>(), 104);
    }

    #[test]
    fn a_balanced_fleet_is_left_alone() {
        // Equal loads, nothing to gain: the assignment must not churn.
        let map = assign_map(&[
            ("a", 5, Some("w1")),
            ("b", 5, Some("w2")),
            ("c", 5, Some("w1")),
            ("d", 5, Some("w2")),
        ]);
        let out = assign(&members(&["w1", "w2"]), &map, &BTreeSet::new(), 8, 7);
        assert_eq!(out["w1"], vec!["a".to_string(), "c".to_string()]);
        assert_eq!(out["w2"], vec!["b".to_string(), "d".to_string()]);
    }

    proptest! {
        /// Invariant 1 — deterministic in `(members, splits, ownership)`.
        #[test]
        fn assignment_is_deterministic(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms) = assignment_input(entries, fleet);
            let a = assign(&ms, &map, &reserved, cap, seed);
            let b = assign(&ms, &map, &reserved, cap, seed);
            prop_assert_eq!(a, b);
        }

        /// Invariant 2 — no split is assigned to two instances.
        #[test]
        fn no_split_is_assigned_twice(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms) = assignment_input(entries, fleet);
            let out = assign(&ms, &map, &reserved, cap, seed);
            let mut seen = BTreeSet::new();
            for id in out.values().flatten() {
                prop_assert!(seen.insert(id.clone()), "split {} assigned twice", id);
            }
        }

        /// Invariant 3 — stable under unchanged input. Feeding the
        /// function's own output back as current ownership must reproduce
        /// it exactly, or a steady-state fleet would drain on every
        /// replan.
        #[test]
        fn assignment_is_stable_under_unchanged_input(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms) = assignment_input(entries, fleet);
            let first = assign(&ms, &map, &reserved, cap, seed);

            // Re-key ownership to match `first`, leaving everything else.
            let mut next = map.clone();
            let mut owner_of: BTreeMap<&str, &str> = BTreeMap::new();
            for (m, ids) in &first {
                for id in ids {
                    owner_of.insert(id.as_str(), m.as_str());
                }
            }
            for (id, st) in next.iter_mut() {
                match owner_of.get(id.as_str()) {
                    Some(o) => {
                        st.lease = Some(lease(o, "n", 1));
                        st.progress.owner = Some((*o).to_string());
                    }
                    None => {
                        st.lease = None;
                        st.progress.owner = None;
                    }
                }
            }
            let second = assign(&ms, &next, &reserved, cap, seed);
            prop_assert_eq!(first, second, "assignment is not a fixpoint");
        }

        /// Invariant 4 — converges to a local optimum: no single split can
        /// move to a member with lane budget and reduce imbalance. This is
        /// the property `MAX_IMPROVING_MOVES` would silently break if it
        /// ever bound, which is why it is asserted rather than assumed.
        #[test]
        fn assignment_admits_no_improving_move(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms) = assignment_input(entries, fleet);
            let out = assign(&ms, &map, &reserved, cap, seed);
            let by_load = loads(&out, &map);
            for (from, ids) in &out {
                for id in ids {
                    let w = map[id].spec.as_ref().map_or(1, |s| s.weight.max(1));
                    for (to, _) in out.iter().filter(|(to, held)| {
                        *to != from && held.len() < cap as usize
                    }) {
                        prop_assert!(
                            by_load[from] <= by_load[to].saturating_add(w),
                            "moving {} from {} ({}) to {} ({}) would improve balance",
                            id, from, by_load[from], to, by_load[to]
                        );
                    }
                }
            }
        }

        /// Invariant 5 — total over the claimable pool: every assignable
        /// split is assigned unless every lane in the fleet is full.
        #[test]
        fn assignment_is_total_over_the_claimable_pool(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms) = assignment_input(entries, fleet);
            let out = assign(&ms, &map, &reserved, cap, seed);
            let assigned: BTreeSet<&String> = out.values().flatten().collect();
            let budget = ms.len() * cap as usize;
            for (id, st) in &map {
                let claimable = st.progress.status == SplitStatus::Runnable
                    && st.spec.is_some()
                    && !st.progress.completed
                    && !reserved.contains(id);
                if claimable && !assigned.contains(id) {
                    prop_assert_eq!(
                        assigned.len(), budget,
                        "{} left unassigned with lane budget to spare", id
                    );
                }
            }
        }

        /// A candidate is never terminal, never foreign-leased, never
        /// locally owned; quarantine appears exactly at the attempts gate.
        #[test]
        fn claim_candidates_are_always_safe(
            entries in proptest::collection::vec(
                (
                    "[a-z0-9]{1,8}",                  // id
                    0u8..3,                            // status
                    proptest::option::of("[a-z]{1,4}"),// record owner
                    0u64..5,                           // epoch
                    0u32..6,                           // attempts
                    proptest::option::of(("[a-z]{1,4}", "[a-z]{1,4}")), // lease owner+nonce
                ),
                0..24
            ),
            max_attempts in 1u32..5,
        ) {
            let me = "me";
            let map: BTreeMap<String, SplitState> = entries
                .into_iter()
                .map(|(id, status, owner, epoch, attempts, lease_parts)| {
                    let status = match status {
                        0 => SplitStatus::Runnable,
                        1 => SplitStatus::Completed,
                        _ => SplitStatus::Quarantined,
                    };
                    let l = lease_parts.map(|(o, n)| lease(&o, &n, epoch));
                    (
                        id.clone(),
                        state(record(&id, status, owner.as_deref(), epoch, attempts), 1, l),
                    )
                })
                .collect();
            let owned: BTreeSet<String> = map.keys().take(2).cloned().collect();
            for (id, action) in
                claim_candidates(&map, |id| owned.contains(id), me, max_attempts)
            {
                let s = &map[&id];
                prop_assert_eq!(s.progress.status, SplitStatus::Runnable);
                prop_assert!(!owned.contains(&id));
                if let Some((l, _)) = &s.lease {
                    prop_assert_eq!(l.owner.as_str(), me, "only own-id leases are claimable");
                }
                let kind = match action {
                    ClaimAction::Claim(k) | ClaimAction::Quarantine(k) => k,
                };
                let would_be = s.progress.attempts + u32::from(kind.consumes_attempt());
                let expect_quarantine = kind.consumes_attempt() && would_be >= max_attempts;
                prop_assert_eq!(
                    matches!(action, ClaimAction::Quarantine(_)),
                    expect_quarantine
                );
            }
        }

    }
}
