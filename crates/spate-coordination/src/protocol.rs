//! The pure decision core of the coordination protocol.
//!
//! Everything here is a function of the observed store state, with no I/O,
//! no channels, and no clocks, so the assignment, claim, and quarantine
//! rules are unit- and property-testable in isolation. The task layer feeds
//! observations in and executes the decisions with conditional writes;
//! losing any race is always safe because the write, not the decision, is
//! what transfers ownership.
//!
//! [`desired_assignment`] is the balance decision in full. Its contract is
//! specified normatively by [the work-assignment page]. The numbered
//! invariants there name the property tests at the bottom of this file,
//! and they are that page's own scheme, separate from the framework's
//! `INV-N`.
//!
//! Liveness discipline: a split is claimable exactly when its durable
//! progress record says `runnable` and no live lease key exists for it.
//! Lease keys expire on the store's clock or on each observer's own, so no
//! decision compares clocks across machines; fencing (the progress record
//! CAS) remains the only *correctness* mechanism regardless.
//!
//! [the work-assignment page]: https://spate.kainth.dev/docs/user-guide/concepts/work-assignment

use crate::records::{AssignmentVal, LeaseVal, SplitProgressRecord, SplitSpecRecord, SplitStatus};
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
    /// The record names no owner after a previous tenancy: a graceful
    /// release or a failure report.
    Released,
    /// A live lease under this worker's stable id on a record that still
    /// names an owner, such as a predecessor restarted under this id.
    /// Reclaimed fast, without waiting out the lease. An own lease on a
    /// record with no owner is [`Create`](Self::Create) or
    /// [`Released`](Self::Released).
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

/// A delay in `[0, base)` keyed by `seed`, so workers started together
/// spread their first tick across one interval.
pub(crate) fn spread(seed: u64, base: Duration) -> Duration {
    let h = stable_hash(seed, u64::MAX) % 1024;
    base.mul_f64((h as f64) / 1024.0)
}

/// Fleet size from the explicit membership keys, plus `member` when its own
/// key is absent; with `None`, only keys count.
pub(crate) fn live_workers(presence: &BTreeMap<String, Revision>, member: Option<&str>) -> usize {
    presence.len() + usize::from(member.is_some_and(|m| !presence.contains_key(m)))
}

/// The leader's desired assignment: which splits each live member should
/// hold. This is the whole balance decision, and the only place it is
/// made. Workers reconcile toward what they are given and never choose
/// for themselves.
///
/// **Balance is on weight, not split count.** A planner may emit splits of
/// very different sizes, so equal counts can mean unequal bytes. The lane
/// budget still caps the *count* per member, as a materialization limit
/// rather than a fairness one. Splits beyond the
/// fleet's summed budget stay unassigned and form the queue.
///
/// `caps` gives each member its own lane budget, as that member advertised
/// it on its presence key; `default_cap` covers a member whose presence
/// value predates the field. A member's budget must be *its own* rather
/// than the leader's, or a fleet of unequally-sized pods strands work: the
/// leader would keep handing splits to whichever member looks least loaded
/// while that member's own `max_in_flight` refuses to claim them, forever.
///
/// `reserved` names splits withheld by the rebalance delay, meaning work
/// whose owner has departed but whose grace window has not elapsed. The
/// window
/// itself is the leader's bookkeeping (it needs a clock); this function
/// only sees the resulting set, so it stays pure and replayable.
///
/// `previous` maps a split to the instance the last published assignment
/// named for it.
///
/// The passes run in order:
///
/// 1. **Sticky.** Every split whose lease owner is a live member with a
///    free lane stays with it. Each remaining split then stays with the
///    first of its `previous` assignee and its progress record's owner that
///    is a live member with a free lane. A move costs a drain, so the
///    assignment does not churn for a marginally better balance. The
///    `previous` assignee keeps a split whose claim the leader has not
///    observed yet with the member told to claim it.
/// 2. **Fill.** Unassigned splits go to the least-loaded member that has
///    lane budget, heaviest split first (longest-processing-time greedy).
/// 3. **Improve.** While some split can move from a heavier member to a
///    lighter one and strictly reduce imbalance, move it.
///
/// Pass 3's admission rule is `load(from) > load(to) + weight`, the
/// condition under which the move reduces the sum of squared loads, the
/// standard balance potential. Every move strictly improves, so none
/// oscillate.
///
/// Because each move strictly decreases an integer potential bounded
/// below, pass 3 terminates on its own; [`MAX_IMPROVING_MOVES`] bounds
/// the pass so a leader publishes instead of spinning on pathological
/// input.
///
/// The result is idempotent: feeding this function's own output back as
/// `previous` reproduces it exactly, whether each split's ownership in view
/// matches that output or is absent because its claim has not been observed
/// yet. Pass 1 restores every placement, pass 2 finds nothing unassigned,
/// and pass 3 finds no improving move because it already ran to fixpoint. A
/// steady-state fleet therefore publishes an unchanging assignment and
/// drains nothing.
///
/// `seed` keys the tie-breaks only (equal loads in pass 2, equal gains in
/// pass 3). It must be a property of the **job**, not of the leader, or
/// two leaders would break ties differently and a failover would churn the
/// fleet for no reason; the caller passes the job fingerprint hash.
pub(crate) fn desired_assignment(
    members: &BTreeSet<String>,
    splits: &BTreeMap<String, SplitState>,
    reserved: &BTreeSet<String>,
    previous: &BTreeMap<&str, &str>,
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
    // input, and a worker cannot start a split whose descriptor no worker
    // has), and not withheld by the rebalance delay.
    let mut pool: Vec<(&str, u64, &SplitState)> = splits
        .iter()
        .filter(|(id, state)| {
            state.progress.status == SplitStatus::Runnable
                && state.spec.is_some()
                && !state.progress.completed
                && !reserved.contains(id.as_str())
        })
        .map(|(id, state)| {
            let weight = state.spec.as_ref().map_or(1, |s| s.weight.max(1));
            (id.as_str(), weight, state)
        })
        .collect();
    // Heaviest first: pass 2 is LPT, and pass 1 needs a stable rule for
    // which splits an over-capacity owner keeps.
    pool.sort_by_key(|(id, weight, _)| (std::cmp::Reverse(*weight), *id));

    // Pass 1 — sticky. Leased splits go first over the whole pool: in a
    // single walk, a split kept only by `previous` can take the lane its
    // lease owner still needs. The record owner covers a split whose owner
    // died and whose lease has expired but which no worker has reclaimed yet.
    //
    // Loads are summed with `saturating_add`: a weight is planner-supplied
    // and unbounded (`spate-s3` reports bytes), and a leader must publish a
    // slightly-wrong assignment rather than panic on an overflow it cannot
    // influence.
    let fits = |out: &BTreeMap<String, Vec<String>>, m: &str| {
        members.contains(m) && out[m].len() < caps[m]
    };
    let place = |out: &mut BTreeMap<String, Vec<String>>,
                 load: &mut BTreeMap<&str, u64>,
                 member: &str,
                 id: &str,
                 weight: u64| {
        out.get_mut(member)
            .expect("live member")
            .push(id.to_string());
        let l = load.get_mut(member).expect("live member");
        *l = l.saturating_add(weight);
    };
    let mut rest: Vec<(&str, u64, &SplitState)> = Vec::new();
    for &(id, weight, state) in &pool {
        let owner = state.lease.as_ref().map(|(lease, _)| lease.owner.as_str());
        match owner.filter(|m| fits(&out, m)) {
            Some(member) => place(&mut out, &mut load, member, id, weight),
            None => rest.push((id, weight, state)),
        }
    }
    let mut unplaced: Vec<(&str, u64)> = Vec::new();
    for (id, weight, state) in rest {
        let sticky = previous
            .get(id)
            .copied()
            .filter(|m| fits(&out, m))
            .or_else(|| state.progress.owner.as_deref().filter(|m| fits(&out, m)));
        match sticky {
            Some(member) => place(&mut out, &mut load, member, id, weight),
            None => unplaced.push((id, weight)),
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

/// Where the published assignment records put each split, as split id to
/// instance: the leader's own record of what it decided. A live member's
/// record wins over a departed instance's when both name the same split; a
/// departed instance's record outlives it and is the only evidence of where
/// its splits were assigned.
pub(crate) fn last_assignees<'a>(
    records: &'a BTreeMap<String, (AssignmentVal, Revision)>,
    members: &BTreeSet<String>,
) -> BTreeMap<&'a str, &'a str> {
    let mut previous = BTreeMap::new();
    for live in [false, true] {
        for (instance, (val, _)) in records {
            if live != members.contains(instance) {
                continue;
            }
            for id in &val.splits {
                previous.insert(id.as_str(), instance.as_str());
            }
        }
    }
    previous
}

/// Termination backstop for the improving-move pass. Each accepted move
/// strictly decreases the sum of squared loads, so the pass converges
/// without this; the bound guarantees a leader publishes *something* on
/// unanticipated input rather than spinning. Two moves per split is far
/// above what greedy needs in practice. If this ever binds, the
/// local-optimality property test reports it.
const MAX_IMPROVING_MOVES: usize = 4096;

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
/// weights (`spate-s3` reports bytes), so `weight * load` overflows `u64`
/// once two splits of a few GiB each sit on one member against an idle
/// peer. That overflow panics the coordination task under the debug
/// profile's overflow checks and silently mis-ranks moves under release.
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
/// in. Whether it *should* claim one is the assignment's call. This
/// function validates eligibility and nothing more.
///
/// A split is claimable when its progress record is `runnable`, this
/// worker does not hold it, and there is no live foreign lease (a live
/// lease under our own stable id does not block a claim). A claim also
/// requires the spec record to have been observed, since a `Gained` event
/// carries the descriptor, while a quarantine does not (it writes only
/// the progress record).
///
/// Ordered by [`ClaimKind`] priority then split id. There is no per-worker
/// hash and no weight tie-break; the leader's assignment decides both
/// placement and order, and two workers never race for the same split.
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
        let kind = match (&state.lease, &state.progress.owner, state.progress.epoch) {
            (Some((lease, _)), _, _) if lease.owner != instance => continue,
            (Some(_), Some(_), _) => ClaimKind::Reclaim,
            (None, Some(_), _) => ClaimKind::Expired,
            (_, None, 0) => ClaimKind::Create,
            (_, None, _) => ClaimKind::Released,
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

    fn lease(owner: &str, run: &str, epoch: u64) -> (LeaseVal, Revision) {
        (
            LeaseVal {
                schema: SCHEMA,
                owner: owner.to_string(),
                nonce: run.to_string(),
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

    /// A live lease under this worker's id is a reclaim, and charges an
    /// attempt, only while the record names an owner. Regression for #894.
    #[test]
    fn an_own_lease_reclaims_only_while_the_record_names_an_owner() {
        let run = uuid::Uuid::new_v4().simple().to_string();
        let own = || Some(lease("me", &run, 2));
        let map = splits(vec![
            state(
                record("owned-by-me", SplitStatus::Runnable, Some("me"), 2, 2),
                1,
                own(),
            ),
            state(
                record("owned-by-peer", SplitStatus::Runnable, Some("peer"), 2, 2),
                1,
                own(),
            ),
            state(
                record("never-owned", SplitStatus::Runnable, None, 0, 2),
                1,
                own(),
            ),
            state(
                record("handed-back", SplitStatus::Runnable, None, 2, 2),
                1,
                own(),
            ),
        ]);
        let candidates = claim_candidates(&map, |_| false, "me", 3);
        let by_id: BTreeMap<&str, ClaimAction> =
            candidates.iter().map(|(id, a)| (id.as_str(), *a)).collect();
        assert_eq!(
            by_id["owned-by-me"],
            ClaimAction::Quarantine(ClaimKind::Reclaim)
        );
        assert_eq!(
            by_id["owned-by-peer"],
            ClaimAction::Quarantine(ClaimKind::Reclaim)
        );
        assert_eq!(by_id["never-owned"], ClaimAction::Claim(ClaimKind::Create));
        assert_eq!(
            by_id["handed-back"],
            ClaimAction::Claim(ClaimKind::Released)
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
        // The leader's LPT fill decides which remainders start first, so
        // this function must be weight-blind.
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

    /// First ticks for different workers fall inside one interval and
    /// differ, so a fleet started together does not reconcile together.
    #[test]
    fn spread_stays_in_one_interval_and_decorrelates() {
        let base = Duration::from_secs(30);
        let ticks: BTreeSet<Duration> = (0..64).map(|seed| spread(seed, base)).collect();
        assert!(ticks.iter().all(|t| *t < base), "{ticks:?}");
        assert!(
            ticks.len() > 32,
            "{} distinct first ticks of 64",
            ticks.len()
        );
    }

    #[test]
    fn membership_counts_self_exactly_once() {
        let mut presence = BTreeMap::new();
        assert_eq!(live_workers(&presence, Some("me")), 1);
        presence.insert("me".to_string(), Revision(1));
        assert_eq!(live_workers(&presence, Some("me")), 1);
        presence.insert("peer".to_string(), Revision(2));
        assert_eq!(live_workers(&presence, Some("me")), 2);
    }

    #[test]
    fn membership_counts_a_parted_member_by_its_key() {
        let mut presence = BTreeMap::new();
        assert_eq!(live_workers(&presence, None), 0);
        presence.insert("peer".to_string(), Revision(2));
        assert_eq!(live_workers(&presence, None), 1);
        presence.insert("me".to_string(), Revision(1));
        assert_eq!(live_workers(&presence, None), 2);
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

    /// `(id, weight, owner slot, ownership, previous slot, status)`. Both
    /// slots are drawn wider than any fleet size the tests use, so a share
    /// of splits name an owner or a last assignee that is *not* a live
    /// member. That is the departed-member case, where reassignment has to
    /// happen. `ownership` picks what the view shows of the owner: a lease
    /// and a record, the record alone (an expired lease), or the lease alone
    /// (a claim whose record write is not in view yet).
    type AssignEntry = (String, u64, Option<u8>, u8, Option<u8>, u8);

    fn assignment_entries() -> impl Strategy<Value = Vec<AssignEntry>> {
        proptest::collection::vec(
            (
                "[a-z]{1,6}",
                // Two scales. Small weights explore the tie-break and
                // lane-cap logic; byte-scale ones are what an object-store
                // planner emits, and are where `weight * load` left `u64`.
                prop_oneof![1u64..50, 1_000_000_000u64..8_000_000_000],
                proptest::option::of(0u8..7),
                0u8..3,
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

    /// The splits, the members, and the last published assignment as a map
    /// from split to member.
    type AssignInput = (
        BTreeMap<String, SplitState>,
        BTreeSet<String>,
        BTreeMap<String, String>,
    );

    fn assignment_input(entries: Vec<AssignEntry>, fleet: usize) -> AssignInput {
        let ms: BTreeSet<String> = (0..fleet).map(|i| format!("w{i}")).collect();
        let mut previous = BTreeMap::new();
        let mut states = Vec::new();
        for (id, weight, owner, ownership, last, status) in entries {
            let status = match status {
                0 => SplitStatus::Runnable,
                1 => SplitStatus::Completed,
                _ => SplitStatus::Quarantined,
            };
            let owner = owner.map(|o| format!("w{o}"));
            let l = owner
                .as_deref()
                .filter(|_| ownership != 1)
                .map(|o| lease(o, "n", 1));
            let record_owner = owner.as_deref().filter(|_| ownership != 2);
            if let Some(last) = last {
                previous.insert(id.clone(), format!("w{last}"));
            }
            states.push(state(record(&id, status, record_owner, 1, 0), weight, l));
        }
        (splits(states), ms, previous)
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
        assign_after(members, splits, reserved, &BTreeMap::new(), cap, seed)
    }

    /// [`assign`] after a published assignment, given as split to member.
    fn assign_after(
        members: &BTreeSet<String>,
        splits: &BTreeMap<String, SplitState>,
        reserved: &BTreeSet<String>,
        previous: &BTreeMap<String, String>,
        cap: u32,
        seed: u64,
    ) -> BTreeMap<String, Vec<String>> {
        let previous: BTreeMap<&str, &str> = previous
            .iter()
            .map(|(id, m)| (id.as_str(), m.as_str()))
            .collect();
        desired_assignment(
            members,
            splits,
            reserved,
            &previous,
            &BTreeMap::new(),
            cap,
            seed,
        )
    }

    /// An assignment as a map from split to member.
    fn published(assignment: &BTreeMap<String, Vec<String>>) -> BTreeMap<String, String> {
        assignment
            .iter()
            .flat_map(|(m, ids)| ids.iter().map(move |id| (id.clone(), m.clone())))
            .collect()
    }

    /// `map` with each split owned, lease and record, by its member in
    /// `owner_of` where `seen` holds, and owned by nobody everywhere else.
    fn claimed(
        map: &BTreeMap<String, SplitState>,
        owner_of: &BTreeMap<String, String>,
        seen: impl Fn(&String) -> bool,
    ) -> BTreeMap<String, SplitState> {
        let mut next = map.clone();
        for (id, st) in &mut next {
            let owner = owner_of.get(id).filter(|_| seen(id));
            st.lease = owner.map(|o| lease(o, "n", 1));
            st.progress.owner = owner.cloned();
        }
        next
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
        // its budget on its presence key; the leader honors it. Without
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
        let out = desired_assignment(
            &members(&["w1", "w2"]),
            &map,
            &BTreeSet::new(),
            &BTreeMap::new(),
            &caps,
            8,
            7,
        );
        assert_eq!(out["w2"].len(), 1, "w2 advertised a single lane");
        assert_eq!(
            out["w1"].len(),
            5,
            "everything else goes where there is budget to run it"
        );
    }

    #[test]
    fn byte_scale_weights_do_not_overflow_the_improving_move() {
        // `spate-s3` reports weight in BYTES, and a compressed object at or
        // above the packing target gets a split to itself, so multi-GiB
        // weights are the designed case. The improving-move gain is
        // `weight * (from - to - weight)`, which leaves `u64` at around two
        // 4.3 GB splits on one member against an idle peer. Under the debug
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

    /// Splits the last publish assigned, and whose claims the leader has not
    /// seen, stay with their assignees when another split completes.
    /// Regression for #820.
    #[test]
    fn a_completion_leaves_unclaimed_assignments_with_their_assignees() {
        let ms = members(&["w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7"]);
        // Each member holds 2 of its 3 lanes, and 20 splits are queued.
        let mut entries: Vec<(String, u64, Option<String>)> = Vec::new();
        for m in 0..8 {
            for k in 0..2 {
                entries.push((format!("a{m}{k}"), 1, Some(format!("w{m}"))));
            }
        }
        for q in 0..20 {
            entries.push((format!("q{q:02}"), 1, None));
        }
        let view = |e: &[(String, u64, Option<String>)]| {
            let r: Vec<(&str, u64, Option<&str>)> = e
                .iter()
                .map(|(id, w, o)| (id.as_str(), *w, o.as_deref()))
                .collect();
            assign_map(&r)
        };
        let first = published(&assign(&ms, &view(&entries), &BTreeSet::new(), 3, 7));
        let unclaimed: Vec<&String> = first.keys().filter(|id| id.starts_with('q')).collect();
        assert_eq!(unclaimed.len(), 8, "one queued split per free lane");

        entries.retain(|(id, _, _)| id != "a50");
        let second = published(&assign_after(
            &ms,
            &view(&entries),
            &BTreeSet::new(),
            &first,
            3,
            7,
        ));
        let moved: Vec<(&String, &String, Option<&String>)> = unclaimed
            .iter()
            .filter(|id| second.get(**id) != first.get(**id))
            .map(|id| (*id, &first[*id], second.get(*id)))
            .collect();
        assert!(moved.is_empty(), "unclaimed assignments moved: {moved:?}");
    }

    /// Pass 1 keeps a split with the first of its lease owner, its last
    /// assignee and its record owner that is a live member.
    #[test]
    fn a_split_sticks_to_its_lease_then_its_last_assignee_then_its_record_owner() {
        let map = splits(vec![
            state(
                record("held", SplitStatus::Runnable, Some("w1"), 1, 0),
                1,
                Some(lease("w1", "n", 1)),
            ),
            state(
                record("unclaimed", SplitStatus::Runnable, Some("w4"), 1, 0),
                1,
                None,
            ),
            state(
                record("expired", SplitStatus::Runnable, Some("w3"), 1, 0),
                1,
                None,
            ),
        ]);
        let previous: BTreeMap<String, String> =
            [("held", "w2"), ("unclaimed", "w2"), ("expired", "gone")]
                .into_iter()
                .map(|(id, m)| (id.to_string(), m.to_string()))
                .collect();
        // One lane each, so the improving pass cannot change what pass 1
        // placed. Every seed, so the fill pass's tie-break cannot land a split
        // where pass 1 should have put it.
        let ms = members(&["w1", "w2", "w3", "w4"]);
        for seed in 0..32 {
            let out = assign_after(&ms, &map, &BTreeSet::new(), &previous, 1, seed);
            assert_eq!(out["w1"], ["held"], "seed {seed}");
            assert_eq!(out["w2"], ["unclaimed"], "seed {seed}");
            assert_eq!(out["w3"], ["expired"], "seed {seed}");
            assert!(out["w4"].is_empty(), "seed {seed}");
        }
    }

    /// A last assignee whose lane budget is full keeps only what fits.
    #[test]
    fn a_last_assignee_at_its_lane_cap_does_not_keep_the_split() {
        let map = assign_map(&[("heavy", 10, Some("w2")), ("a", 1, None), ("b", 1, None)]);
        let previous: BTreeMap<&str, &str> = [("a", "w1"), ("b", "w1")].into_iter().collect();
        let caps: BTreeMap<String, u32> = [("w1".to_string(), 1)].into_iter().collect();
        let out = desired_assignment(
            &members(&["w1", "w2"]),
            &map,
            &BTreeSet::new(),
            &previous,
            &caps,
            8,
            7,
        );
        assert_eq!(out["w1"], ["a"], "w1 advertised a single lane");
        assert_eq!(out["w2"], ["b", "heavy"]);
    }

    /// A split its lease owner still holds keeps that owner's lane ahead of
    /// a split kept only by its last assignee.
    #[test]
    fn a_draining_split_returns_to_its_lease_owner() {
        // b is leased by w1 and was last published to w2; w1 was last given a.
        let map = assign_map(&[("a", 1, None), ("b", 1, Some("w1"))]);
        let previous: BTreeMap<String, String> = [("a", "w1"), ("b", "w2")]
            .into_iter()
            .map(|(s, m)| (s.to_string(), m.to_string()))
            .collect();
        let out = assign_after(
            &members(&["w1", "w2"]),
            &map,
            &BTreeSet::new(),
            &previous,
            1,
            7,
        );
        assert_eq!(out["w1"], ["b"], "the revocation of b is not taken back");
    }

    /// A heavier split kept by its last assignee, which pass 1 reaches first,
    /// does not take the lane of the lease owner it names, whether or not the
    /// leased split's own last assignee is still a member.
    #[test]
    fn a_heavier_unclaimed_split_leaves_a_lease_owner_its_lane() {
        let map = assign_map(&[("d", 1, Some("w0")), ("q", 2, None)]);
        let previous: BTreeMap<String, String> = [("d", "w1"), ("q", "w0")]
            .into_iter()
            .map(|(s, m)| (s.to_string(), m.to_string()))
            .collect();
        for fleet in [["w0", "w1"], ["w0", "w2"]] {
            for seed in 0..32 {
                let out =
                    assign_after(&members(&fleet), &map, &BTreeSet::new(), &previous, 1, seed);
                assert_eq!(out["w0"], ["d"], "fleet {fleet:?}, seed {seed}");
            }
        }
    }

    /// A lease naming a member that has left passes the split to its last
    /// assignee.
    #[test]
    fn a_departed_lease_owner_passes_the_split_to_its_last_assignee() {
        let map = splits(vec![state(
            record("s", SplitStatus::Runnable, None, 1, 0),
            1,
            Some(lease("gone", "n", 1)),
        )]);
        let previous: BTreeMap<String, String> =
            [("s".to_string(), "w2".to_string())].into_iter().collect();
        let ms = members(&["w1", "w2"]);
        for seed in 0..32 {
            let out = assign_after(&ms, &map, &BTreeSet::new(), &previous, 1, seed);
            assert_eq!(out["w2"], ["s"], "seed {seed}");
        }
    }

    /// A live member's record names a split's last assignee over a departed
    /// instance's record naming the same split, and a departed instance's
    /// record still counts for a split no live record names.
    #[test]
    fn a_live_members_record_outranks_a_departed_ones() {
        let record = |splits: &[&str]| {
            let val = AssignmentVal {
                schema: SCHEMA,
                generation: 1,
                splits: splits.iter().map(|s| (*s).to_string()).collect(),
            };
            (val, Revision(1))
        };
        // One departed record sorts before the live one and one after it.
        let records: BTreeMap<String, (AssignmentVal, Revision)> = [
            ("a-gone".to_string(), record(&["shared", "orphan"])),
            ("m-live".to_string(), record(&["shared"])),
            ("z-gone".to_string(), record(&["shared"])),
        ]
        .into_iter()
        .collect();
        let previous = last_assignees(&records, &members(&["m-live"]));
        assert_eq!(previous.get("shared"), Some(&"m-live"));
        assert_eq!(previous.get("orphan"), Some(&"a-gone"));
    }

    proptest! {
        /// Invariant 1 — deterministic in its inputs.
        #[test]
        fn assignment_is_deterministic(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms, previous) = assignment_input(entries, fleet);
            let a = assign_after(&ms, &map, &reserved, &previous, cap, seed);
            let b = assign_after(&ms, &map, &reserved, &previous, cap, seed);
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
            let (map, ms, previous) = assignment_input(entries, fleet);
            let out = assign_after(&ms, &map, &reserved, &previous, cap, seed);
            let mut seen = BTreeSet::new();
            for id in out.values().flatten() {
                prop_assert!(seen.insert(id.clone()), "split {} assigned twice", id);
            }
        }

        /// Invariant 3 — stable under unchanged input. Feeding the
        /// function's own output back as current ownership and as the last
        /// published assignment must reproduce it exactly, or a
        /// steady-state fleet would drain on every replan.
        #[test]
        fn assignment_is_stable_under_unchanged_input(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms, previous) = assignment_input(entries, fleet);
            let first = assign_after(&ms, &map, &reserved, &previous, cap, seed);
            let owner_of = published(&first);
            let next = claimed(&map, &owner_of, |_| true);
            let second = assign_after(&ms, &next, &reserved, &owner_of, cap, seed);
            prop_assert_eq!(first, second, "assignment is not a fixpoint");
        }

        /// Invariant 3 — stable under unchanged ownership alone. Feeding the
        /// output back as current ownership, with no last published
        /// assignment, reproduces it exactly.
        #[test]
        fn assignment_is_stable_under_unchanged_ownership(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms, previous) = assignment_input(entries, fleet);
            let first = assign_after(&ms, &map, &reserved, &previous, cap, seed);
            let next = claimed(&map, &published(&first), |_| true);
            let second = assign(&ms, &next, &reserved, cap, seed);
            prop_assert_eq!(first, second, "ownership alone is not a fixpoint");
        }

        /// Invariant 3 — stable before its claims are seen. Feeding the
        /// output back as the last published assignment reproduces it while
        /// any share of its splits show no owner yet, so a claim still on
        /// its way to the leader's view moves nothing.
        #[test]
        fn assignment_is_stable_before_its_claims_are_seen(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
            seen in any::<u16>(),
        ) {
            let (map, ms, previous) = assignment_input(entries, fleet);
            let first = assign_after(&ms, &map, &reserved, &previous, cap, seed);
            let owner_of = published(&first);
            let index: BTreeMap<&String, usize> =
                map.keys().enumerate().map(|(i, id)| (id, i)).collect();
            let next = claimed(&map, &owner_of, |id| seen & (1 << index[id]) != 0);
            let second = assign_after(&ms, &next, &reserved, &owner_of, cap, seed);
            prop_assert_eq!(first, second, "an unseen claim moved a split");
        }

        /// Invariant 4 — converges to a local optimum: no single split can
        /// move to a member with lane budget and reduce imbalance.
        /// `MAX_IMPROVING_MOVES` binding would silently break this
        /// property, so it is asserted rather than assumed.
        #[test]
        fn assignment_admits_no_improving_move(
            entries in assignment_entries(),
            fleet in 1usize..5,
            cap in 1u32..5,
            seed in any::<u64>(),
            reserved in reserved_ids(),
        ) {
            let (map, ms, previous) = assignment_input(entries, fleet);
            let out = assign_after(&ms, &map, &reserved, &previous, cap, seed);
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
            let (map, ms, previous) = assignment_input(entries, fleet);
            let out = assign_after(&ms, &map, &reserved, &previous, cap, seed);
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
                    proptest::option::of((
                        prop_oneof![Just("me".to_string()), "[a-z]{1,4}"],
                        "[a-z]{1,4}",
                    )), // lease owner+nonce
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
                if s.progress.owner.is_none() {
                    prop_assert!(!kind.consumes_attempt(), "{} has no owner to charge for", id);
                }
                prop_assert_eq!(
                    kind == ClaimKind::Reclaim,
                    s.lease.is_some() && s.progress.owner.is_some()
                );
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
