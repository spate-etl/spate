//! The pure decision core of the work-stealing protocol.
//!
//! Everything here is a function of the observed store state — no I/O, no
//! channels, no clocks — so the claim, quarantine, working-set, and steal
//! rules are unit- and property-testable in isolation. The task layer
//! feeds observations in and executes the decisions with conditional
//! writes; losing any race is always safe because the write, not the
//! decision, is what transfers ownership.
//!
//! Liveness discipline: a split is claimable exactly when its durable
//! progress record says `runnable` and no live lease key exists for it.
//! Lease keys expire server-side (single clock — the store's), so there
//! are no cross-machine clock comparisons anywhere; fencing (the progress
//! record CAS) remains the only *correctness* mechanism regardless.

use crate::records::{LeaseVal, SplitProgressRecord, SplitSpecRecord, SplitStatus};
use crate::store::Revision;
use std::collections::{BTreeMap, HashMap};
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
/// never-owned work first, then instant handoffs, then reclaims, then
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

/// This worker's working-set target: its fair share of the incomplete
/// work, capped by `max_in_flight`. Unclaimed splits above the cap sit
/// unleased in the store — they are the queue, not a liability.
pub(crate) fn target(incomplete: usize, workers: usize, max_in_flight: u32) -> usize {
    incomplete
        .div_ceil(workers.max(1))
        .min(max_in_flight as usize)
}

/// Claimable splits, best first, with the quarantine decision folded in.
///
/// A split is claimable when its progress record is `runnable`, this
/// worker does not hold it, and there is no live foreign lease (a live
/// lease under our own stable id is the fast-reclaim case). A claim also
/// requires the spec record to have been observed — a `Gained` event
/// carries the descriptor — while a quarantine does not (it writes only
/// the progress record). Order: [`ClaimKind`] priority, then per-worker
/// hash so contending workers attack the pool in different orders —
/// except near the tail (`incomplete <= workers`), where weight takes
/// precedence so the heaviest remainders start first.
pub(crate) fn claim_candidates(
    splits: &BTreeMap<String, SplitState>,
    owned: impl Fn(&str) -> bool,
    instance: &str,
    max_attempts: u32,
    incomplete: usize,
    workers: usize,
    seed: u64,
) -> Vec<(String, ClaimAction)> {
    let tail = incomplete <= workers;
    let mut out: Vec<(String, ClaimAction, u64, u64)> = Vec::new();
    for (id, state) in splits {
        if state.progress.status != SplitStatus::Runnable || owned(id) {
            continue;
        }
        let kind = match &state.lease {
            Some((lease, _)) if lease.owner == instance => ClaimKind::Reclaim,
            Some(_) => continue, // live foreign lease: steal territory, not claim
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
        let weight = state.spec.as_ref().map_or(1, |s| s.weight.max(1));
        out.push((
            id.clone(),
            action,
            u64::MAX - weight, // ascending sort => heaviest first
            stable_hash_str(seed, id),
        ));
    }
    if tail {
        out.sort_by_key(|a| (kind_of(a), a.2, a.3));
    } else {
        out.sort_by_key(|a| (kind_of(a), a.3));
    }
    out.into_iter()
        .map(|(id, action, _, _)| (id, action))
        .collect()
}

fn kind_of(entry: &(String, ClaimAction, u64, u64)) -> ClaimKind {
    match entry.1 {
        ClaimAction::Claim(kind) | ClaimAction::Quarantine(kind) => kind,
    }
}

/// Pick one split to steal when below target with nothing claimable: a
/// hash-picked split of the most-loaded live owner holding **at least two
/// more than this worker** (pairwise improvement — the transfer strictly
/// narrows the gap, so ownership converges to ±1 balance and can never
/// oscillate). The rule is deliberately *not* `victim > fair share`: fair
/// share is computed from visible lease holders, and a worker holding
/// nothing is invisible in them — under a fair-share gate, workers joining
/// an at-equilibrium fleet would starve. The steal is a CAS like any
/// claim — the victim keeps working until its next write fences, so the
/// duplicate window is bounded by one commit interval.
pub(crate) fn steal_candidate(
    splits: &BTreeMap<String, SplitState>,
    instance: &str,
    own: usize,
    seed: u64,
) -> Option<String> {
    let mut held_by: HashMap<&str, Vec<&str>> = HashMap::new();
    for (id, state) in splits {
        if state.progress.status != SplitStatus::Runnable || state.spec.is_none() {
            continue;
        }
        if let Some((lease, _)) = &state.lease
            && lease.owner != instance
        {
            held_by.entry(lease.owner.as_str()).or_default().push(id);
        }
    }
    let (_, victims) = held_by
        .into_iter()
        .filter(|(_, held)| held.len() > own + 1)
        // Most-loaded victim; owner-name hash breaks ties deterministically.
        .max_by_key(|(owner, held)| (held.len(), stable_hash_str(seed, owner)))?;
    victims
        .into_iter()
        .max_by_key(|id| stable_hash_str(seed, id))
        .map(str::to_string)
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
        let candidates = claim_candidates(&map, |_| false, "me", 4, 10, 10, 7);
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
        let candidates = claim_candidates(&map, |_| false, "me", 3, 10, 10, 7);
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
        let candidates = claim_candidates(&map, |_| false, "me", 4, 10, 10, 7);
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
    fn tail_ordering_prefers_heavy_splits() {
        let map = splits(vec![
            state(record("light", SplitStatus::Runnable, None, 0, 0), 1, None),
            state(
                record("heavy", SplitStatus::Runnable, None, 0, 0),
                1 << 30,
                None,
            ),
        ]);
        // Not the tail: hash order (whatever it is, both present).
        let wide = claim_candidates(&map, |_| false, "me", 4, 1000, 1000, 7);
        assert_eq!(wide.len(), 2);
        // Tail (incomplete <= workers): heaviest first.
        let tail = claim_candidates(&map, |_| false, "me", 4, 2, 2, 7);
        assert_eq!(tail[0].0, "heavy");
    }

    #[test]
    fn steal_is_pairwise_from_the_most_loaded_owner() {
        let mut states = vec![];
        for i in 0..4 {
            states.push(state(
                record(
                    &format!("rich-{i}"),
                    SplitStatus::Runnable,
                    Some("rich"),
                    1,
                    0,
                ),
                1,
                Some(lease("rich", "n", 1)),
            ));
        }
        states.push(state(
            record("poor-0", SplitStatus::Runnable, Some("poor"), 1, 0),
            1,
            Some(lease("poor", "n", 1)),
        ));
        let map = splits(states);

        // A zero-split newcomer steals from the most-loaded owner.
        let victim_split = steal_candidate(&map, "me", 0, 7).expect("steal");
        assert!(victim_split.starts_with("rich-"), "{victim_split}");
        // Holding 3 against rich's 4: 4 > 3+1 is false — no steal (±1
        // balance reached; stealing would oscillate).
        assert_eq!(steal_candidate(&map, "me", 3, 7), None);
        // The pairwise rule, not fair share: holding 2, rich's 4 > 3.
        assert!(steal_candidate(&map, "me", 2, 7).is_some());
    }

    #[test]
    fn working_set_target_caps_fair_share() {
        assert_eq!(target(1000, 4, 8), 8, "capped by max_in_flight");
        assert_eq!(target(6, 4, 8), 2, "ceil(6/4)");
        assert_eq!(target(0, 4, 8), 0);
        assert_eq!(target(5, 0, 8), 5, "no known workers counts as one");
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

    proptest! {
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
            seed in any::<u64>(),
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
            let incomplete = map
                .values()
                .filter(|s| s.progress.status == SplitStatus::Runnable)
                .count();
            for (id, action) in
                claim_candidates(&map, |id| owned.contains(id), me, max_attempts, incomplete, 3, seed)
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

        /// The target never exceeds the cap, and uncapped it covers the
        /// incomplete work across the fleet.
        #[test]
        fn target_is_a_capped_ceiling(
            incomplete in 0usize..10_000,
            workers in 1usize..64,
            cap in 1u32..64,
        ) {
            let t = target(incomplete, workers, cap);
            prop_assert!(t <= cap as usize);
            if t < cap as usize {
                prop_assert!(t * workers >= incomplete);
            }
        }

        /// Stealing strictly narrows the victim-thief gap (convergence).
        #[test]
        fn steal_is_a_strict_pairwise_improvement(
            victim_count in 0usize..12,
            own in 0usize..12,
            seed in any::<u64>(),
        ) {
            let states: Vec<SplitState> = (0..victim_count)
                .map(|i| {
                    state(
                        record(&format!("s{i}"), SplitStatus::Runnable, Some("victim"), 1, 0),
                        1,
                        Some(lease("victim", "n", 1)),
                    )
                })
                .collect();
            let map = splits(states);
            match steal_candidate(&map, "me", own, seed) {
                Some(_) => {
                    prop_assert!(victim_count > own + 1);
                    // After the transfer the gap strictly narrows.
                    prop_assert!((victim_count - 1) > own || victim_count == own + 2);
                }
                None => prop_assert!(victim_count <= own + 1),
            }
        }
    }
}
