//! The bench corpora are reproducible, and are the fleets the benches claim.
//!
//! An instruction count only means something if both legs of a comparison ran
//! on byte-identical input, so "the corpus is a pure function of nothing" is a
//! property worth a test rather than an assumption. The benches themselves
//! cannot carry it: they need Linux, valgrind and a matching runner. This runs
//! everywhere `cargo test` does.
//!
//! Two of the profiles also claim something about the *state* their fleet is
//! in: the settled one is a fixpoint, and the recovering one carries every
//! claim kind. Neither is visible in a count, and both would
//! degrade silently: a settled corpus that stopped being a fixpoint would
//! measure a rebalance while calling itself steady state, and would still
//! report a number.

use spate_coordination::bench_seams::{ObservedSplit, plan_assignment, scan_claims, snapshot};
use std::collections::{BTreeMap, BTreeSet};

#[path = "../benches/support/fleet.rs"]
mod fleet;

fn owner_of(assignment: &BTreeMap<String, Vec<String>>) -> BTreeMap<&str, &str> {
    assignment
        .iter()
        .flat_map(|(member, splits)| splits.iter().map(|id| (id.as_str(), member.as_str())))
        .collect()
}

fn assign(corpus: Vec<ObservedSplit>, fleet: usize) -> BTreeMap<String, Vec<String>> {
    plan_assignment(
        &snapshot(corpus),
        &fleet::members(fleet),
        &BTreeSet::new(),
        &fleet::caps(fleet),
        fleet::LANE_CAP,
        fleet::SEED,
    )
}

#[test]
fn the_corpora_are_reproducible() {
    assert_eq!(
        fleet::fresh(fleet::ASSIGN_SPLITS, fleet::Weights::Packed),
        fleet::fresh(fleet::ASSIGN_SPLITS, fleet::Weights::Packed)
    );
    assert_eq!(
        fleet::settled(fleet::ASSIGN_SPLITS, fleet::Weights::Packed),
        fleet::settled(fleet::ASSIGN_SPLITS, fleet::Weights::Packed)
    );
    assert_eq!(
        fleet::settled(fleet::JOIN_SPLITS, fleet::Weights::Skewed),
        fleet::settled(fleet::JOIN_SPLITS, fleet::Weights::Skewed)
    );
    assert_eq!(
        fleet::leased(fleet::SCAN_SPLITS),
        fleet::leased(fleet::SCAN_SPLITS)
    );
    assert_eq!(
        fleet::recovering(fleet::SCAN_SPLITS),
        fleet::recovering(fleet::SCAN_SPLITS)
    );
}

/// Ids are the keys of the map both functions walk and the sort key the claim
/// scan orders on, so a generator that collided would quietly shrink every
/// corpus. The map would hold fewer entries than the vector did and every
/// count would fall for a reason unrelated to the code.
#[test]
fn split_ids_are_distinct() {
    let corpus = fleet::recovering(fleet::SCAN_SPLITS);
    let ids: BTreeSet<&str> = corpus.iter().map(|(id, ..)| id.as_str()).collect();
    assert_eq!(ids.len(), corpus.len(), "the id generator collided");
}

/// `settled_fleet` claims to be the tick a healthy fleet pays: the sticky
/// pass places everything and the improving pass finds no move. Nothing in a
/// count says so. If the corpus stopped being a fixpoint the case would
/// silently become a rebalance measurement wearing a steady-state name.
#[test]
fn the_settled_corpus_is_a_fixpoint() {
    let corpus = fleet::settled(fleet::ASSIGN_SPLITS, fleet::Weights::Packed);
    let assignment = assign(corpus.clone(), fleet::MEMBERS);
    let placed = owner_of(&assignment);
    assert_eq!(
        placed.len(),
        corpus.len(),
        "the settled fleet left splits unassigned"
    );
    for (id, weight, _, owner, ..) in &corpus {
        assert_eq!(
            placed.get(id.as_str()).copied(),
            owner.as_deref(),
            "split {id} (weight {weight:?}) moved: the corpus is not settled"
        );
    }
}

/// `unowned_pool` is `settled_fleet`'s pair: same pool, same fleet, same
/// budgets, nothing owned. Its claim is that the fill pass places the whole
/// pool. A lane budget that bound would truncate it instead, and the two
/// counts would stop being a difference in one pass.
#[test]
fn the_unowned_pool_is_placed_in_full() {
    let corpus = fleet::fresh(fleet::ASSIGN_SPLITS, fleet::Weights::Packed);
    let settled = fleet::settled(fleet::ASSIGN_SPLITS, fleet::Weights::Packed);
    let ids: Vec<&str> = corpus.iter().map(|(id, ..)| id.as_str()).collect();
    let settled_ids: Vec<&str> = settled.iter().map(|(id, ..)| id.as_str()).collect();
    assert_eq!(ids, settled_ids, "the pair disagrees on split ids");
    assert!(
        corpus
            .iter()
            .all(|(_, _, _, owner, _, _, lease)| { owner.is_none() && lease.is_none() }),
        "the unowned pool has acquired an owner"
    );
    let assignment = assign(corpus, fleet::MEMBERS);
    let placed: usize = assignment.values().map(Vec::len).sum();
    assert_eq!(
        placed,
        fleet::ASSIGN_SPLITS,
        "splits left unassigned with lane budget to spare"
    );
}

/// The skewed settled corpus has to be a fixpoint too, or the pair the join
/// profiles form is a pair of different questions.
#[test]
fn the_skewed_settled_corpus_is_a_fixpoint() {
    let corpus = fleet::settled(fleet::JOIN_SPLITS, fleet::Weights::Skewed);
    let assignment = assign(corpus.clone(), fleet::MEMBERS);
    let placed = owner_of(&assignment);
    for (id, _, _, owner, ..) in &corpus {
        assert_eq!(
            placed.get(id.as_str()).copied(),
            owner.as_deref(),
            "split {id} moved: the skewed corpus is not settled"
        );
    }
}

/// `joined_member` claims the opposite of `settled_fleet`: adding a member
/// gives the improving pass a rebalance to do. A corpus
/// whose splits all happened to stay put would report the settled number
/// under a name that promises otherwise.
#[test]
fn the_joined_member_corpora_rebalance() {
    for (name, weights) in [
        ("packed", fleet::Weights::Packed),
        ("skewed", fleet::Weights::Skewed),
    ] {
        let corpus = fleet::settled(fleet::JOIN_SPLITS, weights);
        let fleet_size = fleet::MEMBERS + 1;
        let assignment = assign(corpus.clone(), fleet_size);
        let placed = owner_of(&assignment);
        assert_eq!(
            placed.len(),
            corpus.len(),
            "{name}: the grown fleet left splits unassigned; the lane budget binds"
        );
        assert_eq!(
            assignment.len(),
            fleet_size,
            "{name}: the newcomer is missing from the assignment"
        );
        // A token move would satisfy "not empty" while leaving the profile
        // barely distinguishable from the settled one, so the floor is a
        // real share of a balanced fleet. Measured in **weight**, not split
        // count: balance is on weight, and under the skewed profile the
        // newcomer reaches its share by taking two multi-gigabyte splits
        // while every incumbent keeps dozens of small ones.
        let newcomer = fleet::member(fleet::MEMBERS);
        let weight_of: BTreeMap<&str, u64> = corpus
            .iter()
            .map(|(id, weight, ..)| (id.as_str(), weight.expect("the corpus carries weights")))
            .collect();
        let load = |member: &String| -> u64 {
            assignment[member]
                .iter()
                .map(|id| weight_of[id.as_str()])
                .sum()
        };
        let total: u64 = weight_of.values().sum();
        let even_share = total / fleet_size as u64;
        assert!(
            load(&newcomer) >= even_share / 2,
            "{name}: the newcomer was given {} of an even share of {even_share}, \
             so the improving pass barely ran and the profile measures \
             something close to a fixpoint",
            load(&newcomer)
        );
        let moved = corpus
            .iter()
            .filter(|(id, _, _, owner, ..)| placed.get(id.as_str()).copied() != owner.as_deref())
            .count();
        assert!(
            moved >= assignment[&newcomer].len(),
            "{name}: {moved} splits moved but the newcomer holds {}; the \
             assignment does not account for its own placements",
            assignment[&newcomer].len()
        );
    }
}

/// The two join profiles are a pair: same pool size, same fleet, same
/// budgets, differing only in the weight distribution. That is what makes the
/// difference between their counts attributable to the weights.
#[test]
fn the_join_profiles_differ_only_in_weight() {
    let packed = fleet::settled(fleet::JOIN_SPLITS, fleet::Weights::Packed);
    let skewed = fleet::settled(fleet::JOIN_SPLITS, fleet::Weights::Skewed);
    assert_eq!(packed.len(), skewed.len());
    for (a, b) in packed.iter().zip(&skewed) {
        assert_eq!(a.0, b.0, "the two profiles disagree on split ids");
        assert_eq!(a.3, b.3, "the two profiles disagree on ownership");
    }
    let heaviest = |corpus: &[ObservedSplit]| {
        corpus
            .iter()
            .filter_map(|(_, weight, ..)| *weight)
            .max()
            .expect("the corpus carries weights")
    };
    assert!(
        heaviest(&skewed) >= 32 * heaviest(&packed),
        "the skewed profile has stopped carrying object-sized splits: {} against {}",
        heaviest(&skewed),
        heaviest(&packed)
    );
}

/// The claim-scan profiles are distinguished by *how* the pool classifies,
/// not by its size, so the census is the thing that has to be pinned. Any
/// edit to a seed, a roll boundary, a count or `OWNED` moves it, and moving
/// it silently would re-baseline every comparison this bench anchors.
#[test]
fn the_claim_censuses_are_pinned() {
    let fresh = fleet::fresh(fleet::SCAN_SPLITS, fleet::Weights::Packed);
    let (_, census) = scan_claims(
        &snapshot(fresh),
        &BTreeSet::new(),
        fleet::INSTANCE,
        fleet::MAX_ATTEMPTS,
    );
    assert_eq!(
        census,
        [fleet::SCAN_SPLITS, 0, 0, 0, 0],
        "a freshly planned pool is every split a Create and nothing else"
    );

    let leased = fleet::leased(fleet::SCAN_SPLITS);
    let owned = fleet::owned(&leased);
    assert_eq!(owned.len(), fleet::OWNED);
    let (_, census) = scan_claims(
        &snapshot(leased),
        &owned,
        fleet::INSTANCE,
        fleet::MAX_ATTEMPTS,
    );
    assert_eq!(
        census,
        [0, 0, 0, 0, 0],
        "a fully leased pool offers this worker nothing"
    );

    let recovering = fleet::recovering(fleet::SCAN_SPLITS);
    let owned = fleet::owned(&recovering);
    let (_, census) = scan_claims(
        &snapshot(recovering),
        &owned,
        fleet::INSTANCE,
        fleet::MAX_ATTEMPTS,
    );
    assert!(
        census.iter().all(|n| *n > 0),
        "the recovering corpus has stopped carrying a claim kind: {census:?}"
    );
    assert_eq!(
        census,
        fleet::RECOVERING_CENSUS,
        "the recovering corpus classifies differently than it did; if that is \
         intended, the comparison it anchors has been re-baselined and every \
         recorded count for this bench is against a different corpus"
    );
}
