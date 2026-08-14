//! Measurable entry points into work this crate does not otherwise expose.
//!
//! Benches in this workspace drive their crate's public API. `spate-s3` is
//! one standing exception; this module is the second.
//!
//! This crate's public entry point is
//! [`StoreCoordinator`](crate::StoreCoordinator), which is asynchronous and
//! driven by a store trait whose every method returns a future. Both
//! properties stop an instruction count being deterministic: under a runtime
//! the number becomes a function of how the scheduler interleaved polls and
//! how the store's futures happened to be ready. The balance decision and
//! the claim scan are pure functions of an observed store state, with no
//! I/O, no channels and no clocks, and they are the two whose cost scales
//! with the size of a job, so they are countable; reaching them means
//! reaching past the async surface.
//!
//! The seam holds to the same rules as `spate-s3`'s:
//!
//! - It is behind the off-by-default `testing` feature and `#[doc(hidden)]`,
//!   so it is not part of this crate's semver surface and no consumer of the
//!   `spate` facade can see it.
//! - It exports **functions and the aliases they need, never this crate's own
//!   types**. `SplitState`, `SplitProgressRecord`, `ClaimAction` and the rest
//!   stay private and free to change; only the shape of the work is fixed.
//!   [`Opaque`] is an alias over `std` types, which a caller must be able to
//!   name to hold a snapshot across the boundary of a collected region at
//!   all, and it fixes nothing: the value inside it is this crate's business.
//! - Each function is one whole unit of the work a stage does (one leader's
//!   balance decision, one worker's claim scan) rather than one internal
//!   call. Attribution below that level comes from the callgrind profile
//!   the bench already writes.
//!
//! [`snapshot`] is separate from the two functions that consume it because
//! building the observed state is **not** part of either unit. A worker's
//! split map is maintained incrementally by the store watch and lives across
//! ticks; the leader recomputes an assignment over a map it already holds.
//! Folding construction into the measured region would charge every count for
//! per-split record allocation the control loop pays once, at a rate that
//! swamps the `O(members x splits)` scan being measured.

use crate::protocol::{self, ClaimAction, ClaimKind, SplitState};
use crate::records::{LeaseVal, SCHEMA, SplitProgressRecord, SplitSpecRecord, SplitStatus};
use crate::store::Revision;
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};

/// A value this crate hands back for the caller to hold but not inspect.
///
/// The observed store state is a map of a private type, and a bench has to
/// build one in its fixture, outside the collected region, and pass it in.
/// Boxing it as `dyn Any` lets it cross that boundary without the seam
/// publishing the type: a caller can move it, borrow it and drop it, and
/// nothing else.
pub type Opaque = Box<dyn Any + Send>;

/// One split as a store snapshot presents it, in field order:
///
/// 1. the split id;
/// 2. the planner's weight, or `None` when the immutable spec record has not
///    been observed yet; the two states differ for both consumers, and a
///    weight is the only thing either reads out of that record;
/// 3. the durable record's status, spelled as the record's own JSON spells
///    it: `"runnable"`, `"completed"` or `"quarantined"`;
/// 4. the durable record's owner, `None` after a graceful release or before
///    any claim;
/// 5. the fencing epoch, `0` for a split that was never owned;
/// 6. delivery attempts consumed;
/// 7. the owner named by the live lease key, or `None` when no lease exists.
///
/// A tuple rather than a struct because a struct would be one of this
/// crate's own types, which this module does not export. Callers are expected
/// to build these from a shape of their own and convert at the boundary.
pub type ObservedSplit = (
    String,
    Option<u64>,
    &'static str,
    Option<String>,
    u64,
    u32,
    Option<String>,
);

/// How a claim scan classified the pool: the four claim kinds in the
/// protocol's own priority order, then the splits the attempts gate parked
/// instead of claiming, as
/// `[create, released, reclaim, expired, quarantined]`.
///
/// A census rather than the candidate list, because the list is one of this
/// crate's private types. The slots partition the candidates, so the sum is
/// the candidate count: a quarantined split lands in the last slot only.
pub type ClaimCensus = [usize; 5];

/// Build the observed store state the control loop holds: one entry per
/// split, carrying its durable progress record, its spec record if observed,
/// and its live lease if one exists.
///
/// This is fixture work, not measured work; see the module docs. Every
/// advisory field is fixed rather than sampled (`fp` zero, no watermark, no
/// resume state, a zero write stamp), so the same input always produces the
/// same state: neither consumer reads any of them, and a wall clock in a
/// bench corpus would make two legs of a comparison incomparable.
///
/// # Panics
///
/// If a status is not one of the three the record schema defines.
#[must_use]
pub fn snapshot(observed: Vec<ObservedSplit>) -> Opaque {
    let splits: BTreeMap<String, SplitState> = observed
        .into_iter()
        .map(
            |(id, weight, status, owner, epoch, attempts, lease_owner)| {
                let status = match status {
                    "runnable" => SplitStatus::Runnable,
                    "completed" => SplitStatus::Completed,
                    "quarantined" => SplitStatus::Quarantined,
                    other => panic!("{other:?} is not a split status"),
                };
                let state = SplitState {
                    progress: SplitProgressRecord {
                        schema: SCHEMA,
                        id: id.clone(),
                        fp: 0,
                        epoch,
                        status,
                        owner,
                        attempts,
                        watermark: None,
                        state: None,
                        completed: status == SplitStatus::Completed,
                        written_at_ms: 0,
                    },
                    progress_rev: Revision(1),
                    spec: weight.map(|weight| SplitSpecRecord {
                        schema: SCHEMA,
                        id: id.clone(),
                        fp: 0,
                        generation: 1,
                        weight,
                        descriptor: String::new(),
                    }),
                    lease: lease_owner.map(|owner| {
                        (
                            LeaseVal {
                                schema: SCHEMA,
                                owner,
                                nonce: "bench".to_string(),
                                epoch,
                            },
                            Revision(1),
                        )
                    }),
                };
                (id, state)
            },
        )
        .collect();
    Box::new(splits)
}

/// Compute the desired assignment: which splits each live member should
/// hold, given the observed state.
///
/// The whole of what a leader decides in one tick, and the only place the
/// decision is made. All three passes (sticky, fill, improve) run as one
/// unit because that is how the leader runs them and because they share
/// state: the fill pass places what the sticky pass left over, and the
/// improve pass runs to fixpoint over the result of both.
///
/// `caps` gives each member the lane budget it advertised, `default_cap`
/// covers a member that advertised none, and `seed` keys the tie-breaks,
/// taking the job fingerprint hash in production so that a leader failover
/// does not re-break every tie.
///
/// # Panics
///
/// If `snapshot` did not come from [`snapshot`].
#[must_use]
pub fn plan_assignment(
    snapshot: &Opaque,
    members: &BTreeSet<String>,
    reserved: &BTreeSet<String>,
    caps: &BTreeMap<String, u32>,
    default_cap: u32,
    seed: u64,
) -> BTreeMap<String, Vec<String>> {
    let splits = downcast(snapshot);
    protocol::desired_assignment(members, splits, reserved, caps, default_cap, seed)
}

/// Scan the observed state for splits this worker may act on, with the
/// quarantine decision folded in.
///
/// The whole of what a worker's reconcile does before it starts writing:
/// eligibility per split, the attempts gate, and the sort that makes the
/// resulting order deterministic. The sort is inside the unit: the scan
/// produces an ordered list, and the worker never holds an unordered
/// candidate list.
///
/// `owned` is what this worker already holds, which production keys off a map
/// bounded by the lane budget rather than off the split pool.
///
/// The candidates come back inside an [`Opaque`] rather than being dropped:
/// freeing a scan's worth of split ids inside the collected region would
/// charge the count for teardown. The [`ClaimCensus`] beside it is what a
/// caller can assert on, and costs one integer increment per candidate.
///
/// # Panics
///
/// If `snapshot` did not come from [`snapshot`].
#[must_use]
pub fn scan_claims(
    snapshot: &Opaque,
    owned: &BTreeSet<String>,
    instance: &str,
    max_attempts: u32,
) -> (Opaque, ClaimCensus) {
    let splits = downcast(snapshot);
    let candidates =
        protocol::claim_candidates(splits, |id| owned.contains(id), instance, max_attempts);
    let mut census: ClaimCensus = [0; 5];
    for (_, action) in &candidates {
        let slot = match action {
            ClaimAction::Claim(ClaimKind::Create) => 0,
            ClaimAction::Claim(ClaimKind::Released) => 1,
            ClaimAction::Claim(ClaimKind::Reclaim) => 2,
            ClaimAction::Claim(ClaimKind::Expired) => 3,
            ClaimAction::Quarantine(_) => 4,
        };
        census[slot] += 1;
    }
    (Box::new(candidates), census)
}

fn downcast(snapshot: &Opaque) -> &BTreeMap<String, SplitState> {
    snapshot
        .downcast_ref()
        .expect("the snapshot came from `snapshot`")
}
