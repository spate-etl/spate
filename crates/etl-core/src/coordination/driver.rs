//! Reusable source-side choreography for coordinated sources.
//!
//! A coordinated source owns two very different jobs: reading its data
//! (lanes, fetchers, offsets — connector-specific) and translating
//! [`CoordinationEvent`]s into the controller's assignment protocol while
//! keeping fenced-tenancy bookkeeping straight (source-generic, and where
//! every subtle interleaving bug lives). [`CoordinationDriver`] owns the
//! second job wholesale: a source embeds one next to a [`SplitSource`]
//! implementation and delegates `poll_events`/`commit` to it.
//!
//! # Tenancies, lanes, partitions
//!
//! Every continuous ownership span of a split — from `Gained` to whatever
//! ends it — is one **tenancy**, and each tenancy gets a fresh, never
//! reused [`PartitionId`] *and* a fresh, never reused [`LaneId`]: a lane
//! materializes exactly once, when its tenancy's split is staged for
//! opening, and lives untouched until the tenancy ends. Gains are
//! additive ([`SourceEvent::LanesAdded`]) — flowing lanes are never
//! drained because a peer's split arrived. Watermarks come back keyed by
//! partition, so a late drain-commit from a lane that lost its split
//! resolves to a retired tenancy and is skipped — a stale write cannot be
//! folded, committed, or resurrected by construction.
//!
//! # Event choreography
//!
//! One controller event per [`poll_events`](CoordinationDriver::poll_events)
//! call, in priority order:
//!
//! 1. Pending losses → partial [`SourceEvent::LanesRevoked`] (barrier
//!    sized one party per lane, matching the runtime's drain contract).
//!    Once delivered, the retired tenancies they belonged to have
//!    absorbed every late watermark they can see and are pruned.
//! 2. Staged gains → [`SourceEvent::LanesAdded`] with lanes for the
//!    newly-gained splits only; existing lanes and their in-flight acks
//!    are untouched.
//! 3. Otherwise poll the coordinator (the idle wait delegates there),
//!    fold its events into the tenancy table, sweep for completions, and
//!    advance any in-flight cooperative revocations (a revoked split, once
//!    its intake is stopped and its tail acked, takes a final fenced
//!    commit and is handed back barrier-less).
//! 4. [`CoordinationEvent::AllComplete`] → [`SourceEvent::Drained`];
//!    [`CoordinationEvent::Stalled`] → a fatal error by default
//!    (see [`stall_drains`](CoordinationDriver::stall_drains)).
//! 5. Nothing staged and some lane newly at end-of-input
//!    ([`SplitSource::take_finishing`]) → [`SourceEvent::CommitReady`],
//!    so the runtime chases the final acks instead of waiting out its
//!    commit tick.

use super::{
    ControlWaker, CoordinationError, CoordinationErrorKind, CoordinationEvent, LeaseEpoch,
    SplitCoordinator, SplitId, SplitPlanner, SplitProgress, SplitSpec,
};
use crate::error::{ErrorClass, SourceError};
use crate::record::PartitionId;
use crate::source::{DrainBarrier, LaneId, SourceEvent, SourceLane};
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Everything the driver hands a source when a split's lane is
/// materialized — exactly once per tenancy, when the gain is staged into
/// a [`SourceEvent::LanesAdded`].
#[derive(Debug)]
#[non_exhaustive]
pub struct SplitOpening<'a> {
    /// The split to read.
    pub split: &'a SplitSpec,
    /// Authoritative progress to resume from (already validated via
    /// [`SplitSource::validate_resume`]); `None` for a fresh split.
    pub resume: Option<&'a SplitProgress>,
    /// Lane id minted for this tenancy's lifetime; never reused by this
    /// source.
    pub lane: LaneId,
    /// Stable partition id for this tenancy — the key under which this
    /// split's watermarks come back to [`CoordinationDriver::commit`].
    pub partition: PartitionId,
    /// Fencing token of the current tenancy.
    pub epoch: LeaseEpoch,
    /// Wakes the control-plane wait. Clone it into the lane and signal it
    /// the moment the lane decides end-of-input or reports poison —
    /// otherwise the driver only notices between waits and the split's
    /// completion waits out an idle timeout.
    pub waker: &'a ControlWaker,
}

/// What the driver needs from the embedding source.
///
/// Implement it on the source's lane-assembly context (the sub-struct that
/// holds what lane construction needs), not on the source itself — the
/// driver lives beside that context as a sibling field, so both can be
/// borrowed disjointly.
pub trait SplitSource {
    /// The data-plane lane type produced for gained splits.
    type Lane: SourceLane;

    /// Materialize the lane for a gained (or re-assigned) split. Spawn
    /// fetchers here; never block on data.
    fn open_split(&mut self, opening: SplitOpening<'_>) -> Result<Self::Lane, SourceError>;

    /// Drift-check carried progress against this instance's view of the
    /// split (etag pins, schema versions) before it is trusted. Rejecting
    /// stops the pipeline — carried progress that no longer matches the
    /// input is unrecoverable divergence.
    fn validate_resume(
        &self,
        split: &SplitSpec,
        progress: &SplitProgress,
    ) -> Result<(), SourceError> {
        let _ = (split, progress);
        Ok(())
    }

    /// Snapshot the split's committable progress at an acked watermark:
    /// the opaque resume state plus whether that watermark completes the
    /// split (fully delivered **and** fully acknowledged — the source owns
    /// its eof/emitted accounting).
    fn encode_commit(
        &mut self,
        split: &SplitId,
        watermark: i64,
    ) -> Result<SplitProgress, SourceError>;

    /// Completion sweep for an owned split with no new watermark this
    /// tick (empty splits; tails acked exactly at the previous commit):
    /// `Some(terminal progress)` when complete, `None` while data is in
    /// flight.
    fn sweep(&mut self, split: &SplitId) -> Result<Option<SplitProgress>, SourceError>;

    /// The split's lane is being retired (lost, fenced, completed, or
    /// shutdown): detach its fetcher — never abort it, the pipeline thread
    /// may still be draining the lane. Must not block.
    ///
    /// This is the end of the tenancy: the driver never calls
    /// [`SplitSource::encode_commit`] or [`SplitSource::sweep`] for the
    /// split afterwards (its tenancy is retired first, and retired
    /// tenancies absorb late watermarks), so the source may drop the
    /// split's state here.
    fn close_split(&mut self, split: &SplitId);

    /// Splits whose lanes decided end-of-input since the last call (the
    /// edge, not the level). The driver surfaces them as
    /// [`SourceEvent::CommitReady`] so the runtime chases their final acks
    /// instead of waiting out the commit tick — the split then completes
    /// (and frees its working-set slot) within milliseconds of its last
    /// record becoming sink-durable. Purely a latency hint; the default
    /// reports none.
    fn take_finishing(&mut self) -> Vec<SplitId> {
        Vec::new()
    }

    /// Begin a cooperative revocation of an owned split: stop its intake at a
    /// safe boundary while **keeping** its commit state, so the tail can
    /// still be chased to a final fenced commit. Unlike
    /// [`close_split`](SplitSource::close_split) (which ends the tenancy and
    /// lets the source drop the split's state), the split stays commit- and
    /// sweep-adjacent here — the driver keeps committing its acked
    /// watermarks and then calls [`drain_ready`](SplitSource::drain_ready)
    /// until the drain finishes.
    ///
    /// Return `true` to accept the revocation, `false` to decline it (the
    /// default). **Contract: return `false` for any split this source has
    /// not opened or has already closed or completed** — the driver also
    /// guards this (it declines tenancies without an open lane and feeds
    /// the decline back to the backend), but the source must not rely on
    /// that alone.
    ///
    /// Declining is safe but not free. The split still leaves — a
    /// revocation is the leader's decision, not a proposal — so the backend
    /// forces the release instead and this split's uncommitted tail replays
    /// under its next owner. A source that *can* stop intake at a safe
    /// boundary should, because that is the difference between a
    /// replay-free move and a bounded-duplicate one.
    ///
    /// While a split is handing off, [`encode_commit`](SplitSource::encode_commit)
    /// must never report it `completed` — a drain cut can look terminal to
    /// the source (everything emitted is acked) but the split is
    /// half-read; the driver strips a `completed` flag it sees here and
    /// logs an error, so a conforming source should never trigger it.
    fn begin_revoke(&mut self, split: &SplitId) -> bool {
        let _ = split;
        false
    }

    /// Poll a handing-off split for its final progress: `Some(progress)`
    /// with `completed: false` once every record it emitted is acked **and**
    /// that watermark is committed, so the resume point handed to the next
    /// owner covers everything this instance produced (a replay-free
    /// transfer); `None` while any of that tail is still in flight, to be
    /// retried on the next poll. Never reports `completed` — a revocation gives
    /// the split away, it does not finish it. The default reports `None`.
    ///
    /// **Level-triggered, unlike [`take_finishing`](SplitSource::take_finishing):**
    /// keep returning `Some` on every poll until the driver retires the
    /// split — the final store commit can defer on a store hiccup and is
    /// re-attempted from a fresh `drain_ready` answer, so an
    /// edge-triggered implementation would stall the drain until the
    /// backend forced it.
    ///
    /// A source that accepts a revocation in
    /// [`begin_revoke`](SplitSource::begin_revoke) **must** eventually
    /// answer `Some` here. The default `None` never finishes a drain; it
    /// pairs with the default `begin_revoke`, which declines.
    fn drain_ready(&mut self, split: &SplitId) -> Result<Option<SplitProgress>, SourceError> {
        let _ = split;
        Ok(None)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum TenancyState {
    /// Owned; lane live or staged to open.
    Live,
    /// Owned, but intake has stopped at a safe boundary for a cooperative
    /// revocation: the lane is draining toward one final fenced commit. Still
    /// commit-eligible (tick commits keep folding its acked watermarks) and
    /// never swept — a revocation gives the split away rather than completing
    /// it. Becomes `Retired` (drained, so barrier-less) once that final
    /// commit lands, or fenced-`Retired` (the loss path) if a peer fences
    /// it mid-drain.
    Draining,
    /// Ownership over (lost, fenced, failed, completed, handed off); entry
    /// retained only to absorb late watermarks until its revocation is
    /// delivered.
    Retired,
}

#[derive(Debug)]
struct Tenancy {
    split: SplitSpec,
    epoch: LeaseEpoch,
    lane: Option<LaneId>,
    state: TenancyState,
    /// A commit for this tenancy was fenced: fold nothing, commit nothing.
    fenced: bool,
    /// Resume cache: the `Gained` carry, then every acked commit fold.
    /// Acked means sink-durable, so respawning from it can only skip data
    /// that is already safe — at-least-once holds even when the durable
    /// store lags a Retryable commit behind.
    progress: Option<SplitProgress>,
    /// Terminal progress reached the store; nothing further to commit.
    completed: bool,
    /// This tenancy released its split through a cooperative revocation: its
    /// final commit is durable, so the peer resumes replay-free. Routes the
    /// lane out through the barrier-less retired path, exactly like
    /// `completed` (nothing is in flight behind a drained revocation).
    handed_off: bool,
}

/// Source-side coordination choreography, embedded by a coordinated
/// source; see the [module docs](self) for the protocol it implements.
pub struct CoordinationDriver {
    coordinator: Box<dyn SplitCoordinator>,
    /// Parking half of the control-plane wakeup; the waker half is held by
    /// the backend and by every lane this driver opened.
    wait: crossbeam_channel::Receiver<()>,
    waker: ControlWaker,
    tenancies: BTreeMap<PartitionId, Tenancy>,
    by_split: BTreeMap<SplitId, PartitionId>,
    /// Lanes whose loss must still surface as a partial revoke.
    pending_lost: Vec<LaneId>,
    /// Lanes of completed tenancies: terminal progress reached the store,
    /// so they leave without a drain barrier.
    pending_retired: Vec<LaneId>,
    /// Tenancies gained but not yet materialized: their lanes go out in
    /// the next [`SourceEvent::LanesAdded`].
    pending_open: Vec<PartitionId>,
    all_complete: bool,
    stalled: Option<(u64, u64)>,
    stall_drains: bool,
    started: bool,
    next_partition: u32,
    /// Lane ids are minted once per tenancy and never reused.
    next_lane: u32,
}

impl fmt::Debug for CoordinationDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinationDriver")
            .field("tenancies", &self.tenancies.len())
            .field("live", &self.by_split.len())
            .field("pending_lost", &self.pending_lost.len())
            .field("pending_retired", &self.pending_retired.len())
            .field("pending_open", &self.pending_open.len())
            .field("all_complete", &self.all_complete)
            .field("stalled", &self.stalled)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

impl CoordinationDriver {
    /// Wrap a coordinator handle.
    #[must_use]
    pub fn new(mut coordinator: Box<dyn SplitCoordinator>) -> CoordinationDriver {
        let (waker, wait) = super::control_channel();
        coordinator.set_waker(waker.clone());
        CoordinationDriver {
            coordinator,
            wait,
            waker,
            tenancies: BTreeMap::new(),
            by_split: BTreeMap::new(),
            pending_lost: Vec::new(),
            pending_retired: Vec::new(),
            pending_open: Vec::new(),
            all_complete: false,
            stalled: None,
            stall_drains: false,
            started: false,
            next_partition: 0,
            next_lane: 0,
        }
    }

    /// Treat [`CoordinationEvent::Stalled`] as a drain-with-warning
    /// instead of a fatal error. Default `false`: a bounded job that
    /// cannot finish because splits are quarantined fails loudly rather
    /// than exiting as if it were complete.
    #[must_use]
    pub fn stall_drains(mut self, drains: bool) -> CoordinationDriver {
        self.stall_drains = drains;
        self
    }

    /// Join the job. Returns the event the source must return from the
    /// *same* `poll_events` call: the empty `LanesAssigned` ready signal
    /// (it bumps the controller's assignment epoch and marks the pipeline
    /// running while splits are still being claimed).
    pub fn start<L>(
        &mut self,
        planner: Box<dyn SplitPlanner>,
    ) -> Result<SourceEvent<L>, SourceError> {
        assert!(!self.started, "CoordinationDriver::start called twice");
        self.coordinator.start(planner).map_err(as_source_error)?;
        self.started = true;
        Ok(SourceEvent::LanesAssigned(Vec::new()))
    }

    /// Coordinated `poll_events` body: surfaces at most one controller
    /// event per call, per the [module docs](self).
    pub fn poll_events<S: SplitSource>(
        &mut self,
        source: &mut S,
        timeout: Duration,
    ) -> Result<SourceEvent<S::Lane>, SourceError> {
        assert!(self.started, "poll_events before start");

        // Staged work can be consumed without producing an event — a batch
        // of gains every one of which was retired before it could open — and
        // then the drain has to look again. Loop rather than recurse: the
        // fall-through below runs a second `coordinator.poll()`, so under
        // recursion the depth tracked how many such batches a backend
        // produced back to back, with nothing structural bounding it.
        let mut park = timeout;
        loop {
            // 1. Losses first: stop lost lanes before anything else runs.
            if !self.pending_lost.is_empty() {
                let lanes = std::mem::take(&mut self.pending_lost);
                let barrier = DrainBarrier::new(lanes.len());
                return Ok(SourceEvent::LanesRevoked { lanes, barrier });
            }

            // 1b. Completed tenancies leave barrier-less: their terminal
            // progress is in the store and nothing is in flight behind them.
            if !self.pending_retired.is_empty() {
                let lanes = std::mem::take(&mut self.pending_retired);
                return Ok(SourceEvent::LanesRetired { lanes });
            }

            // Reaching here means every queued revocation has been delivered
            // and the controller has drained + committed those lanes (its
            // revoke choreography is synchronous), so retired tenancies have
            // absorbed every late watermark they can ever see. Prune them —
            // and only them: `Live` tenancies are owned, and `Draining`
            // ones are still draining toward their final commit and must
            // survive to be advanced.
            self.tenancies
                .retain(|_, t| t.state != TenancyState::Retired);

            // 2. Staged gains: additive lanes for the newly-gained splits
            // only. Existing lanes are untouched — a routine gain must never
            // drain flowing lanes.
            if !self.pending_open.is_empty() {
                let lanes = self.open_pending(source)?;
                if !lanes.is_empty() {
                    return Ok(SourceEvent::LanesAdded(lanes));
                }
            }

            // 3. Terminal states, once the choreography above has quiesced.
            if let Some((completed, quarantined)) = self.stalled {
                if self.stall_drains {
                    tracing::warn!(
                        completed,
                        quarantined,
                        "job stalled; draining as configured"
                    );
                    return Ok(SourceEvent::Drained);
                }
                return Err(SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!(
                        "coordinated job stalled: {completed} splits completed but {quarantined} \
                     are quarantined and out of delivery attempts; inspect \
                     etl_coordination_splits_quarantined and requeue or exclude them"
                    ),
                });
            }
            if self.all_complete {
                return Ok(SourceEvent::Drained);
            }

            // 4. Drain the coordinator (never blocks — the wait is ours, at
            // the end of this function).
            let events = self.coordinator.poll().map_err(as_source_error)?;
            for event in events {
                self.apply(source, event)?;
            }

            // 5. Completion sweep over live, uncommitted-terminal tenancies.
            self.sweep(source)?;

            // 5b. Advance in-flight cooperative revocations: chase each
            // draining split's tail and, once acked and committed, release
            // it barrier-less to the requesting peer (or, if fenced
            // mid-drain, abort it through the loss path).
            self.advance_drains(source)?;

            if !self.pending_lost.is_empty()
                || !self.pending_retired.is_empty()
                || !self.pending_open.is_empty()
                || self.all_complete
                || self.stalled.is_some()
            {
                // Something is staged: go round and surface it on this same
                // call, without parking on the way.
                park = Duration::ZERO;
                continue;
            }
            break;
        }

        // 5. Nothing staged: surface newly-finishing splits so the runtime
        // chases their final acks instead of waiting out its commit tick.
        let finishing = source.take_finishing();
        if !finishing.is_empty() {
            let partitions: Vec<PartitionId> = finishing
                .iter()
                .filter_map(|split| self.by_split.get(split).copied())
                .collect();
            if !partitions.is_empty() {
                return Ok(SourceEvent::CommitReady { partitions });
            }
        }

        // 6. Nothing to report: park here, not inside the backend. Both
        // producers signal the same waker — the backend when it has events,
        // a lane the moment it decides end-of-input or reports poison — so
        // a completion surfaces in microseconds instead of waiting out the
        // remainder of this timeout. The sender half lives on `self`, so
        // the channel can never disconnect and this can never spin.
        if !park.is_zero() {
            let _ = self.wait.recv_timeout(park);
        }
        Ok(SourceEvent::Idle)
    }

    /// Coordinated `commit` body: per-split fenced commits keyed by the
    /// tenancy partition ids the driver minted.
    pub fn commit<S: SplitSource>(
        &mut self,
        source: &mut S,
        watermarks: &[(PartitionId, i64)],
    ) -> Result<(), SourceError> {
        for &(partition, watermark) in watermarks {
            let Some(tenancy) = self.tenancies.get(&partition) else {
                // Pruned tenancy: a drain commit that arrived after its
                // retirement was fully delivered. Its data replays under
                // the new owner.
                continue;
            };
            if tenancy.state == TenancyState::Retired || tenancy.fenced || tenancy.completed {
                continue;
            }
            let split = tenancy.split.id.clone();
            let progress = source.encode_commit(&split, watermark)?;
            self.commit_progress(source, partition, &split, progress)?;
        }
        Ok(())
    }

    /// Report an owned split as poison: consumes a delivery attempt and
    /// hands it back for another worker (or quarantine, at the cap). The
    /// split's lane is retired through the normal loss path.
    pub fn fail<S: SplitSource>(
        &mut self,
        source: &mut S,
        split: &SplitId,
        reason: &str,
    ) -> Result<(), SourceError> {
        let Some(&partition) = self.by_split.get(split) else {
            return Ok(()); // already lost — nothing to report
        };
        match self.coordinator.fail(split, reason) {
            Ok(()) => {}
            // Fenced: someone already took it; the retire below still applies.
            Err(e) if e.kind == CoordinationErrorKind::Fenced => {}
            Err(e) => return Err(as_source_error(e)),
        }
        self.retire(source, partition, false);
        Ok(())
    }

    /// Best-effort graceful release of every held split, so peers claim
    /// them without waiting out the lease. Call from the source's `Drop`.
    pub fn release(&mut self) {
        if !self.started {
            return;
        }
        let held: Vec<SplitId> = self.by_split.keys().cloned().collect();
        if held.is_empty() {
            return;
        }
        if let Err(e) = self.coordinator.release(&held) {
            tracing::warn!(error = %e, "graceful split release failed; leases will expire");
        }
    }

    /// Current live split → lane view (pause/resume bookkeeping, tests).
    #[must_use]
    pub fn assignments(&self) -> Vec<(SplitId, LaneId)> {
        self.by_split
            .iter()
            .filter_map(|(split, partition)| {
                let lane = self.tenancies.get(partition)?.lane?;
                Some((split.clone(), lane))
            })
            .collect()
    }

    fn apply<S: SplitSource>(
        &mut self,
        source: &mut S,
        event: CoordinationEvent,
    ) -> Result<(), SourceError> {
        match event {
            CoordinationEvent::Gained {
                split,
                epoch,
                progress,
            } => {
                if let Some(&stale) = self.by_split.get(&split.id) {
                    // Backend contract violation (re-gain without a loss);
                    // retire the stale tenancy defensively and continue.
                    tracing::warn!(split = %split.id, "gained a split already held; retiring stale tenancy");
                    self.retire(source, stale, false);
                }
                if let Some(progress) = progress.as_ref() {
                    source.validate_resume(&split, progress)?;
                }
                let partition = PartitionId(self.next_partition);
                self.next_partition += 1;
                self.by_split.insert(split.id.clone(), partition);
                self.tenancies.insert(
                    partition,
                    Tenancy {
                        split,
                        epoch,
                        lane: None,
                        state: TenancyState::Live,
                        fenced: false,
                        progress,
                        completed: false,
                        handed_off: false,
                    },
                );
                self.pending_open.push(partition);
            }
            CoordinationEvent::RevokeRequested { split } => {
                // The leader wants this split back. Accept only a split we
                // hold live with an OPEN lane, un-fenced and not yet
                // completed — a tenancy gained but not yet opened has no
                // intake to stop and no drain to finish, and accepting it
                // would strand it in `Draining` forever — and only if the
                // source can stop its intake at a safe boundary. A refusal
                // is declined back to the backend so it forces the release
                // now instead of waiting out its drain deadline; the split
                // leaves either way.
                let accepted = match self.by_split.get(&split) {
                    Some(&partition) => {
                        let eligible = self.tenancies.get(&partition).is_some_and(|t| {
                            t.state == TenancyState::Live
                                && t.lane.is_some()
                                && !t.fenced
                                && !t.completed
                        });
                        if eligible && source.begin_revoke(&split) {
                            // Re-borrow after `begin_revoke`; the guard
                            // reads above kept no mutable borrow across it.
                            if let Some(t) = self.tenancies.get_mut(&partition) {
                                t.state = TenancyState::Draining;
                            }
                            true
                        } else {
                            false
                        }
                    }
                    None => false,
                };
                if !accepted && let Err(e) = self.coordinator.decline_revoke(&split) {
                    // Liveness cost only: the backend forces the release at
                    // its own deadline regardless.
                    tracing::warn!(split = %split, error = %e, "revocation decline failed");
                }
            }
            CoordinationEvent::Lost { split } => {
                if let Some(&partition) = self.by_split.get(&split) {
                    self.retire(source, partition, false);
                }
                // Else: already retired (e.g. we fenced on commit first).
            }
            CoordinationEvent::Quarantined { split, attempts } => {
                tracing::warn!(split = %split, attempts, "split quarantined");
                if let Some(&partition) = self.by_split.get(&split) {
                    self.retire(source, partition, false);
                }
            }
            CoordinationEvent::AllComplete => {
                // A worker can watch a whole bounded job complete without
                // ever holding a split: the job finished before this
                // instance's first rebalance window (short bounded job),
                // or the fleet simply has more replicas than splits.
                // Normal either way — but without a line here it reads as
                // a silent no-op instance, so say what happened.
                if self.next_partition == 0 {
                    tracing::info!(
                        "coordinated job completed without this instance holding any split — \
                         the job finished before this instance's first rebalance window, or \
                         the fleet has more replicas than splits (see the scaling-out guide)"
                    );
                }
                self.all_complete = true;
            }
            CoordinationEvent::Stalled {
                completed,
                quarantined,
            } => {
                self.stalled = Some((completed, quarantined));
            }
        }
        Ok(())
    }

    /// End a tenancy: detach its fetchers, queue its lane for revocation,
    /// keep the entry to absorb late watermarks until the next assignment.
    fn retire<S: SplitSource>(&mut self, source: &mut S, partition: PartitionId, fenced: bool) {
        let Some(tenancy) = self.tenancies.get_mut(&partition) else {
            return;
        };
        if tenancy.state == TenancyState::Retired {
            if fenced {
                tenancy.fenced = true;
            }
            return;
        }
        tenancy.state = TenancyState::Retired;
        tenancy.fenced |= fenced;
        self.by_split.remove(&tenancy.split.id);
        let split = tenancy.split.id.clone();
        if let Some(lane) = tenancy.lane.take() {
            if tenancy.completed || tenancy.handed_off {
                // Fully delivered, acked, and committed — or drained and
                // handed off with its final commit durable: nothing can be
                // in flight, so the lane leaves without a drain barrier.
                self.pending_retired.push(lane);
            } else {
                self.pending_lost.push(lane);
            }
        }
        source.close_split(&split);
    }

    /// Materialize lanes for the staged gains only. Each tenancy opens
    /// exactly once, with a lane id minted for its lifetime; a staged
    /// tenancy that ended before it could open (gained then immediately
    /// lost or fenced) is skipped — its retirement already handled it.
    ///
    /// All-or-nothing: a failure part way through undoes the whole batch
    /// and re-stages it. Anything else strands the lanes already built —
    /// they never reach the runtime, yet their tenancies stay `Live`
    /// holding a lane id, which the `lane.is_some()` guard then skips
    /// forever. The splits would keep their leases, heartbeated and
    /// unreadable, and the job would stall instead of failing.
    fn open_pending<S: SplitSource>(
        &mut self,
        source: &mut S,
    ) -> Result<Vec<S::Lane>, SourceError> {
        let staged = std::mem::take(&mut self.pending_open);
        let mut lanes = Vec::with_capacity(staged.len());
        // Tenancies this call minted a lane for, so a failure can undo them.
        let mut opened: Vec<PartitionId> = Vec::new();
        for idx in 0..staged.len() {
            let partition = staged[idx];
            let Some(tenancy) = self.tenancies.get_mut(&partition) else {
                continue; // retired and pruned before it could open
            };
            if tenancy.state != TenancyState::Live || tenancy.lane.is_some() {
                continue;
            }
            let lane_id = LaneId(self.next_lane);
            self.next_lane = self
                .next_lane
                .checked_add(1)
                .expect("lane ids exhausted (u32)");
            tenancy.lane = Some(lane_id);
            let opening = SplitOpening {
                split: &tenancy.split,
                resume: tenancy.progress.as_ref(),
                lane: lane_id,
                partition,
                epoch: tenancy.epoch,
                waker: &self.waker,
            };
            match source.open_split(opening) {
                Ok(lane) => {
                    lanes.push(lane);
                    opened.push(partition);
                }
                Err(e) => {
                    // Dropping the lanes detaches whatever `open_split`
                    // spawned for them; clearing `lane` lets the retry mint
                    // a fresh id (ids are burned, never reused).
                    drop(lanes);
                    for p in opened.iter().chain(std::iter::once(&partition)) {
                        if let Some(t) = self.tenancies.get_mut(p) {
                            t.lane = None;
                        }
                    }
                    self.pending_open = staged;
                    return Err(e);
                }
            }
        }
        Ok(lanes)
    }

    fn sweep<S: SplitSource>(&mut self, source: &mut S) -> Result<(), SourceError> {
        let candidates: Vec<PartitionId> = self
            .tenancies
            .iter()
            .filter(|(_, t)| t.state == TenancyState::Live && !t.fenced && !t.completed)
            .map(|(&p, _)| p)
            .collect();
        for partition in candidates {
            let split = self.tenancies[&partition].split.id.clone();
            if let Some(progress) = source.sweep(&split)? {
                self.commit_progress(source, partition, &split, progress)?;
            }
        }
        Ok(())
    }

    /// Advance every in-flight cooperative revocation. For each `Draining`
    /// tenancy whose drain has finished — [`SplitSource::drain_ready`]
    /// returns the final progress once its tail is acked and committed —
    /// take one last fenced commit (never `completed`; a revocation gives the
    /// split away rather than finishing it) and dispose of it:
    ///
    /// - durable → mark drained, hand the split back
    ///   ([`SplitCoordinator::release_drained`]), and retire it barrier-less;
    /// - fenced → a peer fenced this tenancy mid-drain; retire it through
    ///   the loss path (its bounded tail replays under the new owner);
    /// - deferred → the store lagged; stay `Draining` and re-attempt on
    ///   the next poll.
    fn advance_drains<S: SplitSource>(&mut self, source: &mut S) -> Result<(), SourceError> {
        let draining: Vec<PartitionId> = self
            .tenancies
            .iter()
            .filter(|(_, t)| t.state == TenancyState::Draining && !t.fenced)
            .map(|(&p, _)| p)
            .collect();
        for partition in draining {
            let split = self.tenancies[&partition].split.id.clone();
            let Some(progress) = source.drain_ready(&split)? else {
                continue; // tail still in flight — retry next poll
            };
            debug_assert!(
                !progress.completed,
                "a revocation commit hands the split off, it must not complete it"
            );
            self.commit_drained(source, partition, &split, progress)?;
        }
        Ok(())
    }

    /// The disposition of one fenced commit attempt. Tick, sweep, and
    /// drain commits triage the backend's three answers identically; only
    /// what a durable `Ok` *means* differs, so each caller owns just that
    /// arm and shares the fence/retry handling here.
    fn try_commit<S: SplitSource>(
        &mut self,
        source: &mut S,
        partition: PartitionId,
        split: &SplitId,
        progress: &SplitProgress,
    ) -> Result<CommitDisposition, SourceError> {
        match self.coordinator.commit(split, progress) {
            Ok(()) => Ok(CommitDisposition::Durable),
            Err(e) if e.kind == CoordinationErrorKind::Fenced => {
                // Nothing was written; the split belongs to a peer. Retire
                // with the fence flag so nothing of this tenancy is ever
                // folded or respawned (the matching Lost event may still
                // arrive and finds the tenancy already retired).
                tracing::warn!(split = %split, "commit fenced; split lost to a peer");
                self.retire(source, partition, true);
                Ok(CommitDisposition::Fenced)
            }
            Err(e) if e.kind == CoordinationErrorKind::Retryable => {
                // Previous durable state stays authoritative; the caller
                // recommits the merged progress on the next tick.
                tracing::warn!(split = %split, error = %e, "commit deferred; will retry");
                Ok(CommitDisposition::Deferred)
            }
            Err(e) => Err(as_source_error(e)),
        }
    }

    /// Shared fenced-commit path for tick commits and sweep commits.
    fn commit_progress<S: SplitSource>(
        &mut self,
        source: &mut S,
        partition: PartitionId,
        split: &SplitId,
        progress: SplitProgress,
    ) -> Result<(), SourceError> {
        // A handing-off split's drain cut can look terminal to the source
        // (every record it emitted is acked), but committing it
        // `completed: true` would mark a half-read split permanently done
        // and its next owner would never resume it — silent data loss. The
        // etl-s3 source guards this itself; enforce it centrally so a
        // conforming third-party source cannot fall into the trap.
        let progress = if progress.completed
            && self
                .tenancies
                .get(&partition)
                .is_some_and(|t| t.state == TenancyState::Draining)
        {
            tracing::error!(
                split = %split,
                "source reported a handing-off split completed; forcing \
                 completed=false — a drain cut is never terminal"
            );
            SplitProgress::new(progress.watermark, progress.state)
        } else {
            progress
        };
        match self.try_commit(source, partition, split, &progress)? {
            CommitDisposition::Durable => {
                let tenancy = self.tenancies.get_mut(&partition).expect("live tenancy");
                let completed = progress.completed;
                tenancy.progress = Some(progress);
                if completed {
                    tenancy.completed = true;
                    // A completed split frees its lane for the working set.
                    self.retire(source, partition, false);
                }
            }
            // Fenced: already retired inside `try_commit`.
            CommitDisposition::Fenced => {}
            CommitDisposition::Deferred => {
                // The resume cache still advances: the watermark is acked
                // (sink-durable), so respawning past it cannot lose data.
                let tenancy = self.tenancies.get_mut(&partition).expect("live tenancy");
                tenancy.progress = Some(progress);
            }
        }
        Ok(())
    }

    /// Final fenced commit that ends a cooperative revocation. Same triage as
    /// [`commit_progress`](CoordinationDriver::commit_progress), but a
    /// durable commit hands the split back and retires it barrier-less
    /// instead of folding progress into a still-live tenancy.
    fn commit_drained<S: SplitSource>(
        &mut self,
        source: &mut S,
        partition: PartitionId,
        split: &SplitId,
        progress: SplitProgress,
    ) -> Result<(), SourceError> {
        // Same central guard as `commit_progress`: the handover progress is
        // never terminal, whatever the source's `drain_ready` claims.
        let progress = if progress.completed {
            tracing::error!(
                split = %split,
                "drain_ready returned completed=true; forcing completed=false — \
                 a drain cut is never terminal"
            );
            SplitProgress::new(progress.watermark, progress.state)
        } else {
            progress
        };
        match self.try_commit(source, partition, split, &progress)? {
            CommitDisposition::Durable => {
                // The tail is durable and the resume point now covers every
                // record this instance emitted. Mark it handed off (so the
                // lane retires barrier-less) and hand the split back so its
                // next owner claims replay-free.
                let tenancy = self
                    .tenancies
                    .get_mut(&partition)
                    .expect("handing-off tenancy");
                tenancy.progress = Some(progress);
                tenancy.handed_off = true;
                if let Err(e) = self
                    .coordinator
                    .release_drained(std::slice::from_ref(split))
                {
                    // Liveness cost only: the lease expires on its own and a
                    // peer takes over — no data is at risk. Retire anyway so
                    // the lane leaves this instance.
                    tracing::warn!(
                        split = %split,
                        error = %e,
                        "drain release failed; lease will expire and a peer will take over"
                    );
                }
                self.retire(source, partition, false);
            }
            // Fenced mid-drain (a peer took this tenancy): already retired
            // through the loss path inside `try_commit`. Its bounded tail
            // replays under the new owner.
            CommitDisposition::Fenced => {}
            CommitDisposition::Deferred => {
                // Store lagged: stay `Draining` and re-attempt the final
                // commit next poll. The cached progress advances the resume
                // point (the watermark is acked, sink-durable).
                let tenancy = self
                    .tenancies
                    .get_mut(&partition)
                    .expect("handing-off tenancy");
                tenancy.progress = Some(progress);
            }
        }
        Ok(())
    }
}

/// How the backend answered one fenced commit; see
/// [`CoordinationDriver::try_commit`].
enum CommitDisposition {
    /// Durable write. The caller advances its own state.
    Durable,
    /// Fenced: nothing written, the split belongs to a peer; the tenancy
    /// has already been retired with the fence flag.
    Fenced,
    /// Retryable: the previous durable state stays authoritative and the
    /// caller recommits the merged progress next tick.
    Deferred,
}

fn as_source_error(e: CoordinationError) -> SourceError {
    SourceError::Client {
        class: e.class(),
        reason: e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::AckRef;
    use crate::coordination::{PlanContext, PlanFinality, SplitPlan};
    use crate::record::RawPayload;
    use crate::source::PayloadBatch;
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    // ------------------------------------------------------------------
    // Scripted coordinator double (the shape etl-test later publishes).

    #[derive(Default)]
    struct ScriptState {
        batches: VecDeque<Vec<CoordinationEvent>>,
        commit_outcomes: HashMap<String, VecDeque<CoordinationErrorKind>>,
        commits: Vec<(SplitId, SplitProgress)>,
        fails: Vec<(SplitId, String)>,
        released: Vec<SplitId>,
        /// Captured separately from `released` so a test can prove the
        /// driver takes the revocation-release path, not a plain hand-back.
        released_drained: Vec<SplitId>,
        /// Every `decline_revoke` call, so a test can prove the driver feeds
        /// a refusal back to the backend, which then forces the release.
        declined: Vec<SplitId>,
        started: bool,
        waker: Option<ControlWaker>,
    }

    #[derive(Clone, Default)]
    struct Script(Arc<Mutex<ScriptState>>);

    impl Script {
        fn push(&self, events: Vec<CoordinationEvent>) {
            let mut st = self.0.lock().unwrap();
            st.batches.push_back(events);
            if let Some(w) = &st.waker {
                w.wake();
            }
        }

        fn fail_next_commit(&self, split: &str, kind: CoordinationErrorKind) {
            self.0
                .lock()
                .unwrap()
                .commit_outcomes
                .entry(split.to_string())
                .or_default()
                .push_back(kind);
        }

        fn commits(&self) -> Vec<(SplitId, SplitProgress)> {
            self.0.lock().unwrap().commits.clone()
        }

        fn released(&self) -> Vec<SplitId> {
            self.0.lock().unwrap().released.clone()
        }

        fn released_drained(&self) -> Vec<SplitId> {
            self.0.lock().unwrap().released_drained.clone()
        }

        fn declined(&self) -> Vec<SplitId> {
            self.0.lock().unwrap().declined.clone()
        }

        fn fails(&self) -> Vec<(SplitId, String)> {
            self.0.lock().unwrap().fails.clone()
        }
    }

    struct ScriptedCoordinator(Script);

    impl SplitCoordinator for ScriptedCoordinator {
        fn start(&mut self, _planner: Box<dyn SplitPlanner>) -> Result<(), CoordinationError> {
            self.0.0.lock().unwrap().started = true;
            Ok(())
        }

        fn set_waker(&mut self, waker: ControlWaker) {
            self.0.0.lock().unwrap().waker = Some(waker);
        }

        fn poll(&mut self) -> Result<Vec<CoordinationEvent>, CoordinationError> {
            Ok(self
                .0
                .0
                .lock()
                .unwrap()
                .batches
                .pop_front()
                .unwrap_or_default())
        }

        fn commit(
            &mut self,
            split: &SplitId,
            progress: &SplitProgress,
        ) -> Result<(), CoordinationError> {
            let mut s = self.0.0.lock().unwrap();
            if let Some(kinds) = s.commit_outcomes.get_mut(split.as_str())
                && let Some(kind) = kinds.pop_front()
            {
                return Err(CoordinationError::new(kind, "scripted"));
            }
            s.commits.push((split.clone(), progress.clone()));
            Ok(())
        }

        fn fail(&mut self, split: &SplitId, reason: &str) -> Result<(), CoordinationError> {
            self.0
                .0
                .lock()
                .unwrap()
                .fails
                .push((split.clone(), reason.to_string()));
            Ok(())
        }

        fn release(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError> {
            self.0
                .0
                .lock()
                .unwrap()
                .released
                .extend(splits.iter().cloned());
            Ok(())
        }

        fn release_drained(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError> {
            self.0
                .0
                .lock()
                .unwrap()
                .released_drained
                .extend(splits.iter().cloned());
            Ok(())
        }

        fn decline_revoke(&mut self, split: &SplitId) -> Result<(), CoordinationError> {
            self.0.0.lock().unwrap().declined.push(split.clone());
            Ok(())
        }
    }

    struct NoopPlanner;

    impl SplitPlanner for NoopPlanner {
        fn fingerprint(&self) -> String {
            "test:v1".into()
        }

        fn plan(&mut self, _ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
            Ok(SplitPlan::new(vec![], PlanFinality::Final))
        }
    }

    // ------------------------------------------------------------------
    // Stub data plane.

    enum NoBatch {}

    impl<'buf> PayloadBatch<'buf> for NoBatch {
        fn next_payload(&mut self) -> Option<RawPayload<'buf>> {
            match *self {}
        }

        fn ack(&self) -> &AckRef {
            match *self {}
        }
    }

    #[derive(Debug)]
    struct StubLane {
        lane: LaneId,
        partition: PartitionId,
    }

    impl SourceLane for StubLane {
        type Batch<'a> = NoBatch;

        fn id(&self) -> LaneId {
            self.lane
        }

        fn partition(&self) -> PartitionId {
            self.partition
        }

        fn poll(
            &mut self,
            _max: usize,
            _timeout: Duration,
        ) -> Result<Option<NoBatch>, SourceError> {
            Ok(None)
        }
    }

    /// Recording SplitSource: every callback is logged; sweep and
    /// encode_commit outcomes are scripted per split.
    #[derive(Default)]
    struct TestSource {
        opened: Vec<(String, Option<i64>, LaneId, PartitionId, u64)>,
        closed: Vec<String>,
        encoded: Vec<(String, i64)>,
        sweeps: Rc<RefCell<HashMap<String, SplitProgress>>>,
        complete_at: HashMap<String, i64>,
        reject_resume: bool,
        finishing: Vec<String>,
        /// Split ids whose `open_split` fails. Consumed per attempt, so a
        /// retry of the same split succeeds.
        fail_open: Vec<String>,
        /// Split ids for which `begin_revoke` accepts (returns true). Empty
        /// by default, so the double declines like the trait default.
        accept_revoke: HashSet<String>,
        /// Every `begin_revoke` call, in order (accepted or declined).
        begin_revoke_calls: Vec<String>,
        /// Every `drain_ready` call, in order — proves a split did (or did
        /// not) transition to `Draining`.
        drain_ready_calls: Vec<String>,
        /// Scripted `drain_ready` results, sticky per split (returned on
        /// every poll until the tenancy retires), so a retryable final
        /// commit can be re-offered the same tail next poll.
        ready_progress: Rc<RefCell<HashMap<String, SplitProgress>>>,
    }

    impl SplitSource for TestSource {
        type Lane = StubLane;

        fn open_split(&mut self, o: SplitOpening<'_>) -> Result<StubLane, SourceError> {
            let id = o.split.id.as_str().to_string();
            if let Some(i) = self.fail_open.iter().position(|s| *s == id) {
                self.fail_open.remove(i);
                return Err(SourceError::Client {
                    class: ErrorClass::Retryable,
                    reason: format!("open_split failed for {id}"),
                });
            }
            self.opened.push((
                o.split.id.as_str().to_string(),
                o.resume.map(|p| p.watermark),
                o.lane,
                o.partition,
                o.epoch.0,
            ));
            Ok(StubLane {
                lane: o.lane,
                partition: o.partition,
            })
        }

        fn validate_resume(
            &self,
            split: &SplitSpec,
            _progress: &SplitProgress,
        ) -> Result<(), SourceError> {
            if self.reject_resume {
                return Err(SourceError::Client {
                    class: ErrorClass::Fatal,
                    reason: format!("resume drift on {}", split.id),
                });
            }
            Ok(())
        }

        fn encode_commit(
            &mut self,
            split: &SplitId,
            watermark: i64,
        ) -> Result<SplitProgress, SourceError> {
            self.encoded.push((split.as_str().to_string(), watermark));
            let completed = self.complete_at.get(split.as_str()) == Some(&watermark);
            Ok(if completed {
                SplitProgress::completed(watermark, vec![])
            } else {
                SplitProgress::new(watermark, vec![])
            })
        }

        fn sweep(&mut self, split: &SplitId) -> Result<Option<SplitProgress>, SourceError> {
            Ok(self.sweeps.borrow_mut().remove(split.as_str()))
        }

        fn close_split(&mut self, split: &SplitId) {
            self.closed.push(split.as_str().to_string());
        }

        fn take_finishing(&mut self) -> Vec<SplitId> {
            std::mem::take(&mut self.finishing)
                .into_iter()
                .map(|s| SplitId::new(&s).unwrap())
                .collect()
        }

        fn begin_revoke(&mut self, split: &SplitId) -> bool {
            self.begin_revoke_calls.push(split.as_str().to_string());
            self.accept_revoke.contains(split.as_str())
        }

        fn drain_ready(&mut self, split: &SplitId) -> Result<Option<SplitProgress>, SourceError> {
            self.drain_ready_calls.push(split.as_str().to_string());
            Ok(self.ready_progress.borrow().get(split.as_str()).cloned())
        }
    }

    // ------------------------------------------------------------------
    // Helpers.

    fn split(id: &str) -> SplitSpec {
        SplitSpec::new(SplitId::new(id).unwrap(), format!("desc:{id}").into_bytes())
    }

    fn gained(id: &str, epoch: u64, watermark: Option<i64>) -> CoordinationEvent {
        CoordinationEvent::Gained {
            split: split(id),
            epoch: LeaseEpoch(epoch),
            progress: watermark.map(|w| SplitProgress::new(w, vec![])),
        }
    }

    fn driver(script: &Script) -> CoordinationDriver {
        let mut d = CoordinationDriver::new(Box::new(ScriptedCoordinator(script.clone())));
        let ready: SourceEvent<StubLane> = d.start(Box::new(NoopPlanner)).unwrap();
        assert!(
            matches!(ready, SourceEvent::LanesAssigned(ref lanes) if lanes.is_empty()),
            "start must return the empty ready signal"
        );
        d
    }

    fn poll(d: &mut CoordinationDriver, s: &mut TestSource) -> SourceEvent<StubLane> {
        d.poll_events(s, Duration::ZERO).unwrap()
    }

    // ------------------------------------------------------------------
    // Scenarios (each replays a defect class from the PR #34 review).

    #[test]
    fn a_signal_cuts_the_control_plane_park_short() {
        // The driver owns the control-plane wait precisely so that both
        // producers can end it: the backend, and a *lane* deciding
        // end-of-input on a pipeline thread. If a `wake()` call site is
        // ever dropped, the symptom is silent — completions simply wait out
        // an idle timeout again — so assert the park is interruptible
        // rather than trusting the wiring.
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        let park = Duration::from_millis(400);

        // Control: nothing pending and nothing signalling, so the full
        // timeout elapses. Without this the test would pass even if
        // `poll_events` never parked at all.
        let t0 = Instant::now();
        assert!(matches!(
            d.poll_events(&mut s, park).unwrap(),
            SourceEvent::Idle
        ));
        let idle = t0.elapsed();
        assert!(
            idle >= park / 2,
            "expected a real park, returned after {idle:?}"
        );

        // A signal landing mid-park ends it. The event itself surfaces on
        // the following call — the drain runs at the top of `poll_events` —
        // so this asserts the wakeup, not the delivery.
        let signaller = script.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            signaller.push(vec![CoordinationEvent::AllComplete]);
        });
        let t1 = Instant::now();
        let _ = d.poll_events(&mut s, park).unwrap();
        let woken = t1.elapsed();
        handle.join().unwrap();
        assert!(
            woken < park / 2,
            "a signal must cut the park short, but it ran {woken:?} of {park:?}"
        );
        assert!(matches!(
            d.poll_events(&mut s, Duration::ZERO).unwrap(),
            SourceEvent::Drained
        ));
    }

    #[test]
    fn a_failed_open_undoes_the_whole_batch_instead_of_stranding_lanes() {
        // `open_split` failing part way through must not abandon the lanes
        // already built: they never reach the runtime, yet their tenancies
        // would keep a lane id, be skipped by the `lane.is_some()` guard on
        // every later attempt, and hold their leases — heartbeated,
        // unreadable, and a stalled job rather than a failed one.
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource {
            fail_open: vec!["b".into()],
            ..TestSource::default()
        };

        script.push(vec![gained("a", 1, None), gained("b", 1, None)]);
        let err = d
            .poll_events(&mut s, Duration::ZERO)
            .expect_err("the failing open must surface");
        assert!(err.to_string().contains("open_split failed for b"), "{err}");
        assert_eq!(s.opened.len(), 1, "a opened before b failed");

        // The retry re-stages the whole batch and yields both lanes.
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesAdded(lanes) = event else {
            panic!("expected both lanes after the retry, got {event:?}");
        };
        assert_eq!(lanes.len(), 2);
        let reopened: Vec<&str> = s.opened.iter().map(|o| o.0.as_str()).collect();
        assert_eq!(reopened, ["a", "a", "b"], "a re-opens on the retry");
        // The rolled-back ids are burned, never reused.
        assert_eq!(lanes[0].id(), LaneId(2));
        assert_eq!(lanes[1].id(), LaneId(3));
    }

    #[test]
    fn gains_coalesce_into_one_added_batch() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();

        script.push(vec![gained("b", 1, Some(7)), gained("a", 1, None)]);
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesAdded(lanes) = event else {
            panic!("expected added lanes, got {event:?}");
        };
        assert_eq!(lanes.len(), 2);
        // Lane ids minted in gain order; distinct tenancy partitions.
        assert_eq!(s.opened[0].0, "b");
        assert_eq!(s.opened[0].2, LaneId(0));
        assert_eq!(s.opened[0].1, Some(7), "carried progress reaches open");
        assert_eq!(s.opened[1].0, "a");
        assert_eq!(s.opened[1].2, LaneId(1));
        assert_ne!(s.opened[0].3, s.opened[1].3);
        assert_eq!(d.assignments().len(), 2);
    }

    #[test]
    fn a_mid_flow_gain_never_touches_live_lanes_and_their_commits_fold() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let a_partition = s.opened[0].3;

        // Split b arrives while a is live and flowing: strictly additive.
        script.push(vec![gained("b", 1, None)]);
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesAdded(lanes) = event else {
            panic!("expected added lanes, got {event:?}");
        };
        assert_eq!(lanes.len(), 1, "only the new split's lane");
        assert!(
            s.closed.is_empty(),
            "a routine gain must never detach flowing fetchers"
        );

        // The commit window that killed the pipeline pre-fix: a's acked
        // watermark lands right after the gain. It must fold normally.
        d.commit(&mut s, &[(a_partition, 42)]).unwrap();
        assert_eq!(s.encoded, vec![("a".to_string(), 42)]);
        assert_eq!(script.commits().len(), 1);
        assert_eq!(script.commits()[0].0.as_str(), "a");
        // a's lane is the original — never re-minted by the gain.
        assert!(
            d.assignments()
                .contains(&(SplitId::new("a").unwrap(), LaneId(0)))
        );
    }

    #[test]
    fn finishing_splits_surface_as_commit_ready_once() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let a_partition = s.opened[0].3;

        s.finishing.push("a".to_string());
        let event = poll(&mut d, &mut s);
        let SourceEvent::CommitReady { partitions } = event else {
            panic!("expected commit-ready, got {event:?}");
        };
        assert_eq!(partitions, vec![a_partition]);
        // Edge, not level: the hint is consumed.
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));
    }

    #[test]
    fn loss_surfaces_as_partial_revoke_and_detaches_fetchers() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None), gained("b", 1, None)]);
        poll(&mut d, &mut s);

        script.push(vec![CoordinationEvent::Lost {
            split: SplitId::new("a").unwrap(),
        }]);
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesRevoked { lanes, barrier } = event else {
            panic!("expected revoke, got {event:?}");
        };
        assert_eq!(lanes, vec![LaneId(0)]);
        assert_eq!(barrier.remaining(), 1, "one party per revoked lane");
        assert_eq!(s.closed, vec!["a"], "fetcher detached on loss");
        assert_eq!(d.assignments().len(), 1);
    }

    #[test]
    fn late_drain_commit_after_loss_is_skipped() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let partition = s.opened[0].3;

        script.push(vec![CoordinationEvent::Lost {
            split: SplitId::new("a").unwrap(),
        }]);
        poll(&mut d, &mut s);

        // The drain hands back a final watermark for the retired tenancy.
        d.commit(&mut s, &[(partition, 42)]).unwrap();
        assert!(s.encoded.is_empty(), "retired tenancy must not encode");
        assert!(script.commits().is_empty(), "and must not commit");
    }

    #[test]
    fn fenced_commit_quarantines_the_tenancy_and_never_respawns_it() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None), gained("b", 1, None)]);
        poll(&mut d, &mut s);
        let a_partition = s.opened[0].3;
        let b_partition = s.opened[1].3;

        script.fail_next_commit("a", CoordinationErrorKind::Fenced);
        d.commit(&mut s, &[(a_partition, 10), (b_partition, 20)])
            .unwrap();
        // b committed; a wrote nothing and is retired with the fence flag.
        assert_eq!(script.commits().len(), 1);
        assert_eq!(script.commits()[0].0.as_str(), "b");
        assert_eq!(s.closed, vec!["a"]);

        // The fenced lane is revoked...
        let event = poll(&mut d, &mut s);
        assert!(
            matches!(event, SourceEvent::LanesRevoked { ref lanes, .. } if lanes[..] == [LaneId(0)])
        );

        // ...a late watermark for it is skipped...
        s.encoded.clear();
        d.commit(&mut s, &[(a_partition, 11)]).unwrap();
        assert!(s.encoded.is_empty());

        // ...and the mid-cycle Lost that follows the fence is a no-op,
        // while a re-gain (higher epoch) starts a fresh tenancy — added
        // beside b's untouched live lane, never draining it.
        script.push(vec![
            CoordinationEvent::Lost {
                split: SplitId::new("a").unwrap(),
            },
            gained("a", 3, Some(10)),
        ]);
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesAdded(lanes) = event else {
            panic!("expected an added lane for the re-gain, got {event:?}");
        };
        assert_eq!(lanes.len(), 1, "only the fresh tenancy's lane");
        assert_eq!(s.closed, vec!["a"], "b's fetcher was never detached");
        let a_again = s.opened.last().unwrap();
        assert_eq!(a_again.0, "a");
        assert_eq!(a_again.4, 3, "fresh tenancy under the new epoch");
        assert_ne!(a_again.3, a_partition, "fresh partition — no reuse");
        assert_eq!(a_again.2, LaneId(2), "fresh lane id — never reused");
    }

    #[test]
    fn lost_then_regained_in_one_batch_is_a_clean_tenancy_swap() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let first_partition = s.opened[0].3;

        script.push(vec![
            CoordinationEvent::Lost {
                split: SplitId::new("a").unwrap(),
            },
            gained("a", 2, Some(5)),
        ]);
        // Loss first (revoke), then the re-gain's lane is added fresh.
        let event = poll(&mut d, &mut s);
        assert!(matches!(event, SourceEvent::LanesRevoked { .. }));
        let event = poll(&mut d, &mut s);
        assert!(matches!(event, SourceEvent::LanesAdded(ref l) if l.len() == 1));
        let reopened = s.opened.last().unwrap();
        assert_eq!(reopened.4, 2);
        assert_eq!(reopened.1, Some(5), "resume from the carried progress");
        assert_ne!(reopened.3, first_partition);
        assert_eq!(reopened.2, LaneId(1), "lane ids are never recycled");
    }

    #[test]
    fn retryable_commit_defers_and_recommits_idempotently() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let partition = s.opened[0].3;

        script.fail_next_commit("a", CoordinationErrorKind::Retryable);
        d.commit(&mut s, &[(partition, 10)]).unwrap();
        assert!(script.commits().is_empty(), "deferred, not written");

        // Next tick recommits the merged progress and succeeds.
        d.commit(&mut s, &[(partition, 12)]).unwrap();
        assert_eq!(script.commits().len(), 1);
        assert_eq!(script.commits()[0].1.watermark, 12);
    }

    #[test]
    fn completion_sweep_commits_terminal_progress_and_frees_the_lane() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);

        s.sweeps
            .borrow_mut()
            .insert("a".into(), SplitProgress::completed(9, vec![]));
        // The sweep commits terminal progress and retires the lane; being
        // complete (nothing in flight by construction), it leaves
        // barrier-less on this same poll (staged-work fastpath).
        let event = poll(&mut d, &mut s);
        assert!(
            matches!(event, SourceEvent::LanesRetired { ref lanes } if lanes[..] == [LaneId(0)]),
            "completed lanes retire without a drain barrier, got {event:?}"
        );
        assert_eq!(script.commits().len(), 1);
        assert!(script.commits()[0].1.completed);

        // A watermark-carrying commit that completes a split does the same.
        script.push(vec![gained("b", 1, None)]);
        poll(&mut d, &mut s);
        let b_partition = s.opened.last().unwrap().3;
        s.complete_at.insert("b".into(), 20);
        d.commit(&mut s, &[(b_partition, 20)]).unwrap();
        assert!(script.commits().last().unwrap().1.completed);
        let event = poll(&mut d, &mut s);
        assert!(matches!(event, SourceEvent::LanesRetired { .. }));
    }

    #[test]
    fn standby_with_zero_splits_drains_on_all_complete() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();

        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));
        script.push(vec![CoordinationEvent::AllComplete]);
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Drained));
        // Idempotent thereafter.
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Drained));
    }

    #[test]
    fn stalled_is_fatal_by_default_and_drains_when_configured() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![CoordinationEvent::Stalled {
            completed: 7,
            quarantined: 1,
        }]);
        // The stall surfaces on the same call that absorbed the event.
        let err = d.poll_events(&mut s, Duration::ZERO).unwrap_err();
        assert!(err.to_string().contains("quarantined"), "{err}");

        let script = Script::default();
        let mut d = CoordinationDriver::new(Box::new(ScriptedCoordinator(script.clone())))
            .stall_drains(true);
        let _: SourceEvent<StubLane> = d.start(Box::new(NoopPlanner)).unwrap();
        script.push(vec![CoordinationEvent::Stalled {
            completed: 7,
            quarantined: 1,
        }]);
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Drained));
    }

    #[test]
    fn fail_reports_poison_and_retires_the_lane() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);

        d.fail(&mut s, &SplitId::new("a").unwrap(), "undecodable object")
            .unwrap();
        assert_eq!(script.fails().len(), 1);
        assert_eq!(s.closed, vec!["a"]);
        let event = poll(&mut d, &mut s);
        assert!(matches!(event, SourceEvent::LanesRevoked { .. }));
        // Failing a split we no longer hold is a quiet no-op.
        d.fail(&mut s, &SplitId::new("a").unwrap(), "again")
            .unwrap();
        assert_eq!(script.fails().len(), 1);
    }

    #[test]
    fn release_hands_back_every_live_split() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None), gained("b", 1, None)]);
        poll(&mut d, &mut s);

        d.release();
        let released = script.released();
        assert_eq!(released.len(), 2);
        assert!(released.iter().any(|s| s.as_str() == "a"));
        assert!(released.iter().any(|s| s.as_str() == "b"));
    }

    #[test]
    fn resume_validation_rejects_drifted_progress() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource {
            reject_resume: true,
            ..TestSource::default()
        };
        script.push(vec![gained("a", 1, Some(7))]);
        let err = d.poll_events(&mut s, Duration::ZERO).unwrap_err();
        assert!(err.to_string().contains("resume drift"), "{err}");
    }

    // ------------------------------------------------------------------
    // Cooperative revocation: request → drain → commit → release,
    // barrier-less; a fence mid-drain aborts through the existing loss path.

    #[test]
    fn a_drain_keeps_the_tenancy_commit_eligible_until_the_final_commit() {
        // The inverse of `late_drain_commit_after_loss_is_skipped`: a lane
        // put into `Draining` is *still* commit-eligible, so its acked
        // watermarks keep folding to the store right up to the final commit.
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource {
            accept_revoke: HashSet::from(["a".to_string()]),
            ..TestSource::default()
        };
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let partition = s.opened[0].3;

        // The request lands and the source accepts, but the drain is not
        // finished (`drain_ready` returns None), so the tenancy stays.
        script.push(vec![CoordinationEvent::RevokeRequested {
            split: SplitId::new("a").unwrap(),
        }]);
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));
        assert_eq!(
            s.begin_revoke_calls,
            ["a"],
            "the source was asked to stop intake"
        );

        // A tick commit for the handing-off tenancy must still fold.
        d.commit(&mut s, &[(partition, 42)]).unwrap();
        assert_eq!(s.encoded, vec![("a".to_string(), 42)]);
        assert_eq!(script.commits().len(), 1);
        assert_eq!(script.commits()[0].0.as_str(), "a");
        assert!(
            !script.commits()[0].1.completed,
            "a revocation never completes the split"
        );
        assert!(
            script.released_drained().is_empty(),
            "not released while the drain is still in flight"
        );
    }

    #[test]
    fn a_completed_drain_releases_exactly_one_split_and_retires_barrierless() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource {
            accept_revoke: HashSet::from(["a".to_string()]),
            ..TestSource::default()
        };
        script.push(vec![gained("a", 1, None), gained("b", 1, None)]);
        poll(&mut d, &mut s);

        // a's drain has finished — tail acked and committed — so
        // `drain_ready` offers the final (non-terminal) progress.
        s.ready_progress
            .borrow_mut()
            .insert("a".into(), SplitProgress::new(50, vec![]));
        script.push(vec![CoordinationEvent::RevokeRequested {
            split: SplitId::new("a").unwrap(),
        }]);

        // One poll carries the whole grant: accept, chase the tail, final
        // commit, release, and the lane leaves barrier-less.
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesRetired { lanes } = event else {
            panic!("a cooperative revocation must retire barrier-less, got {event:?}");
        };
        assert_eq!(lanes, vec![LaneId(0)], "only a's lane leaves");

        // Exactly the handed-off split, released once, through the revocation
        // path (never the plain-release path).
        assert_eq!(
            script.released_drained(),
            vec![SplitId::new("a").unwrap()],
            "exactly one split, released via the revocation path"
        );
        assert!(script.released().is_empty(), "not a plain hand-back");
        // Its final commit is not a completion.
        let last = script.commits().last().cloned().unwrap();
        assert_eq!(last.0.as_str(), "a");
        assert_eq!(last.1.watermark, 50);
        assert!(!last.1.completed, "drain commits never complete the split");

        // b's live lane is untouched, and no revoke ever follows.
        assert_eq!(s.closed, vec!["a"], "b's fetcher stays attached");
        assert!(
            d.assignments().iter().any(|(id, _)| id.as_str() == "b"),
            "b is still owned"
        );
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));
    }

    #[test]
    fn a_fenced_final_commit_aborts_the_drain_into_a_revoke() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource {
            accept_revoke: HashSet::from(["a".to_string()]),
            ..TestSource::default()
        };
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let partition = s.opened[0].3;

        // The drain is ready, but a peer fenced this tenancy first,
        // so its final commit is rejected.
        s.ready_progress
            .borrow_mut()
            .insert("a".into(), SplitProgress::new(50, vec![]));
        script.fail_next_commit("a", CoordinationErrorKind::Fenced);
        script.push(vec![CoordinationEvent::RevokeRequested {
            split: SplitId::new("a").unwrap(),
        }]);

        // A fenced final commit aborts through the loss path: a *revoke*
        // (with a drain barrier), never a barrier-less retire, and no
        // release.
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesRevoked { lanes, barrier } = event else {
            panic!("a fenced drain must abort into a revoke, got {event:?}");
        };
        assert_eq!(lanes, vec![LaneId(0)]);
        assert_eq!(barrier.remaining(), 1, "one party per revoked lane");
        assert!(
            script.released_drained().is_empty(),
            "a fenced drain never releases"
        );
        assert!(
            script.commits().is_empty(),
            "the fenced final commit wrote nothing"
        );
        assert_eq!(s.closed, vec!["a"], "the fetcher was detached on the abort");

        // A late watermark for the now-retired-fenced tenancy is skipped.
        d.commit(&mut s, &[(partition, 60)]).unwrap();
        assert!(
            s.encoded.is_empty(),
            "a retired-fenced tenancy must not encode"
        );
    }

    #[test]
    fn a_retryable_final_commit_keeps_the_drain_pending() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource {
            accept_revoke: HashSet::from(["a".to_string()]),
            ..TestSource::default()
        };
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);

        // The drain is ready, but the store defers the first final commit.
        s.ready_progress
            .borrow_mut()
            .insert("a".into(), SplitProgress::new(50, vec![]));
        script.fail_next_commit("a", CoordinationErrorKind::Retryable);
        script.push(vec![CoordinationEvent::RevokeRequested {
            split: SplitId::new("a").unwrap(),
        }]);

        // Deferred: nothing written, nothing released, the split stays owned
        // and handing off.
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));
        assert!(script.commits().is_empty(), "deferred, not written");
        assert!(script.released_drained().is_empty());
        assert!(
            d.assignments().iter().any(|(id, _)| id.as_str() == "a"),
            "still owned while the final commit retries"
        );

        // The next poll re-offers the same tail and the commit lands:
        // released and retired barrier-less.
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesRetired { lanes } = event else {
            panic!("the retried drain must finally retire, got {event:?}");
        };
        assert_eq!(lanes, vec![LaneId(0)]);
        assert_eq!(script.commits().len(), 1);
        assert_eq!(script.commits()[0].1.watermark, 50);
        assert_eq!(script.released_drained(), vec![SplitId::new("a").unwrap()]);
    }

    #[test]
    fn a_source_that_cannot_stop_intake_declines_the_revoke() {
        let script = Script::default();
        let mut d = driver(&script);
        // Default `accept_revoke` is empty, so `begin_revoke` declines.
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let partition = s.opened[0].3;

        script.push(vec![CoordinationEvent::RevokeRequested {
            split: SplitId::new("a").unwrap(),
        }]);
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));

        // Asked and declined: the tenancy never enters the drain (no
        // `drain_ready` poll) and stays fully live.
        assert_eq!(s.begin_revoke_calls, ["a"]);
        assert!(
            s.drain_ready_calls.is_empty(),
            "a declined split never drains"
        );
        assert!(script.released_drained().is_empty());

        // A live tenancy keeps committing as normal.
        d.commit(&mut s, &[(partition, 30)]).unwrap();
        assert_eq!(s.encoded, vec![("a".to_string(), 30)]);
        assert_eq!(script.commits().len(), 1);
    }

    #[test]
    fn a_revoke_request_for_an_unheld_split_is_ignored() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource {
            // Even a source that *would* accept is never consulted for a
            // split this instance does not hold.
            accept_revoke: HashSet::from(["a".to_string(), "ghost".to_string()]),
            ..TestSource::default()
        };
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);

        script.push(vec![CoordinationEvent::RevokeRequested {
            split: SplitId::new("ghost").unwrap(),
        }]);
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));
        assert!(
            s.begin_revoke_calls.is_empty(),
            "an unheld split must not consult the source"
        );
        assert!(
            d.assignments().iter().any(|(id, _)| id.as_str() == "a"),
            "the held split is untouched"
        );
    }

    #[test]
    fn a_source_that_declines_feeds_the_decline_back() {
        // `a_source_that_cannot_stop_intake_declines_the_revoke` proves the
        // tenancy stays live; this one proves the refusal is fed BACK to the
        // backend, which forces the release immediately instead of waiting
        // out its drain deadline.
        let script = Script::default();
        let mut d = driver(&script);
        // Default `accept_revoke` is empty, so `begin_revoke` declines.
        let mut s = TestSource::default();
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let partition = s.opened[0].3;

        script.push(vec![CoordinationEvent::RevokeRequested {
            split: SplitId::new("a").unwrap(),
        }]);
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));

        // Asked, refused, and the refusal handed back to the backend exactly
        // once — naming the split, so the backend cools down that split only.
        assert_eq!(s.begin_revoke_calls, ["a"]);
        assert_eq!(
            script.declined(),
            vec![SplitId::new("a").unwrap()],
            "the source's refusal must reach the backend, once"
        );

        // The tenancy stayed Live: commits still flow.
        d.commit(&mut s, &[(partition, 30)]).unwrap();
        assert_eq!(s.encoded, vec![("a".to_string(), 30)]);
        assert_eq!(script.commits().len(), 1);
        assert_eq!(script.commits()[0].0.as_str(), "a");
    }

    #[test]
    fn an_unopened_tenancy_declines_without_asking_the_source() {
        // A gain and a revocation request for the same split arrive in one event
        // batch: the tenancy exists but its lane has not opened yet. Accepting
        // it would strand it in `Draining` forever (no intake to stop, no
        // drain to finish), so the driver declines WITHOUT consulting the
        // source, and the split still opens normally afterwards.
        let script = Script::default();
        let mut d = driver(&script);
        // Even a source that WOULD accept must not be consulted before the
        // lane exists.
        let mut s = TestSource {
            accept_revoke: HashSet::from(["a".to_string()]),
            ..TestSource::default()
        };

        script.push(vec![
            gained("a", 1, None),
            CoordinationEvent::RevokeRequested {
                split: SplitId::new("a").unwrap(),
            },
        ]);
        // Both events are applied (decline included) and then the lane opens,
        // all on this one call.
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesAdded(lanes) = event else {
            panic!("the split must still open after the early decline, got {event:?}");
        };
        assert_eq!(lanes.len(), 1);

        assert!(
            s.begin_revoke_calls.is_empty(),
            "a not-yet-opened tenancy must never be asked to stop intake"
        );
        assert_eq!(
            script.declined(),
            vec![SplitId::new("a").unwrap()],
            "the premature request is declined back to the backend"
        );

        // The split is fully live now: commits flow.
        let partition = s.opened[0].3;
        d.commit(&mut s, &[(partition, 25)]).unwrap();
        assert_eq!(s.encoded, vec![("a".to_string(), 25)]);
        assert_eq!(script.commits().len(), 1);
        // And it was never actually handed off.
        assert!(script.released_drained().is_empty());
    }

    #[test]
    fn a_completed_progress_during_a_drain_is_never_terminal() {
        // A handing-off split's drain cut can look terminal to the source
        // (every record it emitted is acked), but committing it
        // `completed: true` would mark a half-read split permanently done and
        // its next owner would never resume it. The central guard must strip
        // the flag — while still landing the commit (the watermark is acked).
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource {
            accept_revoke: HashSet::from(["a".to_string()]),
            ..TestSource::default()
        };
        script.push(vec![gained("a", 1, None)]);
        poll(&mut d, &mut s);
        let partition = s.opened[0].3;

        // Drive "a" into `Draining` (the drain is not finished yet:
        // `drain_ready` returns None).
        script.push(vec![CoordinationEvent::RevokeRequested {
            split: SplitId::new("a").unwrap(),
        }]);
        assert!(matches!(poll(&mut d, &mut s), SourceEvent::Idle));
        assert_eq!(s.begin_revoke_calls, ["a"]);

        // A tick commit whose `encode_commit` reports the split COMPLETE at
        // this watermark.
        s.complete_at.insert("a".into(), 42);
        d.commit(&mut s, &[(partition, 42)]).unwrap();

        // The commit landed (watermark advanced) but stripped of completion.
        let committed = script.commits().last().cloned().expect("the commit landed");
        assert_eq!(committed.0.as_str(), "a");
        assert_eq!(
            committed.1.watermark, 42,
            "the guard strips the flag, not the commit"
        );
        assert!(
            !committed.1.completed,
            "a drain cut is never terminal, whatever the source claims"
        );

        // The tenancy is neither completed nor retired: still owned, still
        // handing off, no lane has left.
        assert!(
            d.assignments().iter().any(|(id, _)| id.as_str() == "a"),
            "still owned"
        );
        assert!(s.closed.is_empty(), "not retired");

        // The revocation then finishes normally once the drain completes.
        s.ready_progress
            .borrow_mut()
            .insert("a".into(), SplitProgress::new(50, vec![]));
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesRetired { lanes } = event else {
            panic!("the drained revocation must finally retire, got {event:?}");
        };
        assert_eq!(lanes, vec![LaneId(0)]);
        assert_eq!(script.released_drained(), vec![SplitId::new("a").unwrap()]);
        assert!(
            !script.commits().last().unwrap().1.completed,
            "the final revocation commit is not terminal either"
        );
    }
}
