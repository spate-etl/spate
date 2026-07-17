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
//! reused [`PartitionId`]. Lane ids are dense per assignment cycle
//! (re-numbered on every [`SourceEvent::LanesAssigned`]); partition ids
//! are stable for the life of the tenancy. Watermarks arrive keyed by
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
//! 2. Staged gains → [`SourceEvent::LanesAssigned`] with the **full** live
//!    set (the controller defensively drains live lanes on assignment, so
//!    retained splits are re-opened from their resume cache).
//! 3. Otherwise poll the coordinator (the idle wait delegates there),
//!    fold its events into the tenancy table, and sweep for completions.
//! 4. [`CoordinationEvent::AllComplete`] → [`SourceEvent::Drained`];
//!    [`CoordinationEvent::Stalled`] → a fatal error by default
//!    (see [`stall_drains`](CoordinationDriver::stall_drains)).

use super::{
    CoordinationError, CoordinationErrorKind, CoordinationEvent, LeaseEpoch, SplitCoordinator,
    SplitId, SplitPlanner, SplitProgress, SplitSpec,
};
use crate::error::{ErrorClass, SourceError};
use crate::record::PartitionId;
use crate::source::{DrainBarrier, LaneId, SourceEvent, SourceLane};
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Everything the driver hands a source when a split's lane must be
/// (re-)materialized: on first gain and again on every reassignment cycle
/// while the split stays owned.
#[derive(Debug)]
#[non_exhaustive]
pub struct SplitOpening<'a> {
    /// The split to read.
    pub split: &'a SplitSpec,
    /// Authoritative progress to resume from (already validated via
    /// [`SplitSource::validate_resume`]); `None` for a fresh split.
    pub resume: Option<&'a SplitProgress>,
    /// Dense lane id for this assignment cycle.
    pub lane: LaneId,
    /// Stable partition id for this tenancy — the key under which this
    /// split's watermarks come back to [`CoordinationDriver::commit`].
    pub partition: PartitionId,
    /// Fencing token of the current tenancy.
    pub epoch: LeaseEpoch,
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
    fn close_split(&mut self, split: &SplitId);
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum TenancyState {
    /// Owned; lane live or awaiting the next assignment cycle.
    Live,
    /// Ownership over (lost, fenced, failed, completed); entry retained
    /// only to absorb late watermarks until the next assignment cycle.
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
}

/// Source-side coordination choreography, embedded by a coordinated
/// source; see the [module docs](self) for the protocol it implements.
pub struct CoordinationDriver {
    coordinator: Box<dyn SplitCoordinator>,
    tenancies: BTreeMap<PartitionId, Tenancy>,
    by_split: BTreeMap<SplitId, PartitionId>,
    /// Lanes whose loss must still surface as a partial revoke.
    pending_lost: Vec<LaneId>,
    /// The live set changed; the full assignment must be re-emitted.
    reassign: bool,
    all_complete: bool,
    stalled: Option<(u64, u64)>,
    stall_drains: bool,
    started: bool,
    next_partition: u32,
}

impl fmt::Debug for CoordinationDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinationDriver")
            .field("tenancies", &self.tenancies.len())
            .field("live", &self.by_split.len())
            .field("pending_lost", &self.pending_lost.len())
            .field("reassign", &self.reassign)
            .field("all_complete", &self.all_complete)
            .field("stalled", &self.stalled)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

impl CoordinationDriver {
    /// Wrap a coordinator handle.
    #[must_use]
    pub fn new(coordinator: Box<dyn SplitCoordinator>) -> CoordinationDriver {
        CoordinationDriver {
            coordinator,
            tenancies: BTreeMap::new(),
            by_split: BTreeMap::new(),
            pending_lost: Vec::new(),
            reassign: false,
            all_complete: false,
            stalled: None,
            stall_drains: false,
            started: false,
            next_partition: 0,
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

        // 1. Losses first: stop lost lanes before anything else runs.
        if !self.pending_lost.is_empty() {
            let lanes = std::mem::take(&mut self.pending_lost);
            let barrier = DrainBarrier::new(lanes.len());
            return Ok(SourceEvent::LanesRevoked { lanes, barrier });
        }

        // 2. Staged gains, as the controller contract's two-step: revoke
        // every live lane first (their tenancies stay — they respawn from
        // the resume cache), then emit the full new assignment.
        if self.reassign {
            let live: Vec<LaneId> = self
                .tenancies
                .values_mut()
                .filter(|t| t.state == TenancyState::Live)
                .filter_map(|t| t.lane.take())
                .collect();
            if !live.is_empty() {
                // Detach fetchers now: the pipeline threads are about to
                // drain the old lane objects.
                let retained: Vec<SplitId> = self
                    .tenancies
                    .values()
                    .filter(|t| t.state == TenancyState::Live)
                    .map(|t| t.split.id.clone())
                    .collect();
                for split in retained {
                    source.close_split(&split);
                }
                let barrier = DrainBarrier::new(live.len());
                return Ok(SourceEvent::LanesRevoked {
                    lanes: live,
                    barrier,
                });
            }
            self.reassign = false;
            return self.materialize(source).map(SourceEvent::LanesAssigned);
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

        // 4. Poll the coordinator; the source's idle wait delegates here.
        let events = self.coordinator.poll(timeout).map_err(as_source_error)?;
        for event in events {
            self.apply(source, event)?;
        }

        // 5. Completion sweep over live, uncommitted-terminal tenancies.
        self.sweep(source)?;

        if !self.pending_lost.is_empty()
            || self.reassign
            || self.all_complete
            || self.stalled.is_some()
        {
            // Something is staged; surface it on this same call (bounded
            // recursion — every staged branch above returns).
            return self.poll_events(source, Duration::ZERO);
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
                // Pruned tenancy: a drain commit that lost the race with
                // reassignment. Its data replays under the new owner.
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
                    },
                );
                self.reassign = true;
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
            self.pending_lost.push(lane);
        }
        source.close_split(&split);
    }

    /// Emit the full live assignment: prune retired tenancies (nothing
    /// stale can arrive for them once the controller re-epochs), then
    /// re-open every live split with dense lane ids ordered by split id.
    fn materialize<S: SplitSource>(&mut self, source: &mut S) -> Result<Vec<S::Lane>, SourceError> {
        self.tenancies.retain(|_, t| t.state == TenancyState::Live);

        // Detach any still-attached fetchers: the controller is about to
        // drain the old lane objects; the re-opened splits get fresh ones.
        let respawn: Vec<PartitionId> = self
            .tenancies
            .iter()
            .filter(|(_, t)| t.lane.is_some())
            .map(|(&p, _)| p)
            .collect();
        for partition in respawn {
            let split = self.tenancies[&partition].split.id.clone();
            source.close_split(&split);
            if let Some(t) = self.tenancies.get_mut(&partition) {
                t.lane = None;
            }
        }

        let mut order: Vec<(SplitId, PartitionId)> =
            self.by_split.iter().map(|(s, &p)| (s.clone(), p)).collect();
        order.sort();

        let mut lanes = Vec::with_capacity(order.len());
        for (index, (_, partition)) in order.iter().enumerate() {
            let lane_id = LaneId(u32::try_from(index).expect("lane count fits u32"));
            let tenancy = self.tenancies.get_mut(partition).expect("live tenancy");
            tenancy.lane = Some(lane_id);
            let opening = SplitOpening {
                split: &tenancy.split,
                resume: tenancy.progress.as_ref(),
                lane: lane_id,
                partition: *partition,
                epoch: tenancy.epoch,
            };
            lanes.push(source.open_split(opening)?);
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

    /// Shared fenced-commit triage for tick commits and sweep commits.
    fn commit_progress<S: SplitSource>(
        &mut self,
        source: &mut S,
        partition: PartitionId,
        split: &SplitId,
        progress: SplitProgress,
    ) -> Result<(), SourceError> {
        match self.coordinator.commit(split, &progress) {
            Ok(()) => {
                let tenancy = self.tenancies.get_mut(&partition).expect("live tenancy");
                let completed = progress.completed;
                tenancy.progress = Some(progress);
                if completed {
                    tenancy.completed = true;
                    // A completed split frees its lane for the working set.
                    self.retire(source, partition, false);
                }
                Ok(())
            }
            Err(e) if e.kind == CoordinationErrorKind::Fenced => {
                // Nothing was written; the split belongs to a peer. Retire
                // with the fence flag so nothing of this tenancy is ever
                // folded or respawned (the matching Lost event may still
                // arrive and finds the tenancy already retired).
                tracing::warn!(split = %split, "commit fenced; split lost to a peer");
                self.retire(source, partition, true);
                Ok(())
            }
            Err(e) if e.kind == CoordinationErrorKind::Retryable => {
                // Previous durable state stays authoritative; the merged
                // progress recommits on the next tick. The resume cache
                // still advances: the watermark is acked (sink-durable),
                // so respawning past it cannot lose data.
                tracing::warn!(split = %split, error = %e, "commit deferred; will retry");
                let tenancy = self.tenancies.get_mut(&partition).expect("live tenancy");
                tenancy.progress = Some(progress);
                Ok(())
            }
            Err(e) => Err(as_source_error(e)),
        }
    }
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
    use std::collections::{HashMap, VecDeque};
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    // ------------------------------------------------------------------
    // Scripted coordinator double (the shape etl-test later publishes).

    #[derive(Default)]
    struct ScriptState {
        batches: VecDeque<Vec<CoordinationEvent>>,
        commit_outcomes: HashMap<String, VecDeque<CoordinationErrorKind>>,
        commits: Vec<(SplitId, SplitProgress)>,
        fails: Vec<(SplitId, String)>,
        released: Vec<SplitId>,
        started: bool,
    }

    #[derive(Clone, Default)]
    struct Script(Arc<Mutex<ScriptState>>);

    impl Script {
        fn push(&self, events: Vec<CoordinationEvent>) {
            self.0.lock().unwrap().batches.push_back(events);
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

        fn poll(
            &mut self,
            _timeout: Duration,
        ) -> Result<Vec<CoordinationEvent>, CoordinationError> {
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
    }

    impl SplitSource for TestSource {
        type Lane = StubLane;

        fn open_split(&mut self, o: SplitOpening<'_>) -> Result<StubLane, SourceError> {
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
    fn gains_coalesce_into_one_dense_assignment() {
        let script = Script::default();
        let mut d = driver(&script);
        let mut s = TestSource::default();

        script.push(vec![gained("b", 1, Some(7)), gained("a", 1, None)]);
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesAssigned(lanes) = event else {
            panic!("expected assignment, got {event:?}");
        };
        assert_eq!(lanes.len(), 2);
        // Dense lane ids ordered by split id; distinct tenancy partitions.
        assert_eq!(s.opened[0].0, "a");
        assert_eq!(s.opened[0].2, LaneId(0));
        assert_eq!(s.opened[1].0, "b");
        assert_eq!(s.opened[1].2, LaneId(1));
        assert_eq!(s.opened[1].1, Some(7), "carried progress reaches open");
        assert_ne!(s.opened[0].3, s.opened[1].3);
        assert_eq!(d.assignments().len(), 2);
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
        // while a re-gain (higher epoch) starts a fresh tenancy — via the
        // contract's two-step: revoke the still-live lane (b), then the
        // full assignment.
        script.push(vec![
            CoordinationEvent::Lost {
                split: SplitId::new("a").unwrap(),
            },
            gained("a", 3, Some(10)),
        ]);
        let event = poll(&mut d, &mut s);
        assert!(
            matches!(event, SourceEvent::LanesRevoked { ref lanes, .. } if lanes.len() == 1),
            "live lanes are revoked before the reassignment, got {event:?}"
        );
        let event = poll(&mut d, &mut s);
        let SourceEvent::LanesAssigned(lanes) = event else {
            panic!("expected reassignment, got {event:?}");
        };
        assert_eq!(lanes.len(), 2);
        let reopened = s.opened.last().unwrap();
        assert_eq!(reopened.0, "b", "sorted order: b re-opened last has lane 1");
        let a_again = &s.opened[s.opened.len() - 2];
        assert_eq!(a_again.0, "a");
        assert_eq!(a_again.4, 3, "fresh tenancy under the new epoch");
        assert_ne!(a_again.3, a_partition, "fresh partition — no reuse");
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
        // Loss first (revoke), then the re-gain materializes.
        let event = poll(&mut d, &mut s);
        assert!(matches!(event, SourceEvent::LanesRevoked { .. }));
        let event = poll(&mut d, &mut s);
        assert!(matches!(event, SourceEvent::LanesAssigned(ref l) if l.len() == 1));
        let reopened = s.opened.last().unwrap();
        assert_eq!(reopened.4, 2);
        assert_eq!(reopened.1, Some(5), "resume from the carried progress");
        assert_ne!(reopened.3, first_partition);
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
        // The sweep commits terminal progress and retires the lane; the
        // revoke surfaces on this same poll (staged-work fastpath).
        let event = poll(&mut d, &mut s);
        assert!(
            matches!(event, SourceEvent::LanesRevoked { ref lanes, .. } if lanes[..] == [LaneId(0)])
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
        assert!(matches!(event, SourceEvent::LanesRevoked { .. }));
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
}
