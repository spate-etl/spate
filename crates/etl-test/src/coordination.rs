//! Scripted coordination for testing coordinated sources.
//!
//! [`scripted_coordinator`] pairs a [`SplitCoordinator`] implementation
//! with a [`CoordinatorScript`] handle (the `tower-test` philosophy used
//! throughout this crate): the test scripts ownership events and commit
//! outcomes, the source under test runs its real driver choreography, and
//! the script observes every commit, failure report, and release — no
//! store, no clock, fully deterministic.

use etl_core::coordination::ControlWaker;
use etl_core::coordination::{
    CoordinationError, CoordinationErrorKind, CoordinationEvent, LeaseEpoch, SplitCoordinator,
    SplitId, SplitPlanner, SplitProgress, SplitSpec,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    events: Vec<CoordinationEvent>,
    commit_outcomes: HashMap<SplitId, VecDeque<CoordinationErrorKind>>,
    commits: Vec<(SplitId, SplitProgress)>,
    failed: Vec<(SplitId, String)>,
    released: Vec<SplitId>,
    planner: Option<Box<dyn SplitPlanner>>,
    started: bool,
    waker: Option<ControlWaker>,
}

/// A [`SplitCoordinator`] whose events and outcomes are scripted by the
/// paired [`CoordinatorScript`]. Build both with [`scripted_coordinator`].
#[derive(Debug)]
pub struct ScriptedCoordinator {
    state: Arc<Mutex<State>>,
}

/// Scripting and observation handle for a [`ScriptedCoordinator`].
///
/// Events queued between two `poll` calls are delivered as **one batch**,
/// matching the trait contract ("all pending events at once") — queue a
/// [`lose`](CoordinatorScript::lose) and a [`gain`](CoordinatorScript::gain)
/// back to back to exercise same-batch interleavings.
#[derive(Clone, Debug)]
pub struct CoordinatorScript {
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("pending_events", &self.events.len())
            .field("commits", &self.commits.len())
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

/// A scripted coordinator and its handle.
#[must_use]
pub fn scripted_coordinator() -> (ScriptedCoordinator, CoordinatorScript) {
    let state = Arc::new(Mutex::new(State::default()));
    (
        ScriptedCoordinator {
            state: Arc::clone(&state),
        },
        CoordinatorScript { state },
    )
}

impl CoordinatorScript {
    /// Queue a [`CoordinationEvent::Gained`] for the next poll.
    pub fn gain(&self, split: SplitSpec, epoch: u64, progress: Option<SplitProgress>) {
        self.lock().events.push(CoordinationEvent::Gained {
            split,
            epoch: LeaseEpoch(epoch),
            progress,
        });
        self.wake();
    }

    /// Queue a [`CoordinationEvent::Lost`] for the next poll.
    pub fn lose(&self, split: &SplitId) {
        self.lock().events.push(CoordinationEvent::Lost {
            split: split.clone(),
        });
        self.wake();
    }

    /// Queue a [`CoordinationEvent::Quarantined`] for the next poll.
    pub fn quarantine(&self, split: &SplitId, attempts: u32) {
        self.lock().events.push(CoordinationEvent::Quarantined {
            split: split.clone(),
            attempts,
        });
        self.wake();
    }

    /// Queue [`CoordinationEvent::AllComplete`] for the next poll.
    pub fn all_complete(&self) {
        self.lock().events.push(CoordinationEvent::AllComplete);
        self.wake();
    }

    /// Queue [`CoordinationEvent::Stalled`] for the next poll.
    pub fn stalled(&self, completed: u64, quarantined: u64) {
        self.lock().events.push(CoordinationEvent::Stalled {
            completed,
            quarantined,
        });
        self.wake();
    }

    /// Script the outcome of the next `commit` for `split` (repeat to
    /// script a sequence). Unscripted commits succeed and are recorded.
    pub fn fail_next_commit(&self, split: &SplitId, kind: CoordinationErrorKind) {
        self.lock()
            .commit_outcomes
            .entry(split.clone())
            .or_default()
            .push_back(kind);
    }

    /// Every successful commit so far, in order.
    #[must_use]
    pub fn commits(&self) -> Vec<(SplitId, SplitProgress)> {
        self.lock().commits.clone()
    }

    /// The last successful commit for `split`, if any.
    #[must_use]
    pub fn last_commit(&self, split: &SplitId) -> Option<SplitProgress> {
        self.lock()
            .commits
            .iter()
            .rev()
            .find(|(s, _)| s == split)
            .map(|(_, p)| p.clone())
    }

    /// Every `fail` report so far, in order.
    #[must_use]
    pub fn failed(&self) -> Vec<(SplitId, String)> {
        self.lock().failed.clone()
    }

    /// Every released split so far, in release order.
    #[must_use]
    pub fn released(&self) -> Vec<SplitId> {
        self.lock().released.clone()
    }

    /// Whether `start` ran.
    #[must_use]
    pub fn started(&self) -> bool {
        self.lock().started
    }

    /// Take the planner captured at `start`: run it directly to inspect
    /// its plan, then feed the resulting splits back through
    /// [`gain`](CoordinatorScript::gain) — this is how a source tests its
    /// planner and its driver choreography together.
    #[must_use]
    pub fn take_planner(&self) -> Option<Box<dyn SplitPlanner>> {
        self.lock().planner.take()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("coordinator script poisoned")
    }

    /// Wake the driver's control-plane park so a scripted event is picked
    /// up immediately instead of waiting out the caller's poll timeout.
    fn wake(&self) {
        if let Some(w) = &self.lock().waker {
            w.wake();
        }
    }
}

impl SplitCoordinator for ScriptedCoordinator {
    fn start(&mut self, planner: Box<dyn SplitPlanner>) -> Result<(), CoordinationError> {
        let mut state = self.state.lock().expect("coordinator script poisoned");
        state.planner = Some(planner);
        state.started = true;
        Ok(())
    }

    fn set_waker(&mut self, waker: ControlWaker) {
        self.state
            .lock()
            .expect("coordinator script poisoned")
            .waker = Some(waker);
    }

    fn poll(&mut self) -> Result<Vec<CoordinationEvent>, CoordinationError> {
        // Deterministic and non-blocking: queued events are one batch;
        // an empty queue returns immediately rather than waiting out the
        // timeout, so test loops never stall.
        Ok(std::mem::take(
            &mut self
                .state
                .lock()
                .expect("coordinator script poisoned")
                .events,
        ))
    }

    fn commit(
        &mut self,
        split: &SplitId,
        progress: &SplitProgress,
    ) -> Result<(), CoordinationError> {
        let mut state = self.state.lock().expect("coordinator script poisoned");
        if let Some(kinds) = state.commit_outcomes.get_mut(split)
            && let Some(kind) = kinds.pop_front()
        {
            return Err(CoordinationError::new(
                kind,
                format!("scripted {kind:?} for split {split}"),
            ));
        }
        state.commits.push((split.clone(), progress.clone()));
        Ok(())
    }

    fn fail(&mut self, split: &SplitId, reason: &str) -> Result<(), CoordinationError> {
        self.state
            .lock()
            .expect("coordinator script poisoned")
            .failed
            .push((split.clone(), reason.to_string()));
        Ok(())
    }

    fn release(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError> {
        self.state
            .lock()
            .expect("coordinator script poisoned")
            .released
            .extend(splits.iter().cloned());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_core::coordination::{PlanContext, PlanFinality, SplitPlan};

    struct OneSplit;

    impl SplitPlanner for OneSplit {
        fn fingerprint(&self) -> String {
            "test:v1".into()
        }

        fn plan(&mut self, _ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
            Ok(SplitPlan::new(
                vec![etl_core::coordination::PlannedSplit::new(SplitSpec::new(
                    SplitId::new("only").unwrap(),
                    b"all of it".to_vec(),
                ))],
                PlanFinality::Final,
            ))
        }
    }

    #[test]
    fn scripts_events_and_observes_interactions() {
        let (mut coordinator, script) = scripted_coordinator();
        coordinator.start(Box::new(OneSplit)).unwrap();
        assert!(script.started());

        // The captured planner is runnable by the test.
        let mut planner = script.take_planner().expect("planner captured");
        let plan = planner.plan(PlanContext::new(None, 1)).unwrap();
        assert_eq!(plan.splits.len(), 1);
        let split = plan.splits[0].spec.clone();
        let id = split.id.clone();

        // lose+gain queued together arrive as one batch, in order.
        script.gain(split, 1, None);
        script.lose(&id);
        let batch = coordinator.poll().unwrap();
        assert_eq!(batch.len(), 2);
        assert!(matches!(batch[0], CoordinationEvent::Gained { .. }));
        assert!(matches!(batch[1], CoordinationEvent::Lost { .. }));
        assert!(coordinator.poll().unwrap().is_empty());

        // Scripted commit outcomes drain in order, then commits succeed.
        script.fail_next_commit(&id, CoordinationErrorKind::Retryable);
        let progress = SplitProgress::new(5, vec![]);
        let err = coordinator.commit(&id, &progress).unwrap_err();
        assert_eq!(err.kind, CoordinationErrorKind::Retryable);
        coordinator.commit(&id, &progress).unwrap();
        assert_eq!(script.commits().len(), 1);
        assert_eq!(script.last_commit(&id).unwrap().watermark, 5);

        coordinator.fail(&id, "poison").unwrap();
        assert_eq!(script.failed()[0].1, "poison");
        coordinator.release(std::slice::from_ref(&id)).unwrap();
        assert_eq!(script.released(), vec![id]);
    }
}
