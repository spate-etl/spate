//! Leader election and planning: whoever holds the leadership lease runs
//! the source's planner; the plan record's CAS revision is the fence that
//! makes a deposed leader harmless.
//!
//! Election is a lease like any other: `create` the well-known leader key
//! (expired keys are re-creatable), heartbeat it, lose it by fencing or
//! expiry. The winner immediately bumps the plan record's `generation`
//! via CAS. From that point, any in-flight plan write from a previous
//! leader loses by revision. Zombie *split* creates need no fence at all:
//! deterministic ids + create-if-absent make a stale leader's writes
//! byte-equivalent to the live leader's, or losers of the create race.
//!
//! The planner itself runs on the blocking pool and is joined by a select
//! arm in the task loop ([`Task::maybe_start_plan`] starts it,
//! [`Task::finish_plan`] lands it). A slow enumeration must never stall
//! heartbeats, watch processing, or command service.
//!
//! On a store whose watch is polled, a new leader lists every split and
//! spec record before it plans or publishes, so its first assignment covers
//! splits its watch never delivered.

use crate::error::{fatal_only, store_error};
use crate::records::{self, LeaderVal, PlanFinalityRepr, SplitProgressRecord, SplitSpecRecord};
use crate::store::{CasOutcome, CoordinationStore, Keyspace, Revision, StoreError};
use crate::task::Task;
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use spate_core::clock::tokio::Clock;
use spate_core::coordination::{
    CoordinationError, CoordinationErrorKind, PlanContext, PlanFinality, SplitId, SplitPlan,
    SplitPlanner,
};
use spate_core::metrics::ReplanOutcome;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Most splits a plan run seeds concurrently.
const SEED_CONCURRENCY: usize = 64;

/// What the blocking-pool planner call returns: the planner handed back,
/// plus its enumeration result.
pub(crate) type PlannerOutput = (Box<dyn SplitPlanner>, Result<SplitPlan, CoordinationError>);

/// A planner run in flight on the blocking pool, plus the plan-record
/// snapshot its publish will CAS against (anything that moved the record
/// meanwhile, such as a successor's generation bump, makes the publish lose).
pub(crate) struct PlanRun {
    pub(crate) handle: tokio::task::JoinHandle<PlannerOutput>,
    plan: records::PlanRecord,
    plan_rev: Revision,
    generation: u64,
    started: Instant,
}

impl<S: CoordinationStore + Clone> Task<S> {
    /// Race for the leadership lease; the winner schedules a plan run.
    pub(crate) async fn try_elect(&mut self) -> Result<(), CoordinationError> {
        let generation = self.plan.as_ref().map_or(0, |(p, _)| p.generation) + 1;
        let val = records::encode_val(&LeaderVal {
            schema: records::SCHEMA,
            owner: self.instance.clone(),
            nonce: self.nonce.clone(),
            generation,
        });
        match self
            .store
            .create(Keyspace::Ephemeral, records::LEADER_KEY, val)
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                tracing::info!(generation, "elected planner leader");
                self.leadership = Some(rev);
                self.metrics(|m| m.set_leader(true));
                // A fresh leader has published nothing yet, whatever this
                // process's assignment bookkeeping happens to say.
                self.mark_assignment_dirty();
                if self.polled.is_some() {
                    // The watch may never have delivered records written
                    // before this worker led: list them before planning or
                    // publishing.
                    self.caught_up = false;
                    self.catch_up_due = true;
                }
                self.bump_generation(generation).await?;
                Ok(())
            }
            Ok(CasOutcome::Lost) => Ok(()), // someone else won; watch will show them
            Err(e) => {
                tracing::warn!(error = %e, "election write failed; retrying on observation");
                fatal_only("writing the leader key", &e)
            }
        }
    }

    /// The planner fence: CAS the plan record to the new generation. A
    /// deposed predecessor's pending plan CAS now loses by revision.
    async fn bump_generation(&mut self, generation: u64) -> Result<(), CoordinationError> {
        for _ in 0..3 {
            let Some((plan, rev)) = &self.plan else {
                return Ok(()); // join_job guarantees a plan; defensive
            };
            if plan.generation >= generation {
                // A racing successor already fenced past us; demote.
                self.demote().await?;
                return Ok(());
            }
            let mut bumped = plan.clone();
            bumped.generation = generation;
            bumped.updated_at_ms = records::now_ms();
            match self
                .store
                .update(Keyspace::Durable, records::PLAN_KEY, bumped.encode(), *rev)
                .await
            {
                Ok(CasOutcome::Won(new_rev)) => {
                    self.plan_rev_seen = self.plan_rev_seen.max(new_rev.0);
                    self.plan = Some((bumped, new_rev));
                    self.plan_now = true;
                    return Ok(());
                }
                Ok(CasOutcome::Lost) => {
                    // Concurrent plan write (old leader's last breath or a
                    // racing successor): re-read and re-judge.
                    let entry = self
                        .store
                        .get(Keyspace::Durable, records::PLAN_KEY)
                        .await
                        .map_err(|e| store_error("re-reading the plan record", &e))?;
                    let Some(entry) = entry else {
                        return Err(crate::error::fatal(
                            "plan record vanished mid-election; the store prefix was \
                             tampered with",
                        ));
                    };
                    let plan = records::PlanRecord::parse(&entry.value, &self.fingerprint)?;
                    self.plan_rev_seen = self.plan_rev_seen.max(entry.revision.0);
                    self.plan = Some((plan, entry.revision));
                }
                Err(e) if matches!(e, crate::store::StoreError::Retryable(_)) => {
                    tracing::warn!(error = %e, "generation bump failed; retrying");
                }
                Err(e) => return Err(store_error("bumping the plan generation", &e)),
            }
        }
        // Could not fence the generation: give leadership back rather
        // than plan without a fence.
        self.demote().await?;
        Ok(())
    }

    /// Give leadership up. Only a fatal store error is returned.
    pub(crate) async fn demote(&mut self) -> Result<(), CoordinationError> {
        if let Some(rev) = self.leadership.take() {
            self.metrics(|m| m.set_leader(false));
            if let Err(e) = self
                .store
                .delete(Keyspace::Ephemeral, records::LEADER_KEY, Some(rev))
                .await
            {
                fatal_only("deleting the leader key", &e)?;
            }
        }
        Ok(())
    }

    /// Kick a planner run off onto the blocking pool if one is due. The
    /// run is joined by the task loop's select arm and never awaited here,
    /// so heartbeats keep flowing through a slow enumeration.
    pub(crate) fn maybe_start_plan(&mut self) -> Result<Option<PlanRun>, CoordinationError> {
        if !self.plan_now || self.leadership.is_none() || !self.caught_up {
            return Ok(None);
        }
        self.plan_now = false;
        let Some((plan, plan_rev)) = self.plan.clone() else {
            return Ok(None);
        };
        let generation = plan.generation;
        let cursor: Option<Vec<u8>> = match &plan.planner_state {
            Some(encoded) => Some(records::b64_decode(encoded).map_err(|e| {
                crate::error::fatal(format!("plan record: corrupt planner cursor ({e})"))
            })?),
            None => None,
        };
        let Some(mut planner) = self.planner.take() else {
            return Ok(None);
        };
        let started = Instant::now();
        let handle = tokio::task::spawn_blocking(move || {
            let ctx = PlanContext::new(cursor.as_deref(), generation);
            let result = planner.plan(ctx);
            (planner, result)
        });
        Ok(Some(PlanRun {
            handle,
            plan,
            plan_rev,
            generation,
            started,
        }))
    }

    /// Land a planner run: return the seeding of the splits this leader has
    /// not observed, to run beside the task loop. `None` when the planner
    /// failed retryably.
    pub(crate) fn land_plan(
        &mut self,
        joined: Result<PlannerOutput, tokio::task::JoinError>,
        run: PlanRun,
    ) -> Result<Option<SeedRun>, CoordinationError> {
        let (planner, result) = match joined {
            Ok(parts) => parts,
            Err(join_error) => {
                return Err(crate::error::fatal(format!(
                    "the planner panicked: {join_error}"
                )));
            }
        };
        self.planner = Some(planner);
        let split_plan = match result {
            Ok(plan) => plan,
            Err(e) if e.kind == CoordinationErrorKind::Retryable => {
                tracing::warn!(error = %e, "planner failed; next replan tick retries");
                self.metrics(|m| m.replan(ReplanOutcome::Error, run.started.elapsed()));
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        // Skipping splits whose progress and spec are both in the view
        // relies on split records never being deleted.
        let jobs: Vec<SeedJob> = split_plan
            .splits
            .iter()
            .filter(|planned| {
                self.splits
                    .get(planned.spec.id.as_str())
                    .is_none_or(|state| state.spec.is_none())
            })
            .map(|planned| SeedJob {
                id: planned.spec.id.clone(),
                spec: SplitSpecRecord::planned(&planned.spec, self.fp, run.generation),
                progress: SplitProgressRecord::planned(
                    &planned.spec.id,
                    self.fp,
                    planned.seed.as_ref(),
                ),
                spec_done: false,
            })
            .collect();
        let store = self.store.clone();
        let leading = Arc::new(AtomicBool::new(true));
        let bounds = SeedBounds {
            clock: self.clock.clone(),
            patience: self.config.replan_interval,
            leading: leading.clone(),
        };
        let pacing = Pacing::new(self.config.op_timeout);
        let (wins, won) = mpsc::unbounded_channel();
        let seeded = async move {
            let failed = seed_all(&store, jobs.into_iter(), &bounds, pacing, &wins).await;
            drop(wins);
            // `planned` is recounted from an authoritative listing, never
            // accumulated: only creates that WON are countable locally, so
            // a crash or failed publish between seeding and publishing
            // would otherwise leave records no future run ever counts, and
            // terminal detection compares against this number forever.
            let listed = match failed {
                None => Some(
                    store
                        .list(Keyspace::Durable, records::SPLIT_PREFIX)
                        .await
                        .map(|entries| entries.len() as u64),
                ),
                Some(_) => None,
            };
            Seeded { failed, listed }
        }
        .boxed();
        Ok(Some(SeedRun {
            seeded,
            won: Some(won),
            created: 0,
            leading,
            plan: run.plan,
            plan_rev: run.plan_rev,
            generation: run.generation,
            started: run.started,
            split_plan,
        }))
    }

    /// Fold a batch of seeded splits into the view, where they become
    /// assignable.
    pub(crate) fn fold_seeded(&mut self, wins: Vec<SeedWin>) -> Result<(), CoordinationError> {
        let created = wins.len() as u64;
        for (job, rev) in wins {
            // The watch echoes arrive at revisions we already know.
            self.attach_spec(job.id.as_str(), job.spec);
            self.upsert_progress(job.id.as_str(), job.progress, rev)?;
        }
        self.metrics(|m| m.planned(created));
        Ok(())
    }

    /// Whether this worker still leads the generation `run` was planned in.
    pub(crate) fn leads(&self, run: &SeedRun) -> bool {
        self.leadership.is_some()
            && self
                .plan
                .as_ref()
                .is_some_and(|(plan, _)| plan.generation == run.generation)
    }

    /// CAS the plan record for a seeding run that ended, the write that
    /// makes the run count.
    pub(crate) async fn finish_plan(
        &mut self,
        seed: SeedRun,
        seeded: Seeded,
    ) -> Result<(), CoordinationError> {
        let leads = self.leads(&seed);
        let SeedRun {
            plan,
            plan_rev,
            started,
            split_plan,
            created,
            ..
        } = seed;
        let deposed = match seeded.failed {
            Some(SeedFailure::Write {
                split,
                record,
                error,
            }) => {
                tracing::warn!(%split, record, %error,
                    "split seeding gave up; the next replan retries");
                self.metrics(|m| m.replan(ReplanOutcome::Error, started.elapsed()));
                return fatal_only(
                    &format!("seeding the {record} record of split {split}"),
                    &error,
                );
            }
            Some(SeedFailure::Deposed) => true,
            None => !leads,
        };
        if deposed {
            // Whoever leads now plans again; publishing here would lose by
            // revision and demote that leader if it is this worker.
            tracing::info!("leadership changed while seeding; the run publishes nothing");
            self.metrics(|m| m.replan(ReplanOutcome::Error, started.elapsed()));
            return Ok(());
        }
        let listed = match seeded.listed {
            Some(Ok(listed)) => listed,
            Some(Err(e)) => {
                tracing::warn!(error = %e, "planned recount failed; next replan tick retries");
                self.metrics(|m| m.replan(ReplanOutcome::Error, started.elapsed()));
                return fatal_only("recounting planned splits", &e);
            }
            None => return Ok(()),
        };

        // Publish the run: counts, cursor, finality, fenced by revision.
        let finality = PlanFinalityRepr::from(split_plan.finality);
        let finality_changed = plan.finality != finality;
        let count_changed = plan.planned != listed;
        let mut published = plan.clone();
        published.planned = listed;
        published.finality = finality;
        if let Some(state) = &split_plan.planner_state {
            published.planner_state = Some(records::b64_encode(state));
        }
        published.updated_at_ms = records::now_ms();
        match self
            .store
            .update(
                Keyspace::Durable,
                records::PLAN_KEY,
                published.encode(),
                plan_rev,
            )
            .await
        {
            Ok(CasOutcome::Won(rev)) => {
                self.plan_rev_seen = self.plan_rev_seen.max(rev.0);
                self.plan = Some((published, rev));
                let outcome = if created > 0 || finality_changed || count_changed {
                    ReplanOutcome::Ok
                } else {
                    ReplanOutcome::Noop
                };
                self.metrics(|m| m.replan(outcome, started.elapsed()));
                if split_plan.finality == PlanFinality::Final {
                    // A tick that fell during seeding has nothing left to plan.
                    self.plan_now = false;
                    tracing::info!(
                        planned = self.plan.as_ref().map_or(0, |(p, _)| p.planned),
                        "plan is final"
                    );
                }
                Ok(())
            }
            Ok(CasOutcome::Lost) => {
                // Fenced: a successor bumped the generation while we
                // planned. The seeded records are identical to what it
                // will seed (deterministic ids), and its own publish
                // recounts them from the store. Demote quietly.
                tracing::warn!("plan publish fenced; a successor leads");
                self.metrics(|m| m.replan(ReplanOutcome::Error, started.elapsed()));
                self.demote().await?;
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "plan publish failed; next replan tick retries");
                self.metrics(|m| m.replan(ReplanOutcome::Error, started.elapsed()));
                fatal_only("publishing the plan", &e)
            }
        }
    }
}

/// A plan run's seeding and recount, running beside the task loop, and
/// what its publish needs once they finish.
pub(crate) struct SeedRun {
    seeded: BoxFuture<'static, Seeded>,
    won: Option<mpsc::UnboundedReceiver<SeedWin>>,
    /// Progress creates that won, as yielded so far.
    created: u64,
    leading: Arc<AtomicBool>,
    plan: records::PlanRecord,
    plan_rev: Revision,
    generation: u64,
    started: Instant,
    split_plan: SplitPlan,
}

/// What a seeding run yields to the task loop.
pub(crate) enum SeedEvent {
    /// Splits seeded since the last event.
    Wins(Vec<SeedWin>),
    /// The run ended, with the wins not yet yielded.
    Done(Seeded, Vec<SeedWin>),
}

impl SeedRun {
    /// The next batch of seeded splits, or the run's end. Cancel-safe.
    pub(crate) async fn next(&mut self) -> SeedEvent {
        let SeedRun {
            won,
            seeded,
            created,
            ..
        } = self;
        let mut wins = Vec::new();
        loop {
            tokio::select! {
                biased;
                n = async {
                    let won = won.as_mut().expect("guarded by is_some");
                    won.recv_many(&mut wins, SEED_CONCURRENCY).await
                }, if won.is_some() =>
                {
                    if n > 0 {
                        *created += n as u64;
                        return SeedEvent::Wins(wins);
                    }
                    *won = None;
                }
                done = &mut *seeded => {
                    // The run can send its last wins and finish in one poll.
                    if let Some(won) = won {
                        while let Ok(win) = won.try_recv() {
                            wins.push(win);
                        }
                    }
                    *created += wins.len() as u64;
                    return SeedEvent::Done(done, wins);
                }
            }
        }
    }

    /// Stop the run: no write starts after this, and the run publishes nothing.
    pub(crate) fn depose(&self) {
        self.leading.store(false, Ordering::Relaxed);
    }
}

/// A split whose progress create won, at the revision it won.
pub(crate) type SeedWin = (SeedJob, Revision);

/// How a seeding run ended, and the recount, taken only when it seeded
/// every split.
pub(crate) struct Seeded {
    failed: Option<SeedFailure>,
    listed: Option<Result<u64, StoreError>>,
}

/// Why a seeding run stopped before seeding every split.
enum SeedFailure {
    /// A fatal write, or a retryable one after the run's patience ran out.
    Write {
        split: SplitId,
        record: &'static str,
        error: StoreError,
    },
    /// The worker stopped leading the run's generation.
    Deposed,
}

/// One split to seed, owned so its write future borrows only the store.
pub(crate) struct SeedJob {
    id: SplitId,
    spec: SplitSpecRecord,
    progress: SplitProgressRecord,
    /// The spec create has returned Won or Lost.
    spec_done: bool,
}

/// What ends a seeding run early: `patience` on the task's clock without
/// a split seeded, and the leadership it was planned under.
struct SeedBounds {
    clock: Arc<dyn Clock>,
    patience: Duration,
    leading: Arc<AtomicBool>,
}

/// First pause after a retryable seed failure.
const FIRST_BACKOFF: Duration = Duration::from_millis(100);

/// Run-wide retry pacing: each pause doubles the next one up to `cap` and
/// halves the writes in flight; `limit` wins in a row double them again.
struct Pacing {
    backoff: Duration,
    cap: Duration,
    limit: usize,
    streak: usize,
}

impl Pacing {
    /// Pauses top out at a quarter of `op_timeout`.
    fn new(op_timeout: Duration) -> Pacing {
        let cap = op_timeout / 4;
        Pacing {
            backoff: FIRST_BACKOFF.min(cap),
            cap,
            limit: SEED_CONCURRENCY,
            streak: 0,
        }
    }

    /// Start a pause, returning its length.
    fn pause(&mut self) -> Duration {
        let pause = self.backoff;
        self.backoff = (self.backoff * 2).min(self.cap);
        self.limit = (self.limit / 2).max(1);
        self.streak = 0;
        pause
    }

    fn won(&mut self) {
        self.streak += 1;
        if self.streak >= self.limit {
            self.limit = (self.limit * 2).min(SEED_CONCURRENCY);
            self.streak = 0;
        }
    }
}

/// Seed `jobs` with up to [`SEED_CONCURRENCY`] in flight, sending each
/// progress create that won to `wins`. A retryable failure pauses the run
/// and requeues its split behind the unstarted ones, until `bounds.patience`
/// passes without a split seeded. Then, on a fatal error, or once deposed,
/// no new job starts and the ones in flight finish. `None` means every
/// split was seeded.
async fn seed_all<S: CoordinationStore>(
    store: &S,
    mut jobs: impl Iterator<Item = SeedJob>,
    bounds: &SeedBounds,
    mut pacing: Pacing,
    wins: &mpsc::UnboundedSender<SeedWin>,
) -> Option<SeedFailure> {
    let mut in_flight = FuturesUnordered::new();
    let mut retry = VecDeque::new();
    let mut paused_until: Option<Instant> = None;
    let mut deadline = bounds.clock.now() + bounds.patience;
    let mut failed = None;
    loop {
        if failed.is_none() && !bounds.leading.load(Ordering::Relaxed) {
            failed = Some(SeedFailure::Deposed);
        }
        let pause = paused_until.filter(|&until| bounds.clock.now() < until);
        if failed.is_none() && pause.is_none() {
            while in_flight.len() < pacing.limit {
                let Some(job) = jobs.next().or_else(|| retry.pop_front()) else {
                    break;
                };
                in_flight.push(seed(store, job));
            }
        }
        let next = match pause.filter(|_| failed.is_none()) {
            None => in_flight.next().await,
            Some(until) if in_flight.is_empty() => {
                bounds.clock.sleep_until(until).await;
                continue;
            }
            Some(until) => tokio::select! {
                next = in_flight.next() => next,
                () = bounds.clock.sleep_until(until) => continue,
            },
        };
        // Nothing in flight: every split is seeded, or the run stopped.
        let Some((job, outcome)) = next else {
            return failed;
        };
        match outcome {
            Ok(rev) => {
                pacing.won();
                deadline = bounds.clock.now() + bounds.patience;
                if let Some(rev) = rev {
                    // The receiver outlives the run unless the task stopped.
                    let _ = wins.send((job, rev));
                }
            }
            Err((record, error @ StoreError::Retryable(_)))
                if failed.is_none() && bounds.clock.now() < deadline =>
            {
                let now = bounds.clock.now();
                if paused_until.is_none_or(|until| until <= now) {
                    let backoff = pacing.pause();
                    tracing::warn!(split = %job.id, record, %error, ?backoff,
                        limit = pacing.limit, "split seeding failed; retrying");
                    paused_until = Some((now + backoff).min(deadline));
                }
                retry.push_back(job);
            }
            Err((record, error)) => {
                let fatal = matches!(error, StoreError::Fatal(_));
                if fatal || failed.is_none() {
                    failed = Some(SeedFailure::Write {
                        split: job.id,
                        record,
                        error,
                    });
                }
            }
        }
    }
}

/// Create the spec, then the progress record, so a progress record in the
/// store implies its spec exists. `Some` carries the revision of a
/// progress create that won; `None` means the split was already planned.
/// An error names the record whose create failed.
async fn seed<S: CoordinationStore>(
    store: &S,
    mut job: SeedJob,
) -> (
    SeedJob,
    Result<Option<Revision>, (&'static str, StoreError)>,
) {
    let outcome = async {
        if !job.spec_done {
            // Won or Lost, the spec now exists.
            let _ = store
                .create(
                    Keyspace::Durable,
                    &records::spec_key(&job.id),
                    job.spec.encode(),
                )
                .await
                .map_err(|e| ("spec", e))?;
            job.spec_done = true;
        }
        let progress = store
            .create(
                Keyspace::Durable,
                &records::split_key(&job.id),
                job.progress.encode(),
            )
            .await
            .map_err(|e| ("progress", e))?;
        Ok(progress.won())
    }
    .await;
    (job, outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pauses double from 100ms to a quarter of `op_timeout`, each halving
    /// the writes in flight down to one, and `limit` wins in a row double
    /// them again.
    #[test]
    fn pacing_backs_off_then_recovers() {
        let mut pacing = Pacing::new(Duration::from_secs(10));
        let pauses: Vec<u64> = (0..7).map(|_| pacing.pause().as_millis() as u64).collect();
        assert_eq!(pauses, [100, 200, 400, 800, 1600, 2500, 2500]);
        assert_eq!(pacing.limit, 1);
        pacing.won();
        assert_eq!(pacing.limit, 2);
        pacing.won();
        assert_eq!(pacing.limit, 2);
        pacing.won();
        assert_eq!(pacing.limit, 4);
        for _ in 0..(4 + 8 + 16 + 32 + 64) {
            pacing.won();
        }
        assert_eq!(pacing.limit, SEED_CONCURRENCY);
    }
}
