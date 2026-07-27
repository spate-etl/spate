//! Leader election and planning: whoever holds the leadership lease runs
//! the source's planner; the plan record's CAS revision is the fence that
//! makes a deposed leader harmless.
//!
//! Election is a lease like any other: `create` the well-known leader key
//! (expired keys are re-creatable), heartbeat it, lose it by fencing or
//! expiry. The winner immediately bumps the plan record's `generation`
//! via CAS — from that point, any in-flight plan write from a previous
//! leader loses by revision. Zombie *split* creates need no fence at all:
//! deterministic ids + create-if-absent make a stale leader's writes
//! byte-equivalent to the live leader's, or losers of the create race.
//!
//! The planner itself runs on the blocking pool and is joined by a select
//! arm in the task loop ([`Task::maybe_start_plan`] starts it,
//! [`Task::finish_plan`] lands it) — a slow enumeration must never stall
//! heartbeats, watch processing, or command service.

use crate::error::store_error;
use crate::records::{self, LeaderVal, PlanFinalityRepr, SplitProgressRecord, SplitSpecRecord};
use crate::store::{CasOutcome, CoordinationStore, Keyspace, Revision};
use crate::task::Task;
use spate_core::coordination::{
    CoordinationError, CoordinationErrorKind, PlanContext, PlanFinality, SplitPlan, SplitPlanner,
};
use spate_core::metrics::ReplanOutcome;
use tokio::time::Instant;

/// What the blocking-pool planner call returns: the planner handed back,
/// plus its enumeration result.
pub(crate) type PlannerOutput = (Box<dyn SplitPlanner>, Result<SplitPlan, CoordinationError>);

/// A planner run in flight on the blocking pool, plus the plan-record
/// snapshot its publish will CAS against (anything that moved the record
/// meanwhile — a successor's generation bump — makes the publish lose).
pub(crate) struct PlanRun {
    pub(crate) handle: tokio::task::JoinHandle<PlannerOutput>,
    plan: records::PlanRecord,
    plan_rev: Revision,
    generation: u64,
    started: Instant,
}

impl<S: CoordinationStore> Task<S> {
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
                self.bump_generation(generation).await?;
                Ok(())
            }
            Ok(CasOutcome::Lost) => Ok(()), // someone else won; watch will show them
            Err(e) => {
                tracing::warn!(error = %e, "election write failed; retrying on observation");
                Ok(())
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
                self.demote().await;
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
        self.demote().await;
        Ok(())
    }

    pub(crate) async fn demote(&mut self) {
        if let Some(rev) = self.leadership.take() {
            self.metrics(|m| m.set_leader(false));
            let _ = self
                .store
                .delete(Keyspace::Ephemeral, records::LEADER_KEY, Some(rev))
                .await;
        }
    }

    /// Kick a planner run off onto the blocking pool if one is due. The
    /// run is joined by the task loop's select arm — never awaited here —
    /// so heartbeats keep flowing through a slow enumeration.
    pub(crate) fn maybe_start_plan(&mut self) -> Result<Option<PlanRun>, CoordinationError> {
        if !self.plan_now || self.leadership.is_none() {
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
            return Ok(None); // a concurrent run is impossible; defensive
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

    /// Land a planner run: seed splits with create-if-absent (idempotent
    /// by deterministic ids), recount, then CAS the plan record — the
    /// write that makes the run count.
    pub(crate) async fn finish_plan(
        &mut self,
        joined: Result<PlannerOutput, tokio::task::JoinError>,
        run: PlanRun,
    ) -> Result<(), CoordinationError> {
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
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        // Seed the splits, spec before progress: a progress record in the
        // store implies its spec exists. First-writer-wins per id.
        let mut created = 0u64;
        for planned in &split_plan.splits {
            let spec_record = SplitSpecRecord::planned(&planned.spec, self.fp, run.generation);
            match self
                .store
                .create(
                    Keyspace::Durable,
                    &records::spec_key(&planned.spec.id),
                    spec_record.encode(),
                )
                .await
            {
                Ok(_) => {} // Won or Lost: the spec exists either way
                Err(e) => {
                    tracing::warn!(split = %planned.spec.id, error = %e,
                        "spec seeding failed; next replan tick retries");
                    self.metrics(|m| m.replan(ReplanOutcome::Error, run.started.elapsed()));
                    return Ok(());
                }
            }
            let progress =
                SplitProgressRecord::planned(&planned.spec.id, self.fp, planned.seed.as_ref());
            match self
                .store
                .create(
                    Keyspace::Durable,
                    &records::split_key(&planned.spec.id),
                    progress.encode(),
                )
                .await
            {
                Ok(CasOutcome::Won(rev)) => {
                    created += 1;
                    // Fold our own writes into the view; the watch echoes
                    // arrive at revisions we already know.
                    self.attach_spec(planned.spec.id.as_str(), spec_record);
                    self.upsert_progress(planned.spec.id.as_str(), progress, rev)?;
                }
                Ok(CasOutcome::Lost) => {} // already planned: idempotent no-op
                Err(e) => {
                    tracing::warn!(split = %planned.spec.id, error = %e,
                        "split seeding failed; next replan tick retries");
                    self.metrics(|m| m.replan(ReplanOutcome::Error, run.started.elapsed()));
                    return Ok(());
                }
            }
        }
        self.metrics(|m| m.planned(created));

        // `planned` is recounted from an authoritative listing, never
        // accumulated: only creates that WON are countable locally, so a
        // crash or failed publish between seeding and publishing would
        // otherwise leave records no future run ever counts — and
        // terminal detection compares against this number forever.
        let listed = match self
            .store
            .list(Keyspace::Durable, records::SPLIT_PREFIX)
            .await
        {
            Ok(entries) => entries.len() as u64,
            Err(e) => {
                tracing::warn!(error = %e, "planned recount failed; next replan tick retries");
                self.metrics(|m| m.replan(ReplanOutcome::Error, run.started.elapsed()));
                return Ok(());
            }
        };

        // Publish the run: counts, cursor, finality — fenced by revision.
        let finality = PlanFinalityRepr::from(split_plan.finality);
        let finality_changed = run.plan.finality != finality;
        let count_changed = run.plan.planned != listed;
        let mut published = run.plan.clone();
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
                run.plan_rev,
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
                self.metrics(|m| m.replan(outcome, run.started.elapsed()));
                if split_plan.finality == PlanFinality::Final {
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
                self.metrics(|m| m.replan(ReplanOutcome::Error, run.started.elapsed()));
                self.demote().await;
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "plan publish failed; next replan tick retries");
                self.metrics(|m| m.replan(ReplanOutcome::Error, run.started.elapsed()));
                Ok(())
            }
        }
    }
}
