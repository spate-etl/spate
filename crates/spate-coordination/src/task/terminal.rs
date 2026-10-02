//! Judging whether the job is terminal, and the verdict marker that shares it.

use super::{Listed, Task};
use crate::error::fatal_only;
use crate::records::{self};
use crate::store::{CoordinationStore, Keyspace};
use spate_core::coordination::{CoordinationError, CoordinationEvent};

impl<S: CoordinationStore + Clone> Task<S> {
    /// Decide whether the job is over, and how.
    ///
    /// The verdict latches forever and a wrong `AllComplete` reports an
    /// incomplete backfill as a success, so correctness comes before
    /// promptness here:
    ///
    /// - **Quarantine blocks completion explicitly.** The invariant is not
    ///   left to fall out of `completed == total` arithmetic; one slipped
    ///   tally would otherwise dress unprocessed data up as a green exit.
    /// - **The verdict is rendered against an authoritative listing**, not
    ///   against this worker's watch-fed view and not against
    ///   `plan.planned`. `planned` is only a lower bound: `land_plan`'s
    ///   seeding run writes split records *before* it counts the splits in
    ///   the store, `finish_plan` publishes after that, and both
    ///   publish-failure paths leave the seeded records behind, so a `Final`
    ///   plan, which never replans, can name fewer splits than the store
    ///   holds. Judging a *subset* that happens to be all-complete is how a
    ///   quarantined split goes unseen.
    ///
    /// The listing costs one store round trip and is gated behind a local
    /// pre-check, so it runs essentially once per job.
    pub(super) fn check_terminal(&mut self) {
        if self.terminal_reported {
            return;
        }
        let Some((plan, _)) = &self.plan else {
            return;
        };
        if plan.finality != records::PlanFinalityRepr::Final {
            return;
        }
        let planned = plan.planned;
        // A peer has judged the job terminal; this view may be partial.
        if std::mem::take(&mut self.verdict_listing_allowed) {
            self.terminal_due = true;
            return;
        }
        // Cheap gate: pay for the listing only once this worker's view
        // covers what the plan promised and looks terminal. `planned` is
        // a lower bound, so this is `<`, not `!=`.
        let local = self.splits.len() as u64;
        if local < planned || self.completed_count + self.quarantined_count != local {
            return;
        }
        self.terminal_due = true;
    }

    /// Mark the job terminal for workers whose polled view is partial.
    pub(super) async fn write_verdict(&mut self) -> Result<(), CoordinationError> {
        let val = records::encode_val(&records::VerdictVal {
            schema: records::SCHEMA,
            reporter: self.instance.clone(),
        });
        match self
            .store
            .create(Keyspace::Durable, records::VERDICT_KEY, val)
            .await
        {
            Ok(_) => {
                self.verdict_written = true;
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "verdict marker write failed; next step retries");
                fatal_only("writing the verdict marker", &e)
            }
        }
    }

    /// Render the verdict [`Task::check_terminal`] asked for against its
    /// authoritative listing.
    pub(super) fn finish_terminal(&mut self, listed: Listed) -> Result<(), CoordinationError> {
        if self.terminal_reported {
            return Ok(());
        }
        // Authoritative recount. Applying the entries is idempotent, so
        // this doubles as catch-up for a view missing records.
        let entries = match listed {
            Ok(entries) => entries,
            Err(e) => {
                // Refusing to judge is the safe direction.
                tracing::warn!(error = %e, "terminal listing failed; deferring the verdict");
                return fatal_only("listing splits for the verdict", &e);
            }
        };
        for entry in &entries {
            self.apply_state_put(entry)?;
        }

        let total = self.splits.len() as u64;
        if total != entries.len() as u64 {
            // The listing and the folded view disagree on cardinality.
            // Judge nothing this tick.
            return Ok(());
        }
        let (completed, quarantined) = (self.completed_count, self.quarantined_count);
        if completed == total && quarantined == 0 {
            self.terminal_reported = true;
            self.emit(CoordinationEvent::AllComplete);
        } else if completed + quarantined == total {
            self.terminal_reported = true;
            self.emit(CoordinationEvent::Stalled {
                completed,
                quarantined,
            });
        }
        Ok(())
    }
}
