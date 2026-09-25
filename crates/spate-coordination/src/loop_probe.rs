//! The coordinator task's control-loop state as of its last loop top: the
//! watch revisions it has applied, its next timer, and whether it is
//! planning.

use crate::store::{Keyspace, Revision};
use std::sync::Mutex;
use tokio::time::Instant;

/// The task's state the last time it reached the top of its loop, with
/// every select arm and `step` before it complete.
#[derive(Clone, Copy, Debug)]
pub struct LoopState {
    /// The earliest heartbeat, reconcile or replan deadline on the task's
    /// clock.
    pub next_timer: Instant,
    /// A plan is being computed off the loop.
    pub planning: bool,
    /// The highest revision the task has applied from its durable watch.
    pub durable: Revision,
    /// The highest revision the task has applied from its ephemeral watch.
    pub ephemeral: Revision,
}

/// Shared between a [`StoreCoordinator`](crate::StoreCoordinator) and its
/// task.
#[derive(Debug, Default)]
pub struct LoopProbe(Mutex<Probe>);

#[derive(Debug, Default)]
struct Probe {
    durable: u64,
    ephemeral: u64,
    published: Option<LoopState>,
}

impl LoopProbe {
    /// `None` until the task first reaches the top of its loop.
    #[must_use]
    pub fn state(&self) -> Option<LoopState> {
        self.lock().published
    }

    pub(crate) fn applied(&self, ks: Keyspace, revision: Revision) {
        let mut probe = self.lock();
        let applied = match ks {
            Keyspace::Durable => &mut probe.durable,
            Keyspace::Ephemeral => &mut probe.ephemeral,
        };
        *applied = (*applied).max(revision.0);
    }

    pub(crate) fn at_loop_top(&self, next_timer: Instant, planning: bool) {
        let mut probe = self.lock();
        probe.published = Some(LoopState {
            next_timer,
            planning,
            durable: Revision(probe.durable),
            ephemeral: Revision(probe.ephemeral),
        });
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Probe> {
        self.0.lock().expect("loop probe poisoned")
    }
}
