//! Names what a `split.*` write does from the value it replaces, and a
//! leader's writes to the leader key, `plan`, `split.*` and `assign.*` from
//! their key.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::journal::{Progress, Status, WriteOp};

/// What a write does: a `split.*` update to the split, or a leader's write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteKind {
    /// Moves the split to `Quarantined`.
    Quarantine,
    /// Takes ownership at a higher epoch.
    Claim,
    /// Sets `completed`.
    Complete,
    /// Moves the watermark under the current epoch.
    Commit,
    /// Gives up a runnable split without spending an attempt.
    Release,
    /// Gives the split up and spends an attempt.
    FailReport,
    /// Re-arms the split lease, an ephemeral write.
    Renew,
    /// Updates the durable `plan` record.
    Plan,
    /// Creates a durable `split.*` progress record.
    Seed,
    /// Creates or updates a durable `assign.*` record.
    Assign,
    /// The first [`WriteKind::Plan`] write a process sends after its first
    /// [`WriteKind::Seed`]. Only a stop plan names it.
    Publish,
    /// Creates the ephemeral leader key.
    Elect,
    /// Re-arms the ephemeral leader key.
    LeaderRenew,
}

/// Classifies a durable update by `me` from `prev`, the value at its expected
/// revision, to `next`. `None` when the change matches no kind.
#[must_use]
pub fn classify(prev: &Progress, next: &Progress, me: &str) -> Option<WriteKind> {
    let mine = |p: &Progress| p.owner.as_deref() == Some(me);
    // Ahead of `FailReport`: a quarantining failure report also clears the
    // owner and spends an attempt.
    if next.status == Status::Quarantined && prev.status != Status::Quarantined {
        return Some(WriteKind::Quarantine);
    }
    if next.epoch > prev.epoch && mine(next) {
        return Some(WriteKind::Claim);
    }
    // A sweep completion can keep the watermark, so this is not a `Commit`.
    if next.completed && !prev.completed {
        return Some(WriteKind::Complete);
    }
    if next.epoch == prev.epoch && mine(next) && next.watermark != prev.watermark {
        return Some(WriteKind::Commit);
    }
    if mine(prev) && next.owner.is_none() {
        if next.attempts == prev.attempts && next.status == Status::Runnable {
            return Some(WriteKind::Release);
        }
        if next.attempts == prev.attempts + 1 {
            return Some(WriteKind::FailReport);
        }
    }
    None
}

/// Classifies an ephemeral update of `key`: a write to a `split.*` lease is a
/// [`WriteKind::Renew`].
#[must_use]
pub fn classify_ephemeral(key: &str) -> Option<WriteKind> {
    key.starts_with("split.").then_some(WriteKind::Renew)
}

/// Classifies a leader's write of `key` by its key and call: the ephemeral
/// leader key's create or update, or a durable `plan` update, `split.*` create
/// or `assign.*` write. `None` for anything else.
#[must_use]
pub fn classify_leader(ephemeral: bool, op: WriteOp, key: &str) -> Option<WriteKind> {
    if ephemeral {
        return (key == "leader").then_some(match op {
            WriteOp::Create => WriteKind::Elect,
            WriteOp::Update => WriteKind::LeaderRenew,
        });
    }
    match op {
        _ if key.starts_with("assign.") => Some(WriteKind::Assign),
        WriteOp::Update if key == "plan" => Some(WriteKind::Plan),
        WriteOp::Create if key.starts_with("split.") => Some(WriteKind::Seed),
        WriteOp::Create | WriteOp::Update => None,
    }
}

/// The values one process has learned at each `(key, rev)`, from its own
/// landed writes and from its reads. Shared across threads.
#[derive(Debug)]
pub struct Classifier {
    me: String,
    values: Mutex<HashMap<(String, u64), Progress>>,
}

impl Classifier {
    /// An empty classifier for writes by instance `me`.
    #[must_use]
    pub fn new(me: impl Into<String>) -> Classifier {
        Classifier {
            me: me.into(),
            values: Mutex::new(HashMap::new()),
        }
    }

    /// Records that `key` held `value` at `rev`.
    pub fn learn(&self, key: &str, rev: u64, value: Progress) {
        self.lock().insert((key.to_owned(), rev), value);
    }

    /// Classifies a durable update of `key` from revision `expected` to
    /// `next`. `None` when the value at `expected` is unknown or the change
    /// matches no kind.
    #[must_use]
    pub fn classify(&self, key: &str, expected: u64, next: &Progress) -> Option<WriteKind> {
        let values = self.lock();
        let prev = values.get(&(key.to_owned(), expected))?;
        classify(prev, next, &self.me)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, u64), Progress>> {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::SCHEMA;

    fn value(epoch: u64, owner: Option<&str>, watermark: Option<i64>) -> Progress {
        Progress {
            schema: SCHEMA,
            epoch,
            owner: owner.map(str::to_owned),
            watermark,
            completed: false,
            status: Status::Runnable,
            attempts: 0,
        }
    }

    /// One write of each kind classifies as that kind, including a completion
    /// that keeps the watermark and a quarantine that raises the epoch and
    /// clears the owner.
    #[test]
    fn classifies_claim_commit_complete_release_fail_quarantine_renew() {
        let held = value(2, Some("w0"), Some(10));
        let k = |next: &Progress| classify(&held, next, "w0");

        assert_eq!(
            classify(&value(1, Some("w1"), Some(10)), &held, "w0"),
            Some(WriteKind::Claim)
        );
        assert_eq!(k(&value(2, Some("w0"), Some(20))), Some(WriteKind::Commit));

        let mut swept = held.clone();
        swept.completed = true;
        assert_eq!(k(&swept), Some(WriteKind::Complete));
        let mut done = value(2, Some("w0"), Some(30));
        done.completed = true;
        assert_eq!(k(&done), Some(WriteKind::Complete));

        assert_eq!(k(&value(2, None, Some(10))), Some(WriteKind::Release));
        let mut failed = value(2, None, Some(10));
        failed.attempts = 1;
        assert_eq!(k(&failed), Some(WriteKind::FailReport));

        let mut quarantined = value(3, None, Some(10));
        quarantined.status = Status::Quarantined;
        quarantined.attempts = 1;
        assert_eq!(k(&quarantined), Some(WriteKind::Quarantine));

        assert_eq!(k(&held), None, "an unchanged value is no kind");
        assert_eq!(
            classify(&held, &value(3, Some("w1"), Some(10)), "w0"),
            None,
            "a claim by another instance is not this process's write"
        );

        let classifier = Classifier::new("w0");
        assert_eq!(classifier.classify("split.a", 5, &swept), None);
        classifier.learn("split.a", 5, held.clone());
        assert_eq!(
            classifier.classify("split.a", 5, &swept),
            Some(WriteKind::Complete)
        );
        assert_eq!(classifier.classify("split.b", 5, &swept), None);

        assert_eq!(classify_ephemeral("split.a"), Some(WriteKind::Renew));
        assert_eq!(classify_ephemeral("worker.w0"), None);
    }

    /// The leader key's create and update, a durable `plan` update, `split.*`
    /// create and `assign.*` create or update are leader writes; the startup
    /// `plan` create, `spec.*`, `verdict`, `_probe.*`, `split.*` updates and
    /// other ephemeral writes are not.
    #[test]
    fn classifies_leader_writes_by_key_and_call() {
        use WriteOp::{Create, Update};
        let k = |op, key| classify_leader(false, op, key);
        assert_eq!(k(Update, "plan"), Some(WriteKind::Plan));
        assert_eq!(k(Create, "split.a"), Some(WriteKind::Seed));
        assert_eq!(k(Create, "assign.w0"), Some(WriteKind::Assign));
        assert_eq!(k(Update, "assign.w0"), Some(WriteKind::Assign));
        assert_eq!(k(Create, "plan"), None);
        assert_eq!(k(Create, "spec.a"), None);
        assert_eq!(k(Create, "verdict"), None);
        assert_eq!(k(Create, "_probe.w0"), None);
        assert_eq!(k(Update, "_probe.w0"), None);
        assert_eq!(k(Update, "split.a"), None);
        assert_eq!(
            classify_leader(true, Create, "leader"),
            Some(WriteKind::Elect)
        );
        assert_eq!(
            classify_leader(true, Update, "leader"),
            Some(WriteKind::LeaderRenew)
        );
        assert_eq!(k(Create, "leader"), None);
        assert_eq!(classify_leader(true, Create, "split.a"), None);
        assert_eq!(classify_leader(true, Update, "assign.w0"), None);
    }

    /// A `Classifier` judges ownership by the instance id it was built with.
    #[test]
    fn classifier_uses_its_instance_id() {
        let classifier = Classifier::new("w1");
        classifier.learn("split.a", 5, value(1, Some("w0"), Some(10)));
        assert_eq!(
            classifier.classify("split.a", 5, &value(2, Some("w1"), Some(10))),
            Some(WriteKind::Claim)
        );
        assert_eq!(
            classifier.classify("split.a", 5, &value(2, Some("w0"), Some(10))),
            None
        );
    }

    /// An owner-clear by `me` over a `Completed` record matches no kind.
    #[test]
    fn release_over_a_completed_record_is_no_kind() {
        let mut completed = value(2, Some("w0"), Some(10));
        completed.completed = true;
        completed.status = Status::Completed;
        let mut released = completed.clone();
        released.owner = None;
        assert_eq!(classify(&completed, &released, "w0"), None);
    }

    /// An own write that moves the watermark below the stored epoch is not a
    /// `Commit`.
    #[test]
    fn own_write_below_the_stored_epoch_is_no_kind() {
        let peer_claim = value(3, Some("w1"), Some(10));
        let stale = value(2, Some("w0"), Some(20));
        assert_eq!(classify(&peer_claim, &stale, "w0"), None);
    }
}
