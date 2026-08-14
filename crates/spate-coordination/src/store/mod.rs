//! Store primitives the coordination protocol runs on.
//!
//! The protocol needs six operations (create-if-absent, CAS-on-revision
//! update, point read, guarded delete, prefix watch, and a reconcile
//! listing) over two keyspaces: a **durable** one holding split records and
//! the plan record, which survive owner death, and an **ephemeral** one
//! whose keys expire a fixed TTL after their last write, holding leases
//! that every heartbeat rewrite re-arms. [`CoordinationStore`] is
//! that contract, public so deployments can bring their own backend
//! (Redis, etcd) next to the built-in NATS and in-memory stores.
//!
//! # Contract notes for implementors
//!
//! - [`Revision`]s are store-assigned and **strictly increase per key**
//!   across its write history (bucket-wide sequences satisfy this).
//! - `update` on an [`Ephemeral`](Keyspace::Ephemeral) key re-arms its
//!   TTL; expiry surfaces to watchers as [`WatchEvent::Delete`].
//! - A watch delivers a snapshot of live keys, then [`WatchEvent::
//!   SnapshotDone`], then live updates. A broken watch stream is
//!   [`StoreError::Retryable`]: consumers re-watch and apply `Put`s only
//!   at a revision above the last one they saw, so replays are idempotent.
//! - `list` is the loss-proof backstop for missed watch events: a key the
//!   consumer believes live but absent from a listing is treated as
//!   deleted. (The protocol never depends on *which* marker a watch
//!   delivered; graceful release vs expiry is decided from durable
//!   record state, not from watch semantics.)

use std::future::Future;
use std::time::Duration;

pub mod memory;
pub(crate) mod metered;
#[cfg(feature = "nats")]
pub mod nats;

/// Which keyspace an operation targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Keyspace {
    /// Never expires; survives owner death (split records, plan record).
    Durable,
    /// Every write re-arms expiry [`lease_ttl`](CoordinationStore::lease_ttl)
    /// from write time on the store's clock; expiry surfaces to watchers
    /// as a delete.
    Ephemeral,
}

/// Store-assigned version token, strictly increasing per key —
/// content-independent, so it carries none of the ABA hazards a
/// content-hash token would.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(pub u64);

/// One key's current value and revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The key, relative to the keyspace.
    pub key: String,
    /// The value bytes.
    pub value: Vec<u8>,
    /// Revision of the write that produced this value.
    pub revision: Revision,
}

/// Outcome of a conditional write. Losing is a protocol outcome (someone
/// else won the race), never an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum CasOutcome {
    /// The write landed at this revision.
    Won(Revision),
    /// The precondition failed: the key existed (create), or moved past
    /// the expected revision (update/delete).
    Lost,
}

impl CasOutcome {
    /// The winning revision, if the write landed.
    pub fn won(self) -> Option<Revision> {
        match self {
            CasOutcome::Won(rev) => Some(rev),
            CasOutcome::Lost => None,
        }
    }
}

/// One observation from a keyspace watch.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WatchEvent {
    /// A key's current value (snapshot replay or live update).
    Put(Entry),
    /// The key is gone, by explicit delete or ephemeral expiry.
    Delete {
        /// The key, relative to the keyspace.
        key: String,
        /// Revision of the deletion itself, strictly greater than every
        /// revision the key held before it. Deletes and puts for one key
        /// are only ordered through these revisions: a consumer that
        /// rewrote the key must ignore a delete whose revision is below
        /// its own write's (the stale echo of an older deletion).
        revision: Revision,
    },
    /// The initial snapshot is fully delivered; everything after is live.
    SnapshotDone,
}

/// A store operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Transient: the operation may succeed if retried.
    #[error("retryable store error: {0}")]
    Retryable(String),
    /// Unrecoverable: misconfiguration, unsupported server, corrupt state.
    #[error("fatal store error: {0}")]
    Fatal(String),
}

/// Boxed watch stream: ordered per key, snapshot-then-live.
pub type WatchStream = futures_util::stream::BoxStream<'static, Result<WatchEvent, StoreError>>;

/// The minimal primitives the coordination protocol needs; see the
/// [module docs](self) for the cross-implementation contract.
///
/// # Implementor notes
///
/// - [`StoreCoordinator`](crate::StoreCoordinator) additionally requires
///   the store to be `Clone`: implement the trait on a cheap shared
///   handle (wrap your connection state in an `Arc`, as the built-in
///   backends do).
/// - The trait uses `impl Future` returns and is therefore not
///   dyn-compatible (`Box<dyn CoordinationStore>` will not compile).
///   Deployments selecting a backend at runtime branch on the concrete
///   store and box the [`SplitCoordinator`](spate_core::coordination::
///   SplitCoordinator) instead; that trait is the dyn-compatible seam.
pub trait CoordinationStore: Send + Sync + 'static {
    /// The TTL every [`Ephemeral`](Keyspace::Ephemeral) write re-arms.
    /// Fixed at store construction (per-write TTLs are not portable).
    fn lease_ttl(&self) -> Duration;

    /// Create `key` if absent.
    fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> impl Future<Output = Result<CasOutcome, StoreError>> + Send;

    /// Replace `key` iff it is currently at `expected`.
    fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> impl Future<Output = Result<CasOutcome, StoreError>> + Send;

    /// Read `key`'s current value, `None` when absent (deleted and
    /// expired keys read as absent).
    fn get(
        &self,
        ks: Keyspace,
        key: &str,
    ) -> impl Future<Output = Result<Option<Entry>, StoreError>> + Send;

    /// Delete `key`; with `expected`, only if still at that revision.
    /// Deleting an absent key wins vacuously.
    fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> impl Future<Output = Result<CasOutcome, StoreError>> + Send;

    /// Snapshot + live tail of every key under `prefix`.
    fn watch(
        &self,
        ks: Keyspace,
        prefix: &str,
    ) -> impl Future<Output = Result<WatchStream, StoreError>> + Send;

    /// Point-in-time listing of every live key under `prefix`.
    fn list(
        &self,
        ks: Keyspace,
        prefix: &str,
    ) -> impl Future<Output = Result<Vec<Entry>, StoreError>> + Send;
}
