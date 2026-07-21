//! Distributed work coordination for etl-rs sources.
//!
//! A leader-elected worker runs the source's
//! [`SplitPlanner`](etl_core::coordination::SplitPlanner) to enumerate
//! weighted work *splits* into a shared low-latency store, and publishes a
//! desired assignment per instance; every worker leases the splits it was
//! named for, heartbeats them, and cooperatively drains the ones it was
//! not. Progress commits
//! are epoch-fenced compare-and-swap writes on the durable split record: a
//! fenced commit writes **nothing**, and committed progress can only
//! replay, never regress (at-least-once — duplicates possible, loss never).
//!
//! This crate implements the `etl_core::coordination` seam (re-exported
//! here) over the public [`store::CoordinationStore`] trait:
//!
//! - [`store::memory::MemoryStore`] — in-process, for tests and
//!   single-machine embedding.
//! - A NATS JetStream KV store (default `nats` feature, server >= 2.11) —
//!   the production backend.
//!
//! Custom backends (Redis, etcd) implement the store trait; the protocol,
//! fencing, election, and work assignment live above it and are shared.

pub use etl_core::coordination::*;

pub mod config;
pub mod store;

// Time seam behind lease expiry and the self-fence: `SystemClock` in
// production, a frozen/advanced clock in tests. `#[doc(hidden)]` keeps it off
// the documented, semver-stable surface while staying reachable from the
// external `tests/` binaries.
#[doc(hidden)]
pub mod clock;
mod coordinator;
mod error;
mod leader;
mod protocol;
mod records;
mod task;

pub use clock::{Clock, SystemClock};
pub use config::CoordinationConfig;
pub use coordinator::StoreCoordinator;

/// [`StoreCoordinator`] over the in-memory store: tests and
/// single-process embedding.
pub type MemoryCoordinator = StoreCoordinator<store::memory::MemoryStore>;

/// [`StoreCoordinator`] over NATS JetStream KV: the production backend
/// (server >= 2.11). Build the store with
/// [`NatsStore::new`](store::nats::NatsStore::new) — construction is
/// synchronous; the connection is made lazily under the startup budget.
#[cfg(feature = "nats")]
pub type NatsCoordinator = StoreCoordinator<store::nats::NatsStore>;
