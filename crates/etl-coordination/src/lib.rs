//! Distributed work-stealing coordination for etl-rs sources.
//!
//! A leader-elected worker runs the source's
//! [`SplitPlanner`](etl_core::coordination::SplitPlanner) to enumerate
//! weighted work *splits* into a shared low-latency store; every worker
//! leases splits toward a bounded working set, heartbeats them, and steals
//! from over-loaded peers when nothing unclaimed remains. Progress commits
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
//! fencing, election, and stealing live above it and are shared.

pub use etl_core::coordination::*;

pub mod config;
pub mod store;

mod coordinator;
mod error;
mod leader;
mod protocol;
mod records;
mod task;

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
