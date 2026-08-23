//! Distributed work coordination for Spate sources.
//!
//! A leader-elected worker runs the source's
//! [`SplitPlanner`] to enumerate
//! weighted work *splits* into a shared low-latency store, and publishes a
//! desired assignment per instance; every worker leases the splits it was
//! named for, heartbeats them, and cooperatively drains the ones it was
//! not. Progress commits are epoch-fenced compare-and-swap writes on the
//! durable split record. A fenced commit writes **nothing**, and committed
//! progress can only replay, never regress. Delivery is at-least-once, so
//! duplicates are possible and records are never lost.
//!
//! This crate implements the `spate_core::coordination` seam (re-exported
//! here) over the public [`store::CoordinationStore`] trait:
//!
//! - [`store::memory::MemoryStore`] — in-process, for tests and
//!   single-machine embedding.
//! - A NATS JetStream KV store (default `nats` feature, server >= 2.11) —
//!   the production backend.
//!
//! Custom backends (Redis, etcd) implement the store trait; the protocol,
//! fencing, election, and work assignment live above it and are shared.

pub use spate_core::coordination::*;

pub mod config;
pub mod store;

// The `testing` feature also carries `bench_seams`, which reaches the pure,
// synchronous decisions an instruction-count bench cannot get to through an
// async surface, and `fuzz_seams`, which reaches the record and key parsers
// for the fuzz harness. Both modules are `#[doc(hidden)]`, so a link to
// either dangles on docs.rs (where the feature is off) and renders as literal
// text in the published API reference (where it is on).
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod bench_seams;
// Time seam behind every deadline in the control loop: `SystemClock` in
// production, a clock the test advances in tests. `#[doc(hidden)]` hides the
// *module path* only. `Clock`, `SystemClock` and `Sleep` are re-exported
// below without it, so the trait is public, documented, semver-stable
// surface. Adding a required method to it is a breaking change.
#[doc(hidden)]
pub mod clock;
mod coordinator;
mod error;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod fuzz_seams;
mod leader;
mod protocol;
mod records;
mod task;

// `Sleep` is the return type of a required `Clock` method, so an external
// implementor has to be able to name it.
pub use clock::{Clock, Sleep, SystemClock};
pub use config::CoordinationConfig;
pub use coordinator::StoreCoordinator;

/// [`StoreCoordinator`] over the in-memory store: tests and
/// single-process embedding.
pub type MemoryCoordinator = StoreCoordinator<store::memory::MemoryStore>;

/// [`StoreCoordinator`] over NATS JetStream KV: the production backend
/// (server >= 2.11). Build the store with
/// [`NatsStore::new`](store::nats::NatsStore::new). Construction is
/// synchronous; the connection is made lazily under the startup budget.
#[cfg(feature = "nats")]
pub type NatsCoordinator = StoreCoordinator<store::nats::NatsStore>;
