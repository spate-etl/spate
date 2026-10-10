//! Fault runs for coordinated Spate workers: worker processes read a
//! coordinated S3 source under injected faults, and an oracle checks the
//! delivery contract against their journals and the store's final state.
//!
//! This crate provides the journal format, the classification of `split.*`
//! and leader writes, the seeded generator, the outcome kinds a run reports, the
//! delivery oracle, the `spate-faults-worker` binary with the store and sink
//! that journal its traffic, the store wrappers that inject in-process
//! faults, stop the process inside a commit or before a leader write and
//! re-send a commit or seed create that lost its CAS, the seeded fault
//! schedule, the lost-reply evidence check, the container health poller, and
//! the harness that runs worker processes, reads the leader key before each
//! scheduled kill, kills the leader at a seeded stage of its work or resumes it
//! once a peer has claimed the split it stopped before, puts a seeded DynamoDB
//! fault proxy in front of each worker of the scheduled-fault DynamoDB runs,
//! opens seeded Toxiproxy windows on each worker's store link in the
//! scheduled-fault runs, and judges them.
//!
//! # Delivery properties
//!
//! Numbered as [`outcome::Check::property`] reports them. [`oracle::check`]
//! judges these:
//!
//! - **1. Every record arrives.** The set of ids the sinks wrote equals the
//!   set of generated ids.
//! - **2. Duplicates appear only inside a fault or replay window.** Each
//!   record written more than once lies in the replay range of some claim: at
//!   or above the watermark the claim replaced, and at or below the last
//!   record of the split that the previous tenant's process wrote. The claim
//!   either followed that process's release, or spent an attempt and was sent
//!   no earlier than the start of a fault window on that process and no later
//!   than two leases, the drain deadline, two store timeouts, two poll
//!   intervals and two seconds after the window's end or the replacement's
//!   start, whichever is later.
//! - **3. No committed position runs ahead of durable rows.** Every process
//!   that sent a landed value moving a split's watermark from W0 to W had
//!   written each record of the split in `[W0, W)` before that `send`. A value
//!   that sets `completed` has W above every record of the split. Every landed
//!   value that moves the watermark or sets `completed` has a journalled sender.
//! - **4. No split completes twice or goes missing, and after a killed
//!   leader, another instance takes the leader key.** One landed value per
//!   split sets `completed`, the split ends `Completed`, the swept descriptors
//!   partition the generated records, and a DynamoDB store holds a verdict.
//! - **5. No two owners commit on one split.** Per key in revision order, the
//!   epoch never falls, each epoch has at most one owner, and every value
//!   that moves the watermark carries the highest epoch at a lower revision.
//!   Every observation of one revision of a key holds the same value.

// Workers and the harness report on stderr, which the harness keeps per
// process.
#![allow(clippy::print_stderr)]

pub mod classify;
pub mod expect;
pub mod health;
pub mod journal;
pub mod oracle;
pub mod outcome;
#[cfg(unix)]
pub mod run;
pub mod schedule;
pub mod seaweed;
pub mod seed;
pub mod sink;
pub mod store;
pub mod worker;
#[cfg(unix)]
pub mod workers;
