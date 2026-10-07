//! Fault runs for coordinated Spate workers: worker processes read a
//! coordinated S3 source under injected faults, and an oracle checks the
//! delivery contract against their journals and the store's final state.
//!
//! This crate provides the journal format, the classification of `split.*`
//! writes, the seeded generator, the outcome kinds a run reports and the
//! delivery oracle.
//!
//! # Delivery properties
//!
//! Numbered as [`outcome::Check::property`] reports them. [`oracle::check`]
//! judges these:
//!
//! - **1. Every record arrives.** The set of ids the sinks wrote equals the
//!   set of generated ids.
//! - **3. No committed position runs ahead of durable rows.** Every process
//!   that sent a landed value moving a split's watermark from W0 to W had
//!   written each record of the split in `[W0, W)` before that `send`. A value
//!   that sets `completed` has W above every record of the split. Every landed
//!   value that moves the watermark or sets `completed` has a journalled sender.
//! - **4. No split completes twice or goes missing.** One landed value per
//!   split sets `completed`, the split ends `Completed`, the swept descriptors
//!   partition the generated records, and a DynamoDB store holds a verdict.

pub mod classify;
pub mod journal;
pub mod oracle;
pub mod outcome;
pub mod seed;
