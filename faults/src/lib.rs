//! Fault runs for coordinated Spate workers: worker processes read a
//! coordinated S3 source under injected faults, and an oracle checks the
//! delivery contract against their journals and the store's final state.
//!
//! This crate provides the journal format, the classification of `split.*`
//! writes, the seeded generator and the outcome kinds a run reports.

pub mod classify;
pub mod journal;
pub mod outcome;
pub mod seed;
