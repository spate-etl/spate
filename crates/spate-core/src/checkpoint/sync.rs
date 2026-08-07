//! Synchronization-primitive shim: `std` types normally, `loom` types under
//! `--cfg loom` so the acknowledgment primitives can be model-checked.

#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU8, Ordering};
