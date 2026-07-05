//! Backpressure: the global in-flight byte budget and the watermark
//! pause/resume controller with hysteresis.
//!
//! Implemented in the foundations workstream. Invariant: source threads
//! never block on sends — `try_send` + pause + keep polling
//! (see `docs/DESIGN.md` § Backpressure).
