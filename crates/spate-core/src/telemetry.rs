//! Structured logging: `tracing` initialization (JSON for Kubernetes) and
//! rate-limited hot-path logging helpers.
//!
//! # Levels
//!
//! Choose a level by what the event means to an operator:
//!
//! - **`WARN`** — the pipeline is degraded, or has lost work it expected to
//!   keep, and an operator may need to act. Not a routine event the framework
//!   itself causes, however unusual that event looks from inside the one
//!   component that observes it.
//! - **`INFO`** — a lifecycle milestone worth a place in a post-mortem
//!   timeline: startup and shutdown, leadership, fleet membership, a plan
//!   becoming final. Bounded by those events, never by the workload.
//! - **`DEBUG`** — the bookkeeping underneath them: per-split, per-retry,
//!   per-connection.
//!
//! A deployment runs at `INFO`. A dependency's own output arrives through
//! its bridge at whatever level that dependency chose, and a per-record
//! event cannot be bounded by a level at all (see below).
//!
//! # Rate limiting on the hot path
//!
//! A poison-message storm that logs per record will destroy the pinned
//! pipeline threads. Hot-path warnings must go through a [`RateLimit`],
//! most conveniently via [`rate_limited_warn!`](crate::rate_limited_warn):
//!
//! ```
//! use spate_core::rate_limited_warn;
//! use spate_core::telemetry::RateLimit;
//! use std::time::Duration;
//!
//! static DESER_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));
//!
//! // In the record loop:
//! rate_limited_warn!(DESER_WARN, reason = "malformed", "payload skipped");
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Monotonic reference point shared by every [`RateLimit`], so a window
/// deadline can be stored as a plain `AtomicU64` of nanoseconds and compared
/// on the lock-free fast path.
fn base_instant() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

fn nanos_since_base(now: Instant) -> u64 {
    now.saturating_duration_since(base_instant()).as_nanos() as u64
}

/// Output format for logs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum LogFormat {
    /// One JSON object per line with flattened event fields, the shape
    /// Kubernetes log pipelines expect. Default.
    #[default]
    Json,
    /// Human-readable output for local development.
    Pretty,
}

/// Initialize the global `tracing` subscriber.
///
/// The filter comes from `RUST_LOG` when set, else `default_filter`
/// (e.g. `"info,spate_core=debug"`). Idempotent: returns `true` if this call
/// installed the subscriber, `false` if one (ours or foreign) was already
/// installed. Never panics, so libraries and tests can call it freely.
pub fn init(format: LogFormat, default_filter: &str) -> bool {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_target(true)
            .with_env_filter(filter)
            .try_init()
            .is_ok(),
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_target(true)
            .with_env_filter(filter)
            .try_init()
            .is_ok(),
    }
}

/// Decision returned by [`RateLimit::check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Log this event. `suppressed_before` is how many events were dropped
    /// since the last allowed one (attach it to the log line).
    Allow {
        /// Events suppressed since the previous allowed event.
        suppressed_before: u64,
    },
    /// Drop this event silently.
    Suppress,
}

#[derive(Debug)]
struct RateLimitState {
    window_start: Option<Instant>,
    allowed_in_window: u32,
    suppressed: u64,
}

/// A per-callsite token bucket: up to `capacity` events per `window`, then
/// suppression with a count carried into the first event of the next
/// window.
///
/// `const`-constructible for use in `static`s. Under a poison storm every
/// pinned pipeline thread hits the *same* limiter once per failing record,
/// so the exact-accounting mutex is contended. To keep steady-state
/// suppression off the shared lock, a relaxed-atomic fast path
/// short-circuits while the window stays saturated: a single relaxed load
/// of `saturated` plus a deadline compare, no mutex. The mutex path remains
/// the source of truth for allow decisions and window rolls; suppressed
/// events counted on the fast path are folded back in at the next roll, so
/// the carried `suppressed` count is exact in practice (best-effort under
/// concurrent rolls).
#[derive(Debug)]
pub struct RateLimit {
    capacity: u32,
    window: Duration,
    state: Mutex<RateLimitState>,
    /// Set while the current window is exhausted; lets the fast path suppress
    /// without the mutex. Cleared on a window roll (or superseded by
    /// `window_end_nanos` once the deadline passes).
    saturated: AtomicBool,
    /// Deadline of the saturated window, nanoseconds since [`base_instant`].
    window_end_nanos: AtomicU64,
    /// Events suppressed on the lock-free fast path this window, folded into
    /// the carried count when the window rolls.
    fast_suppressed: AtomicU64,
}

impl RateLimit {
    /// A limiter allowing `capacity` events per `window`.
    #[must_use]
    pub const fn new(capacity: u32, window: Duration) -> Self {
        RateLimit {
            capacity,
            window,
            state: Mutex::new(RateLimitState {
                window_start: None,
                allowed_in_window: 0,
                suppressed: 0,
            }),
            saturated: AtomicBool::new(false),
            window_end_nanos: AtomicU64::new(0),
            fast_suppressed: AtomicU64::new(0),
        }
    }

    /// Decide whether to log an event happening now.
    pub fn check(&self) -> Decision {
        self.check_at(Instant::now())
    }

    /// Decide for an event at `now` (injectable for tests).
    pub fn check_at(&self, now: Instant) -> Decision {
        // Fast path: while the window stays saturated, suppress with a single
        // relaxed load and a deadline compare, no mutex. Once the deadline
        // passes the compare fails and we fall through to roll the window.
        if self.saturated.load(Ordering::Relaxed)
            && nanos_since_base(now) < self.window_end_nanos.load(Ordering::Relaxed)
        {
            self.fast_suppressed.fetch_add(1, Ordering::Relaxed);
            return Decision::Suppress;
        }

        let mut s = self.state.lock().expect("rate limit lock");
        let window_expired = match s.window_start {
            None => true,
            Some(start) => now.saturating_duration_since(start) >= self.window,
        };
        if window_expired {
            let suppressed_before = s.suppressed + self.fast_suppressed.swap(0, Ordering::Relaxed);
            s.window_start = Some(now);
            s.suppressed = 0;
            if self.capacity == 0 {
                s.allowed_in_window = 0;
                s.suppressed = 1;
                self.arm(now);
                return Decision::Suppress;
            }
            s.allowed_in_window = 1;
            self.saturated.store(false, Ordering::Relaxed);
            return Decision::Allow { suppressed_before };
        }
        if s.allowed_in_window < self.capacity {
            s.allowed_in_window += 1;
            Decision::Allow {
                suppressed_before: 0,
            }
        } else {
            s.suppressed += 1;
            if let Some(start) = s.window_start {
                self.arm(start);
            }
            Decision::Suppress
        }
    }

    /// Arm the fast path for the window that started at `window_start`.
    fn arm(&self, window_start: Instant) {
        self.window_end_nanos.store(
            nanos_since_base(window_start + self.window),
            Ordering::Relaxed,
        );
        self.saturated.store(true, Ordering::Relaxed);
    }
}

/// `tracing::warn!` behind a [`RateLimit`]. When events were suppressed
/// since the last allowed one, the emitted line carries a `suppressed`
/// field with the count.
#[macro_export]
macro_rules! rate_limited_warn {
    ($limiter:expr, $($arg:tt)+) => {
        match $limiter.check() {
            $crate::telemetry::Decision::Allow { suppressed_before } => {
                if suppressed_before > 0 {
                    ::tracing::warn!(suppressed = suppressed_before, $($arg)+);
                } else {
                    ::tracing::warn!($($arg)+);
                }
            }
            $crate::telemetry::Decision::Suppress => {}
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_capacity_then_suppresses() {
        let limit = RateLimit::new(2, Duration::from_secs(10));
        let t0 = Instant::now();
        assert_eq!(
            limit.check_at(t0),
            Decision::Allow {
                suppressed_before: 0
            }
        );
        assert_eq!(
            limit.check_at(t0 + Duration::from_secs(1)),
            Decision::Allow {
                suppressed_before: 0
            }
        );
        for i in 2..5 {
            assert_eq!(
                limit.check_at(t0 + Duration::from_secs(i)),
                Decision::Suppress
            );
        }
        // New window: allowed again, carrying the suppressed count.
        assert_eq!(
            limit.check_at(t0 + Duration::from_secs(10)),
            Decision::Allow {
                suppressed_before: 3
            }
        );
        // And the fresh window counts from one.
        assert_eq!(
            limit.check_at(t0 + Duration::from_secs(11)),
            Decision::Allow {
                suppressed_before: 0
            }
        );
        assert_eq!(
            limit.check_at(t0 + Duration::from_secs(12)),
            Decision::Suppress
        );
    }

    #[test]
    fn fast_path_suppression_preserves_the_carried_count() {
        // After saturation, most suppressions take the lock-free fast path;
        // their count must still be folded into the next window's first event.
        let limit = RateLimit::new(1, Duration::from_secs(10));
        let t0 = Instant::now();
        assert_eq!(
            limit.check_at(t0),
            Decision::Allow {
                suppressed_before: 0
            }
        );
        for i in 1..=100 {
            assert_eq!(
                limit.check_at(t0 + Duration::from_millis(i)),
                Decision::Suppress
            );
        }
        assert_eq!(
            limit.check_at(t0 + Duration::from_secs(10)),
            Decision::Allow {
                suppressed_before: 100
            }
        );
    }

    #[test]
    fn zero_capacity_suppresses_everything() {
        let limit = RateLimit::new(0, Duration::from_secs(1));
        let t0 = Instant::now();
        assert_eq!(limit.check_at(t0), Decision::Suppress);
        assert_eq!(
            limit.check_at(t0 + Duration::from_secs(2)),
            Decision::Suppress
        );
    }

    #[test]
    fn usable_from_a_static_via_the_macro() {
        static LIMIT: RateLimit = RateLimit::new(1, Duration::from_secs(60));
        // No subscriber installed: the macro must still be safe to call.
        rate_limited_warn!(LIMIT, code = 7, "first is allowed");
        rate_limited_warn!(LIMIT, code = 8, "second is suppressed");
    }

    #[test]
    fn init_is_idempotent() {
        // Whichever test initializes first wins; both calls must be safe.
        let _ = init(LogFormat::Pretty, "warn");
        let second = init(LogFormat::Json, "warn");
        assert!(!second, "second init reports already-installed");
    }
}
