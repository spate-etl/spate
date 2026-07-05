//! Structured logging: `tracing` initialisation (JSON for Kubernetes) and
//! rate-limited hot-path logging helpers.
//!
//! # Rate limiting on the hot path
//!
//! A poison-message storm that logs per record will destroy the pinned
//! pipeline threads. Hot-path warnings must go through a [`RateLimit`],
//! most conveniently via [`rate_limited_warn!`](crate::rate_limited_warn):
//!
//! ```
//! use etl_core::rate_limited_warn;
//! use etl_core::telemetry::RateLimit;
//! use std::time::Duration;
//!
//! static DESER_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));
//!
//! // In the record loop:
//! rate_limited_warn!(DESER_WARN, reason = "malformed", "payload skipped");
//! ```

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Output format for logs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum LogFormat {
    /// One JSON object per line with flattened event fields — the shape
    /// Kubernetes log pipelines expect. Default.
    #[default]
    Json,
    /// Human-readable output for local development.
    Pretty,
}

/// Initialise the global `tracing` subscriber.
///
/// The filter comes from `RUST_LOG` when set, else `default_filter`
/// (e.g. `"info,etl_core=debug"`). Idempotent: returns `true` if this call
/// installed the subscriber, `false` if one (ours or foreign) was already
/// installed — never panics, so libraries and tests can call it freely.
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
/// `const`-constructible for use in `static`s. The mutex is uncontended in
/// practice (one callsite, brief hold) and only reached at all when code
/// decides to log — the happy path of a healthy pipeline never gets here.
#[derive(Debug)]
pub struct RateLimit {
    capacity: u32,
    window: Duration,
    state: Mutex<RateLimitState>,
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
        }
    }

    /// Decide whether to log an event happening now.
    pub fn check(&self) -> Decision {
        self.check_at(Instant::now())
    }

    /// Decide for an event at `now` (injectable for tests).
    pub fn check_at(&self, now: Instant) -> Decision {
        let mut s = self.state.lock().expect("rate limit lock");
        let window_expired = match s.window_start {
            None => true,
            Some(start) => now.saturating_duration_since(start) >= self.window,
        };
        if window_expired {
            let suppressed_before = s.suppressed;
            s.window_start = Some(now);
            s.suppressed = 0;
            if self.capacity == 0 {
                s.allowed_in_window = 0;
                s.suppressed = 1;
                return Decision::Suppress;
            }
            s.allowed_in_window = 1;
            return Decision::Allow { suppressed_before };
        }
        if s.allowed_in_window < self.capacity {
            s.allowed_in_window += 1;
            Decision::Allow {
                suppressed_before: 0,
            }
        } else {
            s.suppressed += 1;
            Decision::Suppress
        }
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
        // Whichever test initialises first wins; both calls must be safe.
        let _ = init(LogFormat::Pretty, "warn");
        let second = init(LogFormat::Json, "warn");
        assert!(!second, "second init reports already-installed");
    }
}
