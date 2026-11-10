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
//!
//! static DESER_WARN: RateLimit = RateLimit::hot_path();
//!
//! // In the record loop:
//! rate_limited_warn!(DESER_WARN, reason = "malformed", "payload skipped");
//! ```

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::SubscriberBuilder;
use tracing_subscriber::fmt::format::{DefaultFields, FormatFields, Writer};

use crate::clock::{Clock, SystemClock};

/// Monotonic reference point shared by every [`RateLimit`], so a window
/// deadline can be stored as a plain `AtomicU64` of nanoseconds and compared
/// on the lock-free fast path.
///
/// On the system clock: every limiter is a process-wide `static`, so no
/// pipeline can hand it a clock of its own.
fn base_instant() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(|| SystemClock.now())
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
    /// Human-readable output for local development, one line per event.
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
        LogFormat::Pretty => pretty().with_env_filter(filter).try_init().is_ok(),
    }
}

/// The subscriber behind [`LogFormat::Pretty`], writing to stdout.
fn pretty() -> SubscriberBuilder<OneLineFields> {
    tracing_subscriber::fmt()
        .with_target(true)
        .fmt_fields(OneLineFields::default())
}

/// [`DefaultFields`] with every control character and line separator written
/// as an escape, so a field's text cannot start a log line of its own or move
/// a terminal's cursor to fake one. `DefaultFields` leaves these raw in a named
/// field.
#[derive(Default)]
struct OneLineFields(DefaultFields);

impl<'writer> FormatFields<'writer> for OneLineFields {
    fn format_fields<R: RecordFields>(&self, writer: Writer<'writer>, fields: R) -> fmt::Result {
        let mut escaping = EscapeControl(writer);
        self.0.format_fields(Writer::new(&mut escaping), fields)
    }
}

struct EscapeControl<'writer>(Writer<'writer>);

impl EscapeControl<'_> {
    fn is_escaped(c: char) -> bool {
        c.is_control() || matches!(c, '\u{2028}' | '\u{2029}')
    }
}

impl fmt::Write for EscapeControl<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let mut rest = text;
        while let Some(at) = rest.find(Self::is_escaped) {
            let (clean, tail) = rest.split_at(at);
            self.0.write_str(clean)?;
            let mut chars = tail.chars();
            if let Some(c) = chars.next() {
                write!(self.0, "{}", c.escape_default())?;
            }
            rest = chars.as_str();
        }
        self.0.write_str(rest)
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

    /// The limiter for a warning on the record path: five events per ten
    /// seconds.
    #[must_use]
    pub const fn hot_path() -> Self {
        RateLimit::new(5, Duration::from_secs(10))
    }

    /// Decide whether to log an event happening now, on the system clock.
    pub fn check(&self) -> Decision {
        self.check_at(SystemClock.now())
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

/// An in-memory writer for a test's subscriber. Clones share the buffer.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct LogBuf(std::sync::Arc<Mutex<Vec<u8>>>);

#[cfg(test)]
impl LogBuf {
    pub(crate) fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

#[cfg(test)]
impl std::io::Write for LogBuf {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pretty_lines(f: impl FnOnce()) -> Vec<String> {
        let buf = LogBuf::default();
        let writer = buf.clone();
        let subscriber = pretty()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        buf.text().lines().map(str::to_owned).collect()
    }

    /// Pins that a line feed or carriage return in an event's message, its
    /// fields or a span's fields stays on the event's log line. Regression
    /// for #1699.
    #[test]
    fn pretty_keeps_an_event_on_one_line() {
        let text = "first\nsecond\rthird";
        let lines = pretty_lines(|| {
            let span = tracing::info_span!("work", unit = %text);
            let _in_span = span.enter();
            tracing::info!(reason = %text, quoted = ?text, "refused: {text}");
        });
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert_eq!(
            lines[0].matches("first\\nsecond\\rthird").count(),
            4,
            "{lines:#?}"
        );
    }

    /// Pins that a named field carries no raw control character or line
    /// separator, which a terminal could act on to fake a log line.
    #[test]
    fn pretty_escapes_every_control_character() {
        let text = "a\u{1b}[1Eb\u{b}c\u{85}d\u{2028}e\tf";
        let lines = pretty_lines(|| tracing::info!(reason = %text, "refused"));
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert!(
            lines[0].contains(r"a\u{1b}[1Eb\u{b}c\u{85}d\u{2028}e\tf"),
            "{lines:#?}"
        );
    }

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
    fn hot_path_allows_five_per_ten_seconds() {
        let limit = RateLimit::hot_path();
        let t0 = Instant::now();
        for _ in 0..5 {
            assert_eq!(
                limit.check_at(t0),
                Decision::Allow {
                    suppressed_before: 0
                }
            );
        }
        assert_eq!(limit.check_at(t0), Decision::Suppress);
        assert_eq!(
            limit.check_at(t0 + Duration::from_millis(9_999)),
            Decision::Suppress
        );
        assert_eq!(
            limit.check_at(t0 + Duration::from_secs(10)),
            Decision::Allow {
                suppressed_before: 2
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
