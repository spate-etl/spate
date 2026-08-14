//! Wall time, and the one place it is converted for display.
//!
//! Records always carry nanoseconds. Adaptive units (ns, µs, ms, s) are a
//! rendering concern and live here, applied when a table is written and never
//! when a number is stored, so two reports of the same run cannot disagree
//! about a value because one of them rounded it to milliseconds first.

use std::time::Instant;

/// A monotonic stopwatch over the measured region.
#[derive(Debug, Clone, Copy)]
pub struct Stopwatch {
    started: Instant,
}

impl Stopwatch {
    /// Starts timing.
    #[must_use]
    pub fn start() -> Self {
        Self {
            started: Instant::now(),
        }
    }

    /// Nanoseconds since [`Stopwatch::start`], saturating at `u64::MAX`.
    ///
    /// Saturating rather than wrapping: a measured region cannot run for 584
    /// years, so the saturation is unreachable, and it is here only so the
    /// conversion out of `u128` needs no `expect` on the per-record path.
    #[must_use]
    pub fn elapsed_ns(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// Renders a nanosecond count with the largest unit that keeps it readable.
///
/// Rendering belongs to the driver; a bench target compiles the stopwatch above
/// and nothing else from this module.
///
/// The thresholds are one full unit each, so `999 ns` stays in nanoseconds and
/// `1000 ns` becomes `1.00 µs`.
#[cfg(feature = "driver")]
#[must_use]
pub fn human_ns(ns: f64) -> String {
    let abs = ns.abs();
    if abs < 1e3 {
        format!("{ns:.1} ns")
    } else if abs < 1e6 {
        format!("{:.2} µs", ns / 1e3)
    } else if abs < 1e9 {
        format!("{:.2} ms", ns / 1e6)
    } else {
        format!("{:.3} s", ns / 1e9)
    }
}

/// Renders a byte count with a binary unit.
#[cfg(feature = "driver")]
#[must_use]
pub fn human_bytes(bytes: f64) -> String {
    const KIB: f64 = 1024.0;
    let abs = bytes.abs();
    if abs < KIB {
        format!("{bytes:.0} B")
    } else if abs < KIB * KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else if abs < KIB * KIB * KIB {
        format!("{:.1} MiB", bytes / (KIB * KIB))
    } else {
        format!("{:.2} GiB", bytes / (KIB * KIB * KIB))
    }
}

/// Renders a rate with an SI prefix: 10^3, not 2^10, because a rate of
/// records per second has no binary meaning.
#[cfg(feature = "driver")]
#[must_use]
pub fn human_rate(per_s: f64) -> String {
    let abs = per_s.abs();
    if abs < 1e3 {
        format!("{per_s:.1}")
    } else if abs < 1e6 {
        format!("{:.2}k", per_s / 1e3)
    } else if abs < 1e9 {
        format!("{:.2}M", per_s / 1e6)
    } else {
        format!("{:.2}G", per_s / 1e9)
    }
}

#[cfg(all(test, feature = "driver"))]
mod tests {
    use super::{human_bytes, human_ns, human_rate};

    #[test]
    fn units_switch_at_a_full_unit_and_not_before() {
        assert_eq!(human_ns(999.0), "999.0 ns");
        assert_eq!(human_ns(1000.0), "1.00 µs");
        assert_eq!(human_ns(1.5e6), "1.50 ms");
        assert_eq!(human_ns(2.0e9), "2.000 s");

        assert_eq!(human_bytes(512.0), "512 B");
        assert_eq!(human_bytes(2048.0), "2.0 KiB");
        assert_eq!(human_bytes(3.0 * 1024.0 * 1024.0), "3.0 MiB");

        assert_eq!(human_rate(999.0), "999.0");
        assert_eq!(human_rate(1500.0), "1.50k");
        assert_eq!(human_rate(2.5e6), "2.50M");
    }

    #[test]
    fn a_negative_value_keeps_its_sign_and_its_unit() {
        assert_eq!(human_ns(-1500.0), "-1.50 µs");
        assert_eq!(human_bytes(-2048.0), "-2.0 KiB");
    }
}
