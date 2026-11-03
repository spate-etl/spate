//! A load-time check that a duration lies within its bounds.
//!
//! [`check_duration`] compares a value against [`DurationBounds`] and returns
//! a [`DurationBoundsError`] whose message states the bounds and the value
//! found but not the key. The caller puts the key in front of it, and the
//! reason for the bound after it, so every section refuses a duration in one
//! wording.

use humantime_serde::re::humantime::format_duration;
use std::fmt;
use std::time::Duration;

/// The range a duration must fall in: a floor, an optional ceiling, and
/// whether `0s` is also accepted.
///
/// Both ends are inclusive. The comparison is on the whole [`Duration`], so a
/// sub-millisecond part counts and nothing is truncated to a unit first. The
/// fields are private so a later rule, such as whole seconds, can be added
/// without breaking a caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurationBounds {
    floor: Duration,
    ceiling: Option<Duration>,
    or_zero: bool,
}

impl DurationBounds {
    /// Bounds that accept `floor` and anything longer.
    #[must_use]
    pub const fn at_least(floor: Duration) -> Self {
        Self {
            floor,
            ceiling: None,
            or_zero: false,
        }
    }

    /// These bounds, also refusing anything longer than `ceiling`.
    ///
    /// # Panics
    ///
    /// If `ceiling` is shorter than the floor. In a `const` item this fails
    /// to compile.
    #[must_use]
    pub const fn at_most(self, ceiling: Duration) -> Self {
        // `Duration`'s ordering is not usable in a `const fn`.
        assert!(
            self.floor.as_nanos() <= ceiling.as_nanos(),
            "a duration ceiling must not be below its floor"
        );
        Self {
            ceiling: Some(ceiling),
            ..self
        }
    }

    /// These bounds, also accepting exactly `0s`, which a caller uses when
    /// zero turns the setting off.
    ///
    /// # Panics
    ///
    /// If the floor is zero, which already accepts `0s`. In a `const` item
    /// this fails to compile.
    #[must_use]
    pub const fn or_zero(self) -> Self {
        assert!(
            !self.floor.is_zero(),
            "or_zero needs a non-zero floor, since a zero floor accepts 0s"
        );
        Self {
            or_zero: true,
            ..self
        }
    }
}

/// A duration found outside its [`DurationBounds`].
///
/// Its message states the bounds and the value, without the key, such as
/// `must be from 20s to 1day; found 5s`. The caller composes
/// `` "`feed.settling_window` {error}" ``, and adds the reason for the bound
/// or the meaning of `0s` after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("must be {}; found {}", Range(.bounds), format_duration(*.value))]
pub struct DurationBoundsError {
    value: Duration,
    bounds: DurationBounds,
}

/// The accepted range of a [`DurationBounds`], as its error states it.
struct Range<'a>(&'a DurationBounds);

impl fmt::Display for Range<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let DurationBounds {
            floor,
            ceiling,
            or_zero,
        } = *self.0;
        let zero = if or_zero { "0s or " } else { "" };
        let floor = format_duration(floor);
        match ceiling {
            Some(ceiling) => write!(f, "{zero}from {floor} to {}", format_duration(ceiling)),
            None => write!(f, "{zero}at least {floor}"),
        }
    }
}

/// Checks that `value` lies within `bounds`.
///
/// # Errors
///
/// [`DurationBoundsError`] when `value` is below the floor or above the
/// ceiling, unless it is `0s` and the bounds accept zero.
///
/// # Examples
///
/// ```
/// use spate_core::config::{DurationBounds, check_duration};
/// use std::time::Duration;
///
/// const WINDOW: DurationBounds =
///     DurationBounds::at_least(Duration::from_secs(20)).at_most(Duration::from_secs(3600));
///
/// assert!(check_duration(Duration::from_secs(60), WINDOW).is_ok());
/// let err = check_duration(Duration::from_secs(5), WINDOW).unwrap_err();
/// assert_eq!(
///     format!("`window` {err}"),
///     "`window` must be from 20s to 1h; found 5s"
/// );
/// ```
pub fn check_duration(value: Duration, bounds: DurationBounds) -> Result<(), DurationBoundsError> {
    if bounds.or_zero && value.is_zero() {
        return Ok(());
    }
    if value < bounds.floor || bounds.ceiling.is_some_and(|ceiling| value > ceiling) {
        return Err(DurationBoundsError { value, bounds });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);
    const NS: Duration = Duration::from_nanos(1);
    const DAY: Duration = Duration::from_secs(24 * 60 * 60);

    fn refused(value: Duration, bounds: DurationBounds) -> String {
        check_duration(value, bounds)
            .expect_err(&format!("{value:?} should be refused"))
            .to_string()
    }

    fn accepted(value: Duration, bounds: DurationBounds) {
        assert_eq!(check_duration(value, bounds), Ok(()), "{value:?}");
    }

    /// Pins each end of each bounds shape, inclusive, on the whole
    /// `Duration`, and the four message forms by equality.
    #[test]
    fn check_duration_refuses_each_boundary_class() {
        const RANGE: DurationBounds =
            DurationBounds::at_least(Duration::from_millis(100)).at_most(DAY);
        accepted(Duration::from_millis(100), RANGE);
        assert_eq!(
            refused(Duration::from_millis(100) - NS, RANGE),
            "must be from 100ms to 1day; found 99ms 999us 999ns"
        );
        accepted(DAY, RANGE);
        assert_eq!(
            refused(DAY + NS, RANGE),
            "must be from 100ms to 1day; found 1day 1ns"
        );
        assert_eq!(
            refused(Duration::ZERO, RANGE),
            "must be from 100ms to 1day; found 0s"
        );
        assert_eq!(
            refused(Duration::MAX, RANGE),
            "must be from 100ms to 1day; found \
             584542046090years 7months 15days 17h 5m 3s 999ms 999us 999ns"
        );

        const FLOOR: DurationBounds = DurationBounds::at_least(Duration::from_millis(100));
        accepted(Duration::MAX, FLOOR);
        assert_eq!(
            refused(Duration::from_micros(99_500), FLOOR),
            "must be at least 100ms; found 99ms 500us"
        );

        const STATISTICS: DurationBounds = DurationBounds::at_least(MS).at_most(DAY).or_zero();
        accepted(Duration::ZERO, STATISTICS);
        assert_eq!(
            refused(NS, STATISTICS),
            "must be 0s or from 1ms to 1day; found 1ns"
        );
        assert_eq!(
            refused(Duration::from_micros(999), STATISTICS),
            "must be 0s or from 1ms to 1day; found 999us"
        );
        accepted(MS, STATISTICS);
        assert_eq!(
            refused(DAY + NS, STATISTICS),
            "must be 0s or from 1ms to 1day; found 1day 1ns"
        );

        const ZERO_OR_FLOOR: DurationBounds = DurationBounds::at_least(MS).or_zero();
        assert_eq!(
            refused(Duration::from_micros(500), ZERO_OR_FLOOR),
            "must be 0s or at least 1ms; found 500us"
        );

        const COMMIT: DurationBounds = DurationBounds::at_least(MS).at_most(DAY);
        assert_eq!(
            refused(Duration::ZERO, COMMIT),
            "must be from 1ms to 1day; found 0s"
        );
        accepted(DAY, COMMIT);

        const MESSAGE_TIMEOUT: DurationBounds =
            DurationBounds::at_least(MS).at_most(Duration::from_millis(i32::MAX as u64));
        assert_eq!(
            refused(Duration::from_millis(i32::MAX as u64) + NS, MESSAGE_TIMEOUT),
            "must be from 1ms to 24days 20h 31m 23s 647ms; found 24days 20h 31m 23s 647ms 1ns"
        );

        let one_second = Duration::from_secs(1);
        accepted(
            one_second,
            DurationBounds::at_least(one_second).at_most(one_second),
        );
    }

    /// Pins that a ceiling below its floor is refused when the bounds are
    /// built.
    #[test]
    #[should_panic(expected = "a duration ceiling must not be below its floor")]
    fn an_inverted_pair_panics_at_construction() {
        let floor = Duration::from_secs(2);
        let _ = DurationBounds::at_least(floor).at_most(floor - NS);
    }

    /// Pins that `or_zero` on a zero floor, which already accepts `0s`, is
    /// refused when the bounds are built.
    #[test]
    #[should_panic(expected = "or_zero needs a non-zero floor")]
    fn or_zero_on_a_zero_floor_panics_at_construction() {
        let floor = Duration::ZERO;
        let _ = DurationBounds::at_least(floor).or_zero();
    }
}
