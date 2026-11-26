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

/// The range a duration must fall in: a floor, an optional ceiling, whether
/// `0s` is also accepted, and whether only whole seconds are.
///
/// The floor is inclusive unless the bounds come from [`above`](Self::above),
/// and the ceiling is inclusive. The comparison is on the whole [`Duration`],
/// so a sub-millisecond part counts and nothing is truncated to a unit first.
/// The fields are private so a later rule can be added without breaking a
/// caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurationBounds {
    floor: Duration,
    exclusive_floor: bool,
    ceiling: Option<Duration>,
    or_zero: bool,
    whole_seconds: bool,
}

impl DurationBounds {
    /// Bounds that accept `floor` and anything longer.
    #[must_use]
    pub const fn at_least(floor: Duration) -> Self {
        Self {
            floor,
            exclusive_floor: false,
            ceiling: None,
            or_zero: false,
            whole_seconds: false,
        }
    }

    /// Bounds that accept anything longer than `floor`, and refuse `floor`
    /// itself.
    ///
    /// # Panics
    ///
    /// If `floor` is [`Duration::MAX`], which no value is longer than. In a
    /// `const` item this fails to compile.
    #[must_use]
    pub const fn above(floor: Duration) -> Self {
        assert!(
            floor.as_nanos() < Duration::MAX.as_nanos(),
            "an exclusive duration floor must be below Duration::MAX"
        );
        Self {
            exclusive_floor: true,
            ..Self::at_least(floor)
        }
    }

    /// These bounds, also refusing anything longer than `ceiling`.
    ///
    /// # Panics
    ///
    /// If `ceiling` is shorter than the floor, equal to a floor from
    /// [`above`](Self::above), or not whole seconds in bounds that accept only
    /// whole seconds. In a `const` item this fails to compile.
    #[must_use]
    pub const fn at_most(self, ceiling: Duration) -> Self {
        // `Duration`'s ordering is not usable in a `const fn`.
        assert!(
            self.floor.as_nanos() <= ceiling.as_nanos(),
            "a duration ceiling must not be below its floor"
        );
        assert!(
            !self.exclusive_floor || self.floor.as_nanos() != ceiling.as_nanos(),
            "a duration ceiling must be above an exclusive floor"
        );
        assert!(
            !self.whole_seconds || ceiling.subsec_nanos() == 0,
            "a whole-seconds ceiling must be whole seconds"
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
    /// If the floor is zero: an inclusive zero floor already accepts `0s`,
    /// and an exclusive one would then accept every value. In a `const` item
    /// this fails to compile.
    #[must_use]
    pub const fn or_zero(self) -> Self {
        assert!(!self.floor.is_zero(), "or_zero needs a non-zero floor");
        Self {
            or_zero: true,
            ..self
        }
    }

    /// These bounds, also refusing a value with a part below one second.
    ///
    /// # Panics
    ///
    /// If the floor or the ceiling is not whole seconds. In a `const` item
    /// this fails to compile.
    #[must_use]
    pub const fn whole_seconds(self) -> Self {
        assert!(
            self.floor.subsec_nanos() == 0,
            "a whole-seconds floor must be whole seconds"
        );
        if let Some(ceiling) = self.ceiling {
            assert!(
                ceiling.subsec_nanos() == 0,
                "a whole-seconds ceiling must be whole seconds"
            );
        }
        Self {
            whole_seconds: true,
            ..self
        }
    }
}

/// A duration found outside its [`DurationBounds`].
///
/// Its message states the bounds and the value, without the key, such as
/// `must be from 20s to 1day; found 5s`. The caller composes
/// `"feed.settling_window: {error}"`, and adds the reason for the bound or
/// the meaning of `0s` after it.
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
            exclusive_floor,
            ceiling,
            or_zero,
            whole_seconds,
        } = *self.0;
        let zero = if or_zero { "0s or " } else { "" };
        let unit = if whole_seconds {
            " in whole seconds"
        } else {
            ""
        };
        let floor = format_duration(floor);
        match (exclusive_floor, ceiling.map(format_duration)) {
            (false, Some(ceiling)) => write!(f, "{zero}from {floor} to {ceiling}{unit}"),
            (false, None) => write!(f, "{zero}at least {floor}{unit}"),
            (true, Some(ceiling)) => {
                write!(f, "{zero}more than {floor} and at most {ceiling}{unit}")
            }
            (true, None) => write!(f, "{zero}more than {floor}{unit}"),
        }
    }
}

/// Checks that `value` lies within `bounds`.
///
/// # Errors
///
/// [`DurationBoundsError`] when `value` is below the floor, equal to a floor
/// from [`DurationBounds::above`], above the ceiling, or not whole seconds in
/// bounds that accept only whole seconds, unless it is `0s` and the bounds
/// accept zero.
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
///     format!("window: {err}"),
///     "window: must be from 20s to 1h; found 5s"
/// );
/// ```
pub fn check_duration(value: Duration, bounds: DurationBounds) -> Result<(), DurationBoundsError> {
    if bounds.or_zero && value.is_zero() {
        return Ok(());
    }
    let below = if bounds.exclusive_floor {
        value <= bounds.floor
    } else {
        value < bounds.floor
    };
    let past_ceiling = bounds.ceiling.is_some_and(|ceiling| value > ceiling);
    let fractional = bounds.whole_seconds && value.subsec_nanos() != 0;
    if below || past_ceiling || fractional {
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

    /// Pins each end of each bounds shape, inclusive or exclusive, on the
    /// whole `Duration`, and each message form by equality.
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

        const RESERVE: Duration = Duration::from_secs(5);
        const ABOVE: DurationBounds = DurationBounds::above(RESERVE);
        assert_eq!(refused(RESERVE, ABOVE), "must be more than 5s; found 5s");
        accepted(RESERVE + NS, ABOVE);
        accepted(Duration::MAX, ABOVE);
        const ABOVE_TO_DAY: DurationBounds = DurationBounds::above(RESERVE).at_most(DAY);
        assert_eq!(
            refused(RESERVE, ABOVE_TO_DAY),
            "must be more than 5s and at most 1day; found 5s"
        );
        accepted(DAY, ABOVE_TO_DAY);
        assert_eq!(
            refused(DAY + NS, ABOVE_TO_DAY),
            "must be more than 5s and at most 1day; found 1day 1ns"
        );
        const ZERO_OR_ABOVE: DurationBounds = DurationBounds::above(RESERVE).or_zero();
        accepted(Duration::ZERO, ZERO_OR_ABOVE);
        assert_eq!(
            refused(RESERVE, ZERO_OR_ABOVE),
            "must be 0s or more than 5s; found 5s"
        );
    }

    /// Pins that whole-seconds bounds refuse any part below a second at each
    /// end and inside the range, with the rule stated after the range.
    #[test]
    fn whole_seconds_refuses_a_part_below_a_second() {
        const HOUR: Duration = Duration::from_secs(60 * 60);
        const THREE: Duration = Duration::from_secs(3);
        const VISIBILITY: DurationBounds = DurationBounds::at_least(THREE)
            .at_most(Duration::from_secs(12 * 60 * 60))
            .whole_seconds();
        accepted(THREE, VISIBILITY);
        accepted(12 * HOUR, VISIBILITY);
        for (value, found) in [
            (Duration::from_secs(2), "2s"),
            (12 * HOUR + Duration::from_secs(1), "12h 1s"),
            (Duration::from_millis(2500), "2s 500ms"),
            (THREE + NS, "3s 1ns"),
            (12 * HOUR - NS, "11h 59m 59s 999ms 999us 999ns"),
        ] {
            assert_eq!(
                refused(value, VISIBILITY),
                format!("must be from 3s to 12h in whole seconds; found {found}")
            );
        }

        const FLOOR: DurationBounds = DurationBounds::at_least(THREE).whole_seconds();
        accepted(Duration::from_secs(4), FLOOR);
        assert_eq!(
            refused(Duration::from_millis(3500), FLOOR),
            "must be at least 3s in whole seconds; found 3s 500ms"
        );

        const ABOVE: DurationBounds = DurationBounds::above(THREE).whole_seconds();
        assert_eq!(
            refused(THREE, ABOVE),
            "must be more than 3s in whole seconds; found 3s"
        );
        accepted(Duration::from_secs(4), ABOVE);

        const ZERO_OR: DurationBounds = DurationBounds::at_least(Duration::from_secs(1))
            .at_most(HOUR)
            .or_zero()
            .whole_seconds();
        accepted(Duration::ZERO, ZERO_OR);
        assert_eq!(
            refused(Duration::from_millis(500), ZERO_OR),
            "must be 0s or from 1s to 1h in whole seconds; found 500ms"
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

    /// Pins that a ceiling equal to an exclusive floor is refused when the
    /// bounds are built.
    #[test]
    #[should_panic(expected = "a duration ceiling must be above an exclusive floor")]
    fn a_ceiling_at_an_exclusive_floor_panics_at_construction() {
        let floor = Duration::from_secs(5);
        let _ = DurationBounds::above(floor).at_most(floor);
    }

    /// Pins that an exclusive floor no value is longer than is refused when
    /// the bounds are built.
    #[test]
    #[should_panic(expected = "an exclusive duration floor must be below Duration::MAX")]
    fn an_exclusive_floor_at_the_maximum_panics_at_construction() {
        let floor = Duration::MAX;
        let _ = DurationBounds::above(floor);
    }

    /// Pins that `or_zero` on an exclusive zero floor, which would then accept
    /// every value, is refused when the bounds are built.
    #[test]
    #[should_panic(expected = "or_zero needs a non-zero floor")]
    fn or_zero_above_zero_panics_at_construction() {
        let floor = Duration::ZERO;
        let _ = DurationBounds::above(floor).or_zero();
    }

    /// Pins that whole-seconds bounds refuse a floor with a part below a
    /// second when they are built.
    #[test]
    #[should_panic(expected = "a whole-seconds floor must be whole seconds")]
    fn whole_seconds_on_a_fractional_floor_panics_at_construction() {
        let floor = Duration::from_millis(2500);
        let _ = DurationBounds::at_least(floor).whole_seconds();
    }

    /// Pins that `whole_seconds` refuses a ceiling, set before it, with a
    /// part below a second.
    #[test]
    #[should_panic(expected = "a whole-seconds ceiling must be whole seconds")]
    fn whole_seconds_after_a_fractional_ceiling_panics_at_construction() {
        let ceiling = Duration::from_secs(10) - NS;
        let _ = DurationBounds::at_least(Duration::from_secs(3))
            .at_most(ceiling)
            .whole_seconds();
    }

    /// Pins that `at_most` refuses a ceiling with a part below a second in
    /// bounds already set to whole seconds.
    #[test]
    #[should_panic(expected = "a whole-seconds ceiling must be whole seconds")]
    fn a_fractional_ceiling_after_whole_seconds_panics_at_construction() {
        let ceiling = Duration::from_secs(10) - NS;
        let _ = DurationBounds::at_least(Duration::from_secs(3))
            .whole_seconds()
            .at_most(ceiling);
    }
}
