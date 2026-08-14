//! `time` crate types for date/time columns, mirroring
//! `clickhouse::serde::time` module-for-module:
//!
//! | Module | Rust field | Column |
//! |---|---|---|
//! | [`date`] | `time::Date` | `Date` |
//! | [`date32`] | `time::Date` | `Date32` |
//! | [`datetime`] | `time::OffsetDateTime` | `DateTime` |
//! | [`datetime64::secs`]/[`millis`](datetime64::millis)/[`micros`](datetime64::micros)/[`nanos`](datetime64::nanos) | `time::OffsetDateTime` | `DateTime64(0/3/6/9)` |
//! | [`time`] | `time::Duration` | `Time` |
//! | [`time64::secs`]/[`millis`](time64::millis)/[`micros`](time64::micros)/[`nanos`](time64::nanos) | `time::Duration` | `Time64(0/3/6/9)` |
//!
//! Semantics match [`crate::serde::chrono`]: out-of-range values fail as
//! record-level errors, each module has an `option` submodule, and the
//! attribute is required: the `time` crate's default serde impls write
//! forms no date/time column reads correctly.

use ::time::{Date, OffsetDateTime};
use serde::de::Error as _;
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// `time::OffsetDateTime` ↔ `DateTime` (`UInt32` seconds since epoch).
pub mod datetime {
    use super::*;

    /// Serialize as `UInt32` seconds since the Unix epoch.
    pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ts = dt.unix_timestamp();
        u32::try_from(ts)
            .map_err(|_| S::Error::custom(format!("{dt} cannot be represented as DateTime")))?
            .serialize(serializer)
    }

    /// Deserialize from `UInt32` seconds since the Unix epoch.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts = u32::deserialize(deserializer)?;
        OffsetDateTime::from_unix_timestamp(i64::from(ts)).map_err(D::Error::custom)
    }

    crate::serde::option_module!(::time::OffsetDateTime);
}

/// `time::OffsetDateTime` ↔ `DateTime64(p)` (`Int64` ticks since epoch).
pub mod datetime64 {
    use super::*;

    fn do_serialize<S>(dt: &OffsetDateTime, div: i128, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ts = dt.unix_timestamp_nanos() / div;
        i64::try_from(ts)
            .map_err(|_| S::Error::custom(format!("{dt} cannot be represented as DateTime64")))?
            .serialize(serializer)
    }

    fn do_deserialize<'de, D>(deserializer: D, mul: i128) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts = i64::deserialize(deserializer)?;
        // Cannot overflow: `mul` fits in i64.
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(ts) * mul).map_err(D::Error::custom)
    }

    macro_rules! datetime64_precision {
        ($(#[$doc:meta])* $mod_name:ident, nanos_per_tick = $div:literal) => {
            $(#[$doc])*
            pub mod $mod_name {
                use super::*;

                /// Serialize as `Int64` ticks at this column precision.
                pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: Serializer,
                {
                    do_serialize(dt, $div, serializer)
                }

                /// Deserialize from `Int64` ticks at this column precision.
                pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    do_deserialize(deserializer, $div)
                }

                crate::serde::option_module!(::time::OffsetDateTime);
            }
        };
    }

    datetime64_precision! {
        /// `DateTime64(0)` — seconds.
        secs, nanos_per_tick = 1_000_000_000
    }
    datetime64_precision! {
        /// `DateTime64(3)` — milliseconds.
        millis, nanos_per_tick = 1_000_000
    }
    datetime64_precision! {
        /// `DateTime64(6)` — microseconds.
        micros, nanos_per_tick = 1_000
    }
    datetime64_precision! {
        /// `DateTime64(9)` — nanoseconds.
        nanos, nanos_per_tick = 1
    }
}

/// `time::Date` ↔ `Date` (`UInt16` days since epoch).
pub mod date {
    use super::*;
    use ::time::Duration;

    const ORIGIN: Result<Date, ::time::error::ComponentRange> = Date::from_ordinal_date(1970, 1);

    /// Serialize as `UInt16` days since 1970-01-01.
    pub fn serialize<S>(d: &Date, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let origin = ORIGIN.unwrap();
        if *d < origin {
            return Err(S::Error::custom(format!(
                "{d} cannot be represented as Date"
            )));
        }
        let days = (*d - origin).whole_days();
        u16::try_from(days)
            .map_err(|_| S::Error::custom(format!("{d} cannot be represented as Date")))?
            .serialize(serializer)
    }

    /// Deserialize from `UInt16` days since 1970-01-01.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Date, D::Error>
    where
        D: Deserializer<'de>,
    {
        let days = u16::deserialize(deserializer)?;
        // Cannot overflow: always far below Date::MAX.
        Ok(ORIGIN.unwrap() + Duration::days(i64::from(days)))
    }

    crate::serde::option_module!(::time::Date);
}

/// `time::Date` ↔ `Date32` (`Int32` days since epoch).
pub mod date32 {
    use super::*;
    use ::time::Duration;

    const ORIGIN: Result<Date, ::time::error::ComponentRange> = Date::from_ordinal_date(1970, 1);
    // The server's documented Date32 range (22.8+).
    const MIN: Result<Date, ::time::error::ComponentRange> = Date::from_ordinal_date(1900, 1);
    const MAX: Result<Date, ::time::error::ComponentRange> = Date::from_ordinal_date(2299, 365);

    /// Serialize as `Int32` days since 1970-01-01 (server range
    /// 1900-01-01 through 2299-12-31).
    pub fn serialize<S>(d: &Date, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if *d < MIN.unwrap() || *d > MAX.unwrap() {
            return Err(S::Error::custom(format!(
                "{d} cannot be represented as Date32"
            )));
        }
        let days = (*d - ORIGIN.unwrap()).whole_days();
        i32::try_from(days)
            .map_err(|_| S::Error::custom(format!("{d} cannot be represented as Date32")))?
            .serialize(serializer)
    }

    /// Deserialize from `Int32` days since 1970-01-01.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Date, D::Error>
    where
        D: Deserializer<'de>,
    {
        let days = i32::deserialize(deserializer)?;
        // Cannot overflow: the server clamps to the Date32 range.
        Ok(ORIGIN.unwrap() + Duration::days(i64::from(days)))
    }

    crate::serde::option_module!(::time::Date);
}

/// `time::Duration` ↔ `Time` (`Int32` seconds).
#[allow(clippy::module_inception)]
pub mod time {
    use super::*;
    use ::time::Duration;

    /// Serialize as `Int32` whole seconds.
    pub fn serialize<S>(t: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        i32::try_from(t.whole_seconds())
            .map_err(|_| S::Error::custom(format!("{t} cannot be represented as Time")))?
            .serialize(serializer)
    }

    /// Deserialize from `Int32` whole seconds.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let seconds = i32::deserialize(deserializer)?;
        Ok(Duration::seconds(i64::from(seconds)))
    }

    crate::serde::option_module!(::time::Duration);
}

/// `time::Duration` ↔ `Time64(p)` (`Int64` ticks).
pub mod time64 {
    use super::*;
    use ::time::Duration;

    macro_rules! time64_precision {
        ($(#[$doc:meta])* $mod_name:ident, whole = $whole:ident, build = $build:ident) => {
            $(#[$doc])*
            pub mod $mod_name {
                use super::*;

                /// Serialize as `Int64` ticks at this column precision.
                pub fn serialize<S>(t: &Duration, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: Serializer,
                {
                    let ticks = i64::try_from(i128::from(t.$whole())).map_err(|_| {
                        S::Error::custom(format!(
                            "{t} has too many ticks for this Time64 precision"
                        ))
                    })?;
                    ticks.serialize(serializer)
                }

                /// Deserialize from `Int64` ticks at this column precision.
                pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    let ticks = i64::deserialize(deserializer)?;
                    Ok(Duration::$build(ticks))
                }

                crate::serde::option_module!(::time::Duration);
            }
        };
    }

    time64_precision! {
        /// `Time64(0)` — seconds.
        secs, whole = whole_seconds, build = seconds
    }
    time64_precision! {
        /// `Time64(3)` — milliseconds.
        millis, whole = whole_milliseconds, build = milliseconds
    }
    time64_precision! {
        /// `Time64(6)` — microseconds.
        micros, whole = whole_microseconds, build = microseconds
    }
    time64_precision! {
        /// `Time64(9)` — nanoseconds.
        nanos, whole = whole_nanoseconds, build = nanoseconds
    }
}

#[cfg(test)]
mod tests {
    use crate::rowbinary::{RowBinaryError, serialize_row};
    use ::time::macros::{date, datetime};
    use ::time::{Date, Duration, OffsetDateTime};
    use bytes::BytesMut;
    use serde::Serialize;

    fn enc<T: Serialize>(v: &T) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_row(v, &mut buf).expect("serialize");
        buf.to_vec()
    }

    fn enc_err<T: Serialize>(v: &T) -> RowBinaryError {
        serialize_row(v, &mut BytesMut::new()).unwrap_err()
    }

    #[derive(Serialize)]
    struct DateRow(#[serde(with = "crate::serde::time::date")] Date);
    #[derive(Serialize)]
    struct Date32Row(#[serde(with = "crate::serde::time::date32")] Date);
    #[derive(Serialize)]
    struct DtRow(#[serde(with = "crate::serde::time::datetime")] OffsetDateTime);
    #[derive(Serialize)]
    struct Dt64MicrosRow(#[serde(with = "crate::serde::time::datetime64::micros")] OffsetDateTime);
    #[derive(Serialize)]
    struct TimeRow(#[serde(with = "crate::serde::time::time")] Duration);
    #[derive(Serialize)]
    struct Time64NanosRow(#[serde(with = "crate::serde::time::time64::nanos")] Duration);
    #[derive(Serialize)]
    struct OptDtRow(#[serde(with = "crate::serde::time::datetime::option")] Option<OffsetDateTime>);

    #[test]
    fn dates_are_days_since_epoch() {
        assert_eq!(enc(&DateRow(date!(1970 - 01 - 02))), 1u16.to_le_bytes());
        let old = date!(1900 - 01 - 01);
        assert_eq!(enc(&Date32Row(old)), (-25567i32).to_le_bytes());
        assert!(matches!(enc_err(&DateRow(old)), RowBinaryError::Custom(_)));
        assert!(matches!(
            enc_err(&Date32Row(date!(1899 - 12 - 31))),
            RowBinaryError::Custom(_)
        ));
    }

    #[test]
    fn datetimes_are_epoch_ticks() {
        let dt = datetime!(2023-11-14 22:13:20 UTC);
        assert_eq!(dt.unix_timestamp(), 1_700_000_000);
        assert_eq!(enc(&DtRow(dt)), 1_700_000_000u32.to_le_bytes());
        assert_eq!(
            enc(&Dt64MicrosRow(dt)),
            1_700_000_000_000_000i64.to_le_bytes()
        );
        let pre = datetime!(1969-12-31 23:59:59 UTC);
        assert!(matches!(enc_err(&DtRow(pre)), RowBinaryError::Custom(_)));
        assert_eq!(enc(&Dt64MicrosRow(pre)), (-1_000_000i64).to_le_bytes());
    }

    #[test]
    fn durations_are_time_ticks() {
        assert_eq!(
            enc(&TimeRow(Duration::seconds(-3599))),
            (-3599i32).to_le_bytes()
        );
        assert_eq!(
            enc(&Time64NanosRow(Duration::microseconds(2))),
            2_000i64.to_le_bytes()
        );
    }

    #[test]
    fn nullable_time_columns_compose_with_the_option_guard() {
        let dt = datetime!(1970-01-01 0:00:07 UTC);
        assert_eq!(enc(&OptDtRow(Some(dt))), [0, 7, 0, 0, 0]);
        assert_eq!(enc(&OptDtRow(None)), [1]);
    }
}
