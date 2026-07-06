//! `chrono` types for date/time columns, mirroring
//! `clickhouse::serde::chrono` module-for-module:
//!
//! | Module | Rust field | Column |
//! |---|---|---|
//! | [`date`] | `chrono::NaiveDate` | `Date` |
//! | [`date32`] | `chrono::NaiveDate` | `Date32` |
//! | [`datetime`] | `chrono::DateTime<Utc>` | `DateTime` |
//! | [`datetime64::secs`]/[`millis`](datetime64::millis)/[`micros`](datetime64::micros)/[`nanos`](datetime64::nanos) | `chrono::DateTime<Utc>` | `DateTime64(0/3/6/9)` |
//! | [`time`] | `chrono::Duration` | `Time` |
//! | [`time64::secs`]/[`millis`](time64::millis)/[`micros`](time64::micros)/[`nanos`](time64::nanos) | `chrono::Duration` | `Time64(0/3/6/9)` |
//!
//! Out-of-range values (a date before 1970 in a `Date` column, a
//! timestamp outside `u32` in a `DateTime`, ...) fail serialization with
//! a record-level error — never a panic. Each module has an `option`
//! submodule for `Nullable(...)` columns.
//!
//! ⚠ Like the other modules here, the attribute is load-bearing:
//! chrono's default serde impls write string forms, which encode
//! *successfully* into bytes no date/time column can read correctly.

use ::chrono::{DateTime, Utc};
use serde::de::Error as _;
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// `chrono::DateTime<Utc>` ↔ `DateTime` (`UInt32` seconds since epoch).
pub mod datetime {
    use super::*;

    /// Serialize as `UInt32` seconds since the Unix epoch.
    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ts = dt.timestamp();
        u32::try_from(ts)
            .map_err(|_| S::Error::custom(format!("{dt} cannot be represented as DateTime")))?
            .serialize(serializer)
    }

    /// Deserialize from `UInt32` seconds since the Unix epoch.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts = u32::deserialize(deserializer)?;
        DateTime::<Utc>::from_timestamp(i64::from(ts), 0)
            .ok_or_else(|| D::Error::custom(format!("{ts} cannot be converted to DateTime<Utc>")))
    }

    crate::serde::option_module!(::chrono::DateTime<::chrono::Utc>);
}

/// `chrono::DateTime<Utc>` ↔ `DateTime64(p)` (`Int64` ticks since epoch).
pub mod datetime64 {
    use super::*;

    macro_rules! datetime64_precision {
        ($(#[$doc:meta])* $mod_name:ident, to = $to:expr, from = $from:expr) => {
            $(#[$doc])*
            pub mod $mod_name {
                use super::*;

                /// Serialize as `Int64` ticks at this column precision.
                pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: Serializer,
                {
                    #[allow(clippy::redundant_closure_call)]
                    let ts: i64 = ($to)(dt).map_err(S::Error::custom)?;
                    ts.serialize(serializer)
                }

                /// Deserialize from `Int64` ticks at this column precision.
                pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    let ts = i64::deserialize(deserializer)?;
                    #[allow(clippy::redundant_closure_call)]
                    ($from)(ts).ok_or_else(|| {
                        D::Error::custom(format!("can't create DateTime<Utc> from {ts}"))
                    })
                }

                crate::serde::option_module!(::chrono::DateTime<::chrono::Utc>);
            }
        };
    }

    datetime64_precision! {
        /// `DateTime64(0)` — seconds.
        secs,
        to = |dt: &DateTime<Utc>| Ok::<_, String>(dt.timestamp()),
        from = |ts| DateTime::<Utc>::from_timestamp(ts, 0)
    }
    datetime64_precision! {
        /// `DateTime64(3)` — milliseconds.
        millis,
        to = |dt: &DateTime<Utc>| Ok::<_, String>(dt.timestamp_millis()),
        from = DateTime::<Utc>::from_timestamp_millis
    }
    datetime64_precision! {
        /// `DateTime64(6)` — microseconds.
        micros,
        to = |dt: &DateTime<Utc>| Ok::<_, String>(dt.timestamp_micros()),
        from = DateTime::<Utc>::from_timestamp_micros
    }
    datetime64_precision! {
        /// `DateTime64(9)` — nanoseconds.
        nanos,
        to = |dt: &DateTime<Utc>| dt
            .timestamp_nanos_opt()
            .ok_or_else(|| format!("{dt} cannot be represented as DateTime64(9)")),
        from = |ts| Some(DateTime::<Utc>::from_timestamp_nanos(ts))
    }
}

/// `chrono::NaiveDate` ↔ `Date` (`UInt16` days since epoch).
pub mod date {
    use super::*;
    use ::chrono::{Duration, NaiveDate};

    const ORIGIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1970, 1);

    /// Serialize as `UInt16` days since 1970-01-01.
    pub fn serialize<S>(d: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let origin = ORIGIN.unwrap();
        if *d < origin {
            return Err(S::Error::custom(format!(
                "{d} cannot be represented as Date"
            )));
        }
        let days = (*d - origin).num_days();
        u16::try_from(days)
            .map_err(|_| S::Error::custom(format!("{d} cannot be represented as Date")))?
            .serialize(serializer)
    }

    /// Deserialize from `UInt16` days since 1970-01-01.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let days = u16::deserialize(deserializer)?;
        // Cannot overflow: always far below NaiveDate::MAX.
        Ok(ORIGIN.unwrap() + Duration::days(i64::from(days)))
    }

    crate::serde::option_module!(::chrono::NaiveDate);
}

/// `chrono::NaiveDate` ↔ `Date32` (`Int32` days since epoch).
pub mod date32 {
    use super::*;
    use ::chrono::{Duration, NaiveDate};

    const ORIGIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1970, 1);
    // The server's documented Date32 range (22.8+).
    const MIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1900, 1);
    const MAX: Option<NaiveDate> = NaiveDate::from_yo_opt(2299, 365);

    /// Serialize as `Int32` days since 1970-01-01 (server range
    /// 1900-01-01 through 2299-12-31).
    pub fn serialize<S>(d: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if *d < MIN.unwrap() || *d > MAX.unwrap() {
            return Err(S::Error::custom(format!(
                "{d} cannot be represented as Date32"
            )));
        }
        let days = (*d - ORIGIN.unwrap()).num_days();
        i32::try_from(days)
            .map_err(|_| S::Error::custom(format!("{d} cannot be represented as Date32")))?
            .serialize(serializer)
    }

    /// Deserialize from `Int32` days since 1970-01-01.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let days = i32::deserialize(deserializer)?;
        // Cannot overflow: the server clamps to the Date32 range.
        Ok(ORIGIN.unwrap() + Duration::days(i64::from(days)))
    }

    crate::serde::option_module!(::chrono::NaiveDate);
}

/// `chrono::Duration` ↔ `Time` (`Int32` seconds).
pub mod time {
    use super::*;
    use ::chrono::Duration;

    /// Serialize as `Int32` whole seconds.
    pub fn serialize<S>(t: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        i32::try_from(t.num_seconds())
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

    crate::serde::option_module!(::chrono::Duration);
}

/// `chrono::Duration` ↔ `Time64(p)` (`Int64` ticks).
pub mod time64 {
    use super::*;
    use ::chrono::Duration;

    macro_rules! time64_precision {
        ($(#[$doc:meta])* $mod_name:ident, to = $to:expr, from = $from:expr) => {
            $(#[$doc])*
            pub mod $mod_name {
                use super::*;

                /// Serialize as `Int64` ticks at this column precision.
                pub fn serialize<S>(t: &Duration, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: Serializer,
                {
                    #[allow(clippy::redundant_closure_call)]
                    let ticks: i64 = ($to)(t).map_err(S::Error::custom)?;
                    ticks.serialize(serializer)
                }

                /// Deserialize from `Int64` ticks at this column precision.
                pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    let ticks = i64::deserialize(deserializer)?;
                    #[allow(clippy::redundant_closure_call)]
                    Ok(($from)(ticks))
                }

                crate::serde::option_module!(::chrono::Duration);
            }
        };
    }

    time64_precision! {
        /// `Time64(0)` — seconds.
        secs,
        to = |t: &Duration| Ok::<_, String>(t.num_seconds()),
        from = Duration::seconds
    }
    time64_precision! {
        /// `Time64(3)` — milliseconds.
        millis,
        to = |t: &Duration| Ok::<_, String>(t.num_milliseconds()),
        from = Duration::milliseconds
    }
    time64_precision! {
        /// `Time64(6)` — microseconds.
        micros,
        to = |t: &Duration| t
            .num_microseconds()
            .ok_or_else(|| format!("{t} has too many microseconds for Time64(6)")),
        from = Duration::microseconds
    }
    time64_precision! {
        /// `Time64(9)` — nanoseconds.
        nanos,
        to = |t: &Duration| t
            .num_nanoseconds()
            .ok_or_else(|| format!("{t} has too many nanoseconds for Time64(9)")),
        from = Duration::nanoseconds
    }
}

#[cfg(test)]
mod tests {
    use crate::rowbinary::{RowBinaryError, serialize_row};
    use ::chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
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
    struct DateRow(#[serde(with = "crate::serde::chrono::date")] NaiveDate);
    #[derive(Serialize)]
    struct Date32Row(#[serde(with = "crate::serde::chrono::date32")] NaiveDate);
    #[derive(Serialize)]
    struct DtRow(#[serde(with = "crate::serde::chrono::datetime")] DateTime<Utc>);
    #[derive(Serialize)]
    struct Dt64MillisRow(#[serde(with = "crate::serde::chrono::datetime64::millis")] DateTime<Utc>);
    #[derive(Serialize)]
    struct Dt64NanosRow(#[serde(with = "crate::serde::chrono::datetime64::nanos")] DateTime<Utc>);
    #[derive(Serialize)]
    struct TimeRow(#[serde(with = "crate::serde::chrono::time")] Duration);
    #[derive(Serialize)]
    struct Time64MicrosRow(#[serde(with = "crate::serde::chrono::time64::micros")] Duration);
    #[derive(Serialize)]
    struct OptDateRow(#[serde(with = "crate::serde::chrono::date::option")] Option<NaiveDate>);

    #[test]
    fn dates_are_days_since_epoch() {
        let d = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        assert_eq!(enc(&DateRow(d)), 1u16.to_le_bytes());
        // Pre-1970 fits Date32 (signed) but not Date.
        let old = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
        assert_eq!(enc(&Date32Row(old)), (-25567i32).to_le_bytes());
        assert!(matches!(enc_err(&DateRow(old)), RowBinaryError::Custom(_)));
        // Outside the server's Date32 range.
        let ancient = NaiveDate::from_ymd_opt(1899, 12, 31).unwrap();
        assert!(matches!(
            enc_err(&Date32Row(ancient)),
            RowBinaryError::Custom(_)
        ));
    }

    #[test]
    fn datetimes_are_epoch_ticks() {
        let dt = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        assert_eq!(enc(&DtRow(dt)), 1_700_000_000u32.to_le_bytes());
        assert_eq!(enc(&Dt64MillisRow(dt)), 1_700_000_000_000i64.to_le_bytes());
        assert_eq!(
            enc(&Dt64NanosRow(dt)),
            1_700_000_000_000_000_000i64.to_le_bytes()
        );
        // Pre-1970 is fine for DateTime64 (signed) but not DateTime (u32).
        let pre = Utc.timestamp_opt(-1, 0).unwrap();
        assert_eq!(enc(&Dt64MillisRow(pre)), (-1_000i64).to_le_bytes());
        assert!(matches!(enc_err(&DtRow(pre)), RowBinaryError::Custom(_)));
    }

    #[test]
    fn durations_are_time_ticks() {
        assert_eq!(
            enc(&TimeRow(Duration::seconds(-3599))),
            (-3599i32).to_le_bytes()
        );
        assert_eq!(
            enc(&Time64MicrosRow(Duration::milliseconds(1))),
            1_000i64.to_le_bytes()
        );
    }

    #[test]
    fn nullable_chrono_columns_compose_with_the_option_guard() {
        let d = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        assert_eq!(enc(&OptDateRow(Some(d))), [0, 1, 0]);
        assert_eq!(enc(&OptDateRow(None)), [1]);
    }
}
