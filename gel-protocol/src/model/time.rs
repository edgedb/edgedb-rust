use crate::model::{OutOfRangeError, ParseDurationError};
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Debug, Display};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

/// A span of time.
///
/// Precision: microseconds.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Duration {
    pub(crate) micros: i64,
}

/// A combination [`LocalDate`] and [`LocalTime`].
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LocalDatetime {
    pub(crate) micros: i64,
}

/// Naive date without a timezone.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LocalDate {
    pub(crate) days: i32,
}

/// Naive time without a timezone.
///
/// Can't be more than 24 hours.
///
/// Precision: microseconds.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LocalTime {
    pub(crate) micros: u64,
}

/// A UTC date and time.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Datetime {
    pub(crate) micros: i64,
}

/// A type that can represent a human-friendly duration like 1 month or two days.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RelativeDuration {
    pub(crate) micros: i64,
    pub(crate) days: i32,
    pub(crate) months: i32,
}

/// A type that can represent a human-friendly date duration like 1 month or two days.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DateDuration {
    pub(crate) days: i32,
    pub(crate) months: i32,
}

const SECS_PER_DAY: u64 = 86_400;
const MICROS_PER_DAY: u64 = SECS_PER_DAY * 1_000_000;

// leap years repeat every 400 years
const DAYS_IN_400_YEARS: u32 = 400 * 365 + 97;

const MIN_YEAR: i32 = 1;
const MAX_YEAR: i32 = 9999;

// year -4800 is a multiple of 400 smaller than the minimum supported year
const BASE_YEAR: i32 = -4800;

#[allow(dead_code)] // only used by specific features
const DAYS_IN_2000_YEARS: i32 = 5 * DAYS_IN_400_YEARS as i32;

const DAY_TO_MONTH_365: [u32; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];
const DAY_TO_MONTH_366: [u32; 13] = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366];

const MICROS_PER_MS: i64 = 1_000;
const MICROS_PER_SECOND: i64 = MICROS_PER_MS * 1_000;
const MICROS_PER_MINUTE: i64 = MICROS_PER_SECOND * 60;
const MICROS_PER_HOUR: i64 = MICROS_PER_MINUTE * 60;

impl Duration {
    pub const MIN: Duration = Duration { micros: i64::MIN };
    pub const MAX: Duration = Duration { micros: i64::MAX };

    pub fn from_micros(micros: i64) -> Duration {
        Duration { micros }
    }

    pub fn to_micros(self) -> i64 {
        self.micros
    }

    // Returns true if self is positive and false if the duration
    // is zero or negative.
    pub fn is_positive(&self) -> bool {
        self.micros.is_positive()
    }
    // Returns true if self is negative and false if the duration
    // is zero or positive.
    pub fn is_negative(&self) -> bool {
        self.micros.is_negative()
    }
    // Returns absolute values as stdlib's duration
    //
    // Note: `std::time::Duration` can't be negative
    pub fn abs_duration(&self) -> std::time::Duration {
        if self.micros.is_negative() {
            std::time::Duration::from_micros(u64::MAX - self.micros as u64 + 1)
        } else {
            std::time::Duration::from_micros(self.micros as u64)
        }
    }

    fn try_from_pg_simple_format(input: &str) -> Result<Self, ParseDurationError> {
        let mut split = input.trim_end().splitn(3, ':');
        let mut value: i64 = 0;
        let negative;
        let mut pos: usize = 0;

        {
            let hour_str = split.next().filter(|s| !s.is_empty()).ok_or_else(|| {
                ParseDurationError::new("EOF met, expecting `+`, `-` or int")
                    .not_final()
                    .pos(input.len())
            })?;
            pos += hour_str.len() - 1;
            let hour_str = hour_str.trim_start();
            let hour = hour_str
                .strip_prefix('-')
                .unwrap_or(hour_str)
                .parse::<i32>()
                .map_err(|e| ParseDurationError::from(e).not_final().pos(pos))?;
            negative = hour_str.starts_with('-');
            value += (hour.abs() as i64) * MICROS_PER_HOUR;
        }

        {
            pos += 1;
            let minute_str = split.next().ok_or_else(|| {
                ParseDurationError::new("EOF met, expecting `:`")
                    .not_final()
                    .pos(pos)
            })?;
            if !minute_str.is_empty() {
                pos += minute_str.len();
                let minute = minute_str
                    .parse::<u8>()
                    .map_err(|e| ParseDurationError::from(e).pos(pos))
                    .and_then(|m| {
                        if m <= 59 {
                            Ok(m)
                        } else {
                            Err(ParseDurationError::new("minutes value out of range").pos(pos))
                        }
                    })?;
                value += (minute as i64) * MICROS_PER_MINUTE;
            }
        }

        if let Some(remaining) = split.last() {
            pos += 1;
            let mut sec_split = remaining.splitn(2, '.');

            {
                let second_str = sec_split.next().unwrap();
                pos += second_str.len();
                let second = second_str
                    .parse::<u8>()
                    .map_err(|e| ParseDurationError::from(e).pos(pos))
                    .and_then(|s| {
                        if s <= 59 {
                            Ok(s)
                        } else {
                            Err(ParseDurationError::new("seconds value out of range").pos(pos))
                        }
                    })?;
                value += (second as i64) * MICROS_PER_SECOND;
            }

            if let Some(sub_sec_str) = sec_split.last() {
                pos += 1;
                for (i, c) in sub_sec_str.char_indices() {
                    let d = c
                        .to_digit(10)
                        .ok_or_else(|| ParseDurationError::new("not a digit").pos(pos + i + 1))?;
                    if i < 6 {
                        value += (d * 10_u32.pow((5 - i) as u32)) as i64;
                    } else {
                        if d >= 5 {
                            value += 1;
                        }
                        break;
                    }
                }
            }
        }

        if negative {
            value = -value;
        }
        Ok(Self { micros: value })
    }

    fn try_from_iso_format(input: &str) -> Result<Self, ParseDurationError> {
        if let Some(input) = input.strip_prefix("PT") {
            let mut pos = 2;
            let mut result = 0;
            let mut parts = input.split_inclusive(|c: char| c.is_alphabetic());
            let mut current = parts.next();

            if let Some(part) = current {
                if let Some(hour_str) = part.strip_suffix('H') {
                    let hour = hour_str
                        .parse::<i32>()
                        .map_err(|e| ParseDurationError::from(e).pos(pos))?;
                    result += (hour as i64) * MICROS_PER_HOUR;
                    pos += part.len();
                    current = parts.next();
                }
            }

            if let Some(part) = current {
                if let Some(minute_str) = part.strip_suffix('M') {
                    let minute = minute_str
                        .parse::<i32>()
                        .map_err(|e| ParseDurationError::from(e).pos(pos))?;
                    result += (minute as i64) * MICROS_PER_MINUTE;
                    pos += part.len();
                    current = parts.next();
                }
            }

            if let Some(part) = current {
                if let Some(second_str) = part.strip_suffix('S') {
                    let (second_str, subsec_str) = second_str
                        .split_once('.')
                        .map(|(sec, sub)| (sec, sub.get(..6).or(Some(sub))))
                        .unwrap_or_else(|| (second_str, None));

                    let second = second_str
                        .parse::<i32>()
                        .map_err(|e| ParseDurationError::from(e).pos(pos))?;
                    result += (second as i64) * MICROS_PER_SECOND;
                    pos += second_str.len() + 1;

                    if let Some(subsec_str) = subsec_str {
                        let subsec = subsec_str
                            .parse::<i32>()
                            .map_err(|e| ParseDurationError::from(e).pos(pos))?;
                        result += (subsec as i64)
                            * 10_i64.pow((6 - subsec_str.len()) as u32)
                            * if second < 0 { -1 } else { 1 };
                        pos += subsec_str.len()
                    }
                    current = parts.next();
                }
            }

            if current.is_some() {
                Err(ParseDurationError::new("expecting EOF").pos(pos))
            } else {
                Ok(Self { micros: result })
            }
        } else {
            Err(ParseDurationError::new("not ISO format").not_final())
        }
    }

    fn get_pg_format_value(
        input: &str,
        start: usize,
        end: usize,
    ) -> Result<i64, ParseDurationError> {
        if let Some(val) = input.get(start..end) {
            match val.parse::<i32>() {
                Ok(v) => Ok(v as i64),
                Err(e) => Err(ParseDurationError::from(e).pos(end.saturating_sub(1))),
            }
        } else {
            Err(ParseDurationError::new("expecting value").pos(end))
        }
    }

    fn try_from_pg_format(input: &str) -> Result<Self, ParseDurationError> {
        enum Expect {
            Numeric { begin: usize },
            Alphabetic { begin: usize, numeric: i64 },
            Whitespace { numeric: Option<i64> },
        }
        let mut seen = Vec::new();
        let mut get_unit = |start: usize, end: usize, default: Option<&str>| {
            input
                .get(start..end)
                .or(default)
                .and_then(|u| match u.to_lowercase().as_str() {
                    "h" | "hr" | "hrs" | "hour" | "hours" => Some(MICROS_PER_HOUR),
                    "m" | "min" | "mins" | "minute" | "minutes" => Some(MICROS_PER_MINUTE),
                    "ms" | "millisecon" | "millisecons" | "millisecond" | "milliseconds" => {
                        Some(MICROS_PER_MS)
                    }
                    "us" | "microsecond" | "microseconds" => Some(1),
                    "s" | "sec" | "secs" | "second" | "seconds" => Some(MICROS_PER_SECOND),
                    _ => None,
                })
                .ok_or_else(|| ParseDurationError::new("unknown unit").pos(start))
                .and_then(|u| {
                    if seen.contains(&u) {
                        Err(ParseDurationError::new("specified more than once").pos(start))
                    } else {
                        seen.push(u.clone());
                        Ok(u)
                    }
                })
        };
        let mut state = Expect::Whitespace { numeric: None };
        let mut result = 0;
        for (pos, c) in input.char_indices() {
            let is_whitespace = c.is_whitespace();
            let is_numeric = c.is_numeric() || c == '+' || c == '-';
            let is_alphabetic = c.is_alphabetic();
            if !(is_whitespace || is_numeric || is_alphabetic) {
                return Err(ParseDurationError::new("unexpected character").pos(pos));
            }
            match state {
                Expect::Numeric { begin } if !is_numeric => {
                    let numeric = Self::get_pg_format_value(input, begin, pos)?;
                    if is_alphabetic {
                        state = Expect::Alphabetic {
                            begin: pos,
                            numeric,
                        };
                    } else {
                        state = Expect::Whitespace {
                            numeric: Some(numeric),
                        };
                    }
                }
                Expect::Alphabetic { begin, numeric } if !is_alphabetic => {
                    result += numeric * get_unit(begin, pos, None)?;
                    if is_numeric {
                        state = Expect::Numeric { begin: pos };
                    } else {
                        state = Expect::Whitespace { numeric: None };
                    }
                }
                Expect::Whitespace { numeric: None } if !is_whitespace => {
                    if is_numeric {
                        state = Expect::Numeric { begin: pos };
                    } else {
                        return Err(
                            ParseDurationError::new("expecting whitespace or numeric").pos(pos)
                        );
                    }
                }
                Expect::Whitespace {
                    numeric: Some(numeric),
                } if !is_whitespace => {
                    if is_alphabetic {
                        state = Expect::Alphabetic {
                            begin: pos,
                            numeric,
                        };
                    } else {
                        return Err(
                            ParseDurationError::new("expecting whitespace or alphabetic").pos(pos),
                        );
                    }
                }
                _ => {}
            }
        }
        match state {
            Expect::Numeric { begin } => {
                result += Self::get_pg_format_value(input, begin, input.len())? * MICROS_PER_SECOND;
            }
            Expect::Alphabetic { begin, numeric } => {
                result += numeric * get_unit(begin, input.len(), Some("s"))?;
            }
            Expect::Whitespace {
                numeric: Some(numeric),
            } => {
                result += numeric * MICROS_PER_SECOND;
            }
            _ => {}
        }
        Ok(Self { micros: result })
    }
}

impl FromStr for Duration {
    type Err = ParseDurationError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Ok(seconds) = input.trim().parse::<i64>() {
            seconds
                .checked_mul(MICROS_PER_SECOND)
                .map(Self::from_micros)
                .ok_or_else(|| Self::Err::new("seconds value out of range").pos(input.len() - 1))
        } else {
            Self::try_from_pg_simple_format(input)
                .or_else(|e| {
                    if e.is_final {
                        Err(e)
                    } else {
                        Self::try_from_iso_format(input)
                    }
                })
                .or_else(|e| {
                    if e.is_final {
                        Err(e)
                    } else {
                        Self::try_from_pg_format(input)
                    }
                })
        }
    }
}

impl LocalDatetime {
    // 0001-01-01T00:00:00
    pub const MIN: LocalDatetime = LocalDatetime {
        micros: -63082281600000000,
    };
    // 9999-12-31T23:59:59.999999
    pub const MAX: LocalDatetime = LocalDatetime {
        micros: 252455615999999999,
    };

    pub(crate) fn from_postgres_micros(micros: i64) -> Result<LocalDatetime, OutOfRangeError> {
        if !(Self::MIN.micros..=Self::MAX.micros).contains(&micros) {
            return Err(OutOfRangeError);
        }
        Ok(LocalDatetime { micros })
    }

    #[deprecated(
        since = "0.5.0",
        note = "use Datetime::try_from_unix_micros(v).into() instead"
    )]
    pub fn from_micros(micros: i64) -> LocalDatetime {
        Self::from_postgres_micros(micros).unwrap_or_else(|_| {
            panic!(
                "LocalDatetime::from_micros({}) is outside the valid datetime range",
                micros
            )
        })
    }

    #[deprecated(since = "0.5.0", note = "use .to_utc().to_unix_micros() instead")]
    pub fn to_micros(self) -> i64 {
        self.micros
    }

    pub fn new(date: LocalDate, time: LocalTime) -> LocalDatetime {
        let micros = date.to_days() as i64 * MICROS_PER_DAY as i64 + time.to_micros() as i64;
        LocalDatetime { micros }
    }

    pub fn date(self) -> LocalDate {
        LocalDate::from_days(self.micros.wrapping_div_euclid(MICROS_PER_DAY as i64) as i32)
    }

    pub fn time(self) -> LocalTime {
        LocalTime::from_micros(self.micros.wrapping_rem_euclid(MICROS_PER_DAY as i64) as u64)
    }

    pub fn to_utc(self) -> Datetime {
        Datetime {
            micros: self.micros,
        }
    }
}

impl From<Datetime> for LocalDatetime {
    fn from(d: Datetime) -> LocalDatetime {
        LocalDatetime { micros: d.micros }
    }
}

impl Display for LocalDatetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.date(), self.time())
    }
}

impl Debug for LocalDatetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}T{}", self.date(), self.time())
    }
}

impl LocalTime {
    pub const MIN: LocalTime = LocalTime { micros: 0 };
    pub const MIDNIGHT: LocalTime = LocalTime { micros: 0 };
    pub const MAX: LocalTime = LocalTime {
        micros: MICROS_PER_DAY - 1,
    };

    pub(crate) fn try_from_micros(micros: u64) -> Result<LocalTime, OutOfRangeError> {
        if micros < MICROS_PER_DAY {
            Ok(LocalTime { micros })
        } else {
            Err(OutOfRangeError)
        }
    }

    pub fn from_micros(micros: u64) -> LocalTime {
        Self::try_from_micros(micros).expect("LocalTime is out of range")
    }

    pub fn to_micros(self) -> u64 {
        self.micros
    }

    fn to_hmsu(self) -> (u8, u8, u8, u32) {
        let micros = self.micros;

        let microsecond = (micros % 1_000_000) as u32;
        let micros = micros / 1_000_000;

        let second = (micros % 60) as u8;
        let micros = micros / 60;

        let minute = (micros % 60) as u8;
        let micros = micros / 60;

        let hour = (micros % 24) as u8;
        let micros = micros / 24;
        debug_assert_eq!(0, micros);

        (hour, minute, second, microsecond)
    }

    #[cfg(test)] // currently only used by tests, will be used by parsing later
    fn from_hmsu(hour: u8, minute: u8, second: u8, microsecond: u32) -> LocalTime {
        assert!(microsecond < 1_000_000);
        assert!(second < 60);
        assert!(minute < 60);
        assert!(hour < 24);

        let micros = microsecond as u64
            + 1_000_000 * (second as u64 + 60 * (minute as u64 + 60 * (hour as u64)));
        LocalTime::from_micros(micros)
    }
}

impl Display for LocalTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Debug for LocalTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (hour, minute, second, microsecond) = self.to_hmsu();
        write!(f, "{:02}:{:02}:{:02}", hour, minute, second)?;
        // like chrono::NaiveTime it outputs either 0, 3 or 6 decimal digits
        if microsecond != 0 {
            if microsecond % 1000 == 0 {
                write!(f, ".{:03}", microsecond / 1000)?;
            } else {
                write!(f, ".{:06}", microsecond)?;
            }
        };
        Ok(())
    }
}

impl LocalDate {
    pub const MIN: LocalDate = LocalDate { days: -730119 }; // 0001-01-01
    pub const MAX: LocalDate = LocalDate { days: 2921939 }; // 9999-12-31
    pub const UNIX_EPOCH: LocalDate = LocalDate {
        days: -(30 * 365 + 7),
    }; // 1970-01-01

    fn try_from_days(days: i32) -> Result<LocalDate, OutOfRangeError> {
        if !(Self::MIN.days..=Self::MAX.days).contains(&days) {
            return Err(OutOfRangeError);
        }
        Ok(LocalDate { days })
    }

    pub fn from_days(days: i32) -> LocalDate {
        Self::try_from_days(days).unwrap_or_else(|_| {
            panic!(
                "LocalDate::from_days({}) is outside the valid date range",
                days
            )
        })
    }

    pub fn to_days(self) -> i32 {
        self.days
    }

    pub fn from_ymd(year: i32, month: u8, day: u8) -> LocalDate {
        Self::try_from_ymd(year, month, day)
            .unwrap_or_else(|_| panic!("invalid date {:04}-{:02}-{:02}", year, month, day))
    }

    fn try_from_ymd(year: i32, month: u8, day: u8) -> Result<LocalDate, OutOfRangeError> {
        if !(1..=31).contains(&day) {
            return Err(OutOfRangeError);
        }
        if !(1..=12).contains(&month) {
            return Err(OutOfRangeError);
        }
        if !(MIN_YEAR..=MAX_YEAR).contains(&year) {
            return Err(OutOfRangeError);
        }

        let passed_years = (year - BASE_YEAR - 1) as u32;
        let days_from_year =
            365 * passed_years + passed_years / 4 - passed_years / 100 + passed_years / 400 + 366;

        let is_leap_year = (year % 400 == 0) || (year % 4 == 0 && year % 100 != 0);
        let day_to_month = if is_leap_year {
            DAY_TO_MONTH_366
        } else {
            DAY_TO_MONTH_365
        };

        let day_in_year = (day - 1) as u32 + day_to_month[month as usize - 1];
        if day_in_year >= day_to_month[month as usize] {
            return Err(OutOfRangeError);
        }

        LocalDate::try_from_days(
            (days_from_year + day_in_year) as i32
                - DAYS_IN_400_YEARS as i32 * ((2000 - BASE_YEAR) / 400),
        )
    }

    fn to_ymd(self) -> (i32, u8, u8) {
        const DAYS_IN_100_YEARS: u32 = 100 * 365 + 24;
        const DAYS_IN_4_YEARS: u32 = 4 * 365 + 1;
        const DAYS_IN_1_YEAR: u32 = 365;
        const DAY_TO_MONTH_MARCH: [u32; 12] =
            [0, 31, 61, 92, 122, 153, 184, 214, 245, 275, 306, 337];
        const MARCH_1: u32 = 31 + 29;
        const MARCH_1_MINUS_BASE_YEAR_TO_POSTGRES_EPOCH: u32 =
            (2000 - BASE_YEAR) as u32 / 400 * DAYS_IN_400_YEARS - MARCH_1;

        let days = (self.days as u32).wrapping_add(MARCH_1_MINUS_BASE_YEAR_TO_POSTGRES_EPOCH);

        let years400 = days / DAYS_IN_400_YEARS;
        let days = days % DAYS_IN_400_YEARS;

        let mut years100 = days / DAYS_IN_100_YEARS;
        if years100 == 4 {
            years100 = 3
        }; // prevent 400 year leap day from overflowing
        let days = days - DAYS_IN_100_YEARS * years100;

        let years4 = days / DAYS_IN_4_YEARS;
        let days = days % DAYS_IN_4_YEARS;

        let mut years1 = days / DAYS_IN_1_YEAR;
        if years1 == 4 {
            years1 = 3
        }; // prevent 4 year leap day from overflowing
        let days = days - DAYS_IN_1_YEAR * years1;

        let years = years1 + years4 * 4 + years100 * 100 + years400 * 400;
        let month_entry = DAY_TO_MONTH_MARCH
            .iter()
            .filter(|d| days >= **d)
            .enumerate()
            .last()
            .unwrap();
        let months = years * 12 + 2 + month_entry.0 as u32;
        let year = (months / 12) as i32 + BASE_YEAR;
        let month = (months % 12 + 1) as u8;
        let day = (days - month_entry.1 + 1) as u8;

        (year, month, day)
    }
}

impl Display for LocalDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Debug for LocalDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (year, month, day) = self.to_ymd();
        if year >= 10_000 {
            // ISO format requires a + on dates longer than 4 digits
            write!(f, "+")?;
        }
        if year >= 0 {
            write!(f, "{:04}-{:02}-{:02}", year, month, day)
        } else {
            // rust counts the sign as a digit when padding
            write!(f, "{:05}-{:02}-{:02}", year, month, day)
        }
    }
}

impl Datetime {
    // -63082281600000000 micros = Jan. 1 year 1
    pub const MIN: Datetime = Datetime {
        micros: LocalDatetime::MIN.micros,
    };
    // 252455615999999999 micros = Dec. 31 year 9999
    pub const MAX: Datetime = Datetime {
        micros: LocalDatetime::MAX.micros,
    };
    pub const UNIX_EPOCH: Datetime = Datetime {
        //micros: 0
        micros: LocalDate::UNIX_EPOCH.days as i64 * MICROS_PER_DAY as i64,
    };

    /// Convert microseconds since unix epoch into a datetime
    pub fn try_from_unix_micros(micros: i64) -> Result<Datetime, OutOfRangeError> {
        Self::_from_micros(micros).ok_or(OutOfRangeError)
    }

    #[deprecated(since = "0.5.0", note = "use try_from_unix_micros instead")]
    pub fn try_from_micros(micros: i64) -> Result<Datetime, OutOfRangeError> {
        Self::from_postgres_micros(micros)
    }

    pub(crate) fn from_postgres_micros(micros: i64) -> Result<Datetime, OutOfRangeError> {
        if !(Self::MIN.micros..=Self::MAX.micros).contains(&micros) {
            return Err(OutOfRangeError);
        }
        Ok(Datetime { micros })
    }

    fn _from_micros(micros: i64) -> Option<Datetime> {
        let micros = micros.checked_add(Self::UNIX_EPOCH.micros)?;
        if !(Self::MIN.micros..=Self::MAX.micros).contains(&micros) {
            return None;
        }
        Some(Datetime { micros })
    }

    #[deprecated(since = "0.5.0", note = "use from_unix_micros instead")]
    pub fn from_micros(micros: i64) -> Datetime {
        Self::from_postgres_micros(micros).unwrap_or_else(|_| {
            panic!(
                "Datetime::from_micros({}) is outside the valid datetime range",
                micros
            )
        })
    }

    /// Convert microseconds since unix epoch into a datetime
    ///
    /// # Panics
    ///
    /// When value is out of range.
    pub fn from_unix_micros(micros: i64) -> Datetime {
        if let Some(result) = Self::_from_micros(micros) {
            return result;
        }
        panic!(
            "Datetime::from_micros({}) is outside the valid datetime range",
            micros
        );
    }

    #[deprecated(since = "0.5.0", note = "use to_unix_micros instead")]
    pub fn to_micros(self) -> i64 {
        self.micros
    }

    /// Convert datetime to microseconds since Unix Epoch
    pub fn to_unix_micros(self) -> i64 {
        // i64 is enough to fit our range with both epochs
        self.micros - Datetime::UNIX_EPOCH.micros
    }

    fn postgres_epoch_unix() -> SystemTime {
        use std::time::Duration;
        // postgres epoch starts at 2000-01-01
        UNIX_EPOCH + Duration::from_micros((-Datetime::UNIX_EPOCH.micros) as u64)
    }
}

impl Display for Datetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} UTC",
            LocalDatetime {
                micros: self.micros
            }
        )
    }
}

impl Debug for Datetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}Z",
            LocalDatetime {
                micros: self.micros
            }
        )
    }
}

impl TryFrom<Datetime> for SystemTime {
    type Error = OutOfRangeError;

    fn try_from(value: Datetime) -> Result<Self, Self::Error> {
        use std::time::Duration;

        if value.micros > 0 {
            Datetime::postgres_epoch_unix().checked_add(Duration::from_micros(value.micros as u64))
        } else {
            Datetime::postgres_epoch_unix()
                .checked_sub(Duration::from_micros((-value.micros) as u64))
        }
        .ok_or(OutOfRangeError)
    }
}

impl TryFrom<std::time::Duration> for Duration {
    type Error = OutOfRangeError;

    fn try_from(value: std::time::Duration) -> Result<Self, Self::Error> {
        TryFrom::try_from(&value)
    }
}

impl TryFrom<&std::time::Duration> for Duration {
    type Error = OutOfRangeError;

    fn try_from(value: &std::time::Duration) -> Result<Self, Self::Error> {
        let secs = value.as_secs();
        let subsec_nanos = value.subsec_nanos();
        let subsec_micros = nanos_to_micros(subsec_nanos.into());
        let micros = i64::try_from(secs)
            .ok()
            .and_then(|x| x.checked_mul(1_000_000))
            .and_then(|x| x.checked_add(subsec_micros))
            .ok_or(OutOfRangeError)?;
        Ok(Duration { micros })
    }
}

impl TryFrom<&Duration> for std::time::Duration {
    type Error = OutOfRangeError;

    fn try_from(value: &Duration) -> Result<std::time::Duration, Self::Error> {
        let micros = value.micros.try_into().map_err(|_| OutOfRangeError)?;
        Ok(std::time::Duration::from_micros(micros))
    }
}
impl TryFrom<Duration> for std::time::Duration {
    type Error = OutOfRangeError;

    fn try_from(value: Duration) -> Result<std::time::Duration, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<SystemTime> for Datetime {
    type Error = OutOfRangeError;

    fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
        match value.duration_since(UNIX_EPOCH) {
            Ok(duration) => {
                let secs = duration.as_secs();
                let subsec_nanos = duration.subsec_nanos();
                let subsec_micros = nanos_to_micros(subsec_nanos.into());
                let micros = i64::try_from(secs)
                    .ok()
                    .and_then(|x| x.checked_mul(1_000_000))
                    .and_then(|x| x.checked_add(subsec_micros))
                    .and_then(|x| x.checked_add(Datetime::UNIX_EPOCH.micros))
                    .ok_or(OutOfRangeError)?;
                if micros > Datetime::MAX.micros {
                    return Err(OutOfRangeError);
                }
                Ok(Datetime { micros })
            }
            Err(e) => {
                let mut secs = e.duration().as_secs();
                let mut subsec_nanos = e.duration().subsec_nanos();
                if subsec_nanos > 0 {
                    secs = secs.checked_add(1).ok_or(OutOfRangeError)?;
                    subsec_nanos = 1_000_000_000 - subsec_nanos;
                }
                let subsec_micros = nanos_to_micros(subsec_nanos.into());
                let micros = i64::try_from(secs)
                    .ok()
                    .and_then(|x| x.checked_mul(1_000_000))
                    .and_then(|x| Datetime::UNIX_EPOCH.micros.checked_sub(x))
                    .and_then(|x| x.checked_add(subsec_micros))
                    .ok_or(OutOfRangeError)?;
                if micros < Datetime::MIN.micros {
                    return Err(OutOfRangeError);
                }
                Ok(Datetime { micros })
            }
        }
    }
}

impl std::ops::Add<&'_ std::time::Duration> for Datetime {
    type Output = Datetime;
    fn add(self, other: &std::time::Duration) -> Datetime {
        let Ok(duration) = Duration::try_from(other) else {
            debug_assert!(false, "duration is out of range");
            return Datetime::MAX;
        };
        if let Some(micros) = self.micros.checked_add(duration.micros) {
            Datetime { micros }
        } else {
            debug_assert!(false, "duration is out of range");
            Datetime::MAX
        }
    }
}

impl std::ops::Add<std::time::Duration> for Datetime {
    type Output = Datetime;
    #[allow(clippy::op_ref)]
    fn add(self, other: std::time::Duration) -> Datetime {
        self + &other
    }
}

impl Display for Duration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let abs = if self.micros < 0 {
            write!(f, "-")?;
            -self.micros
        } else {
            self.micros
        };
        let (sec, micros) = (abs / 1_000_000, abs % 1_000_000);
        if micros != 0 {
            let mut fract = micros;
            let mut zeros = 0;
            while fract % 10 == 0 {
                zeros += 1;
                fract /= 10;
            }
            write!(
                f,
                "{hours}:{minutes:02}:{seconds:02}.{fract:0>fsize$}",
                hours = sec / 3600,
                minutes = sec / 60 % 60,
                seconds = sec % 60,
                fract = fract,
                fsize = 6 - zeros,
            )
        } else {
            write!(f, "{}:{:02}:{:02}", sec / 3600, sec / 60 % 60, sec % 60)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn micros_conv() {
        let datetime = Datetime::from_unix_micros(1645681383000002);
        assert_eq!(datetime.micros, 698996583000002);
        assert_eq!(to_debug(datetime), "2022-02-24T05:43:03.000002Z");
    }

    #[test]
    fn big_duration_abs() {
        use super::Duration as Src;
        use std::time::Duration as Trg;
        assert_eq!(Src { micros: -1 }.abs_duration(), Trg::new(0, 1000));
        assert_eq!(Src { micros: -1000 }.abs_duration(), Trg::new(0, 1000000));
        assert_eq!(Src { micros: -1000000 }.abs_duration(), Trg::new(1, 0));
        assert_eq!(
            Src { micros: i64::MIN }.abs_duration(),
            Trg::new(9223372036854, 775808000)
        );
    }

    #[test]
    fn local_date_from_ymd() {
        assert_eq!(0, LocalDate::from_ymd(2000, 1, 1).to_days());
        assert_eq!(-365, LocalDate::from_ymd(1999, 1, 1).to_days());
        assert_eq!(366, LocalDate::from_ymd(2001, 1, 1).to_days());
        assert_eq!(-730119, LocalDate::from_ymd(1, 1, 1).to_days());
        assert_eq!(2921575, LocalDate::from_ymd(9999, 1, 1).to_days());

        assert_eq!(Err(OutOfRangeError), LocalDate::try_from_ymd(2001, 1, 32));
        assert_eq!(Err(OutOfRangeError), LocalDate::try_from_ymd(2001, 2, 29));
    }

    #[test]
    fn local_date_from_ymd_leap_year() {
        let days_in_month_leap = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let mut total_days = 0;
        let start_of_year = 365 * 4 + 1;
        for month in 1..=12 {
            let start_of_month = LocalDate::from_ymd(2004, month as u8, 1).to_days();
            assert_eq!(total_days, start_of_month - start_of_year);

            let days_in_current_month = days_in_month_leap[month - 1];
            total_days += days_in_current_month;

            let end_of_month =
                LocalDate::from_ymd(2004, month as u8, days_in_current_month as u8).to_days();
            assert_eq!(total_days - 1, end_of_month - start_of_year);
        }
        assert_eq!(366, total_days);
    }

    const DAYS_IN_MONTH_LEAP: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    #[test]
    fn local_date_from_ymd_normal_year() {
        let mut total_days = 0;
        let start_of_year = 365 + 1;
        for month in 1..=12 {
            let start_of_month = LocalDate::from_ymd(2001, month as u8, 1).to_days();
            assert_eq!(total_days, start_of_month - start_of_year);

            let days_in_current_month = DAYS_IN_MONTH_LEAP[month - 1];
            total_days += days_in_current_month as i32;

            let end_of_month =
                LocalDate::from_ymd(2001, month as u8, days_in_current_month).to_days();
            assert_eq!(total_days - 1, end_of_month - start_of_year);
        }
        assert_eq!(365, total_days);
    }

    pub const CHRONO_MAX_YEAR: i32 = 262_143;

    fn extended_test_dates() -> impl Iterator<Item = (i32, u8, u8)> {
        const YEARS: [i32; 36] = [
            1,
            2,
            1000,
            1969,
            1970, // unix epoch
            1971,
            1999,
            2000, // postgres epoch
            2001,
            2002,
            2003,
            2004,
            2008,
            2009,
            2010,
            2100,
            2200,
            2300,
            2400,
            9000,
            9999,
            10_000,
            10_001,
            11_000,
            20_000,
            100_000,
            200_000,
            CHRONO_MAX_YEAR - 1,
            CHRONO_MAX_YEAR,
            CHRONO_MAX_YEAR + 1,
            MAX_YEAR - 1000,
            MAX_YEAR - 31,
            MAX_YEAR - 30, // maximum unix based
            MAX_YEAR - 29, // less than 30 years before maximum, so a unix epoch in microseconds overflows
            MAX_YEAR - 1,
            MAX_YEAR,
        ];

        const MONTHS: std::ops::RangeInclusive<u8> = 1u8..=12;
        const DAYS: [u8; 6] = [1u8, 13, 28, 29, 30, 31];
        let dates = MONTHS.flat_map(|month| DAYS.iter().map(move |day| (month, *day)));

        YEARS
            .iter()
            .flat_map(move |year| dates.clone().map(move |date| (*year, date.0, date.1)))
    }

    pub fn valid_test_dates() -> impl Iterator<Item = (i32, u8, u8)> {
        extended_test_dates().filter(|date| LocalDate::try_from_ymd(date.0, date.1, date.2).is_ok())
    }

    pub fn test_times() -> impl Iterator<Item = u64> {
        const TIMES: [u64; 7] = [
            0,
            10,
            10_020,
            12345 * 1_000_000,
            12345 * 1_001_000,
            12345 * 1_001_001,
            MICROS_PER_DAY - 1,
        ];
        TIMES.iter().copied()
    }

    #[test]
    fn check_test_dates() {
        assert!(valid_test_dates().count() > 1000);
    }

    #[test]
    fn local_date_ymd_roundtrip() {
        for (year, month, day) in valid_test_dates() {
            let date = LocalDate::from_ymd(year, month, day);
            assert_eq!((year, month, day), date.to_ymd());
        }
    }

    #[test]
    fn local_time_parts_roundtrip() {
        for time in test_times() {
            let expected_time = LocalTime::from_micros(time);
            let (hour, minute, second, microsecond) = expected_time.to_hmsu();
            let actual_time = LocalTime::from_hmsu(hour, minute, second, microsecond);
            assert_eq!(expected_time, actual_time);
        }
    }

    #[test]
    fn format_local_date() {
        assert_eq!("2000-01-01", LocalDate::from_days(0).to_string());
        assert_eq!("0001-01-01", LocalDate::MIN.to_string());
        assert_eq!("9999-12-31", LocalDate::MAX.to_string());
    }

    #[test]
    fn format_local_time() {
        assert_eq!("00:00:00", LocalTime::MIDNIGHT.to_string());
        assert_eq!("00:00:00.010", LocalTime::from_micros(10_000).to_string());
        assert_eq!(
            "00:00:00.010020",
            LocalTime::from_micros(10_020).to_string()
        );
        assert_eq!("23:59:59.999999", LocalTime::MAX.to_string());
    }

    pub fn to_debug<T: Debug>(x: T) -> String {
        format!("{:?}", x)
    }

    #[test]
    #[allow(deprecated)]
    fn format_local_datetime() {
        assert_eq!(
            "2039-02-13 23:31:30.123456",
            LocalDatetime::from_micros(1_234_567_890_123_456).to_string()
        );
        assert_eq!(
            "2039-02-13T23:31:30.123456",
            to_debug(LocalDatetime::from_micros(1_234_567_890_123_456))
        );

        assert_eq!("0001-01-01 00:00:00", LocalDatetime::MIN.to_string());
        assert_eq!("0001-01-01T00:00:00", to_debug(LocalDatetime::MIN));

        assert_eq!("9999-12-31 23:59:59.999999", LocalDatetime::MAX.to_string());
        assert_eq!("9999-12-31T23:59:59.999999", to_debug(LocalDatetime::MAX));
    }

    #[test]
    #[allow(deprecated)]
    fn format_datetime() {
        assert_eq!(
            "2039-02-13 23:31:30.123456 UTC",
            Datetime::from_micros(1_234_567_890_123_456).to_string()
        );
        assert_eq!(
            "2039-02-13T23:31:30.123456Z",
            to_debug(Datetime::from_micros(1_234_567_890_123_456))
        );

        assert_eq!("0001-01-01 00:00:00 UTC", Datetime::MIN.to_string());
        assert_eq!("0001-01-01T00:00:00Z", to_debug(Datetime::MIN));

        assert_eq!("9999-12-31 23:59:59.999999 UTC", Datetime::MAX.to_string());
        assert_eq!("9999-12-31T23:59:59.999999Z", to_debug(Datetime::MAX));
    }

    #[test]
    fn format_duration() {
        fn dur_str(msec: i64) -> String {
            Duration::from_micros(msec).to_string()
        }
        assert_eq!(dur_str(1_000_000), "0:00:01");
        assert_eq!(dur_str(1), "0:00:00.000001");
        assert_eq!(dur_str(7_015_000), "0:00:07.015");
        assert_eq!(dur_str(10_000_000_015_000), "2777:46:40.015");
        assert_eq!(dur_str(12_345_678_000_000), "3429:21:18");
    }

    #[test]
    fn parse_duration_str() {
        fn micros(input: &str) -> i64 {
            Duration::from_str(input).unwrap().micros
        }
        assert_eq!(micros(" 100   "), 100_000_000);
        assert_eq!(micros("123"), 123_000_000);
        assert_eq!(micros("-123"), -123_000_000);
        assert_eq!(micros("  20 mins 1hr "), 4_800_000_000);
        assert_eq!(micros("  20 mins -1hr "), -2_400_000_000);
        assert_eq!(micros("  20us  1h    20   "), 3_620_000_020);
        assert_eq!(micros("  -20us  1h    20   "), 3_619_999_980);
        assert_eq!(micros("  -20US  1H    20   "), 3_619_999_980);
        assert_eq!(
            micros("1 hour 20 minutes 30 seconds 40 milliseconds 50 microseconds"),
            4_830_040_050
        );
        assert_eq!(
            micros("1 hour 20 minutes +30seconds 40 milliseconds -50microseconds"),
            4_830_039_950
        );
        assert_eq!(
            micros("1 houR  20 minutes 30SECOND 40 milliseconds 50 us"),
            4_830_040_050
        );
        assert_eq!(micros("  20 us 1H 20 minutes "), 4_800_000_020);
        assert_eq!(micros("-1h"), -3_600_000_000);
        assert_eq!(micros("100h"), 360_000_000_000);
        let h12 = 12 * 3_600_000_000_i64;
        let m12 = 12 * 60_000_000_i64;
        assert_eq!(micros("   12:12:12.2131   "), h12 + m12 + 12_213_100);
        assert_eq!(micros("-12:12:12.21313"), -(h12 + m12 + 12_213_130));
        assert_eq!(micros("-12:12:12.213134"), -(h12 + m12 + 12_213_134));
        assert_eq!(micros("-12:12:12.2131341"), -(h12 + m12 + 12_213_134));
        assert_eq!(micros("-12:12:12.2131341111111"), -(h12 + m12 + 12_213_134));
        assert_eq!(micros("-12:12:12.2131315111111"), -(h12 + m12 + 12_213_132));
        assert_eq!(micros("-12:12:12.2131316111111"), -(h12 + m12 + 12_213_132));
        assert_eq!(micros("-12:12:12.2131314511111"), -(h12 + m12 + 12_213_131));
        assert_eq!(micros("-0:12:12.2131"), -(m12 + 12_213_100));
        assert_eq!(micros("12:12"), h12 + m12);
        assert_eq!(micros("-12:12"), -(h12 + m12));
        assert_eq!(micros("-12:1:1"), -(h12 + 61_000_000));
        assert_eq!(micros("+12:1:1"), h12 + 61_000_000);
        assert_eq!(micros("-12:1:1.1234"), -(h12 + 61_123_400));
        assert_eq!(micros("1211:59:59.9999"), h12 * 100 + h12 - 100);
        assert_eq!(micros("-12:"), -h12);
        assert_eq!(micros("0"), 0);
        assert_eq!(micros("00:00:00"), 0);
        assert_eq!(micros("00:00:10.9"), 10_900_000);
        assert_eq!(micros("00:00:10.09"), 10_090_000);
        assert_eq!(micros("00:00:10.009"), 10_009_000);
        assert_eq!(micros("00:00:10.0009"), 10_000_900);
        assert_eq!(micros("00:00:00.5"), 500_000);
        assert_eq!(micros("  +00005"), 5_000_000);
        assert_eq!(micros("  -00005"), -5_000_000);
        assert_eq!(micros("PT"), 0);
        assert_eq!(micros("PT1H1M1S"), 3_661_000_000);
        assert_eq!(micros("PT1M1S"), 61_000_000);
        assert_eq!(micros("PT1S"), 1_000_000);
        assert_eq!(micros("PT1H1S"), 3_601_000_000);
        assert_eq!(micros("PT1H1M1.1S"), 3_661_100_000);
        assert_eq!(micros("PT1H1M1.01S"), 3_661_010_000);
        assert_eq!(micros("PT1H1M1.10S"), 3_661_100_000);
        assert_eq!(micros("PT1H1M1.1234567S"), 3_661_123_456);
        assert_eq!(micros("PT1H1M1.1234564S"), 3_661_123_456);
        assert_eq!(micros("PT-1H1M1.1S"), -3_538_900_000);
        assert_eq!(micros("PT+1H-1M1.1S"), 3_541_100_000);
        assert_eq!(micros("PT1H+1M-1.1S"), 3_658_900_000);

        fn assert_error(input: &str, expected_pos: usize, pat: &str) {
            let ParseDurationError {
                pos,
                message,
                is_final: _,
            } = Duration::from_str(input).unwrap_err();
            assert_eq!(pos, expected_pos);
            assert!(
                message.contains(pat),
                "`{}` not found in `{}`",
                pat,
                message,
            );
        }
        assert_error("blah", 0, "numeric");
        assert_error("!", 0, "unexpected");
        assert_error("-", 0, "invalid digit");
        assert_error("+", 0, "invalid digit");
        assert_error("  20 us 1H 20 30 minutes ", 14, "alphabetic");
        assert_error("   12:12:121.2131   ", 11, "seconds");
        assert_error("   12:60:21.2131   ", 7, "minutes");
        assert_error("  20us 20   1h       ", 12, "alphabetic");
        assert_error("  20us $ 20   1h       ", 7, "unexpected");
        assert_error(
            "1 houR  20 minutes 30SECOND 40 milliseconds 50 uss",
            47,
            "unit",
        );
        assert_error("PT1M1H", 4, "EOF");
        assert_error("PT1S1M", 4, "EOF");
    }

    #[test]
    fn add_duration_rounding() {
        // round down
        assert_eq!(
            Datetime::UNIX_EPOCH + std::time::Duration::new(17, 500),
            Datetime::UNIX_EPOCH + std::time::Duration::new(17, 0),
        );
        // round up
        assert_eq!(
            Datetime::UNIX_EPOCH + std::time::Duration::new(12345, 1500),
            Datetime::UNIX_EPOCH + std::time::Duration::new(12345, 2000),
        );
    }

    #[test]
    #[allow(deprecated)]
    fn to_and_from_unix_micros_roundtrip() {
        let zero_micros = 0;
        let datetime = Datetime::from_unix_micros(0);
        // Unix micros should equal 0
        assert_eq!(zero_micros, datetime.to_unix_micros());
        // Datetime (Postgres epoch-based) micros should be negative
        // Micros = negative micros to go from 2000 to 1970
        assert_eq!(datetime.micros, datetime.to_micros());
        assert_eq!(datetime.micros, Datetime::UNIX_EPOCH.micros);
        assert_eq!(datetime.micros, -946684800000000);
    }
}

impl RelativeDuration {
    pub fn try_from_years(years: i32) -> Result<RelativeDuration, OutOfRangeError> {
        Ok(RelativeDuration {
            months: years.checked_mul(12).ok_or(OutOfRangeError)?,
            days: 0,
            micros: 0,
        })
    }
    pub fn from_years(years: i32) -> RelativeDuration {
        RelativeDuration::try_from_years(years).unwrap()
    }
    pub fn try_from_months(months: i32) -> Result<RelativeDuration, OutOfRangeError> {
        Ok(RelativeDuration {
            months,
            days: 0,
            micros: 0,
        })
    }
    pub fn from_months(months: i32) -> RelativeDuration {
        RelativeDuration::try_from_months(months).unwrap()
    }
    pub fn try_from_days(days: i32) -> Result<RelativeDuration, OutOfRangeError> {
        Ok(RelativeDuration {
            months: 0,
            days,
            micros: 0,
        })
    }
    pub fn from_days(days: i32) -> RelativeDuration {
        RelativeDuration::try_from_days(days).unwrap()
    }
    pub fn try_from_hours(hours: i64) -> Result<RelativeDuration, OutOfRangeError> {
        Ok(RelativeDuration {
            months: 0,
            days: 0,
            micros: hours.checked_mul(3_600_000_000).ok_or(OutOfRangeError)?,
        })
    }
    pub fn from_hours(hours: i64) -> RelativeDuration {
        RelativeDuration::try_from_hours(hours).unwrap()
    }
    pub fn try_from_minutes(minutes: i64) -> Result<RelativeDuration, OutOfRangeError> {
        Ok(RelativeDuration {
            months: 0,
            days: 0,
            micros: minutes.checked_mul(60_000_000).ok_or(OutOfRangeError)?,
        })
    }
    pub fn from_minutes(minutes: i64) -> RelativeDuration {
        RelativeDuration::try_from_minutes(minutes).unwrap()
    }
    pub fn try_from_secs(secs: i64) -> Result<RelativeDuration, OutOfRangeError> {
        Ok(RelativeDuration {
            months: 0,
            days: 0,
            micros: secs.checked_mul(1_000_000).ok_or(OutOfRangeError)?,
        })
    }
    pub fn from_secs(secs: i64) -> RelativeDuration {
        RelativeDuration::try_from_secs(secs).unwrap()
    }
    pub fn try_from_millis(millis: i64) -> Result<RelativeDuration, OutOfRangeError> {
        Ok(RelativeDuration {
            months: 0,
            days: 0,
            micros: millis.checked_mul(1_000).ok_or(OutOfRangeError)?,
        })
    }
    pub fn from_millis(millis: i64) -> RelativeDuration {
        RelativeDuration::try_from_millis(millis).unwrap()
    }
    pub fn try_from_micros(micros: i64) -> Result<RelativeDuration, OutOfRangeError> {
        Ok(RelativeDuration {
            months: 0,
            days: 0,
            micros,
        })
    }
    pub fn from_micros(micros: i64) -> RelativeDuration {
        RelativeDuration::try_from_micros(micros).unwrap()
    }
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(RelativeDuration {
            months: self.months.checked_add(other.months)?,
            days: self.days.checked_add(other.days)?,
            micros: self.micros.checked_add(other.micros)?,
        })
    }
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        Some(RelativeDuration {
            months: self.months.checked_sub(other.months)?,
            days: self.days.checked_sub(other.days)?,
            micros: self.micros.checked_sub(other.micros)?,
        })
    }
}

impl std::ops::Add for RelativeDuration {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        RelativeDuration {
            months: self.months + other.months,
            days: self.days + other.days,
            micros: self.micros + other.micros,
        }
    }
}

impl std::ops::Sub for RelativeDuration {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        RelativeDuration {
            months: self.months - other.months,
            days: self.days - other.days,
            micros: self.micros - other.micros,
        }
    }
}

impl Display for RelativeDuration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.months == 0 && self.days == 0 && self.micros == 0 {
            return write!(f, "PT0S");
        }
        write!(f, "P")?;
        if self.months.abs() >= 12 {
            write!(f, "{}Y", self.months / 12)?;
        }
        if (self.months % 12).abs() > 0 {
            write!(f, "{}M", self.months % 12)?;
        }
        if self.days.abs() > 0 {
            write!(f, "{}D", self.days)?;
        }
        if self.micros.abs() > 0 {
            write!(f, "T")?;
            if self.micros.abs() >= 3_600_000_000 {
                write!(f, "{}H", self.micros / 3_600_000_000)?;
            }
            let minutes = self.micros % 3_600_000_000;
            if minutes.abs() >= 60_000_000 {
                write!(f, "{}M", minutes / 60_000_000)?;
            }
            let seconds = minutes % 60_000_000;
            if seconds.abs() >= 1_000_000 {
                write!(f, "{}", seconds / 1_000_000)?;
            }
            let micros = seconds % 1_000_000;
            if micros.abs() > 0 {
                let mut buf = [0u8; 6];
                let text = {
                    use std::io::{Cursor, Write};

                    let mut cur = Cursor::new(&mut buf[..]);
                    write!(cur, "{:06}", micros.abs()).unwrap();
                    let mut len = buf.len();
                    while buf[len - 1] == b'0' {
                        len -= 1;
                    }
                    std::str::from_utf8(&buf[..len]).unwrap()
                };
                write!(f, ".{}", text)?;
            }
            if seconds.abs() > 0 {
                write!(f, "S")?;
            }
        }
        Ok(())
    }
}

#[test]
fn relative_duration_display() {
    let dur = RelativeDuration::from_years(2)
        + RelativeDuration::from_months(56)
        + RelativeDuration::from_days(-16)
        + RelativeDuration::from_hours(48)
        + RelativeDuration::from_minutes(245)
        + RelativeDuration::from_secs(7)
        + RelativeDuration::from_millis(600);
    assert_eq!(dur.to_string(), "P6Y8M-16DT52H5M7.6S");

    let dur = RelativeDuration::from_years(2)
        + RelativeDuration::from_months(-56)
        + RelativeDuration::from_days(-16)
        + RelativeDuration::from_minutes(-245)
        + RelativeDuration::from_secs(7)
        + RelativeDuration::from_millis(600);
    assert_eq!(dur.to_string(), "P-2Y-8M-16DT-4H-4M-52.4S");

    let dur = RelativeDuration::from_years(1);
    assert_eq!(dur.to_string(), "P1Y");
    let dur = RelativeDuration::from_months(1);
    assert_eq!(dur.to_string(), "P1M");
    let dur = RelativeDuration::from_hours(1);
    assert_eq!(dur.to_string(), "PT1H");
    let dur = RelativeDuration::from_minutes(1);
    assert_eq!(dur.to_string(), "PT1M");
    let dur = RelativeDuration::from_secs(1);
    assert_eq!(dur.to_string(), "PT1S");
}

impl DateDuration {
    pub fn try_from_years(years: i32) -> Result<DateDuration, OutOfRangeError> {
        Ok(DateDuration {
            months: years.checked_mul(12).ok_or(OutOfRangeError)?,
            days: 0,
        })
    }
    pub fn from_years(years: i32) -> DateDuration {
        DateDuration::try_from_years(years).unwrap()
    }
    pub fn try_from_months(months: i32) -> Result<DateDuration, OutOfRangeError> {
        Ok(DateDuration { months, days: 0 })
    }
    pub fn from_months(months: i32) -> DateDuration {
        DateDuration::try_from_months(months).unwrap()
    }
    pub fn try_from_days(days: i32) -> Result<DateDuration, OutOfRangeError> {
        Ok(DateDuration { months: 0, days })
    }
    pub fn from_days(days: i32) -> DateDuration {
        DateDuration::try_from_days(days).unwrap()
    }
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(DateDuration {
            months: self.months.checked_add(other.months)?,
            days: self.days.checked_add(other.days)?,
        })
    }
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        Some(DateDuration {
            months: self.months.checked_sub(other.months)?,
            days: self.days.checked_sub(other.days)?,
        })
    }
}

impl std::ops::Add for DateDuration {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        DateDuration {
            months: self.months + other.months,
            days: self.days + other.days,
        }
    }
}

impl std::ops::Sub for DateDuration {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        DateDuration {
            months: self.months - other.months,
            days: self.days - other.days,
        }
    }
}

impl Display for DateDuration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.months == 0 && self.days == 0 {
            return write!(f, "PT0D"); // XXX
        }
        write!(f, "P")?;
        if self.months.abs() >= 12 {
            write!(f, "{}Y", self.months / 12)?;
        }
        if (self.months % 12).abs() > 0 {
            write!(f, "{}M", self.months % 12)?;
        }
        if self.days.abs() > 0 {
            write!(f, "{}D", self.days)?;
        }
        Ok(())
    }
}

fn nanos_to_micros(nanos: i64) -> i64 {
    // round to the nearest even
    let mut micros = nanos / 1000;
    let remainder = nanos % 1000;
    if remainder == 500 && micros % 2 == 1 || remainder > 500 {
        micros += 1;
    }
    micros
}

#[cfg(feature = "chrono")]
mod chrono_interop {
    use super::*;
    use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
    use chrono::DateTime;

    type ChronoDatetime = chrono::DateTime<chrono::Utc>;

    impl From<&LocalDatetime> for NaiveDateTime {
        fn from(value: &LocalDatetime) -> NaiveDateTime {
            let timestamp_seconds = value.micros.wrapping_div_euclid(1_000_000)
                - (Datetime::UNIX_EPOCH.micros / 1_000_000);
            let timestamp_nanos = (value.micros.wrapping_rem_euclid(1_000_000) * 1000) as u32;
            DateTime::from_timestamp(timestamp_seconds, timestamp_nanos)
                .expect("NaiveDateTime range is bigger than LocalDatetime")
                .naive_utc()
        }
    }

    impl TryFrom<&NaiveDateTime> for LocalDatetime {
        type Error = OutOfRangeError;
        fn try_from(d: &NaiveDateTime) -> Result<LocalDatetime, Self::Error> {
            let secs = d.and_utc().timestamp();
            let subsec_nanos = d.and_utc().timestamp_subsec_nanos();
            let subsec_micros = nanos_to_micros(subsec_nanos.into());
            let micros = secs
                .checked_mul(1_000_000)
                .and_then(|x| x.checked_add(subsec_micros))
                .and_then(|x| x.checked_add(Datetime::UNIX_EPOCH.micros))
                .ok_or(OutOfRangeError)?;
            if !(LocalDatetime::MIN.micros..=LocalDatetime::MAX.micros).contains(&micros) {
                return Err(OutOfRangeError);
            }
            Ok(LocalDatetime { micros })
        }
    }

    impl From<&Datetime> for ChronoDatetime {
        fn from(value: &Datetime) -> ChronoDatetime {
            use chrono::TimeZone;

            let pg_epoch = chrono::Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
            let duration = chrono::Duration::microseconds(value.micros);
            pg_epoch
                .checked_add_signed(duration)
                .expect("Gel datetime range is smaller than Chrono's")
        }
    }

    impl From<Datetime> for ChronoDatetime {
        fn from(value: Datetime) -> ChronoDatetime {
            (&value).into()
        }
    }

    impl TryFrom<&ChronoDatetime> for Datetime {
        type Error = OutOfRangeError;

        fn try_from(value: &ChronoDatetime) -> Result<Datetime, Self::Error> {
            let min = ChronoDatetime::from(Datetime::MIN);
            let duration = value
                .signed_duration_since(min)
                .to_std()
                .map_err(|_| OutOfRangeError)?;
            let secs = duration.as_secs();
            let subsec_micros = nanos_to_micros(duration.subsec_nanos().into());
            let micros = i64::try_from(secs)
                .ok()
                .and_then(|x| x.checked_mul(1_000_000))
                .and_then(|x| x.checked_add(subsec_micros))
                .and_then(|x| x.checked_add(Datetime::MIN.micros))
                .ok_or(OutOfRangeError)?;
            if micros > Datetime::MAX.micros {
                return Err(OutOfRangeError);
            }
            Ok(Datetime { micros })
        }
    }

    impl TryFrom<&NaiveDate> for LocalDate {
        type Error = OutOfRangeError;
        fn try_from(d: &NaiveDate) -> Result<LocalDate, Self::Error> {
            let days = chrono::Datelike::num_days_from_ce(d);
            Ok(LocalDate {
                days: days
                    .checked_sub(DAYS_IN_2000_YEARS - 365)
                    .ok_or(OutOfRangeError)?,
            })
        }
    }

    impl From<&LocalDate> for NaiveDate {
        fn from(value: &LocalDate) -> NaiveDate {
            value
                .days
                .checked_add(DAYS_IN_2000_YEARS - 365)
                .and_then(NaiveDate::from_num_days_from_ce_opt)
                .expect("NaiveDate range is bigger than LocalDate")
        }
    }

    impl From<&LocalTime> for NaiveTime {
        fn from(value: &LocalTime) -> NaiveTime {
            NaiveTime::from_num_seconds_from_midnight_opt(
                (value.micros / 1_000_000) as u32,
                ((value.micros % 1_000_000) * 1000) as u32,
            )
            .expect("localtime and native time have equal range")
        }
    }

    impl From<&NaiveTime> for LocalTime {
        fn from(time: &NaiveTime) -> LocalTime {
            let sec = chrono::Timelike::num_seconds_from_midnight(time);
            let nanos = nanos_to_micros(chrono::Timelike::nanosecond(time) as i64) as u64;
            let mut micros = sec as u64 * 1_000_000 + nanos;

            if micros >= 86_400_000_000 {
                // this is only possible due to rounding:
                // >= 23:59:59.999999500
                micros -= 86_400_000_000;
            }

            LocalTime { micros }
        }
    }

    impl From<LocalDatetime> for NaiveDateTime {
        fn from(value: LocalDatetime) -> NaiveDateTime {
            (&value).into()
        }
    }

    impl From<LocalDate> for NaiveDate {
        fn from(value: LocalDate) -> NaiveDate {
            (&value).into()
        }
    }

    impl TryFrom<NaiveDate> for LocalDate {
        type Error = OutOfRangeError;
        fn try_from(d: NaiveDate) -> Result<LocalDate, Self::Error> {
            std::convert::TryFrom::try_from(&d)
        }
    }

    impl From<LocalTime> for NaiveTime {
        fn from(value: LocalTime) -> NaiveTime {
            (&value).into()
        }
    }

    impl TryFrom<NaiveDateTime> for LocalDatetime {
        type Error = OutOfRangeError;
        fn try_from(d: NaiveDateTime) -> Result<LocalDatetime, Self::Error> {
            std::convert::TryFrom::try_from(&d)
        }
    }

    impl TryFrom<ChronoDatetime> for Datetime {
        type Error = OutOfRangeError;
        fn try_from(d: ChronoDatetime) -> Result<Datetime, Self::Error> {
            std::convert::TryFrom::try_from(&d)
        }
    }

    impl From<NaiveTime> for LocalTime {
        fn from(time: NaiveTime) -> LocalTime {
            From::from(&time)
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;
        use crate::model::time::test::{test_times, to_debug, valid_test_dates, CHRONO_MAX_YEAR};

        #[test]
        fn chrono_roundtrips() -> Result<(), Box<dyn std::error::Error>> {
            let naive = NaiveDateTime::from_str("2019-12-27T01:02:03.123456")?;
            assert_eq!(
                naive,
                Into::<NaiveDateTime>::into(LocalDatetime::try_from(naive)?)
            );
            let naive = NaiveDate::from_str("2019-12-27")?;
            assert_eq!(naive, Into::<NaiveDate>::into(LocalDate::try_from(naive)?));
            let naive = NaiveTime::from_str("01:02:03.123456")?;
            assert_eq!(naive, Into::<NaiveTime>::into(LocalTime::from(naive)));
            Ok(())
        }

        fn check_display<E: Display, A: Display>(expected_value: E, actual_value: A) {
            let expected_display = expected_value.to_string();
            let actual_display = actual_value.to_string();
            assert_eq!(expected_display, actual_display);
        }

        fn check_debug<E: Debug, A: Debug>(expected_value: E, actual_value: A) {
            let expected_debug = to_debug(expected_value);
            let actual_debug = to_debug(actual_value);
            assert_eq!(expected_debug, actual_debug);
        }

        #[test]
        fn format_local_time() {
            for time in test_times() {
                let actual_value = LocalTime::from_micros(time);
                let expected_value = NaiveTime::from(actual_value);

                check_display(expected_value, actual_value);
                check_debug(expected_value, actual_value);
            }
        }

        #[test]
        fn format_local_date() {
            let dates = valid_test_dates().filter(|d| d.0 <= CHRONO_MAX_YEAR);
            for (y, m, d) in dates {
                let actual_value = LocalDate::from_ymd(y, m, d);
                let expected = NaiveDate::from_ymd_opt(y, m as u32, d as u32).unwrap();

                check_display(expected, actual_value);
                check_debug(expected, actual_value);
            }
        }

        #[test]
        fn format_local_datetime() {
            let dates = valid_test_dates().filter(|d| d.0 <= CHRONO_MAX_YEAR);
            for date in dates {
                for time in test_times() {
                    let actual_date = LocalDate::from_ymd(date.0, date.1, date.2);
                    let actual_time = LocalTime::from_micros(time);
                    let actual_value = LocalDatetime::new(actual_date, actual_time);
                    let expected_value = NaiveDateTime::from(actual_value);

                    check_display(expected_value, actual_value);
                    check_debug(expected_value, actual_value);
                }
            }
        }

        #[test]
        fn format_datetime() {
            let dates = valid_test_dates().filter(|d| d.0 <= CHRONO_MAX_YEAR);
            for date in dates {
                for time in test_times() {
                    let actual_date = LocalDate::from_ymd(date.0, date.1, date.2);
                    let actual_time = LocalTime::from_micros(time);
                    let local_datetime = LocalDatetime::new(actual_date, actual_time);
                    let actual_value = local_datetime.to_utc();
                    let expected_value = ChronoDatetime::from(actual_value);

                    check_display(expected_value, actual_value);
                    check_debug(expected_value, actual_value);
                }
            }
        }

        #[test]
        fn date_duration() -> Result<(), Box<dyn std::error::Error>> {
            assert_eq!(DateDuration::from_years(1).to_string(), "P1Y");
            assert_eq!(DateDuration::from_months(1).to_string(), "P1M");
            assert_eq!(DateDuration::from_days(1).to_string(), "P1D");
            assert_eq!(DateDuration::from_months(10).to_string(), "P10M");
            assert_eq!(DateDuration::from_months(20).to_string(), "P1Y8M");
            assert_eq!(DateDuration::from_days(131).to_string(), "P131D");
            assert_eq!(
                (DateDuration::from_months(7) + DateDuration::from_days(131)).to_string(),
                "P7M131D"
            );
            Ok(())
        }
    }
}
