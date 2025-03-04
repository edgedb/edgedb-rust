//! Internal helpers for parsing and formatting durations. These were copied
//! from gel_protocol and stripped down to only include the functionality
//! required by gel-dsn.

use std::{
    fmt::{self},
    str::FromStr,
};

/// Error parsing string into Gel Duration type.
#[derive(Debug, PartialEq)]
pub struct ParseDurationError {
    pub(crate) message: String,
    pub(crate) pos: usize,
    pub(crate) is_final: bool,
}

impl std::error::Error for ParseDurationError {}
impl fmt::Display for ParseDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_args!(
            "Error parsing input at position {}: {}",
            self.pos, self.message
        )
        .fmt(f)
    }
}

impl From<std::num::ParseIntError> for ParseDurationError {
    fn from(e: std::num::ParseIntError) -> Self {
        Self::new(format!("{}", e))
    }
}

impl ParseDurationError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            pos: 0,
            message: message.into(),
            is_final: true,
        }
    }
    pub(crate) fn not_final(mut self) -> Self {
        self.is_final = false;
        self
    }
    pub(crate) fn pos(mut self, value: usize) -> Self {
        self.pos = value;
        self
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Duration {
    pub(crate) micros: i64,
}

/// A type that can represent a human-friendly duration like 1 month or two days.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct RelativeDuration {
    pub(crate) micros: i64,
}

// leap years repeat every 400 years
const DAYS_IN_400_YEARS: u32 = 400 * 365 + 97;

#[allow(dead_code)] // only used by specific features
const DAYS_IN_2000_YEARS: i32 = 5 * DAYS_IN_400_YEARS as i32;

const MICROS_PER_MS: i64 = 1_000;
const MICROS_PER_SECOND: i64 = MICROS_PER_MS * 1_000;
const MICROS_PER_MINUTE: i64 = MICROS_PER_SECOND * 60;
const MICROS_PER_HOUR: i64 = MICROS_PER_MINUTE * 60;

impl Duration {
    pub fn from_micros(micros: i64) -> Duration {
        Duration { micros }
    }

    pub fn to_micros(self) -> i64 {
        self.micros
    }

    #[allow(clippy::inherent_to_string, clippy::wrong_self_convention)]
    pub fn to_string(&self) -> String {
        RelativeDuration {
            micros: self.micros,
        }
        .to_string()
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
                        seen.push(u);
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

impl fmt::Display for RelativeDuration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.micros == 0 {
            return write!(f, "PT0S");
        }
        write!(f, "P")?;
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

#[cfg(test)]
mod test {
    use super::*;

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
}
