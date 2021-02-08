use crate::model::OutOfRangeError;
use std::convert::{TryFrom, TryInto};
use std::time::SystemTime;
use std::fmt::{Debug, Display};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Duration {
    pub(crate) micros: i64,
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LocalDatetime {
    pub(crate) micros: i64,
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LocalDate {
    pub(crate) days: i32,
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LocalTime {
    pub(crate) micros: u64,
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Datetime {
    pub(crate) micros: i64,
}

const SECS_PER_DAY : u64 = 86_400;
const MICROS_PER_DAY : u64 = SECS_PER_DAY * 1_000_000;

// leap years repeat every 400 years
const DAYS_IN_400_YEARS : u32 = 400 * 365 + 97;

const MIN_YEAR : i32 = -4713; // starts at 4713-11-24
const MAX_YEAR : i32 = 294_276; // ends at +294276-12-31

// year -4800 is a multiple of 400 smaller than the minimum supported year (-4713)
const BASE_YEAR : i32 = -4800;

#[allow(dead_code)] // only used by specific features
const DAYS_IN_2000_YEARS : i32 = 5 * DAYS_IN_400_YEARS as i32;

const DAY_TO_MONTH_365 : [u32; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];
const DAY_TO_MONTH_366 : [u32; 13] = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366];

impl Duration {
    pub const MIN : LocalDatetime = LocalDatetime { micros: i64::MIN };
    pub const MAX : LocalDatetime = LocalDatetime { micros: i64::MAX };

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
            return std::time::Duration::from_micros(
                u64::MAX - self.micros as u64 + 1);
        } else {
            return std::time::Duration::from_micros(self.micros as u64);
        }
    }
}

impl LocalDatetime {
    pub const MIN : LocalDatetime = LocalDatetime { micros: LocalDate::MIN.days as i64 * MICROS_PER_DAY as i64 };
    pub const MAX : LocalDatetime = LocalDatetime {
        micros: LocalDate::MAX.days as i64 * MICROS_PER_DAY as i64
         + LocalTime::MAX.micros as i64
    };

    fn try_from_micros(micros: i64) -> Result<LocalDatetime, OutOfRangeError> {
        if micros < Self::MIN.micros || micros > Self::MAX.micros {
            return Err(OutOfRangeError);
        }
        Ok(LocalDatetime { micros })
    }

    pub fn from_micros(micros: i64) -> LocalDatetime {
        Self::try_from_micros(micros).expect(&format!(
            "LocalDatetime::from_micros({}) is outside the valid datetime range",
             micros))
    }

    pub fn to_micros(self) -> i64 {
        self.micros
    }

    pub fn new(date: LocalDate, time: LocalTime) -> LocalDatetime {
        Self::from_micros(date.to_days() as i64 * MICROS_PER_DAY as i64 + time.to_micros() as i64)
    }

    pub fn date(self) -> LocalDate {
        LocalDate::from_days(self.micros.wrapping_div_euclid(MICROS_PER_DAY as i64) as i32)
    }

    pub fn time(self) -> LocalTime {
        LocalTime::from_micros(self.micros.wrapping_rem_euclid(MICROS_PER_DAY as i64) as u64)
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
    pub const MIDNIGHT : LocalTime = LocalTime { micros: 0 };
    pub const MAX : LocalTime = LocalTime { micros: MICROS_PER_DAY - 1 };

    pub(crate) fn try_from_micros(micros: u64) -> Result<LocalTime, OutOfRangeError> {
        if micros < MICROS_PER_DAY {
            Ok(LocalTime { micros: micros })
        } else {
             Err(OutOfRangeError)
        }
    }

    pub fn from_micros(micros: u64) -> LocalTime {
        Self::try_from_micros(micros).ok().expect("LocalTime is out of range")
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
    fn from_hmsu(hour: u8, minute: u8, second:u8, microsecond: u32) -> LocalTime {
        assert!(microsecond < 1000_000);
        assert!(second < 60);
        assert!(minute < 60);
        assert!(hour < 24);

        let micros =
        microsecond as u64
        + 1000_000 * (second as u64
             + 60 * (minute as u64
                + 60 * (hour as u64)));
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
    pub const MIN : LocalDate = LocalDate { days: -((2000 - (MIN_YEAR + 1)) * 365 + 1665) }; // -4713-11-24 in proleptic Gregorian or -4712-01-01 in Julian
    pub const MAX : LocalDate = LocalDate { days: (MAX_YEAR - 2000) * 365 + 71_242 }; // +294276-12-31
    pub const UNIX_EPOCH : LocalDate = LocalDate { days: -(30 * 365 + 7) }; // 1970-01-01

    fn try_from_days(days: i32) -> Result<LocalDate, OutOfRangeError> {
        if days < Self::MIN.days || days > Self::MAX.days {
            return Err(OutOfRangeError);
        }
        Ok(LocalDate { days })
    }

    pub fn from_days(days: i32) -> LocalDate {
        Self::try_from_days(days)
            .expect(&format!("LocalDate::from_days({}) is outside the valid date range", days))
    }

    pub fn to_days(self) -> i32 {
        self.days
    }

    pub fn from_ymd(year:i32, month: u8, day:u8) -> LocalDate {
        Self::try_from_ymd(year, month, day).expect(&format!(
            "invalid date {:04}-{:02}-{:02}",
            year, month, day))
    }

    fn try_from_ymd(year:i32, month: u8, day:u8) -> Result<LocalDate, OutOfRangeError> {
        if day < 1 || day > 31 {
            return Err(OutOfRangeError);
        }
        if month < 1 || month > 12 {
            return Err(OutOfRangeError);
        }
        if year < MIN_YEAR || year > MAX_YEAR {
           return Err(OutOfRangeError);
        }

        let passed_years = (year - BASE_YEAR - 1) as u32;
        let days_from_year =
            365 * passed_years
            + passed_years / 4
            - passed_years / 100
            + passed_years / 400
            + 366;

        let is_leap_year = (year % 400 == 0) || (year % 4 == 0 && year % 100 != 0);
        let day_to_month =
            if is_leap_year { DAY_TO_MONTH_366 } else { DAY_TO_MONTH_365 };

        let day_in_year = (day - 1) as u32 + day_to_month[month as usize - 1];
        if day_in_year >= day_to_month[month as usize] {
            return Err(OutOfRangeError);
        }

        LocalDate::try_from_days((days_from_year + day_in_year) as i32
         - DAYS_IN_400_YEARS as i32 * ((2000 - BASE_YEAR) / 400))
    }

    fn to_ymd(self) -> (i32, u8, u8) {
        const DAYS_IN_100_YEARS : u32 = 100 * 365 + 24;
        const DAYS_IN_4_YEARS :u32 = 4 * 365 + 1;
        const DAYS_IN_1_YEAR : u32 = 365;
        const DAY_TO_MONTH_MARCH : [u32; 12] = [0, 31, 61, 92, 122, 153, 184, 214, 245, 275, 306, 337];
        const MARCH_1 : u32 = 31 + 29;
        const MARCH_1_MINUS_BASE_YEAR_TO_POSTGRES_EPOCH : u32
            = (2000 - BASE_YEAR) as u32 / 400 * DAYS_IN_400_YEARS - MARCH_1;

        let days = (self.days as u32).wrapping_add(MARCH_1_MINUS_BASE_YEAR_TO_POSTGRES_EPOCH);

        let years400 = days / DAYS_IN_400_YEARS;
        let days = days % DAYS_IN_400_YEARS;

        let mut years100 = days / DAYS_IN_100_YEARS;
        if years100 == 4 { years100 = 3 }; // prevent 400 year leap day from overflowing
        let days = days - DAYS_IN_100_YEARS * years100;

        let years4 = days / DAYS_IN_4_YEARS;
        let days = days % DAYS_IN_4_YEARS;

        let mut years1 = days / DAYS_IN_1_YEAR;
        if years1 == 4 { years1 = 3 }; // prevent 4 year leap day from overflowing
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
        if year >= 10_000 { // ISO format requires a + on dates longer than 4 digits
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
    pub const MIN : Datetime = Datetime { micros: LocalDatetime::MIN.micros };
    pub const MAX : Datetime = Datetime { micros: LocalDatetime::MAX.micros };
    pub const UNIX_EPOCH : Datetime = Datetime { micros: LocalDate::UNIX_EPOCH.days as i64 * MICROS_PER_DAY as i64 };

    pub fn try_from_micros(micros: i64) -> Result<Datetime, OutOfRangeError> {
        if micros < Self::MIN.micros || micros > Self::MAX.micros {
            return Err(OutOfRangeError);
        }
        Ok(Datetime { micros })
    }

    pub fn from_micros(micros: i64) -> Datetime {
        Self::try_from_micros(micros).expect(&format!(
            "Datetime::from_micros({}) is outside the valid datetime range",
             micros))
    }

    pub fn to_micros(self) -> i64 {
        self.micros
    }

    fn postgres_epoch_unix() -> SystemTime {
        use std::time::{ Duration, UNIX_EPOCH };
        // postgres epoch starts at 2020-01-01
        UNIX_EPOCH + Duration::from_micros((-Datetime::UNIX_EPOCH.micros) as u64)
    }

    // I believe this never fails on Linux
    // On Windows it has a smaller maximum value than EdgeDB's native Datetime type
    fn to_system_time(self) -> Result<SystemTime, OutOfRangeError> {
        use std::time::Duration;

        if self.micros > 0 {
            Self::postgres_epoch_unix().checked_add(Duration::from_micros(self.micros as u64))
        } else {
            Self::postgres_epoch_unix().checked_sub(Duration::from_micros((-self.micros) as u64))
        }.ok_or(OutOfRangeError)
    }

    // I believe this can fail on both Windows and Linux, since Postgres can "only" store dates starting 4713 BC
    fn from_system_time(time:SystemTime) -> Result<Datetime, OutOfRangeError> {
        let postgres_epoch = Self::postgres_epoch_unix();

        let nanos = if time >= postgres_epoch {
            time.duration_since(postgres_epoch).unwrap().as_nanos() as i128
        } else {
            -(postgres_epoch.duration_since(time).unwrap().as_nanos() as i128)
        };
        let micros = nanos.wrapping_div_euclid(1000);
        let micros = i64::try_from(micros).map_err(|_| OutOfRangeError)?;
        Ok(Datetime::from_micros(micros))
    }
}

impl Display for Datetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} UTC", LocalDatetime::from_micros(self.to_micros()))
    }
}

impl Debug for Datetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}Z", LocalDatetime::from_micros(self.to_micros()))
    }
}

impl TryFrom<Datetime> for SystemTime {
    type Error = OutOfRangeError;

    fn try_from(value: Datetime) -> Result<Self, Self::Error> {
        Datetime::to_system_time(value)
    }
}

impl TryFrom<SystemTime> for Datetime {
    type Error = OutOfRangeError;

    fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
        Datetime::from_system_time(value)
    }
}
impl std::ops::Add<&'_ std::time::Duration> for Datetime {
    type Output = Datetime;
    fn add(self, other: &std::time::Duration) -> Datetime {
        let micros = match other.as_micros().try_into() {
            Ok(m) => m,
            Err(_) => {
                // crash in debug mode
                debug_assert!(false,
                    "resulting datetime is out of range");
                // saturate in release mode
                return Datetime::MAX;
            }
        };
        let micros = self.micros.saturating_add(micros);
        if micros > Datetime::MAX.micros {
            // crash in debug mode
            debug_assert!(false,
                "resulting datetime is out of range");
            // saturate in release mode
            return Datetime::MAX;
        }
        return Datetime { micros };
    }
}

impl std::ops::Add<std::time::Duration> for Datetime {
    type Output = Datetime;
    fn add(self, other: std::time::Duration) -> Datetime {
        self + &other
    }
}

impl Display for Duration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let abs = if self.micros < 0 {
            write!(f, "-")?;
            - self.micros
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
            write!(f, "{hours:02}:{minutes:02}:{seconds:02}.{fract:0>fsize$}",
                hours=sec / 3600,
                minutes=sec / 60 % 60,
                seconds=sec % 60,
                fract=fract,
                fsize=6 - zeros,
            )
        } else {
            write!(f, "{:02}:{:02}:{:02}",
                sec / 3600, sec / 60 % 60, sec % 60)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn big_duration_abs() {
        use super::Duration as Src;
        use std::time::Duration as Trg;
        assert_eq!(Src { micros: -1 }.abs_duration(), Trg::new(0, 1000));
        assert_eq!(Src { micros: -1000 }.abs_duration(), Trg::new(0, 1000000));
        assert_eq!(Src { micros: -1000000 }.abs_duration(), Trg::new(1, 0));
        assert_eq!(
            Src {
                micros: i64::min_value()
            }
            .abs_duration(),
            Trg::new(9223372036854, 775808000)
        );
    }

    #[test]
    fn local_date_from_ymd() {
        assert_eq!(0, LocalDate::from_ymd(2000, 1, 1).to_days());
        assert_eq!(-365, LocalDate::from_ymd(1999, 1, 1).to_days());
        assert_eq!(366, LocalDate::from_ymd(2001, 1, 1).to_days());
        assert_eq!(-730119, LocalDate::from_ymd(0001, 1, 1).to_days());
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

            let end_of_month = LocalDate::from_ymd(2004, month as u8, days_in_current_month as u8).to_days();
            assert_eq!(total_days - 1, end_of_month - start_of_year);
        }
        assert_eq!(366, total_days);
    }

    const DAYS_IN_MONTH_LEAP :[u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    #[test]
    fn local_date_from_ymd_normal_year() {
        let mut total_days = 0;
        let start_of_year = 365 + 1;
        for month in 1..=12 {
            let start_of_month = LocalDate::from_ymd(2001, month as u8, 1).to_days();
            assert_eq!(total_days, start_of_month - start_of_year);

            let days_in_current_month = DAYS_IN_MONTH_LEAP[month - 1];
            total_days += days_in_current_month as i32;

            let end_of_month = LocalDate::from_ymd(2001, month as u8, days_in_current_month as u8).to_days();
            assert_eq!(total_days - 1, end_of_month - start_of_year);
        }
        assert_eq!(365, total_days);
    }

    pub const CHRONO_MAX_YEAR : i32 = 262_143;

    fn extended_test_dates() -> impl Iterator<Item=(i32, u8, u8)> {
        const YEARS :[i32; 41]= [
            MIN_YEAR + 1,
            -4700,
            -4400,
            -1,
            0,
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

        const MONTHS : std::ops::RangeInclusive<u8>= 1u8..=12;
        const DAYS :[u8; 6] = [1u8, 13, 28, 29, 30, 31];
        let dates = MONTHS
            .flat_map(|month| DAYS.iter().map(move |day| (month, *day)));

        YEARS
            .iter()
            .flat_map(move|year| dates.clone().map(move |date| (*year, date.0, date.1)))
    }

    pub fn valid_test_dates() -> impl Iterator<Item=(i32, u8, u8)> {
        extended_test_dates().filter(|date|
                LocalDate::try_from_ymd(date.0, date.1, date.2).is_ok())
    }

    pub fn test_times() -> impl Iterator<Item=u64> {
        const TIMES: [u64; 7] = [
            0,
            10,
            10_020,
            12345 * 1000_000,
            12345 * 1001_000,
            12345 * 1001_001,
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
        assert_eq!("0000-01-01", LocalDate::from_days(-DAYS_IN_2000_YEARS).to_string());
        assert_eq!("0001-01-01", LocalDate::from_days(-DAYS_IN_2000_YEARS + 366).to_string());
        assert_eq!("-0001-01-01", LocalDate::from_days(-DAYS_IN_2000_YEARS - 365).to_string());
        assert_eq!("-4000-01-01", LocalDate::from_days(-3 * DAYS_IN_2000_YEARS as i32).to_string());
        assert_eq!("+10000-01-01", LocalDate::from_days(4 * DAYS_IN_2000_YEARS as i32).to_string());
        assert_eq!("9999-12-31", LocalDate::from_days(4 * DAYS_IN_2000_YEARS as i32 - 1).to_string());
        assert_eq!("+10001-01-01", LocalDate::from_days(4 * DAYS_IN_2000_YEARS as i32 + 366).to_string());
        assert_eq!("-4713-11-24", LocalDate::MIN.to_string());
        assert_eq!("+294276-12-31", LocalDate::MAX.to_string());
    }

    #[test]
    fn format_local_time() {
        assert_eq!("00:00:00", LocalTime::MIDNIGHT.to_string());
        assert_eq!("00:00:00.010", LocalTime::from_micros(10_000).to_string());
        assert_eq!("00:00:00.010020", LocalTime::from_micros(10_020).to_string());
        assert_eq!("23:59:59.999999", LocalTime::MAX.to_string());
    }

    pub fn to_debug<T:Debug>(x:T) -> String {
        format!("{:?}", x)
    }

    #[test]
    fn format_local_datetime() {
        assert_eq!("2039-02-13 23:31:30.123456", LocalDatetime::from_micros(1_234_567_890_123_456).to_string());
        assert_eq!("2039-02-13T23:31:30.123456", to_debug(LocalDatetime::from_micros(1_234_567_890_123_456)));

        assert_eq!("-4713-11-24 00:00:00", LocalDatetime::MIN.to_string());
        assert_eq!("-4713-11-24T00:00:00", to_debug(LocalDatetime::MIN));

        assert_eq!("+294276-12-31 23:59:59.999999", LocalDatetime::MAX.to_string());
        assert_eq!("+294276-12-31T23:59:59.999999", to_debug(LocalDatetime::MAX));
    }

    #[test]
    fn format_datetime() {
        assert_eq!("2039-02-13 23:31:30.123456 UTC", Datetime::from_micros(1_234_567_890_123_456).to_string());
        assert_eq!("2039-02-13T23:31:30.123456Z", to_debug(Datetime::from_micros(1_234_567_890_123_456)));

        assert_eq!("-4713-11-24 00:00:00 UTC", Datetime::MIN.to_string());
        assert_eq!("-4713-11-24T00:00:00Z", to_debug(Datetime::MIN));

        assert_eq!("+294276-12-31 23:59:59.999999 UTC", Datetime::MAX.to_string());
        assert_eq!("+294276-12-31T23:59:59.999999Z", to_debug(Datetime::MAX));
    }

    #[test]
    fn format_duration() {
        fn dur_str(msec: i64) -> String {
            Duration::from_micros(msec).to_string()
        }
        assert_eq!(dur_str(1_000_000), "00:00:01");
        assert_eq!(dur_str(1), "00:00:00.000001");
        assert_eq!(dur_str(7_015_000), "00:00:07.015");
        assert_eq!(dur_str(10_000_000__015_000), "2777:46:40.015");
        assert_eq!(dur_str(12_345_678__000_000), "3429:21:18");
    }
}

#[cfg(feature = "chrono")]
mod chrono_interop {
    use super::*;
    use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime };
    use std::convert::{From, Into, TryFrom, TryInto};

    type ChronoDatetime = chrono::DateTime<chrono::Utc>;

    impl TryFrom<&LocalDatetime> for NaiveDateTime {
        type Error = OutOfRangeError;
        fn try_from(value: &LocalDatetime) -> Result<NaiveDateTime, Self::Error> {
            // convert between epochs after converting to seconds to avoid integer overflows for values close to the maximum
            // however it looks like from_timestamp_opt fails for these values anyways
            let timestamp_seconds = value.micros.wrapping_div_euclid(1000_000) - (Datetime::UNIX_EPOCH.micros / 1000_000);
            let timestamp_nanos = (value.micros.wrapping_rem_euclid(1000_000) * 1000) as u32;
            NaiveDateTime::from_timestamp_opt(timestamp_seconds, timestamp_nanos)
                .ok_or(OutOfRangeError)
        }
    }

    impl TryFrom<&NaiveDateTime> for LocalDatetime {
        type Error = OutOfRangeError;
        fn try_from(d: &NaiveDateTime)
            -> Result<LocalDatetime, Self::Error>
        {
            let secs = d.timestamp();
            let micros = d.timestamp_subsec_micros();
            let timestamp = secs.checked_mul(1_000_000)
                .and_then(|x| x.checked_add(micros as i64))
                .ok_or(OutOfRangeError)?;
            Ok(LocalDatetime {
                micros: timestamp.checked_add(Datetime::UNIX_EPOCH.micros)
                    .ok_or(OutOfRangeError)?,
            })
        }
    }

    impl TryFrom<&Datetime> for ChronoDatetime {
        type Error = OutOfRangeError;

        fn try_from(value:&Datetime) -> Result<ChronoDatetime, Self::Error> {
            use chrono::TimeZone;

            let postgres_epoch = chrono::Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
            let duration = chrono::Duration::microseconds(value.micros);
            // this overflows for large values,
            // chrono uses an epoch based on year 0
            postgres_epoch.checked_add_signed(duration)
                .ok_or(OutOfRangeError)
        }
    }

    impl TryFrom<&ChronoDatetime> for Datetime {
        type Error = OutOfRangeError;

        fn try_from(value:&ChronoDatetime) -> Result<Datetime, Self::Error> {
           let min = ChronoDatetime::try_from(Datetime::from_micros(i64::MIN)).unwrap();
           let duration = value.signed_duration_since(min).to_std().map_err(|_| OutOfRangeError)?;
           let micros = u64::try_from(duration.as_micros()).map_err(|_| OutOfRangeError)?;
           let micros = i64::MIN.wrapping_add(micros as i64);
           Ok(Datetime::from_micros(micros))
        }
    }

    impl TryFrom<&NaiveDate> for LocalDate {
        type Error = OutOfRangeError;
        fn try_from(d: &NaiveDate) -> Result<LocalDate, Self::Error>
        {
            let days = chrono::Datelike::num_days_from_ce(d);
            Ok(LocalDate {
                days: days.checked_sub(DAYS_IN_2000_YEARS - 365)
                    .ok_or(OutOfRangeError)?,
            })
        }
    }

    impl TryFrom<&LocalDate> for NaiveDate {
        type Error = OutOfRangeError;
        fn try_from(value: &LocalDate) -> Result<NaiveDate, Self::Error> {
            value.days.checked_add(DAYS_IN_2000_YEARS - 365)
            .and_then(NaiveDate::from_num_days_from_ce_opt)
            .ok_or(OutOfRangeError)
        }
    }

    impl From<&LocalTime> for NaiveTime {
        fn from(value: &LocalTime) -> NaiveTime {
            NaiveTime::from_num_seconds_from_midnight(
                (value.micros / 1000_000) as u32,
                ((value.micros % 1000_000) * 1000) as u32)
        }
    }

    impl From<&NaiveTime> for LocalTime {
        fn from(time: &NaiveTime) -> LocalTime {
            let sec = chrono::Timelike::num_seconds_from_midnight(time);
            let nanos = chrono::Timelike::nanosecond(time);
            LocalTime {
                micros: sec as u64 * 1000_000 + nanos as u64 / 1000,
            }
        }
    }

    impl TryFrom<LocalDatetime> for NaiveDateTime {
        type Error = OutOfRangeError;
        fn try_from(value: LocalDatetime) -> Result<NaiveDateTime, Self::Error> {
            (&value).try_into()
        }
    }

    impl TryFrom<Datetime> for ChronoDatetime {
        type Error = OutOfRangeError;

        fn try_from(value: Datetime) -> Result<ChronoDatetime, Self::Error> {
            (&value).try_into()
        }
    }

    impl TryFrom<LocalDate> for NaiveDate {
        type Error = OutOfRangeError;
        fn try_from(value: LocalDate) -> Result<NaiveDate, Self::Error> {
            (&value).try_into()
        }
    }

    impl TryFrom<NaiveDate> for LocalDate {
        type Error = OutOfRangeError;
        fn try_from(d: NaiveDate) -> Result<LocalDate, Self::Error>
        {
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
        fn try_from(d: NaiveDateTime)
            -> Result<LocalDatetime, Self::Error>
        {
            std::convert::TryFrom::try_from(&d)
        }
    }

    impl TryFrom<ChronoDatetime> for Datetime {
        type Error = OutOfRangeError;
        fn try_from(d: ChronoDatetime)
            -> Result<Datetime, Self::Error>
        {
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
        use crate::model::time::test::{ test_times, valid_test_dates, to_debug, CHRONO_MAX_YEAR};
        use crate::model::time::Datetime;
        use std::convert::{TryFrom, TryInto};
        use std::str::FromStr;
        use std::fmt::{ Display, Debug };

        #[test]
        fn chrono_roundtrips() -> Result<(), Box<dyn std::error::Error>> {
            let naive = NaiveDateTime::from_str("2019-12-27T01:02:03.123456")?;
            assert_eq!(naive,
                TryInto::<NaiveDateTime>::try_into(
                    LocalDatetime::try_from(naive)?)?);
            let naive = NaiveDate::from_str("2019-12-27")?;
            assert_eq!(naive,
                TryInto::<NaiveDate>::try_into(LocalDate::try_from(naive)?)?);
            let naive = NaiveTime::from_str("01:02:03.123456")?;
            assert_eq!(naive,
                TryInto::<NaiveTime>::try_into(LocalTime::try_from(naive)?)?);
            Ok(())
        }

        fn check_display<E:Display, A:Display>(expected_value:E, actual_value:A) {
            let expected_display = expected_value.to_string();
            let actual_display = actual_value.to_string();
            assert_eq!(expected_display, actual_display);
        }

        fn check_debug<E:Debug, A:Debug>(expected_value:E, actual_value:A) {
            let expected_debug = to_debug(expected_value);
            let actual_debug = to_debug(actual_value);
            assert_eq!(expected_debug, actual_debug);
        }

        #[test]
        fn format_local_time() {
            for time in test_times() {
                let actual_value = LocalTime::from_micros(time);
                let expected_value = NaiveTime::try_from(actual_value).unwrap();

                check_display(expected_value, actual_value);
                check_debug(expected_value, actual_value);
            }
        }

        #[test]
        fn format_local_date() {
            let dates = valid_test_dates().filter(|d| d.0 <= CHRONO_MAX_YEAR);
            for date in dates {
                let actual_value = LocalDate::from_ymd(date.0, date.1, date.2);
                let expected_value = NaiveDate::from_ymd(date.0, date.1 as u32, date.2 as u32);

                check_display(expected_value, actual_value);
                check_debug(expected_value, actual_value);
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
                    let expected_value = NaiveDateTime::try_from(actual_value).expect(&format!("Could not convert LocalDatetime '{}'", actual_value));

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
                    let actual_value = Datetime::from_micros(local_datetime.to_micros());
                    let expected_value = ChronoDatetime::try_from(actual_value).expect(&format!("Could not convert Datetime '{}'", actual_value));

                    check_display(expected_value, actual_value);
                    check_debug(expected_value, actual_value);
                }
            }
        }
    }
}
