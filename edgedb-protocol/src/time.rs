#[derive(Clone, Debug, PartialEq)]
pub struct Duration {
    pub(crate) micros: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDatetime {
    pub(crate) micros: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDate {
    pub(crate) days: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalTime {
    pub(crate) micros: i64,
}


impl Duration {
    pub fn from_micros(micros: i64) -> Duration {
        Duration { micros }
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
    pub fn from_micros(micros: i64) -> LocalDatetime {
        return LocalDatetime { micros }
    }
}

impl LocalTime {
    pub fn from_micros(micros: u64) -> LocalTime {
        assert!(micros < 86400*1000_000);
        return LocalTime { micros: micros as i64  }
    }
}

impl LocalDate {
    pub fn from_days(days: i32) -> LocalDate {
        return LocalDate { days }
    }
}

#[cfg(test)]
mod test {
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
}

#[cfg(feature = "chrono")]
mod chrono_interop {
    use super::{LocalDate, LocalDatetime, LocalTime};
    use crate::value::OutOfRange;
    use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};

    impl std::convert::TryInto<NaiveDateTime> for &LocalDatetime {
        type Error = OutOfRange;
        fn try_into(self) -> Result<NaiveDateTime, Self::Error> {
            NaiveDateTime::from_timestamp_opt(self.micros/1000_000,
                ((self.micros % 1000_000)*1000) as u32)
            .ok_or(OutOfRange)
        }
    }

    impl std::convert::TryFrom<&NaiveDateTime> for LocalDatetime {
        type Error = OutOfRange;
        fn try_from(d: &NaiveDateTime)
            -> Result<LocalDatetime, Self::Error>
        {
            let secs = d.timestamp();
            let micros = d.timestamp_subsec_micros();
            Ok(LocalDatetime {
                micros: secs.checked_mul(1_000_000)
                    .and_then(|x| x.checked_add(micros as i64))
                    .ok_or(OutOfRange)?,
            })
        }
    }

    impl std::convert::TryFrom<&NaiveDate> for LocalDate {
        type Error = OutOfRange;
        fn try_from(d: &NaiveDate) -> Result<LocalDate, Self::Error>
        {
            let days = chrono::Datelike::num_days_from_ce(d);
            Ok(LocalDate {
                days: days.checked_sub(730120)
                    .ok_or(OutOfRange)?,
            })
        }
    }

    impl std::convert::TryInto<NaiveDate> for &LocalDate {
        type Error = OutOfRange;
        fn try_into(self) -> Result<NaiveDate, Self::Error> {
            self.days.checked_add(730120)
            .and_then(NaiveDate::from_num_days_from_ce_opt)
            .ok_or(OutOfRange)
        }
    }

    impl Into<NaiveTime> for &LocalTime {
        fn into(self) -> NaiveTime {
            NaiveTime::from_num_seconds_from_midnight(
                (self.micros / 1000_000) as u32,
                ((self.micros % 1000_000) * 1000) as u32)
        }
    }

    impl From<&NaiveTime> for LocalTime {
        fn from(time: &NaiveTime) -> LocalTime {
            let sec = chrono::Timelike::num_seconds_from_midnight(time);
            let nanos = chrono::Timelike::nanosecond(time);
            LocalTime {
                micros: sec as i64 * 1000_000 + nanos as i64 / 1000,
            }
        }
    }

    impl std::convert::TryInto<NaiveDateTime> for LocalDatetime {
        type Error = OutOfRange;
        fn try_into(self) -> Result<NaiveDateTime, Self::Error> {
            (&self).try_into()
        }
    }

    impl std::convert::TryInto<NaiveDate> for LocalDate {
        type Error = OutOfRange;
        fn try_into(self) -> Result<NaiveDate, Self::Error> {
            (&self).try_into()
        }
    }

    impl std::convert::TryFrom<NaiveDate> for LocalDate {
        type Error = OutOfRange;
        fn try_from(d: NaiveDate) -> Result<LocalDate, Self::Error>
        {
            std::convert::TryFrom::try_from(&d)
        }
    }

    impl Into<NaiveTime> for LocalTime {
        fn into(self) -> NaiveTime {
            (&self).into()
        }
    }

    impl std::convert::TryFrom<NaiveDateTime> for LocalDatetime {
        type Error = OutOfRange;
        fn try_from(d: NaiveDateTime)
            -> Result<LocalDatetime, Self::Error>
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
        use std::convert::{TryFrom, TryInto};
        use std::str::FromStr;

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
    }
}