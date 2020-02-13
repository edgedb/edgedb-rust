use std::fmt;
use std::time::{SystemTime};
use std::{u32, u64};

use uuid::Uuid;

use crate::codec::{NamedTupleShape, ObjectShape, EnumValue};

#[derive(Clone, Debug, PartialEq)]
pub struct Duration {
    pub(crate) micros: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Nothing,
    Uuid(Uuid),
    Str(String),
    Bytes(Vec<u8>),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    BigInt(BigInt),
    Decimal(Decimal),
    Bool(bool),
    Datetime(SystemTime),
    LocalDatetime(LocalDatetime),
    LocalDate(LocalDate),
    LocalTime(LocalTime),
    Duration(Duration),
    Json(String),  // or should we use serde::Json?
    Set(Vec<Value>),
    Object { shape: ObjectShape, fields: Vec<Value> },
    Tuple(Vec<Value>),
    NamedTuple { shape: NamedTupleShape, fields: Vec<Value> },
    Array(Vec<Value>),
    Enum(EnumValue),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BigInt {
    pub(crate) negative: bool,
    pub(crate) weight: i16,
    pub(crate) digits: Vec<u16>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Decimal {
    pub(crate) negative: bool,
    pub(crate) weight: i16,
    pub(crate) decimal_digits: u16,
    pub(crate) digits: Vec<u16>,
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

#[derive(Debug)]
pub struct OutOfRange;

impl std::error::Error for OutOfRange {}
impl fmt::Display for OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "value is out of range".fmt(f)
    }
}

impl From<std::num::TryFromIntError> for OutOfRange {
    fn from(_: std::num::TryFromIntError) -> OutOfRange {
        OutOfRange
    }
}


impl Value {
    pub fn kind(&self) -> &'static str {
        use Value::*;
        match self {
            Nothing => "nothing",
            Uuid(..) => "uuid",
            Str(..) => "string",
            Bytes(..) => "bytes",
            Int16(..) => "int16",
            Int32(..) => "int32",
            Int64(..) => "int64",
            Float32(..) => "float32",
            Float64(..) => "float64",
            BigInt(..) => "bigint",
            Decimal(..) => "decimal",
            Bool(..) => "bool",
            Datetime(..) => "datetime",
            LocalDatetime(..) => "cal::local_datetime",
            LocalDate(..) => "cal::local_date",
            LocalTime(..) => "cal::local_time",
            Duration(..) => "duration",
            Json(..) => "json",
            Set(..) => "set",
            Object { .. } => "object",
            Tuple(..) => "tuple",
            NamedTuple { .. } => "named_tuple",
            Array(..) => "array",
            Enum(..) => "enum",
        }
    }
    pub fn empty_tuple() -> Value {
        Value::Tuple(Vec::new())
    }
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

impl BigInt {
    fn normalize(mut self) -> BigInt {
        while let Some(0) = self.digits.last() {
            self.digits.pop();
        }
        while let Some(0) = self.digits.first() {
            self.digits.remove(0);
            self.weight -= 1;
        }
        return self
    }
}

impl From<u64> for BigInt {
    fn from(v: u64) -> BigInt {
        return BigInt {
            negative: false,
            weight: 3,
            digits: vec![
                (v / 10000_0000_0000 % 10000) as u16,
                (v / 10000_0000 % 10000) as u16,
                (v / 10000 % 10000) as u16,
                (v % 10000) as u16,
            ],
        }.normalize();
    }
}

impl From<i64> for BigInt {
    fn from(v: i64) -> BigInt {
        let (abs, negative) = if v < 0 {
            (u64::MAX - v as u64 + 1, true)
        } else {
            (v as u64, false)
        };
        return BigInt {
            negative,
            weight: 3,
            digits: vec![
                (abs / 10000_0000_0000 % 10000) as u16,
                (abs / 10000_0000 % 10000) as u16,
                (abs / 10000 % 10000) as u16,
                (abs % 10000) as u16,
            ],
        }.normalize();
    }
}

impl From<u32> for BigInt {
    fn from(v: u32) -> BigInt {
        return BigInt {
            negative: false,
            weight: 1,
            digits: vec![(v / 10000) as u16, (v % 10000) as u16],
        }.normalize();
    }
}

impl From<i32> for BigInt {
    fn from(v: i32) -> BigInt {
        let (abs, negative) = if v < 0 {
            (u32::MAX - v as u32 + 1, true)
        } else {
            (v as u32, false)
        };
        return BigInt {
            negative,
            weight: 1,
            digits: vec![(abs / 10000) as u16, (abs % 10000) as u16],
        }.normalize();
    }
}

#[cfg(feature="num-bigint")]
impl std::convert::TryFrom<num_bigint::BigInt> for BigInt {
    type Error = OutOfRange;
    fn try_from(v: num_bigint::BigInt) -> Result<BigInt, Self::Error> {
        use num_traits::{ToPrimitive, Zero};
        use std::convert::TryInto;

        if v.is_zero() {
            return Ok(BigInt {
                negative: false,
                weight: 0,
                digits: Vec::new(),
            });
        }

        let mut digits = Vec::new();
        let (negative, mut val) = match v.sign() {
            num_bigint::Sign::Minus => (true, -v),
            num_bigint::Sign::NoSign => (false, v),
            num_bigint::Sign::Plus => (false, v),
        };
        while !val.is_zero() {
            digits.push((&val % 10000u16).to_u16().unwrap());
            val /= 10000;
        }
        digits.reverse();

        // This returns "out of range integral type conversion attempted"
        // which should be good enough for this error
        let weight = (digits.len() - 1).try_into()?;

        // TODO(tailhook) normalization can be optimized here
        return Ok(BigInt {
            negative,
            weight,
            digits,
        }.normalize())
    }
}

impl Decimal {
    #[allow(dead_code)]  // isn't used when BigDecimal is disabled
    fn normalize(mut self) -> Decimal {
        while let Some(0) = self.digits.last() {
            self.digits.pop();
        }
        while let Some(0) = self.digits.first() {
            self.digits.remove(0);
            self.weight -= 1;
        }
        return self
    }
}

#[cfg(feature="bigdecimal")]
impl std::convert::TryFrom<bigdecimal::BigDecimal> for Decimal {
    type Error = OutOfRange;
    fn try_from(dec: bigdecimal::BigDecimal) -> Result<Decimal, Self::Error> {
        use num_traits::{ToPrimitive, Zero};
        use std::convert::TryInto;

        let mut digits = Vec::new();
        let (v, decimal_digits) = dec.into_bigint_and_exponent();
        let (negative, mut val) = match v.sign() {
            num_bigint::Sign::Minus => (true, -v),
            num_bigint::Sign::NoSign => (false, v),
            num_bigint::Sign::Plus => (false, v),
        };
        if decimal_digits % 4 > 0 {
            val *= 10u16.pow(4 - (decimal_digits % 4) as u32);
        }
        while !val.is_zero() {
            digits.push((&val % 10000u16).to_u16().unwrap());
            val /= 10000;
        }
        digits.reverse();

        // These return "out of range integral type conversion attempted"
        // which should be good enough for this error
        let decimal_digits = decimal_digits.try_into()?;
        let weight = i16::try_from(digits.len())? -
            (decimal_digits+3) as i16/4 - 1;

        // TODO(tailhook) normalization can be optimized here
        return Ok(Decimal {
            negative,
            weight,
            decimal_digits,
            digits,
        }.normalize())
    }
}

#[cfg(feature="bigdecimal")]
impl Into<bigdecimal::BigDecimal> for Decimal {
    fn into(self) -> bigdecimal::BigDecimal {
        (&self).into()
    }
}

#[cfg(feature="bigdecimal")]
impl Into<bigdecimal::BigDecimal> for &Decimal {
    fn into(self) -> bigdecimal::BigDecimal {
        use bigdecimal::BigDecimal;
        use num_bigint::BigInt;
        use num_traits::pow;
        use std::cmp::max;

        let mut r = BigInt::from(0);
        for &digit in &self.digits {
            r *= 10000;
            r += digit;
        }
        let digits_stored = max(0,
            (self.digits.len() as i64 - self.weight as i64 - 1)*4,
        ) as usize;
        if digits_stored < self.decimal_digits as usize {
            r *= pow(10, self.decimal_digits as usize - digits_stored);
            if (self.weight+1) as usize > self.digits.len() {
                r *= pow(BigInt::from(10000),
                         (self.weight+1) as usize - self.digits.len());
            }
        } else {
            let rem = self.decimal_digits % 4;
            if rem > 0 {
                r /= pow(10, (4 - rem) as usize);
            }
        }
        if self.negative {
            r = -r;
        }
        return BigDecimal::new(r, self.decimal_digits as i64)
    }
}

#[cfg(feature="num-bigint")]
impl Into<num_bigint::BigInt> for BigInt {
    fn into(self) -> num_bigint::BigInt {
        (&self).into()
    }
}

#[cfg(feature="num-bigint")]
impl Into<num_bigint::BigInt> for &BigInt {
    fn into(self) -> num_bigint::BigInt {
        use num_bigint::BigInt;
        use num_traits::pow;

        let mut r = BigInt::from(0);
        for &digit in &self.digits {
            r *= 10000;
            r += digit;
        }
        if (self.weight+1) as usize > self.digits.len() {
            r *= pow(BigInt::from(10000),
                     (self.weight+1) as usize - self.digits.len());
        }
        if self.negative {
            return -r;
        }
        return r;
    }
}

impl LocalDatetime {
    pub fn from_micros(micros: i64) -> LocalDatetime {
        return LocalDatetime { micros }
    }
}

impl LocalTime {
    pub fn from_micros(micros: u64) -> LocalTime {
        assert!(micros < 86400*1000_1000);
        return LocalTime { micros: micros as i64  }
    }
}

impl LocalDate {
    pub fn from_days(days: i32) -> LocalDate {
        return LocalDate { days }
    }
}

#[cfg(feature="chrono")]
impl std::convert::TryInto<chrono::naive::NaiveDateTime> for &LocalDatetime {
    type Error = OutOfRange;
    fn try_into(self) -> Result<chrono::naive::NaiveDateTime, Self::Error> {
        chrono::naive::NaiveDateTime::from_timestamp_opt(self.micros/1000_000,
            ((self.micros % 1000_000)*1000) as u32)
        .ok_or(OutOfRange)
    }
}

#[cfg(feature="chrono")]
impl std::convert::TryFrom<&chrono::naive::NaiveDateTime> for LocalDatetime {
    type Error = OutOfRange;
    fn try_from(d: &chrono::naive::NaiveDateTime)
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

#[cfg(feature="chrono")]
impl std::convert::TryFrom<&chrono::naive::NaiveDate> for LocalDate {
    type Error = OutOfRange;
    fn try_from(d: &chrono::naive::NaiveDate) -> Result<LocalDate, Self::Error>
    {
        let days = chrono::Datelike::num_days_from_ce(d);
        Ok(LocalDate {
            days: days.checked_sub(730120)
                .ok_or(OutOfRange)?,
        })
    }
}


#[cfg(feature="chrono")]
impl std::convert::TryInto<chrono::naive::NaiveDate> for &LocalDate {
    type Error = OutOfRange;
    fn try_into(self) -> Result<chrono::naive::NaiveDate, Self::Error> {
        self.days.checked_add(730120)
        .and_then(chrono::naive::NaiveDate::from_num_days_from_ce_opt)
        .ok_or(OutOfRange)
    }
}

#[cfg(feature="chrono")]
impl Into<chrono::naive::NaiveTime> for &LocalTime {
    fn into(self) -> chrono::naive::NaiveTime {
        chrono::naive::NaiveTime::from_num_seconds_from_midnight(
            (self.micros / 1000_000) as u32,
            ((self.micros % 1000_000) * 1000) as u32)
    }
}

#[cfg(feature="chrono")]
impl From<&chrono::naive::NaiveTime> for LocalTime {
    fn from(time: &chrono::naive::NaiveTime) -> LocalTime {
        let sec = chrono::Timelike::num_seconds_from_midnight(time);
        let nanos = chrono::Timelike::nanosecond(time);
        LocalTime {
            micros: sec as i64 * 1000_000 + nanos as i64 / 1000,
        }
    }
}

#[cfg(feature="chrono")]
impl std::convert::TryInto<chrono::naive::NaiveDateTime> for LocalDatetime {
    type Error = OutOfRange;
    fn try_into(self) -> Result<chrono::naive::NaiveDateTime, Self::Error> {
        (&self).try_into()
    }
}

#[cfg(feature="chrono")]
impl std::convert::TryInto<chrono::naive::NaiveDate> for LocalDate {
    type Error = OutOfRange;
    fn try_into(self) -> Result<chrono::naive::NaiveDate, Self::Error> {
        (&self).try_into()
    }
}

#[cfg(feature="chrono")]
impl std::convert::TryFrom<chrono::naive::NaiveDate> for LocalDate {
    type Error = OutOfRange;
    fn try_from(d: chrono::naive::NaiveDate) -> Result<LocalDate, Self::Error>
    {
        std::convert::TryFrom::try_from(&d)
    }
}

#[cfg(feature="chrono")]
impl Into<chrono::naive::NaiveTime> for LocalTime {
    fn into(self) -> chrono::naive::NaiveTime {
        (&self).into()
    }
}

#[cfg(feature="chrono")]
impl std::convert::TryFrom<chrono::naive::NaiveDateTime> for LocalDatetime {
    type Error = OutOfRange;
    fn try_from(d: chrono::naive::NaiveDateTime)
        -> Result<LocalDatetime, Self::Error>
    {
        std::convert::TryFrom::try_from(&d)
    }
}

#[cfg(feature="chrono")]
impl From<chrono::naive::NaiveTime> for LocalTime {
    fn from(time: chrono::naive::NaiveTime) -> LocalTime {
        From::from(&time)
    }
}

#[cfg(test)]
#[allow(unused_imports)]  // because of optional tests
mod test {
    use std::str::FromStr;
    use std::convert::TryFrom;
    use super::{Decimal, BigInt};

    #[test]
    fn big_int_conversion() {
        assert_eq!(BigInt::from(125u32).weight, 0);
        assert_eq!(&BigInt::from(125u32).digits, &[125]);
        assert_eq!(BigInt::from(30000u32).weight, 1);
        assert_eq!(&BigInt::from(30000u32).digits, &[3]);
        assert_eq!(BigInt::from(30001u32).weight, 1);
        assert_eq!(&BigInt::from(30001u32).digits, &[3, 1]);

        assert_eq!(BigInt::from(125i32).weight, 0);
        assert_eq!(&BigInt::from(125i32).digits, &[125]);
        assert_eq!(BigInt::from(30000i32).weight, 1);
        assert_eq!(&BigInt::from(30000i32).digits, &[3]);
        assert_eq!(BigInt::from(30001i32).weight, 1);
        assert_eq!(&BigInt::from(30001i32).digits, &[3, 1]);

        assert_eq!(BigInt::from(-125i32).weight, 0);
        assert_eq!(&BigInt::from(-125i32).digits, &[125]);
        assert_eq!(BigInt::from(-30000i32).weight, 1);
        assert_eq!(&BigInt::from(-30000i32).digits, &[3]);
        assert_eq!(BigInt::from(-30001i32).weight, 1);
        assert_eq!(&BigInt::from(-30001i32).digits, &[3, 1]);

        assert_eq!(BigInt::from(125u64).weight, 0);
        assert_eq!(&BigInt::from(125u64).digits, &[125]);
        assert_eq!(BigInt::from(30000u64).weight, 1);
        assert_eq!(&BigInt::from(30000u64).digits, &[3]);
        assert_eq!(BigInt::from(30001u64).weight, 1);
        assert_eq!(&BigInt::from(30001u64).digits, &[3, 1]);

        assert_eq!(BigInt::from(125i64).weight, 0);
        assert_eq!(&BigInt::from(125i64).digits, &[125]);
        assert_eq!(BigInt::from(30000i64).weight, 1);
        assert_eq!(&BigInt::from(30000i64).digits, &[3]);
        assert_eq!(BigInt::from(30001i64).weight, 1);
        assert_eq!(&BigInt::from(30001i64).digits, &[3, 1]);

        assert_eq!(BigInt::from(-125i64).weight, 0);
        assert_eq!(&BigInt::from(-125i64).digits, &[125]);
        assert_eq!(BigInt::from(-30000i64).weight, 1);
        assert_eq!(&BigInt::from(-30000i64).digits, &[3]);
        assert_eq!(BigInt::from(-30001i64).weight, 1);
        assert_eq!(&BigInt::from(-30001i64).digits, &[3, 1]);
    }

    #[test]
    #[cfg(feature="bigdecimal_types")]
    fn big_big_int_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let x = BigInt::try_from(num_bigint::BigInt::from_str(
            "10000000000000000000000000000000000000")?)?;
        assert_eq!(x.weight, 9);
        assert_eq!(&x.digits, &[10]);
        Ok(())
    }

    #[test]
    #[cfg(feature="bigdecimal_types")]
    fn decimal_conversion() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal;
        let x = Decimal::try_from(BigDecimal::from_str("42.00")?)?;
        assert_eq!(x.weight, 0);
        assert_eq!(x.decimal_digits, 2);
        assert_eq!(x.digits, &[42]);
        let x = Decimal::try_from(BigDecimal::from_str("42.07")?)?;
        assert_eq!(x.weight, 0);
        assert_eq!(x.decimal_digits, 2);
        assert_eq!(x.digits, &[42, 700]);
        let x = Decimal::try_from(BigDecimal::from_str("0.07")?)?;
        assert_eq!(x.weight, -1);
        assert_eq!(x.decimal_digits, 2);
        assert_eq!(x.digits, &[700]);
        let x = Decimal::try_from(BigDecimal::from_str("420000.00")?)?;
        assert_eq!(x.weight, 1);
        assert_eq!(x.decimal_digits, 2);
        assert_eq!(x.digits, &[42]);

        let x = Decimal::try_from(BigDecimal::from_str("-42.00")?)?;
        assert_eq!(x.weight, 0);
        assert_eq!(x.decimal_digits, 2);
        assert_eq!(x.digits, &[42]);
        let x = Decimal::try_from(BigDecimal::from_str("-42.07")?)?;
        assert_eq!(x.weight, 0);
        assert_eq!(x.decimal_digits, 2);
        assert_eq!(x.digits, &[42, 700]);
        let x = Decimal::try_from(BigDecimal::from_str("-0.07")?)?;
        assert_eq!(x.weight, -1);
        assert_eq!(x.decimal_digits, 2);
        assert_eq!(x.digits, &[700]);
        let x = Decimal::try_from(BigDecimal::from_str(
            "10000000000000000000000000000000000000.00000")?)?;
        assert_eq!(x.weight, 9);
        assert_eq!(x.decimal_digits, 5);
        assert_eq!(x.digits, &[10]);
        Ok(())
    }

    #[test]
    fn big_duration_abs() {
        use super::Duration as Src;
        use std::time::Duration as Trg;
        assert_eq!(Src { micros: -1 }.abs_duration(), Trg::new(0, 1000));
        assert_eq!(Src { micros: -1000 }.abs_duration(), Trg::new(0, 1000000));
        assert_eq!(Src { micros: -1000000 }.abs_duration(), Trg::new(1, 0));
        assert_eq!(Src { micros: i64::min_value() }.abs_duration(),
                   Trg::new(9223372036854, 775808000));
    }

    #[test]
    #[cfg(feature="num-bigint")]
    fn big_int_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        use num_bigint::BigInt as N;
        use crate::value::BigInt as C;

        fn roundtrip(s: &str)-> Result<N, Box<dyn std::error::Error>> {
            Ok(N::try_from(C::try_from(N::from_str(s)?)?)?)
        }

        assert_eq!(roundtrip("1")?, N::from_str("1")?);
        assert_eq!(roundtrip("1000")?, N::from_str("1000")?);
        assert_eq!(roundtrip("0")?, N::from_str("0")?);
        assert_eq!(roundtrip("-1000")?, N::from_str("-1000")?);
        assert_eq!(roundtrip("10000000000000000000000000000000000000000000")?,
                 N::from_str("10000000000000000000000000000000000000000000")?);
        assert_eq!(roundtrip("12345678901234567890012345678901234567890123")?,
                 N::from_str("12345678901234567890012345678901234567890123")?);
        assert_eq!(roundtrip("10000000000000000000000000000000000000")?,
                 N::from_str("10000000000000000000000000000000000000")?);
        Ok(())
    }

    #[test]
    #[cfg(feature="bigdecimal")]
    fn decimal_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal as B;
        use crate::value::Decimal as D;

        fn roundtrip(s: &str)-> Result<B, Box<dyn std::error::Error>> {
            Ok(B::try_from(D::try_from(B::from_str(s)?)?)?)
        }

        assert_eq!(roundtrip("1")?, B::from_str("1")?);
        assert_eq!(roundtrip("1000")?, B::from_str("1000")?);
        assert_eq!(roundtrip("0")?, B::from_str("0")?);
        assert_eq!(roundtrip("-1000")?, B::from_str("-1000")?);
        assert_eq!(roundtrip("1.01")?, B::from_str("1.01")?);
        assert_eq!(roundtrip("1000.0070")?, B::from_str("1000.0070")?);
        assert_eq!(roundtrip("0.00008")?, B::from_str("0.00008")?);
        assert_eq!(roundtrip("-1000.1")?, B::from_str("-1000.1")?);
        assert_eq!(roundtrip("10000000000000000000000000000000000000.00001")?,
                 B::from_str("10000000000000000000000000000000000000.00001")?);
        assert_eq!(roundtrip("12345678901234567890012345678901234567890123")?,
                 B::from_str("12345678901234567890012345678901234567890123")?);
        assert_eq!(roundtrip("1234567890123456789.012345678901234567890123")?,
                 B::from_str("1234567890123456789.012345678901234567890123")?);
        assert_eq!(roundtrip("0.000000000000000000000000000000000000017238")?,
                 B::from_str("0.000000000000000000000000000000000000017238")?);
        assert_eq!(roundtrip("1234.00000")?,
                 B::from_str("1234.00000")?);
        assert_eq!(roundtrip("10000000000000000000000000000000000000.00000")?,
                 B::from_str("10000000000000000000000000000000000000.00000")?);
        Ok(())
    }

    #[test]
    #[cfg(feature="chrono")]
    fn chrono_roundtrips() -> Result<(), Box<dyn std::error::Error>> {
        use std::convert::TryInto;
        use super::{LocalDatetime, LocalDate, LocalTime};
        use chrono::naive::{NaiveDateTime, NaiveDate, NaiveTime};

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
