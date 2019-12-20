use std::time::{SystemTime, Duration as StdDuration};
use std::{u32, u64};

use uuid::Uuid;

use crate::codec::{NamedTupleShape, ObjectShape, EnumValue};

#[derive(Clone, Debug, PartialEq)]
pub struct Duration {
    pub positive: bool,
    pub amount: StdDuration,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Set(Vec<Value>),
    Object { shape: ObjectShape, fields: Vec<Value> },
    Scalar(Scalar),
    Tuple(Vec<Value>),
    NamedTuple { shape: NamedTupleShape, fields: Vec<Value> },
    Array(Vec<Value>),
    Enum(EnumValue),
    Nothing,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
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
    DateTime(SystemTime),
    LocalDatetime(LocalDatetime),
    LocalDate(LocalDate),
    LocalTime(LocalTime),
    Duration(Duration),
    Json(String),  // or should we use serde::Json?
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
    // TODO(tailhook)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDate {
    // TODO(tailhook)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalTime {
    // TODO(tailhook)
}

impl Value {
    pub fn kind(&self) -> &'static str {
        use Value::*;
        match self {
            Set(..) => "set",
            Object { .. } => "object",
            Scalar(s) => s.kind(),
            Tuple(..) => "tuple",
            NamedTuple { .. } => "named_tuple",
            Array(..) => "array",
            Enum(..) => "enum",
            Nothing => "nothing",
        }
    }
}
impl Scalar {
    pub fn kind(&self) -> &'static str {
        use Scalar::*;
        match self {
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
            DateTime(..) => "datetime",
            LocalDatetime(..) => "cal::local_datetime",
            LocalDate(..) => "cal::local_date",
            LocalTime(..) => "cal::local_time",
            Duration(..) => "duration",
            Json(..) => "json",
        }
    }
}

impl Duration {
    pub fn from_secs(val: i64) -> Duration {
        Duration {
            positive: val >= 0,
            amount: StdDuration::from_secs(val.abs() as u64),
        }
    }
    pub fn from_micros(val: i64) -> Duration {
        Duration {
            positive: val >= 0,
            amount: StdDuration::from_micros(val.abs() as u64),
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
    type Error = std::num::TryFromIntError;
    fn try_from(v: num_bigint::BigInt) -> Result<BigInt, Self::Error> {
        use num_traits::{ToPrimitive, Zero};
        use std::convert::TryInto;

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
    type Error = std::num::TryFromIntError;
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
impl Into<bigdecimal::BigDecimal> for &Decimal {
    fn into(self) -> bigdecimal::BigDecimal {
        todo!("bigdecimal don't work now");
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
        Ok(())
    }
}
