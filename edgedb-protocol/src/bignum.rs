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

    fn trailing_zero_groups(&self) -> i16 {
        self.weight - self.digits.len() as i16 + 1
    }
}

impl std::fmt::Display for BigInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.negative {
            write!(f, "-")?;
        }
        if let Some(digit) = self.digits.first() {
            write!(f, "{}", digit)?;
            for digit in &mut self.digits.iter().skip(1) {
                write!(f, "{:04}", digit)?;
            }
            let trailing_zero_groups = self.trailing_zero_groups();
            debug_assert!(trailing_zero_groups >= 0);
            for _ in 0..trailing_zero_groups {
                write!(f, "0000")?;
            }
        } else {
            write!(f, "0")?;
        }
        Ok(())
    }
}

impl From<u64> for BigInt {
    fn from(v: u64) -> BigInt {
        return BigInt {
            negative: false,
            weight: 4,
            digits: vec![
                (v / 10000_0000_0000_0000 % 10000) as u16,
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
            weight: 4,
            digits: vec![
                (abs / 10000_0000_0000_0000 % 10000) as u16,
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
            weight: 2,
            digits: vec![
                (v / 10000_0000) as u16,
                (v / 10000 % 10000) as u16,
                (v % 10000) as u16,
            ],
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
            weight: 2,
            digits: vec![
                (abs / 10000_0000) as u16,
                (abs / 10000 % 10000) as u16,
                (abs % 10000) as u16,
            ],
        }.normalize();
    }
}

#[cfg(feature="num-bigint")]
impl std::convert::TryFrom<num_bigint::BigInt> for BigInt {
    type Error = crate::value::OutOfRange;
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
    type Error = crate::value::OutOfRange;
    fn try_from(dec: bigdecimal::BigDecimal) -> Result<Decimal, Self::Error> {
        use num_traits::{ToPrimitive, Zero};
        use std::convert::TryInto;
        use std::cmp::max;

        let mut digits = Vec::new();
        let (v, scale) = dec.into_bigint_and_exponent();
        let (negative, mut val) = match v.sign() {
            num_bigint::Sign::Minus => (true, -v),
            num_bigint::Sign::NoSign => (false, v),
            num_bigint::Sign::Plus => (false, v),
        };
        let scale_4digits = if scale < 0 {
            scale/4
        } else {
            scale/4 + 1
        };
        let pad = scale_4digits*4 - scale;

        if pad > 0 {
            val *= 10u16.pow(pad as u32);
        }
        while !val.is_zero() {
            digits.push((&val % 10000u16).to_u16().unwrap());
            val /= 10000;
        }
        digits.reverse();

        // These return "out of range integral type conversion attempted"
        // which should be good enough for this error
        let decimal_digits = max(0, scale).try_into()?;
        let weight = i16::try_from(digits.len() as i64 - scale_4digits - 1)?;

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
        // TODO(tailhook) this is quite slow, use preallocated vector
        for &digit in &self.digits {
            r *= 10000;
            r += digit;
        }
        let decimal_stored = 4*max(0,
            self.digits.len() as i64 - self.weight as i64 - 1
        ) as usize;
        let pad = if decimal_stored > 0 {
            let pad = decimal_stored as i64 - self.decimal_digits as i64;
            if pad > 0 {
                r /= pow(10, pad as usize);
            } else if pad < 0 {
                r *= pow(10, (-pad) as usize);
            }
            pad
        } else {
            0
        };

        let scale = if self.decimal_digits == 0 {
            -(self.weight as i64 + 1 - self.digits.len() as i64)*4 - pad as i64
        } else {
            if decimal_stored == 0 {
                let power = (self.weight as usize + 1 - self.digits.len())*4
                    + self.decimal_digits as usize;
                if power > 0 {
                    r *= pow(BigInt::from(10), power);
                }
            }
            self.decimal_digits as i64
        };
        if self.negative {
            r = -r;
        }
        return BigDecimal::new(r, scale)
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

#[cfg(test)]
#[allow(dead_code)] // used by optional tests
pub(self) mod test_helpers{
    use rand::Rng;

    pub fn gen_u64<T: Rng>(rng: &mut T) -> u64 {
        // change distribution to generate different length more frequently
        let max = 10_u64.pow(rng.gen_range(0, 20));
        return rng.gen_range(0, max);
    }

    pub fn gen_i64<T: Rng>(rng: &mut T) -> i64 {
        // change distribution to generate different length more frequently
        let max = 10_i64.pow(rng.gen_range(0, 19));
        return rng.gen_range(-max, max);
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
        assert_eq!(BigInt::from(u32::MAX).weight, 2);
        assert_eq!(BigInt::from(u32::MAX).digits, &[42, 9496, 7295]);

        assert_eq!(BigInt::from(125i32).weight, 0);
        assert_eq!(&BigInt::from(125i32).digits, &[125]);
        assert_eq!(BigInt::from(30000i32).weight, 1);
        assert_eq!(&BigInt::from(30000i32).digits, &[3]);
        assert_eq!(BigInt::from(30001i32).weight, 1);
        assert_eq!(&BigInt::from(30001i32).digits, &[3, 1]);
        assert_eq!(BigInt::from(i32::MAX).weight, 2);
        assert_eq!(BigInt::from(i32::MAX).digits, &[21, 4748, 3647]);

        assert_eq!(BigInt::from(-125i32).weight, 0);
        assert_eq!(&BigInt::from(-125i32).digits, &[125]);
        assert_eq!(BigInt::from(-30000i32).weight, 1);
        assert_eq!(&BigInt::from(-30000i32).digits, &[3]);
        assert_eq!(BigInt::from(-30001i32).weight, 1);
        assert_eq!(&BigInt::from(-30001i32).digits, &[3, 1]);
        assert_eq!(BigInt::from(i32::MIN).weight, 2);
        assert_eq!(BigInt::from(i32::MIN).digits, &[21, 4748, 3648]);

        assert_eq!(BigInt::from(125u64).weight, 0);
        assert_eq!(&BigInt::from(125u64).digits, &[125]);
        assert_eq!(BigInt::from(30000u64).weight, 1);
        assert_eq!(&BigInt::from(30000u64).digits, &[3]);
        assert_eq!(BigInt::from(30001u64).weight, 1);
        assert_eq!(&BigInt::from(30001u64).digits, &[3, 1]);
        assert_eq!(BigInt::from(u64::MAX).weight, 4);
        assert_eq!(
            BigInt::from(u64::MAX).digits,
            &[1844, 6744, 0737, 0955, 1615]
        );

        assert_eq!(BigInt::from(125i64).weight, 0);
        assert_eq!(&BigInt::from(125i64).digits, &[125]);
        assert_eq!(BigInt::from(30000i64).weight, 1);
        assert_eq!(&BigInt::from(30000i64).digits, &[3]);
        assert_eq!(BigInt::from(30001i64).weight, 1);
        assert_eq!(&BigInt::from(30001i64).digits, &[3, 1]);
        assert_eq!(BigInt::from(i64::MAX).weight, 4);
        assert_eq!(
            BigInt::from(i64::MAX).digits,
            &[922, 3372, 0368, 5477, 5807]
        );

        assert_eq!(BigInt::from(-125i64).weight, 0);
        assert_eq!(&BigInt::from(-125i64).digits, &[125]);
        assert_eq!(BigInt::from(-30000i64).weight, 1);
        assert_eq!(&BigInt::from(-30000i64).digits, &[3]);
        assert_eq!(BigInt::from(-30001i64).weight, 1);
        assert_eq!(&BigInt::from(-30001i64).digits, &[3, 1]);
        assert_eq!(BigInt::from(i64::MIN).weight, 4);
        assert_eq!(
            BigInt::from(i64::MIN).digits,
            &[922, 3372, 0368, 5477, 5808]
        );
    }

    #[test]
    fn display() {
        let cases = [
            0,
            1,
            -1,
            1_0000,
            -1_0000,
            1_2345_6789,
            i64::MAX,
            i64::MIN,
        ];
        for i in cases.iter() {
            assert_eq!(BigInt::from(*i).to_string(), i.to_string());
        }
    }

    #[test]
    fn display_rand() {
        use rand::{Rng, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(4);
        for _ in 0..1000 {
            let i = super::test_helpers::gen_i64(&mut rng);
            assert_eq!(BigInt::from(i).to_string(), i.to_string());
        }
    }
}

#[cfg(all(test, feature="num-bigint", feature="bigdecimal"))]
mod decimal {
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::str::FromStr;
    use std::convert::TryFrom;
    use bigdecimal::BigDecimal;
    use num_bigint::ToBigInt;
    use super::{Decimal, BigInt};
    use super::test_helpers::{gen_u64, gen_i64};

    #[test]
    fn big_big_int_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let x = BigInt::try_from(num_bigint::BigInt::from_str(
            "10000000000000000000000000000000000000")?)?;
        assert_eq!(x.weight, 9);
        assert_eq!(&x.digits, &[10]);
        Ok(())
    }

    #[test]
    fn bigint_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let x = BigInt::try_from(BigDecimal::from_str("1e20")?
            .to_bigint().unwrap())?;
        assert_eq!(x.weight, 5);
        assert_eq!(x.digits, &[1]);
        Ok(())
    }

    #[test]
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
        assert_eq!(x.digits, &[10]);
        assert_eq!(x.weight, 9);
        assert_eq!(x.decimal_digits, 5);
        let x = Decimal::try_from(BigDecimal::from_str("1e100")?)?;
        assert_eq!(x.weight, 25);
        assert_eq!(x.decimal_digits, 0);
        assert_eq!(x.digits, &[1]);
        let x = Decimal::try_from(BigDecimal::from_str(
            "-703367234220692490200000000000000000000000000")?)?;
        assert_eq!(x.weight, 11);
        assert_eq!(x.decimal_digits, 0);
        assert_eq!(x.digits, &[7, 336, 7234, 2206, 9249, 200]);
        let x = Decimal::try_from(BigDecimal::from_str(
            "-7033672342206924902e26")?)?;
        assert_eq!(x.weight, 11);
        assert_eq!(x.decimal_digits, 0);
        assert_eq!(x.digits, &[7, 336, 7234, 2206, 9249, 200]);

        let x = Decimal::try_from(BigDecimal::from_str(
            "6545218855030988517.14400196897187081925e47")?)?;
        assert_eq!(x.weight, 16);
        assert_eq!(x.decimal_digits, 0);
        assert_eq!(x.digits, &[
            65,
            4521,
            8855,
            309,
            8851,
            7144,
            19,
            6897,
            1870,
            8192,
            5000]);
        let x = Decimal::try_from(BigDecimal::from_str(
            "-260399300000000000000000000000000000000000000.\
                000000000007745502260")?)?;
        assert_eq!(x.weight, 11);
        assert_eq!(x.decimal_digits, 21);
        assert_eq!(x.digits, &[
            2,
            6039,
            9300,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0, // decimal digits start here
            0,
            7,
            7455,
            226,
        ]);

        Ok(())
    }

    fn int_roundtrip(s: &str) -> num_bigint::BigInt {
        let decimal = BigDecimal::from_str(s).expect("can parse decimal");
        let rust = decimal.to_bigint().expect("can convert to big int");
        let edgedb = BigInt::try_from(rust).expect("can convert for edgedb");
        num_bigint::BigInt::try_from(edgedb)
            .expect("can convert back to big int")
    }

    #[test]
    fn big_int_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        use num_bigint::BigInt as N;


        assert_eq!(int_roundtrip("1"), N::from_str("1")?);
        assert_eq!(int_roundtrip("1000"), N::from_str("1000")?);
        assert_eq!(int_roundtrip("1e20"),
                   N::from_str("100000000000000000000")?);
        assert_eq!(int_roundtrip("0"), N::from_str("0")?);
        assert_eq!(int_roundtrip("-1000"), N::from_str("-1000")?);
        assert_eq!(
            int_roundtrip("10000000000000000000000000000000000000000000"),
            N::from_str("10000000000000000000000000000000000000000000")?);
        assert_eq!(
            int_roundtrip("12345678901234567890012345678901234567890123"),
            N::from_str("12345678901234567890012345678901234567890123")?);
        assert_eq!(int_roundtrip("10000000000000000000000000000000000000"),
                 N::from_str("10000000000000000000000000000000000000")?);
        Ok(())
    }

    fn dec_roundtrip(s: &str) -> BigDecimal {
        let rust = BigDecimal::from_str(s).expect("can parse big decimal");
        let edgedb = Decimal::try_from(rust).expect("can convert for edgedb");
        BigDecimal::try_from(edgedb).expect("can convert back to big decimal")
    }

    #[test]
    fn decimal_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal as B;

        assert_eq!(dec_roundtrip("1"), B::from_str("1")?);
        assert_eq!(dec_roundtrip("1000"), B::from_str("1000")?);
        assert_eq!(dec_roundtrip("1e100"), B::from_str("1e100")?);
        assert_eq!(dec_roundtrip("0"), B::from_str("0")?);
        assert_eq!(dec_roundtrip("-1000"), B::from_str("-1000")?);
        assert_eq!(dec_roundtrip("1.01"), B::from_str("1.01")?);
        assert_eq!(dec_roundtrip("1000.0070"), B::from_str("1000.0070")?);
        assert_eq!(dec_roundtrip("0.00008"), B::from_str("0.00008")?);
        assert_eq!(dec_roundtrip("-1000.1"), B::from_str("-1000.1")?);
        assert_eq!(
            dec_roundtrip("10000000000000000000000000000000000000.00001"),
            B::from_str("10000000000000000000000000000000000000.00001")?);
        assert_eq!(
            dec_roundtrip("12345678901234567890012345678901234567890123"),
            B::from_str("12345678901234567890012345678901234567890123")?);
        assert_eq!(
            dec_roundtrip("1234567890123456789.012345678901234567890123"),
            B::from_str("1234567890123456789.012345678901234567890123")?);
        assert_eq!(
            dec_roundtrip("0.000000000000000000000000000000000000017238"),
            B::from_str("0.000000000000000000000000000000000000017238")?);
        assert_eq!(dec_roundtrip("1234.00000"), B::from_str("1234.00000")?);
        assert_eq!(
            dec_roundtrip("10000000000000000000000000000000000000.00000"),
            B::from_str("10000000000000000000000000000000000000.00000")?);
        assert_eq!(
            dec_roundtrip("100010001000000000000000000000000000"),
            B::from_str("100010001000000000000000000000000000")?);

        Ok(())
    }


    #[test]
    fn decimal_rand_i64() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal as B;

        let mut rng = StdRng::seed_from_u64(1);
        for _ in 0..10000 {
            let head = gen_u64(&mut rng);
            let txt = format!("{}", head);
            assert_eq!(dec_roundtrip(&txt), B::from_str(&txt)?,
                       "parsing: {}", txt);
        }
        Ok(())
    }

    #[test]
    fn decimal_rand_nulls() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal as B;

        let mut rng = StdRng::seed_from_u64(2);
        for iter in 0..10000 {
            let head = gen_u64(&mut rng);
            let nulls = rng.gen_range(0, 100);
            let txt = format!("{0}{1:0<2$}", head, "", nulls);
            assert_eq!(dec_roundtrip(&txt), B::from_str(&txt)?,
                       "parsing {}: {}", iter, txt);
        }
        Ok(())
    }

    #[test]
    fn decimal_rand_eplus() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal as B;

        let mut rng = StdRng::seed_from_u64(3);
        for iter in 0..10000 {
            let head = gen_u64(&mut rng);
            let nulls = rng.gen_range(-100, 100);
            let txt = format!("{}e{}", head, nulls);
            assert_eq!(dec_roundtrip(&txt), B::from_str(&txt)?,
                       "parsing {}: {}", iter, txt);
        }
        Ok(())
    }

    #[test]
    fn decimal_rand_fract_eplus() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal as B;

        let mut rng = StdRng::seed_from_u64(4);
        for iter in 0..10000 {
            let head = gen_i64(&mut rng);
            let fract = gen_u64(&mut rng);
            let nulls = rng.gen_range(-100, 100);
            let txt = format!("{}.{}e{}", head, fract, nulls);
            let rt = dec_roundtrip(&txt);
            let dec = B::from_str(&txt)?;
            assert_eq!(rt, dec,
                       "parsing {}: {}", iter, txt);
            if dec.as_bigint_and_exponent().1 > 0 {
                // check precision
                // (if scale is negative it's integer, we don't have precision)
                assert_eq!(rt.as_bigint_and_exponent().1,
                           dec.as_bigint_and_exponent().1,
                           "precision: {}", txt);
            }
        }
        Ok(())
    }

    #[test]
    fn decimal_rand_nulls_eplus() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal as B;

        let mut rng = StdRng::seed_from_u64(5);
        for iter in 0..10000 {
            let head = gen_i64(&mut rng);
            let nulls1 = rng.gen_range(0, 100);
            let nulls2 = rng.gen_range(0, 100);
            let txt = format!("{0}{1:0<2$}e{3}", head, "", nulls1, nulls2);
            let rt = dec_roundtrip(&txt);
            let dec = B::from_str(&txt)?;
            assert_eq!(rt, dec,
                       "parsing {}: {}", iter, txt);
            if dec.as_bigint_and_exponent().1 > 0 {
                // check precision
                // (if scale is negative it's integer, we don't have precision)
                assert_eq!(rt.as_bigint_and_exponent().1,
                           dec.as_bigint_and_exponent().1,
                           "precision: {}", txt);
            }
        }
        Ok(())
    }

    #[test]
    fn decimal_rand_decim() -> Result<(), Box<dyn std::error::Error>> {
        use bigdecimal::BigDecimal as B;

        let mut rng = StdRng::seed_from_u64(6);
        for iter in 0..10000 {
            let head = gen_i64(&mut rng);
            let nulls1 = rng.gen_range(0, 100);
            let nulls2 = rng.gen_range(0, 100);
            let decimals = gen_u64(&mut rng);
            let txt = format!("{0}{1:0<2$}.{1:0<3$}{4}", head,
                "", nulls1, nulls2, decimals);
            assert_eq!(dec_roundtrip(&txt), B::from_str(&txt)?,
                       "parsing {}: {}", iter, txt);
            assert_eq!(dec_roundtrip(&txt).as_bigint_and_exponent().1,
                       B::from_str(&txt)?.as_bigint_and_exponent().1,
                       "precision: {}", txt);
        }
        Ok(())
    }

    #[test]
    fn int_rand_i64() -> Result<(), Box<dyn std::error::Error>> {
        use num_bigint::BigInt as B;

        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..10000 {
            let head = gen_i64(&mut rng);
            let txt = format!("{}", head);
            assert_eq!(int_roundtrip(&txt), B::from_str(&txt)?,
                       "parsing: {}", txt);
        }
        Ok(())
    }

    #[test]
    fn int_rand_nulls() -> Result<(), Box<dyn std::error::Error>> {
        use num_bigint::BigInt as B;

        let mut rng = StdRng::seed_from_u64(8);
        for iter in 0..10000 {
            let head = gen_i64(&mut rng);
            let nulls = rng.gen_range(0, 100);
            let txt = format!("{0}{1:0<2$}", head, "", nulls);
            assert_eq!(int_roundtrip(&txt), B::from_str(&txt)?,
                       "parsing {}: {}", iter, txt);
        }
        Ok(())
    }

    #[test]
    fn int_rand_eplus() -> Result<(), Box<dyn std::error::Error>> {
        use num_bigint::BigInt as B;

        let mut rng = StdRng::seed_from_u64(9);
        for iter in 0..10000 {
            let head = gen_i64(&mut rng);
            let nulls = rng.gen_range(0, 100);
            let edb = format!("{}e{}", head, nulls);
            let bigint = format!("{}{1:0<2$}", head, "", nulls);
            assert_eq!(int_roundtrip(&edb), B::from_str(&bigint)?,
                       "parsing {}: {}", iter, edb);
        }
        Ok(())
    }

    #[test]
    fn int_rand_nulls_eplus() -> Result<(), Box<dyn std::error::Error>> {
        use num_bigint::BigInt as B;

        let mut rng = StdRng::seed_from_u64(10);
        for iter in 0..10000 {
            let head = gen_i64(&mut rng);
            let nulls1 = rng.gen_range(0, 100);
            let nulls2 = rng.gen_range(0, 100);
            let edb = format!("{0}{1:0<2$}e{3}", head, "", nulls1, nulls2);
            let bigint = format!("{}{1:0<2$}", head, "", nulls1+nulls2);
            assert_eq!(int_roundtrip(&edb), B::from_str(&bigint)?,
                       "parsing {}: {}", iter, edb);
        }
        Ok(())
    }
}