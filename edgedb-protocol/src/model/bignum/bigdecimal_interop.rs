use super::Decimal;
use crate::model::OutOfRangeError;

impl std::convert::TryFrom<bigdecimal::BigDecimal> for Decimal {
    type Error = OutOfRangeError;
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

impl Into<bigdecimal::BigDecimal> for Decimal {
    fn into(self) -> bigdecimal::BigDecimal {
        (&self).into()
    }
}

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

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::convert::TryFrom;
    use super::Decimal;
    use super::super::test_helpers::{gen_i64, gen_u64};
    use bigdecimal::BigDecimal;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    #[test]
    fn decimal_conversion() -> Result<(), Box<dyn std::error::Error>> {
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
            let nulls = rng.gen_range(0..100);
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
            let nulls = rng.gen_range(-100..100);
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
            let nulls = rng.gen_range(-100..100);
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
            let nulls1 = rng.gen_range(0..100);
            let nulls2 = rng.gen_range(0..100);
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
            let nulls1 = rng.gen_range(0..100);
            let nulls2 = rng.gen_range(0..100);
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
}
