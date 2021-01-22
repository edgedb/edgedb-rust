use super::BigInt;
use crate::model::OutOfRangeError;

impl std::convert::TryFrom<num_bigint::BigInt> for BigInt {
    type Error = OutOfRangeError;
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

impl Into<num_bigint::BigInt> for BigInt {
    fn into(self) -> num_bigint::BigInt {
        (&self).into()
    }
}

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
mod test {
    use super::BigInt;
    use std::convert::TryFrom;
    use std::str::FromStr;

    #[test]
    fn big_big_int_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let x = BigInt::try_from(num_bigint::BigInt::from_str(
            "10000000000000000000000000000000000000")?)?;
        assert_eq!(x.weight, 9);
        assert_eq!(&x.digits, &[10]);
        Ok(())
    }
}

// conceptually these tests work on BigInt, but depend on the bigdecimal feature
#[cfg(all(test, feature = "bigdecimal"))]
mod test_with_decimal {
    use super::super::test_helpers::gen_i64;
    use super::BigInt;
    use bigdecimal::BigDecimal;
    use num_bigint::ToBigInt;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use std::convert::TryFrom;
    use std::str::FromStr;

    #[test]
    fn bigint_conversion() -> Result<(), Box<dyn std::error::Error>> {
        let x = BigInt::try_from(BigDecimal::from_str("1e20")?
            .to_bigint().unwrap())?;
        assert_eq!(x.weight, 5);
        assert_eq!(x.digits, &[1]);
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
            let nulls = rng.gen_range(0..100);
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
            let nulls = rng.gen_range(0..100);
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
            let nulls1 = rng.gen_range(0..100);
            let nulls2 = rng.gen_range(0..100);
            let edb = format!("{0}{1:0<2$}e{3}", head, "", nulls1, nulls2);
            let bigint = format!("{}{1:0<2$}", head, "", nulls1+nulls2);
            assert_eq!(int_roundtrip(&edb), B::from_str(&bigint)?,
                       "parsing {}: {}", iter, edb);
        }
        Ok(())
    }
}
