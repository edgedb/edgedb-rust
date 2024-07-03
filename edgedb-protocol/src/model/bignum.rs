use std::fmt::Write;

#[cfg(feature = "num-bigint")]
mod num_bigint_interop;

#[cfg(feature = "bigdecimal")]
mod bigdecimal_interop;

/// Virtually unlimited precision integer.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BigInt {
    pub(crate) negative: bool,
    pub(crate) weight: i16,
    pub(crate) digits: Vec<u16>,
}

/// High-precision decimal number.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
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
        self
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
        BigInt {
            negative: false,
            weight: 4,
            digits: vec![
                (v / 10_000_000_000_000_000 % 10000) as u16,
                (v / 1_000_000_000_000 % 10000) as u16,
                (v / 100_000_000 % 10000) as u16,
                (v / 10000 % 10000) as u16,
                (v % 10000) as u16,
            ],
        }
        .normalize()
    }
}

impl From<i64> for BigInt {
    fn from(v: i64) -> BigInt {
        let (abs, negative) = if v < 0 {
            (u64::MAX - v as u64 + 1, true)
        } else {
            (v as u64, false)
        };
        BigInt {
            negative,
            weight: 4,
            digits: vec![
                (abs / 10_000_000_000_000_000 % 10000) as u16,
                (abs / 1_000_000_000_000 % 10000) as u16,
                (abs / 100_000_000 % 10000) as u16,
                (abs / 10000 % 10000) as u16,
                (abs % 10000) as u16,
            ],
        }
        .normalize()
    }
}

impl From<u32> for BigInt {
    fn from(v: u32) -> BigInt {
        BigInt {
            negative: false,
            weight: 2,
            digits: vec![
                (v / 100_000_000) as u16,
                (v / 10000 % 10000) as u16,
                (v % 10000) as u16,
            ],
        }
        .normalize()
    }
}

impl From<i32> for BigInt {
    fn from(v: i32) -> BigInt {
        let (abs, negative) = if v < 0 {
            (u32::MAX - v as u32 + 1, true)
        } else {
            (v as u32, false)
        };
        BigInt {
            negative,
            weight: 2,
            digits: vec![
                (abs / 100_000_000) as u16,
                (abs / 10000 % 10000) as u16,
                (abs % 10000) as u16,
            ],
        }
        .normalize()
    }
}

impl Decimal {
    #[allow(dead_code)] // isn't used when BigDecimal is disabled
    fn normalize(mut self) -> Decimal {
        while let Some(0) = self.digits.last() {
            self.digits.pop();
        }
        while let Some(0) = self.digits.first() {
            self.digits.remove(0);
            self.weight -= 1;
        }
        self
    }
}

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.negative {
            write!(f, "-")?;
        }

        let mut index = 0;

        // integer part
        while self.weight - index >= 0 {
            if let Some(digit) = self.digits.get(index as usize) {
                if index == 0 {
                    write!(f, "{}", digit)?;
                } else {
                    write!(f, "{:04}", digit)?;
                }
                index += 1;
            } else {
                break;
            }
        }
        if index == 0 {
            write!(f, "0")?;
        }

        // dot
        write!(f, ".")?;

        // decimal part
        let mut decimals = self.decimal_digits;
        while decimals > 0 {
            if let Some(digit) = self.digits.get(index as usize) {
                let digit = format!("{digit:04}");
                let consumed = u16::min(4, decimals);
                f.write_str(&digit[0..consumed as usize])?;
                decimals -= consumed;
                index += 1;
            } else {
                break;
            }
        }
        // trailing zeros
        for _ in 0..decimals {
            f.write_char('0')?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(dead_code)] // used by optional tests
mod test_helpers {
    use rand::Rng;

    pub fn gen_u64<T: Rng>(rng: &mut T) -> u64 {
        // change distribution to generate different length more frequently
        let max = 10_u64.pow(rng.gen_range(0..20));
        rng.gen_range(0..max)
    }

    pub fn gen_i64<T: Rng>(rng: &mut T) -> i64 {
        // change distribution to generate different length more frequently
        let max = 10_i64.pow(rng.gen_range(0..19));
        rng.gen_range(-max..max)
    }

    pub fn gen_f64<T: Rng>(rng: &mut T) -> f64 {
        rng.gen::<f64>()
    }
}

#[cfg(test)]
#[allow(unused_imports)] // because of optional tests
mod test {
    use super::{BigInt, Decimal};
    use std::convert::TryFrom;
    use std::str::FromStr;

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
        assert_eq!(BigInt::from(u64::MAX).digits, &[1844, 6744, 737, 955, 1615]);

        assert_eq!(BigInt::from(125i64).weight, 0);
        assert_eq!(&BigInt::from(125i64).digits, &[125]);
        assert_eq!(BigInt::from(30000i64).weight, 1);
        assert_eq!(&BigInt::from(30000i64).digits, &[3]);
        assert_eq!(BigInt::from(30001i64).weight, 1);
        assert_eq!(&BigInt::from(30001i64).digits, &[3, 1]);
        assert_eq!(BigInt::from(i64::MAX).weight, 4);
        assert_eq!(BigInt::from(i64::MAX).digits, &[922, 3372, 368, 5477, 5807]);

        assert_eq!(BigInt::from(-125i64).weight, 0);
        assert_eq!(&BigInt::from(-125i64).digits, &[125]);
        assert_eq!(BigInt::from(-30000i64).weight, 1);
        assert_eq!(&BigInt::from(-30000i64).digits, &[3]);
        assert_eq!(BigInt::from(-30001i64).weight, 1);
        assert_eq!(&BigInt::from(-30001i64).digits, &[3, 1]);
        assert_eq!(BigInt::from(i64::MIN).weight, 4);
        assert_eq!(BigInt::from(i64::MIN).digits, &[922, 3372, 368, 5477, 5808]);
    }

    #[test]
    fn bigint_display() {
        let cases = [0, 1, -1, 1_0000, -1_0000, 1_2345_6789, i64::MAX, i64::MIN];
        for i in cases.iter() {
            assert_eq!(BigInt::from(*i).to_string(), i.to_string());
        }
    }

    #[test]
    fn bigint_display_rand() {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(4);
        for _ in 0..1000 {
            let i = super::test_helpers::gen_i64(&mut rng);
            assert_eq!(BigInt::from(i).to_string(), i.to_string());
        }
    }
}
