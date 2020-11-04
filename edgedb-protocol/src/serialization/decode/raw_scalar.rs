use std::str;
use std::mem::size_of;
use std::time::SystemTime;

use bytes::Buf;

use crate::errors::{self, DecodeError};
use crate::model::{Json, Uuid};
use snafu::{ResultExt, ensure};
use crate::model::{BigInt, Decimal};
use crate::model::{Duration, LocalDate, LocalTime, LocalDatetime, Datetime};


pub trait RawCodec<'t>: Sized {
    fn decode(buf: &'t[u8]) -> Result<Self, DecodeError>;
}

fn ensure_exact_size(buf:&[u8], expected_size: usize) -> Result<(), DecodeError> {
    if buf.len() != expected_size {
        if buf.len() < expected_size {
            return errors::Underflow.fail();
        } else {
            return errors::ExtraData.fail();
        }
    }
    Ok(())
}

impl<'t> RawCodec<'t> for String {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        <&str>::decode(buf).map(|s|s.to_owned())
    }
}

impl<'t> RawCodec<'t> for &'t str {
    fn decode(buf:&'t [u8]) -> Result<Self, DecodeError> {
        let val = str::from_utf8(buf).context(errors::InvalidUtf8)?;
        Ok(val)
    }
}

impl<'t> RawCodec<'t> for Json {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let format = buf.get_u8();
        ensure!(format == 1, errors::InvalidJsonFormat);
        let val = str::from_utf8(buf)
            .context(errors::InvalidUtf8)?
            .to_owned();
        Ok(Json::new_unchecked(val))
    }
}

impl<'t> RawCodec<'t> for Uuid {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, 16)?;
        let uuid = Uuid::from_slice(buf).unwrap();
        Ok(uuid)
    }
}

impl<'t> RawCodec<'t> for bool {
    fn decode(buf:&[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, 1)?;
        let res = match buf[0] {
            0x00 => false,
            0x01 => true,
            _ => errors::InvalidBool.fail()?,
        };
        Ok(res)
    }
}

impl<'t> RawCodec<'t> for i16 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_i16());
    }
}

impl<'t> RawCodec<'t> for i32 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_i32());
    }
}

impl<'t> RawCodec<'t> for i64 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_i64());
    }
}

impl<'t> RawCodec<'t> for f32 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_f32());
    }
}

impl<'t> RawCodec<'t> for f64 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_f64());
    }
}

impl<'t> RawCodec<'t> for &'t [u8] {
    fn decode(buf: &'t [u8]) -> Result<Self, DecodeError> {
        Ok(buf)
    }
}

impl<'t> RawCodec<'t> for Vec<u8> {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        Ok(buf.to_owned())
    }
}

impl<'t> RawCodec<'t> for Decimal {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let ndigits = buf.get_u16() as usize;
        let weight = buf.get_i16();
        let negative = match buf.get_u16() {
            0x0000 => false,
            0x4000 => true,
            _ => errors::BadSign.fail()?,
        };
        let decimal_digits = buf.get_u16();
        ensure_exact_size(buf, ndigits*2)?;
        let mut digits = Vec::with_capacity(ndigits);
        for _ in 0..ndigits {
            digits.push(buf.get_u16());
        }
        Ok(Decimal {
            negative, weight, decimal_digits, digits,
        })
    }
}

impl<'t> RawCodec<'t> for BigInt {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let ndigits = buf.get_u16() as usize;
        let weight = buf.get_i16();
        let negative = match buf.get_u16() {
            0x0000 => false,
            0x4000 => true,
            _ => errors::BadSign.fail()?,
        };
        let decimal_digits = buf.get_u16();
        ensure!(decimal_digits == 0, errors::NonZeroReservedBytes);
        let mut digits = Vec::with_capacity(ndigits);
        ensure_exact_size(buf, ndigits*2)?;
        for _ in 0..ndigits {
            digits.push(buf.get_u16());
        }
        Ok(BigInt {
            negative, weight, digits,
        })
    }
}

impl<'t> RawCodec<'t> for Duration {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, 16)?;
        let micros = buf.get_i64();
        let days = buf.get_u32();
        let months = buf.get_u32();
        ensure!(months == 0 && days == 0, errors::NonZeroReservedBytes);
        Ok(Duration { micros })
    }
}

impl<'t> RawCodec<'t> for SystemTime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let micros = i64::decode(buf)?;

        use std::time::{ Duration, UNIX_EPOCH };
        let postgres_epoch :SystemTime = UNIX_EPOCH + Duration::from_secs(946684800);

        let val = if micros > 0 {
            postgres_epoch + Duration::from_micros(micros as u64)
        } else {
            postgres_epoch - Duration::from_micros((-micros) as u64)
        };
        Ok(val)
    }
}

impl<'t> RawCodec<'t> for Datetime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let micros = i64::decode(buf)?;
        Ok(Datetime::try_from_micros(micros)
            .map_err(|_| errors::InvalidDate.build())?)
    }
}

impl<'t> RawCodec<'t> for LocalDatetime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let micros = i64::decode(buf)?;
        Ok(LocalDatetime { micros })
    }
}

impl<'t> RawCodec<'t> for LocalDate {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let days = i32::decode(buf)?;
        Ok(LocalDate { days })
    }
}

impl<'t> RawCodec<'t> for LocalTime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let micros = i64::decode(buf)?;
        ensure!(micros >= 0 && micros < 86_400 * 1_000_000, errors::InvalidDate);
        Ok(LocalTime { micros: micros as u64 })
    }
}
