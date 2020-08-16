use std::str;
use std::mem::size_of;
use std::time::SystemTime;

use bytes::Buf;
use uuid::Uuid;

use crate::errors::{self, DecodeError};
use crate::json::Json;
use snafu::{ResultExt, ensure};
use crate::value::{Duration, LocalDate, LocalTime, LocalDatetime, BigInt, Decimal};


pub trait RawCodec<'t>: Sized {
    fn decode_raw(buf: &mut &'t[u8]) -> Result<Self, DecodeError>;
}

impl<'t> RawCodec<'t> for String {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        <&str>::decode_raw(buf).map(|s|s.to_owned())
    }
}

impl<'t> RawCodec<'t> for &'t str {
    fn decode_raw(buf: &mut &'t [u8]) -> Result<Self, DecodeError> {
        let val = str::from_utf8(*buf)
            .context(errors::InvalidUtf8)?;
        buf.advance(buf.bytes().len());
        Ok(val)
    }
}

impl<'t> RawCodec<'t> for Json {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let format = buf.get_u8();
        ensure!(format == 1, errors::InvalidJsonFormat);
        let val = str::from_utf8(&buf.bytes())
            .context(errors::InvalidUtf8)?
            .to_owned();
        buf.advance(val.len());
        Ok(Json(val))
    }
}

impl<'t> RawCodec<'t> for Uuid {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 16, errors::Underflow);
        let uuid = Uuid::from_slice(buf.bytes())
            .context(errors::InvalidUuid)?;
        buf.advance(16);
        Ok(uuid)
    }
}

impl<'t> RawCodec<'t> for bool {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let res = match buf.get_u8() {
            0x00 => false,
            0x01 => true,
            _ => errors::InvalidBool.fail()?,
        };
        Ok(res)
    }
}

impl<'t> RawCodec<'t> for i16 {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= size_of::<Self>(), errors::Underflow);
        return Ok(buf.get_i16());
    }
}

impl<'t> RawCodec<'t> for i32 {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= size_of::<Self>(), errors::Underflow);
        return Ok(buf.get_i32());
    }
}

impl<'t> RawCodec<'t> for i64 {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= size_of::<Self>(), errors::Underflow);
        return Ok(buf.get_i64());
    }
}

impl<'t> RawCodec<'t> for f32 {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= size_of::<Self>(), errors::Underflow);
        return Ok(buf.get_f32());
    }
}

impl<'t> RawCodec<'t> for f64 {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= size_of::<Self>(), errors::Underflow);
        return Ok(buf.get_f64());
    }
}

impl<'t> RawCodec<'t> for &'t [u8] {
    fn decode_raw(buf: &mut &'t [u8]) -> Result<Self, DecodeError> {
        let val = *buf;
        buf.advance(val.len());
        Ok(val)
    }
}

impl<'t> RawCodec<'t> for Vec<u8> {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        <&[u8]>::decode_raw(buf).map(|s|s.to_owned())
    }
}

impl<'t> RawCodec<'t> for Decimal {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let ndigits = buf.get_u16() as usize;
        let weight = buf.get_i16();
        let negative = match buf.get_u16() {
            0x0000 => false,
            0x4000 => true,
            _ => errors::BadSign.fail()?,
        };
        let decimal_digits = buf.get_u16();
        ensure!(buf.remaining() >= ndigits*2, errors::Underflow);
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
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
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
        ensure!(buf.remaining() >= ndigits*2, errors::Underflow);
        for _ in 0..ndigits {
            digits.push(buf.get_u16());
        }
        Ok(BigInt {
            negative, weight, digits,
        })
    }
}

impl<'t> RawCodec<'t> for Duration {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 16, errors::Underflow);
        let micros = buf.get_i64();
        let days = buf.get_u32();
        let months = buf.get_u32();
        ensure!(months == 0 && days == 0, errors::NonZeroReservedBytes);
        Ok(Duration { micros })
    }
}

impl<'t> RawCodec<'t> for SystemTime {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        use std::time::{ Duration, UNIX_EPOCH };
        let postgres_epoch :SystemTime = UNIX_EPOCH + Duration::from_secs(946684800);

        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64();
        let val = if micros > 0 {
            postgres_epoch + Duration::from_micros(micros as u64)
        } else {
            postgres_epoch - Duration::from_micros((-micros) as u64)
        };
        Ok(val)
    }
}

impl<'t> RawCodec<'t> for LocalDatetime {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64();
        Ok(LocalDatetime { micros })
    }
}

impl<'t> RawCodec<'t> for LocalDate {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let days = buf.get_i32();
        Ok(LocalDate { days })
    }
}

impl<'t> RawCodec<'t> for LocalTime {
    fn decode_raw(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64();
        ensure!(micros >= 0 && micros < 86400_000_000, errors::InvalidDate);
        Ok(LocalTime { micros })
    }
}