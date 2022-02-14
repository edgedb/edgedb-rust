use std::convert::TryInto;
use std::mem::size_of;
use std::str;
use std::time::SystemTime;

use bytes::{Buf, BufMut};
use edgedb_errors::{ClientEncodingError, Error, ErrorKind};
use snafu::{ensure, ResultExt};

use crate::codec;
use crate::descriptors::TypePos;
use crate::errors::{self, DecodeError};
use crate::model::ConfigMemory;
use crate::model::RelativeDuration;
use crate::model::{BigInt, Decimal};
use crate::model::{Datetime, Duration, LocalDate, LocalDatetime, LocalTime};
use crate::model::{Json, Uuid};
use crate::query_arg::{DescriptorContext, Encoder, ScalarArg};
use crate::serialization::decode::queryable::scalars::DecodeScalar;

pub trait RawCodec<'t>: Sized {
    fn decode(buf: &'t [u8]) -> Result<Self, DecodeError>;
}

fn ensure_exact_size(buf: &[u8], expected_size: usize) -> Result<(), DecodeError> {
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
        <&str>::decode(buf).map(|s| s.to_owned())
    }
}

fn check_scalar(
    ctx: &DescriptorContext,
    type_pos: TypePos,
    type_id: Uuid,
    name: &str,
) -> Result<(), Error> {
    use crate::descriptors::Descriptor::{BaseScalar, Scalar};
    let desc = ctx.get(type_pos)?;
    match desc {
        Scalar(scalar) => {
            return check_scalar(ctx, scalar.base_type_pos, type_id, name);
        }
        BaseScalar(base) if base.id == type_id => {
            return Ok(());
        }
        _ => {}
    }
    Err(ctx.wrong_type(desc, name))
}

impl ScalarArg for String {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.extend(self.as_bytes());
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl ScalarArg for &'_ str {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.extend(self.as_bytes());
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, String::uuid(), String::typename())
    }
}

impl<'t> RawCodec<'t> for &'t str {
    fn decode(buf: &'t [u8]) -> Result<Self, DecodeError> {
        let val = str::from_utf8(buf).context(errors::InvalidUtf8)?;
        Ok(val)
    }
}

impl ScalarArg for Json {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.reserve(self.len() + 1);
        encoder.buf.put_u8(1);
        encoder.buf.extend(self.as_bytes());
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Json::uuid(), Json::typename())
    }
}

impl<'t> RawCodec<'t> for Json {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let format = buf.get_u8();
        ensure!(format == 1, errors::InvalidJsonFormat);
        let val = str::from_utf8(buf).context(errors::InvalidUtf8)?.to_owned();
        Ok(Json::_new_unchecked(val))
    }
}

impl<'t> RawCodec<'t> for Uuid {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, 16)?;
        let uuid = Uuid::from_slice(buf).unwrap();
        Ok(uuid)
    }
}

impl ScalarArg for Uuid {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.reserve(16);
        encoder.buf.extend(self.as_bytes());
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for bool {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, 1)?;
        let res = match buf[0] {
            0x00 => false,
            0x01 => true,
            _ => errors::InvalidBool.fail()?,
        };
        Ok(res)
    }
}

impl ScalarArg for bool {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.reserve(1);
        encoder.buf.put_u8(match self {
            false => 0x00,
            true => 0x01,
        });
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for i16 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_i16());
    }
}

impl ScalarArg for i16 {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.reserve(2);
        encoder.buf.put_i16(*self);
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for i32 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_i32());
    }
}

impl ScalarArg for i32 {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.reserve(4);
        encoder.buf.put_i32(*self);
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for i64 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_i64());
    }
}

impl<'t> RawCodec<'t> for ConfigMemory {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(ConfigMemory(buf.get_i64()));
    }
}

impl ScalarArg for i64 {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.reserve(8);
        encoder.buf.put_i64(*self);
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for f32 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_f32());
    }
}

impl ScalarArg for f32 {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.reserve(4);
        encoder.buf.put_f32(*self);
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for f64 {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, size_of::<Self>())?;
        return Ok(buf.get_f64());
    }
}

impl ScalarArg for f64 {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.reserve(8);
        encoder.buf.put_f64(*self);
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for &'t [u8] {
    fn decode(buf: &'t [u8]) -> Result<Self, DecodeError> {
        Ok(buf)
    }
}

impl ScalarArg for &'_ [u8] {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.extend(*self);
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, codec::STD_BYTES, "std::bytes")
    }
}

impl<'t> RawCodec<'t> for Vec<u8> {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        Ok(buf.to_owned())
    }
}

impl ScalarArg for Vec<u8> {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.buf.extend(&self[..]);
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, codec::STD_BYTES, "std::bytes")
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
        ensure_exact_size(buf, ndigits * 2)?;
        let mut digits = Vec::with_capacity(ndigits);
        for _ in 0..ndigits {
            digits.push(buf.get_u16());
        }
        Ok(Decimal {
            negative,
            weight,
            decimal_digits,
            digits,
        })
    }
}

#[cfg(feature = "bigdecimal")]
impl<'t> RawCodec<'t> for bigdecimal::BigDecimal {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        use crate::errors::DecodeValue;
        use snafu::IntoError;

        let dec: Decimal = RawCodec::decode(buf)?;
        Ok(dec
            .try_into()
            .map_err(|e| DecodeValue.into_error(Box::new(e)))?)
    }
}

impl ScalarArg for Decimal {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        codec::encode_decimal(encoder.buf, self).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

#[cfg(feature = "num-bigint")]
impl<'t> RawCodec<'t> for num_bigint::BigInt {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        use crate::errors::DecodeValue;
        use snafu::IntoError;

        let dec: BigInt = RawCodec::decode(buf)?;
        Ok(dec
            .try_into()
            .map_err(|e| DecodeValue.into_error(Box::new(e)))?)
    }
}

#[cfg(feature = "bigdecimal")]
impl ScalarArg for bigdecimal::BigDecimal {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        let val = self.clone().try_into().map_err(|e| {
            ClientEncodingError::with_source(e).context("cannot serialize BigDecimal value")
        })?;
        codec::encode_decimal(encoder.buf, &val).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
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
        ensure_exact_size(buf, ndigits * 2)?;
        for _ in 0..ndigits {
            digits.push(buf.get_u16());
        }
        Ok(BigInt {
            negative,
            weight,
            digits,
        })
    }
}

impl ScalarArg for BigInt {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        codec::encode_big_int(encoder.buf, self).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

#[cfg(feature = "bigdecimal")]
impl ScalarArg for num_bigint::BigInt {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        let val = self.clone().try_into().map_err(|e| {
            ClientEncodingError::with_source(e).context("cannot serialize BigInt value")
        })?;
        codec::encode_big_int(encoder.buf, &val).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
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

impl ScalarArg for Duration {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        codec::encode_duration(encoder.buf, self).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for RelativeDuration {
    fn decode(mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure_exact_size(buf, 16)?;
        let micros = buf.get_i64();
        let days = buf.get_i32();
        let months = buf.get_i32();
        Ok(RelativeDuration {
            micros,
            days,
            months,
        })
    }
}

impl ScalarArg for RelativeDuration {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        codec::encode_relative_duration(encoder.buf, self)
            .map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for SystemTime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let micros = i64::decode(buf)?;

        use std::time::{Duration, UNIX_EPOCH};
        let postgres_epoch: SystemTime = UNIX_EPOCH + Duration::from_secs(946684800);

        let val = if micros > 0 {
            postgres_epoch + Duration::from_micros(micros as u64)
        } else {
            postgres_epoch - Duration::from_micros((-micros) as u64)
        };
        Ok(val)
    }
}

impl ScalarArg for SystemTime {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        let val = self.clone().try_into().map_err(|e| {
            ClientEncodingError::with_source(e).context("cannot serialize SystemTime value")
        })?;
        codec::encode_datetime(encoder.buf, &val).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for Datetime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let micros = i64::decode(buf)?;
        Ok(Datetime::try_from_micros(micros).map_err(|_| errors::InvalidDate.build())?)
    }
}

impl ScalarArg for Datetime {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        codec::encode_datetime(encoder.buf, self).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for LocalDatetime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let micros = i64::decode(buf)?;
        Ok(LocalDatetime { micros })
    }
}

impl ScalarArg for LocalDatetime {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        codec::encode_local_datetime(encoder.buf, self)
            .map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for LocalDate {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let days = i32::decode(buf)?;
        Ok(LocalDate { days })
    }
}

impl ScalarArg for LocalDate {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        codec::encode_local_date(encoder.buf, self).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}

impl<'t> RawCodec<'t> for LocalTime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let micros = i64::decode(buf)?;
        ensure!(
            micros >= 0 && micros < 86_400 * 1_000_000,
            errors::InvalidDate
        );
        Ok(LocalTime {
            micros: micros as u64,
        })
    }
}

impl ScalarArg for LocalTime {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        codec::encode_local_time(encoder.buf, self).map_err(|e| ClientEncodingError::with_source(e))
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        check_scalar(ctx, pos, Self::uuid(), Self::typename())
    }
}
