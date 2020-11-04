use crate::queryable::{Queryable, DescriptorContext, DescriptorMismatch};

use crate::errors::DecodeError;
use crate::codec;
use crate::descriptors::TypePos;
use crate::model::{Duration, LocalDate, LocalTime, LocalDatetime, Datetime};
use crate::model::{Json, Uuid, BigInt, Decimal};
use crate::serialization::decode::RawCodec;
use std::time::SystemTime;

fn check_scalar(ctx: &DescriptorContext, type_pos: TypePos, type_id: Uuid, name: &str) -> Result<(), DescriptorMismatch> {
    use crate::descriptors::Descriptor::{Scalar, BaseScalar};
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

impl Queryable for String {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_STR, "std::str")
    }
}

impl Queryable for Json {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_JSON, "std::json")
    }
}

impl Queryable for Vec<u8> {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_BYTES, "std::bytes")
    }
}

impl Queryable for i16 {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_INT16, "std::int16")
    }
}

impl Queryable for i32 {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_INT32, "std::int32")
    }
}

impl Queryable for i64 {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_INT64, "std::int64")
    }
}

impl Queryable for f32 {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_FLOAT32, "std::float32")
    }
}

impl Queryable for f64 {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_FLOAT64, "std::float64")
    }
}

impl Queryable for Uuid {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_UUID, "std::uuid")
    }
}

impl Queryable for bool {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_BOOL, "std::bool")
    }
}

impl Queryable for BigInt {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_BIGINT, "std::bigint")
    }
}

impl Queryable for Decimal {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_DECIMAL, "std::decimal")
    }
}

impl Queryable for LocalDatetime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::CAL_LOCAL_DATETIME, "cal::local_datetime")
    }
}

impl Queryable for LocalDate {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::CAL_LOCAL_DATE, "cal::local_date")
    }
}

impl Queryable for LocalTime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::CAL_LOCAL_DATETIME, "cal::local_datetime")
    }
}

impl Queryable for Duration {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_DURATION, "std::duration")
    }
}

impl Queryable for SystemTime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_DATETIME, "std::datetime")
    }
}

impl Queryable for Datetime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, codec::STD_DATETIME, "std::datetime")
    }
}
