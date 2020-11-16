use crate::queryable::{Queryable, DescriptorContext, Decoder};
use crate::queryable::{DescriptorMismatch};

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

pub trait DecodeScalar: for<'a> RawCodec<'a> + Sized {
    fn uuid() -> Uuid;
    fn typename() -> &'static str;
}

impl<T: DecodeScalar> Queryable for T {
    fn decode(_decoder: &Decoder, buf: &[u8]) -> Result<Self, DecodeError> {
        RawCodec::decode(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos, T::uuid(), T::typename())
    }
}

impl DecodeScalar for String {
    fn uuid() -> Uuid { codec::STD_STR }
    fn typename() -> &'static str { "std::str" }
}

impl DecodeScalar for Json {
    fn uuid() -> Uuid { codec::STD_JSON }
    fn typename() -> &'static str { "std::json" }
}

/*
impl DecodeScalar for Vec<u8> {
    fn uuid() -> Uuid { codec::STD_BYTES }
    fn typename() -> &'static str { "std::bytes" }
}
*/

impl DecodeScalar for i16 {
    fn uuid() -> Uuid { codec::STD_INT16 }
    fn typename() -> &'static str { "std::int16" }
}

impl DecodeScalar for i32 {
    fn uuid() -> Uuid { codec::STD_INT32 }
    fn typename() -> &'static str { "std::int32" }
}

impl DecodeScalar for i64 {
    fn uuid() -> Uuid { codec::STD_INT64 }
    fn typename() -> &'static str { "std::int64" }
}

impl DecodeScalar for f32 {
    fn uuid() -> Uuid { codec::STD_FLOAT32 }
    fn typename() -> &'static str { "std::int32" }
}

impl DecodeScalar for f64 {
    fn uuid() -> Uuid { codec::STD_FLOAT64 }
    fn typename() -> &'static str { "std::int64" }
}

impl DecodeScalar for Uuid {
    fn uuid() -> Uuid { codec::STD_UUID }
    fn typename() -> &'static str { "std::uuid" }
}

impl DecodeScalar for bool {
    fn uuid() -> Uuid { codec::STD_BOOL }
    fn typename() -> &'static str { "std::bool" }
}

impl DecodeScalar for BigInt {
    fn uuid() -> Uuid { codec::STD_BIGINT }
    fn typename() -> &'static str { "std::bigint" }
}

impl DecodeScalar for Decimal {
    fn uuid() -> Uuid { codec::STD_DECIMAL }
    fn typename() -> &'static str { "std::decimal" }
}

impl DecodeScalar for LocalDatetime {
    fn uuid() -> Uuid { codec::CAL_LOCAL_DATETIME }
    fn typename() -> &'static str { "cal::local_datetime" }
}

impl DecodeScalar for LocalDate {
    fn uuid() -> Uuid { codec::CAL_LOCAL_DATE }
    fn typename() -> &'static str { "cal::local_date" }
}

impl DecodeScalar for LocalTime {
    fn uuid() -> Uuid { codec::CAL_LOCAL_TIME }
    fn typename() -> &'static str { "cal::local_time" }
}

impl DecodeScalar for Duration {
    fn uuid() -> Uuid { codec::STD_DURATION }
    fn typename() -> &'static str { "std::duration" }
}

impl DecodeScalar for SystemTime {
    fn uuid() -> Uuid { codec::STD_DATETIME }
    fn typename() -> &'static str { "std::datetime" }
}

impl DecodeScalar for Datetime {
    fn uuid() -> Uuid { codec::STD_DATETIME }
    fn typename() -> &'static str { "std::datetime" }
}
