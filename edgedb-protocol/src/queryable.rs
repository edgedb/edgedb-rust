use std::io::Cursor;

use bytes::{Bytes, Buf};
use snafu::{Snafu, ensure};

use crate::errors::{self, DecodeError};
use crate::codec::raw::RawCodec;
use crate::codec;
use crate::descriptors::{Descriptor, TypePos};
use crate::model::{Json, Uuid};


#[derive(Snafu, Debug)]
#[non_exhaustive]
pub enum DescriptorMismatch {
    #[snafu(display("unexpected type {}, expected {}", unexpected, expected))]
    WrongType { unexpected: String, expected: String },
    #[snafu(display("unexpected field {}, expected {}", unexpected, expected))]
    WrongField { unexpected: String, expected: String },
    #[snafu(display("expected {} fields, got {}", expected, unexpected))]
    FieldNumber { unexpected: usize, expected: usize },
    #[snafu(display("expected {}", expected))]
    Expected { expected: String },
    #[snafu(display("invalid type descriptor"))]
    InvalidDescriptor,
}

pub struct DescriptorContext<'a> {
    descriptors: &'a [Descriptor],
}

pub trait Queryable: Sized {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        let result = Queryable::decode_raw(buf)?;
        ensure!(buf.bytes().len() == 0, errors::ExtraData);
        Ok(result)
    }
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError>;
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>;
}

impl DescriptorContext<'_> {
    pub(crate) fn new(descriptors: &[Descriptor]) -> DescriptorContext {
        DescriptorContext { descriptors }
    }
    pub fn get(&self, type_pos: TypePos)
        -> Result<&Descriptor, DescriptorMismatch>
    {
        self.descriptors.get(type_pos.0 as usize)
            .ok_or(DescriptorMismatch::InvalidDescriptor)
    }
    pub fn wrong_type(&self, descriptor: &Descriptor, expected: &str)
        -> DescriptorMismatch
    {
        DescriptorMismatch::WrongType {
            // TODO(tailhook) human-readable type description
            unexpected: format!("{:?}", descriptor),
            expected: expected.into(),
        }
    }
    pub fn field_number(&self, expected: usize, unexpected: usize)
        -> DescriptorMismatch
    {
        DescriptorMismatch::FieldNumber { expected, unexpected }
    }
    pub fn wrong_field(&self, expected: &str, unexpected: &str)
        -> DescriptorMismatch
    {
        DescriptorMismatch::WrongField {
            expected: expected.into(),
            unexpected: unexpected.into(),
        }
    }
    pub fn expected(&self, expected: &str)
        -> DescriptorMismatch
    {
        DescriptorMismatch::Expected { expected: expected.into() }
    }
}

impl Queryable for String {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        RawCodec::decode_raw(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        use crate::descriptors::Descriptor::{Scalar, BaseScalar};
        let desc = ctx.get(type_pos)?;
        match desc {
            Scalar(scalar) => {
                return Self::check_descriptor(ctx, scalar.base_type_pos);
            }
            BaseScalar(base) if base.id == codec::STD_STR => {
                return Ok(());
            }
            _ => {}
        }
        Err(ctx.wrong_type(desc, "str"))
    }
}

impl Queryable for Json {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        RawCodec::decode_raw(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        use crate::descriptors::Descriptor::{Scalar, BaseScalar};
        let desc = ctx.get(type_pos)?;
        match desc {
            Scalar(scalar) => {
                return Self::check_descriptor(ctx, scalar.base_type_pos);
            }
            BaseScalar(base) if base.id == codec::STD_JSON => {
                return Ok(());
            }
            _ => {}
        }
        Err(ctx.wrong_type(desc, "json"))
    }
}

impl Queryable for i64 {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        RawCodec::decode_raw(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        use crate::descriptors::Descriptor::{Scalar, BaseScalar};
        let desc = ctx.get(type_pos)?;
        match desc {
            Scalar(scalar) => {
                return Self::check_descriptor(ctx, scalar.base_type_pos);
            }
            BaseScalar(base) if base.id == codec::STD_INT64 => {
                return Ok(());
            }
            _ => {}
        }
        Err(ctx.wrong_type(desc, "int64"))
    }
}

impl Queryable for Uuid {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        RawCodec::decode_raw(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        use crate::descriptors::Descriptor::{Scalar, BaseScalar};
        let desc = ctx.get(type_pos)?;
        match desc {
            Scalar(scalar) => {
                return Self::check_descriptor(ctx, scalar.base_type_pos);
            }
            BaseScalar(base) if base.id == codec::STD_UUID => {
                return Ok(());
            }
            _ => {}
        }
        Err(ctx.wrong_type(desc, "uuid"))
    }
}

impl Queryable for bool {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        RawCodec::decode_raw(buf)
    }
    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        use crate::descriptors::Descriptor::{Scalar, BaseScalar};
        let desc = ctx.get(type_pos)?;
        match desc {
            Scalar(scalar) => {
                return Self::check_descriptor(ctx, scalar.base_type_pos);
            }
            BaseScalar(base) if base.id == codec::STD_BOOL => {
                return Ok(());
            }
            _ => {}
        }
        Err(ctx.wrong_type(desc, "bool"))
    }
}
