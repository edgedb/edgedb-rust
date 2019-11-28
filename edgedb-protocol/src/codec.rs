use std::convert::TryInto;
use std::fmt;
use std::str;
use std::io::Cursor;
use std::sync::Arc;

use bytes::{Bytes, Buf};
use uuid::Uuid;
use snafu::{ensure, OptionExt, ResultExt};

use crate::descriptors::{Descriptor, TypePos};
use crate::errors::{self, CodecError, DecodeError};
use crate::value::{Value, Scalar};


const STD_INT32: Uuid = Uuid::from_u128(0x104);
const STD_INT64: Uuid = Uuid::from_u128(0x105);
const STD_STR: Uuid = Uuid::from_u128(0x101);


pub trait Codec: fmt::Debug + Send + Sync + 'static {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumValue(Arc<String>);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectShape(Arc<ObjectShapeInfo>);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedTupleShape(Arc<NamedTupleShapeInfo>);

#[derive(Debug, PartialEq, Eq)]
struct ObjectShapeInfo {
    elements: Vec<ShapeElement>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShapeElement {
    pub flag_implicit: bool,
    pub flag_link_property: bool,
    pub flag_link: bool,
    pub name: String,
}

#[derive(Debug, PartialEq, Eq)]
struct NamedTupleShapeInfo {
    elements: Vec<TupleElement>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TupleElement {
    pub name: String,
}

#[derive(Debug)]
struct Int32 { }

#[derive(Debug)]
struct Int64 { }

#[derive(Debug)]
struct Str { }

struct CodecBuilder<'a> {
    descriptors: &'a [Descriptor],
}

impl<'a> CodecBuilder<'a> {
    fn build(&self, pos: TypePos) -> Result<Arc<dyn Codec>, CodecError> {
        use Descriptor::*;
        if let Some(item) = self.descriptors.get(pos.0 as usize) {
            match item {
                BaseScalar(base) => {
                    return scalar_codec(&base.id);
                }
                _ => unimplemented!(),
            }
        } else {
            return errors::UnexpectedTypePos { position: pos.0 }.fail()?;
        }
    }
}

pub fn build_codec(root: &Uuid, descriptors: &[Descriptor])
    -> Result<Arc<dyn Codec>, CodecError>
{
    let dec = CodecBuilder { descriptors };
    for (idx, desc) in descriptors.iter().enumerate() {
        if desc.id() == root {
            return dec.build(TypePos(
                idx.try_into().ok()
                .context(errors::TooManyDescriptors { index: idx })?
            ));
        }
    }
    errors::UuidNotFound { uuid: root.clone() }.fail()?
}


pub fn scalar_codec(uuid: &Uuid) -> Result<Arc<dyn Codec>, CodecError> {
    match *uuid {
        STD_INT32 => Ok(Arc::new(Int32 {})),
        STD_INT64 => Ok(Arc::new(Int64 {})),
        STD_STR => Ok(Arc::new(Str {})),
        _ => return errors::UndefinedBaseScalar { uuid: uuid.clone() }.fail()?,
    }
}

impl Codec for Int32 {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let inner = buf.get_i32_be();
        Ok(Value::Scalar(Scalar::Int32(inner)))
    }
}

impl Codec for Int64 {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let inner = buf.get_i64_be();
        Ok(Value::Scalar(Scalar::Int64(inner)))
    }
}

impl Codec for Str {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        let val = str::from_utf8(&buf.bytes())
            .context(errors::InvalidUtf8)?
            .to_owned();
        buf.advance(buf.bytes().len());
        Ok(Value::Scalar(Scalar::Str(val)))
    }
}
