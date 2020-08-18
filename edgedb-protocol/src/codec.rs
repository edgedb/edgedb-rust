use std::any::type_name;
use std::convert::{TryInto, TryFrom};
use std::fmt;
use std::str;
use std::time::{UNIX_EPOCH, SystemTime};
use std::io::Cursor;
use std::sync::Arc;
use std::collections::HashSet;
use std::ops::Deref;

use bytes::{Bytes as Buf, Buf as _, BytesMut, BufMut};
use uuid::Uuid as UuidVal;
use snafu::{ensure, OptionExt, ResultExt};

use crate::descriptors::{self, Descriptor, TypePos};
use crate::errors::{self, CodecError, DecodeError, EncodeError};
use crate::value::Value;
use crate::model;

pub mod raw;

pub const STD_UUID: UuidVal = UuidVal::from_u128(0x100);
pub const STD_STR: UuidVal = UuidVal::from_u128(0x101);
pub const STD_BYTES: UuidVal = UuidVal::from_u128(0x102);
pub const STD_INT16: UuidVal = UuidVal::from_u128(0x103);
pub const STD_INT32: UuidVal = UuidVal::from_u128(0x104);
pub const STD_INT64: UuidVal = UuidVal::from_u128(0x105);
pub const STD_FLOAT32: UuidVal = UuidVal::from_u128(0x106);
pub const STD_FLOAT64: UuidVal = UuidVal::from_u128(0x107);
pub const STD_DECIMAL: UuidVal = UuidVal::from_u128(0x108);
pub const STD_BOOL: UuidVal = UuidVal::from_u128(0x109);
pub const STD_DATETIME: UuidVal = UuidVal::from_u128(0x10a);
pub const CAL_LOCAL_DATETIME: UuidVal = UuidVal::from_u128(0x10b);
pub const CAL_LOCAL_DATE: UuidVal = UuidVal::from_u128(0x10c);
pub const CAL_LOCAL_TIME: UuidVal = UuidVal::from_u128(0x10d);
pub const STD_DURATION: UuidVal = UuidVal::from_u128(0x10e);
pub const STD_JSON: UuidVal = UuidVal::from_u128(0x10f);
pub const STD_BIGINT: UuidVal = UuidVal::from_u128(0x110);


pub trait Codec: fmt::Debug + Send + Sync + 'static {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError>;
    fn encode(&self, buf: &mut BytesMut, value: &Value)
        -> Result<(), EncodeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumValue(Arc<str>);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectShape(Arc<ObjectShapeInfo>);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedTupleShape(Arc<NamedTupleShapeInfo>);

#[derive(Debug, PartialEq, Eq)]
pub struct ObjectShapeInfo {
    pub elements: Vec<ShapeElement>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShapeElement {
    pub flag_implicit: bool,
    pub flag_link_property: bool,
    pub flag_link: bool,
    pub name: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct NamedTupleShapeInfo {
    pub elements: Vec<TupleElement>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TupleElement {
    pub name: String,
}

#[derive(Debug)]
pub struct Uuid;

#[derive(Debug)]
pub struct Int16;

#[derive(Debug)]
pub struct Int32;

#[derive(Debug)]
pub struct Int64;

#[derive(Debug)]
pub struct Float32;

#[derive(Debug)]
pub struct Float64;

#[derive(Debug)]
pub struct Str;

#[derive(Debug)]
pub struct Bytes;

#[derive(Debug)]
pub struct Duration;

#[derive(Debug)]
pub struct Datetime;

#[derive(Debug)]
pub struct LocalDatetime;

#[derive(Debug)]
pub struct LocalDate;

#[derive(Debug)]
pub struct LocalTime;

#[derive(Debug)]
pub struct Decimal;

#[derive(Debug)]
pub struct BigInt;

#[derive(Debug)]
pub struct Bool;

#[derive(Debug)]
pub struct Json;

#[derive(Debug)]
pub struct Nothing;

#[derive(Debug)]
pub struct Object {
    shape: ObjectShape,
    codecs: Vec<Arc<dyn Codec>>,
}

#[derive(Debug)]
pub struct Set {
    element: Arc<dyn Codec>,
}

#[derive(Debug)]
pub struct Scalar {
    inner: Arc<dyn Codec>,
}

#[derive(Debug)]
pub struct Tuple {
    elements: Vec<Arc<dyn Codec>>,
}

#[derive(Debug)]
pub struct InputTuple {
    elements: Vec<Arc<dyn Codec>>,
}

#[derive(Debug)]
pub struct NamedTuple {
    shape: NamedTupleShape,
    codecs: Vec<Arc<dyn Codec>>,
}

#[derive(Debug)]
pub struct InputNamedTuple {
    shape: NamedTupleShape,
    codecs: Vec<Arc<dyn Codec>>,
}

#[derive(Debug)]
pub struct Array {
    element: Arc<dyn Codec>,
}

#[derive(Debug)]
pub struct Enum {
    members: HashSet<Arc<str>>,
}

struct CodecBuilder<'a> {
    input: bool,
    descriptors: &'a [Descriptor],
}

impl ObjectShape {
    pub fn new(elements: Vec<ShapeElement>) -> ObjectShape {
        ObjectShape(Arc::new(ObjectShapeInfo { elements }))
    }
}

impl Deref for ObjectShape {
    type Target = ObjectShapeInfo;
    fn deref(&self) -> &ObjectShapeInfo {
        &*self.0
    }
}

impl Deref for NamedTupleShape {
    type Target = NamedTupleShapeInfo;
    fn deref(&self) -> &NamedTupleShapeInfo {
        &*self.0
    }
}

impl dyn Codec {
    pub fn decode_value(&self, buf: &mut Cursor<Buf>)
        -> Result<Value, DecodeError>
    {
        let result = Codec::decode(self, buf)?;
        ensure!(buf.bytes().len() == 0, errors::ExtraData);
        Ok(result)
    }
}

impl<'a> CodecBuilder<'a> {
    fn build(&self, pos: TypePos) -> Result<Arc<dyn Codec>, CodecError> {
        use Descriptor as D;
        if let Some(item) = self.descriptors.get(pos.0 as usize) {
            match item {
                D::BaseScalar(base) => scalar_codec(&base.id),
                D::Set(d) => Ok(Arc::new(Set::build(d, self)?)),
                D::ObjectShape(d) => Ok(Arc::new(Object::build(d, self)?)),
                D::Scalar(d) => Ok(Arc::new(Scalar {
                    inner: self.build(d.base_type_pos)?,
                })),
                D::Tuple(d) => {
                    if self.input {
                        Ok(Arc::new(InputTuple::build(d, self)?))
                    } else {
                        Ok(Arc::new(Tuple::build(d, self)?))
                    }
                }
                D::NamedTuple(d) => {
                    if self.input {
                        Ok(Arc::new(InputNamedTuple::build(d, self)?))
                    } else {
                        Ok(Arc::new(NamedTuple::build(d, self)?))
                    }
                }
                D::Array(d) => Ok(Arc::new(Array {
                    element: self.build(d.type_pos)?,
                })),
                D::Enumeration(d) => Ok(Arc::new(Enum {
                    members: d.members.iter().map(|x| x[..].into()).collect(),
                })),
                // type annotations are stripped from codecs array before
                // bilding a codec
                D::TypeAnnotation(..) => unreachable!(),
            }
        } else {
            return errors::UnexpectedTypePos { position: pos.0 }.fail()?;
        }
    }
}

pub fn build_codec(root_pos: Option<TypePos>,
    descriptors: &[Descriptor])
    -> Result<Arc<dyn Codec>, CodecError>
{
    let dec = CodecBuilder { input: false, descriptors };
    match root_pos {
        Some(pos) => dec.build(pos),
        None => Ok(Arc::new(Nothing {})),
    }
}

pub fn build_input_codec(root_pos: Option<TypePos>,
    descriptors: &[Descriptor])
    -> Result<Arc<dyn Codec>, CodecError>
{
    let dec = CodecBuilder { input: true, descriptors };
    match root_pos {
        Some(pos) => dec.build(pos),
        None => Ok(Arc::new(Nothing {})),
    }
}


pub fn scalar_codec(uuid: &UuidVal) -> Result<Arc<dyn Codec>, CodecError> {
    match *uuid {
        STD_UUID => Ok(Arc::new(Uuid {})),
        STD_STR => Ok(Arc::new(Str {})),
        STD_BYTES => Ok(Arc::new(Bytes {})),
        STD_INT16 => Ok(Arc::new(Int16 {})),
        STD_INT32 => Ok(Arc::new(Int32 {})),
        STD_INT64 => Ok(Arc::new(Int64 {})),
        STD_FLOAT32 => Ok(Arc::new(Float32 {})),
        STD_FLOAT64 => Ok(Arc::new(Float64 {})),
        STD_DECIMAL => Ok(Arc::new(Decimal {})),
        STD_BOOL => Ok(Arc::new(Bool {})),
        STD_DATETIME => Ok(Arc::new(Datetime {})),
        CAL_LOCAL_DATETIME => Ok(Arc::new(LocalDatetime {})),
        CAL_LOCAL_DATE => Ok(Arc::new(LocalDate {})),
        CAL_LOCAL_TIME => Ok(Arc::new(LocalTime {})),
        STD_DURATION => Ok(Arc::new(Duration {})),
        STD_JSON => Ok(Arc::new(Json {})),
        STD_BIGINT => Ok(Arc::new(BigInt {})),
        _ => return errors::UndefinedBaseScalar { uuid: uuid.clone() }.fail()?,
    }
}

impl Codec for Int32 {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let inner = buf.get_i32();
        Ok(Value::Int32(inner))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let &val = match val {
            Value::Int32(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(4);
        buf.put_i32(val);
        Ok(())
    }
}

impl Codec for Int16 {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let inner = buf.get_i16();
        Ok(Value::Int16(inner))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let &val = match val {
            Value::Int16(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(2);
        buf.put_i16(val);
        Ok(())
    }
}

impl Codec for Int64 {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        raw::RawCodec::decode_raw(buf).map(Value::Int64)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let &val = match val {
            Value::Int64(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        buf.put_i64(val);
        Ok(())
    }
}

impl Codec for Float32 {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let inner = buf.get_f32();
        Ok(Value::Float32(inner))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let &val = match val {
            Value::Float32(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(4);
        buf.put_f32(val);
        Ok(())
    }
}

impl Codec for Float64 {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let inner = buf.get_f64();
        Ok(Value::Float64(inner))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let &val = match val {
            Value::Float64(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        buf.put_f64(val);
        Ok(())
    }
}

impl Codec for Str {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        raw::RawCodec::decode_raw(buf).map(Value::Str)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Str(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.extend(val.as_bytes());
        Ok(())
    }
}

impl Codec for Bytes {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        let val = buf.bytes().to_owned();
        buf.advance(val.len());
        Ok(Value::Bytes(val))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Bytes(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.extend(val);
        Ok(())
    }
}

impl Codec for Duration {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 16, errors::Underflow);
        let micros = buf.get_i64();
        let days = buf.get_u32();
        let months = buf.get_u32();
        ensure!(months == 0 && days == 0, errors::NonZeroReservedBytes);
        Ok(Value::Duration(
            model::Duration { micros }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Duration(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(16);
        buf.put_i64(val.micros);
        buf.put_u32(0);
        buf.put_u32(0);
        Ok(())
    }
}

impl Codec for Uuid {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        raw::RawCodec::decode_raw(buf).map(Value::Uuid)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let &val = match val {
            Value::Uuid(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.extend(val.as_bytes());
        Ok(())
    }
}

impl Codec for Nothing {
    fn decode(&self, _buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        Ok(Value::Nothing)
    }
    fn encode(&self, _buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        match val {
            Value::Nothing => Ok(()),
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        }
    }
}

impl Object {
    fn build(d: &descriptors::ObjectShapeDescriptor, dec: &CodecBuilder)
        -> Result<Object, CodecError>
    {
        Ok(Object {
            shape: d.elements.as_slice().into(),
            codecs: d.elements.iter()
                .map(|e| dec.build(e.type_pos))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl Tuple {
    fn build(d: &descriptors::TupleTypeDescriptor, dec: &CodecBuilder)
        -> Result<Tuple, CodecError>
    {
        return Ok(Tuple {
            elements: d.element_types.iter()
                .map(|&t| dec.build(t))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl InputTuple {
    fn build(d: &descriptors::TupleTypeDescriptor, dec: &CodecBuilder)
        -> Result<InputTuple, CodecError>
    {
        return Ok(InputTuple {
            elements: d.element_types.iter()
                .map(|&t| dec.build(t))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl NamedTuple {
    fn build(d: &descriptors::NamedTupleTypeDescriptor, dec: &CodecBuilder)
        -> Result<NamedTuple, CodecError>
    {
        Ok(NamedTuple {
            shape: d.elements.as_slice().into(),
            codecs: d.elements.iter()
                .map(|e| dec.build(e.type_pos))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl InputNamedTuple {
    fn build(d: &descriptors::NamedTupleTypeDescriptor, dec: &CodecBuilder)
        -> Result<InputNamedTuple, CodecError>
    {
        Ok(InputNamedTuple {
            shape: d.elements.as_slice().into(),
            codecs: d.elements.iter()
                .map(|e| dec.build(e.type_pos))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl Codec for Object {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let size = buf.get_u32() as usize;
        ensure!(size == self.codecs.len(), errors::ObjectSizeMismatch);
        let mut fields = Vec::with_capacity(size);
        for codec in &self.codecs {
            ensure!(buf.remaining() >= 8, errors::Underflow);
            let _reserved = buf.get_i32();
            let len = buf.get_i32();
            if len < 0 {
                ensure!(len == -1, errors::InvalidMarker);
                fields.push(None);
                continue;
            }
            let len = len as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off..off + len));
            buf.advance(len);
            fields.push(Some(codec.decode_value(&mut chunk)?));
        }
        return Ok(Value::Object {
            shape: self.shape.clone(),
            fields,
        })
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let (shape, fields) = match val {
            Value::Object { shape, fields } => (shape, fields),
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        ensure!(shape == &self.shape, errors::ObjectShapeMismatch);
        ensure!(self.codecs.len() == fields.len(),
                errors::ObjectShapeMismatch);
        debug_assert_eq!(self.codecs.len(), shape.0.elements.len());
        buf.reserve(4 + 8*self.codecs.len());
        buf.put_u32(self.codecs.len().try_into()
                    .ok().context(errors::TooManyElements)?);
        for (codec, field) in self.codecs.iter().zip(fields) {
            buf.reserve(8);
            buf.put_u32(0);
            match field {
                Some(v) => {
                    let pos = buf.len();
                    buf.put_i32(0);  // replaced after serializing a value
                    codec.encode(buf, v)?;
                    let len = buf.len()-pos-4;
                    buf[pos..pos+4].copy_from_slice(&i32::try_from(len)
                            .ok().context(errors::ElementTooLong)?
                            .to_be_bytes());
                }
                None => {
                    buf.put_i32(-1);
                }
            }
        }
        Ok(())
    }
}

impl<'a> From<&'a [descriptors::ShapeElement]> for ObjectShape {
    fn from(shape: &'a [descriptors::ShapeElement]) -> ObjectShape {
        ObjectShape(Arc::new(ObjectShapeInfo {
                elements: shape.iter().map(|e| {
                    let descriptors::ShapeElement {
                        flag_implicit,
                        flag_link_property,
                        flag_link,
                        name,
                        type_pos: _,
                    } = e;
                    ShapeElement {
                        flag_implicit: *flag_implicit,
                        flag_link_property: *flag_link_property,
                        flag_link: *flag_link,
                        name: name.clone(),
                    }
                }).collect(),
            }))
    }
}

impl<'a> From<&'a [descriptors::TupleElement]> for NamedTupleShape {
    fn from(shape: &'a [descriptors::TupleElement]) -> NamedTupleShape {
        NamedTupleShape(Arc::new(NamedTupleShapeInfo {
                elements: shape.iter().map(|e| {
                    let descriptors::TupleElement {
                        name,
                        type_pos: _,
                    } = e;
                    TupleElement {
                        name: name.clone(),
                    }
                }).collect(),
            }))
    }
}

impl From<&str> for EnumValue {
    fn from(s: &str) -> EnumValue {
        EnumValue(s.into())
    }
}

impl std::ops::Deref for EnumValue {
    type Target = str;
    fn deref(&self) -> &str {
        &*self.0
    }
}

impl Set {
    fn build(d: &descriptors::SetDescriptor, dec: &CodecBuilder)
        -> Result<Set, CodecError>
    {
        Ok(Set {
            element: dec.build(d.type_pos)?,
        })
    }
}

impl Codec for Set {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let ndims = buf.get_u32();
        let _reserved0 = buf.get_u32();
        let _reserved1 = buf.get_u32();
        if ndims == 0 {
            return Ok(Value::Set(Vec::new()));
        }

        ensure!(ndims == 1, errors::InvalidSetShape);
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let size = buf.get_u32() as usize;
        let lower = buf.get_u32();
        ensure!(lower == 1, errors::InvalidSetShape);
        let mut items = Vec::with_capacity(size);
        for _ in 0..size {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            let len = buf.get_u32() as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off..off + len));
            buf.advance(len);
            items.push(self.element.decode_value(&mut chunk)?);
        }
        Ok(Value::Set(items))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let items = match val {
            Value::Set(items) => items,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        if items.is_empty() {
            buf.reserve(12);
            buf.put_u32(0);  // ndims
            buf.put_u32(0);  // reserved0
            buf.put_u32(0);  // reserved1
            return Ok(());
        }
        buf.reserve(20);
        buf.put_u32(1);  // ndims
        buf.put_u32(0);  // reserved0
        buf.put_u32(0);  // reserved1
        buf.put_u32(items.len().try_into().ok()
            .context(errors::ArrayTooLong)?);
        buf.put_u32(1);  // lower
        for item in items {
            buf.reserve(4);
            let pos = buf.len();
            buf.put_u32(0);  // replaced after serializing a value
            self.element.encode(buf, item)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(&u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
        }
        Ok(())
    }
}

impl Codec for Decimal {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
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
        Ok(Value::Decimal(model::Decimal {
            negative, weight, decimal_digits, digits,
        }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Decimal(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8 + val.digits.len()*2);
        buf.put_u16(val.digits.len().try_into().ok()
                .context(errors::BigIntTooLong)?);
        buf.put_i16(val.weight);
        buf.put_u16(if val.negative { 0x4000 } else { 0x0000 });
        buf.put_u16(val.decimal_digits);
        for &dig in &val.digits {
            buf.put_u16(dig);
        }
        Ok(())
    }
}

impl Codec for BigInt {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
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
        Ok(Value::BigInt(model::BigInt {
            negative, weight, digits,
        }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::BigInt(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8 + val.digits.len()*2);
        buf.put_u16(val.digits.len().try_into().ok()
                .context(errors::BigIntTooLong)?);
        buf.put_i16(val.weight);
        buf.put_u16(if val.negative { 0x4000 } else { 0x0000 });
        buf.put_u16(0);
        for &dig in &val.digits {
            buf.put_u16(dig);
        }
        Ok(())
    }
}

impl Codec for Bool {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        raw::RawCodec::decode_raw(buf).map(Value::Bool)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Bool(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(1);
        buf.put_u8(match val {
            true => 1,
            false => 0,
        });
        Ok(())
    }
}

impl Codec for Datetime {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        use std::time::{Duration};

        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64();
        let postgres_epoch: SystemTime = UNIX_EPOCH +
            std::time::Duration::from_secs(946684800);
        let val = if micros > 0 {
            postgres_epoch + Duration::from_micros(micros as u64)
        } else {
            postgres_epoch - Duration::from_micros((-micros) as u64)
        };
        Ok(Value::Datetime(val))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Datetime(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        let postgres_epoch: SystemTime = UNIX_EPOCH +
            std::time::Duration::from_secs(946684800);
        if *val >= postgres_epoch {
            buf.put_i64(val.duration_since(postgres_epoch)
                .ok().context(errors::DatetimeRange)?
                .as_micros()
                .try_into()
                .ok().context(errors::DatetimeRange)?);
        } else {
            let micros: i64 = postgres_epoch.duration_since(*val)
                .ok().context(errors::DatetimeRange)?
                .as_micros()
                .try_into()
                .ok().context(errors::DatetimeRange)?;
            buf.put_i64(-micros);
        }
        Ok(())
    }
}

impl Codec for LocalDatetime {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64();
        Ok(Value::LocalDatetime(
            model::LocalDatetime { micros }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalDatetime(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        buf.put_i64(val.micros);
        Ok(())
    }
}

impl Codec for LocalDate {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let days = buf.get_i32();
        Ok(Value::LocalDate(model::LocalDate { days }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalDate(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(4);
        buf.put_i32(val.days);
        Ok(())
    }
}

impl Codec for LocalTime {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64();
        ensure!(micros >= 0 && micros < 86400_000_000, errors::InvalidDate);
        Ok(Value::LocalTime(model::LocalTime { micros }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalTime(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        buf.put_i64(val.micros);
        Ok(())
    }
}

impl Codec for Json {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        let json: model::Json = raw::RawCodec::decode_raw(buf)?;
        Ok(Value::Json(json.into()))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Json(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(1 + val.len());
        buf.put_u8(1);
        buf.extend(val.as_bytes());
        Ok(())
    }
}

impl Codec for Scalar {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        self.inner.decode(buf)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        self.inner.encode(buf, val)
    }
}

impl Codec for Tuple {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let size = buf.get_u32() as usize;
        ensure!(size == self.elements.len(), errors::TupleSizeMismatch);
        let mut items = Vec::with_capacity(size);
        for codec in &self.elements {
            ensure!(buf.remaining() >= 8, errors::Underflow);
            let _reserved = buf.get_i32();
            let len = buf.get_u32() as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off..off + len));
            buf.advance(len);
            items.push(codec.decode_value(&mut chunk)?);
        }
        return Ok(Value::Tuple(items))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let items = match val {
            Value::Tuple(items) => items,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        ensure!(self.elements.len() == items.len(),
            errors::TupleShapeMismatch);
        buf.reserve(4 + 8*self.elements.len());
        buf.put_u32(self.elements.len().try_into()
                    .ok().context(errors::TooManyElements)?);
        for (codec, item) in self.elements.iter().zip(items) {
            buf.reserve(8);
            buf.put_u32(0);
            let pos = buf.len();
            buf.put_u32(0);  // replaced after serializing a value
            codec.encode(buf, item)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(&u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
        }
        Ok(())
    }
}

impl Codec for InputTuple {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let size = buf.get_u32() as usize;
        ensure!(size == self.elements.len(), errors::TupleSizeMismatch);
        let mut items = Vec::with_capacity(size);
        for codec in &self.elements {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            let len = buf.get_u32() as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off..off + len));
            buf.advance(len);
            items.push(codec.decode_value(&mut chunk)?);
        }
        return Ok(Value::Tuple(items))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let items = match val {
            Value::Tuple(items) => items,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        ensure!(self.elements.len() == items.len(),
            errors::TupleShapeMismatch);
        buf.reserve(4 + 4*self.elements.len());
        buf.put_u32(self.elements.len().try_into()
                    .ok().context(errors::TooManyElements)?);
        for (codec, item) in self.elements.iter().zip(items) {
            buf.reserve(4);
            let pos = buf.len();
            buf.put_u32(0);  // replaced after serializing a value
            codec.encode(buf, item)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(&u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
        }
        Ok(())
    }
}

impl Codec for NamedTuple {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let size = buf.get_u32() as usize;
        ensure!(size == self.codecs.len(), errors::TupleSizeMismatch);
        let mut fields = Vec::with_capacity(size);
        for codec in &self.codecs {
            ensure!(buf.remaining() >= 8, errors::Underflow);
            let _reserved = buf.get_i32();
            let len = buf.get_u32() as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off..off + len));
            buf.advance(len);
            fields.push(codec.decode_value(&mut chunk)?);
        }
        return Ok(Value::NamedTuple {
            shape: self.shape.clone(),
            fields,
        })
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let (shape, fields) = match val {
            Value::NamedTuple { shape, fields } => (shape, fields),
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        ensure!(shape == &self.shape, errors::TupleShapeMismatch);
        ensure!(self.codecs.len() == fields.len(),
                errors::ObjectShapeMismatch);
        debug_assert_eq!(self.codecs.len(), shape.0.elements.len());
        buf.reserve(4 + 8*self.codecs.len());
        buf.put_u32(self.codecs.len().try_into()
                    .ok().context(errors::TooManyElements)?);
        for (codec, field) in self.codecs.iter().zip(fields) {
            buf.reserve(8);
            buf.put_u32(0);
            let pos = buf.len();
            buf.put_u32(0);  // replaced after serializing a value
            codec.encode(buf, field)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(&u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
        }
        Ok(())
    }
}

impl Codec for InputNamedTuple {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let size = buf.get_u32() as usize;
        ensure!(size == self.codecs.len(), errors::TupleSizeMismatch);
        let mut fields = Vec::with_capacity(size);
        for codec in &self.codecs {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            let len = buf.get_u32() as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off..off + len));
            buf.advance(len);
            fields.push(codec.decode_value(&mut chunk)?);
        }
        return Ok(Value::NamedTuple {
            shape: self.shape.clone(),
            fields,
        })
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let (shape, fields) = match val {
            Value::NamedTuple { shape, fields } => (shape, fields),
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        ensure!(shape == &self.shape, errors::TupleShapeMismatch);
        ensure!(self.codecs.len() == fields.len(),
                errors::ObjectShapeMismatch);
        debug_assert_eq!(self.codecs.len(), shape.0.elements.len());
        buf.reserve(4 + 8*self.codecs.len());
        buf.put_u32(self.codecs.len().try_into()
                    .ok().context(errors::TooManyElements)?);
        for (codec, field) in self.codecs.iter().zip(fields) {
            buf.reserve(4);
            let pos = buf.len();
            buf.put_u32(0);  // replaced after serializing a value
            codec.encode(buf, field)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(&u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
        }
        Ok(())
    }
}

impl Codec for Array {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let ndims = buf.get_u32();
        let _reserved0 = buf.get_u32();
        let _reserved1 = buf.get_u32();
        if ndims == 0 {
            return Ok(Value::Array(Vec::new()));
        }
        ensure!(ndims == 1, errors::InvalidArrayShape);
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let size = buf.get_u32() as usize;
        let lower = buf.get_u32();
        ensure!(lower == 1, errors::InvalidArrayShape);
        let mut items = Vec::with_capacity(size);
        for _ in 0..size {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            let len = buf.get_u32() as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off..off + len));
            buf.advance(len);
            items.push(self.element.decode_value(&mut chunk)?);
        }
        Ok(Value::Array(items))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let items = match val {
            Value::Array(items) => items,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        if items.is_empty() {
            buf.reserve(12);
            buf.put_u32(0);  // ndims
            buf.put_u32(0);  // reserved0
            buf.put_u32(0);  // reserved1
            return Ok(());
        }
        buf.reserve(20);
        buf.put_u32(1);  // ndims
        buf.put_u32(0);  // reserved0
        buf.put_u32(0);  // reserved1
        buf.put_u32(items.len().try_into().ok()
            .context(errors::ArrayTooLong)?);
        buf.put_u32(1);  // lower
        for item in items {
            buf.reserve(4);
            let pos = buf.len();
            buf.put_u32(0);  // replaced after serializing a value
            self.element.encode(buf, item)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(&u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
        }
        Ok(())
    }
}

impl Codec for Enum {
    fn decode(&self, buf: &mut Cursor<Buf>) -> Result<Value, DecodeError> {
        let val = str::from_utf8(&buf.bytes())
            .context(errors::InvalidUtf8)?;
        let val = self.members.get(val)
            .context(errors::ExtraEnumValue)?;
        buf.advance(buf.bytes().len());
        Ok(Value::Enum(EnumValue(val.clone())))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Enum(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        ensure!(self.members.get(&val.0).is_some(), errors::MissingEnumValue);
        buf.extend(val.0.as_bytes());
        Ok(())
    }
}
