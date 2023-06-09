use std::any::type_name;
use std::convert::{TryInto, TryFrom};
use std::fmt;
use std::str;
use std::sync::Arc;
use std::collections::HashSet;
use std::ops::Deref;

use bytes::{BytesMut, Buf, BufMut};
use uuid::Uuid as UuidVal;
use snafu::{ensure, OptionExt};

use crate::common::Cardinality;
use crate::descriptors::{self, Descriptor, TypePos};
use crate::errors::{self, CodecError, DecodeError, EncodeError};
use crate::value::{Value, SparseObject};
use crate::model;
use crate::serialization::decode::{RawCodec, DecodeTupleLike, DecodeArrayLike};
use crate::serialization::decode::DecodeRange;
use crate::model::range;

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
pub const CAL_RELATIVE_DURATION: UuidVal = UuidVal::from_u128(0x111);
pub const CAL_DATE_DURATION: UuidVal = UuidVal::from_u128(0x112);
pub const STD_JSON: UuidVal = UuidVal::from_u128(0x10f);
pub const STD_BIGINT: UuidVal = UuidVal::from_u128(0x110);
pub const CFG_MEMORY: UuidVal = UuidVal::from_u128(0x130);
pub const PGVECTOR_VECTOR: UuidVal =
    UuidVal::from_u128(0x9565dd88_04f5_11ee_a691_0b6ebe179825);

pub(crate) fn uuid_to_known_name(uuid: &UuidVal) -> Option<&'static str> {

    match *uuid {
        STD_UUID => Some("BaseScalar(uuid)"),
        STD_STR => Some("BaseScalar(str)"),
        STD_BYTES => Some("BaseScalar(bytes)"),
        STD_INT16 => Some("BaseScalar(int16)"),
        STD_INT32 => Some("BaseScalar(int32)"),
        STD_INT64 => Some("BaseScalar(int64)"),
        STD_FLOAT32 => Some("BaseScalar(float32)"),
        STD_FLOAT64 => Some("BaseScalar(float64)"),
        STD_DECIMAL => Some("BaseScalar(decimal)"),
        STD_BOOL => Some("BaseScalar(bool)"),
        STD_DATETIME => Some("BaseScalar(datetime)"),
        CAL_LOCAL_DATETIME => Some("BaseScalar(cal::local_datetime)"),
        CAL_LOCAL_DATE => Some("BaseScalar(cal::local_date)"),
        CAL_LOCAL_TIME => Some("BaseScalar(cal::local_time)"),
        STD_DURATION => Some("BaseScalar(duration)"),
        CAL_RELATIVE_DURATION => Some("BaseScalar(cal::relative_duration)"),
        CAL_DATE_DURATION => Some("BaseScalar(cal::date_duration)"),
        STD_JSON => Some("BaseScalar(std::json)"),
        STD_BIGINT => Some("BaseScalar(bigint)"),
        CFG_MEMORY => Some("BaseScalar(cfg::memory)"),
        PGVECTOR_VECTOR => Some("BaseScalar(ext::pgvector::vector)"),
        _ => None
    }
}

pub trait Codec: fmt::Debug + Send + Sync + 'static {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError>;
    fn encode(&self, buf: &mut BytesMut, value: &Value)
        -> Result<(), EncodeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumValue(Arc<str>);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectShape(pub(crate) Arc<ObjectShapeInfo>);
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
    pub cardinality: Option<Cardinality>,
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
pub struct RelativeDuration;

#[derive(Debug)]
pub struct DateDuration;

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
pub struct ConfigMemory;

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
pub struct Input {
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
pub struct NamedTuple {
    shape: NamedTupleShape,
    codecs: Vec<Arc<dyn Codec>>,
}

#[derive(Debug)]
pub struct Array {
    element: Arc<dyn Codec>,
}

#[derive(Debug)]
pub struct Vector {
}

#[derive(Debug)]
pub struct Range {
    element: Arc<dyn Codec>,
}

#[derive(Debug)]
pub struct ArrayAdapter(Array);

#[derive(Debug)]
pub struct Enum {
    members: HashSet<Arc<str>>,
}

struct CodecBuilder<'a> {
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
                    Ok(Arc::new(Tuple::build(d, self)?))
                }
                D::NamedTuple(d) => {
                    Ok(Arc::new(NamedTuple::build(d, self)?))
                }
                D::Array(d) => Ok(Arc::new(Array {
                    element: self.build(d.type_pos)?,
                })),
                D::Range(d) => Ok(Arc::new(Range {
                    element: self.build(d.type_pos)?,
                })),
                D::Enumeration(d) => Ok(Arc::new(Enum {
                    members: d.members.iter().map(|x| x[..].into()).collect(),
                })),
                D::InputShape(d) => Ok(Arc::new(Input::build(d, self)?)),
                // type annotations are stripped from codecs array before
                // building a codec
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
    let dec = CodecBuilder { descriptors };
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
        CAL_RELATIVE_DURATION => Ok(Arc::new(RelativeDuration {})),
        CAL_DATE_DURATION => Ok(Arc::new(DateDuration {})),
        STD_JSON => Ok(Arc::new(Json {})),
        STD_BIGINT => Ok(Arc::new(BigInt {})),
        CFG_MEMORY => Ok(Arc::new(ConfigMemory {})),
        PGVECTOR_VECTOR => Ok(Arc::new(Vector {})),
        _ => return errors::UndefinedBaseScalar { uuid: uuid.clone() }.fail()?,
    }
}

impl Codec for Int32 {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Int32)
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Int16)
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Int64)
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

impl Codec for ConfigMemory {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::ConfigMemory)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let &val = match val {
            Value::ConfigMemory(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        buf.put_i64(val.0);
        Ok(())
    }
}

impl Codec for Float32 {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Float32)
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Float64)
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Str)
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Bytes)
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Duration)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Duration(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_duration(buf, val)
    }
}

pub(crate) fn encode_duration(buf: &mut BytesMut, val: &model::Duration)
    -> Result<(), EncodeError>
{
    buf.reserve(16);
    buf.put_i64(val.micros);
    buf.put_u32(0);
    buf.put_u32(0);
    Ok(())
}

impl Codec for RelativeDuration {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::RelativeDuration)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::RelativeDuration(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_relative_duration(buf, val)
    }
}

pub(crate) fn encode_relative_duration(buf: &mut BytesMut,
                                       val: &model::RelativeDuration)
    -> Result<(), EncodeError>
{
    buf.reserve(16);
    buf.put_i64(val.micros);
    buf.put_i32(val.days);
    buf.put_i32(val.months);
    Ok(())
}

impl Codec for DateDuration {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::DateDuration)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::DateDuration(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_date_duration(buf, val)
    }
}

pub(crate) fn encode_date_duration(buf: &mut BytesMut,
                                   val: &model::DateDuration)
    -> Result<(), EncodeError>
{
    buf.reserve(16);
    buf.put_i64(0);
    buf.put_i32(val.days);
    buf.put_i32(val.months);
    Ok(())
}

impl Codec for Uuid {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Uuid)
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
    fn decode(&self, _buf: &[u8]) -> Result<Value, DecodeError> {
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

impl Input {
    fn build(d: &descriptors::InputShapeTypeDescriptor, dec: &CodecBuilder)
        -> Result<Input, CodecError>
    {
        Ok(Input {
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

fn decode_tuple<'t>(mut elements:DecodeTupleLike, codecs:&Vec<Arc<dyn Codec>>) -> Result<Vec<Value>, DecodeError>{
    codecs
        .iter()
        .map(|codec| codec.decode(elements.read()?.ok_or_else(|| errors::MissingRequiredElement.build())?))
        .collect::<Result<Vec<Value>, DecodeError>>()
}

fn decode_array_like<'t>(elements: DecodeArrayLike<'t>, codec:&dyn Codec) -> Result<Vec<Value>, DecodeError>{
    elements
        .map(|element| codec.decode(element?))
        .collect::<Result<Vec<Value>, DecodeError>>()
}

impl Codec for Object {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        let mut elements = DecodeTupleLike::new_object(buf, self.codecs.len())?;
        let fields = self.codecs
            .iter()
            .map(|codec| elements.read()?.map(|element| codec.decode(element)).transpose())
            .collect::<Result<Vec<Option<Value>>, DecodeError>>()?;

        Ok(Value::Object {
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

impl Codec for Input {
    fn decode(&self, mut buf: &[u8]) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let count = buf.get_u32() as usize;
        let mut fields = vec![None; self.codecs.len()];
        for _ in 0..count {
            ensure!(buf.remaining() >= 8, errors::Underflow);
            let index = buf.get_u32() as usize;
            ensure!(index < self.codecs.len(), errors::InvalidIndex { index });
            let length = buf.get_i32();
            if length < 0 {
                fields[index] = Some(None);
            } else {
                let length = length as usize;
                let value = self.codecs[index].decode(&buf[..length])?;
                buf.advance(length);
                fields[index] = Some(Some(value));
            }
        }
        Ok(Value::SparseObject(SparseObject {
            shape: self.shape.clone(),
            fields,
        }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let ob = match val {
            Value::SparseObject(ob) => ob,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        let mut items = Vec::with_capacity(self.codecs.len());
        let dest_els = &self.shape.0.elements;
        for (fld, el) in ob.fields.iter().zip(&ob.shape.0.elements) {
            if let Some(value) = fld {
                if let Some(index) =
                    dest_els.iter().position(|x| x.name == el.name)
                {
                    items.push((index, value));
                }
            }
        }
        buf.reserve(4 + 8*items.len());
        buf.put_u32(items.len().try_into()
                    .ok().context(errors::TooManyElements)?);
        for (index, value) in items {
            buf.reserve(8);
            buf.put_u32(index as u32);
            let pos = buf.len();
            if let Some(value) = value {
                buf.put_i32(0);  // replaced after serializing a value
                self.codecs[index].encode(buf, value)?;
                let len = buf.len()-pos-4;
                buf[pos..pos+4].copy_from_slice(&i32::try_from(len)
                        .ok().context(errors::ElementTooLong)?
                        .to_be_bytes());
            } else {
                buf.put_i32(-1);
            }
        }
        Ok(())
    }
}

impl Codec for ArrayAdapter {
    fn decode(&self, mut buf: &[u8]) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let count = buf.get_u32() as usize;
        ensure!(count == 1, errors::InvalidArrayShape);
        let _reserved = buf.get_i32() as usize;
        let len = buf.get_i32() as usize;
        ensure!(buf.remaining() >= len, errors::Underflow);
        ensure!(buf.remaining() <= len, errors::ExtraData);
        return self.0.decode(buf);
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        buf.reserve(12);
        buf.put_u32(1);
        buf.put_u32(0);
        let pos = buf.len();
        buf.put_i32(0);  // replaced after serializing a value
        self.0.encode(buf, val)?;
        let len = buf.len()-pos-4;
        buf[pos..pos+4].copy_from_slice(&i32::try_from(len)
                .ok().context(errors::ElementTooLong)?
                .to_be_bytes());
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
                        cardinality,
                        name,
                        type_pos: _,
                    } = e;
                    ShapeElement {
                        flag_implicit: *flag_implicit,
                        flag_link_property: *flag_link_property,
                        flag_link: *flag_link,
                        cardinality: *cardinality,
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
        let element = match dec.descriptors.get(d.type_pos.0 as usize) {
            Some(Descriptor::Array(d)) => {
                Arc::new(ArrayAdapter(Array {
                    element: dec.build(d.type_pos)?,
                }))
            }
            _ => dec.build(d.type_pos)?,
        };
        Ok(Set { element })
    }
}

impl Codec for Set {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        let elements = DecodeArrayLike::new_set(buf)?;
        let items = decode_array_like(elements, &*self.element)?;
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Decimal)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Decimal(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_decimal(buf, val)
    }
}

pub(crate) fn encode_decimal(buf: &mut BytesMut, val: &model::Decimal)
    -> Result<(), EncodeError>
{
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

impl Codec for BigInt {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::BigInt)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::BigInt(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_big_int(buf, val)
    }
}


pub(crate) fn encode_big_int(buf: &mut BytesMut, val: &model::BigInt)
    -> Result<(), EncodeError>
{
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

impl Codec for Bool {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Bool)
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::Datetime)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Datetime(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_datetime(buf, val)
    }
}

pub(crate) fn encode_datetime(buf: &mut BytesMut, val: &model::Datetime)
    -> Result<(), EncodeError>
{
    buf.reserve(8);
    buf.put_i64(val.micros);
    Ok(())
}

impl Codec for LocalDatetime {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::LocalDatetime)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalDatetime(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_local_datetime(buf, val)
    }
}

pub(crate) fn encode_local_datetime(buf: &mut BytesMut,
                                    val: &model::LocalDatetime)
    -> Result<(), EncodeError>
{
    buf.reserve(8);
    buf.put_i64(val.micros);
    Ok(())
}

impl Codec for LocalDate {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::LocalDate)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalDate(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_local_date(buf, val)
    }
}

pub(crate) fn encode_local_date(buf: &mut BytesMut, val: &model::LocalDate)
    -> Result<(), EncodeError>
{
    buf.reserve(4);
    buf.put_i32(val.days);
    Ok(())
}

impl Codec for LocalTime {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(Value::LocalTime)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalTime(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        encode_local_time(buf, val)
    }
}

pub(crate) fn encode_local_time(buf: &mut BytesMut, val: &model::LocalTime)
    -> Result<(), EncodeError>
{
    buf.reserve(8);
    buf.put_i64(val.micros as i64);
    Ok(())
}

impl Codec for Json {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        RawCodec::decode(buf).map(|json: model::Json| Value::Json(json.into()))
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
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        self.inner.decode(buf)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        self.inner.encode(buf, val)
    }
}

impl Codec for Tuple {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        let elements = DecodeTupleLike::new_object(buf, self.elements.len())?;
        let items = decode_tuple(elements, &self.elements)?;
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

impl Codec for NamedTuple {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        let elements = DecodeTupleLike::new_tuple(buf, self.codecs.len())?;
        let fields = decode_tuple(elements, &self.codecs)?;
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

impl Codec for Array {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        let elements = DecodeArrayLike::new_array(buf)?;
        let items = decode_array_like(elements, &*self.element)?;
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

impl Codec for Vector {
    fn decode(&self, mut buf: &[u8]) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let length = buf.get_u16() as usize;
        let _reserved = buf.get_u16();
        ensure!(buf.remaining() >= length*4, errors::Underflow);
        let vec = (0..length)
            .map(|_| f32::from_bits(buf.get_u32()))
            .collect();
        Ok(Value::Vector(vec))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let items = match val {
            Value::Vector(items) => items,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        if items.is_empty() {
            buf.reserve(4);
            buf.put_i16(0);  // length
            buf.put_i16(0);  // reserved
            return Ok(());
        }
        buf.reserve(4 + items.len()*4);
        buf.put_i16(items.len().try_into().ok()
            .context(errors::ArrayTooLong)?);
        buf.put_i16(0);  // reserved
        for item in items {
            buf.put_u32(item.to_bits());
        }
        Ok(())
    }
}

impl Codec for Range {
    fn decode(&self, mut buf: &[u8]) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let flags = buf.get_u8() as usize;

        let empty = (flags & range::EMPTY) != 0;
        let inc_lower = (flags & range::LB_INC) != 0;
        let inc_upper = (flags & range::UB_INC) != 0;
        let has_lower = (flags & (range::EMPTY | range::LB_INF)) == 0;
        let has_upper = (flags & (range::EMPTY | range::UB_INF)) == 0;

        let mut range = DecodeRange::new(buf)?;

        let lower = if has_lower {
            Some(Box::new(self.element.decode(range.read()?)?))
        } else {
            None
        };
        let upper = if has_upper {
            Some(Box::new(self.element.decode(range.read()?)?))
        } else {
            None
        };

        Ok(Value::Range(model::Range {
            lower,
            upper,
            inc_lower,
            inc_upper,
            empty,
        }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let rng = match val {
            Value::Range(rng) => rng,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };

        let flags =
            if rng.empty { range::EMPTY } else {
                (if rng.inc_lower { range::LB_INC } else { 0 }) |
                (if rng.inc_upper { range::UB_INC } else { 0 }) |
                (if rng.lower.is_none() { range::LB_INF } else { 0 }) |
                (if rng.upper.is_none() { range::UB_INF } else { 0 })
            };
        buf.reserve(1);
        buf.put_u8(flags as u8);

        if let Some(lower) = &rng.lower {
            let pos = buf.len();
            buf.reserve(4);
            buf.put_u32(0);  // replaced after serializing a value
            self.element.encode(buf, &lower)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(
                &u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
        }

        if let Some(upper) = &rng.upper {
            let pos = buf.len();
            buf.reserve(4);
            buf.put_u32(0);  // replaced after serializing a value
            self.element.encode(buf, &upper)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(
                &u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
        }

        Ok(())
    }
}


impl Codec for Enum {
    fn decode(&self, buf: &[u8]) -> Result<Value, DecodeError> {
        let val : &str = RawCodec::decode(buf)?;
        let val = self.members.get(val)
            .context(errors::ExtraEnumValue)?;
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
