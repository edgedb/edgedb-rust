use std::any::type_name;
use std::convert::{TryInto, TryFrom};
use std::fmt;
use std::str;
use std::time::{UNIX_EPOCH, SystemTime};
use std::sync::Arc;
use std::collections::HashSet;
use std::ops::Deref;

use bytes::{BytesMut, BufMut};
use uuid::Uuid as UuidVal;
use snafu::{ensure, OptionExt};

use crate::descriptors::{self, Descriptor, TypePos};
use crate::errors::{self, CodecError, DecodeError, EncodeError};
use crate::value::Value;
use crate::json;
use crate::serialization::Reader;

pub(crate) mod raw;

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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError>;
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
    pub fn decode_value(&self, mut buf: Reader)
        -> Result<Value, DecodeError>
    {
        let result = Codec::decode(self, &mut buf)?;
        buf.complete()?;
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Int32)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Int16)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Int64)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Float32)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Float64)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Str)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Bytes)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Duration)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Uuid)
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
    fn decode(&self, _buf: &mut Reader) -> Result<Value, DecodeError> {
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

fn decode_object(reader:&mut Reader, codecs:&Vec<Arc<dyn Codec>>) -> Result<Vec<Option<Value>>, DecodeError>{
    let len = reader.read_tuple_like_header()?;
    ensure!(len == codecs.len(), errors::ObjectSizeMismatch);
    let elements = std::iter::repeat_with(|| reader.read_object_element())
        .take(len);
    codecs
        .iter()
        .zip(elements)
        .map(|(codec, element)| element?.map(|element| codec.decode_value(element)).transpose())
        .collect::<Result<Vec<Option<Value>>, DecodeError>>()
}

fn decode_tuple(reader:&mut Reader, codecs:&Vec<Arc<dyn Codec>>) -> Result<Vec<Value>, DecodeError>{
    let len = reader.read_tuple_like_header()?;
    ensure!(len == codecs.len(), errors::TupleSizeMismatch);
    let elements = std::iter::repeat_with(|| reader.read_tuple_element())
        .take(len);
    codecs
        .iter()
        .zip(elements)
        .map(|(codec, element)|codec.decode_value(element?))
        .collect::<Result<Vec<Value>, DecodeError>>()
}

fn decode_input_tuple(reader:&mut Reader, codecs:&Vec<Arc<dyn Codec>>) -> Result<Vec<Value>, DecodeError>{
    let len = reader.read_tuple_like_header()?;
    ensure!(len == codecs.len(), errors::TupleSizeMismatch);
    let elements = std::iter::repeat_with(|| reader.read_array_like_element())
        .take(len);
    codecs
        .iter()
        .zip(elements)
        .map(|(codec, element)|codec.decode_value(element?))
        .collect::<Result<Vec<Value>, DecodeError>>()
}

fn decode_array_like(reader:&mut Reader, codec:&Arc<dyn Codec>, ensure_shape: impl Fn(bool) -> Result<(), DecodeError>) -> Result<Vec<Value>, DecodeError>{
    let len = reader.read_array_like_header(ensure_shape)?;
    let elements = std::iter::repeat_with(|| reader.read_array_like_element())
        .take(len);
    elements
        .map(|element| codec.decode_value(element?))
        .collect::<Result<Vec<Value>, DecodeError>>()
}

impl Codec for Object {
    fn decode(&self, reader: &mut Reader) -> Result<Value, DecodeError> {  
        let fields = decode_object(reader, &self.codecs)?;

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
    fn decode(&self, reader: &mut Reader) -> Result<Value, DecodeError> {
        let items = decode_array_like(reader, &self.element,
             |predicate| Ok(ensure!(predicate, errors::InvalidSetShape)))?;
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Decimal)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::BigInt)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Bool)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::Datetime)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::LocalDatetime)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::LocalDate)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(Value::LocalTime)
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        buf.decode_raw().map(|json: json::Json| Value::Json(json.into()))
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        self.inner.decode(buf)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        self.inner.encode(buf, val)
    }
}

impl Codec for Tuple {
    fn decode(&self, reader: &mut Reader) -> Result<Value, DecodeError> {
        let items = decode_tuple(reader, &self.elements)?;
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
    fn decode(&self, reader: &mut Reader) -> Result<Value, DecodeError> {
        let items = decode_input_tuple(reader, &self.elements)?;
        Ok(Value::Tuple(items))
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
    fn decode(&self, reader: &mut Reader) -> Result<Value, DecodeError> {
        let fields = decode_tuple(reader, &self.codecs)?;
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
    fn decode(&self, reader: &mut Reader) -> Result<Value, DecodeError> {
        let fields = decode_input_tuple(reader, &self.codecs)?;
        Ok(Value::NamedTuple {
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
    fn decode(&self, reader: &mut Reader) -> Result<Value, DecodeError> {
        let items = decode_array_like(reader, &self.element, 
            |predicate| Ok(ensure!(predicate, errors::InvalidArrayShape)))?;
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
    fn decode(&self, buf: &mut Reader) -> Result<Value, DecodeError> {
        let val : &str = buf.decode_raw()?;
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
