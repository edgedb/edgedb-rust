use std::any::type_name;
use std::convert::{TryInto, TryFrom};
use std::fmt;
use std::str;
use std::time::{UNIX_EPOCH, SystemTime};
use std::io::Cursor;
use std::sync::Arc;

use bytes::{Bytes, Buf, BytesMut, BufMut};
use uuid::Uuid;
use snafu::{ensure, OptionExt, ResultExt};

use crate::descriptors::{self, Descriptor, TypePos};
use crate::errors::{self, CodecError, DecodeError, EncodeError};
use crate::value::{self, Value};


const STD_UUID: Uuid = Uuid::from_u128(0x100);
const STD_STR: Uuid = Uuid::from_u128(0x101);
const STD_BYTES: Uuid = Uuid::from_u128(0x102);
const STD_INT16: Uuid = Uuid::from_u128(0x103);
const STD_INT32: Uuid = Uuid::from_u128(0x104);
const STD_INT64: Uuid = Uuid::from_u128(0x105);
const STD_FLOAT32: Uuid = Uuid::from_u128(0x106);
const STD_FLOAT64: Uuid = Uuid::from_u128(0x107);
const STD_DECIMAL: Uuid = Uuid::from_u128(0x108);
const STD_BOOL: Uuid = Uuid::from_u128(0x109);
const STD_DATETIME: Uuid = Uuid::from_u128(0x10a);
const CAL_LOCAL_DATETIME: Uuid = Uuid::from_u128(0x10b);
const CAL_LOCAL_DATE: Uuid = Uuid::from_u128(0x10c);
const CAL_LOCAL_TIME: Uuid = Uuid::from_u128(0x10d);
const STD_DURATION: Uuid = Uuid::from_u128(0x10e);
const STD_JSON: Uuid = Uuid::from_u128(0x10f);
const STD_BIGINT: Uuid = Uuid::from_u128(0x110);


pub trait Codec: fmt::Debug + Send + Sync + 'static {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError>;
    fn encode(&self, buf: &mut BytesMut, value: &Value)
        -> Result<(), EncodeError>;
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
struct UuidCodec { }

#[derive(Debug)]
struct Int16 { }

#[derive(Debug)]
struct Int32 { }

#[derive(Debug)]
struct Int64 { }

#[derive(Debug)]
struct Float32 { }

#[derive(Debug)]
struct Float64 { }

#[derive(Debug)]
struct Str { }

#[derive(Debug)]
struct BytesCodec { }

#[derive(Debug)]
struct Duration { }

#[derive(Debug)]
struct Datetime { }

#[derive(Debug)]
struct LocalDatetime { }

#[derive(Debug)]
struct LocalDate { }

#[derive(Debug)]
struct LocalTime { }

#[derive(Debug)]
struct Decimal { }

#[derive(Debug)]
struct BigInt { }

#[derive(Debug)]
struct Bool { }

#[derive(Debug)]
struct Json { }

#[derive(Debug)]
struct Nothing { }

#[derive(Debug)]
struct Object {
    shape: ObjectShape,
    codecs: Vec<Arc<dyn Codec>>,
}

#[derive(Debug)]
struct SetCodec {
    element: Arc<dyn Codec>,
}

#[derive(Debug)]
struct Scalar {
    inner: Arc<dyn Codec>,
}

struct CodecBuilder<'a> {
    descriptors: &'a [Descriptor],
}

impl dyn Codec {
    pub fn decode_value(&self, buf: &mut Cursor<Bytes>)
        -> Result<Value, DecodeError>
    {
        let result = Codec::decode(self, buf)?;
        ensure!(buf.bytes().len() == 0, errors::ExtraData);
        Ok(result)
    }
}

impl<'a> CodecBuilder<'a> {
    fn build(&self, pos: TypePos) -> Result<Arc<dyn Codec>, CodecError> {
        use Descriptor::*;
        if let Some(item) = self.descriptors.get(pos.0 as usize) {
            match item {
                BaseScalar(base) => {
                    return scalar_codec(&base.id);
                }
                Set(d) => {
                    return Ok(Arc::new(SetCodec::build(d, self)?))
                }
                ObjectShape(d) => {
                    return Ok(Arc::new(Object::build(d, self)?))
                }
                Scalar(d) => {
                    return Ok(Arc::new(self::Scalar {
                        inner: self.build(d.base_type_pos)?,
                    }));
                }
                Tuple(..) => todo!(),
                NamedTuple(..) => todo!(),
                Array(..) => todo!(),
                Enumeration(..) => todo!(),
                TypeAnnotation(..) => todo!(),
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
    if root == &Uuid::from_u128(0) {
        return Ok(Arc::new(Nothing { }));
    }
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
        STD_UUID => Ok(Arc::new(UuidCodec {})),
        STD_STR => Ok(Arc::new(Str {})),
        STD_BYTES => Ok(Arc::new(BytesCodec {})),
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
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let inner = buf.get_i32_be();
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
        buf.put_i32_be(val);
        Ok(())
    }
}

impl Codec for Int16 {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let inner = buf.get_i16_be();
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
        buf.put_i16_be(val);
        Ok(())
    }
}

impl Codec for Int64 {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let inner = buf.get_i64_be();
        Ok(Value::Int64(inner))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let &val = match val {
            Value::Int64(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        buf.put_i64_be(val);
        Ok(())
    }
}

impl Codec for Float32 {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let inner = buf.get_f32_be();
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
        buf.put_f32_be(val);
        Ok(())
    }
}

impl Codec for Float64 {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let inner = buf.get_f64_be();
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
        buf.put_f64_be(val);
        Ok(())
    }
}

impl Codec for Str {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        let val = str::from_utf8(&buf.bytes())
            .context(errors::InvalidUtf8)?
            .to_owned();
        buf.advance(buf.bytes().len());
        Ok(Value::Str(val))
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

impl Codec for BytesCodec {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
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
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 16, errors::Underflow);
        let micros = buf.get_i64_be();
        let days = buf.get_u32_be();
        let months = buf.get_u32_be();
        ensure!(months == 0 && days == 0, errors::NonZeroReservedBytes);
        Ok(Value::Duration(
            value::Duration { micros }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Duration(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(16);
        buf.put_i64_be(val.micros);
        buf.put_u32_be(0);
        buf.put_u32_be(0);
        Ok(())
    }
}

impl Codec for UuidCodec {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 16, errors::Underflow);
        let uuid = Uuid::from_slice(buf.bytes())
            .context(errors::InvalidUuid)?;
        buf.advance(16);
        Ok(Value::Uuid(uuid))
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
    fn decode(&self, _buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
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

impl Codec for Object {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let size = buf.get_u32_be() as usize;
        ensure!(size == self.codecs.len(), errors::ObjectSizeMismatch);
        let mut fields = Vec::with_capacity(size);
        for codec in &self.codecs {
            ensure!(buf.remaining() >= 8, errors::Underflow);
            let _reserved = buf.get_i32_be();
            let len = buf.get_u32_be() as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off, off + len));
            buf.advance(len);
            fields.push(codec.decode_value(&mut chunk)?);
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
        buf.put_u32_be(self.codecs.len().try_into()
                    .ok().context(errors::TooManyElements)?);
        for (codec, field) in self.codecs.iter().zip(fields) {
            buf.reserve(8);
            buf.put_u32_be(0);
            let pos = buf.len();
            buf.put_u32_be(0);  // replaced after serializing a value
            codec.encode(buf, field)?;
            let len = buf.len()-pos-4;
            buf[pos..pos+4].copy_from_slice(&u32::try_from(len)
                    .ok().context(errors::ElementTooLong)?
                    .to_be_bytes());
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

impl SetCodec {
    fn build(d: &descriptors::SetDescriptor, dec: &CodecBuilder)
        -> Result<SetCodec, CodecError>
    {
        Ok(SetCodec {
            element: dec.build(d.type_pos)?,
        })
    }
}

impl Codec for SetCodec {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 20, errors::Underflow);
        let ndims = buf.get_u32_be();
        ensure!(ndims == 1, errors::InvalidSetShape);
        let _reserved0 = buf.get_u32_be();
        let _reserved1 = buf.get_u32_be();
        let size = buf.get_u32_be() as usize;
        let lower = buf.get_u32_be();
        ensure!(lower == 1, errors::InvalidSetShape);
        let mut items = Vec::with_capacity(size);
        for _ in 0..size {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            let len = buf.get_u32_be() as usize;
            ensure!(buf.remaining() >= len, errors::Underflow);
            let off = buf.position() as usize;
            let mut chunk = Cursor::new(buf.get_ref().slice(off, off + len));
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
        buf.reserve(20);
        buf.put_u32_be(1);  // ndims
        buf.put_u32_be(0);  // reserved0
        buf.put_u32_be(0);  // reserved1
        buf.put_u32_be(items.len().try_into().ok()
            .context(errors::ArrayTooLong)?);
        buf.put_u32_be(1);  // lower
        for item in items {
            buf.reserve(4);
            let pos = buf.len();
            buf.put_u32_be(0);  // replaced after serializing a value
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
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let ndigits = buf.get_u16_be() as usize;
        let weight = buf.get_i16_be();
        let negative = match buf.get_u16_be() {
            0x0000 => false,
            0x4000 => true,
            _ => errors::BadSign.fail()?,
        };
        let decimal_digits = buf.get_u16_be();
        ensure!(buf.remaining() >= ndigits*2, errors::Underflow);
        let mut digits = Vec::with_capacity(ndigits);
        for _ in 0..ndigits {
            digits.push(buf.get_u16_be());
        }
        Ok(Value::Decimal(value::Decimal {
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
        buf.put_u16_be(val.digits.len().try_into().ok()
                .context(errors::BigIntTooLong)?);
        buf.put_i16_be(val.weight);
        buf.put_u16_be(if val.negative { 0x4000 } else { 0x0000 });
        buf.put_u16_be(val.decimal_digits);
        for &dig in &val.digits {
            buf.put_u16_be(dig);
        }
        Ok(())
    }
}

impl Codec for BigInt {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let ndigits = buf.get_u16_be() as usize;
        let weight = buf.get_i16_be();
        let negative = match buf.get_u16_be() {
            0x0000 => false,
            0x4000 => true,
            _ => errors::BadSign.fail()?,
        };
        let decimal_digits = buf.get_u16_be();
        ensure!(decimal_digits == 0, errors::NonZeroReservedBytes);
        let mut digits = Vec::with_capacity(ndigits);
        ensure!(buf.remaining() >= ndigits*2, errors::Underflow);
        for _ in 0..ndigits {
            digits.push(buf.get_u16_be());
        }
        Ok(Value::BigInt(value::BigInt {
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
        buf.put_u16_be(val.digits.len().try_into().ok()
                .context(errors::BigIntTooLong)?);
        buf.put_i16_be(val.weight);
        buf.put_u16_be(if val.negative { 0x4000 } else { 0x0000 });
        buf.put_u16_be(0);
        for &dig in &val.digits {
            buf.put_u16_be(dig);
        }
        Ok(())
    }
}

impl Codec for Bool {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let res = match buf.get_u64_be() {
            0x00 => false,
            0x01 => true,
            _ => errors::InvalidBool.fail()?,
        };
        Ok(Value::Bool(res))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::Bool(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(1);
        buf.put_u64_be(match val {
            true => 1,
            false => 0,
        });
        Ok(())
    }
}

impl Codec for Datetime {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        use std::time::{Duration};

        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64_be();
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
            buf.put_i64_be(val.duration_since(postgres_epoch)
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
            buf.put_i64_be(-micros);
        }
        Ok(())
    }
}

impl Codec for LocalDatetime {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64_be();
        Ok(Value::LocalDatetime(
            value::LocalDatetime { micros }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalDatetime(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        buf.put_i64_be(val.micros);
        Ok(())
    }
}

impl Codec for LocalDate {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let days = buf.get_i32_be();
        Ok(Value::LocalDate(value::LocalDate { days }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalDate(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(4);
        buf.put_i32_be(val.days);
        Ok(())
    }
}

impl Codec for LocalTime {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let micros = buf.get_i64_be();
        Ok(Value::LocalTime(value::LocalTime { micros }))
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        let val = match val {
            Value::LocalTime(val) => val,
            _ => Err(errors::invalid_value(type_name::<Self>(), val))?,
        };
        buf.reserve(8);
        buf.put_i64_be(val.micros);
        Ok(())
    }
}

impl Codec for Json {
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let format = buf.get_u8();
        ensure!(format == 1, errors::InvalidJsonFormat);
        let val = str::from_utf8(&buf.bytes())
            .context(errors::InvalidUtf8)?
            .to_owned();
        buf.advance(val.len());
        Ok(Value::Json(val))
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
    fn decode(&self, buf: &mut Cursor<Bytes>) -> Result<Value, DecodeError> {
        self.inner.decode(buf)
    }
    fn encode(&self, buf: &mut BytesMut, val: &Value)
        -> Result<(), EncodeError>
    {
        self.inner.encode(buf, val)
    }
}
