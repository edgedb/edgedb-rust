/*!
Contains the [QueryArg](crate::query_arg::QueryArg) and [QueryArgs](crate::query_arg::QueryArgs) traits.
*/

use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use snafu::OptionExt;
use uuid::Uuid;

use edgedb_errors::ParameterTypeMismatchError;
use edgedb_errors::{ClientEncodingError, DescriptorMismatch, ProtocolError};
use edgedb_errors::{Error, ErrorKind, InvalidReferenceError};

use crate::codec::{self, build_codec, Codec, ObjectShape, ShapeElement};
use crate::descriptors::TypePos;
use crate::descriptors::{Descriptor, EnumerationTypeDescriptor};
use crate::errors;
use crate::features::ProtocolVersion;
use crate::model::range;
use crate::value::Value;

pub struct Encoder<'a> {
    pub ctx: &'a DescriptorContext<'a>,
    pub buf: &'a mut BytesMut,
}

/// A single argument for a query.
pub trait QueryArg: Send + Sync + Sized {
    fn encode_slot(&self, encoder: &mut Encoder) -> Result<(), Error>;
    fn check_descriptor(&self, ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error>;
    fn to_value(&self) -> Result<Value, Error>;
}

pub trait ScalarArg: Send + Sync + Sized {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error>;
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error>;
    fn to_value(&self) -> Result<Value, Error>;
}

/// A tuple of query arguments.
///
/// This trait is implemented for tuples of sizes up to twelve. You can derive
/// it for a structure in this case it's treated as a named tuple (i.e. query
/// should include named arguments rather than numeric ones).
pub trait QueryArgs: Send + Sync {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error>;
}

pub struct DescriptorContext<'a> {
    #[allow(dead_code)]
    pub(crate) proto: &'a ProtocolVersion,
    pub(crate) root_pos: Option<TypePos>,
    pub(crate) descriptors: &'a [Descriptor],
}

impl<'a> Encoder<'a> {
    pub fn new(ctx: &'a DescriptorContext<'a>, buf: &'a mut BytesMut) -> Encoder<'a> {
        Encoder { ctx, buf }
    }
    pub fn length_prefixed(
        &mut self,
        f: impl FnOnce(&mut Encoder) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.buf.reserve(4);
        let pos = self.buf.len();
        self.buf.put_u32(0); // replaced after serializing a value
                             //
        f(self)?;

        let len = self.buf.len() - pos - 4;
        self.buf[pos..pos + 4].copy_from_slice(
            &u32::try_from(len)
                .map_err(|_| ClientEncodingError::with_message("alias is too long"))?
                .to_be_bytes(),
        );

        Ok(())
    }
}

impl DescriptorContext<'_> {
    pub fn get(&self, type_pos: TypePos) -> Result<&Descriptor, Error> {
        self.descriptors
            .get(type_pos.0 as usize)
            .ok_or_else(|| ProtocolError::with_message("invalid type descriptor"))
    }
    pub fn build_codec(&self) -> Result<Arc<dyn Codec>, Error> {
        build_codec(self.root_pos, self.descriptors)
        .map_err(|e| ProtocolError::with_source(e)
            .context("error decoding input codec"))
    }
    pub fn wrong_type(&self, descriptor: &Descriptor, expected: &str) -> Error {
        DescriptorMismatch::with_message(format!(
            "\nEdgeDB returned unexpected type {descriptor:?}\nClient expected {expected}"
        ))
    }
    pub fn field_number(&self, expected: usize, unexpected: usize) -> Error {
        DescriptorMismatch::with_message(format!(
            "expected {} fields, got {}",
            expected, unexpected))
    }
}

impl<T: ScalarArg> ScalarArg for &T {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        (*self).encode(encoder)
    }

    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        T::check_descriptor(ctx, pos)
    }

    fn to_value(&self) -> Result<Value, Error> {
        (*self).to_value()
    }
}

impl QueryArgs for () {
    fn encode(&self, enc: &mut Encoder) -> Result<(), Error> {
        if enc.ctx.root_pos.is_some() {
            if enc.ctx.proto.is_at_most(0, 11) {
                let root = enc.ctx.root_pos.and_then(|p| enc.ctx.get(p).ok());
                match root {
                    Some(Descriptor::Tuple(t))
                        if t.id == Uuid::from_u128(0xFF) && t.element_types.is_empty() => {}
                    _ => {
                        return Err(ParameterTypeMismatchError::with_message(
                            "query arguments expected",
                        ))
                    }
                };
            } else {
                return Err(ParameterTypeMismatchError::with_message(
                    "query arguments expected",
                ));
            }
        }
        if enc.ctx.proto.is_at_most(0, 11) {
            enc.buf.reserve(4);
            enc.buf.put_u32(0);
        }
        Ok(())
    }
}

impl QueryArg for Value {
    fn encode_slot(&self, enc: &mut Encoder) -> Result<(), Error> {
        use Value::*;
        match self {
            Nothing => {
                enc.buf.reserve(4);
                enc.buf.put_i32(-1);
            }
            Uuid(v) => v.encode_slot(enc)?,
            Str(v) => v.encode_slot(enc)?,
            Bytes(v) => v.encode_slot(enc)?,
            Int16(v) => v.encode_slot(enc)?,
            Int32(v) => v.encode_slot(enc)?,
            Int64(v) => v.encode_slot(enc)?,
            Float32(v) => v.encode_slot(enc)?,
            Float64(v) => v.encode_slot(enc)?,
            BigInt(v) => v.encode_slot(enc)?,
            ConfigMemory(v) => v.encode_slot(enc)?,
            Decimal(v) => v.encode_slot(enc)?,
            Bool(v) => v.encode_slot(enc)?,
            Datetime(v) => v.encode_slot(enc)?,
            LocalDatetime(v) => v.encode_slot(enc)?,
            LocalDate(v) => v.encode_slot(enc)?,
            LocalTime(v) => v.encode_slot(enc)?,
            Duration(v) => v.encode_slot(enc)?,
            RelativeDuration(v) => v.encode_slot(enc)?,
            DateDuration(v) => v.encode_slot(enc)?,
            Json(v) => v.encode_slot(enc)?,
            Set(_) => return Err(ClientEncodingError::with_message(
                    "set cannot be query argument")),
            Object {..} => return Err(ClientEncodingError::with_message(
                    "object cannot be query argument")),
            SparseObject(_) => return Err(ClientEncodingError::with_message(
                    "sparse object cannot be query argument")),
            Tuple(_) => return Err(ClientEncodingError::with_message(
                    "tuple object cannot be query argument")),
            NamedTuple {..} => return Err(ClientEncodingError::with_message(
                    "named tuple object cannot be query argument")),
            Array(v) => v.encode_slot(enc)?,
            Enum(v) => v.encode_slot(enc)?,
            Range(v) => v.encode_slot(enc)?,
            Vector(v) => v.encode_slot(enc)?,
        }

        Ok(())
    }
    fn check_descriptor(&self, ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        use Descriptor::*;
        use Value::*;
        let mut desc = ctx.get(pos)?;
        if let Scalar(d) = desc {
            desc = ctx.get(d.base_type_pos)?;
        }
        match (self, desc) {
            (Nothing, _) => Ok(()), // any descriptor works
            (_, Scalar(_)) => unreachable!("scalar dereference to a non-base type"),
            (BigInt(_), BaseScalar(d)) if d.id == codec::STD_BIGINT => Ok(()),
            (Bool(_), BaseScalar(d)) if d.id == codec::STD_BOOL => Ok(()),
            (Bytes(_), BaseScalar(d)) if d.id == codec::STD_BYTES => Ok(()),
            (ConfigMemory(_), BaseScalar(d)) if d.id == codec::CFG_MEMORY => Ok(()),
            (DateDuration(_), BaseScalar(d)) if d.id == codec::CAL_DATE_DURATION => Ok(()),
            (Datetime(_), BaseScalar(d)) if d.id == codec::STD_DATETIME => Ok(()),
            (Decimal(_), BaseScalar(d)) if d.id == codec::STD_DECIMAL => Ok(()),
            (Duration(_), BaseScalar(d)) if d.id == codec::STD_DURATION => Ok(()),
            (Float32(_), BaseScalar(d)) if d.id == codec::STD_FLOAT32 => Ok(()),
            (Float64(_), BaseScalar(d)) if d.id == codec::STD_FLOAT64 => Ok(()),
            (Int16(_), BaseScalar(d)) if d.id == codec::STD_INT16 => Ok(()),
            (Int32(_), BaseScalar(d)) if d.id == codec::STD_INT32 => Ok(()),
            (Int64(_), BaseScalar(d)) if d.id == codec::STD_INT64 => Ok(()),
            (Json(_), BaseScalar(d)) if d.id == codec::STD_JSON => Ok(()),
            (LocalDate(_), BaseScalar(d)) if d.id == codec::CAL_LOCAL_DATE => Ok(()),
            (LocalDatetime(_), BaseScalar(d)) if d.id == codec::CAL_LOCAL_DATETIME => Ok(()),
            (LocalTime(_), BaseScalar(d)) if d.id == codec::CAL_LOCAL_TIME => Ok(()),
            (RelativeDuration(_), BaseScalar(d)) if d.id == codec::CAL_RELATIVE_DURATION => Ok(()),
            (Str(_), BaseScalar(d)) if d.id == codec::STD_STR => Ok(()),
            (Uuid(_), BaseScalar(d)) if d.id == codec::STD_UUID => Ok(()),
            (Enum(val), Enumeration(EnumerationTypeDescriptor { members, .. })) => {
                let val = val.deref();
                check_enum(val, members)
            }
            // TODO(tailhook) all types
            (_, desc) => Err(ctx.wrong_type(desc, self.kind())),
        }
    }
    fn to_value(&self) -> Result<Value, Error> {
        Ok(self.clone())
    }
}

pub(crate) fn check_enum(variant_name: &str, expected_members: &[String]) -> Result<(), Error> {
    if expected_members.iter().any(|c| c == variant_name) {
        Ok(())
    } else {
        let mut members = expected_members
            .into_iter()
            .map(|c| format!("'{c}'"))
            .collect::<Vec<_>>();
        members.sort_unstable();
        let members = members.join(", ");
        Err(InvalidReferenceError::with_message(format!(
            "Expected one of: {members}, while enum value '{variant_name}' was provided"
        )))
    }
}

impl QueryArgs for Value {
    fn encode(&self, enc: &mut Encoder) -> Result<(), Error> {
        let codec = enc.ctx.build_codec()?;
        codec
            .encode(enc.buf, self)
            .map_err(ClientEncodingError::with_source)
    }
}

impl<T: ScalarArg> QueryArg for T {
    fn encode_slot(&self, enc: &mut Encoder) -> Result<(), Error> {
        enc.buf.reserve(4);
        let pos = enc.buf.len();
        enc.buf.put_u32(0); // will fill after encoding
        ScalarArg::encode(self, enc)?;
        let len = enc.buf.len() - pos - 4;
        enc.buf[pos..pos + 4].copy_from_slice(
            &i32::try_from(len)
                .ok()
                .context(errors::ElementTooLong)
                .map_err(ClientEncodingError::with_source)?
                .to_be_bytes(),
        );
        Ok(())
    }
    fn check_descriptor(&self, ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        T::check_descriptor(ctx, pos)
    }
    fn to_value(&self) -> Result<Value, Error> {
        ScalarArg::to_value(self)
    }
}

impl<T: ScalarArg> QueryArg for Option<T> {
    fn encode_slot(&self, enc: &mut Encoder) -> Result<(), Error> {
        if let Some(val) = self {
            QueryArg::encode_slot(val, enc)
        } else {
            enc.buf.reserve(4);
            enc.buf.put_i32(-1);
            Ok(())
        }
    }
    fn check_descriptor(&self, ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        T::check_descriptor(ctx, pos)
    }
    fn to_value(&self) -> Result<Value, Error> {
        match self.as_ref() {
            Some(v) => ScalarArg::to_value(v),
            None => Ok(Value::Nothing),
        }
    }
}

impl<T: ScalarArg> QueryArg for Vec<T> {
    fn encode_slot(&self, enc: &mut Encoder) -> Result<(), Error> {
        enc.buf.reserve(8);
        enc.length_prefixed(|enc| {
            if self.is_empty() {
                enc.buf.reserve(12);
                enc.buf.put_u32(0); // ndims
                enc.buf.put_u32(0); // reserved0
                enc.buf.put_u32(0); // reserved1
                return Ok(());
            }
            enc.buf.reserve(20);
            enc.buf.put_u32(1); // ndims
            enc.buf.put_u32(0); // reserved0
            enc.buf.put_u32(0); // reserved1
            enc.buf.put_u32(
                self.len()
                    .try_into()
                    .map_err(|_| ClientEncodingError::with_message("array is too long"))?,
            );
            enc.buf.put_u32(1); // lower
            for item in self {
                enc.length_prefixed(|enc| item.encode(enc))?;
            }
            Ok(())
        })
    }
    fn check_descriptor(&self, ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        let desc = ctx.get(pos)?;
        if let Descriptor::Array(arr) = desc {
            T::check_descriptor(ctx, arr.type_pos)
        } else {
            Err(ctx.wrong_type(desc, "array"))
        }
    }
    fn to_value(&self) -> Result<Value, Error> {
        Ok(Value::Array(
            self.iter()
                .map(|v| v.to_value())
                .collect::<Result<_, _>>()?,
        ))
    }
}

impl QueryArg for Vec<Value> {
    fn encode_slot(&self, enc: &mut Encoder) -> Result<(), Error> {
        enc.buf.reserve(8);
        enc.length_prefixed(|enc| {
            if self.is_empty() {
                enc.buf.reserve(12);
                enc.buf.put_u32(0); // ndims
                enc.buf.put_u32(0); // reserved0
                enc.buf.put_u32(0); // reserved1
                return Ok(());
            }
            enc.buf.reserve(20);
            enc.buf.put_u32(1); // ndims
            enc.buf.put_u32(0); // reserved0
            enc.buf.put_u32(0); // reserved1
            enc.buf.put_u32(
                self.len()
                    .try_into()
                    .map_err(|_| ClientEncodingError::with_message("array is too long"))?,
            );
            enc.buf.put_u32(1); // lower
            for item in self {
                enc.length_prefixed(|enc| item.encode(enc))?;
            }
            Ok(())
        })
    }
    fn check_descriptor(&self, ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        let desc = ctx.get(pos)?;
        if let Descriptor::Array(arr) = desc {
            for val in self {
                val.check_descriptor(ctx, arr.type_pos)?
            }
            Ok(())
        } else {
            Err(ctx.wrong_type(desc, "array"))
        }
    }
    fn to_value(&self) -> Result<Value, Error> {
        Ok(Value::Array(
            self.iter()
                .map(|v| v.to_value())
                .collect::<Result<_, _>>()?,
        ))
    }
}

impl QueryArg for range::Range<Box<Value>> {
    fn encode_slot(&self, encoder: &mut Encoder) -> Result<(), Error> {
        encoder.length_prefixed(|encoder| {
            let flags = if self.empty {
                range::EMPTY
            } else {
                (if self.inc_lower { range::LB_INC } else { 0 })
                    | (if self.inc_upper { range::UB_INC } else { 0 })
                    | (if self.lower.is_none() {
                        range::LB_INF
                    } else {
                        0
                    })
                    | (if self.upper.is_none() {
                        range::UB_INF
                    } else {
                        0
                    })
            };
            encoder.buf.reserve(1);
            encoder.buf.put_u8(flags as u8);

            if let Some(lower) = &self.lower {
                encoder.length_prefixed(|encoder| lower.encode(encoder))?
            }

            if let Some(upper) = &self.upper {
                encoder.length_prefixed(|encoder| upper.encode(encoder))?;
            }
            Ok(())
        })
    }
    fn check_descriptor(&self, ctx: &DescriptorContext, pos: TypePos) -> Result<(), Error> {
        let desc = ctx.get(pos)?;
        if let Descriptor::Range(rng) = desc {
            self.lower
                .as_ref()
                .map(|v| v.check_descriptor(ctx, rng.type_pos))
                .transpose()?;
            self.upper
                .as_ref()
                .map(|v| v.check_descriptor(ctx, rng.type_pos))
                .transpose()?;
            Ok(())
        } else {
            Err(ctx.wrong_type(desc, "range"))
        }
    }
    fn to_value(&self) -> Result<Value, Error> {
        Ok(Value::Range(self.clone()))
    }
}

macro_rules! implement_tuple {
    ( $count:expr, $($name:ident,)+ ) => {
        impl<$($name:QueryArg),+> QueryArgs for ($($name,)+) {
            fn encode(&self, enc: &mut Encoder)
                -> Result<(), Error>
            {
                #![allow(non_snake_case)]
                let root_pos = enc.ctx.root_pos
                    .ok_or_else(|| DescriptorMismatch::with_message(
                        format!(
                            "provided {} positional arguments, \
                             but no arguments expected by the server",
                             $count)))?;
                let desc = enc.ctx.get(root_pos)?;
                match desc {
                    Descriptor::ObjectShape(desc)
                    if enc.ctx.proto.is_at_least(0, 12)
                    => {
                        if desc.elements.len() != $count {
                            return Err(enc.ctx.field_number(
                                desc.elements.len(), $count));
                        }
                        let mut els = desc.elements.iter().enumerate();
                        let ($(ref $name,)+) = self;
                        $(
                            let (idx, el) = els.next().unwrap();
                            if el.name.parse() != Ok(idx) {
                                return Err(DescriptorMismatch::with_message(
                                    format!("expected positional arguments, \
                                             got {} instead of {}",
                                             el.name, idx)));
                            }
                            $name.check_descriptor(enc.ctx, el.type_pos)?;
                        )+
                    }
                    Descriptor::Tuple(desc) if enc.ctx.proto.is_at_most(0, 11)
                    => {
                        if desc.element_types.len() != $count {
                            return Err(enc.ctx.field_number(
                                desc.element_types.len(), $count));
                        }
                        let mut els = desc.element_types.iter();
                        let ($(ref $name,)+) = self;
                        $(
                            let type_pos = els.next().unwrap();
                            $name.check_descriptor(enc.ctx, *type_pos)?;
                        )+
                    }
                    _ => return Err(enc.ctx.wrong_type(desc,
                        if enc.ctx.proto.is_at_least(0, 12) { "object" }
                        else { "tuple" }))
                }

                enc.buf.reserve(4 + 8*$count);
                enc.buf.put_u32($count);
                let ($(ref $name,)+) = self;
                $(
                    enc.buf.reserve(8);
                    enc.buf.put_u32(0);
                    QueryArg::encode_slot($name, enc)?;
                )*
                Ok(())
            }
        }
    }
}

implement_tuple! {1, T0, }
implement_tuple! {2, T0, T1, }
implement_tuple! {3, T0, T1, T2, }
implement_tuple! {4, T0, T1, T2, T3, }
implement_tuple! {5, T0, T1, T2, T3, T4, }
implement_tuple! {6, T0, T1, T2, T3, T4, T5, }
implement_tuple! {7, T0, T1, T2, T3, T4, T5, T6, }
implement_tuple! {8, T0, T1, T2, T3, T4, T5, T6, T7, }
implement_tuple! {9, T0, T1, T2, T3, T4, T5, T6, T7, T8, }
implement_tuple! {10, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, }
implement_tuple! {11, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, }
implement_tuple! {12, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, }

/// An optional [Value] that can be constructed from `impl Into<Value>`,
/// `Option<impl Into<Value>>`, `Vec<impl Into<Value>>` or
/// `Option<Vec<impl Into<Value>>>`.
/// Used by [eargs!] macro.
pub struct UserValue(Option<Value>);

impl<V: Into<Value>> From<V> for UserValue {
    fn from(value: V) -> Self {
        UserValue(Some(value.into()))
    }
}
impl<V: Into<Value>> From<Option<V>> for UserValue
where
    Value: From<V>
{
    fn from(value: Option<V>) -> Self {
        UserValue(value.map(Value::from))
    }
}
impl<V: Into<Value>> From<Vec<V>> for UserValue
where
    Value: From<V>
{
    fn from(value: Vec<V>) -> Self {
        UserValue(Some(Value::Array(value.into_iter().map(Value::from).collect())))
    }
}
impl<V: Into<Value>> From<Option<Vec<V>>> for UserValue
where
    Value: From<V>
{
    fn from(value: Option<Vec<V>>) -> Self {
        let mapped = value.map(|value| Value::Array(value.into_iter().map(Value::from).collect()));
        UserValue(mapped)
    }
}
impl From<UserValue> for Option<Value> {
    fn from(value: UserValue) -> Self {
        value.0
    }
}

use std::collections::HashMap;
impl QueryArgs for HashMap<&str, UserValue> {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        if self.len() == 0 && encoder.ctx.root_pos.is_none() {
            return Ok(());
        }

        let target_shape = {
            let root_pos = encoder.ctx.root_pos.ok_or_else(|| {
                let msg = format!(
                    "provided {} positional arguments, but no arguments were expected by the server",
                    self.len()
                );
                ClientEncodingError::with_message(msg)
            })?;
            match encoder.ctx.get(root_pos)? {
                Descriptor::ObjectShape(shape) => shape,
                _ => return Err(ClientEncodingError::with_message("query didn't expect named arguments"))
            }
        };

        let mut mapped_shapes: Vec<ShapeElement> = Vec::new();
        let mut field_values: Vec<Option<Value>> = Vec::new();

        for target_shape in target_shape.elements.iter() {
            let user_value = self.get(target_shape.name.as_str());

            if let Some(value) = user_value {
                // these structs are actually from different crates
                mapped_shapes.push(ShapeElement {
                    name: target_shape.name.clone(),
                    cardinality: target_shape.cardinality,
                    flag_implicit: target_shape.flag_implicit,
                    flag_link: target_shape.flag_link,
                    flag_link_property: target_shape.flag_link_property
                });

                field_values.push(value.0.clone());
                continue;
            }

            let error_message = format!("argument for {} missing", target_shape.name);
            return Err(ClientEncodingError::with_message(error_message));
        }

        Value::Object {
            shape: ObjectShape::new(mapped_shapes),
            fields: field_values
        }
        .encode(encoder)
    }
}

#[macro_export]
macro_rules! eargs {
    ($($key:expr => $value:expr,)+) => { $crate::eargs!($($key => $value),+) };
    ($($key:expr => $value:expr),*) => {
        {
            const CAP: usize = <[()]>::len(&[$({ stringify!($key); }),*]);
            let mut map = std::collections::HashMap::with_capacity(CAP);
            $(
                map.insert($key, $crate::query_arg::UserValue::from($value));
            )*
            map
        }
    };
}
