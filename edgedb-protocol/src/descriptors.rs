/*!
([Website reference](https://www.edgedb.com/docs/reference/protocol/typedesc)) Types for the [Descriptor] enum.

```rust,ignore
pub enum Descriptor {
    Set(SetDescriptor),
    ObjectShape(ObjectShapeDescriptor),
    BaseScalar(BaseScalarTypeDescriptor),
    Scalar(ScalarTypeDescriptor),
    Tuple(TupleTypeDescriptor),
    NamedTuple(NamedTupleTypeDescriptor),
    Array(ArrayTypeDescriptor),
    Enumeration(EnumerationTypeDescriptor),
    InputShape(InputShapeTypeDescriptor),
    Range(RangeTypeDescriptor),
    MultiRange(MultiRangeTypeDescriptor),
    TypeAnnotation(TypeAnnotationDescriptor),
}
```

From the website:

>The type descriptor is essentially a list of type information blocks:
>* each block encodes one type;
>* blocks can reference other blocks.

>While parsing the _blocks_, a database driver can assemble an _encoder_ or a _decoder_ of the EdgeDB binary data.

>An _encoder_ is used to encode objects, native to the driver’s runtime, to binary data that EdegDB can decode and work with.

>A _decoder_ is used to decode data from EdgeDB native format to data types native to the driver.
*/

use std::collections::{BTreeMap, BTreeSet};
use std::convert::{TryFrom, TryInto};
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use edgedb_errors::{ClientEncodingError, DescriptorMismatch, Error, ErrorKind};
use snafu::{ensure, OptionExt};
use uuid::Uuid;

use crate::codec::{build_codec, uuid_to_known_name, Codec};
use crate::common::{Cardinality, State};
use crate::encoding::{Decode, Input};
use crate::errors::{self, CodecError, DecodeError};
use crate::errors::{InvalidTypeDescriptor, UnexpectedTypePos};
use crate::features::ProtocolVersion;
use crate::query_arg::{self, Encoder, QueryArg};
use crate::queryable;
use crate::value::Value;

pub use crate::common::RawTypedesc;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TypePos(pub u16);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Descriptor {
    Set(SetDescriptor),
    ObjectShape(ObjectShapeDescriptor),
    BaseScalar(BaseScalarTypeDescriptor),
    Scalar(ScalarTypeDescriptor),
    Tuple(TupleTypeDescriptor),
    NamedTuple(NamedTupleTypeDescriptor),
    Array(ArrayTypeDescriptor),
    Enumeration(EnumerationTypeDescriptor),
    InputShape(InputShapeTypeDescriptor),
    Range(RangeTypeDescriptor),
    MultiRange(MultiRangeTypeDescriptor),
    Object(ObjectTypeDescriptor),
    Compound(CompoundTypeDescriptor),
    TypeAnnotation(TypeAnnotationDescriptor),
}

#[derive(Clone, PartialEq, Eq)]
pub struct DescriptorUuid(Uuid);

impl Debug for DescriptorUuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match uuid_to_known_name(&self.0) {
            Some(known_name) => write!(f, "{known_name}"),
            None => write!(f, "{}", &self.0),
        }
    }
}

impl Deref for DescriptorUuid {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Uuid> for DescriptorUuid {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl PartialEq<Uuid> for DescriptorUuid {
    fn eq(&self, other: &Uuid) -> bool {
        self.0 == *other
    }
}

#[derive(Debug)]
pub struct Typedesc {
    pub(crate) proto: ProtocolVersion,
    pub(crate) array: Vec<Descriptor>,
    pub(crate) root_id: Uuid,
    pub(crate) root_pos: Option<TypePos>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetDescriptor {
    pub id: DescriptorUuid,
    pub type_pos: TypePos,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectShapeDescriptor {
    pub id: DescriptorUuid,
    pub ephemeral_free_shape: bool,
    pub type_pos: Option<TypePos>,
    pub elements: Vec<ShapeElement>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputShapeTypeDescriptor {
    pub id: DescriptorUuid,
    pub elements: Vec<InputShapeElement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShapeElement {
    pub flag_implicit: bool,
    pub flag_link_property: bool,
    pub flag_link: bool,
    pub cardinality: Option<Cardinality>,
    pub name: String,
    pub type_pos: TypePos,
    pub source_type_pos: Option<TypePos>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputShapeElement {
    pub cardinality: Option<Cardinality>,
    pub name: String,
    pub type_pos: TypePos,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaseScalarTypeDescriptor {
    pub id: DescriptorUuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScalarTypeDescriptor {
    pub id: DescriptorUuid,
    pub base_type_pos: Option<TypePos>,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
    pub ancestors: Vec<TypePos>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TupleTypeDescriptor {
    pub id: DescriptorUuid,
    pub element_types: Vec<TypePos>,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
    pub ancestors: Vec<TypePos>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedTupleTypeDescriptor {
    pub id: DescriptorUuid,
    pub elements: Vec<TupleElement>,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
    pub ancestors: Vec<TypePos>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectTypeDescriptor {
    pub id: DescriptorUuid,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeOperation {
    UNION = 1,
    INTERSECTION = 2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompoundTypeDescriptor {
    pub id: DescriptorUuid,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
    pub op: TypeOperation,
    pub components: Vec<TypePos>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleElement {
    pub name: String,
    pub type_pos: TypePos,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArrayTypeDescriptor {
    pub id: DescriptorUuid,
    pub type_pos: TypePos,
    pub dimensions: Vec<Option<u32>>,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
    pub ancestors: Vec<TypePos>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeTypeDescriptor {
    pub id: DescriptorUuid,
    pub type_pos: TypePos,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
    pub ancestors: Vec<TypePos>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiRangeTypeDescriptor {
    pub id: DescriptorUuid,
    pub type_pos: TypePos,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
    pub ancestors: Vec<TypePos>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumerationTypeDescriptor {
    pub id: DescriptorUuid,
    pub members: Vec<String>,
    pub name: Option<String>,
    pub schema_defined: Option<bool>,
    pub ancestors: Vec<TypePos>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeAnnotationDescriptor {
    pub annotated_type: u8,
    pub id: DescriptorUuid,
    pub annotation: String,
}

pub struct StateBorrow<'a> {
    pub module: &'a Option<String>,
    pub aliases: &'a BTreeMap<String, String>,
    pub config: &'a BTreeMap<String, Value>,
    pub globals: &'a BTreeMap<String, Value>,
}

impl Typedesc {
    pub fn id(&self) -> &Uuid {
        &self.root_id
    }
    pub fn descriptors(&self) -> &[Descriptor] {
        &self.array
    }
    pub fn root_pos(&self) -> Option<TypePos> {
        self.root_pos
    }
    pub fn build_codec(&self) -> Result<Arc<dyn Codec>, CodecError> {
        build_codec(self.root_pos(), self.descriptors())
    }
    pub fn get(&self, type_pos: TypePos) -> Result<&Descriptor, CodecError> {
        self.array
            .get(type_pos.0 as usize)
            .context(UnexpectedTypePos {
                position: type_pos.0,
            })
    }
    pub fn nothing(protocol: &ProtocolVersion) -> Typedesc {
        Typedesc {
            proto: protocol.clone(),
            array: Vec::new(),
            root_id: Uuid::from_u128(0),
            root_pos: None,
        }
    }
    pub fn is_empty_tuple(&self) -> bool {
        match self.root() {
            Some(Descriptor::Tuple(t)) => {
                *t.id == Uuid::from_u128(0xFF) && t.element_types.is_empty()
            }
            _ => false,
        }
    }
    pub fn root(&self) -> Option<&Descriptor> {
        self.root_pos.and_then(|pos| self.array.get(pos.0 as usize))
    }
    pub(crate) fn decode_with_id(root_id: Uuid, buf: &mut Input) -> Result<Self, DecodeError> {
        let mut descriptors = Vec::new();
        while buf.remaining() > 0 {
            match Descriptor::decode(buf)? {
                Descriptor::TypeAnnotation(_) => {}
                item => descriptors.push(item),
            }
        }
        let root_pos = if root_id == Uuid::from_u128(0) {
            None
        } else {
            let idx = descriptors
                .iter()
                .position(|x| *x.id() == root_id)
                .context(errors::UuidNotFound { uuid: root_id })?;
            let pos = idx
                .try_into()
                .ok()
                .context(errors::TooManyDescriptors { index: idx })?;
            Some(TypePos(pos))
        };
        Ok(Typedesc {
            proto: buf.proto().clone(),
            array: descriptors,
            root_id,
            root_pos,
        })
    }
    pub fn as_query_arg_context(&self) -> query_arg::DescriptorContext {
        query_arg::DescriptorContext {
            proto: &self.proto,
            descriptors: self.descriptors(),
            root_pos: self.root_pos,
        }
    }
    pub fn as_queryable_context(&self) -> queryable::DescriptorContext {
        let mut ctx = queryable::DescriptorContext::new(self.descriptors());
        ctx.has_implicit_id = self.proto.has_implicit_id();
        ctx.has_implicit_tid = self.proto.has_implicit_tid();
        ctx
    }
    pub fn serialize_state(&self, state: &StateBorrow) -> Result<State, Error> {
        #[derive(Debug)]
        struct Indices {
            module: (u32, TypePos),
            aliases: (u32, TypePos),
            config: (u32, TypePos),
            globals: (u32, TypePos),
        }
        let mut buf = BytesMut::with_capacity(128);
        let ctx = self.as_query_arg_context();
        let mut enc = Encoder::new(&ctx, &mut buf);

        let root = enc
            .ctx
            .root_pos
            .ok_or_else(|| DescriptorMismatch::with_message("invalid state descriptor"))
            .and_then(|p| enc.ctx.get(p))?;
        let indices = match root {
            Descriptor::InputShape(desc) => {
                let mut module = None;
                let mut aliases = None;
                let mut config = None;
                let mut globals = None;
                for (i, elem) in desc.elements.iter().enumerate() {
                    let i = i as u32;
                    match &elem.name[..] {
                        "module" => module = Some((i, elem.type_pos)),
                        "aliases" => aliases = Some((i, elem.type_pos)),
                        "config" => config = Some((i, elem.type_pos)),
                        "globals" => globals = Some((i, elem.type_pos)),
                        _ => {}
                    }
                }
                Indices {
                    module: module.ok_or_else(|| {
                        DescriptorMismatch::with_message("no `module` field in state")
                    })?,
                    aliases: aliases.ok_or_else(|| {
                        DescriptorMismatch::with_message("no `aliases` field in state")
                    })?,
                    config: config.ok_or_else(|| {
                        DescriptorMismatch::with_message("no `config` field in state")
                    })?,
                    globals: globals.ok_or_else(|| {
                        DescriptorMismatch::with_message("no `globals` field in state")
                    })?,
                }
            }
            _ => return Err(DescriptorMismatch::with_message("invalid state descriptor")),
        };

        enc.buf.reserve(4 + 8 * 4);
        enc.buf.put_u32(4);

        let module = state.module.as_deref().unwrap_or("default");
        module.check_descriptor(enc.ctx, indices.module.1)?;

        enc.buf.reserve(8);
        enc.buf.put_u32(indices.module.0);
        module.encode_slot(&mut enc)?;

        match enc.ctx.get(indices.aliases.1)? {
            Descriptor::Array(arr) => match enc.ctx.get(arr.type_pos)? {
                Descriptor::Tuple(tup) => {
                    if tup.element_types.len() != 2 {
                        return Err(DescriptorMismatch::with_message(
                            "invalid type descriptor for aliases",
                        ));
                    }
                    "".check_descriptor(enc.ctx, tup.element_types[0])?;
                    "".check_descriptor(enc.ctx, tup.element_types[1])?;
                }
                _ => {
                    return Err(DescriptorMismatch::with_message(
                        "invalid type descriptor for aliases",
                    ));
                }
            },
            _ => {
                return Err(DescriptorMismatch::with_message(
                    "invalid type descriptor for aliases",
                ));
            }
        }

        enc.buf
            .reserve(4 + 16 + state.aliases.len() * (4 + (8 + 4) * 2));
        enc.buf.put_u32(indices.aliases.0);
        enc.length_prefixed(|enc| {
            enc.buf.put_u32(
                state
                    .aliases
                    .len()
                    .try_into()
                    .map_err(|_| ClientEncodingError::with_message("too many aliases"))?,
            );
            for (key, value) in state.aliases {
                enc.length_prefixed(|enc| {
                    enc.buf.reserve(4 + (8 + 4) * 2);
                    enc.buf.put_u32(2);
                    enc.buf.put_u32(0); // reserved

                    key.encode_slot(enc)?;
                    value.encode_slot(enc)?;
                    Ok(())
                })?;
            }
            Ok(())
        })?;

        enc.buf.reserve(4);
        enc.buf.put_u32(indices.config.0);
        enc.length_prefixed(|enc| {
            serialize_variables(enc, state.config, indices.config.1, "config")
        })?;
        enc.buf.reserve(4);
        enc.buf.put_u32(indices.globals.0);
        enc.length_prefixed(|enc| {
            serialize_variables(enc, state.globals, indices.globals.1, "globals")
        })?;
        let data = buf.freeze();
        Ok(State {
            typedesc_id: self.root_id,
            data,
        })
    }
    pub fn proto(&self) -> &ProtocolVersion {
        &self.proto
    }
}

fn serialize_variables(
    enc: &mut Encoder,
    variables: &BTreeMap<String, Value>,

    type_pos: TypePos,
    tag: &str,
) -> Result<(), Error> {
    enc.buf.reserve(4 + variables.len() * (4 + 4));
    enc.buf.put_u32(
        variables
            .len()
            .try_into()
            .map_err(|_| ClientEncodingError::with_message(format!("too many items in {}", tag)))?,
    );

    let desc = match enc.ctx.get(type_pos)? {
        Descriptor::InputShape(desc) => desc,
        _ => {
            return Err(DescriptorMismatch::with_message(format!(
                "invalid type descriptor for {}",
                tag
            )));
        }
    };

    let mut serialized = 0;
    for (idx, el) in desc.elements.iter().enumerate() {
        if let Some(value) = variables.get(&el.name) {
            value.check_descriptor(enc.ctx, el.type_pos)?;
            serialized += 1;
            enc.buf.reserve(8);
            enc.buf.put_u32(idx as u32);
            value.encode_slot(enc)?;
        }
    }

    if serialized != variables.len() {
        let mut extra_vars = variables.keys().collect::<BTreeSet<_>>();
        for el in &desc.elements {
            extra_vars.remove(&el.name);
        }
        return Err(ClientEncodingError::with_message(format!(
            "non-existing entries {} of {}",
            extra_vars
                .into_iter()
                .map(|x| &x[..])
                .collect::<Vec<_>>()
                .join(", "),
            tag
        )));
    }

    Ok(())
}

impl Descriptor {
    pub fn id(&self) -> &Uuid {
        use Descriptor::*;
        match self {
            Set(i) => &i.id,
            ObjectShape(i) => &i.id,
            BaseScalar(i) => &i.id,
            Scalar(i) => &i.id,
            Tuple(i) => &i.id,
            NamedTuple(i) => &i.id,
            Array(i) => &i.id,
            Range(i) => &i.id,
            MultiRange(i) => &i.id,
            Enumeration(i) => &i.id,
            InputShape(i) => &i.id,
            Object(i) => &i.id,
            Compound(i) => &i.id,
            TypeAnnotation(i) => &i.id,
        }
    }
    pub fn decode(buf: &mut Input) -> Result<Descriptor, DecodeError> {
        <Descriptor as Decode>::decode(buf)
    }
    pub fn normalize_to_base(
        &self,
        ctx: &query_arg::DescriptorContext,
    ) -> Result<Descriptor, Error> {
        let norm = match self {
            Descriptor::Scalar(d) if d.base_type_pos.is_some() => {
                match ctx.get(d.base_type_pos.unwrap())? {
                    Descriptor::Scalar(d) => {
                        Descriptor::BaseScalar(BaseScalarTypeDescriptor { id: d.id.clone() })
                    }
                    desc => desc.clone(),
                }
            }
            Descriptor::Scalar(d) => {
                if ctx.proto.is_2() {
                    Descriptor::BaseScalar(BaseScalarTypeDescriptor { id: d.id.clone() })
                } else {
                    unreachable!("scalar dereference to a non-base type")
                }
            }
            desc => desc.clone(),
        };

        Ok(norm)
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let element_count = buf.get_u16();
        let mut elements = Vec::with_capacity(element_count as usize);
        for _ in 0..element_count {
            elements.push(T::decode(buf)?);
        }
        Ok(elements)
    }
}

impl Decode for Option<u32> {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);

        let val = match buf.get_i32() {
            -1 => None,
            n if n > 0 => Some(n as u32),
            _ => errors::InvalidOptionU32.fail()?,
        };

        Ok(val)
    }
}

impl Decode for TypePos {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 2, errors::Underflow);
        Ok(Self(buf.get_u16()))
    }
}

impl Decode for Descriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        use Descriptor as D;
        if buf.proto().is_2() {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            let desc_len = buf.get_u32() as u64;
            ensure!((buf.remaining() as u64) >= desc_len, errors::Underflow);
        }
        ensure!(buf.remaining() >= 1, errors::Underflow);
        match buf.chunk()[0] {
            0x00 => SetDescriptor::decode(buf).map(D::Set),
            0x01 => ObjectShapeDescriptor::decode(buf).map(D::ObjectShape),
            0x02 => BaseScalarTypeDescriptor::decode(buf).map(D::BaseScalar),
            0x03 => ScalarTypeDescriptor::decode(buf).map(D::Scalar),
            0x04 => TupleTypeDescriptor::decode(buf).map(D::Tuple),
            0x05 => NamedTupleTypeDescriptor::decode(buf).map(D::NamedTuple),
            0x06 => ArrayTypeDescriptor::decode(buf).map(D::Array),
            0x07 => EnumerationTypeDescriptor::decode(buf).map(D::Enumeration),
            0x08 => InputShapeTypeDescriptor::decode(buf).map(D::InputShape),
            0x09 => RangeTypeDescriptor::decode(buf).map(D::Range),
            0x0A => ObjectTypeDescriptor::decode(buf).map(D::Object),
            0x0B => CompoundTypeDescriptor::decode(buf).map(D::Compound),
            0x0C => MultiRangeTypeDescriptor::decode(buf).map(D::MultiRange),
            0x7F..=0xFF => TypeAnnotationDescriptor::decode(buf).map(D::TypeAnnotation),
            descriptor => InvalidTypeDescriptor { descriptor }.fail()?,
        }
    }
}

impl Decode for SetDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 0);
        let id = Uuid::decode(buf)?.into();
        let type_pos = TypePos(buf.get_u16());
        Ok(SetDescriptor { id, type_pos })
    }
}

impl Decode for ObjectShapeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 1);
        let id = Uuid::decode(buf)?.into();
        let type_desc = if buf.proto().is_2() {
            let ephemeral_free_shape = bool::decode(buf)?;
            let type_pos = Some(TypePos::decode(buf)?);
            let elements = Vec::<ShapeElement>::decode(buf)?;
            ObjectShapeDescriptor {
                id,
                elements,
                ephemeral_free_shape,
                type_pos,
            }
        } else {
            let elements = Vec::<ShapeElement>::decode(buf)?;
            ObjectShapeDescriptor {
                id,
                elements,
                ephemeral_free_shape: false,
                type_pos: None,
            }
        };
        Ok(type_desc)
    }
}

impl Decode for ShapeElement {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 7, errors::Underflow);
        let (flags, cardinality) = if buf.proto().is_at_least(0, 11) {
            let flags = buf.get_u32();
            let cardinality = TryFrom::try_from(buf.get_u8())?;
            (flags, Some(cardinality))
        } else {
            (buf.get_u8() as u32, None)
        };
        let name = String::decode(buf)?;
        let type_pos = TypePos::decode(buf)?;
        let source_type_pos = if buf.proto().is_2() {
            Some(TypePos::decode(buf)?)
        } else {
            None
        };
        Ok(ShapeElement {
            flag_implicit: flags & 0b001 != 0,
            flag_link_property: flags & 0b010 != 0,
            flag_link: flags & 0b100 != 0,
            cardinality,
            name,
            type_pos,
            source_type_pos,
        })
    }
}

impl Decode for InputShapeTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 8);
        let id = Uuid::decode(buf)?.into();
        let elements = Vec::<InputShapeElement>::decode(buf)?;
        Ok(InputShapeTypeDescriptor { id, elements })
    }
}

impl Decode for InputShapeElement {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 7, errors::Underflow);
        let _flags = buf.get_u32();
        let cardinality = Some(TryFrom::try_from(buf.get_u8())?);
        let name = String::decode(buf)?;
        let type_pos = TypePos::decode(buf)?;
        Ok(InputShapeElement {
            cardinality,
            name,
            type_pos,
        })
    }
}

impl Decode for BaseScalarTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        let desc_byte = buf.get_u8();
        assert!(desc_byte == 2);
        ensure!(
            !buf.proto().is_2(),
            InvalidTypeDescriptor {
                descriptor: desc_byte
            }
        );
        let id = Uuid::decode(buf)?.into();
        Ok(BaseScalarTypeDescriptor { id })
    }
}

impl Decode for ObjectTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 0x0A);
        let id = Uuid::decode(buf)?.into();
        let name = Some(String::decode(buf)?);
        let schema_defined = Some(bool::decode(buf)?);
        let type_desc = ObjectTypeDescriptor {
            id,
            name,
            schema_defined,
        };
        Ok(type_desc)
    }
}

impl Decode for TypeOperation {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let val = match buf.get_u8() {
            0x00 => TypeOperation::UNION,
            0x01 => TypeOperation::INTERSECTION,
            _ => errors::InvalidTypeOperation.fail()?,
        };
        Ok(val)
    }
}

impl Decode for CompoundTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 0x0B);
        let id = Uuid::decode(buf)?.into();
        let name = Some(String::decode(buf)?);
        let schema_defined = Some(bool::decode(buf)?);
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let op = TypeOperation::decode(buf)?;
        let components = Vec::<TypePos>::decode(buf)?;
        let type_desc = CompoundTypeDescriptor {
            id,
            name,
            schema_defined,
            op,
            components,
        };
        Ok(type_desc)
    }
}

impl Decode for ScalarTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 3);
        let id = Uuid::decode(buf)?.into();
        let type_desc = if buf.proto().is_2() {
            let name = Some(String::decode(buf)?);
            let schema_defined = Some(bool::decode(buf)?);
            let ancestors = Vec::<TypePos>::decode(buf)?;
            let base_type_pos = ancestors.last().copied();
            ScalarTypeDescriptor {
                id,
                base_type_pos,
                name,
                schema_defined,
                ancestors,
            }
        } else {
            let base_type_pos = Some(TypePos(buf.get_u16()));
            ScalarTypeDescriptor {
                id,
                base_type_pos,
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }
        };
        Ok(type_desc)
    }
}

impl Decode for TupleTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 4);
        let id = Uuid::decode(buf)?.into();

        let type_desc = if buf.proto().is_2() {
            let name = Some(String::decode(buf)?);
            let schema_defined = Some(bool::decode(buf)?);
            let ancestors = Vec::<TypePos>::decode(buf)?;
            let element_types = Vec::<TypePos>::decode(buf)?;
            TupleTypeDescriptor {
                id,
                element_types,
                name,
                schema_defined,
                ancestors,
            }
        } else {
            let element_types = Vec::<TypePos>::decode(buf)?;
            TupleTypeDescriptor {
                id,
                element_types,
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }
        };
        Ok(type_desc)
    }
}

impl Decode for NamedTupleTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 5);
        let id = Uuid::decode(buf)?.into();

        let type_desc = if buf.proto().is_2() {
            let name = Some(String::decode(buf)?);
            let schema_defined = Some(bool::decode(buf)?);
            let ancestors = Vec::<TypePos>::decode(buf)?;
            let elements = Vec::<TupleElement>::decode(buf)?;
            NamedTupleTypeDescriptor {
                id,
                elements,
                name,
                schema_defined,
                ancestors,
            }
        } else {
            let elements = Vec::<TupleElement>::decode(buf)?;
            NamedTupleTypeDescriptor {
                id,
                elements,
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }
        };

        Ok(type_desc)
    }
}

impl Decode for TupleElement {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        let name = String::decode(buf)?;
        let type_pos = TypePos::decode(buf)?;
        Ok(TupleElement { name, type_pos })
    }
}

impl Decode for ArrayTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 21, errors::Underflow);
        assert!(buf.get_u8() == 6);
        let id = Uuid::decode(buf)?.into();
        let type_desc = if buf.proto().is_2() {
            let name = Some(String::decode(buf)?);
            let schema_defined = Some(bool::decode(buf)?);
            let ancestors = Vec::<TypePos>::decode(buf)?;
            let type_pos = TypePos::decode(buf)?;
            let dimensions = Vec::<Option<u32>>::decode(buf)?;
            ArrayTypeDescriptor {
                id,
                type_pos,
                dimensions,
                name,
                schema_defined,
                ancestors,
            }
        } else {
            let type_pos = TypePos::decode(buf)?;
            let dimensions = Vec::<Option<u32>>::decode(buf)?;
            ArrayTypeDescriptor {
                id,
                type_pos,
                dimensions,
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }
        };

        Ok(type_desc)
    }
}

impl Decode for RangeTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 9);
        let id = Uuid::decode(buf)?.into();
        let type_desc = if buf.proto().is_2() {
            let name = Some(String::decode(buf)?);
            let schema_defined = Some(bool::decode(buf)?);
            let ancestors = Vec::<TypePos>::decode(buf)?;
            let type_pos = TypePos::decode(buf)?;
            RangeTypeDescriptor {
                id,
                type_pos,
                name,
                schema_defined,
                ancestors,
            }
        } else {
            let type_pos = TypePos::decode(buf)?;
            RangeTypeDescriptor {
                id,
                type_pos,
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }
        };

        Ok(type_desc)
    }
}

impl Decode for MultiRangeTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 0x0C);
        let id = Uuid::decode(buf)?.into();
        let type_desc = if buf.proto().is_2() {
            let name = Some(String::decode(buf)?);
            let schema_defined = Some(bool::decode(buf)?);
            let ancestors = Vec::<TypePos>::decode(buf)?;
            let type_pos = TypePos::decode(buf)?;
            MultiRangeTypeDescriptor {
                id,
                type_pos,
                name,
                schema_defined,
                ancestors,
            }
        } else {
            let type_pos = TypePos::decode(buf)?;
            MultiRangeTypeDescriptor {
                id,
                type_pos,
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }
        };

        Ok(type_desc)
    }
}

impl Decode for EnumerationTypeDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 7);
        let id = Uuid::decode(buf)?.into();
        let type_desc = if buf.proto().is_2() {
            let name = Some(String::decode(buf)?);
            let schema_defined = Some(bool::decode(buf)?);
            let ancestors = Vec::<TypePos>::decode(buf)?;
            let members = Vec::<String>::decode(buf)?;
            EnumerationTypeDescriptor {
                id,
                members,
                name,
                schema_defined,
                ancestors,
            }
        } else {
            let members = Vec::<String>::decode(buf)?;
            EnumerationTypeDescriptor {
                id,
                members,
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }
        };

        Ok(type_desc)
    }
}

impl Decode for TypeAnnotationDescriptor {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 21, errors::Underflow);
        let annotated_type = buf.get_u8();
        assert!(annotated_type >= 0x7F);
        let id = Uuid::decode(buf)?.into();
        let annotation = String::decode(buf)?;
        Ok(TypeAnnotationDescriptor {
            annotated_type,
            id,
            annotation,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::descriptors::{
        BaseScalarTypeDescriptor, Descriptor, DescriptorUuid, SetDescriptor, TypePos,
    };
    use uuid::Uuid;

    #[test]
    fn descriptor_uuid_debug_outputs() {
        let float_32: Uuid = "00000000-0000-0000-0000-000000000106".parse().unwrap();
        let descriptor_id = DescriptorUuid::from(float_32);
        assert_eq!(format!("{descriptor_id:?}"), "BaseScalar(float32)");

        let random_uuid: Uuid = "7cc7e050-ef76-4ae9-b8a6-053ca9baa3d5".parse().unwrap();
        let descriptor_id = DescriptorUuid::from(random_uuid);
        assert_eq!(
            format!("{descriptor_id:?}"),
            "7cc7e050-ef76-4ae9-b8a6-053ca9baa3d5"
        );

        let base_scalar = Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000106"
                .parse::<Uuid>()
                .unwrap()
                .into(),
        });
        assert_eq!(
            format!("{base_scalar:?}"),
            "BaseScalar(BaseScalarTypeDescriptor { id: BaseScalar(float32) })"
        );

        let set_descriptor_with_float32 = Descriptor::Set(SetDescriptor {
            id: "00000000-0000-0000-0000-000000000106"
                .parse::<Uuid>()
                .unwrap()
                .into(),
            type_pos: TypePos(0),
        });
        assert_eq!(
            format!("{set_descriptor_with_float32:?}"),
            "Set(SetDescriptor { id: BaseScalar(float32), type_pos: TypePos(0) })"
        );
    }
}
