use std::io::Cursor;

use bytes::{Bytes, Buf};
use uuid::Uuid;
use snafu::ensure;

use crate::encoding::{Decode};
use crate::errors::{self, DecodeError};
use crate::errors::{InvalidTypeDescriptor};


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
    TypeAnnotation(TypeAnnotationDescriptor),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetDescriptor {
    pub id: Uuid,
    pub type_pos: TypePos,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectShapeDescriptor {
    pub id: Uuid,
    pub elements: Vec<ShapeElement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShapeElement {
    pub flag_implicit: bool,
    pub flag_link_property: bool,
    pub flag_link: bool,
    pub name: String,
    pub type_pos: TypePos,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseScalarTypeDescriptor {
    pub id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarTypeDescriptor {
    pub id: Uuid,
    pub base_type_pos: TypePos,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleTypeDescriptor {
    pub id: Uuid,
    pub element_types: Vec<TypePos>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedTupleTypeDescriptor {
    pub id: Uuid,
    pub elements: Vec<TupleElement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleElement {
    pub name: String,
    pub type_pos: TypePos,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayTypeDescriptor {
    pub id: Uuid,
    pub type_pos: TypePos,
    pub dimensions: Vec<Option<u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumerationTypeDescriptor {
    pub id: Uuid,
    pub members: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeAnnotationDescriptor {
    pub annotated_type: u8,
    pub id: Uuid,
    pub annotation: String,
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
            Enumeration(i) => &i.id,
            TypeAnnotation(i) => &i.id,
        }
    }
    pub fn decode(buf: &mut Cursor<Bytes>) -> Result<Descriptor, DecodeError> {
        <Descriptor as Decode>::decode(buf)
    }
}

impl Decode for Descriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        use Descriptor as D;
        ensure!(buf.remaining() >= 1, errors::Underflow);
        match buf.bytes()[0] {
            0 => SetDescriptor::decode(buf).map(D::Set),
            1 => ObjectShapeDescriptor::decode(buf).map(D::ObjectShape),
            2 => BaseScalarTypeDescriptor::decode(buf).map(D::BaseScalar),
            3 => ScalarTypeDescriptor::decode(buf).map(D::Scalar),
            4 => TupleTypeDescriptor::decode(buf).map(D::Tuple),
            5 => NamedTupleTypeDescriptor::decode(buf).map(D::NamedTuple),
            6 => ArrayTypeDescriptor::decode(buf).map(D::Array),
            7 => EnumerationTypeDescriptor::decode(buf).map(D::Enumeration),
            0xF0..=0xFF => {
                TypeAnnotationDescriptor::decode(buf).map(D::TypeAnnotation)
            }
            descriptor => InvalidTypeDescriptor { descriptor }.fail()?
        }
    }
}

impl Decode for SetDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 0);
        let id = Uuid::decode(buf)?;
        let type_pos = TypePos(buf.get_u16_be());
        Ok(SetDescriptor { id, type_pos })
    }
}

impl Decode for ObjectShapeDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 1);
        let id = Uuid::decode(buf)?;
        let element_count = buf.get_u16_be();
        let mut elements = Vec::with_capacity(element_count as usize);
        for _ in 0..element_count {
            elements.push(ShapeElement::decode(buf)?);
        }
        Ok(ObjectShapeDescriptor { id, elements })
    }
}

impl Decode for ShapeElement {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 7, errors::Underflow);
        let flags = buf.get_u8();
        let name = String::decode(buf)?;
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let type_pos = TypePos(buf.get_u16_be());
        Ok(ShapeElement {
            flag_implicit: flags & 0b001 != 0,
            flag_link_property: flags & 0b010 != 0,
            flag_link: flags & 0b100 != 0,
            name,
            type_pos,
        })
    }
}

impl Decode for BaseScalarTypeDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        assert!(buf.get_u8() == 2);
        let id = Uuid::decode(buf)?;
        Ok(BaseScalarTypeDescriptor { id })
    }
}


impl Decode for ScalarTypeDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 3);
        let id = Uuid::decode(buf)?;
        let base_type_pos = TypePos(buf.get_u16_be());
        Ok(ScalarTypeDescriptor { id, base_type_pos })
    }
}

impl Decode for TupleTypeDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 4);
        let id = Uuid::decode(buf)?;
        let el_count = buf.get_u16_be();
        ensure!(buf.remaining() >= 2*el_count as usize, errors::Underflow);
        let mut element_types = Vec::with_capacity(el_count as usize);
        for _ in 0..el_count {
            element_types.push(TypePos(buf.get_u16_be()));
        }
        Ok(TupleTypeDescriptor { id, element_types })
    }
}

impl Decode for NamedTupleTypeDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 5);
        let id = Uuid::decode(buf)?;
        let element_count = buf.get_u16_be();
        let mut elements = Vec::with_capacity(element_count as usize);
        for _ in 0..element_count {
            elements.push(TupleElement::decode(buf)?);
        }
        Ok(NamedTupleTypeDescriptor { id, elements })
    }
}

impl Decode for TupleElement {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        let name = String::decode(buf)?;
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let type_pos = TypePos(buf.get_u16_be());
        Ok(TupleElement {
            name,
            type_pos,
        })
    }
}

impl Decode for ArrayTypeDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 21, errors::Underflow);
        assert!(buf.get_u8() == 6);
        let id = Uuid::decode(buf)?;
        let type_pos = TypePos(buf.get_u16_be());
        let dim_count = buf.get_u16_be();
        ensure!(buf.remaining() >= 4*dim_count as usize, errors::Underflow);
        let mut dimensions = Vec::with_capacity(dim_count as usize);
        for _ in 0..dim_count {
            dimensions.push(match buf.get_i32_be() {
                -1 => None,
                n if n > 0 => Some(n as u32),
                _ => errors::InvalidArrayShape.fail()?,
            });
        }
        Ok(ArrayTypeDescriptor { id, type_pos, dimensions })
    }
}

impl Decode for EnumerationTypeDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 19, errors::Underflow);
        assert!(buf.get_u8() == 7);
        let id = Uuid::decode(buf)?;
        let member_count = buf.get_u16_be();
        let mut members = Vec::with_capacity(member_count as usize);
        for _ in 0..member_count {
            members.push(String::decode(buf)?);
        }
        Ok(EnumerationTypeDescriptor { id, members })
    }
}

impl Decode for TypeAnnotationDescriptor {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 21, errors::Underflow);
        let annotated_type = buf.get_u8();
        assert!(annotated_type >= 0xF0);
        let id = Uuid::decode(buf)?;
        let annotation = String::decode(buf)?;
        Ok(TypeAnnotationDescriptor { annotated_type, id, annotation })
    }
}
