use bytes::{Buf, Bytes};
use std::error::Error;

use edgedb_protocol::descriptors::BaseScalarTypeDescriptor;
use edgedb_protocol::descriptors::TupleTypeDescriptor;
use edgedb_protocol::descriptors::{Descriptor, TypePos};
use edgedb_protocol::descriptors::{ObjectShapeDescriptor, ShapeElement};
use edgedb_protocol::encoding::Input;
use edgedb_protocol::errors::DecodeError;
use edgedb_protocol::features::ProtocolVersion;
use uuid::Uuid;

mod base;

fn decode(bytes: &[u8]) -> Result<Vec<Descriptor>, DecodeError> {
    let bytes = Bytes::copy_from_slice(bytes);
    let mut input = Input::new(ProtocolVersion::current(), bytes);
    let mut result = Vec::new();
    while input.remaining() > 0 {
        result.push(Descriptor::decode(&mut input)?);
    }
    assert!(input.remaining() == 0);
    Ok(result)
}

fn decode_10(bytes: &[u8]) -> Result<Vec<Descriptor>, DecodeError> {
    let bytes = Bytes::copy_from_slice(bytes);
    let mut input = Input::new(ProtocolVersion::new(0, 10), bytes);
    let mut result = Vec::new();
    while input.remaining() > 0 {
        result.push(Descriptor::decode(&mut input)?);
    }
    assert!(input.remaining() == 0);
    Ok(result)
}

#[test]
fn empty_tuple() -> Result<(), Box<dyn Error>> {
    // `SELECT ()`
    assert_eq!(
        decode(b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0")?,
        vec![Descriptor::Tuple(TupleTypeDescriptor {
            id: "00000000-0000-0000-0000-0000000000FF"
                .parse::<Uuid>()?
                .into(),
            element_types: Vec::new(),
        }),]
    );
    Ok(())
}

#[test]
fn one_tuple() -> Result<(), Box<dyn Error>> {
    // `SELECT (1,)`
    assert_eq!(
        decode(bconcat!(
            b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"
            b"\x04\x1cyGes%\x89Sa\x03\xe7\x87vE\xad9\0\x01\0\0"))?,
        vec![
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::Tuple(TupleTypeDescriptor {
                id: "1c794765-7325-8953-6103-e7877645ad39"
                    .parse::<Uuid>()?
                    .into(),
                element_types: vec![TypePos(0)],
            }),
        ]
    );
    Ok(())
}

#[test]
fn single_int() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        decode(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05")?,
        vec![Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000105"
                .parse::<Uuid>()?
                .into(),
        })]
    );
    Ok(())
}

#[test]
fn duration() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        decode(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x0e")?,
        vec![Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-00000000010e"
                .parse::<Uuid>()?
                .into(),
        })]
    );
    Ok(())
}

#[test]
fn object_10() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        decode_10(bconcat!(
         b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\x02"
         b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x01\x01n"
         b"\xbb\xbe\xda\0P\x14\xfe\x84\xbc\x82\x15@\xb1"
         b"R\xcd\0\x03\x01\0\0\0\x07__tid__\0\0\x01"
         b"\0\0\0\x02id\0\0\0\0\0\0\x05title\0\x01"))?,
        vec![
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "6ebbbeda-0050-14fe-84bc-821540b152cd"
                    .parse::<Uuid>()?
                    .into(),
                elements: vec![
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: None,
                        name: String::from("__tid__"),
                        type_pos: TypePos(0),
                    },
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: None,
                        name: String::from("id"),
                        type_pos: TypePos(0),
                    },
                    ShapeElement {
                        flag_implicit: false,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: None,
                        name: String::from("title"),
                        type_pos: TypePos(1),
                    }
                ]
            })
        ]
    );
    Ok(())
}

#[test]
fn object() -> Result<(), Box<dyn Error>> {
    use edgedb_protocol::common::Cardinality::*;
    assert_eq!(
        decode(bconcat!(
        // equivalent of 0.10
        //b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x01\x02"
        //b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\x01,sT"
        //b"\xf9\x8f\xfac\xed\x10\x8d\x9c\xe4\x156\xd3\x92\0\x03"
        //b"\x01\0\0\0\t__tname__\0\0\x01\0\0\0\x02id\0\x01\0\0\0\0\x05title\0\0"
        b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x01\x02"
        b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\x01n'\xdb\xa0"
        b"xa$\xc2\x86\xa9\x15\xa6\xf2\xe3\xfa\xf5\0\x03\0\0\0"
        b"\x01A\0\0\0\t__tname__\0\0\0\0\0\x01A\0\0\0\x02id"
        b"\0\x01\0\0\0\0o\0\0\0\x05title\0\0"
        ))?,
        vec![
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "6e27dba0-7861-24c2-86a9-15a6f2e3faf5"
                    .parse::<Uuid>()?
                    .into(),
                elements: vec![
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(One),
                        name: String::from("__tname__"),
                        type_pos: TypePos(0),
                    },
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(One),
                        name: String::from("id"),
                        type_pos: TypePos(1),
                    },
                    ShapeElement {
                        flag_implicit: false,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(AtMostOne),
                        name: String::from("title"),
                        type_pos: TypePos(0),
                    }
                ]
            })
        ]
    );
    Ok(())
}
