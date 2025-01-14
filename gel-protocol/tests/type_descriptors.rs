use bytes::{Buf, Bytes};
use std::error::Error;

use gel_protocol::descriptors::BaseScalarTypeDescriptor;
use gel_protocol::descriptors::ObjectTypeDescriptor;
use gel_protocol::descriptors::ScalarTypeDescriptor;
use gel_protocol::descriptors::TupleTypeDescriptor;
use gel_protocol::descriptors::{Descriptor, TypePos};
use gel_protocol::descriptors::{ObjectShapeDescriptor, ShapeElement};
use gel_protocol::encoding::Input;
use gel_protocol::errors::DecodeError;
use gel_protocol::features::ProtocolVersion;
use uuid::Uuid;

mod base;

fn decode(pv: ProtocolVersion, bytes: &[u8]) -> Result<Vec<Descriptor>, DecodeError> {
    let bytes = Bytes::copy_from_slice(bytes);
    let mut input = Input::new(pv, bytes);
    let mut result = Vec::new();
    while input.remaining() > 0 {
        result.push(Descriptor::decode(&mut input)?);
    }
    assert!(input.remaining() == 0);
    Ok(result)
}

fn decode_2_0(bytes: &[u8]) -> Result<Vec<Descriptor>, DecodeError> {
    decode(ProtocolVersion::new(2, 0), bytes)
}

fn decode_1_0(bytes: &[u8]) -> Result<Vec<Descriptor>, DecodeError> {
    decode(ProtocolVersion::new(1, 0), bytes)
}

fn decode_0_10(bytes: &[u8]) -> Result<Vec<Descriptor>, DecodeError> {
    decode(ProtocolVersion::new(0, 10), bytes)
}

#[test]
fn empty_tuple() -> Result<(), Box<dyn Error>> {
    // `SELECT ()`
    assert_eq!(
        decode_1_0(b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0")?,
        vec![Descriptor::Tuple(TupleTypeDescriptor {
            id: "00000000-0000-0000-0000-0000000000FF"
                .parse::<Uuid>()?
                .into(),
            element_types: Vec::new(),
            name: None,
            schema_defined: None,
            ancestors: vec![],
        }),]
    );
    Ok(())
}

#[test]
fn one_tuple() -> Result<(), Box<dyn Error>> {
    // `SELECT (1,)`
    assert_eq!(
        decode_1_0(bconcat!(
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
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }),
        ]
    );
    Ok(())
}

#[test]
fn single_int_1_0() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        decode_1_0(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05")?,
        vec![Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000105"
                .parse::<Uuid>()?
                .into(),
        })]
    );
    Ok(())
}

#[test]
fn single_int_2_0() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        decode_2_0(b"\0\0\0\"\x03\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05\0\0\0\nstd::int64\x01\0\0")?,
        vec![Descriptor::Scalar(ScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000105"
                .parse::<Uuid>()?
                .into(),
            name: Some(String::from("std::int64")),
            schema_defined: Some(true),
            ancestors: vec![],
            base_type_pos: None,
        })]
    );
    Ok(())
}

#[test]
fn single_derived_int_2_0() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        decode_2_0(bconcat!(
            b"\0\0\0\"\x03\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05\0\0\0\n"
            b"std::int64\x01\0\0\0\0\0)\x03\x91v\xff\x8c\x95\xb6\x11\xef\x9c"
            b" [\x0e\x8c=\xaa\xc8\0\0\0\x0fdefault::my_int\x01\0\x01\0\0\0\0\0"
            b"-\x03J\xa0\x08{\x95\xb7\x11\xef\xbd\xe2?\xfa\xe3\r\x13\xe9\0\0\0"
            b"\x11default::my_int_2\x01\0\x02\0\x01\0\0"
        ))?,
        vec![
            Descriptor::Scalar(ScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105"
                    .parse::<Uuid>()?
                    .into(),
                name: Some(String::from("std::int64")),
                schema_defined: Some(true),
                ancestors: vec![],
                base_type_pos: None,
            }),
            Descriptor::Scalar(ScalarTypeDescriptor {
                id: "9176ff8c-95b6-11ef-9c20-5b0e8c3daac8"
                    .parse::<Uuid>()?
                    .into(),
                name: Some(String::from("default::my_int")),
                schema_defined: Some(true),
                ancestors: vec![TypePos(0)],
                base_type_pos: Some(TypePos(0)),
            }),
            Descriptor::Scalar(ScalarTypeDescriptor {
                id: "4aa0087b-95b7-11ef-bde2-3ffae30d13e9"
                    .parse::<Uuid>()?
                    .into(),
                name: Some(String::from("default::my_int_2")),
                schema_defined: Some(true),
                ancestors: vec![TypePos(1), TypePos(0)],
                base_type_pos: Some(TypePos(0)),
            }),
        ]
    );
    Ok(())
}

#[test]
fn duration() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        decode_1_0(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x0e")?,
        vec![Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-00000000010e"
                .parse::<Uuid>()?
                .into(),
        })]
    );
    Ok(())
}

#[test]
fn object_0_10() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        decode_0_10(bconcat!(
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
                ephemeral_free_shape: false,
                type_pos: None,
                elements: vec![
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: None,
                        name: String::from("__tid__"),
                        type_pos: TypePos(0),
                        source_type_pos: None,
                    },
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: None,
                        name: String::from("id"),
                        type_pos: TypePos(0),
                        source_type_pos: None,
                    },
                    ShapeElement {
                        flag_implicit: false,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: None,
                        name: String::from("title"),
                        type_pos: TypePos(1),
                        source_type_pos: None,
                    }
                ]
            })
        ]
    );
    Ok(())
}

#[test]
fn object_1_0() -> Result<(), Box<dyn Error>> {
    use gel_protocol::common::Cardinality::*;
    assert_eq!(
        decode_1_0(bconcat!(
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
                ephemeral_free_shape: false,
                type_pos: None,
                elements: vec![
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(One),
                        name: String::from("__tname__"),
                        type_pos: TypePos(0),
                        source_type_pos: None,
                    },
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(One),
                        name: String::from("id"),
                        type_pos: TypePos(1),
                        source_type_pos: None,
                    },
                    ShapeElement {
                        flag_implicit: false,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(AtMostOne),
                        name: String::from("title"),
                        type_pos: TypePos(0),
                        source_type_pos: None,
                    }
                ]
            })
        ]
    );
    Ok(())
}

#[test]
fn object_2_0() -> Result<(), Box<dyn Error>> {
    use gel_protocol::common::Cardinality::*;
    // SELECT Foo {
    //   id,
    //   title,
    //   [IS Bar].body,
    // }
    assert_eq!(
        decode_2_0(bconcat!(
        b"\0\0\0 \x03\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x01\0\0\0\x08"
        b"std::str\x01\0\0\0\0\0!\x03\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01"
        b"\0\0\0\0\tstd::uuid\x01\0\0\0\0\0\"\n\xc3\xcc\xa7R\x95\xb7"
        b"\x11\xef\xb4\x87\x1d\x1b\x9f\xa20\x03\0\0\0\x0cdefault::Foo"
        b"\x01\0\0\0\"\n\r\xdc\xd7\x1e\x95\xb8\x11\xef\x82M!7\x80\\^4"
        b"\0\0\0\x0cdefault::Bar\x01\0\0\0^\x01\x1dMg\xe7{\xdd]9\x90\x97"
        b"O\x82\xfa\xd8\xaf7\0\0\x02\0\x04\0\0\0\x01A\0\0\0\t__tname__"
        b"\0\0\0\x02\0\0\0\0A\0\0\0\x02id\0\x01\0\x02\0\0\0\0o\0\0\0\x05"
        b"title\0\0\0\x02\0\0\0\0o\0\0\0\x04body\0\0\0\x03"
        ))?,
        vec![
            Descriptor::Scalar(ScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101"
                    .parse::<Uuid>()?
                    .into(),
                name: Some(String::from("std::str")),
                schema_defined: Some(true),
                ancestors: vec![],
                base_type_pos: None,
            }),
            Descriptor::Scalar(ScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100"
                    .parse::<Uuid>()?
                    .into(),
                name: Some(String::from("std::uuid")),
                schema_defined: Some(true),
                ancestors: vec![],
                base_type_pos: None,
            }),
            Descriptor::Object(ObjectTypeDescriptor {
                id: "c3cca752-95b7-11ef-b487-1d1b9fa23003"
                    .parse::<Uuid>()?
                    .into(),
                name: Some(String::from("default::Foo")),
                schema_defined: Some(true),
            }),
            Descriptor::Object(ObjectTypeDescriptor {
                id: "0ddcd71e-95b8-11ef-824d-2137805c5e34"
                    .parse::<Uuid>()?
                    .into(),
                name: Some(String::from("default::Bar")),
                schema_defined: Some(true),
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "1d4d67e7-7bdd-5d39-9097-4f82fad8af37"
                    .parse::<Uuid>()?
                    .into(),
                ephemeral_free_shape: false,
                type_pos: Some(TypePos(2)),
                elements: vec![
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(One),
                        name: String::from("__tname__"),
                        type_pos: TypePos(0),
                        source_type_pos: Some(TypePos(2)),
                    },
                    ShapeElement {
                        flag_implicit: false,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(One),
                        name: String::from("id"),
                        type_pos: TypePos(1),
                        source_type_pos: Some(TypePos(2)),
                    },
                    ShapeElement {
                        flag_implicit: false,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(AtMostOne),
                        name: String::from("title"),
                        type_pos: TypePos(0),
                        source_type_pos: Some(TypePos(2)),
                    },
                    ShapeElement {
                        flag_implicit: false,
                        flag_link_property: false,
                        flag_link: false,
                        cardinality: Some(AtMostOne),
                        name: String::from("body"),
                        type_pos: TypePos(0),
                        source_type_pos: Some(TypePos(3)),
                    },
                ]
            })
        ]
    );
    Ok(())
}
