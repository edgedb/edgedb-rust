use std::error::Error;
use std::io::Cursor;
use bytes::{Bytes, Buf};

use edgedb_protocol::errors::DecodeError;
use edgedb_protocol::descriptors::{Descriptor, TypePos};
use edgedb_protocol::descriptors::TupleTypeDescriptor;
use edgedb_protocol::descriptors::{ObjectShapeDescriptor, ShapeElement};
use edgedb_protocol::descriptors::BaseScalarTypeDescriptor;

mod base;


fn decode(bytes: &[u8]) -> Result<Vec<Descriptor>, DecodeError> {
    let bytes = Bytes::copy_from_slice(bytes);
    let mut cur = Cursor::new(bytes);
    let mut result = Vec::new();
    while cur.remaining() > 0 {
        result.push(Descriptor::decode(&mut cur)?);
    }
    assert!(cur.remaining() == 0);
    Ok(result)
}

#[test]
fn empty_tuple() -> Result<(), Box<dyn Error>> {
    // `SELECT ()`
    assert_eq!(decode(b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0")?,
        vec![
            Descriptor::Tuple(TupleTypeDescriptor {
                id: "00000000-0000-0000-0000-0000000000FF".parse()?,
                element_types: Vec::new(),
            }),
        ]);
    Ok(())
}

#[test]
fn one_tuple() -> Result<(), Box<dyn Error>> {
    // `SELECT (1,)`
    assert_eq!(decode(bconcat!(
            b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"
            b"\x04\x1cyGes%\x89Sa\x03\xe7\x87vE\xad9\0\x01\0\0"))?,
        vec![
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105".parse()?,
            }),
            Descriptor::Tuple(TupleTypeDescriptor {
                id: "1c794765-7325-8953-6103-e7877645ad39".parse()?,
                element_types: vec![TypePos(0)],
            }),
        ]);
    Ok(())
}

#[test]
fn single_int() -> Result<(), Box<dyn Error>> {
    assert_eq!(decode(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05")?,
        vec![
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105".parse()?,
            })
        ]);
    Ok(())
}

#[test]
fn duration() -> Result<(), Box<dyn Error>> {
    assert_eq!(decode(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x0e")?,
        vec![
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-00000000010e".parse()?,
            })
        ]);
    Ok(())
}

#[test]
fn object() -> Result<(), Box<dyn Error>> {
    assert_eq!(decode(bconcat!(
         b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\x02"
         b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x01\x01n"
         b"\xbb\xbe\xda\0P\x14\xfe\x84\xbc\x82\x15@\xb1"
         b"R\xcd\0\x03\x01\0\0\0\x07__tid__\0\0\x01"
         b"\0\0\0\x02id\0\0\0\0\0\0\x05title\0\x01"))?,
        vec![
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100".parse()?,
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101".parse()?,
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "6ebbbeda-0050-14fe-84bc-821540b152cd".parse()?,
                elements: vec![
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        name: String::from("__tid__"),
                        type_pos: TypePos(0),
                    },
                    ShapeElement {
                        flag_implicit: true,
                        flag_link_property: false,
                        flag_link: false,
                        name: String::from("id"),
                        type_pos: TypePos(0),
                    },
                    ShapeElement {
                        flag_implicit: false,
                        flag_link_property: false,
                        flag_link: false,
                        name: String::from("title"),
                        type_pos: TypePos(1),
                    }
                ]
            })
        ]);
    Ok(())
}
