use std::error::Error;
use std::io::Cursor;
use bytes::{Bytes, Buf, BytesMut};

use edgedb_protocol::errors::DecodeError;
use edgedb_protocol::descriptors::{Descriptor, TypePos};
use edgedb_protocol::descriptors::TupleTypeDescriptor;
use edgedb_protocol::descriptors::BaseScalarTypeDescriptor;

macro_rules! bconcat {
    ($($token: expr)*) => {
        &{
            let mut buf = BytesMut::new();
            $(
                buf.extend($token);
            )*
            buf
        }
    }
}

fn decode(bytes: &[u8]) -> Result<Vec<Descriptor>, DecodeError> {
    let bytes = Bytes::from(bytes);
    let mut cur = Cursor::new(bytes);
    let mut result = Vec::new();
    while cur.bytes() != b"" {
        result.push(Descriptor::decode(&mut cur)?);
    }
    assert!(cur.bytes() == b"");
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
