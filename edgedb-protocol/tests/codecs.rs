use std::io::{Cursor};
use std::error::Error;
use std::i32;
use std::i64;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, Buf};

use edgedb_protocol::codec::{build_codec, Codec};
use edgedb_protocol::value::{Value, Scalar};
use edgedb_protocol::descriptors::Descriptor;
use edgedb_protocol::descriptors::BaseScalarTypeDescriptor;



fn decode(codec: &Arc<dyn Codec>, data: &[u8]) -> Result<Value, Box<dyn Error>>
{
    let bytes = Bytes::from(data);
    let mut cur = Cursor::new(bytes);
    let res = codec.decode(&mut cur)?;
    assert!(cur.bytes() == b"");
    Ok(res)
}

#[test]
fn int32() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000104".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000104".parse()?,
            })
        ]
    )?;
    assert_eq!(decode(&codec, b"\0\0\0\0")?,
               Value::Scalar(Scalar::Int32(0)));
    assert_eq!(decode(&codec, b"\0\0\x01\x05")?,
               Value::Scalar(Scalar::Int32(0x105)));
    assert_eq!(decode(&codec, b"\x7F\xFF\xFF\xFF")?,
               Value::Scalar(Scalar::Int32(i32::MAX)));
    assert_eq!(decode(&codec, b"\x80\x00\x00\x00")?,
               Value::Scalar(Scalar::Int32(i32::MIN)));
    assert_eq!(decode(&codec, b"\xFF\xFF\xFF\xFF")?,
               Value::Scalar(Scalar::Int32(-1)));
    Ok(())
}

#[test]
fn int64() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000105".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105".parse()?,
            })
        ]
    )?;
    assert_eq!(decode(&codec, b"\0\0\0\0\0\0\0\0")?,
               Value::Scalar(Scalar::Int64(0)));
    assert_eq!(decode(&codec, b"\0\0\0\0\0\0\x01\x05")?,
               Value::Scalar(Scalar::Int64(0x105)));
    assert_eq!(decode(&codec, b"\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF")?,
               Value::Scalar(Scalar::Int64(i64::MAX)));
    assert_eq!(decode(&codec, b"\x80\x00\x00\x00\x00\x00\x00\x00")?,
               Value::Scalar(Scalar::Int64(i64::MIN)));
    assert_eq!(decode(&codec, b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")?,
               Value::Scalar(Scalar::Int64(-1)));
    Ok(())
}

#[test]
fn str() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000101".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101".parse()?,
            })
        ]
    )?;
    assert_eq!(decode(&codec, b"hello")?,
               Value::Scalar(Scalar::Str(String::from("hello"))));
    assert_eq!(decode(&codec, b"")?,
               Value::Scalar(Scalar::Str(String::from(""))));
    assert_eq!(decode(&codec,
        b"\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82")?,
        Value::Scalar(Scalar::Str(String::from("привет"))));
    Ok(())
}

#[test]
fn duration() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-00000000010e".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-00000000010e".parse()?,
            })
        ]
    )?;

    assert_eq!(decode(&codec, b"\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0")?,
               Value::Scalar(Scalar::Duration(
               Duration::from_secs(86400))));
    // SELECT <datetime>'2019-11-29T00:00:00Z'-<datetime>'2000-01-01T00:00:00Z'
    assert_eq!(decode(&codec, b"\0\0\0\0\0\0\0\0\0\0\x1ch\0\0\0\0")?,
               Value::Scalar(Scalar::Duration(
               Duration::from_secs(7272*86400))));
    // SELECT <datetime>'2019-11-29T00:00:00Z'-<datetime>'2019-11-28T01:00:00Z'
    assert_eq!(decode(&codec, b"\0\0\0\x13GC\xbc\0\0\0\0\0\0\0\0\0")?,
               Value::Scalar(Scalar::Duration(
               Duration::from_secs(82800))));
    Ok(())
}
