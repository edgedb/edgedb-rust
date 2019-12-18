use std::io::{Cursor};
use std::error::Error;
use std::{i16, i32, i64};
use std::sync::Arc;

use bytes::{Bytes, Buf};

use edgedb_protocol::codec::{build_codec, Codec};
use edgedb_protocol::value::{Value, Scalar, Duration};
use edgedb_protocol::descriptors::{Descriptor, TypePos};
use edgedb_protocol::descriptors::BaseScalarTypeDescriptor;
use edgedb_protocol::descriptors::{ObjectShapeDescriptor, ShapeElement};

mod base;

macro_rules! encoding_eq {
    ($codec: expr, $bytes: expr, $value: expr) => {
        let orig_value = $value;
        let value = decode($codec, $bytes)?;
        assert_eq!(value, orig_value);
        let mut bytes = bytes::BytesMut::new();
        $codec.encode(&mut bytes, &orig_value)?;
        println!("Serialized bytes {:?}", bytes);
        let bytes = bytes.freeze();
        assert_eq!(&bytes[..], $bytes);
    }
}

fn decode(codec: &Arc<dyn Codec>, data: &[u8]) -> Result<Value, Box<dyn Error>>
{
    let bytes = Bytes::from(data);
    let mut cur = Cursor::new(bytes);
    let res = codec.decode(&mut cur)?;
    assert!(cur.bytes() == b"");
    Ok(res)
}

#[test]
fn int16() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000103".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000103".parse()?,
            })
        ]
    )?;
    encoding_eq!(&codec, b"\0\0",
               Value::Scalar(Scalar::Int16(0)));
    encoding_eq!(&codec, b"\x01\x05",
               Value::Scalar(Scalar::Int16(0x105)));
    encoding_eq!(&codec, b"\x7F\xFF",
               Value::Scalar(Scalar::Int16(i16::MAX)));
    encoding_eq!(&codec, b"\x80\x00",
               Value::Scalar(Scalar::Int16(i16::MIN)));
    encoding_eq!(&codec, b"\xFF\xFF",
               Value::Scalar(Scalar::Int16(-1)));
    Ok(())
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
    encoding_eq!(&codec, b"\0\0\0\0",
               Value::Scalar(Scalar::Int32(0)));
    encoding_eq!(&codec, b"\0\0\x01\x05",
               Value::Scalar(Scalar::Int32(0x105)));
    encoding_eq!(&codec, b"\x7F\xFF\xFF\xFF",
               Value::Scalar(Scalar::Int32(i32::MAX)));
    encoding_eq!(&codec, b"\x80\x00\x00\x00",
               Value::Scalar(Scalar::Int32(i32::MIN)));
    encoding_eq!(&codec, b"\xFF\xFF\xFF\xFF",
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
    encoding_eq!(&codec, b"\0\0\0\0\0\0\0\0",
               Value::Scalar(Scalar::Int64(0)));
    encoding_eq!(&codec, b"\0\0\0\0\0\0\x01\x05",
               Value::Scalar(Scalar::Int64(0x105)));
    encoding_eq!(&codec, b"\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
               Value::Scalar(Scalar::Int64(i64::MAX)));
    encoding_eq!(&codec, b"\x80\x00\x00\x00\x00\x00\x00\x00",
               Value::Scalar(Scalar::Int64(i64::MIN)));
    encoding_eq!(&codec, b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
               Value::Scalar(Scalar::Int64(-1)));
    Ok(())
}

#[test]
fn float32() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000106".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000106".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\0\0\0\0",
               Value::Scalar(Scalar::Float32(0.0)));
    encoding_eq!(&codec, b"\x80\0\0\0",
               Value::Scalar(Scalar::Float32(-0.0)));
    encoding_eq!(&codec, b"?\x80\0\0",
               Value::Scalar(Scalar::Float32(1.0)));
    encoding_eq!(&codec, b"\xbf\x8f\xbew",
               Value::Scalar(Scalar::Float32(-1.123)));

    match decode(&codec, b"\x7f\xc0\0\0")? {
        Value::Scalar(Scalar::Float32(val)) => assert!(val.is_nan()),
        _ => panic!("could not parse NaN")
    };

    match decode(&codec, b"\x7f\x80\0\0")? {
        Value::Scalar(Scalar::Float32(val)) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_positive())
        },
        _ => panic!("could not parse +inf")
    };

    match decode(&codec, b"\xff\x80\0\0")? {
        Value::Scalar(Scalar::Float32(val)) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_negative())
        }
        _ => panic!("could not parse -inf")
    };

    Ok(())
}

#[test]
fn float64() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000107".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000107".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\0\0\0\0\0\0\0\0",
               Value::Scalar(Scalar::Float64(0.0)));
    encoding_eq!(&codec, b"\x80\0\0\0\0\0\0\0",
               Value::Scalar(Scalar::Float64(-0.0)));
    encoding_eq!(&codec, b"?\xf0\0\0\0\0\0\0",
               Value::Scalar(Scalar::Float64(1.0)));
    encoding_eq!(&codec, b"T\xb2I\xad%\x94\xc3}",
               Value::Scalar(Scalar::Float64(1e100)));

    match decode(&codec, b"\x7f\xf8\0\0\0\0\0\0")? {
        Value::Scalar(Scalar::Float64(val)) => assert!(val.is_nan()),
        _ => panic!("could not parse NaN")
    };

    match decode(&codec, b"\x7f\xf0\0\0\0\0\0\0")? {
        Value::Scalar(Scalar::Float64(val)) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_positive())
        }
        _ => panic!("could not parse +inf")
    };

    match decode(&codec, b"\xff\xf0\0\0\0\0\0\0")? {
        Value::Scalar(Scalar::Float64(val)) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_negative())
        },
        _ => panic!("could not parse -inf")
    };

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
    encoding_eq!(&codec, b"hello",
               Value::Scalar(Scalar::Str(String::from("hello"))));
    encoding_eq!(&codec, b"",
               Value::Scalar(Scalar::Str(String::from(""))));
    encoding_eq!(&codec, b"\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82",
        Value::Scalar(Scalar::Str(String::from("привет"))));
    Ok(())
}

#[test]
fn bytes() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000102".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000102".parse()?,
            })
        ]
    )?;
    encoding_eq!(&codec, b"hello",
               Value::Scalar(Scalar::Bytes(b"hello".to_vec())));
    encoding_eq!(&codec, b"",
               Value::Scalar(Scalar::Bytes(b"".to_vec())));
    encoding_eq!(&codec, b"\x00\x01\x02\x03\x81",
        Value::Scalar(Scalar::Bytes(b"\x00\x01\x02\x03\x81".to_vec())));
    Ok(())
}

#[test]
fn uuid() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000100".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100".parse()?,
            })
        ]
    )?;
    encoding_eq!(&codec, b"I(\xcc\x1e e\x11\xea\x88H{S\xa6\xad\xb3\x83",
               Value::Scalar(Scalar::Uuid(
                "4928cc1e-2065-11ea-8848-7b53a6adb383".parse()?)));
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

    // SELECT <datetime>'2019-11-29T00:00:00Z'-<datetime>'2000-01-01T00:00:00Z'
    encoding_eq!(&codec, b"\0\x02;o\xad\xff\0\0\0\0\0\0\0\0\0\0",
               Value::Scalar(Scalar::Duration(
               Duration::from_secs(7272*86400))));
    // SELECT <datetime>'2019-11-29T00:00:00Z'-<datetime>'2019-11-28T01:00:00Z'
    encoding_eq!(&codec, b"\0\0\0\x13GC\xbc\0\0\0\0\0\0\0\0\0",
               Value::Scalar(Scalar::Duration(
               Duration::from_secs(82800))));
    encoding_eq!(&codec, b"\xff\xff\xff\xff\xd3,\xba\xe0\0\0\0\0\0\0\0\0",
               Value::Scalar(Scalar::Duration(
               Duration::from_micros(-752043296))));

    assert_eq!(
        decode(&codec, b"\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0")
            .unwrap_err().to_string(),
           "invalid duration");
    Ok(())
}

#[test]
fn null_codec() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        &"00000000-0000-0000-0000-000000000000".parse()?,
        &[]
    )?;
    encoding_eq!(&codec, b"", Value::Nothing);
    Ok(())
}

#[test]
fn object_codec() -> Result<(), Box<dyn Error>> {
    let elements = vec![
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            name: String::from("__tid__"),
            type_pos: TypePos(0),
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            name: String::from("id"),
            type_pos: TypePos(0),
        },
    ];
    let shape = elements.as_slice().into();
    let codec = build_codec(
        &"5d5ebe41-eac8-eab7-a24e-cc3a8cd2766c".parse()?,
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100".parse()?,
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "5d5ebe41-eac8-eab7-a24e-cc3a8cd2766c".parse()?,
                elements,
            }),
        ]
    )?;
    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(&codec, bconcat!(
        b"\0\0\0\x02\0\0\x00\x00\0\0\0\x100Wd\0 d"
        b"\x11\xea\x98\xc53\xc5\xcf\xb4r^\0\0\x00"
        b"\x00\0\0\0\x10I(\xcc\x1e e\x11\xea\x88H{S"
        b"\xa6\xad\xb3\x83"), Value::Object {
            shape,
            fields: vec![
                Value::Scalar(Scalar::Uuid(
                    "30576400-2064-11ea-98c5-33c5cfb4725e".parse()?)),
                Value::Scalar(Scalar::Uuid(
                    "4928cc1e-2065-11ea-8848-7b53a6adb383".parse()?)),
            ]
        });
    Ok(())
}
