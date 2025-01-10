#[macro_use]
extern crate pretty_assertions;

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;

use edgedb_protocol::codec::build_codec;
use edgedb_protocol::codec::{Codec, ObjectShape};
use edgedb_protocol::common::RawTypedesc;
use edgedb_protocol::descriptors::ArrayTypeDescriptor;
use edgedb_protocol::descriptors::BaseScalarTypeDescriptor;
use edgedb_protocol::descriptors::EnumerationTypeDescriptor;
use edgedb_protocol::descriptors::ScalarTypeDescriptor;
use edgedb_protocol::descriptors::SetDescriptor;
use edgedb_protocol::descriptors::TupleTypeDescriptor;
use edgedb_protocol::descriptors::{Descriptor, TypePos};
use edgedb_protocol::descriptors::{MultiRangeTypeDescriptor, RangeTypeDescriptor};
use edgedb_protocol::descriptors::{NamedTupleTypeDescriptor, TupleElement};
use edgedb_protocol::descriptors::{ObjectShapeDescriptor, ShapeElement};
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::model::{Datetime, Json, RelativeDuration};
use edgedb_protocol::model::{Duration, LocalDate, LocalTime};
use edgedb_protocol::server_message::StateDataDescription;
use edgedb_protocol::value::{SparseObject, Value};
use uuid::Uuid;

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
    };
}

fn decode(codec: &Arc<dyn Codec>, data: &[u8]) -> Result<Value, Box<dyn Error>> {
    Ok(codec.decode(data)?)
}

#[test]
fn int16() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000103"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(&codec, b"\0\0", Value::Int16(0));
    encoding_eq!(&codec, b"\x01\x05", Value::Int16(0x105));
    encoding_eq!(&codec, b"\x7F\xFF", Value::Int16(i16::MAX));
    encoding_eq!(&codec, b"\x80\x00", Value::Int16(i16::MIN));
    encoding_eq!(&codec, b"\xFF\xFF", Value::Int16(-1));
    Ok(())
}

#[test]
fn int32() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000104"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(&codec, b"\0\0\0\0", Value::Int32(0));
    encoding_eq!(&codec, b"\0\0\x01\x05", Value::Int32(0x105));
    encoding_eq!(&codec, b"\x7F\xFF\xFF\xFF", Value::Int32(i32::MAX));
    encoding_eq!(&codec, b"\x80\x00\x00\x00", Value::Int32(i32::MIN));
    encoding_eq!(&codec, b"\xFF\xFF\xFF\xFF", Value::Int32(-1));
    Ok(())
}

#[test]
fn int64() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000105"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(&codec, b"\0\0\0\0\0\0\0\0", Value::Int64(0));
    encoding_eq!(&codec, b"\0\0\0\0\0\0\x01\x05", Value::Int64(0x105));
    encoding_eq!(
        &codec,
        b"\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
        Value::Int64(i64::MAX)
    );
    encoding_eq!(
        &codec,
        b"\x80\x00\x00\x00\x00\x00\x00\x00",
        Value::Int64(i64::MIN)
    );
    encoding_eq!(
        &codec,
        b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
        Value::Int64(-1)
    );
    Ok(())
}

#[test]
fn float32() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000106"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(&codec, b"\0\0\0\0", Value::Float32(0.0));
    encoding_eq!(&codec, b"\x80\0\0\0", Value::Float32(-0.0));
    encoding_eq!(&codec, b"?\x80\0\0", Value::Float32(1.0));
    encoding_eq!(&codec, b"\xbf\x8f\xbew", Value::Float32(-1.123));

    match decode(&codec, b"\x7f\xc0\0\0")? {
        Value::Float32(val) => assert!(val.is_nan()),
        _ => panic!("could not parse NaN"),
    };

    match decode(&codec, b"\x7f\x80\0\0")? {
        Value::Float32(val) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_positive())
        }
        _ => panic!("could not parse +inf"),
    };

    match decode(&codec, b"\xff\x80\0\0")? {
        Value::Float32(val) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_negative())
        }
        _ => panic!("could not parse -inf"),
    };

    Ok(())
}

#[test]
fn float64() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000107"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(&codec, b"\0\0\0\0\0\0\0\0", Value::Float64(0.0));
    encoding_eq!(&codec, b"\x80\0\0\0\0\0\0\0", Value::Float64(-0.0));
    encoding_eq!(&codec, b"?\xf0\0\0\0\0\0\0", Value::Float64(1.0));
    encoding_eq!(&codec, b"T\xb2I\xad%\x94\xc3}", Value::Float64(1e100));

    match decode(&codec, b"\x7f\xf8\0\0\0\0\0\0")? {
        Value::Float64(val) => assert!(val.is_nan()),
        _ => panic!("could not parse NaN"),
    };

    match decode(&codec, b"\x7f\xf0\0\0\0\0\0\0")? {
        Value::Float64(val) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_positive())
        }
        _ => panic!("could not parse +inf"),
    };

    match decode(&codec, b"\xff\xf0\0\0\0\0\0\0")? {
        Value::Float64(val) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_negative())
        }
        _ => panic!("could not parse -inf"),
    };

    Ok(())
}

#[test]
fn str() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000101"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(&codec, b"hello", Value::Str(String::from("hello")));
    encoding_eq!(&codec, b"", Value::Str(String::from("")));
    encoding_eq!(
        &codec,
        b"\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82",
        Value::Str(String::from("привет"))
    );
    Ok(())
}

#[test]
fn bytes() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000102"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(&codec, b"hello", Value::Bytes(b"hello"[..].into()));
    encoding_eq!(&codec, b"", Value::Bytes(b""[..].into()));
    encoding_eq!(
        &codec,
        b"\x00\x01\x02\x03\x81",
        Value::Bytes(b"\x00\x01\x02\x03\x81"[..].into())
    );
    Ok(())
}

#[test]
fn uuid() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000100"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(
        &codec,
        b"I(\xcc\x1e e\x11\xea\x88H{S\xa6\xad\xb3\x83",
        Value::Uuid("4928cc1e-2065-11ea-8848-7b53a6adb383".parse::<Uuid>()?)
    );
    Ok(())
}

#[test]
fn duration() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-00000000010e"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    // SELECT <datetime>'2019-11-29T00:00:00Z'-<datetime>'2000-01-01T00:00:00Z'
    encoding_eq!(
        &codec,
        b"\0\x02;o\xad\xff\0\0\0\0\0\0\0\0\0\0",
        Value::Duration(Duration::from_micros(7272 * 86400 * 1_000_000))
    );
    // SELECT <datetime>'2019-11-29T00:00:00Z'-<datetime>'2019-11-28T01:00:00Z'
    encoding_eq!(
        &codec,
        b"\0\0\0\x13GC\xbc\0\0\0\0\0\0\0\0\0",
        Value::Duration(Duration::from_micros(82800 * 1_000_000))
    );
    encoding_eq!(
        &codec,
        b"\xff\xff\xff\xff\xd3,\xba\xe0\0\0\0\0\0\0\0\0",
        Value::Duration(Duration::from_micros(-752043296))
    );

    assert_eq!(
        decode(&codec, b"\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0")
            .unwrap_err()
            .to_string(),
        "non-zero reserved bytes received in data"
    );
    Ok(())
}

#[test]
fn relative_duration() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000111"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    // SELECT <cal::relative_duration>
    // '2 years 7 months 16 days 48 hours 45 minutes 7.6 seconds'
    encoding_eq!(
        &codec,
        b"\0\0\0\x28\xdd\x11\x72\x80\0\0\0\x10\0\0\0\x1f",
        Value::RelativeDuration(
            RelativeDuration::from_years(2)
                + RelativeDuration::from_months(7)
                + RelativeDuration::from_days(16)
                + RelativeDuration::from_hours(48)
                + RelativeDuration::from_minutes(45)
                + RelativeDuration::from_secs(7)
                + RelativeDuration::from_millis(600)
        )
    );
    Ok(())
}

#[test]
fn null_codec() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(None, &[])?;
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
            cardinality: None,
            name: String::from("__tid__"),
            type_pos: TypePos(0),
            source_type_pos: None,
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: String::from("id"),
            type_pos: TypePos(0),
            source_type_pos: None,
        },
    ];
    let shape = elements.as_slice().into();
    let codec = build_codec(
        Some(TypePos(1)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "5d5ebe41-eac8-eab7-a24e-cc3a8cd2766c"
                    .parse::<Uuid>()?
                    .into(),
                ephemeral_free_shape: false,
                type_pos: None,
                elements,
            }),
        ],
    )?;
    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(
        &codec,
        bconcat!(
        b"\0\0\0\x02\0\0\x00\x00\0\0\0\x100Wd\0 d"
        b"\x11\xea\x98\xc53\xc5\xcf\xb4r^\0\0\x00"
        b"\x00\0\0\0\x10I(\xcc\x1e e\x11\xea\x88H{S"
        b"\xa6\xad\xb3\x83"),
        Value::Object {
            shape,
            fields: vec![
                Some(Value::Uuid("30576400-2064-11ea-98c5-33c5cfb4725e".parse()?)),
                Some(Value::Uuid("4928cc1e-2065-11ea-8848-7b53a6adb383".parse()?)),
            ]
        }
    );
    Ok(())
}

#[test]
fn input_codec() -> Result<(), Box<dyn Error>> {
    let sdd = StateDataDescription {
        typedesc: RawTypedesc {
            proto: ProtocolVersion::new(1, 0),
            id: "fd6c3b17504a714858ec2282431ce72c".parse()?,
            data: Bytes::from_static(
                b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\
            \x01\x01\x04\xcf\x9d\xce6\x17\xf05O\t%g\x8eW\xa1\x842\0\x02\
            \0\0\0\0\x06\xc6R\xf3\xf1\xdd\xe7\0a?\x07|=&\x0b\xfbt\0\x01\
            \0\x01\xff\xff\xff\xff\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\
            \x0e\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05\0\xa5zjc\xee\
            \xc4@\x91\xabnI\x97#\xf5\xe8\xaa\0\0\x02\0\0\0\0\0\0\0\0\
            \0\0\0\0\0\0\x01\t\x01\xd9\xa1-\xbfH\xfa\xeb\x1a/\xf5xe7\
            \xc8\xb8\xee\0\0\0\x16w\xe5\x87Y\xbd\x05\xb9\x14\xce\x8a\
            \xc2\x99\x85b5\0\x07\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x010\
            \x07\x82w\xed\x1a\xfd\xe0\x11\xec\x8bl\x85\xd0\xc8\xdc\xcd[\
            \0\x02\0\0\0\x0bAlwaysAllow\0\0\0\nNeverAllow\x01v\x9eH\xcb#\\1\
            \x90c&\x9b\x90p-\xa7\x03\0\0\0\xb1\xef6\xe2\xbb%Wr\xafk\x11\x84l\
            \x183n\0\x0b\x07\x85[<\"\xfd\xe0\x11\xec\x9a\xf6\xa1U\x99\xf2+\xc2\
            \0\x03\0\0\0\x03One\0\0\0\x03Two\0\0\0\x05Three\x08t\x13\xa1IP\xe6\
            \xc3\xf9*\xd7U1\x9f\xf1\xe1o\0\x10\0\0\0\0o\0\0\0\x07durprop\0\x03\
            \0\0\0\0o\0\0\0\x14__pg_max_connections\0\x04\0\0\0\0o\
            \0\0\0\x17query_execution_timeout\0\x03\0\0\0\0m\0\0\0\tmultiprop\
            \0\x05\0\0\0\0o\0\0\0\x1b__internal_no_const_folding\0\x06\0\0\0\0\
            m\0\0\0\x06sysobj\0\x08\0\0\0\0o\0\0\0\x07memprop\0\t\0\0\0\0o\0\0\
            \0\x13__internal_testmode\0\x06\0\0\0\0o\
            \0\0\0\x15apply_access_policies\0\x06\0\0\0\0o\
            \0\0\0 session_idle_transaction_timeout\0\x03\0\0\0\0o\
            \0\0\0\x0eallow_bare_ddl\0\n\0\0\0\0o\0\0\0\nsingleprop\
            \0\0\0\0\0\0o\0\0\0\x16allow_dml_in_functions\0\x06\0\0\0\0\
            o\0\0\0\x19__internal_sess_testvalue\0\x04\0\0\0\0m\
            \0\0\0\x07sessobj\0\x0c\0\0\0\0o\0\0\0\x08enumprop\0\r\x08!s\xfc,)\
            \x19\x80\x13/E\xea\xf3!\x98\x84\t\0\x01\0\0\0\0o\
            \0\0\0\x17default::my_globalvar_1\0\0\x08\xfdl;\x17PJqHX\xec\"\x82\
            C\x1c\xe7,\0\x04\0\0\0\0o\0\0\0\x06module\0\0\0\0\0\0o\
            \0\0\0\x07aliases\0\x02\0\0\0\0o\0\0\0\x07globals\0\x0f\0\0\0\0\
            o\0\0\0\x06config\0\x0e",
            ),
        },
    };
    let out_desc = sdd.parse()?;
    let codec = build_codec(Some(TypePos(16)), out_desc.descriptors())?;
    encoding_eq!(
        &codec,
        b"\0\0\0\x03\0\0\0\0\0\0\0\x07default\0\0\0\x02\0\0\0\x1c\
            \0\0\0\x01\0\0\0\0\0\0\0\x10GLOBAL VAR VALUE\
            \0\0\0\x03\0\0\0\x1c\0\0\0\x01\0\0\0\t\0\0\0\x10\
            \0\0\0\0\x11\xe1\xa3\0\0\0\0\0\0\0\0\0",
        Value::SparseObject(SparseObject::from_pairs([
            ("module", Some(Value::Str("default".into()))),
            (
                "globals",
                Some(Value::SparseObject(SparseObject::from_pairs([(
                    "default::my_globalvar_1",
                    Some(Value::Str("GLOBAL VAR VALUE".into()))
                ),])))
            ),
            (
                "config",
                Some(Value::SparseObject(SparseObject::from_pairs([(
                    "session_idle_transaction_timeout",
                    Some(Value::Duration(Duration::from_micros(300_000_000)))
                ),])))
            ),
        ]))
    );
    Ok(())
}

#[test]
fn set_codec() -> Result<(), Box<dyn Error>> {
    let inner_elements = vec![
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "__tid__".into(),
            type_pos: TypePos(0),
            source_type_pos: None,
        },
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "id".into(),
            type_pos: TypePos(0),
            source_type_pos: None,
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "first_name".into(),
            type_pos: TypePos(1),
            source_type_pos: None,
        },
    ];
    let outer_elements = vec![
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "__tid__".into(),
            type_pos: TypePos(0),
            source_type_pos: None,
        },
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "id".into(),
            type_pos: TypePos(0),
            source_type_pos: None,
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "first_name".into(),
            type_pos: TypePos(1),
            source_type_pos: None,
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: true,
            cardinality: None,
            name: "collegues".into(),
            type_pos: TypePos(3),
            source_type_pos: None,
        },
    ];
    let inner_shape = ObjectShape::from(&inner_elements[..]);
    let outer_shape = ObjectShape::from(&outer_elements[..]);
    let codec = build_codec(
        Some(TypePos(4)),
        &[
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
                id: "8faa7193-48c6-4263-18d3-1a127652569b"
                    .parse::<Uuid>()?
                    .into(),
                elements: inner_elements,
                ephemeral_free_shape: false,
                type_pos: None,
            }),
            Descriptor::Set(SetDescriptor {
                id: "afbb389d-aa73-2aae-9310-84a9163cb5ed"
                    .parse::<Uuid>()?
                    .into(),
                type_pos: TypePos(2),
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "9740ff04-324e-08a4-4ac7-2192d72c6967"
                    .parse::<Uuid>()?
                    .into(),
                elements: outer_elements,
                ephemeral_free_shape: false,
                type_pos: None,
            }),
        ],
    )?;
    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(
        &codec,
        bconcat!(
            b"\0\0\0\x04\0\0\x00\x00\0\0\0\x10\x0c\xf06\xbd "
            b"\xbd\x11\xea\xa4\xeb\xe9T\xb4(\x13\x91\0\0\x00\x00\0\0\0\x10"
            b"[\xe3\x9c( \xbd\x11\xea\xaa\xb9g4\x82*\xf1\xc9\0\0\0\x00\0\0\0"
            b"\x04Ryan\0\0\x00\x00\0\0\0\x9f\0\0\0\x01\0\0\0\0\0\0\x00\x00\0"
            b"\0\0\x02\0\0\0\x01\0\0\0?\0\0\0\x03\0\0\x00\x00\0\0\0\x10\x0c\xf0"
            b"6\xbd \xbd\x11\xea\xa4\xeb\xe9T\xb4(\x13\x91\0\0\x00\x00\0\0\0\x10"
            b"[\xe3\x9e\x80 \xbd\x11\xea\xaa\xb9\x17]\xbf\x18G\xe5\0\0\0\x00\0\0"
            b"\0\x03Ana\0\0\0D\0\0\0\x03\0\0\x00\x00\0\0\0\x10\x0c\xf06\xbd "
            b"\xbd\x11\xea\xa4\xeb\xe9T\xb4(\x13\x91\0\0\x00\x00\0\0\0\x10["
            b"\xe3\x97\x14 \xbd\x11\xea\xaa\xb9?7\xe7 \xb8T\0\0\0\x00\0\0\0"
            b"\x08Harrison"
        ),
        Value::Object {
            shape: outer_shape.clone(),
            fields: vec![
                Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391".parse()?)),
                Some(Value::Uuid("5be39c28-20bd-11ea-aab9-6734822af1c9".parse()?)),
                Some(Value::Str(String::from("Ryan"))),
                Some(Value::Set(vec![
                    Value::Object {
                        shape: inner_shape.clone(),
                        fields: vec![
                            Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391".parse()?)),
                            Some(Value::Uuid("5be39e80-20bd-11ea-aab9-175dbf1847e5".parse()?)),
                            Some(Value::Str(String::from("Ana"))),
                        ]
                    },
                    Value::Object {
                        shape: inner_shape,
                        fields: vec![
                            Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391".parse()?)),
                            Some(Value::Uuid("5be39714-20bd-11ea-aab9-3f37e720b854".parse()?)),
                            Some(Value::Str(String::from("Harrison"))),
                        ]
                    }
                ])),
            ]
        }
    );
    encoding_eq!(
        &codec,
        bconcat!(b"\0\0\0\x04\0\0\x00\x00\0\0\0\x10\x0c\xf06"
            b"\xbd \xbd\x11\xea\xa4\xeb\xe9T\xb4(\x13\x91\0\0\x00\x00\0\0\0\x10"
            b"[\xe3\x9c( \xbd\x11\xea\xaa\xb9g4\x82*\xf1\xc9\0\0\0\x00"
            b"\0\0\0\x04Ryan\0\0\x00\x00\0\0\0\x0c\0\0\0\0\0\0\0\0\0\0\x00\x00"
        ),
        Value::Object {
            shape: outer_shape.clone(),
            fields: vec![
                Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391".parse()?)),
                Some(Value::Uuid("5be39c28-20bd-11ea-aab9-6734822af1c9".parse()?)),
                Some(Value::Str(String::from("Ryan"))),
                Some(Value::Set(vec![])),
            ]
        }
    );
    encoding_eq!(
        &codec,
        bconcat!(b"\0\0\0\x04\0\0\x00\x00\0\0\0\x10\x0c\xf06"
            b"\xbd \xbd\x11\xea\xa4\xeb\xe9T\xb4(\x13\x91\0\0\x00\x00\0\0\0\x10"
            b"[\xe3\x9c( \xbd\x11\xea\xaa\xb9g4\x82*\xf1\xc9\0\0\0\x00"
            b"\xFF\xFF\xFF\xFF\0\0\x00\x00\0\0\0\x0c\0\0\0\0\0\0\0\0\0\0\x00\x00"
        ),
        Value::Object {
            shape: outer_shape,
            fields: vec![
                Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391".parse()?)),
                Some(Value::Uuid("5be39c28-20bd-11ea-aab9-6734822af1c9".parse()?)),
                None,
                Some(Value::Set(vec![])),
            ]
        }
    );
    Ok(())
}

#[test]
#[cfg(feature = "num-bigint")]
fn bigint() -> Result<(), Box<dyn Error>> {
    use num_bigint::BigInt;
    use std::convert::TryInto;
    use std::str::FromStr;

    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000110"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(&codec, b"\0\x01\0\0\0\0\0\0\0*", Value::BigInt(42.into()));
    encoding_eq!(
        &codec,
        b"\0\x01\0\x01\0\0\0\0\0\x03",
        Value::BigInt((30000).into())
    );
    encoding_eq!(
        &codec,
        b"\0\x02\0\x01\0\0\0\0\0\x03\0\x01",
        Value::BigInt((30001).into())
    );
    encoding_eq!(
        &codec,
        b"\0\x02\0\x01@\0\0\0\0\x01\x13\x88",
        Value::BigInt((-15000).into())
    );
    encoding_eq!(
        &codec,
        b"\0\x01\0\x05\0\0\0\0\0\n",
        Value::BigInt(BigInt::from_str("1000000000000000000000")?.try_into()?)
    );
    Ok(())
}

#[test]
#[cfg(feature = "bigdecimal")]
fn decimal() -> Result<(), Box<dyn Error>> {
    use bigdecimal::BigDecimal;
    use std::convert::TryInto;
    use std::str::FromStr;

    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000108"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(
        &codec,
        b"\0\x01\0\0\0\0\0\x02\0*",
        Value::Decimal(BigDecimal::from_str("42.00")?.try_into()?)
    );
    encoding_eq!(
        &codec,
        b"\0\x05\0\x01\0\0\0\t\x04\xd2\x16.#4\r\x80\x1bX",
        Value::Decimal(BigDecimal::from_str("12345678.901234567")?.try_into()?)
    );
    encoding_eq!(
        &codec,
        b"\0\x01\0\x19\0\0\0\0\0\x01",
        Value::Decimal(BigDecimal::from_str("1e100")?.try_into()?)
    );
    encoding_eq!(
        &codec,
        b"\0\x06\0\x0b@\0\0\0\0\x07\x01P\x1cB\x08\x9e$!\0\xc8",
        Value::Decimal(
            BigDecimal::from_str("-703367234220692490200000000000000000000000000")?.try_into()?
        )
    );
    encoding_eq!(
        &codec,
        b"\0\x06\0\x0b@\0\0\0\0\x07\x01P\x1cB\x08\x9e$!\0\xc8",
        Value::Decimal(BigDecimal::from_str("-7033672342206924902e26")?.try_into()?)
    );
    Ok(())
}

#[test]
fn bool() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-000000000109"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(&codec, b"\x01", Value::Bool(true));
    encoding_eq!(&codec, b"\x00", Value::Bool(false));
    Ok(())
}

#[test]
fn datetime() -> Result<(), Box<dyn Error>> {
    use std::time::Duration;
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-00000000010a"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(
        &codec,
        b"\0\x02=^\x1bTc\xe7",
        Value::Datetime(Datetime::UNIX_EPOCH + Duration::new(1577109148, 156903000))
    );
    Ok(())
}

#[test]
fn local_datetime() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-00000000010b"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(
        &codec,
        b"\0\x02=^@\xf9\x1f\xfd",
        Value::LocalDatetime(Datetime::from_unix_micros(1577109779709949).into())
    );
    Ok(())
}

#[test]
fn local_date() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-00000000010c"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(
        &codec,
        b"\0\0\x1c\x80",
        Value::LocalDate(LocalDate::from_days(7296))
    );
    Ok(())
}

#[test]
fn vector() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "9565dd88-04f5-11ee-a691-0b6ebe179825"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(
        &codec,
        b"\0\x03\0\0?\x80\0\0@\0\0\0@@\0\0",
        Value::Vector(vec![1., 2., 3.])
    );
    Ok(())
}

#[test]
fn local_time() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-00000000010d"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(
        &codec,
        b"\0\0\0\x0b\xd7\x84\0\x01",
        Value::LocalTime(LocalTime::from_micros(50860392449))
    );
    Ok(())
}

#[test]
fn json() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "00000000-0000-0000-0000-00000000010f"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(
        &codec,
        b"\x01\"txt\"",
        Value::Json(Json::new_unchecked(String::from(r#""txt""#)))
    );
    Ok(())
}

#[test]
fn custom_scalar() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::Scalar(ScalarTypeDescriptor {
                id: "234dc787-2646-11ea-bebd-010d530c06ca"
                    .parse::<Uuid>()?
                    .into(),
                base_type_pos: Some(TypePos(0)),
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }),
        ],
    )?;

    encoding_eq!(&codec, b"xx", Value::Str(String::from("xx")));
    Ok(())
}

#[test]
fn tuple() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(2)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::Tuple(TupleTypeDescriptor {
                id: "6c87a50a-fce2-dcae-6872-8c4c9c4d1e7c"
                    .parse::<Uuid>()?
                    .into(),
                element_types: vec![TypePos(0), TypePos(1)],
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }),
        ],
    )?;

    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(
        &codec,
        bconcat!(b"\0\0\0\x02\0\0\0\x00\0\0\0"
        b"\x08\0\0\0\0\0\0\0\x01\0\0\0\x00\0\0\0\x03str"),
        Value::Tuple(vec![Value::Int64(1), Value::Str("str".into()),])
    );
    Ok(())
}

#[test]
fn named_tuple() -> Result<(), Box<dyn Error>> {
    let elements = vec![
        TupleElement {
            name: "a".into(),
            type_pos: TypePos(0),
        },
        TupleElement {
            name: "b".into(),
            type_pos: TypePos(1),
        },
    ];
    let shape = elements.as_slice().into();
    let codec = build_codec(
        Some(TypePos(2)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::NamedTuple(NamedTupleTypeDescriptor {
                id: "101385c1-d6d5-ec67-eec4-b2b88be8a197"
                    .parse::<Uuid>()?
                    .into(),
                elements,
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }),
        ],
    )?;

    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(
        &codec,
        bconcat!(b"\0\0\0\x02\0\0\0\x00\0\0\0"
        b"\x08\0\0\0\0\0\0\0\x01\0\0\0\x00\0\0\0\x01x"),
        Value::NamedTuple {
            shape,
            fields: vec![Value::Int64(1), Value::Str("x".into()),],
        }
    );
    Ok(())
}

#[test]
fn array() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(1)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::Array(ArrayTypeDescriptor {
                id: "b0105467-a177-635f-e207-0a21867f9be0"
                    .parse::<Uuid>()?
                    .into(),
                type_pos: TypePos(0),
                dimensions: vec![None],
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }),
        ],
    )?;

    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(
        &codec,
        bconcat!(b"\0\0\0\x01\0\0\0\0\0\0\0\x00\0\0\0\x03"
            b"\0\0\0\x01\0\0\0\x08\0\0\0\0\0\0\0\x01"
            b"\0\0\0\x08\0\0\0\0\0\0\0\x02"
            b"\0\0\0\x08\0\0\0\0\0\0\0\x03"),
        Value::Array(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3),])
    );
    encoding_eq!(
        &codec,
        bconcat!(b"\0\0\0\0\0\0\0\0\0\0\0\x00"),
        Value::Array(vec![])
    );
    Ok(())
}

#[test]
fn enums() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::Enumeration(EnumerationTypeDescriptor {
            id: "ac5dc6a4-2656-11ea-aa6d-233f91e80ff6"
                .parse::<Uuid>()?
                .into(),
            members: vec!["x".into(), "y".into()],
            name: None,
            schema_defined: None,
            ancestors: vec![],
        })],
    )?;
    encoding_eq!(&codec, bconcat!(b"x"), Value::Enum("x".into()));
    Ok(())
}

#[test]
fn set_of_arrays() -> Result<(), Box<dyn Error>> {
    let elements = vec![
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: String::from("__tname__"),
            type_pos: TypePos(0),
            source_type_pos: None,
        },
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: String::from("id"),
            type_pos: TypePos(1),
            source_type_pos: None,
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: String::from("sets"),
            type_pos: TypePos(4),
            source_type_pos: None,
        },
    ];
    let shape = ObjectShape::from(&elements[..]);
    let elements = elements.as_slice().into();
    let codec = build_codec(
        Some(TypePos(5)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101"
                    .parse::<Uuid>()?
                    .into(), // str
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100"
                    .parse::<Uuid>()?
                    .into(), // uuid
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105"
                    .parse::<Uuid>()?
                    .into(), // int64
            }),
            Descriptor::Array(ArrayTypeDescriptor {
                id: "b0105467-a177-635f-e207-0a21867f9be0"
                    .parse::<Uuid>()?
                    .into(),
                type_pos: TypePos(2),
                dimensions: vec![None],
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }),
            Descriptor::Set(SetDescriptor {
                id: "499ffd5c-f21b-574d-af8a-1c094c9d6fb0"
                    .parse::<Uuid>()?
                    .into(),
                type_pos: TypePos(3),
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "499ffd5c-f21b-574d-af8a-1c094c9d6fb0"
                    .parse::<Uuid>()?
                    .into(),
                elements,
                ephemeral_free_shape: false,
                type_pos: None,
            }),
        ],
    )?;
    encoding_eq!(
        &codec,
        bconcat!(
        // TODO(tailhook) test with non-zero reserved bytes
        b"\0\0\0\x03\0\0\0\0\0\0\0\x10schema::Function"
        b"\0\0\0\0\0\0\0\x10\xb8\xf2\x91\x99\x8b#\x11"
        b"\xeb\xb9EO\x882\x0e[\xd6\0\0\0\0\0\0\0\x80"
        b"\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\x02\0\0\0\x01\0\0\08"
        b"\0\0\0\x01\0\0\0\0\0\0\0,\0\0\0\x01\0\0\0\0\0\0\0\0"
        b"\0\0\0\x02\0\0\0\x01\0\0\0\x08\0\0\0\0\0\0\0\x01\0\0\0\x08"
        b"\0\0\0\0\0\0\0\x02\0\0\0,\0\0\0\x01\0\0\0\0\0\0\0 "
        b"\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\x01\0\0\0\x08"
        b"\0\0\0\0\0\0\0\x03"),
        Value::Object {
            shape,
            fields: vec![
                Some(Value::Str("schema::Function".into())),
                Some(Value::Uuid("b8f29199-8b23-11eb-b945-4f88320e5bd6".parse()?)),
                Some(Value::Set(vec![
                    Value::Array(vec![Value::Int64(1), Value::Int64(2),]),
                    Value::Array(vec![Value::Int64(3),]),
                ]))
            ]
        }
    );
    Ok(())
}

#[test]
fn range() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(1)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::Range(RangeTypeDescriptor {
                id: "7f8919fd845bb1badae19d40d96ea0a8"
                    .parse::<Uuid>()
                    .unwrap()
                    .into(),
                type_pos: TypePos(0),
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }),
        ],
    )?;

    encoding_eq!(
        &codec,
        b"\x02\0\0\0\x08\0\0\0\0\0\0\0\x07\0\0\0\x08\0\0\0\0\0\0\0'",
        std::ops::Range {
            start: 7i64,
            end: 39
        }
        .into()
    );
    Ok(())
}

#[test]
fn multi_range() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(1)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105"
                    .parse::<Uuid>()?
                    .into(),
            }),
            Descriptor::MultiRange(MultiRangeTypeDescriptor {
                id: "08fc943ff87d44b68e76ba8dbeed4d00"
                    .parse::<Uuid>()
                    .unwrap()
                    .into(),
                type_pos: TypePos(0),
                name: None,
                schema_defined: None,
                ancestors: vec![],
            }),
        ],
    )?;

    encoding_eq!(
        &codec,
        b"\0\0\0\x01\0\0\0\x19\x02\0\0\0\x08\0\0\0\0\0\0\0\x07\0\0\0\x08\0\0\0\0\0\0\0'",
        Value::Array(vec![std::ops::Range {
            start: 7i64,
            end: 39
        }
        .into()])
    );
    Ok(())
}

#[test]
fn postgis_geometry() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "44c901c0-d922-4894-83c8-061bd05e4840"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;

    encoding_eq!(
        &codec,
        /*
         * Point
         * 01 - byteOrder, Little Endian
         * 01000000 - wkbType, WKBPoint
         * 0000000000000040 - x, 2.0
         * 000000000000F03F - y, 1.0
         */
        b"\
        \x01\
        \x01\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        ",
        Value::PostGisGeometry(
            b"\
            \x01\
            \x01\x00\x00\x00\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            "[..]
                .into()
        )
    );
    Ok(())
}

#[test]
fn postgis_geography() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "4d738878-3a5f-4821-ab76-9d8e7d6b32c4"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(
        &codec,
        /*
         * Point
         * 01 - byteOrder, Little Endian
         * 01000000 - wkbType, WKBPoint
         * 0000000000000040 - x, 2.0
         * 000000000000F03F - y, 1.0
         */
        b"\
        \x01\
        \x01\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        ",
        Value::PostGisGeography(
            b"\
            \x01\
            \x01\x00\x00\x00\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            "[..]
                .into()
        )
    );
    Ok(())
}

#[test]
fn postgis_box_2d() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "7fae5536-6311-4f60-8eb9-096a5d972f48"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(
        &codec,
        /*
         * Polygon
         * 01 - byteOrder, Little Endian
         * 03000000 - wkbType, wkbPolygon
         * 01000000 - numRings, 1
         * 05000000 - numPoints, 5
         * 000000000000F03F - x, 1.0
         * 000000000000F03F - y, 1.0
         * 0000000000000040 - x, 2.0
         * 000000000000F03F - y, 1.0
         * 0000000000000040 - x, 2.0
         * 0000000000000040 - y, 2.0
         * 000000000000F03F - x, 1.0
         * 0000000000000040 - y, 2.0
         * 000000000000F03F - x, 1.0
         * 000000000000F03F - y, 1.0
         */
        b"\
        \x01\
        \x03\x00\x00\x00\
        \x01\x00\x00\x00\
        \x05\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        ",
        Value::PostGisBox2d(
            b"\
            \x01\
            \x03\x00\x00\x00\
            \x01\x00\x00\x00\
            \x05\x00\x00\x00\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            "[..]
                .into()
        )
    );
    Ok(())
}

#[test]
fn postgis_box_3d() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(
        Some(TypePos(0)),
        &[Descriptor::BaseScalar(BaseScalarTypeDescriptor {
            id: "c1a50ff8-fded-48b0-85c2-4905a8481433"
                .parse::<Uuid>()?
                .into(),
        })],
    )?;
    encoding_eq!(
        &codec,
        /*
         * Polygon
         * 01 - byteOrder, Little Endian
         * 03000080 - wkbType, wkbPolygonZ
         * 01000000 - numRings, 1
         * 05000000 - numPoints, 5
         * 000000000000F03F - x, 1.0
         * 000000000000F03F - y, 1.0
         * 0000000000000000 - z, 0.0
         * 0000000000000040 - x, 2.0
         * 000000000000F03F - y, 1.0
         * 0000000000000000 - z, 0.0
         * 0000000000000040 - x, 2.0
         * 0000000000000040 - y, 2.0
         * 000000000000F03F - x, 1.0
         * 0000000000000000 - z, 0.0
         * 0000000000000040 - y, 2.0
         * 000000000000F03F - x, 1.0
         * 000000000000F03F - y, 1.0
         * 0000000000000000 - z, 0.0
         */
        b"\
        \x01\
        \x03\x00\x00\x80\
        \x01\x00\x00\x00\
        \x05\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\x00\x40\
        \x00\x00\x00\x00\x00\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\xF0\x3F\
        \x00\x00\x00\x00\x00\x00\x00\x00\
        ",
        Value::PostGisBox3d(
            b"\
            \x01\
            \x03\x00\x00\x80\
            \x01\x00\x00\x00\
            \x05\x00\x00\x00\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\x00\x00\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\x00\x00\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\x00\x00\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\x00\x40\
            \x00\x00\x00\x00\x00\x00\x00\x00\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\xF0\x3F\
            \x00\x00\x00\x00\x00\x00\x00\x00\
            "[..]
                .into()
        )
    );
    Ok(())
}
