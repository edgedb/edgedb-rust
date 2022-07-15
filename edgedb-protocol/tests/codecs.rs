#[macro_use] extern crate pretty_assertions;

use std::error::Error;
use std::{i16, i32, i64};
use std::sync::Arc;

use bytes::Bytes;

use edgedb_protocol::codec::{build_codec};
use edgedb_protocol::codec::{Codec, ObjectShape};
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::value::{Value, SparseObject};
use edgedb_protocol::model::{LocalDatetime, LocalDate, LocalTime, Duration};
use edgedb_protocol::model::{Datetime, RelativeDuration};
use edgedb_protocol::descriptors::{Descriptor, TypePos};
use edgedb_protocol::descriptors::BaseScalarTypeDescriptor;
use edgedb_protocol::descriptors::{ObjectShapeDescriptor, ShapeElement};
use edgedb_protocol::descriptors::{SetDescriptor};
use edgedb_protocol::descriptors::{ScalarTypeDescriptor};
use edgedb_protocol::descriptors::{TupleTypeDescriptor};
use edgedb_protocol::descriptors::{NamedTupleTypeDescriptor, TupleElement};
use edgedb_protocol::descriptors::ArrayTypeDescriptor;
use edgedb_protocol::descriptors::EnumerationTypeDescriptor;
use edgedb_protocol::server_message::StateDataDescription;

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
    Ok(codec.decode(data)?)
}

#[test]
fn int16() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000103".parse()?,
            })
        ]
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
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000104".parse()?,
            })
        ]
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
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105".parse()?,
            })
        ]
    )?;
    encoding_eq!(&codec, b"\0\0\0\0\0\0\0\0", Value::Int64(0));
    encoding_eq!(&codec, b"\0\0\0\0\0\0\x01\x05", Value::Int64(0x105));
    encoding_eq!(&codec, b"\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
               Value::Int64(i64::MAX));
    encoding_eq!(&codec, b"\x80\x00\x00\x00\x00\x00\x00\x00",
               Value::Int64(i64::MIN));
    encoding_eq!(&codec, b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
               Value::Int64(-1));
    Ok(())
}

#[test]
fn float32() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000106".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\0\0\0\0", Value::Float32(0.0));
    encoding_eq!(&codec, b"\x80\0\0\0", Value::Float32(-0.0));
    encoding_eq!(&codec, b"?\x80\0\0", Value::Float32(1.0));
    encoding_eq!(&codec, b"\xbf\x8f\xbew", Value::Float32(-1.123));

    match decode(&codec, b"\x7f\xc0\0\0")? {
        Value::Float32(val) => assert!(val.is_nan()),
        _ => panic!("could not parse NaN")
    };

    match decode(&codec, b"\x7f\x80\0\0")? {
        Value::Float32(val) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_positive())
        },
        _ => panic!("could not parse +inf")
    };

    match decode(&codec, b"\xff\x80\0\0")? {
        Value::Float32(val) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_negative())
        }
        _ => panic!("could not parse -inf")
    };

    Ok(())
}

#[test]
fn float64() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000107".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\0\0\0\0\0\0\0\0", Value::Float64(0.0));
    encoding_eq!(&codec, b"\x80\0\0\0\0\0\0\0", Value::Float64(-0.0));
    encoding_eq!(&codec, b"?\xf0\0\0\0\0\0\0", Value::Float64(1.0));
    encoding_eq!(&codec, b"T\xb2I\xad%\x94\xc3}", Value::Float64(1e100));

    match decode(&codec, b"\x7f\xf8\0\0\0\0\0\0")? {
        Value::Float64(val) => assert!(val.is_nan()),
        _ => panic!("could not parse NaN")
    };

    match decode(&codec, b"\x7f\xf0\0\0\0\0\0\0")? {
        Value::Float64(val) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_positive())
        }
        _ => panic!("could not parse +inf")
    };

    match decode(&codec, b"\xff\xf0\0\0\0\0\0\0")? {
        Value::Float64(val) => {
            assert!(val.is_infinite());
            assert!(val.is_sign_negative())
        },
        _ => panic!("could not parse -inf")
    };

    Ok(())
}

#[test]
fn str() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101".parse()?,
            })
        ]
    )?;
    encoding_eq!(&codec, b"hello", Value::Str(String::from("hello")));
    encoding_eq!(&codec, b"", Value::Str(String::from("")));
    encoding_eq!(&codec, b"\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82",
        Value::Str(String::from("привет")));
    Ok(())
}

#[test]
fn bytes() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000102".parse()?,
            })
        ]
    )?;
    encoding_eq!(&codec, b"hello", Value::Bytes(b"hello".to_vec()));
    encoding_eq!(&codec, b"", Value::Bytes(b"".to_vec()));
    encoding_eq!(&codec, b"\x00\x01\x02\x03\x81",
        Value::Bytes(b"\x00\x01\x02\x03\x81".to_vec()));
    Ok(())
}

#[test]
fn uuid() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100".parse()?,
            })
        ]
    )?;
    encoding_eq!(&codec, b"I(\xcc\x1e e\x11\xea\x88H{S\xa6\xad\xb3\x83",
               Value::Uuid("4928cc1e-2065-11ea-8848-7b53a6adb383".parse()?));
    Ok(())
}

#[test]
fn duration() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-00000000010e".parse()?,
            })
        ]
    )?;

    // SELECT <datetime>'2019-11-29T00:00:00Z'-<datetime>'2000-01-01T00:00:00Z'
    encoding_eq!(&codec, b"\0\x02;o\xad\xff\0\0\0\0\0\0\0\0\0\0",
               Value::Duration(Duration::from_micros(7272*86400*1000_000)));
    // SELECT <datetime>'2019-11-29T00:00:00Z'-<datetime>'2019-11-28T01:00:00Z'
    encoding_eq!(&codec, b"\0\0\0\x13GC\xbc\0\0\0\0\0\0\0\0\0",
               Value::Duration(Duration::from_micros(82800*1000_000)));
    encoding_eq!(&codec, b"\xff\xff\xff\xff\xd3,\xba\xe0\0\0\0\0\0\0\0\0",
               Value::Duration(Duration::from_micros(-752043296)));

    assert_eq!(
        decode(&codec, b"\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0")
            .unwrap_err().to_string(),
           "non-zero reserved bytes received in data");
    Ok(())
}

#[test]
fn relative_duration() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000111".parse()?,
            })
        ]
    )?;


    // SELECT <cal::relative_duration>
    // '2 years 7 months 16 days 48 hours 45 minutes 7.6 seconds'
    encoding_eq!(&codec, b"\0\0\0\x28\xdd\x11\x72\x80\0\0\0\x10\0\0\0\x1f",
       Value::RelativeDuration(
           RelativeDuration::from_years(2) +
           RelativeDuration::from_months(7) +
           RelativeDuration::from_days(16) +
           RelativeDuration::from_hours(48) +
           RelativeDuration::from_minutes(45) +
           RelativeDuration::from_secs(7) +
           RelativeDuration::from_millis(600)
    ));
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
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: String::from("id"),
            type_pos: TypePos(0),
        },
    ];
    let shape = elements.as_slice().into();
    let codec = build_codec(Some(TypePos(1)),
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
                Some(Value::Uuid("30576400-2064-11ea-98c5-33c5cfb4725e"
                    .parse()?)),
                Some(Value::Uuid("4928cc1e-2065-11ea-8848-7b53a6adb383"
                    .parse()?)),
            ]
        });
    Ok(())
}

#[test]
fn input_codec() -> Result<(), Box<dyn Error>> {
    let sdd = StateDataDescription {
        typedesc_id: "fd6c3b17504a714858ec2282431ce72c".parse()?,
        typedesc: Bytes::from_static(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\
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
            o\0\0\0\x06config\0\x0e"),
    };
    let out_desc = sdd.parse(&ProtocolVersion::current())?;
    let codec = build_codec(Some(TypePos(16)),
        &out_desc.descriptors(),
    )?;
    encoding_eq!(&codec,
        b"\0\0\0\x03\0\0\0\0\0\0\0\x07default\0\0\0\x02\0\0\0\x1c\
            \0\0\0\x01\0\0\0\0\0\0\0\x10GLOBAL VAR VALUE\
            \0\0\0\x03\0\0\0\x1c\0\0\0\x01\0\0\0\t\0\0\0\x10\
            \0\0\0\0\x11\xe1\xa3\0\0\0\0\0\0\0\0\0",
        Value::SparseObject(SparseObject::from_pairs([
            ("module", Some(Value::Str("default".into()))),
            ("globals", Some(Value::SparseObject(SparseObject::from_pairs([
                ("default::my_globalvar_1",
                 Some(Value::Str("GLOBAL VAR VALUE".into()))),
            ])))),
            ("config", Some(Value::SparseObject(SparseObject::from_pairs([
                ("session_idle_transaction_timeout",
                 Some(Value::Duration(Duration::from_micros(300_000_000)))),
            ])))),
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
        },
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "id".into(),
            type_pos: TypePos(0),
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "first_name".into(),
            type_pos: TypePos(1),
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
        },
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "id".into(),
            type_pos: TypePos(0),
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: "first_name".into(),
            type_pos: TypePos(1),
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: true,
            cardinality: None,
            name: "collegues".into(),
            type_pos: TypePos(3),
        },
    ];
    let inner_shape = ObjectShape::from(&inner_elements[..]);
    let outer_shape = ObjectShape::from(&outer_elements[..]);
    let codec = build_codec(Some(TypePos(4)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100".parse()?,
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101".parse()?,
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "8faa7193-48c6-4263-18d3-1a127652569b".parse()?,
                elements: inner_elements,
            }),
            Descriptor::Set(SetDescriptor {
                id: "afbb389d-aa73-2aae-9310-84a9163cb5ed".parse()?,
                type_pos: TypePos(2),
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "9740ff04-324e-08a4-4ac7-2192d72c6967".parse()?,
                elements: outer_elements,
            }),
        ]
    )?;
    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(&codec, bconcat!(
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
    ), Value::Object {
        shape: outer_shape.clone(), fields: vec![
            Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391".parse()?)),
            Some(Value::Uuid("5be39c28-20bd-11ea-aab9-6734822af1c9".parse()?)),
            Some(Value::Str(String::from("Ryan"))),
            Some(Value::Set(vec![
                Value::Object {
                    shape: inner_shape.clone(),
                    fields: vec![
                        Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391"
                                         .parse()?)),
                        Some(Value::Uuid("5be39e80-20bd-11ea-aab9-175dbf1847e5"
                                    .parse()?)),
                        Some(Value::Str(String::from("Ana"))),
                ]},
                Value::Object {
                    shape: inner_shape,
                    fields: vec![
                        Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391"
                                         .parse()?)),
                        Some(Value::Uuid("5be39714-20bd-11ea-aab9-3f37e720b854"
                                         .parse()?)),
                        Some(Value::Str(String::from("Harrison"))),
                    ]
                }])),
            ]
    });
    encoding_eq!(&codec, bconcat!(b"\0\0\0\x04\0\0\x00\x00\0\0\0\x10\x0c\xf06"
        b"\xbd \xbd\x11\xea\xa4\xeb\xe9T\xb4(\x13\x91\0\0\x00\x00\0\0\0\x10"
        b"[\xe3\x9c( \xbd\x11\xea\xaa\xb9g4\x82*\xf1\xc9\0\0\0\x00"
        b"\0\0\0\x04Ryan\0\0\x00\x00\0\0\0\x0c\0\0\0\0\0\0\0\0\0\0\x00\x00"
    ), Value::Object {
        shape: outer_shape.clone(), fields: vec![
            Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391".parse()?)),
            Some(Value::Uuid("5be39c28-20bd-11ea-aab9-6734822af1c9".parse()?)),
            Some(Value::Str(String::from("Ryan"))),
            Some(Value::Set(vec![])),
        ]
    });
    encoding_eq!(&codec, bconcat!(b"\0\0\0\x04\0\0\x00\x00\0\0\0\x10\x0c\xf06"
        b"\xbd \xbd\x11\xea\xa4\xeb\xe9T\xb4(\x13\x91\0\0\x00\x00\0\0\0\x10"
        b"[\xe3\x9c( \xbd\x11\xea\xaa\xb9g4\x82*\xf1\xc9\0\0\0\x00"
        b"\xFF\xFF\xFF\xFF\0\0\x00\x00\0\0\0\x0c\0\0\0\0\0\0\0\0\0\0\x00\x00"
    ), Value::Object {
        shape: outer_shape, fields: vec![
            Some(Value::Uuid("0cf036bd-20bd-11ea-a4eb-e954b4281391".parse()?)),
            Some(Value::Uuid("5be39c28-20bd-11ea-aab9-6734822af1c9".parse()?)),
            None,
            Some(Value::Set(vec![])),
        ]
    });
    Ok(())
}

#[test]
#[cfg(feature="num-bigint")]
fn bigint() -> Result<(), Box<dyn Error>> {
    use num_bigint::BigInt;
    use std::convert::TryInto;
    use std::str::FromStr;

    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000110".parse()?,
                },
            ),
        ]
    )?;
    encoding_eq!(&codec, b"\0\x01\0\0\0\0\0\0\0*", Value::BigInt(42.into()));
    encoding_eq!(&codec, b"\0\x01\0\x01\0\0\0\0\0\x03",
        Value::BigInt((30000).into()));
    encoding_eq!(&codec, b"\0\x02\0\x01\0\0\0\0\0\x03\0\x01",
        Value::BigInt((30001).into()));
    encoding_eq!(&codec, b"\0\x02\0\x01@\0\0\0\0\x01\x13\x88",
        Value::BigInt((-15000).into()));
    encoding_eq!(&codec, b"\0\x01\0\x05\0\0\0\0\0\n",
        Value::BigInt(
            BigInt::from_str("1000000000000000000000")?.try_into()?));
    Ok(())
}

#[test]
#[cfg(feature="bigdecimal")]
fn decimal() -> Result<(), Box<dyn Error>> {
    use bigdecimal::BigDecimal;
    use std::convert::TryInto;
    use std::str::FromStr;

    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000108".parse()?,
                },
            ),
        ]
    )?;
    encoding_eq!(&codec, b"\0\x01\0\0\0\0\0\x02\0*",
        Value::Decimal(BigDecimal::from_str("42.00")?.try_into()?));
    encoding_eq!(&codec, b"\0\x05\0\x01\0\0\0\t\x04\xd2\x16.#4\r\x80\x1bX",
        Value::Decimal(
            BigDecimal::from_str("12345678.901234567")?.try_into()?));
    encoding_eq!(&codec, b"\0\x01\0\x19\0\0\0\0\0\x01",
        Value::Decimal(BigDecimal::from_str("1e100")?.try_into()?));
    encoding_eq!(&codec,
        b"\0\x06\0\x0b@\0\0\0\0\x07\x01P\x1cB\x08\x9e$!\0\xc8",
        Value::Decimal(BigDecimal::from_str(
            "-703367234220692490200000000000000000000000000")?.try_into()?));
    encoding_eq!(&codec,
        b"\0\x06\0\x0b@\0\0\0\0\x07\x01P\x1cB\x08\x9e$!\0\xc8",
        Value::Decimal(BigDecimal::from_str(
            "-7033672342206924902e26")?.try_into()?));
    Ok(())
}

#[test]
fn bool() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000109".parse()?,
                },
            ),
        ]
    )?;
    encoding_eq!(&codec, b"\x01", Value::Bool(true));
    encoding_eq!(&codec, b"\x00", Value::Bool(false));
    Ok(())
}

#[test]
fn datetime() -> Result<(), Box<dyn Error>> {
    use std::time::Duration;
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-00000000010a".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\0\x02=^\x1bTc\xe7",
        Value::Datetime(
            Datetime::UNIX_EPOCH + Duration::new(1577109148, 156903000)));
    Ok(())
}

#[test]
fn local_datetime() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-00000000010b".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\0\x02=^@\xf9\x1f\xfd",
        Value::LocalDatetime(LocalDatetime::from_micros(630424979709949)));
    Ok(())
}

#[test]
fn local_date() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-00000000010c".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\0\0\x1c\x80",
        Value::LocalDate(LocalDate::from_days(7296)));
    Ok(())
}

#[test]
fn local_time() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-00000000010d".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\0\0\0\x0b\xd7\x84\0\x01",
        Value::LocalTime(LocalTime::from_micros(50860392449)));
    Ok(())
}

#[test]
fn json() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-00000000010f".parse()?,
            })
        ]
    )?;

    encoding_eq!(&codec, b"\x01\"txt\"",
        Value::Json(String::from(r#""txt""#)));
    Ok(())
}

#[test]
fn custom_scalar() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000101".parse()?,
                },
            ),
            Descriptor::Scalar(
                ScalarTypeDescriptor {
                    id: "234dc787-2646-11ea-bebd-010d530c06ca".parse()?,
                    base_type_pos: TypePos(0),
                },
            ),
        ]
    )?;

    encoding_eq!(&codec, b"xx",
        Value::Str(String::from("xx")));
    Ok(())
}

#[test]
fn tuple() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(2)),
        &[
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000105".parse()?,
                },
            ),
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000101".parse()?,
                },
            ),
            Descriptor::Tuple(
                TupleTypeDescriptor {
                    id: "6c87a50a-fce2-dcae-6872-8c4c9c4d1e7c".parse()?,
                    element_types: vec![TypePos(0), TypePos(1)],
                },
            ),
        ],
    )?;

    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(&codec, bconcat!(b"\0\0\0\x02\0\0\0\x00\0\0\0"
        b"\x08\0\0\0\0\0\0\0\x01\0\0\0\x00\0\0\0\x03str"),
        Value::Tuple(vec![
            Value::Int64(1),
            Value::Str("str".into()),
        ]));
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
    let codec = build_codec(Some(TypePos(2)),
        &[
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000105".parse()?,
                },
            ),
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000101".parse()?,
                },
            ),
            Descriptor::NamedTuple(
                    NamedTupleTypeDescriptor {
                        id: "101385c1-d6d5-ec67-eec4-b2b88be8a197".parse()?,
                        elements,
                    },
                ),
        ],
    )?;

    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(&codec, bconcat!(b"\0\0\0\x02\0\0\0\x00\0\0\0"
        b"\x08\0\0\0\0\0\0\0\x01\0\0\0\x00\0\0\0\x01x"),
        Value::NamedTuple {
            shape,
            fields: vec![
                Value::Int64(1),
                Value::Str("x".into()),
            ],
        });
    Ok(())
}

#[test]
fn array() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(1)),
        &[
            Descriptor::BaseScalar(
                BaseScalarTypeDescriptor {
                    id: "00000000-0000-0000-0000-000000000105".parse()?,
                },
            ),
            Descriptor::Array(
                ArrayTypeDescriptor {
                    id: "b0105467-a177-635f-e207-0a21867f9be0".parse()?,
                    type_pos: TypePos(0),
                    dimensions: vec![None],
                },
            ),
        ],
    )?;

    // TODO(tailhook) test with non-zero reserved bytes
    encoding_eq!(&codec, bconcat!(b"\0\0\0\x01\0\0\0\0\0\0\0\x00\0\0\0\x03"
            b"\0\0\0\x01\0\0\0\x08\0\0\0\0\0\0\0\x01"
            b"\0\0\0\x08\0\0\0\0\0\0\0\x02"
            b"\0\0\0\x08\0\0\0\0\0\0\0\x03"),
        Value::Array(vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Int64(3),
        ]));
    encoding_eq!(&codec, bconcat!(b"\0\0\0\0\0\0\0\0\0\0\0\x00"),
        Value::Array(vec![]));
    Ok(())
}

#[test]
fn enums() -> Result<(), Box<dyn Error>> {
    let codec = build_codec(Some(TypePos(0)),
        &[
            Descriptor::Enumeration(
                EnumerationTypeDescriptor {
                    id: "ac5dc6a4-2656-11ea-aa6d-233f91e80ff6".parse()?,
                    members: vec![
                        "x".into(),
                        "y".into(),
                    ],
                },
            ),
        ]
    )?;
    encoding_eq!(&codec, bconcat!(b"x"),
        Value::Enum("x".into()));
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
        },
        ShapeElement {
            flag_implicit: true,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: String::from("id"),
            type_pos: TypePos(1),
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            cardinality: None,
            name: String::from("sets"),
            type_pos: TypePos(4),
        },
    ];
    let shape = ObjectShape::from(&elements[..]);
    let elements = elements.as_slice().into();
    let codec = build_codec(Some(TypePos(5)),
        &[
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000101".parse()?, // str
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000100".parse()?, // uuid
            }),
            Descriptor::BaseScalar(BaseScalarTypeDescriptor {
                id: "00000000-0000-0000-0000-000000000105".parse()?, // int64
            }),
            Descriptor::Array(ArrayTypeDescriptor {
                id: "b0105467-a177-635f-e207-0a21867f9be0".parse()?,
                type_pos: TypePos(2),
                dimensions: vec![None],
            }),
            Descriptor::Set(SetDescriptor {
                id: "499ffd5c-f21b-574d-af8a-1c094c9d6fb0".parse()?,
                type_pos: TypePos(3),
            }),
            Descriptor::ObjectShape(ObjectShapeDescriptor {
                id: "499ffd5c-f21b-574d-af8a-1c094c9d6fb0".parse()?,
                elements,
            }),
        ]
    )?;
    encoding_eq!(&codec, bconcat!(
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
                    Value::Array(vec![
                        Value::Int64(1),
                        Value::Int64(2),
                    ]),
                    Value::Array(vec![
                        Value::Int64(3),
                    ]),
                ]))
            ]
        });
    Ok(())
}
