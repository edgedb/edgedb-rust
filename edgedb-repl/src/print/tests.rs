use edgedb_protocol::value::Value;
use edgedb_protocol::codec::{ObjectShape, ShapeElement};
use crate::print::{test_format, test_format_cfg, Config};


#[test]
fn int() {
    assert_eq!(test_format(&[Value::Int64(10)]).unwrap(), "{10}");
    assert_eq!(test_format(&[
        Value::Int64(10),
        Value::Int64(20),
    ]).unwrap(), "{10, 20}");
}

#[test]
fn array_ellipsis() {
    assert_eq!(test_format(&[
        Value::Array(vec![
            Value::Int64(10),
            Value::Int64(20),
            Value::Int64(30),
        ]),
    ]).unwrap(), "{[10, 20, 30]}");
    assert_eq!(test_format_cfg(&[
        Value::Array(vec![
            Value::Int64(10),
            Value::Int64(20),
            Value::Int64(30),
        ]),
    ], Config::new().max_items(2)).unwrap(), "{[10, 20, ...]}");
    assert_eq!(test_format_cfg(&[
        Value::Array(vec![
            Value::Int64(10),
            Value::Int64(20),
            Value::Int64(30),
        ]),
    ], Config::new().max_items(2).max_width(10)).unwrap(), r###"{
  [
    10,
    20,
    ... (further results hidden \limit 2)
  ],
}"###);
    assert_eq!(test_format_cfg(&[
        Value::Array(vec![
            Value::Int64(10),
        ]),
    ], Config::new().max_items(2)).unwrap(), "{[10]}");
}

#[test]
fn set_ellipsis() {
    assert_eq!(test_format(&[
        Value::Set(vec![
            Value::Int64(10),
            Value::Int64(20),
            Value::Int64(30),
        ]),
    ]).unwrap(), "{{10, 20, 30}}");
    assert_eq!(test_format_cfg(&[
        Value::Set(vec![
            Value::Int64(10),
            Value::Int64(20),
            Value::Int64(30),
        ]),
    ], Config::new().max_items(2)).unwrap(), "{{10, 20, ...}}");
    assert_eq!(test_format_cfg(&[
        Value::Set(vec![
            Value::Int64(10),
        ]),
    ], Config::new().max_items(2)).unwrap(), "{{10}}");
}

#[test]
fn wrap() {
    assert_eq!(test_format_cfg(&[
        Value::Int64(10),
        Value::Int64(20),
    ], Config::new().max_width(10)).unwrap(), "{10, 20}");
    assert_eq!(test_format_cfg(&[
        Value::Int64(10),
        Value::Int64(20),
        Value::Int64(30),
    ], Config::new().max_width(10)).unwrap(), "{\n  10,\n  20,\n  30,\n}");
}

#[test]
fn object() {
    let shape = ObjectShape::new(vec![
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            name: "field1".into(),
        },
        ShapeElement {
            flag_implicit: false,
            flag_link_property: false,
            flag_link: false,
            name: "field2".into(),
        }
    ]);
    assert_eq!(test_format_cfg(&[
        Value::Object { shape: shape.clone(), fields: vec![
            Some(Value::Int32(10)),
            Some(Value::Int32(20)),
        ]},
        Value::Object { shape: shape.clone(), fields: vec![
            Some(Value::Int32(30)),
            Some(Value::Int32(40)),
        ]},
    ], Config::new().max_width(60)).unwrap(), r###"{
  Object {field1: 10, field2: 20},
  Object {field1: 30, field2: 40},
}"###);
    assert_eq!(test_format_cfg(&[
        Value::Object { shape: shape.clone(), fields: vec![
            Some(Value::Int32(10)),
            Some(Value::Int32(20)),
        ]},
        Value::Object { shape: shape.clone(), fields: vec![
            Some(Value::Int32(30)),
            None,
        ]},
    ], Config::new().max_width(20)).unwrap(), r###"{
  Object {
    field1: 10,
    field2: 20,
  },
  Object {
    field1: 30,
    field2: {},
  },
}"###);
}

#[test]
fn str() {
    assert_eq!(
        test_format(&[Value::Str("hello".into())]).unwrap(),
        r#"{'hello'}"#);
    assert_eq!(
        test_format(&[Value::Str("a\nb".into())]).unwrap(),
        "{\n  'a\nb',\n}");
}
