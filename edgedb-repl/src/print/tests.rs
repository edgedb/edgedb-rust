use edgedb_protocol::value::Value;
use edgedb_protocol::codec::{ObjectShape, ShapeElement};
use crate::print::test_format;


#[test]
fn int() {
    assert_eq!(test_format(&[Value::Int64(10)], 100).unwrap(), "{10}");
    assert_eq!(test_format(&[
        Value::Int64(10),
        Value::Int64(20),
    ], 100).unwrap(), "{10, 20}");
}

#[test]
fn wrap() {
    assert_eq!(test_format(&[
        Value::Int64(10),
        Value::Int64(20),
    ], 10).unwrap(), "{10, 20}");
    assert_eq!(test_format(&[
        Value::Int64(10),
        Value::Int64(20),
        Value::Int64(30),
    ], 10).unwrap(), "{\n  10,\n  20,\n  30,\n}");
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
    assert_eq!(test_format(&[
        Value::Object { shape: shape.clone(), fields: vec![
            Value::Int32(10),
            Value::Int32(20),
        ]},
        Value::Object { shape: shape.clone(), fields: vec![
            Value::Int32(30),
            Value::Int32(40),
        ]},
    ], 60).unwrap(), r###"{
  Object {field1: 10, field2: 20},
  Object {field1: 30, field2: 40},
}"###);
    assert_eq!(test_format(&[
        Value::Object { shape: shape.clone(), fields: vec![
            Value::Int32(10),
            Value::Int32(20),
        ]},
        Value::Object { shape: shape.clone(), fields: vec![
            Value::Int32(30),
            Value::Int32(40),
        ]},
    ], 20).unwrap(), r###"{
  Object {
    field1: 10,
    field2: 20,
  },
  Object {
    field1: 30,
    field2: 40,
  },
}"###);
}
