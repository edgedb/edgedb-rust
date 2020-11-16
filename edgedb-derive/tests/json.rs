use edgedb_derive::Queryable;
use edgedb_protocol::queryable::{Queryable, Decoder};
use serde::Deserialize;


#[derive(Debug, PartialEq, Deserialize)]
struct Data {
    field1: u32,
}

#[derive(Queryable, Debug, PartialEq)]
struct ShapeWithJson {
    name: String,
    #[edgedb(json)]
    data: Data,
}

#[derive(Queryable, Deserialize, Debug, PartialEq)]
#[edgedb(json)]
struct JsonRow {
    field2: u32,
}

fn old_decoder() -> Decoder {
    let mut dec = Decoder::default();
    dec.has_implicit_tid = true;
    return dec;
}

#[test]
fn json_field() {
    let data = b"\0\0\0\x04\0\0\x0b\x86\0\0\0\x10\xf2R\
        \x04I\xd7\x04\x11\xea\xaeX\xcf\xdf\xf6\xd0Q\xac\
        \0\0\x0b\x86\0\0\0\x10\xf2\xe6F9\xd7\x04\x11\xea\
        \xa0<\x83\x9f\xd9\xbd\x88\x94\0\0\0\x19\
        \0\0\0\x02id\0\0\x0e\xda\0\0\0\x10\x01{\"field1\": 123}";
    let res = ShapeWithJson::decode(&old_decoder(), data);
    assert_eq!(res.unwrap(), ShapeWithJson {
        name: "id".into(),
        data: Data {
            field1: 123,
        },
    });
}

#[test]
fn json_row() {
    let data = b"\x01{\"field2\": 234}";
    let res = JsonRow::decode(&old_decoder(), data);
    assert_eq!(res.unwrap(), JsonRow {
        field2: 234,
    });
}
