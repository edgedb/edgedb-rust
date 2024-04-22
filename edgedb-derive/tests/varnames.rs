use edgedb_derive::Queryable;
use edgedb_protocol::queryable::{Decoder, Queryable};

#[derive(Queryable, Debug, PartialEq)]
struct WeirdStruct {
    nfields: i64,
    elements: String,
    decoder: String,
    buf: i64,
}

#[test]
fn decode() {
    let data = b"\0\0\0\x04\0\0\0\x14\0\0\0\x08\0\0\0\0\0\0\x03\0\0\0\
                  \0\x19\0\0\0\0\0\0\0\x19\0\0\0\x0bSomeDecoder\
                  \0\0\0\x14\0\0\0\x08\0\0\0\0\0\0\0{";
    let res = WeirdStruct::decode(&Decoder::default(), data);
    assert_eq!(
        res.unwrap(),
        WeirdStruct {
            decoder: "SomeDecoder".into(),
            buf: 123,
            nfields: 768,
            elements: "".into(),
        }
    );
}
