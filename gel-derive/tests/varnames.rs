use gel_derive::Queryable;
use gel_protocol::queryable::{Decoder, Queryable};

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
    let order = vec![0, 1, 2, 3];
    let res = WeirdStruct::decode(&Decoder::default(), &order, data);
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
