use edgedb_derive::Queryable;
use edgedb_protocol::queryable::{Queryable, Decoder};

#[derive(Queryable, Debug, PartialEq)]
struct ScalarType {
    name: String,
    extending: String,
    kind: String,
}

fn old_decoder() -> Decoder {
    let mut dec = Decoder::default();
    dec.has_implicit_tid = true;
    return dec;
}

#[test]
fn decode_new() {
    let data = b"\0\0\0\x04\0\0\x0b\x86\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\
        \x01\x0c\0\0\0\x19\0\0\0\x0fcal::local_date\
        \0\0\0\x19\0\0\0\x0estd::anyscalar\0\0\0\x19\0\0\0\x06normal";
    let res = ScalarType::decode(&Decoder::default(), data);
    assert_eq!(res.unwrap(), ScalarType {
        name: "cal::local_date".into(),
        extending: "std::anyscalar".into(),
        kind: "normal".into(),
    });
}


#[test]
fn decode_old() {
    let data = b"\0\0\0\x05\0\0\x0b\x86\
        \0\0\0\x10\xb2\xa1\x94\xfb\t\xa4\x11\xeb\x9d\x97\xf9'\
        \xee\xfc\xb6\x12\0\0\x0b\x86\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\
        \x01\x0c\0\0\0\x19\0\0\0\x0fcal::local_date\
        \0\0\0\x19\0\0\0\x0estd::anyscalar\0\0\0\x19\0\0\0\x06normal";
    let res = ScalarType::decode(&old_decoder(), data);
    assert_eq!(res.unwrap(), ScalarType {
        name: "cal::local_date".into(),
        extending: "std::anyscalar".into(),
        kind: "normal".into(),
    });
}

