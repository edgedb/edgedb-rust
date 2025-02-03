use gel_derive::Queryable;
use gel_protocol::queryable::{Decoder, Queryable};

#[derive(Queryable, Debug, PartialEq)]
enum Status {
    Open,
    Closed,
    Invalid,
}

#[test]
fn enumeration() {
    let dec = Decoder::default();
    assert_eq!(
        Status::decode(&dec, &(), &b"Open"[..]).unwrap(),
        Status::Open
    );
    assert_eq!(
        Status::decode(&dec, &(), &b"Closed"[..]).unwrap(),
        Status::Closed
    );
    assert_eq!(
        Status::decode(&dec, &(), &b"Invalid"[..]).unwrap(),
        Status::Invalid
    );
    Status::decode(&dec, &(), &b"closed"[..]).unwrap_err();
}
