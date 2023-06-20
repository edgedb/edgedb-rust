use std::default::Default;

use edgedb_protocol::queryable::Queryable;
use edgedb_protocol::model::Vector;


#[test]
fn decode_vector() {
    let vec = Vector::decode(&Default::default(),
        b"\0\x03\0\0?\x80\0\0@\0\0\0@@\0\0"
    ).unwrap();
    assert_eq!(vec, Vector(vec![1.,2.,3.]));
}
