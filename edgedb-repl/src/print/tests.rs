use edgedb_protocol::value::Value;
use crate::print::print_to_string;


#[test]
fn int() {
    assert_eq!(print_to_string(&[Value::Int64(10)]).unwrap(), "{10}");
    assert_eq!(print_to_string(&[
        Value::Int64(10),
        Value::Int64(20),
    ]).unwrap(), "{10, 20}");
}
