use edgedb_protocol::value::Value;
use crate::print::print_to_string;


#[test]
fn int() {
    assert_eq!(print_to_string(&[Value::Int64(10)]).unwrap(), "{10, }");
}
