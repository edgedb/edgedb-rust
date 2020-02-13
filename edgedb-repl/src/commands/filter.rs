use edgedb_protocol::value::Value;

pub fn pattern_to_value(pattern: &Option<String>, case_sensitive: bool)
    -> Value
{
    match pattern {
        Some(pattern) => {
            let pattern = if case_sensitive {
                pattern.clone()
            } else {
                String::from("(?i)") + pattern
            };
            Value::Tuple(vec![Value::Str(pattern)])
        }
        None => Value::Tuple(Vec::new()),
    }
}
