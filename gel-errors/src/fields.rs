use crate::traits::Field;

pub struct QueryText;

impl Field for QueryText {
    const NAME: &'static str = "source_code";
    type Value = String;
}
