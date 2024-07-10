use std::collections::HashMap;

use edgedb_errors::{ClientEncodingError, Error, ErrorKind};

use crate::codec::{ObjectShape, ShapeElement};
use crate::descriptors::Descriptor;
use crate::query_arg::{Encoder, QueryArgs};
use crate::value::Value;

/// An optional [Value] that can be constructed from `impl Into<Value>`,
/// `Option<impl Into<Value>>`, `Vec<impl Into<Value>>` or
/// `Option<Vec<impl Into<Value>>>`.
/// Used by [named_args!](`crate::named_args!`) macro.
#[derive(Clone, Debug, PartialEq)]
pub struct ValueOpt(Option<Value>);

impl<V: Into<Value>> From<V> for ValueOpt {
    fn from(value: V) -> Self {
        ValueOpt(Some(value.into()))
    }
}
impl<V: Into<Value>> From<Option<V>> for ValueOpt
where
    Value: From<V>,
{
    fn from(value: Option<V>) -> Self {
        ValueOpt(value.map(Value::from))
    }
}
impl<V: Into<Value>> From<Vec<V>> for ValueOpt
where
    Value: From<V>,
{
    fn from(value: Vec<V>) -> Self {
        ValueOpt(Some(Value::Array(
            value.into_iter().map(Value::from).collect(),
        )))
    }
}
impl<V: Into<Value>> From<Option<Vec<V>>> for ValueOpt
where
    Value: From<V>,
{
    fn from(value: Option<Vec<V>>) -> Self {
        let mapped = value.map(|value| Value::Array(value.into_iter().map(Value::from).collect()));
        ValueOpt(mapped)
    }
}
impl From<ValueOpt> for Option<Value> {
    fn from(value: ValueOpt) -> Self {
        value.0
    }
}

impl QueryArgs for HashMap<&str, ValueOpt> {
    fn encode(&self, encoder: &mut Encoder) -> Result<(), Error> {
        if self.is_empty() && encoder.ctx.root_pos.is_none() {
            return Ok(());
        }

        let root_pos = encoder.ctx.root_pos.ok_or_else(|| {
            ClientEncodingError::with_message(format!(
                "provided {} named arguments, but no arguments were expected by the server",
                self.len()
            ))
        })?;

        let Descriptor::ObjectShape(target_shape) = encoder.ctx.get(root_pos)? else {
            return Err(ClientEncodingError::with_message(
                "query didn't expect named arguments",
            ));
        };

        let mut shape_elements: Vec<ShapeElement> = Vec::new();
        let mut fields: Vec<Option<Value>> = Vec::new();

        for param_descriptor in target_shape.elements.iter() {
            let value = self.get(param_descriptor.name.as_str());

            let Some(value) = value else {
                return Err(ClientEncodingError::with_message(format!(
                    "argument for ${} missing",
                    param_descriptor.name
                )));
            };

            shape_elements.push(ShapeElement::from(param_descriptor));
            fields.push(value.0.clone());
        }

        Value::Object {
            shape: ObjectShape::new(shape_elements),
            fields,
        }
        .encode(encoder)
    }
}

/// Constructs named query arguments that implement [QueryArgs] so they can be passed
/// into any query method.
/// ```no_run
/// use edgedb_protocol::value::Value;
///
/// let query = "SELECT (<str>$my_str, <int64>$my_int)";
/// let args = edgedb_protocol::named_args! {
///     "my_str" => "Hello world!".to_string(),
///     "my_int" => Value::Int64(42),
/// };
/// ```
///
/// The value side of an argument must be `impl Into<ValueOpt>`.
/// The type of the returned object is `HashMap<&str, ValueOpt>`.
#[macro_export]
macro_rules! named_args {
    ($($key:expr => $value:expr,)+) => { $crate::named_args!($($key => $value),+) };
    ($($key:expr => $value:expr),*) => {
        {
            const CAP: usize = <[()]>::len(&[$({ stringify!($key); }),*]);
            let mut map = ::std::collections::HashMap::<&str, $crate::value_opt::ValueOpt>::with_capacity(CAP);
            $(
                map.insert($key, $crate::value_opt::ValueOpt::from($value));
            )*
            map
        }
    };
}
