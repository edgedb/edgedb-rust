use std::fmt;
use std::error::Error;

use edgedb_protocol::value::Value;
use edgedb_protocol::codec;
use edgedb_protocol::descriptors::{InputTypedesc, Descriptor};
use crate::repl;
use crate::prompt;


#[derive(Debug)]
pub struct Canceled;


pub async fn input_variables(desc: &InputTypedesc, state: &mut repl::State)
    -> Result<Value, anyhow::Error>
{
    if desc.is_empty_tuple() {
        return Ok(Value::Tuple(Vec::new()));
    }
    match desc.root() {
        Descriptor::Tuple(tuple) => {
            let mut val = Vec::with_capacity(tuple.element_types.len());
            for (idx, el) in tuple.element_types.iter().enumerate() {
                val.push(input_item(&format!("{}", idx),
                    desc.get(*el)?, desc, state).await?);
            }
            return Ok(Value::Tuple(val));
        }
        Descriptor::NamedTuple(tuple) => {
            todo!();
        }
        root => {
            return Err(anyhow::anyhow!(
                "Unknown input type descriptor: {:?}", root));
        }
    }
}

async fn input_item(name: &str, mut item: &Descriptor, all: &InputTypedesc,
    state: &mut repl::State)
    -> Result<Value, anyhow::Error>
{
    match item {
        Descriptor::Scalar(s) => {
            item = all.get(s.base_type_pos)?;
        }
        _ => {},
    }
    match item {
        Descriptor::BaseScalar(s) => {
            if s.id == codec::STD_STR {
                let val = match state.variable_input(name, "str", "").await {
                    | prompt::Input::Text(val) => val,
                    | prompt::Input::Interrupt
                    | prompt::Input::Eof => Err(Canceled)?,
                };
                Ok(Value::Str(val))
            } else {
                Err(anyhow::anyhow!(
                    "Unimplemented input type {}", s.id))
            }
        }
        _ => Err(anyhow::anyhow!(
                "Unimplemented input type descriptor: {:?}", item)),
    }
}

impl Error for Canceled {
}

impl fmt::Display for Canceled {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "Operation canceled".fmt(f)
    }
}
