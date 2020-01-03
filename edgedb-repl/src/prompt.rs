use anyhow;
use async_std::sync::{Sender, Receiver};
use async_std::task;
use rustyline::{self, error::ReadlineError};
use rustyline::{Helper, Context};
use rustyline::hint::Hinter;
use rustyline::line_buffer::LineBuffer;
use rustyline::highlight::Highlighter;
use rustyline::validate::{Validator, ValidationResult};
use rustyline::completion::Completer;

use edgeql_parser::preparser::full_statement;


pub enum Control {
    Input(String),
}

pub enum Input {
    Text(String),
    Eof,
    Interrupt,
}

pub struct EdgeqlHelper {
}

impl Helper for EdgeqlHelper {}
impl Hinter for EdgeqlHelper {}
impl Highlighter for EdgeqlHelper {}
impl Validator for EdgeqlHelper {
    fn validate(&self, line: &mut LineBuffer) -> ValidationResult {
        if full_statement(line.as_str().as_bytes()).is_ok() {
            return ValidationResult::Valid(None)
        } else {
            return ValidationResult::Incomplete
        }
    }
}
impl Completer for EdgeqlHelper {
    type Candidate = String;
    fn complete(&self, _line: &str, pos: usize, _ctx: &Context)
        -> Result<(usize, Vec<Self::Candidate>), ReadlineError>
    {
        Ok((pos, Vec::new()))
    }
}


pub fn main(data: Sender<Input>, control: Receiver<Control>)
    -> Result<(), anyhow::Error>
{
    let mut editor = rustyline::Editor::<EdgeqlHelper>::new();
    editor.set_helper(Some(EdgeqlHelper {}));
    let mut prompt = String::from("> ");
    loop {
        loop {
            match task::block_on(control.recv()) {
                None => return Ok(()),
                Some(Control::Input(name)) => {
                    prompt.clear();
                    prompt.push_str(&name);
                    prompt.push_str("> ");
                    break;
                }
            }
        }
        let text = match editor.readline(&prompt) {
            Ok(text) => text,
            Err(ReadlineError::Eof) => {
                task::block_on(data.send(Input::Eof));
                continue;
            }
            Err(ReadlineError::Interrupted) => {
                task::block_on(data.send(Input::Interrupt));
                continue;
            }
            Err(e) => Err(e)?,
        };
        editor.add_history_entry(&text);
        task::block_on(data.send(Input::Text(text)))
    }
}
