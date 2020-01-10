use anyhow;
use async_std::sync::{Sender, Receiver};
use async_std::task;
use rustyline::{self, error::ReadlineError};
use rustyline::{Helper, Context};
use rustyline::hint::Hinter;
use rustyline::highlight::Highlighter;
use rustyline::validate::{Validator, ValidationResult, ValidationContext};
use rustyline::completion::Completer;

use edgeql_parser::preparser::full_statement;
use crate::commands::backslash;

use colorful::Colorful;


pub enum Control {
    Input(String, String),
}

pub enum Input {
    Text(String),
    Eof,
    Interrupt,
}

pub struct EdgeqlHelper {
}

impl Helper for EdgeqlHelper {}
impl Hinter for EdgeqlHelper {
    fn hint(&self, line: &str, pos: usize, _ctx: &Context) -> Option<String> {
        // TODO(tailhook) strip leading whitespace
        // TODO(tailhook) hint argument name if not on the end of line
        if line.starts_with("\\") && pos == line.len() {
            let mut hint = None;
            for item in backslash::HINTS {
                if item.starts_with(line) {
                    if hint.is_some() {
                        // more than one item matches
                        hint = None;
                        break;
                    } else {
                        hint = Some(item);
                    }
                }
            }
            if let Some(hint) = hint {
                return Some(hint[line.len()..].into())
            }
        }
        return None;
    }
}
impl Highlighter for EdgeqlHelper {
    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        return hint.light_gray().to_string().into()
    }
}
impl Validator for EdgeqlHelper {
    fn validate(&self, ctx: &mut ValidationContext)
        -> Result<ValidationResult, ReadlineError>
    {
        let line = ctx.input().trim();
        if line.starts_with("\\") {
            match backslash::parse(line) {
                Ok(_) => Ok(ValidationResult::Valid(None)),
                Err(e) => {
                    Ok(ValidationResult::Invalid(Some(
                        format!("  ← {}", e.hint))))
                }
            }
        } else {
            if full_statement(ctx.input().as_bytes()).is_ok() {
                Ok(ValidationResult::Valid(None))
            } else {
                Ok(ValidationResult::Incomplete)
            }
        }
    }
}
impl Completer for EdgeqlHelper {
    type Candidate = String;
    fn complete(&self, line: &str, pos: usize, _ctx: &Context)
        -> Result<(usize, Vec<Self::Candidate>), ReadlineError>
    {
        // TODO(tailhook) strip leading whitespace
        // TODO(tailhook) argument completion
        if line.starts_with("\\") && pos == line.len() {
            let mut options = Vec::new();
            for item in backslash::COMMAND_NAMES {
                if item.starts_with(line) {
                    options.push((*item).into());
                }
            }
            return Ok((0, options))
        }
        Ok((pos, Vec::new()))
    }
}


pub fn main(data: Sender<Input>, control: Receiver<Control>)
    -> Result<(), anyhow::Error>
{
    let mut editor = rustyline::Editor::<EdgeqlHelper>::new();
    editor.set_helper(Some(EdgeqlHelper {}));
    let mut prompt = String::from("> ");
    let mut initial;
    loop {
        loop {
            match task::block_on(control.recv()) {
                None => return Ok(()),
                Some(Control::Input(name, prefix)) => {
                    prompt.clear();
                    prompt.push_str(&name);
                    prompt.push_str("> ");
                    initial = prefix;
                    break;
                }
            }
        }
        let text = match editor.readline_with_initial(&prompt, (&initial, ""))
        {
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
