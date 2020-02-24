use std::borrow::Cow;
use std::fs;
use std::io::ErrorKind;

use anyhow::{self, Context as _Context};
use async_std::sync::{Sender, Receiver};
use async_std::task;
use dirs::data_local_dir;
use rustyline::{self, error::ReadlineError, KeyPress, Cmd};
use rustyline::{Editor, Config, Helper, Context};
use rustyline::config::EditMode;
use rustyline::hint::Hinter;
use rustyline::highlight::Highlighter;
use rustyline::validate::{Validator, ValidationResult, ValidationContext};
use rustyline::completion::Completer;

use edgeql_parser::preparser::full_statement;
use edgeql_parser::tokenizer::{TokenStream, Kind};
use crate::commands::backslash;
use crate::print::style::{Styler, Style};

use colorful::Colorful;


pub enum Control {
    EdgeqlInput { database: String, initial: String },
    VariableInput { name: String, type_name: String, initial: String },
    ViMode,
    EmacsMode,
}

pub enum Input {
    Text(String),
    Eof,
    Interrupt,
}

pub struct EdgeqlHelper {
    styler: Styler,
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

fn emit_insignificant(buf: &mut String, styler: &Styler, mut chunk: &str) {
    while let Some(pos) = chunk.find('#') {
        if let Some(end) = chunk[pos..].find('\n') {
            buf.push_str(&chunk[..pos]);
            styler.apply(Style::Comment, &chunk[pos..pos+end], buf);

            // must be unstyled to work well at the end of input
            buf.push('\n');

            chunk = &chunk[pos+end+1..];
        } else {
            break;
        }
    }
    buf.push_str(chunk);
}

impl Highlighter for EdgeqlHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        let line_trim = line.trim_start();
        if line_trim.starts_with('\\') {
            let off = line.len() - line_trim.len();
            if let Some(cmd) = line.split_whitespace().next() {
                if backslash::COMMAND_NAMES.contains(&cmd) {
                    let mut buf = String::with_capacity(line.len() + 8);
                    buf.push_str(&line[..off]);
                    self.styler.apply(Style::BackslashCommand, cmd, &mut buf);
                    buf.push_str(&line[off+cmd.len()..]);
                    return buf.into();
                } else if !backslash::COMMAND_NAMES
                    .iter().any(|c| c.starts_with(cmd))
                {
                    let mut buf = String::with_capacity(line.len() + 8);
                    buf.push_str(&line[..off]);
                    self.styler.apply(Style::Error, cmd, &mut buf);
                    buf.push_str(&line[off+cmd.len()..]);
                    return buf.into();
                }
            }
            return line.into();
        } else {
            let mut outbuf = String::with_capacity(line.len());
            let mut pos = 0;
            let mut token_stream = TokenStream::new(line);
            for res in &mut token_stream {
                let tok = match res {
                    Ok(tok) => tok,
                    Err(_) => {
                        outbuf.push_str(&line[pos..]);
                        break;
                    }
                };
                if tok.start.offset as usize > pos {
                    emit_insignificant(&mut outbuf, &self.styler,
                        &line[pos..tok.start.offset as usize]);
                }
                if let Some(st) = token_style(tok.token.kind) {
                    self.styler.apply(st, tok.token.value, &mut outbuf);
                } else {
                    outbuf.push_str(tok.token.value);
                }
                pos = tok.end.offset as usize;
            }
            emit_insignificant(&mut outbuf, &self.styler, &line[pos..]);
            return outbuf.into();
        }
    }
    fn highlight_char<'l>(&self, _line: &'l str, _pos: usize) -> bool {
        // TODO(tailhook) optimize: only need to return true on insert
        true
    }
    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        return hint.light_gray().to_string().into()
    }
}
impl Validator for EdgeqlHelper {
    fn validate(&self, ctx: &mut ValidationContext)
        -> Result<ValidationResult, ReadlineError>
    {
        let line = ctx.input().trim();
        if line.trim().is_empty() {
            return Ok(ValidationResult::Valid(None));
        }
        if line.starts_with("\\") {
            match backslash::parse(line) {
                Ok(_) => Ok(ValidationResult::Valid(None)),
                Err(e) => {
                    Ok(ValidationResult::Invalid(Some(
                        format!("  ← {}", e.hint))))
                }
            }
        } else {
            if full_statement(ctx.input().as_bytes(), None).is_ok() {
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

fn load_history<H: rustyline::Helper>(ed: &mut Editor<H>, name: &str)
    -> Result<(), anyhow::Error>
{
    let dir = data_local_dir().context("cannot find local data dir")?;
    let app_dir = dir.join("edgedb");
    match ed.load_history(&app_dir.join(format!("{}.history", name))) {
        Err(ReadlineError::Io(e)) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e).context("error loading history")?,
        Ok(()) => {}
    }
    Ok(())
}

fn _save_history<H: Helper>(ed: &mut Editor<H>, name: &str)
    -> Result<(), anyhow::Error>
{
    let dir = data_local_dir().context("cannot find local data dir")?;
    let app_dir = dir.join("edgedb");
    if !app_dir.exists() {
        fs::create_dir_all(&app_dir).context("cannot create application dir")?;
    }
    ed.save_history(&app_dir.join(format!("{}.history", name)))
        .context("error writing history file")?;
    Ok(())
}

fn save_history<H: Helper>(ed: &mut Editor<H>, name: &str) {
    _save_history(ed, name).map_err(|e| {
        eprintln!("Can't save history: {:#}", e);
    }).ok();
}

pub fn create_editor(mode: EditMode) -> Editor<EdgeqlHelper> {
    let config = Config::builder();
    let config = config.edit_mode(mode);
    let mut editor = Editor::<EdgeqlHelper>::with_config(config.build());
    editor.bind_sequence(KeyPress::Enter, Cmd::AcceptOrInsertLine);
    load_history(&mut editor, "edgeql").map_err(|e| {
        eprintln!("Can't load history: {:#}", e);
    }).ok();
    editor.set_helper(Some(EdgeqlHelper {
        styler: Styler::dark_256(),
    }));
    return editor;
}

pub fn var_editor(mode: EditMode, type_name: &str) -> Editor<()> {
    let config = Config::builder();
    let config = config.edit_mode(mode);
    let mut editor = Editor::<()>::with_config(config.build());
    editor.bind_sequence(KeyPress::Enter, Cmd::AcceptOrInsertLine);
    load_history(&mut editor, &format!("var_{}", type_name)).map_err(|e| {
        eprintln!("Can't load history: {:#}", e);
    }).ok();
    return editor;
}


pub fn main(data: Sender<Input>, control: Receiver<Control>)
    -> Result<(), anyhow::Error>
{
    let mut mode = EditMode::Emacs;
    let mut editor = create_editor(mode);
    let mut prompt = String::from("> ");
    'outer: loop {
        match task::block_on(control.recv()) {
            None => break 'outer,
            Some(Control::ViMode) => {
                save_history(&mut editor, "edgeql");
                mode = EditMode::Vi;
                editor = create_editor(mode);
            }
            Some(Control::EmacsMode) => {
                save_history(&mut editor, "edgeql");
                mode = EditMode::Emacs;
                editor = create_editor(mode);
            }
            Some(Control::EdgeqlInput { database, initial }) => {
                prompt.clear();
                prompt.push_str(&database);
                prompt.push_str("> ");
                let text = match
                    editor.readline_with_initial(&prompt, (&initial, ""))
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
            Some(Control::VariableInput { name, type_name, initial })
            => {
                prompt.clear();
                prompt.push_str("Variable <");
                prompt.push_str(&type_name);
                prompt.push_str(">$");
                prompt.push_str(&name);
                prompt.push_str(": ");
                let mut editor = var_editor(mode, &type_name);
                let text = match
                    editor.readline_with_initial(&prompt, (&initial, ""))
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
                save_history(&mut editor, &format!("var_{}", &type_name));
                task::block_on(data.send(Input::Text(text)))
            }
        }
    }
    save_history(&mut editor, "edgeql");
    Ok(())
}

fn token_style(kind: Kind) -> Option<Style> {
    use edgeql_parser::tokenizer::Kind as T;
    use crate::print::style::Style as S;

    match kind {
        T::Keyword => Some(S::Keyword),

        T::At => Some(S::Punctuation),  // TODO(tailhook) but also decorators
        T::Dot => Some(S::Punctuation),
        T::ForwardLink => Some(S::Punctuation),
        T::BackwardLink => Some(S::Punctuation),

        T::Assign => None,
        T::SubAssign => None,
        T::AddAssign => None,
        T::Arrow => None,
        T::Coalesce => None,
        T::Namespace => None,
        T::FloorDiv => None,
        T::Concat => None,
        T::GreaterEq => None,
        T::LessEq => None,
        T::NotEq => None,
        T::NotDistinctFrom => None,
        T::DistinctFrom => None,
        T::Comma => None,
        T::OpenParen => None,
        T::CloseParen => None,
        T::OpenBracket => None,
        T::CloseBracket => None,
        T::OpenBrace => None,
        T::CloseBrace => None,
        T::Semicolon => None,
        T::Colon => None,
        T::Add => None,
        T::Sub => None,
        T::Mul => None,
        T::Div => None,
        T::Modulo => None,
        T::Pow => None,
        T::Less => None,
        T::Greater => None,
        T::Eq => None,
        T::Ampersand => None,
        T::Pipe => None,
        T::Argument => None, // TODO (tailhook)
        T::DecimalConst => Some(S::Constant),
        T::FloatConst => Some(S::Constant),
        T::IntConst => Some(S::Constant),
        T::BigIntConst => Some(S::Constant),
        T::BinStr => Some(S::String),
        T::Str => Some(S::String),
        T::BacktickName => None,
        T::Ident => None,
    }
}
