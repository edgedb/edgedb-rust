use anyhow;
use async_std::sync::{Sender, Receiver};
use async_std::task;
use rustyline::{self, error::ReadlineError};


pub enum Control {
    Input(String),
}

pub enum Input {
    Text(String),
    Eof,
    Interrupt,
}

pub fn main(data: Sender<Input>, control: Receiver<Control>)
    -> Result<(), anyhow::Error>
{
    let mut editor = rustyline::Editor::<()>::new();
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
