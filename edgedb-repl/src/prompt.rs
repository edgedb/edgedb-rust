use std::error::Error;

use anyhow;
use async_std::sync::{Sender, Receiver};
use async_std::task;
use linefeed::{Interface, ReadResult};


pub enum Control {
    Input(String),
}

pub fn main(data: Sender<ReadResult>, control: Receiver<Control>)
    -> Result<(), anyhow::Error>
{
    let interface = Interface::new("edgedb-repl")?;
    loop {
        loop {
            match task::block_on(control.recv()) {
                None => return Ok(()),
                Some(Control::Input(name)) => {
                    interface.set_prompt(&(name + "> "))?;
                    break;
                }
            }
        }
        println!("READLINE");
        let line = interface.read_line()?;
        task::block_on(data.send(line));
    }
}
