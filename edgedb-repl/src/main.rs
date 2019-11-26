use std::thread;

use anyhow;
use async_std::task;
use async_std::sync::{channel};

mod reader;
mod prompt;
mod client;

fn main() -> Result<(), anyhow::Error> {
    interactive_main()
}

fn interactive_main() -> Result<(), anyhow::Error> {
    let (control_wr, control_rd) = channel(1);
    let (repl_wr, repl_rd) = channel(1);
    let thread = thread::spawn(move || {
        task::block_on(client::interactive_main(repl_rd, control_wr))
    });
    prompt::main(repl_wr, control_rd)?;
    thread.join().expect("thread don't panic")?;
    Ok(())
}

