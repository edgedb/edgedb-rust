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
    let handle = task::spawn(client::interactive_main(repl_rd, control_wr));
    prompt::main(repl_wr, control_rd)?;
    task::block_on(handle)?;
    Ok(())
}

