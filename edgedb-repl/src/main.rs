use anyhow;
use async_std::task;
use async_std::sync::{channel};

use crate::options::Options;

mod reader;
mod prompt;
mod client;
mod options;


fn main() -> Result<(), anyhow::Error> {
    let opt = Options::from_args_and_env();
    if opt.interactive {
        interactive_main(&opt)
    } else {
        unimplemented!("Non interactive mode is not implemented yet");
    }
}

fn interactive_main(options: &Options) -> Result<(), anyhow::Error> {
    let (control_wr, control_rd) = channel(1);
    let (repl_wr, repl_rd) = channel(1);
    let handle = task::spawn(client::interactive_main(
        options.clone(), repl_rd, control_wr));
    prompt::main(repl_wr, control_rd)?;
    task::block_on(handle)?;
    Ok(())
}

