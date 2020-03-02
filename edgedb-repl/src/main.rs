use anyhow;

use async_std::task;
use async_std::sync::{channel};

use crate::options::Options;

mod client;
mod commands;
mod options;
mod print;
mod prompt;
mod reader;
mod repl;
mod server_params;
mod statement;
mod variables;


fn main() -> Result<(), anyhow::Error> {
    let opt = Options::from_args_and_env();
    if opt.subcommand.is_some() {
        commands::cli::main(opt)
    } else {
        if opt.interactive {
            interactive_main(opt)
        } else {
            non_interactive_main(opt)
        }
    }
}

fn interactive_main(options: Options) -> Result<(), anyhow::Error> {
    let (control_wr, control_rd) = channel(1);
    let (repl_wr, repl_rd) = channel(1);
    let state = repl::State {
        control: control_wr,
        data: repl_rd,
        print: print::Config::new()
            .max_items(100)
            .clone(),
        verbose_errors: false,
        last_error: None,
        database: options.database.clone(),
        implicit_limit: Some(100),
    };
    let handle = task::spawn(client::interactive_main(options, state));
    prompt::main(repl_wr, control_rd)?;
    task::block_on(handle)?;
    Ok(())
}

fn non_interactive_main(options: Options) -> Result<(), anyhow::Error> {
    task::block_on(client::non_interactive_main(options))
}

