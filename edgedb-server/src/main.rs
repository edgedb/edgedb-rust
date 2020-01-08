use structopt::StructOpt;
use async_std::task;
use anyhow;

mod options;
mod listen;

use options::Options;

fn main() -> Result<(), anyhow::Error> {
    let options = Options::from_args();
    task::block_on(listen::accept_loop(options))?;
    Ok(())
}
