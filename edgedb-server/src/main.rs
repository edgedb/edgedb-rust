use async_std::task;
use anyhow;
use env_logger;

mod options;
mod listen;

use options::Options;

fn main() -> Result<(), anyhow::Error> {
    let options = Options::from_args();
    env_logger::Builder::new()
        .filter_level(options.log_level)
        .init();
    task::block_on(listen::accept_loop(options))?;
    Ok(())
}
