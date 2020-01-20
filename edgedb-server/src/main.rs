use async_std::task;
use anyhow;
use env_logger;

mod connection;
mod listen;
mod options;
mod postgres;
mod reader;
mod writer;

use options::Options;

fn main() -> Result<(), anyhow::Error> {
    let options = Options::from_args();
    env_logger::Builder::new()
        .filter_level(options.log_level)
        .init();
    let dsn = match options.mode {
        options::Mode::External(ref dsn) => dsn.clone(),
        options::Mode::DataDir(_) => unimplemented!("data-dir"),
    };
    task::block_on(postgres::Client::connect(dsn))?;
    task::block_on(listen::accept_loop(options))?;
    Ok(())
}
