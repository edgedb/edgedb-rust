use async_std::task;

use edgeql_parser::helpers::quote_name;
use crate::options::{Options, Command};
use crate::client::Connection;
use crate::commands;


pub fn main(options: Options) -> Result<(), anyhow::Error> {
    let cmdopt = commands::Options {
        command_line: true,
    };
    match options.subcommand.as_ref().expect("subcommand is present") {
        Command::CreateDatabase(d) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                let res = cli.execute(&format!("CREATE DATABASE {}",
                                     quote_name(&d.database_name))).await?;
                eprintln!("  -> {}: Ok",
                    String::from_utf8_lossy(&res[..]));
                Ok(())
            }).into()
        },
        Command::ListDatabases => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::list_databases(&mut cli, &cmdopt).await?;
                Ok(())
            }).into()
        },
        Command::ListScalarTypes(t) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::list_scalar_types(&mut cli, &cmdopt,
                    &t.pattern, t.system, t.insensitive).await?;
                Ok(())
            }).into()
        },
    }
}
