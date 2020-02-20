use async_std::task;

use edgeql_parser::helpers::quote_name;
use crate::options::{Options, Command};
use crate::client::Connection;
use crate::commands;
use crate::server_params::PostgresAddress;


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
        Command::ListAliases(t) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::list_aliases(&mut cli, &cmdopt,
                    &t.pattern, t.system, t.case_sensitive, t.verbose).await?;
                Ok(())
            }).into()
        },
        Command::ListCasts(t) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::list_casts(&mut cli, &cmdopt,
                    &t.pattern, t.case_sensitive).await?;
                Ok(())
            }).into()
        },
        Command::ListIndexes(t) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::list_indexes(&mut cli, &cmdopt,
                    &t.pattern, t.system, t.case_sensitive, t.verbose).await?;
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
                    &t.pattern, t.system, t.case_sensitive).await?;
                Ok(())
            }).into()
        },
        Command::ListObjectTypes(t) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::list_object_types(&mut cli, &cmdopt,
                    &t.pattern, t.system, t.case_sensitive).await?;
                Ok(())
            }).into()
        },
        Command::ListRoles(t) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::list_roles(&mut cli, &cmdopt,
                    &t.pattern, t.case_sensitive).await?;
                Ok(())
            }).into()
        },
        Command::ListModules(t) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::list_modules(&mut cli, &cmdopt,
                    &t.pattern, t.case_sensitive).await?;
                Ok(())
            }).into()
        },
        Command::Pgaddr => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let cli = conn.authenticate(&options).await?;
                match cli.params.get::<PostgresAddress>() {
                    Some(addr) => {
                        println!("{}", serde_json::to_string_pretty(addr)?);
                    }
                    None => {
                        eprintln!("pgaddr requires EdgeDB to run in DEV mode");
                    }
                }
                Ok(())
            }).into()
        },
        Command::Psql => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::psql(&mut cli, &cmdopt).await?;
                Ok(())
            }).into()
        },
        Command::Describe(d) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::describe(&mut cli, &cmdopt,
                    &d.name, d.verbose).await?;
                Ok(())
            }).into()
        },
        Command::Configure(c) => {
            task::block_on(async {
                let mut conn = Connection::from_options(&options).await?;
                let mut cli = conn.authenticate(&options).await?;
                commands::configure(&mut cli, &cmdopt, &c).await?;
                Ok(())
            }).into()
        }
    }
}
