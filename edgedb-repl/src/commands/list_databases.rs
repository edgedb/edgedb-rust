use async_std::prelude::StreamExt;

use crate::commands::Options;
use crate::client::Client;


pub async fn list_databases<'x>(cli: &mut Client<'x>, options: &Options)
    -> Result<(), anyhow::Error>
{
    let mut items = cli.query::<String>(r###"
        SELECT name := sys::Database.name
    "###).await?;
    if !options.command_line {
        println!("List of databases:");
    }
    while let Some(name) = items.next().await.transpose()? {
        if options.command_line {
            println!("{}", name);
        } else {
            println!("  {}", name);
        }
    }
    Ok(())
}
