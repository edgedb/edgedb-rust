use crate::commands::Options;
use crate::client::Client;


pub async fn list_databases<'x>(cli: &mut Client<'x>, options: &Options)
    -> Result<(), anyhow::Error>
{
    let list = cli.query::<String>("SELECT name := sys::Database.name").await?;
    if !options.command_line {
        println!("List of databases:");
    }
    for name in list {
        if options.command_line {
            println!("{}", name);
        } else {
            println!("  {}", name);
        }
    }
    Ok(())
}
