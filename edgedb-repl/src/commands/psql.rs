use std::process::Command;

use anyhow::Context;
use crate::client::Client;
use crate::commands::Options;
use crate::server_params::PostgresAddress;


pub async fn psql<'x>(cli: &mut Client<'x>, _options: &Options)
    -> Result<(), anyhow::Error>
{
    match cli.params.get::<PostgresAddress>() {
        Some(addr) => {
            let mut cmd = Command::new("psql");
            cmd.arg("-h").arg(&addr.host);
            cmd.arg("-U").arg(&addr.user);
            cmd.arg("-p").arg(addr.port.to_string());
            cmd.arg("-d").arg(&addr.database);
            cmd.status()
                .context(format!("Error running {:?}", cmd))?;
        }
        None => {
            eprintln!("psql requires EdgeDB to run in DEV mode");
        }
    }
    Ok(())
}
