use crate::commands::Options;
use crate::client::Client;

pub async fn list_databases<'x>(cli: &mut Client<'x>, options: &Options)
    -> Result<(), anyhow::Error>
{
    todo!();
}
