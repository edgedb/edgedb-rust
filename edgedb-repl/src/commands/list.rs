use async_std::stream::Stream;
use async_std::prelude::StreamExt;

use crate::commands::Options;


pub async fn print<S, E>(mut items: S, title: &str, options: &Options)
    -> Result<(), anyhow::Error>
    where S: Stream<Item=Result<String, E>> + Unpin,
          anyhow::Error: From<E>,
{
    if !options.command_line {
        println!("{}:", title);
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
