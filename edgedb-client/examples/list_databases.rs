use async_std::task;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let databases: Vec<String> = task::block_on(async {
        let pool = edgedb_client::connect().await?;
        pool.query("SELECT name := sys::Database.name", &()).await
    })?;
    println!("Database list:");
    for db in databases {
        println!("{}", db);
    }
    Ok(())
}
