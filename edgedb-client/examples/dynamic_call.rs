use std::error::Error;
use async_std::task;

use edgedb_client::Executor;


async fn list_databases(db: &mut dyn Executor) -> Result<(), Box<dyn Error>> {
    let databases = db.query::<String, _>("
        SELECT name := sys::Database.name
    ", &()).await?;
    println!("Database list:");
    for db in databases {
        println!("{}", db);
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    task::block_on(async {
        let pool = edgedb_client::connect().await?;
        list_databases(&mut pool.clone()).await
    })
}
