use rand::{thread_rng, Rng};
use edgedb_errors::{ClientError, ErrorKind};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    main2().await
}

async fn main2() -> anyhow::Result<()> {
    let conn = edgedb_tokio::create_client().await?;
    conn.transaction(|mut transaction| async move {
        let nval = transaction.query_required_single::<i64, _>(
            "SELECT (UPDATE Counter SET { value := .value + 1}).value LIMIT 1",
            &()
        ).await?;
        println!("counter val: {nval}");
        if thread_rng().gen_bool(0.5) {
            Ok(())
        } else {
            Err(ClientError::with_message("canceled"))
        }
    }).await?;
    Ok(())
}
