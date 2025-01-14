#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let conn = edgedb_tokio::create_client().await?;
    let val = conn
        .transaction(|mut transaction| async move {
            transaction
                .query_required_single::<i64, _>(
                    "SELECT (UPDATE Counter SET { value := .value + 1}).value LIMIT 1",
                    &(),
                )
                .await
        })
        .await?;
    println!("Counter: {val}");
    Ok(())
}
