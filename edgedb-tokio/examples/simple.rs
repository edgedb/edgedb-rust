#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn = edgedb_tokio::create_client().await?;
    let val = conn
        .query_required_single::<i64, _>("SELECT 7*8", &())
        .await?;
    println!("7*8 is: {}", val);
    Ok(())
}
