use anyhow::Context;

async fn do_something() -> anyhow::Result<()> {
    let conn = edgedb_tokio::create_client().await?;
    conn.query::<String, _>("SELECT 1+2)", &()).await
        .context("Query `select 1+2`")?;
    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn")
    ).init();
    match do_something().await {
        Ok(res) => res,
        Err(e) => {
            e.downcast::<edgedb_tokio::Error>()
                .map(|e| eprintln!("{:?}", miette::Report::new(e)))
                .unwrap_or_else(|e| eprintln!("{:#}", e));
            std::process::exit(1);
        }
    }
}

/*
/// Alternative error handling if you use miette thorough your application
#[tokio::main]
async fn main() -> miette::Result<()> {
    let conn = edgedb_tokio::create_client().await?;
    conn.query::<String, _>("SELECT 1+2)", &()).await?;
    Ok(())
}
*/
