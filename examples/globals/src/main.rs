use edgedb_derive::GlobalsDelta;

#[derive(GlobalsDelta)]
struct Globals<'a> {
    str_global: &'a str,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let conn = edgedb_tokio::create_client().await?;
    let conn = conn.with_globals(&Globals { str_global: "val1" });
    let val = conn
        .query_required_single::<String, _>("SELECT (GLOBAL str_global)", &())
        .await?;
    assert_eq!(val, "val1");
    Ok(())
}
