use edgedb_tokio::Client;
use edgedb_errors::NoDataError;
use futures_util::stream::{self, StreamExt};

use crate::server::SERVER;

#[tokio::test]
async fn simple() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let value = client.query::<i64, _>("SELECT 7*93", &()).await?;
    assert_eq!(value, vec![651]);

    let value = client.query_single::<i64, _>("SELECT 5*11", &()).await?;
    assert_eq!(value, Some(55));

    let value = client.query_single::<i64, _>("SELECT <int64>{}", &()).await?;
    assert_eq!(value, None);

    let value = client.query_required_single::<i64, _>(
        "SELECT 5*11", &()).await?;
    assert_eq!(value, 55);

    let err = client.query_required_single::<i64, _>(
        "SELECT <int64>{}", &()).await.unwrap_err();
    assert!(err.is::<NoDataError>());

    let value = client.query_json("SELECT 'x' ++ 'y'", &()).await?;
    assert_eq!(value.as_ref(), r#"["xy"]"#);

    let value = client.query_single_json("SELECT 'x' ++ 'y'", &()).await?;
    assert_eq!(value.as_deref(), Some(r#""xy""#));

    let value = client.query_single_json("SELECT <str>{}", &()).await?;
    assert_eq!(value.as_deref(), None);

    let err = client.query_required_single_json(
        "SELECT <int64>{}", &()).await.unwrap_err();
    assert!(err.is::<NoDataError>());

    Ok(())
}

#[tokio::test]
async fn parallel_queries() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let result = stream::iter(0..10i64).map(|idx| {
        let cli = client.clone();
        async move {
            cli.query_required_single::<i64, _>(
                "SELECT <int64>$0*10", &(idx,)
            ).await
        }
    }).buffer_unordered(7).collect::<Vec<_>>().await;
    let mut result: Vec<_> = result.into_iter().collect::<Result<_, _>>()?;
    result.sort();

    assert_eq!(result, (0..100).step_by(10).collect::<Vec<_>>());

    Ok(())
}
