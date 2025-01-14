use edgedb_tokio::Client;

use crate::server::SERVER;

#[tokio::test]
async fn global_fn() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let value = client
        .with_default_module(Some("test"))
        .with_globals_fn(|m| m.set("str_val", "hello"))
        .query::<String, _>("SELECT (global str_val)", &())
        .await?;
    assert_eq!(value, vec![String::from("hello")]);

    let value = client
        .with_default_module(Some("test"))
        .with_globals_fn(|m| m.set("int_val", 127))
        .query::<i32, _>("SELECT (global int_val)", &())
        .await?;
    assert_eq!(value, vec![127]);
    Ok(())
}

#[derive(gel_derive::GlobalsDelta)]
struct Globals {
    str_val: &'static str,
    int_val: i32,
}

#[cfg(feature = "derive")]
#[tokio::test]
async fn global_struct() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let value = client
        .with_default_module(Some("test"))
        .with_globals(&Globals {
            str_val: "value1",
            int_val: 345,
        })
        .query::<String, _>("SELECT (global str_val)", &())
        .await?;
    assert_eq!(value, vec![String::from("value1")]);

    let value = client
        .with_default_module(Some("test"))
        .with_globals(&Globals {
            str_val: "value2",
            int_val: 678,
        })
        .query::<i32, _>("SELECT (global int_val)", &())
        .await?;
    assert_eq!(value, vec![678]);
    Ok(())
}
