EdgeDB Rust Binding for Tokio
=============================

Work in progress asynchronous bindings of EdgeDB for [Tokio](https://tokio.rs/)
main loop.

# Example Usage

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn = edgedb_tokio::create_client().await?;
    let val = conn.query_required_single::<i64, _>(
        "SELECT 7*8",
        &(),
    ).await?;
    println!("7*8 is: {}", val);
    Ok(())
}
```

# Transaction Example

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn = edgedb_tokio::create_client().await?;
    let val = conn.transaction(|mut transaction| async move {
        transaction.query_required_single::<i64, _>(
            "SELECT (UPDATE Counter SET { value := .value + 1}).value LIMIT 1",
            &()
        ).await
    }).await?;
    println!("Counter: {val}");
    Ok(())
}
```

More [examples on github](https://github.com/edgedb/edgedb-rust/tree/master/edgedb-tokio/examples)


License
=======


Licensed under either of

* Apache License, Version 2.0,
  (./LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license (./LICENSE-MIT or http://opensource.org/licenses/MIT)

at your option.
