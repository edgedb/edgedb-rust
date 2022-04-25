EdgeDB Rust Binding for Async-Std
=================================

Work in progress asynchronous bindings of EdgeDB for
[async-std](https://async.rs/) main loop.

**Note:** development of these bindings are stalled. Use
[edgedb-tokio](https://docs.rs/edgedb-tokio) instead.

# Example Usage

```rust
use async_std::task;

fn main() -> anyhow::Result<()> {
    let val: i64 = task::block_on(async {
        let pool = edgedb_client::connect().await?;
        pool.query("SELECT 7*8", &()).await
    })?;
    println!("7*8 is: {}", val);
    Ok(())
}
```

More [examples on github](https://github.com/edgedb/edgedb-rust/tree/master/edgedb-client/examples)


License
=======


Licensed under either of

* Apache License, Version 2.0,
  (./LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license (./LICENSE-MIT or http://opensource.org/licenses/MIT)

at your option.
