/*!
EdgeDB client for Tokio

👉 New! Check out the new [EdgeDB client tutorial](`tutorial`). 👈

The main way to use EdgeDB bindings is to use the [`Client`]. It encompasses
connection pool to the database that is transparent for user. Individual
queries can be made via methods on the client. Correlated queries are done
via [transactions](Client::transaction).

To create a client, use the [`create_client`] function (it gets a database
connection configuration from environment). You can also use a [`Builder`]
to [`build`](`Builder::new`) custom [`Config`] and [create a
client](Client::new) using that config.

# Example

```rust,no_run
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
More [examples on github](https://github.com/edgedb/edgedb-rust/tree/master/edgedb-tokio/examples)

# Nice Error Reporting

We use [miette] crate for including snippets in your error reporting code.

To make it work, first you need enable `fancy` feature in your top-level
crate's `Cargo.toml`:
```toml
[dependencies]
miette = { version="5.3.0", features=["fancy"] }
edgedb-tokio = { version="*", features=["miette-errors"] }
```

Then if you use `miette` all the way through your application, it just
works:
```rust,no_run
#[tokio::main]
async fn main() -> miette::Result<()> {
    let conn = edgedb_tokio::create_client().await?;
    conn.query::<String, _>("SELECT 1+2)", &()).await?;
    Ok(())
}
```

However, if you use some boxed error container (e.g. [anyhow]), you
might need to downcast error for printing:
```rust,no_run
async fn do_something() -> anyhow::Result<()> {
    let conn = edgedb_tokio::create_client().await?;
    conn.query::<String, _>("SELECT 1+2)", &()).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
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
```

In some cases, where parts of your code use `miette::Result` or
`miette::Report` before converting to the boxed (anyhow) container, you
might want a little bit more complex downcasting:

```rust,no_run
# async fn do_something() -> anyhow::Result<()> { unimplemented!() }
#[tokio::main]
async fn main() {
    match do_something().await {
        Ok(res) => res,
        Err(e) => {
            e.downcast::<edgedb_tokio::Error>()
                .map(|e| eprintln!("{:?}", miette::Report::new(e)))
                .or_else(|e| e.downcast::<miette::Report>()
                    .map(|e| eprintln!("{:?}", e)))
                .unwrap_or_else(|e| eprintln!("{:#}", e));
            std::process::exit(1);
        }
    }
}
```

Note that last two examples do hide error contexts from anyhow and do not
pretty print if `source()` of the error is `edgedb_errors::Error` but not
the top-level one. We leave those more complex cases as an excersize to the
reader.

[miette]: https://crates.io/crates/miette
[anyhow]: https://crates.io/crates/anyhow
*/

#![cfg_attr(
    not(feature = "unstable"),
    warn(missing_docs, missing_debug_implementations)
)]

#[cfg(feature = "unstable")]
pub mod credentials;
#[cfg(feature = "unstable")]
pub mod raw;
#[cfg(feature = "unstable")]
pub mod server_params;
#[cfg(feature = "unstable")]
pub mod tls;

#[cfg(not(feature = "unstable"))]
mod credentials;
#[cfg(not(feature = "unstable"))]
mod raw;
#[cfg(not(feature = "unstable"))]
mod server_params;
#[cfg(not(feature = "unstable"))]
mod tls;

mod builder;
mod client;
mod errors;
mod options;
mod sealed;
pub mod state;
mod transaction;
pub mod tutorial;

pub use edgedb_derive::{ConfigDelta, GlobalsDelta, Queryable};

pub use builder::{Builder, ClientSecurity, Config, InstanceName};
pub use client::Client;
pub use credentials::TlsSecurity;
pub use errors::Error;
pub use options::{RetryCondition, RetryOptions, TransactionOptions};
pub use state::{ConfigDelta, GlobalsDelta};
pub use transaction::Transaction;

#[cfg(feature = "unstable")]
pub use builder::get_project_dir;

/// Create a connection to the database with default parameters
///
/// It's expected that connection parameters are set up using environment
/// (either environment variables or project configuration in `edgedb.toml`)
/// so no configuration is specified here.
///
/// This method tries to esablish single connection immediately to
/// ensure that configuration is valid and will error out otherwise.
///
/// For more fine-grained setup see [`Client`] and [`Builder`] documentation
/// and the source of this function.
#[cfg(feature = "env")]
pub async fn create_client() -> Result<Client, Error> {
    let pool = Client::new(&Builder::new().build_env().await?);
    pool.ensure_connected().await?;
    Ok(pool)
}
