/*!
Client for the Gel database, using async Tokio runtime

To get started, check out the [Rust client tutorial](`tutorial`).

The main way to use Gel bindings is to use the [`Client`]. It encompasses
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
    let conn = gel_tokio::create_client().await?;
    let val = conn.query_required_single::<i64, _>(
        "SELECT 7*8",
        &(),
    ).await?;
    println!("7*8 is: {}", val);
    Ok(())
}
```
More [examples on github](https://github.com/edgedb/edgedb-rust/tree/master/gel-tokio/examples)

# Nice Error Reporting

We use [miette] crate for including snippets in your error reporting code.

To make it work, first you need enable `fancy` feature in your top-level
crate's `Cargo.toml`:
```toml
[dependencies]
miette = { version="5.3.0", features=["fancy"] }
gel-tokio = { version="*", features=["miette-errors"] }
```

Then if you use `miette` all the way through your application, it just
works:
```rust,no_run
#[tokio::main]
async fn main() -> miette::Result<()> {
    let conn = gel_tokio::create_client().await?;
    conn.query::<String, _>("SELECT 1+2)", &()).await?;
    Ok(())
}
```

However, if you use some boxed error container (e.g. [anyhow]), you
might need to downcast error for printing:
```rust,no_run
async fn do_something() -> anyhow::Result<()> {
    let conn = gel_tokio::create_client().await?;
    conn.query::<String, _>("SELECT 1+2)", &()).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    match do_something().await {
        Ok(res) => res,
        Err(e) => {
            e.downcast::<gel_tokio::Error>()
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
            e.downcast::<gel_tokio::Error>()
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
pretty print if `source()` of the error is `gel_errors::Error` but not
the top-level one. We leave those more complex cases as an excersize to the
reader.

[miette]: https://crates.io/crates/miette
[anyhow]: https://crates.io/crates/anyhow
*/

#![cfg_attr(
    not(feature = "unstable"),
    warn(missing_docs, missing_debug_implementations)
)]

macro_rules! unstable_pub_mods {
    ($(mod $mod_name:ident;)*) => {
        $(
            #[cfg(feature = "unstable")]
            pub mod $mod_name;
            #[cfg(not(feature = "unstable"))]
            mod $mod_name;
        )*
    }
}

// If the unstable feature is enabled, the modules will be public.
// If the unstable feature is not enabled, the modules will be private.
unstable_pub_mods! {
    mod builder;
    mod raw;
    mod server_params;
}

pub use gel_dsn::gel::{Builder, CloudName, Config, InstanceName};
pub mod credentials {
    pub use gel_dsn::gel::TlsSecurity;

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    pub struct Credentials {
        pub user: String,
        pub host: Option<String>,
        pub port: Option<u16>,
        pub password: Option<String>,
        pub database: Option<String>,
        pub branch: Option<String>,
        pub tls_ca: Option<String>,
        #[serde(default)]
        pub tls_security: TlsSecurity,
        pub tls_server_name: Option<String>,
    }
    impl From<Credentials> for gel_dsn::gel::Params {
        fn from(credentials: Credentials) -> Self {
            use gel_dsn::gel::Param;
            let mut params = gel_dsn::gel::Params::default();
            params.user = Param::Unparsed(credentials.user);
            params.host = Param::from_unparsed(credentials.host);
            params.port = Param::from_parsed(credentials.port);
            params.password = Param::from_unparsed(credentials.password);
            params.database = Param::from_unparsed(credentials.database);
            params.branch = Param::from_unparsed(credentials.branch);
            params.tls_ca = Param::from_unparsed(credentials.tls_ca);
            params.tls_security = Param::Parsed(credentials.tls_security);
            params.tls_server_name = Param::from_unparsed(credentials.tls_server_name);
            params
        }
    }

    pub trait AsCredentials {
        fn as_credentials(&self) -> anyhow::Result<Credentials>;
    }

    impl AsCredentials for gel_dsn::gel::Config {
        fn as_credentials(&self) -> anyhow::Result<Credentials> {
            let target = self.host.target_name()?;
            let tcp = target.tcp().ok_or(anyhow::anyhow!("no TCP address"))?;
            Ok(Credentials {
                user: self.user.clone(),
                host: Some(tcp.0.to_string()),
                port: Some(tcp.1),
                password: self.authentication.password().map(|s| s.to_string()),
                database: self.db.name().map(|s| s.to_string()),
                branch: self.db.branch().map(|s| s.to_string()),
                tls_ca: self.tls_ca_pem(),
                tls_security: self.tls_security,
                tls_server_name: self.tls_server_name.clone(),
            })
        }
    }
}

mod client;
mod errors;
mod options;
mod query_executor;
mod sealed;
pub mod state;
mod transaction;
pub mod tutorial;

pub use gel_derive::{ConfigDelta, GlobalsDelta, Queryable};

pub use client::Client;
pub use errors::Error;
pub use options::{RetryCondition, RetryOptions, TransactionOptions};
pub use query_executor::{QueryExecutor, ResultVerbose};
pub use state::{ConfigDelta, GlobalsDelta};
pub use transaction::{RetryingTransaction, Transaction};

/// The ordered list of project filenames supported.
pub const PROJECT_FILES: &[&str] = &["gel.toml", "edgedb.toml"];

/// The default project filename.
pub const DEFAULT_PROJECT_FILE: &str = PROJECT_FILES[0];

#[cfg(feature = "unstable")]
pub use transaction::RawTransaction;

/// Create a connection to the database with default parameters
///
/// It's expected that connection parameters are set up using environment
/// (either environment variables or project configuration in a file named by
/// [`PROJECT_FILES`]) so no configuration is specified here.
///
/// This method tries to esablish single connection immediately to ensure that
/// configuration is valid and will error out otherwise.
///
/// For more fine-grained setup see [`Client`] and [`Builder`] documentation and
/// the source of this function.
#[cfg(feature = "env")]
pub async fn create_client() -> Result<Client, Error> {
    use gel_errors::{ClientConnectionError, ErrorKind};
    use tokio::task::spawn_blocking;

    // Run the builder in a blocking context (it's unlikely to pause much but
    // better to be safe)
    let config = spawn_blocking(|| Builder::default().build())
        .await
        .map_err(ClientConnectionError::with_source)??;
    let pool = Client::new(&config);
    pool.ensure_connected().await?;
    Ok(pool)
}
