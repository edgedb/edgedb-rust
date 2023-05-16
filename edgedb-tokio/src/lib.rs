//! EdgeDB client for Tokio
//!
//! Main way to use EdgeDB bindings is to use [`Client`]. It encompasses
//! connection pool to the database that is transparent for user. Individual
//! queries can be made via methods on the client. Correlated queries are done
//! via [transactions](Client::transaction)
//!
//! To create client, use [`create_client`] function (it gets database
//! connection configuration from environment). You can also use [`Builder`]
//! to [`build`](`Builder::build`) custom [`Config`] and [create a
//! client](Client::new) using that config.
//!
//! # Example
//!
//! ```rust,no_run
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let conn = edgedb_tokio::create_client().await?;
//!     let val = conn.query_required_single::<i64, _>(
//!         "SELECT 7*8",
//!         &(),
//!     ).await?;
//!     println!("7*8 is: {}", val);
//!     Ok(())
//! }
//! ```
//! More [examples on github](https://github.com/edgedb/edgedb-rust/tree/master/edgedb-tokio/examples)
#![cfg_attr(not(feature="unstable"),
   warn(missing_docs, missing_debug_implementations))]

#[cfg(feature="unstable")]
pub mod raw;
#[cfg(feature="unstable")]
pub mod server_params;
#[cfg(feature="unstable")]
pub mod credentials;
#[cfg(feature="unstable")]
pub mod tls;

#[cfg(not(feature="unstable"))]
mod raw;
#[cfg(not(feature="unstable"))]
mod server_params;
#[cfg(not(feature="unstable"))]
mod credentials;
#[cfg(not(feature="unstable"))]
mod tls;

mod builder;
mod client;
mod errors;
mod options;
mod sealed;
pub mod state;
mod transaction;

pub use edgedb_derive::{Queryable, GlobalsDelta, ConfigDelta};

pub use builder::{Builder, Config, InstanceName};
pub use credentials::TlsSecurity;
pub use client::Client;
pub use errors::Error;
pub use options::{TransactionOptions, RetryOptions};
pub use state::{GlobalsDelta, ConfigDelta};
pub use transaction::{Transaction};

#[cfg(feature="unstable")]
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
#[cfg(feature="env")]
pub async fn create_client() -> Result<Client, Error> {
    let pool = Client::new(&Builder::new().build_env().await?);
    pool.ensure_connected().await?;
    Ok(pool)
}
