#[warn(missing_docs)]

mod builder;
mod debug;
mod sealed;
mod traits;

#[cfg(feature="unstable")]
pub mod tls;
#[cfg(not(feature="unstable"))]
mod tls;

#[cfg(feature="unstable")]
pub mod client;
#[cfg(not(feature="unstable"))]
mod client;

#[cfg(feature="unstable")]
pub mod reader;
#[cfg(not(feature="unstable"))]
mod reader;

#[cfg(feature="unstable")]
pub mod credentials;
#[cfg(not(feature="unstable"))]
mod credentials;

#[cfg(feature="unstable")]
pub mod server_params;
#[cfg(not(feature="unstable"))]
mod server_params;

pub mod errors;

pub use builder::{Builder, Config};
pub use pool::Client;
pub use errors::{Error};
pub use traits::{Executor, ExecuteResult};

pub use edgedb_protocol::model;
pub use edgedb_protocol::query_arg::{QueryArg, QueryArgs};
pub use edgedb_protocol::{QueryResult};

#[cfg(feature="derive")]
pub use edgedb_derive::Queryable;

mod pool;

/// Create a connection to the database with default parameters.
///
/// It's expected that connection parameters are set using the environment
/// (either environment variables or project configuration in `edgedb.toml`),
/// so no configuration is specified here.
///
/// This method tries to establish a single connection immediately to ensure
/// that the configuration is valid. It will return an error if it cannot do
/// so.
///
/// For more fine-grained setup see the [`Client`] and [`Builder`]
/// documentation and the source of this function.
pub async fn connect() -> Result<Client, Error> {
    let pool = Client::new(Builder::from_env().await?.build()?);
    pool.ensure_connected().await?;
    Ok(pool)
}
