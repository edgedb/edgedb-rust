#[cfg(feature="unstable")]
pub mod raw;

#[cfg(not(feature="unstable"))]
mod raw;

mod builder;
mod client;
mod credentials;
mod errors;
mod sealed;
mod server_params;
mod tls;
mod transaction;

pub use builder::{Builder, Config};
pub use client::Client;
pub use errors::Error;
pub use transaction::{Transaction};

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
    let pool = Client::new(&Builder::from_env().await?.build()?);
    pool.ensure_connected().await?;
    Ok(pool)
}
