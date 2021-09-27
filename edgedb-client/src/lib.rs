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

pub use builder::Builder;
pub use pool::Pool;
pub use errors::{Error};
pub use traits::Executor;

mod pool;

/// Create a connection to the database with default parameters
///
/// It's expected that connection parameters are set up using environment
/// (either environment variables or project configuration in `edgedb.toml`)
/// so no configuration is specified here.
///
/// This method tries to esablish single connection immediately to
/// ensure that configuration is valid and will error out otherwise.
///
/// For more fine-grained setup see [`Pool`] and [`Builder`] documentation and
/// the source of this function.
pub async fn connect() -> Result<Pool, Error> {
    let pool = Pool::new(Builder::from_env().await?);
    pool.ensure_connected().await?;
    Ok(pool)
}
