mod builder;
mod debug;
mod sealed;
mod tls;
mod traits;

pub mod client;
pub mod credentials;
pub mod errors;
pub mod reader;
pub mod server_params;

pub use builder::Builder;
pub use tls::verify_server_cert;
pub use pool::Pool;
pub use errors::{Error};
pub use traits::Executor;

mod pool;

pub async fn connect() -> Result<Pool, Error> {
    let pool = Pool::new(Builder::from_env().await?);
    pool.ensure_connected().await?;
    Ok(pool)
}
