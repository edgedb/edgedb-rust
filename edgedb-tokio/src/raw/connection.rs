use std::sync::Arc;

use crate::raw::ConnInner;
use crate::tls;
use crate::{Config};
use crate::errors::{Error, ClientError, ErrorKind};

use rustls::client::ServerCertVerifier;


impl ConnInner {
    pub fn is_consistent(&self) -> bool {
        todo!();
    }
    pub async fn connect(config: &Config) -> Result<Self, Error> {
        let conn = tls::connector(config.0.verifier.clone())
            .map_err(|e| ClientError::with_source_ref(e)
                     .context("cannot create TLS connector"))?;
        todo!();
    }
}

fn connect(config: &Config, verifier: Arc<dyn ServerCertVerifier>)
    -> Result<ConnInner, Error>
{
    todo!();
}
