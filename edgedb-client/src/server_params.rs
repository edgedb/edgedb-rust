//! Parameters returned by the server on initial handshake
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use typemap::Key;

use crate::sealed::SealedParam;


/// Address of the underlying postgress, available only in dev mode
#[derive(Deserialize, Debug, Serialize)]
pub struct PostgresAddress {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub server_settings: HashMap<String, String>,
}

/// A trait that represents param sent from server
pub trait ServerParam: SealedParam
    + typemap::Key + typemap::DebugAny + Send + Sync
{}


impl Key for PostgresAddress {
    type Value = PostgresAddress;
}

impl SealedParam for PostgresAddress { }
impl ServerParam for PostgresAddress { }
