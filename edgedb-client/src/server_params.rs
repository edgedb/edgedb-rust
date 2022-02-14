//! Parameters returned by the server on initial handshake
use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use typemap::Key;

use crate::sealed::SealedParam;

/// Address of the underlying postgres, available only in dev mode.
#[derive(Deserialize, Debug, Serialize)]
pub struct PostgresAddress {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub server_settings: HashMap<String, String>,
}

/// A trait that represents a param sent from the server.
pub trait ServerParam: SealedParam + typemap::Key + typemap::DebugAny + Send + Sync {}

impl Key for PostgresAddress {
    type Value = PostgresAddress;
}

impl SealedParam for PostgresAddress {}
impl ServerParam for PostgresAddress {}

/// ParameterStatus_SystemConfig
#[derive(Debug)]
pub struct SystemConfig {
    pub session_idle_timeout: Option<Duration>,
}

impl Key for SystemConfig {
    type Value = SystemConfig;
}

impl SealedParam for SystemConfig {}
impl ServerParam for SystemConfig {}
