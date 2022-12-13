//! Parameters returned by the server on initial handshake
use std::collections::HashMap;
use std::time::Duration;
use std::fmt;
use std::any::{Any, TypeId};

use serde::{Deserialize, Serialize};

use crate::sealed::SealedParam;

#[derive(Debug)]
pub(crate) struct ServerParams(HashMap<TypeId, Box<dyn Any + Send + Sync>>);

trait AssertParams: Send + Sync + 'static {}
impl AssertParams for ServerParams {}

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
pub trait ServerParam: SealedParam + 'static {
    type Value: fmt::Debug + Send + Sync + 'static;
}

impl ServerParam for PostgresAddress {
    type Value = PostgresAddress;
}

impl SealedParam for PostgresAddress { }


/// ParameterStatus_SystemConfig
#[derive(Debug)]
pub struct SystemConfig {
    pub session_idle_timeout: Option<Duration>,
}

impl ServerParam for SystemConfig {
    type Value = SystemConfig;
}

impl SealedParam for SystemConfig { }

impl ServerParams {
    pub fn new() -> ServerParams {
        ServerParams(HashMap::new())
    }
    pub fn set<T: ServerParam>(&mut self, value: T::Value) {
        self.0.insert(TypeId::of::<T>(), Box::new(value));
    }
    pub fn get<T: ServerParam>(&self) -> Option<&T::Value> {
        self.0.get(&TypeId::of::<T>()).and_then(|v| v.downcast_ref())
    }
}
