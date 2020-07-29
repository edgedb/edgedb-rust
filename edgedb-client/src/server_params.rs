use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use typemap::Key;

use crate::client::{Sealed, PublicParam};


#[derive(Deserialize, Debug, Serialize)]
pub struct PostgresAddress {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub server_settings: HashMap<String, String>,
}


impl Key for PostgresAddress {
    type Value = PostgresAddress;
}

impl Sealed for PostgresAddress { }
impl PublicParam for PostgresAddress { }
