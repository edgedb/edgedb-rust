use std::collections::HashMap;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use typemap::Key;


#[derive(Deserialize, Debug, Serialize)]
pub struct PostgresAddress {
    pub host: String,
    #[serde(deserialize_with="str_to_u16")]
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub server_settings: HashMap<String, String>,
}


impl Key for PostgresAddress {
    type Value = PostgresAddress;
}

fn str_to_u16<'de, D>(d: D) -> Result<u16, D::Error>
    where D: Deserializer<'de>
{
    let s: String = Deserialize::deserialize(d)?;
    s.parse().map_err(|e| de::Error::custom(e))
}
