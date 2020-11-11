//! Credentials file handling routines
use std::default::Default;
use serde::{Serialize, Deserialize};


/// A structure that represents contents of the credentials file
#[derive(Serialize, Deserialize, Debug)]
#[non_exhaustive]
pub struct Credentials {
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub host: Option<String>,
    #[serde(default="default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub password: Option<String>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub database: Option<String>,
}

fn default_port() -> u16 {
    5656
}

impl Default for Credentials {
    fn default() -> Credentials {
        Credentials {
            host: None,
            port: 5656,
            user: "edgedb".into(),
            password: None,
            database: None,
        }
    }
}
