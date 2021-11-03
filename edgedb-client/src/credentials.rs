//! Credentials file handling routines
use std::default::Default;

use serde::{de, Serialize, Deserialize};


#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum TlsSecurity {
    Insecure,
    NoHostVerification,
    Strict,
    Default,
}


/// A structure that represents the contents of the credentials file.
#[derive(Serialize, Debug)]
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
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub tls_cert_data: Option<String>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub(crate) tls_verify_hostname: Option<bool>, // deprecated
    pub tls_security: TlsSecurity,
    #[serde(skip)]
    pub(crate) file_outdated: bool,
}


#[derive(Deserialize)]
struct CredentialsCompat {
    host: Option<String>,
    #[serde(default="default_port")]
    port: u16,
    user: String,
    password: Option<String>,
    database: Option<String>,
    tls_cert_data: Option<String>,
    tls_verify_hostname: Option<bool>,
    tls_security: Option<TlsSecurity>,
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
            tls_cert_data: None,
            tls_verify_hostname: None,
            tls_security: TlsSecurity::Default,
            file_outdated: false,
        }
    }
}


impl<'de> Deserialize<'de> for Credentials {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let creds = CredentialsCompat::deserialize(deserializer)?;
        let expected_verify = match creds.tls_security {
            Some(TlsSecurity::Strict) => Some(true),
            Some(TlsSecurity::NoHostVerification) => Some(false),
            _ => None,
        };
        if creds.tls_verify_hostname.is_some() &&
            creds.tls_security.is_some() &&
            creds.tls_verify_hostname != expected_verify
        {
            Err(de::Error::custom(format!(
                "detected conflicting settings: \
                 tls_security={} but tls_verify_hostname={}",
                serde_json::to_string(&creds.tls_security)
                    .map_err(de::Error::custom)?,
                serde_json::to_string(&creds.tls_verify_hostname)
                    .map_err(de::Error::custom)?,
            )))
        } else {
            Ok(Credentials {
                host: creds.host,
                port: creds.port,
                user: creds.user,
                password: creds.password,
                database: creds.database,
                tls_cert_data: creds.tls_cert_data,
                tls_verify_hostname: None,
                tls_security: creds.tls_security.unwrap_or(
                    match creds.tls_verify_hostname {
                        None => TlsSecurity::Default,
                        Some(true) => TlsSecurity::Strict,
                        Some(false) => TlsSecurity::NoHostVerification,
                    }
                ),
                file_outdated: creds.tls_verify_hostname.is_some() &&
                    creds.tls_security.is_none(),
            })
        }
    }
}
