//! Credentials file handling routines
use std::fmt;
use std::str::FromStr;

use serde::{ser, Serialize, Deserialize};

use crate::errors::{Error, ErrorKind};


/// TLS Client Security Mode
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum TlsSecurity {
    /// Allow any certificate for TLS connection
    Insecure,
    /// Verify certificate against trusted chain but allow any host name
    ///
    /// This is useful for localhost (you can't make trusted chain certificate
    /// for localhost). And when certificate of specific server is stored in
    /// credentials file so it's secure regardless of which host name was used
    /// to expose the server to the network.
    NoHostVerification,
    /// Normal TLS certificate check (checks trusted chain and hostname)
    Strict,
    /// If there is a specific certificate in credentials, do not check
    /// the host name, otherwise use `Strict` mode
    Default,
}


/// A structure that represents the contents of the credentials file.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Credentials {
    pub host: Option<String>,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: Option<String>,
    pub branch: Option<String>,
    pub tls_ca: Option<String>,
    pub tls_security: TlsSecurity,
    pub(crate) file_outdated: bool,
}


#[derive(Serialize, Deserialize)]
struct CredentialsCompat {
    #[serde(default, skip_serializing_if="Option::is_none")]
    host: Option<String>,
    #[serde(default="default_port")]
    port: u16,
    user: String,
    #[serde(default, skip_serializing_if="Option::is_none")]
    password: Option<String>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    database: Option<String>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    branch: Option<String>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    tls_cert_data: Option<String>,  // deprecated
    #[serde(default, skip_serializing_if="Option::is_none")]
    tls_ca: Option<String>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    tls_verify_hostname: Option<bool>,  // deprecated
    tls_security: Option<TlsSecurity>,
}


fn default_port() -> u16 {
    5656
}

impl FromStr for TlsSecurity {
    type Err = Error;
    fn from_str(val: &str) -> Result<Self, Error> {
        match val {
            "default" => Ok(TlsSecurity::Default),
            "insecure" => Ok(TlsSecurity::Insecure),
            "no_host_verification" => Ok(TlsSecurity::NoHostVerification),
            "strict" => Ok(TlsSecurity::Strict),
            val => Err(crate::errors::ClientError::with_message(format!(
                "Invalid value {:?}. \
                Options: default, insecure, no_host_verification, strict.",
                val,
            ))),
        }
    }
}

impl fmt::Display for TlsSecurity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl TlsSecurity {
    fn as_str(&self) -> &'static str {
        use TlsSecurity::*;

        match self {
            Default => "default",
            Insecure => "insecure",
            NoHostVerification => "no_host_verification",
            Strict => "strict",
        }
    }
}


impl Default for Credentials {
    fn default() -> Credentials {
        Credentials {
            host: None,
            port: 5656,
            user: "edgedb".into(),
            password: None,
            database: None,
            branch: None,
            tls_ca: None,
            tls_security: TlsSecurity::Default,
            file_outdated: false,
        }
    }
}


impl Serialize for Credentials {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let creds = CredentialsCompat {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            tls_ca: self.tls_ca.clone(),
            tls_cert_data: self.tls_ca.clone(),
            tls_security: Some(self.tls_security),
            tls_verify_hostname: match self.tls_security {
                TlsSecurity::Default => None,
                TlsSecurity::Strict => Some(true),
                TlsSecurity::NoHostVerification => Some(false),
                TlsSecurity::Insecure => Some(false),
            },
        };

        CredentialsCompat::serialize(&creds, serializer)
    }
}

#[cfg(feature="fs")]
impl<'de> Deserialize<'de> for Credentials {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let creds = CredentialsCompat::deserialize(deserializer)?;
        let expected_verify = match creds.tls_security {
            Some(TlsSecurity::Strict) => Some(true),
            Some(TlsSecurity::NoHostVerification) => Some(false),
            Some(TlsSecurity::Insecure) => Some(false),
            _ => None,
        };
        if creds.tls_verify_hostname.is_some() &&
            creds.tls_security.is_some() &&
            expected_verify.zip(creds.tls_verify_hostname)
                .map(|(creds, expected)| creds != expected)
                .unwrap_or(false)
        {
            Err(serde::de::Error::custom(format!(
                "detected conflicting settings. \
                 \ntls_security =\n{}\nbut tls_verify_hostname =\n{}",
                serde_json::to_string(&creds.tls_security)
                    .map_err(serde::de::Error::custom)?,
                serde_json::to_string(&creds.tls_verify_hostname)
                    .map_err(serde::de::Error::custom)?,
            )))
        } else if creds.tls_ca.is_some() &&
            creds.tls_cert_data.is_some() &&
            creds.tls_ca != creds.tls_cert_data
        {
            Err(serde::de::Error::custom(format!(
                "detected conflicting settings. \
                 \ntls_ca =\n{:#?}\nbut tls_cert_data =\n{:#?}",
                creds.tls_ca,
                creds.tls_cert_data,
            )))
        } else {
            Ok(Credentials {
                host: creds.host,
                port: creds.port,
                user: creds.user,
                password: creds.password,
                database: creds.database,
                branch: creds.branch,
                tls_ca: creds.tls_ca.or(creds.tls_cert_data.clone()),
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
