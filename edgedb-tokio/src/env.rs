use std::fmt::Debug;
use std::io;
use std::num::NonZeroU16;
use std::time::Duration;
use std::{env, path::PathBuf, str::FromStr};

use edgedb_protocol::model;
use url::Url;

use crate::errors::{ClientError, Error, ErrorKind};
use crate::{builder::CloudCerts, ClientSecurity, InstanceName, TlsSecurity};

macro_rules! define_env {
    ($(
        #[doc=$doc:expr]
        #[env($($env_name:expr),+)]
        $(#[preprocess=$preprocess:expr])?
        $(#[parse=$parse:expr])?
        $(#[validate=$validate:expr])?
        $name:ident: $type:ty
    ),* $(,)?) => {
        #[derive(Debug, Clone)]
        pub struct Env {
        }

        impl Env {
            $(
                pub fn $name() -> ::std::result::Result<::std::option::Option<$type>, $crate::errors::Error> {
                    let mut name;
                    let mut value = None;
                    $(
                        let env_name = stringify!($env_name);
                        name = env_name;
                        if let Some(s) = $crate::env::get_env(env_name)? {
                            if value.is_some() {
                                return Err($crate::errors::ClientError::with_source(
                                    ::std::io::Error::new(
                                        ::std::io::ErrorKind::InvalidInput,
                                        format!("multiple environment variables with the same name: {env_name}, {name}"),
                                    ),
                                ));
                            }
                            value = Some(s);
                        };
                    )+
                    let Some(s) = value else {
                        return Ok(None);
                    };
                    $(let Some(s) = $preprocess(s) else {
                        return Ok(None);
                    };)?
                    #[allow(unused_labels)]
                    let value: $type = 'block: {
                        $(
                            break 'block $parse(&name, &s)?;

                            // Disable the fallback parser
                            #[cfg(all(debug_assertions, not(debug_assertions)))]
                        )?
                        $crate::env::parse(&name, &s)?
                    };

                    $($validate(name, &value)?;)?
                    Ok(Some(value))
                }
            )*
        }
    };
}

define_env!(
    /// The host to connect to.
    #[env(GEL_HOST, EDGEDB_HOST)]
    host: String,

    /// The port to connect to.
    #[env(GEL_PORT, EDGEDB_PORT)]
    #[preprocess=ignore_docker_tcp_port]
    port: NonZeroU16,

    /// The database name to connect to.
    #[env(GEL_DATABASE, EDGEDB_DATABASE)]
    #[validate=non_empty_string]
    database: String,

    /// The branch name to connect to.
    #[env(GEL_BRANCH, EDGEDB_BRANCH)]
    #[validate=non_empty_string]
    branch: String,

    /// The username to connect as.
    #[env(GEL_USER, EDGEDB_USER)]
    #[validate=non_empty_string]
    user: String,

    /// The password to use for authentication.
    #[env(GEL_PASSWORD, EDGEDB_PASSWORD)]
    password: String,

    /// TLS server name to verify.
    #[env(GEL_TLS_SERVER_NAME, EDGEDB_TLS_SERVER_NAME)]
    tls_server_name: String,

    /// Path to credentials file.
    #[env(GEL_CREDENTIALS_FILE, EDGEDB_CREDENTIALS_FILE)]
    credentials_file: String,

    /// Instance name to connect to.
    #[env(GEL_INSTANCE, EDGEDB_INSTANCE)]
    instance: InstanceName,

    /// Connection DSN string.
    #[env(GEL_DSN, EDGEDB_DSN)]
    dsn: Url,

    /// Secret key for authentication.
    #[env(GEL_SECRET_KEY, EDGEDB_SECRET_KEY)]
    secret_key: String,

    /// Client security mode.
    #[env(GEL_CLIENT_SECURITY, EDGEDB_CLIENT_SECURITY)]
    client_security: ClientSecurity,

    /// TLS security mode.
    #[env(GEL_CLIENT_TLS_SECURITY, EDGEDB_CLIENT_TLS_SECURITY)]
    client_tls_security: TlsSecurity,

    /// Path to TLS CA certificate file.
    #[env(GEL_TLS_CA, EDGEDB_TLS_CA)]
    tls_ca: String,

    /// Path to TLS CA certificate file.
    #[env(GEL_TLS_CA_FILE, EDGEDB_TLS_CA_FILE)]
    tls_ca_file: PathBuf,

    /// Cloud profile name.
    #[env(GEL_CLOUD_PROFILE, EDGEDB_CLOUD_PROFILE)]
    cloud_profile: String,

    /// Cloud certificates mode.
    #[env(_GEL_CLOUD_CERTS, _EDGEDB_CLOUD_CERTS)]
    _cloud_certs: CloudCerts,

    /// How long to wait for server to become available.
    #[env(GEL_WAIT_UNTIL_AVAILABLE, EDGEDB_WAIT_UNTIL_AVAILABLE)]
    #[parse=parse_duration]
    wait_until_available: Duration,
);

fn ignore_docker_tcp_port(s: String) -> Option<String> {
    static PORT_WARN: std::sync::Once = std::sync::Once::new();

    if s.starts_with("tcp://") {
        PORT_WARN.call_once(|| {
            eprintln!("GEL_PORT/EDGEDB_PORT is ignored when using Docker TCP port");
        });
        None
    } else {
        Some(s)
    }
}

fn non_empty_string(var: &str, s: &str) -> Result<(), Error> {
    if s.is_empty() {
        Err(ClientError::with_source(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {var} value: {s}"),
        )))
    } else {
        Ok(())
    }
}

fn parse<T: FromStr>(var: &str, s: &str) -> Result<T, Error>
where
    <T as FromStr>::Err: Debug,
{
    Ok(s.parse().map_err(|e| {
        ClientError::with_source(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {var} value: {s}: {e:?}"),
        ))
    })?)
}

pub(crate) fn get_env(name: &str) -> Result<Option<String>, Error> {
    match env::var(name) {
        Ok(v) if v.is_empty() => Ok(None),
        Ok(v) => Ok(Some(v)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(ClientError::with_source(e)
            .context(format!("Cannot decode environment variable {:?}", name))),
    }
}

fn parse_duration(var: &str, s: &str) -> Result<Duration, Error> {
    let duration = model::Duration::from_str(s).map_err(|e| {
        ClientError::with_source(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {var} value: {s}: {e:?}"),
        ))
    })?;

    duration.try_into().map_err(|e| {
        ClientError::with_source(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {var} value: {s}: {e:?}"),
        ))
    })
}
