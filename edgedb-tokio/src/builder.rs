use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::ffi::{OsString, OsStr};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::{self, FromStr};
use std::sync::Arc;
use std::time::{Duration};

use tokio::fs;
use rustls::client::ServerCertVerifier;
use serde_json::{from_slice, json};
use sha1::Digest;

use edgedb_protocol::model;

use crate::credentials::{Credentials, TlsSecurity};
use crate::errors::{ClientError};
use crate::errors::{ClientNoCredentialsError};
use crate::errors::{Error, ErrorKind, ResultExt};
use crate::errors::{InterfaceError, InvalidArgumentError};
use crate::tls;

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WAIT: Duration = Duration::from_secs(30);
pub const DEFAULT_POOL_SIZE: usize = 10;
pub const DEFAULT_HOST: &str = "localhost";
pub const DEFAULT_PORT: u16 = 5656;
const EDGEDB_CLOUD_DEFAULT_DNS_ZONE: &str = "aws.edgedb.cloud";

type Verifier = Arc<dyn ServerCertVerifier>;

/// Client security mode.
#[derive(Debug, Clone, Copy)]
pub enum ClientSecurity {
    InsecureDevMode,
    Strict,
    Default,
}

/// A builder used to create connections.
#[derive(Debug, Clone)]
pub struct Builder {
    address: Address,
    admin: bool,
    user: String,
    password: Option<String>,
    secret_key: Option<String>,
    database: String,
    pem: Option<String>,
    tls_security: TlsSecurity,
    instance_name: Option<String>,
    #[allow(dead_code)] // used only on env feature
    con_params: HashMap<String, String>,

    initialized: bool,
    wait: Duration,
    connect_timeout: Duration,
    client_security: ClientSecurity,
    creds_file_outdated: bool,
    cloud_profile: Option<String>,

    // Pool configuration
    pub(crate) max_connections: usize,
}
/// Configuration of the client
///
/// Use [`Builder`][] to create an instance
#[derive(Clone)]
pub struct Config(pub(crate) Arc<ConfigInner>);

/// Skip reading specified fields
#[derive(Default)]
pub struct SkipFields {
    ///
    pub user: bool,
    ///
    pub database: bool,
    ///
    pub wait_until_available: bool,
    ///
    pub secret_key: bool,
    ///
    pub password: bool,
    ///
    pub tls_ca_file: bool,
    ///
    pub tls_security: bool,
}

pub(crate) struct ConfigInner {
    pub address: Address,
    #[allow(dead_code)] // TODO(tailhook) for cli only
    pub admin: bool,
    pub user: String,
    pub password: Option<String>,
    pub secret_key: Option<String>,
    pub database: String,
    pub verifier: Arc<dyn ServerCertVerifier>,
    #[allow(dead_code)] // TODO(tailhook) maybe for errors
    pub instance_name: Option<String>,
    pub wait: Duration,
    pub connect_timeout: Duration,
    #[allow(dead_code)] // TODO(tailhook) maybe for future things
    pub client_security: ClientSecurity,
    #[allow(dead_code)] // used only on env feature
    pub con_params: HashMap<String, String>,

    // Pool configuration
    pub max_connections: usize,
}

#[derive(Debug, Clone)]
pub(crate) enum Address {
    Tcp((String, u16)),
    #[allow(dead_code)] // TODO(tailhook), but for cli only
    Unix(PathBuf),
}

struct DisplayAddr<'a>(Option<&'a Address>);

struct DsnHelper<'a> {
    url: &'a url::Url,
    admin: bool,
    query: HashMap<Cow<'a, str>, Cow<'a, str>>,
}

#[derive(Clone, Debug)]
enum InstanceName {
    Local(String),
    Cloud {
        org_slug: String,
        name: String,
    },
}

#[derive(Debug, serde::Deserialize)]
pub struct CloudConfig {
    pub secret_key: String,
}

#[derive(Debug, serde::Deserialize)]
struct Claims {
    #[serde(rename = "iss", skip_serializing_if = "Option::is_none")]
    issuer: Option<String>,
}

fn get_env(name: &str) -> Result<Option<String>, Error> {
    match env::var(name) {
        Ok(v) if v.is_empty() => Ok(None),
        Ok(v) => Ok(Some(v)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(e) => {
            Err(
                ClientError::with_source(e)
                .context(
                   format!("Cannot decode environment variable {:?}", name))
            )
        }
    }
}

fn get_port_env() -> Result<Option<String>, Error> {
    static PORT_WARN: std::sync::Once = std::sync::Once::new();

    let port = get_env("EDGEDB_PORT")?;
    if let Some(port) = &port {
        // ignore port if it's docker-specified string
        if port.starts_with("tcp://") {

            PORT_WARN.call_once(|| {
                log::warn!("Environment variable `EDGEDB_PORT` contains \
                           docker-link-like definition. Ingoring...");
            });

            return Ok(None);
        }
    }
    Ok(port)
}

fn get_host_port() -> Result<Option<(Option<String>, Option<u16>)>, Error> {
    let host = get_env("EDGEDB_HOST")?;
    let port = get_port_env()?.map(|port| {
        port.parse().map_err(|e| {
            ClientError::with_source(e)
                .context("cannot parse env var EDGEDB_PORT")
        })
    }).transpose()?;
    if host.is_some() || port.is_some() {
        Ok(Some((host, port)))
    } else {
        Ok(None)
    }
}

pub async fn search_dir(base: &Path) -> Result<Option<&Path>, Error>
{
    let mut path = base;
    if fs::metadata(path.join("edgedb.toml")).await.is_ok() {
        return Ok(Some(path.into()));
    }
    while let Some(parent) = path.parent() {
        if fs::metadata(parent.join("edgedb.toml")).await.is_ok() {
            return Ok(Some(parent.into()));
        }
        path = parent;
    }
    Ok(None)
}

#[cfg(unix)]
fn path_bytes<'x>(path: &'x Path) -> &'x [u8] {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes()
}

#[cfg(windows)]
fn path_bytes<'x>(path: &'x Path) -> &'x [u8] {
    path.to_str().expect("windows paths are always valid UTF-16").as_bytes()
}

fn hash(path: &Path) -> String {
    format!("{:x}", sha1::Sha1::new_with_prefix(path_bytes(path)).finalize())
}

fn stash_name(path: &Path) -> OsString {
    let hash = hash(path);
    let base = path.file_name().unwrap_or(OsStr::new(""));
    let mut base = base.to_os_string();
    base.push("-");
    base.push(&hash);
    return base;
}

fn config_dir() -> Result<PathBuf, Error> {
    let dir = if cfg!(windows) {
        dirs::data_local_dir()
            .ok_or_else(|| ClientError::with_message(
                "cannot determine local data directory"))?
            .join("EdgeDB")
            .join("config")
    } else {
        dirs::config_dir()
            .ok_or_else(|| ClientError::with_message(
                "cannot determine config directory"))?
            .join("edgedb")
    };
    Ok(dir)
}

#[allow(dead_code)]
#[cfg(target_os="linux")]
fn default_runtime_base() -> Result<PathBuf, Error> {
    extern "C" {
        fn geteuid() -> u32;
    }
    Ok(Path::new("/run/user").join(unsafe { geteuid() }.to_string()))
}

#[allow(dead_code)]
#[cfg(not(target_os="linux"))]
fn default_runtime_base() -> Result<PathBuf, Error> {
    Err(ClientError::with_message("no default runtime dir for the platform"))
}

fn stash_path(project_dir: &Path) -> Result<PathBuf, Error> {
    Ok(config_dir()?.join("projects").join(stash_name(project_dir)))
}

fn is_valid_instance_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return false;
        }
    }
    return true;
}

fn is_valid_org_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphanumeric() => {}
        _ => return false,
    }
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '-' {
            return false;
        }
    }
    return true;
}

impl fmt::Display for InstanceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstanceName::Local(name) => name.fmt(f),
            InstanceName::Cloud { org_slug, name } => write!(f, "{}/{}", org_slug, name),
        }
    }
}

impl FromStr for InstanceName {
    type Err = Error;
    fn from_str(name: &str) -> Result<InstanceName, Error> {
        if let Some((org_slug, name)) = name.split_once('/') {
            if !is_valid_instance_name(name) {
                return Err(ClientError::with_message(format!(
                    "instance name \"{}\" must be a valid identifier, \
                     regex: ^[a-zA-Z_][a-zA-Z_0-9]*$",
                    name,
                )));
            }
            if !is_valid_org_name(org_slug) {
                return Err(ClientError::with_message(format!(
                    "org name \"{}\" must be a valid identifier, \
                     regex: ^[a-zA-Z0-9][a-zA-Z0-9-]{{0,38}}$",
                    org_slug,
                )));
            }
            Ok(InstanceName::Cloud {
                org_slug: org_slug.into(),
                name: name.into(),
            })
        } else {
            if !is_valid_instance_name(name) {
                return Err(ClientError::with_message(format!(
                    "instance name must be a valid identifier, \
                     regex: ^[a-zA-Z_][a-zA-Z_0-9]*$ or \
                     a cloud instance name ORG/INST."
                )));
            }
            Ok(InstanceName::Local(name.into()))
        }
    }
}

fn cloud_config_file(profile: &str) -> anyhow::Result<PathBuf> {
    Ok(config_dir()?
        .join("cloud-credentials")
        .join(format!("{}.json", profile)))
}

impl fmt::Display for DisplayAddr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.0 {
            Some(Address::Tcp((host, port))) => {
                write!(f, "{}:{}", host, port)
            }
            Some(Address::Unix(path)) => write!(f, "unix:{}", path.display()),
            None => write!(f, "<no address>"),
        }
    }
}

impl<'a> DsnHelper<'a> {
    fn from_url(url: &'a url::Url) -> Result<Self, Error> {
        let admin = url.scheme() == "edgedbadmin";
        let mut query = HashMap::new();
        for (k, v) in url.query_pairs() {
            if query.contains_key(&k) {
                return Err(ClientError::with_message(format!(
                    "{k:?} is defined multiple times in the DSN query"
                )));
            } else {
                query.insert(k, v);
            }
        }
        Ok(Self { url, admin, query })
    }

    async fn retrieve_value<T>(
        &mut self,
        key: &'static str,
        v_from_url: Option<T>,
        conv: impl FnOnce(String) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let v_query = self.query.remove(key);
        let k_env = format!("{key}_env");
        let v_env = self.query.remove(k_env.as_str());
        let k_file = format!("{key}_file");
        let v_file = self.query.remove(k_file.as_str());

        let defined_param_names = vec![
            v_from_url.as_ref().map(|_| format!("{key} of URL")),
            v_query.as_ref().map(|_| format!("query {key}")),
            v_env.as_ref().map(|_| format!("query {k_env}")),
            v_file.as_ref().map(|_| format!("query {k_file}")),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        if defined_param_names.len() > 1 {
            return Err(InterfaceError::with_message(format!(
                "{key} defined multiple times: {}",
                defined_param_names.join(", "),
            )));
        }

        if v_from_url.is_some() {
            Ok(v_from_url)
        } else if let Some(val) = v_query {
            conv(val.to_string())
                .map(|rv| Some(rv))
                .with_context(|| format!("failed to parse value of query {key}"))
        } else if let Some(env_name) = v_env {
            let val = get_env(&env_name)?.ok_or(ClientError::with_message(format!(
                "{k_env}: {env_name} is not set"
            )))?;
            conv(val)
                .map(|rv| Some(rv))
                .with_context(|| format!("failed to parse value of {k_env}: {env_name}"))
        } else if let Some(file_path) = v_file {
            let val = fs::read_to_string(Path::new(file_path.as_ref()))
                .await
                .map_err(|e| {
                    ClientError::with_source(e)
                        .context(format!("error reading {k_file}: {file_path}"))
                })?;
            conv(val)
                .map(|rv| Some(rv))
                .with_context(|| format!("failed to parse content of {k_file}: {file_path}"))
        } else {
            Ok(None)
        }
    }

    async fn retrieve_host(&mut self, default: impl ToString) -> Result<String, Error> {
        if let Some(url::Host::Ipv6(host)) = self.url.host() {
            // async-std uses raw IPv6 address without "[]"
            Ok(host.to_string())
        } else {
            self.retrieve_value("host", self.url.host_str().map(|s| s.to_owned()), |s| Ok(s))
                .await
                .map(|rv| rv.unwrap_or_else(|| default.to_string()))
        }
    }

    async fn retrieve_port(&mut self, default: u16) -> Result<u16, Error> {
        self.retrieve_value("port", self.url.port(), |s| {
            s.parse()
                .map_err(|e| InterfaceError::with_source(e).context("invalid port"))
        })
        .await
        .map(|rv| rv.unwrap_or(default))
    }

    async fn retrieve_user(&mut self, default: impl ToString) -> Result<String, Error> {
        let username = self.url.username();
        let v = if username.is_empty() {
            None
        } else {
            Some(username.to_owned())
        };
        self.retrieve_value("user", v, |s| Ok(s))
            .await
            .map(|rv| rv.unwrap_or_else(|| default.to_string()))
    }

    async fn retrieve_password(&mut self) -> Result<Option<String>, Error> {
        let v = self.url.password().map(|s| s.to_owned());
        self.retrieve_value("password", v, |s| Ok(s)).await
    }

    async fn retrieve_database(&mut self, default: impl ToString) -> Result<String, Error> {
        let v = self.url.path().strip_prefix("/").and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_owned())
            }
        });
        self.retrieve_value("database", v, |s| {
            Ok(s.strip_prefix("/").unwrap_or(&s).into())
        })
        .await
        .map(|rv| rv.unwrap_or_else(|| default.to_string()))
    }

    async fn retrieve_secret_key(&mut self) -> Result<Option<String>, Error> {
        self.retrieve_value("secret_key", None, |s| Ok(s)).await
    }

    async fn retrieve_tls_ca_file(&mut self) -> Result<Option<String>, Error> {
        self.retrieve_value("tls_ca_file", None, |s| Ok(s)).await
    }

    async fn retrieve_tls_security(&mut self) -> Result<Option<TlsSecurity>, Error> {
        self.retrieve_value("tls_security", None, TlsSecurity::from_str).await
    }

    async fn retrieve_wait_until_available(&mut self) -> Result<Option<Duration>, Error> {
        self.retrieve_value("wait_until_available", None, |s| {
            s.parse::<model::Duration>()
                .map_err(ClientError::with_source)
                .and_then(|d| match d.is_negative() {
                    false => Ok(d.abs_duration()),
                    true => Err(ClientError::with_message(
                        "negative durations are unsupported",
                    )),
                })
        })
        .await
    }

    fn remaining_queries(&self) -> HashMap<String, String> {
        self.query
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}


impl Builder {

    /// Initializes a Builder using environment variables or project config.
    pub async fn from_env() -> Result<Builder, Error> {
        let mut builder = Builder::uninitialized();

        // optimize discovering project if defined by environment variable
        if get_env("EDGEDB_HOST")?.is_none() &&
           get_port_env()?.is_none() &&
           get_env("EDGEDB_INSTANCE")?.is_none() &&
           get_env("EDGEDB_DSN")?.is_none() &&
           get_env("EDGEDB_CONFIGURATION_FILE")?.is_none()
        {
            builder.read_project(None, true).await?;
        }

        builder.read_env_vars().await?;
        Ok(builder)
    }

    /// Reads the project config if it exists.
    ///
    /// Projects are initialized using the command-line tool:
    /// ```shell
    /// edgedb project init
    /// ```
    /// Linking to an already running EdgeDB is also possible:
    /// ```shell
    /// edgedb project init --link
    /// ```
    ///
    /// Returns a boolean value indicating whether the project was found.
    pub async fn read_project(&mut self,
        override_dir: Option<&Path>, search_parents: bool)
        -> Result<&mut Self, Error>
    {
        let dir = match get_project_dir(override_dir, search_parents).await? {
            Some(dir) => dir,
            None => return Ok(self),
        };
        let canon = fs::canonicalize(&dir).await
            .map_err(|e| ClientError::with_source(e).context(
                format!("failed to canonicalize dir {:?}", dir)
            ))?;
        let stash_path = stash_path(canon.as_ref())?;
        if fs::metadata(&stash_path).await.is_ok() {
            let instance =
                fs::read_to_string(stash_path.join("instance-name")).await
                .map_err(|e| ClientError::with_source(e).context(
                    format!("error reading project settings {:?}", dir)
                ))?;
            // read_instance() would use secret_key and cloud_profile for cloud
            // instances, so read them from env vars here first.
            // Note: even though we may set secret_key to the one from env var,
            // the outer code may still overwrite it back to what we are using
            // here correctly. This kind of setting secret_key multiple times
            // back and forth is very confusing, and it should be set only once
            // based on priority, and read_instance() should be the last step.
            if instance.contains("/") {
                self.secret_key = get_env("EDGEDB_SECRET_KEY")?;
                self.cloud_profile = get_env("EDGEDB_CLOUD_PROFILE")?;
                if self.secret_key.is_none() && self.cloud_profile.is_none() {
                    let profile =
                        fs::read_to_string(stash_path.join("cloud-profile")).await
                            .map_err(|e| ClientError::with_source(e).context(
                                format!("error reading project settings {:?}", dir)
                            ))?;
                    self.cloud_profile = Some(profile);
                }
            }
            self.read_instance(instance.trim()).await?;

        }
        Ok(self)
    }

    /// Indicates whether credentials are set for this builder.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }
    /// Read environment variables and set respective configuration parameters.
    ///
    /// This function initializes the builder if one of the following is set:
    ///
    /// * `EDGEDB_CREDENTIALS_FILE`
    /// * `EDGEDB_INSTANCE`
    /// * `EDGEDB_DSN`
    /// * `EDGEDB_HOST` or `EDGEDB_PORT`
    ///
    /// If it finds one of these then it will reset all previously set
    /// credentials.
    ///
    /// If one of the following are set:
    ///
    /// * `EDGEDB_DATABASE`
    /// * `EDGEDB_USER`
    /// * `EDGEDB_PASSWORD`
    /// * `EDGEDB_SECRET_KEY`
    ///
    /// Then the value of that environment variable will be used to set just
    /// the parameter matching that environment variable.
    ///
    /// The `client_security` and connection parameters are never modified by
    /// this function for now.
    pub async fn read_env_vars(&mut self) -> Result<&mut Self, Error> {
        let host_port = get_host_port()?;
        let creds_file = get_env("EDGEDB_CREDENTIALS_FILE")?;
        let instance = get_env("EDGEDB_INSTANCE")?;
        let dsn = get_env("EDGEDB_DSN")?;
        let compound_env_names = vec![
            host_port.as_ref().map(|_| "EDGEDB_HOST/EDGEDB_PORT"),
            creds_file.as_ref().map(|_| "EDGEDB_CREDENTIALS_FILE"),
            instance.as_ref().map(|_| "EDGEDB_INSTANCE"),
            dsn.as_ref().map(|_| "EDGEDB_DSN"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        if compound_env_names.len() > 1 {
            return Err(ClientError::with_message(format!(
                "multiple compound env vars found: {:?}",
                compound_env_names
            )));
        }
        if let Some((host, port)) = host_port {
            self.host_port(host, port)?;
        } else if let Some(path) = creds_file {
            self.read_credentials(path).await?;
        } else if let Some(instance) = instance {
            // read_instance() would use secret_key and cloud_profile for cloud
            // instances, so read them from env vars here first if they are not
            // already set in higher-level layers like explicit options.
            // Note: even though we may set secret_key to the one from env var,
            // the outer code may still overwrite it back to what we are using
            // here correctly. This kind of setting secret_key multiple times
            // back and forth is very confusing, and it should be set only once
            // based on priority, and read_instance() should be the last step.
            if instance.contains("/") {
                if self.secret_key.is_none() {
                    self.secret_key = get_env("EDGEDB_SECRET_KEY")?;
                }
            }
            self.read_instance(&instance).await?;
        } else if let Some(dsn) = dsn {
            let skip = SkipFields {
                user: get_env("EDGEDB_USER")?.is_some(),
                database: get_env("EDGEDB_DATABASE")?.is_some(),
                wait_until_available: get_env("EDGEDB_WAIT_UNTIL_AVAILABLE")?.is_some(),
                secret_key: get_env("EDGEDB_SECRET_KEY")?.is_some(),
                password: get_env("EDGEDB_PASSWORD")?.is_some(),
                tls_ca_file: get_env("EDGEDB_TLS_CA")?.is_some()
                    || get_env("EDGEDB_TLS_CA_FILE")?.is_some(),
                tls_security: get_env("EDGEDB_CLIENT_TLS_SECURITY")?.is_some(),
            };
            self.read_dsn(&dsn, skip).await.map_err(|e|
                e.context("cannot parse env var EDGEDB_DSN"))?;
        }
        if let Some(database) = get_env("EDGEDB_DATABASE")? {
            self.database = database;
        }
        if let Some(user) = get_env("EDGEDB_USER")? {
            self.user = user;
        }
        if let Some(password) = get_env("EDGEDB_PASSWORD")? {
            self.password = Some(password);
        }
        if let Some(secret_key) = get_env("EDGEDB_SECRET_KEY")? {
            self.secret_key = Some(secret_key);
        }
        if let Some(wait) = get_env("EDGEDB_WAIT_UNTIL_AVAILABLE")? {
            self.wait = wait.parse::<model::Duration>()
                .map_err(ClientError::with_source)
                .and_then(|d| match d.is_negative() {
                    false => Ok(d.abs_duration()),
                    true => Err(ClientError::with_message(
                        "negative durations are unsupported")),
                })
                .context("Invalid value {:?} for env var \
                          EDGEDB_WAIT_UNTIL_AVAILABLE.")?;
        }
        if let Some(sec) = get_env("EDGEDB_CLIENT_TLS_SECURITY")? {
            self.tls_security = TlsSecurity::from_str(&sec)
                .with_context(|| "invalid env var EDGEDB_CLIENT_TLS_SECURITY")?;
        }
        let tls_ca = get_env("EDGEDB_TLS_CA")?;
        if let Some(tls_ca_file) = get_env("EDGEDB_TLS_CA_FILE")? {
            if tls_ca.is_some() {
                return Err(ClientError::with_message(
                    "Environment variables EDGEDB_TLS_CA and \
                     EDGEDB_TLS_CA_FILE are mutually exclusive"
                ));
            }
            let pem = fs::read_to_string(&tls_ca_file).await
                .map_err(|e| ClientError::with_source(e).context(
                    format!("error reading TLS CA file {:?}", tls_ca_file)
                ))?;
            self.pem_certificates(&pem)?;
        }
        if let Some(pem) = tls_ca {
            self.pem_certificates(&pem)?;
        }
        self.read_extra_env_vars()?;
        Ok(self)
    }
    /// Read environment variables that aren't credentials
    pub fn read_extra_env_vars(&mut self) -> Result<&mut Self, Error> {
        use ClientSecurity::*;
        if let Some(mode) = get_env("EDGEDB_CLIENT_SECURITY")? {
            self.client_security = match &mode[..] {
                "default" => Default,
                "strict" => Strict,
                "insecure_dev_mode" => InsecureDevMode,
                _ => {
                    return Err(ClientError::with_message(
                        format!("Invalid value {:?} for env var \
                                EDGEDB_CLIENT_SECURITY. \
                                Options: default, strict, insecure_dev_mode.",
                                mode)
                    ));
                }
            };
        }
        Ok(self)
    }

    /// Set all credentials.
    ///
    /// This marks the builder as initialized.
    pub fn credentials(&mut self, credentials: &Credentials)
        -> Result<&mut Self, Error>
    {
        if let Some(cert_data) = &credentials.tls_ca {
            validate_certs(&cert_data)
                .context("invalid certificates in `tls_ca`")?;
        }
        self.reset_compound();
        self.address = Address::Tcp((
            credentials.host.clone()
                .unwrap_or_else(|| DEFAULT_HOST.into()),
            credentials.port,
        ));
        self.admin = false;
        self.user = credentials.user.clone();
        self.password = credentials.password.clone();
        self.database = credentials.database.clone()
                .unwrap_or_else(|| "edgedb".into());
        self.creds_file_outdated = credentials.file_outdated;
        self.tls_security = credentials.tls_security;
        self.pem = credentials.tls_ca.clone();
        self.initialized = true;
        Ok(self)
    }

    /// Returns if the credentials file is outdated.
    #[cfg(feature="unstable")]
    pub fn is_creds_file_outdated(&self) -> bool {
        self.creds_file_outdated
    }

    /// Returns the instance name if any.
    #[cfg(feature="unstable")]
    pub fn get_instance_name(&self) -> Option<&str> {
        self.instance_name.as_deref()
    }

    /// Returns the secret key if any
    #[cfg(feature="unstable")]
    pub fn get_secret_key(&self) -> Option<&str> {
        self.secret_key.as_deref()
    }

    /// Read credentials from the named instance.
    ///
    /// Named instances are created using the command-line tool, either
    /// directly:
    /// ```shell
    /// edgedb instance create <name>
    /// ```
    /// or when initializing a project:
    /// ```shell
    /// edgedb project init
    /// ```
    /// In the latter case you should use [`read_project()`][Builder::read_project]
    /// instead if possible.
    ///
    /// This will mark the builder as initialized (if reading is successful)
    /// and overwrite all credentials. However, `client_security`, pools
    /// sizes, and timeouts are kept intact.
    pub async fn read_instance(&mut self, name: &str)
        -> Result<&mut Self, Error>
    {
        let name = InstanceName::from_str(name)?;
        match &name {
            InstanceName::Local(name) => {
                self.read_credentials(
                    config_dir()?
                        .join("credentials")
                        .join(format!("{}.json", name)),
                )
                .await?;
            }
            InstanceName::Cloud { org_slug, name } => {
                let secret_key = if let Some(secret_key) = &self.secret_key {
                    secret_key.clone()
                } else {
                    let profile = self
                        .cloud_profile
                        .as_deref()
                        .map(|s| Ok(s.to_string()))
                        .or_else(|| get_env("EDGEDB_CLOUD_PROFILE").transpose())
                        .unwrap_or_else(|| Ok(String::from("default")))?;
                    let path = cloud_config_file(&profile)?;
                    let data = fs::read(path).await
                        .map_err(ClientError::with_source)?;
                    let config: CloudConfig = from_slice(&data)
                        .map_err(ClientError::with_source)?;
                    config.secret_key
                };
                let claims_b64 = secret_key
                    .splitn(3, ".")
                    .skip(1)
                    .next()
                    .ok_or(ClientError::with_message("Illegal JWT token"))?;
                let claims = base64::decode_config(claims_b64, base64::URL_SAFE_NO_PAD)
                    .map_err(ClientError::with_source)?;
                let claims: Claims = from_slice(&claims)
                    .map_err(ClientError::with_source)?;
                let dns_zone = claims
                    .issuer
                    .unwrap_or_else(|| EDGEDB_CLOUD_DEFAULT_DNS_ZONE.to_string());
                let msg = format!("{}/{}", org_slug, name);
                let checksum = crc16::State::<crc16::XMODEM>::calculate(msg.as_bytes());
                let dns_bucket = format!("c-{:x}", checksum % 9900);
                let host = format!("{}.{}.{}.i.{}", name, org_slug, dns_bucket, dns_zone);
                self.host_port(Some(host), None)?;
                self.secret_key(secret_key);
            }
        }
        self.instance_name = Some(name.to_string());
        Ok(self)
    }

    /// Read credentials from a file.
    ///
    /// This will mark the builder as initialized (if reading is successful)
    /// and overwrite all credentials. However, `client_security`, pools
    /// sizes, and timeouts are kept intact.
    pub async fn read_credentials(&mut self, path: impl AsRef<Path>)
        -> Result<&mut Self, Error>
    {
        let path = path.as_ref();
        async {
            let data = fs::read(path).await
                .map_err(ClientError::with_source)?;
            let creds = serde_json::from_slice(&data)
                .map_err(ClientError::with_source)?;
            self.credentials(&creds)?;
            Ok(())
        }.await.map_err(|e: Error| e.context(
            format!("cannot read credentials file {}", path.display())
        ))?;
        Ok(self)
    }

    /// Initialize credentials using data source name (DSN).
    ///
    /// DSN's that EdgeDB like are URL with `egdgedb::/scheme`:
    /// ```text
    /// edgedb://user:secret@localhost:5656/
    /// ```
    /// All the credentials can be specified using a DSN, although parsing a
    /// DSN may also lead to reading of environment variables (if query
    /// arguments of the for `*_env` are specified) and local files (for query
    /// arguments named `*_file`).
    ///
    /// This will mark the builder as initialized (if reading is successful)
    /// and overwrite all the credentials. However, `client_security`, pools
    /// sizes, and timeouts are kept intact.
    // TODO(tailhook) fix this ugly interface with skip fields
    pub async fn read_dsn(&mut self, dsn: &str, skip: SkipFields) -> Result<&mut Self, Error> {
        if !dsn.starts_with("edgedb://") && !dsn.starts_with("edgedbadmin://") {
            return Err(ClientError::with_message(format!(
                "String {:?} is not a valid DSN",
                dsn,
            )));
        };
        let url = url::Url::parse(dsn).map_err(|e| {
            ClientError::with_source(e).context(format!("cannot parse DSN {:?}", dsn))
        })?;
        let mut dsn = DsnHelper::from_url(&url)?;
        self.reset_compound();
        let host = dsn.retrieve_host(DEFAULT_HOST).await?;
        let port = dsn.retrieve_port(DEFAULT_PORT).await?;
        self.host_port(Some(host), Some(port))?;
        self.initialized = false;  // we're not done yet
        self.admin = dsn.admin;
        let user = dsn.retrieve_user("edgedb").await;
        if skip.user {
            user.ok();
        } else {
            self.user(user?)?;
        }
        let password = dsn.retrieve_password().await;
        if skip.password {
            password.ok();
        } else {
            self.password = password?;
        }
        let database = dsn.retrieve_database("edgedb").await;
        if skip.database {
            database.ok();
        } else {
            self.database(database?)?;
        }
        let secret_key = dsn.retrieve_secret_key().await;
        if skip.secret_key {
            secret_key.ok();
        } else {
            self.secret_key = secret_key?;
        }
        let tls_ca_file = dsn.retrieve_tls_ca_file().await;
        if skip.tls_ca_file {
            tls_ca_file.ok();
        } else if let Some(tls_ca_file) = tls_ca_file? {
            let pem = fs::read_to_string(Path::new(&tls_ca_file))
                .await
                .map_err(|e| {
                    ClientError::with_source(e).context("error reading TLS CA file")
                })?;
            self.pem_certificates(&pem)?;
        }
        let tls_security = dsn.retrieve_tls_security().await;
        if skip.tls_security {
            tls_security.ok();
        } else if let Some(s) = tls_security? {
            self.tls_security = s;
        }

        let wait_until_available = dsn.retrieve_wait_until_available().await;
        if skip.wait_until_available {
            wait_until_available.ok();
        } else if let Some(d) = wait_until_available? {
            self.wait = d;
        }
        self.con_params = dsn.remaining_queries();
        self.initialized = true;
        Ok(self)
    }
    /// Creates a new builder that has to be intialized by calling some methods.
    ///
    /// This is only useful if you have connections to multiple unrelated
    /// databases, or you want to have total control of the database
    /// initialization process.
    ///
    /// Usually, `Builder::from_env()` should be used instead.
    pub fn uninitialized() -> Builder {
        Builder {
            address: Address::Tcp((DEFAULT_HOST.into(), DEFAULT_PORT)),
            admin: false,
            user: "edgedb".into(),
            password: None,
            secret_key: None,
            database: "edgedb".into(),
            tls_security: TlsSecurity::Default,
            pem: None,
            instance_name: None,
            con_params: HashMap::new(),

            wait: DEFAULT_WAIT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            initialized: false,
            client_security: ClientSecurity::Default,
            creds_file_outdated: false,
            cloud_profile: None,

            max_connections: DEFAULT_POOL_SIZE,
        }
    }
    fn reset_compound(&mut self) {
        *self = Builder {
            // replace all of them
            address: Address::Tcp((DEFAULT_HOST.into(), DEFAULT_PORT)),
            admin: false,
            user: "edgedb".into(),
            password: None,
            secret_key: None,
            database: "edgedb".into(),
            tls_security: TlsSecurity::Default,
            pem: None,
            instance_name: None,
            con_params: HashMap::new(),

            initialized: false,
            // keep old values
            wait: self.wait,
            connect_timeout: self.connect_timeout,
            client_security: self.client_security,
            creds_file_outdated: false,
            cloud_profile: None,

            max_connections: self.max_connections,
        };
    }
    /// Extract credentials from the [Builder] so they can be saved as JSON.
    pub fn as_credentials(&self) -> Result<Credentials, Error> {
        let (host, port) = match &self.address {
            Address::Tcp(pair) => pair,
            Address::Unix(_) => {
                return Err(ClientError::with_message(
                    "Unix socket address cannot \
                    be saved as credentials file"));
            }
        };
        Ok(Credentials {
            host: Some(host.clone()),
            port: *port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: Some( self.database.clone()),
            tls_ca: self.pem.clone(),
            tls_security: self.tls_security,
            file_outdated: false,
        })
    }
    /// Get the `host` this builder is configured to connect to.
    ///
    /// For unix-socket-configured builder (only if `admin_socket` feature is
    /// enabled) returns "localhost"
    pub fn get_host(&self) -> &str {
        match &self.address {
            Address::Tcp((host, _)) => host,
            Address::Unix(_) => "localhost",
        }
    }
    /// Get the `port` this builder is configured to connect to.
    pub fn get_port(&self) -> u16 {
        match &self.address {
            Address::Tcp((_, port)) => *port,
            Address::Unix(_) => 5656
        }
    }
    /// Initialize credentials using host/port data.
    ///
    /// If either of host or port is `None`, they are replaced with the
    /// default of `localhost` and `5656` respectively.
    ///
    /// This will mark the builder as initialized and overwrite all the
    /// credentials. However, `client_security`, pools sizes, and timeouts
    /// are kept intact.
    pub fn host_port(&mut self,
        host: Option<impl Into<String>>, port: Option<u16>)
        -> Result<&mut Self, Error>
    {
        let host = host.map_or_else(|| DEFAULT_HOST.into(), |h| h.into());
        let port = port.unwrap_or(DEFAULT_PORT);
        if host.is_empty() {
            return Err(InvalidArgumentError::with_message(
                "invalid host: empty string"
            ));
        } else if host.contains(",") {
            return Err(InvalidArgumentError::with_message(
                "invalid host: multiple hosts"
            ));
        }
        if port == 0 {
            return Err(InvalidArgumentError::with_message("invalid port: 0"));
        }
        self.reset_compound();
        self.address = Address::Tcp((host, port));
        self.initialized = true;
        Ok(self)
    }

    #[cfg(feature="admin_socket")]
    /// Use admin socket instead of normal socket
    pub fn admin(&mut self) -> Result<&mut Self, Error> {
        let prefix = if let Some(name) = &self.instance_name {
            if cfg!(windows) {
                return Err(ClientError::with_message(
                    "unix sockets are not supported on Windows"));
            } else if let Some(dir) = dirs::runtime_dir() {
                dir.join(format!("edgedb-{}", name))
            } else {
                dirs::cache_dir()
                    .ok_or_else(|| ClientError::with_message(
                        "cannot determine cache directory"))?
                    .join("edgedb")
                    .join("run")
                    .join(name)
            }
        } else {
            if cfg!(target_os="macos") {
                "/var/run/edgedb".into()
            } else {
                "/run/edgedb".into()
            }
        };
        match self.address {
            Address::Tcp((_, port)) => {
                self.address = Address::Unix(
                    prefix.join(format!(".s.EDGEDB.admin.{}", port))
                );
            }
            Address::Unix(_) => {},
        }
        Ok(self)
    }

    #[cfg(feature="admin_socket")]
    /// Initialize credentials using unix socket
    pub fn unix_path(&mut self, path: impl Into<PathBuf>,
                     port: Option<u16>, admin: bool)
        -> &mut Self
    {
        self.reset_compound();
        self.admin = admin;
        let path = path.into();
        let has_socket_name = path.file_name()
            .and_then(|x| x.to_str())
            .map(|x| x.contains(".s.EDGEDB"))
            .unwrap_or(false);
        let path = if has_socket_name {
            // it's the full path
            path
        } else {
            let port = port.unwrap_or(5656);
            let socket_name = if admin {
                format!(".s.EDGEDB.admin.{}", port)
            } else {
                format!(".s.EDGEDB.{}", port)
            };
            path.join(socket_name)
        };
        // TODO(tailhook) figure out whether it's a prefix or full socket?
        self.address = Address::Unix(path.into());
        self.initialized = true;
        self
    }
    /// Get the user name for SCRAM authentication.
    pub fn get_user(&self) -> &str {
        &self.user
    }
    /// Set the user name for SCRAM authentication.
    pub fn user(&mut self, user: impl Into<String>) -> Result<&mut Self, Error> {
        let user = user.into();
        if user.is_empty() {
            return Err(InvalidArgumentError::with_message(
                "invalid user: empty string"
            ));
        }
        self.user = user;
        Ok(self)
    }
    /// Set the password for SCRAM authentication.
    pub fn password(&mut self, password: impl Into<String>) -> &mut Self {
        self.password = Some(password.into());
        self
    }
    /// Set the secret key for JWT authentication.
    pub fn secret_key(&mut self, secret_key: impl Into<String>) -> &mut Self {
        self.secret_key = Some(secret_key.into());
        self
    }
    /// Set the EdgeDB Cloud profile name to locate instances.
    pub fn cloud_profile(&mut self, cloud_profile: impl Into<String>) -> &mut Self {
        self.cloud_profile = Some(cloud_profile.into());
        self
    }
    /// Set the database name.
    pub fn database(&mut self, database: impl Into<String>) -> Result<&mut Self, Error> {
        let database = database.into();
        if database.is_empty() {
            return Err(InvalidArgumentError::with_message(
                "invalid database: empty string"
            ));
        }
        self.database = database;
        Ok(self)
    }
    /// Get the database name.
    pub fn get_database(&self) -> &str {
        &self.database
    }
    /// Set the time to wait for the database server to become available.
    ///
    /// This works by ignoring certain errors known to happen while the
    /// database is starting up or restarting (e.g. "connection refused" or
    /// early "connection reset").
    ///
    /// Note: the amount of time establishing a connection can take is the sum
    /// of `wait_until_available` plus `connect_timeout`
    pub fn wait_until_available(&mut self, time: Duration) -> &mut Self {
        self.wait = time;
        self
    }
    /// A timeout for a single connect attempt.
    ///
    /// The default is 10 seconds. A subsecond timeout should be fine for most
    /// networks. However, in some cases this can be much slower. That's
    /// because this timeout includes authentication, during which:
    /// * The password is checked (slow by design).
    /// * A compiler process is launched (slow now, may be optimized later).
    ///
    /// So in a concurrent case on slower VMs (such as CI with parallel
    /// tests), 10 seconds is more reasonable default.
    ///
    /// The `wait_until_available` setting should be larger than this value to
    /// allow multiple attempts.
    ///
    /// Note: the amount of time establishing a connection can take is the sum
    /// of `wait_until_available` plus `connect_timeout`
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the allowed certificate as a PEM file.
    pub fn pem_certificates(&mut self, cert_data: &String)
        -> Result<&mut Self, Error>
    {
        validate_certs(cert_data).context("invalid PEM certificate")?;
        self.pem = Some(cert_data.clone());
        Ok(self)
    }

    /// Updates the client TLS security mode.
    ///
    /// By default, the certificate chain is always verified; but hostname
    /// verification is disabled if configured to use only a
    /// specific certificate, and enabled if root certificates are used.
    pub fn tls_security(&mut self, value: TlsSecurity) -> &mut Self {
        self.tls_security = value;
        self
    }

    /// Modifies the client security mode.
    ///
    /// InsecureDevMode changes tls_security only from Default to Insecure
    /// Strict ensures tls_security is also Strict
    pub fn client_security(&mut self, value: ClientSecurity) -> &mut Self {
        self.client_security = value;
        self
    }

    /// A displayable form for an address this builder will connect to
    pub fn display_addr<'x>(&'x self) -> impl fmt::Display + 'x {
        if self.initialized {
            DisplayAddr(Some(&self.address))
        } else {
            DisplayAddr(None)
        }
    }

    fn trust_anchors(&self) -> Result<Vec<tls::OwnedTrustAnchor>, Error> {
        tls::OwnedTrustAnchor::read_all(
            self.pem.as_deref().unwrap_or("")
        ).map_err(ClientError::with_source_ref)
    }

    #[cfg(feature="unstable")]
    /// Returns certificate store
    pub fn root_cert_store(&self) -> Result<rustls::RootCertStore, Error> {
        self._root_cert_store()
    }

    fn _root_cert_store(&self) -> Result<rustls::RootCertStore, Error> {
        let mut roots = rustls::RootCertStore::empty();
        if self.pem.is_some() {
            roots.add_server_trust_anchors(
                self.trust_anchors()?.into_iter().map(Into::into)
            );
        } else {
            roots.add_server_trust_anchors(
                webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
                    rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                        ta.subject,
                        ta.spki,
                        ta.name_constraints,
                    )
                })
            );
            if matches!(get_env("_EDGEDB_CLOUD_CERTS")?.as_deref(), Some("staging")) {
                roots.add_server_trust_anchors(
                    tls::OwnedTrustAnchor::read_all(
                        "-----BEGIN CERTIFICATE-----
MIIFmDCCA4CgAwIBAgIQU9C87nMpOIFKYpfvOHFHFDANBgkqhkiG9w0BAQsFADBm
MQswCQYDVQQGEwJVUzEzMDEGA1UEChMqKFNUQUdJTkcpIEludGVybmV0IFNlY3Vy
aXR5IFJlc2VhcmNoIEdyb3VwMSIwIAYDVQQDExkoU1RBR0lORykgUHJldGVuZCBQ
ZWFyIFgxMB4XDTE1MDYwNDExMDQzOFoXDTM1MDYwNDExMDQzOFowZjELMAkGA1UE
BhMCVVMxMzAxBgNVBAoTKihTVEFHSU5HKSBJbnRlcm5ldCBTZWN1cml0eSBSZXNl
YXJjaCBHcm91cDEiMCAGA1UEAxMZKFNUQUdJTkcpIFByZXRlbmQgUGVhciBYMTCC
AiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBALbagEdDTa1QgGBWSYkyMhsc
ZXENOBaVRTMX1hceJENgsL0Ma49D3MilI4KS38mtkmdF6cPWnL++fgehT0FbRHZg
jOEr8UAN4jH6omjrbTD++VZneTsMVaGamQmDdFl5g1gYaigkkmx8OiCO68a4QXg4
wSyn6iDipKP8utsE+x1E28SA75HOYqpdrk4HGxuULvlr03wZGTIf/oRt2/c+dYmD
oaJhge+GOrLAEQByO7+8+vzOwpNAPEx6LW+crEEZ7eBXih6VP19sTGy3yfqK5tPt
TdXXCOQMKAp+gCj/VByhmIr+0iNDC540gtvV303WpcbwnkkLYC0Ft2cYUyHtkstO
fRcRO+K2cZozoSwVPyB8/J9RpcRK3jgnX9lujfwA/pAbP0J2UPQFxmWFRQnFjaq6
rkqbNEBgLy+kFL1NEsRbvFbKrRi5bYy2lNms2NJPZvdNQbT/2dBZKmJqxHkxCuOQ
FjhJQNeO+Njm1Z1iATS/3rts2yZlqXKsxQUzN6vNbD8KnXRMEeOXUYvbV4lqfCf8
mS14WEbSiMy87GB5S9ucSV1XUrlTG5UGcMSZOBcEUpisRPEmQWUOTWIoDQ5FOia/
GI+Ki523r2ruEmbmG37EBSBXdxIdndqrjy+QVAmCebyDx9eVEGOIpn26bW5LKeru
mJxa/CFBaKi4bRvmdJRLAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNVHRMB
Af8EBTADAQH/MB0GA1UdDgQWBBS182Xy/rAKkh/7PH3zRKCsYyXDFDANBgkqhkiG
9w0BAQsFAAOCAgEAncDZNytDbrrVe68UT6py1lfF2h6Tm2p8ro42i87WWyP2LK8Y
nLHC0hvNfWeWmjZQYBQfGC5c7aQRezak+tHLdmrNKHkn5kn+9E9LCjCaEsyIIn2j
qdHlAkepu/C3KnNtVx5tW07e5bvIjJScwkCDbP3akWQixPpRFAsnP+ULx7k0aO1x
qAeaAhQ2rgo1F58hcflgqKTXnpPM02intVfiVVkX5GXpJjK5EoQtLceyGOrkxlM/
sTPq4UrnypmsqSagWV3HcUlYtDinc+nukFk6eR4XkzXBbwKajl0YjztfrCIHOn5Q
CJL6TERVDbM/aAPly8kJ1sWGLuvvWYzMYgLzDul//rUF10gEMWaXVZV51KpS9DY/
5CunuvCXmEQJHo7kGcViT7sETn6Jz9KOhvYcXkJ7po6d93A/jy4GKPIPnsKKNEmR
xUuXY4xRdh45tMJnLTUDdC9FIU0flTeO9/vNpVA8OPU1i14vCz+MU8KX1bV3GXm/
fxlB7VBBjX9v5oUep0o/j68R/iDlCOM4VVfRa8gX6T2FU7fNdatvGro7uQzIvWof
gN9WUwCbEMBy/YhBSrXycKA8crgGg3x1mIsopn88JKwmMBa68oS7EHM9w7C4y71M
7DiA+/9Qdp9RBWJpTS9i/mDnJg1xvo8Xz49mrrgfmcAXTCJqXi24NatI3Oc=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIICTjCCAdSgAwIBAgIRAIPgc3k5LlLVLtUUvs4K/QcwCgYIKoZIzj0EAwMwaDEL
MAkGA1UEBhMCVVMxMzAxBgNVBAoTKihTVEFHSU5HKSBJbnRlcm5ldCBTZWN1cml0
eSBSZXNlYXJjaCBHcm91cDEkMCIGA1UEAxMbKFNUQUdJTkcpIEJvZ3VzIEJyb2Nj
b2xpIFgyMB4XDTIwMDkwNDAwMDAwMFoXDTQwMDkxNzE2MDAwMFowaDELMAkGA1UE
BhMCVVMxMzAxBgNVBAoTKihTVEFHSU5HKSBJbnRlcm5ldCBTZWN1cml0eSBSZXNl
YXJjaCBHcm91cDEkMCIGA1UEAxMbKFNUQUdJTkcpIEJvZ3VzIEJyb2Njb2xpIFgy
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEOvS+w1kCzAxYOJbA06Aw0HFP2tLBLKPo
FQqR9AMskl1nC2975eQqycR+ACvYelA8rfwFXObMHYXJ23XLB+dAjPJVOJ2OcsjT
VqO4dcDWu+rQ2VILdnJRYypnV1MMThVxo0IwQDAOBgNVHQ8BAf8EBAMCAQYwDwYD
VR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU3tGjWWQOwZo2o0busBB2766XlWYwCgYI
KoZIzj0EAwMDaAAwZQIwRcp4ZKBsq9XkUuN8wfX+GEbY1N5nmCRc8e80kUkuAefo
uc2j3cICeXo1cOybQ1iWAjEA3Ooawl8eQyR4wrjCofUE8h44p0j7Yl/kBlJZT8+9
vbtH7QiVzeKCOTQPINyRql6P
-----END CERTIFICATE-----"
                    )
                    .map_err(ClientError::with_source_ref)?
                    .into_iter()
                    .map(Into::into),
                );
            }
        }
        Ok(roots)
    }

    #[cfg(feature="unstable")]
    pub fn build_with_cert_verifier(&self,
                                    verifier: Arc<dyn ServerCertVerifier>)
        -> Result<Config, Error>
    {
        self._build_with_cert_verifier(verifier)
    }
    fn _build_with_cert_verifier(&self, verifier: Arc<dyn ServerCertVerifier>)
        -> Result<Config, Error>
    {
        if !self.initialized {
            return Err(ClientNoCredentialsError::with_message(
                "EdgeDB connection options are not initialized. \
                Run `edgedb project init` or use environment variables \
                to configure connection."));
        }

        Ok(Config(Arc::new(ConfigInner {
            address: self.address.clone(),
            admin: self.admin,
            user: self.user.clone(),
            password: self.password.clone(),
            secret_key: self.secret_key.clone(),
            database: self.database.clone(),
            verifier,
            instance_name: self.instance_name.clone(),
            wait: self.wait,
            connect_timeout: self.connect_timeout,
            client_security: self.client_security,
            con_params: self.con_params.clone(),

            // Pool configuration
            max_connections: self.max_connections,
        })))
    }

    fn compute_tls_security(&self) -> Result<TlsSecurity, Error> {
        use TlsSecurity::*;

        match (self.client_security, self.tls_security) {
            (ClientSecurity::Strict, Insecure | NoHostVerification) => {
                Err(ClientError::with_message(format!(
                    "client_security=strict and tls_security={} don't comply",
                    serde_json::to_string(&self.tls_security).unwrap(),
                )))
            }
            (ClientSecurity::Strict, _) => Ok(Strict),
            (ClientSecurity::InsecureDevMode, Default) => Ok(Insecure),
            (_, Default) if self.pem.is_none() => Ok(Strict),
            (_, Default) => Ok(NoHostVerification),
            (_, ts) => Ok(ts),
        }
    }

    /// Build connection and pool configuration object
    pub fn build(&self) -> Result<Config, Error> {
        use TlsSecurity::*;

        let verifier = match self.compute_tls_security()? {
            Insecure => Arc::new(tls::NullVerifier) as Verifier,
            NoHostVerification => Arc::new(tls::NoHostnameVerifier::new(
                self.trust_anchors()?
            )) as Verifier,
            Strict => Arc::new(rustls::client::WebPkiVerifier::new(
                self._root_cert_store()?,
                None,
            )) as Verifier,
            Default => unreachable!(),
        };

        self._build_with_cert_verifier(verifier)
    }

    /// Set the maximum number of underlying database connections.
    pub fn max_connections(&mut self, value: usize) -> &mut Self {
        self.max_connections = value;
        self
    }

    /// Get the path of the Unix socket if that is configured to be used.
    ///
    /// This is a deprecated API and should only be used by the command-line
    /// tool.
    #[cfg(feature="admin_socket")]
    pub fn get_unix_path(&self) -> Option<PathBuf> {
        self._get_unix_path().unwrap_or(None)
    }
    fn _get_unix_path(&self) -> Result<Option<PathBuf>, Error> {
        match &self.address {
            Address::Unix(path) => Ok(Some(path.clone())),
            Address::Tcp(_) => Ok(None),
        }
    }

    /// Generate debug JSON string
    #[cfg(feature="unstable")]
    pub fn to_json(&self) -> Result<String, Error> {
        Ok(json!({
            "address": match &self.address {
                Address::Tcp((host, port)) => json!([host, port]),
                Address::Unix(path) => json!(path.to_str().unwrap()),
            },
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "secretKey": self.secret_key,
            "tlsCAData": self.pem,
            "tlsSecurity": self.compute_tls_security()?,
            "serverSettings": self.con_params,
            "waitUntilAvailable": self.wait.as_micros() as i64,
        }).to_string())
    }
}

fn validate_certs(data: &str) -> Result<(), Error> {
    let anchors = tls::OwnedTrustAnchor::read_all(data)
        .map_err(|e| ClientError::with_source_ref(e))?;
    if anchors.is_empty() {
        return Err(ClientError::with_message(
                "PEM data contains no certificate"));
    }
    Ok(())
}

impl Config {

    /// A displayable form for an address this builder will connect to
    pub fn display_addr<'x>(&'x self) -> impl fmt::Display + 'x {
        DisplayAddr(Some(&self.0.address))
    }

    fn _get_unix_path(&self) -> Result<Option<PathBuf>, Error> {
        match &self.0.address {
            Address::Unix(path) => Ok(Some(path.clone())),
            Address::Tcp(_) => Ok(None),
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Config")
            .field("address", &self.0.address)
            .field("max_connections", &self.0.max_connections)
            // TODO(tailhook) more fields
            .finish()
    }
}

#[tokio::test]
async fn read_credentials() {
    let mut bld = Builder::uninitialized();
    bld.read_credentials("tests/credentials1.json").await.unwrap();
    assert!(matches!(&bld.address, Address::Tcp((_, 10702))));
    assert_eq!(&bld.user, "test3n");
    assert_eq!(&bld.database, "test3n");
    assert_eq!(bld.password, Some("lZTBy1RVCfOpBAOwSCwIyBIR".into()));
}

#[tokio::test]
async fn display() {
    let mut bld = Builder::uninitialized();
    bld.read_dsn("edgedb://localhost:1756", SkipFields::default()).await
        .unwrap();
    assert!(matches!(
        &bld.address,
        Address::Tcp((host, 1756)) if host == "localhost"
    ));
    /* TODO(tailhook)
    bld.unix_path("/test/my.sock");
    assert_eq!(bld.build().unwrap()._get_unix_path().unwrap(),
               Some("/test/my.sock/.s.EDGEDB.5656".into()));
    */
    #[cfg(feature="admin_socket")] {
        bld.unix_path("/test/.s.EDGEDB.8888", None, false);
        assert_eq!(bld.build().unwrap()._get_unix_path().unwrap(),
                   Some("/test/.s.EDGEDB.8888".into()));
        bld.unix_path("/test", Some(8888), false);
        assert_eq!(bld.build().unwrap()._get_unix_path().unwrap(),
                   Some("/test/.s.EDGEDB.8888".into()));
    }
}

#[tokio::test]
async fn from_dsn() {
    let mut bld = Builder::uninitialized();
    bld.read_dsn(
        "edgedb://user1:EiPhohl7@edb-0134.elb.us-east-2.amazonaws.com/db2",
        SkipFields::default(),
    ).await.unwrap();
    assert!(matches!(
        &bld.address,
        Address::Tcp((host, 5656))
        if host == "edb-0134.elb.us-east-2.amazonaws.com",
    ));
    assert_eq!(&bld.user, "user1");
    assert_eq!(&bld.database, "db2");
    assert_eq!(bld.password, Some("EiPhohl7".into()));

    let mut bld = Builder::uninitialized();
    bld.read_dsn(
        "edgedb://user2@edb-0134.elb.us-east-2.amazonaws.com:1756/db2",
        SkipFields::default(),
    ).await.unwrap();
    assert!(matches!(
        &bld.address,
        Address::Tcp((host, 1756))
        if host == "edb-0134.elb.us-east-2.amazonaws.com",
    ));
    assert_eq!(&bld.user, "user2");
    assert_eq!(&bld.database, "db2");
    assert_eq!(bld.password, None);

    // Tests overriding
    bld.read_dsn(
        "edgedb://edb-0134.elb.us-east-2.amazonaws.com:1756",
        SkipFields::default(),
    ).await.unwrap();
    assert!(matches!(
        &bld.address,
        Address::Tcp((host, 1756))
        if host == "edb-0134.elb.us-east-2.amazonaws.com",
    ));
    assert_eq!(&bld.user, "edgedb");
    assert_eq!(&bld.database, "edgedb");
    assert_eq!(bld.password, None);

    bld.read_dsn(
        "edgedb://user3:123123@[::1]:5555/abcdef",
        SkipFields::default(),
    ).await.unwrap();
    assert!(matches!(
        &bld.address,
        Address::Tcp((host, 5555)) if host == "::1",
    ));
    assert_eq!(&bld.user, "user3");
    assert_eq!(&bld.database, "abcdef");
    assert_eq!(bld.password, Some("123123".into()));
}

#[tokio::test]
#[should_panic]  // servo/rust-url#424
async fn from_dsn_ipv6_scoped_address() {
    let mut bld = Builder::uninitialized();
    bld.read_dsn(
        "edgedb://user3@[fe80::1ff:fe23:4567:890a%25eth0]:3000/ab",
        SkipFields::default(),
    ).await.unwrap();
    assert!(matches!(
        &bld.address,
        Address::Tcp((host, 3000)) if host == "fe80::1ff:fe23:4567:890a%eth0",
    ));
    assert_eq!(&bld.user, "user3");
    assert_eq!(&bld.database, "ab");
    assert_eq!(bld.password, None);
}

/// Searches for project dir either from current dir or from specified
pub async fn get_project_dir(override_dir: Option<&Path>, search_parents: bool)
    -> Result<Option<PathBuf>, Error>
{
    let dir = match override_dir {
        Some(v) => Cow::Borrowed(v),
        None => {
            Cow::Owned(env::current_dir()
                .map_err(|e| ClientError::with_source(e)
                    .context("failed to get current directory"))?
                .into())
        }
    };

    if search_parents {
        if let Some(ancestor) = search_dir(&dir).await? {
            return Ok(Some(ancestor.to_path_buf()));
        } else {
            return Ok(None);
        }
    } else {
        if !fs::metadata(dir.join("edgedb.toml")).await.is_ok() {
            return Ok(None)
        }
        return Ok(Some(dir.to_path_buf()))
    };
}
