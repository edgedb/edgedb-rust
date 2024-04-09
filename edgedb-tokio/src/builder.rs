use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::ffi::{OsString, OsStr};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::str::{self, FromStr};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use rustls::client::danger::ServerCertVerifier;
use serde_json::from_slice;
use sha1::Digest;
use tokio::fs;

use edgedb_protocol::model;

use crate::credentials::{Credentials, TlsSecurity};
use crate::errors::{ClientNoCredentialsError, NoCloudConfigFound};
use crate::errors::{ClientError, Error, ErrorKind, ResultExt};
use crate::errors::{InterfaceError, InvalidArgumentError};
use crate::tls;

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WAIT: Duration = Duration::from_secs(30);
pub const DEFAULT_POOL_SIZE: usize = 10;
pub const DEFAULT_HOST: &str = "localhost";
pub const DEFAULT_PORT: u16 = 5656;
pub const COMPOUND_ENV_VARS: &[&str] = &[
    "EDGEDB_HOST",
    // "EDGEDB_PORT", // port check is special because of Docker
    "EDGEDB_CREDENTIALS_FILE",
    "EDGEDB_INSTANCE",
    "EDGEDB_DSN",
];
const DOMAIN_LABEL_MAX_LENGTH: usize = 63;
const CLOUD_INSTANCE_NAME_MAX_LENGTH: usize = DOMAIN_LABEL_MAX_LENGTH - 2 + 1;  // "--" -> "/"

static PORT_WARN: std::sync::Once = std::sync::Once::new();

type Verifier = Arc<dyn ServerCertVerifier>;

/// Client security mode.
#[derive(Debug, Clone, Copy)]
pub enum ClientSecurity {
    /// Disable security checks
    InsecureDevMode,
    /// Always verify domain an certificate
    Strict,
    /// Verify domain only if no specific certificate is configured
    Default,
}

/// Client security mode.
#[derive(Debug, Clone, Copy)]
pub enum CloudCerts {
    Staging,
    Local,
}

/// A builder used to create connections.
#[derive(Debug, Clone, Default)]
pub struct Builder {
    instance: Option<InstanceName>,
    dsn: Option<url::Url>,
    credentials: Option<Credentials>,
    credentials_file: Option<PathBuf>,
    host: Option<String>,
    port: Option<u16>,
    unix_path: Option<PathBuf>,
    user: Option<String>,
    database: Option<String>,
    branch: Option<String>,
    password: Option<String>,
    tls_ca_file: Option<PathBuf>,
    tls_security: Option<TlsSecurity>,
    tls_server_name: Option<String>,
    client_security: Option<ClientSecurity>,
    pem_certificates: Option<String>,
    wait_until_available: Option<Duration>,
    admin: bool,
    connect_timeout: Option<Duration>,
    secret_key: Option<String>,
    cloud_profile: Option<String>,

    // Pool configuration
    max_concurrency: Option<usize>,
}
/// Configuration of the client
///
/// Use [`Builder`][] to create an instance
#[derive(Clone)]
pub struct Config(pub(crate) Arc<ConfigInner>);

impl Config {
    /// The duration for which the client will attempt to establish a connection.
    pub fn wait_until_available(&self) -> Duration {
        self.0.wait
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigInner {
    pub address: Address,
    pub admin: bool,
    pub user: String,
    pub password: Option<String>,
    pub secret_key: Option<String>,
    pub cloud_profile: Option<String>,
    pub database: String,
    pub branch: String,
    pub verifier: Verifier,
    pub wait: Duration,
    pub connect_timeout: Duration,
    pub cloud_certs: Option<CloudCerts>,
    #[allow(dead_code)] // used only only for tests
    pub extra_dsn_query_args: HashMap<String, String>,
    #[allow(dead_code)] // used only on unstable feature
    pub creds_file_outdated: bool,

    // Pool configuration
    pub max_concurrency: Option<usize>,

    pub tls_server_name: Option<String>,

    instance_name: Option<InstanceName>,
    tls_security: TlsSecurity,
    client_security: ClientSecurity,
    pem_certificates: Option<String>,
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

/// Parsed EdgeDB instance name.
#[derive(Clone, Debug)]
pub enum InstanceName {
    /// Instance configured locally
    Local(String),
    /// Instance running on the EdgeDB Cloud
    Cloud {
        /// Organization name
        org_slug: String,
        /// Instance name within the organization
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

fn has_port_env() -> bool {
    if let Some(port) = env::var_os("EDGEDB_PORT") {
        port.to_str().map(|s| !s.starts_with("tcp://")).unwrap_or(true)
    } else {
        false
    }
}

pub async fn search_dir(base: &Path) -> Result<Option<&Path>, Error>
{
    let mut path = base;
    if fs::metadata(path.join("edgedb.toml")).await.is_ok() {
        return Ok(Some(path));
    }
    while let Some(parent) = path.parent() {
        if fs::metadata(parent.join("edgedb.toml")).await.is_ok() {
            return Ok(Some(parent));
        }
        path = parent;
    }
    Ok(None)
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> &'_ [u8] {
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
    base
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

fn is_valid_local_instance_name(name: &str) -> bool {
    // For local instance names:
    //  1. Allow only letters, numbers, underscores and single dashes
    //  2. Must not start or end with a dash
    // regex: ^[a-zA-Z_0-9]+(-[a-zA-Z_0-9]+)*$
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphanumeric() || c == '_' => {}
        _ => return false,
    }
    let mut was_dash = false;
    for c in chars {
        if c == '-' {
            if was_dash {
                return false;
            } else {
                was_dash = true;
            }
        } else {
            if !c.is_ascii_alphanumeric() && c != '_' {
                return false;
            }
            was_dash = false;
        }
    }
    !was_dash
}

fn is_valid_cloud_name(name: &str) -> bool {
    // For cloud instance name parts (organization slugs and instance names):
    //  1. Allow only letters, numbers and single dashes
    //  2. Must not start or end with a dash
    // regex: ^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphanumeric() => {}
        _ => return false,
    }
    let mut was_dash = false;
    for c in chars {
        if c == '-' {
            if was_dash {
                return false;
            } else {
                was_dash = true;
            }
        } else {
            if !c.is_ascii_alphanumeric() {
                return false;
            }
            was_dash = false;
        }
    }
    !was_dash
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
            if !is_valid_cloud_name(name) {
                return Err(ClientError::with_message(format!(
                    "invalid cloud instance name \"{}\", must follow \
                     regex: ^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$",
                    name,
                )));
            }
            if !is_valid_cloud_name(org_slug) {
                return Err(ClientError::with_message(format!(
                    "invalid cloud org name \"{}\", must follow \
                     regex: ^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$",
                    org_slug,
                )));
            }
            if name.len() > CLOUD_INSTANCE_NAME_MAX_LENGTH {
                return Err(ClientError::with_message(format!(
                    "invalid cloud instance name \"{}\": \
                    length cannot exceed {} characters",
                    name, CLOUD_INSTANCE_NAME_MAX_LENGTH,
                )));
            }
            Ok(InstanceName::Cloud {
                org_slug: org_slug.into(),
                name: name.into(),
            })
        } else {
            if !is_valid_local_instance_name(name) {
                return Err(ClientError::with_message(format!(
                    "invalid instance name \"{}\", must be either following \
                     regex: ^[a-zA-Z_0-9]+(-[a-zA-Z_0-9]+)*$ or \
                     a cloud instance name ORG/INST.",
                    name,
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
        use std::collections::hash_map::Entry::*;
        
        let admin = url.scheme() == "edgedbadmin";
        let mut query = HashMap::new();
        for (k, v) in url.query_pairs() {
            match query.entry(k) {
                Vacant(e) => {
                    e.insert(v);
                },
                Occupied(e) => {
                    return Err(ClientError::with_message(format!(
                        "{:?} is defined multiple times in the DSN query",
                        e.key()
                    )).context("invalid DSN"));
                }
            }
        }
        Ok(Self { url, admin, query })
    }

    fn ignore_value(&mut self, key: &str) {
        self.query.remove(key);
        self.query.remove(&format!("{}_env", key)[..]);
        self.query.remove(&format!("{}_file", key)[..]);
    }

    async fn retrieve_value<T>(
        &mut self,
        key: &'static str,
        v_from_url: Option<T>,
        conv: impl FnOnce(String) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        self._retrieve_value(key, v_from_url, conv).await
            .context("invalid DSN")
    }

    async fn _retrieve_value<T>(
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

    async fn retrieve_host(&mut self) -> Result<Option<String>, Error> {
        if let Some(url::Host::Ipv6(host)) = self.url.host() {
            // async-std uses raw IPv6 address without "[]"
            Ok(Some(host.to_string()))
        } else {
            let url_host = if let Some(host) = self.url.host_str() {
                validate_host(host)?;
                Some(host.to_owned())
            } else {
                None
            };
            self.retrieve_value("host", url_host, validate_host).await
        }
    }

    async fn retrieve_tls_server_name(&mut self) -> Result<Option<String>, Error> {
        self.retrieve_value("tls_server_name", None, Ok).await
    }

    async fn retrieve_port(&mut self) -> Result<Option<u16>, Error> {
        self.retrieve_value("port", self.url.port(), |s| {
            s.parse().map_err(|e| {
                InterfaceError::with_source(e).context("invalid port")
            })
        })
        .await
    }

    async fn retrieve_user(&mut self) -> Result<Option<String>, Error> {
        let username = self.url.username();
        let v = if username.is_empty() {
            None
        } else {
            Some(username.to_owned())
        };
        self.retrieve_value("user", v, validate_user).await
    }

    async fn retrieve_password(&mut self) -> Result<Option<String>, Error> {
        let v = self.url.password().map(|s| s.to_owned());
        self.retrieve_value("password", v, Ok).await
    }

    async fn retrieve_database(&mut self) -> Result<Option<String>, Error> {
        let v = self.url.path().strip_prefix('/').and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_owned())
            }
        });
        self.retrieve_value("database", v, |s| {
            let s = s.strip_prefix('/').unwrap_or(&s);
            validate_database(&s)?;
            Ok(s.to_owned())
        }).await
    }

    async fn retrieve_branch(&mut self) -> Result<Option<String>, Error> {
        let v = self.url.path().strip_prefix('/').and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_owned())
            }
        });
        self.retrieve_value("branch", v, |s| {
            let s = s.strip_prefix('/').unwrap_or(&s);
            dbg!("here");
            validate_branch(&s)?;
            Ok(s.to_owned())
        }).await
    }

    async fn retrieve_secret_key(&mut self) -> Result<Option<String>, Error> {
        self.retrieve_value("secret_key", None, Ok).await
    }

    async fn retrieve_tls_ca_file(&mut self) -> Result<Option<String>, Error> {
        self.retrieve_value("tls_ca_file", None, Ok).await
    }

    async fn retrieve_tls_security(&mut self) -> Result<Option<TlsSecurity>, Error> {
        self.retrieve_value("tls_security", None, |x| x.parse()).await
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

    /// Create a builder with empty options
    pub fn new() -> Builder {
        Default::default()
    }

    /// Set instance name
    #[cfg(feature="env")]
    pub fn instance(&mut self, name: &str) -> Result<&mut Self, Error> {
        self.instance = Some(name.parse()?);
        Ok(self)
    }

    /// Set connection parameters as DSN
    #[cfg(feature="env")]
    pub fn dsn(&mut self, dsn: &str) -> Result<&mut Self, Error> {
        if !dsn.starts_with("edgedb://") && !dsn.starts_with("edgedbadmin://")
        {
            return Err(InvalidArgumentError::with_message(format!(
                "String {:?} is not a valid DSN", dsn)));
        };
        let url = url::Url::parse(dsn)
            .map_err(|e| InvalidArgumentError::with_source(e)
                .context(format!("cannot parse DSN {:?}", dsn)))?;
        self.dsn = Some(url);
        Ok(self)
    }

    /// Set connection parameters as credentials structure
    pub fn credentials(&mut self, credentials: &Credentials)
        -> Result<&mut Self, Error>
    {
        if let Some(cert_data) = &credentials.tls_ca {
            validate_certs(cert_data).context("invalid certificates in `tls_ca`")?;
        }
        self.credentials = Some(credentials.clone());
        Ok(self)
    }

    /// Set connection parameters from file
    ///
    /// Note: file is not read immediately but is read when configuration is
    /// being built.
    #[cfg(feature="fs")]
    pub fn credentials_file(&mut self, path: impl AsRef<Path>) -> &mut Self {
        self.credentials_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set host to connect to
    pub fn host(&mut self, host: &str) -> Result<&mut Self, Error> {
        validate_host(host)?;
        self.host = Some(host.to_string());
        Ok(self)
    }

    /// Override server name indication (SNI) in TLS handshake
    pub fn tls_server_name(&mut self, tls_server_name: &str) -> Result<&mut Self, Error> {
        validate_host(tls_server_name)?;
        self.tls_server_name = Some(tls_server_name.to_string());
        Ok(self)
    }

    /// Set port to connect to
    pub fn port(&mut self, port: u16) -> Result<&mut Self, Error> {
        validate_port(port)?;
        self.port = Some(port);
        Ok(self)
    }

    /// Set path to unix socket
    #[cfg(feature="admin_socket")]
    pub fn unix_path(&mut self, path: impl AsRef<Path>)
        -> &mut Self
    {
        self.unix_path = Some(path.as_ref().to_path_buf());
        self
    }

    #[cfg(feature="admin_socket")]
    pub fn admin(&mut self, admin: bool) -> &mut Self {
        self.admin = admin;
        self
    }

    /// Set the user name for authentication.
    pub fn user(&mut self, user: &str) -> Result<&mut Self, Error> {
        validate_user(user)?;
        self.user = Some(user.to_string());
        Ok(self)
    }

    /// Set the password for SCRAM authentication.
    pub fn password(&mut self, password: &str) -> &mut Self {
        self.password = Some(password.to_string());
        self
    }
    /// Set the database name.
    pub fn database(&mut self, database: &str) -> Result<&mut Self, Error> {
        validate_database(database)?;
        self.database = Some(database.into());
        Ok(self)
    }

    /// Set the branch name.
    pub fn branch(&mut self, branch: &str) -> Result<&mut Self, Error> {
        dbg!("here");
        validate_branch(branch)?;
        self.branch = Some(branch.into());
        Ok(self)
    }

    /// Set certificate authority for TLS from file
    ///
    /// Note: file is not read immediately but is read when configuration is
    /// being built.
    #[cfg(feature="fs")]
    pub fn tls_ca_file(&mut self, path: &Path) -> &mut Self {
        self.tls_ca_file = Some(path.to_path_buf());
        self
    }

    /// Updates the client TLS security mode.
    ///
    /// By default, the certificate chain is always verified; but hostname
    /// verification is disabled if configured to use only a
    /// specific certificate, and enabled if root certificates are used.
    pub fn tls_security(&mut self, value: TlsSecurity) -> &mut Self {
        self.tls_security = Some(value);
        self
    }

    /// Modifies the client security mode.
    ///
    /// InsecureDevMode changes tls_security only from Default to Insecure
    /// Strict ensures tls_security is also Strict
    pub fn client_security(&mut self, value: ClientSecurity) -> &mut Self {
        self.client_security = Some(value);
        self
    }

    /// Set the allowed certificate as a PEM file.
    pub fn pem_certificates(&mut self, cert_data: &str)
        -> Result<&mut Self, Error>
    {
        validate_certs(cert_data).context("invalid PEM certificate")?;
        self.pem_certificates = Some(cert_data.into());
        Ok(self)
    }

    /// Set the secret key for JWT authentication.
    pub fn secret_key(&mut self, secret_key: &str) -> &mut Self {
        self.secret_key = Some(secret_key.into());
        self
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
        self.wait_until_available = Some(time);
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
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set the maximum number of underlying database connections.
    pub fn max_concurrency(&mut self, value: usize) -> &mut Self {
        self.max_concurrency = Some(value);
        self
    }

    /// Build connection and pool configuration in constrained mode
    ///
    /// Normal [`Builder::build_env()`], reads environment variables and files
    /// if appropriate to build configuration variables. This method never reads
    /// files or environment variables. Therefore it never blocks, so is not
    /// asyncrhonous.
    ///
    /// The limitations are:
    ///
    /// 1. [`Builder::credentials_file()`] is not supported
    /// 2. [`Builder::dsn()`] is not supported yet (although, will be
    ///    implemented later restricing `*_file` and `*_env` query args
    #[cfg(any(feature="unstable", feature="test"))]
    pub fn constrained_build(&self) -> Result<Config, Error> {
        let address = if let Some(unix_path) = &self.unix_path {
            let port = self.port.unwrap_or(DEFAULT_PORT);
            Address::Unix(resolve_unix(unix_path, port, self.admin))
        } else if let Some(credentials) = &self.credentials {
            let host = self.host.clone()
                .or_else(|| credentials.host.clone())
                .unwrap_or(DEFAULT_HOST.into());
            let port = self.port.unwrap_or(credentials.port);
            Address::Tcp((host, port))
        } else {
            Address::Tcp((
                self.host.clone().unwrap_or_else(|| DEFAULT_HOST.into()),
                self.port.unwrap_or(DEFAULT_PORT),
            ))
        };
        if self.instance.is_some()
            || self.dsn.is_some()
            || self.credentials_file.is_some()
            || self.tls_ca_file.is_some()
            || self.secret_key.is_some()
            || self.cloud_profile.is_some()
        {
            return Err(InterfaceError::with_message(
                    "unsupported constraint builder param"));
        }
        let creds = self.credentials.as_ref();
        let mut cfg = ConfigInner {
            address,
            tls_server_name: self.tls_server_name.clone(),
            admin: self.admin,
            user: self.user.clone()
                .or_else(|| creds.map(|c| c.user.clone()))
                .unwrap_or_else(|| "edgedb".into()),
            password: self.password.clone()
                .or_else(|| creds.and_then(|c| c.password.clone())),
            secret_key: self.secret_key.clone(),
            cloud_profile: self.cloud_profile.clone(),
            cloud_certs: None,
            database: self.database.clone()
                .or_else(|| creds.and_then(|c| c.database.clone()))
                .unwrap_or_else(|| "edgedb".into()),
            branch: self.branch.clone()
                .or_else(|| creds.and_then(|c| c.branch.clone()))
                .unwrap_or_else(|| "__default__".into()),
            instance_name: None,
            wait: self.wait_until_available.unwrap_or(DEFAULT_WAIT),
            connect_timeout: self.connect_timeout
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT),
            extra_dsn_query_args: HashMap::new(),
            creds_file_outdated: false,
            pem_certificates: self.pem_certificates.clone()
                .or_else(|| creds.and_then(|c| c.tls_ca.clone())),

            // Pool configuration
            max_concurrency: self.max_concurrency,

            // Temporary placeholders
            verifier: Arc::new(tls::NullVerifier),
            client_security: self.client_security
                .unwrap_or(ClientSecurity::Default),
            tls_security: self.tls_security
                .or_else(|| creds.map(|c| c.tls_security))
                .unwrap_or(TlsSecurity::Default),
        };

        cfg.verifier = cfg.make_verifier(cfg.compute_tls_security()?);

        Ok(Config(Arc::new(cfg)))
    }

    /// Build connection and pool configuration object
    pub async fn build_env(&self) -> Result<Config, Error> {
        let (complete, config, mut errors) = self._build_no_fail().await;
        if !complete {
            return Err(ClientNoCredentialsError::with_message(
                "EdgeDB connection options are not initialized. \
                Run `edgedb project init` or use environment variables \
                to configure connection."));
        }
        if !errors.is_empty() {
            return Err(errors.remove(0));
        }
        Ok(config)
    }

    async fn compound_owned(&self, cfg: &mut ConfigInner,
                            errors: &mut Vec<Error>)
    {
        let mut conflict = None;
        if let Some(instance) = &self.instance {
            conflict = Some("instance");
            read_instance(cfg, instance).await
                .map_err(|e| errors.push(e)).ok();
        }
        if let Some(dsn) = &self.dsn {
            if let Some(conflict) = conflict {
                errors.push(InvalidArgumentError::with_message(format!(
                    "dsn argument conflicts with {}", conflict
                )));
            }
            conflict = Some("dsn");
            self.read_dsn(cfg, dsn, errors).await;
        }
        if let Some(credentials_file) = &self.credentials_file {
            if let Some(conflict) = conflict {
                errors.push(InvalidArgumentError::with_message(format!(
                    "credentials_file argument conflicts with {}", conflict
                )));
            }
            conflict = Some("credentials_file");
            read_credentials(cfg, credentials_file).await
                .map_err(|e| errors.push(e)).ok();
        }
        if let Some(credentials) = &self.credentials {
            if let Some(conflict) = conflict {
                errors.push(InvalidArgumentError::with_message(format!(
                    "credentials argument conflicts with {}", conflict
                )));
            }
            conflict = Some("credentials");
            set_credentials(cfg, credentials)
                .map_err(|e| errors.push(e)).ok();
        }
        if let Some(host) = &self.host {
            if let Some(conflict) = conflict {
                errors.push(InvalidArgumentError::with_message(format!(
                    "host argument conflicts with {}", conflict
                )));
            }
            conflict = Some("host");
            cfg.address = Address::Tcp((
                host.into(),
                self.port.unwrap_or(DEFAULT_PORT),
            ));
        } else if let Some(port) = &self.port {
            if let Some(conflict) = conflict {
                errors.push(InvalidArgumentError::with_message(format!(
                    "port argument conflicts with {}", conflict
                )));
            }
            if let Address::Tcp((_, ref mut portref)) = &mut cfg.address {
                *portref = *port
            }
        }
        if let Some(unix_path) = &self.unix_path {
            if let Some(conflict) = conflict {
                errors.push(InvalidArgumentError::with_message(format!(
                    "unix_path argument conflicts with {}", conflict
                )));
            }
            #[allow(unused_assignments)] {
                conflict = Some("unix_path");
            }
            let port = match cfg.address {
                Address::Tcp((_, port)) => port,
                Address::Unix(_) => DEFAULT_PORT,
            };
            let full_path = resolve_unix(unix_path, port, self.admin);
            cfg.address = Address::Unix(full_path);
        }
        if let Some((d, b)) = &self.database.as_ref().zip(self.branch.as_ref()) {
            if d != b {
                errors.push(InvalidArgumentError::with_message(format!(
                    "database {d} conflicts with branch {b}"
                )))
            }
        }
    }

    async fn granular_owned(&self, cfg: &mut ConfigInner,
                            errors: &mut Vec<Error>)
    {
        if let Some(database) = &self.database {
            cfg.database = database.clone();
        }

        if let Some(branch) = &self.branch {
            cfg.branch = branch.clone();
        }

        if let Some(user) = &self.user {
            cfg.user = user.clone();
        }

        if let Some(password) = &self.password {
            cfg.password = Some(password.clone());
        }

        if let Some(tls_server_name) = &self.tls_server_name {
            cfg.tls_server_name = Some(tls_server_name.clone());
        }

        if let Some(tls_ca_file) = &self.tls_ca_file {
            match read_certificates(tls_ca_file).await {
                Ok(pem) => cfg.pem_certificates = Some(pem),
                Err(e) => errors.push(e),
            }
        }

        if let Some(pem) = &self.pem_certificates {
            cfg.pem_certificates = Some(pem.clone());
        }

        if let Some(security) = self.tls_security {
            cfg.tls_security = security;
        }

        if let Some(wait) = self.wait_until_available {
            cfg.wait = wait;
        }
    }

    async fn compound_env(&self, cfg: &mut ConfigInner,
                          errors: &mut Vec<Error>)
    {
        // Due to how shared-test-cases are implemented we have to check for
        // conflicts first and then do the actual parsing
        let mut conflict = None;
        let mut check_conflict = |var_name: &'static str| {
            if env::var_os(var_name).is_some() {
                if let Some(cvar) = conflict {
                    errors.push(ClientError::with_message(format!(
                            "{} conflicts with {}", var_name, cvar)));
                }
                conflict = Some(var_name);
            }
        };
        check_conflict("EDGEDB_INSTANCE");
        check_conflict("EDGEDB_DSN");
        check_conflict("EDGEDB_CREDENTIALS_FILE");
        check_conflict("EDGEDB_HOST");
        if let Some(port) = env::var_os("EDGEDB_PORT") {
            if !port.to_str().map(|s| s.starts_with("tcp://")).unwrap_or(false)
            {
                if let Some(cvar) = conflict {
                    if cvar != "EDGEDB_HOST" {
                        errors.push(ClientError::with_message(format!(
                                "{} conflicts with {}", "EDGEDB_PORT", cvar)));
                    }
                }
            }
            // note: not setting conflict to work with HOST
        }

        let str_env = |var_name: &'static str, errors: &mut Vec<Error>| {
            get_env(var_name).map_err(|e| errors.push(e)).ok().flatten()
        };
        if let Some(instance) = str_env("EDGEDB_INSTANCE", errors) {
            match instance.parse() {
                Ok(instance) => {
                    read_instance(cfg, &instance).await
                        .map_err(|e| errors.push(e)).ok();
                }
                Err(e) => {
                    errors.push(ClientError::with_source(e)
                                .context("EDGEDB_INSTANCE is invalid"));
                }
            }
        }
        if let Some(dsn) = str_env("EDGEDB_DSN", errors) {
            match dsn.parse() {
                Ok(url) => self.read_dsn(cfg, &url, errors).await,
                Err(e) => {
                    errors.push(ClientError::with_source(e)
                                .context("EDGEDB_DSN is invalid"));
                }
            }
        }
        if let Some(fpath) = str_env("EDGEDB_CREDENTIALS_FILE", errors) {
            read_credentials(cfg, fpath).await
                .map_err(|e| errors.push(e)).ok();
        }
        if let Some(host) = str_env("EDGEDB_HOST", errors) {
            match validate_host(&host) {
                Ok(_) => {
                    cfg.address = Address::Tcp((host, DEFAULT_PORT));
                }
                Err(e) => errors.push(e.context("EDGEDB_HOST is invalid")),
            }
        }
        if let Some(port_str) = str_env("EDGEDB_PORT", errors) {
            let port = port_str.parse()
                .map_err(ClientError::with_source)
                .and_then(validate_port)
                .context("EDGEDB_PORT is invalid");
            match port {
                Ok(port) => {
                    if let Address::Tcp((_, ref mut portref)) = &mut cfg.address {
                        *portref = port
                    }
                },
                Err(e) => {
                    if port_str.starts_with("tcp://") {
                        PORT_WARN.call_once(|| {
                            log::warn!("Environment variable `EDGEDB_PORT` \
                                contains docker-link-like definition. \
                                Ignoring...");
                        });
                    } else {
                        errors.push(e);
                    }
                }
            }
        }
    }

    async fn secret_key_env(&self, cfg: &mut ConfigInner,
                             errors: &mut Vec<Error>)
    {
        cfg.secret_key = self.secret_key.clone().or_else(|| {
            get_env("EDGEDB_SECRET_KEY")
                .map_err(|e| errors.push(e)).ok().flatten()
        });
    }

    async fn granular_env(&self, cfg: &mut ConfigInner,
                          errors: &mut Vec<Error>)
    {
        let database_branch = self.database.as_ref().or(self.branch.as_ref())
            .cloned()
            .or_else(|| {
                let database = get_env("EDGEDB_DATABASE")
                    .map_err(|e| errors.push(e)).ok()?;
                let branch = get_env("EDGEDB_BRANCH")
                    .map_err(|e| errors.push(e)).ok()?;
                
                if database.is_some() && branch.is_some() {
                    errors.push(InvalidArgumentError::with_message(
                        "Invalid environment: variables `EDGEDB_DATABASE` and `EDGEDB_BRANCH` are mutually exclusive",
                    ));
                    return None;
                }

                database.or(branch)
            });
        if let Some(name) = database_branch {
            cfg.database = name.clone();
            cfg.branch = name;
        }

        let user = self.user.clone().or_else(|| {
            get_env("EDGEDB_USER")
                .and_then(|v| v.map(validate_user).transpose())
                .map_err(|e| errors.push(e)).ok().flatten()
        });
        if let Some(user) = user {
            cfg.user = user;
        }

        let tls_server_name = self.tls_server_name.clone().or_else(|| {
            get_env("EDGEDB_TLS_SERVER_NAME")
                .map_err(|e| errors.push(e)).ok().flatten()
        });
        if let Some(tls_server_name) = tls_server_name {
            cfg.tls_server_name = Some(tls_server_name);
        }

        let password = self.password.clone().or_else(|| {
            get_env("EDGEDB_PASSWORD")
                .map_err(|e| errors.push(e)).ok().flatten()
        });
        if let Some(password) = password {
            cfg.password = Some(password);
        }

        let tls_ca_file = self.tls_ca_file.clone().or_else(|| {
            get_env("EDGEDB_TLS_CA_FILE")
                .map_err(|e| errors.push(e)).ok().flatten()
                .map(|p| p.into())
        });
        if let Some(tls_ca_file) = tls_ca_file {
            match read_certificates(tls_ca_file).await {
                Ok(pem) => cfg.pem_certificates = Some(pem),
                Err(e) => errors.push(e),
            }
        }

        let tls_ca = get_env("EDGEDB_TLS_CA")
            .map_err(|e| errors.push(e)).ok().flatten();
        if let Some(pem) = tls_ca {
            match validate_certs(&pem) {
                Ok(()) => cfg.pem_certificates = Some(pem),
                Err(e) => errors.push(e),
            }
        }

        let security = get_env("EDGEDB_CLIENT_TLS_SECURITY")
            .map_err(|e| errors.push(e)).ok().flatten()
            .and_then(|x| x.parse::<TlsSecurity>().map_err(|e| {
                errors.push(e.context("EDGEDB_CLIENT_TLS_SECURITY error"));
            }).ok());
        if let Some(security) = security {
            cfg.tls_security = security;
        }

        let wait = self.wait_until_available.or_else(|| {
            get_env("EDGEDB_WAIT_UNTIL_AVAILABLE")
            .map_err(|e| errors.push(e)).ok().flatten()
            .and_then(|x| x.parse::<model::Duration>().map_err(|e| {
                errors.push(ClientError::with_source(e)
                            .context("EDGEDB_WAIT_UNTIL_AVAILABLE error"));
            }).ok())
            .and_then(|x| x.try_into().map_err(|e| {
                errors.push(ClientError::with_source(e)
                            .context("EDGEDB_WAIT_UNTIL_AVAILABLE error"));
            }).ok())
        });
        if let Some(wait) = wait {
            cfg.wait = wait;
        }
    }

    async fn read_dsn(&self, cfg: &mut ConfigInner, url: &url::Url,
                      errors: &mut Vec<Error>)
    {
        let mut dsn = match DsnHelper::from_url(url) {
            Ok(dsn) => dsn,
            Err(e) => {
                errors.push(e);
                return;
            }
        };
        let host = dsn.retrieve_host().await
            .map_err(|e| errors.push(e)).ok().flatten()
            .unwrap_or_else(|| DEFAULT_HOST.into());
        let port = dsn.retrieve_port().await
            .map_err(|e| errors.push(e)).ok().flatten()
            .unwrap_or(DEFAULT_PORT);
        match dsn.retrieve_tls_server_name().await {
            Ok(Some(value)) => cfg.tls_server_name = Some(value),
            Ok(None) => {},
            Err(e) => errors.push(e),
        }
        cfg.address = Address::Tcp((host, port));
        cfg.admin = dsn.admin;
        match dsn.retrieve_user().await {
            Ok(Some(value)) => cfg.user = value,
            Ok(None) => {},
            Err(e) => errors.push(e),
        }
        if self.password.is_none() {
            match dsn.retrieve_password().await {
                Ok(Some(value)) => cfg.password = Some(value),
                Ok(None) => {},
                Err(e) => errors.push(e),
            }
        } else {
            dsn.ignore_value("password");
        }

        let has_query_branch = dsn.query.contains_key("branch") || dsn.query.contains_key("branch_env") || dsn.query.contains_key("branch_file");
        let has_query_database = dsn.query.contains_key("database") || dsn.query.contains_key("database_env") || dsn.query.contains_key("database_file");
        if has_query_branch && has_query_database {
            errors.push(InvalidArgumentError::with_message(
                "Invalid DSN: `database` and `branch` are mutually exclusive",
            ));
        }
        if self.branch.is_none() && self.database.is_none() {
            let database_or_branch = if has_query_database {
                dsn.retrieve_database().await
            } else {
                dsn.retrieve_branch().await
            };

            match database_or_branch {
                Ok(Some(name)) => {
                    cfg.branch = name.clone();
                    cfg.database = name;
                },
                Ok(None) => {}
                Err(e) => errors.push(e),
            }
        } else {
            dsn.ignore_value("branch");
            dsn.ignore_value("database");
        }

        match dsn.retrieve_secret_key().await {
            Ok(Some(value)) => cfg.secret_key = Some(value),
            Ok(None) => {},
            Err(e) => errors.push(e),
        }
        if self.tls_ca_file.is_none() {
            match dsn.retrieve_tls_ca_file().await {
                Ok(Some(path)) => match read_certificates(&path).await {
                    Ok(pem) => cfg.pem_certificates = Some(pem),
                    Err(e) => errors.push(e),
                },
                Ok(None) => {}
                Err(e) => errors.push(e),
            }
        } else {
            dsn.ignore_value("tls_ca_file");
        }
        match dsn.retrieve_tls_security().await {
            Ok(Some(value)) => cfg.tls_security = value,
            Ok(None) => {},
            Err(e) => errors.push(e),
        }
        match dsn.retrieve_wait_until_available().await {
            Ok(Some(value)) => cfg.wait = value,
            Ok(None) => {},
            Err(e) => errors.push(e),
        }

        cfg.extra_dsn_query_args = dsn.remaining_queries();
    }

    async fn read_project(&self, cfg: &mut ConfigInner,
                          errors: &mut Vec<Error>)
        -> bool
    {
        let pair = self._get_stash_path().await
            .map_err(|e| errors.push(e)).ok().flatten();
        if let Some((project, stash)) = pair {
            self._read_project(cfg, &project, &stash).await
                .map_err(|e| errors.push(e)).ok();
            true
        } else {
            false
        }
    }

    async fn _get_stash_path(&self)
        -> Result<Option<(PathBuf, PathBuf)>, Error>
    {
        let dir = match get_project_dir(None, true).await? {
            Some(dir) => dir,
            None => return Ok(None),
        };
        let canon = fs::canonicalize(&dir).await
            .map_err(|e| ClientError::with_source(e).context(
                format!("failed to canonicalize dir {:?}", dir)
            ))?;
        let stash_path = stash_path(canon.as_ref())?;
        if fs::metadata(&stash_path).await.is_ok() {
            return Ok(Some((dir, stash_path)));
        }
        Ok(None)
    }

    async fn _read_project(&self, cfg: &mut ConfigInner,
                           project_dir: &Path, stash_path: &Path)
        -> Result<(), Error>
    {
        let path = stash_path.join("instance-name");
        let instance =
            fs::read_to_string(&path).await
            .map_err(|e| ClientError::with_source(e).context(
                format!("error reading project settings {:?}: {:?}",
                        project_dir, path)
            ))?;
        let instance = instance.trim().parse()
            .map_err(|e| {
                ClientError::with_source(e).context(format!(
                    "cannot parse project's instance name: {:?}", instance
                ))
            })?;
        if matches!(instance, InstanceName::Cloud {..}) {
             if cfg.secret_key.is_none() && cfg.cloud_profile.is_none() {
                 let path = stash_path.join("cloud-profile");
                 let profile = fs::read_to_string(&path).await
                     .map_err(|e| ClientError::with_source(e).context(
                         format!("error reading project settings {:?}: {:?}",
                                 project_dir, path)
                     ))?.trim().into();
                 cfg.cloud_profile = Some(profile);
             }
        }
        read_instance(cfg, &instance).await?;
        let path = stash_path.join("database");
        match fs::read_to_string(&path).await {
            Ok(text) => {
                cfg.database = validate_database(text.trim())
                    .with_context(|| {
                        format!("error reading project settings {:?}: {:?}",
                                project_dir, path)
                    })?
                    .to_owned();
                cfg.branch = cfg.database.clone();
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(ClientError::with_source(e).context(
                    format!("error reading project settings {:?}: {:?}",
                            project_dir, path)
                ))
            }
        }
        Ok(())
    }


    /// Build connection and pool configuration object
    ///
    /// This is similar to `build_env` but never fails and fills in whatever
    /// fields possible in `Config`.
    ///
    /// First boolean item in the tuple is `true` if configuration is complete
    /// and can be used for connections.
    #[cfg(any(feature="unstable", feature="test"))]
    pub async fn build_no_fail(&self) -> (bool, Config, Vec<Error>) {
        self._build_no_fail().await
    }

    async fn _build_no_fail(&self) -> (bool, Config, Vec<Error>) {
        let mut errors = Vec::new();

        let mut cfg = ConfigInner {
            address: Address::Tcp((DEFAULT_HOST.into(), DEFAULT_PORT)),
            tls_server_name: self.tls_server_name.clone(),
            admin: self.admin,
            user: "edgedb".into(),
            password: None,
            secret_key: None,
            cloud_profile: None,
            cloud_certs: None,
            database: "edgedb".into(),
            branch: "__default__".into(),
            instance_name: None,
            wait: self.wait_until_available.unwrap_or(DEFAULT_WAIT),
            connect_timeout: self.connect_timeout
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT),
            extra_dsn_query_args: HashMap::new(),
            creds_file_outdated: false,
            pem_certificates: self.pem_certificates.clone(),
            client_security: self.client_security
                .unwrap_or(ClientSecurity::Default),
            tls_security: self.tls_security.unwrap_or(TlsSecurity::Default),

            // Pool configuration
            max_concurrency: self.max_concurrency,

            // Temporary placeholders
            verifier: Arc::new(tls::NullVerifier),
        };

        cfg.cloud_profile = self.cloud_profile.clone().or_else(|| {
            get_env("EDGEDB_CLOUD_PROFILE")
                .map_err(|e| errors.push(e)).ok().flatten()
        });

        let complete = if self.host.is_some() ||
           self.port.is_some() ||
           self.unix_path.is_some() ||
           self.dsn.is_some() ||
           self.instance.is_some() ||
           self.credentials.is_some() ||
           self.credentials_file.is_some()
        {
            cfg.secret_key = self.secret_key.clone();
            self.compound_owned(&mut cfg, &mut errors).await;
            self.granular_owned(&mut cfg, &mut errors).await;
            true
        } else if
            COMPOUND_ENV_VARS.iter().any(|x| env::var_os(x).is_some()) ||
            has_port_env()
        {
            self.secret_key_env(&mut cfg, &mut errors).await;
            self.compound_env(&mut cfg, &mut errors).await;
            self.granular_env(&mut cfg, &mut errors).await;
            true
        } else {
            self.secret_key_env(&mut cfg, &mut errors).await;
            let complete = self.read_project(&mut cfg, &mut errors).await;
            self.granular_env(&mut cfg, &mut errors).await;
            complete
        };

        let security = get_env("EDGEDB_CLIENT_SECURITY")
            .map_err(|e| errors.push(e)).ok().flatten()
            .and_then(|x| x.parse::<ClientSecurity>().map_err(|e| {
                errors.push(e.context("EDGEDB_CLIENT_SECURITY error"));
            }).ok());
        if let Some(security) = security {
            cfg.client_security = security;
        }

        let cloud_certs = get_env("_EDGEDB_CLOUD_CERTS")
            .map_err(|e| errors.push(e)).ok().flatten()
            .and_then(|x| x.parse::<CloudCerts>().map_err(|e| {
                errors.push(e.context("_EDGEDB_CLOUD_CERTS error"));
            }).ok());
        if let Some(cloud_certs) = cloud_certs {
            cfg.cloud_certs = Some(cloud_certs);
        }

        // we don't overwrite this param in cfg because we want
        // `with_pem_certificates` to bump security to Strict
        let tls_security = cfg.compute_tls_security()
            .map_err(|e| errors.push(e))
            .unwrap_or(TlsSecurity::Strict);
        cfg.verifier = cfg.make_verifier(tls_security);

        (complete, Config(Arc::new(cfg)), errors)
    }

}

fn resolve_unix(path: impl AsRef<Path>, port: u16, admin: bool) -> PathBuf {
    let has_socket_name = path.as_ref().file_name()
        .and_then(|x| x.to_str())
        .map(|x| x.contains(".s.EDGEDB"))
        .unwrap_or(false);
    let path = if has_socket_name {
        // it's the full path
        path.as_ref().to_path_buf()
    } else {
        let socket_name = if admin {
            format!(".s.EDGEDB.admin.{}", port)
        } else {
            format!(".s.EDGEDB.{}", port)
        };
        path.as_ref().join(socket_name)
    };
    path
}

async fn read_instance(cfg: &mut ConfigInner, name: &InstanceName)
    -> Result<(), Error>
{
    cfg.instance_name = Some(name.clone());
    match name {
        InstanceName::Local(name) => {
            read_credentials(cfg,
                config_dir()?
                    .join("credentials")
                    .join(format!("{}.json", name)),
            ).await?;
        }
        InstanceName::Cloud { org_slug, name } => {
            let secret_key = if let Some(secret_key) = &cfg.secret_key {
                secret_key.clone()
            } else {
                let profile = cfg.cloud_profile.as_deref().unwrap_or("default");
                let path = cloud_config_file(profile)?;
                let data = match fs::read(path).await {
                    Ok(data) => data,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        let hint_cmd = if profile == "default" {
                            "edgedb cloud login".into()
                        } else {
                            format!("edgedb cloud login --cloud-profile {:?}",
                                    profile)
                        };
                        return Err(NoCloudConfigFound::with_message(
                            "connecting cloud instance requires a secret key")
                            .with_headers(HashMap::from([(
                                0x_00_01,  // FIELD_HINT
                                bytes::Bytes::from(format!(
                                    "try `{}`, or provide a secret key to connect with", hint_cmd
                                )),
                            )]))
                        );
                    }
                    Err(e) => return Err(ClientError::with_source(e))?,
                };
                let config: CloudConfig = from_slice(&data)
                    .map_err(ClientError::with_source)?;
                config.secret_key
            };
            let claims_b64 = secret_key
                .split('.').nth(1)
                .ok_or(ClientError::with_message("Illegal JWT token"))?;
            let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(claims_b64)
                .map_err(ClientError::with_source)?;
            let claims: Claims = from_slice(&claims)
                .map_err(ClientError::with_source)?;
            let dns_zone = claims
                .issuer
                .ok_or(ClientError::with_message("Invalid secret key"))?;
            let org_slug = org_slug.to_lowercase();
            let name = name.to_lowercase();
            let msg = format!("{}/{}", org_slug, name);
            let checksum = crc16::State::<crc16::XMODEM>::calculate(
                msg.as_bytes());
            let dns_bucket = format!("c-{:02}", checksum % 100);
            cfg.address = Address::Tcp((
                format!("{}--{}.{}.i.{}",
                        name, org_slug, dns_bucket, dns_zone),
                DEFAULT_PORT,
            ));
            cfg.secret_key = Some(secret_key);
        }
    }
    Ok(())
}

async fn read_credentials(cfg: &mut ConfigInner, path: impl AsRef<Path>)
    -> Result<(), Error>
{
    let path = path.as_ref();
    async {
        let data = fs::read(path).await
            .map_err(ClientError::with_source)?;
        let creds = serde_json::from_slice(&data)
            .map_err(ClientError::with_source)?;
        set_credentials(cfg, &creds)?;
        Ok(())
    }.await.map_err(|e: Error| e.context(
        format!("cannot read credentials file {}", path.display())
    ))?;
    Ok(())
}


async fn read_certificates(path: impl AsRef<Path>) -> Result<String, Error> {

    let data = fs::read_to_string(path.as_ref()).await
        .map_err(|e| ClientError::with_source(e)
                 .context("error reading TLS CA file"))?;
    validate_certs(&data)
        .context("invalid certificates")?;
    Ok(data)
}

fn set_credentials(cfg: &mut ConfigInner, creds: &Credentials)
    -> Result<(), Error>
{
    if let Some(cert_data) = &creds.tls_ca {
        validate_certs(cert_data)
            .context("invalid certificates in `tls_ca`")?;
        cfg.pem_certificates = Some(cert_data.into());
    }
    cfg.address = Address::Tcp((
        creds.host.clone().unwrap_or_else(|| DEFAULT_HOST.into()),
        creds.port,
    ));
    cfg.user = creds.user.clone();
    cfg.password = creds.password.clone();
    
    if let Some((b, d)) = creds.branch.as_ref().zip(creds.database.as_ref()) {
        if b != d {
            return Err(ClientError::with_message(
                "branch and database are mutually exclusive")
            );
        }
    }
    let db_branch = creds.branch.as_ref().or(creds.database.as_ref());
    cfg.database = db_branch.cloned().unwrap_or_else(|| "edgedb".into());
    cfg.branch = db_branch.cloned().unwrap_or_else(|| "__default__".into());
    cfg.tls_security = creds.tls_security;
    cfg.creds_file_outdated = creds.file_outdated;
    Ok(())
}

fn validate_certs(data: &str) -> Result<(), Error> {
    let root_store = tls::read_root_cert_pem(data)
        .map_err(ClientError::with_source_ref)?;
    if root_store.is_empty() {
        return Err(ClientError::with_message(
                "PEM data contains no certificate"));
    }
    Ok(())
}

fn validate_host<T: AsRef<str>>(host: T) -> Result<T, Error> {
    if host.as_ref().is_empty() {
        return Err(InvalidArgumentError::with_message(
            "invalid host: empty string"
        ));
    } else if host.as_ref().contains(',') {
        return Err(InvalidArgumentError::with_message(
            "invalid host: multiple hosts"
        ));
    }
    Ok(host)
}

fn validate_port(port: u16) -> Result<u16, Error> {
    if port == 0 {
        return Err(InvalidArgumentError::with_message(
                "invalid port: port cannot be zero"));
    }
    Ok(port)
}

fn validate_branch<T: AsRef<str>>(branch: T) -> Result<T, Error> {
    if branch.as_ref().is_empty() {
        return Err(InvalidArgumentError::with_message(
            "invalid branch: empty string"
        ));
    }
    Ok(branch)
}

fn validate_database<T: AsRef<str>>(database: T) -> Result<T, Error> {
    if database.as_ref().is_empty() {
        return Err(InvalidArgumentError::with_message(
            "invalid database: empty string"
        ));
    }
    Ok(database)
}

fn validate_user<T: AsRef<str>>(user: T) -> Result<T, Error> {
    if user.as_ref().is_empty() {
        return Err(InvalidArgumentError::with_message(
            "invalid user: empty string"
        ));
    }
    Ok(user)
}

impl Config {

    /// A displayable form for an address this builder will connect to
    pub fn display_addr(&self) -> impl fmt::Display + '_ {
        DisplayAddr(Some(&self.0.address))
    }

    /// Is admin connection desired
    #[cfg(feature="admin_socket")]
    pub fn admin(&self) -> bool {
        self.0.admin
    }

    /// User name
    pub fn user(&self) -> &str {
        &self.0.user
    }

    /// Database name
    pub fn database(&self) -> &str {
        &self.0.database
    }

    /// Extract credentials from the [Builder] so they can be saved as JSON.
    pub fn as_credentials(&self) -> Result<Credentials, Error> {
        let (host, port) = match &self.0.address {
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
            user: self.0.user.clone(),
            password: self.0.password.clone(),
            database: if self.0.branch == "__default__" { Some(self.0.database.clone()) } else { None },
            branch: if self.0.branch == "__default__" { None } else { Some(self.0.branch.clone()) },
            tls_ca: self.0.pem_certificates.clone(),
            tls_security: self.0.tls_security,
            file_outdated: false,
        })
    }

    /// Generate debug JSON string
    #[cfg(feature="unstable")]
    pub fn to_json(&self) -> String {
        serde_json::json!({
            "address": match &self.0.address {
                Address::Tcp((host, port)) => serde_json::json!([host, port]),
                Address::Unix(path) => serde_json::json!(path.to_str().unwrap()),
            },
            "database": self.0.database,
            "branch": self.0.branch,
            "user": self.0.user,
            "password": self.0.password,
            "secretKey": self.0.secret_key,
            "tlsCAData": self.0.pem_certificates,
            "tlsSecurity": self.0.compute_tls_security().unwrap(),
            "tlsServerName": self.0.tls_server_name,
            "serverSettings": self.0.extra_dsn_query_args,
            "waitUntilAvailable": self.0.wait.as_micros() as i64,
        }).to_string()
    }

    /// Server host name (if doesn't use unix socket)
    pub fn host(&self) -> Option<&str> {
        match self.0.address {
            Address::Tcp((ref host, _)) => Some(host),
            _ => None,
        }
    }

    /// Server port (if doesn't use unix socket)
    pub fn port(&self) -> Option<u16> {
        match self.0.address {
            Address::Tcp((_, port)) => Some(port),
            _ => None,
        }
    }

    /// Instance name if set and if it's local
    pub fn local_instance_name(&self) -> Option<&str> {
        match self.0.instance_name {
            Some(InstanceName::Local(ref name)) => Some(name),
            _ => None,
        }
    }

    /// Name of the instance if set
    pub fn instance_name(&self) -> Option<&InstanceName> {
        self.0.instance_name.as_ref()
    }

    /// Secret key if set
    pub fn secret_key(&self) -> Option<&str> {
        self.0.secret_key.as_deref()
    }

    /// Return HTTP(s) url to server
    ///
    /// If not connected via unix socket
    pub fn http_url(&self, tls: bool) -> Option<String> {
        match &self.0.address {
            Address::Tcp((host, port)) => {
                let s = if tls { "s" } else {""};
                Some(format!("http{}://{}:{}", s, host, port))
            }
            Address::Unix(_) => None,
        }
    }

    fn _get_unix_path(&self) -> Result<Option<PathBuf>, Error> {
        match &self.0.address {
            Address::Unix(path) => Ok(Some(path.clone())),
            Address::Tcp(_) => Ok(None),
        }
    }

    /// Return the same config with changed password
    pub fn with_password(mut self, password: &str) -> Config {
        Arc::make_mut(&mut self.0).password = Some(password.to_owned());
        self
    }

    /// Return the same config with changed database
    pub fn with_database(mut self, database: &str) -> Result<Config, Error> {
        if database.is_empty() {
            return Err(InvalidArgumentError::with_message(
                "invalid database: empty string"
            ));
        }
        Arc::make_mut(&mut self.0).database = database.to_owned();
        Ok(self)
    }

    /// Return the same config with changed database branch
    pub fn with_branch(mut self, branch: &str) -> Result<Config, Error> {
        if branch.is_empty() {
            return Err(InvalidArgumentError::with_message(
                "invalid branch: empty string"
            ));
        }
        Arc::make_mut(&mut self.0).branch = branch.to_owned();
        Ok(self)
    }

    /// Return the same config with changed wait until available timeout
    #[cfg(any(feature="unstable", feature="test"))]
    pub fn with_wait_until_available(mut self, wait: Duration) -> Config {
        Arc::make_mut(&mut self.0).wait = wait;
        self
    }

    /// Return the same config with changed certificates
    #[cfg(any(feature="unstable", feature="test"))]
    pub fn with_pem_certificates(mut self, pem: &str) -> Result<Config, Error>
    {
        validate_certs(pem).context("invalid PEM certificate")?;
        let cfg = Arc::make_mut(&mut self.0);
        cfg.pem_certificates = Some(pem.to_owned());
        cfg.verifier = cfg.make_verifier(cfg.compute_tls_security()?);
        Ok(self)
    }

    #[cfg(feature="admin_socket")]
    pub fn with_unix_path(mut self, path: &Path) -> Config {
        Arc::make_mut(&mut self.0).address = Address::Unix(path.into());
        self
    }

    /// Returns true if credentials file is in outdated format
    #[cfg(any(feature="unstable", feature="test"))]
    pub fn is_creds_file_outdated(&self) -> bool {
        self.0.creds_file_outdated
    }

    /// Return the certificate store of the config
    #[cfg(any(feature="unstable", feature="test"))]
    pub fn root_cert_store(&self) -> Result<rustls::RootCertStore, Error> {
        Ok(self.0.root_cert_store())
    }

    /// Return the same config with changed certificate verifier
    ///
    /// Command-line tool uses this for interactive verifier
    #[cfg(any(feature="unstable", feature="test"))]
    pub fn with_cert_verifier(mut self, verifier: Verifier) -> Config {
        Arc::make_mut(&mut self.0).verifier = verifier;
        self
    }
}

impl ConfigInner {
    fn compute_tls_security(&self) -> Result<TlsSecurity, Error> {
        use TlsSecurity::*;

        match (self.client_security, self.tls_security) {
            (ClientSecurity::Strict, Insecure | NoHostVerification) => {
                Err(ClientError::with_message(format!(
                    "client_security=strict and tls_security={} don't comply",
                    self.tls_security,
                )))
            }
            (ClientSecurity::Strict, _) => Ok(Strict),
            (ClientSecurity::InsecureDevMode, Default) => Ok(Insecure),
            (_, Default) if self.pem_certificates.is_none() => Ok(Strict),
            (_, Default) => Ok(NoHostVerification),
            (_, ts) => Ok(ts),
        }
    }
    fn root_cert_store(&self) -> rustls::RootCertStore {
        if self.pem_certificates.is_some() {
            tls::read_root_cert_pem(
                self.pem_certificates.as_deref().unwrap_or("")
            ).expect("all certificates have been verified previously")
        } else {
            let mut root_store = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.into()
            };
            if let Some(certs) = self.cloud_certs {
                let data = match certs {
                    // Staging certs retrieved from
                    // https://letsencrypt.org/docs/staging-environment/#root-certificates
                    CloudCerts::Staging => include_str!("letsencrypt_staging.pem"),
                    // Local nebula development root cert found in
                    // nebula/infra/terraform/local/ca/root.certificate.pem
                    CloudCerts::Local => include_str!("nebula_development.pem"),
                };
                root_store.extend(
                    tls::read_root_cert_pem(data).expect("embedded certs are correct").roots
                );
            }

            root_store
        }
    }
    fn make_verifier(&self, tls_security: TlsSecurity) -> Verifier {
        use TlsSecurity::*;

        let root_store = Arc::new(self.root_cert_store());

        match tls_security {
            Insecure => {
                Arc::new(tls::NullVerifier) as Verifier
            },
            NoHostVerification => {
                Arc::new(tls::NoHostnameVerifier::new(root_store)) as Verifier
            },
            Strict => {
                rustls::client::WebPkiServerVerifier
                    ::builder(root_store)
                    .build()
                    .expect("WebPkiServerVerifier to build correctly")
                    as Verifier
            },
            Default => unreachable!(),
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Config")
            .field("address", &self.0.address)
            .field("max_concurrency", &self.0.max_concurrency)
            // TODO(tailhook) more fields
            .finish()
    }
}

impl FromStr for ClientSecurity {
    type Err = Error;
    fn from_str(s: &str) -> Result<ClientSecurity, Error> {
        use ClientSecurity::*;

        match s {
            "default" => Ok(Default),
            "strict" => Ok(Strict),
            "insecure_dev_mode" => Ok(InsecureDevMode),
            mode => Err(ClientError::with_message(
                format!("Invalid client security: {:?}. \
                        Options: default, strict, insecure_dev_mode.",
                        mode)
            )),
        }
    }
}

impl FromStr for CloudCerts {
    type Err = Error;
    fn from_str(s: &str) -> Result<CloudCerts, Error> {
        use CloudCerts::*;

        match s {
            "staging" => Ok(Staging),
            "local" => Ok(Local),
            option => Err(ClientError::with_message(
                format!("Invalid cloud certificates: {:?}. \
                        Options: staging, local.",
                        option)
            )),
        }
    }
}

#[tokio::test]
async fn test_read_credentials() {
    let cfg = Builder::new()
        .credentials_file("tests/credentials1.json")
        .build_env().await.unwrap();
    assert!(matches!(&cfg.0.address, Address::Tcp((_, 10702))));
    assert_eq!(&cfg.0.user, "test3n");
    assert_eq!(&cfg.0.database, "test3n");
    assert_eq!(cfg.0.password, Some("lZTBy1RVCfOpBAOwSCwIyBIR".into()));
}

#[tokio::test]
async fn display() {
    let cfg = Builder::new()
        .dsn("edgedb://localhost:1756").unwrap()
        .build_env().await.unwrap();
    assert!(matches!(
        &cfg.0.address,
        Address::Tcp((host, 1756)) if host == "localhost"
    ));
    /* TODO(tailhook)
    bld.unix_path("/test/my.sock");
    assert_eq!(bld.build().unwrap()._get_unix_path().unwrap(),
               Some("/test/my.sock/.s.EDGEDB.5656".into()));
    */
    #[cfg(feature="admin_socket")] {
        let cfg = Builder::new()
            .unix_path("/test/.s.EDGEDB.8888")
            .build_env().await.unwrap();
        assert_eq!(cfg._get_unix_path().unwrap(),
                   Some("/test/.s.EDGEDB.8888".into()));
        let cfg = Builder::new()
            .port(8888).unwrap()
            .unix_path("/test")
            .build_env().await.unwrap();
        assert_eq!(cfg._get_unix_path().unwrap(),
                   Some("/test/.s.EDGEDB.8888".into()));
    }
}

#[tokio::test]
async fn from_dsn() {
    let cfg = Builder::new()
        .dsn(
            "edgedb://user1:EiPhohl7@edb-0134.elb.us-east-2.amazonaws.com/db2",
        ).unwrap()
        .build_env().await.unwrap();
    assert!(matches!(
        &cfg.0.address,
        Address::Tcp((host, 5656))
        if host == "edb-0134.elb.us-east-2.amazonaws.com",
    ));
    assert_eq!(&cfg.0.user, "user1");
    assert_eq!(&cfg.0.database, "db2");
    assert_eq!(&cfg.0.branch, "db2");
    assert_eq!(cfg.0.password, Some("EiPhohl7".into()));

    let cfg = Builder::new()
        .dsn(
            "edgedb://user2@edb-0134.elb.us-east-2.amazonaws.com:1756/db2",
        ).unwrap()
        .build_env().await.unwrap();
    assert!(matches!(
        &cfg.0.address,
        Address::Tcp((host, 1756))
        if host == "edb-0134.elb.us-east-2.amazonaws.com",
    ));
    assert_eq!(&cfg.0.user, "user2");
    assert_eq!(&cfg.0.database, "db2");
    assert_eq!(&cfg.0.branch, "db2");
    assert_eq!(cfg.0.password, None);

    // Tests overriding
    let cfg = Builder::new()
        .dsn(
            "edgedb://edb-0134.elb.us-east-2.amazonaws.com:1756",
        ).unwrap()
        .build_env().await.unwrap();
    assert!(matches!(
        &cfg.0.address,
        Address::Tcp((host, 1756))
        if host == "edb-0134.elb.us-east-2.amazonaws.com",
    ));
    assert_eq!(&cfg.0.user, "edgedb");
    assert_eq!(&cfg.0.database, "edgedb");
    assert_eq!(&cfg.0.branch, "__default__");
    assert_eq!(cfg.0.password, None);

    let cfg = Builder::new()
        .dsn("edgedb://user3:123123@[::1]:5555/abcdef").unwrap()
        .build_env().await.unwrap();
    assert!(matches!(
        &cfg.0.address,
        Address::Tcp((host, 5555)) if host == "::1",
    ));
    assert_eq!(&cfg.0.user, "user3");
    assert_eq!(&cfg.0.database, "abcdef");
    assert_eq!(&cfg.0.branch, "abcdef");
    assert_eq!(cfg.0.password, Some("123123".into()));
}

#[tokio::test]
#[should_panic]  // servo/rust-url#424
async fn from_dsn_ipv6_scoped_address() {
    let cfg = Builder::new()
        .dsn(
            "edgedb://user3@[fe80::1ff:fe23:4567:890a%25eth0]:3000/ab",
        ).unwrap()
        .build_env().await.unwrap();
    assert!(matches!(
        &cfg.0.address,
        Address::Tcp((host, 3000)) if host == "fe80::1ff:fe23:4567:890a%eth0",
    ));
    assert_eq!(&cfg.0.user, "user3");
    assert_eq!(&cfg.0.database, "ab");
    assert_eq!(cfg.0.password, None);
}

#[test]
fn test_instance_name() {
    for inst_name in [
        "abc",
        "_localdev",
        "123",
        "___",
        "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
        "abc-123",
        "a-b-c_d-e-f",
        "_-_-_-_",

        "abc/def",
        "123/456",
        "abc-123/def-456",
        "123-abc/456-def",
        "a-b-c/1-2-3",
    ] {
        match InstanceName::from_str(inst_name) {
            Ok(InstanceName::Local(name)) => assert_eq!(name, inst_name),
            Ok(InstanceName::Cloud { org_slug, name }) => {
                let (o, i) = inst_name
                    .split_once('/')
                    .expect("test case must have one slash");
                assert_eq!(org_slug, o);
                assert_eq!(name, i);
            }
            Err(e) => panic!("{:#}", e),
        }
    }
    for name in [
        "",
        "-leading-dash",
        "trailing-dash-",
        "double--dash",
        "-leading-dash/abc",
        "trailing-dash-/abc",
        "double--dash/abc",
        "abc/-leading-dash",
        "abc/trailing-dash-",
        "abc/double--dash",
        "abc/_localdev",
        "under_score/abc",
        "123/45678901234567890123456789012345678901234567890123456789012345678901234567890",
    ] {
        assert!(InstanceName::from_str(name).is_err(), "unexpected success: {}", name);
    }
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
                    .context("failed to get current directory"))?)
        }
    };

    if search_parents {
        if let Some(ancestor) = search_dir(&dir).await? {
            Ok(Some(ancestor.to_path_buf()))
        } else {
            Ok(None)
        }
    } else {
        if fs::metadata(dir.join("edgedb.toml")).await.is_err() {
            return Ok(None)
        }
        Ok(Some(dir.to_path_buf()))
    }
}
