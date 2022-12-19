use std::env;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;
use std::time::{Duration};

use rustls::client::ServerCertVerifier;

use crate::credentials::{Credentials, TlsSecurity};
use crate::errors::{ClientError};
use crate::errors::{ClientNoCredentialsError};
use crate::errors::{Error, ErrorKind, ResultExt};
use crate::tls;

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WAIT: Duration = Duration::from_secs(30);
pub const DEFAULT_POOL_SIZE: usize = 10;
pub const DEFAULT_HOST: &str = "localhost";
pub const DEFAULT_PORT: u16 = 5656;

type Verifier = Arc<dyn ServerCertVerifier>;


/// A builder used to create connection configuration
///
/// Note: in most cases you don't need to tweak connection configuration as
/// it's read from the environment. So using
/// [`create_client`][crate::create_client] in this case
/// is encouraged.
#[derive(Debug, Clone)]
pub struct Builder {
    addr: Address,
    admin: bool,
    user: String,
    password: Option<String>,
    database: String,
    pem: Option<String>,
    tls_security: TlsSecurity,
    instance_name: Option<String>,

    initialized: bool,
    wait: Duration,
    connect_timeout: Duration,
    insecure_dev_mode: bool,
    creds_file_outdated: bool,

    // Pool configuration
    pub(crate) max_connections: usize,
}

#[derive(Debug, Clone)]
pub(crate) enum Address {
    Tcp((String, u16)),
    #[allow(dead_code)] // only with feature="unstable"
    Unix(PathBuf),
}

/// Configuration of the client
///
/// Use [`Builder`][] to create an instance
#[derive(Clone)]
pub struct Config(pub(crate) Arc<ConfigInner>);

pub(crate) struct ConfigInner {
    pub address: Address,
    #[allow(dead_code)] // TODO(tailhook), but for cli only
    pub admin: bool,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub verifier: Arc<dyn ServerCertVerifier>,
    #[allow(dead_code)] // TODO(tailhook) maybe for errors
    pub instance_name: Option<String>,
    pub wait: Duration,
    pub connect_timeout: Duration,
    #[allow(dead_code)] // TODO(tailhook) maybe for future things
    pub insecure_dev_mode: bool,

    // Pool configuration
    pub max_connections: usize,
}

struct DisplayAddr<'a>(bool, &'a Address);

impl fmt::Display for DisplayAddr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.0 {
            write!(f, "<no address>")
        // TODO
        } else {
            match &self.1 {
                Address::Unix(path) => write!(f, "{}", path.display()),
                Address::Tcp((host, port)) => write!(f, "{}:{}", host, port),
            }
        }
    }
}

#[cfg(feature="env")]
/// Searches for project dir either from current dir or from specified
pub async fn get_project_dir(override_dir: Option<&Path>, search_parents: bool)
    -> Result<Option<PathBuf>, Error>
{
    use std::borrow::Cow;
    use tokio::fs;

    let dir = match override_dir {
        Some(v) => Cow::Borrowed(v.as_ref()),
        None => {
            Cow::Owned(env::current_dir()
                .map_err(|e| ClientError::with_source(e)
                    .context("failed to get current directory"))?
                .into())
        }
    };

    if search_parents {
        if let Some(ancestor) = search_dir(&dir).await? {
            return Ok(Some(ancestor.into()));
        } else {
            return Ok(None);
        }
    } else {
        if !fs::metadata(&dir.join("edgedb.toml")).await.is_ok() {
            return Ok(None)
        }
        return Ok(Some(dir.to_path_buf().into()))
    };
}

#[cfg(feature="env")]
fn get_env(name: &str) -> Result<Option<String>, Error> {
    match std::env::var(name) {
        Ok(v) if v.is_empty() => Ok(None),
        Ok(v) => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => {
            Err(
                ClientError::with_source(e)
                .context(
                   format!("Cannot decode environment variable {:?}", name))
            )
        }
    }
}

#[cfg(feature="env")]
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

#[cfg(feature="env")]
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

#[cfg(feature="fs")]
pub async fn search_dir(base: &Path) -> Result<Option<&Path>, Error>
{
    use tokio::fs;

    for dir in base.ancestors() {
        if fs::metadata(&dir.join("edgedb.toml")).await.is_ok() {
            return Ok(Some(dir.into()));
        }
    }
    Ok(None)
}

#[cfg(feature="fs")]
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

#[cfg(feature="fs")]
fn stash_path(project_dir: &Path) -> Result<PathBuf, Error> {
    Ok(config_dir()?.join("projects").join(stash_name(project_dir)))
}

#[cfg(all(unix, feature="fs"))]
fn path_bytes<'x>(path: &'x Path) -> &'x [u8] {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes()
}

#[cfg(windows)]
fn path_bytes<'x>(path: &'x Path) -> &'x [u8] {
    path.to_str().expect("windows paths are always valid UTF-16").as_bytes()
}

#[cfg(feature="fs")]
fn hash(path: &Path) -> String {
    use sha1::Digest;

    base16ct::lower::encode_string(
        &sha1::Sha1::new_with_prefix(path_bytes(path)).finalize()[..]
    )
}

#[cfg(feature="fs")]
fn stash_name(path: &Path) -> std::ffi::OsString {
    let hash = hash(path);
    let base = path.file_name().unwrap_or(std::ffi::OsStr::new(""));
    let mut base = base.to_os_string();
    base.push("-");
    base.push(&hash);
    return base;
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

#[cfg(feature="fs")]
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

impl Config {

    /// A displayable form for an address.
    pub fn display_addr<'x>(&'x self) -> impl fmt::Display + 'x {
        DisplayAddr(true, &self.0.address)
    }
}

impl Builder {

    /// A displayable form for an address.
    pub fn display_addr<'x>(&'x self) -> impl fmt::Display + 'x {
        DisplayAddr(self.initialized, &self.addr)
    }
    /// Indicates whether credentials are set for this builder.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Initializes a Builder using environment variables or project config.
    #[cfg(feature="env")]
    pub async fn from_env() -> Result<Builder, Error> {
        let mut builder = Builder::uninitialized();

        // optimize discovering project if defined by environment variable
        if get_env("EDGEDB_HOST")?.is_none() &&
           get_port_env()?.is_none() &&
           get_env("EDGEDB_INSTANCE")?.is_none() &&
           get_env("EDGEDB_DSN")?.is_none() &&
           get_env("EDGEDB_CONFIGURATION_FILE")?.is_none()
        {
            builder.read_project(None, false).await?;
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
    #[cfg(feature="fs")]
    pub async fn read_project(&mut self,
        override_dir: Option<&Path>, search_parents: bool)
        -> Result<&mut Self, Error>
    {
        use tokio::fs;

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
            self.read_instance(instance.trim()).await?;

        }
        Ok(self)
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
    ///
    /// Then the value of that environment variable will be used to set just
    /// the parameter matching that environment variable.
    ///
    /// The `insecure_dev_mode` and connection parameters are never modified by
    /// this function for now.
    #[cfg(feature="env")]
    pub async fn read_env_vars(&mut self) -> Result<&mut Self, Error> {
        if let Some((host, port)) = get_host_port()? {
            self.host_port(host, port);
        } else if let Some(path) = get_env("EDGEDB_CREDENTIALS_FILE")? {
            self.read_credentials(path).await?;
        } else if let Some(instance) = get_env("EDGEDB_INSTANCE")? {
            self.read_instance(&instance).await?;
        } else if let Some(dsn) = get_env("EDGEDB_DSN")? {
            self.read_dsn(&dsn).await.map_err(|e|
                e.context("cannot parse env var EDGEDB_DNS"))?;
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
        if let Some(sec) = get_env("EDGEDB_CLIENT_TLS_SECURITY")? {
            self.tls_security = match &sec[..] {
                "default" => TlsSecurity::Default,
                "insecure" => TlsSecurity::Insecure,
                "no_host_verification" => TlsSecurity::NoHostVerification,
                "strict" => TlsSecurity::Strict,
                _ => {
                    return Err(ClientError::with_message(
                        format!("Invalid value {:?} for env var \
                                EDGEDB_CLIENT_TLS_SECURITY. \
                                Options: default, insecure, \
                                no_host_verification, strict.",
                                sec)
                    ));
                }
            };
        }
        let tls_ca = get_env("EDGEDB_TLS_CA")?;

        #[cfg(feature="fs")]
        if let Some(tls_ca_file) = get_env("EDGEDB_TLS_CA_FILE")? {
            if tls_ca.is_some() {
                return Err(ClientError::with_message(
                    "Environment variables EDGEDB_TLS_CA and \
                     EDGEDB_TLS_CA_FILE are mutually exclusive"
                ));
            }
            let pem = tokio::fs::read_to_string(&tls_ca_file).await
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
    #[cfg(feature="env")]
    pub fn read_extra_env_vars(&mut self) -> Result<&mut Self, Error> {
        use edgedb_protocol::model;

        if let Some(mode) = get_env("EDGEDB_CLIENT_SECURITY")? {
            self.insecure_dev_mode = match &mode[..] {
                "default" => false,
                "insecure_dev_mode" => true,
                _ => {
                    return Err(ClientError::with_message(
                        format!("Invalid value {:?} for env var \
                                EDGEDB_CLIENT_SECURITY. \
                                Options: default, insecure_dev_mode.",
                                mode)
                    ));
                }
            };
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
        Ok(self)
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
    /// and overwrite all credentials. However, `insecure_dev_mode`, pools
    /// sizes, and timeouts are kept intact.
    #[cfg(feature="fs")]
    pub async fn read_instance(&mut self, name: &str)
        -> Result<&mut Self, Error>
    {
        if !is_valid_instance_name(name) {
            return Err(ClientError::with_message(format!(
                "instance name {:?} contains unsupported characters", name)));
        }
        self.read_credentials(
            config_dir()?.join("credentials").join(format!("{}.json", name))
        ).await?;
        self.instance_name = Some(name.into());
        Ok(self)
    }

    /// Read credentials from a file.
    ///
    /// This will mark the builder as initialized (if reading is successful)
    /// and overwrite all credentials. However, `insecure_dev_mode`, pools
    /// sizes, and timeouts are kept intact.
    #[cfg(feature="fs")]
    pub async fn read_credentials(&mut self, path: impl AsRef<Path>)
        -> Result<&mut Self, Error>
    {
        use tokio::fs;

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
        let host = credentials.host.clone()
                .unwrap_or_else(|| DEFAULT_HOST.into());
        let port = credentials.port;
        self.addr = Address::Tcp((host, port));
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

    /// Returns the instance name if any when the credentials file is outdated.
    #[cfg(feature="unstable")]
    pub fn get_instance_name_for_creds_update(&self) -> Option<&str> {
        if self.creds_file_outdated {
            self.instance_name.as_deref()
        } else {
            None
        }
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


    #[cfg(feature="unstable")]
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
        self.addr = Address::Unix(path.into());
        self.initialized = true;
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
        match &self.addr {
            Address::Unix(path) => Ok(Some(path.clone())),
            Address::Tcp(_) => Ok(None),
        }
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
    /// and overwrite all the credentials. However, `insecure_dev_mode`, pools
    /// sizes, and timeouts are kept intact.
    pub async fn read_dsn(&mut self, dsn: &str) -> Result<&mut Self, Error> {
        let admin = dsn.starts_with("edgedbadmin://");
        if !dsn.starts_with("edgedb://") && !admin {
            return Err(ClientError::with_message(format!(
                "String {:?} is not a valid DSN", dsn)));
        };
        let url = url::Url::parse(dsn)
            .map_err(|e| ClientError::with_source(e)
                .context(format!("cannot parse DSN {:?}", dsn)))?;
        self.reset_compound();
        let host = if let Some(url::Host::Ipv6(host)) = url.host() {
            // async-std uses raw IPv6 address without "[]"
            host.to_string()
        } else {
            url.host_str().unwrap_or(DEFAULT_HOST).to_owned()
        };
        let port = url.port().unwrap_or(DEFAULT_PORT);
        self.addr = Address::Tcp((host, port));
        self.admin = admin;
        self.user = if url.username().is_empty() {
            "edgedb".to_owned()
        } else {
            url.username().to_owned()
        };
        self.password = url.password().map(|s| s.to_owned());
        self.database = url.path().strip_prefix("/")
                .unwrap_or("edgedb").to_owned();
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
            addr: Address::Tcp((DEFAULT_HOST.into(), DEFAULT_PORT)),
            admin: false,
            user: "edgedb".into(),
            password: None,
            database: "edgedb".into(),
            tls_security: TlsSecurity::Default,
            pem: None,
            instance_name: None,

            wait: DEFAULT_WAIT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            initialized: false,
            insecure_dev_mode: false,
            creds_file_outdated: false,

            max_connections: DEFAULT_POOL_SIZE,
        }
    }
    fn reset_compound(&mut self) {
        *self = Builder {
            // replace all of them
            addr: Address::Tcp((DEFAULT_HOST.into(), DEFAULT_PORT)),
            admin: false,
            user: "edgedb".into(),
            password: None,
            database: "edgedb".into(),
            tls_security: TlsSecurity::Default,
            pem: None,
            instance_name: None,

            initialized: false,
            // keep old values
            wait: self.wait,
            connect_timeout: self.connect_timeout,
            insecure_dev_mode: self.insecure_dev_mode,
            creds_file_outdated: false,

            max_connections: self.max_connections,
        };
    }
    /// Extract credentials from the [Builder] so they can be saved as JSON.
    pub fn as_credentials(&self) -> Result<Credentials, Error> {
        if let Address::Tcp((host, port)) = &self.addr {
            Ok(Credentials {
                host: Some(host.clone()),
                port: *port,
                user: self.user.clone(),
                password: self.password.clone(),
                database: Some( self.database.clone()),
                tls_ca: self.pem.clone(),
                tls_security: self.tls_security,
                file_outdated: false
            })
        } else {
            return Err(ClientError::with_message(
                    "only TCP addresses are supported in credentials"));
        }
    }
    /// Create an admin socket instead of a regular one.
    ///
    /// This behavior is deprecated and is only used for command-line tools.
    #[cfg(feature="admin_socket")]
    pub fn admin(&mut self, value: bool)
        -> &mut Self
    {
        self.admin = value;
        self
    }
    /// Get the `host` this builder is configured to connect to.
    pub fn get_host(&self) -> &str {
        match &self.addr {
            Address::Tcp((host, _)) => host,
            _ => panic!("not a TCP address"),
        }
    }
    /// Get the `port` this builder is configured to connect to.
    pub fn get_port(&self) -> u16 {
        match self.addr {
            Address::Tcp((_, port)) => port,
            _ => panic!("not a TCP address"),
        }
    }
    /// Initialize credentials using host/port data.
    ///
    /// If either of host or port is `None`, they are replaced with the
    /// default of `localhost` and `5656` respectively.
    ///
    /// This will mark the builder as initialized and overwrite all the
    /// credentials. However, `insecure_dev_mode`, pools sizes, and timeouts
    /// are kept intact.
    pub fn host_port(&mut self,
        host: Option<impl Into<String>>, port: Option<u16>)
        -> &mut Self
    {
        self.reset_compound();
        let host = host.map_or_else(|| DEFAULT_HOST.into(), |h| h.into());
        let port = port.unwrap_or(DEFAULT_PORT);
        self.addr = Address::Tcp((host, port));
        self.initialized = true;
        self
    }
    /// Get the user name for SCRAM authentication.
    pub fn get_user(&self) -> &str {
        &self.user
    }
    /// Set the user name for SCRAM authentication.
    pub fn user(&mut self, user: impl Into<String>) -> &mut Self {
        self.user = user.into();
        self
    }
    /// Set the password for SCRAM authentication.
    pub fn password(&mut self, password: impl Into<String>) -> &mut Self {
        self.password = Some(password.into());
        self
    }
    /// Set the database name.
    pub fn database(&mut self, database: impl Into<String>) -> &mut Self {
        self.database = database.into();
        self
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

    /// Enables insecure dev mode.
    ///
    /// This disables certificate validation entirely.
    pub fn insecure_dev_mode(&mut self, value: bool) -> &mut Self {
        self.insecure_dev_mode = value;
        self
    }

    /// Set the maximum number of underlying database connections.
    pub fn max_connections(&mut self, value: usize) -> &mut Self {
        self.max_connections = value;
        self
    }

    fn insecure(&self) -> bool {
        use TlsSecurity::Insecure;
        self.insecure_dev_mode || self.tls_security == Insecure
    }

    fn root_cert_store(&self) -> Result<rustls::RootCertStore, Error> {
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
        }
        Ok(roots)
    }

    fn trust_anchors(&self) -> Result<Vec<tls::OwnedTrustAnchor>, Error> {
        tls::OwnedTrustAnchor::read_all(
            self.pem.as_deref().unwrap_or("")
        ).map_err(ClientError::with_source_ref)
    }

    /// Create configuration object that can be used for connections
    pub fn build(&self) -> Result<Config, Error> {
        use TlsSecurity::*;

        if !self.initialized {
            return Err(ClientNoCredentialsError::with_message(
                "EdgeDB connection options are not initialized. \
                Run `edgedb project init` or use environment variables \
                to configure connection."));
        }
        let address = self.addr.clone();
        let verifier = match self.tls_security {
            _ if self.insecure() => Arc::new(tls::NullVerifier) as Verifier,
            Insecure => Arc::new(tls::NullVerifier) as Verifier,
            NoHostVerification => {
                Arc::new(tls::NoHostnameVerifier::new(
                        self.trust_anchors()?
                )) as Verifier
            }
            Strict => {
                Arc::new(rustls::client::WebPkiVerifier::new(
                    self.root_cert_store()?,
                    None,
                )) as Verifier
            }
            Default => match self.pem {
                Some(_) => {
                    Arc::new(tls::NoHostnameVerifier::new(
                            self.trust_anchors()?
                    )) as Verifier
                }
                None => {
                    Arc::new(rustls::client::WebPkiVerifier::new(
                        self.root_cert_store()?,
                        None,
                    )) as Verifier
                }
            },
        };

        Ok(Config(Arc::new(ConfigInner {
            address,
            admin: self.admin,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            verifier,
            instance_name: self.instance_name.clone(),
            wait: self.wait,
            connect_timeout: self.connect_timeout,
            insecure_dev_mode: self.insecure_dev_mode,

            // Pool configuration
            max_connections: self.max_connections,
        })))
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

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Config")
            .field("address", &self.0.address)
            .field("max_connections", &self.0.max_connections)
            // TODO(tailhook) more fields
            .finish()
    }
}
