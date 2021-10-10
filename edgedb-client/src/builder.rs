use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::error::Error as _;
use std::ffi::{OsString, OsStr};
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::{self, FromStr};
use std::sync::Arc;
use std::time::{Instant, Duration};

use async_std::fs;
use async_std::path::{Path as AsyncPath};
use async_std::future::Future;
use async_std::net::TcpStream;
use async_std::task::sleep;
use bytes::{Bytes, BytesMut};
use futures_util::AsyncReadExt;
use rand::{thread_rng, Rng};
use rustls::ServerCertVerifier;
use scram::ScramClient;
use serde_json::from_slice;
use typemap::TypeMap;
use tls_api::{TlsConnectorBox, TlsConnector as _, TlsConnectorBuilder as _};
use tls_api::{TlsStream, TlsStreamDyn as _};
use tls_api_not_tls::TlsConnector as PlainConnector;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::server_message::{TransactionState, ServerHandshake};

use crate::client::{Connection, Sequence};
use crate::credentials::Credentials;
use crate::errors::{ClientConnectionError, ProtocolError, ProtocolTlsError};
use crate::errors::{ClientConnectionFailedError, AuthenticationError};
use crate::errors::{ClientError, ClientConnectionFailedTemporarilyError};
use crate::errors::{ClientNoCredentialsError};
use crate::errors::{Error, ErrorKind, PasswordRequired};
use crate::server_params::PostgresAddress;
use crate::tls;

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WAIT: Duration = Duration::from_secs(30);
pub const DEFAULT_POOL_SIZE: usize = 10;
pub const DEFAULT_HOST: &str = "localhost";
pub const DEFAULT_PORT: u16 = 5656;


/// A builder used to create connections.
#[derive(Debug, Clone)]
pub struct Builder {
    host: String,
    port: u16,
    admin: bool,
    user: String,
    password: Option<String>,
    database: String,
    pem: Option<String>,
    cert: rustls::RootCertStore,
    verify_hostname: Option<bool>,

    initialized: bool,
    wait: Duration,
    connect_timeout: Duration,
    insecure_dev_mode: bool,

    // Pool configuration
    pub(crate) max_connections: usize,
}

struct DisplayAddr<'a>(&'a Builder);

impl fmt::Display for DisplayAddr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.0.initialized {
            write!(f, "<no address>")
        } else if let Some(path) = self.0._get_unix_path() {
            write!(f, "{}", path.display())
        } else {
            write!(f, "{}:{}", self.0.host, self.0.port)
        }
    }
}

pub async fn timeout<F, T>(dur: Duration, f: F) -> Result<T, Error>
    where F: Future<Output = Result<T, Error>>,
{
    use async_std::future::timeout;

    timeout(dur, f).await
    .unwrap_or_else(|_| {
        Err(ClientConnectionFailedTemporarilyError::with_source(
            io::Error::from(io::ErrorKind::TimedOut)
        ))
    })
}

fn sleep_duration() -> Duration {
    Duration::from_millis(thread_rng().gen_range(10u64..200u64))
}

fn is_temporary(e: &Error) -> bool {
    use io::ErrorKind::{ConnectionRefused, TimedOut, NotFound};
    use io::ErrorKind::{ConnectionAborted, ConnectionReset, UnexpectedEof};

    if e.is::<ClientConnectionFailedTemporarilyError>() {
        return true;
    }
    if e.is::<ClientConnectionError>() {
        let io_err = e.source().and_then(|src| {
            src.downcast_ref::<io::Error>()
            .or_else(|| src.downcast_ref::<Box<io::Error>>().map(|b| &**b))
        });
        if let Some(e) = io_err {
            match e.kind() {
                | ConnectionRefused
                | ConnectionReset
                | ConnectionAborted
                | NotFound  // For unix sockets
                | TimedOut
                | UnexpectedEof  // For Docker server which is starting up
                => return true,
                _ => {},
            }
        }
    }
    return false;
}

fn tls_fail(e: tls_api::Error) -> Error {
    match e.into_inner().downcast::<io::Error>() {
        Ok(e) => {
            if let Some(e) = e.get_ref() {
                if let Some(e) = e.downcast_ref::<rustls::TLSError>() {
                    if matches!(e, rustls::TLSError::CorruptMessage) {
                        return ProtocolTlsError::with_message(
                            "corrupt message, possibly server \
                             does not support TLS connection."
                        );
                    }
                }
            }
            // We ignore that this error is inside TLS code,
            // when rethrowing here. But it should be fine for IO errors,
            // as underlying reason of IO failure is more important than
            // which operation is was running at that time.
            ClientConnectionError::with_source(e)
        }
        Err(e) => ClientConnectionError::with_source_box(e),
    }
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

pub async fn search_dir(base: &AsyncPath) -> Result<Option<&AsyncPath>, Error>
{
    let mut path = base;
    if path.join("edgedb.toml").exists().await {
        return Ok(Some(path.into()));
    }
    while let Some(parent) = path.parent() {
        if parent.join("edgedb.toml").exists().await {
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
    sha1::Sha1::from(path_bytes(path)).hexdigest()
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
    pub async fn read_project(&mut self,
        override_dir: Option<&Path>, search_parents: bool)
        -> Result<&mut Self, Error>
    {
        let dir = match override_dir {
            Some(v) => Cow::Borrowed(v.as_ref()),
            None => {
                Cow::Owned(env::current_dir()
                    .map_err(|e| ClientError::with_source(e)
                        .context("failed to get current directory"))?
                    .into())
            }
        };

        let dir = if search_parents {
            if let Some(ancestor) = search_dir(&dir).await? {
                Cow::Borrowed(ancestor)
            } else {
                return Ok(self);
            }
        } else {
            if !dir.join("edgedb.toml").exists().await {
                return Ok(self);
            }
            dir
        };
        let canon = fs::canonicalize(&dir).await
            .map_err(|e| ClientError::with_source(e).context(
                format!("failed to canonicalize dir {:?}", dir)
            ))?;
        let stash_path = stash_path(canon.as_ref())?;
        if AsRef::<AsyncPath>::as_ref(&stash_path).exists().await {
            let instance =
                fs::read_to_string(stash_path.join("instance-name")).await
                .map_err(|e| ClientError::with_source(e).context(
                    format!("error reading project settings {:?}", dir)
                ))?;
            self.read_instance(instance.trim()).await?;

        }
        Ok(self)
    }
    /// A displayable form for an address.
    pub fn display_addr<'x>(&'x self) -> impl fmt::Display + 'x {
        DisplayAddr(self)
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
    ///
    /// Then the value of that environment variable will be used to set just
    /// the parameter matching that environment variable.
    ///
    /// The `insecure_dev_mode` and connection parameters are never modified by
    /// this function for now.
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
        self.read_extra_env_vars()?;
        Ok(self)
    }
    /// Read environment variables that aren't credentials
    pub fn read_extra_env_vars(&mut self) -> Result<&mut Self, Error> {
        if let Some(mode) = get_env("EDGEDB_INSECURE_DEV_MODE")? {
            self.insecure_dev_mode = mode != "";
        }
        Ok(self)
    }

    /// Set all credentials.
    ///
    /// This marks the builder as initialized.
    pub fn credentials(&mut self, credentials: &Credentials)
        -> Result<&mut Self, Error>
    {
        let mut cert = rustls::RootCertStore::empty();
        let pem;
        if let Some(cert_data) = &credentials.tls_cert_data {
            pem = Some(cert_data.clone());
            match
                cert.add_pem_file(&mut io::Cursor::new(cert_data.as_bytes()))
            {
                Ok((0, 0)) => {
                    return Err(ClientError::with_message(
                        "Empty certificate data"));
                }
                Ok((_, 0)) => {}
                Ok((_, _)) | Err(()) => {
                    return Err(ClientError::with_message(
                        "Invalid certificates are contained in `tls_certdata`"
                    ));
                }
            }
        } else {
            pem = None;
        }
        self.reset_compound();
        *self = Builder {
            // replace all of them
            host: credentials.host.clone()
                .unwrap_or_else(|| DEFAULT_HOST.into()),
            port: credentials.port,
            admin: false,
            user: credentials.user.clone(),
            password: credentials.password.clone(),
            database: credentials.database.clone()
                .unwrap_or_else(|| "edgedb".into()),
            verify_hostname: credentials.tls_verify_hostname,
            cert,
            pem,

            initialized: true,
            ..*self
        };
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
        Ok(self)
    }

    /// Read credentials from a file.
    ///
    /// This will mark the builder as initialized (if reading is successful)
    /// and overwrite all credentials. However, `insecure_dev_mode`, pools
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
        *self = Builder {
            // replace all of them
            host: url.host_str().unwrap_or(DEFAULT_HOST).to_owned(),
            port: url.port().unwrap_or(DEFAULT_PORT),
            admin: admin,
            user: if url.username().is_empty() {
                "edgedb".to_owned()
            } else {
                url.username().to_owned()
            },
            password: url.password().map(|s| s.to_owned()),
            database: url.path().strip_prefix("/")
                .unwrap_or("edgedb").to_owned(),
            verify_hostname: None,
            cert: rustls::RootCertStore::empty(),
            pem: None,

            initialized: true,
            ..*self
        };
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
            host: DEFAULT_HOST.into(),
            port: DEFAULT_PORT,
            admin: false,
            user: "edgedb".into(),
            password: None,
            database: "edgedb".into(),
            verify_hostname: None,
            cert: rustls::RootCertStore::empty(),
            pem: None,

            wait: DEFAULT_WAIT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            initialized: false,
            insecure_dev_mode: false,

            max_connections: DEFAULT_POOL_SIZE,
        }
    }
    fn reset_compound(&mut self) {
        *self = Builder {
            // replace all of them
            host: DEFAULT_HOST.into(),
            port: DEFAULT_PORT.into(),
            admin: false,
            user: "edgedb".into(),
            password: None,
            database: "edgedb".into(),
            verify_hostname: None,
            cert: rustls::RootCertStore::empty(),
            pem: None,

            initialized: false,
            // keep old values
            wait: self.wait,
            connect_timeout: self.connect_timeout,
            insecure_dev_mode: self.insecure_dev_mode,

            max_connections: self.max_connections,
        };
    }
    /// Extract credentials from the [Builder] so they can be saved as JSON.
    pub fn as_credentials(&self) -> Result<Credentials, Error> {
        Ok(Credentials {
            host: Some(self.host.clone()),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: Some(self.database.clone()),
            tls_cert_data: self.pem.clone(),
            tls_verify_hostname: self.verify_hostname,
        })
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
        &self.host
    }
    /// Get the `port` this builder is configured to connect to.
    pub fn get_port(&self) -> u16 {
        self.port
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
        self.host = host.map_or_else(|| DEFAULT_HOST.into(), |h| h.into());
        self.port = port.unwrap_or(DEFAULT_PORT);
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
        self.cert.roots.clear();
        self.pem = None;
        self.cert.add_pem_file(&mut io::Cursor::new(cert_data.as_bytes()))
            .map_err(|()| ClientError::with_message(
                "error reading certificate"))?;
        self.pem = Some(cert_data.clone());
        Ok(self)
    }

    /// Instructs the TLS code to enable or disable verification.
    ///
    /// By default, verification is disabled if a configured to use only a
    /// specific certificate, and enabled if root certificates are used.
    pub fn verify_hostname(&mut self, value: bool) -> &mut Self {
        self.verify_hostname = Some(value);
        self
    }

    /// Enables insecure dev mode.
    ///
    /// This disables certificate validation entirely.
    pub fn insecure_dev_mode(&mut self, value: bool) -> &mut Self {
        self.insecure_dev_mode = value;
        self
    }

    /// Connect with a custom certificate verifier.
    ///
    /// Unstable API
    #[cfg(feature="unstable")]
    pub async fn connect_with_cert_verifier(
        &self, cert_verifier: Arc<dyn ServerCertVerifier>
    ) -> Result<Connection, Error> {
        self._connect_with_cert_verifier(cert_verifier).await
    }

    async fn _connect_with_cert_verifier(
        &self, cert_verifier: Arc<dyn ServerCertVerifier>
    ) -> Result<Connection, Error> {
        if !self.initialized {
            return Err(ClientNoCredentialsError::with_message(
                "EdgeDB connection options are not initialized. \
                Run `edgedb project init` or use environment variables \
                to configure connection."));
        }
        self.connect_inner(cert_verifier).await.map_err(|e| {
            if e.is::<ClientConnectionError>() {
                e.refine_kind::<ClientConnectionFailedError>()
            } else {
                e
            }
        })
    }

    /// Get the path of the Unix socket if that is configured to be used.
    ///
    /// This is a deprecated API and should only be used by the command-line
    /// tool.
    #[cfg(feature="admin_socket")]
    pub fn get_unix_path(&self) -> Option<PathBuf> {
        self._get_unix_path()
    }
    fn _get_unix_path(&self) -> Option<PathBuf> {
        let unix_host = self.host.contains("/");
        if self.admin || unix_host {
            let prefix = if unix_host {
                &self.host
            } else {
                "/var/run/edgedb"
            };
            let path = if prefix.contains(".s.EDGEDB") {
                // it's the full path
                self.host.clone()
            } else {
                if self.admin {
                    format!("{}/.s.EDGEDB.admin.{}", prefix, self.port)
                } else {
                    format!("{}/.s.EDGEDB.{}", prefix, self.port)
                }
            };
            Some(path.into())
        } else {
            None
        }
    }

    async fn connect_inner(
        &self, cert_verifier: Arc<dyn ServerCertVerifier>
    ) -> Result<Connection, Error> {
        let tls = tls::connector(&self.cert, cert_verifier).map_err(tls_fail)?;
        if log::log_enabled!(log::Level::Info) {
            if let Some(path) = self._get_unix_path() {
                log::info!("Connecting via Unix `{}`", path.display());
            } else {
                log::info!("Connecting via TCP {}:{}", self.host, self.port);
            }
        }

        let start = Instant::now();
        let ref mut warned = false;
        let conn = loop {
            match
                timeout(self.connect_timeout, self._connect(&tls, warned)).await
            {
                Err(e) if is_temporary(&e) => {
                    log::debug!("Temporary connection error: {:#}", e);
                    if self.wait > start.elapsed() {
                        sleep(sleep_duration()).await;
                        continue;
                    } else if self.wait > Duration::new(0, 0) {
                        return Err(e.context(
                            format!("cannot establish connection for {:?}",
                                    self.wait)));
                    } else {
                        return Err(e);
                    }
                }
                Err(e) => {
                    log::debug!("Connection error: {:#}", e);
                    return Err(e)?;
                }
                Ok(conn) => break conn,
            }
        };
        Ok(conn)
    }

    /// Set the maximum number of underlying database connections.
    pub fn max_connections(&mut self, value: usize) -> &mut Self {
        self.max_connections = value;
        self
    }

    fn do_verify_hostname(&self) -> bool {
        self.verify_hostname.unwrap_or(self.cert.is_empty())
    }
    /// Return a single connection.
    #[cfg(feature="unstable")]
    pub async fn connect(&self) -> Result<Connection, Error> {
        self.private_connect().await
    }
    pub(crate) async fn private_connect(&self) -> Result<Connection, Error> {
        if self.insecure_dev_mode {
            self._connect_with_cert_verifier(Arc::new(tls::NullVerifier)).await
        } else {
            let verify_host = self.do_verify_hostname();
            if verify_host && IpAddr::from_str(&self.host).is_ok() {
                // FIXME: https://github.com/rustls/rustls/issues/184
                // When rustls issue ix fixed we may lift this limitation
                return Err(ClientError::with_message(
                    "Cannot use `verify_hostname` or system \
                    root certificates with an IP address"));
            }
            self._connect_with_cert_verifier(
                Arc::new(tls::CertVerifier::new(verify_host))
            ).await
        }
    }
    async fn _connect(&self, tls: &TlsConnectorBox, warned: &mut bool)
        -> Result<Connection, Error>
    {
        let stream = match self._connect_stream(tls).await {
            Err(e) if e.is::<ProtocolTlsError>() => {
                if !*warned {
                    log::warn!("TLS connection failed. \
                        Trying plaintext...");
                    *warned = true;
                }
                self._connect_stream(
                    &PlainConnector::builder()
                        .map_err(ClientError::with_source)?
                        .build().map_err(ClientError::with_source)?
                        .into_dyn()
                ).await?
            }
            Err(e) => return Err(e),
            Ok(r) => match r.get_alpn_protocol() {
                Ok(Some(protocol)) if protocol == b"edgedb-binary" => r,
                _ => match self._get_unix_path() {
                    None => Err(ClientConnectionFailedError::with_message(
                        "Server does not support the EdgeDB binary protocol."
                    ))?,
                    Some(_) => r,  // don't check ALPN on UNIX stream
                }
            }
        };
        self._connect_with(stream).await
    }

    async fn _connect_stream(&self, tls: &TlsConnectorBox)
        -> Result<TlsStream, Error>
    {
        match self._get_unix_path() {
            None => {
                let conn = TcpStream::connect(
                    &(&self.host[..], self.port)
                ).await.map_err(ClientConnectionError::with_source)?;
                let host = if IpAddr::from_str(&self.host).is_ok() {
                    // FIXME: https://github.com/rustls/rustls/issues/184
                    Cow::from(format!("{}.host-for-ip.edgedb.net", self.host)
                        .replace(":", "-"))  // for ipv6addr
                } else {
                    Cow::from(&self.host[..])
                };
                Ok(tls.connect(&host[..], conn).await.map_err(tls_fail)?)
            }
            Some(path) => {
                #[cfg(windows)] {
                    return Err(ClientError::with_message(
                        "Unix socket are not supported on windows",
                    ));
                }
                #[cfg(unix)] {
                    use async_std::os::unix::net::UnixStream;
                    let conn = UnixStream::connect(&path).await
                        .map_err(ClientConnectionError::with_source)?;
                    Ok(
                        PlainConnector::builder()
                            .map_err(ClientError::with_source)?
                            .build().map_err(ClientError::with_source)?
                            .into_dyn()
                        .connect("localhost", conn).await.map_err(tls_fail)?
                    )
                }
            }
        }
    }

    async fn _connect_with(&self, stream: TlsStream)
        -> Result<Connection, Error>
    {
        let mut version = ProtocolVersion::current();
        let (input, output) = stream.split();
        let mut conn = Connection {
            input,
            output,
            input_buf: BytesMut::with_capacity(8192),
            output_buf: BytesMut::with_capacity(8192),
            params: TypeMap::custom(),
            transaction_state: TransactionState::NotInTransaction,
            dirty: false,
            version: version.clone(),
        };
        let mut seq = conn.start_sequence().await?;
        let mut params = HashMap::new();
        params.insert(String::from("user"), self.user.clone());
        params.insert(String::from("database"), self.database.clone());

        let (major_ver, minor_ver) = version.version_tuple();
        seq.send_messages(&[
            ClientMessage::ClientHandshake(ClientHandshake {
                major_ver,
                minor_ver,
                params,
                extensions: HashMap::new(),
            }),
        ]).await?;

        let mut msg = seq.message().await?;
        if let ServerMessage::ServerHandshake(ServerHandshake {
            major_ver, minor_ver, extensions: _
        }) = msg {
            version = ProtocolVersion::new(major_ver, minor_ver);
            // TODO(tailhook) record extensions
            msg = seq.message().await?;
        }
        match msg {
            ServerMessage::Authentication(Authentication::Ok) => {}
            ServerMessage::Authentication(Authentication::Sasl { methods })
            => {
                if methods.iter().any(|x| x == "SCRAM-SHA-256") {
                    if let Some(password) = &self.password {
                        scram(&mut seq, &self.user, password).await
                            .map_err(ClientError::with_source)?;
                    } else {
                        return Err(PasswordRequired::with_message(
                            "Password required for the specified user/host"));
                    }
                } else {
                    return Err(AuthenticationError::with_message(format!(
                        "No supported authentication \
                        methods: {:?}", methods)));
                }
            }
            ServerMessage::ErrorResponse(err) => {
                return Err(err.into());
            }
            msg => {
                return Err(ProtocolError::with_message(format!(
                    "Error authenticating, unexpected message {:?}", msg)));
            }
        }

        let mut server_params = TypeMap::custom();
        loop {
            let msg = seq.message().await?;
            match msg {
                ServerMessage::ReadyForCommand(ready) => {
                    seq.reader.consume_ready(ready);
                    seq.end_clean();
                    break;
                }
                ServerMessage::ServerKeyData(_) => {
                    // TODO(tailhook) store it somehow?
                }
                ServerMessage::ParameterStatus(par) => {
                    match &par.name[..] {
                        b"pgaddr" => {
                            let pgaddr: PostgresAddress;
                            pgaddr = match from_slice(&par.value[..]) {
                                Ok(a) => a,
                                Err(e) => {
                                    log::warn!("Can't decode param {:?}: {}",
                                        par.name, e);
                                    continue;
                                }
                            };
                            server_params.insert::<PostgresAddress>(pgaddr);
                        }
                        _ => {}
                    }
                }
                _ => {
                    log::warn!("unsolicited message {:?}", msg);
                }
            }
        }
        conn.version = version;
        conn.params = server_params;
        Ok(conn)
    }
}

async fn scram(seq: &mut Sequence<'_>, user: &str, password: &str)
    -> Result<(), Error>
{
    use edgedb_protocol::client_message::SaslInitialResponse;
    use edgedb_protocol::client_message::SaslResponse;

    let scram = ScramClient::new(&user, &password, None);

    let (scram, first) = scram.client_first();
    seq.send_messages(&[
        ClientMessage::AuthenticationSaslInitialResponse(
            SaslInitialResponse {
            method: "SCRAM-SHA-256".into(),
            data: Bytes::copy_from_slice(first.as_bytes()),
        }),
    ]).await?;
    let msg = seq.message().await?;
    let data = match msg {
        ServerMessage::Authentication(
            Authentication::SaslContinue { data }
        ) => data,
        ServerMessage::ErrorResponse(err) => {
            return Err(err.into());
        }
        msg => {
            return Err(ProtocolError::with_message(format!(
                "Bad auth response: {:?}", msg)));
        }
    };
    let data = str::from_utf8(&data[..])
        .map_err(|e| ProtocolError::with_source(e).context(
            "invalid utf-8 in SCRAM-SHA-256 auth"))?;
    let scram = scram.handle_server_first(&data)
        .map_err(AuthenticationError::with_source)?;
    let (scram, data) = scram.client_final();
    seq.send_messages(&[
        ClientMessage::AuthenticationSaslResponse(
            SaslResponse {
                data: Bytes::copy_from_slice(data.as_bytes()),
            }),
    ]).await?;
    let msg = seq.message().await?;
    let data = match msg {
        ServerMessage::Authentication(Authentication::SaslFinal { data })
        => data,
        ServerMessage::ErrorResponse(err) => {
            return Err(err.into());
        }
        msg => {
            return Err(ProtocolError::with_message(format!(
                "auth response: {:?}", msg)));
        }
    };
    let data = str::from_utf8(&data[..])
        .map_err(|_| ProtocolError::with_message(
            "invalid utf-8 in SCRAM-SHA-256 auth"))?;
    scram.handle_server_final(&data)
        .map_err(|e| AuthenticationError::with_message(format!(
            "Authentication error: {}", e)))?;
    loop {
        let msg = seq.message().await?;
        match msg {
            ServerMessage::Authentication(Authentication::Ok) => break,
            msg => {
                log::warn!("unsolicited message {:?}", msg);
            }
        };
    }
    Ok(())
}

#[test]
fn read_credentials() {
    let mut bld = Builder::uninitialized();
    async_std::task::block_on(
        bld.read_credentials("tests/credentials1.json")).unwrap();
    assert!(matches!(bld.port, 10702));
    assert_eq!(&bld.user, "test3n");
    assert_eq!(&bld.database, "test3n");
    assert_eq!(bld.password, Some("lZTBy1RVCfOpBAOwSCwIyBIR".into()));
}

#[test]
fn display() {
    let mut bld = Builder::uninitialized();
    async_std::task::block_on(
        bld.read_dsn("edgedb://localhost:1756")).unwrap();
    assert_eq!(bld.host, "localhost");
    assert_eq!(bld.port, 1756);
    bld.host_port(Some("/test/my.sock"), None);
    assert_eq!(bld._get_unix_path(),
               Some("/test/my.sock/.s.EDGEDB.5656".into()));
    bld.host_port(Some("/test/.s.EDGEDB.8888"), None);
    assert_eq!(bld._get_unix_path(), Some("/test/.s.EDGEDB.8888".into()));
}

#[test]
fn from_dsn() {
    let mut bld = Builder::uninitialized();
    async_std::task::block_on(bld.read_dsn(
        "edgedb://user1:EiPhohl7@edb-0134.elb.us-east-2.amazonaws.com/db2"
    )).unwrap();
    assert_eq!(bld.host, "edb-0134.elb.us-east-2.amazonaws.com");
    assert_eq!(bld.port, DEFAULT_PORT);
    assert_eq!(&bld.user, "user1");
    assert_eq!(&bld.database, "db2");
    assert_eq!(bld.password, Some("EiPhohl7".into()));

    let mut bld = Builder::uninitialized();
    async_std::task::block_on(bld.read_dsn(
        "edgedb://user2@edb-0134.elb.us-east-2.amazonaws.com:1756/db2"
    )).unwrap();
    assert_eq!(bld.host, "edb-0134.elb.us-east-2.amazonaws.com");
    assert_eq!(bld.port, 1756);
    assert_eq!(&bld.user, "user2");
    assert_eq!(&bld.database, "db2");
    assert_eq!(bld.password, None);

    // Tests overriding
    async_std::task::block_on(bld.read_dsn(
        "edgedb://edb-0134.elb.us-east-2.amazonaws.com:1756"
    )).unwrap();
    assert_eq!(bld.host, "edb-0134.elb.us-east-2.amazonaws.com");
    assert_eq!(bld.port, 1756);
    assert_eq!(&bld.user, "edgedb");
    assert_eq!(&bld.database, "edgedb");
    assert_eq!(bld.password, None);
}
