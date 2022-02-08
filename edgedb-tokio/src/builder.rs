use std::env;
use std::error::Error as _;
use std::ffi::{OsString, OsStr};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;
use std::time::{Duration};

use rand::{thread_rng, Rng};
use rustls::client::ServerCertVerifier;
use typemap::{TypeMap, DebugAny};

use edgedb_protocol::server_message::ParameterStatus;
use edgedb_protocol::value::Value;

use crate::errors::{ClientConnectionError};
use crate::errors::{ClientError, ClientConnectionFailedTemporarilyError};
use crate::errors::{Error, ErrorKind, ResultExt};
use crate::errors::{ProtocolEncodingError, ClientNoCredentialsError};
use crate::server_params::{SystemConfig};
use crate::tls;

use crate::credentials::{Credentials, TlsSecurity};

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WAIT: Duration = Duration::from_secs(30);
pub const DEFAULT_POOL_SIZE: usize = 10;
pub const DEFAULT_HOST: &str = "localhost";
pub const DEFAULT_PORT: u16 = 5656;

type Verifier = Arc<dyn ServerCertVerifier>;


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

#[derive(Debug)]
pub(crate) enum Address {
    Tcp((String, u16)),
    Unix(PathBuf),
}

#[derive(Clone)]
pub struct Config(pub(crate) Arc<ConfigInner>);

pub(crate) struct ConfigInner {
    pub address: Address,
    pub admin: bool,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub verifier: Arc<dyn ServerCertVerifier>,
    pub instance_name: Option<String>,
    pub wait: Duration,
    pub connect_timeout: Duration,
    pub insecure_dev_mode: bool,

    // Pool configuration
    pub max_connections: usize,
}

struct DisplayAddr<'a>(&'a Builder);

impl fmt::Display for DisplayAddr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.0.initialized {
            write!(f, "<no address>")
        // TODO
        //} else if let Some(path) = self.0._get_unix_path().unwrap_or(None) {
        //    write!(f, "{}", path.display())
        } else {
            write!(f, "{}:{}", self.0.host, self.0.port)
        }
    }
}

fn sleep_duration() -> Duration {
    Duration::from_millis(thread_rng().gen_range(10u64..200u64))
}

fn is_temporary(e: &Error) -> bool {
    use io::ErrorKind::{ConnectionRefused, TimedOut, NotFound};
    use io::ErrorKind::{ConnectionAborted, ConnectionReset, UnexpectedEof};
    use io::ErrorKind::{AddrNotAvailable};

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
                | UnexpectedEof     // For Docker server which is starting up
                | AddrNotAvailable  // Docker exposed ports not yet bound
                => return true,
                _ => {},
            }
        }
    }
    return false;
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

    /// A displayable form for an address.
    pub fn display_addr<'x>(&'x self) -> impl fmt::Display + 'x {
        DisplayAddr(self)
    }
    /// Indicates whether credentials are set for this builder.
    pub fn is_initialized(&self) -> bool {
        self.initialized
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
        self.host = credentials.host.clone()
                .unwrap_or_else(|| DEFAULT_HOST.into());
        self.port = credentials.port;
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
        if let Some(url::Host::Ipv6(host)) = url.host() {
            // async-std uses raw IPv6 address without "[]"
            self.host = host.to_string();
        } else {
            self.host = url.host_str().unwrap_or(DEFAULT_HOST).to_owned();
        }
        self.port = url.port().unwrap_or(DEFAULT_PORT);
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
            host: DEFAULT_HOST.into(),
            port: DEFAULT_PORT,
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
            host: DEFAULT_HOST.into(),
            port: DEFAULT_PORT.into(),
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
        Ok(Credentials {
            host: Some(self.host.clone()),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: Some( self.database.clone()),
            tls_ca: self.pem.clone(),
            tls_security: self.tls_security,
            file_outdated: false
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

    fn do_verify_hostname(&self) -> bool {
        use TlsSecurity::*;
        if self.insecure_dev_mode {
            return false;
        }
        match self.tls_security {
            Insecure => false,
            NoHostVerification => false,
            Strict => true,
            Default => self.pem.is_none(),
        }
    }
    fn insecure(&self) -> bool {
        use TlsSecurity::Insecure;
        self.insecure_dev_mode || self.tls_security == Insecure
    }

    fn handle_system_config(
        &self,
        param_status: ParameterStatus,
        server_params: &mut TypeMap<dyn DebugAny + Send + Sync>
    ) -> Result<(), Error> {
        let (typedesc, data) = param_status.parse_system_config()
            .map_err(ProtocolEncodingError::with_source)?;
        let codec = typedesc.build_codec()
            .map_err(ProtocolEncodingError::with_source)?;
        let system_config = codec.decode(data.as_ref())
            .map_err(ProtocolEncodingError::with_source)?;
        let mut config = SystemConfig {
            session_idle_timeout: None,
        };
        if let Value::Object { shape, fields } = system_config {
            for (el, field) in shape.elements.iter().zip(fields) {
                match el.name.as_str() {
                    "id" => {},
                    "session_idle_timeout" => {
                        config.session_idle_timeout = match field {
                            Some(Value::Duration(timeout)) =>
                                Some(timeout.abs_duration()),
                            _ => {
                                log::warn!(
                                    "Wrong protocol: {}={:?}", el.name, field
                                );
                                None
                            },
                        };
                    }
                    name => {
                        log::debug!(
                            "Unhandled system config: {}={:?}", name, field
                        );
                    }
                }
            }
            server_params.insert::<SystemConfig>(config);
        } else {
            log::warn!("Received empty system config message.");
        }
        Ok(())
    }
    pub fn build(&self) -> Result<Config, Error> {
        use TlsSecurity::*;

        if !self.initialized {
            return Err(ClientNoCredentialsError::with_message(
                "EdgeDB connection options are not initialized. \
                Run `edgedb project init` or use environment variables \
                to configure connection."));
        }
        let address = Address::Tcp((self.host.clone(), self.port));
        let verifier = match self.tls_security {
            _ if self.insecure() => Arc::new(tls::NullVerifier) as Verifier,
            Insecure => Arc::new(tls::NullVerifier) as Verifier,
            NoHostVerification => Arc::new(tls::NoHostnameVerifier) as Verifier,
            Strict => Arc::new(rustls::client::WebPkiVerifier::new(
                    todo!(), None)) as Verifier,
            Default => match self.pem {
                Some(_) => Arc::new(tls::NoHostnameVerifier) as Verifier,
                None => Arc::new(rustls::client::WebPkiVerifier::new(
                    todo!(), None)) as Verifier,
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

fn validate_certs(data: &String) -> Result<(), Error> {
    let mut cert = 0;
    let open_data = rustls_pemfile::read_all(&mut io::Cursor::new(data))
            .map_err(|e| ClientError::with_source(e)
                .context("error reading PEM data"))?;
    for item in open_data {
        match item {
            rustls_pemfile::Item::X509Certificate(data) => {
                cert += 1;
                webpki::TrustAnchor::try_from_cert_der(&data)
                    .map_err(|e| ClientError::with_source(e)
                        .context("certificate data found, \
                                  but trust anchor is invalid"))?;
            }
            | rustls_pemfile::Item::RSAKey(_)
            | rustls_pemfile::Item::PKCS8Key(_)
            | rustls_pemfile::Item::ECKey(_)
            => {
                log::debug!("Skipping private key in cert data");
            }
            _ => {
                log::debug!("Skipping unknown item cert data");
            }
        }
    }
    if cert == 0 {
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
            .finish()
    }
}
