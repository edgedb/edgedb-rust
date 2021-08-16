use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::{self, FromStr};
use std::sync::Arc;
use std::time::{Instant, Duration};

use async_std::fs;
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
use crate::errors::{Anyhow};
use crate::errors::{ClientConnectionError, ProtocolError, ProtocolTlsError};
use crate::errors::{ClientConnectionFailedError, AuthenticationError};
use crate::errors::{ClientError, ClientConnectionFailedTemporarilyError};
use crate::errors::{Error, ErrorKind, ResultExt, PasswordRequired};
use crate::reader::ReadError;
use crate::server_params::PostgresAddress;
use crate::tls;

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WAIT: Duration = Duration::from_secs(30);


#[derive(Debug, Clone)]
pub struct Addr(AddrImpl);

#[derive(Debug, Clone)]
enum AddrImpl {
    Tcp(String, u16),
    Unix(PathBuf),
}

impl Addr {
    pub fn get_tcp_addr(&self) -> Option<(&String, &u16)> {
        if let Addr(AddrImpl::Tcp(host, port)) = self {
            Some((host, port))
        } else {
            None
        }
    }

    pub fn get_unix_addr(&self) -> Option<&PathBuf> {
        if let Addr(AddrImpl::Unix(path)) = self {
            Some(path)
        } else {
            None
        }
    }
}

/// A builder used to create connections
#[derive(Debug, Clone)]
pub struct Builder {
    addr: Addr,
    user: String,
    password: Option<String>,
    database: String,
    wait: Duration,
    connect_timeout: Duration,
    pem: Option<String>,
    cert: rustls::RootCertStore,
    verify_hostname: Option<bool>,
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

/// This maps error that is connection failure,
/// which means client failed to establish connection (not arbitrary IO error
/// between connection attempts).
fn io_fail(e: io::Error) -> Error {
    use io::ErrorKind::{ConnectionRefused, TimedOut, NotFound};
    use io::ErrorKind::{ConnectionAborted, ConnectionReset, UnexpectedEof};

    let temporary = match e.kind() {
        | ConnectionRefused
        | ConnectionReset
        | ConnectionAborted
        | NotFound  // For unix sockets
        | TimedOut
        | UnexpectedEof  // For Docker server which is starting up
        => true,
        _ => false,
    };
    if temporary {
        crate::errors::ClientConnectionFailedTemporarilyError::with_source(e)
    } else {
        crate::errors::ClientConnectionFailedError::with_source(e)
    }
}

/// This maps error that is connection failure,
/// which means client failed to establish connection (not arbitrary TLS error
/// between connection attempts).
fn tls_fail(e: tls_api::Error) -> Error {
    //// TODO HEREEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEe
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

/// This maps error that is connection failure,
/// which means client failed to establish connection
/// (not arbitrary ReadError error between connection attempts).
fn read_fail(e: ReadError) -> Error {
    use io::ErrorKind::{ConnectionRefused, TimedOut};
    use io::ErrorKind::{ConnectionAborted, ConnectionReset};

    let temporary = match &e {
        | ReadError::Eos => true,
        | ReadError::Io { source, .. } => {
            matches!(source.kind(),
                ConnectionRefused | ConnectionReset | ConnectionAborted |
                TimedOut
            )
        }
        _ => false,
    };
    if temporary {
        crate::errors::ClientConnectionFailedTemporarilyError::with_source(e)
    } else {
        crate::errors::ClientConnectionFailedError::with_source(e)
    }
}

impl Builder {
    pub fn from_credentials(credentials: &Credentials)
        -> Result<Builder, Error>
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
        Ok(Builder {
            addr: Addr(AddrImpl::Tcp(
                credentials.host.clone().unwrap_or_else(|| "localhost".into()),
                credentials.port)),
            user: credentials.user.clone(),
            password: credentials.password.clone(),
            database: credentials.database.clone()
                .unwrap_or_else(|| "edgedb".into()),
            wait: DEFAULT_WAIT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            verify_hostname: None,
            cert,
            pem,
        })
    }
    pub async fn read_credentials(path: impl AsRef<Path>)
        -> Result<Builder, Error>
    {
        let path = path.as_ref();
        let res: Result<Builder, Error> = async {
            let data = fs::read(path).await
                .map_err(ClientError::with_source)?;
            let creds = serde_json::from_slice(&data)
                .map_err(ClientError::with_source)?;
            Ok(Builder::from_credentials(&creds)?)
        }.await;
        Ok(res.with_context(|| {
            format!("cannot read credentials file {}", path.display())
        })?)
    }
    pub fn from_dsn(dsn: &str) -> Result<Builder, Error> {
        if !dsn.starts_with("edgedb://") {
            return Err(ClientError::with_message(format!(
                "String {:?} is not a valid DSN", dsn)));
        };
        let url = url::Url::parse(dsn)
            .map_err(|e| ClientError::with_source(e)
                .context(format!("cannot parse DSN {:?}", dsn)))?;
        Ok(Builder {
            addr: Addr(AddrImpl::Tcp(
                url.host_str().unwrap_or("127.0.0.1").to_owned(),
                url.port().unwrap_or(5656),
            )),
            user: if url.username().is_empty() {
                "edgedb".to_owned()
            } else {
                url.username().to_owned()
            },
            password: url.password().map(|s| s.to_owned()),
            database: url.path().strip_prefix("/")
                .unwrap_or("edgedb").to_owned(),
            wait: DEFAULT_WAIT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            cert: rustls::RootCertStore::empty(),
            pem: None,
            verify_hostname: None,
        })
    }
    pub fn new() -> Builder {
        Builder {
            addr: Addr(AddrImpl::Tcp("127.0.0.1".into(), 5656)),
            user: "edgedb".into(),
            password: None,
            database: "edgedb".into(),
            wait: DEFAULT_WAIT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            cert: rustls::RootCertStore::empty(),
            pem: None,
            verify_hostname: None,
        }
    }
    pub fn as_credentials(&self) -> Result<Credentials, Error> {
        let (host, port) = match &self.addr {
            Addr(AddrImpl::Tcp(host, port)) => (host, port),
            Addr(AddrImpl::Unix(_)) => {
                return Err(ClientError::with_message(
                    "Cannot generate credentials with UNIX socket."));
            }
        };
        Ok(Credentials {
            host: Some(host.into()),
            port: port.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            database: Some(self.database.clone()),
            tls_cert_data: self.pem.clone(),
            tls_verify_hostname: self.verify_hostname,
        })
    }
    pub fn get_addr(&self) -> &Addr {
        &self.addr
    }
    pub fn unix_addr(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.addr = Addr(AddrImpl::Unix(path.into()));
        self
    }
    pub fn tcp_addr(&mut self, addr: impl Into<String>, port: u16)
        -> &mut Self
    {
        self.addr = Addr(AddrImpl::Tcp(addr.into(), port));
        self
    }
    pub fn get_user(&self) -> &str {
        &self.user
    }
    pub fn user(&mut self, user: impl Into<String>) -> &mut Self {
        self.user = user.into();
        self
    }
    pub fn password(&mut self, password: impl Into<String>) -> &mut Self {
        self.password = Some(password.into());
        self
    }
    pub fn database(&mut self, database: impl Into<String>) -> &mut Self {
        self.database = database.into();
        self
    }
    pub fn get_database(&self) -> &str {
        &self.database
    }
    /// Time to wait for database server to become available
    ///
    /// This works by ignoring certain errors known to happen while database is
    /// starting up or restarting (e.g. "connecction refused" or early
    /// "connection reset")
    ///
    /// Note: the whole time that connection is being established can be up to
    /// `wait_until_available + connect_timeout`
    pub fn wait_until_available(&mut self, time: Duration) -> &mut Self {
        self.wait = time;
        self
    }
    /// A timeout for a single connect attempt
    ///
    /// Default is 10 seconds. Subsecond timeout should be fine for most
    /// networks, but since this timeout includes authentication, and currently
    /// that means:
    /// * Checking a password (slow by design)
    /// * Creating a compiler process (slow now, may be optimized later)
    ///
    /// So in concurrent case on slower VM (such as CI with parallel tests)
    /// 10 seconds is more reasonable default.
    ///
    /// The `wait_until_available` should be larger than this value to allow
    /// multiple attempts. And also the whole time that connection is being
    /// established can be up to `wait_until_available + connect_timeout`
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set allowed certificate as pem file
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

    /// Instructs TLS code to enable or disable verification
    ///
    /// By default verification is disable if specific certificate are
    /// configured and enabled if root certificates are used.
    pub fn verify_hostname(&mut self, value: bool) -> &mut Self {
        self.verify_hostname = Some(value);
        self
    }

    pub async fn connect_with_cert_verifier(
        &self, cert_verifier: Arc<dyn ServerCertVerifier>
    ) -> Result<Connection, Error> {
        let tls = tls::connector(&self.cert, cert_verifier).map_err(tls_fail)?;

        match &self.addr {
            Addr(AddrImpl::Tcp(host, port)) => {
                log::info!("Connecting via TCP {}:{}", host, port);
            }
            Addr(AddrImpl::Unix(path)) => {
                log::info!("Connecting via Unix `{}`", path.display());
            }
        };

        let start = Instant::now();
        let ref mut warned = false;
        let conn = loop {
            match
                timeout(self.connect_timeout, self._connect(&tls, warned)).await
            {
                Err(e) if e.is::<ClientConnectionFailedTemporarilyError>() => {
                    log::debug!("Temporary connection error: {:#}", e);
                    if self.wait > start.elapsed() {
                        sleep(sleep_duration()).await;
                        continue;
                    } else if self.wait > Duration::new(0, 0) {
                        return Err(ClientConnectionError::with_source(e)
                            .context(format!("cannot establish \
                                              connection for {:?}",
                                              self.wait)));
                    } else {
                        return Err(ClientConnectionError::with_source(e));
                    }
                }
                Err(e) => {
                    log::debug!("Connection error: {:#}", e);
                    return Err(ClientConnectionError::with_source(e))?;
                }
                Ok(conn) => break conn,
            }
        };
        Ok(conn)
    }
    fn do_verify_hostname(&self) -> bool {
        self.verify_hostname.unwrap_or(self.cert.is_empty())
    }
    pub async fn connect(&self) -> Result<Connection, Error> {
        self.connect_with_cert_verifier(Arc::new(tls::CertVerifier::new(
            self.do_verify_hostname()
        ))).await
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
                _ => match self.addr.get_tcp_addr() {
                    Some(_) => Err(ClientConnectionFailedError::with_message(
                        "Server does not support the EdgeDB binary protocol."
                    ))?,
                    None => r,  // don't check ALPN on UNIX stream
                }
            }
        };
        self._connect_with(stream).await
    }

    async fn _connect_stream(&self, tls: &TlsConnectorBox)
        -> Result<TlsStream, Error>
    {
        match &self.addr {
            Addr(AddrImpl::Tcp(host, port)) => {
                let conn = TcpStream::connect(&(&host[..], *port)).await
                    .map_err(io_fail)?;
                let host = if IpAddr::from_str(&host).is_ok() {
                    if self.do_verify_hostname() {
                        return Err(ClientError::with_message(
                            "Cannot use `verify_hostname` or system \
                            root certificates with an IP address"));
                    }
                    Cow::from(format!("{}.host-for-ip.edgedb.net", host)
                        .replace(":", "-"))  // for ipv6addr
                } else {
                    Cow::from(host)
                };
                Ok(tls.connect(&host[..], conn).await.map_err(tls_fail)?)
            }
            Addr(AddrImpl::Unix(path)) => {
                #[cfg(windows)] {
                    return Err(ClientError::with_message(
                        "Unix socket are not supported on windows",
                    ));
                }
                #[cfg(unix)] {
                    use async_std::os::unix::net::UnixStream;
                    let conn = UnixStream::connect(&path).await
                        .map_err(io_fail)?;
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
        let mut seq = conn.start_sequence().await.err_kind::<ClientError>()?;
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
        ]).await.err_kind::<ClientError>()?;

        let mut msg = seq.message().await.map_err(read_fail)?;
        if let ServerMessage::ServerHandshake(ServerHandshake {
            major_ver, minor_ver, extensions: _
        }) = msg {
            version = ProtocolVersion::new(major_ver, minor_ver);
            // TODO(tailhook) record extensions
            msg = seq.message().await.map_err(read_fail)?;
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
                return Err(AuthenticationError::with_message(format!(
                    "Error authenticating: {}", err)));
            }
            msg => {
                return Err(ProtocolError::with_message(format!(
                    "Error authenticating, unexpected message {:?}", msg)));
            }
        }

        let mut server_params = TypeMap::custom();
        loop {
            let msg = seq.message().await.map_err(read_fail)?;
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

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Addr(AddrImpl::Tcp(host, port)) => write!(f, "{}:{}", host, port),
            Addr(AddrImpl::Unix(path)) => write!(f, "{}", path.display()),
        }
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
    ]).await.err_kind::<ClientError>()?;
    let msg = seq.message().await.map_err(read_fail)?;
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
    ]).await.err_kind::<ClientError>()?;
    let msg = seq.message().await.map_err(read_fail)?;
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
        let msg = seq.message().await.map_err(read_fail)?;
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
    let bld = async_std::task::block_on(
        Builder::read_credentials("tests/credentials1.json")).unwrap();
    assert!(matches!(bld.addr, Addr(AddrImpl::Tcp(_, x)) if x == 10702));
    assert_eq!(&bld.user, "test3n");
    assert_eq!(&bld.database, "test3n");
    assert_eq!(bld.password, Some("lZTBy1RVCfOpBAOwSCwIyBIR".into()));
}

#[test]
fn display() {
    let mut bld = Builder::from_dsn("edgedb://localhost:1756").unwrap();
    assert_eq!(bld.get_addr().to_string(), "localhost:1756");
    bld.unix_addr("/test/my.sock");
    assert_eq!(bld.get_addr().to_string(), "/test/my.sock");
}

#[test]
fn get_addr() {
    let mut bld = Builder::from_dsn("edgedb://localhost:1756").unwrap();
    let (host, port) = bld.get_addr().get_tcp_addr().unwrap();
    assert_eq!(*host, "localhost");
    assert_eq!(*port, 1756);
    assert!(bld.get_addr().get_unix_addr().is_none());

    bld.unix_addr("/test/my.sock");
    let unix = bld.get_addr().get_unix_addr().unwrap().display().to_string();
    assert_eq!(unix, "/test/my.sock");
    assert!(bld.get_addr().get_tcp_addr().is_none());
}

#[test]
fn from_dsn() {
    let bld = Builder::from_dsn(
        "edgedb://user1:EiPhohl7@edb-0134.elb.us-east-2.amazonaws.com/db2").unwrap();
    assert!(matches!(bld.addr, Addr(AddrImpl::Tcp(h, p)) if
        h == "edb-0134.elb.us-east-2.amazonaws.com" &&
        p == 5656));
    assert_eq!(&bld.user, "user1");
    assert_eq!(&bld.database, "db2");
    assert_eq!(bld.password, Some("EiPhohl7".into()));

    let bld = Builder::from_dsn(
        "edgedb://user2@edb-0134.elb.us-east-2.amazonaws.com:1756/db2").unwrap();
    assert!(matches!(bld.addr, Addr(AddrImpl::Tcp(h, p)) if
        h == "edb-0134.elb.us-east-2.amazonaws.com" &&
        p == 1756));
    assert_eq!(&bld.user, "user2");
    assert_eq!(&bld.database, "db2");
    assert_eq!(bld.password, None);

    let bld = Builder::from_dsn(
        "edgedb://edb-0134.elb.us-east-2.amazonaws.com:1756").unwrap();
    assert!(matches!(bld.addr, Addr(AddrImpl::Tcp(h, p)) if
        h == "edb-0134.elb.us-east-2.amazonaws.com" &&
        p == 1756));
    assert_eq!(&bld.user, "edgedb");
    assert_eq!(&bld.database, "edgedb");
    assert_eq!(bld.password, None);
}
