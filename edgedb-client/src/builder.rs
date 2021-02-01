use std::collections::HashMap;
use std::io;
use std::str;
use std::fmt;
use std::time::{Instant, Duration};
use std::path::{Path, PathBuf};

use anyhow::{self, Context};
use async_std::fs;
use async_std::future::Future;
use async_std::net::TcpStream;
use async_std::task::sleep;
use async_listen::ByteStream;
use bytes::{Bytes, BytesMut};
use rand::{thread_rng, Rng};
use scram::ScramClient;
use serde_json::from_slice;
use typemap::TypeMap;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::server_message::{TransactionState, ServerHandshake};

use crate::client::{Connection, Sequence};
use crate::credentials::Credentials;
use crate::errors::PasswordRequired;
use crate::features::ProtocolVersion;
use crate::reader::ReadError;
use crate::server_params::PostgresAddress;

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WAIT: Duration = Duration::from_secs(30);


#[derive(Debug, Clone)]
pub struct Addr(AddrImpl);

#[derive(Debug, Clone)]
enum AddrImpl {
    Tcp(String, u16),
    Unix(PathBuf),
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
}

pub async fn timeout<F, T>(dur: Duration, f: F) -> anyhow::Result<T>
    where F: Future<Output = anyhow::Result<T>>,
{
    use async_std::future::timeout;

    timeout(dur, f).await
    .unwrap_or_else(|_| Err(io::Error::from(io::ErrorKind::TimedOut).into()))
}

fn sleep_duration() -> Duration {
    Duration::from_millis(thread_rng().gen_range(10u64..200u64))
}

fn is_temporary_error(e: &anyhow::Error) -> bool {
    use io::ErrorKind::{ConnectionRefused, TimedOut, NotFound};
    use io::ErrorKind::{ConnectionAborted, ConnectionReset};

    match e.downcast_ref::<ReadError>() {
        | Some(ReadError::Eos) => return true,
        | Some(ReadError::Io { source, .. }) => {
            return matches!(source.kind(),
                ConnectionRefused | ConnectionReset | ConnectionAborted |
                TimedOut
            );
        }
        Some(_) => return false,
        _ => {},
    }
    match e.downcast_ref::<io::Error>().map(|e| e.kind()) {
        | Some(ConnectionRefused)
        | Some(ConnectionReset)
        | Some(ConnectionAborted)
        | Some(NotFound)  // For unix sockets
        | Some(TimedOut)
        => return true,
        _ => {},
    }
    return false;
}

impl Builder {
    pub fn from_credentials(credentials: &Credentials) -> Builder {
        Builder {
            addr: Addr(AddrImpl::Tcp(
                credentials.host.clone().unwrap_or_else(|| "127.0.0.1".into()),
                credentials.port)),
            user: credentials.user.clone(),
            password: credentials.password.clone(),
            database: credentials.database.clone()
                .unwrap_or_else(|| "edgedb".into()),
            wait: DEFAULT_WAIT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }
    pub async fn read_credentials(path: impl AsRef<Path>)
        -> anyhow::Result<Builder>
    {
        let path = path.as_ref();
        let res: anyhow::Result<Builder> = async {
            let data = fs::read(path).await?;
            let creds = serde_json::from_slice(&data)?;
            Ok(Builder::from_credentials(&creds))
        }.await;
        Ok(res.with_context(|| {
                format!("cannot read credentials file {}", path.display())
        })?)
    }
    pub fn from_dsn(dsn: &str) -> anyhow::Result<Builder> {
        if !dsn.starts_with("edgedb://") {
            anyhow::bail!("String {:?} is not a valid DSN", dsn)
        };
        let url = url::Url::parse(dsn)
            .with_context(|| format!("cannot parse DSN {:?}", dsn))?;
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
        }
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
    pub async fn connect(&self) -> anyhow::Result<Connection> {
        match &self.addr {
            Addr(AddrImpl::Tcp(host, port)) => {
                log::info!("Connecting via TCP {}:{}", host, port);
            }
            Addr(AddrImpl::Unix(path)) => {
                log::info!("Connecting via Unix `{}`", path.display());
            }
        };

        let start = Instant::now();
        let conn = loop {
            match timeout(self.connect_timeout, self._connect()).await
            {
                Err(e) if is_temporary_error(&e) => {
                    log::debug!("Temporary connection error: {:#}", e);
                    if self.wait > start.elapsed() {
                        sleep(sleep_duration()).await;
                        continue;
                    } else if self.wait > Duration::new(0, 0) {
                        return Err(e).context(format!("cannot establish \
                                                       connection for {:?}",
                                                       self.wait))?;
                    } else {
                        return Err(e)?;
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
    async fn _connect(&self)
        -> anyhow::Result<Connection>
    {
        let sock = match &self.addr {
            Addr(AddrImpl::Tcp(host, port)) => {
                let conn = TcpStream::connect(&(&host[..], *port)).await?;
                ByteStream::new_tcp_detached(conn)
            }
            Addr(AddrImpl::Unix(path)) => {
                #[cfg(windows)] {
                    anyhow::bail!("Unix socket are not supported on windows");
                }
                #[cfg(unix)] {
                    use async_std::os::unix::net::UnixStream;
                    let conn = UnixStream::connect(&path).await?;
                    ByteStream::new_unix_detached(conn)
                }
            }
        };
        let mut version = ProtocolVersion::current();
        let mut conn = Connection {
            stream: sock,
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

        seq.send_messages(&[
            ClientMessage::ClientHandshake(ClientHandshake {
                major_ver: version.major_ver,
                minor_ver: version.minor_ver,
                params,
                extensions: HashMap::new(),
            }),
        ]).await?;

        let mut msg = seq.message().await?;
        if let ServerMessage::ServerHandshake(ServerHandshake {
            major_ver, minor_ver, extensions: _
        }) = msg {
            version = ProtocolVersion { major_ver, minor_ver };
            // TODO(tailhook) record extensions
            msg = seq.message().await?;
        }
        match msg {
            ServerMessage::Authentication(Authentication::Ok) => {}
            ServerMessage::Authentication(Authentication::Sasl { methods })
            => {
                if methods.iter().any(|x| x == "SCRAM-SHA-256") {
                    if let Some(password) = &self.password {
                        scram(&mut seq, &self.user, password)
                            .await?;
                    } else {
                        Err(PasswordRequired)?;
                    }
                } else {
                    return Err(anyhow::anyhow!("No supported authentication \
                        methods: {:?}", methods));
                }
            }
            ServerMessage::ErrorResponse(err) => {
                return Err(anyhow::anyhow!("Error authenticating: {}", err));
            }
            msg => {
                return Err(anyhow::anyhow!(
                    "Error authenticating, unexpected message {:?}", msg));
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

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Addr(AddrImpl::Tcp(host, port)) => write!(f, "{}:{}", host, port),
            Addr(AddrImpl::Unix(path)) => write!(f, "{}", path.display()),
        }
    }
}

async fn scram(seq: &mut Sequence<'_>, user: &str, password: &str)
    -> anyhow::Result<()>
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
            return Err(anyhow::anyhow!("Bad auth response: {:?}", msg));
        }
    };
    let data = str::from_utf8(&data[..])
        .map_err(|_| anyhow::anyhow!(
            "invalid utf-8 in SCRAM-SHA-256 auth"))?;
    let scram = scram.handle_server_first(&data)
        .map_err(|e| anyhow::anyhow!("Authentication error: {}", e))?;
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
            return Err(anyhow::anyhow!(err));
        }
        msg => {
            return Err(anyhow::anyhow!("Bad auth response: {:?}", msg));
        }
    };
    let data = str::from_utf8(&data[..])
        .map_err(|_| anyhow::anyhow!(
            "invalid utf-8 in SCRAM-SHA-256 auth"))?;
    scram.handle_server_final(&data)
        .map_err(|e| anyhow::anyhow!("Authentication error: {}", e))?;
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
