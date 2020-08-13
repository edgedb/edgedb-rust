use std::collections::HashMap;
use std::fmt;
use std::io;
use std::str;
use std::time::{Instant, Duration};
use std::path::PathBuf;

use anyhow::{self, Context};
use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::task::sleep;
use async_listen::ByteStream;
use bytes::{Bytes, BytesMut};
use scram::ScramClient;
use serde_json::from_slice;
use typemap::TypeMap;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::server_message::{TransactionState};

use crate::server_params::PostgresAddress;
use crate::client::{Connection, Sequence};
use crate::errors::PasswordRequired;

#[derive(Debug, Clone)]
enum Addr {
    Tcp(String, u16),
    Unix(PathBuf),
}

#[derive(Debug, Clone)]
pub struct Builder {
    addr: Addr,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    wait: Option<Duration>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder {
            addr: Addr::Tcp("127.0.0.1".into(), 5656),
            user: None,
            password: None,
            database: None,
            wait: None,
        }
    }
    pub fn unix_addr(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.addr = Addr::Unix(path.into());
        self
    }
    pub fn tcp_addr(&mut self, addr: impl Into<String>, port: u16)
        -> &mut Self
    {
        self.addr = Addr::Tcp(addr.into(), port);
        self
    }
    pub fn user(&mut self, user: impl Into<String>) -> &mut Self {
        self.user = Some(user.into());
        self
    }
    pub fn password(&mut self, password: impl Into<String>) -> &mut Self {
        self.password = Some(password.into());
        self
    }
    pub fn database(&mut self, database: impl Into<String>) -> &mut Self {
        self.database = Some(database.into());
        self
    }
    pub fn get_effective_database(&self) -> String {
        self.database.as_ref().or(self.user.as_ref())
            .map(|x| x.clone())
            .unwrap_or_else(|| whoami::username())
    }
    pub fn wait_until_available(&mut self, time: Duration) -> &mut Self {
        self.wait = Some(time);
        self
    }
    async fn connect_tcp(&self, addr: impl ToSocketAddrs + fmt::Debug)
        -> anyhow::Result<ByteStream>
    {
        let start = Instant::now();
        let conn = loop {
            log::info!("Connecting via TCP {:?}", addr);
            let cres = TcpStream::connect(&addr).await;
            match cres {
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    if let Some(wait) = self.wait {
                        if wait > start.elapsed() {
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        } else {
                            Err(e).context(format!("Can't establish \
                                                    connection for {:?}",
                                                    wait))?
                        }
                    } else {
                        Err(e).with_context(
                            || format!("Can't connect to {:?}", addr))?;
                    }
                }
                Err(e) => {
                    Err(e).with_context(
                        || format!("Can't connect to {:?}", addr))?;
                }
                Ok(conn) => break conn,
            }
        };
        Ok(ByteStream::new_tcp_detached(conn))
    }
    #[cfg(unix)]
    async fn connect_unix(&self, path: &PathBuf) -> anyhow::Result<ByteStream>
    {
        use async_std::os::unix::net::UnixStream;

        let start = Instant::now();
        let conn = loop {
            log::info!("Connecting via {:?}", path);
            let cres = UnixStream::connect(&path).await;
            match cres {
                Err(e) if matches!(e.kind(),
                    io::ErrorKind::ConnectionRefused |
                    io::ErrorKind::NotFound)
                => {
                    if let Some(wait) = self.wait {
                        if wait > start.elapsed() {
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        } else {
                            Err(e).context(format!("Can't establish \
                                                    connection for {:?}",
                                                    wait))?
                        }
                    } else {
                        Err(e).with_context(|| format!(
                            "Can't connect to unix socket {:?}", path))?
                    }
                }
                Err(e) => {
                    Err(e).with_context(|| format!(
                        "Can't connect to unix socket {:?}", path))?
                }
                Ok(conn) => break conn,
            }
        };
        Ok(ByteStream::new_unix_detached(conn))
    }
    #[cfg(windows)]
    async fn connect_unix(&self, _path: &PathBuf)
        -> anyhow::Result<ByteStream>
    {
        anyhow::bail!("Unix socket are not supported on windows");
    }
    pub async fn connect(&self) -> anyhow::Result<Connection> {
        let user = if let Some(user) = &self.user {
            user.clone()
        } else {
            whoami::username()
        };
        let database = if let Some(database) = &self.database {
            database
        } else {
            &user
        };
        let sock = match &self.addr {
            Addr::Tcp(host, port) => {
                self.connect_tcp((host.as_ref(), *port)).await?
            }
            Addr::Unix(path) => self.connect_unix(path).await?,
        };
        let mut conn = Connection {
            stream: sock,
            input_buf: BytesMut::with_capacity(8192),
            output_buf: BytesMut::with_capacity(8192),
            params: TypeMap::custom(),
            transaction_state: TransactionState::NotInTransaction,
            dirty: false,
        };
        let mut seq = conn.start_sequence().await?;
        let mut params = HashMap::new();
        params.insert(String::from("user"), user.clone());
        params.insert(String::from("database"), database.clone());

        seq.send_messages(&[
            ClientMessage::ClientHandshake(ClientHandshake {
                major_ver: 0,
                minor_ver: 7,
                params,
                extensions: HashMap::new(),
            }),
        ]).await?;

        let mut msg = seq.message().await?;
        if let ServerMessage::ServerHandshake {..} = msg {
            eprintln!("WARNING: Connection negotiantion issue {:?}", msg);
            // TODO(tailhook) react on this somehow
            msg = seq.message().await?;
        }
        match msg {
            ServerMessage::Authentication(Authentication::Ok) => {}
            ServerMessage::Authentication(Authentication::Sasl { methods })
            => {
                if methods.iter().any(|x| x == "SCRAM-SHA-256") {
                    if let Some(password) = &self.password {
                        scram(&mut seq, &user, password)
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
                                    eprintln!("Can't decode param {:?}: {}",
                                        par.name, e);
                                    continue;
                                }
                            };
                            server_params.insert::<PostgresAddress>(pgaddr);
                        }
                        _ => {},
                    }
                }
                _ => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        }
        conn.params = server_params;
        Ok(conn)
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
                eprintln!("WARNING: unsolicited message {:?}", msg);
            }
        };
    }
    Ok(())
}
