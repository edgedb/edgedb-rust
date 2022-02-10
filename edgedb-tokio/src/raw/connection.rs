use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error as _;
use std::future::Future;
use std::io;
use std::str;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rand::{thread_rng, Rng};
use scram::ScramClient;
use tls_api::{TlsConnector, TlsConnectorBox, TlsStream, TlsStreamDyn};
use tls_api::{TlsConnectorBuilder};
use tls_api_not_tls::TlsConnector as PlainConnector;
use tokio::io::{AsyncReadExt, ReadHalf};
use tokio::io::{AsyncWrite, AsyncWriteExt, split, WriteHalf};
use tokio::net::TcpStream;
use tokio::time::sleep;
use typemap::{TypeMap, DebugAny};
use webpki::DnsNameRef;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::encoding::{Input, Output};
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::server_message::ParameterStatus;
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::server_message::{TransactionState, ServerHandshake};
use edgedb_protocol::value::Value;

use crate::raw::ConnInner;
use crate::tls;
use crate::builder::{Config, Address};
use crate::errors::{Error, ClientError, ErrorKind};
use crate::errors::{ClientConnectionFailedTemporarilyError, ProtocolTlsError};
use crate::errors::{ClientConnectionError, ClientConnectionFailedError};
use crate::errors::{ClientEncodingError, ClientConnectionEosError};
use crate::errors::{ProtocolEncodingError, ProtocolError};
use crate::errors::{AuthenticationError, PasswordRequired};
use crate::server_params::{PostgresAddress, SystemConfig};

const MAX_MESSAGE_SIZE: usize = 1_048_576;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum State {
    Normal {
        idle_since: Instant,
    },
    Dirty,
    AwaitingPing,
}

impl ConnInner {
    pub fn is_consistent(&self) -> bool {
        matches!(self.state, State::Normal {..})
    }
    pub async fn connect(config: &Config) -> Result<Self, Error> {
        connect(config).await.map_err(|e| {
            if e.is::<ClientConnectionError>() {
                e.refine_kind::<ClientConnectionFailedError>()
            } else {
                e
            }
        })
    }
    pub async fn send_messages<'x>(&mut self,
        msgs: impl IntoIterator<Item=&'x ClientMessage>)
        -> Result<(), Error>
    {
        send_messages(&mut self.stream, &mut self.out_buf, &self.proto, msgs)
            .await
    }
    pub async fn message(&mut self) -> Result<ServerMessage, Error> {
        wait_message(&mut self.stream, &mut self.in_buf, &self.proto).await
    }
}

async fn connect(cfg: &Config) -> Result<ConnInner, Error> {
    let tls = tls::connector(cfg.0.verifier.clone())
        .map_err(|e| ClientError::with_source_ref(e)
                 .context("cannot create TLS connector"))?;
    match &cfg.0.address {
        Address::Unix(path) => {
            log::info!("Connecting via Unix `{}`", path.display());
        }
        Address::Tcp((host, port)) => {
            log::info!("Connecting via TCP {host}:{port}");
        }
    }

    let start = Instant::now();
    let wait = cfg.0.wait;
    let ref mut warned = false;
    let conn = loop {
        match connect_timeout(cfg, connect2(cfg, &tls, warned)).await {
            Err(e) if is_temporary(&e) => {
                log::debug!("Temporary connection error: {:#}", e);
                if wait > start.elapsed() {
                    sleep(connect_sleep()).await;
                    continue;
                } else if wait > Duration::new(0, 0) {
                    return Err(e.context(
                        format!("cannot establish connection for {wait:?}")));
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

async fn connect2(cfg: &Config, tls: &TlsConnectorBox, warned: &mut bool)
    -> Result<ConnInner, Error>
{
    let stream = match connect3(cfg, tls).await {
        Err(e) if e.is::<ProtocolTlsError>() => {
            if !*warned {
                log::warn!("TLS connection failed. \
                    Trying plaintext...");
                *warned = true;
            }
            connect3(
                cfg,
                &PlainConnector::builder()
                    .map_err(ClientError::with_source_ref)?
                    .build().map_err(ClientError::with_source_ref)?
                    .into_dyn(),
            ).await?
        }
        Err(e) => return Err(e),
        Ok(r) => match r.get_alpn_protocol() {
            Ok(Some(protocol)) if protocol == b"edgedb-binary" => r,
            _ => match &cfg.0.address {
                Address::Tcp(_) => {
                    Err(ClientConnectionFailedError::with_message(
                        "Server does not support the EdgeDB binary protocol."
                    ))?
                },
                Address::Unix(_) => r,  // don't check ALPN on UNIX stream
            }
        }
    };
    connect4(cfg, stream).await
}

async fn connect3(cfg: &Config, tls: &TlsConnectorBox)
    -> Result<TlsStream, Error>
{
    match &cfg.0.address {
        Address::Tcp(addr@(host,_)) => {
            let conn = TcpStream::connect(addr).await
                .map_err(ClientConnectionError::with_source)?;
            let is_valid_dns = DnsNameRef::try_from_ascii_str(host).is_ok();
            let host = if !is_valid_dns {
                // FIXME: https://github.com/rustls/rustls/issues/184
                // If self.host is neither an IP address nor a valid DNS
                // name, the hacks below won't make it valid anyways.
                let host = format!("{}.host-for-ip.edgedb.net", host);
                // for ipv6addr
                let host = host.replace(":", "-").replace("%", "-");
                if host.starts_with("-") {
                    Cow::from(format!("i{}", host))
                } else {
                    Cow::from(host)
                }
            } else {
                Cow::from(host)
            };
            Ok(tls.connect(&host[..], conn).await.map_err(tls_fail)?)
        }
        Address::Unix(path) => {
            #[cfg(windows)] {
                return Err(ClientError::with_message(
                    "Unix socket are not supported on windows",
                ));
            }
            #[cfg(unix)] {
                use tokio::net::UnixStream;
                let conn = UnixStream::connect(&path).await
                    .map_err(ClientConnectionError::with_source)?;
                Ok(
                    PlainConnector::builder()
                        .map_err(ClientError::with_source_ref)?
                        .build().map_err(ClientError::with_source_ref)?
                        .into_dyn()
                    .connect("localhost", conn).await.map_err(tls_fail)?
                )
            }
        }
    }
}

async fn connect4(cfg: &Config, mut stream: TlsStream)
    -> Result<ConnInner, Error>
{
    let mut proto = ProtocolVersion::current();
    let mut out_buf = BytesMut::with_capacity(8192);
    let mut in_buf = BytesMut::with_capacity(8192);

    let mut params = HashMap::new();
    params.insert(String::from("user"), cfg.0.user.clone());
    params.insert(String::from("database"), cfg.0.database.clone());
    let (major_ver, minor_ver) = proto.version_tuple();
    send_messages(&mut stream, &mut out_buf, &proto, &[
        ClientMessage::ClientHandshake(ClientHandshake {
            major_ver,
            minor_ver,
            params,
            extensions: HashMap::new(),
        }),
    ]).await?;

    let mut msg = wait_message(&mut stream, &mut in_buf, &proto).await?;
    if let ServerMessage::ServerHandshake(ServerHandshake {
        major_ver, minor_ver, extensions: _
    }) = msg {
        proto = ProtocolVersion::new(major_ver, minor_ver);
        // TODO(tailhook) record extensions
        msg = wait_message(&mut stream, &mut in_buf, &proto).await?;
    }
    match msg {
        ServerMessage::Authentication(Authentication::Ok) => {}
        ServerMessage::Authentication(Authentication::Sasl { methods })
        => {
            if methods.iter().any(|x| x == "SCRAM-SHA-256") {
                if let Some(password) = &cfg.0.password {
                    scram(&mut stream, &mut in_buf, &mut out_buf, &proto,
                          &cfg.0.user, password).await?;
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
        let msg = wait_message(&mut stream, &mut in_buf, &proto).await?;
        match msg {
            ServerMessage::ReadyForCommand(_) => {
                break;
            }
            ServerMessage::ServerKeyData(_) => {
                // TODO(tailhook) store it somehow?
            }
            ServerMessage::ParameterStatus(par) => {
                match &par.name[..] {
                    b"pgaddr" => {
                        let pgaddr: PostgresAddress;
                        pgaddr = match serde_json::from_slice(&par.value[..]) {
                            Ok(a) => a,
                            Err(e) => {
                                log::warn!("Can't decode param {:?}: {}",
                                    par.name, e);
                                continue;
                            }
                        };
                        server_params.insert::<PostgresAddress>(pgaddr);
                    }
                    b"system_config" => {
                        handle_system_config(par, &mut server_params)?;
                    }
                    _ => {}
                }
            }
            _ => {
                log::warn!("unsolicited message {:?}", msg);
            }
        }
    }
    Ok(ConnInner {
        proto,
        params: server_params,
        state: State::Normal { idle_since: Instant::now() },
        in_buf,
        out_buf,
        stream,
    })
}

async fn scram(
    stream: &mut TlsStream, in_buf: &mut BytesMut, out_buf: &mut BytesMut,
    proto: &ProtocolVersion,
    user: &str, password: &str)
    -> Result<(), Error>
{
    use edgedb_protocol::client_message::SaslInitialResponse;
    use edgedb_protocol::client_message::SaslResponse;

    let scram = ScramClient::new(&user, &password, None);

    let (scram, first) = scram.client_first();
    send_messages(stream, out_buf, &proto, &[
        ClientMessage::AuthenticationSaslInitialResponse(
            SaslInitialResponse {
            method: "SCRAM-SHA-256".into(),
            data: Bytes::copy_from_slice(first.as_bytes()),
        }),
    ]).await?;
    let msg = wait_message(stream, in_buf, &proto).await?;
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
    send_messages(stream, out_buf, &proto, &[
        ClientMessage::AuthenticationSaslResponse(
            SaslResponse {
                data: Bytes::copy_from_slice(data.as_bytes()),
            }),
    ]).await?;
    let msg = wait_message(stream, in_buf, &proto).await?;
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
        let msg = wait_message(stream, in_buf, &proto).await?;
        match msg {
            ServerMessage::Authentication(Authentication::Ok) => break,
            msg => {
                log::warn!("unsolicited message {:?}", msg);
            }
        };
    }
    Ok(())
}

fn handle_system_config(
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

async fn send_messages<'x>(
    stream: &mut TlsStream,
    buf: &mut BytesMut,
    proto: &ProtocolVersion,
    messages: impl IntoIterator<Item=&'x ClientMessage>
) -> Result<(), Error> {
    buf.truncate(0);
    for msg in messages {
        msg.encode(&mut Output::new(proto, buf))
            .map_err(ClientEncodingError::with_source)?;
    }
    stream.write_all_buf(buf).await
        .map_err(ClientConnectionError::with_source)?;
    Ok(())
}

fn conn_err(err: io::Error) -> Error {
    ClientConnectionError::with_source(err)
}

async fn wait_message<'x>(stream: &mut TlsStream,
                          buf: &mut BytesMut, proto: &ProtocolVersion)
    -> Result<ServerMessage, Error>
{
    while buf.len() < 5 {
        buf.reserve(5);
        if stream.read_buf(buf).await.map_err(conn_err)? == 0 {
            return Err(ClientConnectionEosError::with_message(
                "end of stream while reading message"));
        }
    }
    let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(ClientConnectionError::with_message(
            format!("message length {len} is too long")));
    }
    let frame_len = len + 1;

    while buf.len() < frame_len {
        buf.reserve(frame_len - buf.len());
        if stream.read_buf(buf).await.map_err(conn_err)? == 0 {
            return Err(ClientConnectionEosError::with_message(
                "end of stream while reading message"));
        }
    }
    let frame = buf.split_to(frame_len).freeze();
    let result = ServerMessage::decode(&mut Input::new(
        proto.clone(),
        frame,
    )).map_err(ProtocolEncodingError::with_source)?;

    log::debug!(target: "edgedb::incoming::frame",
                "Frame Contents: {:#?}", result);

    Ok(result)
}

fn connect_sleep() -> Duration {
    Duration::from_millis(thread_rng().gen_range(10u64..200u64))
}

async fn connect_timeout<F, T>(cfg: &Config, f: F) -> Result<T, Error>
    where F: Future<Output = Result<T, Error>>,
{
    use tokio::time::timeout;

    timeout(cfg.0.connect_timeout, f).await
    .unwrap_or_else(|_| {
        Err(ClientConnectionFailedTemporarilyError::with_source(
            io::Error::from(io::ErrorKind::TimedOut)
        ))
    })
}

fn is_temporary(e: &Error) -> bool {
    use io::ErrorKind::{ConnectionRefused, TimedOut, NotFound};
    use io::ErrorKind::{ConnectionAborted, ConnectionReset, UnexpectedEof};
    use io::ErrorKind::{AddrNotAvailable};

    if e.is::<ClientConnectionFailedTemporarilyError>() {
        return true;
    }
    // todo(tailhook) figure out whether TLS api errors are properly unpacked
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

fn tls_fail(e: anyhow::Error) -> Error {
    if let Some(e) = e.downcast_ref::<rustls::Error>() {
        if matches!(e, rustls::Error::CorruptMessage) {
            return ProtocolTlsError::with_message(
                "corrupt message, possibly server \
                 does not support TLS connection."
            );
        }
    }
    ClientConnectionError::with_source_ref(e)
}
