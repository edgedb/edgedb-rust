use std::borrow::Cow;
use std::cmp::min;
use std::collections::HashMap;
use std::error::Error as _;
use std::future::{self, Future};
use std::io;
use std::net::IpAddr;
use std::str::{self, FromStr};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use gel_stream::{CommonError, ConnectionError, Connector, Target, TlsAlpn, TlsParameters};
use log::warn;
use rand::{rng, Rng};
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::{sleep, timeout_at, Instant};

use gel_protocol::client_message::{ClientHandshake, ClientMessage};
use gel_protocol::encoding::{Input, Output};
use gel_protocol::features::ProtocolVersion;
use gel_protocol::server_message::{
    Authentication, ErrorResponse, ServerHandshake, ServerMessage,
};
use gel_protocol::server_message::{
    MessageSeverity, ParameterStatus, RawTypedesc, TransactionState,
};
use gel_protocol::value::Value;

use crate::builder::Config;
use crate::errors::{
    AuthenticationError, ClientConnectionEosError, ClientConnectionError,
    ClientConnectionFailedError, ClientConnectionFailedTemporarilyError, ClientEncodingError,
    ClientError, Error, ErrorKind, IdleSessionTimeoutError, PasswordRequired,
    ProtocolEncodingError, ProtocolError, ProtocolTlsError,
};
use crate::raw::queries::Guard;
use crate::raw::{Connection, PingInterval};
use crate::server_params::{ServerParam, ServerParams, SystemConfig};
use crate::ClientSecurity;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum Mode {
    Normal { idle_since: Instant },
    Dirty,
    AwaitingPing,
}

impl Connection {
    pub fn is_consistent(&self) -> bool {
        matches!(self.mode, Mode::Normal { .. })
    }
    pub async fn is_connection_reset(&mut self) -> bool {
        tokio::select! { biased;
            msg = wait_message(&mut self.stream, &mut self.in_buf, &self.proto)
            => {
                match msg {
                    Ok(ServerMessage::ErrorResponse(e)) => {
                        let e: Error = e.into();
                        if e.is::<IdleSessionTimeoutError>() {
                            log::debug!("Connection reset due to inactivity.");
                        } else {
                            log::warn!("Unexpected error: {:#}", e);
                        }
                        true
                    }
                    Ok(m) => {
                        log::warn!("Unsolicited message: {:?}", m);
                        true
                    }
                    Err(e) => {
                        log::debug!("I/O error: {:#}", e);
                        true
                    }
                }
            }
            _ = future::ready(()) => {
                if self.in_buf.is_empty() {
                    false
                } else {
                    log::warn!("Unsolicited partial data {:?}",
                               &self.in_buf[..min(self.in_buf.len(), 16)]);
                    true
                }
            }
        }
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
    pub async fn send_messages<'x>(
        &mut self,
        msgs: impl IntoIterator<Item = &'x ClientMessage>,
    ) -> Result<(), Error> {
        send_messages(&mut self.stream, &mut self.out_buf, &self.proto, msgs).await
    }
    pub async fn message(&mut self) -> Result<ServerMessage, Error> {
        wait_message(&mut self.stream, &mut self.in_buf, &self.proto).await
    }
    pub fn get_server_param<T: ServerParam>(&self) -> Option<&T::Value> {
        self.server_params.get::<T>()
    }
    #[cfg(feature = "unstable")]
    pub async fn ping_while<T, F>(&mut self, other: F) -> T
    where
        F: Future<Output = T>,
    {
        if self.ping_interval == PingInterval::Unknown {
            self.ping_interval = self.calc_ping_interval();
        }
        if let PingInterval::Interval(interval) = self.ping_interval {
            let result = tokio::select! { biased;
                _ = self.background_pings(interval) => unreachable!(),
                res = other => res,
            };
            if self.mode == Mode::AwaitingPing {
                self.synchronize_ping().await.ok();
            }
            result
        } else {
            other.await
        }
    }
    async fn do_pings(&mut self, interval: Duration) -> Result<(), Error> {
        if self.mode == Mode::AwaitingPing {
            self.synchronize_ping().await?;
        }

        while let Mode::Normal {
            idle_since: last_pong,
        } = self.mode
        {
            match timeout_at(last_pong + interval, self.passive_wait()).await {
                Err(_) => {}
                Ok(Err(e)) => {
                    self.mode = Mode::Dirty;
                    return Err(ClientConnectionError::with_source(e))?;
                }
                Ok(Ok(_)) => unreachable!(),
            }

            self.mode = Mode::Dirty;
            self.send_messages(&[ClientMessage::Sync]).await?;
            self.mode = Mode::AwaitingPing;
            self.synchronize_ping().await?;
        }
        Ok(())
    }
    async fn background_pings<T>(&mut self, interval: Duration) -> T {
        self.do_pings(interval)
            .await
            .map_err(|e| log::info!("Connection error during background pings: {}", e))
            .ok();
        debug_assert_eq!(self.mode, Mode::Dirty);
        future::pending::<()>().await;
        unreachable!();
    }
    async fn synchronize_ping<'a>(&mut self) -> Result<(), Error> {
        debug_assert_eq!(self.mode, Mode::AwaitingPing);

        // Guard mechanism was invented for real queries, so we have to
        // make a little bit of workaround just for Pings
        let spurious_guard = Guard;
        match self.expect_ready(spurious_guard).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.mode = Mode::Dirty;
                Err(e)
            }
        }
    }
    pub async fn passive_wait(&mut self) -> io::Result<()> {
        loop {
            let msg = self
                .message()
                .await
                .map_err(|_| io::ErrorKind::InvalidData)?;
            match msg {
                // TODO(tailhook) update parameters?
                ServerMessage::ParameterStatus(_) => {}
                _ => return Err(io::ErrorKind::InvalidData)?,
            }
        }
    }
    fn calc_ping_interval(&self) -> PingInterval {
        if let Some(config) = self.server_params.get::<SystemConfig>() {
            if let Some(timeout) = config.session_idle_timeout {
                if timeout.is_zero() {
                    log::info!(
                        "Server disabled session_idle_timeout; \
                         pings are disabled."
                    );
                    PingInterval::Disabled
                } else {
                    let interval = Duration::from_secs(
                        (timeout.saturating_sub(Duration::from_secs(1)).as_secs_f64() * 0.9).ceil()
                            as u64,
                    );
                    if interval.is_zero() {
                        log::warn!(
                            "session_idle_timeout={:?} is too short; \
                             pings are disabled.",
                            timeout,
                        );
                        PingInterval::Disabled
                    } else {
                        log::info!(
                            "Setting ping interval to {:?} as \
                             session_idle_timeout={:?}",
                            interval,
                            timeout,
                        );
                        PingInterval::Interval(interval)
                    }
                }
            } else {
                PingInterval::Unknown
            }
        } else {
            PingInterval::Unknown
        }
    }
    pub async fn terminate(mut self) -> Result<(), Error> {
        let _ = self.begin_request()?; // not need to cleanup after that
        self.send_messages(&[ClientMessage::Terminate]).await?;
        match self.message().await {
            Err(e) if e.is::<ClientConnectionEosError>() => Ok(()),
            Err(e) => Err(e),
            Ok(msg) => Err(ProtocolError::with_message(format!(
                "unsolicited message {:?}",
                msg
            ))),
        }
    }
    pub fn transaction_state(&self) -> TransactionState {
        self.transaction_state
    }
    pub fn state_descriptor(&self) -> &RawTypedesc {
        &self.state_desc
    }
    pub fn protocol(&self) -> &ProtocolVersion {
        &self.proto
    }
}

async fn connect(cfg: &Config) -> Result<Connection, Error> {
    let mut tls = TlsParameters::insecure();
    tls.alpn = TlsAlpn::new_str(&["edgedb-binary", "gel-binary"]);
    tls.sni_override = match &cfg.0.tls_server_name {
        Some(server_name) => Some(Cow::from(server_name.clone())),
        None => {
            let host = cfg.0.address.host();
            if let Some(host) = host {
                if let Ok(ip) = IpAddr::from_str(&host) {
                    // FIXME: https://github.com/rustls/rustls/issues/184
                    let host = format!("{}.host-for-ip.edgedb.net", ip);
                    // for ipv6addr
                    let host = host.replace([':', '%'], "-");
                    if host.starts_with('-') {
                        Some(Cow::from(format!("i{}", host)))
                    } else {
                        Some(Cow::from(host))
                    }
                } else {
                    if let Some(host) = cfg.0.address.host() {
                        Some(Cow::from(host.to_string()))
                    } else {
                        None
                    }
                }
            } else {
                None
            }
        }
    };

    let mut target = cfg.0.address.clone();
    target.try_set_tls(tls);

    let start = Instant::now();
    let wait = cfg.0.wait;
    let warned = &mut false;
    let conn = loop {
        match connect_timeout(cfg, connect2(cfg, target.clone(), warned)).await {
            Err(e) if is_temporary(&e) => {
                log::debug!("Temporary connection error: {:#}", e);
                if wait > start.elapsed() {
                    sleep(connect_sleep()).await;
                    continue;
                } else if wait > Duration::new(0, 0) {
                    return Err(e.context(format!("cannot establish connection for {wait:?}")));
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

async fn connect2(
    cfg: &Config,
    mut target: Target,
    warned: &mut bool,
) -> Result<Connection, Error> {
    let mut connector = Connector::new(target.clone()).map_err(ClientConnectionError::with_source)?;
    connector.set_keepalive(cfg.0.tcp_keepalive);
    let mut res = connector.connect().await;

    // Allow plaintext reconnection if and only if ClientSecurity is InsecureDevMode and
    // the server replied with something that looks like TLS handshake failure.
    if let Err(ConnectionError::SslError(e)) = &res {
        if e.common_error() == Some(CommonError::InvalidTlsProtocolData) {
            if cfg.0.client_security == ClientSecurity::InsecureDevMode {
                target.try_remove_tls();
                warn!("TLS handshake failed, trying again without TLS");
                *warned = true;

                let mut connector = Connector::new(target.clone()).map_err(ClientConnectionError::with_source)?;
                connector.set_keepalive(cfg.0.tcp_keepalive);
                res = connector.connect().await;
            }
        }
    }

    let stream = res.map_err(ClientConnectionError::with_source)?;
    connect4(cfg, stream).await
}

async fn connect4(cfg: &Config, mut stream: gel_stream::RawStream) -> Result<Connection, Error> {
    let mut proto = ProtocolVersion::current();
    let mut out_buf = BytesMut::with_capacity(8192);
    let mut in_buf = BytesMut::with_capacity(8192);

    let mut params = HashMap::new();
    params.insert(String::from("user"), cfg.0.user.clone());
    params.insert(String::from("database"), cfg.0.database.clone());
    params.insert(String::from("branch"), cfg.0.branch.clone());
    if let Some(secret_key) = cfg.0.secret_key.clone() {
        params.insert(String::from("secret_key"), secret_key);
    }
    let (major_ver, minor_ver) = proto.version_tuple();
    send_messages(
        &mut stream,
        &mut out_buf,
        &proto,
        &[ClientMessage::ClientHandshake(ClientHandshake {
            major_ver,
            minor_ver,
            params,
            extensions: HashMap::new(),
        })],
    )
    .await?;

    let mut msg = wait_message(&mut stream, &mut in_buf, &proto).await?;
    if let ServerMessage::ServerHandshake(ServerHandshake {
        major_ver,
        minor_ver,
        extensions: _,
    }) = msg
    {
        proto = ProtocolVersion::new(major_ver, minor_ver);
        // TODO(tailhook) record extensions
        msg = wait_message(&mut stream, &mut in_buf, &proto).await?;
    }
    match msg {
        ServerMessage::Authentication(Authentication::Ok) => {}
        ServerMessage::Authentication(Authentication::Sasl { methods }) => {
            if methods.iter().any(|x| x == "SCRAM-SHA-256") {
                if let Some(password) = &cfg.0.password {
                    scram(
                        &mut stream,
                        &mut in_buf,
                        &mut out_buf,
                        &proto,
                        &cfg.0.user,
                        password,
                    )
                    .await?;
                } else {
                    return Err(PasswordRequired::with_message(
                        "Password required for the specified user/host",
                    ));
                }
            } else {
                return Err(AuthenticationError::with_message(format!(
                    "No supported authentication \
                    methods: {:?}",
                    methods
                )));
            }
        }
        ServerMessage::ErrorResponse(err) => {
            return Err(err.into());
        }
        msg => {
            return Err(ProtocolError::with_message(format!(
                "Error authenticating, unexpected message {:?}",
                msg
            )));
        }
    }

    let mut server_params = ServerParams::new();
    let mut state_desc = RawTypedesc::uninitialized();
    loop {
        let msg = wait_message(&mut stream, &mut in_buf, &proto).await?;
        match msg {
            ServerMessage::ReadyForCommand(ready) => {
                assert_eq!(ready.transaction_state, TransactionState::NotInTransaction);
                break;
            }
            ServerMessage::ServerKeyData(_) => {
                // TODO(tailhook) store it somehow?
            }
            ServerMessage::ParameterStatus(par) => match &par.name[..] {
                #[cfg(feature = "unstable")]
                b"pgaddr" => {
                    use crate::server_params::PostgresAddress;

                    let pgaddr: PostgresAddress = match serde_json::from_slice(&par.value[..]) {
                        Ok(a) => a,
                        Err(e) => {
                            log::warn!("Can't decode param {:?}: {}", par.name, e);
                            continue;
                        }
                    };
                    server_params.set::<PostgresAddress>(pgaddr);
                }
                #[cfg(feature = "unstable")]
                b"pgdsn" => {
                    use crate::server_params::PostgresDsn;

                    let pgdsn = match str::from_utf8(&par.value) {
                        Ok(a) => a.to_owned(),
                        Err(e) => {
                            log::warn!("Can't decode param {:?}: {}", par.name, e);
                            continue;
                        }
                    };

                    server_params.set::<PostgresDsn>(PostgresDsn(pgdsn));
                }
                b"system_config" => {
                    handle_system_config(par, &mut server_params)?;
                }
                _ => {}
            },
            ServerMessage::StateDataDescription(d) => {
                state_desc = d.typedesc;
            }
            ServerMessage::ErrorResponse(ErrorResponse {
                severity,
                code,
                message,
                attributes,
            }) => {
                log::warn!("Error received from server: {message}. Severity: {severity:?}. Code: {code:#x}");
                log::debug!("Error details: {attributes:?}");
            }
            _ => {
                log::warn!("unsolicited message {msg:#?}");
            }
        }
    }
    Ok(Connection {
        proto,
        server_params,
        mode: Mode::Normal {
            idle_since: Instant::now(),
        },
        transaction_state: TransactionState::NotInTransaction,
        state_desc,
        in_buf,
        out_buf,
        stream,
        ping_interval: PingInterval::Unknown,
    })
}

async fn scram(
    stream: &mut gel_stream::RawStream,
    in_buf: &mut BytesMut,
    out_buf: &mut BytesMut,
    proto: &ProtocolVersion,
    user: &str,
    password: &str,
) -> Result<(), Error> {
    use gel_protocol::client_message::SaslInitialResponse;
    use gel_protocol::client_message::SaslResponse;

    let mut scram = gel_auth::scram::ClientTransaction::new(Cow::Owned(user.to_string()));
    struct Env<'a>(&'a str);
    impl <'a> gel_auth::scram::ClientEnvironment for Env<'a> {
        fn get_salted_password(&self, salt: &[u8], iterations: usize) -> gel_auth::scram::Sha256Out {
            gel_auth::scram::generate_salted_password(self.0.as_bytes(), salt, iterations)
        }
        fn generate_nonce(&self) -> String {
            gel_auth::scram::generate_nonce()
        }
    }

    let env = Env(password);
    let msg = scram.process_message(&[], &env).map_err(AuthenticationError::with_source)?;
    send_messages(
        stream,
        out_buf,
        proto,
        &[ClientMessage::AuthenticationSaslInitialResponse(
            SaslInitialResponse {
                method: "SCRAM-SHA-256".into(),
                data: Bytes::from(msg.unwrap_or_default()),
            },
        )],
    )
    .await?;
    let msg = wait_message(stream, in_buf, proto).await?;
    let data = match msg {
        ServerMessage::Authentication(Authentication::SaslContinue { data }) => data,
        ServerMessage::ErrorResponse(err) => {
            return Err(err.into());
        }
        msg => {
            return Err(ProtocolError::with_message(format!(
                "Bad auth response: {:?}",
                msg
            )));
        }
    };

    let data = scram.process_message(&data, &env).map_err(AuthenticationError::with_source)?;
    send_messages(
        stream,
        out_buf,
        proto,
        &[ClientMessage::AuthenticationSaslResponse(SaslResponse {
            data: Bytes::from(data.unwrap_or_default()),
        })],
    )
    .await?;
    let msg = wait_message(stream, in_buf, proto).await?;
    let data = match msg {
        ServerMessage::Authentication(Authentication::SaslFinal { data }) => data,
        ServerMessage::ErrorResponse(err) => {
            return Err(err.into());
        }
        msg => {
            return Err(ProtocolError::with_message(format!(
                "auth response: {:?}",
                msg
            )));
        }
    };
    let data = scram.process_message(&data, &env).map_err(AuthenticationError::with_source)?;
    if data.is_some() || !scram.success() {
        return Err(AuthenticationError::with_message("Authentication failed"));
    }
    loop {
        let msg = wait_message(stream, in_buf, proto).await?;
        match msg {
            ServerMessage::Authentication(Authentication::Ok) => break,
            ServerMessage::ErrorResponse(ErrorResponse {
                severity,
                code,
                message,
                attributes,
            }) => {
                log::warn!("Error received from server: {message}. Severity: {severity:?}. Code: {code:#x}");
                log::debug!("Error details: {attributes:?}");
            }
            msg => {
                log::warn!("unsolicited message {msg:?}");
            }
        };
    }
    Ok(())
}

fn handle_system_config(
    param_status: ParameterStatus,
    server_params: &mut ServerParams,
) -> Result<(), Error> {
    let (typedesc, data) = param_status
        .parse_system_config()
        .map_err(ProtocolEncodingError::with_source)?;
    let codec = typedesc
        .build_codec()
        .map_err(ProtocolEncodingError::with_source)?;
    let system_config = codec
        .decode(data.as_ref())
        .map_err(ProtocolEncodingError::with_source)?;
    let mut config = SystemConfig {
        session_idle_timeout: None,
    };
    if let Value::Object { shape, fields } = system_config {
        for (el, field) in shape.elements.iter().zip(fields) {
            match el.name.as_str() {
                "id" => {}
                "session_idle_timeout" => {
                    config.session_idle_timeout = match field {
                        Some(Value::Duration(timeout)) => Some(timeout.abs_duration()),
                        _ => {
                            log::warn!("Wrong protocol: {}={:?}", el.name, field);
                            None
                        }
                    };
                }
                name => {
                    log::debug!("Unhandled system config: {}={:?}", name, field);
                }
            }
        }
        server_params.set::<SystemConfig>(config);
    } else {
        log::warn!("Received empty system config message.");
    }
    Ok(())
}

pub(crate) async fn send_messages<'x>(
    stream: &mut (impl AsyncWrite + Unpin),
    buf: &mut BytesMut,
    proto: &ProtocolVersion,
    messages: impl IntoIterator<Item = &'x ClientMessage>,
) -> Result<(), Error> {
    buf.truncate(0);
    for msg in messages {
        log::debug!(target: "edgedb::outgoing::frame",
                    "Frame Contents: {:#?}", msg);
        msg.encode(&mut Output::new(proto, buf))
            .map_err(ClientEncodingError::with_source)?;
    }
    stream
        .write_all_buf(buf)
        .await
        .map_err(ClientConnectionError::with_source)?;
    Ok(())
}

fn conn_err(err: io::Error) -> Error {
    ClientConnectionError::with_source(err)
}

pub async fn wait_message<'x>(
    stream: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
    proto: &ProtocolVersion,
) -> Result<ServerMessage, Error> {
    loop {
        match _wait_message(stream, buf, proto).await? {
            ServerMessage::LogMessage(msg) => {
                match msg.severity {
                    MessageSeverity::Debug => {
                        log::debug!("[{}] {}", msg.code, msg.text);
                    }
                    MessageSeverity::Notice | MessageSeverity::Info => {
                        log::info!("[{}] {}", msg.code, msg.text);
                    }
                    MessageSeverity::Warning | MessageSeverity::Unknown(_) => {
                        log::warn!("[{}] {}", msg.code, msg.text);
                    }
                }
                continue;
            }
            msg => return Ok(msg),
        }
    }
}

async fn _read_buf(stream: &mut (impl AsyncRead + Unpin), buf: &mut BytesMut) -> io::Result<usize> {
    // Because of a combination of multiple different API impedence
    // mismatches, when read_buf is called on a tls_api tokio stream,
    // tls_api will zero the entire buffer on each call. This leads to
    // pathological quadratic repeated zeroing when the buffer is much
    // larger than the bytes read per call.
    // (like for the 10MiB buffers for dump packets)
    //
    // We fix this by capping the size of the buffer that we pass to
    // read_buf.
    let cap = buf.spare_capacity_mut();
    let cap_len = cap.len();
    let mut rbuf = ReadBuf::uninit(&mut cap[..min(cap_len, 16 * 1024)]);
    let n = stream.read_buf(&mut rbuf).await?;
    unsafe {
        buf.set_len(buf.len() + n);
    }
    Ok(n)
}

async fn _wait_message<'x>(
    stream: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
    proto: &ProtocolVersion,
) -> Result<ServerMessage, Error> {
    while buf.len() < 5 {
        buf.reserve(5);
        if _read_buf(stream, buf).await.map_err(conn_err)? == 0 {
            return Err(ClientConnectionEosError::with_message(
                "end of stream while reading message",
            ));
        }
    }
    let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
    let frame_len = len + 1;

    while buf.len() < frame_len {
        buf.reserve(frame_len - buf.len());
        if _read_buf(stream, buf).await.map_err(conn_err)? == 0 {
            return Err(ClientConnectionEosError::with_message(
                "end of stream while reading message",
            ));
        }
    }
    let frame = buf.split_to(frame_len).freeze();
    let result = ServerMessage::decode(&mut Input::new(proto.clone(), frame))
        .map_err(ProtocolEncodingError::with_source)?;

    log::debug!(target: "edgedb::incoming::frame",
                "Frame Contents: {:#?}", result);

    Ok(result)
}

fn connect_sleep() -> Duration {
    Duration::from_millis(rng().random_range(10u64..200u64))
}

async fn connect_timeout<F, T>(cfg: &Config, f: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    use tokio::time::timeout;

    timeout(cfg.0.connect_timeout, f).await.unwrap_or_else(|_| {
        Err(ClientConnectionFailedTemporarilyError::with_source(
            io::Error::from(io::ErrorKind::TimedOut),
        ))
    })
}

fn is_temporary(e: &Error) -> bool {
    use io::ErrorKind::{
        AddrNotAvailable, ConnectionAborted, ConnectionRefused, ConnectionReset, NotFound,
        TimedOut, UnexpectedEof,
    };

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
    false
}
