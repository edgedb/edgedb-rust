use std::collections::HashMap;
use std::fmt;
use std::io;
use std::str;
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::path::PathBuf;

use anyhow::{self, Context};
use async_std::prelude::StreamExt;
use async_std::future::{timeout, pending};
use async_std::io::prelude::WriteExt;
use async_std::io::ReadExt;
use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::task::sleep;
use async_listen::ByteStream;
use bytes::{Bytes, BytesMut};
use scram::ScramClient;
use serde_json::from_slice;
use typemap::TypeMap;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::client_message::{Prepare, IoFormat, Cardinality};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{Execute, ExecuteScript};
use edgedb_protocol::codec::Codec;
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::server_message::{TransactionState};
use edgedb_protocol::queryable::{Queryable};
use edgedb_protocol::value::Value;
use edgedb_protocol::descriptors::OutputTypedesc;

use crate::reader::{self, QueryableDecoder, QueryResponse};
use crate::server_params::PostgresAddress;

pub use crate::reader::Reader;


#[derive(Debug, thiserror::Error)]
#[error("Connection is inconsistent state. Please reconnect.")]
pub struct ConnectionDirty;


#[derive(Debug, thiserror::Error)]
#[error("Password required for the specified user/host")]
pub struct PasswordRequired;


pub trait Sealed {}  // TODO(tailhook) private
pub trait PublicParam: Sealed + typemap::Key + typemap::DebugAny + Send + Sync
{}


pub struct Connection {
    stream: ByteStream,
    input_buf: BytesMut,
    output_buf: BytesMut,
    params: TypeMap<dyn typemap::DebugAny + Send + Sync>,
    transaction_state: TransactionState,
    dirty: bool,
}

pub struct Sequence<'a> {
    pub writer: Writer<'a>,
    pub reader: Reader<'a>,
    dirty: &'a mut bool,
}

#[derive(Debug, Clone)]
pub struct Builder {
    addr: Addr,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    wait: Option<Duration>,
}

pub struct Writer<'a> {
    stream: &'a ByteStream,
    outbuf: &'a mut BytesMut,
}

#[derive(Debug)]
pub struct NoResultExpected {
    pub completion_message: Bytes,
}

#[derive(Debug, Clone)]
pub enum Addr {
    Tcp(String, u16),
    Unix(PathBuf),
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

impl<'a> Sequence<'a> {

    pub fn response<D: reader::Decode>(self, decoder: D)
        -> QueryResponse<'a, D>
    {
        reader::QueryResponse {
            seq: Some(self),
            buffer: Vec::new(),
            error: None,
            complete: false,
            decoder,
        }
    }

    pub fn end_clean(self) {
        *self.dirty = false;
    }
}

impl Connection {
    pub async fn passive_wait<T>(&mut self) -> T {
        let mut buf = [0u8; 1];
        self.stream.read(&mut buf[..]).await.ok();
        // any erroneous or successful read (even 0) means need reconnect
        self.dirty = true;
        pending::<()>().await;
        unreachable!();
    }
    pub fn is_consistent(&self) -> bool {
        !self.dirty
    }
    pub async fn terminate(mut self) -> anyhow::Result<()> {
        let mut seq = self.start_sequence().await?;
        seq.send_messages(&[ClientMessage::Terminate]).await?;
        match seq.message().await {
            Err(reader::ReadError::Eos) => Ok(()),
            Err(e) => Err(e)?,
            Ok(msg) => anyhow::bail!("unsolicited message {:?}", msg),
        }
    }
    pub async fn start_sequence<'x>(&'x mut self)
        -> anyhow::Result<Sequence<'x>>
    {
        if self.dirty {
            anyhow::bail!("Connection is inconsistent state. \
                Please reconnect.");
        }
        self.dirty = true;
        let reader = Reader {
            buf: &mut self.input_buf,
            stream: &self.stream,
            transaction_state: &mut self.transaction_state,
        };
        let writer = Writer {
            outbuf: &mut self.output_buf,
            stream: &self.stream,
        };
        Ok(Sequence { writer, reader, dirty: &mut self.dirty})
    }

    pub fn get_param<T: PublicParam>(&self)
        -> Option<&<T as typemap::Key>::Value>
        where <T as typemap::Key>::Value: fmt::Debug + Send + Sync
    {
        self.params.get::<T>()
    }
    pub fn transaction_state(&self) -> TransactionState {
        self.transaction_state
    }
}

impl<'a> Writer<'a> {

    pub async fn send_messages<'x, I>(&mut self, msgs: I)
        -> Result<(), anyhow::Error>
        where I: IntoIterator<Item=&'x ClientMessage>
    {
        self.outbuf.truncate(0);
        for msg in msgs {
            msg.encode(&mut self.outbuf)?;
        }
        self.stream.write_all(&self.outbuf[..]).await?;
        Ok(())
    }

}

async fn scram(seq: &mut Sequence<'_>, user: &str, password: &str)
    -> Result<(), anyhow::Error>
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

impl<'a> Sequence<'a> {
    pub async fn send_messages<'x, I>(&mut self, msgs: I)
        -> Result<(), anyhow::Error>
        where I: IntoIterator<Item=&'x ClientMessage>
    {
        self.writer.send_messages(msgs).await
    }

    pub async fn expect_ready(mut self) -> Result<(), reader::ReadError> {
        self.reader.wait_ready().await?;
        self.end_clean();
        Ok(())
    }

    pub fn message(&mut self) -> reader::MessageFuture<'_, 'a> {
        self.reader.message()
    }

    // TODO(tailhook) figure out if this is the best way
    pub async fn err_sync(mut self) -> Result<(), anyhow::Error> {
        self.writer.send_messages(&[ClientMessage::Sync]).await?;
        timeout(Duration::from_secs(10), self.expect_ready()).await??;
        Ok(())
    }

    pub async fn _process_exec(mut self) -> anyhow::Result<Bytes> {
        let status = loop {
            match self.reader.message().await? {
                ServerMessage::CommandComplete(c) => {
                    self.reader.wait_ready().await?;
                    self.end_clean();
                    break c.status_data;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.reader.wait_ready().await?;
                    self.end_clean();
                    return Err(anyhow::anyhow!(err));
                }
                ServerMessage::Data(_) => { }
                msg => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        Ok(status)
    }

    async fn _query(&mut self, request: &str, arguments: &Value,
        io_format: IoFormat)
        -> Result<OutputTypedesc, anyhow::Error >
    {
        let statement_name = Bytes::from_static(b"");

        self.send_messages(&[
            ClientMessage::Prepare(Prepare {
                headers: HashMap::new(),
                io_format,
                expected_cardinality: Cardinality::Many,
                statement_name: statement_name.clone(),
                command_text: String::from(request),
            }),
            ClientMessage::Sync,
        ]).await?;

        loop {
            let msg = self.reader.message().await?;
            match msg {
                ServerMessage::PrepareComplete(..) => {
                    self.reader.wait_ready().await?;
                    break;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.reader.wait_ready().await?;
                    return Err(anyhow::anyhow!(err));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsolicited message {:?}", msg));
                }
            }
        }

        self.send_messages(&[
            ClientMessage::DescribeStatement(DescribeStatement {
                headers: HashMap::new(),
                aspect: DescribeAspect::DataDescription,
                statement_name: statement_name.clone(),
            }),
            ClientMessage::Flush,
        ]).await?;

        let data_description = loop {
            let msg = self.reader.message().await?;
            match msg {
                ServerMessage::CommandDataDescription(data_desc) => {
                    break data_desc;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.reader.wait_ready().await?;
                    return Err(anyhow::anyhow!(err));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsolicited message {:?}", msg));
                }
            }
        };
        let desc = data_description.output()?;
        let incodec = data_description.input()?.build_codec()?;

        let mut arg_buf = BytesMut::with_capacity(8);
        incodec.encode(&mut arg_buf, &arguments)?;

        self.send_messages(&[
            ClientMessage::Execute(Execute {
                headers: HashMap::new(),
                statement_name: statement_name.clone(),
                arguments: arg_buf.freeze(),
            }),
            ClientMessage::Sync,
        ]).await?;
        Ok(desc)
    }
}

impl Connection {
    pub async fn execute<S>(&mut self, request: S)
        -> Result<Bytes, anyhow::Error>
        where S: ToString,
    {
        let mut seq = self.start_sequence().await?;
        seq.send_messages(&[
            ClientMessage::ExecuteScript(ExecuteScript {
                headers: HashMap::new(),
                script_text: request.to_string(),
            }),
        ]).await?;
        let status = loop {
            match seq.message().await? {
                ServerMessage::CommandComplete(c) => {
                    seq.expect_ready().await?;
                    break c.status_data;
                }
                ServerMessage::ErrorResponse(err) => {
                    seq.expect_ready().await?;
                    return Err(anyhow::anyhow!(err));
                }
                msg => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        Ok(status)
    }

    pub async fn query<R>(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<QueryResponse<'_, QueryableDecoder<R>>>
        where R: Queryable,
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(request, arguments, IoFormat::Binary).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                R::check_descriptor(
                    &desc.as_queryable_context(), root_pos)?;
                Ok(seq.response(QueryableDecoder::new()))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected { completion_message })?
            }
        }
    }

    pub async fn query_row<R>(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<R>
        where R: Queryable,
    {
        let mut query = self.query(request, arguments).await?;
        if let Some(result) = query.next().await.transpose()? {
            if let Some(_) = query.next().await.transpose()? {
                query.skip_remaining().await?;
                anyhow::bail!("extra row returned for query_row");
            }
            Ok(result)
        } else {
            anyhow::bail!("no results returned")
        }
    }

    pub async fn query_row_opt<R>(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<Option<R>>
        where R: Queryable,
    {
        let mut query = self.query(request, arguments).await?;
        if let Some(result) = query.next().await.transpose()? {
            if let Some(_) = query.next().await.transpose()? {
                anyhow::bail!("extra row returned for query_row");
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub async fn query_json(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<QueryResponse<'_, QueryableDecoder<String>>>
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(request, arguments, IoFormat::Json).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                String::check_descriptor(
                    &desc.as_queryable_context(), root_pos)?;
                Ok(seq.response(QueryableDecoder::new()))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected { completion_message })?
            }
        }
    }

    pub async fn query_json_els(&mut self, request: &str, arguments: &Value)
        -> Result<
            QueryResponse<'_, QueryableDecoder<String>>,
            anyhow::Error
        >
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(request, arguments,
            IoFormat::JsonElements).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                String::check_descriptor(
                    &desc.as_queryable_context(), root_pos)?;
                Ok(seq.response(QueryableDecoder::new()))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected { completion_message })?
            }
        }
    }

    pub async fn query_dynamic(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<QueryResponse<'_, Arc<dyn Codec>>>
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(request, arguments, IoFormat::Binary).await?;
        let codec = desc.build_codec()?;
        Ok(seq.response(codec))
    }


    #[allow(dead_code)]
    pub async fn execute_args(&mut self, request: &str, arguments: &Value)
        -> Result<Bytes, anyhow::Error>
    {
        let mut seq = self.start_sequence().await?;
        seq._query(request, arguments, IoFormat::Binary).await?;
        return seq._process_exec().await;
    }

    pub async fn get_version(&mut self) -> Result<String, anyhow::Error> {
        self.query_row(
            "SELECT sys::get_version_as_str()",
            &Value::empty_tuple(),
        ).await
        .context("cannot fetch database version")
    }
}


impl std::error::Error for NoResultExpected {}

impl fmt::Display for NoResultExpected {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "no result expected: {}",
            String::from_utf8_lossy(&self.completion_message[..]))
    }
}
