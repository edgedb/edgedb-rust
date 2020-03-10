use std::collections::HashMap;
use std::fmt;
use std::io;
use std::mem::replace;
use std::str;
use std::sync::Arc;

use anyhow::{self, Context};
use async_std::prelude::StreamExt;
use async_std::io::{stdin, stdout};
use async_std::io::prelude::WriteExt;
use async_std::net::{TcpStream, ToSocketAddrs};
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
use edgedb_protocol::queryable::{Queryable};
use edgedb_protocol::value::Value;
use edgedb_protocol::descriptors::OutputTypedesc;
use crate::commands::backslash;
use crate::options::Options;
use crate::print::{self, print_to_stdout, PrintError};
use crate::prompt;
use crate::reader::{Reader, ReadError, QueryableDecoder, QueryResponse};
use crate::repl;
use crate::server_params::PostgresAddress;
use crate::statement::{ReadStatement, EndOfFile};
use crate::variables::input_variables;
use crate::error_display::print_query_error;


const QUERY_OPT_IMPLICIT_LIMIT: u16 = 0xFF01;


pub struct Connection {
    stream: ByteStream,
}

pub struct Client<'a> {
    stream: &'a ByteStream,
    outbuf: BytesMut,
    reader: Reader<&'a ByteStream>,
    pub params: TypeMap<dyn typemap::DebugAny + Send>,
}

#[derive(Debug)]
pub struct NoResultExpected {
    completion_message: Bytes,
}


impl Connection {
    pub async fn new<A: ToSocketAddrs>(addrs: A)
        -> Result<Connection, io::Error>
    {
        let s = ByteStream::new_tcp_detached(
                TcpStream::connect(addrs).await?);
        s.set_nodelay(true)?;
        Ok(Connection {
            stream: s,
        })
    }
    #[cfg(unix)]
    pub async fn from_options(options: &Options)
        -> Result<Connection, io::Error>
    {
        let unix_host = options.host.contains("/");
        if options.admin || unix_host {
            let prefix = if unix_host {
                &options.host
            } else {
                "/var/run/edgedb"
            };
            let path = if prefix.contains(".s.EDGEDB") {
                // it's the full path
                prefix.into()
            } else {
                if options.admin {
                    format!("{}/.s.EDGEDB.admin.{}", prefix, options.port)
                } else {
                    format!("{}/.s.EDGEDB.{}", prefix, options.port)
                }
            };
            let s = ByteStream::new_unix_detached(
                async_std::os::unix::net::UnixStream::connect(path).await?
            );
            s.set_nodelay(true)?;
            Ok(Connection {
                stream: s,
            })
        } else {
            Ok(Connection::new((&options.host[..], options.port)).await?)
        }
    }

    #[cfg(windows)]
    pub async fn from_options(options: &Options)
        -> Result<Connection, io::Error>
    {
        Ok(Connection::new((&options.host[..], options.port)).await?)
    }

    pub async fn authenticate<'x>(&'x mut self, options: &Options,
        database: &str)
        -> Result<Client<'x>, anyhow::Error>
    {
        let (rd, stream) = (&self.stream, &self.stream);
        let reader = Reader::new(rd);
        let mut cli = Client {
            stream, reader,
            outbuf: BytesMut::with_capacity(8912),
            params: TypeMap::custom(),
        };
        let mut params = HashMap::new();
        params.insert(String::from("user"), options.user.clone());
        params.insert(String::from("database"), String::from(database));

        cli.send_messages(&[
            ClientMessage::ClientHandshake(ClientHandshake {
                major_ver: 0,
                minor_ver: 7,
                params,
                extensions: HashMap::new(),
            }),
        ]).await?;

        let mut msg = cli.reader.message().await?;
        if let ServerMessage::ServerHandshake {..} = msg {
            eprintln!("WARNING: Connection negotiantion issue {:?}", msg);
            // TODO(tailhook) react on this somehow
            msg = cli.reader.message().await?;
        }
        match msg {
            ServerMessage::Authentication(Authentication::Ok) => {}
            ServerMessage::Authentication(Authentication::Sasl { methods })
            => {
                if methods.iter().any(|x| x == "SCRAM-SHA-256") {
                    cli.scram(&options).await?;
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

        loop {
            let msg = cli.reader.message().await?;
            match msg {
                ServerMessage::ReadyForCommand(..) => break,
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
                            cli.params.insert::<PostgresAddress>(pgaddr);
                        }
                        _ => {},
                    }
                }
                _ => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        }
        Ok(cli)
    }
}


pub async fn interactive_main(options: Options, mut state: repl::State)
    -> Result<(), anyhow::Error>
{
    loop {
        let mut conn = Connection::from_options(&options).await?;
        let cli = conn.authenticate(&options, &state.database).await?;
        match _interactive_main(cli, &options, &mut state).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if let Some(err) = e.downcast_ref::<backslash::ChangeDb>() {
                    state.database = err.target.clone();
                    continue;
                }
                if let Some(err) = e.downcast_ref::<io::Error>() {
                    if err.kind() == io::ErrorKind::BrokenPipe {
                        eprintln!("Connection is broken. Reconnecting...");
                        continue;
                    }
                }
                return Err(e);
            }
        }
    }
}

async fn _interactive_main(
    mut cli: Client<'_>, options: &Options, mut state: &mut repl::State)
    -> Result<(), anyhow::Error>
{
    use crate::repl::OutputMode::*;
    let mut initial = String::new();
    let statement_name = Bytes::from_static(b"");

    'input_loop: loop {
        let inp = match
            state.edgeql_input(&replace(&mut initial, String::new())).await
        {
            prompt::Input::Eof => {
                cli.send_messages(&[ClientMessage::Terminate]).await?;
                match cli.reader.message().await {
                    Err(ReadError::Eos) => {}
                    Err(e) => {
                        eprintln!("WARNING: error on terminate: {}", e);
                    }
                    Ok(msg) => {
                        eprintln!("WARNING: unsolicited message {:?}", msg);
                    }
                }
                return Ok(());
            }
            prompt::Input::Interrupt => continue,
            prompt::Input::Text(inp) => inp,
        };
        if inp.trim().is_empty() {
            continue;
        }
        if inp.trim_start().starts_with("\\") {
            use backslash::ExecuteResult::*;
            let cmd = match backslash::parse(&inp) {
                Ok(cmd) => cmd,
                Err(e) => {
                    eprintln!("Error parsing backslash command: {}",
                              e.message);
                    // Quick-edit command on error
                    initial = inp.trim_start().into();
                    continue;
                }
            };
            let exec_res = backslash::execute(&mut cli, cmd, &mut state).await;
            match exec_res {
                Ok(Skip) => continue,
                Ok(Input(text)) => initial = text,
                Err(e) => {
                    if e.is::<backslash::ChangeDb>() {
                        return Err(e);
                    }
                    eprintln!("Error executing command: {}", e);
                    // Quick-edit command on error
                    initial = inp.trim_start().into();
                    state.last_error = Some(e);
                }
            }
            continue;
        }
        let mut headers = HashMap::new();
        if let Some(implicit_limit) = state.implicit_limit {
            headers.insert(
                QUERY_OPT_IMPLICIT_LIMIT,
                Bytes::from(format!("{}", implicit_limit+1)));
        }

        cli.send_messages(&[
            ClientMessage::Prepare(Prepare {
                headers,
                io_format: match state.output_mode {
                    Default | TabSeparated => IoFormat::Binary,
                    Json => IoFormat::Json,
                    JsonElements => IoFormat::JsonElements,
                },
                expected_cardinality: Cardinality::Many,
                statement_name: statement_name.clone(),
                command_text: String::from(&inp),
            }),
            ClientMessage::Sync,
        ]).await?;

        loop {
            let msg = cli.reader.message().await?;
            match msg {
                ServerMessage::PrepareComplete(..) => {
                    cli.reader.wait_ready().await?;
                    break;
                }
                ServerMessage::ErrorResponse(err) => {
                    print_query_error(&err, &inp, state.verbose_errors)?;
                    state.last_error = Some(err.into());
                    cli.reader.wait_ready().await?;
                    continue 'input_loop;
                }
                _ => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        }

        cli.send_messages(&[
            ClientMessage::DescribeStatement(DescribeStatement {
                headers: HashMap::new(),
                aspect: DescribeAspect::DataDescription,
                statement_name: statement_name.clone(),
            }),
            ClientMessage::Flush,
        ]).await?;

        let data_description = loop {
            let msg = cli.reader.message().await?;
            match msg {
                ServerMessage::CommandDataDescription(data_desc) => {
                    break data_desc;
                }
                ServerMessage::ErrorResponse(err) => {
                    eprintln!("{}", err.display(state.verbose_errors));
                    state.last_error = Some(err.into());
                    cli.reader.wait_ready().await?;
                    continue 'input_loop;
                }
                _ => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        if options.debug_print_descriptors {
            println!("Descriptor: {:?}", data_description);
        }
        let desc = data_description.output()?;
        let indesc = data_description.input()?;
        if options.debug_print_descriptors {
            println!("InputDescr {:#?}", indesc.descriptors());
            println!("Output Descr {:#?}", desc.descriptors());
        }
        let codec = desc.build_codec()?;
        if options.debug_print_codecs {
            println!("Codec {:#?}", codec);
        }
        let incodec = indesc.build_codec()?;
        if options.debug_print_codecs {
            println!("Input Codec {:#?}", incodec);
        }

        let input = match input_variables(&indesc, state).await {
            Ok(input) => input,
            Err(e) => {
                eprintln!("{:#?}", e);
                state.last_error = Some(e);
                continue 'input_loop;
            }
        };

        let mut arguments = BytesMut::with_capacity(8);
        incodec.encode(&mut arguments, &input)?;

        cli.send_messages(&[
            ClientMessage::Execute(Execute {
                headers: HashMap::new(),
                statement_name: statement_name.clone(),
                arguments: arguments.freeze(),
            }),
            ClientMessage::Sync,
        ]).await?;

        let mut items = cli.reader.response(codec);

        match state.output_mode {
            TabSeparated => {
                while let Some(row) = items.next().await.transpose()? {
                    let mut text = match value_to_tab_separated(&row) {
                        Ok(text) => text,
                        Err(e) => {
                            eprintln!("Error: {}", e);
                            // exhaust the iterator to get connection in the
                            // consistent state
                            while let Some(_) = items.next().await.transpose()?
                            {}
                            continue 'input_loop;
                        }
                    };
                    // trying to make writes atomic if possible
                    text += "\n";
                    stdout().write_all(text.as_bytes()).await?;
                }
            }
            Default => {
                match print_to_stdout(items, &state.print).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            PrintError::StreamErr {
                                source: ReadError::RequestError { ref error, ..},
                                ..
                            } => {
                                eprintln!("{}", error);
                            }
                            _ => eprintln!("{:#?}", e),
                        }
                        state.last_error = Some(e.into());
                        cli.reader.wait_ready().await?;
                        continue;
                    }
                }
            }
            Json | JsonElements => {
                while let Some(row) = items.next().await.transpose()? {
                    let mut text = match row {
                        Value::Str(s) => s,
                        _ => unreachable!(),
                    };
                    // trying to make writes atomic if possible
                    text += "\n";
                    stdout().write_all(text.as_bytes()).await?;
                }
            }
        }
        state.last_error = None;
    }
}

impl<'a> Client<'a> {
    pub async fn scram(&mut self, options: &Options)
        -> Result<(), anyhow::Error>
    {
        use edgedb_protocol::client_message::SaslInitialResponse;
        use edgedb_protocol::client_message::SaslResponse;
        use crate::options::Password::*;

        let password = match options.password {
            NoPassword => return Err(anyhow::anyhow!("Password is required. \
                Please specify --password or --password-from-stdin on the \
                command-line.")),
            FromTerminal => {
                rpassword::read_password_from_tty(
                    Some(&format!("Password for '{}': ",
                                  options.user.escape_default())))?
            }
            Password(ref s) => s.clone(),
        };

        let scram = ScramClient::new(&options.user, &password, None)?;

        let (scram, first) = scram.client_first();
        self.send_messages(&[
            ClientMessage::AuthenticationSaslInitialResponse(
                SaslInitialResponse {
                method: "SCRAM-SHA-256".into(),
                data: Bytes::copy_from_slice(first.as_bytes()),
            }),
        ]).await?;
        let msg = self.reader.message().await?;
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
        self.send_messages(&[
            ClientMessage::AuthenticationSaslResponse(
                SaslResponse {
                    data: Bytes::copy_from_slice(data.as_bytes()),
                }),
        ]).await?;
        let msg = self.reader.message().await?;
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
            let msg = self.reader.message().await?;
            match msg {
                ServerMessage::Authentication(Authentication::Ok) => break,
                msg => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            };
        }
        Ok(())
    }

    async fn send_messages<'x, I>(&mut self, msgs: I)
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

    pub async fn execute<S>(&mut self, request: S)
        -> Result<Bytes, anyhow::Error>
        where S: ToString,
    {
        self.send_messages(&[
            ClientMessage::ExecuteScript(ExecuteScript {
                headers: HashMap::new(),
                script_text: request.to_string(),
            }),
        ]).await?;
        let status = loop {
            match self.reader.message().await? {
                ServerMessage::CommandComplete(c) => {
                    self.reader.wait_ready().await?;
                    break c.status_data;
                }
                ServerMessage::ErrorResponse(err) => {
                    return Err(anyhow::anyhow!(err));
                }
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

    pub async fn query<R>(&mut self, request: &str, arguments: &Value)
        -> Result<
            QueryResponse<'_, &'a ByteStream, QueryableDecoder<R>>,
            anyhow::Error
        >
        where R: Queryable,
    {
        let desc = self._query(request, arguments, IoFormat::Binary).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                R::check_descriptor(
                    &desc.as_queryable_context(), root_pos)?;
                Ok(self.reader.response(QueryableDecoder::new()))
            }
            None => {
                Err(NoResultExpected {
                    completion_message: self._process_exec().await?
                })?
            }
        }
    }

    pub async fn query_json(&mut self, request: &str, arguments: &Value)
        -> Result<
            QueryResponse<'_, &'a ByteStream, QueryableDecoder<String>>,
            anyhow::Error
        >
    {
        let desc = self._query(request, arguments, IoFormat::Json).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                String::check_descriptor(
                    &desc.as_queryable_context(), root_pos)?;
                Ok(self.reader.response(QueryableDecoder::new()))
            }
            None => {
                Err(NoResultExpected {
                    completion_message: self._process_exec().await?
                })?
            }
        }
    }

    pub async fn query_json_els(&mut self, request: &str, arguments: &Value)
        -> Result<
            QueryResponse<'_, &'a ByteStream, QueryableDecoder<String>>,
            anyhow::Error
        >
    {
        let desc = self._query(request, arguments,
            IoFormat::JsonElements).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                String::check_descriptor(
                    &desc.as_queryable_context(), root_pos)?;
                Ok(self.reader.response(QueryableDecoder::new()))
            }
            None => {
                Err(NoResultExpected {
                    completion_message: self._process_exec().await?
                })?
            }
        }
    }

    pub async fn query_dynamic(&mut self, request: &str, arguments: &Value)
        -> Result<
            QueryResponse<'_, &'a ByteStream, Arc<dyn Codec>>,
            anyhow::Error
        >
    {
        let desc = self._query(request, arguments, IoFormat::Binary).await?;
        let codec = desc.build_codec()?;
        Ok(self.reader.response(codec))
    }

    async fn _process_exec(&mut self) -> Result<Bytes, anyhow::Error> {
        let status = loop {
            match self.reader.message().await? {
                ServerMessage::CommandComplete(c) => {
                    self.reader.wait_ready().await?;
                    break c.status_data;
                }
                ServerMessage::ErrorResponse(err) => {
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

    #[allow(dead_code)]
    pub async fn execute_args(&mut self, request: &str, arguments: &Value)
        -> Result<Bytes, anyhow::Error>
    {
        self._query(request, arguments, IoFormat::Binary).await?;
        return self._process_exec().await;
    }
}

fn value_to_string(v: &Value) -> Result<String, anyhow::Error> {
    use edgedb_protocol::value::Value::*;
    match v {
        Nothing => Ok(String::new()),
        Uuid(uuid) => Ok(uuid.to_string()),
        Str(s) => Ok(s.clone()),
        Int16(v) => Ok(v.to_string()),
        Int32(v) => Ok(v.to_string()),
        Int64(v) => Ok(v.to_string()),
        Float32(v) => Ok(v.to_string()),
        Float64(v) => Ok(v.to_string()),
        Bool(v) => Ok(v.to_string()),
        Json(v) => Ok(v.to_string()),
        Enum(v) => Ok(v.to_string()),
        | Datetime(_) // TODO(tailhook)
        | BigInt(_) // TODO(tailhook)
        | Decimal(_) // TODO(tailhook)
        | LocalDatetime(_) // TODO(tailhook)
        | LocalDate(_) // TODO(tailhook)
        | LocalTime(_) // TODO(tailhook)
        | Duration(_) // TODO(tailhook)
        | Bytes(_)
        | Object {..}
        | NamedTuple {..}
        | Array(_)
        | Set(_)
        | Tuple(_)
        => {
            Err(anyhow::anyhow!(
                "Complex objects like {:?} cannot be printed tab-separated",
                v))
        }
    }
}

fn value_to_tab_separated(v: &Value) -> Result<String, anyhow::Error> {
    use edgedb_protocol::value::Value::*;
    match v {
        Object { shape, fields } => {
            Ok(shape.elements.iter().zip(fields)
                .filter(|(s, _)| !s.flag_implicit)
                .map(|(_, v)| match v {
                    Some(v) => value_to_string(v),
                    None => Ok(String::new()),
                })
                .collect::<Result<Vec<_>,_>>()?.join("\t"))
        }
        _ => value_to_string(v),
    }
}

pub async fn non_interactive_main(options: Options)
    -> Result<(), anyhow::Error>
{
    use crate::repl::OutputMode::*;
    let mut conn = Connection::from_options(&options).await?;
    let mut cli = conn.authenticate(&options, &options.database).await?;
    let stdin_obj = stdin();
    let mut stdin = stdin_obj.lock().await; // only lock *after* authentication
    let mut inbuf = BytesMut::with_capacity(8192);
    loop {
        let stmt = match ReadStatement::new(&mut inbuf, &mut stdin).await {
            Ok(chunk) => chunk,
            Err(e) if e.is::<EndOfFile>() => break,
            Err(e) => return Err(e),
        };
        let stmt = str::from_utf8(&stmt[..])
            .context("can't decode statement")?;
        match options.output_mode {
            TabSeparated => {
                let mut items = match
                    cli.query_dynamic(stmt, &Value::empty_tuple()).await
                {
                    Ok(items) => items,
                    Err(e) => match e.downcast::<NoResultExpected>() {
                        Ok(e) => {
                            eprintln!("  -> {}: Ok",
                                String::from_utf8_lossy(
                                    &e.completion_message[..]));
                            continue;
                        }
                        Err(e) => Err(e)?,
                    },
                };
                while let Some(row) = items.next().await.transpose()? {
                    let mut text = value_to_tab_separated(&row)?;
                    // trying to make writes atomic if possible
                    text += "\n";
                    stdout().write_all(text.as_bytes()).await?;
                }
            }
            Default => {
                let items = match
                    cli.query_dynamic(stmt, &Value::empty_tuple()).await
                {
                    Ok(items) => items,
                    Err(e) => match e.downcast::<NoResultExpected>() {
                        Ok(e) => {
                            eprintln!("  -> {}: Ok",
                                String::from_utf8_lossy(
                                    &e.completion_message[..]));
                            continue;
                        }
                        Err(e) => Err(e)?,
                    },
                };
                match print_to_stdout(items, &print::Config::new()).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            PrintError::StreamErr {
                                source: ReadError::RequestError {
                                    ref error, ..
                                },
                                ..
                            } => {
                                eprintln!("{}", error);
                            }
                            _ => eprintln!("{:#?}", e),
                        }
                        continue;
                    }
                }
            }
            JsonElements => {
                let mut items = match
                    cli.query_json_els(stmt, &Value::empty_tuple()).await
                {
                    Ok(items) => items,
                    Err(e) => match e.downcast::<NoResultExpected>() {
                        Ok(e) => {
                            eprintln!("  -> {}: Ok",
                                String::from_utf8_lossy(
                                    &e.completion_message[..]));
                            continue;
                        }
                        Err(e) => Err(e)?,
                    },
                };
                while let Some(mut row) = items.next().await.transpose()? {
                    // trying to make writes atomic if possible
                    row += "\n";
                    stdout().write_all(row.as_bytes()).await?;
                }
            }
            Json => {
                let mut items = match
                    cli.query_json(stmt, &Value::empty_tuple()).await
                {
                    Ok(items) => items,
                    Err(e) => match e.downcast::<NoResultExpected>() {
                        Ok(e) => {
                            eprintln!("  -> {}: Ok",
                                String::from_utf8_lossy(
                                    &e.completion_message[..]));
                            continue;
                        }
                        Err(e) => Err(e)?,
                    },
                };
                while let Some(mut row) = items.next().await.transpose()? {
                    // trying to make writes atomic if possible
                    row += "\n";
                    stdout().write_all(row.as_bytes()).await?;
                }
            }
        }
    }
    Ok(())
}

impl std::error::Error for NoResultExpected {}

impl fmt::Display for NoResultExpected {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "no result expected: {}",
            String::from_utf8_lossy(&self.completion_message[..]))
    }
}
