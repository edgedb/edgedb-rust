use std::io;
use std::fmt;
use std::collections::HashMap;
use std::str;
use std::mem::replace;

use anyhow::{self, Context};
use async_std::prelude::StreamExt;
use async_std::io::{stdin, stdout};
use async_std::io::prelude::WriteExt;
use async_std::net::{TcpStream, ToSocketAddrs};
use bytes::{Bytes, BytesMut};
use scram::ScramClient;
use serde_json::from_slice;
use typemap::TypeMap;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::client_message::{Prepare, IoFormat, Cardinality};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{Execute, ExecuteScript};
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::queryable::{Queryable};
use edgedb_protocol::value::Value;
use edgedb_protocol::descriptors::OutputTypedesc;
use crate::commands::backslash;
use crate::options::Options;
use crate::print::{print_to_stdout, PrintError};
use crate::prompt;
use crate::reader::{Reader, ReadError, QueryableDecoder, QueryResponse};
use crate::repl;
use crate::server_params::PostgresAddress;
use crate::statement::{ReadStatement, EndOfFile};
use crate::variables::input_variables;

pub struct Connection {
    stream: TcpStream,
}

pub struct Client<'a> {
    stream: &'a TcpStream,
    outbuf: BytesMut,
    reader: Reader<&'a TcpStream>,
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
        Ok(Connection {
            stream: TcpStream::connect(addrs).await?,
        })
    }
    pub async fn from_options(options: &Options)
        -> Result<Connection, io::Error>
    {
        Ok(Connection::new((&options.host[..], options.port)).await?)
    }

    pub async fn authenticate<'x>(&'x mut self, options: &Options)
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
        params.insert(String::from("database"), options.database.clone());

        cli.send_message(&ClientMessage::ClientHandshake(ClientHandshake {
            major_ver: 0,
            minor_ver: 7,
            params,
            extensions: HashMap::new(),
        })).await?;

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


pub async fn interactive_main(mut options: Options, mut state: repl::State)
    -> Result<(), anyhow::Error>
{
    loop {
        let mut conn = Connection::from_options(&options).await?;
        let cli = conn.authenticate(&options).await?;
        match _interactive_main(cli, &options, &mut state).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if let Some(err) = e.downcast_ref::<backslash::ChangeDb>() {
                    options.database = err.target.clone();
                    continue;
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
    let mut initial = String::new();
    let statement_name = Bytes::from_static(b"");

    'input_loop: loop {
        let inp = match
            state.edgeql_input(
                &options.database,
                &replace(&mut initial, String::new()),
            ).await
        {
            prompt::Input::Eof => {
                cli.send_message(&ClientMessage::Terminate).await?;
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
            if let Err(e) = exec_res {
                if e.is::<backslash::ChangeDb>() {
                    return Err(e);
                }
                eprintln!("Error executing command: {}", e);
                // Quick-edit command on error
                initial = inp.trim_start().into();
                state.last_error = Some(e);
            }
            continue;
        }

        cli.send_message(&ClientMessage::Prepare(Prepare {
            headers: HashMap::new(),
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
            statement_name: statement_name.clone(),
            command_text: String::from(inp),
        })).await?;
        // TODO(tailhook) optimize
        cli.send_message(&ClientMessage::Sync).await?;

        loop {
            let msg = cli.reader.message().await?;
            match msg {
                ServerMessage::PrepareComplete(..) => {
                    cli.reader.wait_ready().await?;
                    break;
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
        }

        cli.send_message(&ClientMessage::DescribeStatement(DescribeStatement {
            headers: HashMap::new(),
            aspect: DescribeAspect::DataDescription,
            statement_name: statement_name.clone(),
        })).await?;
        cli.send_message(&ClientMessage::Flush).await?;

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
            println!("Input Codec {:#?}", codec);
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

        cli.send_message(&ClientMessage::Execute(Execute {
            headers: HashMap::new(),
            statement_name: statement_name.clone(),
            arguments: arguments.freeze(),
        })).await?;
        cli.send_message(&ClientMessage::Sync).await?;

        match print_to_stdout(cli.reader.response(codec), &state.print).await {
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
        self.send_message(&ClientMessage::AuthenticationSaslInitialResponse(
                SaslInitialResponse {
                method: "SCRAM-SHA-256".into(),
                data: Bytes::copy_from_slice(first.as_bytes()),
            })).await?;
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
        self.send_message(&ClientMessage::AuthenticationSaslResponse(
            SaslResponse {
                data: Bytes::copy_from_slice(data.as_bytes()),
            })).await?;
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

    async fn send_message(&mut self, msg: &ClientMessage)
        -> Result<(), anyhow::Error>
    {
        self.outbuf.truncate(0);
        msg.encode(&mut self.outbuf)?;
        self.stream.write_all(&self.outbuf[..]).await?;
        Ok(())
    }

    pub async fn execute<S>(&mut self, request: S)
        -> Result<Bytes, anyhow::Error>
        where S: ToString,
    {
        self.send_message(&ClientMessage::ExecuteScript(ExecuteScript {
            headers: HashMap::new(),
            script_text: request.to_string(),
        })).await?;
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

        self.send_message(&ClientMessage::Prepare(Prepare {
            headers: HashMap::new(),
            io_format,
            expected_cardinality: Cardinality::Many,
            statement_name: statement_name.clone(),
            command_text: String::from(request),
        })).await?;
        // TODO(tailhook) optimize
        self.send_message(&ClientMessage::Sync).await?;

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

        self.send_message(&ClientMessage::DescribeStatement(DescribeStatement {
            headers: HashMap::new(),
            aspect: DescribeAspect::DataDescription,
            statement_name: statement_name.clone(),
        })).await?;
        // TODO(tailhook)
        self.send_message(&ClientMessage::Flush).await?;

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

        self.send_message(&ClientMessage::Execute(Execute {
            headers: HashMap::new(),
            statement_name: statement_name.clone(),
            arguments: arg_buf.freeze(),
        })).await?;
        self.send_message(&ClientMessage::Sync).await?;
        Ok(desc)
    }

    pub async fn query<R>(&mut self, request: &str, arguments: &Value)
        -> Result<
            QueryResponse<'_, &'a TcpStream, QueryableDecoder<R>>,
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
            QueryResponse<'_, &'a TcpStream, QueryableDecoder<String>>,
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

pub async fn non_interactive_main(options: Options)
    -> Result<(), anyhow::Error>
{
    let mut conn = Connection::from_options(&options).await?;
    let mut cli = conn.authenticate(&options).await?;
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
        let mut items = match cli.query_json(stmt, &Value::empty_tuple()).await
        {
            Ok(items) => items,
            Err(e) => match e.downcast::<NoResultExpected>() {
                Ok(e) => {
                    eprintln!("  -> {}: Ok",
                        String::from_utf8_lossy(&e.completion_message[..]));
                    continue;
                }
                Err(e) => Err(e)?,
            },
        };
        let out = stdout();
        let mut out = out.lock().await;
        while let Some(mut row) = items.next().await.transpose()? {
            // trying to make writes atomic if possible
            row += "\n";
            out.write_all(row.as_bytes()).await?;
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
