use std::io;
use std::collections::HashMap;
use std::str;
use std::mem::replace;

use anyhow;
use async_std::io::stdin;
use async_std::io::prelude::WriteExt;
use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::sync::{Receiver};
use bytes::{Bytes, BytesMut, BufMut};
use scram::ScramClient;
use serde_json::from_slice;
use typemap::TypeMap;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::client_message::{Prepare, IoFormat, Cardinality};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{Execute, ExecuteScript};
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::queryable::{Queryable};
use crate::commands::backslash;
use crate::options::Options;
use crate::print::print_to_stdout;
use crate::prompt;
use crate::reader::{Reader, ReadError, QueryableDecoder, QueryResponse};
use crate::repl;
use crate::server_params::PostgresAddress;

pub struct Connection {
    stream: TcpStream,
}

pub struct Client<'a> {
    stream: &'a TcpStream,
    outbuf: BytesMut,
    reader: Reader<&'a TcpStream>,
    pub params: TypeMap<dyn typemap::DebugAny + Send>,
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

pub async fn interactive_main(options: Options, data: Receiver<prompt::Input>,
        mut state: repl::State)
    -> Result<(), anyhow::Error>
{
    let mut conn = Connection::from_options(&options).await?;
    let mut cli = conn.authenticate(&options).await?;
    let mut initial = String::new();
    let statement_name = Bytes::from_static(b"");

    'input_loop: loop {
        state.control.send(prompt::Control::Input(
            options.database.clone(),
            replace(&mut initial, String::new()),
        )).await;
        let inp = match data.recv().await {
            None | Some(prompt::Input::Eof) => {
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
                return Ok(())
            }
            Some(prompt::Input::Interrupt) => continue,
            Some(prompt::Input::Text(inp)) => inp,
        };
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
                eprintln!("Error executing command: {}", e);
                // Quick-edit command on error
                initial = inp.trim_start().into();
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
                    eprintln!("{}", err);
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
                    eprintln!("{}", err);
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
        if options.debug_print_descriptors {
            println!("Descriptors {:#?}", desc.descriptors());
        }
        let codec = desc.build_codec()?;
        if options.debug_print_codecs {
            println!("Codec {:#?}", codec);
        }

        let mut arguments = BytesMut::with_capacity(8);
        // empty tuple
        arguments.put_u32(0);

        cli.send_message(&ClientMessage::Execute(Execute {
            headers: HashMap::new(),
            statement_name: statement_name.clone(),
            arguments: arguments.freeze(),
        })).await?;
        cli.send_message(&ClientMessage::Sync).await?;

        print_to_stdout(cli.reader.response(codec), &state.print).await?;
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
                return Err(anyhow::anyhow!(err));
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

    pub async fn query<R>(&mut self, request: &str)
        -> Result<
            QueryResponse<'_, &'a TcpStream, QueryableDecoder<R>>,
            anyhow::Error
        >
        where R: Queryable,
    {
        let statement_name = Bytes::from_static(b"");

        self.send_message(&ClientMessage::Prepare(Prepare {
            headers: HashMap::new(),
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
            statement_name: statement_name.clone(),
            command_text: String::from(request),
        })).await?;
        // TODO(tailhook) optimize
        self.send_message(&ClientMessage::Flush).await?;

        loop {
            let msg = self.reader.message().await?;
            match msg {
                ServerMessage::PrepareComplete(..) => break,
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
        let root_pos = desc.root_pos()
            .ok_or_else(|| anyhow::anyhow!("no result expected"))?;
        R::check_descriptor(&desc.as_queryable_context(), root_pos)?;

        let mut arguments = BytesMut::with_capacity(8);
        // empty tuple
        arguments.put_u32(0);

        self.send_message(&ClientMessage::Execute(Execute {
            headers: HashMap::new(),
            statement_name: statement_name.clone(),
            arguments: arguments.freeze(),
        })).await?;
        self.send_message(&ClientMessage::Sync).await?;

        return Ok(self.reader.response(QueryableDecoder::new()));
    }
}

pub async fn non_interactive_main(options: Options)
    -> Result<(), anyhow::Error>
{
    let mut conn = Connection::from_options(&options).await?;
    let _cli = conn.authenticate(&options).await?;
    let stdin_obj = stdin();
    let _stdin = stdin_obj.lock(); // only lock *after* authentication
    todo!();
}
