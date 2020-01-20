use std::io;
use std::collections::HashMap;
use std::str;
use std::mem::replace;

use anyhow;
use async_std::io::prelude::WriteExt;
use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::sync::{Sender, Receiver};
use bytes::{Bytes, BytesMut, BufMut};
use scram::ScramClient;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::client_message::{Prepare, IoFormat, Cardinality};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{Execute, ExecuteScript};
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::queryable::{Queryable};
use crate::reader::{Reader, ReadError, QueryableDecoder, QueryResponse};
use crate::options::Options;
use crate::print::print_to_stdout;
use crate::prompt;
use crate::commands::backslash;

pub struct Connection {
    stream: TcpStream,
}

pub struct Client<'a> {
    stream: &'a TcpStream,
    reader: Reader<&'a TcpStream>,
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
        let mut cli = Client { stream, reader };
        let mut bytes = BytesMut::new();
        let mut params = HashMap::new();
        params.insert(String::from("user"), options.user.clone());
        params.insert(String::from("database"), options.database.clone());

        ClientMessage::ClientHandshake(ClientHandshake {
            major_ver: 0,
            minor_ver: 7,
            params,
            extensions: HashMap::new(),
        }).encode(&mut bytes)?;

        cli.stream.write_all(&bytes[..]).await?;
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
                ServerMessage::ParameterStatus(_) => {
                    // TODO(tailhook) should we read any params?
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
        control: Sender<prompt::Control>)
    -> Result<(), anyhow::Error>
{
    let mut conn = Connection::from_options(&options).await?;
    let mut cli = conn.authenticate(&options).await?;
    let mut bytes = BytesMut::new();
    let mut initial = String::new();
    let statement_name = Bytes::from_static(b"");

    'input_loop: loop {
        control.send(prompt::Control::Input(
            options.database.clone(),
            replace(&mut initial, String::new()),
        )).await;
        let inp = match data.recv().await {
            None | Some(prompt::Input::Eof) => {
                bytes.truncate(0);
                ClientMessage::Terminate.encode(&mut bytes)?;
                cli.stream.write_all(&bytes[..]).await?;
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
            if let Err(e) = backslash::execute(&mut cli, cmd).await {
                eprintln!("Error executing command: {}", e);
                // Quick-edit command on error
                initial = inp.trim_start().into();
            }
            continue;
        }

        bytes.truncate(0);
        ClientMessage::Prepare(Prepare {
            headers: HashMap::new(),
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
            statement_name: statement_name.clone(),
            command_text: String::from(inp),
        }).encode(&mut bytes)?;
        ClientMessage::Sync.encode(&mut bytes)?;
        cli.stream.write_all(&bytes[..]).await?;

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

        bytes.truncate(0);
        ClientMessage::DescribeStatement(DescribeStatement {
            headers: HashMap::new(),
            aspect: DescribeAspect::DataDescription,
            statement_name: statement_name.clone(),
        }).encode(&mut bytes)?;
        ClientMessage::Flush.encode(&mut bytes)?;
        cli.stream.write_all(&bytes[..]).await?;

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

        bytes.truncate(0);
        ClientMessage::Execute(Execute {
            headers: HashMap::new(),
            statement_name: statement_name.clone(),
            arguments: arguments.freeze(),
        }).encode(&mut bytes)?;
        ClientMessage::Sync.encode(&mut bytes)?;
        cli.stream.write_all(&bytes[..]).await?;

        print_to_stdout(cli.reader.response(codec)).await?;
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

        let mut bytes = BytesMut::new();
        let (scram, first) = scram.client_first();
        ClientMessage::AuthenticationSaslInitialResponse(SaslInitialResponse {
            method: "SCRAM-SHA-256".into(),
            data: Bytes::copy_from_slice(first.as_bytes()),
        }).encode(&mut bytes)?;
        self.stream.write_all(&bytes[..]).await?;
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
        bytes.clear();
        ClientMessage::AuthenticationSaslResponse(SaslResponse {
            data: Bytes::copy_from_slice(data.as_bytes()),
        }).encode(&mut bytes)?;
        self.stream.write_all(&bytes[..]).await?;
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

    pub async fn execute<S>(&mut self, request: S)
        -> Result<Bytes, anyhow::Error>
        where S: ToString,
    {
        let mut bytes = BytesMut::new();
        bytes.truncate(0);
        ClientMessage::ExecuteScript(ExecuteScript {
            headers: HashMap::new(),
            script_text: request.to_string(),
        }).encode(&mut bytes)?;
        self.stream.write_all(&bytes[..]).await?;
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
        let mut bytes = BytesMut::new();
        let statement_name = Bytes::from_static(b"");

        ClientMessage::Prepare(Prepare {
            headers: HashMap::new(),
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
            statement_name: statement_name.clone(),
            command_text: String::from(request),
        }).encode(&mut bytes)?;
        ClientMessage::Flush.encode(&mut bytes)?;
        self.stream.write_all(&bytes[..]).await?;

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

        bytes.truncate(0);
        ClientMessage::DescribeStatement(DescribeStatement {
            headers: HashMap::new(),
            aspect: DescribeAspect::DataDescription,
            statement_name: statement_name.clone(),
        }).encode(&mut bytes)?;
        ClientMessage::Flush.encode(&mut bytes)?;
        self.stream.write_all(&bytes[..]).await?;

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

        bytes.truncate(0);
        ClientMessage::Execute(Execute {
            headers: HashMap::new(),
            statement_name: statement_name.clone(),
            arguments: arguments.freeze(),
        }).encode(&mut bytes)?;
        ClientMessage::Sync.encode(&mut bytes)?;
        self.stream.write_all(&bytes[..]).await?;

        return Ok(self.reader.response(QueryableDecoder::new()));
    }
}
