use std::collections::HashMap;

use anyhow;
use async_std::io::prelude::WriteExt;
use async_std::net::{TcpStream};
use async_std::sync::{Sender, Receiver};
use bytes::{Bytes, BytesMut, BufMut};

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::client_message::{Prepare, IoFormat, Cardinality};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{Execute};
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use crate::reader::Reader;
use crate::prompt;


pub async fn interactive_main(data: Receiver<prompt::Input>,
        control: Sender<prompt::Control>)
    -> Result<(), anyhow::Error>
{
    let db_name = "edgedb";

    let stream = TcpStream::connect("127.0.0.1:5656").await?;
    let (rd, mut stream) = (&stream, &stream);
    let mut reader = Reader::new(rd);

    let mut bytes = BytesMut::new();
    let mut params = HashMap::new();
    params.insert(String::from("user"), String::from("edgedb"));
    params.insert(String::from("database"), String::from(db_name));

    ClientMessage::ClientHandshake(ClientHandshake {
        major_ver: 1,
        minor_ver: 0,
        params,
        extensions: HashMap::new(),
    }).encode(&mut bytes)?;

    stream.write_all(&bytes[..]).await?;
    let mut msg = reader.message().await?;
    if let ServerMessage::ServerHandshake {..} = msg {
        eprintln!("WARNING: Connection negotiantion issue {:?}", msg);
        // TODO(tailhook) react on this somehow
        msg = reader.message().await?;
    }
    if let ServerMessage::Authentication(Authentication::Ok) = msg {
    } else {
        return Err(anyhow::anyhow!("Error authenticating: {:?}", msg));
    }

    loop {
        let msg = reader.message().await?;
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

    let statement_name = Bytes::from_static(b"");
    control.send(prompt::Control::Input(db_name.into())).await;
    loop {
        let inp = match data.recv().await {
            None | Some(prompt::Input::Eof) => return Ok(()),
            Some(prompt::Input::Interrupt) => continue,
            Some(prompt::Input::Text(inp)) => inp,
        };

        bytes.truncate(0);
        ClientMessage::Prepare(Prepare {
            headers: HashMap::new(),
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::One,
            statement_name: statement_name.clone(),
            command_text: String::from(inp),
        }).encode(&mut bytes)?;
        ClientMessage::Sync.encode(&mut bytes)?;
        stream.write_all(&bytes[..]).await?;

        loop {
            let msg = reader.message().await?;
            match msg {
                ServerMessage::PrepareComplete(..) => {}
                ServerMessage::ReadyForCommand(..) => break,
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
        ClientMessage::Sync.encode(&mut bytes)?;
        stream.write_all(&bytes[..]).await?;

        let mut tmp_desc = None;
        let data_description = loop {
            let msg = reader.message().await?;
            match msg {
                ServerMessage::CommandDataDescription(data_desc) => {
                    if tmp_desc.is_some() {
                        eprintln!("WARNING: two data descriptions?");
                    }
                    tmp_desc = Some(data_desc);
                }
                ServerMessage::ReadyForCommand(..) => {
                    if let Some(desc) = tmp_desc {
                        break desc;
                    }
                }
                _ => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };

        let mut arguments = BytesMut::with_capacity(8);
        // empty tuple
        arguments.put_u32_be(0);

        bytes.truncate(0);
        ClientMessage::Execute(Execute {
            headers: HashMap::new(),
            statement_name: statement_name.clone(),
            arguments: arguments.freeze(),
        }).encode(&mut bytes)?;
        ClientMessage::Sync.encode(&mut bytes)?;
        stream.write_all(&bytes[..]).await?;

        loop {
            let msg = reader.message().await?;
            match msg {
                ServerMessage::Data(data) => {
                    println!("DATA {:?}", data);
                }
                ServerMessage::CommandComplete(..) => {
                    // TODO(tailhook) flag state somehow?
                }
                ServerMessage::ReadyForCommand(..) => break,
                _ => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        }

        control.send(prompt::Control::Input(db_name.into())).await;
    }
}
