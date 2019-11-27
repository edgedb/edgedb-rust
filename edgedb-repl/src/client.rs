use std::io;
use std::collections::HashMap;

use anyhow;
use async_std::io::prelude::WriteExt;
use async_std::net::{TcpStream};
use async_std::sync::{Sender, Receiver};
use bytes::{Bytes, BytesMut, BufMut, Buf};

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::client_message::{Prepare, IoFormat, Cardinality};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{Execute};
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use edgedb_protocol::descriptors::{Descriptor};
use edgedb_protocol::codec::{build_codec};
use crate::reader::Reader;
use crate::prompt;


pub async fn interactive_main(data: Receiver<prompt::Input>,
        control: Sender<prompt::Control>)
    -> Result<(), anyhow::Error>
{
    let db_name = "tutorial";

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
    'input_loop: loop {
        control.send(prompt::Control::Input(db_name.into())).await;
        let inp = match data.recv().await {
            None | Some(prompt::Input::Eof) => return Ok(()),
            Some(prompt::Input::Interrupt) => continue,
            Some(prompt::Input::Text(inp)) => inp,
        };

        bytes.truncate(0);
        ClientMessage::Prepare(Prepare {
            headers: HashMap::new(),
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
            statement_name: statement_name.clone(),
            command_text: String::from(inp),
        }).encode(&mut bytes)?;
        ClientMessage::Sync.encode(&mut bytes)?;
        stream.write_all(&bytes[..]).await?;

        loop {
            let msg = reader.message().await?;
            match msg {
                ServerMessage::PrepareComplete(..) => {}
                ServerMessage::ErrorResponse(err) => {
                    eprintln!("{}", err);
                    reader.wait_ready().await?;
                    continue 'input_loop;
                }
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
                ServerMessage::ErrorResponse(err) => {
                    eprintln!("{}", err);
                    reader.wait_ready().await?;
                    continue 'input_loop;
                }
                ServerMessage::ReadyForCommand(..) => {
                    if let Some(desc) = tmp_desc {
                        break desc;
                    } else {
                        eprintln!("PROTOCOL ERROR: Got no description");
                        reader.wait_ready().await?;
                        continue 'input_loop;
                    }
                }
                _ => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        println!("Descriptor: {:?}", data_description);
        let mut cur = io::Cursor::new(data_description.output_typedesc);
        let mut desc = Vec::new();
        while cur.bytes() != b"" {
            desc.push(Descriptor::decode(&mut cur)?);
        }
        let codec = build_codec(&data_description.output_typedesc_id, &desc)?;
        println!("Codec {:?}", codec);

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
                    for chunk in data.data {
                        let mut cur = io::Cursor::new(chunk);
                        let value = codec.decode(&mut cur);
                        assert!(cur.bytes() == b"");
                        println!("Row {:?}", value);
                    }
                }
                ServerMessage::CommandComplete(..) => {
                    reader.wait_ready().await?;
                    break;
                }
                ServerMessage::ReadyForCommand(..) => break,
                _ => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        }
    }
}
