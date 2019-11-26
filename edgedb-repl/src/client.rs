use std::collections::HashMap;
use std::process::exit;

use anyhow;
use async_std::io::prelude::WriteExt;
use async_std::net::{TcpStream};
use async_std::task;
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
        println!("Handshake {:?}", msg);
        // TODO(tailhook) react on this somehow
        msg = reader.message().await?;
    }
    if let ServerMessage::Authentication(Authentication::Ok) = msg {
    } else {
        eprintln!("Error authenticating: {:?}", msg);
        exit(1);
    }

    loop {
        let msg = reader.message().await?;
        println!("message: {:?}", msg);
        match msg {
            ServerMessage::ReadyForCommand(..) => break,
            _ => continue,  // TODO(tailhook) consume msgs
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
            println!("message: {:?}", msg);
            match msg {
                ServerMessage::PrepareComplete(..) => {}
                ServerMessage::ReadyForCommand(..) => break,
                _ => continue,  // TODO(tailhook) consume msgs
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

        loop {
            let msg = reader.message().await?;
            println!("message: {:?}", msg);
            match msg {
                ServerMessage::CommandDataDescription(..) => {}
                ServerMessage::ReadyForCommand(..) => break,
                _ => continue,  // TODO(tailhook) consume msgs
            }
        }

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

        control.send(prompt::Control::Input(db_name.into())).await;
    }
}
