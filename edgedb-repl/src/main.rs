use std::error::Error;
use std::collections::HashMap;
use std::process::exit;

use bytes::BytesMut;
use async_std::task;
use async_std::net::{TcpStream};
use async_std::io::prelude::WriteExt;

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::server_message::{ServerMessage, Authentication};
use crate::reader::Reader;

mod reader;

fn main() -> Result<(), Box<dyn Error>> {
    task::block_on(run_repl())
}

async fn run_repl() -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:5656").await?;

    let mut bytes = BytesMut::new();
    let mut params = HashMap::new();
    params.insert(String::from("user"), String::from("edgedb"));
    params.insert(String::from("database"), String::from("edgedb"));

    ClientMessage::ClientHandshake(ClientHandshake {
        major_ver: 1,
        minor_ver: 0,
        params,
        extensions: HashMap::new(),
    }).encode(&mut bytes)?;

    stream.write_all(&bytes[..]).await?;
    let mut reader = Reader::new(&stream);
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
    }
}
