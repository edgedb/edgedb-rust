use std::error::Error;
use std::collections::HashMap;

use bytes::BytesMut;
use async_std::task;
use async_std::net::{TcpStream};
use async_std::io::prelude::WriteExt;

use edgedb_protocol::message::{Message, ClientHandshake};
use crate::reader::Reader;

mod reader;

fn main() -> Result<(), Box<dyn Error>> {
    task::block_on(run_repl())
}

async fn run_repl() -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:5656").await?;

    let mut bytes = BytesMut::new();
    Message::ClientHandshake(ClientHandshake {
        major_ver: 1,
        minor_ver: 1,
        params: HashMap::new(),
        extensions: HashMap::new(),
    }).encode(&mut bytes)?;

    stream.write_all(&bytes[..]).await?;
    let mut reader = Reader::new(&stream);
    let msg = reader.message().await?;
    if let Message::ServerHandshake {..} = msg {
        println!("Handshake {:?}", msg);
        let msg = reader.message().await?;
        println!("Message {:?}", msg);
    }

    Ok(())
}
