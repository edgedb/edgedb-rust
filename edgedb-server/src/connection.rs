use std::io;
use std::collections::HashMap;

use async_listen::{ByteStream};

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::server_message::{ServerMessage, ServerHandshake};
use edgedb_protocol::server_message::{Authentication};
use edgedb_protocol::server_message::{ReadyForCommand, TransactionState};
use crate::reader::Reader;
use crate::writer::Writer;

pub trait ConnectionErr {
    type Output;
    fn connection_err(self, name: &str) -> Result<Self::Output, ()>;
}

impl<T> ConnectionErr for Result<T, io::Error> {
    type Output = T;
    fn connection_err(self, name: &str) -> Result<Self::Output, ()> {
        self.map_err(|e| {
            if is_important_error(&e) {
                log::error!("{}: {}", name, e);
            } else {
                log::debug!("{}: Network error: {}", name, e);
            }
        })
    }
}

fn is_important_error(e: &io::Error) -> bool {
    match e.kind() {
        io::ErrorKind::BrokenPipe => false,
        io::ErrorKind::TimedOut => false,
        io::ErrorKind::ConnectionReset => false,
        io::ErrorKind::ConnectionAborted => false,
        _ => true,
    }
}

pub async fn connection_loop(stream: ByteStream) -> Result<(), ()> {
    let log_name = stream.peer_addr()
        .map(|a| format!("<peer: {}>", a))
        .unwrap_or_else(|e| {
            log::debug!("Can't get peer name: {}", e);
            String::from("<unidentified-peer>")
        });
    let (tx, rx) = (&stream, &stream);
    let mut rx = Reader::new(rx, &log_name);
    let mut tx = Writer::new(tx, &log_name);
    let handshake = rx.message().await?;
    match handshake {
        ClientMessage::ClientHandshake(ClientHandshake {
            major_ver: 0,
            minor_ver: 7,
            params, extensions,
        }) if extensions.len() == 0 => {
            eprintln!("Params {:?}", params);
        }
        ClientMessage::ClientHandshake(_) => {
            tx.send_message(ServerMessage::ServerHandshake(ServerHandshake {
                major_ver: 0,
                minor_ver: 7,
                extensions: HashMap::new(),
            })).await?;
        }
        msg => {
            eprintln!("Bad message {:?}", msg);
            return Err(());
        }
    }
    // TODO(tailhook) implement authentication
    // TODO(tailhook) optimize sends
    tx.send_message(ServerMessage::Authentication(Authentication::Ok)).await?;
    tx.send_message(ServerMessage::ReadyForCommand(ReadyForCommand {
        headers: HashMap::new(),
        transaction_state: TransactionState::NotInTransaction,
    })).await?;
    loop {
        let msg = rx.message().await?;
        log::error!("Unimplemented: {:?}", msg);
        return Err(());
    }
}
