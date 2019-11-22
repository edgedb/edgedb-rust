use std::fs;
use std::collections::HashMap;
use std::error::Error;

use bytes::{Bytes, BytesMut};

use edgedb_protocol::message::{Message, ClientHandshake};
use edgedb_protocol::message::{ServerHandshake};
use edgedb_protocol::message::{ErrorResponse, ErrorSeverity};
use edgedb_protocol::message::{ReadyForCommand, TransactionState};

macro_rules! encoding_eq {
    ($message: expr, $bytes: expr) => {
        let mut bytes = BytesMut::new();
        $message.encode(&mut bytes)?;
        let bytes = bytes.freeze();
        assert_eq!(&bytes[..], $bytes);
        assert_eq!(Message::decode(&bytes)?, $message);
    }
}
macro_rules! map {
    ($($key:expr => $value:expr),*) => {
        {
            #[allow(unused_mut)]
            let mut h = HashMap::new();
            $(
                h.insert($key, $value);
            )*
            h
        }
    }
}


#[test]
fn client_handshake() -> Result<(), Box<dyn Error>> {
    encoding_eq!(Message::ClientHandshake(ClientHandshake {
        major_ver: 1,
        minor_ver: 2,
        params: HashMap::new(),
        extensions: HashMap::new(),
    }), b"\x56\x00\x00\x00\x0C\x00\x01\x00\x02\x00\x00\x00\x00");
    Ok(())
}

#[test]
fn server_handshake() -> Result<(), Box<dyn Error>> {
    encoding_eq!(Message::ServerHandshake(ServerHandshake {
        major_ver: 1,
        minor_ver: 0,
        extensions: HashMap::new(),
    }), b"v\0\0\0\n\0\x01\0\0\0\0");
    Ok(())
}

#[test]
fn ready_for_command() -> Result<(), Box<dyn Error>> {
    encoding_eq!(Message::ReadyForCommand(ReadyForCommand {
        transaction_state: TransactionState::NotInTransaction,
        headers: HashMap::new(),
    }), b"Z\0\0\0\x07\0\0I");
    Ok(())
}

#[test]
fn error_response() -> Result<(), Box<dyn Error>> {
    encoding_eq!(Message::ErrorResponse(ErrorResponse {
        severity: ErrorSeverity::Error,
        code: 50397184,
        message: String::from("missing required connection parameter \
                               in ClientHandshake message: \"user\""),
        headers: map!{
            257 => Bytes::from_static("Traceback (most recent call last):\n  File \"edb/server/mng_port/edgecon.pyx\", line 1077, in edb.server.mng_port.edgecon.EdgeConnection.main\n    await self.auth()\n  File \"edb/server/mng_port/edgecon.pyx\", line 178, in auth\n    raise errors.BinaryProtocolError(\nedb.errors.BinaryProtocolError: missing required connection parameter in ClientHandshake message: \"user\"\n".as_bytes())
        },
    }), &fs::read("tests/error_response.bin")?[..]);
    Ok(())
}
