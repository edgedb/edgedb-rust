use std::collections::HashMap;
use std::error::Error;

use bytes::{BytesMut};

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::client_message::{ExecuteScript};

macro_rules! encoding_eq {
    ($message: expr, $bytes: expr) => {
        let data: &[u8] = $bytes;
        let mut bytes = BytesMut::new();
        $message.encode(&mut bytes)?;
        println!("Serialized bytes {:?}", bytes);
        let bytes = bytes.freeze();
        assert_eq!(&bytes[..], data);
        assert_eq!(ClientMessage::decode(&data.into())?, $message);
    }
}

#[test]
fn client_handshake() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ClientMessage::ClientHandshake(ClientHandshake {
        major_ver: 1,
        minor_ver: 2,
        params: HashMap::new(),
        extensions: HashMap::new(),
    }), b"\x56\x00\x00\x00\x0C\x00\x01\x00\x02\x00\x00\x00\x00");
    Ok(())
}

#[test]
fn execute_script() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ClientMessage::ExecuteScript(ExecuteScript {
        headers: HashMap::new(),
        script_text: String::from("START TRANSACTION"),
    }), b"Q\0\0\0\x1b\0\0\0\0\0\x11START TRANSACTION");
    Ok(())
}
