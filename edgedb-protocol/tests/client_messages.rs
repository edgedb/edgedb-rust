use std::fs;
use std::collections::HashMap;
use std::error::Error;

use bytes::{Bytes, BytesMut};

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};

macro_rules! encoding_eq {
    ($message: expr, $bytes: expr) => {
        let data: &[u8] = $bytes;
        assert_eq!(ClientMessage::decode(&data.into())?, $message);
        let mut bytes = BytesMut::new();
        $message.encode(&mut bytes)?;
        let bytes = bytes.freeze();
        assert_eq!(&bytes[..], data);
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
    encoding_eq!(ClientMessage::ClientHandshake(ClientHandshake {
        major_ver: 1,
        minor_ver: 2,
        params: HashMap::new(),
        extensions: HashMap::new(),
    }), b"\x56\x00\x00\x00\x0C\x00\x01\x00\x02\x00\x00\x00\x00");
    Ok(())
}
