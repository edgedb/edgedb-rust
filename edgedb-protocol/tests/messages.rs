use std::collections::HashMap;
use std::error::Error;

use bytes::BytesMut;

use edgedb_protocol::message::{Message, ClientHandshake};


#[test]
fn client_handshake() -> Result<(), Box<dyn Error>> {
    let mut bytes = BytesMut::new();
    Message::ClientHandshake(ClientHandshake {
        major_ver: 1,
        minor_ver: 2,
        params: HashMap::new(),
        extensions: HashMap::new(),
    }).encode(&mut bytes)?;
    assert_eq!(&bytes[..],
               b"\x56\x00\x00\x00\x0C\x00\x01\x00\x02\x00\x00\x00\x00");
    Ok(())
}
